/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ttl.h"

#define BATCH_SIZE    1000

ExpirationHandler::ExpirationHandler(SSDB *ssdb) {
	this->ssdb = ssdb;
	this->thread_quit = false;
	this->list_name = EXPIRATION_LIST_KEY;
	this->first_timeout = 0;
	this->start();
}

ExpirationHandler::~ExpirationHandler() {
	Locking l(&this->mutex);
	this->stop();
	ssdb = NULL;
}

void ExpirationHandler::start() {
	thread_quit = false;
	pthread_t tid;
	int err = pthread_create(&tid, NULL, &ExpirationHandler::thread_func, this);
	if (err != 0) {
		log_fatal("can't create thread: %s", strerror(err));
		exit(0);
	}
}

void ExpirationHandler::stop() {
	thread_quit = true;
	for (int i = 0; i < 100; i++) {
		if (!thread_quit) {
			break;
		}
		usleep(100 * 1000);
	}
}


int ExpirationHandler::set_ttl(const Bytes &key, int64_t expired, char log_type){
	char data[30];
	int size = snprintf(data, sizeof(data), "%" PRId64, expired);
	if (size <= 0) {
		log_error("snprintf return error!");
		return -1;
	}
	//将去激活从端的过期队列检测，从端自己检测过期。存在一个问题，当一个key
	//在master被删除前设为不过期，这个设置不过期命令同步到从端延迟了，从端已经
	//删除此key,这时候这个key是在slave消失，数据不一致，解决这个问题的方法目前是
	//从端向master恢复数据
	int ret = this->ssdb->zset_ttl(this->list_name, key, Bytes(data, size), log_type);

	if (ret < 0) {
		return -1;
	}

	if (expired == 0){
		return del_ttl(key, log_type);
	}

	if(expired < first_timeout){
		first_timeout = expired;
	}
	Locking l(&this->mutex);
	std::string s_key = key.String();
	if (!fast_keys.empty() && expired <= fast_keys.max_score()) {
		fast_keys.add(s_key, expired);
		if (fast_keys.size() > BATCH_SIZE) {
			log_debug("pop_back");
			fast_keys.pop_back();
		}
	} else {
		fast_keys.del(s_key);
		//log_debug("don't put in fast_keys");
	}

	return 1;
}

int ExpirationHandler::del_ttl(const Bytes &key, char log_type){
	Locking l(&this->mutex);
    fast_keys.del(key.String());
    return this->ssdb->zdel_ttl(this->list_name, key, log_type);
}

int ExpirationHandler::del_fastkey(const Bytes &key){
	Locking l(&this->mutex);
    fast_keys.del(key.String());
}

int ExpirationHandler::set_fastkey(const Bytes &key, int64_t expired){
	//Locking l(&this->mutex);
	Locking l(&this->mutex);
	if(expired < first_timeout){
		first_timeout = expired;
	}
	std::string s_key = key.String();
	if(!fast_keys.empty() && expired <= fast_keys.max_score()){
		fast_keys.add(s_key, expired);
		if(fast_keys.size() > BATCH_SIZE){
			log_debug("pop_back");
			fast_keys.pop_back();
		}
	}else{
		fast_keys.del(s_key);
		//log_debug("don't put in fast_keys");
	}
	return 0;
}

int64_t ExpirationHandler::get_ttl(const Bytes &key, bool convert) {
	std::string score;
	if (ssdb->zget(this->list_name, key, &score) == 1) {
		int64_t ex = str_to_int64(score);
		if (convert) {
			return (ex - time_ms()) / 1000;
		} else {
			return ex;
		}
	}
	return -1;
}

void ExpirationHandler::load_expiration_keys_from_db(int num) {
	Iterator *it = nullptr;
	int ret = ssdb->zscan(&it, this->list_name, "", "", "", num);
	int n = 0;
	if (ret > 0) {
		while (it->next()) {
			n ++;
			const std::string &key = it->key();
			int64_t score = str_to_int64(it->score());
			if (score < 2000000000) {
				// older version compatible
				score *= 1000;
			}
			fast_keys.add(key, score);
		}
		delete it;
		log_info("load %d keys into fast_keys", n);
	}
}

void ExpirationHandler::expire_loop(){
	if(!this->ssdb){
		return;
	}
	
	if(this->fast_keys.empty()){
		Locking l(&this->mutex);
		this->load_expiration_keys_from_db(BATCH_SIZE);
		if (this->fast_keys.empty()) {
			this->first_timeout = INT64_MAX;
			return;
		}
	}

	Transaction trans(ssdb->getBinlog());

	int64_t score;
	std::string key;
	if (this->fast_keys.front(&key, &score)) {
		this->first_timeout = score;

		if (score <= time_ms()) {
			Locking l(&this->mutex);
			log_debug("expired %s", key.c_str());
			this->ssdb->zdel_ttl(this->list_name, key);
			ssdb->del(key, BinlogType::NONE, false);
			fast_keys.pop_front();
			leveldb::Status s = ssdb->getBinlog()->commit();
			if (!s.ok()) {
				log_error("delkv commit error: %s", s.ToString().c_str());
			}
		}
	}
}

void* ExpirationHandler::thread_func(void *arg) {
	ExpirationHandler *handler = (ExpirationHandler *)arg;
	while(!handler->thread_quit){
		if(handler->first_timeout > time_ms()){
			continue;
		}
		handler->expire_loop();
		usleep(500 * 1000);
	}

	log_debug("ExpirationHandler thread quit");
	handler->thread_quit = false;
	return (void *)NULL;
}
