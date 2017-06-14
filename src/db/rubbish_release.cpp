#include "rubbish_release.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_list.h"
#include "t_table.h"

#define BATCH_SIZE    1000

RubbishRelease::RubbishRelease(SSDB *ssdb){
	this->ssdb = ssdb;
	this->thread_quit = false;
	this->list_name = RUBBISH_LIST_KEY;
	this->queue_first_version = 0;
	this->start();
}

RubbishRelease::~RubbishRelease(){
	Locking l(&this->mutex);
	this->stop();
	ssdb = NULL;
}

void RubbishRelease::start(){
	thread_quit = false;
	pthread_t tid;
	int err = pthread_create(&tid, NULL, &RubbishRelease::thread_func, this);
	if(err != 0){
		log_fatal("can't create clear rubbishrelese thread: %s", strerror(err));
		exit(0);
	}
}

void RubbishRelease::stop(){
	thread_quit = true;
	for(int i=0; i<100; i++){
		if(!thread_quit){
			break;
		}
		usleep(10 * 1000);
	}
}

int RubbishRelease::push_clear_queue(TMH &metainfo, const Bytes &name){	
	std::string s_ver = str(metainfo.version);
	std::string s_key = encode_release_key(metainfo.datatype, name);

	//尝试将那么name加入垃圾收集队列中
	int ret = this->ssdb->hset_rubbish_queue(this->list_name, s_key, s_ver);
	//log_debug("push_clear_queue key:%s, hset_rubbish_queue ret:%d", name.data(), ret);
	if(ret < 0){
		return -1;
	}

	//成功就删除meta信息
	std::string metalkey = EncodeMetaKey(name);
	ssdb->getBinlog()->Delete(metalkey);

	static_cast<SSDBImpl *>(ssdb)->expiration->del_ttl(name);

	//是否发送同步信息,函数外配置TODO:slot不准
	//ssdb->calculateSlotRefer(Slots::encode_slot_id(name),  -1);

	if(metainfo.version < queue_first_version){
		queue_first_version = metainfo.version;
	}
	return 1;
}

int RubbishRelease::push_clear_queue(const Bytes &name, const char log_type, 
									const char cmd_type, bool clearttl){
	Transaction trans(ssdb->getBinlog());

	//找出name的meta信息
	TMH metainfo = {0};
	//ignore ttl,del_ttl will called this function
	if (ssdb->loadobjectbyname(&metainfo, name, 0, false) <= 0){
		return 0;
	}

	std::string s_ver = str(metainfo.version);
	std::string s_key = encode_release_key(metainfo.datatype, name);

	//尝试将那么name加入垃圾收集队列中
	int ret = this->ssdb->hset_rubbish_queue(this->list_name, s_key, s_ver);
	//log_debug("push_clear_queue key:%s, hset_rubbish_queue ret:%d", name.data(), ret);
	if(ret < 0){
		return -1;
	}

	//成功就删除meta信息
	std::string metalkey = EncodeMetaKey(name);
	ssdb->getBinlog()->Delete(metalkey);
	if (clearttl){
		static_cast<SSDBImpl *>(ssdb)->expiration->del_ttl(name);
	}
	//是否发送同步信息,函数外配置
	ssdb->calculateSlotRefer(Slots::encode_slot_id(name),  -1);
	ssdb->getBinlog()->add_log(log_type, cmd_type, metalkey);
	leveldb::Status s = ssdb->getBinlog()->commit();
	if(!s.ok()){
		return 0;
	}else if(!s.ok()){
		return -1;
	}
	if(metainfo.version < queue_first_version){
		queue_first_version = metainfo.version;
	}

	// //不为空才进入,否则还会操作一次,在load_expiration_keys_from_db中
	// if(!fast_queues.empty() && fast_queues.size() < BATCH_SIZE){
	// 	//因为是队列.不需要pop最后一个判断,后进入的肯定版本高
	// 	fast_queues.add(s_key, metainfo.version);
	// }

	return 1;
}

void RubbishRelease::load_expiration_keys_from_db(int num){
	Iterator *it = nullptr;
	int ret = ssdb->hscan(&it, this->list_name, "", "\xff", num);
	int n = 0;
	if (ret > 0){
		while(it->next()){
			n ++;
			const std::string &key = it->key();
			int64_t version = str_to_int64(it->value());
		
			fast_queues.add(key, version);
			char type = 0;
			std::string name;
			decode_release_key(key, &type, &name);
			log_debug("add clear queue:%s:%c", name.c_str(), type);
		}
		delete it;
	}
	log_debug("load %d keys into clear queue", n);
}

void RubbishRelease::expire_loop(){
	//Locking l(&this->mutex);
	if(!this->ssdb){
		return;
	}

	if(this->fast_queues.empty()){
		this->load_expiration_keys_from_db(BATCH_SIZE);
		if(this->fast_queues.empty()){
			this->queue_first_version = INT64_MAX;
			return;
		}
	}
	
	int64_t version;
	std::string key;
	if(this->fast_queues.front(&key, &version)){
		log_debug("clear name: %s, version: %llu", hexmem(key.c_str(), key.size()).data(), version);
        if (lazy_clear(key, version) > 0){
        	log_debug("continue clear key:%s,%llu", key.c_str(), version);
        }else{
        	fast_queues.pop_front();
        	this->ssdb->hdel(this->list_name, key, BinlogType::NONE);
        }
	}
}

void* RubbishRelease::thread_func(void *arg){
	RubbishRelease *handler = (RubbishRelease *)arg;
	
	while(!handler->thread_quit){
		usleep(10000);
		if(handler->queue_first_version > time_us()){
			continue;
		}
		handler->expire_loop();
	}
	
	log_debug("rubbishrelease thread quit");
	handler->thread_quit = false;
	return (void *)NULL;
}

int RubbishRelease::lazy_clear(const Bytes &raw_key, int64_t version)
{
	char type;
	std::string name;
	
	if (decode_release_key(raw_key, &type, &name)){
		return -1;
	}

    std::string key_start = encode_data_key_start(type, name, version);    
    std::string key_end = encode_data_key_end(type, name, version);
   
    Iterator *it = IteratorFactory::iterator(this->ssdb, dataType2IterType2(type), name, 
                version, key_start, key_end, 500);
    int num = 0;
    leveldb::WriteBatch batch;
    while(it->next()){
    	//log_debug("-----------del:%s, %llu, value:%s", it->key().c_str(), it->seq(), it->value().c_str());
		switch(type){
            case DataType::ZSIZE:{
                batch.Delete(EncodeSortSetScoreKey(name, it->key(), it->score(), version));
				batch.Delete(EncodeSortSetKey(name, it->key(), version));
				break;
            }
            case DataType::HSIZE:{
                batch.Delete(EncodeHashKey(name, it->key(), version));
                break;
            }
            case DataType::LSIZE:{
                batch.Delete(EncodeListKey(name, it->seq(), version));
                break;
            }
            case DataType::TABLE:{
            	batch.Delete(it->key());
            	//log_debug("clear version: %lld, key: %s", it->version(), hexmem(it->key().c_str(), it->key().size()).data());
            	break;
            }
        }
        num++;
    }
    delete it;
    
    leveldb::Status s = ssdb->getlDb()->Write(leveldb::WriteOptions(), &batch);
	if(!s.ok()){
		log_error("clear_rubbish_queue error:%s", s.ToString().data());
		return -1;
	}

	//清除数据应该CompactRange一次，但是如果CompactRange时间超过100毫秒就应该报警，并且可配停止CompactRange动作。
	int64_t ts = time_us();
	leveldb::Slice slice_s(key_start);
	leveldb::Slice slice_e(key_end);
	ssdb->getlDb()->CompactRange(&slice_s, &slice_e);
	int64_t te = time_us();
	if ( te-ts > 100000 ) {
		log_info("rubbish compactRange time to long :%s -> %lld", key_start.data(), te - ts);
	}
    return num;
}

std::string RubbishRelease::encode_data_key_start(const char type, const Bytes &name, int64_t version){
	std::string key_s;
	switch(type){
		case DataType::ZSIZE:{
			key_s = EncodeSortSetScoreKey(name, "", SSDB_SCORE_MIN, version);
			break;
		}
		case DataType::LSIZE:{
			key_s = EncodeListKey(name, "", version);
			break;
		}
		case DataType::HSIZE:{
			key_s = EncodeHashKey(name, "", version);		
			break;
		}
		case DataType::TABLE:{
			key_s = EncodeCltPrefixKey(name, version);
			break;
		}
	}
	return key_s;
}

std::string RubbishRelease::encode_data_key_end(const char type, const Bytes &name, int64_t version){
	std::string key_e;
	switch(type){
		case DataType::ZSIZE:{
			key_e = EncodeSortSetScoreKey(name, "\xff", SSDB_SCORE_MAX, version);
			break;
		}
		case DataType::LSIZE:{
			key_e = EncodeListKey(name, "\xff", version);
			break;
		}
		case DataType::HSIZE:{
			key_e = EncodeHashKey(name, "\xff", version);
			break;
		}
		case DataType::TABLE:{
			key_e = EncodeCltPrefixKey(name, version);
			key_e.append(1, 255);
			break;
		}
	}
	return key_e;
}

