/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "leveldb/iterator.h"
#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_list.h"
#include "t_table.h"

#include "../util/config.h"


Iterator::Iterator(
		leveldb::Iterator *it,
		const std::string &end,
		uint64_t limit,
		Direction direction)
{
	this->it = it;
	this->end = end;
	this->limit_ = limit;
	this->is_first = true;
	this->direction = direction;
}

Iterator::~Iterator(){
	delete it;
}

// Bytes Iterator::key(){
// 	leveldb::Slice s = it->key();
// 	return Bytes(s.data(), s.size());
// }

// Bytes Iterator::val(){
// 	leveldb::Slice s = it->value();
// 	return Bytes(s.data(), s.size());
// }

bool Iterator::skip(uint64_t offset){
	while(offset-- > 0){
		if(this->next() == false){
			return false;
		}
	}
	return true;
}
void Iterator::seek(const std::string &start){
	it->Seek(start);
}

bool Iterator::valid(){
	return it->Valid();
}
        
void Iterator::seekToLast(){
	it->SeekToLast();
}
void Iterator::seekToFirst(){
	it->SeekToFirst();
}

void Iterator::prev(){
	it->Prev();
}

Bytes Iterator::rawkey(){
	return Bytes(it->key());
}
Bytes Iterator::rawvalue() {
	return Bytes(it->value());
}
bool Iterator::walk(){

	if(limit_ == 0){
		return false;
	}

	if(is_first){
		is_first = false;
	}else{
		if(direction == FORWARD){
			it->Next();
		}else{
			it->Prev();
		}
	}

	if(!it->Valid()){
		// make next() safe to be called after previous return false.
		limit_ = 0;
		return false;
	}

	if(direction == FORWARD){
		if(!end.empty() && it->key().compare(end) > 0){
			limit_ = 0;
			return false;
		}
	}else{
		if(!end.empty() && it->key().compare(end) < 0){
			limit_ = 0;
			return false;
		}
	}
	limit_--;

	return true;
}


RawIterator::RawIterator(leveldb::Iterator *it,const std::string &end,uint64_t limit,
				Direction direction): Iterator(it, end, limit, direction)
{

}

int RawIterator::decode_key()
{ 
	this->key_ = it->key().ToString();
	return 1; 
}

int RawIterator::decode_value()
{ 
	this->value_ = it->value().ToString();
	return 1;
}

bool RawIterator::next(){
	while(walk()){
		decode_key();
		decode_value();
		return true;
	}
	return false;
}

/* KV */
KIterator::KIterator(leveldb::Iterator *it, const Bytes &name, int64_t version, 
	const std::string &end,uint64_t limit, Direction direction): Iterator(it, end, limit, direction)
{
	this->version_ = version;
}

KIterator::~KIterator(){}
		
bool KIterator::next(){
	while(walk()){
		//因为kv type和metal type 混在了一起,所以需要先解析值
		if (decode_value() == -1){
			continue;
		}
		if(decode_key() == -1){
			continue;
		}
		return true;
	}
	return  false;
}

int KIterator::decode_key(){
	if (DecodeMetaKey(it->key(), &name_, &slot_) == -1){
		return -1;
	}
	this->key_ = this->name_;
	return 1;
}

int KIterator::decode_value(){
	TMH pth;
	memcpy(&pth, it->value().data(), sizeof(TMH));

	if (pth.datatype != DataType::KSIZE){
		return -1;
	}
	/*if (pth.expire != 0 &&  pth.expire < time_ms()){
		log_info("expire......");
		return -2;
	}*/
	if (this->version_ != 0 && pth.version != this->version_){
		return -1;
	}

	if (return_val_){
		return DecodeDataValue(DataType::KV, it->value().ToString(), &value_);
	} 
	return 1;
}

/* Metal */
MetalIterator::MetalIterator(leveldb::Iterator *it, const char type, const std::string &end,
	uint64_t limit, Direction direction): Iterator(it, end, limit, direction){
	this->type_ = type;
}
MetalIterator::~MetalIterator(){}

bool MetalIterator::next() {
	if (limit_ == 0) {
		return false;
	}
	while (1) {
		if (is_first) {
			is_first = false;
		} else {
			direction == FORWARD ? it->Next() : it->Prev();
		}
		if (!it->Valid()) {
			// make next() safe to be called after previous return false.
			limit_ = 0;
			return false;
		}
		//不能解析name就退出
		if (decode_key() == -1) {
			return false;
		}

		//类型不正确需要继续,因为metal前缀没有类型
		int ret = decode_value();
		if (ret == 0) {
			continue;
		} else if (ret == -1) {
			return false;
		} else if (ret == -2){           
           continue;
        }
		break;
	}
	if (direction == FORWARD) {
		if (!end.empty() && it->key() > end) {
			limit_ = 0;
			return false;
		}
	} else {
		if (!end.empty() && it->key() < end) {
			limit_ = 0;
			return false;
		}
	}
	limit_--;

	return true;
}

int MetalIterator::decode_key(){
	if (DecodeMetaKey(it->key(), &name_, &slot_) == -1){
		return -1;
	}
	this->key_ = this->name_;
	return 1;
}

int MetalIterator::decode_value(){
	TMH pth;
	memcpy(&pth, it->value().data(), sizeof(TMH));
	
	if (this->type_ == '\xff'){
		this->type_ = pth.datatype;
	}
	else if (pth.datatype != this->type_){
		return 0;
	}
	if (pth.expire != 0 &&  pth.expire < time_ms()){
		return -2;
	}

	if (pth.expire != 0 &&  pth.expire < time_ms()){
		return -2;
	}
	if (return_val_ && this->type_ == DataType::KSIZE){
		return DecodeDataValue(DataType::KSIZE, it->value().ToString(), &value_);
	} 
	return 1;
}

/* HASH */
HIterator::HIterator(leveldb::Iterator *it, const Bytes &name, int64_t version, const std::string &end,
			uint64_t limit, Direction direction) : Iterator(it, end, limit, direction){
	this->name_.assign(name.data(), name.size());
	this->version_ = version;
}

HIterator::~HIterator(){}

bool HIterator::next(){
	while(walk()){
		int rst = decode_key();

		if (rst == 0){
			continue;
		}else if (rst == -1){
			return false;
		}

		if(decode_value() == -1){
			return false;
		}
		return true;
	}
	return  false;
}

int HIterator::decode_key(){
	std::string n;
	int64_t ver;

	//这是因为如果end为空的话rocksdb内置迭代器判断＜end就无效，可能迭代出所有key,需要判断名字是否相同
	if (it->key().data()[0] != DataType::HASH){
		return -1;
	}

	if (DecodeHashKey(it->key(), &n, &this->key_, &ver, &slot_) == -1){
		return 0;
	}

	if ((this->version_ != 0 && ver != this->version_) || (!this->name_.empty() && n != name_)){
		return -1;
	}

	if (this->version_ == 0){
		this->version_ = ver;
	}

	if (this->name_.empty()){
		this->name_ = n;
	}
	return 1;
}

int HIterator::decode_value(){
	if (return_val_){
		return DecodeDataValue(DataType::HASH, it->value().ToString(), &value_);
	} 
	return 1;
}

/* ZSET */
ZIterator::ZIterator(leveldb::Iterator *it, const Bytes &name, int64_t version, const std::string &end,
			uint64_t limit, Direction direction): Iterator(it, end, limit, direction) 
{
	this->name_.assign(name.data(), name.size());
	this->version_ = version;
}

ZIterator::~ZIterator(){}

bool ZIterator::next(){
	while(walk()){
		int rst = decode_key();

		if (rst == 0){
			continue;
		}else if (rst == -1){
			return false;
		}

		if(decode_value() == -1){
			return false;
		}
		return true;
	}

	return  false;
}

int ZIterator::decode_key(){
	std::string n;
	int64_t ver;

	if (it->key().data()[0] != DataType::ZSCORE){
		return -1;
	}
	if (DecodeSortSetScoreKey(it->key(), &n, &key_, &score_, &ver, &slot_) == -1){
		return 0;
	}
	if ((this->version_ != 0 && ver != this->version_) || (!this->name_.empty() && n != name_)){
		return -1;
	}

	if (this->version_ == 0){
		this->version_ = ver;
	}

	if (this->name_.empty()){
		this->name_ = n;
	}
	return 1;
}

int ZIterator::decode_value(){
	value_ = key_;
	return 1;
}

/* Queue */
QIterator::QIterator(leveldb::Iterator *it, const Bytes &name, int64_t version,
			const std::string &end, uint64_t limit, Direction direction) :Iterator(it, end, limit, direction){
	this->name_.assign(name.data(), name.size());
	this->version_ = version;
}

QIterator::~QIterator(){}

bool QIterator::next(){
	while(walk()){
		int rst = decode_key();

		if (rst == 0){
			continue;
		}else if (rst == -1){
			return false;
		}

		if(decode_value() == -1){
			return false;
		}
		return true;
	}
	return  false;
}

int QIterator::decode_key(){
	std::string n;
	int64_t ver;

	
	if (it->key().data()[0] != DataType::LIST){
		return -1;
	}

	if (DecodeListKey(it->key(), &n, &seq_, &ver, &slot_) == -1){
		return 0;
	}
	if ((this->version_ != 0 && ver != this->version_) || (!this->name_.empty() && n != name_)){
		return -1;	
	}

	if (this->version_ == 0){
		this->version_ = ver;
	}

	if (this->name_.empty()){
		this->name_ = n;
	}
	return 1;
}

int QIterator::decode_value(){
	if (return_val_){
		return DecodeDataValue(DataType::LIST, it->value().ToString(), &value_);
	} 
	return 1;
}

CltIterator::CltIterator(leveldb::Iterator *it, const Bytes &name, int64_t version,
					const std::string &end, uint64_t limit, Direction direction) :Iterator(it, end, limit, direction){
	this->name_.assign(name.data(), name.size());
	this->version_ = version;
}

CltIterator::~CltIterator(){

}

bool CltIterator::next(){
	while(walk()){
		int ret = decode_key();
		if (ret == 0){
			continue;
		}else if (ret == -1){
			return false;
		}
		return true;
	}
	return false;
}

int CltIterator::decode_key(){
	if (it->key().data()[0] != DataType::TABLE){
		return -1;
	}
	int64_t ver;
	std::string name;
	if (decode_clt_prefix_key(it->key(), &name, &ver, &slot_) == -1){
		log_debug("decode_clt_prefix_key error");
		return -1;
	}
	//log_debug("-------------%s", hexmem(it->key().data(), it->key().size()).data());
	if (name != this->name_ || ver != this->version_){
		return -1;
	}

	return 1;
}

int CltIterator::decode_value(){
	if (return_val_){
		return DecodeDataValue(DataType::TABLE, it->value().ToString(), &value_);
	} 
	return 1;
}

std::string CltIterator::key(){
	return it->key().ToString();
}

//colltction index 
CltIdxIterator::CltIdxIterator(leveldb::Iterator *it, const Bytes &name, const Bytes &index, int64_t version,
			const std::string &end, uint64_t limit, Direction direction) :Iterator(it, end, limit, direction){
	this->name_.assign(name.data(), name.size());
	this->index_.assign(index.data(), index.size());
	this->version_ = version;
}

CltIdxIterator::~CltIdxIterator(){}

bool CltIdxIterator::next(){
	while(walk()){
		int rst = decode_key();

		if (rst == 0){
			continue;
		}else if (rst == -1){
			return false;
		}

		return true;
	}
	return  false;
}

int CltIdxIterator::decode_key(){
	if (it->key().data()[0] != DataType::TABLE){
		log_error("decode_clt_index_key faild: %s", hexmem(this->rawkey().data(), this->rawkey().size()).data());
		return -1;
	}
	int64_t ver;
	std::string name, index;
	if (decode_clt_index_key(it->key(), &name, &index, &ver, &value_, &slot_) == -1){
		log_error("decode_clt_index_key faild: %s", hexmem(this->rawkey().data(), this->rawkey().size()).data());
		return -1;
	}
	if (name != this->name_ || index != this->index_ || ver != this->version_){
		log_error("decode_clt_index_key faild: index:%s %s, name %s %s, version:%lld %lld",index.data(), this->index_.data(),
			name.data(), this->name_.data(), ver, this->version_);
		return -1;
	}

	return 1;
}

CltKeyIterator::CltKeyIterator(leveldb::Iterator *it, const Bytes &name, 
			int64_t version, const std::string &end, uint64_t limit, Direction direction) 
:Iterator(it, end, limit, direction){
	this->name_.assign(name.data(), name.size());
	this->version_ = version;
}

CltKeyIterator::~CltKeyIterator(){}

bool CltKeyIterator::next(){
	while(walk()){
		int rst = decode();

		if (rst == 0){
			continue;
		}else if (rst == -1){
			return false;
		}
		return true;
	}
	return  false;
}

int CltKeyIterator::decode(){
	int rst = decode_key();

	if (rst == 0) {
		log_error("decode-key faild %s", hexmem(this->rawkey().data(), this->rawkey().size()).data());
		return 0;
	} else if (rst == -1) {
		log_error("decode-key faild %s", hexmem(this->rawkey().data(), this->rawkey().size()).data());
		return -1;
	}
	if (decode_value() == -1) {
		log_error("decode-key faild %s", hexmem(this->rawkey().data(), this->rawkey().size()).data());
		return -1;
	}
	return 1;
}

int CltKeyIterator::decode_key(){
	int64_t ver;
	if (it->key().data()[0] != DataType::TABLE){
		return -1;
	}
	std::string name;
	if (DecodeCltPrimaryKey(it->key(), &name, &key_, &ver, &slot_) == -1){
		return -1;
	}
	if (name != name_ || ver != this->version_){
		log_info("result error:name:%s \n key:%s\n slot:%d \n value:%s\n version  %lld, want find version: %lld", 
							name.data(), 
							key_.data(), 
							slot_, 
							it->value().ToString().data(),  ver, this->version_);
		return -1;	
	}

	return 1;
}

int CltKeyIterator::decode_value(){
	if (return_val_){
		return DecodeDataValue(DataType::TABLE, it->value().ToString(), &value_);
	} 
	return 1;
}

Iterator* IteratorFactory::iterator(const SSDB *ssdb, const char dataType, 
							const std::string &start, const std::string &end, uint64_t limit)
{
	leveldb::Iterator *internal_it = internal_iterator(ssdb, start, Iterator::FORWARD);
	return new MetalIterator(internal_it, dataType, end, limit, Iterator::FORWARD);
}

Iterator* IteratorFactory::rev_iterator(const SSDB *ssdb, const char dataType, 
							const std::string &start, const std::string &end, uint64_t limit)
{
	leveldb::Iterator *internal_it = internal_iterator(ssdb, start, Iterator::BACKWARD);
	return new MetalIterator(internal_it, dataType, end, limit, Iterator::BACKWARD);
}

Iterator* IteratorFactory::iterator(const SSDB *ssdb, IteratorType dtType, const Bytes &name, int64_t version,
							const std::string &start, const std::string &end, uint64_t limit)
{
	leveldb::Iterator *internal_it = internal_iterator(ssdb, start, Iterator::FORWARD);
	return createIter(dtType, internal_it, name, version, start, end, limit, Iterator::FORWARD);
}

CltIdxIterator* IteratorFactory::clt_idx_iterator(const SSDB *ssdb, const Bytes &name, const Bytes &index, 
							int64_t version, const std::string &start, const std::string &end, uint64_t limit, int32_t desc){
	leveldb::Iterator *internal_it = internal_clt_iterator(ssdb, start, desc > 0 ? Iterator::FORWARD : Iterator::BACKWARD);
	return new CltIdxIterator(internal_it, name, index, version, end, limit, desc > 0 ? Iterator::FORWARD : Iterator::BACKWARD);
}

CltKeyIterator* IteratorFactory::clt_key_iterator(const SSDB *ssdb, const Bytes &name, 
							int64_t version, const std::string &start, const std::string &end, uint64_t limit, int32_t desc){
	leveldb::Iterator *internal_it = internal_clt_iterator(ssdb, start, desc > 0 ? Iterator::FORWARD : Iterator::BACKWARD);
	return new CltKeyIterator(internal_it, name, version, end, limit, desc > 0 ? Iterator::FORWARD : Iterator::BACKWARD);
}

Iterator* IteratorFactory::rev_iterator(const SSDB *ssdb, IteratorType dtType, const Bytes &name, int64_t version,
							const std::string &start, const std::string &end, uint64_t limit)
{
	leveldb::Iterator *internal_it = internal_iterator(ssdb, start, Iterator::BACKWARD);
	return createIter(dtType, internal_it, name, version, start, end, limit, Iterator::BACKWARD);
}

leveldb::Iterator *IteratorFactory::internal_iterator(const SSDB *ssdb, const std::string &start, Iterator::Direction direct)
{
	leveldb::Iterator *it;
    leveldb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    if (direct != Iterator::FORWARD){
    	iterate_options.total_order_seek = true;
    }
    it = ssdb->getlDb()->NewIterator(iterate_options);
    it->Seek(start);
    if (direct == Iterator::FORWARD){
    	if(it->Valid() && it->key() == start){
        	it->Next();
    	}
    }else{
    	if(!it->Valid()){
        	it->SeekToLast();
    	}else{
        	it->Prev();
    	}
    }
    return it;
}

leveldb::Iterator *IteratorFactory::internal_clt_iterator(const SSDB *ssdb, const std::string &start, Iterator::Direction direct)
{
	leveldb::Iterator *it;
    leveldb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    if (direct != Iterator::FORWARD){
    	iterate_options.total_order_seek = true;
    }
    it = ssdb->getlDb()->NewIterator(iterate_options);
    it->Seek(start);

    if (direct == Iterator::FORWARD){
    	if(!it->Valid()){
    		log_error("seek FORWARD error:%s", start.data());
        	it->SeekToFirst();
    	}
    }else{
    	if(!it->Valid()){
    		log_error("seek BACKWARD error:%s", start.data());
        	it->SeekToLast();
    	}else{
			it->Prev();
		}
    }
    return it;
}

Iterator *IteratorFactory::createIter(IteratorType dtType, leveldb::Iterator *it, const Bytes &name, int64_t version, 
    					const std::string &start, const std::string &end, uint64_t limit, Iterator::Direction direct){
	switch (dtType){
		case kKIter: {
			return new KIterator(it, name, version, end, limit, direct);
		}
		case kHIter: {
			return new HIterator(it, name, version, end, limit, direct);
		}
		case kZIter: {
			return new ZIterator(it, name, version, end, limit, direct);
		}
		case kQIter: {
			return new QIterator(it, name, version, end, limit, direct);
		}
		case kCltIter: {
			return new CltIterator(it, name, version, end, limit, direct);
		}
		default: 
			break;
	}
	return new RawIterator(it, end, limit, direct);
}
