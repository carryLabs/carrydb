#include "t_hash.h"

static int hset_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, const Bytes &key, const Bytes &val, char log_type);
static int hsetv_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, const Bytes &key, 
								const Bytes &val, int64_t *version, char log_type);
static int hdel_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, const Bytes &key, char log_type);
static int incr_hsize(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, int64_t incr);
static int get_hashvalue(const SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, 
							const Bytes &key, std::string *val, TVV *val_head = nullptr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	TMH metainfo = {0};
	
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret < 0){
		return ret;
	}

	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	ret = hset_one(this, metainfo, name, key, val, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, metainfo, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hsetv(const Bytes &name, const Bytes &key, const Bytes &val, int64_t *version, char log_type){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret < 0){
		return ret;
	}
	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	ret = hsetv_one(this, metainfo, name, key, val, version, log_type);
	if (ret >= 0) {
		if(ret > 0){
			if (incr_hsize(this, metainfo, name, ret) == -1) {
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hsetnx(const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);	
	//if (ret != 0 && ret != ObjectErr::object_expire){
		//return 0;
	//}
	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);

	 //查找hget name key是否存在
	 std::string hval;
	 ret = get_hashvalue(this, metainfo, name, key, &hval);
	 if (ret != 0){
	 	return 0;
	 }

	ret = hset_one(this, metainfo, name, key, val, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, metainfo, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hdel(const Bytes &name, const Bytes &key, char log_type){
	Transaction trans(binlogs);
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}

	ret = hdel_one(this, metainfo, name, key, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, metainfo, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret < 0 ){
		return ret;
	}
	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	std::string old;
	ret = get_hashvalue(this, metainfo, name, key, &old);
	
	if (ret == -1){
		return ret;
	}else if (ret == 0){
		*new_val = by;
	}else{
		ret = isoverflow(old, by, new_val);
		if (ret < 0){
			return ret;
		}
	}

	ret = hset_one(this, metainfo, name, key, str(*new_val), log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, metainfo, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int64_t SSDBImpl::hsize(const Bytes &name){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	return metainfo.refcount;
}

int SSDBImpl::hget(const Bytes &name, const Bytes &key, std::string *val){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	ret = get_hashvalue(this, metainfo, name, key, val);
	return ret;
}

int SSDBImpl::hgetv(const Bytes &name, const Bytes &key, std::string *val, int64_t *version){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	} 
	TVV val_header;
	ret = get_hashvalue(this, metainfo, name, key, val, &val_header);
	*version = val_header.version;
	return ret;
}

int SSDBImpl::hscan(Iterator **it, const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	std::string key_start, key_end;
	key_start = EncodeHashKey(name, start, metainfo.version);
	if(!end.empty()){
		key_end = EncodeHashKey(name, end, metainfo.version);
	}

	*it = IteratorFactory::iterator(this, kHIter, name, metainfo.version, key_start, key_end, limit);
	return 1;
}

int SSDBImpl::hrscan(Iterator **it, const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	std::string key_start, key_end;
	key_start = EncodeHashKey(name, start, metainfo.version);
	if(start.empty()){
		key_start.append(1, 255);
	}
	if(!end.empty()){
		key_end = EncodeHashKey(name, end, metainfo.version);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");
	*it = IteratorFactory::rev_iterator(this, kHIter, name, metainfo.version, key_start, key_end, limit);
	return 1;
}

int SSDBImpl::hclear(const Bytes &name, char log_type, bool delttl){
	if (releasehandler->push_clear_queue(name, log_type, BinlogCommand::HCLEAR, delttl) == 1){
		incrNsRefCount(DataType::HSIZE, name, -1);
	}
	return 1;
}

int SSDBImpl::hgetall(const Bytes &name, std::vector<std::string> *resp){
	Iterator *it = nullptr;
	int ret = this->hscan(&it, name, "", "", 200000000);
	if (ret <= 0){
		return ret;
	}
	
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->value());
	}
	delete it;

	return 1;
}

// returns the number of newly added items
static int hset_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}
	initVersion(metainfo, metainfo.refcount, DataType::HSIZE);
	int ret = 0;
	std::string datakey = EncodeHashKey(name, key, metainfo.version);
	std::string datavalue;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &datavalue);
	if(s.IsNotFound()){
		ssdb->binlogs->Put(datakey, EncodeDataValue(val));
		ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, datakey);
		ret = 1;
	}else{
		if(!s.ok()){
			return -1;
		}

		std::string value;
		TVV valinfo;
		ret = DecodeDataValue(DataType::HASH, datavalue, &value, &valinfo);
		if (ret == -1){
			return -1;
		}
		if (valinfo.tag_header.reserve[1] == ValueType::kVersion){
			valinfo.version++;//version 递增
			log_debug("hahah");
		}
		
		if (value != val){
			if (valinfo.tag_header.reserve[1] == ValueType::kVersion){
				ssdb->binlogs->Put(datakey, EncodeDataValue(val, ValueType::kNone, &valinfo.version));
			}else{
				ssdb->binlogs->Put(datakey, EncodeDataValue(val));
			}
			ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, datakey);
		}
		ret = 0;
	}

	return ret;
}

static int hsetv_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, 
				const Bytes &key, const Bytes &val, int64_t *version, char log_type){
	if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}
	initVersion(metainfo, metainfo.refcount, DataType::HSIZE);
	int ret = 0;
	std::string datakey = EncodeHashKey(name, key, metainfo.version);
	std::string datavalue;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &datavalue);
	if(s.IsNotFound()){
		*version = 20170310;
		ssdb->binlogs->Put(datakey, EncodeDataValue(val, ValueType::kNone, version));
		ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, datakey);
		ret = 1;
	}else{
		if(!s.ok()){
			return -1;
		}

		std::string value;
		TVV valinfo;
		ret = DecodeDataValue(DataType::HASH, datavalue, &value, &valinfo);
		if (ret == -1){
			return -1;
		}
		if (valinfo.version != *version){
			return 0;
		}
		valinfo.version++;//version 递增
		*version = valinfo.version;

		if (value != val){
			ssdb->binlogs->Put(datakey, EncodeDataValue(val, ValueType::kNone, &valinfo.version));
			ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, datakey);
		}else {
			ssdb->binlogs->Put(datakey, EncodeDataValue(val, ValueType::kNone, &valinfo.version));
		}
		ret = 0;
	}

	return ret;
}

static int hdel_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, const Bytes &key, char log_type){
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}

	std::string datakey = EncodeHashKey(name, key, metainfo.version);
	std::string datavalue;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &datavalue);
	if(s.IsNotFound()){
		return 0;
	}

	if(!s.ok()){
		return -1;
	}

	ssdb->binlogs->Delete(datakey);
	ssdb->binlogs->add_log(log_type, BinlogCommand::HDEL, datakey);

	return 1;
}

static int incr_hsize(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, int64_t incr) {
	std::string metalkey = EncodeMetaKey(name);

    metainfo.refcount += incr;
    //log_debug("hash key:%s, refcnt:%u, expire:%u", name.data(), metainfo.refcount, metainfo.expire);
    
    //为0和1才在函数内部处理
	ssdb->incrSlotsRefCount(name, metainfo, incr);

	if(metainfo.refcount == 0){
		ssdb->binlogs->Delete(metalkey);
	}else{
		ssdb->binlogs->Put(metalkey, EncodeMetaVal(metainfo));
	}

	return 0;
}

static int get_hashvalue(const SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, 
				const Bytes &key, std::string *val, TVV *val_head){
	std::string datakey = EncodeHashKey(name, key, metainfo.version);
	std::string datavalue;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &datavalue);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		return -1;
	}
	int ret = DecodeDataValue(DataType::HASH, datavalue, val, val_head);
	if (ret == -1){
		return -1;
	}

	return 1;
}


int SSDBImpl::hlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list){
	Iterator *it = scan_slot(DataType::HSIZE, slot, name_s, name_e, limit);
	while(it->next()){
		list->push_back(it->name());
	}
	delete it;
	return 1;
}

int SSDBImpl::hrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
					uint64_t limit, std::vector<std::string> *list){
	Iterator *it = rscan_slot(DataType::HSIZE, slot, name_s, name_e, limit);
	while(it->next()){
		list->push_back(it->name());
	}
	delete it;
	return 1;
}

int SSDBImpl::batch_del_hash_names(const std::map<std::string, std::map<std::string, std::string>>& kvmap) {
	for (auto itmap = kvmap.begin(); itmap != kvmap.end(); itmap ++){
		hclear(itmap->first);
	}
	return 1;
}

int SSDBImpl::hset_rubbish_queue(const Bytes &name, const Bytes &key, const Bytes &val){
	TMH metainfo = {0};
	//rubbish 不设置过期,不进行判断
	int ret = VerifyStructState(&metainfo, name, DataType::HSIZE);
	if (ret < 0 ){
		return ret;
	}
	
	ret = hset_one(this, metainfo, name, key, val, BinlogType::NONE);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, metainfo, name, ret) == -1){
				return -1;
			}
		}
	}
	return ret;
}
