#include "t_kv.h"

static inline int get_kvvalue(const SSDBImpl *ssdb, const Bytes &name, std::string *val){
	std::string datakey = encode_slotskvData_key(name);
	std::string datavalue;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &datavalue);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		return -1;
	}
	
	int ret = DecodeDataValue(DataType::KSIZE, datavalue, val);
	if (ret == -1){
		return -1;
	}

	return 1;
}

static inline
void kset_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &key, const Bytes &val, char log_type){
	//创建新值,打时间戳当版本
	initVersion(metainfo, metainfo.refcount, DataType::KSIZE);
	//不能移进函数的原因是因为其他类型有subkey,操作subkey不自增
	if (metainfo.refcount == 0){
		metainfo.refcount++;
		//这样写mul_set是有bug的,因为可能写入失败
		ssdb->incrNsRefCount(DataType::KSIZE, key, 1);
	}
	std::string datakey = encode_slotskvData_key(key);
	ssdb->binlogs->Put(datakey, encode_slotskvData_val(metainfo, val));
	ssdb->binlogs->add_log(log_type, BinlogCommand::KSET, datakey);
}

int SSDBImpl::multi_set(const std::vector<Bytes> &kvs, int offset, char log_type){
	Transaction trans(binlogs);
	std::map<Bytes, TMH> metainfos;

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;

	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		TMH metainfo = {0};
		int ret = VerifyStructState(&metainfo, key, DataType::KSIZE);
		if (ret < 0){
			return ret;
		}
		//需要在前面,先记录状态
		metainfos.insert(std::pair<Bytes, TMH>(key, metainfo));

		removeExpire(key, metainfo);
		kset_one(this, metainfo, key, *(it + 1), log_type);
	}
	
	incrMultiSlotsRefCount(this, metainfos, 1);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		return -1;
	}
	return (kvs.size() - offset)/2;
}

/*KV/String*/
int SSDBImpl::multi_setnx(const std::vector<Bytes> &kvs, int offset, char log_type){
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it = kvs.begin() + offset;
	std::map<Bytes, TMH> metainfos;

	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;

		TMH metainfo = {0};
		int ret = VerifyStructState(&metainfo, key, DataType::KSIZE);
		if (ret != 0 && ret != ObjectErr::object_expire) {
			return ret;
		}
		metainfos.insert(std::pair<Bytes, TMH>(key, metainfo));
		removeExpire(key, metainfo);

		kset_one(this, metainfo, key, *(it + 1), log_type);
	}

	incrMultiSlotsRefCount(this, metainfos, 1);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_setnx error: %s", s.ToString().c_str());
		return -1;
	}
	return (kvs.size() - offset)/2;
}

int SSDBImpl::set(const Bytes &key, const Bytes &val, char log_type){
	
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, key, DataType::KSIZE);
	if (ret < 0){
		return ret;
	}

	Transaction trans(binlogs);

	::incrSlotsRefCount(this, key, metainfo, 1);

	//直接将kv的val写进metalval中
	kset_one(this, metainfo, key, val, log_type);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}

	return 1;
}

int SSDBImpl::setnx(const Bytes &key, const Bytes &val, char log_type){
	if(key.empty()){
		log_error("empty key!");
		return 0;
	}
	Transaction trans(binlogs);

	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, key, DataType::KSIZE);
	//过期当作没有此key处理
	if (ret != 0 && ret != ObjectErr::object_expire){
		return 0;
	}

	::incrSlotsRefCount(this, key, metainfo, 1);

	kset_one(this, metainfo, key, val, log_type);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("setnx error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::getset(const Bytes &key, std::string *val, const Bytes &newval, char log_type){
	if(key.empty()){
		log_error("empty key!");
		return 0;
	}
	Transaction trans(binlogs);

	TMH metainfo = {0};
	//get已经做处理,过期当作不存在,返回val也是empty值
	int found = this->get(key, val, &metainfo);

	if (found < 0){
		return found;
	}

	::incrSlotsRefCount(this, key, metainfo, 1);
	kset_one(this, metainfo, key, newval, log_type);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}

	return found;
}

//int incr(const Bytes &key, int64_t by, int64_t *new_val, char log_type=BinlogType::SYNC);
int SSDBImpl::incr(const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	Transaction trans(binlogs);

	std::string old;
	TMH metainfo = {0};
	int ret = this->get(key, &old, &metainfo);//过期返回0. old is null
	if (ret < 0){
		return ret;
	}
	
	if (ret == 0){
		*new_val = by;
	}else{
		ret = isoverflow(old, by, new_val);
		if (ret < 0){
			return ret;
		}
	}
	::incrSlotsRefCount(this, key, metainfo, 1);

	kset_one(this, metainfo, key, str(*new_val), log_type);

	leveldb::Status s  = binlogs->commit();
	if(!s.ok()){
		log_error("incr error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::get(const Bytes &key, std::string *val, TMH *metainfo){
	TMH metainfo_tmp = {0};
	int ret = VerifyStructState(metainfo == NULL ? &metainfo_tmp: metainfo, key, DataType::KSIZE);
	if (ret <= 0){
		return ret;
	}

	if (ret & ObjectErr::object_expire){
		return 0;
	}
	//直接返回metalval
	return get_kvvalue(this, key, val);
}

int SSDBImpl::setbit(const Bytes &key, int bitoffset, int on, char log_type){
	if(key.empty()){
		log_error("empty key!");
		return 0;
	}

	Transaction trans(binlogs);

	std::string val;
	TMH metainfo = {0};
	int found = this->get(key, &val, &metainfo);
	if (found < 0){
		return found;
	}

	int len = bitoffset / 8;
	int bit = bitoffset % 8;
	if(len >= val.size()){
		val.resize(len + 1, 0);
	}
	int orig = val[len] & (1 << bit);
	if(on == 1){
		val[len] |= (1 << bit);
	}else{
		val[len] &= ~(1 << bit);
	}
	//setbit 不清除过期时间
	::incrSlotsRefCount(this, key, metainfo, 1, false);
	kset_one(this, metainfo, key, val, log_type);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return orig;
}

int SSDBImpl::getbit(const Bytes &key, int bitoffset){
	std::string val;

	int ret = this->get(key, &val, NULL);
	if(ret <= 0){
		return ret;
	}

	int len = bitoffset / 8;
	int bit = bitoffset % 8;
	if(len >= val.size()){
		return 0;
	}
	return val[len] & (1 << bit);
}

int SSDBImpl::clear(const Bytes &key, char log_type, bool ttl_clear) {
	TMH metainfo = {0};
	int ret = this->loadobjectbyname(&metainfo, key, 0, false);
	if (ret <= 0) {
		return 0;
	}
	if (ttl_clear) {
		Transaction trans(binlogs);

		::incrSlotsRefCount(this, key, metainfo, -1, ttl_clear);
		incrNsRefCount(DataType::KSIZE, key, -1);
		std::string datakey = encode_slotskvData_key(key);
		binlogs->Delete(datakey);
		binlogs->add_log(log_type, BinlogCommand::KDEL, datakey);
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			log_error("delkv commit error: %s", s.ToString().c_str());
			return -1;
		}
	}else{
		::incrSlotsRefCount(this, key, metainfo, -1, ttl_clear);
		incrNsRefCount(DataType::KSIZE, key, -1);
		std::string datakey = encode_slotskvData_key(key);
		binlogs->Delete(datakey);
		binlogs->add_log(log_type, BinlogCommand::KDEL, datakey);
	}
	return 1;
}

int SSDBImpl::append(const Bytes &key, const Bytes &val, uint64_t *newlen, char log_type){
	if(key.empty()){
		log_error("empty key!");
		return 0;
	}
	Transaction trans(binlogs);
	std::string old, newval;
	TMH metainfo = {0};
	int found = this->get(key, &old, &metainfo);
	if (found < 0){
		return found;
	}

	newval.append(old);
	newval.append(val.data());
	*newlen = newval.size();

	//append 命令不清除过期时间m
	::incrSlotsRefCount(this, key, metainfo, 1, false);
	kset_one(this, metainfo, key, newval, log_type);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::setex(const Bytes &key, int64_t expire, const Bytes &val, char log_type){
	if(key.empty()){
		log_error("empty key!");
		return 0;
	}

	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, key, DataType::KSIZE);
	if (ret < 0){
		return ret;
	}

	Transaction trans(binlogs);

	::incrSlotsRefCount(this, key, metainfo, 1, false);

	metainfo.expire = expire;
	kset_one(this, metainfo, key, val, log_type);

   	ret = this->expiration->set_ttl(key, expire, log_type);
    if (ret < 0){
        return -1;
    }

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("setex error: %s", s.ToString().c_str());
		return -1;
	}

	return 1;
}

int SSDBImpl::batch_del_kv_names(const std::map<std::string, std::string>& kvmap)
{
    for(auto it = kvmap.cbegin(); it != kvmap.cend(); it++){
    	clear(it->first);
	}
	
	return 1;
}
