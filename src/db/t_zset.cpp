#include "t_zset.h"
#include "../cluster_migrate.h"

//元信息
static int zset_one(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, const Bytes &key, const Bytes &score, char log_type);
static int incr_zsize(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, int incr);
static int zdel_one(SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, const Bytes &key, char log_type);
static int get_zsetvalue(const SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, const Bytes &key, std::string *val);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::zset(const Bytes &name, const Bytes &key, const Bytes &score, char log_type){
	int ret = validInt64(score);
	if (ret < 0)
		return ret;
	
	TMH metainfo = {0};
	ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret < 0 ){
		return ret;
	}
	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	ret = zset_one(this, metainfo, name, key, score, log_type);

	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, metainfo, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("zset error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::zdel(const Bytes &name, const Bytes &key, char log_type){
	Transaction trans(binlogs);
	TMH metainfo = {0};
	int ret  = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	ret = zdel_one(this, metainfo, name, key, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, metainfo, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("zdel error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::zincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);

	if (ret < 0 ){
		return ret;
	}
	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	std::string old;
	ret = get_zsetvalue(this, metainfo, name, key, &old);

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

	ret = zset_one(this, metainfo, name, key, str(*new_val), log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, metainfo, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("zset error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

int64_t SSDBImpl::zsize(const Bytes &name){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
    return metainfo.refcount;
}

int64_t SSDBImpl::zcount(const Bytes &name, const Bytes &s_score, const Bytes &e_score, uint64_t &count){
	count = 0;
	Iterator *it = nullptr;
	int ret = zscan(&it, name, "", s_score, e_score, -1);
	if (ret <= 0){
		return ret;
	}

	while(it->next()){
		count++;
	}
	delete it;
	return ret;
}

int SSDBImpl::zget(const Bytes &name, const Bytes &key, std::string *score){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	return get_zsetvalue(this, metainfo, name, key, score);
}

static Iterator* ziterator(
	const SSDBImpl *ssdb, const TMH &metainfo,
	const Bytes &name, const Bytes &key_start,
	const Bytes &score_start, const Bytes &score_end,
	uint64_t limit, Iterator::Direction direction)
{
	if(direction == Iterator::FORWARD){
		std::string start, end;
		if(score_start.empty()){
			start = EncodeSortSetScoreKey(name, key_start, SSDB_SCORE_MIN, metainfo.version);
		}else{
			start = EncodeSortSetScoreKey(name, key_start, score_start, metainfo.version);
		}
		if(score_end.empty()){
			end = EncodeSortSetScoreKey(name, "\xff", SSDB_SCORE_MAX, metainfo.version);
		}else{
			end = EncodeSortSetScoreKey(name, "\xff", score_end, metainfo.version);
		}
		return IteratorFactory::iterator(ssdb, kZIter, name, metainfo.version, start, end, limit);
	}else{
		std::string start, end;
		if(score_start.empty()){
			start = EncodeSortSetScoreKey(name, key_start, SSDB_SCORE_MAX, metainfo.version);
		}else{
			if(key_start.empty()){
				start = EncodeSortSetScoreKey(name, "\xff", score_start, metainfo.version);
			}else{
				start = EncodeSortSetScoreKey(name, key_start, score_start, metainfo.version);
			}
		}
		if(score_end.empty()){
			end = EncodeSortSetScoreKey(name, "", SSDB_SCORE_MIN, metainfo.version);
		}else{
			end = EncodeSortSetScoreKey(name, "", score_end, metainfo.version);
		}

		return IteratorFactory::rev_iterator(ssdb, kZIter, name, metainfo.version, start, end, limit);
	}
}

int64_t SSDBImpl::zrank(const Bytes &name, const Bytes &key){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	Iterator *it = ziterator(this, metainfo, name, "", "", "", INT_MAX, Iterator::FORWARD);
	int64_t index = 0;
	while(true){
		if(it->next() == false){
			index = -1;
			break;
		}
		if(key == it->key()){
			break;
		}
		index ++;
	}
	delete it;
	return index;
}

int64_t SSDBImpl::zrrank(const Bytes &name, const Bytes &key){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	Iterator *it = ziterator(this, metainfo, name, "", "", "", INT_MAX, Iterator::BACKWARD);
	int64_t index = 0;
	while(true){
		if(it->next() == false){
			index = -1;
			break;
		}
		if(key == it->key()){
			break;
		}
		index ++;
	}
	delete it;
	return index;
}

int SSDBImpl::zrangebyscore(const Bytes &name, const Bytes &key, const Bytes &score_start, const Bytes &score_end,
	uint64_t offset, uint64_t limit, std::vector<std::string> *resp){
	Iterator *it = nullptr;
	int ret = zscan(&it, name, key, score_start, score_end, limit);
	if (ret <= 0){
		return ret;
	}

	assert(it);
	if(offset > 0){
		it->skip(offset);
	}
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	delete it;

	return 1;
}

int SSDBImpl::zrevrangebyscore(const Bytes &name, const Bytes &key, const Bytes &score_start, 
	const Bytes &score_end, uint64_t offset, uint64_t limit, std::vector<std::string> *resp){
	Iterator *it = nullptr; 
	int ret = zrscan(&it, name, key, score_start, score_end, limit);
	if (ret <= 0){
		return ret;
	}

	assert(it);
	if(offset > 0){
		it->skip(offset);
	}

	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	delete it;

	return 1;
}

int SSDBImpl::zremrangebyrank(const Bytes &name, uint64_t offset, uint64_t limit, uint64_t &count){	
	count = 0;
	Iterator *it = nullptr;
	int ret = zrange(&it, name, offset, limit);
	if (ret <= 0){
		return ret;
	}
	while(it->next()){
		count ++;
		int ret = zdel(name, it->key());
		if(ret == -1){
			delete it;
			return -1;
		}
	}
	delete it;
	return 1;
}

int SSDBImpl::zremrangebyscore(const Bytes &name, const Bytes &score_start, const Bytes &score_end, uint64_t &count){
	Iterator *it = nullptr;
	int ret = zscan(&it, name, "", score_start, score_end, -1);
	if (ret <= 0){
		return ret;
	}
	count = 0;
	while(it->next()){
		count ++;
		int ret = zdel(name, it->key());
		if(ret == -1){
			delete it;
			return ret;
		}
	}
	delete it;
	return 1;
}

int SSDBImpl::zrange(Iterator **it, const Bytes &name, uint64_t offset, uint64_t limit){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	if(offset + limit > limit){
		limit = offset + limit;
	}
    *it = ziterator(this, metainfo, name, "", "", "", limit, Iterator::FORWARD);
	(*it)->skip(offset);
	return 1;
}

int SSDBImpl::zrrange(Iterator **it, const Bytes &name, uint64_t offset, uint64_t limit){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	if(offset + limit > limit){
		limit = offset + limit;
	}
	*it = ziterator(this, metainfo, name, "", "", "", limit, Iterator::BACKWARD);
	(*it)->skip(offset);
	return 1;
}

int SSDBImpl::zscan(Iterator **it, const Bytes &name, const Bytes &key,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit)
{
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		get_zsetvalue(this, metainfo, name, key, &score);
	}else{
		score = score_start.String();
	}

	*it = ziterator(this, metainfo, name, key, score, score_end, limit, Iterator::FORWARD);
	return 1;
}

int SSDBImpl::zrscan(Iterator **it, const Bytes &name, const Bytes &key,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit)
{
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		get_zsetvalue(this, metainfo, name, key, &score);
	}else{
		score = score_start.String();
	}
	*it = ziterator(this, metainfo, name, key, score, score_end, limit, Iterator::BACKWARD);
	return 1;
}

int SSDBImpl::zlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list){
	Iterator *it = scan_slot(DataType::ZSIZE, slot, name_s, name_e, limit);
	while(it->next()){
		list->push_back(it->name());
	}
	delete it;
	return 1;
}

int SSDBImpl::zrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
					uint64_t limit, std::vector<std::string> *list){
	Iterator *it = rscan_slot(DataType::ZSIZE, slot, name_s, name_e, limit);
	while(it->next()){
		list->push_back(it->name());
	}
	delete it;
	return 1;
}

// returns the number of newly added items
static std::string filter_score(const Bytes &score){
	int64_t s = score.Int64();
	return str(s);
}

static int zset_one(SSDBImpl *ssdb, TMH &taginfo, const Bytes &name, const Bytes &key, const Bytes &score, char log_type)
{
    if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return 0;
		//return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long!");
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long!");
		return -1;
	}
	initVersion(taginfo, taginfo.refcount, DataType::ZSIZE);
	
	std::string datakey = EncodeSortSetKey(name, key, taginfo.version);
	std::string datavalue;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &datavalue);
	std::string new_score = filter_score(score);
	int ret = 0;
	
	if(s.IsNotFound()){
		ssdb->binlogs->Put(datakey, EncodeDataValue(new_score));
		ssdb->binlogs->Put(EncodeSortSetScoreKey(name, key, new_score, taginfo.version), "");

        if (name == ssdb->expiration->listName()){
        	log_debug("set ttl to sync queue: name: %s, key: %s, core: %s", name.data(),
						key.data(), hexmem(score.data(), score.size()).data());
            ssdb->binlogs->add_log(log_type, BinlogCommand::ZSET_TTL, datakey);
        }else{
			ssdb->binlogs->add_log(log_type, BinlogCommand::ZSET, datakey);
		}
		ret = 1;
	}else{
		if(!s.ok()){
			return -1;
		}

		std::string old_score;
		ret = DecodeDataValue(DataType::ZSET, datavalue, &old_score);
		if (ret == -1){
		    log_debug("get error");
        	return -1;
		}
		if (old_score != new_score){
			ssdb->binlogs->Delete(EncodeSortSetScoreKey(name, key, old_score, taginfo.version));
			ssdb->binlogs->Put(datakey, EncodeDataValue(new_score));
			ssdb->binlogs->Put(EncodeSortSetScoreKey(name, key, new_score, taginfo.version), "");
			if (name == ssdb->expiration->listName()){
				log_debug("add ttl to sync queue: name: %s, key: %s, score: %s", name.data(),
									key.data(), hexmem(score.data(), score.size()).data());
                ssdb->binlogs->add_log(log_type, BinlogCommand::ZSET_TTL, datakey);
            }else{
                ssdb->binlogs->add_log(log_type, BinlogCommand::ZSET, datakey);
            }
		}
		ret = 0;
	}

	return ret;
}

static int zdel_one(SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, const Bytes &key, char log_type){

	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long!");
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long!");
		return -1;
	}
	std::string old_score;
	int ret = get_zsetvalue(ssdb, metainfo, name, key, &old_score);
	if (ret != 1){
    	return ret;
    }

	std::string k0, k1;

	// delete zscore key
	k1 = EncodeSortSetScoreKey(name, key, old_score, metainfo.version);
	ssdb->binlogs->Delete(k1);

	// delete zset
	k0 = EncodeSortSetKey(name, key, metainfo.version);
	ssdb->binlogs->Delete(k0);
	
	if (name != ssdb->expiration->listName()){
		ssdb->binlogs->add_log(log_type, BinlogCommand::ZDEL, k0);
	}else{
		ssdb->binlogs->add_log(log_type, BinlogCommand::ZDEL_TTL, k0);
	}

	return 1;
}

static int incr_zsize(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, int incr){
	std::string metalkey = EncodeMetaKey(name);

	metainfo.refcount += incr;

	ssdb->incrSlotsRefCount(name, metainfo, incr);

	if(metainfo.refcount == 0){
        ssdb->binlogs->Delete(metalkey);
	}else{
		ssdb->binlogs->Put(metalkey, EncodeMetaVal(metainfo));
	}

	return 0;
}

static int get_zsetvalue(const SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, const Bytes &key, std::string *val){
	std::string datakey = EncodeSortSetKey(name, key, metainfo.version);
	std::string datavalue;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &datavalue);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		return -1;
	}

	int ret = DecodeDataValue(DataType::ZSET, datavalue, val);
	if (ret == -1){
		return -1;
	}

	return 1;
}

int SSDBImpl::zclear(const Bytes &name, char log_type, bool delttl){
	if (releasehandler->push_clear_queue(name, log_type, BinlogCommand::ZCLEAR, delttl) == 1){
		incrNsRefCount(DataType::ZSIZE, name, -1);
	}
	return 1;
}

int SSDBImpl::zset_ttl(const Bytes &name, const Bytes &key, const Bytes &score, char log_type, bool lock){

	TMH metainfo = {0};

	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret < 0 ) {
		return ret;
	}
	if (lock) {
		Transaction trans(binlogs);
		TMH pTagInfo = {0};
		std::string val;
		loadobjectbyname(&pTagInfo, key, 0, false, &val);

		pTagInfo.expire = score.Int64();
		binlogs->Put(EncodeMetaKey(key), EncodeMetaVal(pTagInfo, val));
		expiration->set_fastkey(key, pTagInfo.expire);
		//需要进行从服务器队列激活
		ret = zset_one(this, metainfo, name, key, score, log_type);

		if (ret > 0) {
			if (incr_zsize(this, metainfo, name, ret) == -1) {
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			return -1;
		}
	} else{

		//需要进行从服务器队列激活
		ret = zset_one(this, metainfo, name, key, score, log_type);

		if (ret > 0) {
			if (incr_zsize(this, metainfo, name, ret) == -1) {
				return -1;
			}
		}
	}

	return ret;
}

// int SSDBImpl::zset_ttl(const Bytes &name, const Bytes &key, 
// 		const Bytes &score, char log_type, bool resetMetal){
// 	log_debug("-----------------------0000000");
//     Transaction trans(binlogs);
//     	log_debug("-----------------------1111111");

// 	TMH metainfo = {0};
	
// 	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
// 	if (ret < 0 ){
// 		return ret;
// 	}

// 	//过期队列不过期,不进行过期判断
//     if (resetMetal){
//     	TMH pTagInfo = {0};
//     	std::string val;
//     	loadobjectbyname(&pTagInfo, key, 0, false, &val);

//     	pTagInfo.expire = score.Int64();
//     	binlogs->Put(EncodeMetaKey(key), EncodeMetaVal(pTagInfo, val));
//     }
//     //需要进行从服务器队列激活
//     ret = zset_one(this, metainfo, name, key, score, log_type, true);

//     if(ret >= 0){
//         if(ret > 0){
//             if(incr_zsize(this, metainfo, name, ret) == -1){
//                 return -1;
//             }
//         }
//         leveldb::Status s = binlogs->commit();
//         if(!s.ok()){
//             log_error("zset error: %s", s.ToString().c_str());
//             return -1;
//         }
//     }
//     return ret;
// }

int SSDBImpl::zdel_ttl(const Bytes &name, const Bytes &key, char log_type) {
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0 ){
		return ret;
	}
 	ret = zdel_one(this, metainfo, name, key, log_type);

 	if (ret > 0){
		if(incr_zsize(this, metainfo, name, -ret) == -1){
       		return -1;
       	}
 	}
	return 1;
}

//如果不单独写的话,removeExpire没法实现是一个原子操作.就需要在所有的需要删除过期时间的操作外面加锁
// int SSDBImpl::zdel_ttl(const Bytes &name, const Bytes &key, char log_type){ 
// 	//leveldb::WriteBatch batch;
// 	log_debug("errrrrrrrrrrrrrr");
	// TMH metainfo = {0};
	// int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	// if (ret <= 0 ){
	// 	return ret;
	// }
	// //过期队列不判断过期(ret == 2)
	// std::string old_score;
	// ret = get_zsetvalue(this, metainfo, name, key, &old_score);
	// if (ret != 1){
 //    	return ret;
 //    }
 //    log_debug("del_ttl--------------------:%s:%s", name.data(), key.data());
	// // delete zscore key
	// std::string k1 = EncodeSortSetScoreKey(name, key, old_score, metainfo.version);
	// binlogs->Delete(k1);

	// // delete zset
	// std::string k0 = EncodeSortSetKey(name, key, metainfo.version);
	// binlogs->Delete(k0);

	// //不需要统计引用个数
	// if(--metainfo.refcount == 0){
	// 	binlogs->Delete(EncodeMetaKey(name));
 //        //batch.Delete(EncodeMetaKey(name));
	// }else{
	// 	binlogs->Put(EncodeMetaKey(name), EncodeMetaVal(metainfo));
	// 	//batch.Put(EncodeMetaKey(name), EncodeMetaVal(metainfo));
	// }
	// binlogs->add_log(log_type, BinlogCommand::ZSET_TTL, datakey);
    //leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &batch);
	// if(!s.ok()){
	// 	log_error("zdel error: %s", s.ToString().c_str());
	// 	return -1;
	// }
	// leveldb::WriteBatch batch;

	// TMH metainfo = {0};
	// int ret = VerifyStructState(&metainfo, name, DataType::ZSIZE);
	// if (ret <= 0 ){
	// 	return ret;
	// }
	// //过期队列不判断过期(ret == 2)
	// std::string old_score;
	// ret = get_zsetvalue(this, metainfo, name, key, &old_score);
	// if (ret != 1){
 //    	return ret;
 //    }
 //    //log_debug("del_ttl--------------------:%s:%s", name.data(), key.data());
	// // delete zscore key
	// std::string k1 = EncodeSortSetScoreKey(name, key, old_score, metainfo.version);
	// batch.Delete(k1);

	// // delete zset
	// std::string k0 = EncodeSortSetKey(name, key, metainfo.version);
	// batch.Delete(k0);

	// //不需要统计引用个数
	// if(--metainfo.refcount == 0){
 //        batch.Delete(EncodeMetaKey(name));
	// }else{
	// 	batch.Put(EncodeMetaKey(name), EncodeMetaVal(metainfo));
	// }

 //    leveldb::Status s = ldb->Write(leveldb::WriteOptions(), &batch);
	// if(!s.ok()){
	// 	log_error("zdel error: %s", s.ToString().c_str());
	// 	return -1;
	// }
//     return 1;
// }

int SSDBImpl::sync_zset_ttl(const Bytes &name, const Bytes &key, const Bytes &score,
		const char *mstIp, int mstPort, char log_type){    
    Transaction trans(binlogs);
    TMH metainfo = {0};

    log_debug("sync_zset_ttl_info: key:%s, score str:%s, remote ip: %s, remote port: %d", 
	 			hexmem(key.data(), key.size()).data(), 
	 			hexmem(score.data(), score.size()).data(), 
	 			mstIp, mstPort);

    std::string val;
    int ret = loadobjectbyname(&metainfo, key, 0, true, &val);
    checkObjectExpire(metainfo, key, ret);

	//表示命令延迟，key信息已经被删除了，需要向master恢复这个key
    if (ret <= 0 || ret & ObjectErr::object_expire){
    	int fd = ClusterMigrateHandle::instance()->fetchMgrtSocketFd(mstIp, mstPort, 10000);
    	if (fd <= 0 ){
    		return -1;
    	}
    	ClusterMigrateHandle::instance()->retrieveKey(fd, key, 10000);
    }else {
    	ret = expiration->set_ttl(key, score.Int64(), log_type);

    	metainfo.expire = score.Int64();
    	binlogs->Put(EncodeMetaKey(key), EncodeMetaVal(metainfo, val));

        expiration->activeExpireQueue();
        leveldb::Status s = binlogs->commit();
    	if(!s.ok()) {
        	return -1;
    	}
    	return ret;
    }
	return 1;
}


int SSDBImpl::batch_del_zset_names(const std::map<std::string, std::map<std::string, std::string>>& kvmap) {
	for (auto itmap = kvmap.begin(); itmap != kvmap.end(); itmap ++){
		zclear(itmap->first);
	}
	return 1;
}
