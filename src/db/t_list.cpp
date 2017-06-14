#include "t_list.h"


static int lget_by_seq(leveldb::DB* db, const TMH &metainfo, const Bytes &name, uint64_t seq, std::string *val){
	std::string key = EncodeListKey(name, seq, metainfo.version);
	std::string strdataval;
	leveldb::Status s;

	s = db->Get(leveldb::ReadOptions(), key, &strdataval);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		log_error("Get() error!");
		return -1;
	}else{
		DecodeDataValue(DataType::LIST, strdataval, val);
		return 1;
	}
}

static int lget_uint64(leveldb::DB* db, const TMH &metainfo, const Bytes &name, uint64_t seq, uint64_t *ret){
	std::string val;
	*ret = 0;
	int s = lget_by_seq(db, metainfo, name, seq, &val);
	if(s == 1){
		if(val.size() != sizeof(uint64_t)){
			return -1;
		}
		*ret = *(uint64_t *)val.data();
	}
	return s;
}

static int ldel_one(SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, uint64_t seq){
	std::string key = EncodeListKey(name, seq, metainfo.version);
	leveldb::Status s;

	ssdb->binlogs->Delete(key);
	return 0;
}

static int lset_one(SSDBImpl *ssdb, const TMH &metainfo, const Bytes &name, uint64_t seq, const Bytes &item){
	std::string key = EncodeListKey(name, seq, metainfo.version);
	std::string val = EncodeDataValue(item);
	leveldb::Status s;

	ssdb->binlogs->Put(key, val);
	return 0;
}

static int64_t incr_lsize(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, int64_t incr){
	std::string metalkey = EncodeMetaKey(name);
	
	metainfo.refcount += incr;

	//为0和1才在函数内部处理
	ssdb->incrSlotsRefCount(name, metainfo, incr);

	if(metainfo.refcount == 0){
		ssdb->binlogs->Delete(metalkey);
		ldel_one(ssdb, metainfo, name, QFRONT_SEQ);
		ldel_one(ssdb, metainfo, name, QBACK_SEQ);
	}else{
		ssdb->binlogs->Put(metalkey, EncodeMetaVal(metainfo));
	}

	return metainfo.refcount;
}

/****************/

int64_t SSDBImpl::lsize(const Bytes &name){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	return metainfo.refcount;
}

// @return 0: empty queue, 1: item peeked, -1: error
int SSDBImpl::lfront(const Bytes &name, std::string *item){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	uint64_t seq;
	ret = lget_uint64(this->ldb, metainfo, name, QFRONT_SEQ, &seq);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}
	ret = lget_by_seq(this->ldb, metainfo, name, seq, item);
	return ret;
}

// @return 0: empty queue, 1: item peeked, -1: error
int SSDBImpl::lback(const Bytes &name, std::string *item){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	uint64_t seq;
	ret = lget_uint64(this->ldb, metainfo, name, QBACK_SEQ, &seq);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}
	ret = lget_by_seq(this->ldb, metainfo, name, seq, item);
	return ret;
}

int64_t SSDBImpl::_lpush(const Bytes &name, const Bytes &item, uint64_t front_or_back_seq, char log_type){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret < 0){
		return ret;
	}
	//需要写在Transcation之前,否则会死锁,因为push_clean_queue可能同步信息,所以没新写Batch
	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	// generate seq
	uint64_t seq;
	ret = lget_uint64(this->ldb, metainfo, name, front_or_back_seq, &seq);
	if(ret == -1){
		return -1;
	}
	// update front and/or back
	if(ret == 0){
		seq = QITEM_SEQ_INIT;
		initVersion(metainfo, ret, DataType::LSIZE);
		ret = lset_one(this, metainfo, name, QFRONT_SEQ, Bytes(&seq, sizeof(seq)));
		if(ret == -1){
			return -1;
		}
		ret = lset_one(this, metainfo, name, QBACK_SEQ, Bytes(&seq, sizeof(seq)));
	}else{
        //QITEM_SEQ_INIT(9223372036854775807/2)
		seq += (front_or_back_seq == QFRONT_SEQ)? -1 : +1;
		ret = lset_one(this, metainfo, name, front_or_back_seq, Bytes(&seq, sizeof(seq)));
	}
	if(ret == -1){
		return -1;
	}
	if(seq <= QITEM_MIN_SEQ || seq >= QITEM_MAX_SEQ){
		log_info("queue is full, seq: %" PRIu64 " out of range", seq);
		return -1;
	}

	// prepend/append item
    ret = lset_one(this, metainfo, name, seq, item);
	if(ret == -1){
		return -1;
	}

	std::string buf = EncodeListKey(name, seq, metainfo.version);
	if(front_or_back_seq == QFRONT_SEQ){
		binlogs->add_log(log_type, BinlogCommand::LPUSH_FRONT, buf);
	}else{
		binlogs->add_log(log_type, BinlogCommand::LPUSH_BACK, buf);
	}

	// update size
	int64_t size = incr_lsize(this, metainfo, name, 1);
	if(size == -1){
		return -1;
	}

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return size;
}

int64_t SSDBImpl::lpush_front(const Bytes &name, const Bytes &item, char log_type){
	return _lpush(name, item, QFRONT_SEQ, log_type);
}

int64_t SSDBImpl::lpush_back(const Bytes &name, const Bytes &item, char log_type){
	return _lpush(name, item, QBACK_SEQ, log_type);
}

int SSDBImpl::_lpop(const Bytes &name, std::string *item, uint64_t front_or_back_seq, char log_type){
	Transaction trans(binlogs);

	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	uint64_t seq;
	ret = lget_uint64(this->ldb, metainfo, name, front_or_back_seq, &seq);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	ret = lget_by_seq(this->ldb, metainfo, name, seq, item);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	// delete item
	ret = ldel_one(this, metainfo, name, seq);
	if(ret == -1){
		return -1;
	}

	if(front_or_back_seq == QFRONT_SEQ){
		binlogs->add_log(log_type, BinlogCommand::LPOP_FRONT, name.String());
	}else{
		binlogs->add_log(log_type, BinlogCommand::LPOP_BACK, name.String());
	}

	// update size
	int64_t size = incr_lsize(this, metainfo, name, -1);
	if(size == -1){
		return -1;
	}

	// update front
	if(size > 0){
		seq += (front_or_back_seq == QFRONT_SEQ)? +1 : -1;
		//log_debug("seq: %" PRIu64 ", ret: %d", seq, ret);
		ret = lset_one(this, metainfo, name, front_or_back_seq, Bytes(&seq, sizeof(seq)));
		if(ret == -1){
			return -1;
		}
	}

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return 1;
}

int SSDBImpl::litems(Iterator **it, const Bytes &name, const Bytes &seq_s, const Bytes &seq_e, uint64_t limit){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	std::string start;
	std::string end;
	
	start = EncodeListKey(name, seq_s, metainfo.version);
	end = EncodeListKey(name, seq_e, metainfo.version);
	
	*it = IteratorFactory::iterator(this, kQIter, name, metainfo.version, start, end, limit);
	return 1;
}

// @return 0: empty queue, 1: item popped, -1: error
int SSDBImpl::lpop_front(const Bytes &name, std::string *item, char log_type){
	return _lpop(name, item, QFRONT_SEQ, log_type);
}

int SSDBImpl::lpop_back(const Bytes &name, std::string *item, char log_type){
	return _lpop(name, item, QBACK_SEQ, log_type);
}

int SSDBImpl::lget(const Bytes &name, int64_t index, std::string *item){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	uint64_t seq;
	if(index >= 0){
		ret = lget_uint64(this->ldb, metainfo, name, QFRONT_SEQ, &seq);
		seq += index;
	}else{
		ret = lget_uint64(this->ldb, metainfo, name, QBACK_SEQ, &seq);
		seq += index + 1;
	}
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	ret = lget_by_seq(this->ldb, metainfo, name, seq, item);
	return ret;
}

int SSDBImpl::lslice(const Bytes &name, int64_t begin, int64_t end,	std::vector<std::string> *list){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	if (ret & ObjectErr::object_expire){
		return 0;
	}
	uint64_t seq_begin, seq_end;
	if(begin >= 0 && end >= 0){
		uint64_t tmp_seq;
		ret = lget_uint64(this->ldb, metainfo, name, QFRONT_SEQ, &tmp_seq);
		if(ret != 1){
			return ret;
		}
		seq_begin = tmp_seq + begin;
		seq_end = tmp_seq + end;
	}else if(begin < 0 && end < 0){
		uint64_t tmp_seq;
		ret = lget_uint64(this->ldb, metainfo, name, QBACK_SEQ, &tmp_seq);
		if(ret != 1){
			return ret;
		}
		seq_begin = tmp_seq + begin + 1;
		seq_end = tmp_seq + end + 1;
	}else{
		uint64_t f_seq, b_seq;
		ret = lget_uint64(this->ldb, metainfo, name, QFRONT_SEQ, &f_seq);
		if(ret != 1){
			return ret;
		}
		ret = lget_uint64(this->ldb, metainfo, name, QBACK_SEQ, &b_seq);
		if(ret != 1){
			return ret;
		}
		if(begin >= 0){
			seq_begin = f_seq + begin;
		}else{
			seq_begin = b_seq + begin + 1;
		}
		if(end >= 0){
			seq_end = f_seq + end;
		}else{
			seq_end = b_seq + end + 1;
		}
	}

	for(; seq_begin <= seq_end; seq_begin++){
		std::string item;
		ret = lget_by_seq(this->ldb, metainfo, name, seq_begin, &item);
		if(ret == -1){
			return -1;
		}
		if(ret == 0){
			return 0;
		}
		list->push_back(item);
	}
	return 0;
}

int SSDBImpl::lclear(const Bytes &name, char log_type, bool delttl){
	if (releasehandler->push_clear_queue(name, log_type, BinlogCommand::LCLEAR, delttl) == 1){
		incrNsRefCount(DataType::LSIZE, name, -1);
	}
	return 1;
}

int SSDBImpl::lset(const Bytes &name, int64_t index, const Bytes &item, char log_type){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	//需要写在Transcation之前,否则会死锁,因为push_clean_queue可能同步信息,所以没新写Batch
	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	int64_t size = metainfo.refcount;
	if(size <= -1){
		return -1;
	}
	if(index >= size || index < -size){
		return 0;
	}
	
	uint64_t seq;
	if(index >= 0){
		ret = lget_uint64(this->ldb, metainfo, name, QFRONT_SEQ, &seq);
		seq += index;
	}else{
		ret = lget_uint64(this->ldb, metainfo, name, QBACK_SEQ, &seq);
		seq += index + 1;
	}
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	ret = lset_one(this, metainfo, name, seq, item);
	if(ret == -1){
		return -1;
	}

	std::string buf = EncodeListKey(name, seq, metainfo.version);
	binlogs->add_log(log_type, BinlogCommand::LSET, buf);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}

	return 1;
}

int SSDBImpl::lset_by_seq(const Bytes &name, uint64_t seq, const Bytes &item, char log_type){
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::LSIZE);
	if (ret <= 0){
		return ret;
	}
	//需要写在Transcation之前,否则会死锁,因为push_clean_queue可能同步信息,所以没新写Batch

	Transaction trans(binlogs);
	checkObjectExpire(metainfo, name, ret);
	int64_t size = metainfo.refcount;
	uint64_t min_seq, max_seq;

	ret = lget_uint64(this->ldb, metainfo, name, QFRONT_SEQ, &min_seq);
	if(ret == -1){
		return -1;
	}
	max_seq = min_seq + size;
	if(seq < min_seq || seq > max_seq){
		return 0;
	}

	ret = lset_one(this, metainfo, name, seq, item);
	if(ret == -1){
		return -1;
	}

	std::string buf = EncodeListKey(name, seq, metainfo.version);
	binlogs->add_log(log_type, BinlogCommand::LSET, buf);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return 1;
}

int SSDBImpl::llist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list){
	Iterator *it = scan_slot(DataType::LSIZE, slot, name_s, name_e, limit);
	while(it->next()){
		list->push_back(it->name());
	}
	delete it;
	return 1;
}

int SSDBImpl::lrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
					uint64_t limit, std::vector<std::string> *list){
	Iterator *it = rscan_slot(DataType::LSIZE, slot, name_s, name_e, limit);
	while(it->next()){
		list->push_back(it->name());
	}
	delete it;
	return 1;
}


int SSDBImpl::batch_del_list_names(const std::map<std::string, std::vector<std::string>>& kvmap) {
	for (auto itmap = kvmap.begin(); itmap != kvmap.end(); itmap ++){
		lclear(itmap->first);
	}
	
	return 1;
}
