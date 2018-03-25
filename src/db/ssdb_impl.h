/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_IMPL_H_
#define SSDB_IMPL_H_

#include <vector>
#include <map>
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"

#include "../util/log.h"
#include "../util/config.h"
#include "../util/slots.h"
#include "../util/strings.h"
#include "../util/methods.h"
#include "t_meta.h"
#include "binlog.h"
#include "iterator.h"
#include "ttl.h"
#include "rubbish_release.h"

inline
static leveldb::Slice slice(const Bytes &b){
	return leveldb::Slice(b.data(), b.size());
}

class SSDBImpl : public SSDB
{
private:
	friend class SSDB;
	
	leveldb::Options options;
	
	SSDBImpl();
public:
	BinlogQueue *binlogs;
	leveldb::DB* ldb;
	bool is_mirror_;
	ExpirationHandler *expiration;
	RubbishRelease *releasehandler;
	std::string dbpath;
	int is_compact_ = 0;

	void IncreaseParallelism();
	void OptimizeLevelCompaction(uint64_t memtable_memory_budget);
	virtual ~SSDBImpl();
	virtual int flushdb();
	virtual leveldb::DB* getlDb() const { return ldb; }
	virtual BinlogQueue *getBinlog() const { return binlogs; }
	virtual bool mirror() const { return is_mirror_; }

	// return (start, end], not include start
	//virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit);
	//virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit);

	virtual uint64_t size();
	virtual std::vector<std::string> info();
	virtual void compact();
	virtual int key_range(std::vector<std::string> *keys){return 0;}
	
	/* raw operates */

	// repl: whether to sync this operation to slaves
	virtual int raw_set(const Bytes &key, const Bytes &val);
	virtual int raw_del(const Bytes &key);
	virtual int raw_get(char cmdtype, const Bytes &key, std::string *val);

	/* global */
	virtual int del(const Bytes &key, char log_type=BinlogType::SYNC, bool delttl = true);
	virtual int64_t ttl(const Bytes &name);
	virtual int expire(const Bytes &name, int64_t ttl);
	virtual int persist(const Bytes &name);
	virtual int exists(const Bytes &key, std::string *type);
	/* key value */

	virtual int set(const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC);
	virtual int setnx(const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC);
	virtual int clear(const Bytes &key, char log_type=BinlogType::SYNC, bool lock_clear = true);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int incr(const Bytes &key, int64_t by, int64_t *new_val, char log_type=BinlogType::SYNC);

	virtual int multi_set(const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC);
	virtual int multi_setnx(const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC);		

	virtual int setbit(const Bytes &key, int bitoffset, int on, char log_type=BinlogType::SYNC);
	virtual int getbit(const Bytes &key, int bitoffset);
	
	virtual int setex(const Bytes &key, int64_t expire, const Bytes &val, char log_type=BinlogType::SYNC);
	virtual int append(const Bytes &key, const Bytes &val, uint64_t *newlen, char log_type=BinlogType::SYNC);

	virtual int get(const Bytes &key, std::string *val, TMH *metainfo = NULL);
	virtual int getset(const Bytes &key, std::string *val, const Bytes &newval, char log_type=BinlogType::SYNC);

	/* hash */

	virtual int hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC);
	virtual int hsetv(const Bytes &name, const Bytes &key, const Bytes &val, int64_t *version, char log_type=BinlogType::SYNC);
	virtual int hdel(const Bytes &name, const Bytes &key, char log_type=BinlogType::SYNC);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type=BinlogType::SYNC);
	//int multi_hset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC);
	//int multi_hdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0, char log_type=BinlogType::SYNC);
	virtual int hsetnx(const Bytes &name, const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC);
	virtual int64_t hsize(const Bytes &name);
	virtual int hclear(const Bytes &name, char log_type=BinlogType::SYNC, bool delttl = true);
	virtual int hget(const Bytes &name, const Bytes &key, std::string *val);
	virtual int hgetv(const Bytes &name, const Bytes &key, std::string *val, int64_t *version);
	virtual int hgetall(const Bytes &name, std::vector<std::string> *resp);
	virtual int hlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list);
	virtual int hrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list);

	virtual int hscan(Iterator **it, const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit);
	virtual int hrscan(Iterator **it, const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit);
	virtual int hset_rubbish_queue(const Bytes &name, const Bytes &key, const Bytes &val);

	/* zset */

	virtual int zset(const Bytes &name, const Bytes &key, const Bytes &score, char log_type=BinlogType::SYNC);
	virtual int zdel(const Bytes &name, const Bytes &key, char log_type=BinlogType::SYNC);
	virtual int zclear(const Bytes &name, char log_type=BinlogType::SYNC, bool delttl = true);
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type=BinlogType::SYNC);
	//int multi_zset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC);
	//int multi_zdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0, char log_type=BinlogType::SYNC);
	
	virtual int64_t zsize(const Bytes &name);
	virtual int64_t zcount(const Bytes &name, const Bytes &s_score, const Bytes &e_score ,uint64_t &count);
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &name, const Bytes &key, std::string *score);
	virtual int64_t zrank(const Bytes &name, const Bytes &key);
	virtual int64_t zrrank(const Bytes &name, const Bytes &key);
	virtual int zrange(Iterator **it, const Bytes &name, uint64_t offset, uint64_t limit);
	virtual int zrrange(Iterator **it, const Bytes &name, uint64_t offset, uint64_t limit);
	virtual int zremrangebyrank(const Bytes &name, uint64_t offset, uint64_t limit, uint64_t &count);
	virtual int zremrangebyscore(const Bytes &name, const Bytes &score_start, const Bytes &score_end, uint64_t &count);
	virtual int zrangebyscore(const Bytes &name, const Bytes &key, const Bytes &score_start, const Bytes &score_end,
						uint64_t offset, uint64_t limit, std::vector<std::string> *resp);
	virtual int zrevrangebyscore(const Bytes &name, const Bytes &key, const Bytes &score_start, 
						const Bytes &score_end, uint64_t offset, uint64_t limit, std::vector<std::string> *resp);
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual int zscan(Iterator **it, const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit);
	virtual int zrscan(Iterator **it, const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit);

	virtual int zlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list);
	virtual int zrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list);
	//virtual int64_t zfix(const Bytes &name);
	
	virtual int64_t lsize(const Bytes &name);
	virtual int litems(Iterator **it, const Bytes &name, const Bytes &seq_s, const Bytes &seq_e, uint64_t limit);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int lfront(const Bytes &name, std::string *item);
	virtual int lclear(const Bytes &name, char log_type=BinlogType::SYNC, bool delttl = true);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int lback(const Bytes &name, std::string *item);
	// @return -1: error, other: the new length of the queue
	virtual int64_t lpush_front(const Bytes &name, const Bytes &item, char log_type=BinlogType::SYNC);
	virtual int64_t lpush_back(const Bytes &name, const Bytes &item, char log_type=BinlogType::SYNC);
	// @return 0: empty queue, 1: item popped, -1: error
	virtual int lpop_front(const Bytes &name, std::string *item, char log_type=BinlogType::SYNC);
	virtual int lpop_back(const Bytes &name, std::string *item, char log_type=BinlogType::SYNC);
	//virtual int qfix(const Bytes &name);
	virtual int llist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list);
	virtual int lrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list);

	virtual int lslice(const Bytes &name, int64_t offset, int64_t limit,
			std::vector<std::string> *list);
	virtual int lget(const Bytes &name, int64_t index, std::string *item);
	virtual int lset(const Bytes &name, int64_t index, const Bytes &item, char log_type=BinlogType::SYNC);
	virtual int lset_by_seq(const Bytes &name, uint64_t seq, const Bytes &item, char log_type=BinlogType::SYNC);
	
	//ttl
	virtual int zdel_ttl(const Bytes &name, const Bytes &key, char log_type = BinlogType::NONE);
	virtual int zset_ttl(const Bytes &name, const Bytes &key, const Bytes &score, 
				char log_type = BinlogType::SYNC, bool lock = false);	
	virtual int sync_zset_ttl(const Bytes &name, const Bytes &key, const Bytes &score, 
					  const char *mstIp, int mstPort, char log_type=BinlogType::SYNC);

	//global
	virtual Iterator* scan_slot(char type, unsigned int slot, const Bytes &start, const Bytes &end, uint64_t limit);
	virtual Iterator* rscan_slot(char type, unsigned int slot, const Bytes &start, const Bytes &end, uint64_t limit);
	int findoutName(const Bytes &name, uint32_t ref);
	virtual void calculateSlotRefer(const std::string &encode_slot_id, int ref);

	void removeExpire(const Bytes &name, TMH &metainfo);
	virtual int incrSlotsRefCount(const Bytes &name, TMH &metainfo, int incr);
 	
 	virtual int VerifyStructState(TMH *pth, const Bytes &name, const char datatype, std::string *val = nullptr);
 	virtual int loadobjectbyname(TMH *pth, const Bytes &name, const char datatype, bool expire, std::string *val = nullptr);
 	void checkObjectExpire(TMH &metainfo, const Bytes &name, int ret);
 	void incrNsRefCount(char datatype, const Bytes &name, int incr);
 	int batch_del_kv_names(const std::map<std::string, std::string>& kvmap);
    int batch_del_hash_names(const std::map<std::string, std::map<std::string, std::string> > &kvmap);
	int batch_del_zset_names(const std::map<std::string, std::map<std::string, std::string> >& kvmap);
	int batch_del_list_names(const std::map<std::string, std::vector<std::string> >& kvmap);	

	void MemoryInfo(std::string &info);
	void DiskInfo(std::string &info);
	virtual void DataInfo(std::string &info);
	int IsCompact(){
		return is_compact_;
	}
private:
	int64_t _lpush(const Bytes &name, const Bytes &item, uint64_t front_or_back_seq, char log_type=BinlogType::SYNC);
	int _lpop(const Bytes &name, std::string *item, uint64_t front_or_back_seq, char log_type=BinlogType::SYNC);

};

#endif
