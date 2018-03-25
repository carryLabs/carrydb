/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_H_
#define SSDB_H_

#include <vector>
#include <string>
#include <set>
#include "../util/const.h"
#include "options.h"
#include "iterator.h"
#include "leveldb/db.h"
#include "binlog.h"

class Bytes;
class Config;

class SSDB{
public:
	SSDB(){}

	virtual ~SSDB(){};
	static SSDB* open(const Options &opt, const std::string &base_dir);
	
	virtual int flushdb() = 0;
	virtual leveldb::DB* getlDb() const = 0 ;
	virtual BinlogQueue *getBinlog() const = 0;
	virtual bool mirror() const= 0;
	virtual int loadobjectbyname(TMH *pth, const Bytes &name, const char datatype, bool expire, std::string *val = nullptr) = 0;
	virtual int VerifyStructState(TMH *pth, const Bytes &name, const char datatype, std::string *val = nullptr) = 0;
	virtual int incrSlotsRefCount(const Bytes &name, TMH &metainfo, int incr) = 0;
	virtual void calculateSlotRefer(const std::string &encode_slot_id, int ref) = 0;

	//void flushdb();
	virtual uint64_t size() = 0;
	virtual std::vector<std::string> info() = 0;
	virtual void compact() = 0;
	virtual int key_range(std::vector<std::string> *keys) = 0;
	virtual void DataInfo(std::string &info) = 0;

	/* raw operates */

	// repl: whether to sync this operation to slaves
	virtual int raw_set(const Bytes &key, const Bytes &val) = 0;
	virtual int raw_del(const Bytes &key) = 0;
	virtual int raw_get(char cmdtype, const Bytes &key, std::string *val) = 0;


	/* global */
	virtual int del(const Bytes &key, char log_type=BinlogType::SYNC, bool delttl = true) = 0;
	virtual int64_t ttl(const Bytes &name) = 0;
	virtual int expire(const Bytes &name, int64_t ttl) = 0;
	virtual int persist(const Bytes &name) = 0;
	virtual int exists(const Bytes &key, std::string *type) = 0;
	//global
	virtual Iterator* scan_slot(char type, unsigned int slot, const Bytes &start, const Bytes &end, uint64_t limit) = 0;
	virtual Iterator* rscan_slot(char type, unsigned int slot, const Bytes &start, const Bytes &end, uint64_t limit) = 0;

	/* key value */

	virtual int set(const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC) = 0;
	virtual int setnx(const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC) = 0;
	virtual int clear(const Bytes &key, char log_type=BinlogType::SYNC, bool lock_clear = true) = 0;
	// -1: error, 1: ok, 0: value is not an integer or out of range
    virtual int incr(const Bytes &key, int64_t by, int64_t *new_val, char log_type=BinlogType::SYNC) = 0;
	virtual int multi_set(const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC) = 0;
	virtual int multi_setnx(const std::vector<Bytes> &kvs, int offset=0, char log_type=BinlogType::SYNC) = 0;		
	
	virtual int setbit(const Bytes &key, int bitoffset, int on, char log_type=BinlogType::SYNC) = 0;
	virtual int getbit(const Bytes &key, int bitoffset) = 0;
	virtual int setex(const Bytes &key, int64_t expire, const Bytes &val, char log_type) = 0;
	virtual int append(const Bytes &key, const Bytes &val, uint64_t *newlen, char log_type) = 0;
	virtual int get(const Bytes &key, std::string *val, TMH *metainfo = NULL) = 0;
	virtual int getset(const Bytes &key, std::string *val, const Bytes &newval, char log_type=BinlogType::SYNC) = 0;

	/* hash */
	virtual int hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC) = 0;
	virtual int hsetv(const Bytes &name, const Bytes &key, const Bytes &val, int64_t *version, char log_type=BinlogType::SYNC) = 0;
	virtual int hdel(const Bytes &name, const Bytes &key, char log_type=BinlogType::SYNC) = 0;
	virtual int hsetnx(const Bytes &name, const Bytes &key, const Bytes &val, char log_type=BinlogType::SYNC) = 0;

	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type=BinlogType::SYNC) = 0;

	virtual int64_t hsize(const Bytes &name) = 0;
	virtual int hclear(const Bytes &name, char log_type=BinlogType::SYNC, bool delttl = true) = 0;
	virtual int hget(const Bytes &name, const Bytes &key, std::string *val) = 0;
	virtual int hgetv(const Bytes &name, const Bytes &key, std::string *val, int64_t *version) = 0;
	virtual int hgetall(const Bytes &name, std::vector<std::string> *resp) = 0;
	virtual int hlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list) = 0;
	virtual int hrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list) = 0;

	virtual int hscan(Iterator **it, const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit) = 0;
	virtual int hrscan(Iterator **it, const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit) = 0;
	virtual int hset_rubbish_queue(const Bytes &name, const Bytes &key, const Bytes &val) = 0;

	/* zset */

	virtual int zset(const Bytes &name, const Bytes &key, const Bytes &score, char log_type=BinlogType::SYNC) = 0;
	virtual int zdel(const Bytes &name, const Bytes &key, char log_type=BinlogType::SYNC) = 0;
	virtual int zclear(const Bytes &name, char log_type=BinlogType::SYNC, bool delttl = true) = 0;
	// -1: error, 1: ok, 0: value is not an integer or out of range
	virtual int zincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type=BinlogType::SYNC) = 0;
	
	virtual int64_t zsize(const Bytes &name) = 0;
	virtual int64_t zcount(const Bytes &name, const Bytes &s_score, const Bytes &e_score ,uint64_t &count) = 0;
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &name, const Bytes &key, std::string *score) = 0;
	virtual int64_t zrank(const Bytes &name, const Bytes &key) = 0;
	virtual int64_t zrrank(const Bytes &name, const Bytes &key) = 0;
	virtual int zrange(Iterator **it, const Bytes &name, uint64_t offset, uint64_t limit) = 0;
	virtual int zrrange(Iterator **it, const Bytes &name, uint64_t offset, uint64_t limit) = 0;
	virtual int zremrangebyrank(const Bytes &name, uint64_t offset, uint64_t limit, uint64_t &count)= 0;
	virtual int zremrangebyscore(const Bytes &name, const Bytes &score_start, const Bytes &score_end, uint64_t &count)= 0;
	virtual int zrangebyscore(const Bytes &name, const Bytes &key, const Bytes &score_start, const Bytes &score_end,
					uint64_t offset, uint64_t limit, std::vector<std::string> *resp)= 0;
	virtual int zrevrangebyscore(const Bytes &name, const Bytes &key, const Bytes &score_start, 
					const Bytes &score_end, uint64_t offset, uint64_t limit, std::vector<std::string> *resp) = 0;

	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual int zscan(Iterator **it, const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit) = 0;
	virtual int zrscan(Iterator **it, const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit) = 0;


	virtual int zlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list) = 0;
	virtual int zrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list) = 0;
	//virtual int64_t zfix(const Bytes &name) = 0;
	virtual int zdel_ttl(const Bytes &name, const Bytes &key, char log_type = BinlogType::NONE) = 0;
	virtual int zset_ttl(const Bytes &name, const Bytes &key, const Bytes &score,
				 char log_type = BinlogType::SYNC, bool lock = false) = 0;
	virtual int sync_zset_ttl(const Bytes &name, const Bytes &key, const Bytes &score, 
					  const char *mstIp, int mstPort, char log_type=BinlogType::SYNC) = 0;

	virtual int64_t lsize(const Bytes &name) = 0;
	virtual int litems(Iterator **it, const Bytes &name, const Bytes &seq_s, const Bytes &seq_e, uint64_t limit)=0;
	virtual int lclear(const Bytes &name, char log_type=BinlogType::SYNC, bool delttl = true) = 0;
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int lfront(const Bytes &name, std::string *item) = 0;
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int lback(const Bytes &name, std::string *item) = 0;
	// @return -1: error, other: the new length of the queue
	virtual int64_t lpush_front(const Bytes &name, const Bytes &item, char log_type=BinlogType::SYNC) = 0;
	virtual int64_t lpush_back(const Bytes &name, const Bytes &item, char log_type=BinlogType::SYNC) = 0;
	// @return 0: empty queue, 1: item popped, -1: error
	virtual int lpop_front(const Bytes &name, std::string *item, char log_type=BinlogType::SYNC) = 0;
	virtual int lpop_back(const Bytes &name, std::string *item, char log_type=BinlogType::SYNC) = 0;
	//virtual int qfix(const Bytes &name) = 0;
	virtual int llist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list) = 0;
	virtual int lrlist(unsigned int slot, const Bytes &name_s, const Bytes &name_e, 
						uint64_t limit, std::vector<std::string> *list) = 0;
	virtual int lslice(const Bytes &name, int64_t offset, int64_t limit,
			std::vector<std::string> *list) = 0;
	virtual int lget(const Bytes &name, int64_t index, std::string *item) = 0;
	virtual int lset(const Bytes &name, int64_t index, const Bytes &item, char log_type=BinlogType::SYNC) = 0;
	virtual int lset_by_seq(const Bytes &name, uint64_t seq, const Bytes &item, char log_type=BinlogType::SYNC) = 0;
};


#endif
