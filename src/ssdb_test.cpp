#include "serv.h"
#include "include.h"
#include "db/ttl.h"
#include "util/config.h"
#include "db/iterator.h"
#include "version.h"
#include "net/server.h"
#include "db/ssdb.h"
#include "util/app.h"
#include "db/options.h"
#include "db/rubbish_release.h"
#include <gtest/gtest.h>


class SSDBTest : public testing::Test{
public:
	SSDBTest(){
		if (pssdb == NULL){
			

			Config *conf = Config::load("./ssdb.conf");
			Options option;
			option.load(*conf);
			pssdb = static_cast<SSDBImpl*>(SSDB::open(option, "./var"));
			expiration = new ExpirationHandler(pssdb);
			pssdb->expiration= this->expiration;
			release = new RubbishRelease(pssdb);
			pssdb->releasehandler = this->release;
		}
	}
	static SSDBImpl* getDb(){return pssdb;}
	static ExpirationHandler* getExpiration(){return expiration;}
	
protected:  // You should make the members protected s.t. they can be
             // accessed from sub-classes.

  // virtual void SetUp() will be called before each test is run.  You
  // should define it if you need to initialize the varaibles.
  // Otherwise, this can be skipped.
	virtual void SetUp() {
	}
	static ExpirationHandler *expiration;
	static SSDBImpl *pssdb;
	static RubbishRelease *release; 
};

SSDBImpl* SSDBTest::pssdb = NULL;
ExpirationHandler *SSDBTest::expiration = NULL; 
RubbishRelease *SSDBTest::release = NULL;

int expire(const Bytes &name, int ttl){
	Locking l(&SSDBTest::getExpiration()->mutex);
	int64_t times = time_ms() + ttl * 1000;
	int ret = SSDBTest::getDb()->expire(name, times);
    if (ret > 0){
        ret = SSDBTest::getExpiration()->set_ttl(name, times);
    }
    return ret;
}

int setex(const Bytes &name, int ttl, const Bytes &val, bool mill){
    int nflag = mill ? 1000 : 1;
    Locking l(&SSDBTest::getExpiration()->mutex);
    int64_t times = time_ms() + ttl * nflag;
    int ret = SSDBTest::getDb()->setex(name, times, val);
    if (ret > 0){
        ret = SSDBTest::getExpiration()->set_ttl(name, times);
    }
    return ret;
}

int multi_set(std::vector<Bytes> &kvs){
	std::map<Bytes, TMH> metainfos;
	Locking l(&SSDBTest::getExpiration()->mutex);
	int ret = SSDBTest::getDb()->multi_set(metainfos, kvs, 1);

	if (ret > 0){
		for (auto it = metainfos.cbegin(); it != metainfos.cend(); ++it){
			if (it->second.expire != 0){
				SSDBTest::getExpiration()->del_ttl(it->first, false);
			}
		}
	}
	return ret;
}

int multi_setnx(std::vector<Bytes> &kvs){
	Locking l(&SSDBTest::getExpiration()->mutex);
	std::map<Bytes, TMH> metainfos;
	int ret = SSDBTest::getDb()->multi_setnx(metainfos, kvs, 1);

	if (ret > 0){
		for (auto it = metainfos.cbegin(); it != metainfos.cend(); ++it){
			if (it->second.expire != 0){
				SSDBTest::getExpiration()->del_ttl(it->first, false);
			}
		}
	}
	return ret;
}

uint64_t slotsinfo(const Bytes &name){
	unsigned int slotstart;
	unsigned int slotend;
	int count;
	int ok = 0;



	std::string buf = Slots::encode_slot_id(name);
	std::string val;

	leveldb::Status s = SSDBTest::getDb()->ldb->Get(leveldb::ReadOptions(), buf, &val);
	if (!s.IsNotFound() && s.ok()){
		return str_to_int64(val);
	}

	return 0;
}

int zrangecmd(const Bytes &name, uint64_t offset, uint64_t limit, std::vector<std::string> *resp){
	TMH metainfo = {0};
	int ret = SSDBTest::getDb()->VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
        return ret;
	}
	resp->clear();
    Iterator *it = nullptr;
    ret = SSDBTest::getDb()->zrange(&it, Bytes("test:name1"), offset, limit);
    if (ret <= 0){
    	return ret;
    }
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	return 1;
}


int zrevrangecmd(const Bytes &name, uint64_t offset, uint64_t limit, std::vector<std::string> *resp){
	TMH metainfo = {0};
	int ret = SSDBTest::getDb()->VerifyStructState(&metainfo, name, DataType::ZSIZE);
	if (ret <= 0){
        return ret;
	}
	resp->clear();
    Iterator *it = nullptr;
    ret = SSDBTest::getDb()->zrrange(&it, name, offset, limit);
    if (ret <= 0){
    	return ret;
    }
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	return 1;
}


TEST_F(SSDBTest, slotsinfo)
{
	pssdb->del(Bytes("test:k1"));
	int64_t oldk1= slotsinfo(Bytes("test:k1"));

	EXPECT_EQ(oldk1, slotsinfo(Bytes("test:k1")));
	EXPECT_EQ(1, pssdb->set(Bytes("test:k1"), Bytes("test:kval1")));
	EXPECT_EQ(oldk1 + 1, slotsinfo(Bytes("test:k1")));
	EXPECT_EQ(1, pssdb->del("test:k1"));
	EXPECT_EQ(oldk1, slotsinfo(Bytes("test:k1")));

	pssdb->del(Bytes("test:zset"));
	int64_t oldzset= slotsinfo(Bytes("test:zset"));
	EXPECT_EQ(1, pssdb->zset(Bytes("test:zset"), Bytes("test:subname1"), Bytes("10")));
	EXPECT_EQ(1, pssdb->zset(Bytes("test:zset"), Bytes("test:subname2"), Bytes("10")));
	EXPECT_EQ(1, pssdb->zset(Bytes("test:zset"), Bytes("test:subname3"), Bytes("10")));
	EXPECT_EQ(1, pssdb->zset(Bytes("test:zset"), Bytes("test:subname4"), Bytes("10")));
	EXPECT_EQ(1, pssdb->zset(Bytes("test:zset"), Bytes("test:subname5"), Bytes("10")));
	EXPECT_EQ(oldzset + 1, slotsinfo(Bytes("test:zset")));
	EXPECT_EQ(1, pssdb->del("test:zset"));
	EXPECT_EQ(oldzset, slotsinfo(Bytes("test:zset")));

	pssdb->del(Bytes("test:hash"));
	int64_t oldhash = slotsinfo(Bytes("test:hash"));

	EXPECT_EQ(1, pssdb->hset(Bytes("test:hash"), Bytes("test:subname1"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:hash"), Bytes("test:subname2"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:hash"), Bytes("test:subname3"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:hash"), Bytes("test:subname4"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:hash"), Bytes("test:subname5"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:hash"), Bytes("test:subname6"), Bytes("test:hval1")));

	EXPECT_EQ(oldhash + 1, slotsinfo(Bytes("test:hash")));
	EXPECT_EQ(1, pssdb->del("test:hash"));
	EXPECT_EQ(oldhash, slotsinfo(Bytes("test:hash")));

	pssdb->del(Bytes("test:list"));
	int64_t oldlist = slotsinfo(Bytes("test:list"));
	EXPECT_EQ(1, pssdb->lpush_front(Bytes("test:list"), Bytes("test:value1")));
	EXPECT_EQ(2, pssdb->lpush_front(Bytes("test:list"), Bytes("test:value2")));
	EXPECT_EQ(3, pssdb->lpush_front(Bytes("test:list"), Bytes("test:value3")));
	EXPECT_EQ(oldlist + 1, slotsinfo(Bytes("test:list")));
	EXPECT_EQ(1, pssdb->del("test:list"));
	EXPECT_EQ(oldlist, slotsinfo(Bytes("test:list")));
}

//全局相关命令
TEST_F(SSDBTest, global){
	std::string val;
	std::string type;
    //不存在的key值
    pssdb->del("test:k1");
	EXPECT_EQ(0, pssdb->exists("test:k1", &type));
	EXPECT_EQ(0, pssdb->del("test:k1"));

	//过期的kv值 del
	pssdb->set(Bytes("test:k1"), Bytes("test:v1"));
	expire(Bytes("test:k1"), 1);
	sleep(2);
	EXPECT_EQ(0, pssdb->exists("test:k1", &type));
	EXPECT_EQ(0, pssdb->del("test:k1"));
	EXPECT_EQ(0, pssdb->get(Bytes("test:k1"), &val));

	//过期的hash del
	pssdb->hset(Bytes("test:name1"), Bytes("test:key1"), Bytes("test:v1"));
	expire(Bytes("test:name1"), 1);
	sleep(2);
	EXPECT_EQ(0, pssdb->exists(Bytes("test:name1"), &type));
	EXPECT_EQ(0, pssdb->del("test:name1"));
	EXPECT_EQ(0, pssdb->hget(Bytes("test:name1"), Bytes("test:key1"), &val));

	//过期的zset del
	pssdb->zset(Bytes("test:name1"), Bytes("test:k1"), Bytes("10"));
	expire(Bytes("test:name1"), 1);
	sleep(2);
	EXPECT_EQ(0, pssdb->exists(Bytes("test:name1"), &type));
	EXPECT_EQ(0, pssdb->del("test:name1"));
	EXPECT_EQ(0, pssdb->zget(Bytes("test:name1"), Bytes("test:k1"), &val));

	//过期的list del
	pssdb->lpush_front(Bytes("test:list1"), Bytes("test:item1"));
	expire(Bytes("test:list1"), 1);
	sleep(2);
	EXPECT_EQ(0, pssdb->exists(Bytes("test:list1"), &type));
	EXPECT_EQ(0, pssdb->del("test:list1"));
	EXPECT_EQ(0, pssdb->lpop_front(Bytes("test:list1"), &val));

	//返回kv类型
	pssdb->set(Bytes("test:k1"), Bytes("test:v1"));
	EXPECT_EQ(1, pssdb->exists(Bytes("test:k1"), &type));
	EXPECT_STREQ("string", type.c_str());
	EXPECT_EQ(1, pssdb->del("test:k1"));

	//返回hash类型
	pssdb->hset(Bytes("test:name1"), Bytes("test:key1"), Bytes("test:v1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:key2"), Bytes("test:v2"));
	EXPECT_EQ(1, pssdb->exists(Bytes("test:name1"), &type));
	EXPECT_STREQ("hash", type.c_str());
	EXPECT_EQ(1, pssdb->del("test:name1"));

	//返回zset类型
	pssdb->zset(Bytes("test:name1"), Bytes("test:k1"), Bytes("10"));
	EXPECT_EQ(1, pssdb->exists(Bytes("test:name1"), &type));
	EXPECT_STREQ("zset", type.c_str());
	EXPECT_EQ(1, pssdb->del("test:name1"));

	//返回queue类型
	//lpush_front(req[1], item)
	pssdb->lpush_front(Bytes("test:name1"), Bytes("item"));
	EXPECT_EQ(1, pssdb->exists(Bytes("test:name1"), &type));
	EXPECT_STREQ("llist", type.c_str());
	EXPECT_EQ(1, pssdb->del("test:name1"));//返回queue类型

}

//kv相关命令
TEST_F(SSDBTest, get){
	//存在key值
	std::string val;
	pssdb->del(Bytes("test:k1"));
	pssdb->set(Bytes("test:k1"), Bytes("test:kval1"));
	EXPECT_EQ(1, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_STREQ("test:kval1", val.c_str());

	//不存在的key
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(0, pssdb->get(Bytes("test:k1"), &val));

	//不存在的key，由于时间过期
	pssdb->set(Bytes("test:k1"), Bytes("test:kval1"));
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(0, pssdb->get(Bytes("test:k1"), &val));

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->get(Bytes("test:name1"), &val));
}

TEST_F(SSDBTest, set){
	//不存在key值
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(1, pssdb->set(Bytes("test:k1"), Bytes("test:kval1")));

	//存在key值
	EXPECT_EQ(1, pssdb->set(Bytes("test:k1"), Bytes("test:kval2")));

	//不存在key，由于时间过期
	pssdb->set(Bytes("test:k1"), Bytes("test:kval1"));
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(1, pssdb->set(Bytes("test:k1"), Bytes("test:kval1")));

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->set(Bytes("test:name1"), Bytes("test:kval1")));
}

TEST_F(SSDBTest, setnx){
	//不存在key值
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(1, pssdb->setnx(Bytes("test:k1"), Bytes("test:kval1")));

	//存在key值
	EXPECT_EQ(0, pssdb->setnx(Bytes("test:k1"), Bytes("test:kval2")));

	//不存在key，由于时间过期
	pssdb->set(Bytes("test:k1"), Bytes("test:kval1"));
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(1, pssdb->setnx(Bytes("test:k1"), Bytes("test:kval1")));

	//key值类型不匹配
	std::string type;
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(0, pssdb->setnx(Bytes("test:name1"), Bytes("test:kval1")));//此处返回的是已经存在
	pssdb->exists(Bytes("test:name1"), &type);
	EXPECT_STREQ("hash", type.c_str());

}

TEST_F(SSDBTest, setex){
	//不存在key值
	pssdb->del(Bytes("test:k1"));
    EXPECT_EQ(1, setex(Bytes("test:k1"), 100, Bytes("test:kval1"), true));

	//存在key值
    EXPECT_EQ(1, setex(Bytes("test:k1"), 200, Bytes("test:kval2"), true));

	//不存在key，由于时间过期
	pssdb->set(Bytes("test:k1"), Bytes("test:kval1"));
	expire(Bytes("test:k1"), 1);//设置1秒过期
    sleep(2);
    EXPECT_EQ(1, setex(Bytes("test:k1"), 300, Bytes("test:kval1"), true));

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
    pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
    EXPECT_EQ(-2, setex(Bytes("test:name1"), 400, Bytes("test:kval1"), true));

}

TEST_F(SSDBTest, append){
	//不存在key值
	uint64_t newlen = 0;
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(1, pssdb->append(Bytes("test:k1"), Bytes("test:kval1"), &newlen));

	//存在key值
	EXPECT_EQ(1, pssdb->append(Bytes("test:k1"), Bytes("test:kval1"), &newlen));

	//不存在key，由于时间过期
	pssdb->set(Bytes("test:k1"), Bytes("test:kval1"));
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(1, pssdb->append(Bytes("test:k1"), Bytes("test:kval1"), &newlen));

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->append(Bytes("test:name1"), Bytes("test:kval1"), &newlen));

}

TEST_F(SSDBTest, incr){
	//不存在key值
	int64_t new_val;
	int64_t by = 1;
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(1, pssdb->incr(Bytes("test:k1"), 1, &new_val));
	EXPECT_STREQ("1", str(new_val).c_str());
	EXPECT_EQ(1, pssdb->incr(Bytes("test:k1"), -1, &new_val));
	EXPECT_STREQ("0", str(new_val).c_str());

	//存在key值
	EXPECT_EQ(1, pssdb->incr(Bytes("test:k1"), by, &new_val));
	EXPECT_STREQ("1", str(new_val).c_str());

	//不存在key值，由于时间过期
	expire(Bytes("test:k1"), 1);
	sleep(2);
	EXPECT_EQ(1, pssdb->incr(Bytes("test:k1"), by, &new_val));
	EXPECT_STREQ("1",str(new_val).c_str());

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->incr(Bytes("test:name1"), by, &new_val));

	//超出int64范围-9223372036854775808~9223372036854775807
	pssdb->del(Bytes("test:k1"));
	pssdb->set(Bytes("test:k1"), Bytes("9223372036854775807"));
	EXPECT_EQ(-5, pssdb->incr(Bytes("test:k1"), 1, &new_val));
	pssdb->set(Bytes("test:k1"), Bytes("-9223372036854775808"));
	EXPECT_EQ(-5, pssdb->incr(Bytes("test:k1"), -1, &new_val));
}

TEST_F(SSDBTest, setbit){
	//不存在key值
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(0, pssdb->setbit(Bytes("test:k1"), 1024, 1));//返回值为原始bit值

	//存在key值
	EXPECT_EQ(1, pssdb->setbit(Bytes("test:k1"), 1024, 0));//返回值为原始bit值

	//不存在key，由于时间过期
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(0, pssdb->setbit(Bytes("test:k1"), 1024, 1));

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->setbit(Bytes("test:name1"), 1024, 1));

}

TEST_F(SSDBTest, getbit){
	//存在key值
	int offset = 1024;
	pssdb->del(Bytes("test:k1"));
	pssdb->setbit(Bytes("test:k1"), offset, 1);
	EXPECT_EQ(1, pssdb->getbit(Bytes("test:k1"), offset));

	//不存在的key
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(0, pssdb->getbit(Bytes("test:k1"), offset));

	//不存在的key，由于时间过期
	pssdb->setbit(Bytes("test:k1"), offset, 1);
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(0, pssdb->getbit(Bytes("test:k1"), offset));
	
	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->getbit(Bytes("test:name1"), offset));
}

TEST_F(SSDBTest, getset){
	//存在key值
	std::string oldval;
	pssdb->del(Bytes("test:k1"));
	pssdb->set(Bytes("test:k1"), Bytes("test:v1"));
	EXPECT_EQ(1, pssdb->getset(Bytes("test:k1"), &oldval, Bytes("test:v2")));

	//不存在的key
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(0, pssdb->getset(Bytes("test:k1"), &oldval, Bytes("test:v1")));

	//不存在的key，由于时间过期
	pssdb->set(Bytes("test:k1"), Bytes("test:v1"));
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(0, pssdb->getset(Bytes("test:k1"), &oldval, Bytes("test:v2")));
	
	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->getset(Bytes("test:name1"), &oldval, Bytes("test:v1")));
}

TEST_F(SSDBTest, getrange){
	//存在key值
	std::string oldval;
	pssdb->del(Bytes("test:k1"));
	pssdb->set(Bytes("test:k1"), Bytes("test:v1"));
	EXPECT_EQ(1, pssdb->getset(Bytes("test:k1"), &oldval, Bytes("test:v2")));

	//不存在的key
	pssdb->del(Bytes("test:k1"));
	EXPECT_EQ(0, pssdb->getset(Bytes("test:k1"), &oldval, Bytes("test:v1")));

	//不存在的key，由于时间过期
	pssdb->set(Bytes("test:k1"), Bytes("test:v1"));
	expire(Bytes("test:k1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(0, pssdb->getset(Bytes("test:k1"), &oldval, Bytes("test:v2")));
	
	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->getset(Bytes("test:name1"), &oldval, Bytes("test:v1")));
}

TEST_F(SSDBTest, mset){

	std::vector<Bytes> kvs;
	std::string val;
	kvs.push_back(Bytes("multi_set"));
	kvs.push_back(Bytes("test:k1"));
	kvs.push_back(Bytes("test:v1"));
	kvs.push_back(Bytes("test:k2"));
	kvs.push_back(Bytes("test:v2"));
	kvs.push_back(Bytes("test:k3"));
	kvs.push_back(Bytes("test:v3"));
	pssdb->del(Bytes("test:k1"));
	pssdb->del(Bytes("test:k2"));
	pssdb->del(Bytes("test:k3"));
	EXPECT_EQ(3, multi_set(kvs));

	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_STREQ("test:v1", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k2"), &val));
	EXPECT_STREQ("test:v2", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k3"), &val));
	EXPECT_STREQ("test:v3", val.c_str());

	//设置某值过期，仍然设置进去了
	expire(Bytes("test:k1"), 1);
	pssdb->del(Bytes("test:k2"));
	pssdb->del(Bytes("test:k3"));
	sleep(2);
	multi_set(kvs);
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_STREQ("test:v1", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k2"), &val));
	EXPECT_STREQ("test:v2", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k3"), &val));
	EXPECT_STREQ("test:v3", val.c_str());

	//设置某值类型不匹配，某个key set失败，则所有key失败
	pssdb->del(Bytes("test:k1"));
	pssdb->del(Bytes("test:k2"));
	pssdb->del(Bytes("test:k3"));
	pssdb->hset(Bytes("test:k2"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, multi_set(kvs));

	EXPECT_EQ(0, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_EQ(-2, pssdb->get(Bytes("test:k2"), &val));
	EXPECT_EQ(0, pssdb->get(Bytes("test:k3"), &val));
}

TEST_F(SSDBTest, msetnx){

	std::vector<Bytes> kvs;
	std::string val;
	kvs.push_back(Bytes("multi_set"));
	kvs.push_back(Bytes("test:k1"));
	kvs.push_back(Bytes("test:v1"));
	kvs.push_back(Bytes("test:k2"));
	kvs.push_back(Bytes("test:v2"));
	kvs.push_back(Bytes("test:k3"));
	kvs.push_back(Bytes("test:v3"));
	pssdb->del(Bytes("test:k1"));
	pssdb->del(Bytes("test:k2"));
	pssdb->del(Bytes("test:k3"));
	EXPECT_EQ(3, multi_setnx(kvs));

	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_STREQ("test:v1", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k2"), &val));
	EXPECT_STREQ("test:v2", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k3"), &val));
	EXPECT_STREQ("test:v3", val.c_str());

	//设置某值过期，仍然设置进去了
	expire(Bytes("test:k1"), 1);
	pssdb->del(Bytes("test:k2"));
	pssdb->del(Bytes("test:k3"));
	sleep(2);
	EXPECT_EQ(3, multi_setnx(kvs));

	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_STREQ("test:v1", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k2"), &val));
	EXPECT_STREQ("test:v2", val.c_str());
	val.clear();
	EXPECT_EQ(1, pssdb->get(Bytes("test:k3"), &val));
	EXPECT_STREQ("test:v3", val.c_str());

	//设置某值类型不匹配，某个key set失败，则所有key失败
	pssdb->del(Bytes("test:k1"));
	pssdb->del(Bytes("test:k2"));
	pssdb->del(Bytes("test:k3"));
	pssdb->hset(Bytes("test:k2"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, multi_setnx(kvs));

	EXPECT_EQ(0, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_EQ(-2, pssdb->get(Bytes("test:k2"), &val));
	EXPECT_EQ(0, pssdb->get(Bytes("test:k3"), &val));

	//某个key存在，则所有key set失败
	pssdb->del(Bytes("test:k1"));
	pssdb->del(Bytes("test:k2"));
	pssdb->del(Bytes("test:k3"));
	pssdb->set(Bytes("test:k2"), Bytes("test:v2"));
	EXPECT_EQ(1, multi_setnx(kvs));

	EXPECT_EQ(0, pssdb->get(Bytes("test:k1"), &val));
	EXPECT_EQ(1, pssdb->get(Bytes("test:k2"), &val));
	EXPECT_EQ(0, pssdb->get(Bytes("test:k3"), &val));

}

//hash相关命令
TEST_F(SSDBTest, hset){
	//不存在key值
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1")));

	//存在name key值
	EXPECT_EQ(0, pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval2")));

	//不存在key，由于时间过期
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	expire(Bytes("test:name1"), 1);
	sleep(2);
	EXPECT_EQ(1, pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1")));

	//name类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->set(Bytes("test:name1"), Bytes("test:kval1"));
	EXPECT_EQ(-2, pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1")));
}

TEST_F(SSDBTest, hget){
	//存在key值
	std::string val;
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hget(Bytes("test:name1"),Bytes("test:k1"), &val));
	EXPECT_STREQ("test:hval1", val.c_str());

	//不存在的key
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(0, pssdb->hget(Bytes("test:name1"), Bytes("test:k1"), &val));

	//不存在的key，由于时间过期
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	expire(Bytes("test:name1"), 1);
	sleep(2);
	EXPECT_EQ(0, pssdb->hget(Bytes("test:name1"), Bytes("test:k1"), &val));

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->set(Bytes("test:name1"), Bytes("test:kval1"));
	EXPECT_EQ(-2, pssdb->hget(Bytes("test:name1"), Bytes("test:k1"), &val));
}

TEST_F(SSDBTest, hdel){
	//存在name值,存在key值
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(1, pssdb->hdel(Bytes("test:name1"),Bytes("test:k1")));

	//存在name值，不存在key值
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	EXPECT_EQ(0, pssdb->hdel(Bytes("test:name1"),Bytes("test:k2")));

	std::string val;
	//存在name值，存在key值，时间过期
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k2"), Bytes("test:hval2"));
	expire(Bytes("test:name1"), 1);
	sleep(2);
	EXPECT_EQ(0, pssdb->hdel(Bytes("test:name1"),Bytes("test:k2")));
	EXPECT_EQ(0, pssdb->hget(Bytes("test:name1"), Bytes("test:k1"), &val));

	//name值与key值均不存在
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(0, pssdb->hdel(Bytes("test:name1"),Bytes("test:k2")));

	//name类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->set(Bytes("test:name1"), Bytes("test:v1"));
	EXPECT_EQ(-2, pssdb->hdel(Bytes("test:name1"),Bytes("test:k2")));
}

TEST_F(SSDBTest, hincr){
	//不存在
	int64_t new_val;
	int64_t by = 1;
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(1, pssdb->hincr(Bytes("test:name1"), Bytes("test:k1"), 2, &new_val));
	EXPECT_STREQ("2", str(new_val).c_str());
	EXPECT_EQ(0, pssdb->hincr(Bytes("test:name1"), Bytes("test:k1"), -2, &new_val));
	EXPECT_STREQ("0", str(new_val).c_str());

	//存在key值
	EXPECT_EQ(0, pssdb->hincr(Bytes("test:name1"), Bytes("test:k1"), by, &new_val));
	EXPECT_STREQ("1", str(new_val).c_str());

	//不存在key值，由于时间过期
	expire(Bytes("test:name1"), 1);
	sleep(2);
	EXPECT_EQ(1, pssdb->hincr(Bytes("test:name1"), Bytes("test:k1"), by, &new_val));
	EXPECT_STREQ("1", str(new_val).c_str());

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->set(Bytes("test:name1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->hincr(Bytes("test:name1"),Bytes("test:k1"), by, &new_val));

	//超出int64范围-9223372036854775808~9223372036854775807
	pssdb->del(Bytes("test:name1"));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("9223372036854775807"));
	EXPECT_EQ(-5, pssdb->hincr(Bytes("test:name1"), Bytes("test:k1"), 1, &new_val));
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("-9223372036854775808"));
	EXPECT_EQ(-5, pssdb->hincr(Bytes("test:name1"), Bytes("test:k1"), -1, &new_val));
}

TEST_F(SSDBTest, hlen){
	//不存在
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(0, pssdb->hsize(Bytes("test:name1")));

	//不存在，由于过期
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:name1"), Bytes("test:k2"), Bytes("test:hval2")));
	expire(Bytes("test:name1"), 1);
	sleep(2);
	EXPECT_EQ(0, pssdb->hsize(Bytes("test:name1")));

	//不存在，由于过期
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:hval1")));
	EXPECT_EQ(1, pssdb->hset(Bytes("test:name1"), Bytes("test:k2"), Bytes("test:hval2")));
	EXPECT_EQ(2, pssdb->hsize(Bytes("test:name1")));

	//key值类型不匹配
	pssdb->del(Bytes("test:name1"));
	pssdb->set(Bytes("test:name1"), Bytes("test:hval1"));
	EXPECT_EQ(-2, pssdb->hsize(Bytes("test:name1")));

}

TEST_F(SSDBTest, hsetnx){
	//不存在key值
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(1, pssdb->hsetnx(Bytes("test:name1"),Bytes("test:k1"), Bytes("test:kval1")));

	//存在key值
	EXPECT_EQ(0, pssdb->hsetnx(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:kval2")));

	//不存在key，由于时间过期
	pssdb->hset(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:kval1"));
	expire(Bytes("test:name1"), 1);//设置1秒过期
	sleep(2);
	EXPECT_EQ(1, pssdb->hsetnx(Bytes("test:name1"), Bytes("test:k1"), Bytes("test:kval1")));

	//key值类型不匹配
	std::string type;
	pssdb->del(Bytes("test:name1"));
	pssdb->set(Bytes("test:name1"), Bytes("test:k1"));
	EXPECT_EQ(0, pssdb->hsetnx(Bytes("test:name1"), Bytes("test:k1"),Bytes("test:kval1")));//此处返回的是已经存在
	pssdb->exists(Bytes("test:name1"), &type);
	EXPECT_STREQ("string", type.c_str());

}

//lpop
TEST_F(SSDBTest, lpush_front)
{
	pssdb->del(Bytes("test:name1"));
	EXPECT_EQ(1, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value1")));
	EXPECT_EQ(2, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value2")));
	EXPECT_EQ(3, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value3")));
	EXPECT_EQ(3, pssdb->lsize("test:name1"));

	std::vector<std::string> list;
	EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), 0, 2000000000, &list));
	EXPECT_EQ(3, list.size());
	std::string str;
	for (int i = 0; i < list.size(); ++i){
		str += list.at(i);
	}
	EXPECT_STREQ(str.c_str(), "test:value3test:value2test:value1");

	expire(Bytes("test:name1"), 1);
	
	sleep(2);
	EXPECT_EQ(0, pssdb->lsize("test:name1"));
}

//rpush
TEST_F(SSDBTest, lpush_back)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value1")));
    EXPECT_EQ(2, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value2")));
    EXPECT_EQ(3, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value3")));
    EXPECT_EQ(3, pssdb->lsize("test:name1"));
    std::vector<std::string> list;
    EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), 0, 2000000000, &list));
    EXPECT_EQ(3, list.size());
    std::string str;
    for (int i = 0; i < list.size(); ++i){
        str += list.at(i);
    }
    EXPECT_STREQ(str.c_str(), "test:value1test:value2test:value3");
    expire(Bytes("test:name1"), 1);

    sleep(2);
    EXPECT_EQ(0, pssdb->lsize("test:name1"));
}

//lpop
TEST_F(SSDBTest, lpop_front)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value1")));
    EXPECT_EQ(2, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value2")));
    EXPECT_EQ(3, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value3")));
    EXPECT_EQ(4, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value4")));

    std::string item;
    EXPECT_EQ(1, pssdb->lpop_front(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value1", item.c_str());
    item.clear();
    EXPECT_EQ(1, pssdb->lpop_front(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value2", item.c_str());

    EXPECT_EQ(2, pssdb->lsize("test:name1"));
    std::vector<std::string> list;
    EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), 0, 2000000000, &list));
    EXPECT_EQ(2, list.size());
    std::string str;
    for (int i = 0; i < list.size(); ++i){
        str += list.at(i);
    }
    EXPECT_STREQ(str.c_str(), "test:value3test:value4");
    expire(Bytes("test:name1"), 1);

    sleep(2);
    EXPECT_EQ(0, pssdb->lsize("test:name1"));
}

//rpop
TEST_F(SSDBTest, lpop_back)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value1")));
    EXPECT_EQ(2, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value2")));
    EXPECT_EQ(3, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value3")));
    EXPECT_EQ(4, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value4")));

    std::string item;
    EXPECT_EQ(1, pssdb->lpop_front(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value1", item.c_str());
    item.clear();
    EXPECT_EQ(1, pssdb->lpop_front(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value2", item.c_str());

    EXPECT_EQ(2, pssdb->lsize("test:name1"));
    std::vector<std::string> list;
    EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), 0, 2000000000, &list));
    EXPECT_EQ(2, list.size());
    std::string str;
    for (int i = 0; i < list.size(); ++i){
        str += list.at(i);
    }
    EXPECT_STREQ(str.c_str(), "test:value3test:value4");
    expire(Bytes("test:name1"), 1);

    sleep(2);
    EXPECT_EQ(0, pssdb->lsize("test:name1"));
}

//llen
TEST_F(SSDBTest, lsize)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value1")));
    EXPECT_EQ(2, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value2")));
    EXPECT_EQ(3, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value3")));
    EXPECT_EQ(4, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value4")));
    EXPECT_EQ(4, pssdb->lsize("test:name1"));

    std::string item;
    EXPECT_EQ(1, pssdb->lpop_front(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value4", item.c_str());
    EXPECT_EQ(3, pssdb->lsize("test:name1"));
    item.clear();
    EXPECT_EQ(1, pssdb->lpop_back(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value1", item.c_str());
    item.clear();
    EXPECT_EQ(1, pssdb->lpop_back(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value2", item.c_str());
    item.clear();
    EXPECT_EQ(1, pssdb->lpop_back(Bytes("test:name1"), &item));
    EXPECT_STREQ("test:value3", item.c_str());
    EXPECT_EQ(0, pssdb->lsize("test:name1"));
    item.clear();
    EXPECT_EQ(0, pssdb->lpop_back(Bytes("test:name1"), &item));
    EXPECT_EQ(0, pssdb->lsize("test:name1"));
}

//lindex
TEST_F(SSDBTest, lget)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value1")));
    EXPECT_EQ(2, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value2")));
    EXPECT_EQ(3, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value3")));
    EXPECT_EQ(4, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value4")));
    EXPECT_EQ(5, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value5")));
    EXPECT_EQ(5, pssdb->lsize("test:name1"));
    //54321
    std::string item;
    pssdb->lget(Bytes("test:name1"), 0, &item);
    EXPECT_STREQ("test:value5", item.c_str());

    item.clear();
    pssdb->lget(Bytes("test:name1"), 2, &item);
    EXPECT_STREQ("test:value3", item.c_str());
    item.clear();
    pssdb->lget(Bytes("test:name1"), 4, &item);
    EXPECT_STREQ("test:value1", item.c_str());
    expire(Bytes("test:name1"), 1);
    sleep(2);
    item.clear();
    pssdb->lget(Bytes("test:name1"), 0, &item);
    EXPECT_STREQ("", item.c_str());
}

//lrange
TEST_F(SSDBTest, lslice)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value1")));
    EXPECT_EQ(2, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value2")));
    EXPECT_EQ(3, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value3")));
    EXPECT_EQ(4, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value4")));
    EXPECT_EQ(5, pssdb->lpush_front(Bytes("test:name1"), Bytes("test:value5")));

    EXPECT_EQ(6, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value6")));
    EXPECT_EQ(7, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value7")));
    EXPECT_EQ(8, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value8")));
    EXPECT_EQ(9, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value9")));
    EXPECT_EQ(10, pssdb->lpush_back(Bytes("test:name1"), Bytes("test:value10")));
    //54321678910
    EXPECT_EQ(10, pssdb->lsize("test:name1"));
    std::vector<std::string> list;
    EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), 0, 3, &list));
    EXPECT_EQ(4, list.size());
    std::string str;
    for (int i = 0; i < list.size(); ++i){
        str += list.at(i);
    }
    EXPECT_STREQ("test:value5test:value4test:value3test:value2", str.c_str());
    //负数判断
    list.clear();
    str.clear();

    EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), 15, 0, &list));
    EXPECT_EQ(0, list.size());
    for (int i = 0; i < list.size(); ++i){
        str += list.at(i);
    }
    EXPECT_STREQ("", str.c_str());

    list.clear();
    str.clear();

    EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), -2, 10, &list));
    EXPECT_EQ(2, list.size());
    for (int i = 0; i < list.size(); ++i){
        str += list.at(i);
    }
    EXPECT_STREQ("test:value9test:value10", str.c_str());

    list.clear();
    str.clear();

    EXPECT_EQ(0, pssdb->lslice(Bytes("test:name1"), 0, -5, &list));
    EXPECT_EQ(6, list.size());
    for (int i = 0; i < list.size(); ++i){
        str += list.at(i);
    }
    EXPECT_STREQ("test:value5test:value4test:value3test:value2test:value1test:value6", str.c_str());
}

//zadd
TEST_F(SSDBTest, zadd)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));

    EXPECT_EQ(0, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));

    expire(Bytes("test:name1"), 1);
    sleep(2);
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));
}

//zdel
TEST_F(SSDBTest, zdel)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));

    EXPECT_EQ(1, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname5")));
    EXPECT_EQ(1, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname4")));
    EXPECT_EQ(0, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname4")));
    EXPECT_EQ(1, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname3")));
    EXPECT_EQ(1, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname2")));
    EXPECT_EQ(1, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname1")));
    EXPECT_EQ(0, expire(Bytes("test:name1"), 1));
}

//zincr/zdecr
TEST_F(SSDBTest, zincr)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));

    int64_t val;
    EXPECT_EQ(0, pssdb->zincr(Bytes("test:name1"), Bytes("test:subname1"), 10, &val));
    EXPECT_STREQ("11", str(val).c_str());

    EXPECT_EQ(1, pssdb->zincr(Bytes("test:name1"), Bytes("test:subname2"), 10, &val));
    EXPECT_STREQ("10", str(val).c_str());

    EXPECT_EQ(0, pssdb->zincr(Bytes("test:name1"), Bytes("test:subname2"), -3, &val));
    EXPECT_STREQ("7", str(val).c_str());

    EXPECT_EQ(1, pssdb->zincr(Bytes("test:name1"), Bytes("test:subname4"), -3, &val));
    EXPECT_STREQ("-3", str(val).c_str());

    //数目过大
    EXPECT_EQ(1, pssdb->zincr(Bytes("test:name1"), Bytes("test:subname5"), 45900000000, &val));
    EXPECT_STREQ("45900000000", str(val).c_str());
    EXPECT_EQ(0, pssdb->zincr(Bytes("test:name1"), Bytes("test:subname5"), -45900000000, &val));


    pssdb->del(Bytes("test:name1"));
    int64_t new_val;
	pssdb->zset(Bytes("test:name1"), Bytes("test:k1"), Bytes("9223372036854775807"));
	EXPECT_EQ(-5, pssdb->zincr(Bytes("test:name1"), Bytes("test:k1"), 1, &new_val));
	pssdb->zset(Bytes("test:name1"), Bytes("test:k1"), Bytes("-9223372036854775808"));
	EXPECT_EQ(-5, pssdb->zincr(Bytes("test:name1"), Bytes("test:k1"), -1, &new_val));
}

//zrank
TEST_F(SSDBTest, zrank)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));

    EXPECT_EQ(4, pssdb->zrank(Bytes("test:name1"), Bytes("test:subname5")));
   
    EXPECT_EQ(3, pssdb->zrank(Bytes("test:name1"), Bytes("test:subname4")));

    EXPECT_EQ(2, pssdb->zrank(Bytes("test:name1"), Bytes("test:subname3")));
  
    EXPECT_EQ(1, pssdb->zrank(Bytes("test:name1"), Bytes("test:subname2")));

    EXPECT_EQ(0, pssdb->zrank(Bytes("test:name1"), Bytes("test:subname1")));
 
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname6"), Bytes("115")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname7"), Bytes("-115")));
    EXPECT_EQ(6, pssdb->zrank(Bytes("test:name1"), Bytes("test:subname6")));
 
    EXPECT_EQ(0, pssdb->zrank(Bytes("test:name1"), Bytes("test:subname7")));
  

    expire(Bytes("test:name1"), 1);
    sleep(2);
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));
}

//zrevrank
TEST_F(SSDBTest, zrrank)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));


    EXPECT_EQ(0, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname5")));
 
    EXPECT_EQ(1, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname4")));
    
    EXPECT_EQ(2, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname3")));
   
    EXPECT_EQ(3, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname2")));

    EXPECT_EQ(4, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname1")));

    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname6"), Bytes("115")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname7"), Bytes("-115")));
    EXPECT_EQ(0, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname6")));
   
    EXPECT_EQ(6, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname7")));

    expire(Bytes("test:name1"), 1);
    sleep(2);
    //没找到
    EXPECT_EQ(0, pssdb->zrrank(Bytes("test:name1"), Bytes("test:subname7")));

}

//zrange
TEST_F(SSDBTest, zrange)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));

    //offset limit(外部传入的值-1，3就是limit2)
    std::vector<std::string> resp;
    EXPECT_EQ(1, zrangecmd(Bytes("test:name1"), 0, 3, &resp));

    std::string str;
    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname1test:subname2test:subname3", str.c_str());

    str.clear();
    EXPECT_EQ(1, zrangecmd(Bytes("test:name1"), 0, -1, &resp));


    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname1test:subname2test:subname3test:subname4test:subname5", str.c_str());


    expire(Bytes("test:name1"), 1);
    sleep(2);

    EXPECT_EQ(0, zrangecmd(Bytes("test:name1"), 0, -1, &resp));
}

TEST_F(SSDBTest, zrevrange)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));

    std::vector<std::string> resp;
    EXPECT_EQ(1, zrevrangecmd(Bytes("test:name1"), 0, 3, &resp));

    std::string str;
    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname5test:subname4test:subname3", str.c_str());

    str.clear();
    EXPECT_EQ(1, zrevrangecmd(Bytes("test:name1"), 0, -1, &resp));

    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname5test:subname4test:subname3test:subname2test:subname1", str.c_str());


    str.clear();
    //offset limit(外部传入的值-1，3就是limit2 zrevrange zs offset limit+1)
    EXPECT_EQ(1, zrevrangecmd(Bytes("test:name1"), 2, 2, &resp));

    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname3test:subname2", str.c_str());

    expire(Bytes("test:name1"), 1);
    sleep(2);
    EXPECT_EQ(0, zrevrangecmd(Bytes("test:name1"), 0, -1, &resp));
}

TEST_F(SSDBTest, zrangebyscore)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname6"), Bytes("6")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname7"), Bytes("7")));

    std::vector<std::string> list;
    EXPECT_EQ(1, pssdb->zrangebyscore(Bytes("test:name1"), "", Bytes("0"), Bytes("4"), 0, 2000000, &list));

    std::string str;
    for (int i = 1; i < list.size(); i+=2){
        str += list.at(i);
    }

    EXPECT_STREQ("1234", str.c_str());
    str.clear();
    list.clear();
    EXPECT_EQ(1, pssdb->zrangebyscore(Bytes("test:name1"), "", Bytes("3"), Bytes("10"), 1, 2000000, &list));
    for (int i = 1; i < list.size(); i+=2){
        str += list.at(i);
    }
    EXPECT_STREQ("4567", str.c_str());

    str.clear();
    list.clear();
    EXPECT_EQ(1, pssdb->zrangebyscore(Bytes("test:name1"), "", Bytes("1"), Bytes("5"), 1, 2, &list));
    for (int i = 1; i < list.size(); i+=2){
        str += list.at(i);
    }
    EXPECT_STREQ("2", str.c_str());
    expire(Bytes("test:name1"), 1);
    sleep(2);
    list.clear();
    EXPECT_EQ(0, pssdb->zrangebyscore(Bytes("test:name1"), "", Bytes("1"), Bytes("5"), 1, 2, &list));
}

TEST_F(SSDBTest, zrevrangebyscore)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname6"), Bytes("6")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname7"), Bytes("7")));

    std::vector<std::string> list;
    EXPECT_EQ(1, pssdb->zrevrangebyscore(Bytes("test:name1"), "",  Bytes("4"), Bytes("0"), 0, 2000000, &list));

    std::string str;
    for (int i = 1; i < list.size(); i+=2){
        str += list.at(i);
    }
    EXPECT_STREQ("4321", str.c_str());
    str.clear();
    list.clear();
    EXPECT_EQ(1, pssdb->zrevrangebyscore(Bytes("test:name1"), "", Bytes("10"), Bytes("3"), 1, 2000000, &list));
    for (int i = 1; i < list.size(); i+=2){
        str += list.at(i);
    }
    EXPECT_STREQ("6543", str.c_str());

    str.clear();
    list.clear();
    EXPECT_EQ(1, pssdb->zrevrangebyscore(Bytes("test:name1"), "", Bytes("5"), Bytes("1"), 1, 2, &list));
    for (int i = 1; i < list.size(); i+=2){
        str += list.at(i);
    }
    EXPECT_STREQ("4", str.c_str());
    expire(Bytes("test:name1"), 1);
    sleep(2);
    list.clear();
    EXPECT_EQ(0, pssdb->zrevrangebyscore(Bytes("test:name1"), "", Bytes("5"), Bytes("1"), 1, 2, &list));
}

TEST_F(SSDBTest, zremrangebyrank)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname6"), Bytes("6")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname7"), Bytes("7")));
    uint64_t nCnt = 0;
    EXPECT_EQ(1, pssdb->zremrangebyrank(Bytes("test:name1"), 0, 3, nCnt));
    std::vector<std::string> resp;
    EXPECT_EQ(1, zrangecmd(Bytes("test:name1"), 0, 3, &resp));
    std::string str;
    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname4test:subname5test:subname6", str.c_str());

    EXPECT_EQ(1, pssdb->zremrangebyrank(Bytes("test:name1"), 0, -1, nCnt));
    EXPECT_EQ(0, pssdb->zsize(Bytes("test:name1")));
}

TEST_F(SSDBTest, zremrangebyscore)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname6"), Bytes("6")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname7"), Bytes("7")));
    uint64_t nCnt = 0;
    EXPECT_EQ(1, pssdb->zremrangebyscore(Bytes("test:name1"), Bytes("0"), Bytes("3"), nCnt));

    std::vector<std::string> resp;
    EXPECT_EQ(1, zrangecmd(Bytes("test:name1"), 0, -1, &resp));
    std::string str;
    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname4test:subname5test:subname6test:subname7", str.c_str());

    EXPECT_EQ(1, pssdb->zremrangebyscore(Bytes("test:name1"), Bytes("5"), Bytes("4"), nCnt));
    EXPECT_EQ(0, nCnt);
    EXPECT_EQ(1, pssdb->zremrangebyscore(Bytes("test:name1"), Bytes("5"), Bytes("6"), nCnt));
    EXPECT_EQ(2, nCnt);

    str.clear();
    resp.clear();
    EXPECT_EQ(1, zrangecmd(Bytes("test:name1"), 0, -1, &resp));

    for (int i = 1; i < resp.size(); i+=2){
        str += resp.at(i);
    }
    EXPECT_STREQ("test:subname4test:subname7", str.c_str());
    expire(Bytes("test:name1"), 1);
    sleep(2);
    resp.clear();
    EXPECT_EQ(0, pssdb->zremrangebyscore(Bytes("test:name1"), Bytes("5"), Bytes("6"), nCnt));
}



TEST_F(SSDBTest, zcard)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("4")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));

    EXPECT_EQ(5, pssdb->zsize(Bytes("test:name1")));
    EXPECT_EQ(1, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname5")));
    EXPECT_EQ(4, pssdb->zsize(Bytes("test:name1")));
    EXPECT_EQ(0, pssdb->zset(Bytes("test:name1"), Bytes("test:subname4"), Bytes("41")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname5"), Bytes("5")));
    EXPECT_EQ(5, pssdb->zsize(Bytes("test:name1")));
    expire(Bytes("test:name1"), 1);
    sleep(2);
    EXPECT_EQ(0, pssdb->zsize(Bytes("test:name1")));
}

TEST_F(SSDBTest, zscore)
{
    pssdb->del(Bytes("test:name1"));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("1")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname2"), Bytes("2")));
    EXPECT_EQ(1, pssdb->zset(Bytes("test:name1"), Bytes("test:subname3"), Bytes("3")));

    std::string score;
    EXPECT_EQ(1, pssdb->zget(Bytes("test:name1"), Bytes("test:subname3"), &score));
    EXPECT_STREQ("3", score.c_str());

    EXPECT_EQ(1, pssdb->zget(Bytes("test:name1"), Bytes("test:subname1"), &score));
    EXPECT_STREQ("1", score.c_str());

    EXPECT_EQ(0, pssdb->zset(Bytes("test:name1"), Bytes("test:subname1"), Bytes("10")));

    EXPECT_EQ(1, pssdb->zget(Bytes("test:name1"), Bytes("test:subname1"), &score));
    EXPECT_STREQ("10", score.c_str());
    score.clear();
    EXPECT_EQ(1, pssdb->zdel(Bytes("test:name1"), Bytes("test:subname1")));
    EXPECT_EQ(0, pssdb->zget(Bytes("test:name1"), Bytes("test:subname1"), &score));
    EXPECT_STREQ("", score.c_str());

    expire(Bytes("test:name1"), 1);
    sleep(2);
    EXPECT_EQ(0, pssdb->zget(Bytes("test:name1"), Bytes("test:subname2"), &score));
    EXPECT_STREQ("", score.c_str());
}

int main(int argc, char **argv) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
