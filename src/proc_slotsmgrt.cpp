/*
 *文件名称: proc_slotsmgrt.cpp
 *文件描述: 实现slots系列命令，slotsinfo,slotshashkey,
 *slotsdel,slotsmgrts,slotsrestore
 *时    间: 2016-05-04
 *作    者: YPS
 *版    本: 0.1
 */

#include "serv.h"
#include "net/proc.h"
#include "net/server.h"
#include "net/network.h"
#include "util/siobuffer.h"
#include "cluster_migrate.h"

/* 函数名称: proc_slotsinfo
 * 说    明: 实现slotsinfo命令函数，按照slotindex统计
 */
int proc_slotsinfo(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	int slots_slot[HASH_SLOTS_SIZE];
	int slots_size[HASH_SLOTS_SIZE];
	unsigned int slotstart;
	unsigned int slotend;
	int count;
	int ok = 0;

	memset(slots_slot, 0x00, sizeof(slots_slot));
	memset(slots_size, 0x00, sizeof(slots_size));
	resp->clear();

	if (req.size() == 1){
		slotstart = 0;
		count = HASH_SLOTS_SIZE;
	}else if (req.size() == 2){
		slotstart = req[1].Int();
		if (slotstart >= HASH_SLOTS_SIZE){
			resp->push_back("client_error");
			return -1;
		}
		count = HASH_SLOTS_SIZE - slotstart;
	}else if (req.size() == 3){
		slotstart = req[1].Int();
		slotend = req[2].Int();
		if (slotstart >= HASH_SLOTS_SIZE){
			resp->push_back("client_error");
			return -1;
		}

		if (slotstart + slotend > HASH_SLOTS_SIZE)
			count = HASH_SLOTS_SIZE - slotstart;
		else{
			count = slotend;
		}
	}else{
		resp->push_back("client_error");
		return -1;
	}
	uint64_t nCnt = 0;
	for (unsigned int nSlotIndex = slotstart; nSlotIndex < slotstart + count; ++nSlotIndex)
    {
		std::string buf = Slots::encode_slot_id(nSlotIndex);
		std::string val;

		leveldb::Status s = serv->ssdb->getlDb()->Get(leveldb::ReadOptions(), buf, &val);
		if (!s.IsNotFound() && s.ok())
		{
			if (!ok)
			{
				resp->push_back("ok");
				ok = 1;
			}
			nCnt += nSlotIndex;
			resp->push_back(str(nSlotIndex));
			resp->push_back(val);
		}
	}

	log_debug("total count:%u", nCnt);

	if (resp->size() <= 0){
		resp->push_back("slotsinfo_empty");
	}
	return 0;
}

/* 函数名称: proc_slotshashkey
 * 说    明: 实现slotshashkey命令函数，计算字符串所属slotsindex
 */
int proc_slotshashkey(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);

	resp->push_back("ok");
	
	//此处并不保存实际数据到文件中，故不需要转换为大端
	resp->push_back(str(Slots::slots_num(hexmem(req[1].data(), req[1].size()).c_str(), nullptr, nullptr)));

	return 0;
}

/* 函数名称: proc_slotsdel
 * 说    明: 实现slotsdel命令函数，删除kv,hash,zset,queue四个数据结构所属的slotsindex
 */
int proc_slotsdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return 0;
}

/* 函数名称: proc_slotsmgrttagslot
 * 说    明: 实现slotsmgrttagslot命令函数，检索对应的slotsindex，并发送给对应的服务器
 */
int proc_slotsmgrttagslot(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);

	const char *dstip = req[1].data();
	int dstport = req[2].Int();
	int timeout = req[3].Int();
	int slotindex = req[4].Int();
	uint64_t nMgrtNums = 100;
	if (req.size() == 6) 
		nMgrtNums = req[5].Uint64();//迁移数据的个数

	log_info("cluter mgrt slot:dstip:%s, dspport:%d, timeout:%d, slot:%d, nMgrtNums:%u",
		dstip, dstport, timeout, slotindex, nMgrtNums);

	int caechfd = ClusterMigrateHandle::instance()->fetchMgrtSocketFd(dstip, dstport, timeout);
	if (caechfd < 0){
		resp->push_back("connect_failed");
		return 0;
	}
	clusterRst result = ClusterMigrateHandle::instance()->migrateSlotsData(caechfd, slotindex, nMgrtNums);
	
	if (result.rst == -1) {
		ClusterMigrateHandle::instance()->closeMgrtSocket(dstip, dstport);
		resp->push_back("connect_failed");
		log_error("connect_failed");
	}else if (result.rst == 1) {
		resp->push_back("ok");
	 	resp->push_back(str(result.mgrtkey));  //删除的个数
	 	resp->push_back(str(result.remainkey));//返回剩余的个数
	} else{
		resp->push_back("ok");
	 	resp->push_back("0");//删除的个数
	 	resp->push_back(str(result.remainkey));//返回剩余的个数
	}

    return 0;
}

int proc_slotsmgrttagone(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);

	const char *dstip = req[1].data();
	int dstport = req[2].Int();
	int timeout = req[3].Int();
	Bytes name = req[4];

	log_info("cluter mgrt key: %s, dspport:%d, timeout:%d, key: %s",
		dstip, dstport, timeout, name.data());

	int caechfd = ClusterMigrateHandle::instance()->fetchMgrtSocketFd(dstip, dstport, timeout);
	if (caechfd < 0){
		resp->push_back("connect_failed");
		log_error("connect_failed");
		return 0;
	}
	int nRst = ClusterMigrateHandle::instance()->migrateSingleKey(caechfd, name, true);

	if (nRst == -1) {
		log_error("connect_failed");
		ClusterMigrateHandle::instance()->closeMgrtSocket(dstip, dstport);
		resp->push_back("connect_failed");
	} else if (nRst == 1) {
		resp->push_back("ok");
	 	resp->push_back("1");//删除的个数
	} else{
		resp->push_back("ok");
	 	resp->push_back("0");//删除的个数
	}

    return 0;
}

int proc_retrievekey(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);

	const char *dstip = req[1].data();
	int dstport = req[2].Int();
	int timeout = req[3].Int();
	Bytes name = req[4];

	TMH nameInfo = {0};
	int ret = serv->ssdb->loadobjectbyname(&nameInfo, name, 0, true);
	if (ret <= 0){
		resp->push_back("key_not_find");
		return 0;
	}
	log_info("want to %s:%d request retrievekey: %s", dstip, dstport, name.data());
	
	int caechfd = ClusterMigrateHandle::instance()->fetchMgrtSocketFd(dstip, dstport, timeout);
	if (caechfd < 0){
		resp->push_back("connect_failed");
		return 0;
	}

	ret = ClusterMigrateHandle::instance()->migrateSingleKey(caechfd, name, false);

	if (ret < 0){
		ClusterMigrateHandle::instance()->closeMgrtSocket(dstip, dstport);
		log_error("retrievekey error:%d", ret);
		resp->push_back("retrieveKey_error");
	}else {
		resp->push_back("ok");
	}
	return 0;
}
/* 函数名称: proc_slotsrestore
 * 说    明: 实现slotsmgrtrestore命令函数，此命令是执行slotsmgrtslot命令时，产生的命令
 */
int proc_slotsrestore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);
	const char* datatype = req[1].data();
	const char* retrieveFlag = req[2].data();
	uint64_t reqindex = 0;
	uint32_t crc = req[3].Int();
	uint32_t calcrc = 0;
	char log_type = (strcasecmp(retrieveFlag, "retrievekey") == 0) ? 
			BinlogType::NONE : BinlogType::SYNC;
	
	std::string strtotal = str((int)req.size());
	calcrc = Crc32::crc32_update(calcrc, strtotal.c_str(), strtotal.size());
	calcrc = Crc32::crc32_update(calcrc, req[0].data(), req[0].size());
	calcrc = Crc32::crc32_update(calcrc, req[1].data(), req[1].size());
	calcrc = Crc32::crc32_update(calcrc, req[2].data(), req[2].size());


	//for(int i=0; i<req.size(); i++){
	//	fprintf(stderr, "req[%d]:%s\n", i, req[i].data());
	//}

	if (crc != calcrc){
		resp->push_back("crc32_error");
		return 0;
	}

	reqindex += 4;
	int setret = 0;
	uint64_t kvcnt = 0;
	uint64_t kvfailed = 0;


	if (datatype[0] == DataType::KV) {
		kvcnt = req[reqindex++].Uint64();
		for (uint64_t i = 0; i < kvcnt; i++, reqindex += 2){
				setret = serv->ssdb->set(req[reqindex], req[reqindex+1], log_type);
				if (setret == -1){
					//此处应该记录失败的key并返回
					kvfailed++;
				}
		}
		reqindex++;//跳过kvcrc校验
	}

	uint64_t hashnamecnt = 0;
	uint64_t hashfailed = 0;

	if(datatype[1] == DataType::HASH){
		hashnamecnt = req[reqindex++].Uint64();
		for (uint64_t i = 0; i < hashnamecnt; i++){

			std::string hashname = req[reqindex++].data();
			uint64_t hashkvcnt = req[reqindex++].Uint64();

			for(uint64_t j = 0; j < hashkvcnt; j++,reqindex += 2){
				setret = serv->ssdb->hset(Bytes(hashname), req[reqindex], req[reqindex+1], log_type);
				if (setret == -1){//此处记录失败
					hashfailed++;
				}
			}//for j
		}//for i
		reqindex++;//跳过hashcrc校验
	}

	uint64_t zsetnamecnt = 0;
	uint64_t zsetfailed = 0;

	if(datatype[2] == DataType::ZSET){
		zsetnamecnt = req[reqindex++].Uint64();
		for (uint64_t i = 0; i < zsetnamecnt; i++){
			std::string zsetname = req[reqindex++].data();
			uint64_t zsetkscnt = req[reqindex++].Uint64();
			for(uint64_t j = 0; j < zsetkscnt; j++,reqindex += 2){
				setret = serv->ssdb->zset(Bytes(zsetname), req[reqindex], req[reqindex+1], log_type);
				if (setret == -1){//此处记录失败
					zsetfailed++;
				}
			}//for j
		}//for i
		reqindex++;//跳过zsetcrc校验
	}

	uint64_t queuenamecnt = 0;
	uint64_t queuefailed = 0;

	if(datatype[3] == DataType::LIST){
		queuenamecnt = req[reqindex++].Uint64();
		for (uint64_t i = 0; i < queuenamecnt; i++){
			std::string queuename = req[reqindex++].data();
			uint64_t queueitemcnt = req[reqindex++].Uint64();
			for(uint64_t j = 0; j < queueitemcnt; j++,reqindex++){
				setret = serv->ssdb->lpush_back(Bytes(queuename), req[reqindex], log_type);
				if (setret == -1){//此处记录失败
					queuefailed++;
				}
			}//for j
		}//for i
		reqindex++;//跳过queuecrc校验
	}

	uint64_t ttlnamecnt = 0;
	uint64_t ttlfailed = 0;

	if(datatype[4] == DataType::TTL){
		ttlnamecnt = req[reqindex++].Uint64();
		for (uint64_t i = 0; i < ttlnamecnt; i++){
			std::string zsetname = req[reqindex++].data();
			uint64_t zsetkscnt = req[reqindex++].Uint64();
			for(uint64_t j = 0; j < zsetkscnt; j++,reqindex += 2){
				if (strcasecmp(retrieveFlag, "retrievekey") == 0){
					setret = serv->ssdb->zset_ttl(Bytes(zsetname), req[reqindex], req[reqindex+1], BinlogType::MIRROR, true);
				}else{
					setret = serv->ssdb->zset_ttl(Bytes(zsetname), req[reqindex], req[reqindex+1], log_type, true);					
				}
				if (setret == -1){//此处记录失败
					ttlfailed++;
				}
			}//for j
		}//for i
		reqindex++;//跳过zsetcrc校验
		serv->expiration->activeExpireQueue();
	}

	if (kvfailed == 0 && hashfailed == 0
			&& zsetfailed == 0	&& queuefailed == 0 && ttlfailed == 0){
		resp->push_back("slotsrestore_succ");
	}else{
		resp->push_back("slotsmgrtrestore_fail");
	}

	return 0;
}

int proc_role(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	bool mirror = serv->ssdb->mirror();
	resp->push_back("ok");

	if (mirror){
		resp->push_back("master");
	}else{
		resp->push_back("slave");
	}

	return 0;
}

int proc_config(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	
	if(req.size() == 3 && req[1].comparestrnocase(Bytes("get")) == 0
			&& req[2].comparestrnocase(Bytes("maxmemory")) ==0){
		resp->push_back("ok");
		resp->push_back("maxmemory");
		resp->push_back("0");
	}else{
		resp->push_back("error");
	}

	return 0;
}

int proc_slaveof(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	bool mirror = serv->ssdb->mirror();
	UNUSED(mirror);
	resp->push_back("ok");

	return 0;
}

int proc_inforedis(NetworkServer *net, Link *link, const Request &req, Response *resp){
	if (req.size() > 2){
		resp->push_back("client_error");
	}else if( req.size() == 2
			&& req[1].comparestrnocase(Bytes("replication")) == 0){
		std::string strespon;
		resp->push_back("ok");
		strespon.append("# Replication\r\n");
		strespon.append("role:slave\r\n");
		strespon.append("slave_priority:100\r\n");
		strespon.append("slave_repl_offset:0\r\n");
		resp->push_back(strespon);
	}else{
		resp->push_back("error");
	}

	return 0;
}
