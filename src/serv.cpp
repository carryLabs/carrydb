/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "version.h"
#include "util/log.h"
#include "util/strings.h"
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

DEF_PROC(type);
DEF_PROC(del);
DEF_PROC(ttl);
DEF_PROC(expire);
DEF_PROC(exists);
DEF_PROC(persist);

DEF_PROC(get);
DEF_PROC(set);
DEF_PROC(setex);
DEF_PROC(setnx);
DEF_PROC(append);
DEF_PROC(psetex);
DEF_PROC(getset);
DEF_PROC(getbit);
DEF_PROC(setbit);
DEF_PROC(countbit);
DEF_PROC(substr);
DEF_PROC(getrange);
DEF_PROC(strlen);
DEF_PROC(bitcount);
DEF_PROC(incr);
DEF_PROC(incrby);
DEF_PROC(decr);
DEF_PROC(decrby);

DEF_PROC(scan);
DEF_PROC(rscan);
DEF_PROC(keys);
DEF_PROC(rkeys);
//DEF_PROC(multi_exists);
DEF_PROC(multi_get);

DEF_PROC(multi_set);
DEF_PROC(multi_setnx);
//DEF_PROC(multi_del);

DEF_PROC(hsize);
DEF_PROC(hget);
DEF_PROC(hgetv);
DEF_PROC(hset);
DEF_PROC(hsetv);
DEF_PROC(hsetnx);
DEF_PROC(hdel);
DEF_PROC(hincr);
DEF_PROC(hdecr);
DEF_PROC(hclear);
DEF_PROC(hgetall);
DEF_PROC(hscan);
DEF_PROC(hrscan);
DEF_PROC(hkeys);
DEF_PROC(hvals);
DEF_PROC(hlist);
DEF_PROC(hrlist);
//DEF_PROC(hexists);
//DEF_PROC(multi_hexists);
//DEF_PROC(multi_hsize);
DEF_PROC(multi_hget);
DEF_PROC(multi_hset);
DEF_PROC(multi_hdel);

DEF_PROC(zrank);
DEF_PROC(zrrank);
DEF_PROC(zrange);
DEF_PROC(zrrange);
DEF_PROC(zsize);
DEF_PROC(zget);
DEF_PROC(zset);
DEF_PROC(zdel);
DEF_PROC(zincr);
DEF_PROC(zdecr);
DEF_PROC(zclear);
//DEF_PROC(zfix);
DEF_PROC(zscan);
DEF_PROC(zrscan);
DEF_PROC(zkeys);
DEF_PROC(zlist);
DEF_PROC(zrlist);
DEF_PROC(zcount);
//DEF_PROC(zsum);
//DEF_PROC(zavg);
DEF_PROC(zexists);
DEF_PROC(zremrangebyrank);
DEF_PROC(zremrangebyscore);
DEF_PROC(multi_zexists);
DEF_PROC(multi_zsize);
DEF_PROC(multi_zget);
DEF_PROC(multi_zset);
DEF_PROC(multi_zdel);
DEF_PROC(zpop);
DEF_PROC(zrevpop);
DEF_PROC(zpopbyscore);
DEF_PROC(zrevpopbyscore);
DEF_PROC(zpoppush);
DEF_PROC(zrevpoppush);
DEF_PROC(zpoppushbyscore);
DEF_PROC(zrevpoppushbyscore);
DEF_PROC(zrangebyscore);
DEF_PROC(zrevrangebyscore);
	
DEF_PROC(lsize);
DEF_PROC(lfront);
DEF_PROC(lback);
DEF_PROC(lpush);
DEF_PROC(lpush_front);
DEF_PROC(lpush_back);
DEF_PROC(lpop);
DEF_PROC(lpop_front);
DEF_PROC(lpop_back);
DEF_PROC(ltrim_front);
DEF_PROC(ltrim_back);
//DEF_PROC(qfix);
DEF_PROC(lclear);
DEF_PROC(llist);
DEF_PROC(lrlist);
DEF_PROC(lrange);
DEF_PROC(lget);
DEF_PROC(lset);
DEF_PROC(dump);

//manager
DEF_PROC(sync140);
DEF_PROC(info);
DEF_PROC(version);
DEF_PROC(compact);
DEF_PROC(clear_binlog);
DEF_PROC(flushdb);
DEF_PROC(lowertime);
DEF_PROC(alarm);
DEF_PROC(clear_slowrequest);

DEF_PROC(ping);
DEF_PROC(slotsinfo);
DEF_PROC(slotshashkey);
DEF_PROC(slotsmgrttagslot);
DEF_PROC(slotsmgrttagone);
DEF_PROC(slotsrestore);
DEF_PROC(role);
DEF_PROC(config);
DEF_PROC(slaveof);
DEF_PROC(retrievekey);


#define REG_PROC(c, f)     net->proc_map.set_proc(#c, f, proc_##c)

void SSDBServer::reg_procs(NetworkServer *net){
	REG_PROC(type, "rt"),
	REG_PROC(del, "wt"),
	REG_PROC(ttl, "rt"),
	REG_PROC(expire, "wt"),
	REG_PROC(exists, "rt"),
	REG_PROC(persist, "wt");

	REG_PROC(get, "rt");
	REG_PROC(set, "wt");
	REG_PROC(setnx, "wt");
	REG_PROC(setex, "wt");
	REG_PROC(psetex, "wt");
	REG_PROC(getset, "wt");
	REG_PROC(getbit, "rt");
	REG_PROC(setbit, "wt");
	REG_PROC(countbit, "rt");
	REG_PROC(substr, "rt");
	REG_PROC(getrange, "rt");
	REG_PROC(strlen, "rt");
	REG_PROC(bitcount, "rt");
	REG_PROC(incr, "wt");
	REG_PROC(decr, "wt");
	REG_PROC(append, "wt");
	REG_PROC(incrby, "wt");
	REG_PROC(decrby, "wt");

	REG_PROC(scan, "rt");
	REG_PROC(rscan, "rt");
	REG_PROC(keys, "rt");
	REG_PROC(rkeys, "rt");
	//REG_PROC(exists, "rt");
	//REG_PROC(multi_exists, "rt");
	REG_PROC(multi_get, "rt");
	REG_PROC(multi_set, "wt");
	REG_PROC(multi_setnx, "wt");
	//REG_PROC(multi_del, "wt");

	REG_PROC(hsize, "rt");
	REG_PROC(hget, "rt");
	REG_PROC(hset, "wt");
	REG_PROC(hgetv, "rt");
	REG_PROC(hsetv, "wt");
	REG_PROC(hdel, "wt");
	REG_PROC(hincr, "wt");
	REG_PROC(hdecr, "wt");
	REG_PROC(hclear, "wt");
	REG_PROC(hgetall, "rt");
	REG_PROC(hscan, "rt");
	REG_PROC(hrscan, "rt");
	REG_PROC(hkeys, "rt");
	REG_PROC(hvals, "rt");
	REG_PROC(hlist, "rt");
	REG_PROC(hrlist, "rt");
	REG_PROC(hsetnx, "wt");
	//REG_PROC(hexists, "rt");
	//REG_PROC(multi_hexists, "rt");
	//REG_PROC(multi_hsize, "rt");
	REG_PROC(multi_hget, "rt");
	REG_PROC(multi_hset, "wt");
	REG_PROC(multi_hdel, "wt");

	// because zrank may be extremly slow, execute in a seperate thread
	REG_PROC(zrank, "rt");
	REG_PROC(zrrank, "rt");
	REG_PROC(zrange, "rt");
	REG_PROC(zrrange, "rt");
	REG_PROC(zsize, "rt");
	REG_PROC(zget, "rt");
	REG_PROC(zset, "wt");
	REG_PROC(zdel, "wt");
	REG_PROC(zincr, "wt");
	REG_PROC(zdecr, "wt");
	REG_PROC(zclear, "wt");
	//REG_PROC(zfix, "wt");
	REG_PROC(zscan, "rt");
	REG_PROC(zrscan, "rt");
	REG_PROC(zkeys, "rt");
	REG_PROC(zlist, "rt");
	REG_PROC(zrlist, "rt");
	REG_PROC(zcount, "rt");
	//REG_PROC(zsum, "rt");
	//REG_PROC(zavg, "rt");
	REG_PROC(zremrangebyrank, "wt");
	REG_PROC(zremrangebyscore, "wt");
	REG_PROC(zexists, "rt");
	REG_PROC(multi_zexists, "rt");
	REG_PROC(multi_zsize, "rt");
	REG_PROC(multi_zget, "rt");
	REG_PROC(multi_zset, "wt");
	REG_PROC(multi_zdel, "wt");
	REG_PROC(zpop, "wt");
	REG_PROC(zrevpop, "wt");
	REG_PROC(zpopbyscore, "wt");
	REG_PROC(zrevpopbyscore, "wt");
	REG_PROC(zpoppush, "wt");
	REG_PROC(zrevpoppush, "wt");
	REG_PROC(zpoppushbyscore, "wt");
	REG_PROC(zrevpoppushbyscore, "wt");
	REG_PROC(zrangebyscore, "rt");
	REG_PROC(zrevrangebyscore, "rt");

	REG_PROC(lsize, "rt");
	REG_PROC(lfront, "rt");
	REG_PROC(lback, "rt");
	REG_PROC(lpush, "wt");
	REG_PROC(lpush_front, "wt");
	REG_PROC(lpush_back, "wt");
	REG_PROC(lpop, "wt");
	REG_PROC(lpop_front, "wt");
	REG_PROC(lpop_back, "wt");
	REG_PROC(ltrim_front, "wt");
	REG_PROC(ltrim_back, "wt");
	//REG_PROC(qfix, "wt");
	REG_PROC(lclear, "wt");
	REG_PROC(llist, "rt");
	REG_PROC(lrlist, "rt");
	//REG_PROC(lslice, "rt");
	REG_PROC(lrange, "rt");
	REG_PROC(lget, "rt");
	REG_PROC(lset, "wt");

	REG_PROC(clear_binlog, "wt");
	REG_PROC(flushdb, "wt");

	REG_PROC(dump, "b");
	REG_PROC(sync140, "b");
	REG_PROC(info, "rt");
	REG_PROC(version, "r");

	// doing compaction in a reader thread, because we have only one
	// writer thread(for performance reason); we don't want to block writes
	REG_PROC(compact, "rt");


	REG_PROC(ping, "rt");
	REG_PROC(slotsinfo, "rt");
	REG_PROC(slotshashkey, "rt");

	REG_PROC(slotsmgrttagslot, "wt");
	REG_PROC(slotsmgrttagone, "wt");
	REG_PROC(slotsrestore, "wt");
	REG_PROC(retrievekey, "wt");

	REG_PROC(role, "rt");
	REG_PROC(config, "rt");
	REG_PROC(slaveof, "rt");
	REG_PROC(retrievekey, "rt");
	REG_PROC(lowertime, "rt");
	REG_PROC(alarm, "rt");
	REG_PROC(clear_slowrequest, "wt");
}


SSDBServer::SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net){
	this->ssdb = (SSDBImpl *)ssdb;
	this->meta = meta;

	net->data = this;
	this->reg_procs(net);

	int sync_speed = conf.get_num("replication.sync_speed");
	backend_dump = new BackendDump(this->ssdb);
	backend_sync = new BackendSync(this->ssdb, sync_speed);

	expiration = new ExpirationHandler(this->ssdb);
	this->ssdb->expiration = this->expiration;
	
	releasehandler = new RubbishRelease(this->ssdb);
	this->ssdb->releasehandler = this->releasehandler;

	{ // slaves
		const Config *repl_conf = conf.get("replication");
		if(repl_conf != NULL){
			std::vector<Config *> children = repl_conf->children;
			for(std::vector<Config *>::iterator it = children.begin(); it != children.end(); it++){
				Config *c = *it;
				if(c->key != "slaveof"){
					continue;
				}
				std::string ip = c->get_str("ip");
				int port = c->get_num("port");
				if(ip == ""){
					ip = c->get_str("host");
				}
				if(ip == "" || port <= 0 || port > 65535){
					continue;
				}
				bool is_mirror = false;
				std::string type = c->get_str("type");
				if(type == "mirror"){
					this->ssdb->is_mirror_ = is_mirror = true;
				}else{
					type = "sync";
					this->ssdb->is_mirror_ = is_mirror = false;
				}
				
				std::string id = c->get_str("id");
				
				log_info("slaveof: %s:%d, type: %s", ip.c_str(), port, type.c_str());
				Slave *slave = new Slave(ssdb, meta, ip.c_str(), port, is_mirror);
				if(!id.empty()){
					slave->set_id(id);
				}
				slave->auth = conf.get_str("server.auth");
				slave->start();
				slaves.push_back(slave);
			}
		}
	}
}

SSDBServer::~SSDBServer(){
	std::vector<Slave *>::iterator it;
	for(it = slaves.begin(); it != slaves.end(); it++){
		Slave *slave = *it;
		slave->stop();
		delete slave;
	}
	delete backend_dump;
	delete backend_sync;
	delete expiration;
	delete releasehandler;
	
	log_debug("SSDBServer finalized");
}

void SSDBServer::ReplicationInfo(std::string &info)
{
    std::stringstream tmp_stream;

    tmp_stream << "# Replication\r\n"; 
    tmp_stream << "role:slave" << "\r\n";
    tmp_stream << "slave_priority:100" << "\r\n";
    tmp_stream << "slave_repl_offset:0" << "\r\n";

    info.append(tmp_stream.str());
}

void SSDBServer::SlaveInfo(std::string &info)
{

	std::stringstream tmp_stream;

	std::string sRst;
	{		
		tmp_stream << "# BinlogStats" << "\r\n";
		tmp_stream << ssdb->binlogs->stats() << "\r\n";
	}
	{
		tmp_stream << "# BackSyncStats" << "\r\n";

		std::vector<std::string> syncs = backend_sync->stats();
		for(auto it = syncs.cbegin(); it != syncs.cend(); ++it){
			tmp_stream << *it;
		}
	}

	{
		tmp_stream << "# SlavesStats" << "\r\n";
		for(auto it = slaves.cbegin(); it != slaves.cend(); ++it){
			Slave *slave = *it;
			tmp_stream << slave->stats();
			tmp_stream << "master_port:" << slave->masterPort() << "\r\n";
			tmp_stream << "master_host:" << slave->masterAddr() << "\r\n";
			tmp_stream << "master_link_status:" << slave->masterStatus() << "\r\n";
			tmp_stream << "master_link_down_since_seconds:" << 1 << "\r\n";
		}
	}
	info.append(tmp_stream.str());
}

int proc_clear_binlog(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->ssdb->binlogs->flush();
	resp->push_back("ok");
	return 0;
}

int proc_flushdb(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(serv->slaves.size() > 0 || serv->backend_sync->stats().size() > 0){
		resp->push_back("error");
		resp->push_back("flushdb is not allowed when replication is in use!");
		return 0;
	}
	serv->ssdb->flushdb();
	resp->push_back("ok");
	return 0;
}

int proc_dump(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->backend_dump->proc(link);
	return PROC_BACKEND;
}

int proc_sync140(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->backend_sync->proc(link);
	return PROC_BACKEND;
}

int proc_compact(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	serv->ssdb->compact();
	resp->push_back("ok");
	resp->push_back("ok");
	return 0;
}

int proc_dbsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back(str(0));
	return 0;
}

int proc_lowertime(NetworkServer *net, Link *link, const Request &req, Response *resp){
	if (req.size() == 1) {
		resp->push_back("ok");
		resp->push_back(str(net->lower_time()));
	} else {
		int millisecond = req[1].Int();
		if (millisecond > 0) {
			net->set_lower_time(millisecond);
			resp->push_back("ok");
			resp->push_back("ok");
		} else {
			resp->push_back("error");
			resp->push_back("time invalid");
		}
	}
	return 0;
}

int proc_alarm(NetworkServer *net, Link *link, const Request &req, Response *resp){
	std::string result;
	result.append("# SlowRequest");

	resp->push_back("ok");
    resp->push_back(net->slow_cmd_list());
   	return PROC_OK;
}

int proc_clear_slowrequest(NetworkServer *net, Link *link, const Request &req, Response *resp){
	net->clear_cmd_list();
	resp->push_back("ok");
	return 0;
}

int proc_version(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back(CARRYDB_VERSION);
	return 0;
}

int proc_ttl(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS_EQUAL(2);

	int64_t ttl = serv->ssdb->ttl(req[1]);
	resp->push_back("ok");
	resp->push_back(str(ttl));
	
	return 0;
}


int proc_expire(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS_EQUAL(3);

	int ret = validInt64(req[2]);
	if (ret > 0) {
        int64_t times = time_ms() + req[2].Int64() * 1000;
		ret = serv->ssdb->expire(req[1], times);
        if (ret > 0){
			resp->push_back("ok");
			resp->push_back("1");
			return 0;
        }
	}
	resp->reply_incr(ret, 0);
	
	return 0;
}

int proc_persist(NetworkServer *net, Link *link, const Request &req, Response *resp)
{
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS_EQUAL(2);

	int ret = serv->ssdb->persist(req[1]);
	if (ret > 0) {
		resp->push_back("ok");
		resp->push_back("1");
	}else if (ret == 0) {
		resp->push_back("ok");
		resp->push_back("0");
	}else {
    	resp->reply_incr(ret, 0);
	}
	return 0;
}

int proc_ping(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	return 0;
}

int proc_del(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	int ret = serv->ssdb->del(req[1]);
	if(ret <= -1){
		resp->push_back("error");
	}else if (ret == 0){
		resp->push_back("ok");
		resp->push_back("0");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}

	return 0;
}

int proc_exists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string type;
	int ret = serv->ssdb->exists(req[1], &type);
	if(ret <= -1){
		resp->push_back("error");
	}else if (ret == 0){
		resp->push_back("ok");
		resp->push_back("0");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	
	return 0;
}

int proc_type(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string type;
	int ret = serv->ssdb->exists(req[1], &type);
	if(ret <= -1){
		resp->push_back("error");
	}else if (ret == 0){
		resp->push_back("ok");
		resp->push_back("0");
	}else {
		resp->push_back("ok");
		resp->push_back(type);
	}
	
	return 0;
}

int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
    if (req.size() > 3){
        resp->push_back("client_error");
        return PROC_ERROR;
    }
    const char *section = req.size() == 2 ? req[1].data() : nullptr;

    bool bAllSection = false;
    bAllSection = section == nullptr || strcasecmp(section, "all") == 0;
    resp->push_back("ok");

    std::string sInfo;
    if (bAllSection || !strcasecmp(section, "server")) {
        sInfo.append("# Server\r\n");
        sInfo.append(strprintfln("carrydb_version:%s", CARRYDB_VERSION));
    	sInfo.append(strprintfln("link_count:%d", net->link_count));
    	sInfo.append(strprintfln("process_id:%d", getpid()));
    	sInfo.append(strprintfln("is_compact:%d", serv->ssdb->IsCompact()));
    }

    if (bAllSection || !strcasecmp(section, "replication")) {
        serv->ReplicationInfo(sInfo);
    }

    if (bAllSection || !strcasecmp(section, "slaveinfo")) {
        serv->SlaveInfo(sInfo);
    }

    if (bAllSection || !strcasecmp(section, "memory")) {
        serv->ssdb->MemoryInfo(sInfo);
    }

    if (bAllSection || !strcasecmp(section, "disk")) {
        serv->ssdb->DiskInfo(sInfo);
    }


    if (bAllSection || !strcasecmp(section, "cmd")) {
        sInfo.append("# Cmd（calls,time_wait,time_proc）\r\n");
        std::string buf;
        proc_map_t::iterator it;
		for(it=net->proc_map.begin(); it!=net->proc_map.end(); it++){
			Command *cmd = it->second;
			if (cmd->calls)
			buf.append(strprintfln("cmd_%s:%" PRIu64 ",%.0f,%.0f", cmd->name.data(),
                             cmd->calls, cmd->time_wait, cmd->time_proc));
		}
		sInfo.append(buf);
    }

    resp->push_back(sInfo);
   	return PROC_OK;
}

/*
//DEF_PROC(zfix);
DEF_PROC(zsum);
DEF_PROC(zavg);

	
//DEF_PROC(qfix);
DEF_PROC(lclear);

DEF_PROC(dump);
DEF_PROC(clear_binlog);
DEF_PROC(flushdb);
*/
