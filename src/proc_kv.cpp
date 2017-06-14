/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* kv */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string val;
	int ret = serv->ssdb->get(req[1], &val);
	resp->reply_get(ret, &val);
	return 0;
}

int proc_getset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	std::string val;
	int ret = serv->ssdb->getset(req[1], &val, req[2]);
	resp->reply_get(ret, &val);
	return 0;
}

int proc_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	
	int ret = serv->ssdb->set(req[1], req[2]);
	if(ret == -1){
		resp->push_back("error");
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
	}
	return 0;
}

int proc_setnx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int ret = serv->ssdb->setnx(req[1], req[2]);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_set(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 3 || req.size() % 2 != 1){
		resp->push_back("client_error");
	}else{
		int ret = serv->ssdb->multi_set(req, 1);
		resp->reply_int(0, ret);
	}
	return 0;
}

int proc_multi_setnx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 3 || req.size() % 2 != 1){
		resp->push_back("client_error");
	}else{
		int ret = serv->ssdb->multi_setnx(req, 1);
		resp->reply_int(0, ret);
	}
	return 0;
}

int proc_multi_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	resp->push_back("ok");
	for(int i=1; i<req.size(); i++){
		std::string val;

		int ret = serv->ssdb->get(req[i], &val);
		//redis 找不到就是nil
		if (ret == -1){
			resp->clear();
			resp->push_back("error");
			return 0;
		}
		
		resp->push_back(req[i].String());
		resp->push_back(val);
	}
	return 0;
}

int proc_scan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);

	unsigned int slot =req[1].Int();
	uint64_t limit = req[4].Uint64();

	Iterator *it = serv->ssdb->scan_slot(DataType::KSIZE, slot, req[2], req[3], limit);
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->value());
	}
	delete it;
	return 0;
}

int proc_rscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
    SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);
        
    unsigned int slot = req[1].Int();
    uint64_t limit = req[4].Uint64();
    Iterator *it = serv->ssdb->rscan_slot(DataType::KSIZE, slot, req[2], req[3], limit);
    resp->push_back("ok");
    while(it->next()){
        resp->push_back(it->key());
        resp->push_back(it->value());
    }   
    delete it;   
    return 0;
}

int proc_keys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);

	unsigned int slot =req[1].Int();
	uint64_t limit = req[4].Uint64();

	Iterator *it = serv->ssdb->scan_slot(DataType::KSIZE, slot, req[2], req[3], limit);
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
	}
	delete it;
	return 0;
}

int proc_rkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);
        
    unsigned int slot = req[1].Int();
    uint64_t limit = req[4].Uint64();
    Iterator *it = serv->ssdb->rscan_slot(DataType::KSIZE, slot, req[2], req[3], limit);
    resp->push_back("ok");
    while(it->next()){
        resp->push_back(it->key());
    }   
    delete it;  
	return 0;
}

// dir := +1|-1
static int _incr(SSDB *ssdb, const Request &req, Response *resp, int dir, bool incr){
	
	if(incr){
		CHECK_NUM_PARAMS(2);
	}else if (!incr){
		CHECK_NUM_PARAMS(3);
	}
	
	int64_t by = 1;
	if(req.size() > 2){
		by = req[2].Int64();
	}
	int64_t new_val;
	int ret = ssdb->incr(req[1], dir * by, &new_val);
	resp->reply_incr(ret, new_val);
	return 0;
}

int proc_incr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _incr(serv->ssdb, req, resp, 1, true);
}

int proc_decr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _incr(serv->ssdb, req, resp, -1, true);
}

int proc_incrby(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _incr(serv->ssdb, req, resp, 1, false);
}

int proc_decrby(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _incr(serv->ssdb, req, resp, -1, false);
}

int proc_getbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int ret = serv->ssdb->getbit(req[1], req[2].Int());
	resp->reply_bool(ret);
	return 0;
}

int proc_setbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &key = req[1];
	int offset = req[2].Int();
	if(req[3].size() == 0 || (req[3].data()[0] != '0' && req[3].data()[0] != '1')){
		resp->push_back("client_error");
		resp->push_back("bit is not an integer or out of range");
		return 0;
	}
	if(offset < 0 || offset > Link::MAX_PACKET_SIZE * 8){
		std::string msg = "offset is out of range [0, ";
		msg += str(Link::MAX_PACKET_SIZE * 8);
		msg += "]";
		resp->push_back("client_error");
		resp->push_back(msg);
		return 0;
	}
	int on = req[3].Int();
	int ret = serv->ssdb->setbit(key, offset, on);
	resp->reply_bool(ret);
	return 0;
}

int proc_countbit(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	std::string val;
	
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else{
		std::string str;
		int size = -1;
		if(req.size() > 3){
			size = req[3].Int();
			str = substr(val, start, size);
		}else{
			str = substr(val, start, val.size());
		}
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
}

int proc_bitcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		start = req[2].Int();
	}
	int end = -1;
	if(req.size() > 3){
		end = req[3].Int();
	}
	std::string val;
	
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
	}else{
		std::string str = str_slice(val, start, end);
		int count = bitcount(str.data(), str.size());
		resp->reply_int(0, count);
	}
	return 0;
}

int proc_substr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	int start = 0;
	if(req.size() > 2){
		if (validInt64(req[2]) < 0){
			resp->reply_incr(-4, 0);
			return 0;
		}
		start = req[2].Int();
	}
	int size = 2000000000;
	if(req.size() > 3){
		if (validInt64(req[3]) < 0){
			resp->reply_incr(-4, 0);
			return 0;
		}
		size = req[3].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
	}else{
		std::string str = substr(val, start, size);
		resp->push_back("ok");
		resp->push_back(str);
	}
	return 0;
}

int proc_getrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	int start = 0;

	if(req.size() > 2){
		if (validInt64(req[2]) < 0){
			resp->reply_incr(-4, 0);
			return 0;
		}
		start = req[2].Int();
	}
	int size = -1;
	if(req.size() > 3){
		if (validInt64(req[3]) < 0){
			resp->reply_incr(-4, 0);
			return 0;
		}
		size = req[3].Int();
	}
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	if(ret == -1){
		resp->push_back("error");
	}else if (ret == 0){
		resp->push_back("not_found");
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
	}else{
		std::string str = str_slice(val, start, size);
		resp->push_back("ok");
		resp->push_back(str);
	}
	return 0;
}

int proc_strlen(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &key = req[1];
	std::string val;
	int ret = serv->ssdb->get(key, &val);
	resp->reply_int(ret, val.size());
	return 0;
}

int proc_append(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	
	uint64_t newlen = 0;
	int ret = serv->ssdb->append(req[1], req[2], &newlen);
	resp->reply_int(ret, newlen);

	return 0;
}

int proc_setex(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);
	
	int ret = validInt64(req[2]);
	if (ret > 0) {
        int64_t times = time_ms() + req[2].Int64() * 1000;
		ret = serv->ssdb->setex(req[1], times, req[3]);
        if (ret > 0){
	        resp->reply_int(0, 1);
			return 0;
        }
	}
	resp->reply_incr(ret, 0);
	return 0;
}

int proc_psetex(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);
	

	int ret = validInt64(req[2]);
	if (ret > 0) {
        int64_t times = time_ms() + req[2].Int64();
		ret = serv->ssdb->setex(req[1], times, req[3]);
        if (ret > 0){
	        resp->reply_int(0, 1);
			return 0;
        }
	}
	resp->reply_incr(ret, 0);
	return 0;
}
