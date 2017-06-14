/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* hash */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_hexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	const Bytes &name = req[1];
	const Bytes &key = req[2];
	std::string val;
	int ret = serv->ssdb->hget(name, key, &val);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_hexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	resp->push_back("ok");
	const Bytes &name = req[1];
	std::string val;
	for(Request::const_iterator it=req.begin()+2; it!=req.end(); it++){
		const Bytes &key = *it;
		int64_t ret = serv->ssdb->hget(name, key, &val);
		resp->push_back(key.String());
		if(ret > 0){
			resp->push_back("1");
		}else{
			resp->push_back("0");
		}
	}
	return 0;
}

int proc_hsetnx(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int ret = serv->ssdb->hsetnx(req[1], req[2], req[3]);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_hsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	resp->push_back("ok");
	for(Request::const_iterator it=req.begin()+1; it!=req.end(); it++){
		const Bytes &key = *it;
		int64_t ret = serv->ssdb->hsize(key);
		resp->push_back(key.String());
		if(ret <= -1){
			resp->push_back("-1");
		}else{
			resp->add(ret);
		}
	}
	return 0;
}

int proc_multi_hset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	if(req.size() < 4 || req.size() % 2 != 0){
		resp->push_back("client_error");
	}else{
		int num = 0;
		const Bytes &name = req[1];
		std::vector<Bytes>::const_iterator it = req.begin() + 2;
		for(; it != req.end(); it += 2){
			const Bytes &key = *it;
			const Bytes &val = *(it + 1);
			int ret = serv->ssdb->hset(name, key, val);
			if(ret == -1){
				resp->push_back("error");
				return 0;
			}else if(ret == -2){
				resp->push_back("datatype_invalid");
				return 0;
			}else{
				num += ret;
			}
		}
		resp->reply_int(0, num);
	}
	return 0;
}

int proc_multi_hdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	int num = 0;
	const Bytes &name = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 1){
		const Bytes &key = *it;
		int ret = serv->ssdb->hdel(name, key);
		if(ret == -1){
			resp->push_back("error");
			return 0;
		}else if(ret == -2){
			resp->push_back("datatype_invalid");
			return 0;
		}else{
			num += ret;
		}
	}
	resp->reply_int(0, num);
	return 0;
}

int proc_multi_hget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	resp->push_back("ok");
	Request::const_iterator it=req.begin() + 1;
	const Bytes name = *it;
	it ++;
	for(; it!=req.end(); it+=1){
		const Bytes &key = *it;
		std::string val;
		int ret = serv->ssdb->hget(name, key, &val);
		if (ret == -2){
			resp->push_back("datatype_invalid");
			return 0;
		}else if(ret == 1){
			resp->push_back(key.String());
			resp->push_back(val);
		}else{
			resp->push_back(key.String());
			resp->push_back("0");
		}
	}
	return 0;
}

int proc_hsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	int64_t ret = serv->ssdb->hsize(req[1]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_hset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	int ret = serv->ssdb->hset(req[1], req[2], req[3]);
	resp->reply_bool(ret);
	return 0;
}

int proc_hsetv(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;
	int64_t version = req[4].Int64();
	int ret = serv->ssdb->hsetv(req[1], req[2], req[3], &version);
	if(ret == -1){
		resp->push_back("error");
	}else if(ret == 0 || ret == 2){
		resp->push_back("ok");
		resp->push_back("0");
		resp->push_back(str(version));
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
	}else{
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back(str(version));
	}
	return 0;
}

int proc_hget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string val;
	int ret = serv->ssdb->hget(req[1], req[2], &val);
	resp->reply_get(ret, &val);
	return 0;
}

int proc_hgetv(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string val;
	int64_t version = 0;
	int ret = serv->ssdb->hgetv(req[1], req[2], &val, &version);
	if (ret == 1){		
		resp->push_back("ok");
		resp->push_back(val);
		resp->push_back(str(version));
	}else if(ret == 0 || ret == 2){
		resp->push_back("not_found");
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
	}else {
		resp->push_back("error");
	}
	return 0;
}

int proc_hdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	int ret = serv->ssdb->hdel(req[1], req[2]);
	resp->reply_bool(ret);
	return 0;
}

int proc_hclear(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;
	
	const Bytes &name = req[1];
	int64_t count = serv->ssdb->hclear(name);
	resp->reply_int(0, count);

	return 0;
}

int proc_hgetall(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::vector<std::string> list;
	int ret = serv->ssdb->hgetall(req[1], &list);
	resp->reply_list(ret, list);

	return 0;
}

int proc_hscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	Iterator *it = nullptr;
	int ret = serv->ssdb->hscan(&it, req[1], req[2], req[3], limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}

	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->value());
	}
	delete it;
	return 0;
}

int proc_hrscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	Iterator *it = nullptr;
	int ret = serv->ssdb->hrscan(&it, req[1], req[2], req[3], limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->value());
	}
	delete it;
	return 0;
}

int proc_hkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	Iterator *it = nullptr;
	int ret = serv->ssdb->hscan(&it, req[1], req[2], req[3], limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}
	it->return_val(false);

	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
	}
	delete it;
	return 0;
}

int proc_hvals(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
	SSDBServer *serv = (SSDBServer *)net->data;

	uint64_t limit = req[4].Uint64();
	Iterator *it = nullptr;
	int ret = serv->ssdb->hscan(&it, req[1], req[2], req[3], limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->value());
	}
	delete it;
	return 0;
}

int proc_hlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
 	SSDBServer *serv = (SSDBServer *)net->data;

    unsigned int slot = req[1].Int();
	uint64_t limit = req[4].Uint64();

	std::vector<std::string> list;
    int ret = serv->ssdb->hlist(slot, req[2], req[3], limit, &list);
    resp->reply_list(ret, list);

	return 0;
}

int proc_hrlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
 	SSDBServer *serv = (SSDBServer *)net->data;
    unsigned int slot = req[1].Int();
    uint64_t limit = req[4].Uint64();
    
    std::vector<std::string> list;
	int ret = serv->ssdb->hrlist(slot, req[2], req[3], limit, &list);
	resp->reply_list(ret, list);
	
	return 0;
}

// dir := +1|-1
static int _hincr(SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(3);

	int64_t by = 1;
	if(req.size() > 3){
		by = req[3].Int64();
	}
	int64_t new_val;
	int ret = ssdb->hincr(req[1], req[2], dir * by, &new_val);
	resp->reply_incr(ret, new_val);
	
	return 0;
}

int proc_hincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _hincr(serv->ssdb, req, resp, 1);
}

int proc_hdecr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _hincr(serv->ssdb, req, resp, -1);
}


