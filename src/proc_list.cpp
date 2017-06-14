/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* queue */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_lsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	int64_t ret = serv->ssdb->lsize(req[1]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_lfront(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string item;
	int ret = serv->ssdb->lfront(req[1], &item);
	resp->reply_get(ret, &item);
	return 0;
}

int proc_lback(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	std::string item;
	int ret = serv->ssdb->lback(req[1], &item);
	resp->reply_get(ret, &item);
	return 0;
}

static int QFRONT = 2;
static int QBACK  = 3;

static inline
int proc_lpush_func(NetworkServer *net, Link *link, const Request &req, Response *resp, int front_or_back){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t size = 0;
	std::vector<Bytes>::const_iterator it;
	it = req.begin() + 2;
	for(; it != req.end(); it += 1){
		const Bytes &item = *it;
		if(front_or_back == QFRONT){
			size = serv->ssdb->lpush_front(req[1], item);
		}else{
			size = serv->ssdb->lpush_back(req[1], item);
		}
		if(size < 0){
			resp->reply_bool(size, "error");
			return 0;
		}
	}
	resp->reply_int(0, size);
	return 0;
}

int proc_lpush_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_lpush_func(net, link, req, resp, QFRONT);
}

int proc_lpush_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_lpush_func(net, link, req, resp, QBACK);
}

int proc_lpush(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_lpush_func(net, link, req, resp, QBACK);
}

static inline
int proc_lpop_func(NetworkServer *net, Link *link, const Request &req, Response *resp, int front_or_back){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	
	uint64_t size = 1;
	if(req.size() > 2){
		size = req[2].Uint64();
	}
		
	int ret;
	std::string item;

	if(size == 1){
		if(front_or_back == QFRONT){
			ret = serv->ssdb->lpop_front(req[1], &item);
		}else{
			ret = serv->ssdb->lpop_back(req[1], &item);
		}
		resp->reply_get(ret, &item);
	}else{
		resp->push_back("ok");
		while(size-- > 0){
			if(front_or_back == QFRONT){
				ret = serv->ssdb->lpop_front(req[1], &item);
			}else{
				ret = serv->ssdb->lpop_back(req[1], &item);
			}
			if(ret <= 0){
				break;
			}else{
				resp->push_back(item);
			}
		}
	}

	return 0;
}

int proc_lpop_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_lpop_func(net, link, req, resp, QFRONT);
}

int proc_lpop_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_lpop_func(net, link, req, resp, QBACK);
}

int proc_lpop(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_lpop_func(net, link, req, resp, QFRONT);
}

static inline
int proc_ltrim_func(NetworkServer *net, Link *link, const Request &req, Response *resp, int front_or_back){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	
	uint64_t size = 1;
	if(req.size() > 2){
		size = req[2].Uint64();
	}
		
	int count = 0;
	for(; count<size; count++){
		int ret;
		std::string item;
		if(front_or_back == QFRONT){
			ret = serv->ssdb->lpop_front(req[1], &item);
		}else{
			ret = serv->ssdb->lpop_back(req[1], &item);
		}
		if(ret <= 0){
			break;
		}
	}
	resp->reply_int(0, count);

	return 0;
}

int proc_ltrim_front(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_ltrim_func(net, link, req, resp, QFRONT);
}

int proc_ltrim_back(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return proc_ltrim_func(net, link, req, resp, QBACK);
}

int proc_llist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
 	SSDBServer *serv = (SSDBServer *)net->data;

    unsigned int slot = req[1].Int();
	uint64_t limit = req[4].Uint64();

	std::vector<std::string> list;
    int ret = serv->ssdb->llist(slot, req[2], req[3], limit, &list);
    resp->reply_list(ret, list);

	return 0;
}

int proc_lrlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
 	SSDBServer *serv = (SSDBServer *)net->data;
    unsigned int slot = req[1].Int();
    uint64_t limit = req[4].Uint64();
    
    std::vector<std::string> list;
	int ret = serv->ssdb->lrlist(slot, req[2], req[3], limit, &list);
	resp->reply_list(ret, list);
	
	return 0;
}

// int proc_qfix(NetworkServer *net, Link *link, const Request &req, Response *resp){
// 	SSDBServer *serv = (SSDBServer *)net->data;
// 	CHECK_NUM_PARAMS(2);

// 	int ret = serv->ssdb->qfix(req[1]);
// 	if(ret == -1){
// 		resp->push_back("error");
// 	}else{
// 		resp->push_back("ok");
// 	}
// 	return 0;
// }

int proc_lclear(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	int64_t count = 0;
	while(1){
		std::string item;
		int ret = serv->ssdb->lpop_front(req[1], &item);
		if(ret == 0){
			break;
		}
		if(ret == -1){
			return -1;
		}
		count += 1;
	}
	resp->reply_int(0, count);
	return 0;
}

int proc_lrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t begin = req[2].Int64();
	int64_t limit = req[3].Uint64();
	int64_t end;
	if(limit >= 0){
		end = begin + limit - 1;
	}else{
		end = -1;
	}
	std::vector<std::string> list;
	int ret = serv->ssdb->lslice(req[1], begin, end, &list);
	resp->reply_list(ret, list);
	return 0;
}

int proc_lget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t index = req[2].Int64();
	std::string item;
	int ret = serv->ssdb->lget(req[1], index, &item);
	resp->reply_get(ret, &item);
	return 0;
}

int proc_lset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &name = req[1];
	int64_t index = req[2].Int64();
	const Bytes &item = req[3];
	int ret = serv->ssdb->lset(name, index, item);
	if(ret == -1 || ret == 0){
		resp->push_back("error");
		resp->push_back("index out of range");
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
	}else{
		resp->push_back("ok");
	}
	return 0;
}
