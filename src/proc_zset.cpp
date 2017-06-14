/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* zset */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_zexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	const Bytes &name = req[1];
	const Bytes &key = req[2];
	std::string val;
	int ret = serv->ssdb->zget(name, key, &val);
	resp->reply_bool(ret);
	return 0;
}

int proc_multi_zexists(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	resp->push_back("ok");
	const Bytes &name = req[1];
	std::string val;
	for(Request::const_iterator it=req.begin()+2; it!=req.end(); it++){
		const Bytes &key = *it;
		int64_t ret = serv->ssdb->zget(name, key, &val);
		resp->push_back(key.String());
		if(ret > 0){
			resp->push_back("1");
		}else{
			resp->push_back("0");
		}
	}
	return 0;
}

int proc_multi_zsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	resp->push_back("ok");
	for(Request::const_iterator it=req.begin()+1; it!=req.end(); it++){
		const Bytes &key = *it;
		int64_t ret = serv->ssdb->zsize(key);
		resp->push_back(key.String());
		if(ret <= -1){
			resp->push_back("-1");
		}else{
			resp->add(ret);
		}
	}
	return 0;
}

int proc_multi_zset(NetworkServer *net, Link *link, const Request &req, Response *resp){
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
			int ret = serv->ssdb->zset(name, key, val);
			if(ret == -1){
				resp->push_back("error");
				return 0;
			}else if (ret == -2){
				resp->push_back("datatype_invalid");
				return 0;
			}else if (ret == -3){
				resp->push_back("error");
				resp->push_back("value is not a valid float or out of range");
				return 0;	
			}else{
				num += ret;
			}
		}
		resp->reply_int(0, num);
	}
	return 0;
}

int proc_multi_zdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int num = 0;
	const Bytes &name = req[1];
	std::vector<Bytes>::const_iterator it = req.begin() + 2;
	for(; it != req.end(); it += 1){
		const Bytes &key = *it;
		int ret = serv->ssdb->zdel(name, key);
		if(ret <= -1){
			resp->push_back("error");
			return 0;
		}else{
			num += ret;
		}
	}
	resp->reply_int(0, num);
	return 0;
}

int proc_multi_zget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	resp->push_back("ok");
	Request::const_iterator it=req.begin() + 1;
	const Bytes name = *it;
	it ++;
	for(; it!=req.end(); it+=1){
		const Bytes &key = *it;
		std::string score;
		int ret = serv->ssdb->zget(name, key, &score);
		if(ret == 1){
			resp->push_back(score);
		}else{
			resp->push_back("nil");
		}
	}
	return 0;
}

int proc_zset(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int ret = serv->ssdb->zset(req[1], req[2], req[3]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_zsize(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	int64_t ret = serv->ssdb->zsize(req[1]);
	resp->reply_int(ret, ret);
	return 0;
}

int proc_zget(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	std::string score;
	int ret = serv->ssdb->zget(req[1], req[2], &score);
	resp->reply_get(ret, &score);
	return 0;
}

int proc_zdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int ret = serv->ssdb->zdel(req[1], req[2]);
	resp->reply_bool(ret);
	return 0;
}

int proc_zrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t ret = serv->ssdb->zrank(req[1], req[2]);
	resp->reply_zrank(ret, ret);
	return 0;
}

int proc_zrrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	int64_t ret = serv->ssdb->zrrank(req[1], req[2]);
	resp->reply_zrank(ret, ret);
	return 0;
}

int proc_zrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t offset = req[2].Uint64();
	uint64_t limit = req[3].Uint64();
	Iterator *it = nullptr;
	int ret = serv->ssdb->zrange(&it, req[1], offset, limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	delete it;
	return 0;
}

int proc_zrrange(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t offset = req[2].Uint64();
	uint64_t limit = req[3].Uint64();
	Iterator *it = nullptr;
	int ret = serv->ssdb->zrrange(&it, req[1], offset, limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	delete it;

	return 0;
}


int proc_zclear(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	
	int ret = serv->ssdb->zclear(req[1]);

	resp->reply_int(ret, ret);

	return 0;
}

int proc_zscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);

	uint64_t limit = req[5].Uint64();
	uint64_t offset = 0;
	if(req.size() > 6){
		offset = limit;
		limit = offset + req[6].Uint64();
	}
	Iterator *it = nullptr;
	int ret = serv->ssdb->zscan(&it, req[1], req[2], req[3], req[4], limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}

	if(offset > 0){
		it->skip(offset);
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	delete it;
	return 0;
}

int proc_zrscan(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);
	
	uint64_t limit = req[5].Uint64();
	uint64_t offset = 0;
	if(req.size() > 6){
		offset = limit;
		limit = offset + req[6].Uint64();
	}
	Iterator *it = nullptr;
	int ret = serv->ssdb->zrscan(&it, req[1], req[2], req[3], req[4], limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}

	if(offset > 0){
		it->skip(offset);
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
		resp->push_back(it->score());
	}
	delete it;
	return 0;
}

int proc_zkeys(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	uint64_t limit = 2000000000;
	std::string key;
	std::string start_score;
	std::string end_score;

	if (req.size() > 2){
		key = req[2].String();
	}
	if (req.size() > 3){
		start_score = req[3].String();
	}
	if (req.size() > 4){
		end_score = req[4].String();
	}
	if (req.size() > 5){
		limit = req[5].Uint64();
	}

	Iterator *it = nullptr;
	int ret = serv->ssdb->zscan(&it, req[1], key, start_score, end_score, limit);
	if (!resp->reply_valid_scan(ret)){
		return 0;
	}
	resp->push_back("ok");
	while(it->next()){
		resp->push_back(it->key());
	}
	delete it;
	return 0;
}

int proc_zlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
 	SSDBServer *serv = (SSDBServer *)net->data;

    unsigned int slot = req[1].Int();
	uint64_t limit = req[4].Uint64();

	std::vector<std::string> list;
    int ret = serv->ssdb->zlist(slot, req[2], req[3], limit, &list);
    resp->reply_list(ret, list);

	return 0;
}

int proc_zrlist(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(5);
 	SSDBServer *serv = (SSDBServer *)net->data;
    unsigned int slot = req[1].Int();
    uint64_t limit = req[4].Uint64();
    
    std::vector<std::string> list;
	int ret = serv->ssdb->zrlist(slot, req[2], req[3], limit, &list);
	resp->reply_list(ret, list);
	
	return 0;
}

// dir := +1|-1
static int _zincr(SSDB *ssdb, const Request &req, Response *resp, int dir){
	CHECK_NUM_PARAMS(3);

	int64_t by = 1;
	if(req.size() > 3){
		int ret = validInt64(req[3]);
		if (ret < 0){
			resp->reply_incr(ret, 0);
		}
		by = req[3].Int64();
	}
	int64_t new_val;
	int ret = ssdb->zincr(req[1], req[2], dir * by, &new_val);
	resp->reply_incr(ret, new_val);
	
	return 0;
}

int proc_zincr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _zincr(serv->ssdb, req, resp, 1);
}

int proc_zdecr(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	return _zincr(serv->ssdb, req, resp, -1);
}

int proc_zcount(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t count = 0;
	int ret = serv->ssdb->zcount(req[1], req[2], req[3], count);
	
	resp->reply_int(ret, count);
	return 0;
}

int proc_zsum(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t sum = 0;
	Iterator *it = nullptr;

	int ret = serv->ssdb->zscan(&it, req[1], "", req[2], req[3], -1);
	if (!resp->reply_valid_scan(ret, "0")){
		return 0;
	}

	while(it->next()){
		sum += str_to_int64(it->score());
	}
	delete it;
	
	resp->reply_int(0, sum);
	return 0;
}

int proc_zavg(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	int64_t sum = 0;
	int64_t count = 0;
	Iterator *it = nullptr;
	int ret = serv->ssdb->zscan(&it, req[1], "", req[2], req[3], -1);
	if (!resp->reply_valid_scan(ret, "0")){
		return 0;
	}

	while(it->next()){
		sum += str_to_int64(it->score());
		count ++;
	}
	delete it;
	double avg = (double)sum/count;
	
	resp->push_back("ok");
	resp->add(avg);
	return 0;
}

int proc_zremrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t count = 0;
	int ret = serv->ssdb->zremrangebyscore(req[1], req[2], req[3], count);
	resp->reply_int(ret, count);
	return 0;
}

int proc_zremrangebyrank(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	uint64_t start = req[2].Uint64();
	uint64_t end = req[3].Uint64();

	uint64_t count = 0;
	int ret = serv->ssdb->zremrangebyrank(req[1], start, end - start + 1, count);
	resp->reply_int(ret, count);
	return 0;
}

// 因为 writer 线程只有一个, 所以这个操作还是线程安全的
// 返回 -1 为失败， 返回0 为空
static inline
int zpop(Iterator *it, SSDBServer *serv, const Bytes &name, std::vector<std::string> *resp){
    int n = 0;
    int ret = 0;
	while(it->next()){
		ret = serv->ssdb->zdel(name, it->key());
		if(ret == 1){
			resp->push_back(it->key());
			resp->push_back(it->score());
            n++;
		}else{
            return (n == 0) ? -1 : n;
        }
	}
    return n;
}
static inline
int zpoppush(Iterator *it, SSDBServer *serv, const Bytes &src_name, const Bytes &dst_name, std::vector<std::string> *resp){
    int n = 0;
    int ret = 0;
	while(it->next()){
        ret = serv->ssdb->zset(dst_name, it->key(), it->score());
		if(ret >= 0){
		    ret = serv->ssdb->zdel(src_name, it->key());
            if(ret == 1){
			    resp->push_back(it->key());
			    resp->push_back(it->score());
                n ++;
            }else{
                return (n == 0) ? ret : n;
            }
		}else{
            return (n == 0) ? ret : n;
        }
	}
    return n;
}

int proc_zpop(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	
	const Bytes &name = req[1];
	uint64_t limit = 1;
	if (req.size() >= 3){
		limit = req[2].Uint64();
	}
	
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zscan(&it, name, "", "", "", limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
        resp->push_back("ok");
		goto CLEAN;
    }

    ret = zpop(it, serv, name, &list);
	resp->reply_list(ret, list);

CLEAN:
	delete it;
	return 0;
}
int proc_zpoppush(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	
	const Bytes &name = req[1];
	const Bytes &dst_name = req[2];
	uint64_t limit = 1;
	if (req.size() >= 4){
		limit = req[3].Uint64();
	}
	
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zscan(&it, name, "", "", "", limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
        resp->push_back("ok");
		goto CLEAN;
    }

    ret = zpoppush(it, serv, name, dst_name, &list);
	resp->reply_list(ret, list);

CLEAN:
	delete it;
	return 0;
}

int proc_zpopbyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);
	
	const Bytes &name = req[1];
	uint64_t limit = 1;
	if (req.size() >= 5){
		limit = req[4].Uint64();
	}
	
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zscan(&it, name, "", req[2], req[3], limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
		resp->push_back("ok");
		goto CLEAN;
	}

    ret = zpop(it, serv, name, &list);
	resp->reply_list(ret, list);

CLEAN:
	delete it;
	return 0;
}

int proc_zpoppushbyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);
	
	const Bytes &name = req[1];
	const Bytes &dst_name = req[2];
	uint64_t limit = 1;
	if (req.size() >= 6){
		limit = req[5].Uint64();
	}
	
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zscan(&it, name, "", req[3], req[4], limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
		resp->push_back("ok");
		goto CLEAN;
	}
    ret = zpoppush(it, serv, name, dst_name, &list);
	resp->reply_list(ret, list);

CLEAN:
	delete it;
	return 0;
}
int proc_zrevpop(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);

	const Bytes &name = req[1];
	uint64_t limit = 1;
	if (req.size() >= 3){
		limit = req[2].Uint64();
	}
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zrscan(&it, name, "", "", "", limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
		resp->push_back("ok");
		goto CLEAN;
	}
    ret = zpop(it, serv, name, &list);
	resp->reply_list(ret, list);
CLEAN:
	delete it;
	return 0;
}

int proc_zrevpoppush(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	const Bytes &name = req[1];
	const Bytes &dst_name = req[2];
	uint64_t limit = 1;
	if (req.size() >= 4){
		limit = req[3].Uint64();
	}
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zrscan(&it, name, "", "", "", limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
		resp->push_back("ok");
		goto CLEAN;
	}
    ret = zpoppush(it, serv, name, dst_name, &list);
	resp->reply_list(ret, list);
CLEAN:
	delete it;
	return 0;
}
int proc_zrevpopbyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	const Bytes &name = req[1];
	uint64_t limit = 1;
	if (req.size() >= 5){
		limit = req[4].Uint64();
	}
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zrscan(&it, name, "", req[2], req[3], limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
		resp->push_back("ok");
		goto CLEAN;
	}

    ret = zpop(it, serv, name, &list);
	resp->reply_list(ret, list);
CLEAN:
	delete it;
	return 0;
}
int proc_zrevpoppushbyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(5);

	const Bytes &name = req[1];
	const Bytes &dst_name = req[2];
	uint64_t limit = 1;
	if (req.size() >= 6){
		limit = req[5].Uint64();
	}
	Iterator *it = nullptr;
	std::vector<std::string> list;
	int ret = serv->ssdb->zrscan(&it, name, "", req[3], req[4], limit);
	if(ret == -1){
		resp->push_back("error");
		goto CLEAN;
	}else if (ret == -2){
		resp->push_back("datatype_invalid");
		goto CLEAN;
	}else if (ret == 0){
		resp->push_back("ok");
		goto CLEAN;
	}

    ret = zpoppush(it, serv, name, dst_name, &list);
	resp->reply_list(ret, list);
CLEAN:
	delete it;
	return 0;
}
// int proc_zfix(NetworkServer *net, Link *link, const Request &req, Response *resp){
// 	SSDBServer *serv = (SSDBServer *)net->data;
// 	CHECK_NUM_PARAMS(2);
	
// 	const Bytes &name = req[1];
// 	int64_t ret = serv->ssdb->zfix(name);
// 	resp->reply_int(ret, ret);
// 	return 0;
// }

int proc_zrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);
	
	uint64_t limit = req[5].Uint64();
	uint64_t offset = 0;
	if(req.size() > 6){
		offset = limit;
		limit = offset + req[6].Uint64();
	}

	std::vector<std::string> list;
	int ret = serv->ssdb->zrangebyscore(req[1], req[2], req[3], req[4], offset, limit, &list);
	resp->reply_list(ret, list);
	return 0;
}

int proc_zrevrangebyscore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(6);

	uint64_t limit = req[5].Uint64();
	uint64_t offset = 0;
	if(req.size() > 6){
		offset = limit;
		limit = offset + req[6].Uint64();
	}
	std::vector<std::string> list;
	int ret = serv->ssdb->zrevrangebyscore(req[1], req[2], req[3], req[4], offset, limit, &list);
	resp->reply_list(ret, list);

	return 0;
}
