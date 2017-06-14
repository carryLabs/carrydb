#include "serv.h"
#include "net/proc.h"
#include "net/server.h"
#include "util/bson_tool.h"

int clt_create(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	bson_t *pd = parser_bson_from_data(req[2].data(), req[2].size(), pretty);
	if (!pd) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{	
		int ret = serv->ssdb->clt_create(req[1], &pd);
		resp->reply_clt_int(ret);
	}

	bson_destroy(pd);
	return 0;
}


int clt_recreate(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	
	Condition opts;
	if (req.size() >= 4){
		if (!ParserCmdCondition(req[3].data(), req[3].size(), &opts, pretty)) {
			resp->reply_clt_int(CmdStatus::kCltBsonError);
			return 0;
		}
	}
	//pd的内存会在函数里释放
	bson_t *pd = parser_bson_from_data(req[2].data(), req[2].size(), pretty);
	if (!pd) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{	
		int ret = serv->ssdb->clt_recreate(req[1], &pd, opts);
		if (ret > 0){
			resp->push_back("ok");
			resp->push_back("ok");
		}else {
			resp->reply_status(ret);
		}
	}
	return 0;
}



int clt_insert(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	Condition opts;
	opts.upsert = false;

	bson_t *pd = parser_bson_from_data(req[2].data(), req[2].size(), pretty);
	if (!pd) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{	
		int ret = serv->ssdb->clt_insert(req[1], pd, opts);
		resp->reply_clt_int(ret);
	}

	bson_destroy(pd);
	return 0;
}


int clt_upsert(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	Condition opts;
	opts.upsert = true;

	bson_t *pd = parser_bson_from_data(req[2].data(), req[2].size(), pretty);

	if (!pd) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{	
		int ret = serv->ssdb->clt_insert(req[1], pd, opts);
		resp->reply_clt_int(ret);
	}

	bson_destroy(pd);
	return 0;
}

int clt_find(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);

	Condition opts;
	if (req.size() >= 4){
		if (!ParserCmdCondition(req[3].data(), req[3].size(), &opts, pretty)) {
			resp->reply_clt_int(CmdStatus::kCltBsonError);
			return 0;
		}
	}

	bson_t *pd = parser_bson_from_data(req[2].data(), req[2].size(), pretty);
	opts.multi = true;
	if (!pd) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{
		std::vector<std::string> list;
		int ret = serv->ssdb->clt_find(req[1], pd, opts, &list);
		for (int i = 0; i < list.size(); ++i){
			if (pretty){
				bson_to_json(&list[i]);
			}
		}
		resp->reply_list(ret, list);
	}

	bson_destroy(pd);
	return 0;
}

int clt_update(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(4);

	Condition opts;
	if (req.size() >= 5){
		if (!ParserCmdCondition(req[4].data(), req[4].size(), &opts, pretty)) {
			resp->reply_clt_int(CmdStatus::kCltBsonError);
			return 0;
		}
	}
	bson_t *query = parser_bson_from_data(req[2].data(), req[2].size(), pretty);
	bson_t *update = parser_bson_from_data(req[3].data(), req[3].size(), pretty);
	if (!query || !update) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{
		int ret = serv->ssdb->clt_update(req[1], query, update, opts);
		resp->reply_clt_int(ret);
	}
	bson_clear(&query);
	bson_clear(&update); 
	return 0;
}

int clt_remove(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	Condition opts;
	opts.remove = true;
	if (req.size() >= 4){
		if (!ParserCmdCondition(req[3].data(), req[3].size(), &opts, pretty)) {
			resp->reply_clt_int(CmdStatus::kCltBsonError);
			return 0;
		}
	}
	bson_t *pd = parser_bson_from_data(req[2].data(), req[2].size(), pretty);
	if (!pd) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{	
		int ret = serv->ssdb->clt_remove(req[1], pd, opts);
		resp->reply_clt_int(ret);
	}
	bson_destroy(pd);
	return 0;
}

int clt_get(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	bson_t *pd = parser_bson_from_data(req[2].data(), req[2].size(), pretty);
	if (!pd) {
		resp->reply_clt_int(CmdStatus::kCltBsonError);
	}else{	
		std::string val;
		int ret = serv->ssdb->clt_get(req[1], pd, &val);
		
		if (ret > 0 && pretty){
			ret = bson_to_json(&val);
		}
		resp->reply_clt_get(ret, &val);
	}
	bson_destroy(pd);

	return 0;
}

int clt_multi_get(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	CHECK_NUM_PARAMS(3);
	SSDBServer *serv = (SSDBServer *)net->data;

	Request::const_iterator it=req.begin() + 1;
	const Bytes name = *it;
	it++;
	std::vector<std::string> list;
	std::vector<bson_t *> bson_keys;

	for (; it != req.end(); ++it){
		bson_t *pd = parser_bson_from_data(it->data(), it->size(), pretty);
		if (!pd) {
			resp->reply_clt_int(CmdStatus::kCltBsonError);
			return 0;
		}
		bson_keys.push_back(pd);
	}

	int ret = serv->ssdb->clt_multi_get(name, bson_keys, &list);

	if (ret > 0 && pretty) {
		for (auto it = list.begin(); it != list.end(); ++it){
			if (it->empty()){
				continue;
			}
			ret = bson_to_json(&(*it));
			if (ret < 0){
				break;
			}
		}
	}
	resp->reply_list(ret, list);
	for (auto it = bson_keys.begin(); it != bson_keys.end(); ++it){
		bson_destroy(*it);
	}
	return 0;
}

int clt_stat(NetworkServer *net, Link *link, const Request &req, Response *resp, bool pretty){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);
	
	std::string val;
	int ret = serv->ssdb->clt_stat(req[1], &val);
	if (ret > 0 && pretty){
		ret = bson_to_json(&val);
	}
	resp->reply_clt_get(ret, &val);
	return 0;
}

int proc_clt_clear(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);


	int ret = serv->ssdb->clt_clear(req[1]);
	resp->reply_clt_int(ret);
	return 0;
}

int proc_clt_drop(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);


	int ret = serv->ssdb->clt_drop(req[1]);
	resp->reply_clt_int(ret);
	return 0;
}

int proc_clt_create_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_create(net, link, req, resp, true);
}

int proc_clt_create(NetworkServer *net, Link *link, const Request &req, Response *resp){	
	return clt_create(net, link, req, resp, false);
}

int proc_clt_recreate_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_recreate(net, link, req, resp, true);
}

int proc_clt_recreate(NetworkServer *net, Link *link, const Request &req, Response *resp){	
	return clt_recreate(net, link, req, resp, false);
}

int proc_clt_isbuilding(NetworkServer *net, Link *link, const Request &req, Response *resp){	
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(2);


	int ret = serv->ssdb->clt_isbuilding(req[1]);
	resp->reply_clt_int(ret);
	return 0;
}

int proc_clt_insert_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_insert(net, link, req, resp, true);
}

int proc_clt_insert(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_insert(net, link, req, resp, false);
}

int proc_clt_upsert_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_upsert(net, link, req, resp, true);
}

int proc_clt_upsert(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_upsert(net, link, req, resp, false);
}

int proc_clt_find_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_find(net, link, req, resp, true);
}
int proc_clt_find(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_find(net, link, req, resp, false);
}

int proc_clt_update_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_update(net, link, req, resp, true);
}


int proc_clt_update(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_update(net, link, req, resp, false);
}

int proc_clt_remove_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_remove(net, link, req, resp, true);
}

int proc_clt_remove(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_remove(net, link, req, resp, false);
}

int proc_clt_get_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_get(net, link, req, resp, true);
}

int proc_clt_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_get(net, link, req, resp, false);
}

int proc_clt_multi_get_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_multi_get(net, link, req, resp, true);
}

int proc_clt_multi_get(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_multi_get(net, link, req, resp, false);
}

int proc_clt_stat(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_stat(net, link, req, resp, false);
}

int proc_clt_stat_pretty(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return clt_stat(net, link, req, resp, true);
}
