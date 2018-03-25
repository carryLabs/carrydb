/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SERVER_H_
#define SSDB_SERVER_H_


#include <map>
#include <vector>
#include <string>
#include "db/ssdb_impl.h"
#include "db/ttl.h"
#include "db/rubbish_release.h"
#include "backend_dump.h"
#include "backend_sync.h"
#include "slave.h"
#include "net/server.h"
#include "include.h"

class SSDBServer
{
private:
	void reg_procs(NetworkServer *net);
	
	std::string kv_range_s;
	std::string kv_range_e;
	
	SSDB *meta;

public:
	SSDBImpl *ssdb;
	BackendDump *backend_dump;
	BackendSync *backend_sync;
	ExpirationHandler *expiration;
	RubbishRelease *releasehandler;
	std::vector<Slave *> slaves;
	bool is_compact_ = false;

	SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net);
	~SSDBServer();
	void ReplicationInfo(std::string &info);
	void SlaveInfo(std::string &info);
};

#define CHECK_NUM_PARAMS(n) do{ \
		if(req.size() < n){ \
			resp->push_back("client_error"); \
			resp->push_back("wrong number of arguments"); \
			return 0; \
		} \
	}while(0)

#define CHECK_NUM_PARAMS_EQUAL(n) do{ \
		if(req.size() != n){ \
			resp->push_back("client_error"); \
			resp->push_back("wrong number of arguments"); \
			return 0; \
		} \
	}while(0)
#endif
