/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_RESP_H_
#define NET_RESP_H_

#include <unistd.h>
#include <inttypes.h>
#include <string>
#include <vector>
#include "../util/const.h"
class Response
{
public:
	std::vector<std::string> resp;

	int size() const;
	void push_back(const std::string &s);
	void add(int s);
	void add(int64_t s);
	void add(uint64_t s);
	void add(double s);
	void add(const std::string &s);

	void reply_status(int status, const char *errmsg=NULL);
	void reply_bool(int status, const char *errmsg=NULL);
	void reply_int(int status, int64_t val);
	void reply_incr(int status, int64_t new_val);
	void reply_zrank(int status, int64_t val);
	// the same as Redis.REPLY_BULK
	void reply_get(int status, const std::string *val=NULL, const char *errmsg=NULL);
	void reply_list(int status, const std::vector<std::string> &list);
	bool reply_valid_scan(int status, const char * mess = "ok");

	void reply_clt_int(int status, const char *errmsg=NULL);
	void reply_clt_get(int status, const std::string *val, const char *errmsg=NULL);

	void clear(){resp.clear();}
};

#endif
