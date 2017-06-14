/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "resp.h"
#include <stdio.h>
#include "../util/log.h"
int Response::size() const{
	return (int)resp.size();
}

void Response::push_back(const std::string &s){
	resp.push_back(s);
}

void Response::add(int s){
	add((int64_t)s);
}

void Response::add(int64_t s){
	char buf[20];
	sprintf(buf, "%" PRId64 "", s);
	resp.push_back(buf);
}

void Response::add(uint64_t s){
	char buf[20];
	sprintf(buf, "%" PRIu64 "", s);
	resp.push_back(buf);
}

void Response::add(double s){
	char buf[30];
	snprintf(buf, sizeof(buf), "%f", s);
	resp.push_back(buf);
}

void Response::add(const std::string &s){
	resp.push_back(s);
}

void Response::reply_status(int status, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else{
		resp.push_back("ok");
	}
}

bool Response::reply_valid_scan(int status, const char * mess){
	if(status == -1){
		resp.push_back("error");
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else if (status == 0){
		if (mess){
			resp.push_back(mess);
		}
	}else {
		return true;
	}
	return false;
}
void Response::reply_incr(int status, int64_t new_val){
	if (status == -3 || status == -4){
		resp.push_back("error");
		resp.push_back("value is not an integer or out of range");
	}else if (status == -5){
		resp.push_back("error");
		resp.push_back("increment or decrement would overflow");
	}else{
		reply_int(status, new_val);
	}
}

void Response::reply_bool(int status, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else if(status == 0 || status == 2){
		resp.push_back("ok");
		resp.push_back("0");
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else if (status == CmdStatus::kCltNotFoundConfig){
		resp.push_back("error");
		resp.push_back("Collection not found");
	}else{
		resp.push_back("ok");
		resp.push_back("1");
	}
}

void Response::reply_zrank(int status, int64_t val){
	if(status == -1){
		resp.push_back("ok");
		resp.push_back("nil");
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else{
		resp.push_back("ok");
		this->add(val);
	}
}

void Response::reply_int(int status, int64_t val){
	if(status == -1){
		resp.push_back("error");
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else if (status == -3 || status == -4) {
		resp.push_back("error");
		resp.push_back("value is not a valid float or out of range");
	}else{
		resp.push_back("ok");
		this->add(val);
	}
}
void Response::reply_clt_int(int status, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else if (status == -3 || status == -4) {
		resp.push_back("error");
		resp.push_back("value is not a valid float or out of range");
	}else if (status == CmdStatus::kCltNotFoundConfig){
		resp.push_back("error");
		resp.push_back("Collection not found");
	}else if (status == CmdStatus::kCltBsonError){
		resp.push_back("error");
		resp.push_back("Parser bson error");
	}else if(status == CmdStatus::kCltUpdateFaild){
		resp.push_back("error");
		resp.push_back("Primary update faild");
	}else if (status == CmdStatus::kCltNotFullKey){
		resp.push_back("error");
		resp.push_back("Primary key number error");
	}else if (status == CmdStatus::kCltUniqConflict){
		resp.push_back("error");
		resp.push_back("Unique index conflict");
	}else if (status == CmdStatus::kCltReBuilding){
		resp.push_back("error");
		resp.push_back("Collection is rebuilding");
	}else{
		resp.push_back("ok");
		this->add(status);
	}
}

void Response::reply_clt_get(int status, const std::string *val, const char *errmsg){
	if (status == 0 || status == 2){
		resp.push_back("not_found");
	}else if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else if (status == -3 || status == -4) {
		resp.push_back("error");
		resp.push_back("value is not a valid float or out of range");
	}else if (status == CmdStatus::kCltNotFoundConfig){
		resp.push_back("error");
		resp.push_back("Collection not found");
	}else if (status == CmdStatus::kCltBsonError){
		resp.push_back("error");
		resp.push_back("Parser bson error");
	}else if(status == CmdStatus::kCltUpdateFaild){
		resp.push_back("error");
		resp.push_back("Primary update faild");
	}else if (status == CmdStatus::kCltUniqConflict){
		resp.push_back("error");
		resp.push_back("Unique index conflict");
	}else if (status == CmdStatus::kCltReBuilding){
		resp.push_back("error");
		resp.push_back("Collection is rebuilding");
	}else{
		resp.push_back("ok");
		if(val){
			resp.push_back(*val);
		}
	}
}

void Response::reply_get(int status, const std::string *val, const char *errmsg){
	if (status == 1){		
		resp.push_back("ok");
		if(val){
			resp.push_back(*val);
		}
		return;
	}else if(status == 0 || status == 2){
		resp.push_back("not_found");
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else {
		resp.push_back("error");
	}
	if(errmsg){
		resp.push_back(errmsg);
	}
} 

void Response::reply_list(int status, const std::vector<std::string> &list){
	if(status == -1){
		resp.push_back("error");
	}else if (status == -2){
		resp.push_back("datatype_invalid");
	}else if (status == CmdStatus::kCltNotFoundConfig){
		resp.push_back("error");
		resp.push_back("Collection not found");
	}else if (status == CmdStatus::kCltBsonError){
		resp.push_back("error");
		resp.push_back("Parser bson error");
	}else if(status == CmdStatus::kCltUpdateFaild){
		resp.push_back("error");
		resp.push_back("Primary update faild");
	}else if (status == CmdStatus::kCltUniqConflict){
		resp.push_back("error");
		resp.push_back("Unique index conflict");
	}else if (status == CmdStatus::kCltReBuilding){
		resp.push_back("error");
		resp.push_back("Collection is rebuilding");
	}else{
		resp.push_back("ok");
		for(int i=0; i<list.size(); i++){
			resp.push_back(list[i]);
		}
	}
}
