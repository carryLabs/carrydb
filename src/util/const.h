/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_CONST_H_
#define SSDB_CONST_H_

#include <string>
static const int SSDB_SCORE_WIDTH				= 9;
static const int SSDB_KEY_LEN_MAX				= 255;

enum CmdStatus {
	kDBError = -1,
	kOptionConflict = -9,
	kCltNotFoundConfig = -10,
	kCltBsonError = -11,
	kCltUpdateFaild = -12,
	kCltNotFullKey = -13,
	kCltUniqConflict = -14,
	kCltReBuilding = -15,

	kNotFound = 0,
	kOk = 1,
};

class DataType {
public:
	//类型不能随便定义字母,需要小于h或者大于s,这些保留类型是用于copy时候进行底层解析的
	//h i j l m n  p r 剩余可用
	static const char SYNCLOG		= 1;
	static const char KV			= 'k';
	static const char KSIZE			= 'K';
	static const char HASH			= 'h'; // hashmap(sorted by key)
	static const char HSIZE			= 'H';
	static const char ZSET			= 's'; // key => score
	static const char ZSCORE		= 'z'; // key|score => ""
	static const char ZSIZE			= 'Z';
	static const char LIST			= 'q';
	static const char LSIZE			= 'Q';
	static const char SLOTID		= 't';
	static const char TTL 			= 'T'; //迁移的时候标示数据类型用到
	static const char METAL     	= 'k'; //'M';
	static const char RUBBISH   	= 'e'; //需要删除的版本前缀
	static const char TABLE			= 'o';
	static const char MIN_PREFIX 	= HASH; //KV,HASH,LIST,ZSET
	static const char MAX_PREFIX 	= ZSET;
};

enum ValueType : char{
	kNone    = 0x00,
	kNoCopy  = 0x01, //不进行copy data操作，表示是索引数据
	kReBuild = 0x02,
	kVersion = 0x03
};

class BinlogType {
public:
	static const char NOOP			= 0;//记录binlog，slave不处理命令
	static const char SYNC			= 1;
	static const char MIRROR		= 2;
	static const char COPY			= 3;
	static const char CTRL      	= 4;
	static const char NONE			= 10;//不记录binlog,slave不知道此命令
};

class BinlogCommand {
public:
	static const char NONE  		= 0;
	static const char KSET  		= 1;
	static const char KDEL  		= 2;
	static const char HSET  		= 3;
	static const char HDEL  		= 4;
	static const char ZSET  		= 5;
	static const char ZDEL  		= 6;
	
	static const char LPUSH_BACK	= 10;
	static const char LPUSH_FRONT	= 11;
	static const char LPOP_BACK		= 12;
	static const char LPOP_FRONT	= 13;
	static const char LSET			= 14;

	static const char BEGIN  		= 7;
	static const char END    		= 8;

	static const char HDEL_SET 		= 20;
	static const char HCLEAR 		= 21;
	static const char ZCLEAR 		= 22;
	static const char ZDEL_SET 		= 23;
	static const char LCLEAR 		= 24;

    static const char ZSET_TTL 		= 25;
    static const char ZDEL_TTL 		= 26;

	static const char CLT_CREATE 	= 30;
	static const char CLT_INSERT 	= 31;
	static const char CLT_DEL 		= 32;
	static const char CLT_CLEAR 	= 33;
	static const char CLT_DROP 		= 34;
	static const char CLT_RECREATE	= 35;
};

class ObjectErr {
public:
	static const int object_normal 		= 1;
	static const int object_expire 		= 1 << 1;
	static const int object_typeinvalid = 1 << 2;
};

typedef struct TagMetalHeader {
	int64_t version;
	int64_t expire;
	uint32_t refcount;
	uint8_t datatype;
	uint8_t reserve[3];
} TMH;

typedef struct TagValueHeader {
	int64_t expire;
	uint8_t reserve[8];
	//reserve[0]第一位用于标识这个data需要不需要拷贝
	//reserve[1]第二位用于标识这个data的版本的长度，拼接在TVH后
} TVH;

typedef struct TagValueVersion {
	TVH tag_header = {0};
	int64_t version = 0;
} TVV;


enum ColumnType : char {
	kColNone 		= 0x0,
	kColPrimary 	= 0x1,//主键索引，会将信息写入此键value中
	kColIndex 		= 0x2,//索引字段
	kColStat 		= 0x3,//统计字段
	kColUniqueIndex	= 0x4
};

//聚集更新命令操作
enum UpdateCmd : char {
	kOptCmdNone 	= 0x0,
	kOptCmdInsert 	= 0x1,
	kOptCmdSet 		= 0x2,
	kOptCmdUnset 	= 0x3,
	kOptCmdRename 	= 0x4,
	kOptCmdPullAll 	= 0x6,
	kOptCmdPull 	= 0x7,
	kOptCmdPush		= 0x8,
	kOptCmdAddToSet	= 0x9
};

//聚集过滤标识
enum FilterFlag : char {
	kFilterTypeNone 	= 0x0,
	kFilterTypeIn		= 0x1,
	kFilterTypeAll		= 0x2
};

enum IndexType : char {
	kIndexNormal = 0x0,	//普通索引字段
	kIndexUnique = 0x1  //唯一索引字段
};

//附加条件
struct Condition {
	bool upsert 	= false;
	bool slave_upsert = false;
	bool multi 		= false;
	bool remove 	= false;
	std::string start;
	std::string end;
	int32_t cursor 	= 0;
	int32_t desc 	= 1;
	int32_t limit 	= 200000000;
	bool background = false;
	bool refresh_stat = false;
	bool rm_idx       = true;
	bool upsert_success = false;
	bool force = false;
	bool has_ranged_start = false;
	bool has_ranged_end = false;

};

struct Column {
	std::string name;
	std::string value;
	int hash_bucket;
	UpdateCmd cmd_type_ 	= UpdateCmd::kOptCmdNone;
	FilterFlag filter_type_ = FilterFlag::kFilterTypeNone;

	Column(const std::string &name, const std::string &value, int hash_bucket) {
		this->name = name;
		this->value = value;
		this->hash_bucket = hash_bucket;
	}
	Column(const std::string &name, const std::string &value, int hash_bucket, UpdateCmd type) {
		this->name = name;
		this->value = value;
		this->cmd_type_ = type;
		this->hash_bucket = hash_bucket;
	}
	Column(const std::string &name, const std::string &value, int hash_bucket, 
								UpdateCmd type, FilterFlag flag) {
		this->name = name;
		this->value = value;
		this->cmd_type_ = type;
		this->filter_type_ = flag;
		this->hash_bucket = hash_bucket;
	}

	bool operator<(const Column& b) const {
		return this->name < b.name
		       || (this->name == b.name && this->value < b.value);
	}
};


#endif