#ifndef SSDB_RUBBISH_RELEASE_H
#define SSDB_RUBBISH_RELEASE_H

#include <string>
#include <pthread.h>
#include <time.h>
#include "ssdb.h"
#include "../util/thread.h"
#include "../util/log.h"
#include "../util/sorted_set.h"
#include "../include.h"

static inline 
std::string encode_release_key(const char type, const Bytes &name){
	std::string buf;

	buf.append(1, DataType::RUBBISH);
	buf.append(1, type);
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	return buf;
}

static inline
int decode_release_key(const Bytes &slice, char *type, std::string *name){
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if(decoder.read_char(&ch) == -1 || ch != DataType::RUBBISH){
		return -1;
	}
	if(decoder.read_char(type) == -1){
		return -1;
	}

	if(decoder.read_8_data(name) == -1){
		return -1;
	}
	
	return 0;
}

class RubbishRelease
{
public:
	Mutex mutex;

	RubbishRelease(SSDB *ssdb);
	~RubbishRelease();
	int push_clear_queue(const Bytes &key, const char log_type, const char log_cmd, bool clearttl); 
	int push_clear_queue(TMH &metainfo, const Bytes &name);
	std::string listName() { return list_name; }
	void activeQueue(int64_t version){
		if(version < queue_first_version){
			queue_first_version = version;
		}
	}
private:
	SSDB *ssdb;
	volatile bool thread_quit;
	std::string list_name;
	int64_t queue_first_version;
	SortedSet fast_queues;
	leveldb::WriteBatch batch;
	void start();
	void stop();
	void expire_loop();
	static void* thread_func(void *arg);
	void load_expiration_keys_from_db(int num);
	int lazy_clear(const Bytes &raw_key, int64_t version);

	std::string encode_data_key_start(const char type, const Bytes &name, int64_t version);
	std::string encode_data_key_end(const char type, const Bytes &name, int64_t version);
};

#endif
