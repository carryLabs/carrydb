#ifndef SSDB_MONITOR_THREAD_H_
#define SSDB_MONITOR_THREAD_H_

#include <inttypes.h>
#include <string>
#include <set>
#include "../util/bytes.h"
#include "../util/log.h"
#include "../util/slots.h"
#include "../net/ae.h"
#include "ssdb.h"

#define kNameSpaceSize  8

//http://zh-google-styleguide.readthedocs.io/en/latest/google-cpp-styleguide/naming/
typedef struct NameSpaceStruct{
	std::map<char, uint64_t> keys;
	uint64_t maxkeys;
}NSS;

class MonitorThread{
public:
	MonitorThread(SSDB *data, SSDB *meta);
	~MonitorThread();
	void incrNsRefCount(char type, const Bytes &name, int incr);
	inline std::string listName(){return list_name;}

	std::string getKeySize(const Bytes &name);
	void getNameSpaces(std::vector<std::string> &list);
	void DataInfo(std::string &info);
	void setDbSizeCron(int second);
	int dbSizeCron();
public:
	void recodeDataToDisk();
	void loadDataFromDisk();
	bool run_with_period(int million);
	static void* thread_func(void *arg);
	std::string getNamePrefix(const Bytes &name);
	void stop_thread();
	void start_thread();

	std::string typeToKeyflag(char type);
	char keyflagToType(const std::string &flag);

	std::unordered_map<std::string, NSS> namespace_dict_;

	volatile bool thread_quit;
	int backup_cronloops_;
	std::string list_name;
	SSDB *data_db_;//用于获取数据库信息y
	SSDB *meta_db_;//用于信息落地
	aeEventLoop *serverEventloop_;
	std::string persistence_cache_;

	int dbsize_cron_;
};
int backupsCron(struct aeEventLoop *eventLoop, long long id, void *clientData);
int filesize(struct aeEventLoop *eventLoop, long long id, void *clientData);
#endif