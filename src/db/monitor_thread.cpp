#include "monitor_thread.h"

#define CONFIG_DEFAULT_MAX_CLIENTS 10

#define CONFIG_DEFAULT_BACKUPCOUNT 100//1//1000    // 超过个数才备份key个数.优化效率
#define CONFIG_DEFAULT_LOWER_THRESHOLD 300

#define KEY_KEYSIZE "KEY_SIZE"

#define KEY_KSIZE "KSIZE"
#define KEY_LSIZE "LSIZE"
#define KEY_ZSIZE "ZSIZE"
#define KEY_HSIZE "HSIZE"


MonitorThread::MonitorThread(SSDB *data, SSDB *meta) : data_db_(data), meta_db_(meta)
{
	this->data_db_ = data;
	this->meta_db_ = meta;
	this->list_name = MONITOR_INFO_KEY;
	this->backup_cronloops_ = 0;
	this->dbsize_cron_ = 10;
	loadDataFromDisk();
	this->start_thread();
}


MonitorThread::~MonitorThread() {
	this->stop_thread();
	recodeDataToDisk();
	data_db_ = NULL;
	meta_db_ = NULL;
}

void MonitorThread::start_thread() {
	//新建事件循环, 主要用于定时操作, 可以加入描述符监控
	serverEventloop_ = aeCreateEventLoop(CONFIG_DEFAULT_MAX_CLIENTS);

	pthread_t tid_;
	thread_quit = false;
	int err = pthread_create(&tid_, NULL, &MonitorThread::thread_func, this);
	if (err != 0) {
		log_fatal("can't create thread: %s", strerror(err));
		exit(0);
	}

	if (aeCreateTimeEvent(serverEventloop_, 1 * 1000, backupsCron, this, NULL) == AE_ERR) {
		log_error("Can't create the backupsCron time event.");
		exit(1);
	}

	if (aeCreateTimeEvent(serverEventloop_, 1 * 1000, filesize, this, NULL) == AE_ERR) {
		log_error("Can't create the filesize time event.");
		exit(1);
	}
}

void MonitorThread::stop_thread() {
	thread_quit = true;
	for (int i = 0; i < 100; i++) {
		if (!thread_quit) {
			break;
		}
		usleep(10 * 1000);
	}

	//关闭事件循环
	aeDeleteEventLoop(this->serverEventloop_);
}

std::string MonitorThread::getKeySize(const Bytes &name) {
	std::string name_prefix = getNamePrefix(name);
	std::string types;

	if (namespace_dict_.find(name_prefix) != namespace_dict_.cend()) {
		for (auto it = namespace_dict_[name_prefix].keys.cbegin();
						it != namespace_dict_[name_prefix].keys.cend(); ++it) {
			uint64_t size = it->second;
			if (size > 0) {
				switch (it->first) {
				case DataType::KSIZE:
					types += " string:" + str(size);
					break;
				case DataType::LSIZE:
					types += " list:" + str(size);
					break;
				case DataType::ZSIZE:
					types += " zset:" + str(size);
					break;
				case DataType::HSIZE:
					types += " hash:" + str(size);
					break;
				}
			}
		}
		if (!types.empty()) {
			types = types.substr(1);
		}
	}
	return types;
}

void MonitorThread::getNameSpaces(std::vector<std::string> &list){
	for (auto it = namespace_dict_.begin(); it != namespace_dict_.end(); ++it) {
		std::string types;
		for (auto dt_it = it->second.keys.begin(); dt_it != it->second.keys.end(); ++dt_it) {
			uint64_t dt_size = dt_it->second;
			if (dt_size > 0) {
				switch (dt_it->first) {
				case DataType::KSIZE:
					types += " string:" + str(dt_size);
					break;
				case DataType::LSIZE:
					types += " list:" + str(dt_size);
					break;
				case DataType::ZSIZE:
					types += " zset:" + str(dt_size);
					break;
				case DataType::HSIZE:
					types += " hash:" + str(dt_size);
					break;
				}
			}
		}
		if (!types.empty()) {
			types = types.substr(1);
			list.push_back(it->first);
			list.push_back(types);
		}
	}
}

void MonitorThread::DataInfo(std::string &info){
	if (!this->persistence_cache_.empty()){
		info.append(this->persistence_cache_);
	}else{
		log_info("persistence_cache_.empty is empty");
		data_db_->DataInfo(info);				
	}
}

void MonitorThread::setDbSizeCron(int second){
	this->dbsize_cron_ = second;
}

int MonitorThread::dbSizeCron(){
	return dbsize_cron_;
}

void MonitorThread::incrNsRefCount(char type, const Bytes &name, int incr) {
	//the size of benchmark rand key is twelve
	if (name.size() > 12) {
		std::string name_prefix = getNamePrefix(name);
		//必须判断是否前缀有效,因为有的是没有前缀的,判断规则,前缀是%08x
		//插入元素不会使迭代器失效，移除元素仅会使指向已移除元素的迭代器失效。
		//所以最好不加锁的情况下不进行删除，防止遍历error
		if (isxdigit(name_prefix)) {
			if (namespace_dict_[name_prefix].keys[type] != 0 || incr >= 0) {
				namespace_dict_[name_prefix].keys[type] += incr;
				this->backup_cronloops_++;
			}
			
			// if (namespace_dict_[name_prefix].keys[type] <= 0){
			// 	Locking l(&this->mutex);
			// 	auto it = namespace_dict_[name_prefix].keys.find(type);
			// 	namespace_dict_[name_prefix].keys.erase(it);
			// 	this->meta_db_->hdel(this->list_name, name_prefix + typeToKeyflag(type),
			// 								BinlogType::NONE);
			// }
		}
	}
}

std::string MonitorThread::getNamePrefix(const Bytes &name) {
	if (name.size() <= kNameSpaceSize) {
		return name.String();
	}

	std::string ret;
	ret.assign(name.data(), kNameSpaceSize);
	return ret;
}

void* MonitorThread::thread_func(void *arg) {
	MonitorThread *worker = (MonitorThread *)arg;

	while (!worker->thread_quit) {
		if (worker->serverEventloop_->beforesleep != NULL)
			worker->serverEventloop_->beforesleep(worker->serverEventloop_);
		aeProcessEvents(worker->serverEventloop_, AE_ALL_EVENTS);
	}

	worker->thread_quit = false;
	return (void *)NULL;
}

void MonitorThread::loadDataFromDisk() {
	log_info("load MonitorThread record");
	std::vector<std::string> list;
	meta_db_->hgetall(this->list_name, &list);

	for (int i = 0; i < list.size(); i += 2) {
		std::vector<std::string> ls = split(list[i], ":");
		if (ls.size() == 3){
			if (ls.at(1).compare(KEY_KEYSIZE) == 0){
				namespace_dict_[ls.at(0)].keys[keyflagToType(ls.at(2))] = str_to_uint64(list[i + 1]);
			}
		}
	}
}

std::string MonitorThread::typeToKeyflag(char type){
	switch (type) {
		case DataType::KSIZE: {
			return KEY_KSIZE;
		}
		case DataType::HSIZE: {
			return KEY_HSIZE;
		}
		case DataType::LSIZE: {
			return KEY_LSIZE;
		}
		case DataType::ZSIZE: {
			return KEY_ZSIZE;
		}
	}
	return std::string();
}

char MonitorThread::keyflagToType(const std::string &flag){
	if (flag.compare(KEY_LSIZE) == 0){
		return DataType::LSIZE;
	}else if (flag.compare(KEY_HSIZE) == 0){
		return DataType::HSIZE;
	}else if (flag.compare(KEY_ZSIZE) == 0){
		return DataType::ZSIZE;
	}else {
		return DataType::KSIZE;
	}	
}

void MonitorThread::recodeDataToDisk() {
	if (!this->meta_db_) {
		return;
	}
	for (auto it = namespace_dict_.begin(); it != namespace_dict_.end(); ++it) {
		for (auto dt_it = it->second.keys.begin(); dt_it != it->second.keys.end(); ++dt_it) {
			std::string keyflag = typeToKeyflag(dt_it->first);
			if (dt_it->second > 0){
				this->meta_db_->hset(this->list_name, it->first + ":" + KEY_KEYSIZE + ":" + keyflag, 
										str(dt_it->second), BinlogType::NONE);
			}else{
				this->meta_db_->hdel(this->list_name, it->first + + ":" + KEY_KEYSIZE + ":" + keyflag,
			 								BinlogType::NONE);
			}
		}
	}
}

////////////////////////////////////////Worker Proc////////////////////////////////////////////
int backupsCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
	UNUSED(eventLoop);
	UNUSED(id);
	UNUSED(clientData);

	MonitorThread *worker = static_cast<MonitorThread *>(clientData);
	aeTimeEvent *event = aeGetTimerEvent(eventLoop, id);

	if (!worker || !event) {
		log_error("backupsCron error!");
		return CONFIG_DEFAULT_LOWER_THRESHOLD;
	}
	if (worker->backup_cronloops_ >= CONFIG_DEFAULT_BACKUPCOUNT) {	
		worker->recodeDataToDisk();
		worker->backup_cronloops_ = 0;
	}


	return CONFIG_DEFAULT_LOWER_THRESHOLD * 1000;
}

int filesize(struct aeEventLoop *eventLoop, long long id, void *clientData) {
	UNUSED(eventLoop);
	UNUSED(id);
	UNUSED(clientData);
	MonitorThread *worker = static_cast<MonitorThread *>(clientData);
	aeTimeEvent *event = aeGetTimerEvent(eventLoop, id);
	if (!worker || !event) {
		log_error("backupsCron error!");
		return worker->dbsize_cron_ * 1000;
	}
	worker->persistence_cache_.clear();
	worker->data_db_->DataInfo(worker->persistence_cache_);
	return worker->dbsize_cron_ * 1000;
}
