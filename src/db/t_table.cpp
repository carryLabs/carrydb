#include "t_table.h"

static inline int CreateCltConfig(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, bson_t **data, char log_type);

static inline int UpdateElements(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, char log_type);
static inline int UpdateElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd,
                                const std::string &pkey, const Bytes &datavalue, char log_type);
static inline int InsertElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, char log_type);

static inline int GetElements(std::vector<std::string> *list, SSDBImpl *ssdb, CltCmdInfo *ptr_cmd);
static inline int GetElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const Bytes &key, std::string *val, bool filter = false);

static inline int DeleteElements(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, char log_type);
static inline int DeleteElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const Bytes &key, const std::string &value, char log_type);
static inline int delete_one(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd,
                             const Bytes &pkey, bson_t *oldval, char log_type, bool decr = true);

static inline int IncrCltSize(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, int64_t incr);
static inline int ReBuildIndex(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd,
                               const std::string &pkey, bson_t *oldvalue, bson_t *newvalue, char log_type);

static inline int CheckUniqueIndexValid(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const char *first_exclude, ...);
static inline int CreateNewPrimaryKey(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const std::string &oldkey,
                                      const std::string &newkey, bson_t *store_bson, char log_type);
static inline int ScanIndexForward(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, std::vector<std::string> *list, char log_type);
static inline int ScanPrimaryForward(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, std::vector<std::string> *list, char log_type);

static int DeleteElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const Bytes &key, const std::string &value, char log_type) {
	bson_t *store_bson = parser_bson_from_data(value.data(), value.size());
	if (!store_bson) {
		return -1;
	}

	delete_one(ssdb, ptr_cmd, key, store_bson, log_type);
	bson_destroy(store_bson);
	return 1;
}

int SSDBImpl::clt_create(const Bytes &name, bson_t **data, char log_type) {
	TMH metainfo = {0};
	int ret = VerifyStructState(&metainfo, name, DataType::TABLE);

	//变更表结构需要重写metakey,重设版本号,不支持聚集过期设置
	if (ret == 1) {
		return 0;//变更索引记录需要使用别的命令
	} else if (ret != 0) {
		return ret;
	}
	Transaction trans(binlogs);
	ret = CreateCltConfig(this, metainfo, name, data, log_type);
	if (ret >= 0) {
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::clt_recreate(const Bytes &name, bson_t **data, Condition &opts, char log_type) {
	if (!recreate_table_config(data)) {
		return CmdStatus::kCltBsonError;
	}
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, nullptr, nullptr, &opts);
	std::string metakey = EncodeMetaKey(name);
	print_bson(*data);
	if (ret == 1) {
		//正在重建中不能再使用重建命令.加入force是防止阻塞创建过程中，程序崩溃，符号还存在以后不能写入了。
		if (cmd_info.metainfo.reserve[0] == ValueType::kReBuild && !opts.force) {
			return CmdStatus::kCltReBuilding;
		}

		{
			//重建前先对原聚集写入标识位，如果是background = true，将不写入，而是后台线程。
			Transaction trans(binlogs);
			cmd_info.metainfo.reserve[0] = ValueType::kReBuild;
			print_bson(cmd_info.option_data_);
			binlogs->Put(metakey, EncodeCltMetalVal(cmd_info.metainfo, cmd_info.option_data_));
			leveldb::Status s = binlogs->commit();
			if (!s.ok()) {
				return -1;
			}
		}

		{
			TMH old_metainfo = cmd_info.metainfo;
			CltKeyIterator *it = this->scan_primary_keys(nullptr, &cmd_info);
			initVersion(cmd_info.metainfo, 0, DataType::TABLE);
			log_debug("------%s----old ver:%lld, new ver:%lld", name.data(), old_metainfo.version, cmd_info.metainfo.version);

			cmd_info.metainfo.refcount = 1;
			cmd_info.metainfo.reserve[0] = ValueType::kNone;
			cmd_info.option_data_ = *data;
			it->return_val(false);
			while (it->next()) {
				Transaction trans(binlogs);
				bson_t *store_bson = DecodeDataBson(it->rawvalue());
				if (!store_bson) {
					continue;
				}
				cmd_info.InitCltOption(cmd_info.option_data_, nullptr, store_bson);
				int ret = InsertElement(this, &cmd_info, BinlogType::NONE);
				cmd_info.IncrRefcount(ret);
				bson_clear(&store_bson);
				if (ret < 0) {
					continue;
				}
				leveldb::Status s = binlogs->commit();
				if (!s.ok()) {
					return -1;
				}
			}
			delete it;

			{
				Transaction trans(binlogs);
				//先删除旧的聚集，因为是原子操作，必须是新数据也能写入才能真的删除。
				ret = this->hset_rubbish_queue(releasehandler->listName(),
				                               encode_release_key(old_metainfo.datatype, name), str(old_metainfo.version));
				binlogs->Put(metakey, EncodeCltMetalVal(cmd_info.metainfo, cmd_info.option_data_));
				binlogs->add_log(log_type, BinlogCommand::CLT_RECREATE, metakey);

				leveldb::Status s = binlogs->commit();
				if (!s.ok()) {
					return -1;
				}
				releasehandler->activeQueue(old_metainfo.version);
			}
			return 1;
		}
	} else if (ret == CmdStatus::kCltNotFoundConfig) {
		return clt_create(name, data, log_type);
	} else {
		return ret;
	}
}

int SSDBImpl::clt_isbuilding(const Bytes &name) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, nullptr, nullptr, nullptr);
	std::string metakey = EncodeMetaKey(name);

	return cmd_info.metainfo.reserve[0] == ValueType::kReBuild;
}

int SSDBImpl::InitCltCmdInfo(CltCmdInfo *ptr_cmd, const Bytes &name, bson_t *query, bson_t *update, Condition *opts) {
	TMH metainfo = {0};
	std::string meta_str;
	int ret = VerifyStructState(&metainfo, name, DataType::TABLE, &meta_str);
	CHECK_TABLE_EXIST(ret);
	ptr_cmd->metainfo = metainfo;
	ptr_cmd->name = name;

	if (opts) {
		ptr_cmd->opts = *opts;
	}

	std::string str;
	if (DecodeCltMetaVal(meta_str, &str) == -1) {
		return -1;
	}

	bson_t *bson_config = parser_bson_from_data(str.data(), str.size());
	if (!bson_config) {
		return -1;
	}

	if (ptr_cmd->InitCltOption(bson_config, query, update) == -1) {
		return -1;
	}

	return 1;
}

int SSDBImpl::clt_insert(const Bytes &name, bson_t *data, Condition &opts, char log_type) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, nullptr, data, &opts);
	CHECK_TABLE_EXIST(ret);
	CHECK_TABLE_ISREBUILDING(cmd_info.metainfo.reserve[0]);
	Transaction trans(binlogs);

	if (!cmd_info.UpdateHasFullKeys()) {
		return CmdStatus::kCltNotFullKey;
	}

	ret = InsertElement(this, &cmd_info, log_type);
	if (ret >= 0) {
		if (ret > 0 && !cmd_info.opts.upsert_success) {
			if (IncrCltSize(this, &cmd_info, ret) == -1) {
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::clt_stat(const Bytes &name, std::string *val) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, nullptr, nullptr, nullptr);
	CHECK_TABLE_EXIST(ret);

	bson_t *pd = bson_new();
	bson_init(pd);

	const std::vector<Column> &v = cmd_info.GetCltOptionCols(ColumnType::kColStat);
	for (auto it = v.cbegin(); it != v.cend(); ++it) {
		bson_iter_t iter;
		if (bson_iter_init_find(&iter, cmd_info.option_data_, statName(it->name).c_str())) {
			switch (bson_iter_type(&iter)) {
			case BSON_TYPE_DOUBLE:
			case BSON_TYPE_INT32:
			case BSON_TYPE_INT64:
				BSON_APPEND_INT64(pd, it->name.c_str(), bson_iter_as_int64 (&iter));
				break;
			default:
				log_error("stat type error: %s:%d", it->name.c_str(), bson_iter_type(&iter));
				return 0;
			}
		}
	}
	BSON_APPEND_INT32(pd, "_refcount", cmd_info.metainfo.refcount - 1);
	val->assign((char *)bson_get_data(pd), pd->len);
	bson_destroy(pd);
	return 1;
}

int SSDBImpl::clt_find(const Bytes &name, bson_t *data, Condition &opts, std::vector<std::string> *list) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, data, nullptr, &opts);
	CHECK_TABLE_EXIST(ret);

	return GetElements(list, this, &cmd_info);
}

int SSDBImpl::clt_update(const Bytes &name, bson_t *query, bson_t *update, Condition &opts, char log_type) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, query, update, &opts);
	CHECK_TABLE_EXIST(ret);
	CHECK_TABLE_ISREBUILDING(cmd_info.metainfo.reserve[0]);

	Transaction trans(binlogs);

	ret = UpdateElements(this, &cmd_info, log_type);
	if (ret >= 0) {
		if (ret > 0) {
			if (IncrCltSize(this, &cmd_info, 0) == -1) {
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			return -1;
		}
	}
	return ret;
}


int SSDBImpl::clt_remove(const Bytes &name, bson_t *data, Condition &opts, char log_type) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, data, nullptr, &opts);
	CHECK_TABLE_EXIST(ret);
	CHECK_TABLE_ISREBUILDING(cmd_info.metainfo.reserve[0]);

	Transaction trans(binlogs);

	ret = DeleteElements(this, &cmd_info, log_type);
	if (ret >= 0) {
		if (ret > 0) {
			if (IncrCltSize(this, &cmd_info, -ret) == -1) {
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			return -1;
		}
	}
	return ret;
}

int SSDBImpl::clt_get(const Bytes &name, bson_t *data, std::string *val) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, data, nullptr, nullptr);
	CHECK_TABLE_EXIST(ret);

	return GetElement(this, &cmd_info, cmd_info.GetQueryPrimaryKey(), val, true);
}

int SSDBImpl::clt_multi_get(const Bytes &name, const std::vector<bson_t *> &bson_querys, std::vector<std::string> *reult) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, nullptr, nullptr, nullptr);
	CHECK_TABLE_EXIST(ret);
	std::set<std::string> sort_keys;
	std::vector<std::string> keys;
	for (auto it = bson_querys.cbegin(); it != bson_querys.cend(); ++it){
		ret = cmd_info.GetPrimaryKeys(*it);
		if (ret < 0){
			return ret;
		}
		std::string tmp_key = cmd_info.GetQueryPrimaryKey();
		keys.push_back(tmp_key);
		sort_keys.insert(tmp_key);
		log_debug("-----key:%s", tmp_key.data());
	}

	CltKeyIterator *it = this->scan_primary_keys(&sort_keys, &cmd_info);

	for (auto key_it = keys.cbegin(); key_it != keys.cend(); ++key_it) {
		it->seek(EncodeCltPrimaryKey(cmd_info.Name(), *key_it, cmd_info.Version()));
		if (!it->valid() || it->decode() == -1) {
			log_error("seek error:%s", hexmem(it->rawkey().data(), it->rawkey().size()).data());
			reult->push_back("");
			continue;
		}
		//不符合条件直接跳过
		if (it->key() != *key_it) {
			reult->push_back("");
			continue;
		}
		reult->push_back(it->value());
	}
	delete it;
	return 1;
}

int GetElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const Bytes &key, std::string *val, bool filter) {
	std::string datakey = EncodeCltPrimaryKey(ptr_cmd->Name(), key, ptr_cmd->Version());
	std::string dataval;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &dataval);

	if (s.IsNotFound()) {
		return 0;
	}
	if (!s.ok()) {
		return -1;
	}

	//expire check
	if (DecodeDataValue(DataType::TABLE, dataval, val) == -1) {
		return -1;
	}

	if ((filter && ptr_cmd->FilterQueryValue(*val) && ptr_cmd->opts.limit > 0) || !filter) {
		return 1;
	}

	return 0;
}

int SSDBImpl::clt_drop(const Bytes &name, char log_type) {
	TMH metainfo = {0};
	std::string meta_str;
	int ret = VerifyStructState(&metainfo, name, DataType::TABLE, &meta_str);
	CHECK_TABLE_EXIST(ret);
	CHECK_TABLE_ISREBUILDING(metainfo.reserve[0]);

	Transaction trans(binlogs);
	std::string metalkey = EncodeMetaKey(name);

	std::string s_ver = str(metainfo.version);
	std::string s_key = encode_release_key(metainfo.datatype, name);
	ret = this->hset_rubbish_queue(releasehandler->listName(), s_key, s_ver);
	if (ret < 0) {
		return -1;
	}

	binlogs->Delete(metalkey);
	binlogs->add_log(log_type, BinlogCommand::CLT_DROP, metalkey);

	leveldb::Status s = binlogs->commit();
	if (!s.ok()) {
		log_error("clt_drop commit error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::clt_clear(const Bytes &name, char log_type) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, nullptr, nullptr, nullptr);
	CHECK_TABLE_EXIST(ret);
	CHECK_TABLE_ISREBUILDING(cmd_info.metainfo.reserve[0]);

	Transaction trans(binlogs);

	std::string s_ver = str(cmd_info.Version());
	std::string s_key = encode_release_key(cmd_info.metainfo.datatype, name);
	ret = this->hset_rubbish_queue(releasehandler->listName(), s_key, s_ver);
	if (ret < 0) {
		return -1;
	}
	releasehandler->activeQueue(cmd_info.Version());

	initVersion(cmd_info.metainfo, 0, DataType::TABLE);
	cmd_info.metainfo.refcount = 1;
	const std::vector<Column> &v = cmd_info.GetCltOptionCols(ColumnType::kColStat);

	for (auto it = v.cbegin(); it != v.cend(); ++it) {
		bson_iter_t iter;
		if (bson_iter_init_find(&iter, cmd_info.option_data_, statName(it->name).c_str())) {
			bson_iter_overwrite_int64(&iter, 0);
		}
	}

	std::string metalkey = EncodeMetaKey(name);
	binlogs->Put(metalkey, EncodeCltMetalVal(cmd_info.metainfo, cmd_info.option_data_));
	binlogs->add_log(log_type, BinlogCommand::CLT_CLEAR, metalkey);

	leveldb::Status s = binlogs->commit();
	if (!s.ok()) {
		return -1;
	}
	return 1;
}

static int delete_one(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const Bytes &pkey,
                      bson_t *oldval, char log_type, bool decr) {
	std::string prefix = EncodeCltPrefixKey(ptr_cmd->Name(), ptr_cmd->Version());
	std::string datakey = EncodeCltPrimaryKey(prefix, pkey);

	bool tmp = ptr_cmd->opts.remove;
	ptr_cmd->opts.remove = true;

	ssdb->RemoveIndexs(nullptr, ptr_cmd, oldval, prefix, pkey);

	ptr_cmd->opts.remove = tmp;
	ssdb->binlogs->Delete(datakey);
	ssdb->binlogs->add_log(log_type, BinlogCommand::CLT_DEL, datakey);

	if (decr) ptr_cmd->IncrStatSize(oldval, -1);
	return 1;
}

static int DeleteElements(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, char log_type) {
	if (ptr_cmd->QueryHasFullKeys()) {
		std::string value;
		int ret = GetElement(ssdb, ptr_cmd, ptr_cmd->GetQueryPrimaryKey(), &value, true);
		if (ret > 0) {
			ret = DeleteElement(ssdb, ptr_cmd, ptr_cmd->GetQueryPrimaryKey(), value, log_type);
		}
		return ret;
	} else if (ptr_cmd->QueryHasIndexs()) {
		ptr_cmd->forward_ = CltCmdInfo::kDelete;
		return ScanIndexForward(ssdb, ptr_cmd, nullptr, log_type);
	}

	if (bson_empty(ptr_cmd->query_bson_)) {
		return 0;
	}
	ptr_cmd->forward_ = CltCmdInfo::kDelete;
	return ScanPrimaryForward(ssdb, ptr_cmd, nullptr, log_type);
}

int SSDBImpl::scan_index_infos(std::set<std::string> *keys, CltCmdInfo *ptr_cmd, const Column &index) {

	std::string key_start = EncodeCltIndexPrefix(ptr_cmd->Name(), ptr_cmd->Version(), index);
	//为了联合主键需要
	if (!ptr_cmd->opts.has_ranged_start && !ptr_cmd->opts.has_ranged_end) {
		key_start.append(ptr_cmd->GetQueryPrimaryKey());
	}

	std::string key_end = key_start;

	if (ptr_cmd->opts.desc > 0) {
		if (ptr_cmd->opts.has_ranged_start) {
			key_start.append(ptr_cmd->opts.start);
		}

		//需要key_end.append(1, 255)的原因是因为，iterator判断是否越界做了不空判断
		//!end.empty() && it->key().compare(end) < 0，而end需要不为空的，因为是name下的数据
		if (ptr_cmd->opts.has_ranged_end && !ptr_cmd->opts.end.empty()) {
			key_end.append(ptr_cmd->opts.end);
		} else {
			key_end.append(1, 255);
		}
	} else {
		if (ptr_cmd->opts.has_ranged_start && !ptr_cmd->opts.start.empty()) {
			key_start.append(ptr_cmd->opts.start);
		} else {
			key_start.append(1, 255);
		}
		if (ptr_cmd->opts.has_ranged_end) {
			key_end.append(ptr_cmd->opts.end);
		}
	}
	log_debug("scan index:%s, %s", hexmem(key_start.data(), key_start.size()).data(), hexmem(key_end.data(), key_end.size()).data());

	CltIdxIterator *it = IteratorFactory::clt_idx_iterator(this, ptr_cmd->Name(), index.value, ptr_cmd->Version(),
	                     key_start, key_end, 20000000, ptr_cmd->opts.desc);
	it->seek(key_start);
	if (ptr_cmd->opts.desc > 0) {
		if (it->valid() && ptr_cmd->opts.has_ranged_start && it->rawkey() == key_start) {
			it->next();
		}
	} else {
		if (!it->valid()) {
			log_error("seekToLast invalid");
			it->seekToLast();
		} else {
			it->prev();
		}
	}
	//it->skip(ptr_cmd->opts.cursor);
	//query_bson_不仅有index信息，可能还包含其他的过滤条件
	while (it->next()) {
		keys->insert(it->value());
	}

	delete it;
	return 1;
}

CltKeyIterator *SSDBImpl::scan_primary_keys(const std::set<std::string> *keys, CltCmdInfo *ptr_cmd) {
	std::string key_start = EncodeCltPrimaryKey(ptr_cmd->Name(), Bytes(), ptr_cmd->Version());

	std::string key_end = key_start;

	if (keys && !keys->empty()) {
		log_debug("start:%s end:%s", hexmem((*keys->begin()).data(), (*keys->begin()).size()).data()
		          , hexmem((*keys->rbegin()).data(), (*keys->rbegin()).size()).data());
		if (ptr_cmd->opts.desc > 0) {
			key_start.append(*keys->begin());
			key_end.append(*keys->rbegin());
		} else {
			key_start.append(*keys->rbegin());
			key_end.append(*keys->begin());
		}
	} else {
		if (ptr_cmd->opts.desc > 0) {
			if (ptr_cmd->opts.has_ranged_start) {
				key_start.append(ptr_cmd->opts.start);
			}

			//需要key_end.append(1, 255)的原因是因为，iterator判断是否越界做了不空判断
			//!end.empty() && it->key().compare(end) < 0，而end需要不为空的，因为是name下的数据
			if (ptr_cmd->opts.has_ranged_end && !ptr_cmd->opts.end.empty()) {
				key_end.append(ptr_cmd->opts.end);
			} else {
				key_end.append(1, 255);
			}
		} else {
			if (ptr_cmd->opts.has_ranged_start && !ptr_cmd->opts.start.empty()) {
				key_start.append(ptr_cmd->opts.start);
			} else {
				key_start.append(1, 255);
			}
			if (ptr_cmd->opts.has_ranged_end) {
				key_end.append(ptr_cmd->opts.end);
			}
		}

		log_debug("start:%s", hexmem(key_start.data(), key_start.size()).data());
	}
	CltKeyIterator *it = IteratorFactory::clt_key_iterator(this, ptr_cmd->Name(), ptr_cmd->Version(),
	                     key_start, key_end, 20000000, ptr_cmd->opts.desc);
	it->seek(key_start);
	if (ptr_cmd->opts.desc > 0) {
		if (!keys && it->valid() && ptr_cmd->opts.has_ranged_start && it->rawkey() == key_start) {
			it->next();
		}
	} else {
		if (!it->valid()) {
			it->seekToLast();
		} else if (!keys || keys->empty()) {
			it->prev();
		}
	}

	return it;
}

static int CreateCltConfig(SSDBImpl *ssdb, TMH &metainfo, const Bytes &name, bson_t **data, char log_type) {
	if (name.empty()) {
		log_error("empty name or key!");
		return -1;
	}
	if (name.size() > SSDB_KEY_LEN_MAX) {
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}

	//检查参数合法性,并添加统计字段
	if (!recreate_table_config(data)) {
		return CmdStatus::kCltBsonError;
	}

	initVersion(metainfo, metainfo.refcount, DataType::TABLE);
	metainfo.refcount++;
	//为0和1才在函数内部处理
	ssdb->incrSlotsRefCount(name, metainfo, metainfo.refcount);

	std::string metalkey = EncodeMetaKey(name);
	ssdb->binlogs->Put(metalkey, EncodeCltMetalVal(metainfo, *data));
	ssdb->binlogs->add_log(log_type, BinlogCommand::CLT_CREATE, metalkey);
	return 1;
}

static int InsertElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, char log_type) {
	std::string suf_key = ptr_cmd->GetUpdatePrimaryKey();
	std::string datakey = EncodeCltPrimaryKey(ptr_cmd->Name(), suf_key, ptr_cmd->Version());
	std::string dataval;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &dataval);
	if (s.IsNotFound()) {
		//唯一性索引检测
		int ret = CheckUniqueIndexValid(ssdb, ptr_cmd, suf_key.data(), nullptr);
		if (ret < 0) {
			return ret;
		}

		const Columns &indexs = ptr_cmd->GetUpdateIndexCols();
		for (int i = 0; i < indexs.size(); ++i) {
			std::string buf = EncodeCltIndexKey(ptr_cmd->Name(), suf_key, indexs[i], ptr_cmd->Version());
			log_debug("save normalindex %s:%s:%s", indexs[i].name.data(),
			          indexs[i].value.data(), hexmem(buf.data(), buf.size()).data());
			ssdb->binlogs->Put(buf, "");
		}

		const Columns &unique_indexs = ptr_cmd->GetUpdateUniqueIndexCols();
		for (int i = 0; i < unique_indexs.size(); ++i) {
			std::string buf = EncodeCltUniqueIndexKey(ptr_cmd->Name(), unique_indexs[i], ptr_cmd->Version());
			log_debug("save unique index %s:%s:%s", unique_indexs[i].name.data(), unique_indexs[i].value.data(),
			          hexmem(buf.data(), buf.size()).data());
			ssdb->binlogs->Put(buf, EncodeDataValue(suf_key, ValueType::kNoCopy));
		}

		ssdb->binlogs->add_log(log_type, BinlogCommand::CLT_INSERT, datakey);
		ptr_cmd->IncrStatSize(ptr_cmd->update_bson_, 1);

		log_debug("insert new data key:%s", hexmem(datakey.data(), datakey.size()).data());
		ssdb->binlogs->Put(datakey, EncodeCltPrimaryVal(ptr_cmd->update_bson_));
		return 1;
	} else {
		if (!s.ok()) {
			return -1;
		}

		//upsert表示是clt_upsert更新命令，slave_upsert表示是slave同步更新命令，两者区别是，
		//upsert是更新，slave_upsert是覆盖更新
		if (!ptr_cmd->opts.upsert && !ptr_cmd->opts.slave_upsert) { //不存在添加upsert
			return 0;
		}
		ptr_cmd->opts.upsert_success = true;

		std::string newvalue = EncodeCltPrimaryVal(ptr_cmd->update_bson_);
		int ret = 0;
		if (newvalue != dataval) {
			if (ptr_cmd->opts.upsert) {
				//里面又进行了一次get,todo修改
				return UpdateElement(ssdb, ptr_cmd, suf_key, dataval, log_type);
			} else {
				bson_t *store_bson = DecodeDataBson(dataval);
				if (!store_bson) {
					return -1;
				}

				std::set<Column> filters;

				ret = ssdb->RemoveIndexs(&filters, ptr_cmd, store_bson,
				                         EncodeCltPrefixKey(ptr_cmd->Name(), ptr_cmd->Version()), suf_key);
				bson_destroy(store_bson);
				if (ret < 0) {
					return ret;
				}

				const Columns &idx = ptr_cmd->GetUpdateIndexCols();
				for (int i = 0; i < idx.size(); ++i) {
					if (filters.find(idx[i]) == filters.end()) { //找不到表示是新增,找到了不处理,因为是更新,不重复操作
						std::string buf = EncodeCltIndexKey(ptr_cmd->Name(), suf_key, idx[i], ptr_cmd->Version());
						log_debug("dont find, and add to index: save index %d:%s:%s",
						          idx[i].hash_bucket, idx[i].value.data(), hexmem(buf.data(), buf.size()).data());

						ssdb->binlogs->Put(buf, "");
					} else {
						log_debug("already exist %s, dont not need add", idx[i].value.data());
					}
				}

				const Columns &indexs = ptr_cmd->GetUpdateUniqueIndexCols();
				for (int i = 0; i < indexs.size(); ++i) {
					if (filters.find(idx[i]) == filters.end()) {
						std::string buf = EncodeCltUniqueIndexKey(ptr_cmd->Name(), indexs[i], ptr_cmd->Version());
						log_debug("save unique index %s:%s:%s", indexs[i].name.data(), indexs[i].value.data());
						ssdb->binlogs->Put(buf, EncodeDataValue(suf_key, ValueType::kNoCopy));
					} else {
						log_debug("already exist unique %s, dont not need add", indexs[i].value.data());
					}
				}

				ssdb->binlogs->Put(datakey, newvalue);
				ssdb->binlogs->add_log(log_type, BinlogCommand::CLT_INSERT, datakey);
				return 1;
			}
		}

		return ret;
	}
}

//查询到元素集，各个进行更新
static int UpdateElements(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, char log_type) {
	if (ptr_cmd->QueryHasFullKeys()) {//query 语法有完整的主键信息
		std::string suf_key = ptr_cmd->GetQueryPrimaryKey();//datakey的后缀
		std::string datakey = EncodeCltPrimaryKey(ptr_cmd->Name(), suf_key, ptr_cmd->Version());
		std::string dataval;
		leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), datakey, &dataval);
		if (s.IsNotFound()) {
			if (ptr_cmd->opts.upsert) { //不存在添加upsert
				return InsertElement(ssdb, ptr_cmd, log_type);
			}
			return 0;
		} else {
			if (!s.ok()) {
				return -1;
			}
			return UpdateElement(ssdb, ptr_cmd, suf_key, dataval, log_type);
		}
	} else if (ptr_cmd->QueryHasIndexs()) {
		//根据索引query时候，不能更新最后一个主键，因为此时更新主键会删除旧的值，设置新值，
		//但是索引找到的列表可能多个，此时，新值不确定哪个
		if (ptr_cmd->IsUpdatePkeysValid()) {
			return CmdStatus::kCltUpdateFaild;
		}

		ptr_cmd->forward_ = CltCmdInfo::kUpdate;
		return ScanIndexForward(ssdb, ptr_cmd, nullptr, log_type);
	} 
	if (bson_empty(ptr_cmd->query_bson_)) {
		return 0;
	}
	ptr_cmd->forward_ = CltCmdInfo::kUpdate;
	return ScanPrimaryForward(ssdb, ptr_cmd, nullptr, log_type);
}

// 更新单个元素
// 如果需要更新唯一索引，这种情况判断
// 情况1：不需要更新主键，需要判断新索引对应的主键是否就是查询的主键或者新索引不存在，才能更新。
// 情况2：需要更新主键,如果新的唯一索引对应的主键就是需要更新的新的主键，表示唯一索引不冲突，
// 				   如果新的唯一索引不存在，可以更新，
// 				   如果新的唯一索引对应的主键就是原来的主键，可以更新。
// 		新主键对应的唯一索引是否就是新的索引或者新索引不存在，是的话才能更新，否则表示唯一索引冲突，禁止更新。
static int UpdateElement(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd,
                         const std::string &suf_key, const Bytes &datavalue, char log_type) {
	bson_t *store_bson = DecodeDataBson(datavalue);
	if (!store_bson) {
		return -1;
	}
	//每次都得初始化，因为共享同一个 ptr_cmd
	ptr_cmd->need_update_key_ = false;
	ptr_cmd->opts.rm_idx = true;
	int ret = 0;

	//如果需要更新主键
	if (ptr_cmd->NeedUpdateKey()) {
		//生成一个新主键，并且返回新主键与原主键是否一样
		std::string newkey = ptr_cmd->CreateNewKey(store_bson, &(ptr_cmd->need_update_key_));

		//需要更新主键（表示update_bson_ 中的主键与已经落地的主键不一样）
		if (ptr_cmd->need_update_key_) {
			ret = CreateNewPrimaryKey(ssdb, ptr_cmd, suf_key, newkey, store_bson, log_type);
			goto CleanUp;
		}
	}
	log_debug("dont need up pkey:%s", suf_key.data());
	//检查更新唯一索引是否合法，不合法的条件表示唯一索引存在了且对应主键是别的
	ret = CheckUniqueIndexValid(ssdb, ptr_cmd, suf_key.c_str(), nullptr);
	if (ret < 0) {
		log_debug("unique index conflict");
		goto CleanUp;
	}
	{
		std::string data_key = EncodeCltPrimaryKey(ptr_cmd->Name(), suf_key, ptr_cmd->Version());
		bson_t *dst = ptr_cmd->CreateNewStoreData(store_bson);
		ssdb->binlogs->add_log(log_type, BinlogCommand::CLT_INSERT, data_key);
		ssdb->binlogs->Put(data_key, EncodeCltPrimaryVal(dst));
		bson_destroy(dst);

		ret = ReBuildIndex(ssdb, ptr_cmd, suf_key, store_bson, store_bson, log_type);
	}

CleanUp:
	bson_destroy(store_bson);
	return ret;
}

//更新primary key,需要删除旧的索引等数据，新建新的数据
static int CreateNewPrimaryKey(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const std::string &oldkey,
                               const std::string &newkey, bson_t *store_bson, char log_type) {
	int ret = CheckUniqueIndexValid(ssdb, ptr_cmd, newkey.c_str(), oldkey.c_str(), nullptr);
	if (ret < 0) {
		return ret;
	}

	//主键值被更新，需要将旧值索引信息删除、主键信息key,value删除(false 表示不进行空间统计增减)
	delete_one(ssdb, ptr_cmd, oldkey, store_bson, log_type, false);
	log_debug("need_update_key:%s", hexmem(newkey.data(), newkey.size()).data());

	//写入的主键key-value
	std::string data_key = EncodeCltPrimaryKey(ptr_cmd->Name(), newkey, ptr_cmd->Version());
	bson_t *dst = ptr_cmd->CreateNewStoreData(store_bson);
	ssdb->binlogs->Put(data_key, EncodeCltPrimaryVal(dst));
	ssdb->binlogs->add_log(log_type, BinlogCommand::CLT_INSERT, data_key);
	bson_destroy(dst);

	//查看更新主键值的新值是否存在
	std::string tmp;
	leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), data_key, &tmp);

	if (s.IsNotFound()) {
		log_debug("not find new primary key");
		//表示不用重新删除新值的索引，因为是新值不存在，索引肯定也不存在，rm_idx是记录这一信息的标识位
		ptr_cmd->opts.rm_idx = false;
		ret = ReBuildIndex(ssdb, ptr_cmd, newkey, store_bson, store_bson, log_type);
		return ret;
	} else {
		log_debug("find new primary key");
		if (!s.ok()) {
			bson_destroy(store_bson);
			return -1;
		}

		bson_t *old_bson = DecodeDataBson(tmp);
		if (!old_bson) {
			return -1;
		}
		//因为旧值被覆盖（删除），所以需要更改空间统计与主键个数
		ptr_cmd->DecrRefcount();
		ptr_cmd->IncrStatSize(old_bson, -1);

		ret = ReBuildIndex(ssdb, ptr_cmd, newkey, old_bson, store_bson, log_type);
		bson_destroy(old_bson);
		return ret;
	}
}

static int CheckUniqueIndexValid(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, const char *first_exclude, ...) {
	int ret = 1;
	va_list args;
	va_list args_copy;

	va_start(args, first_exclude);
	const char *exclude = first_exclude;

	const Columns &uniques = ptr_cmd->GetUpdateUniqueIndexCols();
	for (int i = 0; i < uniques.size(); ++i) {
		std::string unique_key = EncodeCltUniqueIndexKey(ptr_cmd->Name(), uniques[i], ptr_cmd->Version());
		std::string val;
		leveldb::Status s = ssdb->ldb->Get(leveldb::ReadOptions(), unique_key, &val);
		if (!s.IsNotFound() && s.ok()) {
			std::string old_key;
			DecodeDataValue(DataType::TABLE, val, &old_key);
			va_copy (args_copy, args);

			bool valid = false;
			log_debug("check unique:%s:%s", uniques[i].name.data(), uniques[i].value.data());
			do {
				log_debug(">>>>>>>>>>>>>>>>>>>>>>>>>%s:%s", old_key.data(), exclude);
				if (!strcasecmp (old_key.data(), exclude)) {
					valid = true;
					break;
				}
			} while ((exclude = va_arg (args_copy, const char *)));

			va_end (args_copy);

			if (!valid) {
				ret = kCltUniqConflict;
				break;
			}
		}
	}
	va_end (args);
	return ret;
}

static int ReBuildIndex(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd,
                        const std::string &suf_key, bson_t *oldvalue, bson_t *newvalue, char log_type) {
	std::set<Column> filters;

	//先更新旧的索引，此索引可能要被删除，或者先删除再写入，这种情况会出现在filters里面
	ssdb->RemoveIndexs(&filters, ptr_cmd, oldvalue, EncodeCltPrefixKey(ptr_cmd->Name(), ptr_cmd->Version()), suf_key);

	std::set<Column> indexs;
	ptr_cmd->GetUpdateIndexCols(&indexs, filters, ColumnType::kColIndex);

	for (auto it = indexs.cbegin(); it != indexs.cend(); ++it) {
		std::string buf = EncodeCltIndexKey(ptr_cmd->Name(), suf_key, *it, ptr_cmd->Version());
		log_debug("update save index:%s", hexmem(buf.data(), buf.size()).data());
		ssdb->binlogs->Put(buf, "");
	}
	indexs.clear();
	ptr_cmd->GetUpdateIndexCols(&indexs, filters, ColumnType::kColUniqueIndex);

	for (auto it = indexs.cbegin(); it != indexs.cend(); ++it) {
		std::string buf = EncodeCltUniqueIndexKey(ptr_cmd->Name(), *it, ptr_cmd->Version());
		log_debug("update save unique index:%s", hexmem(buf.data(), buf.size()).data());
		ssdb->binlogs->Put(buf, EncodeDataValue(suf_key, ValueType::kNoCopy));
	}

	//主键更新了，需要重建索引
	if (ptr_cmd->need_update_key_) {
		log_debug("update new primary");
		const Columns &opidxs = ptr_cmd->GetCltOptionCols(ColumnType::kColIndex);
		for (auto it = opidxs.cbegin(); it != opidxs.cend(); ++it) {

			//已经记录的update字段就不需要再更新了.
			if (!ptr_cmd->IsUpdateExistIndexField(it->name, ColumnType::kColIndex)) {
				std::vector<std::string> list;
				//这里表示新的pkey,需要建索引，从已经存储的数据中寻找值
				if (getBsonValues(newvalue, it->name, &list) > 0) {
					for (int i = 0; i < list.size(); ++i) {
						//对先前已经存在的索引怎么处理
						Column record(it->name, list[i], it->hash_bucket);
						std::string buf = EncodeCltIndexKey(ptr_cmd->Name(), suf_key, record, ptr_cmd->Version());

						log_debug("update save index all :%s", hexmem(buf.data(), buf.size()).data());
						ssdb->binlogs->Put(buf, "");
					}
				}
			}
		}

		const Columns &unique_idxs = ptr_cmd->GetCltOptionCols(ColumnType::kColUniqueIndex);
		for (auto it = unique_idxs.cbegin(); it != unique_idxs.cend(); ++it) {
			if (!ptr_cmd->IsUpdateExistIndexField(it->name, ColumnType::kColUniqueIndex)) {
				std::vector<std::string> list;
				//这里表示新的pkey,需要建索引，从已经存储的数据中寻找值
				if (getBsonValues(newvalue, it->name, &list) > 0) {
					for (int i = 0; i < list.size(); ++i) {
						//对先前已经存在的索引怎么处理
						Column record(it->name, list[i], it->hash_bucket);
						std::string buf = EncodeCltUniqueIndexKey(ptr_cmd->Name(), record, ptr_cmd->Version());

						log_debug("update save index all :%s", hexmem(buf.data(), buf.size()).data());
						ssdb->binlogs->Put(buf, EncodeDataValue(suf_key, ValueType::kNoCopy));
					}
				}
			}
		}
	}
	return 1;
}

static int IncrCltSize(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, int64_t incr) {
	std::string metalkey = EncodeMetaKey(ptr_cmd->Name());

	ptr_cmd->IncrRefcount(incr);

	//为0和1才在函数内部处理,一张表只有一个slotinfo信息，类似hash
	ssdb->incrSlotsRefCount(ptr_cmd->Name(), ptr_cmd->metainfo, incr);

	if (ptr_cmd->Refcount() != -1) {
		ssdb->binlogs->Put(metalkey, EncodeCltMetalVal(ptr_cmd->metainfo, ptr_cmd->option_data_));
	}
	return 1;
}

static inline
int GetElements(std::vector<std::string> *list, SSDBImpl *ssdb, CltCmdInfo *ptr_cmd) {
	//如果包含完整的主键信息,直接get操作
	if (ptr_cmd->QueryHasFullKeys()) {
		std::string val;
		int ret = GetElement(ssdb, ptr_cmd, ptr_cmd->GetQueryPrimaryKey(), &val, true);
		if (ret > 0) {
			list->push_back(val);
		}
		return ret;
	} else if (ptr_cmd->QueryHasIndexs()) {
		ptr_cmd->forward_ = CltCmdInfo::kGet;
		return ScanIndexForward(ssdb, ptr_cmd, list, BinlogType::NONE);
	}

	ptr_cmd->forward_ = CltCmdInfo::kGet;
	return ScanPrimaryForward(ssdb, ptr_cmd, list, BinlogType::NONE);
}

static inline
int ScanIndexForward(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, std::vector<std::string> *list, char log_type) {
	int opcnt = 0;
	int ret = 0;
	std::set<std::string> key_set;
	std::vector<std::string> key_list;
	const std::vector<Column> &idx = ptr_cmd->GetQueryIndexCols();

	for (int i = 0; i < idx.size(); ++i) {
		ssdb->scan_index_infos(&key_set, ptr_cmd, idx[i]);
	}
	if (key_set.empty()) {
		return opcnt;
	}
	CltKeyIterator *it = ssdb->scan_primary_keys(&key_set, ptr_cmd);
	if (ptr_cmd->opts.desc > 0) {
		for (auto key_it = key_set.cbegin(); key_it != key_set.cend(); ++key_it) {
			key_list.push_back(*key_it);
		}
	} else {
		for (auto key_it = key_set.crbegin(); key_it != key_set.crend(); ++key_it) {
			key_list.push_back(*key_it);
		}
	}

	for (auto begin_kit = key_list.cbegin(); begin_kit != key_list.cend(); ++begin_kit) {
		it->seek(EncodeCltPrimaryKey(ptr_cmd->Name(), *begin_kit, ptr_cmd->Version()));
		if (!it->valid() || it->decode() == -1) {
			log_error("seek error:%s", hexmem(it->rawkey().data(), it->rawkey().size()).data());
			continue;
		}
		//不符合条件直接跳过
		if (it->key() != *begin_kit || !ptr_cmd->FilterQueryValue(it->value())) {
			continue;
		}
		//符合条件，在结果集里跳跃游标
		if (ptr_cmd->opts.cursor > 0) {
			ptr_cmd->opts.cursor--;
			continue;
		}
		switch (ptr_cmd->forward_) {
		case CltCmdInfo::kGet: {
			list->push_back(it->value());
			break;
		}
		case CltCmdInfo::kDelete: {
			ret = DeleteElement(ssdb, ptr_cmd, it->key(), it->value(), log_type);
			break;
		}
		case CltCmdInfo::kUpdate: {
			ret = UpdateElement(ssdb, ptr_cmd, it->key(), it->rawvalue(), log_type);
			break;
		}
		}
		if (ret < 0) {
			delete it;
			return ret;
		}
		ptr_cmd->opts.limit--;
		opcnt++;

		if (!ptr_cmd->opts.multi || ptr_cmd->opts.limit <= 0) {
			break;
		}

	}
	delete it;
	return opcnt;
}

static inline
int ScanPrimaryForward(SSDBImpl *ssdb, CltCmdInfo *ptr_cmd, std::vector<std::string> *list, char log_type){
	int opcnt = 0;
	int ret = 0;
	CltKeyIterator *it = ssdb->scan_primary_keys(nullptr, ptr_cmd);
	ptr_cmd->Skip(it);
	while (it->next()) {
		if (ptr_cmd->FilterQueryValue(it->value())) {
			switch (ptr_cmd->forward_) {
				case CltCmdInfo::kGet: {
					list->push_back(it->value());
					break;
				}
				case CltCmdInfo::kDelete: {
					ret = DeleteElement(ssdb, ptr_cmd, it->key(), it->value(), log_type);
					break;
				}
				case CltCmdInfo::kUpdate: {
					ret = UpdateElement(ssdb, ptr_cmd, it->key(), it->rawvalue(), log_type);
					break;
				}
			}
			if (ret < 0) {
				delete it;
				return ret;
			}

			ptr_cmd->opts.limit--;
			opcnt++;

			if (!ptr_cmd->opts.multi || ptr_cmd->opts.limit <= 0){
				break;
			}
		}
	}
	delete it;
	return opcnt;
}


int SSDBImpl::RemoveIndexs(std::set<Column> *keys, CltCmdInfo *ptr_cmd, bson_t *oldval,
                           const Bytes &prefix, const Bytes &pkey) {
	//表示不需要删除索引处理
	if (!ptr_cmd->opts.rm_idx) {
		return 0;
	}
	const Columns &idx_cfg = ptr_cmd->GetCltOptionCols(ColumnType::kColIndex);

	//遍历需要记录的索引项名称
	for (auto it = idx_cfg.begin(); it != idx_cfg.end(); ++it) {
		std::vector<std::string> list;
		if (getBsonValues(oldval, it->name, &list)) {
			for (int i = 0; i < list.size(); ++i) {
				//对先前已经存在的索引怎么处理
				Column record(it->name, list[i], it->hash_bucket);

				//2种情况不需要删除此找到的索引：
				//1：不是clt_remove命令
				//2：例如同一条命令先unset 再set，先删再写
				if (!ptr_cmd->opts.remove && !ptr_cmd->CheckRemoveIndexValid(record, ColumnType::kColIndex)) {
					keys->insert(record);
					continue;
				}

				std::string buf = EncodeCltIndexKey(prefix, pkey, record);
				this->binlogs->Delete(buf);
				log_debug("delete index:%s：%s", record.value.data(), hexmem(buf.data(), buf.size()).data());
			}
		}
	}

	const Columns &unique_idxs = ptr_cmd->GetCltOptionCols(ColumnType::kColUniqueIndex);
	for (auto it = unique_idxs.cbegin(); it != unique_idxs.cend(); ++it) {
		std::vector<std::string> list;
		if (getBsonValues(oldval, it->name, &list)) {
			for (int i = 0; i < list.size(); ++i) {
				//对先前已经存在的索引怎么处理
				Column record(it->name, list[i], it->hash_bucket);

				if (!ptr_cmd->opts.remove && !ptr_cmd->CheckRemoveIndexValid(record, ColumnType::kColUniqueIndex)) {
					keys->insert(record);
					continue;
				}

				std::string buf = EncodeCltUniqueIndexKey(ptr_cmd->Name(), record, ptr_cmd->Version());
				this->binlogs->Delete(buf);
				log_debug("delete unique index:%s：%s", record.value.data(), hexmem(buf.data(), buf.size()).data());
			}
		}
	}
	return 1;
}


int SSDBImpl::sync_clt_remove(const Bytes &name, const Bytes &pkey, char log_type) {
	CltCmdInfo cmd_info;
	int ret = InitCltCmdInfo(&cmd_info, name, nullptr, nullptr, nullptr);
	CHECK_TABLE_EXIST(ret);

	Transaction trans(binlogs);
	cmd_info.opts.remove = true;

	{
		std::string value;
		int ret = GetElement(this, &cmd_info, pkey, &value, true);

		if (ret <= 0) {
			return ret;
		}
		ret = DeleteElement(this, &cmd_info, pkey, value, log_type);
	}
	if (ret >= 0) {
		if (ret > 0) {
			if (IncrCltSize(this, &cmd_info, -ret) == -1) {
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if (!s.ok()) {
			return -1;
		}
	}
	return ret;
}
