#include "cmdinfo.h"

CltCmdInfo::~CltCmdInfo() {
	if (option_data_) {
		bson_destroy(option_data_);
	}
}

bool CltCmdInfo::IsUpdateExistIndexField(const std::string &name, ColumnType type) {
	if (clt_updates_.find(type) != clt_updates_.end()) {
		Columns &values = clt_updates_[type];
		for (int i = 0; i < values.size(); ++i) {
			if (values[i].name == name) {
				return true;
			}
		}
	}
	return false;
}

void CltCmdInfo::IncrStatSize(bson_t *store, int incr) {
	if (store && clt_configs_.find(ColumnType::kColStat) != clt_configs_.end()) {
		const std::vector<Column> &v = clt_configs_[ColumnType::kColStat];
		for (int i = 0; i < v.size(); ++i) {
	        bson_iter_t iter;
	        bson_iter_t opt_it;
			if (bson_iter_init_find(&iter, store, v[i].name.c_str())) {
				int64_t add = 0;
				const bson_value_t *add_val = NULL;

				switch (bson_iter_type (&iter)) {
				case BSON_TYPE_DOUBLE:
				case BSON_TYPE_INT32:
				case BSON_TYPE_INT64:
					add = bson_iter_as_int64 (&iter);
					break;
				case BSON_TYPE_UTF8:
				    add_val = bson_iter_value(&iter);
					add = str_to_int64(std::string(add_val->value.v_utf8.str, add_val->value.v_utf8.len));
					break;
				default:
					log_info("stat type error %d", bson_iter_type (&iter));
					return;
				}
				if (bson_iter_init_find(&opt_it, this->option_data_, statName(v[i].name).c_str())) {
					log_debug("add value:%lld, now:%lld", add, bson_iter_value(&opt_it)->value.v_int64 + incr * add);
					bson_iter_overwrite_int64(&opt_it, bson_iter_value(&opt_it)->value.v_int64 + incr * add);
				}
			}
		}
	}
}
void CltCmdInfo::IncrStatSize(const char *column_key, int64_t column_value) {
    if ( NULL == column_key || 0 == column_value){
        return;
    }

	if (clt_configs_.find(ColumnType::kColStat) != clt_configs_.end()) {
	    bson_iter_t opt_it;
		const std::vector<Column> &v = clt_configs_[ColumnType::kColStat];
		for (int i = 0; i < v.size(); ++i) {
            if ( strcasecmp(v[i].name.c_str(), column_key) ) {
                continue;
            }
			if (bson_iter_init_find(&opt_it, this->option_data_, statName(v[i].name).c_str())) {
				log_debug("now:%lld", bson_iter_value(&opt_it)->value.v_int64 + column_value);
				bson_iter_overwrite_int64(&opt_it, bson_iter_value(&opt_it)->value.v_int64 + column_value);
			}
		}
	}
}

// store == NULL 时，直接累加update_iter
void CltCmdInfo::UpdateStatSize(bson_t *store, bson_iter_t *update_iter, int incr){
    if ( NULL == update_iter ){
        return;
    }
    // 先验证update_iter的类型,非如下类型，直接返回
    switch (bson_iter_type (update_iter)) {
		case BSON_TYPE_DOUBLE:
		case BSON_TYPE_INT32:
		case BSON_TYPE_INT64:
		case BSON_TYPE_UTF8:
			break;
		default:
			return;
    }

	if (clt_configs_.find(ColumnType::kColStat) != clt_configs_.end()) {
        const char *column_key = bson_iter_key(update_iter);
		const std::vector<Column> &v = clt_configs_[ColumnType::kColStat];
		for (int i = 0; i < v.size(); ++i) {
            if ( strcasecmp(v[i].name.c_str(), column_key) ) {
                continue;
            }
	        int64_t origin_val = 0;
            int64_t update_val = 0;
			const bson_value_t *bson_val = NULL;
            // 检查update_iter的类型
            switch (bson_iter_type (update_iter)) {
				case BSON_TYPE_DOUBLE:
				case BSON_TYPE_INT32:
				case BSON_TYPE_INT64:
					update_val = bson_iter_as_int64 (update_iter);
					break;
				case BSON_TYPE_UTF8:
				    bson_val = bson_iter_value(update_iter);
					update_val = str_to_int64(std::string(bson_val->value.v_utf8.str, bson_val->value.v_utf8.len));
					break;
				default:
					log_info("stat type error %d", bson_iter_type (update_iter));
					return;
            }
            // 读取原值
            bson_iter_t iter;
			if (store && bson_iter_init_find(&iter, store, v[i].name.c_str())) {

			    const bson_value_t *bson_val = NULL;
				switch (bson_iter_type (&iter)) {
				case BSON_TYPE_DOUBLE:
				case BSON_TYPE_INT32:
				case BSON_TYPE_INT64:
					origin_val = bson_iter_as_int64 (&iter);
					break;
				case BSON_TYPE_UTF8:
				    bson_val = bson_iter_value(&iter);
					origin_val = str_to_int64(std::string(bson_val->value.v_utf8.str, bson_val->value.v_utf8.len));
					break;
				default:
					log_info("stat type error %d", bson_iter_type (&iter));
					return;
				}
			}
            // 更新stat统计
	        bson_iter_t opt_it;
			if (bson_iter_init_find(&opt_it, this->option_data_, statName(v[i].name).c_str())) {
				log_debug("origin_val value:%lld, now:%lld", origin_val, bson_iter_value(&opt_it)->value.v_int64 -  origin_val + update_val * incr);
				bson_iter_overwrite_int64(&opt_it, bson_iter_value(&opt_it)->value.v_int64 - origin_val + update_val * incr);
			}
		}
	}
}

bool CltCmdInfo::Skip(CltKeyIterator *it){
	while(opts.cursor > 0){
		if(it->next() == false){
			return false;
		}
		if (FilterQueryValue(it->value())){
			opts.cursor--;
		}
	}
	return true;
}

bool CltCmdInfo::FilterQueryValue(const std::string &value) {
	if (query_bson_) {
		if (bson_empty(query_bson_)){
			return true;
		}
		bson_t *store = parser_bson_from_data(value.data(), value.size(), false);
		if (!store) {
			return false;
		}
		bson_iter_t iter;
		if (bson_iter_init(&iter, query_bson_)) {
			while (bson_iter_next(&iter)) {
				bson_iter_t kiter;

				if (bson_iter_init_find (&kiter, store, bson_iter_key(&iter))) {
					if (!parserQueryResult(&kiter, &iter)) {
						goto NOSUIT;
					}
				} else{
					bson_iter_t subiter;
					if (BSON_ITER_HOLDS_DOCUMENT(&iter) && bson_iter_recurse(&iter, &subiter)) {
						//log_debug("------------%s:%s", bson_iter_key(&iter),bson_iter_key(&subiter));
						while (bson_iter_next(&subiter)) {
							if (strcasecmp("$not", bson_iter_key(&subiter)) == 0) {
								//log_debug("not find and not:%s", bson_iter_key(&subiter));
							} else {
								goto NOSUIT;
							}
						}
						
					} else {
						goto NOSUIT;
					}
				}
			}
		}
		bson_destroy(store);
		return true;
	NOSUIT:
		bson_destroy(store);
		return false;
	}
	return true;
}

int CltCmdInfo::InitCltOption(bson_t *config, bson_t *query, bson_t *update) {
	if (!config) {
		return -1;
	}
	this->query_bson_ = query;
	this->update_bson_ = update;
	this->option_data_ = config;

	bson_iter_t iter;

	clt_configs_.clear();	
	clt_querys_.clear();
	clt_updates_.clear();

	if (bson_iter_init(&iter, this->option_data_)) {
		while (bson_iter_next(&iter)) {
			if (BSON_ITER_IS_KEY(&iter, "primary")) {
				this->InitCltCmdInfo(&iter, ColumnType::kColPrimary);
			} else if (BSON_ITER_IS_KEY(&iter, "index")) {
				this->InitCltCmdInfo(&iter, ColumnType::kColIndex);
			} else if (BSON_ITER_IS_KEY(&iter, "stat")) {
				this->InitCltCmdInfo(&iter, ColumnType::kColStat);
			}
		}
		return 1;
	}

	return -1;
}

int CltCmdInfo::GetPrimaryKeys(bson_t *query) {
	bson_iter_t iter;
	this->query_bson_ = query;
	clt_querys_.clear();
	if (bson_iter_init(&iter, this->option_data_)) {
		while (bson_iter_next(&iter)) {
			if (BSON_ITER_IS_KEY(&iter, "primary")) {
				this->InitCltCmdInfo(&iter, ColumnType::kColPrimary);
			}
		}
		return 1;
	}
	return -1;
}

bool CltCmdInfo::InitCltCmdInfo(bson_iter_t *iter, ColumnType type) {
	bson_iter_t subiter, subsubiter;
	if (!BSON_ITER_HOLDS_ARRAY(iter) || !bson_iter_recurse(iter, &subiter)) { //[]
		return false;
	}
	int hash_bucket = 0;
	while (bson_iter_next (&subiter)) {//{}
		hash_bucket++;
		if (ColumnType::kColStat == type && BSON_ITER_HOLDS_UTF8(&subiter)) {
			this->clt_configs_[type].push_back(Column(bson_iter_utf8(&subiter, NULL), "", hash_bucket));
		} else if (bson_iter_recurse(&subiter, &subsubiter)) {
			std::string name;
			if (bson_iter_find (&subsubiter, "name") && BSON_ITER_HOLDS_UTF8(&subsubiter)) {
				name.assign(bson_iter_utf8(&subsubiter, NULL));
				if (bson_iter_find(&subsubiter, "type") && BSON_ITER_HOLDS_UTF8(&subsubiter)) {
					if (!strcasecmp(bson_iter_utf8(&subsubiter, NULL), "unique")){
						type = ColumnType::kColUniqueIndex;
					}
				}
			} else {
				return false;
			}
			if (this->query_bson_) {
				parser_clt_request_info(&this->clt_querys_[type], this->query_bson_, name, hash_bucket);
			}

			if (this->update_bson_) {
				parser_clt_request_info(&this->clt_updates_[type], this->update_bson_, name, hash_bucket);
			}
			this->clt_configs_[type].push_back(Column(name, "", hash_bucket));
		}
	}

	if (type == ColumnType::kColPrimary && !clt_configs_[ColumnType::kColPrimary].empty()) {
		this->primary_key_name_ = clt_configs_[ColumnType::kColPrimary].back().name;
	}
	return true;
}

bool CltCmdInfo::QueryHasFullKeys() {
	if (clt_querys_[ColumnType::kColPrimary].size() == clt_configs_[ColumnType::kColPrimary].size()){
		int i = 0;
		for (auto it = clt_configs_[ColumnType::kColPrimary].cbegin(); 
				it != clt_configs_[ColumnType::kColPrimary].cend(); ++it, ++i) {
			if (it->name != clt_querys_[ColumnType::kColPrimary][i].name) {
				return false;
			}
		}
		return true;
	}
	return false;
}

bool CltCmdInfo::UpdateHasFullKeys() {
	return clt_updates_[ColumnType::kColPrimary].size() == clt_configs_[ColumnType::kColPrimary].size();
}

bool CltCmdInfo::NeedUpdateKey() {
	return !clt_updates_[ColumnType::kColPrimary].empty();
}

bool CltCmdInfo::IsUpdatePkeysValid() {
	if (!clt_updates_[ColumnType::kColPrimary].empty()) {
		if (clt_updates_[ColumnType::kColPrimary].back().name == primary_key_name_) {
			return true;
		}
	}
	return false;
}

bool CltCmdInfo::GetUpdatePkeyValue(const std::string &name, std::string *val) {
	std::vector<Column> &keys = clt_updates_[ColumnType::kColPrimary];
	for (int i = 0; i < keys.size(); ++i) {
		if (keys[i].name == name) {
			*val = keys[i].value;
			return true;
		}
	}
	return false;
}

std::string CltCmdInfo::packPkeysValue(std::map<char, std::vector<Column>> &query) {
	std::string buf;
	int size = clt_configs_[ColumnType::kColPrimary].size();
	//buf.append(1, (uint8_t)size);
	
	if (!query[ColumnType::kColPrimary].empty()) {
		const Columns &vals = query[ColumnType::kColPrimary];
		
		for (int i = 0; i < vals.size(); ++i) {
			//这里是为了防止输入的primary key顺序中断情况，例如不输入zone,只输入fid
			if (size > i && clt_configs_[ColumnType::kColPrimary][i].name != vals[i].name) {
				break;
			}
			//buf.append(1, (uint8_t)vals[i].value.size());
			buf.append(vals[i].value.data(), vals[i].value.size());
		}
	}
	return buf;
}

std::string CltCmdInfo::CreateNewKey(bson_t *store_bson, bool *update) {
	const std::vector<Column> &vec = this->clt_configs_[ColumnType::kColPrimary];
	std::vector<Column> newpkeys;

	*update = false;

	std::string buf;
	//buf.append(1, (uint8_t)vec.size());

	for (int i = 0; i < vec.size(); ++i) {
		std::string old_val, new_val;
		bool find = getBsonValue(&old_val, store_bson, vec[i].name);
		if (GetUpdatePkeyValue(vec[i].name, &new_val)) {
			//buf.append(1, (uint8_t)new_val.size());
			buf.append(new_val.data(), new_val.size());

			//找到store，与存储的主键值不一样 或者 没找到都需要更新主键
			if ((find && old_val != new_val) || !find) {
				log_debug("need update primarys:%s, old val:%s, new val:%s", 
								vec[i].name.data(), old_val.data(), new_val.data());
				*update = true;
			}
		} else {
			//如果此主键不需要更新，就使用原来的值，如果此主键不存在，使用的是默认值
			//buf.append(1, (uint8_t)old_val.size());
			buf.append(old_val.data(), old_val.size());
		}
	}
	return buf;
}

void CltCmdInfo::GetUpdateIndexCols(std::set<Column> *v, std::set<Column> &filter, ColumnType type) {
	bool setOpt = false;
	std::map<std::string, std::set<Column>> list;
	const Columns &indexs = clt_updates_[type];

	for (auto it = indexs.cbegin(); it != indexs.cend(); ++it) {
		switch (it->cmd_type_) {
		case UpdateCmd::kOptCmdRename:
		case UpdateCmd::kOptCmdUnset:
			setOpt = false;
			list.erase(it->name);
			break;
		case UpdateCmd::kOptCmdPull:
		case UpdateCmd::kOptCmdPullAll:
			list[it->name].erase(*it);
			setOpt = false;
			break;
		case UpdateCmd::kOptCmdPush:
		case UpdateCmd::kOptCmdAddToSet:
			list[it->name].insert(*it);
			setOpt = false;
			break;
		case UpdateCmd::kOptCmdInsert:
		case UpdateCmd::kOptCmdSet:
			if (!setOpt) {
				setOpt = true;
				list[it->name].erase(*it);
			}
			list[it->name].insert(*it);
			break;
		default:
			break;
		}
	}

	for (auto it = filter.cbegin(); it != filter.cend(); ++it) {
		log_debug("dont need save index:%s:%s", it->name.data(), it->value.data());
	}

	for (auto it = list.cbegin(); it != list.cend(); ++it) {
		for (auto itt = it->second.cbegin(); itt != it->second.cend(); ++itt) {
			if (filter.find(*itt) == filter.end()) {
				v->insert(*itt);
			}
		}
	}
}

bool CltCmdInfo::CheckRemoveIndexValid(const Column &record, ColumnType type) {
	//不会是长列表，使用遍历
	bool setIndex = false;
	const Columns &indexs = clt_updates_[type];
	for (auto it = indexs.crbegin(); it != indexs.crend(); ++it) {
		if (it->name == record.name) {
			switch (it->cmd_type_) {
			case UpdateCmd::kOptCmdRename:
			case UpdateCmd::kOptCmdUnset:
				return true;

			case UpdateCmd::kOptCmdPull:
			case UpdateCmd::kOptCmdPullAll:
				if (it->value == record.value) {
					return true;
				}
				break;
			case UpdateCmd::kOptCmdPush:
			case UpdateCmd::kOptCmdAddToSet:
				if (setIndex) { //这里是最后￥Set的情况
					return true;
				}
				if (it->value == record.value) {
					return false;
				}
				break;
			case UpdateCmd::kOptCmdInsert:
			case UpdateCmd::kOptCmdSet:
				setIndex = true;
				if (it->value == record.value) {
					return false;
				}
				break;
			case UpdateCmd::kOptCmdNone:
				log_debug("-----------heiheiehei:%s:%s", it->name.c_str(), it->value.c_str());
				break;
			default:
				break;
			}
		}
	}
	return setIndex;
}

//CLT_CLEAR
bson_t *CltCmdInfo::CreateNewStoreData(bson_t *oldData) {
	bson_t *dst = bson_new();
	bson_init(dst);
	bson_copy_to(oldData, dst);
	bson_iter_t iter, subiter;
	if (bson_iter_init(&iter, update_bson_)) {
		while (bson_iter_next (&iter)) {
			const char *key = bson_iter_key (&iter);

			if (key[0] != '$') {
				dst = setCmdNormal(dst, &iter);
            } else if (!strcasecmp(key, "$set") && BSON_ITER_HOLDS_DOCUMENT(&iter) && bson_iter_recurse(&iter, &subiter)) {
				dst = setCmdOption(dst, &subiter);
			} else if (!strcasecmp(key, "$incrby") && bson_iter_recurse(&iter, &subiter)) {
				dst = incrbyCmdOption(dst, &subiter);
			} else if (!strcasecmp(key, "$unset") && bson_iter_recurse(&iter, &subiter)) {
				dst = unsetCmdOption(dst, &subiter);
			} else if (!strcasecmp(key, "$pullAll") && bson_iter_recurse(&iter, &subiter)) {
				dst = pullAllCmdOption(dst, &subiter);
			} else if (!strcasecmp(key, "$addToSet") && bson_iter_recurse(&iter, &subiter)) {
				dst = addToSetCmdOption(dst, &subiter);
			} else if (!strcasecmp(key, "$push") && bson_iter_recurse(&iter, &subiter)) {
				dst = pushCmdOption(dst, &subiter);
			} else if (!strcasecmp(key, "$pull") && bson_iter_recurse(&iter, &subiter)) {
				dst = pullCmdOption(dst, &subiter);
            }
        }
	}

	return dst;
}

bson_t* CltCmdInfo::setCmdNormal(bson_t *src, bson_iter_t *find_iter) {
	bson_t *result = bson_new();
	bson_init(result);

	bson_iter_t iter;
	bool is_find = false;

	if (bson_iter_init(&iter, src)) {
	    const uint8_t *raw = 0;
	    uint32_t len = 0;
		raw = iter.raw;
		len = iter.len;
		while (bson_iter_next(&iter)) {
			if (strcasecmp(bson_iter_key (find_iter), bson_iter_key(&iter))) {
				bson_append_iter(result, NULL, 0, &iter);
			} else {
				is_find = true;
				bson_append_iter(result, NULL, 0, find_iter);
                //更新column
                UpdateStatSize(src, find_iter, 1);
			}
		}
		iter.raw = raw;
		iter.len = len;
	}
	if (!is_find) {
		bson_append_iter(result, NULL, 0, find_iter);
        //  新增column
        UpdateStatSize(NULL, find_iter, 1);
	}
	bson_clear(&src);

	return result;
} 

bson_t* CltCmdInfo::setCmdOption(bson_t *src, bson_iter_t *find_iter) { //find_iter 到的层次是 filename:111
	bson_t *result = bson_new();
	bson_init(result);

	bson_iter_t iter;

	if (bson_iter_init(&iter, src)) {
		const uint8_t *raw = iter.raw;
		uint32_t len = iter.len;
		while (bson_iter_next(&iter)) {
			if (!bson_iter_reset_find(find_iter, bson_iter_key(&iter))) {
	            log_debug("key :%s, type:%d", bson_iter_key(&iter), bson_iter_type(&iter));
				bson_append_iter(result, NULL, 0, &iter);
			} else {
	            log_debug("key :%s, type:%d", bson_iter_key(find_iter), bson_iter_type(find_iter));
				bson_append_iter(result, NULL, 0, find_iter);
                // 更新stat
                UpdateStatSize(src, find_iter, 1);
			}
		}
		iter.raw = raw;
		iter.len = len;
	}
	bson_iter_reset(find_iter);
	while (bson_iter_next(find_iter)) {
		if (!bson_iter_reset_find(&iter, bson_iter_key(find_iter))) {
			bson_append_iter(result, NULL, 0, find_iter);
            // 新增column
            UpdateStatSize(NULL, find_iter, 1);
		}
	}
	bson_clear(&src);
	return result;
}

bson_t* CltCmdInfo::unsetCmdOption(bson_t *src, bson_iter_t *find_iter) {
	bson_t *result = bson_new();
	bson_init(result);

	bson_iter_t iter;
	if (bson_iter_init(&iter, src)) {
		while (bson_iter_next(&iter)) {
			if (!bson_iter_reset_find(find_iter, bson_iter_key(&iter))) {
				bson_append_iter(result, NULL, 0, &iter);
			}else{
                // 
                UpdateStatSize(NULL, &iter, -1);
            }
		}
	}
	bson_clear(&src);
	return result;
}

// incrby 只支持int32,int64,不支持double类型
bson_t* CltCmdInfo::incrbyCmdOption(bson_t *src, bson_iter_t *find_iter) {
	bson_iter_t iter;

    while (bson_iter_next(find_iter)){
        bson_type_t find_type = bson_iter_type(find_iter);
        if ( find_type != BSON_TYPE_INT32 && find_type != BSON_TYPE_INT64) {
            continue;
        }
        const char *key = bson_iter_key(find_iter);
        if ( bson_iter_init_find(&iter, src, key) ) {
            switch (bson_iter_type (&iter)) {
                case BSON_TYPE_INT32:
                    bson_iter_overwrite_int32(&iter, 
                            bson_iter_int32(&iter) + 
                            bson_iter_int32(find_iter));
                    IncrStatSize(key, bson_iter_as_int64(find_iter));
                    break;
                case BSON_TYPE_INT64:
                    bson_iter_overwrite_int64(&iter, 
                            bson_iter_as_int64(&iter) + 
                            bson_iter_as_int64(find_iter));
                    IncrStatSize(key, bson_iter_as_int64(find_iter));
                    break;
                default:
                    log_info("incrby type error %d", bson_iter_type (&iter));
                    break;
            }
        }else{
            bson_append_iter(src, NULL, 0, find_iter);
            // 如果find_iter 不是数字类型，bson_iter_as_int64 返回会是0
            IncrStatSize(key, bson_iter_as_int64(find_iter));
        }
    }

	return src;
}

// pullAll操作的是数组，无需更新stat统计
bson_t* CltCmdInfo::pullAllCmdOption(bson_t *src, bson_iter_t *find_iter) {
	bson_t *result = bson_new();
	bson_init(result);

	bson_iter_t iter, subiter, find_subiter;

	if (bson_iter_init(&iter, src)) {
		while (bson_iter_next(&iter)) {
			if (!bson_iter_reset_find(find_iter, bson_iter_key(&iter))) {
				bson_append_iter(result, NULL, 0, &iter);
			} else if (BSON_ITER_HOLDS_ARRAY(&iter) && bson_iter_recurse(&iter, &subiter)
			           && BSON_ITER_HOLDS_ARRAY(find_iter) && bson_iter_recurse(find_iter, &find_subiter)) {
				bson_t child;
				bson_append_array_begin(result, bson_iter_key(&iter), -1, &child);

				while (bson_iter_next(&subiter)) {
					if (!bson_iter_find_value(&find_subiter, &subiter)) {
						bson_append_iter(&child, NULL, 0, &subiter);
					}
				}

				bson_append_array_end(result, &child);
			}
		}
	}
	bson_clear(&src);
	return result;
}

// pull操作的是数组，无需更新stat统计
bson_t* CltCmdInfo::pullCmdOption(bson_t *src, bson_iter_t *find_iter) {
	bson_t *result = bson_new();
	bson_init(result);
	bson_iter_t iter, subiter, find_subiter, find_subsubiter;

	if (bson_iter_init(&iter, src)) {
		while (bson_iter_next(&iter)) {
			const char *idxName = bson_iter_key(&iter);
			if (!bson_iter_reset_find(find_iter, idxName)) {
				bson_append_iter(result, NULL, 0, &iter);
			} else if (BSON_ITER_HOLDS_ARRAY(&iter) && bson_iter_recurse(&iter, &subiter)) {
				//特定条件的doc
				if (BSON_ITER_HOLDS_DOCUMENT(find_iter) && bson_iter_recurse(find_iter, &find_subiter) &&
				        bson_iter_next(&find_subiter)) {
					bool goit = true;
					const char *key = bson_iter_key(&find_subiter);

					if (strcasecmp(key, "$all") == 0 &&
					        BSON_ITER_HOLDS_ARRAY(&find_subiter) && bson_iter_recurse(&find_subiter, &find_subsubiter)) {
						while (bson_iter_next(&find_subsubiter)) {
							if (!bson_iter_find_value(&subiter, &find_subsubiter)) {
								log_debug("not find %s", bson_iter_utf8(&find_subsubiter, 0));
								goit = false;
								for (auto it = clt_updates_[ColumnType::kColIndex].begin();
										it != clt_updates_[ColumnType::kColIndex].end(); ++it) {
									if (strcasecmp(idxName, it->name.c_str()) == 0) {
										log_debug("don't need remove index name:%s, value:%s", it->name.c_str(),
																	it->value.c_str());
										it->cmd_type_ = UpdateCmd::kOptCmdNone;
									}
								}
								break;
							}
						}
						if (!goit) {
							bson_append_iter(result, NULL, 0, &iter);
						}
					}
					if (((goit && strcasecmp(key, "$all") == 0) || strcasecmp(key, "$in") == 0)
					        && BSON_ITER_HOLDS_ARRAY(&find_subiter) && bson_iter_recurse(&find_subiter, &find_subsubiter)) {
						bson_t child;
						bson_iter_reset(&subiter);
						bson_append_array_begin(result, bson_iter_key(&iter), -1, &child);
						
						while (bson_iter_next(&subiter)) {
							if (!bson_iter_find_value(&find_subsubiter, &subiter)) {
								bson_append_iter(&child, NULL, 0, &subiter);
							}
						}
						
						bson_append_array_end(result, &child);
					}
				} else if (!BSON_ITER_HOLDS_ARRAY(find_iter)) { //普通value
					bson_t child;
					bson_append_array_begin(result, bson_iter_key(&iter), -1, &child);

					while (bson_iter_next(&subiter)) {
						if (!bson_matcher_iter_eq_match(find_iter, &subiter)) {
							bson_append_iter(&child, NULL, 0, &subiter);
						}
					}
					bson_append_array_end(result, &child);
				}
			}
		}
	}
	bson_clear(&src);
	return result;
}

// addToSet 操作的是数组，无需更新stat统计
bson_t* CltCmdInfo::addToSetCmdOption(bson_t *src, bson_iter_t *find_iter) {
	bson_t *result = bson_new();
	bson_init(result);

	bson_iter_t iter, subiter, find_subiter;
	if (bson_iter_init(&iter, src)) {
		const uint8_t *raw = iter.raw;
		int32_t len = iter.len;
		while (bson_iter_next(&iter)) {
			if (!bson_iter_reset_find(find_iter, bson_iter_key(&iter))) {
				bson_append_iter(result, NULL, 0, &iter);
			} else if (BSON_ITER_HOLDS_ARRAY(&iter) && bson_iter_recurse(&iter, &subiter)
			           && BSON_ITER_HOLDS_ARRAY(find_iter) && bson_iter_recurse(find_iter, &find_subiter)) {
				bson_t child;

				bson_append_array_begin(result, bson_iter_key(&iter), -1, &child);

				const uint8_t *raw = subiter.raw;
				uint32_t len = subiter.len;
				while (bson_iter_next(&subiter)) {
					bson_append_iter(&child, NULL, 0, &subiter);
				}
				subiter.raw = raw;
				subiter.len = len;

				while (bson_iter_next(&find_subiter)) {
					if (!bson_iter_find_value(&subiter, &find_subiter)) {
						bson_append_value(&child, bson_iter_key(&iter), -1, bson_iter_value(&find_subiter));
					}
				}

				bson_append_array_end(result, &child);
			}
		}
		iter.len = len;
		iter.raw = raw;
		bson_iter_reset(&iter);
		bson_iter_reset(find_iter);
		while (bson_iter_next(find_iter)) {
			if (!bson_iter_find(&iter, bson_iter_key(find_iter))) {
				bson_append_iter(result, NULL, 0, find_iter);
			}
		}
	}
	bson_clear(&src);
	return result;
}

// push 操作的是数组，无需更新stat统计
bson_t* CltCmdInfo::pushCmdOption(bson_t *src, bson_iter_t *find_iter) {
	bson_t *result = bson_new();
	bson_init(result);

	bson_iter_t iter, subiter, find_subiter;
	if (bson_iter_init(&iter, src)) {
		const uint8_t *raw = iter.raw;
		int32_t len = iter.len;
		while (bson_iter_next(&iter)) {
			if (!bson_iter_reset_find(find_iter, bson_iter_key(&iter))) {
				bson_append_iter(result, NULL, 0, &iter);
			} else if (BSON_ITER_HOLDS_ARRAY(&iter) && bson_iter_recurse(&iter, &subiter)
			           && BSON_ITER_HOLDS_ARRAY(find_iter) && bson_iter_recurse(find_iter, &find_subiter)) {
				bson_t child;

				bson_append_array_begin(result, bson_iter_key(&iter), -1, &child);

				while (bson_iter_next(&subiter)) {
					bson_append_iter(&child, NULL, 0, &subiter);
				}

				while (bson_iter_next(&find_subiter)) {
					bson_append_value(&child, bson_iter_key(&iter), -1, bson_iter_value(&find_subiter));
				}

				bson_append_array_end(result, &child);
			}
		}
		iter.len = len;
		iter.raw = raw;
		bson_iter_reset(&iter);
		bson_iter_reset(find_iter);
		while (bson_iter_next(find_iter)) {
			if (!bson_iter_find(&iter, bson_iter_key(find_iter))) {
				bson_append_iter(result, NULL, 0, find_iter);
			}
		}
	}
	bson_clear(&src);
	return result;
}

