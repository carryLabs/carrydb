#include "bson_tool.h"

bson_t *parser_bson_from_json(const char *data, size_t length) {
	bson_error_t error;
	bson_t *pd = bson_new_from_json((uint8_t*)data, length, &error);

	if (!pd) {
		log_error("Error: %s\n", error.message);
		return nullptr;
	}
	return pd;
}

bson_t *parser_bson_from_data(const char *data, size_t length, bool pretty) {
	if (data) {
		bson_error_t error;
		bson_t *pd = !pretty ? bson_new_from_data((uint8_t*)data, length) :
		             bson_new_from_json((uint8_t*)data, length, &error);
		if (!pd) {
			log_error("Error: %d %s\n", error.code, error.message);
			return nullptr;
		} else {
			return pd;
		}
	} else {
		return nullptr;
	}
}

int bson_to_json(std::string *data) {
	bson_t *tmp = parser_bson_from_data(data->data(), data->size());
	std::string val;
	if (tmp) {
		char *str = bson_as_json (tmp, NULL);
		data->assign(str);
		bson_free (str);
		bson_destroy(tmp);
		return 1;
	}
	return CmdStatus::kCltBsonError;
}

void print_bson(bson_t *tmp) {
	//bson_t *tmp = parser_bson_from_data(data.data(), data.size());
	if (tmp){
		char *str = bson_as_json (tmp, NULL);
		log_debug("print_bson:%s", str);
		bson_free (str);
	}
}

void parser_clt_request_info(std::vector<Column> *record, bson_t *bson, const std::string &name, int hash_bucket) {
	bson_iter_t iter;
	bson_iter_init(&iter, bson);

	while (bson_iter_next (&iter)) {
		const char *key = bson_iter_key (&iter);

		if (!strcasecmp(name.c_str(), key)) {
			set_column_values(record, &iter, name, hash_bucket, UpdateCmd::kOptCmdInsert);
		} else if (!strcasecmp (key, "$set")) {
			set_column_values(record, &iter, name, hash_bucket, UpdateCmd::kOptCmdSet);
		} else if (!strcasecmp (key, "$unset")) {
			set_column_values(record, &iter, name, hash_bucket, UpdateCmd::kOptCmdUnset);
		} else if (!strcasecmp (key, "$pullAll")) {
			set_column_values(record, &iter, name, hash_bucket, UpdateCmd::kOptCmdPullAll);
		} else if (!strcasecmp (key, "$addToSet")) {
			set_column_values(record, &iter, name, hash_bucket, UpdateCmd::kOptCmdAddToSet);
		} else if (!strcasecmp (key, "$push")) {
			set_column_values(record, &iter, name, hash_bucket, UpdateCmd::kOptCmdPush);
		} else if (!strcasecmp (key, "$pull")) {
			set_column_values(record, &iter, name, hash_bucket, UpdateCmd::kOptCmdPull);
		}
	}
}

void set_column_values(std::vector<Column> *record, bson_iter_t *iter, 
				const std::string &name, int hash_bucket, UpdateCmd cmd_type) {
	std::vector<std::string> value;
	bson_iter_t subiter;
	if (cmd_type != UpdateCmd::kOptCmdInsert) {
		if (bson_iter_recurse(iter, &subiter) && bson_iter_find(&subiter, name.c_str())) {
			get_column_values(&subiter, name, record, hash_bucket, cmd_type);			
		} 
	} else {
		get_column_values(iter, name, record, hash_bucket, cmd_type);
	}
}

void get_column_values(bson_iter_t *iter, const std::string &name, std::vector<Column> *records,
                    			int hash_bucket, UpdateCmd cmd_type, FilterFlag flag){
	const bson_value_t *value = bson_iter_value(iter);
	switch (bson_iter_type(iter)) {
	case BSON_TYPE_DOUBLE:
		records->push_back(Column(name, str(value->value.v_double), hash_bucket, cmd_type, flag));
		break;
	case BSON_TYPE_INT32:
		records->push_back(Column(name, str(value->value.v_int32), hash_bucket, cmd_type, flag));
		break;
	case BSON_TYPE_INT64:
		records->push_back(Column(name, str(value->value.v_int64), hash_bucket, cmd_type, flag));
		break;
	case BSON_TYPE_UTF8:
		records->push_back(Column(name, 
				std::string(value->value.v_utf8.str, value->value.v_utf8.len), hash_bucket, cmd_type, flag));
		break;
	case BSON_TYPE_ARRAY:
		{
			bson_iter_t subiter;
			if (bson_iter_recurse(iter, &subiter)) {
				while (bson_iter_next(&subiter)) {
					get_column_values(&subiter, name, records, hash_bucket, cmd_type, flag);
				}
			}
		}
		break;

	case BSON_TYPE_DOCUMENT:
		{
			bson_iter_t subiter, subsubiter;
			if (bson_iter_recurse(iter, &subiter)) {
				while (bson_iter_next(&subiter)) {
					if (strcasecmp("$in", bson_iter_key(&subiter)) == 0 &&
					BSON_ITER_HOLDS_ARRAY(&subiter) && bson_iter_recurse(&subiter, &subsubiter)) {
						while (bson_iter_next(&subsubiter)) {
							get_column_values(&subsubiter, name, records, hash_bucket, cmd_type, FilterFlag::kFilterTypeIn);
						}
					} else if (strcasecmp("$all", bson_iter_key(&subiter)) == 0 &&
					           BSON_ITER_HOLDS_ARRAY(&subiter) && bson_iter_recurse(&subiter, &subsubiter)) {
						while (bson_iter_next(&subsubiter)) {
							get_column_values(&subsubiter, name, records, hash_bucket, cmd_type, FilterFlag::kFilterTypeAll);
						}
					}
				}
			}
		}
		break;
	default:
		break;
	}
}

bool getBsonValues(bson_iter_t *iter, std::vector<std::string> *list) {
	const bson_value_t *value = bson_iter_value(iter);
	switch (bson_iter_type(iter)) {
	case BSON_TYPE_INT32:
		list->push_back(str(value->value.v_int32));
		return true;
	case BSON_TYPE_INT64:
		list->push_back(str(value->value.v_int64));
		return true;
	case BSON_TYPE_UTF8:
		list->push_back(std::string(value->value.v_utf8.str, value->value.v_utf8.len));
		return true;
	case BSON_TYPE_ARRAY:
		bson_iter_t subiter;
		if (bson_iter_recurse(iter, &subiter)) {
			while (bson_iter_next(&subiter)) {
				getBsonValues(&subiter, list);
			}
			return true;

		}
	//不继续处理document,语法需要告知是哪一层object
	default:
		break;
	}
	return false;
}


bool getBsonValues(const bson_t *bson, const std::string &key, std::vector<std::string> *list) {
	bson_iter_t iter;
	if (bson_iter_init_find(&iter, bson, key.c_str())) {
		return getBsonValues(&iter, list);
	} else {
		return false;
	}
}

bool getBsonValue(std::string *val, const bson_t *bson, const std::string &key) {
	bson_iter_t iter;

	if (bson_iter_init_find(&iter, bson, key.c_str())) {
		const bson_value_t *value = bson_iter_value(&iter);
		switch (bson_iter_type(&iter)) {
		case BSON_TYPE_INT32:
			if (val) *val = str(value->value.v_int32);
			return true;
		case BSON_TYPE_INT64:
			if (val) *val = str(value->value.v_int64);
			return true;
		case BSON_TYPE_UTF8:
			if (val) *val = std::string(value->value.v_utf8.str, value->value.v_utf8.len);
			return true;
		default:
			break;
		}
	}
	return false;
}

bool ParserCmdCondition(const char *data, size_t length, Condition *opts, bool pretty) {
	bson_t *cond = parser_bson_from_data(data, length, pretty);
	if (!cond) {
		return false;
	}

	bson_iter_t iter;
	if (bson_iter_init(&iter, cond)) {
		while (bson_iter_next(&iter)) {
			const char *key = bson_iter_key(&iter);
			if (strcasecmp(key, "limit") == 0 && BSON_ITER_HOLDS_INT32(&iter)) {
				opts->limit = bson_iter_int32 (&iter);
			}else if (strcasecmp(key, "multi") == 0 && BSON_ITER_HOLDS_BOOL(&iter)) {
				opts->multi = bson_iter_bool(&iter);
			}else if (strcasecmp(key, "cursor") == 0 && BSON_ITER_HOLDS_INT32(&iter)) {
				opts->cursor = bson_iter_int32(&iter);
			}else if (strcasecmp(key, "order") == 0 && BSON_ITER_HOLDS_INT32(&iter)) {
				opts->desc = bson_iter_int32 (&iter);
			}else if (strcasecmp(key, "background") == 0 && BSON_ITER_HOLDS_BOOL(&iter)) {
				opts->background = bson_iter_bool(&iter);
			}else if (strcasecmp(key, "force") == 0 && BSON_ITER_HOLDS_BOOL(&iter)) {
				opts->force = bson_iter_bool(&iter);
			}else if (strcasecmp(key, "start") == 0 && BSON_ITER_HOLDS_UTF8(&iter)) {
				opts->start = bson_iter_utf8(&iter, nullptr);
				opts->has_ranged_start = true;
			}else if (strcasecmp(key, "end") == 0 && BSON_ITER_HOLDS_UTF8(&iter)) {
				opts->end = bson_iter_utf8(&iter, nullptr);
				opts->has_ranged_end = true;
			}
		}
	}
	bson_destroy(cond);
	return true;
}

bool paser_bson_int32(bson_iter_t *iter, int32_t *rst) {
	if (!iter) {
		return false;
	}
	if (BSON_ITER_HOLDS_INT32 (iter)) {
		*rst = bson_iter_int32 (iter);
		return true;
	} else {
		return false;
	}
}

bool paser_bson_boolean(bson_iter_t *iter, bool *rst) {
	if (!iter) {
		return false;
	}
	if (BSON_ITER_HOLDS_BOOL (iter)) {
		*rst = bson_iter_bool (iter);
		return true;
	} else {
		return false;
	}
}

bool recreate_table_config(bson_t **data) {
	bson_t *dst = bson_new();
	bson_init(dst);

	bson_iter_t iter, subiter, subsubiter;
	BSON_ASSERT (bson_iter_init (&iter, *data));
	while (bson_iter_next (&iter)) {
		const char *key = bson_iter_key (&iter);
		//数据库保留字段
		if (key[0] == '_'){
			continue;
		}

		if ((strcasecmp("primary", key) == 0 || strcasecmp("index", key) == 0) && 
				BSON_ITER_HOLDS_ARRAY(&iter) && bson_iter_recurse(&iter, &subiter)){
			while (bson_iter_next(&subiter)){
				if (BSON_ITER_HOLDS_DOCUMENT (&subiter) && bson_iter_recurse(&subiter, &subsubiter)){
					if (!bson_iter_find (&subsubiter, "name") || !BSON_ITER_HOLDS_UTF8(&subsubiter)) {
						return false;
					}
				}else {
					return false;
				}
			}
			bson_append_iter(dst, NULL, 0, &iter);
		}

		if (strcasecmp("stat", key) == 0 && 
				BSON_ITER_HOLDS_ARRAY (&iter) && bson_iter_recurse (&iter, &subiter)){
			bson_append_iter(dst, NULL, 0, &iter);
			while (bson_iter_next (&subiter)) {
				if (!BSON_ITER_HOLDS_UTF8(&subiter)) {
					return false;
				} else {
					const bson_value_t *sub_value = bson_iter_value(&subiter);
					BSON_APPEND_INT64(dst, statName(std::string(sub_value->value.v_utf8.str,
				                                 sub_value->value.v_utf8.len)).data(), 0);
				}
			}
		}
	}
	bson_clear(data);
	*data = dst;
	// char *str1 = bson_as_json (*data, NULL);
	// log_debug("%s", str1);
	
	// bson_free(str1);
	// log_debug("-----------1");
	return true;	
}

bool has_dollar_fields (const bson_t *bson)
{
	bson_iter_t iter;
	const char *key;

	BSON_ASSERT (bson_iter_init (&iter, bson));
	while (bson_iter_next (&iter)) {
		key = bson_iter_key (&iter);

		if (key[0] == '$') {
			return true;
		}
	}

	return false;
}

bool bson_matcher_iter_eq_match (bson_iter_t *compare_iter, bson_iter_t *iter)
{
	int code;

	code = _TYPE_CODE (bson_iter_type (compare_iter), bson_iter_type (iter));

	switch (code) {
	/* Double on Left Side */
	case _TYPE_CODE (BSON_TYPE_DOUBLE, BSON_TYPE_DOUBLE):
		return _EQ_COMPARE (_double, _double);
	case _TYPE_CODE (BSON_TYPE_DOUBLE, BSON_TYPE_BOOL):
		return _EQ_COMPARE (_double, _bool);
	case _TYPE_CODE (BSON_TYPE_DOUBLE, BSON_TYPE_INT32):
		return _EQ_COMPARE (_double, _int32);
	case _TYPE_CODE (BSON_TYPE_DOUBLE, BSON_TYPE_INT64):
		return _EQ_COMPARE (_double, _int64);

	/* UTF8 on Left Side */
	case _TYPE_CODE (BSON_TYPE_UTF8, BSON_TYPE_UTF8): {
		uint32_t llen;
		uint32_t rlen;
		const char *lstr;
		const char *rstr;

		lstr = bson_iter_utf8 (compare_iter, &llen);
		rstr = bson_iter_utf8 (iter, &rlen);

		return ((llen == rlen) && (0 == memcmp (lstr, rstr, llen)));
	}

	/* Int32 on Left Side */
	case _TYPE_CODE (BSON_TYPE_INT32, BSON_TYPE_DOUBLE):
		return _EQ_COMPARE (_int32, _double);
	case _TYPE_CODE (BSON_TYPE_INT32, BSON_TYPE_BOOL):
		return _EQ_COMPARE (_int32, _bool);
	case _TYPE_CODE (BSON_TYPE_INT32, BSON_TYPE_INT32):
		return _EQ_COMPARE (_int32, _int32);
	case _TYPE_CODE (BSON_TYPE_INT32, BSON_TYPE_INT64):
		return _EQ_COMPARE (_int32, _int64);

	/* Int64 on Left Side */
	case _TYPE_CODE (BSON_TYPE_INT64, BSON_TYPE_DOUBLE):
		return _EQ_COMPARE (_int64, _double);
	case _TYPE_CODE (BSON_TYPE_INT64, BSON_TYPE_BOOL):
		return _EQ_COMPARE (_int64, _bool);
	case _TYPE_CODE (BSON_TYPE_INT64, BSON_TYPE_INT32):
		return _EQ_COMPARE (_int64, _int32);
	case _TYPE_CODE (BSON_TYPE_INT64, BSON_TYPE_INT64):
		return _EQ_COMPARE (_int64, _int64);

	/* Null on Left Side */
	case _TYPE_CODE (BSON_TYPE_NULL, BSON_TYPE_NULL):
	case _TYPE_CODE (BSON_TYPE_NULL, BSON_TYPE_UNDEFINED):
		return true;

	case _TYPE_CODE (BSON_TYPE_ARRAY, BSON_TYPE_ARRAY): {
		bson_iter_t left_array;
		bson_iter_t right_array;
		bson_iter_recurse (compare_iter, &left_array);
		bson_iter_recurse (iter, &right_array);

		while (true) {
			bool left_has_next = bson_iter_next (&left_array);
			bool right_has_next = bson_iter_next (&right_array);

			if (left_has_next != right_has_next) {
				/* different lengths */
				return false;
			}

			if (!left_has_next) {
				/* finished */
				return true;
			}

			if (!bson_matcher_iter_eq_match (&left_array, &right_array)) {
				return false;
			}
		}
	}

	case _TYPE_CODE (BSON_TYPE_DOCUMENT, BSON_TYPE_DOCUMENT): {
		uint32_t llen;
		uint32_t rlen;
		const uint8_t *ldoc;
		const uint8_t *rdoc;

		bson_iter_document (compare_iter, &llen, &ldoc);
		bson_iter_document (iter, &rlen, &rdoc);

		return ((llen == rlen) && (0 == memcmp (ldoc, rdoc, llen)));
	}

	default:
		return false;
	}
}

bool bson_iter_reset(bson_iter_t *iter) {
	iter->off = 0;
	iter->type = 0;
	iter->key = 0;
	iter->d1 = 0;
	iter->d2 = 0;
	iter->d3 = 0;
	iter->d4 = 0;
	iter->next_off = 4;
	iter->err_off = 0;

	return true;
}

bool bson_iter_reset_find(bson_iter_t *iter, const char *keys) {
	bson_iter_reset(iter);
	const uint8_t *raw = iter->raw;
	uint32_t len = iter->len;

	bool find = bson_iter_find(iter, keys);
	iter->raw = raw;
	iter->len = len;

	return find;
}

bool bson_iter_find_value(bson_iter_t *iter,  bson_iter_t *value) {
	bson_iter_reset(iter);
	const uint8_t *raw = iter->raw;
	uint32_t len = iter->len;
	while (bson_iter_next(iter)) {
		if (bson_matcher_iter_eq_match(iter, value)) {
			return true;
		}
	}
	iter->raw = raw;
	iter->len = len;
	return false;
}

bool bson_compare_base_value(bson_iter_t *lft_it, bson_iter_t *rgt_it) { 
	switch (bson_iter_type(lft_it)) {
	case BSON_TYPE_INT32:
	case BSON_TYPE_INT64:
	case BSON_TYPE_DOUBLE:
	case BSON_TYPE_BOOL:
		if (BSON_ITER_HOLDS_DOUBLE(rgt_it) || BSON_ITER_HOLDS_BOOL(rgt_it) || BSON_ITER_HOLDS_INT32(rgt_it)
				|| BSON_ITER_HOLDS_INT64(rgt_it)) {
			return bson_iter_as_int64(lft_it) == bson_iter_as_int64(rgt_it);
		} else {
			return false;
		}
	case BSON_TYPE_UTF8:
		bson_iter_t subiter;
		if (BSON_ITER_HOLDS_ARRAY(rgt_it) && bson_iter_recurse(rgt_it, &subiter)) {
			while (bson_iter_next(&subiter)) {
				return BSON_ITER_HOLDS_UTF8(&subiter) &&
		       			std::string(bson_iter_utf8(&subiter, NULL)) == std::string(bson_iter_utf8(lft_it, NULL));
			}
		}else {
			return BSON_ITER_HOLDS_UTF8(rgt_it) &&
		       std::string(bson_iter_utf8(rgt_it, NULL)) == std::string(bson_iter_utf8(lft_it, NULL));
		}
	default:
		break;
	}
	return false;
}

bool bson_compare_value(bson_iter_t *lft_it, bson_iter_t *rgt_it) {
	if (BSON_ITER_HOLDS_DOUBLE(lft_it) || BSON_ITER_HOLDS_BOOL(lft_it) || BSON_ITER_HOLDS_INT32(lft_it)
				||BSON_ITER_HOLDS_INT64(lft_it) || BSON_ITER_HOLDS_UTF8(lft_it)){
		return bson_compare_base_value(lft_it, rgt_it);
	}
	if (BSON_ITER_HOLDS_ARRAY(lft_it)){
		bson_iter_t sub_iter;
		if (bson_iter_recurse(lft_it, &sub_iter)){
			while (bson_iter_next(&sub_iter)) {
				if (bson_compare_value(&sub_iter, rgt_it)){
					return true;
				}
   			}	 
		}
	}
	return false;
}

bool bson_comparison(int *ret, bson_iter_t *lft_it, bson_iter_t *rgt_it){
	if (BSON_ITER_HOLDS_UTF8(lft_it) && BSON_ITER_HOLDS_UTF8(rgt_it)){
		*ret = strcmp(bson_iter_utf8(lft_it, NULL), bson_iter_utf8(rgt_it, NULL));
		return true;
	}
	if ((BSON_ITER_HOLDS_INT32(lft_it) || BSON_ITER_HOLDS_INT64(lft_it) || BSON_ITER_HOLDS_DOUBLE(lft_it)) &&
		(BSON_ITER_HOLDS_INT32(rgt_it) || BSON_ITER_HOLDS_INT64(rgt_it) || BSON_ITER_HOLDS_DOUBLE(rgt_it))) {
		*ret = bson_iter_as_int64(lft_it) - bson_iter_as_int64(rgt_it);
		return true;
	}
	return false;
}

bool parserQueryResult(bson_iter_t *store_it, bson_iter_t *query_it){
	bson_iter_t subiter;
	if (BSON_ITER_HOLDS_DOCUMENT(query_it) && bson_iter_recurse(query_it, &subiter)){
		bson_iter_t subsubiter, store_subiter;
		bool find = false;
		int ret = 0;
		while (bson_iter_next(&subiter)) {
			if (strcasecmp("$in", bson_iter_key(&subiter)) == 0 &&
				BSON_ITER_HOLDS_ARRAY(&subiter) && bson_iter_recurse(&subiter, &subsubiter) &&
				BSON_ITER_HOLDS_ARRAY(store_it) && bson_iter_recurse(store_it, &store_subiter)) {
				while (bson_iter_next(&subsubiter)) {
					if (bson_iter_find_value(&store_subiter, &subsubiter)) {
						find = true;
						break;
					}
				}
			} else if (strcasecmp("$all", bson_iter_key(&subiter)) == 0 &&
				BSON_ITER_HOLDS_ARRAY(&subiter) && bson_iter_recurse(&subiter, &subsubiter) &&
				BSON_ITER_HOLDS_ARRAY(store_it) && bson_iter_recurse(store_it, &store_subiter)) {
				while (bson_iter_next(&subsubiter)) {
					if (!bson_iter_find_value(&store_subiter, &subsubiter)) {
						return false;
					}
				}
				find = true;
			} else if (strcasecmp("$lt", bson_iter_key(&subiter)) == 0) {
				if (bson_comparison(&ret, store_it, &subiter)){
					find = ret < 0;
					if (!find){
						return false;
					}
				}else{
					return false;
				}
			} else if (strcasecmp("$lte", bson_iter_key(&subiter)) == 0) {
				if (bson_comparison(&ret, store_it, &subiter)){
					find = ret <= 0;
					if (!find){
						return false;
					}
				}else{
					return false;
				}
			} else if (strcasecmp("$gt", bson_iter_key(&subiter)) == 0) {
				if (bson_comparison(&ret, store_it, &subiter)){
					find = ret > 0;
					if (!find){
						return false;
					}
				}else{
					return false;
				}
			} else if (strcasecmp("$gte", bson_iter_key(&subiter)) == 0) {
				if (bson_comparison(&ret, store_it, &subiter)){
					find = ret >= 0;
					if (!find){
						return false;
					}
				}else{
					return false;
				}
			} else if(strcasecmp("$not", bson_iter_key(&subiter)) == 0) {
				if (bson_matcher_iter_eq_match(store_it, &subiter)){
					//log_debug("---%s:", bson_iter_key(store_it));
					return false;
				}else{
					find = true;
				}
			}
		}
		return find;
	}else {
		return bson_compare_value(store_it, query_it);
	}
}




