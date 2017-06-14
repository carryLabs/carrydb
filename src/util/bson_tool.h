#ifndef UTIL_BSON_TOOL_H_
#define UTIL_BSON_TOOL_H_

#include <set>
#include "bson/bson.h"
#include "log.h"
#include "strings.h"
#include "const.h"

#define _TYPE_CODE(l, r) ((((int) (l)) << 8) | ((int) (r)))
#define _NATIVE_COMPARE(op, t1, t2) \
   (bson_iter##t2 (iter) op bson_iter##t1 (compare_iter))
#define _EQ_COMPARE(t1, t2) _NATIVE_COMPARE (==, t1, t2)

/////////////////////////////base function///////////////////////////////////////
bson_t *parser_bson_from_json(const char *data, size_t length);
bson_t *parser_bson_from_data(const char *data, size_t length, bool pretty = false);
int bson_to_json(std::string *data);
void print_bson(bson_t *tmp);
bool bson_matcher_iter_eq_match(bson_iter_t *compare_iter, bson_iter_t *iter);
bool has_dollar_fields (const bson_t *bson);

bool paser_bson_int32(bson_iter_t *iter, int32_t *rst);
bool paser_bson_boolean(bson_iter_t *iter, bool *rst);

bool bson_iter_reset_find(bson_iter_t *iter, const char *key);
bool bson_iter_reset(bson_iter_t *iter);
bool bson_iter_find_value(bson_iter_t *iter,  bson_iter_t *value);


/////////////////////////////////////////////////////////////////////////////////
bool recreate_table_config(bson_t **data);

//解析条件bson的值 out opts
bool ParserCmdCondition(const char *data, size_t length, Condition *opts, bool pretty);

//解析命令中的bson串，获取index与primary key 信息
void parser_clt_request_info(std::vector<Column> *record, bson_t *bson, const std::string &name, int hash_bucket);
void set_column_values(std::vector<Column> *record, bson_iter_t *iter, 
				const std::string &name, int hash_bucket, UpdateCmd type);
static inline std::string statName(const std::string &name) {
	return "_" + name + "_stat_info";
}
bool bson_comparison(int *ret, bson_iter_t *lft_it, bson_iter_t *rgt_it);
bool getBsonValue(std::string *val, const bson_t *bson, const std::string &key);
bool getBsonValues( std::vector<std::string> *list);
bool getBsonValues(const bson_t *bson, const std::string &key, std::vector<std::string> *list);

void get_column_values(bson_iter_t *iter, const std::string &name, std::vector<Column> *records,
                    			int hash_bucket, UpdateCmd type, FilterFlag flag = FilterFlag::kFilterTypeNone);

bool bson_compare_base_value(bson_iter_t *lft_it, bson_iter_t *rgt_it);

bool bson_compare_value(bson_iter_t *lft_it, bson_iter_t *rgt_it);

bool parserQueryResult(bson_iter_t *lft_it, bson_iter_t *rgt_it);
#endif