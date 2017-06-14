#ifndef SSDB_CMDINFO_H_
#define SSDB_CMDINFO_H_

#include <map>
#include <vector>
#include "bson/bson.h"
#include "const.h"
#include "strings.h"
#include "log.h"
#include "bson_tool.h"
#include "bytes.h"
#include "../db/iterator.h"
typedef std::vector<Column> Columns;

class CltCmdInfo {
public:
	enum ScanIndexForward{
		kGet = 1,
		kDelete = 2,
		kUpdate = 3
	};
	//聚集的语法 clt_xxx name <query_bson_> <update_bson_> <condition>
	bson_t *option_data_ = nullptr;		//聚集的表格配置
	bson_t *store_data_  = nullptr;		//已经落地的bson数据，更新的时候分析用
	bson_t *query_bson_  = nullptr;		//query的bson语法
	bson_t *update_bson_ = nullptr;		//update的bson语法(insert时候，bson串当成是update_bson_)

	//map容器存的是索引和主键的信息
	//map的key用于区分是主键或者索引或者空间统计标识
	//map的value是个list集合，元素是每个索引/主键的信息（名字，值，操作类型（set/unset/pull/push）等）
	std::map<char, Columns> clt_configs_;	
	std::map<char, Columns> clt_querys_;
	std::map<char, Columns> clt_updates_;


	Condition opts;						//condition，有cursor/limit/multi等信息
	bool need_update_key_ = false;			//更新命令是否涉及更新主键

	TMH metainfo;
	Bytes name;
	ScanIndexForward forward_;
public:
	~CltCmdInfo();
	void ResetCmdInfo(bson_t *query, bson_t *update);
	//初始化cmdinfo类，获取query_bson_,update_bson_中的主键信息，索引信息，空间统计信息
	int InitCltOption(bson_t *config, bson_t *query, bson_t *update);
	int64_t Version() {
		return metainfo.version;
	}
	Bytes Name() {
		return name;
	}
	int64_t Refcount() {
		return metainfo.refcount;
	}
	void DecrRefcount(){
		metainfo.refcount--;
	}
	void IncrRefcount(int64_t n = 1){
		metainfo.refcount += n;
	}
	//取得聚集的配置信息，例如主键有哪些，索引有哪些
	//type：标识是需要找主键或者找索引等
	const std::vector<Column>& GetCltOptionCols(char type) {
		return clt_configs_[type];
	}

	//取得query_bson_中的index数据集合（有顺序）
	const std::vector<Column>& GetQueryIndexCols() {
		return clt_querys_[ColumnType::kColIndex];
	}

	//返回query_bson_中是否有index信息
	bool QueryHasIndexs() {
		if (clt_querys_.find(ColumnType::kColIndex) == clt_querys_.end()) {
			return false;
		}
		return !clt_querys_[ColumnType::kColIndex].empty();
	}
	// std::string GetUpdateUniqueIndex(std::string &name){
	// 	if (clt_updates_[ColumnType::kColUniqueIndex].empty()){
	// 		return std::string();
	// 	}
	// 	int size = clt_updates_[ColumnType::kColUniqueIndex].size();
	// 	for (int i = 0; i < size; ++i) {
	// 		if (clt_updates_[ColumnType::kColUniqueIndex][i].name == name){
	// 			return clt_updates_[ColumnType::kColUniqueIndex][i].value;
	// 		}
	// 	}
	// 	return std::string();
	// }
	bool Skip(CltKeyIterator *it);
	//取得update_bson_中的index数据集合（有顺序）
	const std::vector<Column>& GetUpdateIndexCols() {
		return clt_updates_[ColumnType::kColIndex];
	}
	const std::vector<Column>& GetUpdateUniqueIndexCols() {
		return clt_updates_[ColumnType::kColUniqueIndex];
	}

	//update_bson_是否存在name对应的索引域信息
	bool IsUpdateExistIndexField(const std::string &name, ColumnType type);

	//query_bson_是否含有所有主键
	bool QueryHasFullKeys();

	//update_bson_是否含有所有主键
	bool UpdateHasFullKeys();

	//update_bson_是否含有某一个主键/是否有主键需要更新
	bool NeedUpdateKey();

	bool UpdateHasUniqueIndex() {
		return !clt_updates_[ColumnType::kColUniqueIndex].empty();
	}
	//返回更新主键是否合法，因为根据索引更新批量数据，不能更新最后一个主键
	bool IsUpdatePkeysValid();

	//如果传入的name与需要更新就设置对应的val值，并且返回ture，返回返回false
	bool GetUpdatePkeyValue(const std::string &name, std::string *val);

	//组装query_bson_中的主键信息
	//pkey count + key1 length + key1 val + key2 length + key2 val + ...
	std::string GetQueryPrimaryKey() {
		return packPkeysValue(clt_querys_);
	}

	//组装update_bson_中的主键信息
	//pkey count + key1 length + key1 val + key2 length + key2 val + ...
	std::string GetUpdatePrimaryKey() {
		return packPkeysValue(clt_updates_);
	}

	void IncrStatSize(bson_t *store, int incr);
    void IncrStatSize(const char *column_key, int64_t column_value);
    void UpdateStatSize(bson_t *store, bson_iter_t *iter, int incr);

	//主键比对，因为主键可能是默认值，不存在store数据中
	bool FilterQueryValue(const std::string &value);

	//生成一个新的落地主键信息
	std::string CreateNewKey(bson_t *bson, bool *update);

	//修改落地的bson数据，生成一个新的bson数据
	//oldData与返回的bson_data都需要手动释放内存
	bson_t *CreateNewStoreData(bson_t *oldData);

	bool CheckRemoveIndexValid(const Column &record, ColumnType type);

	//根据filter信息过滤需要重新落地的索引信息
	void GetUpdateIndexCols(std::set<Column> *v, std::set<Column> &filter, ColumnType type);
	int GetPrimaryKeys(bson_t *quert);
private:
	bool InitCltCmdInfo(bson_iter_t *iter, ColumnType type);
	std::string packPkeysValue(std::map<char, std::vector<Column>> &query);
	std::string primary_key_name_;

	//根据$set命令的信息，修改src_bson中的相应信息，返回一个新的bson串，src在函数内被释放了，以下方法都是类似的操作
	bson_t* setCmdOption(bson_t *src, bson_iter_t *find_iter);
	bson_t* unsetCmdOption(bson_t *src, bson_iter_t *find_iter);
	bson_t* incrbyCmdOption(bson_t *src, bson_iter_t *find_iter);
	bson_t* pullAllCmdOption(bson_t *src, bson_iter_t *find_iter);
	bson_t* pullCmdOption(bson_t *src, bson_iter_t *find_iter);
	bson_t* addToSetCmdOption(bson_t *src, bson_iter_t *find_iter);
	bson_t* pushCmdOption(bson_t *src, bson_iter_t *find_iter);
	bson_t* setCmdNormal(bson_t *src, bson_iter_t *find_iter);
};
#endif
