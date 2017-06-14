#ifndef SSDB_TABLE_H_
#define SSDB_TABLE_H_

#include "../util/cmdinfo.h"
#include "ssdb_impl.h"
#include "t_meta.h"

#define CHECK_TABLE_EXIST(ret) do{ \
		if (ret < 0) { \
			return ret; \
		} else if (ret == 0) { \
			return CmdStatus::kCltNotFoundConfig; \
		} \
	}while(0)

#define CHECK_TABLE_ISREBUILDING(type) do{ \
		if (type == ValueType::kReBuild) { \
			return CmdStatus::kCltReBuilding; \
		} \
	}while(0)

static inline bson_t *DecodeDataBson(const Bytes &dataval) {
	std::string value;
	if (DecodeDataValue(DataType::TABLE, dataval, &value) == -1) {
		return nullptr;
	}
	bson_t *store_bson = parser_bson_from_data(value.data(), value.size());
	if (!store_bson) {
		return nullptr;
	}
	return store_bson;
}

static inline
std::string EncodeCltMetalVal(TMH &metainfo, bson_t *option) {
	std::string buf;

	buf.append((char*)&metainfo, sizeof(TMH));


	uint32_t len = big_endian((uint32_t)option->len);//size_t = unsigned int
	buf.append((char*)&len, sizeof(uint32_t));
	buf.append((char *)bson_get_data(option), option->len);
	return buf;
}
static inline
int DecodeCltMetaVal(const Bytes &slice, std::string *value) {
	Decoder decoder(slice.data(), slice.size());

	if (decoder.read_uint32_data(value) == -1) {
		return -1;
	}

	return 0;
}

static inline
std::string EncodeCltPrefixKey(const Bytes &name, int64_t version) {
	unsigned int slot;
	std::string buf;
	slot = big_endian((Slots::slots_num(name.String().c_str(), NULL, NULL)));
	buf.append(1, DataType::TABLE);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	version = encode_version(version);
	buf.append((char*)&version, sizeof(int64_t));

	return buf;
}

static inline
std::string EncodeCltPrimaryKey(const Bytes &name, const Bytes &pkey, int64_t version) {
	std::string buf = EncodeCltPrefixKey(name, version);
	buf.append(1, ColumnType::kColPrimary);
	buf.append(pkey.data(), pkey.size());
	return buf;
}

static inline
std::string EncodeCltPrimaryKey(const Bytes &prefix, const Bytes &pkey) {
	std::string buf;
	buf.append(prefix.data(), prefix.size());
	buf.append(1, ColumnType::kColPrimary);
	buf.append(pkey.data(), pkey.size());
	return buf;
}

static inline
std::string EncodeCltPrimaryVal(bson_t *data) {
	std::string buf;

	TVH th = {0};

	buf.append((char*)&th, sizeof(TVH));
	if (data) {
		buf.append((char *)bson_get_data(data), data->len);
	}
	return buf;
}

static inline 
std::string EncodeCltUniqueVal(const Bytes &value){
	std::string buf; 
	return buf;
}


static inline
std::string EncodeCltIndexPrefix(const Bytes &name, int64_t version, const Column &index) {
	std::string buf = EncodeCltPrefixKey(name, version);

	//加这个字节是为了让fid集合排在前面
	buf.append(1, ColumnType::kColIndex);
	//buf.append(1, 1);

	buf.append(1, (uint8_t)index.hash_bucket);
	buf.append(1, (uint8_t)index.value.size());
	buf.append(index.value.data(), index.value.size());
	return buf;
}



static inline
std::string EncodeCltIndexKey(const Bytes &name, const Bytes &key, const Column &index, int64_t version) {
	std::string buf = EncodeCltPrefixKey(name, version);

	//加这个字节是为了让fid集合排在前面
	buf.append(1, ColumnType::kColIndex);
	//buf.append(1, 1);//为了能够使用联合索引，在这里保留一个字段写index联合索引个数

	buf.append(1, (uint8_t)index.hash_bucket);
	buf.append(1, (uint8_t)index.value.size());
	buf.append(index.value.data(), index.value.size());
	buf.append(key.data(), key.size());
	return buf;
}

static inline
std::string EncodeCltUniqueIndexKey(const Bytes &name, const Column &index, int64_t version) {
	std::string buf = EncodeCltPrefixKey(name, version);
	//加这个字节是为了让fid集合排在前面
	buf.append(1, ColumnType::kColUniqueIndex);
	//buf.append(1, 1);//为了能够使用联合索引，在这里保留一个字段写index联合索引个数

	buf.append(1, (uint8_t)index.hash_bucket);
	buf.append(1, (uint8_t)index.value.size());
	buf.append(index.value.data(), index.value.size());
	return buf;
}

static inline
std::string EncodeCltIndexKey(const Bytes &prefix, const Bytes &key, const Column &index) {
	std::string buf;
	buf.append(prefix.data(), prefix.size());
	//加这个字节是为了让fid集合排在前面jia
	buf.append(1, ColumnType::kColIndex);
	//buf.append(1, 1);
	buf.append(1, (uint8_t)index.hash_bucket);
	buf.append(1, (uint8_t)index.value.size());
	buf.append(index.value.data(), index.value.size());
	buf.append(key.data(), key.size());
	return buf;
}

static inline
int DecodeCltPrimaryKey(const Bytes &slice, std::string *name, std::string *key,
                           int64_t *version, unsigned int *slot) {
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if (decoder.read_char(&ch) == -1 || ch != DataType::TABLE) {
		return -1;
	}
	if (decoder.read_uint32_bigendian(slot) == -1) {
		return -1;
	}
	if (decoder.read_8_data(name) == -1) {
		return -1;
	}

	if (decoder.read_int64(version) == -1) {
		return -1;
	} else {
		if (version != nullptr) {
			*version = decode_version(*version);
		}
	}

	if (decoder.read_char(&ch) == -1 || ch != ColumnType::kColPrimary) {
		return -1;
	}

	if (decoder.read_data(key) == -1) {
		return -1;
	}

	return 0;
}

static inline
int decode_clt_index_key(const Bytes &slice, std::string *name, std::string *index,
                         int64_t *version, std::string *pkey, unsigned int *slot) {
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if (decoder.read_char(&ch) == -1 || ch != DataType::TABLE) {
		log_error("decode index error 1:%s", hexmem(slice.data(), slice.size()).data());
		return -1;
	}
	if (decoder.read_uint32_bigendian(slot) == -1) {
		log_error("decode index error 2:%s", hexmem(slice.data(), slice.size()).data());
		return -1;
	}
	if (decoder.read_8_data(name) == -1) {
		log_error("decode index error 3:%s", hexmem(slice.data(), slice.size()).data());
		return -1;
	}

	if (decoder.read_int64(version) == -1) {
		log_error("decode index error 4:%s", hexmem(slice.data(), slice.size()).data());
		return -1;
	} else {
		if (version != nullptr) {
			*version = decode_version(*version);
		}
	}

	if (decoder.read_char(&ch) == -1 || ch != ColumnType::kColIndex) {
		log_error("decode index error 5:%s %d", hexmem(slice.data(), slice.size()).data(), ch);
		return -1;
	}
	uint8_t hash_bucket;
	if (decoder.read_uint8(&hash_bucket) == -1) {
		log_error("decode index error 6:%s", hexmem(slice.data(), slice.size()).data());
		return -1;
	}

	if (decoder.read_8_data(index) == -1) {
		log_error("decode index error 7:%s", hexmem(slice.data(), slice.size()).data());
		return -1;
	}

	if (decoder.read_data(pkey) == -1) {
		log_error("decode index error 8:%s", hexmem(slice.data(), slice.size()).data());
		return -1;
	}

	return 0;
}

static inline
int decode_clt_prefix_key(const Bytes &slice, std::string *name, int64_t *version, unsigned int *slot) {
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if (decoder.read_char(&ch) == -1 || ch != DataType::TABLE) {
		return -1;
	}
	if (decoder.read_uint32_bigendian(slot) == -1) {
		return -1;
	}
	if (decoder.read_8_data(name) == -1) {
		return -1;
	}

	if (decoder.read_int64(version) == -1) {
		return -1;
	} else {
		if (version != nullptr) {
			*version = decode_version(*version);
		}
	}

	return 0;
}

#endif
