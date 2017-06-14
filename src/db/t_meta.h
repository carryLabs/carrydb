#ifndef _UTIL_COMPOSE_H_
#define _UTIL_COMPOSE_H_

#include "../include.h"
#include "../util/const.h"
#include "../util/slots.h"

/*
 *  slotnum(4 byte) + namelen(1 byte) + name
 */
static inline
std::string EncodeMetaKey(const Bytes &name){
	std::string buf;
	unsigned int slot;
	
	slot = big_endian((Slots::slots_num(name.String().c_str(), NULL, NULL)));

	buf.append(1, DataType::METAL);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(name.data(), name.size());
	return buf;
}

static inline 
std::string EncodeMetaKeyPrefix(const Bytes &name, unsigned int slot){
	std::string buf;
	slot = big_endian(slot);
	buf.append(1, DataType::METAL);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(name.data(), name.size());
	return buf;
}


static inline
std::string EncodeMetaVal(TMH &tag, const Bytes &value = Bytes("")){
	std::string buf;

	buf.append((char*)&tag, sizeof(TMH));

	if (tag.datatype == DataType::KSIZE){
		buf.append(value.data(), value.size());
	}

	return buf;
}

static inline
int DecodeMetaKey(const Bytes &slice, std::string *name, unsigned int *slot){
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if(decoder.read_char(&ch) == -1
			|| ch != DataType::METAL){
		return -1;
	}

	if(decoder.read_uint32_bigendian(slot) == -1){
		return -1;
	}

	if(decoder.read_data(name) == -1){
		return -1;
	}
	return 0;
}

static inline 
std::string EncodeDataValue(const Bytes &val, ValueType option = ValueType::kNone, int64_t *version = nullptr) {
	std::string buf;

	TVH header = {0};
	//加ValueType的意义是，新起一个从服务时，唯一索引不需要进行CopyData操作。
	header.reserve[0] = option;

	if (version){
		int64_t ver = *version;
		header.reserve[1] = ValueType::kVersion;
		ver = encode_version(ver);

		buf.append((char*)&header, sizeof(TVH));
		buf.append((char*)&ver, sizeof(int64_t));
	}else {
		buf.append((char*)&header, sizeof(TVH));
	}
	
	buf.append(val.data(), val.size());
	return buf;
}

static inline
int DecodeDataValue(char type, const Bytes &data, std::string *val, TVV *val_head = nullptr){
	val->clear();
	Decoder decoder(data.data(), data.size());//浅拷贝

	uint32_t head_len;
	if (type == DataType::METAL || type == DataType::KV || type == DataType::KSIZE){//String类型与其他类型的value前缀不一样
		head_len = sizeof(TMH);
	}else {
		head_len = sizeof(TVH);
	}
	if (data.size() < head_len){
		if (type != DataType::TABLE){//collection 的index只有key
			log_error("decode_data_value len error :%d %s", head_len, data.data());
		}
		return -1;
	}

	if (head_len == sizeof(TVH)){
		TVH valinfo = {0};
		int64_t version = 0;
		if (decoder.read_tag_val_header(&valinfo) == -1){
			return -1;
		}

		if (valinfo.reserve[1] == ValueType::kVersion){
			if(decoder.read_int64(&version) == -1){
				return -1;
			}else{
				version = decode_version(version);
			}
		}
		if (val_head){
			val_head->tag_header = valinfo;
			val_head->version = version;
		}
	}else{
		decoder.read_tag_meta_header(nullptr);
	}

	return decoder.read_data(val);
}

static inline
void initVersion(TMH &metainfo, uint32_t ret, char type){
	if (ret == 0){
		metainfo.version = time_us();
		metainfo.datatype = type;
		// if (type == DataType::TABLE){
		// 	metainfo.reserve[0] = type;
		// }
	}
}
#endif
