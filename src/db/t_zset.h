#ifndef SSDB_ZSET_H_
#define SSDB_ZSET_H_

#include "ssdb_impl.h"


static inline
std::string EncodeSortSetScoreKey(const Bytes &name, const Bytes &key, const Bytes &score, int64_t version){
	std::string buf;
	unsigned int slot;

	slot = big_endian((Slots::slots_num(name.String().c_str(), NULL, NULL)));

	buf.append(1, DataType::ZSCORE);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());

	version = encode_version(version);
	buf.append((char*)&version, sizeof(int64_t));

	int64_t s = score.Int64();
	if(s < 0){
		buf.append(1, '-');
	}else{
		buf.append(1, '=');
	}
	s = encode_score(s);

	buf.append((char *)&s, sizeof(int64_t));
	buf.append(1, '=');
	buf.append(key.data(), key.size());

	return buf;
}

//DataType::ZSCORE slot(4byte) namelen(1byte) name '-'/'='(1byte) score(8byte) '='(1byte) key
static inline
int DecodeSortSetScoreKey(const Bytes &slice, std::string *name, std::string *key, 
			std::string *score, int64_t *version, unsigned int *slot){
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if (decoder.read_char(&ch) == -1 || ch != DataType::ZSCORE){
		return -1;
	}

	if(decoder.read_uint32_bigendian(slot) == -1){
		return -1;
	}
	if(decoder.read_8_data(name) == -1){
		return -1;
	}
	
	if(decoder.read_int64(version) == -1){
		return -1;
	}else{
		if (version != NULL){
			*version = decode_version(*version);
		}
	}

	if(decoder.skip(1) == -1){
		return -1;
	}
	int64_t s;
	if(decoder.read_int64(&s) == -1){
		return -1;
	}else{
		if(score != NULL){
			s = decode_score(s);
			char buf[21] = {0};
			snprintf(buf, sizeof(buf), "%" PRId64 "", s);
			score->assign(buf);
		}
	}
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(key) == -1){
		return -1;
	}
	return 0;
}

static inline
std::string EncodeSortSetKey(const Bytes &name, const Bytes &key, int64_t version){
	std::string buf;
	unsigned int slot;
	
	slot = big_endian((Slots::slots_num(name.String().c_str(), NULL, NULL)));
	buf.append(1, DataType::ZSET);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	
	version = encode_version(version);
	buf.append((char*)&version, sizeof(int64_t));

	buf.append(key.data(), key.size());

	return buf;
}

static inline
int DecodeSortSetKey(const Bytes &slice, std::string *name, std::string *key, int64_t *version, unsigned int *slot){
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if(decoder.read_char(&ch) == -1
			|| ch != DataType::ZSET){
		return -1;
	}
	if(decoder.read_uint32_bigendian(slot) == -1){
		return -1;
	}
	if(decoder.read_8_data(name) == -1){
		return -1;
	}

	if(decoder.read_int64(version) == -1){
		return -1;
	}else{
		if (version != nullptr){
			*version = decode_version(*version);
		}
	}
	
	if(decoder.read_data(key) == -1){
		return -1;
	}

	return 0;
}

#endif
