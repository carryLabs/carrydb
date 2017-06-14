#ifndef SSDB_HASH_H_
#define SSDB_HASH_H_

#include "ssdb_impl.h"

static inline
std::string EncodeHashKey(const Bytes &name, const Bytes &key, int64_t version){
	std::string buf;
	unsigned int slot;

	slot = big_endian((Slots::slots_num(name.String().c_str(), NULL, NULL)));
	buf.append(1, DataType::HASH);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	
	version = encode_version(version);
	buf.append((char*)&version, sizeof(int64_t));

	buf.append(key.data(), key.size());

	return buf;
}

static inline
int DecodeHashKey(const Bytes &slice, std::string *name, std::string *key, int64_t *version, unsigned int *slot){
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if(decoder.read_char(&ch) == -1
			|| ch != DataType::HASH){
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
