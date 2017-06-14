#ifndef SSDB_LIST_H_
#define SSDB_LIST_H_

#include "ssdb_impl.h"

const uint64_t QFRONT_SEQ = 2;
const uint64_t QBACK_SEQ  = 3;
const uint64_t QITEM_MIN_SEQ = 10000;
const uint64_t QITEM_MAX_SEQ = 9223372036854775807ULL;
const uint64_t QITEM_SEQ_INIT = QITEM_MAX_SEQ/2;

inline static
std::string EncodeListKey(const Bytes &name, const Bytes &prefix, int64_t version){
	std::string buf;
	unsigned int slot;

	slot = big_endian((Slots::slots_num(name.String().c_str(), NULL, NULL)));

	buf.append(1, DataType::LIST);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());

	version = encode_version(version);
	buf.append((char*)&version, sizeof(int64_t));

	buf.append(prefix.data(), prefix.size());

	return buf;
}

inline static
std::string EncodeListKey(const Bytes &name, uint64_t seq, int64_t version){
	std::string buf;
	unsigned int slot;
	slot = big_endian((Slots::slots_num(name.String().c_str(), NULL, NULL)));

	buf.append(1, DataType::LIST);
	buf.append((char*)&slot, sizeof(unsigned int));
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	
	version = encode_version(version);
	buf.append((char*)&version, sizeof(int64_t));

	seq = big_endian(seq);
	buf.append((char *)&seq, sizeof(uint64_t));

	return buf;
}

inline static
int DecodeListKey(const Bytes &slice, std::string *name, uint64_t *seq, 
					int64_t *version, uint32_t *slot){
	Decoder decoder(slice.data(), slice.size());
	char ch = 0;
	if(decoder.read_char(&ch) == -1
			|| ch != DataType::LIST){
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

	if(decoder.read_uint64_bigendian(seq) == -1){
		return -1;
	}

	return 0;
}

#endif
