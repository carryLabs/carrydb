/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_BYTES_H_
#define UTIL_BYTES_H_

#include "leveldb/slice.h"
#include "strings.h"
#include "const.h"

// readonly
// to replace std::string
class Bytes{
	private:
		const char *data_;
		int size_;
	public:
		Bytes(){
			data_ = "";
			size_ = 0;
		}

		Bytes(void *data, int size){
			data_ = (char *)data;
			size_ = size;
		}

		Bytes(const char *data, int size){
			data_ = data;
			size_ = size;
		}

		Bytes(const std::string &str){
			data_ = str.data();
			size_ = (int)str.size();
		}

		Bytes(const leveldb::Slice &slice){
			data_ = slice.data();
			size_ = slice.size();
		}

		Bytes(const char *str){
			data_ = str;
			size_ = (int)strlen(str);
		}

		const char* data() const{
			return data_;
		}

		int skip(int n){
			if(size_ < n){
				return -1;
			}
			data_ += n;
			size_ -= n;
			return n;
		}


		bool empty() const{
			return size_ == 0;
		}

		int size() const{
			return size_;
		}

		int compare(const Bytes &b) const{
			const int min_len = (size_ < b.size_) ? size_ : b.size_;
			int r = memcmp(data_, b.data_, min_len);
			if(r == 0){
				if (size_ < b.size_) r = -1;
				else if (size_ > b.size_) r = +1;
			}
			return r;
		}
		
		int comparestrnocase(const Bytes &b) const{
			return strcasecmp(data_, b.data_);
		}

		leveldb::Slice Slice() const{
			return leveldb::Slice(data_, size_);
		}

		std::string String() const{
			return std::string(data_, size_);
		}

		int Int() const{
			return str_to_int(data_, size_);
		}

		int64_t Int64() const{
			return str_to_int64(data_, size_);
		}

		uint64_t Uint64() const{
			return str_to_uint64(data_, size_);
		}

		double Double() const{
			return str_to_double(data_, size_);
		}
};

inline
bool operator==(const Bytes &x, const Bytes &y){
	return ((x.size() == y.size()) &&
			(memcmp(x.data(), y.data(), x.size()) == 0));
}

inline
bool operator!=(const Bytes &x, const Bytes &y){
	return !(x == y);
}

inline
bool operator>(const Bytes &x, const Bytes &y){
	return x.compare(y) > 0;
}

inline
bool operator>=(const Bytes &x, const Bytes &y){
	return x.compare(y) >= 0;
}

inline
bool operator<(const Bytes &x, const Bytes &y){
	return x.compare(y) < 0;
}

inline
bool operator<=(const Bytes &x, const Bytes &y){
	return x.compare(y) <= 0;
}



class Buffer{
	private:
		char *buf;
		char *data_;
		int size_;
		int total_;
		//int origin_total;
	public:
		Buffer(int total);
		~Buffer();

		// 缓冲区大小
		int total() const{ 
			return total_;
		}

		bool empty() const{
			return size_ == 0;
		}

		// 数据
		char* data() const{
			return data_;
		}

		// 数据大小
		int size() const{
			return size_;
		}

		// 指向空闲处
		char* slot() const{
			return data_ + size_;
		}

		int space() const{
			return total_ - (int)(data_ - buf) - size_;
		}

		void incr(int num){
			size_ += num;
		}

		void decr(int num){
			size_ -= num;
			data_ += num;
		}

		// 保证不改变后半段的数据, 以便使已生成的 Bytes 不失效.
		void nice();
		// 扩大缓冲区
		int grow();
		// 缩小缓冲区, 如果指定的 total 太小超过数据范围, 或者不合理, 则不会缩小
		void shrink(int total=0);
		
		std::string stats() const;
		int read_record(Bytes *s);

		int append(char c);
		int append(const char *p);
		int append(const void *p, int size);
		int append(const Bytes &s);

		int append_record(const Bytes &s);
};


class Decoder{
private:
	const char *p;
	int size;
	Decoder(){}
public:
	Decoder(const char *p, int size){
		this->p = p;
		this->size = size;
	}
	int skip(int n){
		if(size < n){
			return -1;
		}
		p += n;
		size -= n;
		return n;
	}

	int read_int64(int64_t *ret){
		if(size_t(size) < sizeof(int64_t)){
			return -1;
		}
		if(ret){
			*ret = *(int64_t *)p;
		}
		p += sizeof(int64_t);
		size -= sizeof(int64_t);
		return sizeof(int64_t);
	}
	int read_uint64(uint64_t *ret){
		if(size_t(size) < sizeof(uint64_t)){
			return -1;
		}
		if(ret){
			*ret = *(uint64_t *)p;
		}
		p += sizeof(uint64_t);
		size -= sizeof(uint64_t);
		return sizeof(uint64_t);
	}
	int read_data(std::string *ret){
		int n = size;
		if(ret){
			ret->assign(p, size);
		}
		p += size;
		size = 0;
		return n;
	}

	int read_tag_val_header(TVH *ret){
		if(size_t(size) < sizeof(TVH)){
			return -1;
		}
		if (ret){
			*ret = *(TVH *)p;
		}
		p += sizeof(TVH);
		size -= sizeof(TVH);
		return sizeof(TVH);
	}

	int read_tag_meta_header(TMH *ret){
		if(size_t(size) < sizeof(TMH)){
			return -1;
		}
		if (ret){
			*ret = *(TMH *)p;
		}
		p += sizeof(TMH);
		size -= sizeof(TMH);
		return sizeof(TMH);
	}

	int read_uint32_data(std::string *ret){
		if (size < sizeof(uint32_t)){
			return -1;
		}

		uint32_t len;
		if (read_uint32_bigendian(&len) == -1){
			return -1;
		}

		if(size < len){
			return -1;
		}
		if (ret){
			ret->assign(p, len);
		}
		p += len;
		size -= len;
		return sizeof(uint32_t) + len;
	}

	int read_uint16_data(std::string *ret){
		if(size < sizeof(uint16_t)){
			return -1;
		}

		uint16_t len;
		if (read_uint16_bigendian(&len) == -1){
			return -1;
		}

		if (size < len){
			return -1;
		}
		if (ret){
			ret->assign(p, len);
		}

		p += len;
		size -= len;
		return sizeof(uint16_t) + len;
	}

	int read_8_data(std::string *ret=NULL){
		if(size < 1){
			return -1;
		}
		int len = (uint8_t)p[0];
		p += 1;
		size -= 1;
		if(size < len){
			return -1;
		}
		if(ret){
			ret->assign(p, len);
		}
		p += len;
		size -= len;
		return 1 + len;
	}

	int read_char(char *ch){
		if(size < 1){
			return -1;
		}
		*ch = (char)p[0];
		p += 1;
		size -= 1;
		return 1;
	}
	int read_uint8(uint8_t *ret){
		if (size < 1){
			return -1;
		}
		
		if (ret){
			*ret = (uint8_t)p[0];
		}
		p += 1;
		size -= 1;
		return 1;
	}
	int read_uint16_bigendian(uint16_t *ret){
		if(size < sizeof(uint16_t)){
			return -1;
		}
		if(ret){
			*ret = big_endian((*(uint16_t*)p));
		}
		p += sizeof(uint16_t);
		size -= sizeof(uint16_t);
		return sizeof(uint16_t);
	}

	int read_uint32_bigendian(uint32_t *ret){
		if(size < sizeof(uint32_t)){
			return -1;
		}
		if(ret){
			
			*ret = bigendian_uint32((*(uint32_t*)p));
		}
		p += sizeof(uint32_t);
		size -= sizeof(uint32_t);
		return sizeof(uint32_t);
	}

	int read_uint64_bigendian(uint64_t *ret){
		if(size < sizeof(uint64_t)){
			return -1;
		}
		if(ret){
			*ret = bigendian_uint64((*(uint64_t*)p));
		}
		p += sizeof(uint64_t);
		size -= sizeof(uint64_t);
		return sizeof(uint64_t);
	}
};

#endif

