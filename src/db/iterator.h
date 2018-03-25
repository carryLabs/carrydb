/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ITERATOR_H_
#define SSDB_ITERATOR_H_

#include <inttypes.h>
#include <string>
#include "../util/const.h"
#include "../util/bytes.h"
#include "../util/log.h"
class SSDB;
namespace leveldb{
	class Iterator;
}

enum IteratorType{
	kRawIter = 0,
	kKIter,
	kHIter,
	kZIter,
	kQIter,
	kTblPkeyIter,
	kTblIdxIter,
	kMetaIter
};

inline 
static IteratorType dataType2IterType2(char dtType){
	switch(dtType){
		case DataType::KSIZE:
			return kKIter;
		case DataType::HSIZE:
			return kHIter;
		case DataType::ZSIZE:
			return kZIter;
		case DataType::LSIZE:
			return kQIter;
	}
	return kRawIter;
}

class Iterator{
	public:
		enum Direction{
			FORWARD, 
			BACKWARD
		};

		//需要实现,怎么解析获得的key与value
		virtual int decode_key() = 0;
		virtual int decode_value() = 0;
		virtual bool next() = 0;
	public:
		Iterator(leveldb::Iterator *it, const std::string &end, 
				uint64_t limit, Direction direction);
		virtual ~Iterator();
		bool skip(uint64_t offset);
		void seek(const std::string &start);
		void return_val(bool onoff){
			this->return_val_ = onoff;
		}
		
		//kv(name) zset(name) hash(name) queue(name)
		std::string name() { return name_; }
		//kv(name) zset(subkey) hash(subkey)
		virtual std::string key() { return key_; }
		//kv(value) zset(subkey) hash(value)  queue(value)
		virtual std::string value() { return value_; }
		//zset(score)
		std::string score() { return score_; }
		int64_t version() { return version_; }
		//queue(seq)
		uint64_t seq() { return seq_; }
		char dataType() { return type_; }
		Bytes rawkey();
		Bytes rawvalue();
		bool valid();
		void seekToLast();
		void seekToFirst();
		void prev();
	protected:
		bool walk();
		leveldb::Iterator *it;
		
		int64_t version_ = 0;
		uint32_t slot_;
		uint64_t limit_;
		bool return_val_ = true;

		std::string name_;		
		std::string score_;
		std::string key_;
		std::string value_;
		uint64_t seq_;
	
		std::string start;
		std::string end;
		bool is_first;
		int direction;
		char type_;
};

class RawIterator : public Iterator
{
public:
	RawIterator(leveldb::Iterator *it, const std::string &end,
				uint64_t limit, Direction direction);
	int decode_key();
	int decode_value();
	bool next();
};

class MetalIterator : public Iterator
{
public:
	MetalIterator(leveldb::Iterator *it, const char type, const std::string &end,
				uint64_t limit, Direction direction);
	~MetalIterator();
	virtual bool next();
	virtual int decode_key();
	virtual int decode_value();
};

class KIterator : public Iterator
{
	public:				
		KIterator(leveldb::Iterator *it, const Bytes &name, int64_t version,
			const std::string &end, uint64_t limit, Direction direction);
		~KIterator();
		bool next();
	private:
		int decode_key();
		int decode_value();
};

class HIterator: public Iterator
{
	public:
		HIterator(
			leveldb::Iterator *it, const Bytes &name, int64_t version,
					const std::string &end, uint64_t limit, Direction direction);
		~HIterator();
		
		bool next();
	private:
		int decode_key();
		int decode_value();
};

class ZIterator: public Iterator
{
	public:
		ZIterator(leveldb::Iterator *it, const Bytes &name, int64_t version,
					const std::string &end, uint64_t limit, Direction direction);
		~ZIterator();

		bool next();
	private:
		int decode_key();
		int decode_value();
};

class QIterator: public Iterator
{
	public:
		QIterator(leveldb::Iterator *it, const Bytes &name, int64_t version,
					const std::string &end, uint64_t limit, Direction direction);

		~QIterator();
		bool next();
	private:
		int decode_key();
		int decode_value();
};

class IteratorFactory
{
public:
	// return (start, end], not include start
	static Iterator* iterator(const SSDB *ssdb, const char dataType, const std::string &start, const std::string &end, uint64_t limit);
	static Iterator* rev_iterator(const SSDB *ssdb, const char dataType, const std::string &start, const std::string &end, uint64_t limit);

	// return (start, end], not include start
	static Iterator* iterator(const SSDB *ssdb, IteratorType dtType, const Bytes &name, int64_t version,
							const std::string &start, const std::string &end, uint64_t limit);
	static Iterator* rev_iterator(const SSDB *ssdb, IteratorType dtType, const Bytes &name, int64_t version,
							const std::string &start, const std::string &end, uint64_t limit);
// private:
	static leveldb::Iterator *internal_iterator(const SSDB *ssdb, const std::string &start, Iterator::Direction direct);
    static Iterator *createIter(IteratorType dtType, leveldb::Iterator *it, const Bytes &name, int64_t version, 
    							const std::string &start, const std::string &end, uint64_t limit, Iterator::Direction direct);
};

#endif
