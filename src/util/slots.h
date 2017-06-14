/*
 *文件名称: slots.h
 *文件描述: slots相关操作
 *时    间: 2016-04-19
 *作    者: YPS
 *版    本: 0.1
 */

#ifndef UTIL__SLOTS_H
#define UTIL__SLOTS_H

#include "bytes.h"

#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

class Slots
{
	private:
		static const void *slots_tag(const char *key, int *plen);

	public:
		static unsigned int slots_num(const char *key, unsigned int *pcrc, int *phastag);
		static std::string encode_slot_id(const Bytes &name);
        static std::string encode_slot_id(unsigned int slotnum);

};

#endif
