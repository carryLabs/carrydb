/*
 *文件名称: slots.cpp
 *文件描述: slots相关操作
 *时    间: 2016-04-19
 *作    者: YPS
 *版    本: 0.1
 */

#include "slots.h"
#include "crc32.h"
#include "const.h"
const void *Slots::slots_tag(const char *key, int *plen)
{
	int i, j, n = strlen(key);
	const char *s = key;

	for (i = 0; i < n && s[i] != '{'; i ++) {}

    if (i == n)
	{
        return NULL;
    }

    i++;

    for (j = i; j < n && s[j] != '}'; j ++) {}

    if (j == n)
	{
        return NULL;
    }

    if (plen != NULL)
	{
        *plen = j - i;
    }

    return s + i;
}

unsigned int Slots::slots_num(const char *key, unsigned int *pcrc, int *phastag)
{
    int taglen;
    int hastag = 0;
    const void *tag = slots_tag(key, &taglen);

    if (tag == NULL)
    {
		tag = (const void*)key;
		taglen = strlen(key);
    }
	else
	{
        hastag = 1;
    }

    unsigned int crc = Crc32::crc32_checksum((const char*)tag, taglen);
    if (pcrc != NULL)
	{
        *pcrc = crc;
    }
    if (phastag != NULL)
	{
        *phastag = hastag;
    }

    return crc & HASH_SLOTS_MASK;
}

std::string Slots::encode_slot_id(const Bytes &name) {
    unsigned int slot = big_endian(slots_num(name.data(), NULL, NULL));
    std::string sKey;
    sKey.append(1, DataType::SLOTID);
    sKey.append((char*)&slot, sizeof(unsigned int));
    sKey.append(1, DataType::SLOTID);
    return sKey;
}
std::string Slots::encode_slot_id(unsigned int slotnum) {
    unsigned int slot = big_endian(slotnum);
    std::string sKey;
    sKey.append(1, DataType::SLOTID);
    sKey.append((char*)&slot, sizeof(unsigned int));
    sKey.append(1, DataType::SLOTID);
    return sKey;
}
