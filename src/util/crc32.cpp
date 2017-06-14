/*
 *文件名称: crc32.cpp
 *文件描述: crc32摘要算法 
 *时    间: 2016-04-18 
 *作    者: YPS
 *版    本: 0.1
 */
#include "crc32.h"

bool Crc32::bInit = false;
unsigned int Crc32::IEEE_POLY = 0xedb88320;
unsigned int Crc32::CAST_POLY = 0x82f63b78;
unsigned int Crc32::KOOP_POLY = 0xeb31d82e;
unsigned int Crc32::crc32tab[256] = {0};

void Crc32::crc32_tabinit(unsigned int poly)
{
	int i, j;
    for (i = 0; i < 256; i ++) {
        unsigned int crc = i;
        for (j = 0; j < 8; j ++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ poly;
            } else {
                crc = (crc >> 1);
            }
        }
        crc32tab[i] = crc;
    }
}

unsigned int Crc32::crc32_update(unsigned int crc, const char *buf, int len)
{
	int i;
    crc = ~crc;
    for (i = 0; i < len; i ++) {
        crc = crc32tab[(unsigned char)((char)crc ^ buf[i])] ^ (crc >> 8);
    }
    return ~crc;
}

void Crc32::InitCrc32()
{
	if (!bInit)
    {
		crc32_tabinit(IEEE_POLY);
		bInit = true;
    }
}

unsigned int Crc32::crc32_checksum(const char *buf, int len)
{
	if (!bInit)
	{
		InitCrc32();
	}
	
	return crc32_update(0, buf, len);
}
