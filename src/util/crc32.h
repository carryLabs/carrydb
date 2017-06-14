/*
 *文件名称: crc32.h
 *文件描述: crc32摘要算法 
 *时    间: 2016-04-18 
 *作    者: YPS
 *版    本: 0.1
 */

#ifndef UTIL__CRC32_H
#define UTIL__CRC32_H

class Crc32
{
	private:
		static bool bInit;
		static unsigned int crc32tab[256];
		
		static unsigned int IEEE_POLY;
		static unsigned int CAST_POLY;
		static unsigned int KOOP_POLY;

		static void crc32_tabinit(unsigned int poly);


	public:
		static void InitCrc32();
		static unsigned int crc32_checksum(const char *buf, int len);
		static unsigned int crc32_update(unsigned int crc, const char *buf, int len);
};

#endif
