/*
 *文件名称: siobuffer.h
 *文件描述: 拼凑redis协议格式数据 
 *时    间: 2016-05-16
 *作    者: YPS
 *版    本: 0.1
 */

#ifndef __SIOBUFFER_H__
#define __SIOBUFFER_H__

#include "bytes.h"
#include "string.h"

inline static
int sioWriteBulkCount(Buffer& buf, char prefix, int count){
	std::string strcnt;
	int len = 0;

	strcnt = str(count);

	len += buf.append(prefix);
	len += buf.append(strcnt.c_str());
	len += buf.append("\r\n");

	return len;
}

inline static
int sioWriteBulkCountu64(Buffer& buf, char prefix, uint64_t count){
	std::string strcnt;
	int len = 0;

	strcnt = str(count);

	len += buf.append(prefix);
	len += buf.append(strcnt.c_str());
	len += buf.append("\r\n");

	return len;
}


inline static
int sioWriteBuffString(Buffer& buf, const char *str, int strlens){
	int len = 0;
	
	len += sioWriteBulkCount(buf, '$', strlens);
	len += buf.append(str, strlens);
	len += buf.append("\r\n");

	return len;
}

inline static
int sioWriteInt(Buffer& buf, int value){
	std::string strcnt;
	int len = 0;

	strcnt = str(value);
	len += sioWriteBulkCount(buf, '$', strcnt.size());
	len += buf.append(strcnt.c_str());
	len += buf.append("\r\n");

	return len;
}

inline static
int sioWriteuInt(Buffer& buf, uint32_t value){
	std::string strcnt;
	int len = 0;

	strcnt = str(value);
	len += sioWriteBulkCount(buf, '$', strcnt.size());
	len += buf.append(strcnt.c_str());
	len += buf.append("\r\n");

	return len;
}

inline static
int sioWriteInt64(Buffer& buf, int64_t value){
	std::string strcnt;
	int len = 0;

	strcnt = str(value);
	len += sioWriteBulkCount(buf, '$', strcnt.size());
	len += buf.append(strcnt.c_str());
	len += buf.append("\r\n");

	return len;
}

inline static
int sioWriteuInt64(Buffer& buf, uint64_t value){
	std::string strcnt;
	int len = 0;

	strcnt = str(value);
	len += sioWriteBulkCount(buf, '$', strcnt.size());
	len += buf.append(strcnt.c_str());
	len += buf.append("\r\n");

	return len;
}
#endif
