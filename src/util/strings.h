/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_STRING_H
#define UTIL_STRING_H

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string>
#include <algorithm>
#include <vector>
#include "../include.h"
#include <vector>
inline static
int is_empty_str(const char *str) {
	const char *p = str;
	while (*p && isspace(*p)) {
		p++;
	}
	return *p == '\0';
}

/* 返回左边不包含空白字符的字符串的指针 */
inline static
char *ltrim(const char *str) {
	const char *p = str;
	while (*p && isspace(*p)) {
		p++;
	}
	return (char *)p;
}

/* 返回指向字符串结尾的指针, 会修改字符串内容 */
inline static
char *rtrim(char *str) {
	char *p;
	p = str + strlen(str) - 1;
	while (p >= str && isspace(*p)) {
		p--;
	}
	*(++p) = '\0';
	return p;
}

/* 返回左边不包含空白字符的字符串的指针 */
inline static
char *trim(char *str) {
	char *p;
	p = ltrim(str);
	rtrim(p);
	return p;
}

inline static
void strtolower(std::string *str) {
	std::transform(str->begin(), str->end(), str->begin(), ::tolower);
}

inline static
void strtoupper(std::string *str) {
	std::transform(str->begin(), str->end(), str->begin(), ::toupper);
}

inline static
std::string real_dirname(const char *filepath) {
	std::string dir;
	if (filepath[0] != '/') {
		char buf[1024];
		char *p = getcwd(buf, sizeof(buf));
		if (p != NULL) {
			dir.append(p);
		}
		dir.append("/");
	}

	const char *p = strrchr(filepath, '/');
	if (p != NULL) {
		dir.append(filepath, p - filepath);
	}
	return dir;
}

inline static
std::string str_escape(const char *s, int size) {
	static const char *hex = "0123456789abcdef";
	std::string ret;
	for (int i = 0; i < size; i++) {
		char c = s[i];
		switch (c) {
		case '\r':
			ret.append("\\r");
			break;
		case '\n':
			ret.append("\\n");
			break;
		case '\t':
			ret.append("\\t");
			break;
		case '\\':
			ret.append("\\\\");
			break;
		case ' ':
			ret.push_back(c);
			break;
		default:
			if (c >= '!' && c <= '~') {
				ret.push_back(c);
			} else {
				ret.append("\\x");
				unsigned char d = c;
				ret.push_back(hex[d >> 4]);
				ret.push_back(hex[d & 0x0f]);
			}
			break;
		}
	}
	return ret;
}

inline static
std::string str_escape(const std::string &s) {
	return str_escape(s.data(), (int)s.size());
}

inline static
int hex_int(char c) {
	if (c >= '0' && c <= '9') {
		return c - '0';
	} else {
		return c - 'a' + 10;
	}
}

inline static
std::string str_unescape(const char *s, int size) {
	std::string ret;
	for (int i = 0; i < size; i++) {
		char c = s[i];
		if (c != '\\') {
			ret.push_back(c);
		} else {
			if (i >= size - 1) {
				continue;
			}
			char c2 = s[++i];
			switch (c2) {
			case 'a':
				ret.push_back('\a');
				break;
			case 'b':
				ret.push_back('\b');
				break;
			case 'f':
				ret.push_back('\f');
				break;
			case 'v':
				ret.push_back('\v');
				break;
			case 'r':
				ret.push_back('\r');
				break;
			case 'n':
				ret.push_back('\n');
				break;
			case 't':
				ret.push_back('\t');
				break;
			case '\\':
				ret.push_back('\\');
				break;
			case 'x':
				if (i < size - 2) {
					char c3 = s[++i];
					char c4 = s[++i];
					ret.push_back((char)((hex_int(c3) << 4) + hex_int(c4)));
				}
				break;
			default:
				ret.push_back(c2);
				break;
			}
		}
	}
	return ret;
}

inline static
std::string str_unescape(const std::string &s) {
	return str_unescape(s.data(), (int)s.size());
}

inline static
std::string hexmem(const void *p, int size) {
	return str_escape((char *)p, size);
	/*
	std::string ret;
	char buf[4];
	for(int i=0; i<size; i++){
		char c = ((char *)p)[i];
		if(isalnum(c) || isprint(c)){
			ret.append(1, c);
		}else{
			switch(c){
				case '\r':
					ret.append("\\r", 2);
					break;
				case '\n':
					ret.append("\\n", 2);
					break;
				default:
					sprintf(buf, "\\%02x", (unsigned char)c);
					ret.append(buf, 3);
			}
		}
	}
	return ret;
	*/
}

// TODO: mem_printf("%5c%d%s", p, size);
static inline
void dump(const void *p, int size, const char *msg = NULL) {
	if (msg == NULL) {
		printf("dump <");
	} else {
		printf("%s <", msg);
	}
	std::string s = hexmem(p, size);
	printf("%s>\n", s.c_str());
}


static inline
std::string str(const char *s) {
	return std::string(s);
}

static inline
std::string str(uint32_t v) {
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%u", v);
	return std::string(buf);
}

static inline
std::string str(int v) {
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%d", v);
	return std::string(buf);
}

static inline
std::string str(int64_t v) {
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRId64 "", v);
	return std::string(buf);
}

static inline
std::string str(uint64_t v) {
	char buf[21] = {0};
	snprintf(buf, sizeof(buf), "%" PRIu64 "", v);
	return std::string(buf);
}

static inline
std::string str(double v) {
	char buf[21] = {0};
	if (v - floor(v) == 0) {
		snprintf(buf, sizeof(buf), "%.0f", v);
	} else {
		snprintf(buf, sizeof(buf), "%f", v);
	}
	return std::string(buf);
}

static inline
std::string str(float v) {
	return str((double)v);
}


// all str_to_xx methods set errno on error

static inline
int str_to_int(const std::string &str) {
	const char *start = str.c_str();
	char *end;
	int ret = (int)strtol(start, &end, 10);
	// the WHOLE string must be string represented integer
	if (*end == '\0' && size_t(end - start) == str.size()) {
		errno = 0;
	} else {
		// strtoxx do not set errno all the time!
		if (errno == 0) {
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
int str_to_int(const char *p, int size) {
	return str_to_int(std::string(p, size));
}

static inline
int64_t str_to_int64(const std::string &str) {
	const char *start = str.c_str();
	char *end;
	int64_t ret = (int64_t)strtoll(start, &end, 10);
	// the WHOLE string must be string represented integer
	if (*end == '\0' && size_t(end - start) == str.size()) {
		errno = 0;
	} else {
		// strtoxx do not set errno all the time!
		if (errno == 0) {
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
int isoverflow(const std::string &str, int64_t by, int64_t *new_val) {
	int64_t val = str_to_int64(str);
	if (errno != 0) {
		return -4;
	}

	if ((by < 0 && val < 0 && by < (LLONG_MIN - val)) ||
	        (by > 0 && val > 0 && by > (LLONG_MAX - val))) {
		return -5;
	}
	if (new_val) {
		*new_val = val + by;
	}
	return 1;
}

static inline
int64_t str_to_int64(const char *p, int size) {
	return str_to_int64(std::string(p, size));
}

static inline
uint64_t str_to_uint64(const std::string &str) {
	const char *start = str.c_str();
	char *end;
	uint64_t ret = (uint64_t)strtoull(start, &end, 10);
	// the WHOLE string must be string represented integer
	if (*end == '\0' && size_t(end - start) == str.size()) {
		errno = 0;
	} else {
		// strtoxx do not set errno all the time!
		if (errno == 0) {
			errno = EINVAL;
		}
	}
	return ret;
}

static inline
std::string trim(const std::string& str) {
	if (str.empty()) return std::string();
	size_t start = 0;
	size_t end = str.size() - 1;
	while (isspace(str[start]) != 0 && start <= end) {
		++start;
	}
	while (isspace(str[end]) != 0 && start <= end) {
		--end;
	}
	if (start <= end) {
		return str.substr(start, end - start + 1);
	}
	return std::string();
}

static inline
std::vector<std::string> split( const std::string &str, const std::string &delimiters, bool skip_empty = true) {
	std::vector<std::string> elems;
	std::string::size_type pos, prev = 0;
	while ( ( pos = str.find_first_of(delimiters, prev) ) != std::string::npos ) {
		if ( pos > prev ) {
			std::string sub = str.substr(prev, pos - prev);
			if (!skip_empty || !trim(sub).empty()) {
				elems.emplace_back(str, prev, pos - prev );
			}
		}
		prev = pos + 1;
	}
	if ( prev < str.size() ) elems.emplace_back( str, prev, str.size() - prev );
	return elems;
}

static inline 
bool isxdigit(const std::string &str){
	for (int i = 0; i < str.size(); ++i){
		if (!isxdigit(str[i])){
			return false;
		}
	}
	return true;
}

static inline
uint64_t str_to_uint64(const char *p, int size) {
	return str_to_uint64(std::string(p, size));
}

static inline
double str_to_double(const char *p, int size) {
	return atof(std::string(p, size).c_str());
}

static inline
std::string substr(const std::string &str, int start, int size) {
	if (start < 0) {
		start = (int)str.size() + start;
	}
	if (size < 0) {
		// 忽略掉 abs(size) 个字节
		size = ((int)str.size() + size) - start;
	}
	if (start < 0 || size_t(start) >= str.size() || size < 0) {
		return "";
	}
	return str.substr(start, size);
}

static inline
std::string str_slice(const std::string &str, int start, int end) {
	if (start < 0) {
		start = (int)str.size() + start;
	}
	int size;
	if (end < 0) {
		size = ((int)str.size() + end + 1) - start;
	} else {
		size = end - start + 1;
	}
	if (start < 0 || size_t(start) >= str.size() || size < 0) {
		return "";
	}
	return str.substr(start, size);
}

static inline
int bitcount(const char *p, int size) {
	int n = 0;
	for (int i = 0; i < size; i++) {
		unsigned char c = (unsigned char)p[i];
		while (c) {
			n += c & 1;
			c = c >> 1;
		}
	}
	return n;
}

// is big endia. TODO: auto detect
#if 0
#define big_endian(v) (v)
#else
static inline
uint16_t big_endian(uint16_t v) {
	return (v >> 8) | (v << 8);
}

static inline
uint32_t big_endian(uint32_t v) {
	return (v >> 24) | ((v >> 8) & 0xff00) | ((v << 8) & 0xff0000) | (v << 24);
}

static inline
uint64_t big_endian(uint64_t v) {
	uint32_t h = v >> 32;
	uint32_t l = v & 0xffffffffull;
	return big_endian(h) | ((uint64_t)big_endian(l) << 32);
}

static inline
uint32_t bigendian_uint32(uint32_t a) {
	uint32_t num = 0;
	unsigned char *tmp = (unsigned char *) &a;

	num = (((uint32_t) tmp[0]) << 24) |
	      (((uint32_t) tmp[1]) << 16) |
	      (((uint32_t) tmp[2]) << 8) |
	      ((uint32_t) tmp[3]);

	return num;
}

static inline
uint64_t bigendian_uint64(uint64_t a) {
	uint64_t num = 0;
	unsigned char *tmp = (unsigned char *) &a;

	num = (((uint64_t) tmp[0]) << 56) |
	      (((uint64_t) tmp[1]) << 48) |
	      (((uint64_t) tmp[2]) << 40) |
	      (((uint64_t) tmp[3]) << 32) |
	      (((uint64_t) tmp[4]) << 24) |
	      (((uint64_t) tmp[5]) << 16) |
	      (((uint64_t) tmp[6]) << 8) |
	      ((uint64_t) tmp[7]);

	return num;
}



#define encode_score(s) big_endian((uint64_t)(s))
#define decode_score(s) big_endian((uint64_t)(s))

#define encode_version(s) big_endian((uint64_t)(s))
#define decode_version(s) big_endian((uint64_t)(s))
#endif

static inline bool isnum(std::string str, bool neg_allow)
{
	if (str.size() == 0)
		return false;

	if (!neg_allow) {
		//不允许负号
		for (unsigned int i = 0; i < str.size(); i++) {
			if (!isdigit(str[i]))
				return false;
		}

		return true;
	}
	else {
		//允许负号
		if (str[0] == '-') {
			//首字符为负号
			if (str.size() == 1) {
				//只有一个负号
				return false;
			}

			for (unsigned int i = 1; i < str.size(); i++) {
				if (!isdigit(str[i]))
					return false;
			}

			return true;
		}
		else {
			//首字符不为负号
			for (unsigned int i = 0; i < str.size(); i++) {
				if (!isdigit(str[i]))
					return false;
			}

			return true;
		}
	}
}

#endif
