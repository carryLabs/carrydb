/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/


#ifndef SSDB_INCLUDE_H_
#define SSDB_INCLUDE_H_

#define leveldb rocksdb

#include <inttypes.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <string.h>
#include <math.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "version.h"

#ifndef UINT64_MAX
	#define UINT64_MAX		18446744073709551615ULL
#endif
#ifndef INT64_MAX
	#define INT64_MAX		0x7fffffffffffffffLL
#endif
#ifndef INT64_MIN
	#define INT64_MIN	    -9223372036854775808
#endif

#ifndef INT64_MAX_STR
	#define INT64_MAX_STR		"9223372036854775807"
#endif
#ifndef INT64_MIN_STR
	#define INT64_MIN_STR	    "-9223372036854775808"
#endif

#ifndef SSDB_SCORE_MIN
	#define SSDB_SCORE_MIN		 "-9223372036854775808"
#endif

#ifndef SSDB_SCORE_MAX
	#define SSDB_SCORE_MAX		 "+9223372036854775807"
#endif

#ifndef EXPIRATION_LIST_KEY
	#define EXPIRATION_LIST_KEY "\xff\xff\xff\xff\xff|EXPIRE_LIST|KV"
#endif

#ifndef RUBBISH_LIST_KEY
	#define RUBBISH_LIST_KEY "\xff\xff\xff\xff\xff|RUBBUSH_LIST|HASH"
#endif

#ifndef MONITOR_INFO_KEY 
	#define MONITOR_INFO_KEY "\xff\xff\xff\xff\xff|MONITOR_INFO"
#endif

#define UNUSED(V) ((void) V)

static inline double millitime(){
	struct timeval now;
	gettimeofday(&now, NULL);
	double ret = now.tv_sec + now.tv_usec/1000.0/1000.0;
	return ret;
}

static inline int64_t time_ms(){
	struct timeval now;
	gettimeofday(&now, NULL);
	return (int64_t)now.tv_sec * 1000 + (int64_t)now.tv_usec/1000;
	//return now.tv_sec * 1000 + now.tv_usec/1000;
}
static inline int64_t time_us(){
	struct timeval now;
	gettimeofday( &now, NULL );
	return now.tv_sec * 1000000 + now.tv_usec;
}



#endif

