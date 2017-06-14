#!/bin/sh
BASE_DIR=`pwd`
JEMALLOC_PATH="$BASE_DIR/deps/jemalloc-4.1.0"
LEVELDB_PATH="$BASE_DIR/deps/rocksdb-4.9"
SNAPPY_PATH="$BASE_DIR/deps/snappy-1.1.0"
BSON_PATH="$BASE_DIR/deps/libbson-1.6.0"

ln -snf $LEVELDB_PATH/include/rocksdb $LEVELDB_PATH/include/leveldb

# dependency check
which autoconf > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
	echo ""
	echo "ERROR! autoconf required! install autoconf first"
	echo ""
	exit 1
fi

if test -z "$TARGET_OS"; then
	TARGET_OS=`uname -s`
fi
if test -z "$MAKE"; then
	MAKE=make
fi
if test -z "$CC"; then
	CC=gcc
fi
if test -z "$CXX"; then
	CXX=g++
fi

case "$TARGET_OS" in
    Darwin)
        #PLATFORM_CLIBS="-pthread"
		#PLATFORM_CFLAGS=""
        ;;
    Linux)
        PLATFORM_CLIBS="-pthread -lrt"
        ;;
    OS_ANDROID_CROSSCOMPILE)
        PLATFORM_CLIBS="-pthread"
        SNAPPY_HOST="--host=i386-linux"
        ;;
    CYGWIN_*)
        PLATFORM_CLIBS="-lpthread"
        ;;
    SunOS)
        PLATFORM_CLIBS="-lpthread -lrt"
        ;;
    FreeBSD)
        PLATFORM_CLIBS="-lpthread"
		MAKE=gmake
        ;;
    NetBSD)
        PLATFORM_CLIBS="-lpthread -lgcc_s"
        ;;
    OpenBSD)
        PLATFORM_CLIBS="-pthread"
        ;;
    DragonFly)
        PLATFORM_CLIBS="-lpthread"
        ;;
    HP-UX)
        PLATFORM_CLIBS="-pthread"
        ;;
    *)
        echo "Unknown platform!" >&2
        exit 1
esac


DIR=`pwd`
cd $SNAPPY_PATH
if [ ! -f Makefile ]; then
	echo ""
	echo "##### building snappy... #####"
	./configure $SNAPPY_HOST
	# FUCK! snappy compilation doesn't work on some linux!
	find . | xargs touch
	make
	echo "##### building snappy finished #####"
	echo ""
fi
cd "$DIR"


DIR=`pwd`
cd $BSON_PATH
if [ ! -f Makefile ]; then
	echo `pwd`
	echo "##### building libbson... #####"
	sh ./autogen.sh && ./configure && make
	echo "##### building libbson finished #####"
	echo ""
fi
cd "$DIR"

case "$TARGET_OS" in
	CYGWIN*|FreeBSD|OS_ANDROID_CROSSCOMPILE)
		echo "not using jemalloc on $TARGET_OS"
	;;
	*)
		DIR=`pwd`
		cd $JEMALLOC_PATH
		if [ ! -f Makefile ]; then
			echo ""
			echo "##### building jemalloc... #####"
			sh ./autogen.sh
			./configure
			make
			echo "##### building jemalloc finished #####"
			echo ""
		fi
		cd "$DIR"
	;;
esac


rm -f src/version.h
echo "#ifndef CARRYDB_DEPS_H" >> src/version.h
echo "#ifndef CARRYDB_VERSION" >> src/version.h
echo "#define CARRYDB_VERSION \"`cat version`\"" >> src/version.h
echo "#endif" >> src/version.h
echo "#endif" >> src/version.h
case "$TARGET_OS" in
	CYGWIN*|FreeBSD)
	;;
	OS_ANDROID_CROSSCOMPILE)
        echo "#define OS_ANDROID 1" >> src/version.h
	;;
	*)
		echo "#ifndef IOS" >> src/version.h
		echo "#include <stdlib.h>" >> src/version.h
		echo "#include <jemalloc/jemalloc.h>" >> src/version.h
		echo "#endif" >> src/version.h
	;;
esac

rm -f build_config.mk
echo CC=$CC >> build_config.mk
echo CXX=$CXX >> build_config.mk
echo "MAKE=$MAKE" >> build_config.mk
echo "LEVELDB_PATH=$LEVELDB_PATH" >> build_config.mk
echo "JEMALLOC_PATH=$JEMALLOC_PATH" >> build_config.mk
echo "SNAPPY_PATH=$SNAPPY_PATH" >> build_config.mk
echo "BSON_PATH=$BSON_PATH" >> build_config.mk

echo "CFLAGS=" >> build_config.mk
echo "CFLAGS = -std=c++0x -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare" >> build_config.mk
echo "CFLAGS += ${PLATFORM_CFLAGS}" >> build_config.mk
echo "CFLAGS += -I \"$LEVELDB_PATH/include\"" >> build_config.mk
echo "CFLAGS += -I \"$BSON_PATH/src\"" >> build_config.mk

echo "CLIBS=" >> build_config.mk
echo "CLIBS += -lbz2 -lz \"$LEVELDB_PATH/librocksdb.a\"" >> build_config.mk
#echo "CLIBS += -L \"/usr/local/lib\" -lbz2 -lz -llz4">> build_config.mk
echo "CLIBS += \"$SNAPPY_PATH/.libs/libsnappy.a\"" >> build_config.mk
echo "CLIBS += \"$BSON_PATH/.libs/libbson.a\"" >> build_config.mk

case "$TARGET_OS" in
	CYGWIN*|FreeBSD|OS_ANDROID_CROSSCOMPILE)
	;;
	*)
		echo "CLIBS += \"$JEMALLOC_PATH/lib/libjemalloc.a\"" >> build_config.mk
		echo "CFLAGS += -I \"$JEMALLOC_PATH/include\"" >> build_config.mk
	;;
esac

echo "CLIBS += ${PLATFORM_CLIBS}" >> build_config.mk


if test -z "$TMPDIR"; then
    TMPDIR=/tmp
fi

g++ -x c++ - -o $TMPDIR/carrydb_build_test.$$ 2>/dev/null <<EOF
	#include <unordered_map>
	int main() {}
EOF
if [ "$?" = 0 ]; then
	echo "CFLAGS += -DNEW_MAC" >> build_config.mk
fi

