PREFIX=/usr/local/carrydb

$(shell sh build.sh 1>&2)
include build_config.mk

all:
	mkdir -p var var_slave
	chmod u+x deps/cpy/cpy
	chmod u+x tools/carrydb-cli
	cd "${LEVELDB_PATH}"; ${MAKE} static_lib #PORTABLE=1
	cd "${BSON_PATH}"; ${MAKE}
	cd src/util; ${MAKE}
	cd src/net; ${MAKE}
	cd src/client; ${MAKE}
	cd src/db; ${MAKE}
	cd src; ${MAKE}
	cd tools; ${MAKE}
test:
	cd src; ${MAKE} test
	
.PHONY: ios
	
ios:
	cd "${LEVELDB_PATH}"; make clean; CXXFLAGS=-stdlib=libc++ ${MAKE} PLATFORM=IOS
	cd "${SNAPPY_PATH}"; make clean; make -f Makefile-ios
	mkdir -p ios
	mv ${LEVELDB_PATH}/libleveldb-ios.a ${SNAPPY_PATH}/libsnappy-ios.a ios/
	cd src/util; make clean; ${MAKE} -f Makefile-ios
	cd src/db; make clean; ${MAKE} -f Makefile-ios

install:
	mkdir -p ${PREFIX}
	mkdir -p ${PREFIX}/_cpy_
	mkdir -p ${PREFIX}/deps
	mkdir -p ${PREFIX}/var
	mkdir -p ${PREFIX}/var_slave
	cp -f carrydb-server carrydb.conf carrydb_slave.conf ${PREFIX}
	cp -rf api ${PREFIX}
	cp -rf \
		tools/carrydb-bench \
		tools/carrydb-cli tools/carrydb_cli \
		tools/carrydb-cli.cpy tools/carrydb-dump \
		tools/carrydb-repair \
		${PREFIX}
	cp -rf deps/cpy ${PREFIX}/deps
	chmod 755 ${PREFIX}
	chmod -R ugo+rw ${PREFIX}/*
	rm -f ${PREFIX}/Makefile

clean:
	rm -f *.exe.stackdump
	rm -rf api/cpy/_cpy_
	rm -f api/python/SSDB.pyc
	rm -rf db_test
	cd deps/cpy; ${MAKE} clean
	cd tools; ${MAKE} clean
	cd src/util; ${MAKE} clean
	cd src/db; ${MAKE} clean
	cd src/net; ${MAKE} clean
	cd src; ${MAKE} clean



clean_all: clean
	cd "${LEVELDB_PATH}"; ${MAKE} clean
	rm -f ${JEMALLOC_PATH}/Makefile
	cd "${SNAPPY_PATH}"; ${MAKE} clean
	rm -f ${SNAPPY_PATH}/Makefile
	cd "${BSON_PATH}"; ${MAKE} clean
	
