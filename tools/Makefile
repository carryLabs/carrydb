include ../build_config.mk

OBJS += ../src/net/link.o ../src/net/fde.o ../src/util/log.o ../src/util/bytes.o
CFLAGS += -I../src
EXES = carrydb-bench carrydb-dump carrydb-repair carrydb-import

all: carrydb-bench.o carrydb-dump.o carrydb-repair.o leveldb-import.o carrydb-migrate.o
	${CXX} -o carrydb-bench carrydb-bench.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o carrydb-dump carrydb-dump.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o carrydb-repair carrydb-repair.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o leveldb-import leveldb-import.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o carrydb-migrate carrydb-migrate.o ../api/cpp/libssdb-client.a ../src/util/libutil.a

carrydb-migrate.o: carrydb-migrate.cpp
	${CXX} ${CFLAGS} -I../api/cpp -c carrydb-migrate.cpp
carrydb-bench.o: carrydb-bench.cpp
	${CXX} ${CFLAGS} -c carrydb-bench.cpp
carrydb-dump.o: carrydb-dump.cpp
	${CXX} ${CFLAGS} -c carrydb-dump.cpp
carrydb-repair.o: carrydb-repair.cpp
	${CXX} ${CFLAGS} -c carrydb-repair.cpp
leveldb-import.o: leveldb-import.cpp
	${CXX} ${CFLAGS} -c leveldb-import.cpp

clean:
	rm -f *.exe *.exe.stackdump *.o ${EXES}
	rm -rf _cpy_

