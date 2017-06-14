include ../build_config.mk

OBJS = proc_kv.o proc_hash.o proc_zset.o proc_list.o \
	proc_table.o proc_slotsmgrt.o backend_dump.o \
	backend_sync.o slave.o serv.o cluster_migrate.o

LIBS = ./db/libdb.a ./util/libutil.a ./net/libnet.a
EXES = ../carrydb-server

all: ${OBJS} carrydb-server.o
	${CXX} -o ../carrydb-server carrydb-server.o ${OBJS} ${LIBS} ${CLIBS} client/SSDB_impl.o

cluster_migrate.o: cluster_migrate.h cluster_migrate.cpp
	${CXX} ${CFLAGS} -c cluster_migrate.cpp

carrydb-server.o: carrydb-server.cpp
	${CXX} ${CFLAGS} -c carrydb-server.cpp
slave.o: slave.h slave.cpp
	${CXX} ${CFLAGS} -c slave.cpp
backend_dump.o: backend_dump.h backend_dump.cpp
	${CXX} ${CFLAGS} -c backend_dump.cpp
backend_sync.o: backend_sync.h backend_sync.cpp
	${CXX} ${CFLAGS} -c backend_sync.cpp

proc.o: serv.h proc.cpp
	${CXX} ${CFLAGS} -c proc.cpp
proc_kv.o: serv.h proc_kv.cpp
	${CXX} ${CFLAGS} -c proc_kv.cpp
proc_hash.o: serv.h proc_hash.cpp
	${CXX} ${CFLAGS} -c proc_hash.cpp
proc_zset.o: serv.h proc_zset.cpp
	${CXX} ${CFLAGS} -c proc_zset.cpp
proc_list.o: serv.h proc_list.cpp
	${CXX} ${CFLAGS} -c proc_list.cpp
proc_table.o: serv.h proc_table.cpp
	${CXX} ${CFLAGS} -c proc_table.cpp
proc_slotsmgrt.o: serv.h proc_slotsmgrt.cpp
	${CXX} ${CFLAGS} -c proc_slotsmgrt.cpp
serv.o: serv.h serv.cpp
	${CXX} ${CFLAGS} -c serv.cpp

clean:
	rm -f ${EXES} *.o *.exe *.a

