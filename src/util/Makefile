include ../../build_config.mk

OBJS = log.o config.o bytes.o sorted_set.o app.o crc32.o slots.o dict.o bson_tool.o cmdinfo.o
EXES = 

all: ${OBJS}
	ar -cru ./libutil.a ${OBJS}

app.o: app.h app.cpp
	${CXX} ${CFLAGS} -c app.cpp

log.o: log.h log.cpp
	${CXX} ${CFLAGS} -c log.cpp

config.o: config.h config.cpp
	${CXX} ${CFLAGS} -c config.cpp

bytes.o: bytes.h bytes.cpp
	${CXX} ${CFLAGS} -c bytes.cpp

sorted_set.o: sorted_set.h sorted_set.cpp
	${CXX} ${CFLAGS} -c sorted_set.cpp

crc32.o: crc32.h crc32.cpp
	${CXX} ${CFLAGS} -c crc32.cpp

slots.o: slots.h slots.cpp
	${CXX} ${CFLAGS} -c slots.cpp

dict.o: dict.h dict.cpp
	${CXX} ${CFLAGS} -c dict.cpp

bson_tool.o: const.h bson_tool.h bson_tool.cpp
	${CXX} ${CFLAGS} -c bson_tool.cpp

cmdinfo.o: cmdinfo.h cmdinfo.cpp
	${CXX} ${CFLAGS} -c cmdinfo.cpp

test:
	$(CXX) ${CFLAGS} test_sorted_set.cpp $(OBJS)

clean:
	rm -f ${EXES} ${OBJS} *.o *.exe *.a

