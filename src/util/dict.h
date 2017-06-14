#ifndef _UTIL_DICT_H_
#define _UTIL_DICT_H_

#include <map>
#include <string>
#include "../net/network.h"
#include "../net/ae.h"
typedef struct {
    int fd;
    int64_t lasttime;
} slotsmgrt_sockfd;

class dict
{
	public:
		dict();
		~dict();
		slotsmgrt_sockfd *dictFetchValue(const char *ip, int port);
		slotsmgrt_sockfd *dictFetchValue(const std::string &name);
		void dictDelete(const char *ip, int port);
		slotsmgrt_sockfd *newMgrtNonBlockConnFd(const char *ip, int port, int milltimeout);
		bool empty(){ return mgrt_cached_fds.empty(); }
		void releaseMgrtFds();
	private:
		void dictAdd(const std::string &name, slotsmgrt_sockfd *pfd);
	private:
		std::map<std::string, slotsmgrt_sockfd *> mgrt_cached_fds;
};

#endif