#include "dict.h"

dict::dict()
{

}

dict::~dict()
{
    for (auto it = mgrt_cached_fds.begin(); it != mgrt_cached_fds.end(); ++it){
        delete it->second;
    }
    mgrt_cached_fds.clear();
}

slotsmgrt_sockfd *dict::dictFetchValue(const char *ip, int port)
{
	char pName[20];
	snprintf(pName, sizeof(pName), "%s:%d", ip, port);
	return dictFetchValue(std::string(pName));
}

slotsmgrt_sockfd *dict::dictFetchValue(const std::string &name)
{
	auto it = mgrt_cached_fds.find(name);

	if (it != mgrt_cached_fds.end()){
		return it->second;
	}
	return NULL;
}

void dict::dictDelete(const char *ip, int port)
{
    char pName[20];
    snprintf(pName, sizeof(pName), "%s:%d", ip, port);
    auto it = mgrt_cached_fds.find(std::string(pName));
    if (it->second){
        close(it->second->fd);
        delete it->second;
    }
    mgrt_cached_fds.erase(it);
}

slotsmgrt_sockfd *dict::newMgrtNonBlockConnFd(const char *ip, int port, int milltimeout)
{
	char pName[20];
	snprintf(pName, sizeof(pName), "%s:%d", ip, port);
	slotsmgrt_sockfd *pfd = NULL;
	int fd = anetTcpNonBlockConnect(ip, port);
    if (fd == -1) {
        log_error("slotsmgrt: connect to target %s:%s faild", ip, port);
        return NULL;
    }
    anetEnableTcpNoDelay(fd);
    //todo time
    if ((aeWait(fd, AE_WRITABLE, milltimeout) & AE_WRITABLE) == 0) {
        log_error("slotsmgrt: connect to target %s:%s, aewait error", ip, port);
        close(fd);
        return NULL;
    }
    pfd = new slotsmgrt_sockfd;
    pfd->fd = fd;
    pfd->lasttime = time_ms();
    dictAdd(std::string(pName), pfd);
    return pfd;
}

void dict::dictAdd(const std::string &name, slotsmgrt_sockfd *pfd)
{
    mgrt_cached_fds.insert(std::pair<std::string, slotsmgrt_sockfd*>(name, pfd));
}

void dict::releaseMgrtFds()
{
    for (auto it = mgrt_cached_fds.begin(); it != mgrt_cached_fds.end();){
        if ((time_ms() - it->second->lasttime)/1000 > 120) {
            log_info("slotsmgrt: timeout target %s, lasttime = %ld, now = %ld",
                it->first.data(), it->second->lasttime, time_ms());
            close(it->second->fd);
            delete it->second;
            mgrt_cached_fds.erase(it++);
        }else {
            ++it;
        }
    }
}
