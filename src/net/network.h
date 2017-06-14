/*
 *文件名称: network.h
 *文件描述: 提供迁移命令时socket通信接口 
 *时    间: 2016-05-16
 *作    者: YPS
 *版    本: 0.1
 */

#ifndef __NETWORK_H__
#define __NETWORK_H__

#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <netdb.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <time.h>
#include "../util/log.h"
#include "../include.h"
#define SNDBUF 64*1024
#define RCVBUF 64*1024

#define NETWORK_ERR -1
#define NETWORK_OK 0

#define AE_OK 0
#define AE_ERR -1

#define AE_NONE 0
#define AE_READABLE 1
#define AE_WRITABLE 2

#define ANET_CONNECT_NONE 0
#define ANET_CONNECT_NONBLOCK 1

#define ANET_OK 0
#define ANET_ERR -1
#define ANET_ERR_LEN 256
 /* Flags used with certain functions. */
#define ANET_NONE 0
#define ANET_IP_ONLY (1<<0)


/* 函数名称: netNonBlock
 * 说    明: 设置socket阻塞或非阻塞 
 */
inline static
int netNonBlock(int fd, int non_block){
	int flags;

	if ((flags = fcntl(fd, F_GETFL)) == -1){
		return NETWORK_ERR;
	}

	if (non_block)
		flags |= O_NONBLOCK;
	else
		flags &= ~O_NONBLOCK;

	if (fcntl(fd, F_SETFL, flags) == -1){
		return NETWORK_ERR;
	}

	return NETWORK_OK;
}

/* 函数名称: netCreateConnSocket 
 * 说    明: 建立socket并在timeout时间内建立tcp连接 
 */
inline static
int netCreateConnSocket(const char* dstip, int port, int timeout){
	int sock = 0, maxsock = 0, retsel = 0;
	struct sockaddr_in addr = {0};
	fd_set rest, west;
	struct timeval tv = {0};
	int sndBuf = SNDBUF;
	int rcvBuf = RCVBUF;

	if((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1){
		log_error("socket error: %d, %s", errno, strerror(errno));
		goto sock_err;
	}

	addr.sin_family = AF_INET;
	addr.sin_port = htons((short)port);
	inet_pton(AF_INET, dstip, &addr.sin_addr);

	if (netNonBlock(sock, 1) == NETWORK_ERR){
		log_error("netNonBlock error: %d, %s", errno, strerror(errno));
		goto sock_err;
	}

	if(connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0){
		if (errno != EINPROGRESS){
			log_error("connect error: %d, %s", errno, strerror(errno));
			goto sock_err;
		}
	}else{
		goto sock_succ;

	}

	FD_ZERO(&rest);
	FD_ZERO(&west);
	FD_SET(sock, &rest);
	FD_SET(sock, &west);
	maxsock = sock + 1;
	tv.tv_sec = timeout;
	tv.tv_usec = 0;

	retsel = select(maxsock, &rest, &west, NULL, &tv);
	if (retsel <= 0){
		log_error("select error: %d, %s", errno, strerror(errno));
		goto sock_err;
	}else{
		if (!FD_ISSET(sock, &west)){
			log_error("FD_ISSET error: %d, %s", errno, strerror(errno));
			goto sock_err;
		}
	}

sock_succ:
	setsockopt(sock,SOL_SOCKET,SO_RCVBUF,(const char*)&sndBuf,sizeof(int));
	setsockopt(sock,SOL_SOCKET,SO_SNDBUF,(const char*)&rcvBuf,sizeof(int));
	return sock;

sock_err:
	if (sock >=0)
		close(sock);

	return NETWORK_ERR;
}

/* 函数名称: netSendBuffer 
 * 说    明: 在timeout时间内，发送数据 
 */
inline static
int netSendBuffer(int fd, char *sendbuf, int ptrlen, int timeout_ms) {
	int leftlen = ptrlen;
	int sendlen = 0;
	int retsend = 0;
	fd_set west;
	timeval tv;
	char *ptr = sendbuf;
	while (1) {
		tv.tv_sec = timeout_ms / 1000;
		tv.tv_usec = (timeout_ms % 1000) * 1000;
		FD_ZERO(&west);
		FD_SET(fd, &west);

		switch (::select(fd + 1, NULL, &west, NULL, &tv)) {
		case -1:
			if (errno == EINTR) {
				continue;
			}
			log_error("select faild:%d", errno);
			return NETWORK_ERR;
		case 0:
			log_error("select timeout, no descriptors can be read or written");
			return NETWORK_ERR;
		default:
			if (FD_ISSET(fd, &west)) {
				do {
					retsend = send(fd, ptr, leftlen, 0);
					if (retsend < 0) {
						if (errno == EAGAIN || errno == EWOULDBLOCK)
							break;
						else if (errno == EINTR)
							retsend = 0;
						else{
							log_error("send data error is %d", errno);
							return 0;
						}
					}
					leftlen -= retsend;
					sendlen += retsend;
					ptr += retsend;
				} while (leftlen);
			}else {
				return NETWORK_ERR;
			}
			return sendlen;
		}
	}
	return 0;
}

/* 函数名称: netRecvBuffer 
 * 说    明: 在timeout时间内，接收数据
 */
inline static
int netRecvBuffer(int fd, char *ptr, int ptrlen, int timeout_ms){
	int retlen = 0;	
	int leftlen = ptrlen;
	int recvlen = 0;
	fd_set rest;
	timeval tv;

	while (1) {
		tv.tv_sec = timeout_ms / 1000;
		tv.tv_usec = (timeout_ms % 1000) * 1000;

		FD_ZERO(&rest);
		FD_SET(fd, &rest);
		switch (::select(fd + 1, &rest, NULL, NULL, &tv)) {
		case -1:
			if (errno == EINTR) {
				continue;
			}
			log_error("select faild:%d", errno);
			return NETWORK_ERR;
		case 0:
			log_error("select timeout, no descriptors can be read or written");
			return NETWORK_ERR;
		default:
			if (FD_ISSET(fd, &rest)) {
				do{
					retlen = ::recv(fd, ptr, leftlen, 0) ;
					if (retlen < 0) {
						if (errno == EAGAIN || errno == EWOULDBLOCK) {
							break;
						}else if (errno == EINTR) {
							retlen = 0;
						}else {
							log_error("recv data error is %d", errno);
							return NETWORK_ERR;
						}
					}
					else if (retlen == 0) {
						log_error("socket is closed");
						return 0;
					}
					recvlen += retlen;
					leftlen -= retlen;
					ptr += retlen;
				}while(leftlen);
			}else {
				return NETWORK_ERR;
			}
			return recvlen;
		}
	}
	return 0;
}

inline static int anetSetReuseAddr(int fd) {
    int yes = 1;
    /* Make sure connection-intensive things like the redis benckmark
     * will be able to close/open sockets a zillion of times */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
        log_error("setsockopt SO_REUSEADDR: %d, %s", errno, strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

// inline static int aeWait(int fd, int mask, long long milliseconds) {
//     struct pollfd pfd;
//     int retmask = 0, retval;

//     memset(&pfd, 0, sizeof(pfd));
//     pfd.fd = fd;
//     if (mask & AE_READABLE) pfd.events |= POLLIN;
//     if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

//     if ((retval = poll(&pfd, 1, milliseconds))== 1) {
//         if (pfd.revents & POLLIN) retmask |= AE_READABLE;
//         if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
// 	if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
//         if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
//         return retmask;
//     } else {
//         return retval;
//     }
// }

inline static int anetSetTcpNoDelay(int fd, int val)
{
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1)
    {
        log_error("setsockopt TCP_NODELAY: %d, %s", errno, strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

inline static int anetSetBlock(int fd, int non_block) {
    int flags;

    /* Set the socket blocking (if non_block is zero) or non-blocking.
     * Note that fcntl(2) for F_GETFL and F_SETFL can't be
     * interrupted by a signal. */
    if ((flags = fcntl(fd, F_GETFL)) == -1) {
        log_error("fcntl(F_GETFL): %s", strerror(errno));
        return ANET_ERR;
    }

    if (non_block)
        flags |= O_NONBLOCK;
    else
        flags &= ~O_NONBLOCK;

    if (fcntl(fd, F_SETFL, flags) == -1) {
        log_error("fcntl(F_SETFL,O_NONBLOCK): %s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

inline static int anetNonBlock(int fd) {
    return anetSetBlock(fd,1);
}

inline static int anetTcpGenericConnect(const char *addr, int port, int flags)
{
    int s = ANET_ERR, rv;
    char portstr[6];  /* strlen("65535") + 1; */
    struct addrinfo hints, *servinfo, *p;

    snprintf(portstr,sizeof(portstr),"%d",port);
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(addr,portstr,&hints,&servinfo)) != 0) {
        log_error("getaddrinfo error: %d, %s", errno, strerror(errno));
        return ANET_ERR;
    }
    int i = 0;
    for (p = servinfo; p != NULL; p = p->ai_next) {
        /* Try to create the socket and to connect it.
         * If we fail in the socket() call, or on connect(), we retry with
         * the next entry in servinfo. */
        log_debug("getaddrinfo:%i, family:%d, socket type:%d, protocol%d", ++i, p->ai_family, 
        	p->ai_socktype, p->ai_protocol);
        if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
            continue;
        if (anetSetReuseAddr(s) == ANET_ERR) goto error;
        if (flags & ANET_CONNECT_NONBLOCK && anetNonBlock(s) != ANET_OK)
            goto error;
        if (connect(s,p->ai_addr,p->ai_addrlen) == -1) {
            /* If the socket is non-blocking, it is ok for connect() to
             * return an EINPROGRESS error here. */
            if (errno == EINPROGRESS && flags & ANET_CONNECT_NONBLOCK)
                goto end;
            close(s);
            s = ANET_ERR;
            continue;
        }

        /* If we ended an iteration of the for loop without errors, we
         * have a connected socket. Let's return to the caller. */
        goto end;
    }
    if (p == NULL)
        log_error("creating socket:%d, %s", errno, strerror(errno));

error:
    if (s != ANET_ERR) {
        close(s);
        s = ANET_ERR;
    }
end:
    freeaddrinfo(servinfo);
    return s;
}

inline static int anetEnableTcpNoDelay(int fd)
{
    return anetSetTcpNoDelay(fd, 1);
}

inline static int anetTcpNonBlockConnect(const char *addr, int port)
{
    return anetTcpGenericConnect(addr, port, ANET_CONNECT_NONBLOCK);
}

#endif
