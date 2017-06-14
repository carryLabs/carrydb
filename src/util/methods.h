#ifndef PUB_METHOD_
#define PUB_METHOD_

#include <unistd.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <ifaddrs.h> 
#include <fstream>
#include <sstream>
#include <iterator>

#include "../include.h"
#include "string.h"
#include "bytes.h"
#include "log.h"
#include "file.h"

static inline uint64_t getPhyMemory(const pid_t pid)
{
    char file[64] = {0};
    FILE *fd;
    char lineBuffer[256] = {0};
    snprintf(file, sizeof(file), "/proc/%d/status", pid);

    fd = fopen (file, "r");
    if (!fd) {
        log_error("can not open file: /proc/%d/statu", pid);
        return 0;
    }
    char name[32];
    int val = 0;
    bool find = false;
    while( fgets(lineBuffer, sizeof(lineBuffer), fd) != NULL ) {
        sscanf(lineBuffer, "%s %d", name, &val);
        if (strncasecmp("VmRSS", name, 5) == 0) {
            find = true;
            break;
        }
    }
    fclose(fd);
    if (!find) val = 0;
    return static_cast<uint64_t>(val) * 1024;
}

static inline void bytesToHuman(char *s, unsigned long long n)
{
    double d;
    if (n < 1024) {
        /* Bytes */
        sprintf(s, "%lluB", n);
        return;
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    } else if (n < (1024LL*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024);
        sprintf(s,"%.2fT",d);
    } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024*1024);
        sprintf(s,"%.2fP",d);
    } else {
        /* Let's hope we never need this */
        sprintf(s,"%lluB",n);
    }
}

static inline std::string strprintfln(const char *fmt, ...)
{
    std::string sRst;
    va_list ap;
    va_list cpy;
    va_start(ap, fmt);
    
    char staticbuf[256], *buf = staticbuf;

    size_t buflen = strlen(fmt) * 2;
    if (buflen > sizeof(staticbuf)) {
        buf = (char *)malloc(buflen);
        if (!buf) {
            return sRst;
        }
    }else {
        buflen = sizeof(staticbuf);
    }

    while(1) {
        buf[buflen - 2] = '\0';
        va_copy(cpy, ap);
        vsnprintf(buf, buflen, fmt, cpy);
        va_end(cpy);
        if (buf[buflen - 2] != '\0') {
            if (buf != staticbuf) free(buf);
            buflen *= 2;
            buf = (char *)malloc (buflen + sizeof(size_t));
            if (!buf) return sRst;
            continue;
        }
        break;
    }
    sRst.append(buf);
    if (buf != staticbuf) free (buf);
    va_end(ap);
    if (!sRst.empty()) sRst.append("\r\n");
    return sRst;
}

static inline std::string getInterface(const char *intername) {
    char *host_ = nullptr;
    std::string network_interface(intername);
    if (network_interface == "") {
        std::ifstream routeFile("/proc/net/route", std::ios_base::in);
        if (!routeFile.good()) {
            log_error("read /proc/net/route error");
            return std::string();
        }
        std::string line;
        std::vector<std::string> tokens;
        while(std::getline(routeFile, line)){
            std::istringstream stream(line);
            std::copy(std::istream_iterator<std::string>(stream),
                std::istream_iterator<std::string>(),
                std::back_inserter<std::vector<std::string> >(tokens));
            // the default interface is the one having the second 
            // field, Destination, set to "00000000"
            if ((tokens.size() >= 2) && (tokens[1] == std::string("00000000"))){
                network_interface = tokens[0];
                break;
            }
      
            tokens.clear();
        }
        routeFile.close();
    } 
    log_info("Using Networker Interface: %s", network_interface.data());
    struct ifaddrs * ifAddrStruct = NULL;
    struct ifaddrs * ifa = NULL;
    void * tmpAddrPtr = NULL;
    getifaddrs(&ifAddrStruct);
    for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa ->ifa_addr->sa_family==AF_INET) { // Check it is
            // a valid IPv4 address
            tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
                if (std::string(ifa->ifa_name) == network_interface) {
                    host_ = addressBuffer;
                    break;
                }
        } else if (ifa->ifa_addr->sa_family==AF_INET6) { // Check it is
            // a valid IPv6 address
            tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
            char addressBuffer[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
            if (std::string(ifa->ifa_name) == network_interface) {
                host_ = addressBuffer;
                break;
            }
        }
    }
    if (ifAddrStruct != NULL) {
        freeifaddrs(ifAddrStruct);
    }
    if (ifa == NULL) {
        log_error("error network interface: %s , please check!", network_interface.data());
    }   
    log_error("host: %s", host_);
    return std::string(host_);
}


static inline int validInt64(const Bytes &score){
    if (score.compare(Bytes("0")) == 0){
        return 1;
    }

    if (!isnum(score.String(), true)){
        return -3;
    }

    int64_t val = score.Int64();
    if (val == 0){
        fprintf(stderr, "%s:%d\n", __FILE__, __LINE__);
        return -3;
    }

    if (val == INT64_MAX || val == INT64_MIN){//判断val是否合法
        if (strncmp(score.data(), INT64_MAX_STR, strlen(INT64_MAX_STR)) != 0
            && strncmp(score.data(), INT64_MIN_STR, strlen(INT64_MIN_STR)) !=0){
            fprintf(stderr, "%s:%d\n", __FILE__, __LINE__);
            return -3;
        }
    }

    return 1;
}


#endif
