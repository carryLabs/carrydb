#ifndef _CLUSTER_MIGRATE_
#define _CLUSTER_MIGRATE_

#include <string.h>
#include <mutex>  
#include <thread>
#include "net/network.h"
#include "util/siobuffer.h"
#include "util/crc32.h"
#include "util/dict.h"
#include "db/ssdb_impl.h"
#include "db/t_kv.h"
#include "include.h"
#include "serv.h"
#include "slave.h"

struct clusterRst{
    int rst;
    uint64_t mgrtkey;
    uint64_t remainkey;
};

class ClusterMigrateHandle//:boost::noncopyable  
{
public:
    static ClusterMigrateHandle *instance(){  
        if (instance_ == NULL) {
            instance_ = new ClusterMigrateHandle;
        }
        return instance_;
    }

    void setInternalDb(SSDBImpl *db){
        ssdb = db;
    }
    void setInterface(const std::string &interface){
        if (interface.empty()){
            serverip = "127.0.0.1";
        }else{
            serverip = interface;
        }
    }
    void setServerPort(int port){
        serverport = port;
    }
    int fetchMgrtSocketFd(const char *host, int port, int milltimeout){
        return slotsMgrtCachedFd(host, port, milltimeout);
    }
    void closeMgrtSocket(const char *host, int port);
    clusterRst migrateSlotsData(int sockfd, uint32_t slotindex, uint64_t limit);
    int migrateSingleKey(int sockfd, const Bytes &name, bool deletekey);
    int retrieveKey(int sockfd, const Bytes &name, int timeout);
    uint64_t migrateKeyNums();//迁移的key个数
    uint64_t remainKeyNums();
    dict slotsmgrt_cachedfd_dict;
private:  
    ClusterMigrateHandle();
    ~ClusterMigrateHandle();
    ClusterMigrateHandle &operator=(const ClusterMigrateHandle &);  

    int slotsMgrtCachedFd(const char *host, int port, int milltimeout);
    
    void cleanupMgrtSocket();
    static void* eventloop_func(void *arg);
    void stop_eventloop();
    bool eventloop_quit = false;

    
    SSDBImpl *ssdb;
    std::string serverip;
    int serverport;
    static ClusterMigrateHandle* instance_;
};

class ClusterMigrate
{
public:
    ClusterMigrate(SSDBImpl *db){
        ssdb = db;
    }
    ~ClusterMigrate(){}

    int migrateSlotsData(int sockfd, uint32_t slotindex, int nlimit);
    int migrateSingleKey(int sockfd, const Bytes &name_, bool deletekey);
    int retrieveKey(int sockfd, const Bytes &name_, int timeout,
            const std::string &serverip, int serverport);
    bool remoteServerValid(const char *dstIp, int dstPort);//过滤不可迁移的服务器，如果主迁移从，数据将丢失
    uint64_t migrateKeyNums();                             //迁移的key个数
    uint64_t remainKeyNums();

    int retrieveKey(const Bytes &name_, const char *ip, int port);

    void calculateTotal();

private:
    int getMgtKeysTtl();
    void makeSendBuffer(Buffer &sendbuff);
    void makeMgrtCmdHeader(Buffer &sendbuff);
    void makeMgrtKvBuffer(Buffer &sendbuff);
    void makeMgrtMulMapBuffer(Buffer &sendbuff,
            std::map<std::string, std::map<std::string, std::string>> &dataMap);
    void makeMgrtQueueBuffer(Buffer &sendbuff);
    int sendMgrtData(Buffer &sendbuff);

    void getValuesByName(const Bytes &name_, const char dataType);

    template<class T>
    void insertTtlInfo(const std::map<std::string, T> &member,
                std::map<std::string, std::string> &ttlInfo);
    void getItemsBySlot();
private:
    SSDBImpl *ssdb;
    uint32_t slotIndex_ = 0;
    uint64_t totalCount_ = 0;
    uint64_t limitNums_ = 0;
    int millTimeout_ = 10000;
    int sockfd_;
    char datatype[5];
    Bytes name_;
    bool deleteKeys_ = true;
    std::map<std::string, std::string> kvmap_;
    std::map<std::string, std::map<std::string, std::string>> hashmap_;
    std::map<std::string, std::map<std::string, std::string>> zsetmap_;
    std::map<std::string, std::vector<std::string>> queuemap_;
    std::map<std::string, std::map<std::string, std::string>> ttlmap_;
};

template<class T>
void ClusterMigrate::insertTtlInfo(const std::map<std::string, T> &member,
    std::map<std::string, std::string> &ttlInfo)
{
    for (auto it = member.cbegin(); it != member.cend(); ++it){
        int64_t time = ssdb->expiration->get_ttl(it->first, false);
        if (time > 0){
            ttlInfo.insert(std::pair<std::string,std::string>(it->first, str(time)));
       }
    }
}

#endif
