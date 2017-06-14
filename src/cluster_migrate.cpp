#include "cluster_migrate.h"

//////////////////////////////////////Handle//////////////////////////////////////////////
ClusterMigrateHandle* ClusterMigrateHandle::instance_ = 0;
ClusterMigrateHandle::ClusterMigrateHandle()
{
    pthread_t tid;
    int err = pthread_create(&tid, NULL, &ClusterMigrateHandle::eventloop_func, this);
    if (err != 0) {
        log_fatal("can't create event loop thread: %s", strerror(err));
        exit(0);
    }
}

ClusterMigrateHandle::~ClusterMigrateHandle()
{
    this->stop_eventloop();
}

clusterRst ClusterMigrateHandle::migrateSlotsData(int sockfd, uint32_t slotindex, uint64_t limit)
{
    clusterRst result;
    ClusterMigrate cluter(ssdb);
    int ret = cluter.migrateSlotsData(sockfd, slotindex, limit);

    result.rst = ret;
    result.mgrtkey = cluter.migrateKeyNums();
    result.remainkey = cluter.remainKeyNums();
    return result;
}

int ClusterMigrateHandle::migrateSingleKey(int sockfd, const Bytes &name, bool deletekey)
{
    ClusterMigrate cluter(ssdb);
    return cluter.migrateSingleKey(sockfd, name, deletekey);
}

int ClusterMigrateHandle::retrieveKey(int sockfd, const Bytes &name, int timeout)
{
    ClusterMigrate cluter(ssdb);
    return cluter.retrieveKey(sockfd, name, timeout, serverip, serverport);
}

int ClusterMigrateHandle::slotsMgrtCachedFd(const char *dstip, int dstport, int milltimeout)
{
    slotsmgrt_sockfd *cachedfds = slotsmgrt_cachedfd_dict.dictFetchValue(dstip, dstport);
    if (cachedfds) {
        cachedfds->lasttime = time_ms();
        return cachedfds->fd;
    } else {
        cachedfds = slotsmgrt_cachedfd_dict.newMgrtNonBlockConnFd(dstip, dstport, milltimeout);
        if (cachedfds) {
            return cachedfds->fd;
        }
    }
    return -1;
}

void ClusterMigrateHandle::closeMgrtSocket(const char *host, int port)
{
    slotsmgrt_sockfd *pfd = slotsmgrt_cachedfd_dict.dictFetchValue(host, port);
    if (pfd == NULL) {
        log_error("slotsmgrt: close target %s:%d again", host, port);
        return;
    } else {
        log_info("slotsmgrt: close target %s:%d", host, port);
    }
    slotsmgrt_cachedfd_dict.dictDelete(host, port);
}

void ClusterMigrateHandle::cleanupMgrtSocket()
{
    slotsmgrt_cachedfd_dict.releaseMgrtFds();
}

void* ClusterMigrateHandle::eventloop_func(void *arg) {
    ClusterMigrateHandle *serv = (ClusterMigrateHandle *)arg;

    while (!serv->eventloop_quit) {
        sleep(20);
        if (serv->slotsmgrt_cachedfd_dict.empty()) {
            continue;
        }
        serv->cleanupMgrtSocket();
    }

    log_debug("eventloop_func thread quit");
    serv->eventloop_quit = false;
    return (void *)NULL;
}

void ClusterMigrateHandle::stop_eventloop() {
    eventloop_quit = true;
    for (int i = 0; i < 100; i++) {
        if (!eventloop_quit) {
            break;
        }
        usleep(10 * 1000);
    }
    log_info("stop mgrt link thread");
}

/////////////////////////////////ClusterMigrate//////////////////////////////////////////////
int ClusterMigrate::migrateSlotsData(int connfd, uint32_t slotindex, int nlimit)
{
    this->deleteKeys_ = true;
    this->slotIndex_ = slotindex;
    this->limitNums_ = nlimit;
    this->sockfd_ = connfd;

    //kv结构放到最后是因为kv和其他结构的metalkey混合在一起,放到后面省的每次都得取出value分析
    getItemsBySlot();

    getMgtKeysTtl();

    calculateTotal();
    if (totalCount_ == 0) {
        return 0;
    } else {
        totalCount_ += 4;//命令头
    }

    Buffer sendbuff(1024);
    makeSendBuffer(sendbuff); //协议组装

    return sendMgrtData(sendbuff);
}

int ClusterMigrate::migrateSingleKey(int sockfd, const Bytes &name, bool deletekey)
{
    this->deleteKeys_ = deletekey;
    this->name_ = name;
    this->sockfd_ = sockfd;

    TMH metainfo = {0};
    if (ssdb->loadobjectbyname(&metainfo, this->name_, 0, true) <= 0) {
        return 0;
    }

    getValuesByName(this->name_, metainfo.datatype);
    getMgtKeysTtl();
    calculateTotal();

    if (totalCount_ == 0) {
        return 0;
    }
    else {
        totalCount_  += 4;//命令头
    }

    Buffer sendbuff(1024);
    makeSendBuffer(sendbuff); //协议组装

    return sendMgrtData(sendbuff);
}

int ClusterMigrate::retrieveKey(int fd, const Bytes &name, int timeout,
                                const std::string &serverip, int serverport)
{
    this->sockfd_ = fd;
    this->millTimeout_ = timeout;
    this->deleteKeys_ = false;

    Buffer sendbuff(1024);
    sioWriteBulkCountu64(sendbuff, '*', 5);
    sioWriteBuffString(sendbuff, "retrievekey", 11);

    sioWriteBuffString(sendbuff, serverip.data(), serverip.size());
    sioWriteuInt(sendbuff, serverport);
    sioWriteuInt(sendbuff, timeout);
    sioWriteBuffString(sendbuff, name.data(), name.size());

    return sendMgrtData(sendbuff);
}

int ClusterMigrate::sendMgrtData(Buffer &sendbuff)
{
    int sbufflen = sendbuff.size();
    int sendlen, wantlen;
    log_debug("send buff %s", sendbuff.data());
    //发送迁移数据
    while (sbufflen > 0 ) {
        wantlen = (sbufflen > (SNDBUF)) ? SNDBUF : sbufflen;

        sendlen = netSendBuffer(sockfd_, sendbuff.data(), wantlen, millTimeout_);
        if (sendlen < 0) {
            log_error("netSendBuffer error:%d sendlen:%d, wantlen:%d, %s", errno, sendlen, wantlen, sendbuff.data());
            return -1;
        } else if (sendlen == 0) {
            log_error("netSendBuffer error send zero:%d sendlen:%d, wantlen:%d, %s", errno, sendlen, wantlen, sendbuff.data());
        }

        sbufflen -= sendlen;
        sendbuff.decr(sendlen);
    }
    if (this->deleteKeys_) {
        char buf_res[3] = {0};
        int retrecv = 0;
        retrecv = netRecvBuffer(sockfd_, buf_res, sizeof(buf_res), millTimeout_);

        if (retrecv > 0) {
            if (buf_res[0] == '+') { //迁移成功,做slot删除
                log_info("迁移成功，准备删除数据:%lld", migrateKeyNums());
                if (!kvmap_.empty()) {
                    log_debug("total kv size: %d", kvmap_.size());
                    ssdb->batch_del_kv_names(kvmap_);
                }

                if (!hashmap_.empty()) {
                    log_debug("total hash size: %d", hashmap_.size());
                    ssdb->batch_del_hash_names(hashmap_);
                }

                if (!zsetmap_.empty()) {
                    log_debug("total zsetmap size: %d", zsetmap_.size());
                    ssdb->batch_del_zset_names(zsetmap_);
                }

                if (!queuemap_.empty()) {
                    log_debug("total queuemap size: %d", queuemap_.size());
                    ssdb->batch_del_list_names(queuemap_);
                }
            }
            log_info("recv msg:%s", hexmem(buf_res, 3).data());
            return 1;
        } else {
            log_info("recv_fail:%s", hexmem(buf_res, 10).data());
            return -1;
        }
    } else{
        return 1;
    }
    return 0;
}

void ClusterMigrate::makeSendBuffer(Buffer &sendbuff)
{
    //顺序不能换，因dataType顺序固定了
    makeMgrtCmdHeader(sendbuff);
    makeMgrtKvBuffer(sendbuff);
    makeMgrtMulMapBuffer(sendbuff, hashmap_);
    makeMgrtMulMapBuffer(sendbuff, zsetmap_);
    makeMgrtQueueBuffer(sendbuff);
    makeMgrtMulMapBuffer(sendbuff, ttlmap_);
}

void ClusterMigrate::makeMgrtCmdHeader(Buffer &sendbuff)
{
    std::string strtotal;
    uint32_t crc = 0;
    //按redis协议拼装数据
    //拼装命令头

    sioWriteBulkCountu64(sendbuff, '*', totalCount_);
    strtotal = str(totalCount_);
    crc = Crc32::crc32_update(crc, strtotal.c_str(), strtotal.size());
    sioWriteBuffString(sendbuff, "slotsrestore", 12);
    crc = Crc32::crc32_update(crc, "slotsrestore", 12);
    sioWriteBuffString(sendbuff, datatype, 5);
    crc = Crc32::crc32_update(crc, datatype, 5);


    if (!this->deleteKeys_) {
        //key
        sioWriteBuffString(sendbuff, "retrievekey", 11);
        crc = Crc32::crc32_update(crc, "retrievekey", 11);
    }
    else {
        //slot
        sioWriteBuffString(sendbuff, (char *)&slotIndex_, sizeof(int));
        crc = Crc32::crc32_update(crc, (char *)&slotIndex_, sizeof(int));
    }
    sioWriteuInt(sendbuff, crc);
}

void ClusterMigrate::makeMgrtKvBuffer(Buffer &sendbuff)
{
    //拼装kv数据结构数据
    if (!kvmap_.empty()) {
        uint32_t crc = 0;
        uint64_t kvcnt;
        std::string strkvcnt;

        kvcnt = kvmap_.size();
        strkvcnt = str(kvcnt);
        sioWriteuInt64(sendbuff, kvcnt);
        crc = Crc32::crc32_update(crc, strkvcnt.c_str(), strkvcnt.size());

        std::map<std::string, std::string>::const_iterator it;
        for (it = kvmap_.begin(); it != kvmap_.end(); it++) {
            sioWriteBuffString(sendbuff, it->first.c_str(), it->first.size());
            crc = Crc32::crc32_update(crc, it->first.c_str(), it->first.size());
            sioWriteBuffString(sendbuff, it->second.c_str(), it->second.size());
            crc = Crc32::crc32_update(crc, it->second.c_str(), it->second.size());
        }
        sioWriteuInt(sendbuff, crc);
    }
}

void ClusterMigrate::makeMgrtMulMapBuffer(Buffer &sendbuff, std::map<std::string,
        std::map<std::string, std::string>> &dataMap)
{
    //拼装zset数据结构数据
    if (!dataMap.empty()) {
        uint32_t crc = 0;
        uint64_t zsetnamecnt;
        std::string strzsetnamecnt;

        zsetnamecnt = dataMap.size();
        strzsetnamecnt = str(zsetnamecnt);
        sioWriteuInt64(sendbuff, zsetnamecnt);
        crc = Crc32::crc32_update(crc, strzsetnamecnt.c_str(), strzsetnamecnt.size());

        std::map<std::string, std::map<std::string, std::string>>::const_iterator it;
        for (it = dataMap.begin(); it != dataMap.end(); it++) {
            sioWriteBuffString(sendbuff, it->first.c_str(), it->first.size());
            crc = Crc32::crc32_update(crc, it->first.c_str(), it->first.size());

            uint64_t zsetkscnt;
            std::string strzsetkscnt;
            zsetkscnt = it->second.size();
            strzsetkscnt = str(zsetkscnt);
            sioWriteuInt64(sendbuff, zsetkscnt);
            crc = Crc32::crc32_update(crc, strzsetkscnt.c_str(), strzsetkscnt.size());

            std::map<std::string, std::string>::const_iterator itmap;
            for (itmap = it->second.begin(); itmap != it->second.end(); itmap++) {
                sioWriteBuffString(sendbuff, itmap->first.c_str(), itmap->first.size());
                crc = Crc32::crc32_update(crc, itmap->first.c_str(), itmap->first.size());

                sioWriteBuffString(sendbuff, itmap->second.c_str(), itmap->second.size());
                crc = Crc32::crc32_update(crc, itmap->second.c_str(), itmap->second.size());
            }
        }
        sioWriteuInt(sendbuff, crc);
    }
}


void ClusterMigrate::makeMgrtQueueBuffer(Buffer &sendbuff)
{
    uint32_t crc = 0;
    uint64_t qnamecnt;
    std::string strqnamecnt;
    //拼装queue数据结构数据
    if (!queuemap_.empty()) {
        qnamecnt = queuemap_.size();
        strqnamecnt = str(qnamecnt);
        sioWriteuInt64(sendbuff, qnamecnt);
        crc = Crc32::crc32_update(crc, strqnamecnt.c_str(), strqnamecnt.size());

        std::map<std::string, std::vector<std::string>>::const_iterator it;
        for (it = queuemap_.begin(); it != queuemap_.end(); it++) {
            sioWriteBuffString(sendbuff, it->first.c_str(), it->first.size());
            crc = Crc32::crc32_update(crc, it->first.c_str(), it->first.size());

            uint64_t qitemcnt;
            std::string strqitemcnt;
            qitemcnt = it->second.size();
            strqitemcnt = str(qitemcnt);
            sioWriteuInt64(sendbuff, qitemcnt);
            crc = Crc32::crc32_update(crc, strqitemcnt.c_str(), strqitemcnt.size());

            std::vector<std::string>::const_iterator itmap;
            for (itmap = it->second.begin(); itmap != it->second.end(); itmap++) {
                sioWriteBuffString(sendbuff, (*itmap).c_str(), (*itmap).size());
                crc = Crc32::crc32_update(crc, (*itmap).c_str(), (*itmap).size());
            }
        }
        sioWriteuInt(sendbuff, crc);
    }
}


uint64_t ClusterMigrate::migrateKeyNums()
{
    return kvmap_.size() + hashmap_.size() + zsetmap_.size() + queuemap_.size();
}

uint64_t ClusterMigrate::remainKeyNums()
{
    if (migrateKeyNums()) {
        std::string buf = Slots::encode_slot_id(slotIndex_);
        std::string val;

        leveldb::Status s = ssdb->getlDb()->Get(leveldb::ReadOptions(), buf, &val);

        if (!s.IsNotFound() && s.ok()) {
            return str_to_int64(val);
        }
    }
    return 0;
}

//迁移过期信息
int ClusterMigrate::getMgtKeysTtl() {
    std::map<std::string, std::string> ttlInfo;
    insertTtlInfo(kvmap_, ttlInfo);
    insertTtlInfo(hashmap_, ttlInfo);
    insertTtlInfo(zsetmap_, ttlInfo);
    insertTtlInfo(queuemap_, ttlInfo);
    if (!ttlInfo.empty()) {
        ttlmap_.insert(std::pair<std::string, std::map<std::string, std::string>>(
                           ssdb->expiration->listName(), ttlInfo));
    }
    return ttlmap_.size();
}


void ClusterMigrate::calculateTotal()
{
    if (!kvmap_.empty()) {
        totalCount_ += kvmap_.size() * 2;//hashname项和kv个数项
        totalCount_ += 2;
        datatype[0] = DataType::KV;
    }

    if (!hashmap_.empty()) {
        for (auto kit = hashmap_.cbegin(); kit != hashmap_.cend(); ++kit) {
            totalCount_ += 2;//hashname项和kv个数项
            for (auto vit = kit->second.cbegin(); vit != kit->second.cend(); ++vit) {
                totalCount_ += 2;//key项和val项
            }
        }
        totalCount_ += 2;//hash个数项 crc32校验项
        datatype[1] = DataType::HASH;
    }

    if (!zsetmap_.empty()) {
        for (auto kit = zsetmap_.cbegin(); kit != zsetmap_.cend(); ++kit) {
            totalCount_ += 2;//zsetname项和ks个数项
            for (auto vit = kit->second.cbegin(); vit != kit->second.cend(); ++vit) {
                totalCount_ += 2;//key项和val项
            }
        }
        totalCount_ += 2;//zset个数项crc32校验项
        datatype[2] = DataType::ZSET;
    }

    if (!queuemap_.empty()) {
        for (auto kit = queuemap_.cbegin(); kit != queuemap_.cend(); ++kit) {
            totalCount_ += 2;//queuename项和val个数项
            totalCount_ += kit->second.size();//val项
        }
        totalCount_ += 2;////queue个数项,crc32校验项
        datatype[3] = DataType::LIST;
    }

    if (!ttlmap_.empty()) {
        for (auto kit = ttlmap_.cbegin(); kit != ttlmap_.cend(); ++kit) {
            totalCount_ += 2;//zsetname项和ks个数项
            for (auto vit = kit->second.cbegin(); vit != kit->second.cend(); ++vit) {
                totalCount_ += 2;//key项和val项
            }
        }
        totalCount_ += 2;//zset个数项crc32校验项
        datatype[4] = DataType::TTL;
    }
    log_debug("migrate size : k%d z%d h%d q%d t%d", kvmap_.size(), zsetmap_.size(),
              hashmap_.size() , queuemap_.size(), ttlmap_.size());
}

void ClusterMigrate::getValuesByName(const Bytes &name, const char dataType) {
    if (name.String().compare(ssdb->expiration->listName()) == 0
            || name.String().compare(ssdb->releasehandler->listName()) == 0) {
        return;
    }

    switch (dataType) {
    case DataType::KSIZE:
    {
        std::string kvalue;
        if (ssdb->get(name, &kvalue) > 0) {
            limitNums_--;
            kvmap_.insert(std::pair<std::string, std::string>(name.String(), kvalue));
        }
        break;
    }
    case DataType::HSIZE:
    {
        //hlist
        std::map<std::string, std::string> hvalue;
        Iterator *hit = nullptr;
        int ret = ssdb->hscan(&hit, name, "", "", UINT64_MAX);
        if (ret <= 0) {
            return;
        }
        while (hit->next()) {
            hvalue.insert(std::pair<std::string, std::string>(hit->key(), hit->value()));
        }
        delete hit;
        if (hvalue.size()) {
            limitNums_--;
            hashmap_.insert(std::pair<std::string, std::map<std::string, std::string>>(name.String(), hvalue));
        }
        break;
    }
    case DataType::ZSIZE:
    {
        std::map<std::string, std::string> zvalue;
        Iterator *zit = nullptr;
        int ret = ssdb->zrange(&zit, name, 0, UINT64_MAX);
        if (ret <= 0) {
            return;
        }
        while (zit->next()) {
            zvalue.insert(std::pair<std::string, std::string>(zit->key(), zit->score()));
        }
        delete zit;
        if (zvalue.size()) {
            limitNums_--;
            zsetmap_.insert(std::pair<std::string, std::map<std::string, std::string>>(name.data(), zvalue));
        }
        break;
    }
    case DataType::LSIZE:
    {
        std::vector<std::string> qvalue;
        ssdb->lslice(name, 0, INT64_MAX, &qvalue);
        if (qvalue.size()) {
            limitNums_--;
            queuemap_.insert(std::pair<std::string, std::vector<std::string>>(name.data(), qvalue));
        }
        break;
    }
    }
}

void ClusterMigrate::getItemsBySlot()
{
    if (limitNums_ <= 0) {
        return;
    }

    std::string key_start, key_end;
    uint32_t slot = big_endian(slotIndex_);

    key_start.append(1, DataType::METAL);
    key_start.append((char *)&slot, sizeof(uint32_t));
    key_end = key_start;
    key_end.append("\xff", 1);

    Iterator *iter = IteratorFactory::iterator(ssdb, '\xff', key_start, key_end, -1);

    while (limitNums_ > 0 && iter->next()) {
        log_debug("%s:%c", iter->name().data(), iter->dataType());
        getValuesByName(iter->name(), iter->dataType());
    }
    delete iter;
}
