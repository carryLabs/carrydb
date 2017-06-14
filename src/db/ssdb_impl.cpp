/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "ssdb_impl.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/memtablerep.h"
#include "leveldb/slice_transform.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"

#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_list.h"
#include "t_table.h"

SSDBImpl::SSDBImpl() {
    ldb = NULL;
    binlogs = NULL;
}

SSDBImpl::~SSDBImpl() {
    if (binlogs) {
        delete binlogs;
    }
    if (ldb) {
        delete ldb;
    }
}

SSDB* SSDB::open(const Options &opt, const std::string &dir) {
    SSDBImpl *ssdb = new SSDBImpl();
    ssdb->dbpath = dir;
    ssdb->options.create_if_missing = true;
    ssdb->options.max_open_files = opt.max_open_files;
    ssdb->options.write_buffer_size = opt.write_buffer_size * 1024 * 1024;
    
    //ssdb->options.compaction_speed = opt.compaction_speed;
    ssdb->options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(5));
    ssdb->options.memtable_prefix_bloom_bits = 100000000;
    ssdb->options.memtable_prefix_bloom_probes = 10;
    ssdb->options.memtable_prefix_bloom_huge_page_tlb_size = 2 * 1024 * 1024;
    ssdb->options.info_log_level = leveldb::InfoLogLevel::INFO_LEVEL;
    
	ssdb->options.db_log_dir = opt.log_dir;
    leveldb::BlockBasedTableOptions tableOption;
    //测试kBinarySearch kHashSearch 效率区别，kHashSearch在SST查找会快，但是内存与文件占用大
    tableOption.index_type = leveldb::BlockBasedTableOptions::kBinarySearch;
    tableOption.block_size = opt.block_size * 1024;
    tableOption.block_cache = leveldb::NewLRUCache(opt.cache_size * 1048576);//1073741825
    //tableOption.filter_policy.reset(leveldb::NewBloomFilterPolicy(10, false));
    //各个表的结构对比在下面网址，比较重要
    //https://github.com/facebook/rocksdb/wiki/Hash-based-memtable-implementations
    ssdb->options.table_factory.reset(leveldb::NewBlockBasedTableFactory(tableOption));

	ssdb->OptimizeLevelCompaction(512 * 1024 * 1024);
	ssdb->IncreaseParallelism();
    if (opt.compression == "yes") {
        ssdb->options.compression = leveldb::kSnappyCompression;
    } else {
        ssdb->options.compression = leveldb::kNoCompression;
    }

    leveldb::Status status;

    status = leveldb::DB::Open(ssdb->options, dir, &ssdb->ldb);
    if (!status.ok()) {
        log_error("open db failed: %s", status.ToString().c_str());
        goto err;
    }
    ssdb->binlogs = new BinlogQueue(ssdb->ldb, opt.binlog, opt.binlog_capacity);

    return ssdb;
err:
    if (ssdb) {
        delete ssdb;
    }
    return NULL;
}

void SSDBImpl::OptimizeLevelCompaction(uint64_t memtable_memory_budget){
	//设置内存中memtable的大小．越大写性能越优秀，默认６４Ｍ,这里改成１２８Ｍ
	options.write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
	//当memtables刷盘到Ｌ０时候，先合并在一起再进行刷盘的memtable的最小数量，默认是１，每个
	//memtables都会被当成Ｌ０的一个独立文件，增加读放大，因为每次读取，都会检测Ｌ０的所有文件．
	options.min_write_buffer_number_to_merge = 2;
	//内存中memtable的数量，默认是２，当１个写满进行刷盘时候，另一个可以继续工作．默认情况下
	//compaction速度可以跟不上写速度．如果大于３，当写最后一个memtable时候，写速度会被限速至
	//options.delayed_write_rate．
	options.max_write_buffer_number = 6;

	//触发Ｌ０合并的文件数目．默认是４．想法是尽快进行Ｌ０合并．防止文件堆积．	
	options.level0_file_num_compaction_trigger = 2;
	 //Level-1的文件大小．L1以上level的文件大小计算公式是：
	//target_file_size_base * (target_file_size_multiplier ^ (L-1))))
	//target_file_size_multiplier默认是１．
	options.target_file_size_base = memtable_memory_budget / 8;
	
	//Level-1的文件总大小最大值．默认是２５６Ｍ．当L0的总大小等于L1时，L0->L1最快．
	options.max_bytes_for_level_base = memtable_memory_budget;
	
	//这个是默认值，还有一种是Universal compaction style,没有level的概念，文件都在L0,写最快，读放大
	//最严重．
	options.compaction_style = leveldb::kCompactionStyleLevel;

	//想法是L0与L1不进行压缩，L2-L7使用snappy压缩算法．db中90％的数据都在LMAX
	options.compression_per_level.resize(options.num_levels);
	for (int i = 0; i < options.num_levels; ++i) {
		if (i < 2) {
			options.compression_per_level[i] = leveldb::kNoCompression;
		} else {
			options.compression_per_level[i] = leveldb::kSnappyCompression;
		}
	}
}

void SSDBImpl::IncreaseParallelism(){
	options.max_background_compactions = 6;
	//flush线程开发Ｑ＆Ａ建议是4.
	options.max_background_flushes = 4;

	options.env->SetBackgroundThreads(7, leveldb::Env::LOW);
	options.env->SetBackgroundThreads(4, leveldb::Env::HIGH);
	
}

int SSDBImpl::flushdb() {
    Transaction trans(binlogs);
    int ret = 0;
    bool stop = false;
    while (!stop) {
        leveldb::Iterator *it;
        leveldb::ReadOptions iterate_options;
        iterate_options.fill_cache = false;
        leveldb::WriteOptions write_opts;

        it = ldb->NewIterator(iterate_options);
        it->SeekToFirst();
        for (int i = 0; i < 10000; i++) {
            if (!it->Valid()) {
                stop = true;
                break;
            }
            //log_debug("%s", hexmem(it->key().data(), it->key().size()).c_str());
            leveldb::Status s = ldb->Delete(write_opts, it->key());
            if (!s.ok()) {
                log_error("del error: %s", s.ToString().c_str());
                stop = true;
                ret = -1;
                break;
            }
            it->Next();
        }
        delete it;
    }
    binlogs->flush();
    return ret;
}

/* raw operates */

int SSDBImpl::raw_set(const Bytes &key, const Bytes &val) {
    leveldb::WriteOptions write_opts;
    leveldb::Status s = ldb->Put(write_opts, slice(key), slice(val));
    if (!s.ok()) {
        log_error("set error: %s", s.ToString().c_str());
        return -1;
    }
    return 1;
}

int SSDBImpl::raw_del(const Bytes &key) {
    leveldb::WriteOptions write_opts;
    leveldb::Status s = ldb->Delete(write_opts, slice(key));
    if (!s.ok()) {
        log_error("del error: %s", s.ToString().c_str());
        return -1;
    }
    return 1;
}

int SSDBImpl::raw_get(char cmdtype, const Bytes &key, std::string *val) {
    leveldb::ReadOptions opts;
    opts.fill_cache = false;

    std::string valdata;
    leveldb::Status s = ldb->Get(opts, slice(key), &valdata);
    if (s.IsNotFound()) {
        return 0;
    }
    if (!s.ok()) {
        log_error("get error: %s", s.ToString().c_str());
        return -1;
    }
    //除了Stirng,其他类型的数据的value 前缀都一样,是TVH类型.String的value前缀是TMH类型
    if (cmdtype == BinlogCommand::KSET || cmdtype == BinlogCommand::CLT_CREATE
                || cmdtype == BinlogCommand::CLT_RECREATE) {
        DecodeDataValue(DataType::METAL, valdata, val);
    } else {
        DecodeDataValue(DataType::HSIZE, valdata, val);
    }
    return 1;
}

// static uint64_t getDBSize(SSDBImpl *ssdb, const std::string start)
// {
//     leveldb::Range ranges[1];
//     std::string end = start;
//     //start.append(1, 0);
//     end.append(1, 255);

//     ranges[0] = leveldb::Range(start, end);
//     uint64_t size;
//     ssdb->ldb->GetApproximateSizes(ranges, 1, &size, true);
//     return size;
// }

uint64_t SSDBImpl::size() {
    uint64_t sizes = 0;
    // leveldb::Range ranges[1];
    // for (unsigned int slot = 0; slot < HASH_SLOTS_SIZE; ++slot) {
    //     std::string start, end;

    //     for (auto it = data_type.cbegin(); it != data_type.cend(); ++it) {
    //         if (it->first == DataType::KSIZE){
    //             continue;
    //         }
    //         for (int len = 0; len < 255; ++len) {
    //             if (it->first == DataType::HSIZE && it->second > 0) {
    //                 sizes += getDBSize(this, encode_slotshashData_nsKey(name, slot, len));
    //             } else if (it->first == DataType::LSIZE && it->second > 0) {
    //                 sizes += getDBSize(this, encode_slotsqueueData_nskey(name, slot, len));
    //             } else if (it->first == DataType::ZSIZE && it->second > 0) {
    //                 sizes += getDBSize(this, encode_slotszsetData_nskey(name, slot, len));
    //             }
    //         }
    //     }
    //     sizes += getDBSize(this, encode_slotskvData_nskey(name, slot));
    // }

    return sizes;
}

std::vector<std::string> SSDBImpl::info() {
    //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
    //     where <N> is an ASCII representation of a level number (e.g. "0").
    //  "leveldb.stats" - returns a multi-line string that describes statistics
    //     about the internal operation of the DB.
    //  "leveldb.sstables" - returns a multi-line string that describes all
    //     of the sstables that make up the db contents.
    std::vector<std::string> info;
    std::vector<std::string> keys;

    keys.push_back("leveldb.stats");
    //keys.push_back("leveldb.sstables");

    for (size_t i = 0; i < keys.size(); i++) {
        std::string key = keys[i];
        std::string val;
        if (ldb->GetProperty(key, &val)) {
            info.push_back(key);
            info.push_back(val);
        }
    }

    return info;
}

void SSDBImpl::compact() {
    if (!is_compact_){
        is_compact_ = 1;
        ldb->CompactRange(NULL, NULL);
        is_compact_ = 0;
    }
}

int SSDBImpl::loadobjectbyname(TMH *pth, const Bytes &name, const char datatype, bool expire, std::string *val) {
    std::string metalkey, metalval;
    metalkey = EncodeMetaKey(name);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), metalkey, &metalval);
    if (s.IsNotFound()) {
        return 0;
    }

    if (!s.ok()) {
        log_error("get object error: %s", s.ToString().c_str());
        return -1;
    }

    int ret = ObjectErr::object_normal;
    memcpy(pth, metalval.c_str(), sizeof(TMH));


    if (val && metalval.size() - sizeof(TMH) > 0) {
        val->append(metalval, sizeof(TMH), metalval.size() - sizeof(TMH));
    }

    if (expire && pth->expire != 0 && pth->expire < time_ms()) {
        ret |= ObjectErr::object_expire;
    }

    if (datatype != 0 && pth->datatype != datatype) {
        ret |= ObjectErr::object_typeinvalid;
    }

    return ret;
}

//0表示未找到,-1表示出错,-2类型不匹配,-3过期了,不能处理
int SSDBImpl::VerifyStructState(TMH *th, const Bytes &name, const char datatype, std::string *val) {
    //loadobjectbyname return -1(获取失败) 0(未找到对象) 1<<1(对象过期) 1<<2(对象类型不匹配)
    int ret = loadobjectbyname(th, name, datatype, true, val);
    if (ret <= 0) { //等于0代表未找到 -1代表出错，直接返回
        return ret;
    }
    else {
        //对象类型不匹配
        if (ret & ObjectErr::object_typeinvalid) {
            return -2;
        }

        if (ret & ObjectErr::object_expire) {
            //不能为负数(错误码),过期表示还没删除的key,1<<1
            return ObjectErr::object_expire;
        }
    }

    return 1;
}

int SSDBImpl::expire(const Bytes &name, int64_t ttl) {
    Transaction trans(binlogs);

    TMH th = {0};
    std::string metalval, val;
    std::string metalkey = EncodeMetaKey(name);
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), metalkey, &metalval);
    if (s.IsNotFound()) {
        return 0;
    }

    if (!s.ok()) {
        log_error("get object error: %s", s.ToString().c_str());
        return -1;
    }
    memcpy(&th, metalval.c_str(), sizeof(TMH));

    log_debug("set %s expire ttl: %u", name.data(), ttl);
    if (th.expire != 0 && th.expire < time_ms()){
        return 0;
    }
    th.expire = ttl;
    DecodeDataValue(DataType::METAL, metalval, &val);
    this->binlogs->Put(EncodeMetaKey(name), EncodeMetaVal(th, val));

    int ret = this->expiration->set_ttl(name, ttl, BinlogType::SYNC);

    if (ret < 0){
        return -1;
    }

    s = binlogs->commit();
    if(!s.ok()) {
        return -1;
    }

    return 1;
}

int SSDBImpl::persist(const Bytes &name) {
    Transaction trans(binlogs);

    TMH th = {0};

    std::string val;
    int ret = VerifyStructState(&th, name, 0, &val);
    
    if (ret <= 0){
        return ret;
    }

    if (ret & ObjectErr::object_expire || th.expire == 0){
        return 0;
    }

    th.expire = 0;
    this->binlogs->Put(EncodeMetaKey(name), EncodeMetaVal(th, val));

    ret = this->expiration->del_ttl(name, BinlogType::SYNC);

    if (ret < 0){
        return ret;
    }

    leveldb::Status s = binlogs->commit();
    if(!s.ok()) {
        return -1;
    }

    return 1;
}

int64_t SSDBImpl::ttl(const Bytes &name) {
    TMH th = {0};

    int ret = loadobjectbyname(&th, name, 0, true);
    if ( ret == -1 )
        return ret;
    if (ret == 0 || (ret & ObjectErr::object_expire)) { //时间过期，被认为未找到
        return -2;
    }
    if (th.expire == 0) { //永久key的情况处理
        return -1;
    }
    int64_t expire = (th.expire - time_ms()) / 1000;
    return expire;
}

int SSDBImpl::del(const Bytes &key, char log_type, bool del_ttl) {
    TMH metainfo = {0};
    /* loadobjectbyname return 0(未找到对象),1<<2(对象类型不匹配)
     1<<1(对象过期) -1(获取失败)
     第三个参数为0，不对数据类型进行判断，第四个参数true
     需要过期时间进行判断，返回不同值*/
    int ret = this->loadobjectbyname(&metainfo, key, 0, true);
    if (ret <= 0) {
        return ret;
    } else {
        if (ret & ObjectErr::object_expire) {
            ret = 0;//时间过期删除时客户端认为没有进行删除
        } else {
            ret = 1;
        }
        switch (metainfo.datatype) {
        case DataType::KSIZE://独立执行delkv命令，需要写入binlog
            this->clear(key, log_type, del_ttl);
            break;
        case DataType::HSIZE:
            this->hclear(key, log_type, del_ttl);
            break;
        case DataType::LSIZE:
            this->lclear(key, log_type, del_ttl);
            break;
        case DataType::ZSIZE:
            this->zclear(key, log_type, del_ttl);
            break;
        default:
            return -1;
        }
    }

    return ret;
}

int SSDBImpl::exists(const Bytes &key, std::string *type) {
    TMH th = {0};
    /*loadobjectbyname return 0(未找到对象),1<<2(对象类型不匹配)
     1<<1(对象过期) -1(获取失败)
     第三个参数为0，不对数据类型进行判断，第四个参数false
     不对过期时间进行判断*/
    int ret = this->loadobjectbyname(&th, key, 0, true);
    if (ret <= 0 ) {
        return ret;
    } else {

        if (ret & ObjectErr::object_expire) { //时间过期，被认为未找到
            return 0;
        }

        switch (th.datatype) {
        case DataType::KSIZE:
            *type = "string";
            break;
        case DataType::HSIZE:
            *type = "hash";
            break;
        case DataType::ZSIZE:
            *type = "zset";
            break;
        case DataType::LSIZE:
            *type = "llist";
            break;
        default:
            return 0;
        }
    }

    return 1;
}

Iterator* SSDBImpl::scan_slot(char type, unsigned int slot, const Bytes &start, const Bytes &end, uint64_t limit) {
    std::string key_start, key_end;
    key_start = EncodeMetaKeyPrefix(start, slot);
    key_end = EncodeMetaKeyPrefix(end, slot);
    if (end.empty()) {
        key_end.append(1, 255);
    }
    //dump(key_start.data(), key_start.size(), "scan.start");
    //dump(key_end.data(), key_end.size(), "scan.end");
    return IteratorFactory::iterator(this, type, key_start, key_end, limit);
}

Iterator* SSDBImpl::rscan_slot(char type, unsigned int slot, const Bytes &start, const Bytes &end, uint64_t limit) {
    std::string key_start, key_end;

    key_start = EncodeMetaKeyPrefix(start, slot);
    if (start.empty()) {
        key_start.append(1, 255);
    }

    key_end = EncodeMetaKeyPrefix(end, slot);
    return IteratorFactory::rev_iterator(this, type, key_start, key_end, limit);
}

int SSDBImpl::incrSlotsRefCount(const Bytes &name, TMH &metainfo, int incr)
{
    //这个几个key不去监控,代表特殊意义
    if (name.compare(EXPIRATION_LIST_KEY) == 0 || name.compare(RUBBISH_LIST_KEY)==0
            || name.compare(MONITOR_INFO_KEY) == 0) {
        return 0;
    }

    //新增第一个与删除最后一个才记录slotinfo,过期时间删除,新增namespace记录等操作
    if ((metainfo.refcount == 1 && incr > 0) || metainfo.refcount == 0) {
        incrNsRefCount(metainfo.datatype, name, incr);
        removeExpire(name, metainfo);
        calculateSlotRefer(Slots::encode_slot_id(name), metainfo.refcount <= 0 ? -1 : metainfo.refcount);
    } else {
        return 0;
    }

    removeExpire(name, metainfo);
    calculateSlotRefer(Slots::encode_slot_id(name), metainfo.refcount <= 0 ? -1 : metainfo.refcount);


    return 1;
}

void SSDBImpl::removeExpire(const Bytes &name, TMH &metainfo)
{
    //不为0就表示过期了
    if (metainfo.expire != 0) {
        if (expiration->del_ttl(name) >= 0) {
            metainfo.expire = 0;
        }
    }
}

void SSDBImpl::calculateSlotRefer(const std::string &encode_slot_id, int ref)
{
    std::string sRefVal;
    leveldb::Status s = ldb->Get(leveldb::ReadOptions(), encode_slot_id, &sRefVal);


    if (s.IsNotFound() && ref > 0) {
        binlogs->Put(encode_slot_id, str(ref));
    } else if (s.ok()) {
        int64_t nRefVal = str_to_int64(sRefVal);
        nRefVal += ref;

        if (nRefVal > 0) {
            binlogs->Put(encode_slot_id, str(nRefVal));
        } else {
            binlogs->Delete(encode_slot_id);
        }
    }
}

int SSDBImpl::findoutName(const Bytes &name, uint32_t ref)
{
    TMH th = {0};
    int ret = loadobjectbyname(&th, name, 0, false);

    //找到这个KV就不需要自增引用个数了,因为是把name去做成slot的，所以只根据name来判断唯一
    if (ret == 0) {
        return ref > 0 ? 1 : 0;
    } else if (ret == -1) {
        log_error("get object error");
        return 0;
    }

    return ref <= 0 ? -1 : 0;
}


void SSDBImpl::incrNsRefCount(char datatype, const Bytes &name, int incr) {
    //如果是meta_db,就没有设置此值
    if (monitor_handler != nullptr) {
        monitor_handler->incrNsRefCount(datatype, name, incr);
    }
}

void SSDBImpl::checkObjectExpire(TMH &metainfo, const Bytes &name, int ret) {
    //过期还没删除key时候应该先删除,然后再进行下一步操作
    if (ret & ObjectErr::object_expire) {
        //expiration->del_ttl(name, true);
        this->releasehandler->push_clear_queue(metainfo, name);
        metainfo = {0};
        log_debug("----------------key expire %" PRId64, metainfo.version);
    }
}

void SSDBImpl::MemoryInfo(std::string &info)
{
    std::stringstream tmp_stream;

    tmp_stream << "# Memory" << "\r\n"; 

    uint64_t memtable_usage, table_reader_usage;
    ldb->GetIntProperty("rocksdb.cur-size-all-mem-tables", &memtable_usage);
    ldb->GetIntProperty("rocksdb.estimate-table-readers-mem", &table_reader_usage);

    tmp_stream << "used_memory:" << (memtable_usage + table_reader_usage) << "\r\n";
    tmp_stream << "used_memory_human:" << ((memtable_usage + table_reader_usage) >> 20) << "M\r\n";
    tmp_stream << "db_memtable_usage:" << memtable_usage << "\r\n";
    tmp_stream << "db_tablereader_usage:" << table_reader_usage << "\r\n";

    info.append(tmp_stream.str());
}

#ifdef __linux
    #include <sys/vfs.h>
#else
    #include <sys/param.h>
    #include <sys/mount.h>
#endif

void SSDBImpl::DiskInfo(std::string &info)
{
    struct statfs diskInfo;
    if (statfs(".", &diskInfo) == -1){
        return;
    }

    std::stringstream tmp_stream;
    tmp_stream << "# Disk" << "\r\n"; 

    long blocksize = diskInfo.f_bsize;
    long available = diskInfo.f_bavail;
    long used  = diskInfo.f_blocks - diskInfo.f_bfree;

    int pct = 0;
    if (diskInfo.f_blocks != 0) {
        pct = (used * 100) / (used + available) + ((used * 100) % (used + available) != 0);
    }
    tmp_stream << "disk_magnetic:" << pct << "%" << "\r\n";
    tmp_stream << "disk_available:" << available * blocksize << "\r\n";
    tmp_stream << "disk_available_human:" << ((available * blocksize)>>30) << "G\r\n";
    info.append(tmp_stream.str()); 
}

void SSDBImpl::DataInfo(std::string &info)
{
    std::stringstream tmp_stream;
    tmp_stream << "# Data" << "\r\n"; 

    uint64_t nDiskBytes = Du(dbpath);
    char humanDiskBytes[64];
    bytesToHuman(humanDiskBytes, nDiskBytes);

    tmp_stream << "rocksdb_data_size:" << nDiskBytes << "\r\n";
    tmp_stream << "rocksdb_data_size_human:" << humanDiskBytes << "\r\n";
    tmp_stream << "rocksdb_data_path:" << dbpath << "\r\n";

    //这里需要获取群的集合
    //leveldb::ColumnFamilyHandle *pHandle = db->DefaultColumnFamily();
    int nLevels = ldb->NumberLevels();
    tmp_stream << "rocksdb_file_num_levels:" << nLevels << "\r\n";

    for (int i = 0; i < nLevels; ++i)
    {
        std::string val;
        if (ldb->GetProperty("rocksdb.num-files-at-level" + str(i), &val)){
            tmp_stream << "rocksdb_num_files_at_level" << i << ":" << val << "\r\n";
        }
    }
    info.append(tmp_stream.str());
}
