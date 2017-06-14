#ifndef SSDB_KV_H_
#define SSDB_KV_H_

#include "ssdb_impl.h"

#define encode_slotskvData_key EncodeMetaKey
#define encode_slotskvData_val EncodeMetaVal
#define decode_slotskvData_key DecodeMetaKey

static inline 
int incrVerify(int refcount, int incr){
    if (refcount == 0){//没找到这个key,并且incr大于0,表示是新值
        return incr > 0 ? 1 : 0;
    }

    //找到这个key,并且incr < 0
    return incr < 0 ? -1 : 0;
}

static inline
int incrSlotsRefCount(SSDBImpl *ssdb, const Bytes &name, TMH &metainfo, int incr, bool clear_ttl = true)
{
    //lock表示是ttl类里面删除调用的,防止死锁
    if (clear_ttl){
        ssdb->removeExpire(name, metainfo);
    }
    
	//先判断是否需要自增自减引用个数
	if (incrVerify(metainfo.refcount, incr) == 0){
        return 0;
    }
    ssdb->calculateSlotRefer(Slots::encode_slot_id(name), incr);
    return 1;
}

template<typename T>
void incrMultiSlotsRefCount(SSDBImpl *ssdb, std::map<T, TMH> &names, int incr){
	
	int64_t slotvalue[HASH_SLOTS_SIZE] = {0};
    for (auto it = names.begin(); it != names.end(); ++it) {
        if (incrVerify(it->second.refcount, incr) != 0) {
            slotvalue[Slots::slots_num((it->first).data(), NULL, NULL)] += incr;
        }
    }

    for (unsigned int i = 0; i < HASH_SLOTS_SIZE; ++i) {
        if (slotvalue[i] == 0) {
            continue;
        }
        ssdb->calculateSlotRefer(Slots::encode_slot_id(i), slotvalue[i]);
    }
}

#endif
