//
// Created by 潘许飞 on 2021/10.
//

#ifndef P_ART_INNERARRAY_H
#define P_ART_INNERARRAY_H
#include "N.h"
#include <atomic>
#include <bitset>
#include <functional>

namespace PART_ns {

//内部数组容量
const size_t InnerArrayLength = 64;
//计算Finger特征值的偏移量
//const size_t FingerPrintShift = 48;

//内部节点数组类
class InnerArray : public N {
  public:
    //内部数组,存储子节点的指针
    std::atomic<N *> childArray[InnerArrayLength];
    //内部数组,存储特征值
    std::atomic<uint16_t> fingerArray[InnerArrayLength];
    //内部数组，存储主键Key切片的内容。用于数组分裂
    //std::atomic<uint8_t *>keyArray[InnerArrayLength];
    std::atomic<char *> keyArray[InnerArrayLength];
    //内部数组，存储主键Key的长度
    std::atomic<uint8_t>lenArray[InnerArrayLength];
    //内部数组的bitmap
    std::atomic<std::bitset<InnerArrayLength>>
        bitmap; // 0 means used slot; 1 means empty slot


  public:
    //构造函数。分配空间。初始化level=-1
    InnerArray(uint32_t level = -1) : N(NTypes::InnerArray, level, {}, 0) {
        bitmap.store(std::bitset<InnerArrayLength>{}.reset());
        memset(childArray, 0, sizeof(childArray));
        memset(fingerArray, 0, sizeof(fingerArray));
        memset(keyArray, 0, sizeof(keyArray));
        memset(lenArray, 0, sizeof(lenArray));
    }

    virtual ~InnerArray() {}

    size_t getRightmostSetBit() const;

    void setBit(size_t bit_pos, bool to = true);

    uint16_t getFingerPrint(size_t pos) const;

    Leaf *getLeafAt(size_t pos) const;

    N *getAnyChild() const;

    static uintptr_t fingerPrintLeaf(uint16_t fingerPrint, Leaf *l);

    N *lookup(const Key *k ,uint32_t & MatchKeyLen) const;

    bool update(const Key *k, N *child);

    //bool insert(size_t key_len,uint8_t* fkey,uint16_t fingerPrint , N *n, bool flush);
    bool insert(size_t key_len,char* fkey,uint16_t fingerPrint , N *n, bool flush);

    bool remove(const Key *k);

    void reload();

    uint32_t getCount() const;

    bool isFull() const;

    void splitAndUnlock(N *parentNode, uint8_t parentKey, bool &need_restart);

    std::vector<N *> getSortedChildren(const Key *start, const Key *end,int start_level,bool compare_start,bool compare_end);

    void graphviz_debug(std::ofstream &f);

    char * getKey(int pos);

} __attribute__((aligned(64)));
} // namespace PART_ns
#endif // P_ART_LEAFARRAY_H
