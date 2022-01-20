//
// Created by 潘许飞 on 2021/10.
//

#include "InnerArray.h"
#include "EpochGuard.h"
#include "threadinfo.h"
#include "Key.h"

using namespace NVMMgr_ns;
namespace PART_ns {
//寻找到内部数组的bitmap的从低位到高位的第一个1的位置
size_t PART_ns::InnerArray::getRightmostSetBit() const {
    auto b = bitmap.load();
    auto pos = b._Find_first();
    assert(pos < InnerArrayLength);
    return pos;
}
//设置给定bit_pos位置的bitmap值为参数值to
void PART_ns::InnerArray::setBit(size_t bit_pos, bool to) {
    auto b = bitmap.load();
    b[bit_pos] = to;
    bitmap.store(b);
}
//获取给定位置pos的特征值finger
uint16_t PART_ns::InnerArray::getFingerPrint(size_t pos) const {
    if(pos>=InnerArrayLength){
        return 0;
    }
    //直接根据位置从fingerArray数组中获取特征值
    uint16_t re = reinterpret_cast<uint16_t>(fingerArray[pos].load());
    return re;
}
//通过给定的主键Key进行查找操作
N *InnerArray::lookup(const Key *k ,uint32_t & MatchKeyLen) const {
    //uint16_t finger_print = k->getFingerPrint();

    auto b = bitmap.load();

    int keyLen=0;
    uint16_t tmpFinger = 0;
    uint32_t thisLevel = this->getLevel();

#ifdef FIND_FIRST
    //若首个位置已使用，则i=0；若首个位置未使用，则初始位置为i=1.
    auto i = b[0] ? 0 : 1;
    while (i < InnerArrayLength) {
        auto finger = this->fingerArray[i].load();
        keyLen = this->lenArray[i].load();

        //计算特征值。若Key.h中getFingerPrint()函数的实现有所更改，该部分也需要更改。
        tmpFinger = 0;
        for (int i = 0; i < keyLen; i++) {
            tmpFinger = tmpFinger * 131 + k->fkey[thisLevel+i];
        }

        if (tmpFinger == finger) {
            MatchKeyLen = this->lenArray[i].load();
            return this->childArray[i].load();
        }
        //寻找下一个
        i = b._Find_next(i);
    }
#else
    for (int i = 0; i < InnerArrayLength; i++) {
        //若是空闲的、未使用的，则直接跳过
        if (b[i] == false)
            continue;
        auto finger = this->fingerArray[i].load();
        keyLen = this->lenArray[i].load();

        tmpFinger = 0;
        for (int i = 0; i < keyLen; i++) {
            tmpFinger = tmpFinger * 131 + k->fkey[thisLevel+i];
        }

        //如果特征值相等，则认为是匹配的.最好能再进行一次判断，避免Hash冲突
        if (tmpFinger == finger) {
            MatchKeyLen = this->lenArray[i].load();
            return this->childArray[i].load();
        }
    }
#endif

    return nullptr;
}
//插入操作
//bool InnerArray::insert(size_t key_len,uint8_t* fkey,uint16_t fingerPrint , N *n, bool flush) {
bool InnerArray::insert(size_t key_len,char* fkey,uint16_t fingerPrint , N *n, bool flush) {
    auto b = bitmap.load();
    b.flip();
    auto pos = b._Find_first();
    if (pos < InnerArrayLength) {
        b.flip();
        b[pos] = true;
        bitmap.store(b);

        fingerArray[pos].store(fingerPrint);
        if(flush)
            //flush the finger
            flush_data((void *)&fingerArray[pos],sizeof(std::atomic<uint16_t>));
        
        childArray[pos].store(n);
        if (flush)
            //flush the pointers
            flush_data((void *)&childArray[pos], sizeof(std::atomic<N *>));

        lenArray[pos].store(key_len);
        if(flush)
            flush_data((void *)&lenArray[pos],sizeof(std::atomic<uint8_t>));

        //keyArray[pos].store(new (alloc_new_node_from_size(key_len)) uint8_t[key_len]);
        keyArray[pos].store(new (alloc_new_node_from_size(key_len)) char[key_len]);
        memcpy(keyArray[pos], fkey, key_len);

        if(flush){
            //flush the religion which records the String Key
            flush_data((void *)keyArray[pos],key_len);
            //flsh the  pointer which record the address of String Key
            //flush_data((void *)&keyArray[pos], sizeof(std::atomic<uint8_t *>));
            flush_data((void *)&keyArray[pos], sizeof(std::atomic<char *>));
        }
        return true;
    } else {
        return false;
    }
}
//删除操作
bool InnerArray::remove(const Key *k) {
    uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;

    int keyLen =0;
    uint16_t tmpFinger =0;
    uint32_t thisLevel = this->getLevel();

    while (i < InnerArrayLength) {
        auto finger = this->fingerArray[i].load();
        keyLen = this->lenArray[i].load();

        //计算特征值。若Key.h中getFingerPrint()函数的实现有所更改，该部分也需要更改。
        tmpFinger = 0;
        for (int i = 0; i < keyLen; i++) {
            tmpFinger = tmpFinger * 131 + k->fkey[thisLevel+i];
        }


        //特征值匹配成功，找到需要删除的位置
        if (tmpFinger == finger) {
            //Question 0 需要注意的是，由于与Leaf Node相连的是LeafArray，
            //因此InnerArray删除某一个Key-Value Pair时，不一定要删除child node

            //目前的想法是，此时查询子节点的类型、子节点的索引项的数目、以及是否lookup成功，从而判断是否删除该子节点的条目(实现的可能有问题)
            uint32_t childCount=N::getCount(childArray[i]);
            //1.若子节点内部无任何节点项，则删除innerArray表项 2.若子节点为LeafArray，且LeafArray仅有一个Leaf Node子节点，且为我们需要删除的Key的LeafNode，才删除Inner Array表项
            if(childCount==0 || (childArray[i].load()->getType()==NTypes::LeafArray && childCount==1 && getLeafArray(childArray[i].load())->lookup(k)!=nullptr)){
                fingerArray[i].store(0);
                flush_data(&fingerArray[i], sizeof(std::atomic<uint16_t>));
                childArray[i].store(nullptr);
                flush_data(&childArray[i], sizeof(std::atomic<N *>));
                keyArray[i].store(nullptr);
                //flush_data(&keyArray[i], sizeof(std::atomic<uint8_t*>));
                flush_data(&keyArray[i], sizeof(std::atomic<char *>));
                lenArray[i].store(0);
                flush_data(&lenArray[i],sizeof(std::atomic<uint8_t>));

                //设置bitmap
                b[i] = false;
                bitmap.store(b);
            }
            return true;
        }
        i = b._Find_next(i);
    }
    return false;
}
//通过遍历特征值数组，重新加载Bitmap
void InnerArray::reload() {
    auto b = bitmap.load();
    for (int i = 0; i < InnerArrayLength; i++) {
        if (fingerArray[i].load() != 0) {
            b[i] = true;
        } else {
            b[i] = false;
        }
    }
    bitmap.store(b);
}
//打印调试信息
void InnerArray::graphviz_debug(std::ofstream &f) {
    char buf[1000] = {};
    sprintf(buf + strlen(buf), "node%lx [label=\"",
            reinterpret_cast<uintptr_t>(this));
    sprintf(buf + strlen(buf), "InnerArray\n");
    sprintf(buf + strlen(buf), "count: %zu\n", bitmap.load().count());
    sprintf(buf + strlen(buf), "\"]\n");

    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;
    while (i < InnerArrayLength) {
        auto finger = this->fingerArray[i].load();
        sprintf(buf + strlen(buf), "node%lx -- node%lx \n",
                    reinterpret_cast<uintptr_t>(this),
                    reinterpret_cast<uintptr_t>(this->childArray[i].load()));
        i = b._Find_next(i);
    }

    f << buf;
}

//获取主键Key的内容
char * InnerArray::getKey(int pos){
        //return reinterpret_cast<char*> (keyArray[pos]);
        return keyArray[pos].load();
}

//节点分裂
void InnerArray::splitAndUnlock(N *parentNode, uint8_t parentKey,
                               bool &need_restart) {
    //写锁
    parentNode->writeLockOrRestart(need_restart);

    if (need_restart) {
        this->writeUnlock();
        return;
    }
    //加载Bitmap与有效索引项的数目
    auto b = bitmap.load();
    auto innerArray_count = b.count();
    std::vector<char *> keys;
    //    char **keys = new char *[innerArray_count];
    std::vector<int> lens;
    //    int *lens = new int[innerArray_count];

    //存储主键与主键长度信息
    auto i = b[0] ? 0 : 1;
    while (i < InnerArrayLength) {
        auto finger = this->fingerArray[i].load();
        if (finger != 0) {
            keys.push_back(getKey(i));
            lens.push_back(lenArray[i]);
        }
        i = b._Find_next(i);
    }
    //    printf("spliting\n");

    //获取最长公共前缀
    std::vector<char> common_prefix;
    int level = 0;
    level = parentNode->getLevel() + 1;
    // assume keys are not substring of another key

    // todo: get common prefix can be optimized by binary search
    //假设存储的Key中，并不存在字串关系
    while (true) {
        bool out = false;
        //比较每一个字符串的第level个字符是否是相等的
        for (i = 0; i < innerArray_count; i++) {
            if (level < lens[i]) {
                if (i == 0) {
                    common_prefix.push_back(keys[i][level]);
                } else {
                    if (keys[i][level] != common_prefix.back()) {

                        common_prefix.pop_back();

                        out = true;
                        break;
                    }
                }
            } else {
                // assume keys are not substring of another key
                assert(0);
            }
        }
        if (out)
            break;
        level++;
    }
    //分配新的内部数组的空间。并插入子节点的地址
    std::map<char, InnerArray *> split_array;
    for (i = 0; i < innerArray_count; i++) {
        if (split_array.count(keys[i][level]) == 0) {
            split_array[keys[i][level]] =
                new (alloc_new_node_from_type(NTypes::InnerArray))
                    InnerArray(level);
        }
        split_array.at(keys[i][level])->insert(lenArray[i],keyArray[i] ,fingerArray[i] ,childArray[i], false);
    }

    N *n;
    //获取公共前缀的第一个字节
    uint8_t *prefix_start = reinterpret_cast<uint8_t *>(common_prefix.data());
    //获取公共前缀的长度
    auto prefix_len = common_prefix.size();
    //获取内部数组分裂后的 内部数组的个数
    auto innerArray_array_count = split_array.size();
    //根据分裂后内部数组的数量，来决定父节点采用N4还是N16、N48、N256
    if (innerArray_array_count <= 4) {
        n = new (alloc_new_node_from_type(NTypes::N4))
            N4(level, prefix_start, prefix_len);
    } else if (innerArray_array_count > 4 && innerArray_array_count <= 16) {
        n = new (alloc_new_node_from_type(NTypes::N16))
            N16(level, prefix_start, prefix_len);
    } else if (innerArray_array_count > 16 && innerArray_array_count <= 48) {
        n = new (alloc_new_node_from_type(NTypes::N48))
            N48(level, prefix_start, prefix_len);
    } else if (innerArray_array_count > 48 && innerArray_array_count <= 256) {
        n = new (alloc_new_node_from_type(NTypes::N256))
            N256(level, prefix_start, prefix_len);
    } else {
        assert(0);
    }
    //将分裂后的内部数组的信息存入父节点中
    for (const auto &p : split_array) {
        //unchecked_insert(n, p.first, setInnerArray(p.second), true);
        unchecked_insert(n, p.first, p.second, true);
        flush_data(p.second, sizeof(InnerArray));
    }
    //更改原本的父节点的指针，使其指向新生成的父节点
    N::change(parentNode, parentKey, n);
    //解锁
    parentNode->writeUnlock();

    this->writeUnlockObsolete();
    EpochGuard::DeleteNode(this);
}
//获取Bitmap的计数
uint32_t InnerArray::getCount() const { return bitmap.load().count(); }
//判断内部数组是否已满
bool InnerArray::isFull() const { return getCount() == InnerArrayLength; }


//获取在某个start-end范围内的子节点，根据要求决定是否对子节点进行排序

//Question 1
//需要注意的是：由于InnerArray中仅存储Key的部分切片与切片长度，因此比较时可能存在某些问题
std::vector<N *> InnerArray::getSortedChildren(const Key *start, const Key *end,
                                             int start_level,
                                             bool compare_start,
                                             bool compare_end) {
    std::vector<N *> children;
    std::map<Key*, N *> sortMap;

    auto b = bitmap.load();
    auto i = b[0] ? 0 : 1;

    //选出满足要求的子节点
    while (i < InnerArrayLength) {
        auto child = childArray[i].load();
        auto subKey = this->keyArray[i].load();

        i = b._Find_next(i);
        // start <= ptr < end
        if (compare_start) {
            auto lt_start = key_keylen_lt(subKey, this->lenArray[i] ,(char*)start->fkey, start->key_len ,start_level);
            if (lt_start == true) {
                continue;
            }
        }
        if (compare_end) {
            auto lt_end = key_keylen_lt(subKey,  this->lenArray[i] ,(char*)end->fkey, end->key_len ,start_level);
            if (lt_end == false) {
                continue;
            }
        }
        
        Key* tmpKey = tmpKey->make_leaf(subKey,lenArray[i],0);
        sortMap.insert(std::pair<Key*,N*>(tmpKey,child));
    }
#ifdef SORT_LEAVES
    //对子节点进行排序
    //将lambda表达式作为参数传递给std::sort作为参数
    std::sort(sortMap.begin(), sortMap.end(),
              [start_level](pair<Key*,N*> *a, pair<Key*,N*> *b) -> bool {
                  key_keylen_lt(a->first->fkey, a->first->key_len, b->first->fkey, b->first->key_len,start_level)
              });
#endif
    for(auto iter=sortMap.rbegin();iter!=sortMap.rend();iter++){
        children.push_back(iter->second);
    }
    return children;
}


//Question 2
//更新操作
/**
 * 
 * 
 * (1)由于InnerArray的特征值代表的Key切片的位数不固定，因此在进行Update操作时，无法直接进行Update。
 * 暂时的想法是，在进行Update操作时，转化为remove与insert两个操作
 * 
 * 
 * (2)Update操作应该是指：根据某一给定的Key值，寻找其对应的Value，并用新的Value值进行替换。而Key值并不会进行修改。因此，InnerArray不需要update操作。
 * 
 * */

bool InnerArray::update(const Key *k, N *child) {
    //uint16_t finger_print = k->getFingerPrint();
    auto b = bitmap.load();

    int keyLen=0;
    uint16_t tmpFinger = 0;
    uint32_t thisLevel = this->getLevel();

#ifdef FIND_FIRST
    //若首个位置已使用，则i=0；若首个位置未使用，则初始位置为i=1.
    auto i = b[0] ? 0 : 1;
    while (i < InnerArrayLength) {
        auto finger = this->fingerArray[i].load();
        keyLen = this->lenArray[i].load();

        if(finger!=0){
            //计算特征值。若Key.h中getFingerPrint()函数的实现有所更改，该部分也需要更改。
            tmpFinger = 0;
            for (int i = 0; i < keyLen; i++) {
                tmpFinger = tmpFinger * 131 + k->fkey[thisLevel+i];
            }

            if (tmpFinger == finger) {

                fingerArray[i].store(0);
                flush_data(&fingerArray[i], sizeof(std::atomic<uint16_t>));

                childArray[i].store(child);
                flush_data(&childArray[i], sizeof(std::atomic<N *>));

                keyArray[i].store(nullptr);
                flush_data(&keyArray[i], sizeof(std::atomic<uint8_t*>));

                lenArray[i].store(0);
                flush_data(&lenArray[i],sizeof(std::atomic<uint8_t>));

                return true;
            }
        }
        //寻找下一个
        i = b._Find_next(i);
    }
#else
    for (int i = 0; i < InnerArrayLength; i++) {
        if (b[i] == false)
            continue;
        auto finger = this->fingerArray[i].load();
        if (finger != 0) {
            fingerArray[i].store(news);
            flush_data(&fingerArray[i], sizeof(std::atomic<uintptr_t>));
            return true;
        }
    }
#endif
    return false;
}

//Question 3
//返回任意子节点（其他node类型的该函数的实现 都是 优先返回叶子节点，但是由于InnerArray不与叶子节点相连，因此直接返回任意节点即可
N *InnerArray::getAnyChild() const {
    auto b = bitmap.load();
    auto i = b._Find_first();
    if (i == InnerArrayLength) {
        return nullptr;
    } else {
        return childArray[i];
    }
}

} // namespace PART_ns