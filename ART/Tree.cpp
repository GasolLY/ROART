#include "Tree.h"
#include <assert.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <functional>
#include "Epoche.cpp"
#include "N.cpp"

#ifdef ARTDEBUG
std::ostream &art_cout = std::cout;
#else
std::ofstream dev_null("/dev/null");
std::ostream &art_cout = dev_null;
#endif

namespace ART_ROWEX {

Tree::Tree() : root(new N256(0, {})) {
    N::clflush((char *)root, sizeof(N256), true, true);
}

Tree::~Tree() {
    N::deleteChildren(root);
    N::deleteNode(root);
}

ThreadInfo Tree::getThreadInfo() { return ThreadInfo(this->epoche); }

void *Tree::lookup(const Key *k, ThreadInfo &threadEpocheInfo) const {
    EpocheGuardReadonly epocheGuard(threadEpocheInfo);
    N *node = root;
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;

    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    // art_cout << "Searching key " << k->fkey << std::endl;
    while (true) {
        switch (checkPrefix(node, k, level)) {  // increases level
            case CheckPrefixResult::NoMatch:
                return NULL;
            case CheckPrefixResult::OptimisticMatch:
                optimisticPrefixMatch = true;
                // fallthrough
            case CheckPrefixResult::Match: {
                if (k->getKeyLen() <= level) {
                    return NULL;
                }
                parentref = curref;
                curref = N::getChild(k->fkey[level], node);
                if (curref == nullptr)
                    node = nullptr;
                else
                    node = N::clearDirty(curref->load());

                if (node == nullptr) {
                    N::helpFlush(parentref);
                    N::helpFlush(curref);
                    return NULL;
                }
                if (N::isLeaf(node)) {
                    Leaf *ret = N::getLeaf(node);
                    N::helpFlush(parentref);
                    N::helpFlush(curref);
                    if (level < k->getKeyLen() - 1 || optimisticPrefixMatch) {
                        if (ret->checkKey(k)) {
                            return &ret->value;
                        } else {
                            return NULL;
                        }
                    } else {
                        return &ret->value;
                    }
                }
            }
        }
        level++;
    }
}

bool Tree::lookupRange(const Key *start, const Key *end, const Key *continueKey,
                       Leaf *result[], std::size_t resultSize,
                       std::size_t &resultsFound,
                       ThreadInfo &threadEpocheInfo) const {
    for (uint32_t i = 0; i < std::min(start->getKeyLen(), end->getKeyLen());
         ++i) {
        if (start->fkey[i] > end->fkey[i]) {
            resultsFound = 0;
            return false;
        } else if (start->fkey[i] < end->fkey[i]) {
            break;
        }
    }
    EpocheGuard epocheGuard(threadEpocheInfo);
    Leaf *toContinue = NULL;
    bool restart;
    std::function<void(const N *)> copy = [&result, &resultSize, &resultsFound,
                                           &toContinue, &copy](const N *node) {
        if (N::isLeaf(node)) {
            if (resultsFound == resultSize) {
                toContinue = N::getLeaf(node);
                return;
            }
            // result[resultsFound] =
            // reinterpret_cast<TID>((N::getLeaf(node))->value);
            result[resultsFound] = N::getLeaf(node);
            resultsFound++;
        } else {
            std::tuple<uint8_t, N *> children[256];
            uint32_t childrenCount = 0;
            N::getChildren(node, 0u, 255u, children, childrenCount);
            for (uint32_t i = 0; i < childrenCount; ++i) {
                const N *n = std::get<1>(children[i]);
                copy(n);
                if (toContinue != NULL) {
                    break;
                }
            }
        }
    };
    std::function<void(const N *, uint32_t)> findStart =
        [&copy, &start, &findStart, &toContinue, &restart, this](
            const N *node, uint32_t level) {
            if (N::isLeaf(node)) {
                copy(node);
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, start, level, loadKey);
            switch (prefixResult) {
                case PCCompareResults::Bigger:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel =
                        (start->getKeyLen() > level) ? start->fkey[level] : 0;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    N::getChildren(node, startLevel, 255, children,
                                   childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        const N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, level + 1);
                        } else if (k > startLevel) {
                            copy(n);
                        }
                        if (toContinue != NULL || restart) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::SkippedLevel:
                    restart = true;
                    break;
                case PCCompareResults::Smaller:
                    break;
            }
        };
    std::function<void(const N *, uint32_t)> findEnd =
        [&copy, &end, &toContinue, &restart, &findEnd, this](const N *node,
                                                             uint32_t level) {
            if (N::isLeaf(node)) {
                return;
            }

            PCCompareResults prefixResult;
            prefixResult = checkPrefixCompare(node, end, level, loadKey);

            switch (prefixResult) {
                case PCCompareResults::Smaller:
                    copy(node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t endLevel =
                        (end->getKeyLen() > level) ? end->fkey[level] : 255;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    N::getChildren(node, 0, endLevel, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        const N *n = std::get<1>(children[i]);
                        if (k == endLevel) {
                            findEnd(n, level + 1);
                        } else if (k < endLevel) {
                            copy(n);
                        }
                        if (toContinue != NULL || restart) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Bigger:
                    break;
                case PCCompareResults::SkippedLevel:
                    restart = true;
                    break;
            }
        };

restart:
    restart = false;
    resultsFound = 0;

    uint32_t level = 0;
    N *node = nullptr;
    N *nextNode = root;
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;

    while (true) {
        if (!(node = nextNode) || toContinue) break;
        PCEqualsResults prefixResult;
        prefixResult = checkPrefixEquals(node, level, start, end, loadKey);
        switch (prefixResult) {
            case PCEqualsResults::SkippedLevel:
                goto restart;
            case PCEqualsResults::NoMatch: {
                return false;
            }
            case PCEqualsResults::Contained: {
                copy(node);
                break;
            }
            case PCEqualsResults::BothMatch: {
                uint8_t startLevel =
                    (start->getKeyLen() > level) ? start->fkey[level] : 0;
                uint8_t endLevel =
                    (end->getKeyLen() > level) ? end->fkey[level] : 255;
                if (startLevel != endLevel) {
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    N::getChildren(node, startLevel, endLevel, children,
                                   childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        const N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, level + 1);
                        } else if (k > startLevel && k < endLevel) {
                            copy(n);
                        } else if (k == endLevel) {
                            findEnd(n, level + 1);
                        }
                        if (restart) {
                            goto restart;
                        }
                        if (toContinue) {
                            break;
                        }
                    }
                } else {
                    parentref = curref;
                    curref = N::getChild(startLevel, node);
                    if (curref == nullptr)
                        nextNode = nullptr;
                    else
                        nextNode = N::clearDirty(curref->load());
                    level++;
                    continue;
                }
                break;
            }
        }
        break;
    }

    if (toContinue != NULL) {
        Key *newkey =
            new Key(toContinue->key, toContinue->key_len, toContinue->value);
        continueKey = newkey;
        return true;
    } else {
        return false;
    }
}

bool Tree::checkKey(const Key *ret, const Key *k) const {
    if (ret->getKeyLen() == k->getKeyLen() &&
        memcmp(ret->fkey, k->fkey, k->getKeyLen()) == 0) {
        return true;
    }
    return false;
}

typename Tree::OperationResults Tree::insert(const Key *k,
                                             ThreadInfo &epocheInfo) {
    // N::clflush((char *)k, sizeof(Key) + k->key_len, false, true);
    EpocheGuard epocheGuard(epocheInfo);
restart:
    bool needRestart = false;
    // art_cout << __func__ << "Start/Restarting insert.." << std::endl;
    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->getVersion();

        uint32_t nextLevel = level;

        uint8_t nonMatchingKey;
        Prefix remainingPrefix;
        switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey,
                                       remainingPrefix,
                                       this->loadKey)) {  // increases level
            case CheckPrefixPessimisticResult::SkippedLevel:
                goto restart;
            case CheckPrefixPessimisticResult::NoMatch: {
                assert(nextLevel < k->getKeyLen());  // prevent duplicate key
                node->lockVersionOrRestart(v, needRestart);
                if (needRestart) goto restart;

                // 1) Create new node which will be parent of node, Set common
                // prefix, level to this node
                Prefix prefi = node->getPrefi();
                prefi.prefixCount = nextLevel - level;
                auto newNode = new N4(nextLevel, prefi);

                // 2)  add node and (tid, *k) as children
                Leaf *newLeaf = new Leaf(k);
                newNode->insert(k->fkey[nextLevel], N::setLeaf(newLeaf), false);
                newNode->insert(nonMatchingKey, node, false);
                N::clflush((char *)newNode, sizeof(N4), true, true);

                // 3) lockVersionOrRestart, update parentNode to point to the
                // new node, unlock
                parentNode->writeLockOrRestart(needRestart);
                if (needRestart) {
                    delete newNode;
                    node->writeUnlock();
                    goto restart;
                }
                N::change(parentNode, parentKey, newNode);
                parentNode->writeUnlock();

                // 4) update prefix of node, unlock
                node->setPrefix(
                    remainingPrefix.prefix,
                    node->getPrefi().prefixCount - ((nextLevel - level) + 1),
                    true);

                node->writeUnlock();
                return OperationResults::Success;

            }  // end case  NoMatch
            case CheckPrefixPessimisticResult::Match:
                break;
        }
        assert(nextLevel < k->getKeyLen());  // prevent duplicate key
        // TODO
        // maybe one string is substring of another, so it fkey[level] will be 0
        // solve problem of substring
        level = nextLevel;
        nodeKey = k->fkey[level];

        parentref = curref;
        curref = N::getChild(nodeKey, node);
        if (curref == nullptr)
            nextNode = nullptr;
        else
            nextNode = N::clearDirty(curref->load());

        if (nextNode == nullptr) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart) goto restart;

            Leaf *newLeaf = new Leaf(k);
            N::insertAndUnlock(node, parentNode, parentKey, nodeKey,
                               N::setLeaf(newLeaf), epocheInfo, needRestart);
            if (needRestart) goto restart;
            return OperationResults::Success;
        }
        if (N::isLeaf(nextNode)) {
            node->lockVersionOrRestart(v, needRestart);
            if (needRestart) goto restart;
            Leaf *key = N::getLeaf(nextNode);

            level++;
            // assert(level < key->getKeyLen());
            // prevent inserting when
            // prefix of key exists already
            // but if I want to insert a prefix of this key, i also need to
            // insert successfully
            uint32_t prefixLength = 0;
            while (level + prefixLength <
                       std::min(k->getKeyLen(), key->getKeyLen()) &&
                   key->fkey[level + prefixLength] ==
                       k->fkey[level + prefixLength]) {
                prefixLength++;
            }
            if (k->getKeyLen() == key->getKeyLen() &&
                level + prefixLength == k->getKeyLen()) {
                // duplicate key
                node->writeUnlock();
                return OperationResults::Existed;
            }

            auto n4 =
                new N4(level + prefixLength, &k->fkey[level], prefixLength);
            Leaf *newLeaf = new Leaf(k);
            n4->insert(k->fkey[level + prefixLength], N::setLeaf(newLeaf),
                       false);
            n4->insert(key->fkey[level + prefixLength], nextNode, false);
            N::clflush((char *)n4, sizeof(N4), true, true);

            N::change(node, k->fkey[level - 1], n4);
            node->writeUnlock();
            return OperationResults::Success;
        }
        level++;
    }
}

typename Tree::OperationResults Tree::remove(const Key *k,
                                             ThreadInfo &threadInfo) {
    EpocheGuard epocheGuard(threadInfo);
restart:
    bool needRestart = false;

    N *node = nullptr;
    N *nextNode = root;
    N *parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint32_t level = 0;
    std::atomic<N *> *parentref = nullptr;
    std::atomic<N *> *curref = nullptr;
    // bool optimisticPrefixMatch = false;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->getVersion();

        switch (checkPrefix(node, k, level)) {  // increases level
            case CheckPrefixResult::NoMatch:
                if (N::isObsolete(v) || !node->readUnlockOrRestart(v)) {
                    goto restart;
                }
                return OperationResults::NotFound;
            case CheckPrefixResult::OptimisticMatch:
                // fallthrough
            case CheckPrefixResult::Match: {
                // if (level >= k->getKeyLen()) {
                //     // key is too short
                //     // but it next fkey is 0
                //     return OperationResults::NotFound;
                // }
                nodeKey = k->fkey[level];

                parentref = curref;
                curref = N::getChild(nodeKey, node);
                if (curref == nullptr)
                    nextNode = nullptr;
                else
                    nextNode = N::clearDirty(curref->load());

                if (nextNode == nullptr) {
                    if (N::isObsolete(v) ||
                        !node->readUnlockOrRestart(v)) {  // TODO benötigt??
                        goto restart;
                    }
                    return OperationResults::NotFound;
                }
                if (N::isLeaf(nextNode)) {
                    node->lockVersionOrRestart(v, needRestart);
                    if (needRestart) goto restart;

                    Leaf *leaf = N::getLeaf(nextNode);
                    if (!leaf->checkKey(k)) {
                        node->writeUnlock();
                        return OperationResults::NotFound;
                    }
                    assert(parentNode == nullptr || node->getCount() != 1);
                    if (node->getCount() == 2 && node != root) {
                        // 1. check remaining entries
                        N *secondNodeN;
                        uint8_t secondNodeK;
                        std::tie(secondNodeN, secondNodeK) =
                            N::getSecondChild(node, nodeKey);
                        if (N::isLeaf(secondNodeN)) {
                            parentNode->writeLockOrRestart(needRestart);
                            if (needRestart) {
                                node->writeUnlock();
                                goto restart;
                            }

                            // N::remove(node, k[level]); not necessary
                            N::change(parentNode, parentKey, secondNodeN);

                            parentNode->writeUnlock();
                            node->writeUnlockObsolete();
                            this->epoche.markNodeForDeletion(node, threadInfo);
                        } else {
                            uint64_t vChild = secondNodeN->getVersion();
                            secondNodeN->lockVersionOrRestart(vChild,
                                                              needRestart);
                            if (needRestart) {
                                node->writeUnlock();
                                goto restart;
                            }
                            parentNode->writeLockOrRestart(needRestart);
                            if (needRestart) {
                                node->writeUnlock();
                                secondNodeN->writeUnlock();
                                goto restart;
                            }

                            // N::remove(node, k[level]); not necessary
                            N::change(parentNode, parentKey, secondNodeN);

                            secondNodeN->addPrefixBefore(node, secondNodeK);

                            parentNode->writeUnlock();
                            node->writeUnlockObsolete();
                            this->epoche.markNodeForDeletion(node, threadInfo);
                            secondNodeN->writeUnlock();
                        }
                    } else {
                        N::removeAndUnlock(node, k->fkey[level], parentNode,
                                           parentKey, threadInfo, needRestart);
                        if (needRestart) goto restart;
                    }
                    return OperationResults::Success;
                }
                level++;
            }
        }
    }
}

typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key *k,
                                                   uint32_t &level) {
    if (k->getKeyLen() <= n->getLevel()) {
        return CheckPrefixResult::NoMatch;
    }
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        level = n->getLevel();
        return CheckPrefixResult::OptimisticMatch;
    }
    if (p.prefixCount > 0) {
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < std::min(p.prefixCount, maxStoredPrefixLength); ++i) {
            if (p.prefix[i] != k->fkey[level]) {
                return CheckPrefixResult::NoMatch;
            }
            ++level;
        }
        if (p.prefixCount > maxStoredPrefixLength) {
            // level += p.prefixCount - maxStoredPrefixLength;
            level = n->getLevel();
            return CheckPrefixResult::OptimisticMatch;
        }
    }
    return CheckPrefixResult::Match;
}

typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(
    N *n, const Key *k, uint32_t &level, uint8_t &nonMatchingKey,
    Prefix &nonMatchingPrefix, LoadKeyFunction loadKey) {
    Prefix p = n->getPrefi();
    // art_cout << __func__ << ":Actual=" << p.prefixCount + level <<
    // ",Expected=" << n->getLevel() << std::endl;
    if (p.prefixCount + level != n->getLevel()) {
        // Intermediate or inconsistent state from path compression "split" or
        // "merge" is detected Inconsistent path compressed prefix should be
        // recovered in here
        bool needRecover = false;
        auto v = n->getVersion();
        art_cout << __func__ << " INCORRECT LEVEL ENCOUNTERED " << std::endl;
        n->lockVersionOrRestart(v, needRecover);
        if (!needRecover) {
            // Inconsistent state due to prior system crash is suspected --> Do
            // recovery
            // TODO: recovery algorithm will be added
            // 1) Picking up arbitrary two leaf nodes and then 2) rebuilding
            // correct compressed prefix
            art_cout << __func__ << " PERFORMING RECOVERY" << std::endl;
            uint32_t discrimination =
                (n->getLevel() > level ? n->getLevel() - level
                                       : level - n->getLevel());
            Leaf *kr = N::getAnyChildTid(n);
            p.prefixCount = discrimination;
            for (uint32_t i = 0;
                 i < std::min(discrimination, maxStoredPrefixLength); i++)
                p.prefix[i] = kr->fkey[level + i];
            n->setPrefix(p.prefix, p.prefixCount, true);
            n->writeUnlock();
        }

        // path compression merge is in progress --> restart from root
        // path compression split is in progress --> skipping an intermediate
        // compressed prefix by using level (invariant)
        if (p.prefixCount + level < n->getLevel()) {
            return CheckPrefixPessimisticResult::SkippedLevel;
        }
    }

    if (p.prefixCount > 0) {
        uint32_t prevLevel = level;
        Leaf *kt = NULL;
        bool load_flag = false;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i >= maxStoredPrefixLength && !load_flag) {
                // Optimistic path compression
                kt = N::getAnyChildTid(n);
                load_flag = true;
            }
            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
            if (curKey != k->fkey[level]) {
                nonMatchingKey = curKey;
                if (p.prefixCount > maxStoredPrefixLength) {
                    if (i < maxStoredPrefixLength) {
                        kt = N::getAnyChildTid(n);
                    }
                    for (uint32_t j = 0;
                         j < std::min((p.prefixCount - (level - prevLevel) - 1),
                                      maxStoredPrefixLength);
                         ++j) {
                        nonMatchingPrefix.prefix[j] = kt->fkey[level + j + 1];
                    }
                } else {
                    for (uint32_t j = 0; j < p.prefixCount - i - 1; ++j) {
                        nonMatchingPrefix.prefix[j] = p.prefix[i + j + 1];
                    }
                }
                return CheckPrefixPessimisticResult::NoMatch;
            }
            ++level;
        }
    }
    return CheckPrefixPessimisticResult::Match;
}

typename Tree::PCCompareResults Tree::checkPrefixCompare(
    const N *n, const Key *k, uint32_t &level, LoadKeyFunction loadKey) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        return PCCompareResults::SkippedLevel;
    }
    if (p.prefixCount > 0) {
        Leaf *kt = NULL;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i == maxStoredPrefixLength) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
            }
            uint8_t kLevel = (k->getKeyLen() > level) ? k->fkey[level] : 0;

            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
            if (curKey < kLevel) {
                return PCCompareResults::Smaller;
            } else if (curKey > kLevel) {
                return PCCompareResults::Bigger;
            }
            ++level;
        }
    }
    return PCCompareResults::Equal;
}

typename Tree::PCEqualsResults Tree::checkPrefixEquals(
    const N *n, uint32_t &level, const Key *start, const Key *end,
    LoadKeyFunction loadKey) {
    Prefix p = n->getPrefi();
    if (p.prefixCount + level < n->getLevel()) {
        return PCEqualsResults::SkippedLevel;
    }
    if (p.prefixCount > 0) {
        Leaf *kt = NULL;
        for (uint32_t i = ((level + p.prefixCount) - n->getLevel());
             i < p.prefixCount; ++i) {
            if (i == maxStoredPrefixLength) {
                // loadKey(N::getAnyChildTid(n), kt);
                kt = N::getAnyChildTid(n);
            }
            uint8_t startLevel =
                (start->getKeyLen() > level) ? start->fkey[level] : 0;
            uint8_t endLevel =
                (end->getKeyLen() > level) ? end->fkey[level] : 0;

            uint8_t curKey =
                i >= maxStoredPrefixLength ? kt->fkey[level] : p.prefix[i];
            if (curKey > startLevel && curKey < endLevel) {
                return PCEqualsResults::Contained;
            } else if (curKey < startLevel || curKey > endLevel) {
                return PCEqualsResults::NoMatch;
            }
            ++level;
        }
    }
    return PCEqualsResults::BothMatch;
}
}  // namespace ART_ROWEX