//
// Created by Desktop on 2026/1/24.
//

#ifndef RAFTKVSTORAGE_SKIP_LIST_H
#define RAFTKVSTORAGE_SKIP_LIST_H

#include <cstring>
#include <random>
#include <stdexcept>
#include <vector>

/**
 * 使用SkipList(而不是unordered_map)的理由
 * 1. 便于实现range_scan
 * 2. LevelDB / RocksDB也这么使用
 * 3. 实现更方便
 * 4. Snapshot / Checkpoint 更自然
 */


/**
 * SkipList的期望高度 H = Log(1/p)(N)
 * p = 0.5 or p = 0.25(处理大数据量)
 * p = 0.25, N = 10^7(百万), H = 12(可以处理百万级以上KV)
 */
template<typename K, typename V>
class SkipList {
    class SkipNode {
    public:
        K m_Key;
        V m_Val;
        int m_Level{};

        // 柔性数组
        SkipNode *m_Next[];
        template<typename _K, typename _V>
        SkipNode(_K &&k, _V &&v, int level) : m_Key(std::forward<_K>(k)), m_Val(std::forward<_V>(v)), m_Level(level) {}

        template<typename _K, typename _V>
        static SkipNode* create(_K &&k, _V &&v, int level) {
            void *mem = ::operator new(sizeof(SkipNode) + sizeof(SkipNode*) * level);
            auto* node = new (mem) SkipNode(std::forward<_K>(k), std::forward<_V>(v), level);
            memset(node->m_Next, 0, sizeof(SkipNode*) * level);
            return node;
        }

        static void destroy(SkipNode* node) {
            if (node) ::operator delete(node);
        }
    };

public:
    explicit SkipList(int maxLevel = 16, float prob = 0.25)
        :m_MaxLevel(maxLevel), m_Prob(prob)
    {
        m_Head = SkipNode::create(K(), V(), maxLevel);
    }

    ~SkipList() {
        // SkipNode* curr = m_Head;
        // while (curr) {
        //     SkipNode* temp = curr->m_Next[0];
        //     SkipNode::destroy(curr);
        //     curr = temp;
        // }
        // SkipNode::destroy(curr);
        clear();
        SkipNode::destroy(m_Head);
    }

    class Iterator {
        const SkipNode* m_CurrNode{};
        Iterator(const SkipNode* node): m_CurrNode(node) {}

        friend class SkipList;
    public:
        bool hasNext() const {
            return m_CurrNode != nullptr;
        }

        std::pair<K, V> next() {
            if (!hasNext()) {
                throw std::runtime_error("No more element");
            }

            auto pair = std::make_pair(m_CurrNode->m_Key, m_CurrNode->m_Val);
            m_CurrNode = m_CurrNode->m_Next[0];
            return pair;
        }
    };

    Iterator iterator() const {
        return Iterator(m_Head->m_Next[0]);
    }

    size_t size() const {
        return m_Size;
    }

    void clear() {
        SkipNode* curr = m_Head;
        while (curr) {
            SkipNode* temp = curr->m_Next[0];
            SkipNode::destroy(curr);
            curr = temp;
        }
        SkipNode::destroy(curr);

        m_Head = SkipNode::create(K(), V(), m_MaxLevel);
        m_Size = 0;
    }

    bool get(const K& k, V& v) const {
        const SkipNode* currNode = m_Head;
        for (int i = m_MaxLevel - 1; i >= 0; --i) {
            while (currNode->m_Next[i] && currNode->m_Next[i]->m_Key < k) {
                currNode = currNode->m_Next[i];
            }
        }

        if (!currNode->m_Next[0] || currNode->m_Next[0]->m_Key != k) {
            return false;
        }
        v = currNode->m_Next[0]->m_Val;
        return true;
    }

    template<typename _K, typename _V>
    void put(_K&& k, _V&& v) {
        std::vector<SkipNode*> nodePrevs(m_MaxLevel, nullptr);
        SkipNode* currNode = m_Head;
        for (int i = m_MaxLevel - 1; i >= 0; --i) {
            while (currNode->m_Next[i] && currNode->m_Next[i]->m_Key < k) {
                currNode = currNode->m_Next[i];
            }
            nodePrevs[i] = currNode;
        }

        // Key已存在，直接更新值
        if (currNode->m_Next[0] && currNode->m_Next[0]->m_Key == k) {
            currNode->m_Next[0]->m_Val = std::forward<_V>(v);
            return;
        }

        // Key不存在，添加
        int level = getRandomLevel();
        SkipNode* node = SkipNode::create(std::forward<_K>(k), std::forward<_V>(v), level);
        for (int i{}; i < level; ++i) {
            node->m_Next[i] = nodePrevs[i]->m_Next[i];
            nodePrevs[i]->m_Next[i] = node;
        }
        ++m_Size;
    }

    bool erase(const K& k) {
        SkipNode* currNode{m_Head};
        std::vector<SkipNode*> nodePrevs(m_MaxLevel);
        for (int i = m_MaxLevel - 1; i >= 0; --i) {
            while (currNode->m_Next[i] && currNode->m_Next[i]->m_Key < k) {
                currNode = currNode->m_Next[i];
            }

            nodePrevs[i] = currNode;
        }

        // Key不存在
        if (!currNode->m_Next[0] || currNode->m_Next[0]->m_Key != k) {
            return false;
        }

        // Key存在，执行删除
        SkipNode* eraseNode = currNode->m_Next[0];
        int level = eraseNode->m_Level;
        for (int i = 0; i < level; ++i) {
            nodePrevs[i]->m_Next[i] = eraseNode->m_Next[i];
        }

        SkipNode::destroy(eraseNode);
        --m_Size;
        return true;
    }

private:
    SkipNode* m_Head{};
    int m_MaxLevel{};
    float m_Prob{};
    size_t m_Size{};

    int getRandomLevel() const {
        static std::random_device rd;
        static std::mt19937_64 gen(rd());

        std::uniform_real_distribution<> dis(0.0, 1.0);
        int level{1};
        while (dis(gen) < m_Prob && level < m_MaxLevel) {
            ++level;
        }
        return level;
    }
};

#endif // RAFTKVSTORAGE_SKIP_LIST_H
