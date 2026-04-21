//
// Created by Desktop on 2026/1/23.
//

#ifndef RAFTKVSTORAGE_KV_SKIP_LIST_H
#define RAFTKVSTORAGE_KV_SKIP_LIST_H
#include <string>
#include <utility>
#include <vector>
#include "kv_store_interface.h"
#include "skip_list.h"

class KVSkipList : public IKVStore {
public:
    KVSkipList();
    ~KVSkipList(){}

    std::optional<std::string> get(const std::string &key) override;
    void put(const std::string &key, const std::string &value) override;
    bool erase(const std::string &key) override;
    std::string makeSnapshot() override;
    void loadSnapshot(const std::string &snapshot) override;

    // clear exists kv and load
    void loadData(const std::vector<std::pair<std::string, std::string>>& data);
    std::vector<std::pair<std::string, std::string>> dumpData() const;

private:
    SkipList<std::string, std::string> m_SkipList{};
};

#endif //RAFTKVSTORAGE_KV_SKIP_LIST_H