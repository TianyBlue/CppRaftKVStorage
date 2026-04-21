//
// Created by Desktop on 2026/1/23.
//

#ifndef RAFTKVSTORAGE_KV_STORE_INTERFACE_H
#define RAFTKVSTORAGE_KV_STORE_INTERFACE_H

#include <string>
#include <optional>

class IKVStore {
public:
    virtual ~IKVStore(){}

    virtual std::optional<std::string> get(const std::string& key) = 0;
    virtual void put(const std::string& key, const std::string& value) = 0;
    virtual bool erase(const std::string &key) = 0;

    virtual std::string makeSnapshot() = 0;
    virtual void loadSnapshot(const std::string& snapshot) = 0;
};


#endif //RAFTKVSTORAGE_KV_STORE_INTERFACE_H