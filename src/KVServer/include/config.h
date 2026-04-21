//
// Created by Desktop on 2026/1/27.
//

#ifndef RAFTKVSTORAGE_CONFIG_H
#define RAFTKVSTORAGE_CONFIG_H

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
struct KVNodeConfig {
    int id;
    std::string bindAddr;

    std::string persistDir;
    std::string logDir;
    std::vector<std::string> peerAddr;
    std::vector<int> peerId;

    uint32_t maxRaftLogCount;
    uint32_t maxRaftStateSize;

    std::chrono::milliseconds queueMaxWaitTime;

    static std::optional<KVNodeConfig> parseFromFile(const std::string &path, std::string* error = nullptr);
    static bool checkConfig(const KVNodeConfig &cfg, std::string *error = nullptr);
};


#endif // RAFTKVSTORAGE_CONFIG_H
