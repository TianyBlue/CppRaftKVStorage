//
// Created by Desktop on 2026/1/27.
//

#include "config.h"
#include <algorithm>
#include <format>
#include <filesystem>
#include <yaml-cpp/yaml.h>

namespace {
std::string resolveDirPath(const std::filesystem::path& configPath, const std::string& dirPath) {
    if (dirPath.empty()) {
        return dirPath;
    }

    const std::filesystem::path path{dirPath};
    if (path.is_absolute()) {
        return path.lexically_normal().string();
    }

    return (configPath.parent_path() / path).lexically_normal().string();
}

void setError(std::string* error, const std::string& message) {
    if (error) {
        *error = message;
    }
}
}

std::optional<KVNodeConfig> KVNodeConfig::parseFromFile(const std::string& path, std::string* error) {
    namespace fs = std::filesystem;

    // check exist
    const fs::path configPath{path};
    std::error_code ec;
    if (!fs::exists(configPath, ec)) {
        const std::string message = ec
            ? std::format("failed to check config file '{}': {}", path, ec.message())
            : std::format("config file '{}' does not exist", path);
        setError(error, message);
        return std::nullopt;
    }

    if (!fs::is_regular_file(configPath, ec)) {
        const std::string message = ec
            ? std::format("failed to inspect config file '{}': {}", path, ec.message())
            : std::format("config path '{}' is not a regular file", path);
        setError(error, message);
        return std::nullopt;
    }

    YAML::Node fileRoot;
    try {
        fileRoot = YAML::LoadFile(path);
    } catch (const YAML::Exception& e) {
        setError(error, std::format("failed to parse config file '{}': {}", path, e.what()));
        return std::nullopt;
    }

    KVNodeConfig cfg;
    try {
        YAML::Node root = fileRoot["kvserver"];
        if (!root || !root.IsDefined()) {
            setError(error, std::format("config file '{}' missing required root field 'kvserver'", path));
            return std::nullopt;
        }

        const fs::path absoluteConfigPath = fs::absolute(configPath);
        cfg.id = root["id"].as<int>();
        cfg.bindAddr = root["bind_addr"].as<std::string>();
        cfg.peerId = root["peer_id"].as<std::vector<int>>();
        cfg.peerAddr = root["peer_addr"].as<std::vector<std::string>>();
        cfg.persistDir = resolveDirPath(absoluteConfigPath, root["persist_dir"].as<std::string>());
        cfg.logDir = resolveDirPath(absoluteConfigPath, root["log_dir"] ? root["log_dir"].as<std::string>() : "../logs");
        cfg.maxRaftLogCount = root["max_raft_log_count"].as<uint32_t>();
        cfg.maxRaftStateSize = root["max_raft_state_size"].as<uint32_t>();
        cfg.queueMaxWaitTime = std::chrono::milliseconds(root["queue_max_wait_time"].as<int>());
    } catch (const YAML::Exception& e) {
        setError(error, std::format("invalid config file '{}': {}", path, e.what()));
        return std::nullopt;
    }

    if (!checkConfig(cfg, error)) {
        if (error && !error->empty()) {
            *error = std::format("invalid config file '{}': {}", path, *error);
        }
        return std::nullopt;
    }

    return cfg;
}

bool KVNodeConfig::checkConfig(const KVNodeConfig &cfg, std::string *error){
    if (cfg.id <= 0) {
        setError(error, std::format("field 'kvserver.id' must be positive, got {}", cfg.id));
        return false;
    }

    if (cfg.bindAddr.empty()) {
        setError(error, "field 'kvserver.bind_addr' must not be empty");
        return false;
    }

    if (cfg.persistDir.empty()) {
        setError(error, "field 'kvserver.persist_dir' must not be empty");
        return false;
    }

    if (cfg.logDir.empty()) {
        setError(error, "field 'kvserver.log_dir' must not be empty");
        return false;
    }

    if (cfg.peerId.empty()) {
        setError(error, "field 'kvserver.peer_id' must not be empty");
        return false;
    }
    if (cfg.peerAddr.empty()) {
        setError(error, "field 'kvserver.peer_addr' must not be empty");
        return false;
    }
    if (cfg.peerId.size() != cfg.peerAddr.size()) {
        setError(error, std::format("field 'kvserver.peer_id' size {} does not match 'kvserver.peer_addr' size {}",
            cfg.peerId.size(), cfg.peerAddr.size()));
        return false;
    }
    if (std::find(cfg.peerId.begin(), cfg.peerId.end(), cfg.id) == cfg.peerId.end()) {
        setError(error, std::format("field 'kvserver.peer_id' must contain current node id {}", cfg.id));
        return false;
    }
    for (size_t i = 0; i < cfg.peerAddr.size(); ++i) {
        if (cfg.peerAddr[i].empty()) {
            setError(error, std::format("field 'kvserver.peer_addr[{}]' must not be empty", i));
            return false;
        }
    }

    if (cfg.maxRaftLogCount == 0) {
        setError(error, "field 'kvserver.max_raft_log_count' must be greater than 0");
        return false;
    }

    if (cfg.maxRaftStateSize == 0) {
        setError(error, "field 'kvserver.max_raft_state_size' must be greater than 0");
        return false;
    }

    if (cfg.queueMaxWaitTime <= std::chrono::milliseconds::zero()) {
        setError(error, "field 'kvserver.queue_max_wait_time' must be greater than 0");
        return false;
    }

    if (error) {
        error->clear();
    }
    return true;
}
