//
// Created by Desktop on 2026/1/20.
//

#include "include/snapshot_persister.h"

#include <filesystem>
#include <format>

bool SnapshotPersister::saveRaftSnapshot(std::string snapshot) const {
    std::lock_guard lk{m_Mutex};
    auto stream = std::fstream(m_RaftSnapshotFileName, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    if (!stream.is_open()) {
        return false;
    }

    stream << snapshot;
    stream.close();
    return true;
}

bool SnapshotPersister::saveAppSnapshot(std::string snapshot) const {
    std::lock_guard lk{m_Mutex};
    auto stream = std::fstream(m_AppSnapshotFileName, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    if (!stream.is_open()) {
        return false;
    }

    stream << snapshot;
    stream.close();
    return true;
}
bool SnapshotPersister::save(std::string raftSnapshot, std::string appSnapshot) const {
    std::lock_guard lk{m_Mutex};

    auto stream = std::fstream(m_RaftSnapshotFileName, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    if (!stream.is_open()) {
        return false;
    }

    stream << raftSnapshot;
    stream.close();

    stream = std::fstream(m_AppSnapshotFileName, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    if (!stream.is_open()) {
        return false;
    }

    stream << appSnapshot;
    stream.close();

    return true;
}

std::string SnapshotPersister::readRaftSnapshot() const {
    std::lock_guard lk{m_Mutex};

    auto stream = std::fstream(m_RaftSnapshotFileName, std::ios::in | std::ios::binary);
    auto content = readFromStream(stream);
    stream.close();

    return content;
}

std::string SnapshotPersister::readAppSnapshot() const {
    std::lock_guard lk{m_Mutex};
    auto stream = std::fstream(m_AppSnapshotFileName, std::ios::in | std::ios::binary);
    auto content = readFromStream(stream);
    stream.close();

    return content;
}

SnapshotPersister::SnapshotPersister(std::string_view myLabel, std::string_view configDir) {
    using std::filesystem::path;

    m_RaftSnapshotFileName = std::format("{}/raftSnapshotFile-{}.archive", path(configDir).string(), myLabel);
    m_AppSnapshotFileName = std::format("{}/appSnapshotFile-{}.archive",  path(configDir).string(), myLabel);

    auto raftStream = std::fstream(m_AppSnapshotFileName, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    auto appStream = std::fstream(m_RaftSnapshotFileName, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);

    if (!raftStream.is_open() || !appStream.is_open()) {
        throw std::runtime_error("snapshot file open failed");
    }

    raftStream.close();
    appStream.close();
}

std::string SnapshotPersister::readFromStream(std::fstream &stream) {
    if (!stream.is_open() || !stream.good()) {
        return "";
    }

    // 获取文件大小
    stream.seekg(0, std::ios::end);
    size_t fileSize = stream.tellg();
    stream.seekg(0, std::ios::beg);

    // 分配缓冲区并读取内容
    std::string content;
    content.resize(fileSize);
    stream.read(&content[0], fileSize);

    return content;
}
