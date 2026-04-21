//
// Created by Desktop on 2026/1/20.
//

#ifndef RAFTKVSTORAGE_SNAPSHOT_PERSISTER_H
#define RAFTKVSTORAGE_SNAPSHOT_PERSISTER_H

#include <string>
#include <string_view>
#include <fstream>
#include <mutex>

class SnapshotPersister {
public:
    bool saveRaftSnapshot(std::string snapshot) const;
    bool saveAppSnapshot(std::string snapshot) const;
    bool save(std::string raftSnapshot, std::string appSnapshot) const;

    std::string readRaftSnapshot() const;
    std::string readAppSnapshot() const;

    SnapshotPersister(std::string_view myLabel, std::string_view configDir = "./");


private:
    mutable std::mutex m_Mutex;
    std::string m_RaftSnapshotFileName;
    std::string m_AppSnapshotFileName;


    static std::string readFromStream(std::fstream &stream);
};


#endif //RAFTKVSTORAGE_SNAPSHOT_PERSISTER_H