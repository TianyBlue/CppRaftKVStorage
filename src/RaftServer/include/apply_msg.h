//
// Created by Desktop on 2026/1/22.
//

#ifndef RAFTKVSTORAGE_APPLY_MSG_H
#define RAFTKVSTORAGE_APPLY_MSG_H

#include <string>
#include <vector>


struct ApplyMsg {
    // Snapshot
    bool isSnapshot{};
    std::string snapshot;
    int lastSnapshotedIdx;
    int lastSnapShotedTerm;


    // 承载此Command的日志索引和任期
    int logIdx;
    int logTerm;


    // Logs
    // std::vector<std::string> applyLogs;
    std::string applyCommand;
};




#endif //RAFTKVSTORAGE_APPLY_MSG_H