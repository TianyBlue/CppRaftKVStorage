//
// Created by Desktop on 2026/1/20.
//

#ifndef RAFTKVSTORAGE_APPSNAPSHOTMSG_H
#define RAFTKVSTORAGE_APPSNAPSHOTMSG_H

#include <string>

struct AppSnapshotMsg {
    int SnapshotIdx;
    int SnapshotTerm;
    std::string SnapshotData;
};


#endif //RAFTKVSTORAGE_APPSNAPSHOTMSG_H