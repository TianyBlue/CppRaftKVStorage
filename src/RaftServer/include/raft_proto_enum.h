//
// Created by Desktop on 2026/1/16.
//

#ifndef RAFTKVSTORAGE_PROTO_ENUM_H
#define RAFTKVSTORAGE_PROTO_ENUM_H

namespace VoteState {
    // VOTE_STATE
    enum VoteState {
        KILLED = 0,
        // 已经投过票
        VOTED = 1,
        // Term过期
        EXPIRE = 2,
        // lastLog更旧
        OLDER = 3,
        NORMAL = 4
    };
}


namespace AppendState{
    enum AppendState {
        NOT_SPECIFIED = 0,
        SUCCESS = 1,
        TERM_TOO_OLD = 2,
        INDEX_TOO_LARGE = 3,
        LOG_NOT_MATCH = 4
    };
}





#endif //RAFTKVSTORAGE_PROTO_ENUM_H