//
// Created by Desktop on 2026/1/22.
//

#ifndef RAFTKVSTORAGE_KV_SERVER_PROTO_ENUM_H
#define RAFTKVSTORAGE_KV_SERVER_PROTO_ENUM_H


// namespace KVGetState {
//     enum KVGetState {
//         SUCCESS = 0,
//         NOT_LEADER = 1,
//         TIME_OUT = 2,
//         NOT_FOUND = 3
//     };
// }
//
//
// namespace KVPutState {
//     enum KVPutState {
//         SUCCESS = 0,
//         NOT_LEADER = 1,
//         TIME_OUT = 2
//     };
// }
//
// namespace KVDeleteState {
//     enum KVDeleteState {
//         SUCCESS = 0,
//         NOT_LEADER = 1,
//         TIME_OUT = 2,
//         NOT_FOUND = 3
//     };
// }


namespace KVExecuteState {
    enum KVExecuteState {
        SUCCESS = 0,
        NOT_LEADER = 1,
        TIME_OUT = 2,
        KEY_NOT_FOUND = 3,
        REQUEST_EXPIRED = 4,
        REQUEST_DUPLICATE = 5
    };
}

#endif //RAFTKVSTORAGE_KV_SERVER_PROTO_ENUM_H