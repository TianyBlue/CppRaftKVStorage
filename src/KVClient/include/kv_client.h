//
// Created by Desktop on 2026/1/24.
//

#ifndef RAFTKVSTORAGE_KV_CLIENT_H
#define RAFTKVSTORAGE_KV_CLIENT_H

#include <vector>
#include <string>
#include <memory>

#include "kv_server_rpc.grpc.pb.h"

class KVClient {
public:
    enum class OperationResult {
        SUCCESS,
        FAILURE,
        INVALID_KEY,
        KEY_NOT_FOUND,
        TIMEOUT
    };

    KVClient() = default;
    void init(const std::vector<std::string>& serverAddr);

    OperationResult put(const std::string& key, const std::string& value);
    OperationResult get(const std::string& key, std::string& value);
    OperationResult remove(const std::string& key);

private:
    int64_t m_ClientId;
    int64_t m_RequestId;
    int m_LastLeaderIdx{};

    const std::chrono::milliseconds RequestTimeout{1000};
    std::vector<std::unique_ptr<KVServerProto::KVRpc::Stub>> m_Stubs;


    static int64_t getRandomClientId();

};


#endif // RAFTKVSTORAGE_KV_CLIENT_H
