//
// Created by Desktop on 2026/1/24.
//

#include "include/kv_client.h"

#include <grpcpp/create_channel.h>
#include <random>

#include "kv_server_proto_enum.h"

using Result = KVClient::OperationResult;
void KVClient::init(const std::vector<std::string> &serverAddr) {
    m_ClientId = getRandomClientId();
    m_RequestId = 0;

    for (const auto& addr : serverAddr) {
        auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        auto stub = KVServerProto::KVRpc::NewStub(channel);
        m_Stubs.push_back(std::move(stub));
    }
}

Result KVClient::put(const std::string &key, const std::string &value) {
    if (m_Stubs.empty())
        throw std::runtime_error("No server inited");

    if (key.empty())
        return Result::INVALID_KEY;

    const int requestId = ++m_RequestId;

    int tryLeaderIdx = m_LastLeaderIdx;
    int serverTryCount{};
    while (++serverTryCount <= m_Stubs.size()) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + RequestTimeout);

        KVServerProto::PutReply reply;
        KVServerProto::PutRequest request;
        request.set_client_id(m_ClientId);
        request.set_request_id(requestId);
        request.set_key(key);
        request.set_value(value);

        const auto& server = m_Stubs[tryLeaderIdx];
        if (!server->Put(&context, request, &reply).ok()) {
            std::cout << "put network failed, try next" << std::endl;
            tryLeaderIdx = (tryLeaderIdx + 1) % m_Stubs.size();
            continue;
        }

        if (reply.status() == KVExecuteState::NOT_LEADER || reply.status() == KVExecuteState::TIME_OUT) {
            tryLeaderIdx = (tryLeaderIdx + 1) % m_Stubs.size();
            // std::cout << "put not leader or timeout, try next" << std::endl;
            continue;
        }

        m_LastLeaderIdx = tryLeaderIdx;
        return Result::SUCCESS;
    }

    std::cout << "put failed, have try all server" << std::endl;
    return Result::FAILURE;
}

Result KVClient::get(const std::string &key, std::string &value) {
    if (m_Stubs.empty())
        throw std::runtime_error("no server inited");

    if (key.empty())
        return Result::INVALID_KEY;

    const int requestId = ++m_RequestId;

    int tryLeaderIdx = m_LastLeaderIdx;
    int serverTryCount{};
    while (serverTryCount++ < m_Stubs.size()) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + RequestTimeout);

        KVServerProto::GetReply reply;
        KVServerProto::GetRequest request;
        request.set_client_id(m_ClientId);
        request.set_request_id(requestId);
        request.set_key(key);

        const auto& server = m_Stubs[tryLeaderIdx];
        if (!server->Get(&context, request, &reply).ok()) {
            // std::cout << "get network failed, try next" << std::endl;
            tryLeaderIdx = (tryLeaderIdx + 1) % m_Stubs.size();
            continue;
        }

        if (reply.status() == KVExecuteState::NOT_LEADER || reply.status() == KVExecuteState::TIME_OUT) {
            tryLeaderIdx = (tryLeaderIdx + 1) % m_Stubs.size();
            // std::cout << "get not leader or timeout, try next" << std::endl;
            continue;
        }
        else if (reply.status() == KVExecuteState::KEY_NOT_FOUND) {
            return Result::KEY_NOT_FOUND;
        }

        value = reply.value();
        m_LastLeaderIdx = tryLeaderIdx;
        return Result::SUCCESS;
    }

    std::cout << "get failed, have try all server" << std::endl;
    return Result::FAILURE;
}

Result KVClient::remove(const std::string &key) {
    if (m_Stubs.empty())
        throw std::runtime_error("No server inited");

    if (key.empty())
        return Result::INVALID_KEY;

    const int requestId = ++m_RequestId;

    int tryLeaderIdx = m_LastLeaderIdx;
    int serverTryCount{};
    while (++serverTryCount < m_Stubs.size()) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + RequestTimeout);

        KVServerProto::DeleteReply reply;
        KVServerProto::DeleteRequest request;
        request.set_client_id(m_ClientId);
        request.set_request_id(requestId);
        request.set_key(key);

        const auto& server = m_Stubs[tryLeaderIdx];
        if (!server->Delete(&context, request, &reply).ok()) {
            // std::cout << "remove network failed, try next" << std::endl;
            tryLeaderIdx = (tryLeaderIdx + 1) % m_Stubs.size();
            continue;
        }

        if (reply.status() == KVExecuteState::NOT_LEADER || reply.status() == KVExecuteState::TIME_OUT) {
            tryLeaderIdx = (tryLeaderIdx + 1) % m_Stubs.size();
            // std::cout << "remove not leader or timeout, try next" << std::endl;
            continue;
        }
        else if (reply.status() == KVExecuteState::KEY_NOT_FOUND) {
            return Result::KEY_NOT_FOUND;
        }

        m_LastLeaderIdx = tryLeaderIdx;
        return Result::SUCCESS;
    }

    std::cout << "remove failed, have try all server" << std::endl;
    return Result::FAILURE;
}
int64_t KVClient::getRandomClientId() {
    static std::random_device rd{};
    static std::mt19937 gen{rd()};
    static std::uniform_int_distribution<int64_t> dis{0, std::numeric_limits<int64_t>::max()};

    return dis(gen);
}
