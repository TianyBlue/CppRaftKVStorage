//
// Created by Desktop on 2026/1/22.
//

#include "kv_server_node.h"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <mutex>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>


#include "blocking_queue.h"
#include "kv_command.h"
#include "kv_server_proto_enum.h"
#include "scoped_timer.h"
#include "thread_pool.h"


void KVServerNode::ensureDirectoryReady(const std::string &dirPath, const std::string &dirLabel) {
    namespace fs = std::filesystem;

    const fs::path path{dirPath};

    try {
        fs::create_directories(path);

        if (!fs::is_directory(path)) {
            throw std::runtime_error(std::format("{} '{}' is not a directory", dirLabel, dirPath));
        }

        const fs::path probeFile = path / ".write_test";
        std::ofstream out(probeFile);
        if (!out.is_open()) {
            throw std::runtime_error(std::format("no write permission for {} '{}'", dirLabel, dirPath));
        }
        out.close();

        fs::remove(probeFile);
    } catch (const std::exception &e) {
        throw std::runtime_error(std::format("failed to prepare {} '{}': {}", dirLabel, dirPath, e.what()));
    }
}

void KVServerNode::init(int id, const std::vector<int> &clusterId, const std::vector<std::string> &clusterAddr,
                        std::string bindAddr, std::string persistDir, std::string logDir) {

    m_Id = id;
    m_BindAddr = std::move(bindAddr);
    m_LogDir = std::move(logDir);
    m_Persister = std::make_unique<SnapshotPersister>(std::to_string(id), persistDir);
    m_ApplyQueue = std::make_shared<BlockingQueue<ApplyMsg>>(10);
    std::shared_ptr<spdlog::logger> kvLogger;
    std::shared_ptr<spdlog::logger> raftLogger;
    initLogger(m_Id, kvLogger, raftLogger);
    m_Logger = kvLogger;
    m_ThreadPool = std::make_shared<ThreadPool>(1);
    std::shared_ptr<ThreadPool> raftPool = std::make_shared<ThreadPool>(6);

    m_RaftNode->init(id, clusterId, clusterAddr, m_Persister, m_ApplyQueue, raftLogger, raftPool);

    auto kvSnapshot = m_Persister->readAppSnapshot();
    if (!kvSnapshot.empty()) {
        loadAppSnapshot(std::move(kvSnapshot));
    }

    grpc::EnableDefaultHealthCheckService(true);

    grpc::ServerBuilder serverBuilder;
    serverBuilder.AddListeningPort(m_BindAddr, grpc::InsecureServerCredentials());
    serverBuilder.RegisterService(m_Service.get());
    serverBuilder.RegisterService(m_AdminService.get());
    serverBuilder.RegisterService(m_RaftNode->getService());


    m_ListenServer = serverBuilder.BuildAndStart();
    m_HealthCheckService = m_ListenServer->GetHealthCheckService();
    m_HealthCheckService->SetServingStatus(false);
    m_ListenServerThread = std::thread([this]() { m_ListenServer->Wait(); });
}
void KVServerNode::init(KVNodeConfig cfg) {
    ensureDirectoryReady(cfg.persistDir, "persist_dir");
    ensureDirectoryReady(cfg.logDir, "log_dir");

    m_MaxRaftLogCount = cfg.maxRaftLogCount;
    m_MaxRaftStateSize = cfg.maxRaftStateSize;
    m_QueueWaitTimeout = cfg.queueMaxWaitTime;
    init(cfg.id, cfg.peerId, cfg.peerAddr, cfg.bindAddr, cfg.persistDir, cfg.logDir);
}

void KVServerNode::run() {
    m_RaftNode->run();
    m_ApplyMsgLooperThread = std::thread(&KVServerNode::applyMsgListener, this);

    m_IsServing = true;
    m_HealthCheckService->SetServingStatus(true);
}
void KVServerNode::runAndWait() {
    m_RaftNode->run();
    applyMsgListener();
}

KVServerNode::~KVServerNode() {
    m_IsServing = false;
    m_HealthCheckService->SetServingStatus(false);

    if (m_ListenServerThread.joinable()) {
        m_ListenServerThread.join();
    }

    if (m_ApplyMsgLooperThread.joinable()) {
        m_ApplyMsgLooperThread.join();
    }

    m_Logger->flush();
}

void KVServerNode::stop() {
    m_Stop = true;
    m_IsServing = false;
    m_HealthCheckService->SetServingStatus(false);

    m_ListenServer->Shutdown();

    {
        std::lock_guard lk{m_Mutex};
        for (auto &[_, chan]: m_CommandQueueMap) {
            chan->close();
        }
    }

    m_RaftNode->stop();
    m_ApplyQueue->close();
}


void KVServerNode::applyMsgListener() {
    ApplyMsg msg;
    while (m_ApplyQueue->pop(msg) == PopResult::Success) {
        // std::cout << std::format("server recv applyMsg from raft.\n");
        m_Logger->info("server {} recv applyMsg from raft.", m_Id);

        if (msg.isSnapshot) {
            installSnapshot(std::move(msg));
        } else {
            {
                std::lock_guard lk{m_Mutex};
                if (msg.logIdx <= m_LastSnapshotIdx) {
                    continue;
                }
            }

            applyCommand(msg);
        }
    }

    // std::cout << std::format("err, server applyMsg listener exit.\n");
    m_Logger->warn("server {} applyMsg listener exit.", m_Id);
}
void KVServerNode::applyCommand(const ApplyMsg &applyMsg) {
    if (applyMsg.applyCommand.empty()) {
        return;
    }

    auto cmd = KVCommand::fromString(applyMsg.applyCommand);
    // 根据requestId大小去重
    {
        std::unique_lock lk{m_Mutex};
        auto it = m_ClientLastResult.find(cmd.clientId);
        if (it != m_ClientLastResult.end() && it->second.requestId >= cmd.requestId) {
            if (it->second.requestId == cmd.requestId) {
                cmd.execStatus = it->second.execStatus;
                cmd.val = it->second.value;
            } else {
                cmd.execStatus = KVExecuteState::REQUEST_EXPIRED;
                cmd.val = "";
            }

            lk.unlock();
            notifyWaiter(cmd);
            return;
        }
    }

    // 执行命令
    switch (cmd.type) {
            // PUT
        case CommandType::PUT:
            executePutCommand(cmd);
            break;
            // DELETE
        case CommandType::DELETE:
            executeDeleteCommand(cmd);
            break;
        default:
            throw std::runtime_error{"Command type not support."};
    }

    {
        std::lock_guard lk{m_Mutex};
        m_ClientLastResult[cmd.clientId] = ClientLastResult{
                .requestId = cmd.requestId, .clientId = cmd.clientId, .execStatus = cmd.execStatus, .value = cmd.val};
    }

    notifyWaiter(std::move(cmd));
    {
        // 检查是否要触发快照
        std::lock_guard lk{m_Mutex};
        conditionalMakeSnapshotUnsafe(applyMsg.logIdx);
    }
}

void KVServerNode::executePutCommand(KVCommand &command) {
    std::lock_guard lk{m_Mutex};
    m_KVStore.put(command.key, command.val);
    command.execStatus = KVExecuteState::SUCCESS;
    // std::cout << std::format("server put key on db, key: {}, val: {}\n", command.key, command.val);
    m_Logger->info("server {} put key on db, key: {}, val: {}", m_Id, command.key, command.val);
}
void KVServerNode::executeDeleteCommand(KVCommand &command) {
    std::lock_guard lk{m_Mutex};
    bool status = m_KVStore.erase(command.key);
    command.execStatus = status ? KVExecuteState::SUCCESS : KVExecuteState::KEY_NOT_FOUND;
    // std::cout << std::format("server erase key on db: {}\n", command.key);
    m_Logger->info("server {} erase key on db: {}", m_Id, command.key);
}
void KVServerNode::installSnapshot(ApplyMsg snapshotMsg) {
    if (snapshotMsg.snapshot.empty() || snapshotMsg.lastSnapshotedIdx < m_LastSnapshotIdx)
        return;

    ++m_Metrics.snapshotInstallTotal;
    // 安装快照时，清空现有的请求队列(收到快照说明非Leader)
    {
        std::lock_guard lk{m_Mutex};
        m_LastSnapshotIdx = snapshotMsg.lastSnapshotedIdx;

        for (auto &[_, queue]: m_CommandQueueMap) {
            queue->close();
        }

        m_CommandQueueMap.clear();
    }
    // m_LastSnapshotTerm = snapshotMsg.lastSnapShotedTerm;
    // m_KVStore.loadSnapshot(snapshotMsg.snapshot);

    loadAppSnapshot(std::move(snapshotMsg.snapshot));
}

void KVServerNode::loadAppSnapshot(std::string appSnapshot) {
    std::lock_guard lk{m_Mutex};
    std::stringstream ss{appSnapshot};
    boost::archive::text_iarchive ia{ss};

    AppSnapshotData data;
    ia >> data;

    m_KVStore.loadData(data.kvData);
    m_ClientLastResult = data.clientLastResult;
}

void KVServerNode::conditionalMakeSnapshotUnsafe(int currCmdIdx) {
    auto stateSize = m_RaftNode->getCurrentStateSize();
    auto logCount = m_RaftNode->getCurrentLogCount();

    if (logCount > m_MaxRaftLogCount || stateSize > m_MaxRaftStateSize) {
        // m_RaftNode->doSnapshot(currCmdIdx, m_KVStore.makeSnapshot());

        ++m_Metrics.snapshotCreateTotal;
        m_RaftNode->doSnapshot(currCmdIdx, makeSnapshotUnsafe());
        m_LastSnapshotIdx = currCmdIdx;
    }
}

std::string KVServerNode::makeSnapshotUnsafe() {
    AppSnapshotData snapshot;
    snapshot.kvData = m_KVStore.dumpData();
    snapshot.clientLastResult = m_ClientLastResult;

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << snapshot;
    return ss.str();
}

void KVServerNode::initLogger(int mId, std::shared_ptr<spdlog::logger> &kvLogger,
                              std::shared_ptr<spdlog::logger> &raftLogger) {
    const auto *pattern = "[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] Thread: [%t] %v";
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_level(spdlog::level::info);
    console_sink->set_pattern(pattern);

    // 文件 Sink (每个文件10MB, 最多保留5个)
    std::string fileName = (std::filesystem::path(m_LogDir) / std::format("node_{}.log", mId)).string();
    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(fileName, 1024 * 1024 * 10, 5);
    file_sink->set_level(spdlog::level::debug);
    file_sink->set_pattern(pattern);

    std::vector<spdlog::sink_ptr> sinks{console_sink, file_sink};

    std::shared_ptr<spdlog::logger> logger = std::make_shared<spdlog::logger>("kvserver", sinks.begin(), sinks.end());
    auto raft_logger = std::make_shared<spdlog::logger>("RAFT", sinks.begin(), sinks.end());
    raft_logger->set_level(spdlog::level::debug);
    auto kv_logger = std::make_shared<spdlog::logger>("KV", sinks.begin(), sinks.end());
    kv_logger->set_level(spdlog::level::info);

    kvLogger = kv_logger;
    raftLogger = raft_logger;
}


void KVServerNode::notifyWaiter(KVCommand command) {
    auto clientRequestId = ClientRequestId{.clientId = command.clientId, .requestId = command.requestId};

    m_Logger->info("[applyCommand] server {} client {} requestId {} try push queue.", m_Id, command.clientId,
                   command.requestId);

    std::shared_ptr<BlockingQueue<KVCommand>> queue;
    {
        std::lock_guard lk{m_Mutex};
        if (!m_CommandQueueMap.contains(clientRequestId)) {
            return;
        }
        queue = m_CommandQueueMap.at(clientRequestId);
    }

    queue->push(std::move(command));
    m_Logger->info("[applyCommand] server {} client {} requestId {} pushed to queue.", m_Id, command.clientId,
                   command.requestId);
}

void KVServerNode::eraseWaitQueueItem(const ClientRequestId &id) {
    std::lock_guard lk{m_Mutex};
    m_CommandQueueMap.erase(id);
}

void KVServerNode::resultStatistics(KVExecuteState::KVExecuteState state) {
    switch (state) {
        case KVExecuteState::SUCCESS:
            ++m_Metrics.successTotal;
            break;
        case KVExecuteState::NOT_LEADER:
            ++m_Metrics.notLeaderTotal;
            break;
        case KVExecuteState::KEY_NOT_FOUND:
            ++m_Metrics.keyNotFoundTotal;
            break;
        case KVExecuteState::TIME_OUT:
            ++m_Metrics.timeoutTotal;
            break;
        case KVExecuteState::REQUEST_EXPIRED:
            ++m_Metrics.requestExpiredTotal;
            break;
        case KVExecuteState::REQUEST_DUPLICATE:
            ++m_Metrics.requestDuplicateTotal;
            break;
        default:
            break;
    }
}

KVServerNodeMetrics KVServerNode::getMetrics() const {
    return KVServerNodeMetrics{.getTotal = m_Metrics.getTotal.load(),
                               .putTotal = m_Metrics.putTotal.load(),
                               .deleteTotal = m_Metrics.deleteTotal.load(),
                               .successTotal = m_Metrics.successTotal.load(),
                               .timeoutTotal = m_Metrics.timeoutTotal.load(),
                               .notLeaderTotal = m_Metrics.notLeaderTotal.load(),
                               .keyNotFoundTotal = m_Metrics.keyNotFoundTotal.load(),
                               .requestExpiredTotal = m_Metrics.requestExpiredTotal.load(),
                               .snapshotInstallTotal = m_Metrics.snapshotInstallTotal.load(),
                               .snapshotCreateTotal = m_Metrics.snapshotCreateTotal.load(),
                               .slowRequestTotal = m_Metrics.slowRequestTotal.load()};
}

RaftNode::RaftStatus KVServerNode::getRaftStatus() const { return m_RaftNode->getRaftStatus(); }

grpc::Status KVServerNode::recvGet(const KVServerProto::GetRequest *request, KVServerProto::GetReply *response) {
    return recvGetReadIndex(request, response);
}

grpc::Status KVServerNode::recvPut(const KVServerProto::PutRequest *request, KVServerProto::PutReply *response) {
    if (m_Stop) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "server shuting down.");
    }

    ++m_Metrics.putTotal;

    ScopedTimer scoped{[this](std::chrono::milliseconds duration){
        if(duration > m_SlowRequestLimit){
            ++m_Metrics.slowRequestTotal;
            m_Logger->warn("[slow_request] op=put latency_ms={}", duration.count());
        }
    }};

    // std::cout << std::format("server recv put request from client {}, key: {}, val: {}\n", request->client_id(),
    // request->key(), request->value());
    m_Logger->info("[recvPut] server recv put request from client {}, key: {}, val: {}", request->client_id(),
                   request->key(), request->value());

    KVCommand command;
    command.clientId = request->client_id();
    command.requestId = request->request_id();
    command.key = request->key();
    command.val = request->value();
    command.type = CommandType::PUT;

    response->set_success(false);
    auto clientRequestId = ClientRequestId{.clientId = command.clientId, .requestId = command.requestId};
    auto waitQueue = std::make_shared<BlockingQueue<KVCommand>>(1);
    {
        std::lock_guard lk{m_Mutex};
        // 去重
        if (m_CommandQueueMap.contains(clientRequestId)) {
            resultStatistics(KVExecuteState::REQUEST_DUPLICATE);
            m_Logger->warn("[recvGet] server {} client {} request {} already exists", m_Id, command.clientId,
                           command.requestId);
            response->set_success(false);
            response->set_status(KVExecuteState::REQUEST_DUPLICATE);
            return grpc::Status::OK;
        }

        m_CommandQueueMap.emplace(clientRequestId, waitQueue);
    }


    bool isLeader{false};
    int logIdx, logTerm;
    m_RaftNode->addLog(command.toString(), isLeader, logIdx, logTerm);

    if (!isLeader) {
        resultStatistics(KVExecuteState::NOT_LEADER);

        // std::cout << std::format("server {} is not leader.\n", m_Id);
        eraseWaitQueueItem(clientRequestId);
        m_Logger->info("server {} is not leader.", m_Id);
        response->set_status(KVExecuteState::NOT_LEADER);
        return grpc::Status(grpc::Status::OK);
    }

    KVCommand applyCommand;
    // auto popResult = waitQueue->popWithTimeout(applyCommand, QUEUE_WAIT_TIMEOUT);
    auto popResult = waitQueue->popWithTimeout(applyCommand, m_QueueWaitTimeout);
    if (popResult == PopResult::Timeout || popResult == PopResult::Closed) {
        resultStatistics(KVExecuteState::TIME_OUT);

        // std::cout << std::format("server {} recv put request timeout.\n", m_Id);
        m_Logger->warn("server {} recv put request timeout.", m_Id);
        response->set_status(KVExecuteState::TIME_OUT);

        eraseWaitQueueItem(clientRequestId);
        return grpc::Status::OK;
    }


    // assert(applyCommand.execStatus == KVExecuteState::SUCCESS);
    // std::cout << std::format("server {} put key: {}, val: {}\n", m_Id, request->key(), request->value());
    m_Logger->info("server {} put, state: {}, key: {}, val: {}", m_Id, int(applyCommand.execStatus), request->key(),
                   request->value());

    response->set_success(applyCommand.execStatus == KVExecuteState::SUCCESS);
    response->set_status(applyCommand.execStatus);
    eraseWaitQueueItem(clientRequestId);
    resultStatistics(applyCommand.execStatus);
    return grpc::Status::OK;
}
grpc::Status KVServerNode::recvDelete(const KVServerProto::DeleteRequest *request,
                                      KVServerProto::DeleteReply *response) {
    if (m_Stop) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "server shuting down.");
    }
    ++m_Metrics.deleteTotal;

    ScopedTimer scoped{[this](std::chrono::milliseconds duration){
        if(duration > m_SlowRequestLimit){
            ++m_Metrics.slowRequestTotal;
            m_Logger->warn("[slow_request] op=delete latency_ms={}", duration.count());
        }
    }};

    // std::cout << std::format("server recv delete request from client {}, key: {}\n", request->client_id(),
    // request->key());
    m_Logger->info("server recv delete request from client {}, key: {}", request->client_id(), request->key());
    KVCommand command;
    command.type = CommandType::DELETE;
    command.key = request->key();
    command.clientId = request->client_id();
    command.requestId = request->request_id();

    response->set_success(false);
    auto clientRequestId = ClientRequestId{.clientId = command.clientId, .requestId = command.requestId};
    std::shared_ptr<BlockingQueue<KVCommand>> waitQueue = std::make_shared<BlockingQueue<KVCommand>>(1);
    {
        std::lock_guard lk{m_Mutex};
        if (m_CommandQueueMap.contains(clientRequestId)) {
            resultStatistics(KVExecuteState::REQUEST_DUPLICATE);
            m_Logger->warn("[recvGet] server {} client {} request {} already exists", m_Id, command.clientId,
                           command.requestId);
            response->set_success(false);
            response->set_status(KVExecuteState::REQUEST_DUPLICATE);
            return grpc::Status::OK;
        }
        m_CommandQueueMap.emplace(clientRequestId, waitQueue);
    }

    bool isLeader;
    int logIdx, logTerm;
    m_RaftNode->addLog(command.toString(), isLeader, logIdx, logTerm);

    if (!isLeader) {
        resultStatistics(KVExecuteState::NOT_LEADER);
        eraseWaitQueueItem(clientRequestId);
        response->set_status(KVExecuteState::NOT_LEADER);
        return grpc::Status::OK;
    }

    KVCommand applyCommand;
    auto popResult = waitQueue->popWithTimeout(applyCommand, m_QueueWaitTimeout);
    if (popResult == PopResult::Timeout || popResult == PopResult::Closed) {
        resultStatistics(KVExecuteState::TIME_OUT);

        response->set_status(KVExecuteState::TIME_OUT);
        eraseWaitQueueItem(clientRequestId);
        return grpc::Status::OK;
    }

    response->set_success(applyCommand.execStatus == KVExecuteState::SUCCESS);
    response->set_status(applyCommand.execStatus);
    eraseWaitQueueItem(clientRequestId);
    resultStatistics(applyCommand.execStatus);
    return grpc::Status::OK;
}
grpc::Status KVServerNode::recvGetReadIndex(const KVServerProto::GetRequest *request,
                                            KVServerProto::GetReply *response) {

    if (m_Stop) {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "server shuting down.");
    }

    ++m_Metrics.getTotal;

    ScopedTimer scoped{[this](std::chrono::milliseconds duration){
        if(duration > m_SlowRequestLimit){
            ++m_Metrics.slowRequestTotal;
            m_Logger->warn("[slow_request] op=get_readindex latency_ms={}", duration.count());
        }
    }};

    // std::cout << std::format("server recv get request from client {}, key: {}\n", request->client_id(),
    // request->key());
    m_Logger->info("server {} recv get request from client {}, key: {}", m_Id, request->client_id(), request->key());

    response->set_success(false);
    response->set_status(KVExecuteState::NOT_LEADER);
    std::uint64_t readIndex;
    if (m_RaftNode->readIndex(readIndex)) {
        bool waitStatus = m_RaftNode->waitApplied(readIndex, m_QueueWaitTimeout);
        response->set_status(KVExecuteState::TIME_OUT);
        if (waitStatus) {
            // 再次检查Raft状态
            if (!m_RaftNode->isLeader()) {
                response->set_status(KVExecuteState::NOT_LEADER);
                resultStatistics(KVExecuteState::NOT_LEADER);
                return grpc::Status::OK;
            }

            std::optional<std::string> result;
            {
                std::lock_guard lk{m_Mutex};
                result = m_KVStore.get(request->key());
            }

            if (result.has_value()) {
                response->set_success(true);
                response->set_status(KVExecuteState::SUCCESS);
                response->set_value(result.value());
            } else {
                response->set_status(KVExecuteState::KEY_NOT_FOUND);
            }
        }
    } else {
        m_Logger->warn("[KV Get]server {} read index failed.", m_Id);
    }

    m_Logger->info("[KV Get] server {} response success {}, status {}, value {}", m_Id, response->success(),
                   response->status(), response->value());
    resultStatistics(static_cast<KVExecuteState::KVExecuteState>(response->status()));
    return grpc::Status::OK;
}
