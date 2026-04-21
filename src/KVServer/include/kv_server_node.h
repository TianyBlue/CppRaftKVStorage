//
// Created by Desktop on 2026/1/22.
//

#ifndef RAFTKVSTORAGE_KV_SERVER_NODE_H
#define RAFTKVSTORAGE_KV_SERVER_NODE_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <grpcpp/health_check_service_interface.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include "admin_metrics_service.h"
#include "blocking_queue.h"
#include "config.h"
#include "kv_command.h"
#include "kv_server_proto_enum.h"
#include "kv_server_rpc.pb.h"
#include "kv_service.h"
#include "kv_skip_list.h"
#include "raft_node.h"


namespace grpc {
    class Server;
}


class ThreadPool;

struct ClientLastResult {
    std::int64_t requestId;
    std::int64_t clientId;
    KVExecuteState::KVExecuteState execStatus;
    std::string value;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & requestId;
        ar & clientId;
        ar & execStatus;
        ar & value;
    }
};

struct ClientRequestId {
    std::int64_t clientId;
    std::int64_t requestId;

    bool operator==(const ClientRequestId &other) const = default;

    struct IdHash {
        std::size_t operator()(const ClientRequestId &key) const {
            const std::size_t h1 = std::hash<int64_t>{}(key.clientId);
            const std::size_t h2 = std::hash<int64_t>{}(key.requestId);
            return h1 ^ (h2 << 1);
        }
    };
};

struct AppSnapshotData {
    std::vector<std::pair<std::string, std::string>> kvData;
    std::unordered_map<std::int64_t, ClientLastResult> clientLastResult;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & kvData;
        ar & clientLastResult;
    }
};

struct KVServerNodeMetrics {
    std::uint64_t getTotal{};
    std::uint64_t putTotal{};
    std::uint64_t deleteTotal{};

    std::uint64_t successTotal{};
    std::uint64_t timeoutTotal{};
    std::uint64_t notLeaderTotal{};
    std::uint64_t keyNotFoundTotal{};
    std::uint64_t requestExpiredTotal{};
    std::uint64_t requestDuplicateTotal{};

    std::uint64_t snapshotInstallTotal{};
    std::uint64_t snapshotCreateTotal{};

    std::uint64_t slowRequestTotal{};
};


class KVServerNode {
public:
    // 初始化节点，开启监听，添加连接
    void init(int id, const std::vector<int> &clusterId, const std::vector<std::string> &clusterAddr,
              std::string bindAddr, std::string persistDir = "./", std::string logDir = "logs");
    void init(KVNodeConfig cfg);

    // 开始运行
    void run();
    void runAndWait();

    void stop();

    bool isServing() const { return m_IsServing.load(std::memory_order::relaxed); }

    ~KVServerNode();

private:
    std::unique_ptr<RaftNode> m_RaftNode = std::make_unique<RaftNode>();
    std::unique_ptr<KVService> m_Service = std::make_unique<KVService>(this);
    std::unique_ptr<AdminMetricsService> m_AdminService = std::make_unique<AdminMetricsService>(this);
    std::shared_ptr<SnapshotPersister> m_Persister;
    std::shared_ptr<BlockingQueue<ApplyMsg>> m_ApplyQueue;

    std::unique_ptr<grpc::Server> m_ListenServer;
    std::thread m_ListenServerThread;
    std::thread m_ApplyMsgLooperThread;
    // 上次快照包含的Log的最大索引(保存或安装快照时更新)
    int m_LastSnapshotIdx{};
    // int m_LastSnapshotTerm;

    mutable std::mutex m_Mutex;
    // 实际存储
    KVSkipList m_KVStore;

    int m_Id{};
    std::string m_BindAddr;
    std::string m_LogDir;
    // KVCommand的等待队列
    std::unordered_map<ClientRequestId, std::shared_ptr<BlockingQueue<KVCommand>>, ClientRequestId::IdHash>
            m_CommandQueueMap;

    std::shared_ptr<spdlog::logger> m_Logger{};
    std::shared_ptr<ThreadPool> m_ThreadPool{};

    // 客户端上一次执行的结果，用于Apply时去重
    std::unordered_map<std::int64_t, ClientLastResult> m_ClientLastResult;

    // 触发快照的条件
    uint32_t m_MaxRaftLogCount = 10000;
    // 16mb
    uint32_t m_MaxRaftStateSize = 1024 * 1024 * 16;

    std::chrono::milliseconds m_SlowRequestLimit{200};

    std::atomic<bool> m_Stop{false};

    grpc::HealthCheckServiceInterface *m_HealthCheckService{nullptr};
    static constexpr std::string HEALTH_SERVICE_NAME = "";

    std::chrono::milliseconds m_QueueWaitTimeout{500};

    std::atomic<bool> m_IsServing{};

    struct Metrics {
        std::atomic<std::uint64_t> getTotal{};
        std::atomic<std::uint64_t> putTotal{};
        std::atomic<std::uint64_t> deleteTotal{};

        std::atomic<std::uint64_t> successTotal{};
        std::atomic<std::uint64_t> timeoutTotal{};
        std::atomic<std::uint64_t> notLeaderTotal{};
        std::atomic<std::uint64_t> keyNotFoundTotal{};
        std::atomic<std::uint64_t> requestExpiredTotal{};
        std::atomic<std::uint64_t> requestDuplicateTotal{};
        std::atomic<std::uint64_t> snapshotInstallTotal{};
        std::atomic<std::uint64_t> snapshotCreateTotal{};

        std::atomic<std::uint64_t> slowRequestTotal{};
    };

    Metrics m_Metrics{};

    // void setHealthServing();
    // void setHealthNotServing();

    // ApplyMsg监听
    void applyMsgListener();

    void applyCommand(const ApplyMsg &applyMsg);
    void executePutCommand(KVCommand &command);
    void executeDeleteCommand(KVCommand &command);

    void installSnapshot(ApplyMsg snapshotMsg);
    void loadAppSnapshot(std::string appSnapshot);

    // 判断是否要触发快照.
    void conditionalMakeSnapshotUnsafe(int currCmdIdx);

    std::string makeSnapshotUnsafe();

    static void ensureDirectoryReady(const std::string &dirPath, const std::string &dirLabel);
    void initLogger(int mId, std::shared_ptr<spdlog::logger> &kvLogger, std::shared_ptr<spdlog::logger> &raftLogger);

    // 提醒对应的wait_queue已经执行完成
    void notifyWaiter(KVCommand command);

    void eraseWaitQueueItem(const ClientRequestId &id);

    void resultStatistics(KVExecuteState::KVExecuteState state);
    KVServerNodeMetrics getMetrics() const;
    RaftNode::RaftStatus getRaftStatus() const;

    // rpc
    grpc::Status recvGet(const KVServerProto::GetRequest *request, KVServerProto::GetReply *response);
    grpc::Status recvPut(const KVServerProto::PutRequest *request, KVServerProto::PutReply *response);
    grpc::Status recvDelete(const KVServerProto::DeleteRequest *request, KVServerProto::DeleteReply *response);

    grpc::Status recvGetReadIndex(const KVServerProto::GetRequest *request, KVServerProto::GetReply *response);


    friend class KVService;
    friend class AdminMetricsService;
};


#endif // RAFTKVSTORAGE_KV_SERVER_NODE_H
