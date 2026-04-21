//
// Created by Desktop on 2026/1/13.
//

#ifndef RAFTKVSTORAGE_RAFT_NODE_H
#define RAFTKVSTORAGE_RAFT_NODE_H

#include <atomic>
#include <boost/serialization/serialization.hpp>
#include <chrono>
#include <grpcpp/impl/status.h>
#include <grpcpp/server.h>
#include <mutex>
#include <thread>
#include <vector>


#include "app_snapshot_msg.h"
#include "apply_msg.h"
#include "blocking_queue.h"
#include "raft_rpc.pb.h"
#include "raft_rpc_client.h"
#include "raft_rpc_service.h"
#include "snapshot_persister.h"


namespace spdlog {
    class logger;
}

class ThreadPool;

constexpr int FACTOR = 1;

struct RaftConfig {
    // 选举超时时间
    int MinElectionTimeout = 300 * FACTOR;
    int MaxElectionTimeout = 500 * FACTOR;
    // 心跳超时时间
    int HeartBeatTimeout = 50 * FACTOR;
    // 应用间隔时间
    int ApplyInterval = 20 * FACTOR;
};


class RaftNode {
public:
    RaftNode();
    ~RaftNode();

    enum class State { Leader, Follower, Candidate };

    struct RaftStatus {
        int nodeId;
        RaftNode::State state;
        int term;
        int committedIdx;
        int appliedIdx;
        int lastSnapshotedIdx;
        size_t logCount;
        int stateSize;
        bool isLeader;
    };

    // @depreciated
    // void init(int id, const std::vector<int>& clusterId, const std::vector<std::string> &clusterAddr, std::string
    // bindAddr, std::
    //           shared_ptr<SnapshotPersister> persister, std::shared_ptr<BlockingQueue<ApplyMsg>> applyQueue);

    void init(int id, const std::vector<int> &clusterId, const std::vector<std::string> &clusterAddr,
              std::shared_ptr<SnapshotPersister> persister, std::shared_ptr<BlockingQueue<ApplyMsg>> applyQueue,
              std::shared_ptr<spdlog::logger> logger, std::shared_ptr<ThreadPool> pool);
    void run();

    void stop();

    void addLog(std::string logCommand, bool &outIsLeader, int &outLogIdx, int &outLogTerm);
    bool readIndex(std::uint64_t &readIndex);

    // 等待m_ApplyIndex推进到applyIndex
    bool waitApplied(int applyIndex, std::chrono::milliseconds timeout);

    RaftProtoRPC::RaftRPC::Service *getService() { return m_RPCService.get(); }

    bool isLeader();

    RaftStatus getRaftStatus() const;
    size_t getCurrentLogCount() const;
    int getCurrentStateSize() const;

    void doSnapshot(int lastSnapshotIdx, std::string appSnapshot);

    void debugSendRPC(int peerId);

private:
    struct RaftPersistStates {
        // 这两个状态不需要持久化，因为CommitIdx会随Log提交而更新，ApplyIdx因为KV操作幂等性，无需担心重复指令造成不一致
        // int CommittedIdx;
        // int ApplyedIdx;
        int VoteFor;
        int LastSnapshotedIdx;
        int lastSnapshotedTerm;
        int Term;
        std::vector<std::string> Logs;

        // Serialization
        template<typename Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar & Term;
            ar & VoteFor;
            ar & LastSnapshotedIdx;
            ar & lastSnapshotedTerm;
            ar & Logs;
        }

        friend class boost::serialization::access;
    };

    int m_Id{};
    int m_Term{};
    int m_VotedFor{-1};

    // Commit、Apply的最后一个位置(end, not last)
    int m_CommittedIdx{};
    int m_ApplyedIdx{};

    // 上次快照的最后一个日志的Idx
    int m_LastSnapshotedIdx{};
    int m_LastSnapshotedTerm{};

    std::string m_BindAddr;
    // 第一条为dummy，idx从1为日志
    std::vector<RaftProtoRPC::LogEntry> m_Logs;

    std::vector<int> m_NextIndex{};
    std::vector<int> m_MatchIndex{};
    std::vector<int> m_ClusterID{};

    std::mutex m_ApplyMutex;
    std::condition_variable m_ApplyCV;

    // 串行化ReadIndex
    std::mutex m_ReadMutex;
    std::atomic<int> m_ReadAckCount;

    State m_State{State::Follower};
    const RaftConfig m_Config{};

    std::chrono::steady_clock::time_point m_LastHeartbeatTime{};
    std::chrono::steady_clock::time_point m_LastElectionTime{};

    mutable std::mutex m_Mutex{};
    std::shared_ptr<SnapshotPersister> m_Persister{};
    std::shared_ptr<BlockingQueue<ApplyMsg>> m_ApplyQueue{};

    std::unique_ptr<RaftRPCService> m_RPCService;
    // 目前没做保护，初始化后不应该修改
    std::unordered_map<int, std::unique_ptr<RaftRPCClient>> m_RPCClients;
    std::function<void(const AppSnapshotMsg &)> m_ApplyAppSnapshotFunction;

    std::shared_ptr<spdlog::logger> m_Logger;
    std::shared_ptr<ThreadPool> m_ThreadPool;

    std::atomic<bool> m_Stop{false};

    std::thread m_ElectionThread{};
    std::thread m_HeartbeatThread{};
    std::thread m_ApplyThread{};

    void electionTicker();
    void heartbeatTicker();
    void applyTicker();

    // 成为Leader后，立刻添加一个DummyLog
    void addDummyLogUnsafe();

    void doHeartbeatUnsafe();

    // 用于ReadIndex的特化版本，发送空日志用于确认仍为Leader
    bool readIndexHeartbeatUnsafe(int term);

    std::vector<RaftProtoRPC::LogEntry> getApplyableLogsUnsafe();

    int getRealLogIdxUnsafe(int logicalLogIdx) const;
    void getLastLogIdxAndTermUnsafe(int &idx, int &term) const;
    int getLastLogIdxUnsafe() const;
    int getLogTermUnsafe(int idx) const;
    void getClusterPrevLogIdxAndTermUnsafe(int serverIdx, int &prevIdx, int &prevTerm) const;
    // 检查logIdx处的log的Term是否匹配
    bool logTermMatchUnsafe(int inLogIdx, int inLogTerm) const;

    // 持久化
    std::string serializeStatesUnsafe() const;
    void persistStatesUnsafe() const;
    void readPersistStates(std::string snapshot);

    void sendAppendEntriesUnsafe(int peerId, std::shared_ptr<RaftProtoRPC::AppendEntriesRequest> request,
                                 std::shared_ptr<std::atomic<int>> appendNum);
    void sendInstallSnapshot(int peerId);

    // rpc
    grpc::Status recvRequestVote(const RaftProtoRPC::RequestVoteRequest &request,
                                 RaftProtoRPC::RequestVoteReply *reply);
    grpc::Status recvAppendEntries(const RaftProtoRPC::AppendEntriesRequest &request,
                                   RaftProtoRPC::AppendEntriesReply *reply);
    grpc::Status recvInstallSnapshot(const RaftProtoRPC::InstallSnapshotRequest &request,
                                     RaftProtoRPC::InstallSnapshotReply *reply);

    void processRequestVoteReply(int peerId, const RaftProtoRPC::RequestVoteReply &reply,
                                 std::shared_ptr<std::atomic<int>> voteNum);
    void processAppendEntriesReply(int peerId, const RaftProtoRPC::AppendEntriesRequest &request,
                                   const RaftProtoRPC::AppendEntriesReply &reply,
                                   std::shared_ptr<std::atomic<int>> appendNum);
    void processInstallSnapshot(int peerId, const RaftProtoRPC::InstallSnapshotRequest &request,
                                const RaftProtoRPC::InstallSnapshotReply &reply);

    // util
    [[nodiscard]] std::chrono::milliseconds getRandomElectionTimeout() const;
    int findPeerIdxUnsafe(int peerId) const;

    friend class RaftRPCService;
};


#endif // RAFTKVSTORAGE_RAFT_NODE_H