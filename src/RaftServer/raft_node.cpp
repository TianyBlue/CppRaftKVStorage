//
// Created by Desktop on 2026/1/13.
//

#include "raft_node.h"

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <magic_enum/magic_enum.hpp>
#include <mutex>
#include <random>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <thread>
#include "thread_pool.h"

#include "raft_proto_enum.h"
#include "utils.h"

using milliseconds = std::chrono::milliseconds;
using seconds = std::chrono::seconds;

RaftNode::RaftNode() {}

RaftNode::~RaftNode() {
    if (m_ElectionThread.joinable()) {
        m_ElectionThread.join();
    }
    if (m_HeartbeatThread.joinable()) {
        m_HeartbeatThread.join();
    }

    if (m_ApplyThread.joinable()) {
        m_ApplyThread.join();
    }
    m_Logger->flush();
}

// void RaftNode::init(int id, const std::vector<int> &clusterId, const std::vector<std::string> &clusterAddr,
// std::string bindAddr, std::
//                     shared_ptr<SnapshotPersister> persister, std::shared_ptr<BlockingQueue<ApplyMsg>> applyQueue) {
//     // 初始化Node
//     const int clusterSize = clusterId.size();
//     m_ClusterID = clusterId;
//     m_Id = id;
//     m_Term = 0;
//     m_State = State::Follower;
//
//     m_NextIndex.resize(clusterSize, 0);
//     m_MatchIndex.resize(clusterSize, 0);
//
//     m_BindAddr = std::move(bindAddr);
//     m_Persister = persister;
//     m_ApplyQueue = applyQueue;
//
//     assert(m_Persister && m_ApplyQueue);
//     // dummy log
//     // m_Logs.push_back(DummyEntry);
//
//     // 从快照读取
//     readPersistStates(m_Persister->readRaftSnapshot());
//     if (m_LastSnapshotedIdx > 0) {
//         m_ApplyedIdx = m_LastSnapshotedIdx;
//     }
//
//     // 初始化Server
//     m_RPCService = std::make_unique<RaftRPCService>(this, m_BindAddr);
//
//     // 初始化Client
//     for (int i{}; i < clusterId.size(); ++i) {
//         int pId = clusterId[i];
//         const std::string& pAddr = clusterAddr[i];
//         if (pId != m_Id) {
//             m_RPCClients.emplace(
//                 pId,
//                 std::make_unique<RaftRPCClient>(pId, pAddr)
//                 );
//         }
//     }
// }

void RaftNode::init(int id, const std::vector<int> &clusterId, const std::vector<std::string> &clusterAddr,
                    std::shared_ptr<SnapshotPersister> persister, std::shared_ptr<BlockingQueue<ApplyMsg>> applyQueue,
                    std::shared_ptr<spdlog::logger> logger, std::shared_ptr<ThreadPool> pool) {

    // 初始化Node
    const int clusterSize = clusterId.size();
    m_ClusterID = clusterId;
    m_Id = id;
    m_Term = 0;
    m_State = State::Follower;

    m_Logger = logger;
    m_ThreadPool = pool;

    if (!m_Logger) {
        m_Logger = spdlog::stdout_color_mt("raftnode_" + std::to_string(id));
        m_Logger->set_level(spdlog::level::info);
    }

    m_NextIndex.resize(clusterSize, 0);
    m_MatchIndex.resize(clusterSize, 0);

    m_Persister = persister;
    m_ApplyQueue = applyQueue;

    assert(m_Persister && m_ApplyQueue);
    // dummy log
    // m_Logs.push_back(DummyEntry);

    // 从快照读取
    readPersistStates(m_Persister->readRaftSnapshot());
    if (m_LastSnapshotedIdx > 0) {
        m_ApplyedIdx = m_LastSnapshotedIdx;
    }

    // 初始化Server
    m_RPCService = std::make_unique<RaftRPCService>(this);

    // 初始化Client
    for (int i{}; i < clusterId.size(); ++i) {
        int pId = clusterId[i];
        const std::string &pAddr = clusterAddr[i];
        if (pId != m_Id) {
            m_RPCClients.emplace(pId, std::make_unique<RaftRPCClient>(pId, pAddr));
        }
    }
}

void RaftNode::run() {
    m_LastHeartbeatTime = std::chrono::steady_clock::now();
    m_LastElectionTime = std::chrono::steady_clock::now();

    // 后台线程
    m_ElectionThread = std::thread(&RaftNode::electionTicker, this);
    m_HeartbeatThread = std::thread(&RaftNode::heartbeatTicker, this);
    m_ApplyThread = std::thread(&RaftNode::applyTicker, this);

    // t1.detach();
    // t2.detach();
    // t3.detach();
}


void RaftNode::stop() { m_Stop.store(true); }

void RaftNode::addLog(std::string logCommand, bool &outIsLeader, int &outLogIdx, int &outLogTerm) {
    std::lock_guard lk{m_Mutex};
    if (m_State != State::Leader) {
        outIsLeader = false;
        return;
    }

    int lastLogIdx, lastLogTerm;
    getLastLogIdxAndTermUnsafe(lastLogIdx, lastLogTerm);

    RaftProtoRPC::LogEntry entry;
    entry.set_command(std::move(logCommand));
    entry.set_log_index(lastLogIdx + 1);
    entry.set_log_term(m_Term);

    outLogIdx = entry.log_index();
    outLogTerm = entry.log_term();
    outIsLeader = true;

    m_Logs.push_back(std::move(entry));
}

// TODO.先串行化ReadIndex，再尝试批量
bool RaftNode::readIndex(std::uint64_t &readIndex) {
    std::lock_guard readLock{m_ReadMutex};

    std::lock_guard lk{m_Mutex};
    if (m_State != State::Leader) {
        return false;
    }

    int term = m_Term;
    readIndex = m_CommittedIdx;

    // 实现ReadIndex
    bool stillLeader = readIndexHeartbeatUnsafe(term);
    // m_Logger->info("[ReadIndex] leader {} heartbeat, term: {}, readIndex: {}, result {}", m_Id, term, readIndex,
    // stillLeader);
    return stillLeader;
}

bool RaftNode::waitApplied(int applyIndex, std::chrono::milliseconds timeout) {
    std::unique_lock lk{m_ApplyMutex};

    return m_ApplyCV.wait_for(lk, timeout,
                              [this, applyIndex]() { return this->m_Stop ? false : m_ApplyedIdx >= applyIndex; });
}

bool RaftNode::isLeader() {
    std::lock_guard lk{m_Mutex};
    return m_State == State::Leader;
}

RaftNode::RaftStatus RaftNode::getRaftStatus() const{
    RaftNode::RaftStatus status;
    std::lock_guard lk{m_Mutex};

    status.nodeId = m_Id;
    status.state = m_State;
    status.term = m_Term;
    status.committedIdx = m_CommittedIdx;
    status.appliedIdx = m_ApplyedIdx;
    status.lastSnapshotedIdx = m_LastSnapshotedIdx;
    status.logCount = m_Logs.size();
    status.isLeader = (m_State == State::Leader);

    return status;
}

size_t RaftNode::getCurrentLogCount() const {
    std::lock_guard lk{m_Mutex};
    return m_Logs.size();
}
int RaftNode::getCurrentStateSize() const {
    std::lock_guard lk{m_Mutex};
    auto states = serializeStatesUnsafe();
    const int currStateSize = states.size();
    // m_Persister->saveRaftSnapshot(std::move(states));

    return currStateSize;
}

void RaftNode::doSnapshot(int lastSnapshotIdx, std::string appSnapshot) {
    std::lock_guard lk{m_Mutex};

    auto lastLogIdx = getLastLogIdxUnsafe();
    if (lastSnapshotIdx <= m_LastSnapshotedIdx || lastSnapshotIdx > lastLogIdx || lastSnapshotIdx > m_CommittedIdx) {
        // std::cout << "[doSnapshot] snapshot idx out of range" << std::endl;
        m_Logger->warn("[doSnapshot] snapshot idx out of range");
        return;
    }

    if (m_Logs.empty()) {
        m_Persister->save(serializeStatesUnsafe(), std::move(appSnapshot));
        return;
    }

    int realLastSnapshotIdx = getRealLogIdxUnsafe(lastSnapshotIdx);
    int lastSnapshotTerm = getLogTermUnsafe(lastSnapshotIdx);

    std::vector<RaftProtoRPC::LogEntry> truncLogs(m_Logs.begin() + realLastSnapshotIdx + 1, m_Logs.end());
    m_Logs = std::move(truncLogs);
    m_LastSnapshotedIdx = lastSnapshotIdx;
    m_LastSnapshotedTerm = lastSnapshotTerm;

    m_CommittedIdx = std::max(m_CommittedIdx, lastSnapshotIdx);
    m_ApplyedIdx = std::max(m_ApplyedIdx, lastSnapshotIdx);

    m_Persister->save(serializeStatesUnsafe(), std::move(appSnapshot));
}

void RaftNode::debugSendRPC(int peerId) {
    const auto &client = m_RPCClients[peerId];

    RaftProtoRPC::RequestVoteRequest request;
    request.set_term(m_Term);
    request.set_candidate_id(m_Id);
    RaftProtoRPC::RequestVoteReply reply;
    auto status = client->requestVote(request, reply);
    if (!status.ok()) {
        std::cout << status.error_message() << std::endl;
        return;
    }

    // std::cout << std::format("server {} send request to {}\n", m_Id, peerId) << std::endl;
    m_Logger->info(std::format("server {} send request to {}\n", m_Id, peerId));
}

void RaftNode::electionTicker() {
    // ZoneScoped;
    while (!m_Stop.load()) {
        {
            m_Mutex.lock();
            if (m_State == State::Leader) {
                m_Mutex.unlock();
                std::this_thread::sleep_for(milliseconds(m_Config.HeartBeatTimeout));
                continue;
            }
            m_Mutex.unlock();
        }

        auto sleepBeginTime = std::chrono::steady_clock::now();
        std::chrono::steady_clock::time_point suitableWeakTime;
        {
            // 保护对 m_LastElectionTime的访问
            std::lock_guard lk{m_Mutex};
            suitableWeakTime = m_LastElectionTime + getRandomElectionTimeout();
        }

        while (std::chrono::steady_clock::now() < suitableWeakTime) {
            auto sleepTime =
                    std::chrono::duration_cast<milliseconds>(suitableWeakTime - std::chrono::steady_clock::now());
            std::this_thread::sleep_for(std::max(sleepTime, milliseconds(10)));
        }

        auto sleepEndTime = std::chrono::steady_clock::now();
        // std::cout << std::format("[electionTicker]server {} election timeout, sleep time: {}\n",
        //     m_Id, std::chrono::duration_cast<milliseconds>(sleepEndTime - sleepBeginTime)) << std::endl;

        std::lock_guard lk{m_Mutex};
        // 睡眠中重置了m_LastElectionTime，说明有心跳到来，无需选举
        if ((m_LastElectionTime - sleepBeginTime).count() > 0 || m_State == State::Leader) {
            continue;
        }

        // doElection
        // std::cout << std::format("server {} start election", m_Id) << std::endl;
        m_Logger->info(std::format("server {} start election", m_Id));
        m_LastElectionTime = std::chrono::steady_clock::now();

        ++m_Term;
        m_State = State::Candidate;
        m_VotedFor = m_Id;
        persistStatesUnsafe();
        int lastLogIdx, lastLogTerm;
        getLastLogIdxAndTermUnsafe(lastLogIdx, lastLogTerm);
        std::shared_ptr<RaftProtoRPC::RequestVoteRequest> request =
                std::make_shared<RaftProtoRPC::RequestVoteRequest>();
        request->set_term(m_Term);
        request->set_candidate_id(m_Id);
        request->set_last_log_index(lastLogIdx);
        request->set_last_log_term(lastLogTerm);

        std::shared_ptr<std::atomic<int>> votedNum = std::make_shared<std::atomic<int>>(1);
        for (int i{}; i < m_ClusterID.size(); ++i) {
            int peerId = m_ClusterID[i];
            if (peerId == m_Id)
                continue;

            auto &client = m_RPCClients[peerId];
            // std::thread t([this, peerId, request, votedNum, &client]() {
            //     RaftProtoRPC::RequestVoteReply reply;
            //     auto status = client->requestVote(*request.get(), reply);
            //     if (!status.ok()) {
            //         // std::cout << std::format("server {} send requestVote to {} failed, reason: {}", m_Id, peerId,
            //         status.error_message()) << std::endl; m_Logger->warn(std::format("server {} send requestVote to
            //         {} failed, reason: {}", m_Id, peerId, status.error_message())); return;
            //     }
            //     this->processRequestVoteReply(peerId, reply, votedNum);
            // });
            // t.detach();

            m_ThreadPool->enqueue([this, peerId, request, votedNum, &client]() {
                RaftProtoRPC::RequestVoteReply reply;
                auto status = client->requestVote(*request.get(), reply);
                if (!status.ok()) {
                    // std::cout << std::format("server {} send requestVote to {} failed, reason: {}", m_Id, peerId,
                    // status.error_message()) << std::endl;
                    m_Logger->warn(std::format("server {} send requestVote to {} failed, reason: {}", m_Id, peerId,
                                               status.error_message()));
                    return;
                }
                this->processRequestVoteReply(peerId, reply, votedNum);
            });
        }
    }
}

void RaftNode::doHeartbeatUnsafe() {
    // std::lock_guard lk{m_Mutex};

    // std::cout << std::format("server {} start heartbeat\n", m_Id) << std::endl;
    std::shared_ptr<std::atomic<int>> appendNum = std::make_shared<std::atomic<int>>(1);
    for (int i{}; i < m_ClusterID.size(); ++i) {
        int peerId = m_ClusterID[i];
        if (peerId == m_Id)
            continue;

        if (m_NextIndex[i] <= m_LastSnapshotedIdx) {
            m_ThreadPool->enqueue([this, peerId]() { sendInstallSnapshot(peerId); });
        } else {
            int prevIdx, prevTerm;
            getClusterPrevLogIdxAndTermUnsafe(i, prevIdx, prevTerm);

            std::shared_ptr<RaftProtoRPC::AppendEntriesRequest> request =
                    std::make_shared<RaftProtoRPC::AppendEntriesRequest>();
            request->set_term(m_Term);
            request->set_leader_id(m_Id);
            request->set_leader_commit(m_CommittedIdx);
            request->set_prev_log_index(prevIdx);
            request->set_prev_log_term(prevTerm);
            request->clear_entries();

            const int beginIdx = getRealLogIdxUnsafe(prevIdx);
            for (int currIdx{beginIdx + 1}; currIdx < m_Logs.size(); ++currIdx) {
                auto *entry = request->add_entries();
                *entry = m_Logs[currIdx];
            }

            m_ThreadPool->enqueue([this, peerId, request, appendNum]() {
                this->sendAppendEntriesUnsafe(peerId, request, appendNum);
            });
        }
    }

    m_LastHeartbeatTime = std::chrono::steady_clock::now();
}
bool RaftNode::readIndexHeartbeatUnsafe(int term) {
    std::vector<std::future<bool>> futures;

    int successCount = 1;
    const int majority = m_ClusterID.size() / 2 + 1;

    for (int i{}; i < m_ClusterID.size(); ++i) {
        int peerId = m_ClusterID[i];
        if (peerId == m_Id)
            continue;

        int prevIdx, prevTerm;
        getClusterPrevLogIdxAndTermUnsafe(i, prevIdx, prevTerm);

        std::shared_ptr<RaftProtoRPC::AppendEntriesRequest> request =
                std::make_shared<RaftProtoRPC::AppendEntriesRequest>();
        request->set_term(m_Term);
        request->set_leader_id(m_Id);
        request->set_leader_commit(m_CommittedIdx);
        request->set_prev_log_index(prevIdx);
        request->set_prev_log_term(prevTerm);
        request->clear_entries();

        futures.emplace_back(m_ThreadPool->enqueue([this, peerId, request, term]() -> bool {
            RaftProtoRPC::AppendEntriesReply reply;
            auto status = m_RPCClients[peerId]->appendEntries(*request, reply);
            if (!status.ok())
                return false;

            if (reply.term() > term)
                return false;

            return reply.success();
        }));
    }

    for (auto &f: futures) {
        if (f.get()) {
            ++successCount;
        }

        if (successCount >= majority) {
            return true;
        }
    }

    return false;
}

void RaftNode::heartbeatTicker() {
    while (!m_Stop.load()) {
        {
            m_Mutex.lock();
            if (m_State != State::Leader) {
                m_Mutex.unlock();
                std::this_thread::sleep_for(milliseconds(m_Config.HeartBeatTimeout));
                continue;
            }
            m_Mutex.unlock();
        }

        auto sleepBeginTime = std::chrono::steady_clock::now();
        std::chrono::steady_clock::time_point suitableWeakTime;
        {
            // 保护对 m_LastElectionTime的访问
            std::lock_guard lk{m_Mutex};
            suitableWeakTime = m_LastHeartbeatTime + milliseconds(m_Config.HeartBeatTimeout);
        }

        while (std::chrono::steady_clock::now() < suitableWeakTime) {
            auto sleepTime =
                    std::chrono::duration_cast<milliseconds>(suitableWeakTime - std::chrono::steady_clock::now());
            std::this_thread::sleep_for(std::max(sleepTime, milliseconds(10)));
        }

        auto sleepEndTime = std::chrono::steady_clock::now();
        // std::cout << std::format("[heartbeatTicker]server {} heartbeat timeout, sleep time: {}\n",
        //     m_Id, std::chrono::duration_cast<milliseconds>(sleepEndTime - sleepBeginTime)) << std::endl;

        std::lock_guard lk{m_Mutex};
        if (m_State != State::Leader) {
            continue;
        }
        doHeartbeatUnsafe();
    }
}

void RaftNode::applyTicker() {
    while (!m_Stop.load()) {
        std::this_thread::sleep_for(milliseconds(m_Config.ApplyInterval));
        std::vector<RaftProtoRPC::LogEntry> logs;
        {
            std::lock_guard lk{m_Mutex};
            logs = getApplyableLogsUnsafe();
        }

        if (!logs.empty()) {
            // std::cout << std::format("server {} apply logs, size: {}\n", m_Id, logs.size()) << std::endl;
            m_Logger->info(std::format("server {} apply logs, size: {}", m_Id, logs.size()));
        }
        for (auto &log: logs) {
            ApplyMsg applyMsg;
            applyMsg.isSnapshot = false;
            applyMsg.applyCommand = std::move(log.command());
            applyMsg.logIdx = log.log_index();
            applyMsg.logTerm = log.log_term();

            m_ApplyQueue->push(std::move(applyMsg));
        }

        m_ApplyCV.notify_all();
    }

    m_ApplyCV.notify_all();
}

void RaftNode::addDummyLogUnsafe() {
    RaftProtoRPC::LogEntry dummy;
    dummy.set_log_term(m_Term);
    dummy.set_log_index(getLastLogIdxUnsafe() + 1);
    m_Logs.push_back(dummy);
}

std::vector<RaftProtoRPC::LogEntry> RaftNode::getApplyableLogsUnsafe() {
    std::vector<RaftProtoRPC::LogEntry> applyable{};
    applyable.reserve(std::max(m_CommittedIdx - m_ApplyedIdx, 0));
    while (m_ApplyedIdx < m_CommittedIdx) {
        m_ApplyedIdx++;
        applyable.push_back(m_Logs[getRealLogIdxUnsafe(m_ApplyedIdx)]);
    }

    return applyable;
}

int RaftNode::getRealLogIdxUnsafe(int logicalLogIdx) const { return logicalLogIdx - m_LastSnapshotedIdx - 1; }

void RaftNode::getLastLogIdxAndTermUnsafe(int &idx, int &term) const {
    if (m_Logs.empty()) {
        idx = m_LastSnapshotedIdx;
        term = m_LastSnapshotedTerm;
        return;
    }
    idx = m_Logs.back().log_index();
    term = m_Logs.back().log_term();
}

int RaftNode::getLastLogIdxUnsafe() const {
    int idx, _;
    getLastLogIdxAndTermUnsafe(idx, _);
    return idx;
}

int RaftNode::getLogTermUnsafe(int idx) const {
    return idx == m_LastSnapshotedIdx ? m_LastSnapshotedTerm : m_Logs[getRealLogIdxUnsafe(idx)].log_term();
}

void RaftNode::getClusterPrevLogIdxAndTermUnsafe(int serverIdx, int &prevIdx, int &prevTerm) const {
    if (m_NextIndex[serverIdx] == m_LastSnapshotedIdx + 1) {
        prevIdx = m_LastSnapshotedIdx;
        prevTerm = m_LastSnapshotedTerm;
    } else {
        int nxtIdx = m_NextIndex[serverIdx];
        prevIdx = nxtIdx - 1;
        prevTerm = m_Logs[getRealLogIdxUnsafe(prevIdx)].log_term();
    }
}

bool RaftNode::logTermMatchUnsafe(int inLogIdx, int inLogTerm) const {
    // return m_Logs[getRealLogIdxUnsafe(inLogIdx)].log_term() == inLogTerm;

    return getLogTermUnsafe(inLogIdx) == inLogTerm;
}

std::string RaftNode::serializeStatesUnsafe() const {
    RaftPersistStates states;
    states.Term = m_Term;
    states.VoteFor = m_VotedFor;
    states.LastSnapshotedIdx = m_LastSnapshotedIdx;
    states.lastSnapshotedTerm = m_LastSnapshotedTerm;
    states.Logs.reserve(m_Logs.size());
    for (const auto &log: m_Logs) {
        states.Logs.push_back(log.SerializeAsString());
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << states;
    return ss.str();
}

void RaftNode::persistStatesUnsafe() const { bool status = m_Persister->saveRaftSnapshot(serializeStatesUnsafe()); }

void RaftNode::readPersistStates(std::string snapshot) {
    if (snapshot == "")
        return;

    RaftPersistStates states{};
    std::stringstream ss(snapshot);
    boost::archive::text_iarchive ia(ss);
    ia >> states;

    std::lock_guard lk{m_Mutex};
    m_Term = states.Term;
    m_VotedFor = states.VoteFor;
    m_LastSnapshotedTerm = states.lastSnapshotedTerm;
    m_LastSnapshotedIdx = states.LastSnapshotedIdx;
    m_Logs.clear();
    m_Logs.reserve(states.Logs.size());
    for (const auto &log: states.Logs) {
        RaftProtoRPC::LogEntry l;
        l.ParseFromString(log);
        m_Logs.push_back(std::move(l));
    }
}

void RaftNode::sendAppendEntriesUnsafe(int peerId, std::shared_ptr<RaftProtoRPC::AppendEntriesRequest> request,
                                       std::shared_ptr<std::atomic<int>> appendNum) {

    const auto &client = m_RPCClients[peerId];
    RaftProtoRPC::AppendEntriesReply reply;
    auto status = client->appendEntries(*request, reply);
    if (status.ok()) {
        this->processAppendEntriesReply(peerId, *request, reply, appendNum);
    }
}

void RaftNode::sendInstallSnapshot(int peerId) {
    std::shared_ptr<RaftProtoRPC::InstallSnapshotRequest> request =
            std::make_shared<RaftProtoRPC::InstallSnapshotRequest>();
    RaftProtoRPC::InstallSnapshotReply reply;
    {
        std::lock_guard lk{m_Mutex};
        request->set_term(m_Term);
        request->set_leader_id(m_Id);
        request->set_last_included_index(m_LastSnapshotedIdx);
        request->set_last_included_term(m_LastSnapshotedTerm);
        request->set_snapshot(m_Persister->readAppSnapshot());
    }
    const auto &client = m_RPCClients[peerId];
    auto status = client->installSnapshot(*request, reply);

    if (status.ok()) {
        this->processInstallSnapshot(peerId, *request, reply);
    }
}

grpc::Status RaftNode::recvRequestVote(const RaftProtoRPC::RequestVoteRequest &request,
                                       RaftProtoRPC::RequestVoteReply *reply) {
    // ZoneScoped;

    std::lock_guard lk{m_Mutex};
    // std::cout << std::format("server {} receive RequestVote, from: {}\n", m_Id, request.candidate_id());
    m_Logger->info(std::format("server {} receive RequestVote, from: {}", m_Id, request.candidate_id()));

    if (request.term() < m_Term) {
        reply->set_term(m_Term);
        reply->set_vote_granted(false);
        reply->set_vote_state(VoteState::EXPIRE);
        return grpc::Status::OK;
    }

    if (request.term() > m_Term) {
        m_State = State::Follower;
        m_VotedFor = -1;
        m_Term = request.term();
    }

    persistStatesUnsafe();
    assert(m_Term == request.term());
    // 遇到candidate重发请求的时候不能回复已经投过票了
    if (m_VotedFor != -1 && m_VotedFor != request.candidate_id()) {
        reply->set_term(m_Term);
        reply->set_vote_granted(false);
        reply->set_vote_state(VoteState::VOTED);

        return grpc::Status::OK;
    }

    int lastLogIdx, lastLogTerm;
    getLastLogIdxAndTermUnsafe(lastLogIdx, lastLogTerm);

    // 本地Log更新，拒绝投票
    if (lastLogTerm > request.last_log_term() || lastLogIdx > request.last_log_index()) {
        reply->set_term(m_Term);
        reply->set_vote_granted(false);
        reply->set_vote_state(VoteState::OLDER);
        return grpc::Status::OK;
    }

    m_VotedFor = request.candidate_id();
    persistStatesUnsafe();

    reply->set_term(m_Term);
    reply->set_vote_granted(true);
    reply->set_vote_state(VoteState::NORMAL);

    m_LastElectionTime = std::chrono::steady_clock::now();
    persistStatesUnsafe();
    return grpc::Status::OK;
}

grpc::Status RaftNode::recvAppendEntries(const RaftProtoRPC::AppendEntriesRequest &request,
                                         RaftProtoRPC::AppendEntriesReply *reply) {
    // ZoneScoped;

    // auto startRecvTime = std::chrono::steady_clock::now();

    std::lock_guard lk{m_Mutex};

    if (request.term() < m_Term) {
        reply->set_term(m_Term);
        reply->set_next_index(-1);
        reply->set_success(false);
        reply->set_append_state(AppendState::TERM_TOO_OLD);

        // std::cout << std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, EntrySize: {}, term too
        // old\n", m_Id, request.leader_id(), request.entries_size());
        return grpc::Status::OK;
    }

    // std::cout << std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, EntrySize: {}\n", m_Id,
    // request.leader_id(), request.entries_size());
    if (request.entries_size() > 0) {
        // std::cout << std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, EntrySize: {}\n", m_Id,
        // request.leader_id(), request.entries_size());
        m_Logger->info(std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, EntrySize: {}", m_Id,
                                   request.leader_id(), request.entries_size()));
    }

    if (request.term() > m_Term) {
        m_Term = request.term();
        m_State = State::Follower;
        m_VotedFor = -1;
    }
    reply->set_term(m_Term);
    m_State = State::Follower;
    persistStatesUnsafe();

    // 刷新选举时间，防止触发选举
    m_LastElectionTime = std::chrono::steady_clock::now();

    int lastLogIdx, lastLogTerm;
    getLastLogIdxAndTermUnsafe(lastLogIdx, lastLogTerm);
    // term一致，现在比较log
    if (request.prev_log_index() > lastLogIdx) {
        reply->set_success(false);
        reply->set_next_index(lastLogIdx + 1);
        reply->set_append_state(AppendState::INDEX_TOO_LARGE);
        // std::cout << std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, EntrySize: {}, index too
        // large\n", m_Id, request.leader_id(), request.entries_size());
        m_Logger->warn(
                std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, EntrySize: {}, index too large",
                            m_Id, request.leader_id(), request.entries_size()));
        return grpc::Status::OK;
    }

    // 查看 初始Log是否匹配
    // term不匹配，说明该term的所有log都得丢弃
    if (!logTermMatchUnsafe(request.prev_log_index(), request.prev_log_term())) {
        reply->set_next_index(request.prev_log_index());
        const int thisTerm = getLogTermUnsafe(request.prev_log_index());
        int correctNextIdx{-1};
        for (int i = request.prev_log_index() - 1; i >= 0; i--) {
            if (getLogTermUnsafe(i) != thisTerm) {
                correctNextIdx = i + 1;
                break;
            }
        }

        reply->set_success(false);
        reply->set_next_index(correctNextIdx);
        reply->set_append_state(AppendState::LOG_NOT_MATCH);
        // std::cout << std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, LOG_NOT_MATCH\n", m_Id,
        // request.leader_id());
        m_Logger->warn(std::format("[recvAppendEntry] Server {} recv AppendEntry from {}, LOG_NOT_MATCH", m_Id,
                                   request.leader_id()));

        return grpc::Status::OK;
    }

    // term匹配，看看log是否匹配
    for (int i = 0; i < request.entries_size(); i++) {
        const auto &log = request.entries(i);
        if (log.log_index() > lastLogIdx) {
            m_Logs.push_back(log);
        } else {
            // 检查每个log是否匹配，不匹配替换，匹配跳过
            int idx = log.log_index();
            if (getLogTermUnsafe(idx) != log.log_term()) {
                m_Logs[getRealLogIdxUnsafe(idx)] = log;
            }
        }
    }

    getLastLogIdxAndTermUnsafe(lastLogIdx, lastLogTerm);
    if (m_CommittedIdx < request.leader_commit()) {
        m_CommittedIdx = std::min(request.leader_commit(), lastLogIdx);
    }

    reply->set_next_index(lastLogIdx + 1);
    reply->set_success(true);
    reply->set_append_state(AppendState::SUCCESS);

    // auto endRecvTime = std::chrono::steady_clock::now();
    // std::cout << std::format("server {} revice AppendEntries, from: {}, start {}, end {}\n",
    //     m_Id, request.leader_id(), utils::getFormattedTime(startRecvTime), utils::getFormattedTime(endRecvTime));

    return grpc::Status::OK;
}

grpc::Status RaftNode::recvInstallSnapshot(const RaftProtoRPC::InstallSnapshotRequest &request,
                                           RaftProtoRPC::InstallSnapshotReply *reply) {
    // 接收到Snapshot
    std::lock_guard lk{m_Mutex};
    if (request.term() < m_Term) {
        reply->set_term(m_Term);
        reply->set_success(false);
        return grpc::Status::OK;
    }

    if (request.term() > m_Term) {
        m_State = State::Follower;
        m_Term = request.term();
        m_VotedFor = -1;
        persistStatesUnsafe();
        return grpc::Status::OK;
    }

    if (request.last_included_index() < m_LastSnapshotedIdx) {
        reply->set_success(false);
        reply->set_term(m_Term);
        return grpc::Status::OK;
    }

    int lastLogTerm, lastLogIdx;
    getLastLogIdxAndTermUnsafe(lastLogIdx, lastLogTerm);
    // 快照包含部分Log
    if (lastLogIdx > request.last_included_index()) {
        int eraseCount = request.last_included_index() - m_LastSnapshotedIdx;
        m_Logs.erase(m_Logs.begin(), m_Logs.begin() + eraseCount);
    }
    // 快照包含所有log，直接更新快照，清空logs
    else {
        m_Logs.clear();
    }

    // 更新属性
    m_CommittedIdx = std::max(m_CommittedIdx, request.last_included_index());
    m_ApplyedIdx = std::max(m_ApplyedIdx, request.last_included_index());
    m_LastSnapshotedIdx = request.last_included_index();
    m_LastSnapshotedTerm = request.last_included_term();


    std::shared_ptr<ApplyMsg> snapshotMsg = std::make_shared<ApplyMsg>();
    snapshotMsg->isSnapshot = true;
    snapshotMsg->snapshot = request.snapshot();
    snapshotMsg->lastSnapshotedIdx = request.last_included_index();
    snapshotMsg->lastSnapShotedTerm = request.last_included_term();

    // 发送快照
    // auto t = std::thread([this, snapshotMsg]() {
    //     // this->m_ApplyAppSnapshotFunction(*snapshotMsg);
    //     this->m_ApplyQueue->push(std::move(*snapshotMsg));
    // });

    // t.detach();

    m_ThreadPool->enqueue([this, snapshotMsg]() { this->m_ApplyQueue->push(std::move(*snapshotMsg)); });

    return grpc::Status::OK;
}

void RaftNode::processRequestVoteReply(int peerId, const RaftProtoRPC::RequestVoteReply &reply,
                                       std::shared_ptr<std::atomic<int>> voteNum) {
    // ZoneScoped;
    std::lock_guard lk{m_Mutex};
    // peerTerm更新，改变状态
    if (reply.term() > m_Term) {
        m_Logger->warn("[Vote] Server {} recv vote from {}, peer Term {} is newer than me {}", m_Id, peerId,
                       reply.term(), m_Term);
        m_Term = reply.term();
        m_VotedFor = -1;
        m_State = State::Follower;
        persistStatesUnsafe();
        return;
    }
    // 非本term的投票，忽略
    else if (reply.term() < m_Term) {
        m_Logger->info("[Vote] Server {} recv vote from {}, peer Term {} is older than me {}", m_Id, peerId,
                       reply.term(), m_Term);
        return;
    }

    m_Logger->info(
            std::format("[Vote] Server {} recv vote from {}, vote granted: {}", m_Id, peerId, reply.vote_granted()));
    assert(reply.term() == m_Term);
    if (!reply.vote_granted()) {
        return;
    }

    ++(*voteNum);
    // 成为Leader
    if (voteNum->load() > m_ClusterID.size() / 2) {
        voteNum->store(0);

        // std::cout << std::format("[Vote] Server {} becomes Leader.", m_Id) << std::endl;
        m_Logger->info(std::format("[Vote] Server {} becomes Leader.", m_Id));
        if (m_State == State::Leader) {
            return;
        }

        m_State = State::Leader;
        int lastLogIdx, _;
        getLastLogIdxAndTermUnsafe(lastLogIdx, _);
        for (int i = 0; i < m_NextIndex.size(); i++) {
            m_NextIndex[i] = lastLogIdx + 1; // 有效下标从1开始，因此要+1
            m_MatchIndex[i] = 0; // 每换一个领导都是从0开始，见fig2
        }

        addDummyLogUnsafe();
        doHeartbeatUnsafe();
    }
}

void RaftNode::processAppendEntriesReply(int peerId, const RaftProtoRPC::AppendEntriesRequest &request,
                                         const RaftProtoRPC::AppendEntriesReply &reply,
                                         std::shared_ptr<std::atomic<int>> appendNum) {
    // ZoneScoped;

    AppendState::AppendState appendState = static_cast<AppendState::AppendState>(reply.append_state());
    // std::cout << std::format("[AppendReply] Server {} recvAppendEntryReply from {}, appendState {}.\n",
    //     m_Id, peerId,
    //     magic_enum::enum_name(appendState)
    //     );
    std::lock_guard lk{m_Mutex};

    if (reply.term() > m_Term) {
        m_Logger->warn("[processAppendRely] Server {} recv vote from {}, peer Term {} is newer than me {}", m_Id,
                       peerId, reply.term(), m_Term);

        m_State = State::Follower;
        m_VotedFor = -1;
        m_Term = reply.term();
        persistStatesUnsafe();
    } else if (reply.term() < m_Term) {
        m_Logger->info("[processAppendRely] Server {} recv vote from {}, peer Term {} is older than me {}", m_Id,
                       peerId, reply.term(), m_Term);

        return;
    }

    int peerIdx = findPeerIdxUnsafe(peerId);
    assert(m_Term == reply.term());
    if (!reply.success()) {
        int correctNxtIdx = reply.next_index();
        m_NextIndex[peerIdx] = std::max(1, correctNxtIdx);
        // std::cout << std::format("[AppendReply] Server {} recvAppendEntryReply from {} not success, appendState
        // {}.\n",
        //     m_Id, peerId,
        //     magic_enum::enum_name(appendState)
        //     );

        m_Logger->warn(std::format("[AppendReply] Server {} recvAppendEntryReply from {} not success, appendState {}.",
                                   m_Id, peerId, magic_enum::enum_name(appendState)));
        return;
    }

    ++(*appendNum);
    m_MatchIndex[peerIdx] = std::max(m_MatchIndex[peerIdx], request.prev_log_index() + request.entries_size());
    m_NextIndex[peerIdx] = m_MatchIndex[peerIdx] + 1;

    // 每次HeartBeat都会尝试向Follower节点发送从NextIdx->Leader.lastLog的所有日志
    // 因此当过半节点收到时，可更新commitIdx到LastLog
    if (appendNum->load() > m_ClusterID.size() / 2) {
        appendNum->store(0);

        m_CommittedIdx = std::max(m_CommittedIdx, request.prev_log_index() + request.entries_size());
        assert(m_CommittedIdx <= getLastLogIdxUnsafe());
    }
}

void RaftNode::processInstallSnapshot(int peerId, const RaftProtoRPC::InstallSnapshotRequest &request,
                                      const RaftProtoRPC::InstallSnapshotReply &reply) {
    std::lock_guard lk{m_Mutex};
    if (reply.term() > m_Term) {
        m_Term = reply.term();
        m_VotedFor = -1;
        m_State = State::Follower;
        return;
    } else if (reply.term() < m_Term || !reply.success()) {
        return;
    }

    int peerIdx = findPeerIdxUnsafe(peerId);
    assert(peerIdx != -1);
    m_NextIndex[peerIdx] = request.last_included_index() + 1;
    m_MatchIndex[peerIdx] = request.last_included_index();
}

std::chrono::milliseconds RaftNode::getRandomElectionTimeout() const {
    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<> dis(m_Config.MinElectionTimeout, m_Config.MaxElectionTimeout);
    return std::chrono::milliseconds(dis(engine));
}

int RaftNode::findPeerIdxUnsafe(int peerId) const {
    auto it = std::find(m_ClusterID.begin(), m_ClusterID.end(), peerId);
    return it == m_ClusterID.end() ? -1 : it - m_ClusterID.begin();
}
