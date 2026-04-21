//
// Created by Desktop on 2026/1/14.
//

#ifndef RAFTKVSTORAGE_RAFT_RPC_CLIENT_H
#define RAFTKVSTORAGE_RAFT_RPC_CLIENT_H

#include <string>
#include <grpc++/channel.h>

#include "raft_rpc.grpc.pb.h"

// TODO. 考虑将接口改成异步的，减少直接的线程创建
class RaftRPCClient {
public:
    RaftRPCClient(int peerId, const std::string& address);

    grpc::Status requestVote(const RaftProtoRPC::RequestVoteRequest &request, RaftProtoRPC::RequestVoteReply &reply,
                             std::chrono::milliseconds
                             timeout = std::chrono::milliseconds(100)) const;

    grpc::Status appendEntries(const RaftProtoRPC::AppendEntriesRequest &request,
                               RaftProtoRPC::AppendEntriesReply &reply, std::chrono::milliseconds
                               timeout = std::chrono::milliseconds(100)) const;

    grpc::Status installSnapshot(const RaftProtoRPC::InstallSnapshotRequest& request,
                                 RaftProtoRPC::InstallSnapshotReply &reply,
                                 std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) const;

private:

    int m_PeerId;
    std::string m_PeerAddress;

    std::shared_ptr<grpc::Channel> m_Channel;
    std::unique_ptr<RaftProtoRPC::RaftRPC::Stub> m_Stub;
};


#endif //RAFTKVSTORAGE_RAFT_RPC_CLIENT_H