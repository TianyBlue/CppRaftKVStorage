//
// Created by Desktop on 2026/1/14.
//

#ifndef RAFTKVSTORAGE_RAFT_RPC_SERVICE_H
#define RAFTKVSTORAGE_RAFT_RPC_SERVICE_H
#include <thread>

#include "raft_rpc.grpc.pb.h"
#include "raft_rpc.pb.h"
#include <grpcpp/server.h>
class RaftNode;

class RaftRPCService : public RaftProtoRPC::RaftRPC::Service
{
public:
    RaftRPCService(RaftNode* node, std::string bindAddress);
    RaftRPCService(RaftNode* node);
    ~RaftRPCService();

    grpc::Status RequestVote(
        grpc::ServerContext* context,
        const RaftProtoRPC::RequestVoteRequest* request,
        RaftProtoRPC::RequestVoteReply* reply) override;

    grpc::Status AppendEntries(
        grpc::ServerContext* context,
        const RaftProtoRPC::AppendEntriesRequest* request,
        RaftProtoRPC::AppendEntriesReply* reply) override;

    grpc::Status InstallSnapshot(
        grpc::ServerContext* context,
        const RaftProtoRPC::InstallSnapshotRequest* request,
        RaftProtoRPC::InstallSnapshotReply* reply) override;


private:
    RaftNode* node_;
    std::string m_Address;
    std::unique_ptr<grpc::Server> m_Server;

    std::thread m_ServerThread;
};


#endif //RAFTKVSTORAGE_RAFT_RPC_SERVICE_H