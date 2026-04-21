//
// Created by Desktop on 2026/1/14.
//

#include "raft_rpc_service.h"

#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>

#include "raft_node.h"

RaftRPCService::RaftRPCService(RaftNode *node, std::string bindAddress)
    : node_(node), m_Address(std::move(bindAddress)){

    // grpc::ServerBuilder builder;
    // builder.AddListeningPort(m_Address, grpc::InsecureServerCredentials());
    // builder.RegisterService(this);
    //
    // m_Server = builder.BuildAndStart();
    // m_ServerThread = std::thread([this]() {
    //     m_Server->Wait();
    // });
}

RaftRPCService::RaftRPCService(RaftNode *node)
    :node_(node){

}

RaftRPCService::~RaftRPCService() {
    if (m_ServerThread.joinable()) {
        m_ServerThread.join();
    }
}

grpc::Status RaftRPCService::RequestVote(grpc::ServerContext *context, const RaftProtoRPC::RequestVoteRequest *request,
                                         RaftProtoRPC::RequestVoteReply *reply) {
    return node_->recvRequestVote(*request, reply);
}

grpc::Status RaftRPCService::AppendEntries(grpc::ServerContext *context,
    const RaftProtoRPC::AppendEntriesRequest *request, RaftProtoRPC::AppendEntriesReply *reply) {
    return node_->recvAppendEntries(*request, reply);
}

grpc::Status RaftRPCService::InstallSnapshot(grpc::ServerContext *context,
    const RaftProtoRPC::InstallSnapshotRequest *request, RaftProtoRPC::InstallSnapshotReply *reply) {
    return node_->recvInstallSnapshot(*request, reply);
}
