//
// Created by Desktop on 2026/1/14.
//

#include "raft_rpc_client.h"
#include <grpc++/grpc++.h>


RaftRPCClient::RaftRPCClient(int peerId, const std::string &address)
    : m_PeerId(peerId), m_PeerAddress(address){

    m_Channel = grpc::CreateChannel(
        m_PeerAddress,
        grpc::InsecureChannelCredentials()
    );
    m_Stub = RaftProtoRPC::RaftRPC::NewStub(m_Channel);
}

grpc::Status RaftRPCClient::requestVote(const RaftProtoRPC::RequestVoteRequest &request,
                                        RaftProtoRPC::RequestVoteReply &reply, std::chrono::milliseconds timeout) const {

    grpc::ClientContext context;
    const auto deadline = std::chrono::system_clock::now() + timeout;
    context.set_deadline(deadline);

    return m_Stub->RequestVote(&context, request, &reply);
}

grpc::Status RaftRPCClient::appendEntries(const RaftProtoRPC::AppendEntriesRequest &request,
                                          RaftProtoRPC::AppendEntriesReply &reply,
                                          std::chrono::milliseconds timeout) const {

    grpc::ClientContext context;
    const auto deadline = std::chrono::system_clock::now() + timeout;
    context.set_deadline(deadline);

    return m_Stub->AppendEntries(&context, request, &reply);
}

grpc::Status RaftRPCClient::installSnapshot(const RaftProtoRPC::InstallSnapshotRequest &request,
                                            RaftProtoRPC::InstallSnapshotReply &reply, std::chrono::milliseconds timeout) const {
    grpc::ClientContext context;
    const auto deadline = std::chrono::system_clock::now() + timeout;
    context.set_deadline(deadline);

    return m_Stub->InstallSnapshot(&context, request, &reply);
}
