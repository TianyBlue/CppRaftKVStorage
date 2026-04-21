//
// Created by Desktop on 2026/1/22.
//

#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>

#include "kv_service.h"
#include "kv_server_node.h"

KVService::KVService(KVServerNode *node)
    :node_(node){
}

grpc::Status KVService::Put(grpc::ServerContext *context, const KVServerProto::PutRequest *request,
                            KVServerProto::PutReply *response) {
    return node_->recvPut(request, response);
}

grpc::Status KVService::Get(grpc::ServerContext *context, const KVServerProto::GetRequest *request,
    KVServerProto::GetReply *response) {
    return node_->recvGet(request, response);
}
grpc::Status KVService::Delete(grpc::ServerContext *context, const KVServerProto::DeleteRequest *request,
                               KVServerProto::DeleteReply *response) {
    return node_->recvDelete(request, response);
}
