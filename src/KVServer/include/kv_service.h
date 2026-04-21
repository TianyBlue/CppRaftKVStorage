//
// Created by Desktop on 2026/1/22.
//

#ifndef RAFTKVSTORAGE_KV_SERVICE_H
#define RAFTKVSTORAGE_KV_SERVICE_H
#include "kv_server_rpc.grpc.pb.h"

class KVServerNode;

class KVService : public KVServerProto::KVRpc::Service {
public:
    KVService(KVServerNode* node);

    grpc::Status Put(grpc::ServerContext *context, const KVServerProto::PutRequest *request,
        KVServerProto::PutReply *response) override;

    grpc::Status Get(grpc::ServerContext *context, const KVServerProto::GetRequest *request,
        KVServerProto::GetReply *response) override;

    grpc::Status Delete(grpc::ServerContext *context, const KVServerProto::DeleteRequest *request,
                        KVServerProto::DeleteReply *response) override;


private:
    KVServerNode* node_;
};


#endif //RAFTKVSTORAGE_KV_SERVICE_H