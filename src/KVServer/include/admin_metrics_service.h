#ifndef ADMIN_METRCIS_SERVICE
#define ADMIN_METRCIS_SERVICE

#include <grpcpp/support/status.h>
#include "admin_metrics.grpc.pb.h"

class KVServerNode;

class AdminMetricsService : public AdminProto::AdminRpc::Service {
public:
    AdminMetricsService(KVServerNode* owner)
    : owner{owner}{}

    grpc::Status GetNodeStatus(grpc::ServerContext *context, const AdminProto::Empty *request,
                               AdminProto::NodeStatusReply *response) override;
    grpc::Status GetMetrics(grpc::ServerContext *context, const AdminProto::Empty *request,
                            AdminProto::MetricsReply *response) override;
private:
    KVServerNode* owner{};
};

#endif