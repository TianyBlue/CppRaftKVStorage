#include "include/admin_metrics_service.h"
#include <grpcpp/support/status.h>
#include <magic_enum/magic_enum.hpp>
#include "kv_server_node.h"
#include "raft_node.h"

grpc::Status AdminMetricsService::GetNodeStatus(grpc::ServerContext *context, const AdminProto::Empty *request,
                                                AdminProto::NodeStatusReply *response) {
    (void) context;
    (void) request;

    auto status = owner->getRaftStatus();
    response->set_node_id(status.nodeId);
    response->set_raft_state(std::string{magic_enum::enum_name<RaftNode::State>(status.state)});
    response->set_term(status.term);
    response->set_commit_idx(status.committedIdx);
    response->set_apply_idx(status.appliedIdx);
    response->set_last_snapshot_idx(status.lastSnapshotedIdx);
    response->set_raft_log_size(status.logCount);

    response->set_grpc_serving(owner->isServing());
    response->set_is_leader(status.isLeader);

    return grpc::Status::OK;
}

grpc::Status AdminMetricsService::GetMetrics(grpc::ServerContext *context, const AdminProto::Empty *request,
                                             AdminProto::MetricsReply *response) {
    (void) context;
    (void) request;

    const auto metrics = owner->getMetrics();
    response->set_get_total(metrics.getTotal);
    response->set_put_total(metrics.putTotal);
    response->set_delete_total(metrics.deleteTotal);

    response->set_success_total(metrics.successTotal);
    response->set_timeout_total(metrics.timeoutTotal);
    response->set_not_leader_total(metrics.notLeaderTotal);
    response->set_key_not_found_total(metrics.keyNotFoundTotal);
    response->set_request_expired_total(metrics.requestExpiredTotal);
    response->set_request_duplicate_total(metrics.requestDuplicateTotal);

    response->set_snapshot_install_total(metrics.snapshotInstallTotal);
    response->set_snapshot_create_total(metrics.snapshotCreateTotal);

    response->set_slow_request_total(metrics.slowRequestTotal);

    return grpc::Status::OK;
}
