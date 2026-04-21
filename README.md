# RaftKVStorage

一个基于 Raft 共识算法的分布式 KV 存储系统，使用 C++20、gRPC、Protobuf和SkipList实现。项目包含 Raft 节点、KV 状态机、快照持久化、多节点部署配置，以及一个简单的交互式客户端测试程序。

项目参考[代码随想录](https://github.com/youngyangyang04/KVstorageBaseRaft-cpp)实现。

当前代码已经实现了：

- Raft 选举、心跳、日志复制、快照和日志压缩
- KV状态机快照和恢复
- `ReadIndex`
- `Docker Compose` 三节点部署配置

## 项目结构

```text
.
├── src/
│   ├── RaftServer/      # Raft 核心逻辑、RPC、快照持久化
│   ├── KVServer/        # KV 服务
│   ├── KVStore/         # SkipList
│   ├── KVClient/
│   ├── protos/
│   ├── ThreadPool/
│   ├── BlockingQueue/
│   └── Common/
├── test/KVClientTest/
├── deploy/config/       # 3 节点部署配置
├── docker-compose.yml   # Docker 部署
├── Dockerfile
└── raft.pdf
```

## 核心设计

### 1. Raft 层

`RaftNode` 提供以下能力：

- Follower / Candidate / Leader 三状态切换
- 随机选举超时
- AppendEntries 心跳与日志复制
- RequestVote 选举
- InstallSnapshot 快照同步
- 本地状态持久化与恢复

### 2. KV 层

`KVServerNode` 在 Raft 之上实现状态机：

- `Put`
- `Get`
- `Delete`

写请求会先写入 Raft 日志，读请求走 `ReadIndex` 。

### 3. 存储层

底层 KV 存储使用 `SkipList`。


### 4. 快照

快照由两部分组成：

- Raft 状态快照
- 应用状态快照（即 KV 数据）


## 构建与运行

### 依赖

开发环境`Ubuntu 22.04`，依赖库：

- C++20
- CMake >= 3.22
- Protobuf 29.6
- gRPC
- Boost.Serialization
- magic_enum
- spdlog
- yaml-cpp


### 本地构建

```bash
cmake -S . -B build
cmake --build build -j
```

构建出的KVServerNode作为启动入口，可以参考config/kv_config.yaml作为单节点的启动配置。

`./KVServerNode -config=kv_config1.yaml`
`./KVServerNode -config=kv_config2.yaml`
`./KVServerNode -config=kv_config3.yaml`


### 配置文件介绍


配置文件根字段为 `kvserver`，当前支持：

- `id`: 当前节点 ID
- `bind_addr`: gRPC 监听地址
- `persist_dir`: 快照和持久化文件目录
- `log_dir`: 日志目录
- `peer_id`: 集群节点 ID 列表
- `peer_addr`: 集群节点地址列表
- `max_raft_log_count`: 最大日志数量阈值
- `max_raft_state_size`: Raft 状态大小阈值
- `queue_max_wait_time`: KV请求超时时间，单位毫秒


示例：

```yaml
kvserver:
  id: 1
  bind_addr: 0.0.0.0:50001
  persist_dir: ./data
  log_dir: ./logs
  peer_id: [1, 2, 3]
  peer_addr: [localhost:50001, localhost:50002, localhost:50003]
  max_raft_log_count: 10000
  max_raft_state_size: 6291456
  queue_max_wait_time: 500
```

##  Docker Compose启动集群


### 1. 构建二进制

先在宿主机完成编译，确保 `bin/KVServerNode` 已存在。

### 2. 收集运行时动态库

项目自带脚本：

```bash
bash script/collect_runtime_libs.sh
```

收集 `bin/KVServerNode` 的依赖，并复制到 `deploy/runtime/lib/`，供镜像构建时使用。


### 3. 启动集群

在`deploy`中提供了config示例：

- `deploy/config/config-node1.yml`
- `deploy/config/config-node2.yml`
- `deploy/config/config-node3.yml`


```bash
mkdir -p deploy/data/node1 deploy/data/node2 deploy/data/node3
mkdir -p deploy/logs/node1 deploy/logs/node2 deploy/logs/node3
docker compose up
```

## 客户端测试

测试程序位于 `test/KVClientTest/main.cpp`，是一个简单的交互式 CLI。

```bash
./bin/kv_client_test
```

默认连接地址为：

```text
127.0.0.1:50001
127.0.0.1:50002
127.0.0.1:50003
```

支持的命令：

```text
put <key> <value>
get <key>
remove <key>
q
```

示例：

```text
put name raft
get name
remove name
q
```
