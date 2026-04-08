# 以图搜商品系统 — K8s 生产部署方案 v1.0

> 编写日期: 2026-04-05, 更新: 2026-04-06
> 适用版本: v0.3.x
> 前置条件: 多 Collection 架构已验证、分布式 Milvus Docker 测试通过
> K8s 方案: K3s 轻量级 Kubernetes
> 集群规模: 4 台同配服务器

---

## 一、集群规划

### 1.1 硬件规格 (4 台同配)

| 节点 | 角色 | CPU | 内存 | GPU | 存储 |
|------|------|-----|------|-----|------|
| Node 1 | Master+Worker | 256c | 2TB | 4×4090D | 4TB NVMe |
| Node 2 | Master+Worker | 256c | 2TB | 4×4090D | 4TB NVMe |
| Node 3 | Master+Worker | 256c | 2TB | 4×4090D | 4TB NVMe |
| Node 4 | Worker | 256c | 2TB | 4×4090D | 4TB NVMe |

### 1.2 资源总计

| 类别 | CPU请求 | CPU限制 | 内存请求 | 内存限制 |
|------|---------|---------|---------|---------|
| 应用服务 | 32c | 64c | 88Gi | 128Gi |
| Milvus集群 | 55c | 110c | 250Gi | 500Gi |
| 基础设施 | 39c | 82c | 80Gi | 168Gi |
| **总计** | **126c** | **256c** | **418Gi** | **796Gi** |

---

## 二、全组件 K8s 部署类型

| 组件 | K8s 资源 | 副本 | 说明 |
|------|---------|------|------|
| inference-service | Deployment | 4 | GPU亲和, 每Pod 1卡 |
| search-service | Deployment + HPA | 3-10 | 无状态, 自动扩缩 |
| write-service | Deployment | 2-3 | 无状态 |
| bitmap-filter | StatefulSet | 2 | 主从, RocksDB WAL复制 |
| cron-scheduler | Deployment | 2 | etcd选举, 单活 |
| Milvus | Helm (Operator) | 分布式 | proxy+coord+node |
| PostgreSQL | StatefulSet | 3 | Patroni HA |
| Redis | StatefulSet | 3 | Sentinel |
| Kafka | StatefulSet | 3 | KRaft |
| etcd | StatefulSet | 3 | 已有集群迁移 |
| MinIO | StatefulSet | 4 | Erasure Code |

---

## 三、Milvus 分布式 (Helm)

```bash
helm repo add milvus https://zilliztech.github.io/milvus-helm/
helm install milvus-cluster milvus/milvus -f milvus-cluster-values.yaml -n milvus
```

关键配置:
- `queryNode.replicas: 3` (各128GB, 加载热区索引)
- `indexNode.replicas: 4` (并行建索引, 速度4x)
- `dataNode.replicas: 2` (写入处理)
- 复用外部 etcd + MinIO + Kafka

---

## 四、Inference GPU 集群

```yaml
# 每 Pod 绑定 1 张 GPU
resources:
  limits:
    nvidia.com/gpu: 1
    memory: 12Gi
replicas: 4  # 4×4090D = 4 Pod
```

Service 自动负载均衡, 推理吞吐 4x。

---

## 五、PostgreSQL HA (Patroni)

```yaml
kind: StatefulSet
replicas: 3  # Primary + 2 Standby
# Patroni + etcd 协调自动故障转移
# Service: postgres-primary (读写) + postgres-replica (只读)
```

---

## 六、Redis Sentinel

```yaml
kind: StatefulSet
replicas: 3  # 每 Pod 内置 redis-server + redis-sentinel
```

---

## 七、Kafka KRaft 集群

```yaml
kind: StatefulSet
replicas: 3  # broker + controller 合一
# KRaft 模式, 无需 ZooKeeper
```

---

## 八、bitmap-filter 主从

```yaml
kind: StatefulSet
replicas: 2  # Pod-0=Primary, Pod-1=Secondary
# RocksDB WAL 同步, etcd leader election
# PVC: 50Gi fast-nvme
```

---

## 九、HPA 自动扩缩容

```yaml
# search-service 示例
minReplicas: 3
maxReplicas: 10
metrics:
- type: Resource
  resource:
    name: cpu
    target:
      averageUtilization: 70
```

---

## 十、监控告警

关键指标:
- `search_latency_p99 < 400ms`
- `milvus_query_latency_p99 < 250ms`
- `pg_replication_lag_bytes < 100MB`
- `kafka_consumer_lag < 10000`
- `etcd_server_has_leader == 1`

---

## 十一、实施路线图

| 阶段 | 周期 | 内容 |
|------|------|------|
| 第一阶段 | 2周 | K8s集群 + etcd/MinIO/PG/Redis/Kafka |
| 第二阶段 | 2周 | Milvus分布式 + 4GPU推理集群 |
| 第三阶段 | 1-2周 | 应用服务 + bitmap主从 + HPA |
| 第四阶段 | 持续 | 混沌测试 + 性能调优 + 灰度上线 |

---

## 十二、为什么生产必须用 K8s 而非 Docker

### 12.1 Docker 单机分布式的局限

| 能力 | Docker Compose/Run | K8s |
|------|-------------------|-----|
| 崩溃自动恢复 | 基础 (`restart: always`) | 智能 (探针+重调度) |
| 自动扩缩容 | ❌ 手动改 replicas | ✅ HPA 按 CPU/QPS 自动 |
| 滚动更新 | ❌ 停服更新 | ✅ 零停机 |
| 跨机故障转移 | ❌ 单机挂全挂 | ✅ 自动调度到其他节点 |
| 健康检查 | 有限 | ✅ liveness+readiness+startup |
| 资源隔离 | 基础 cgroup | ✅ 精确到 CPU/内存/GPU |
| 密钥管理 | ❌ 明文环境变量 | ✅ Secret + 加密存储 |
| 服务发现 | Docker DNS | ✅ CoreDNS + Service mesh |
| 日志采集 | 手动 | ✅ 标准化 (Fluentd/Loki) |
| 配置管理 | 文件挂载 | ✅ ConfigMap + 热更新 |

### 12.2 Docker 部署的核心风险

| 风险 | 影响 | K8s 如何解决 |
|------|------|-------------|
| **单点故障** | 服务器宕机 → 全部服务不可用 | 多节点, Pod 自动迁移 |
| **资源争抢** | Milvus indexing 吃满 CPU → 推理服务饿死 | 资源配额 + QoS 等级 |
| **手动运维** | Milvus 崩了需人工 restart + 重提交索引 | 自动探测 + 自愈 |
| **无法横向扩展** | 流量高峰时 search-service 无法加实例 | HPA 秒级扩容 |
| **更新风险** | 更新镜像需停服 | 滚动更新, 金丝雀发布 |
| **数据安全** | 磁盘损坏 → 数据丢失 | PVC + 多副本 + 备份策略 |
| **网络隔离** | 所有容器共享网络, 无隔离 | NetworkPolicy + namespace |
| **GPU 利用率** | 手动绑定, 无法动态调度 | Device Plugin + 拓扑感知 |

### 12.3 Docker 开发测试的价值

Docker 分布式测试 **不是浪费**，而是通往生产的必经之路：

```
Docker 开发环境                              K8s 生产环境
─────────────                               ──────────────
✅ 验证组件间通信                    ──复用──→ Service 配置
✅ 调通 Milvus 分布式参数           ──复用──→ Helm values.yaml
✅ 验证多Collection查询逻辑         ──复用──→ 应用代码不变
✅ 发现内存/CPU 瓶颈               ──复用──→ resource requests/limits
✅ 确认数据迁移路径可行             ──复用──→ 相同 etcd+MinIO 路径
✅ 建立监控指标基线                 ──复用──→ PrometheusRule 阈值
```

**核心原则**：Docker 环境出的配置参数可以直接写入 K8s YAML，零返工。

---

## 十三、回退方案

| 场景 | 操作 | 回退时间 |
|------|------|---------|
| K8s 集群故障 | 保留 docker-compose.yml，切 DNS 到 Docker 环境 | < 30min |
| Milvus 分布式异常 | 启 standalone (同一 etcd+MinIO) | < 10min |
| PG Patroni 故障 | 停 Patroni，直接启原生 PG | < 5min |
| 灰度失败 | Ingress 切流量回旧版本 | < 1min |

**关键保障**：新旧环境并行运行至少 1 周，确认稳定后才下线旧环境。

---

## 十四、生产就绪检查清单

上线前必须通过以下检查：

### 基础设施
- [ ] K8s 集群 3 master + 6 worker 就绪
- [ ] GPU 节点 NVIDIA Device Plugin 正常
- [ ] 存储类 (StorageClass) 配置完成
- [ ] Ingress Controller 部署
- [ ] CoreDNS 解析正常

### 数据层
- [ ] etcd 3 节点集群, leader 选举正常
- [ ] PostgreSQL Patroni 故障转移测试通过 (手动 kill primary, standby 自动提升)
- [ ] Redis Sentinel 故障转移测试通过
- [ ] Kafka 3 Broker, topic replication factor = 3
- [ ] MinIO Erasure Code, 模拟 1 节点故障仍可读写
- [ ] Milvus 分布式所有 Collection 可 load + 查询

### 应用层
- [ ] search-service HPA 扩缩测试 (压测 → 自动扩到 5+ → 回落)
- [ ] inference-service 4 GPU Pod 均正常响应
- [ ] write-service 写入 → Milvus + PG 数据一致
- [ ] bitmap-filter 主从同步延迟 < 100ms

### 可观测性
- [ ] Prometheus 采集所有组件 metrics
- [ ] Grafana 仪表盘: 搜索延迟/QPS/GPU利用率/PG复制延迟
- [ ] 告警规则: 搜索P99 > 400ms, PG lag > 100MB, etcd no leader
- [ ] 日志聚合: 所有 Pod 日志可查

### 容灾
- [ ] 混沌测试: 随机杀 Pod, 验证自动恢复
- [ ] etcd snapshot 定时备份 (每小时)
- [ ] PG PITR 备份 (每日全量 + 连续 WAL)
- [ ] 回退演练: K8s → Docker 切换 < 30min

---

## 十五、K3s 一键部署 (脚本+配置全集)

### 15.1 文件清单

所有文件位于 `deploy/k3s/` 目录:

```
deploy/k3s/
├── install.sh                          # K3s 集群安装脚本 (527行)
├── deploy-all.sh                       # 全量服务部署脚本 (375行)
└── k8s-manifests/                      # K8s YAML (18文件, 2448行)
    ├── namespace.yaml                  # 3 个命名空间
    ├── postgres.yaml                   # StatefulSet ×3 (Patroni)
    ├── redis.yaml                      # StatefulSet ×3 + Sentinel
    ├── kafka.yaml                      # StatefulSet ×3 (KRaft)
    ├── minio.yaml                      # StatefulSet ×4 (Erasure Code)
    ├── clickhouse.yaml                 # StatefulSet ×1
    ├── milvus-etcd.yaml                # StatefulSet ×3 (独立 etcd)
    ├── milvus.yaml                     # 分布式 Milvus 全组件
    ├── inference.yaml                  # Deployment ×4 (GPU 调度)
    ├── search-service.yaml             # Deployment + HPA (3-5)
    ├── write-service.yaml              # Deployment + HPA (2-3)
    ├── bitmap-filter.yaml              # StatefulSet ×2 (RocksDB)
    ├── cron-scheduler.yaml             # Deployment ×2
    ├── web-console.yaml                # Deployment ×2
    ├── prometheus.yaml                 # StatefulSet ×1
    ├── grafana.yaml                    # Deployment ×1
    ├── pipeline-monitor.yaml           # Deployment ×1
    └── ingress.yaml                    # Ingress 路由规则
```

### 15.2 K3s 集群安装 (`install.sh`)

```bash
# Node 1 (第一台 master):
sudo ./install.sh init

# Node 2, 3 (加入为 master):
sudo ./install.sh join-master <Node1-IP>

# Node 4 (加入为 worker):
sudo ./install.sh join-worker <Node1-IP>

# 查看集群状态:
./install.sh status

# 卸载 (如需要):
sudo ./install.sh uninstall
```

install.sh 自动完成:
- 先决条件检查 (Ubuntu 24, Docker, NVIDIA 驱动)
- K3s 安装 (embedded etcd HA 模式)
- NVIDIA Container Toolkit + containerd runtime 配置
- NVIDIA device plugin 安装 (GPU 调度)
- Helm 安装
- kubeconfig 配置

### 15.3 全量服务部署 (`deploy-all.sh`)

```bash
# 部署全部 (4 阶段, 自动等待每阶段就绪):
sudo ./deploy-all.sh

# 按阶段部署:
sudo ./deploy-all.sh --phase 1    # 基础设施 (PG/Redis/Kafka/MinIO/etcd)
sudo ./deploy-all.sh --phase 2    # Milvus 分布式集群
sudo ./deploy-all.sh --phase 3    # 业务服务 (inference/search/write/...)
sudo ./deploy-all.sh --phase 4    # 监控 (Prometheus/Grafana/monitor)

# 查看状态:
sudo ./deploy-all.sh --status

# 删除全部 (危险):
sudo ./deploy-all.sh --delete
```

### 15.4 服务部署详情

#### Phase 1: 基础设施 (`imgsrch-infra` 命名空间)

| 服务 | K8s 类型 | 副本 | 存储 | 关键配置 |
|------|---------|------|------|---------|
| PostgreSQL | StatefulSet | 3 | 100Gi PVC | Patroni 主从, liveness probe |
| Redis | StatefulSet | 3 | 10Gi PVC | Sentinel sidecar, maxmemory 4GB |
| Kafka | StatefulSet | 3 | 200Gi PVC | KRaft 模式, 无 ZooKeeper |
| MinIO | StatefulSet | 4 | 500Gi PVC | Erasure Code 跨节点 |
| ClickHouse | StatefulSet | 1 | 200Gi PVC | 分析存储 |
| etcd (Milvus) | StatefulSet | 3 | 20Gi PVC | 独立集群, 调优参数 |

#### Phase 2: Milvus 分布式 (`imgsrch-infra` 命名空间)

| 组件 | 类型 | 副本 | 内存 | 说明 |
|------|------|------|------|------|
| Proxy | Deployment | 2 | 4GB | 入口路由 |
| RootCoord | Deployment | 1 | 2GB | 元数据 |
| DataCoord | Deployment | 1 | 2GB | 数据调度 |
| QueryCoord | Deployment | 1 | 2GB | 查询调度 |
| IndexCoord | Deployment | 1 | 2GB | 索引调度 |
| DataNode | Deployment | 2 | 32GB | 写入处理 |
| QueryNode | Deployment | 3 | 128GB | 查询+索引加载 |
| IndexNode | Deployment | 4 | 32GB | 并行建索引 |

#### Phase 3: 业务服务 (`imgsrch-app` 命名空间)

| 服务 | 类型 | 副本 | GPU | 特殊配置 |
|------|------|------|-----|---------|
| inference | Deployment | 4 | 1×4090D/实例 | nodeSelector gpu=true, startupProbe 300s |
| search-service | Deployment+HPA | 3-5 | - | CPU 70% 触发扩缩 |
| write-service | Deployment+HPA | 2-3 | - | |
| bitmap-filter | StatefulSet | 2 | - | RocksDB PVC 100Gi |
| cron-scheduler | Deployment | 2 | - | |
| web-console | Deployment | 2 | - | |

#### Phase 4: 监控 (`imgsrch-monitor` 命名空间)

| 服务 | 类型 | 副本 | 配置 |
|------|------|------|------|
| Prometheus | StatefulSet | 1 | K8s 服务发现, 100Gi, RBAC |
| Grafana | Deployment | 1 | 自动配置 Prometheus 数据源 |
| pipeline-monitor | Deployment | 1 | 实时任务监控页 |
| Ingress | Ingress | - | /api/search, /api/write, /grafana, /monitor |

### 15.5 节点角色分配 (4 台同配服务器)

```
┌──────────────┬──────────────────────────────────────────────┐
│   Node 1     │ K3s Server (master) + etcd                   │
│ (当前开发机) │ Milvus Coord (5组件) + PostgreSQL Primary    │
│              │ Kafka-0 + etcd-0 + Redis-0                   │
│              │ GPU: inference ×4 (4090D ×4)                 │
│              │ Prometheus + Grafana                          │
├──────────────┼──────────────────────────────────────────────┤
│   Node 2     │ K3s Server (master) + etcd                   │
│              │ Milvus QueryNode ×2 + IndexNode ×2           │
│              │ PostgreSQL Standby + Kafka-1 + Redis-1       │
│              │ GPU: inference ×4                             │
├──────────────┼──────────────────────────────────────────────┤
│   Node 3     │ K3s Server (master) + etcd                   │
│              │ Milvus QueryNode ×1 + IndexNode ×2 + DataNode│
│              │ PostgreSQL Standby + Kafka-2 + Redis-2       │
│              │ GPU: inference ×4                             │
├──────────────┼──────────────────────────────────────────────┤
│   Node 4     │ K3s Agent (worker)                           │
│              │ Milvus DataNode + MinIO (Erasure Code)       │
│              │ search/write 扩展实例                         │
│              │ GPU: inference ×4                             │
└──────────────┴──────────────────────────────────────────────┘
```

### 15.6 执行步骤汇总

```bash
# ========== 第一阶段: 集群搭建 (4台机器各 SSH 执行) ==========

# Node 1:
scp -r deploy/k3s/ node1:~/deploy/k3s/
ssh node1 "cd ~/deploy/k3s && sudo ./install.sh init"
# 记录输出的 TOKEN 和 IP

# Node 2, 3:
ssh node2 "cd ~/deploy/k3s && sudo ./install.sh join-master <Node1-IP>"
ssh node3 "cd ~/deploy/k3s && sudo ./install.sh join-master <Node1-IP>"

# Node 4:
ssh node4 "cd ~/deploy/k3s && sudo ./install.sh join-worker <Node1-IP>"

# 验证:
ssh node1 "kubectl get nodes"
# 应该看到 4 个 Ready 节点, 每个有 nvidia.com/gpu: 4

# ========== 第二阶段: 服务部署 (在任意 master 上执行) ==========
ssh node1 "cd ~/deploy/k3s && sudo ./deploy-all.sh"
# 自动按 Phase 1→2→3→4 顺序部署, 约 15-30 分钟

# ========== 第三阶段: 数据迁移 ==========
# 将 Docker 环境中的数据迁移到 K8s:
# - Milvus: 同一 etcd+MinIO 路径, 自动识别
# - PostgreSQL: pg_dump → pg_restore
# - Redis: 无需迁移 (缓存)
# - Kafka: 无需迁移 (消息队列)

# ========== 第四阶段: 验证 ==========
kubectl get pods -A                    # 所有 Pod Running
kubectl top nodes                      # 资源使用
kubectl describe nodes | grep gpu      # GPU 分配
curl http://<INGRESS_IP>/api/search/healthz  # 搜索服务
```

### 15.7 NodePort 端口映射 (52000 起点)

> K8s 服务通过 NodePort 暴露到宿主机，使用 52000 起始段，与 Docker 原始端口共存。
> K3s 启动参数需加: `--service-node-port-range=52000-52399`

#### Docker ↔ K8s 端口对照

| 服务 | Docker (现有) | K8s NodePort | 容器内端口 | 协议 |
|------|:---:|:---:|:---:|------|
| **基础设施** | | | | |
| MinIO API | 9000 | **52000** | 9000 | HTTP |
| MinIO Console | 9001 | **52001** | 9001 | HTTP |
| etcd client | 2379 | **52010** | 2379 | gRPC |
| PostgreSQL | 5432 | **52020** | 5432 | TCP |
| Redis | 6379 | **52030** | 6379 | TCP |
| Kafka | 29092 | **52040** | 9092 | TCP |
| ClickHouse HTTP | 8123 | **52050** | 8123 | HTTP |
| ClickHouse TCP | 9000 | **52051** | 9000 | TCP |
| **Milvus** | | | | |
| Milvus Proxy | 19530 | **52100** | 19530 | gRPC |
| Milvus metrics | 9091 | **52101** | 9091 | HTTP |
| **业务服务** | | | | |
| inference-service | 8090 | **52200** | 8090 | HTTP |
| search-service | 8080 | **52210** | 8080 | HTTP |
| write-service | 8081 | **52220** | 8081 | HTTP |
| cron-scheduler | 8082 | **52230** | 8082 | HTTP |
| bitmap-filter | 50051 | **52240** | 50051 | gRPC |
| web-console | 80 | **52250** | 3000 | HTTP |
| **监控** | | | | |
| Prometheus | 9099 | **52300** | 9090 | HTTP |
| Grafana | 3002 | **52310** | 3000 | HTTP |
| pipeline-monitor | 9800 | **52320** | 9800 | HTTP |

#### 域名规划

| 层级 | 域名格式 | 示例 | 说明 |
|------|---------|------|------|
| 外部访问 | `<service>.imagesearchsystemv3.com` | `search.imagesearchsystemv3.com` | 用户/客户端访问 |
| 内部运维 | `<service>.ops.imagesearchsystemv3.com` | `grafana.ops.imagesearchsystemv3.com` | 运维人员访问 |
| K8s 内部 | `<svc>.<ns>.svc.cluster.local` | `postgres.imgsrch-infra.svc.cluster.local` | Pod 间通信 |

#### 外部域名 → Ingress 路由

| 域名 | 后端服务 | NodePort (备用) |
|------|---------|----------------|
| `search.imagesearchsystemv3.com` | search-service:8080 | 52210 |
| `write.imagesearchsystemv3.com` | write-service:8081 | 52220 |
| `inference.imagesearchsystemv3.com` | inference-service:8090 | 52200 |
| `console.imagesearchsystemv3.com` | web-console:3000 | 52250 |
| `grafana.ops.imagesearchsystemv3.com` | grafana:3000 | 52310 |
| `monitor.ops.imagesearchsystemv3.com` | pipeline-monitor:9800 | 52320 |
| `prometheus.ops.imagesearchsystemv3.com` | prometheus:9090 | 52300 |
| `minio.ops.imagesearchsystemv3.com` | minio:9001 | 52001 |

#### K8s 内部 DNS (Pod 间通信)

```
# 应用服务连接基础设施 — 使用 K8s 内部 DNS, 原始端口:
MILVUS_HOST=milvus-proxy.imgsrch-infra.svc.cluster.local:19530
PG_DSN=postgresql://imgsrch:pass@postgres.imgsrch-infra.svc.cluster.local:5432/image_search
REDIS_URL=redis://redis.imgsrch-infra.svc.cluster.local:6379
KAFKA_BROKER=kafka.imgsrch-infra.svc.cluster.local:9092
INFERENCE_URL=http://inference-service.imgsrch-app.svc.cluster.local:8090

# 同命名空间内可省略后缀:
# search-service → write-service (同在 imgsrch-app):
#   http://write-service:8081
```

#### 访问示例

```bash
# --- 通过域名 (生产推荐, 需配置 DNS/hosts) ---

# 搜索服务
curl https://search.imagesearchsystemv3.com/healthz

# Grafana 监控
https://grafana.ops.imagesearchsystemv3.com

# 监控页
https://monitor.ops.imagesearchsystemv3.com

# --- 通过 NodePort (开发调试/DNS 未配置时) ---

# PostgreSQL
psql -h <任意节点IP> -p 52020 -U imgsrch -d image_search

# Redis
redis-cli -h <任意节点IP> -p 52030

# Milvus
python3 -c "from pymilvus import connections; connections.connect(host='<任意节点IP>', port='52100')"
```

#### DNS 配置

```bash
# 方案 A: 内网 DNS 服务器 (推荐)
# 在内网 DNS 添加 A 记录, 指向 K8s Ingress VIP 或任意节点 IP

# 方案 B: /etc/hosts (快速验证)
echo "<Node1-IP>  search.imagesearchsystemv3.com write.imagesearchsystemv3.com console.imagesearchsystemv3.com" >> /etc/hosts
echo "<Node1-IP>  grafana.ops.imagesearchsystemv3.com monitor.ops.imagesearchsystemv3.com" >> /etc/hosts

# 方案 C: 通配符域名 (最省事)
# DNS: *.imagesearchsystemv3.com → <Ingress-IP>
# 所有子域名自动解析, Ingress 按 Host 头路由
```

#### 注意事项

1. **Pod 间通信**: 使用 K8s 内部 DNS (`<svc>.<ns>.svc.cluster.local`) + 原始端口，不走 NodePort
2. **外部访问**: 优先通过 Ingress + 域名 (80/443)，NodePort (52xxx) 作为备用/调试方案
3. Headless Service 保持 `ClusterIP: None`，不暴露 NodePort
4. 详细端口文档见 `deploy/k3s/PORT_MAP.md`
