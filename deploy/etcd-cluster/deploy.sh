#!/bin/bash
# 部署 3 节点 etcd 集群
# 使用独立 Docker 容器, 同一网络 (sandbox_default)

set -eo pipefail
NETWORK="sandbox_default"
ETCD_IMAGE="quay.io/coreos/etcd:v3.5.12"
CLUSTER_TOKEN="milvus-etcd-cluster"

echo "$(date '+%Y-%m-%d %H:%M:%S') 部署 etcd 3 节点集群..."

# 清理旧容器
for i in 1 2 3; do
    docker rm -f etcd-node-${i} 2>/dev/null || true
done

# 集群初始配置
INITIAL_CLUSTER="etcd-node-1=http://etcd-node-1:2380,etcd-node-2=http://etcd-node-2:2380,etcd-node-3=http://etcd-node-3:2380"

for i in 1 2 3; do
    echo "启动 etcd-node-${i}..."
    docker run -d \
        --name etcd-node-${i} \
        --network ${NETWORK} \
        --memory=2g \
        -e ETCD_AUTO_COMPACTION_RETENTION=1 \
        -e ETCD_QUOTA_BACKEND_BYTES=4294967296 \
        -e ETCD_HEARTBEAT_INTERVAL=500 \
        -e ETCD_ELECTION_TIMEOUT=5000 \
        -e ETCD_MAX_REQUEST_BYTES=10485760 \
        -e ETCD_SNAPSHOT_COUNT=50000 \
        -v etcd-cluster-${i}:/etcd-data \
        ${ETCD_IMAGE} etcd \
        --name etcd-node-${i} \
        --data-dir /etcd-data \
        --initial-advertise-peer-urls http://etcd-node-${i}:2380 \
        --listen-peer-urls http://0.0.0.0:2380 \
        --advertise-client-urls http://etcd-node-${i}:2379 \
        --listen-client-urls http://0.0.0.0:2379 \
        --initial-cluster ${INITIAL_CLUSTER} \
        --initial-cluster-state new \
        --initial-cluster-token ${CLUSTER_TOKEN} \
        --max-txn-ops=25000
done

echo "等待集群启动 (10s)..."
sleep 10

# 验证集群健康
echo "=== 集群健康检查 ==="
docker exec etcd-node-1 etcdctl \
    --endpoints=http://etcd-node-1:2379,http://etcd-node-2:2379,http://etcd-node-3:2379 \
    endpoint health

echo "=== 集群成员 ==="
docker exec etcd-node-1 etcdctl \
    --endpoints=http://etcd-node-1:2379 \
    member list

echo "$(date '+%Y-%m-%d %H:%M:%S') etcd 集群部署完成"
