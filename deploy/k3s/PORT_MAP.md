# NodePort 端口映射表

> K8s 服务通过 NodePort 暴露到宿主机，使用 52000 起始段，与 Docker 原始端口共存。
> Docker 使用原始端口，K8s 使用 52xxx 端口。

## 基础设施 (52000-52099)

| NodePort | 服务            | 容器端口 | 协议  | 命名空间        | YAML 文件         |
|----------|----------------|---------|-------|----------------|-------------------|
| 52000    | MinIO API      | 9000    | HTTP  | imgsrch-infra  | minio.yaml        |
| 52001    | MinIO Console  | 9001    | HTTP  | imgsrch-infra  | minio.yaml        |
| 52010    | etcd client    | 2379    | gRPC  | imgsrch-infra  | milvus-etcd.yaml  |
| 52020    | PostgreSQL     | 5432    | TCP   | imgsrch-infra  | postgres.yaml     |
| 52030    | Redis          | 6379    | TCP   | imgsrch-infra  | redis.yaml        |
| 52040    | Kafka          | 9092    | TCP   | imgsrch-infra  | kafka.yaml        |
| 52050    | ClickHouse HTTP| 8123    | HTTP  | imgsrch-infra  | clickhouse.yaml   |
| 52051    | ClickHouse TCP | 9000    | TCP   | imgsrch-infra  | clickhouse.yaml   |

## Milvus (52100-52199)

| NodePort | 服务               | 容器端口 | 协议  | 命名空间        | YAML 文件     |
|----------|-------------------|---------|-------|----------------|---------------|
| 52100    | Milvus Proxy gRPC | 19530   | gRPC  | imgsrch-infra  | milvus.yaml   |
| 52101    | Milvus metrics    | 9091    | HTTP  | imgsrch-infra  | milvus.yaml   |

## 业务服务 (52200-52299)

| NodePort | 服务               | 容器端口 | 协议  | 命名空间       | YAML 文件            |
|----------|--------------------|---------|-------|---------------|---------------------|
| 52200    | inference-service  | 8090    | HTTP  | imgsrch-app   | inference.yaml      |
| 52210    | search-service     | 8080    | HTTP  | imgsrch-app   | search-service.yaml |
| 52220    | write-service      | 8081    | HTTP  | imgsrch-app   | write-service.yaml  |
| 52230    | cron-scheduler     | 8082    | HTTP  | imgsrch-app   | cron-scheduler.yaml |
| 52240    | bitmap-filter gRPC | 50051   | gRPC  | imgsrch-app   | bitmap-filter.yaml  |
| 52250    | web-console        | 3000    | HTTP  | imgsrch-app   | web-console.yaml    |

## 监控 (52300-52399)

| NodePort | 服务              | 容器端口 | 协议  | 命名空间          | YAML 文件              |
|----------|------------------|---------|-------|------------------|----------------------|
| 52300    | Prometheus       | 9090    | HTTP  | imgsrch-monitor  | prometheus.yaml      |
| 52310    | Grafana          | 3000    | HTTP  | imgsrch-monitor  | grafana.yaml         |
| 52320    | pipeline-monitor | 9800    | HTTP  | imgsrch-monitor  | pipeline-monitor.yaml|

## 访问示例

```bash
# 宿主机 IP 访问 (假设 NODE_IP=192.168.1.100)

# MinIO Console
curl http://192.168.1.100:52001

# PostgreSQL
psql -h 192.168.1.100 -p 52020 -U imgsrch -d image_search

# Redis
redis-cli -h 192.168.1.100 -p 52030

# Milvus gRPC
# pymilvus: connections.connect(host="192.168.1.100", port="52100")

# Search API
curl http://192.168.1.100:52210/healthz

# Grafana
curl http://192.168.1.100:52310

# Prometheus
curl http://192.168.1.100:52300
```

## 注意事项

1. K3s 默认 NodePort 范围是 30000-32767，需要修改 K3s 启动参数以支持 52xxx 端口段:
   ```bash
   # /etc/systemd/system/k3s.service 中添加:
   ExecStart=/usr/local/bin/k3s server --service-node-port-range=52000-52399
   ```
2. Headless Service (如 minio-headless, redis-headless 等) 保持 ClusterIP: None，不暴露 NodePort
3. Pod 间通信继续使用 ClusterIP (原始端口)，NodePort 仅用于宿主机外部访问
