# 以图搜商品系统 · Image Search Platform

> **版本**: 系统设计 v1.2 · BRD v4.0.1 对齐  
> **架构**: 两区 HNSW+DiskANN · 双路径延迟模型 · 五级降级

## 服务矩阵

| 服务 | 语言 | 框架 | 端口 | 职责 |
|------|------|------|------|------|
| **search-service** | Python 3.11 | FastAPI + uvicorn | 8080 | 检索主链路 + 特征提取 (GPU) |
| **write-service** | Python 3.11 | FastAPI + uvicorn | 8081 | 写入主链路 + 去重 + 事件发布 |
| **bitmap-filter-service** | Java 21 | Spring Boot 3.4 + gRPC | 50051 | 商家 Bitmap 过滤 (内嵌 RocksDB) |
| **flink-pipeline** | Java 21 | Flink 2.0 | — | 商家关联事件聚合 → PG |
| **cron-scheduler** | Python 3.11 | APScheduler | 8082 | 分区轮转 / 常青治理 / 对账 |

## 两区架构 (v1.2)

```
┌─────────────────────┐  ┌─────────────────────────┐
│  热区 HNSW (近5月)   │  │  非热区 DiskANN (6-18月) │
│  ~7.5 亿向量         │  │  ~22 亿向量              │
│  M=24, ef=192       │  │  MD=64, SL=200          │
│  内存常驻            │  │  NVMe 磁盘               │
│  99.99% SLA         │  │  99.5% SLA               │
│  ~80% 查询 (快路径)  │  │  ~20% 查询 (级联路径)    │
└─────────────────────┘  └─────────────────────────┘
```

## 快速启动

```bash
# 开发环境
docker-compose -f docker-compose.dev.yml up -d

# 生产构建
docker-compose build
docker-compose up -d
```

## 项目结构

```
image-search-platform/
├── proto/                       # gRPC Protobuf 定义
├── config/                      # 共享配置
├── services/
│   ├── search-service/          # Python 检索服务
│   ├── write-service/           # Python 写入服务
│   ├── bitmap-filter-service/   # Java Bitmap 过滤服务
│   ├── flink-pipeline/          # Java Flink 管道
│   └── cron-scheduler/          # Python 定时调度
├── deploy/
│   ├── kubernetes/              # K8s 部署清单
│   └── scripts/                 # 运维脚本
└── docs/                        # 文档
```
