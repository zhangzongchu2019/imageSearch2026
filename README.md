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

## 测试

覆盖 65 个业务场景, 62 个测试文件, 379 个测试用例。

```bash
# 按服务运行单元测试 (无需外部依赖)
make test-write          # write-service (15 文件, 74 用例)
make test-search         # search-service (27 文件, 202 用例)
make test-lifecycle      # cron-scheduler (8 文件, 35 用例)
make test-flink          # flink-pipeline (5 文件, 28 用例)
make test-bitmap         # bitmap-filter-service (5 文件, 35 用例)

# 全部单元测试
make test-all

# 集成测试 (需 docker-compose 基础设施)
make test-write-integ
make test-search-integ
make test-lifecycle-integ
make test-flink-integ
make test-bitmap-integ

# 跨服务端到端测试 (需全部服务运行)
make test-e2e

# 全部集成 + E2E
docker-compose up -d && sleep 30 && make test-all-integ
```

| 服务 | 单元测试 | 集成测试 | E2E |
|------|---------|---------|-----|
| write-service | 74 | 6 | 2 |
| search-service | 202 | 8 | — |
| cron-scheduler | 35 | 8 | 2 |
| flink-pipeline | 28 | 5 | — |
| bitmap-filter-service | 35 | 5 | — |
| 跨服务 E2E | — | — | 5 |

## 项目结构

```
image-search-platform/
├── proto/                       # gRPC Protobuf 定义
├── config/                      # 共享配置
├── services/
│   ├── search-service/          # Python 检索服务
│   │   └── tests/               #   单元 + 集成测试
│   ├── write-service/           # Python 写入服务
│   │   └── tests/               #   单元 + 集成测试
│   ├── bitmap-filter-service/   # Java Bitmap 过滤服务
│   │   └── src/test/            #   JUnit 5 测试
│   ├── flink-pipeline/          # Java Flink 管道
│   │   └── src/test/            #   JUnit 5 测试
│   └── cron-scheduler/          # Python 定时调度
│       └── tests/               #   单元 + 集成测试
├── tests/
│   └── e2e/                     # 跨服务端到端测试
├── deploy/
│   ├── kubernetes/              # K8s 部署清单
│   └── scripts/                 # 运维脚本
└── docs/                        # 文档
```
