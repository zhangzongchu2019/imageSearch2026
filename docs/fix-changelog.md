# 代码修复变更日志

**修复日期**: 2026-02-11
**基于**: 10 份外部评审交叉核实后的确认修复清单 (29 项)

---

## 修复统计

| 指标 | 数值 |
|------|------|
| P1 修复 | 14/14 ✅ |
| P2 修复 | 4/15 (代码级) ✅ |
| 修改文件 | 18 个 |
| 净增行数 | +280 行 (3475→3755) |
| 涉及服务 | 6 个 (search/write/bitmap-filter/flink/cron/deploy) |

---

## P1 修复明细

### search-service (Python)

| FIX | 文件 | 变更 |
|-----|------|------|
| **FIX-C** | routes.py | readyz 增加运行时依赖检查 (Redis ping / Milvus connection / model loaded) |
| **FIX-D** | metrics.py + pipeline.py | error_code_total 增加 `source` 维度; pipeline 错误点标注 source=milvus/gpu |
| **FIX-E** | pipeline.py | 空结果终极兜底: 所有召回路径为空时, 查询常青池 evergreen 推荐 |
| **FIX-F** | schemas.py + pipeline.py | 新增 EffectiveParamsSnapshot 模型; SearchMeta 返回实际生效的 ef_search/time_range 等参数 |
| **FIX-G** | degrade_fsm.py + pipeline.py + schemas.py | FSM 保存 last_reason; PipelineContext 传播; SearchMeta 增加 degrade_reason 字段 |

### write-service (Python)

| FIX | 文件 | 变更 |
|-----|------|------|
| **FIX-A** | update_image.py | PushUpdate 改为 2 次重试 + 超时递增; 双失败写补偿事件到 Kafka topic `bitmap-push-compensate` |
| **FIX-B** | update_image.py | 记录 T0 (write_accepted) / T1 (milvus_upsert) / T2 (push_complete) 端到端时间戳 |

### bitmap-filter-service (Java)

| FIX | 文件 | 变更 |
|-----|------|------|
| **FIX-V** | BitmapFilterHandler.java + BitmapFilterApplication.java | 新增 shutdown() 方法关闭 multiGetPool; shutdown hook 增加 handler.shutdown() |
| **FIX-W** | RocksDBStore.java | close() 后置 db/cfRolling/cfEvergreen = null, 使 isOpen() 正确返回 false |
| **FIX-X** | CdcKafkaConsumer.java | 注入 KafkaTemplate; 处理失败发送到 `bitmap-cdc-dlq` topic; 增加 DLQ 发送指标 |

### flink-pipeline (Java)

| FIX | 文件 | 变更 |
|-----|------|------|
| **FIX-AA** | MerchantDictEncoder.java | 裸 DriverManager 替换为 HikariCP 连接池 (min=1, max=3); 新增 close() 方法 |
| **FIX-AB** | BitmapPostgresqlSink.java | PG commit 后发起 best-effort gRPC PushUpdate; open() 初始化 gRPC channel; close() 关闭 channel |

### 部署配置

| FIX | 文件 | 变更 |
|-----|------|------|
| **FIX-T** | search-service.yaml | HPA 指标从 counter `image_search_requests_total` 改为 rate `image_search_requests_per_second` |
| **FIX-U** | pg_init.sql | api_keys 表 DDL 统一为 api_key_hash CHAR(64) PRIMARY KEY, 对齐 init_db.sql |

---

## P2 修复明细

| FIX | 文件 | 变更 |
|-----|------|------|
| **FIX-Y** | main.py + pg_init.sql | partition_rotation 分步状态记录; 失败写补偿表 `partition_rotation_compensation`; 新增 DDL |
| **FIX-Z** | bitmap_reconcile.py | 对账增加 RocksDB 侧 gRPC HealthCheck 调用, 对比 PG 与 RocksDB 统计 |
| **FIX-AC** | MerchantEventDeserializer.java | 反序列化失败记录完整消息 (前 500 字节) + 失败计数器, 供 DLQ 回放 |
| **FIX-AD** | Makefile | 新增 `proto` 目标, 自动编译 bitmap_filter.proto → Python/Java stubs; `build` 依赖 `proto` |

---

## 未修复的 P2 项 (配置/流程类, 非代码阻断)

| FIX | 描述 | 原因 |
|-----|------|------|
| FIX-H | bitmap push 版本控制 | 需产品定义版本号语义 |
| FIX-I | 配置审计日志 | 需运维平台集成 |
| FIX-K | Kafka consumer lag 监控 | 需 Grafana dashboard 配置 |
| FIX-L | 限流内部状态暴露 | 低优先级 admin API |
| FIX-M | image_pk 跨语言契约测试 | ✅ test_degrade_fsm.py TestMurmurhashRampConsistency 覆盖跨语言一致性 |
| FIX-N | proto 向后兼容性验证 | 需引入 buf 工具链 |
| FIX-O | 集成测试 docker-compose | ✅ Makefile 13 个 test target + 62 测试文件全覆盖 |
| FIX-P | golden dataset | 需业务方提供标注数据 |
| FIX-Q | 商家级保护策略 | 需产品定义优先级规则 |
| FIX-R | A/B experiment 路由 | 需产品定义实验框架 |
| FIX-S | RocksDB compaction 策略 | 需压测确定参数 |
