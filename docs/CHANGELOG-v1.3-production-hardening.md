# 以图搜商品系统 · v1.3 生产加固 Changelog

> 对齐版本: BRD v4.0.1 / 系统设计 v1.3 / 技术架构 v1.3
> 日期: 2026-02-11

---

## 一、变更概览

| # | 修复项 | 风险等级 | 涉及服务 | 状态 |
|---|--------|---------|---------|------|
| 1 | 连接初始化指数退避重试 | P0 | search / write | ✅ |
| 2 | Milvus 有界线程池 | P0 | search | ✅ |
| 3 | Flush 异常捕获 + 超时保护 | P0 | write | ✅ |
| 4 | 分布式锁 Watchdog 续约 | P0 | write | ✅ |
| 5 | gRPC Channel 生命周期管理 | P1 | search | ✅ |
| 6 | API Key SHA256 认证 | P1 | search | ✅ |
| 7 | Write 服务认证 | Low | — | ⏭ 跳过 (内网) |
| 8 | Milvus expr 注入防护 | Low | — | ⏭ 跳过 (SHA256 hex) |
| 9 | 事务写入 + 补偿日志 | P0 | write | ✅ |
| 10 | BitmapFilterPushClient 单例 | P1 | write | ✅ |
| 11 | Flink HikariCP 连接池 | P1 | flink-pipeline | ✅ |
| 12 | W3C TraceContext 传播 | P1 | write / config | ✅ |
| 13 | 实时 Prometheus 指标 + 动态阈值 | P1 | search | ✅ |
| 14 | 集成测试 (10 cases) | P1 | search (tests) | ✅ |
| 15 | 单元测试 (20 cases) | P1 | search (tests) | ✅ |
| — | ConfigService 统一配置层 | 基础设施 | search / write | ✅ |
| — | HealthChecker PG↔RocksDB 差值检测 | 增强 | bitmap-filter | ✅ |
| — | production.yaml v1.3 | 配置 | 全局 | ✅ |

---

## 二、核心修复详解

### FIX #1: 连接初始化指数退避重试

**问题**: 服务启动时若 Redis/PG/Milvus/Kafka 任一不可用, 进程直接崩溃, 无法自愈。

**方案**: `_retry_with_backoff()` 通用重试器 — 最多 5 次, 指数退避 1s→30s 封顶。

**文件**:
- `search-service/app/core/lifecycle.py` — Milvus / Redis / PG / Kafka 初始化
- `write-service/app/core/dependencies.py` — 同上

**配置** (`production.yaml`):
```yaml
reliability:
  init_retry:
    max_retries: 5
    base_delay_s: 1.0
    max_delay_s: 30.0
```

### FIX #2: Milvus 有界线程池

**问题**: `run_in_executor(None, ...)` 使用默认全局线程池, 无队列上限, 高并发下线程爆炸。

**方案**: 专用 `ThreadPoolExecutor(max_workers=32)`, 注入 `MilvusSearchClient`, 所有同步 Milvus SDK 调用经此池。

**文件**:
- `search-service/app/core/lifecycle.py` — 创建 executor
- `search-service/app/infra/milvus_client.py` — 构造函数接收 executor

### FIX #3: Flush 异常捕获 + 超时保护

**问题**: `asyncio.create_task(_do_flush())` fire-and-forget, 异常静默丢失。

**方案**: `await asyncio.wait_for(_do_flush(), timeout=5.0)` + try-except 显式日志。

**文件**: `write-service/app/api/update_image.py` → `_safe_incr_flush_counter()`

### FIX #4: 分布式锁 Watchdog 续约

**问题**: 锁 TTL 30s, 但图片下载+推理可能超过 30s, 锁过期导致并发写入。

**方案**: `_LockWatchdog` 类 — 每 TTL/3 秒原子续约 (Lua 脚本验证锁所有权), 任务完成或丢锁时停止。

**文件**: `write-service/app/api/update_image.py` → `class _LockWatchdog`

### FIX #5: gRPC Channel 生命周期管理

**问题**: 长期运行的 search-service 从不关闭 gRPC channel, FD 泄漏。

**方案**: `BitmapFilterGrpcClient.close()` + `Lifecycle.shutdown()` 调用。

**文件**:
- `search-service/app/infra/bitmap_grpc_client.py` → `async def close()`
- `search-service/app/core/lifecycle.py` → shutdown 阶段

### FIX #6: API Key SHA256 认证

**问题**: 仅前缀匹配, 安全性不足。

**方案**: `SHA256(api_key)` → 与配置服务中存储的 hash CSV 比对。开发环境保留前缀降级。

**文件**: `search-service/app/api/routes.py` → `_verify_api_key()`

### FIX #9: 事务写入 + 补偿日志

**问题**: Milvus upsert → PG dedup → Redis cache 三步非原子, 中途失败导致不一致。

**方案**:
- PG 写入包在 `async with conn.transaction()` 中
- 记录 `steps_done` 列表, 失败时写补偿日志到 Redis (`compensation:{image_pk}:{trace_id}`, TTL 24h)
- Milvus upsert 幂等 (相同 PK 覆盖), PG 用 `ON CONFLICT DO NOTHING`

**文件**: `write-service/app/api/update_image.py` → `_process_new_image()`

### FIX #10: BitmapFilterPushClient 单例

**问题**: 每次请求新建 gRPC channel, 连接风暴。

**方案**: `init_dependencies()` 中创建一次, 存入 `deps["bitmap_push_client"]`。

**文件**: `write-service/app/core/dependencies.py` → `class BitmapFilterPushClient`

### FIX #11: Flink HikariCP 连接池

**问题**: `BitmapPostgresqlSink` 单 JDBC 连接, 无重连, 无池化。

**方案**: HikariCP 连接池 (min=2, max=5, 泄漏检测 10s)。

**文件**:
- `flink-pipeline/.../BitmapPostgresqlSink.java` — HikariDataSource
- `flink-pipeline/pom.xml` — 新增 `com.zaxxer:HikariCP:5.1.0`

### FIX #12: W3C TraceContext 传播

**问题**: 无法跨服务关联请求, 排障困难。

**方案**: 入口生成 32-hex trace_id, Kafka 消息头携带 `traceparent: 00-{trace_id}-{span_id}-01`。

**文件**:
- `write-service/app/api/update_image.py` → `_gen_trace_id()`
- `config/production.yaml` → `tracing:` 配置段

### FIX #13: 实时 Prometheus 指标 + 动态阈值

**问题**: 降级 FSM 的 P99 和错误率读取硬编码为 (0.0, 0.0), 永远不触发降级。

**方案**:
- `_read_live_metrics()` 从 `prometheus_client.REGISTRY` 实时读取
- `_get_degrade_thresholds()` 优先从 ConfigService / Nacos 读取阈值, 支持不重启调整

**文件**:
- `search-service/app/core/lifecycle.py` → `_read_live_metrics()`
- `search-service/app/core/degrade_fsm.py` → `_get_degrade_thresholds()`

---

## 三、新增基础设施

### ConfigService 统一配置层

三级优先级: **ENV > Nacos > K8s ConfigMap > YAML > 默认值**

特性: Nacos 长轮询热更新、Secret 管理、变更监听器。

**文件**: `search-service/app/core/config_service.py` (同步复制到 write-service)

### HealthChecker PG↔RocksDB 差值检测

每 30s 对比 PG count 与 RocksDB `estimateKeyCount()`:
- 差值 >10K 连续 3 次 (90s) → 节点摘除 (`NOT_SERVING`)
- 差值 <5K 连续 3 次 → 自动恢复
- Prometheus: `bitmap.pg_rocksdb_diff`, `bitmap.node_ejected`

**文件**: `bitmap-filter-service/.../HealthChecker.java`, `RocksDBStore.java`

---

## 四、测试覆盖

### v1.3 加固测试 (30 cases, 原有)

| 类型 | 文件 | 用例数 |
|------|------|--------|
| 集成测试 | `search-service/tests/integration/test_integration.py` | 10 |
| 单元测试 | `search-service/tests/unit/test_production_fixes.py` | 20 |

需真实中间件: `INTEGRATION_TEST=true pytest tests/integration/ -v`

### 全量业务场景测试 (336 cases 全通过, v1.6.1 验证)

覆盖 **56 个业务场景**, 62 个测试文件, 5 个微服务全覆盖。

```bash
make test-all            # 全部单元测试 (无需外部依赖)
make test-all-integ      # 全部集成 + E2E (需 docker-compose)
```

| 服务 | 测试文件 | 实际通过用例 | 覆盖业务场景 |
|------|---------|-------------|-------------|
| search-service | 27 | **184** | 8-Stage Pipeline、熔断器、5态 FSM、Bitmap 3级降级、scope 解析、3级缓存、4路融合、精排、Tag 召回、级联/兜底、API 限流鉴权、Admin API、行为上报、搜索日志、配置服务 |
| write-service | 15 | **70** | 去重、下载重试、特征提取、常青分类、词表编码、Milvus upsert、PG 补偿、Bitmap 推送、锁 watchdog、Kafka 事件、端点处理器、全流程编排、flush 批次 |
| cron-scheduler | 8 | **28** | 分区轮转、URI 清理、Bitmap 对账、常青治理、Milvus compaction |
| flink-pipeline | 5 | **24** | Kafka 反序列化、窗口聚合、PG Sink、字典编码、MiniCluster 集成 |
| bitmap-filter-service | 5 | **30** | RocksDB 双列族、CDC 路由+DLQ、并行 MultiGet+AND、健康检查+弹出 |
| **合计** | **60** | **336** | **56 个业务场景** |

> **v1.6.1 修复记录** (2026-02-28):
> - search-service: 修复 16 项测试 (CircuitBreaker state 转换、SearchLogEmitter enum mock、METRICS 延迟初始化等)
> - write-service: 修复 10 项测试 (_check_dedup 返回值、httpx AsyncClient mock、pymilvus 导入等)
> - cron-scheduler: 修复 marshmallow 4.x 兼容性 (降级至 3.x)
> - flink-pipeline: Flink 2.0.0→1.20.0 降级 (源码使用 1.x API)、修正 connector 版本、添加 gRPC 依赖
> - bitmap-filter-service: 修复 6 处编译错误 (构造参数不匹配、multiGetAsList 签名、缺失依赖等)

---

## 五、部署检查清单

```
□ 部署配置后端 (K8s ConfigMap 或 Nacos)
□ 注入 Secrets (K8s Secret 或环境变量):
    IMGSRCH_SECRET_PG_PASSWORD
    IMGSRCH_SECRET_REDIS_PASSWORD
    IMGSRCH_SECRET_MILVUS_TOKEN
    IMGSRCH_SECRET_KAFKA_USER / KAFKA_PASSWORD
□ 生成 API Key SHA256 hash, 写入 api_keys.hashes
□ Flink pom.xml 已添加 HikariCP 5.1.0 依赖
□ 运行全部单元测试: make test-all
□ 运行集成测试: make test-all-integ
□ 运行 E2E 测试: make test-e2e
□ 监控 Prometheus 指标:
    bitmap.pg_rocksdb_diff / bitmap.node_ejected
    image_search_errors_total / image_search_requests_total
□ 验证 trace_id 出现在日志和 Kafka header 中
□ 模拟长写入 (>30s) 验证锁 Watchdog 续约
□ Kill Redis 后重启, 验证指数退避重连
□ 强制 PG 故障, 检查 Redis compensation:* 补偿日志
□ 通过 Nacos/ConfigMap 动态调整降级阈值, 验证无需重启生效
```

---

## 六、文件变更清单

| 文件 | 行数 | 操作 |
|------|------|------|
| search-service/app/core/config_service.py | 239 | 新增 |
| search-service/app/core/lifecycle.py | 401 | 修改 |
| search-service/app/core/degrade_fsm.py | 264 | 修改 |
| search-service/app/core/metrics.py | — | 修改 (11 新指标) |
| search-service/app/api/routes.py | — | 修改 |
| search-service/app/infra/milvus_client.py | — | 修改 |
| search-service/app/infra/bitmap_grpc_client.py | — | 修改 |
| write-service/app/api/update_image.py | 480 | 修改 |
| write-service/app/core/dependencies.py | 191 | 修改 |
| write-service/app/core/config_service.py | 239 | 新增 |
| bitmap-filter-service/.../HealthChecker.java | 209 | 修改 |
| bitmap-filter-service/.../RocksDBStore.java | — | 修改 |
| flink-pipeline/.../BitmapPostgresqlSink.java | 128 | 修改 |
| flink-pipeline/pom.xml | — | 修改 (+HikariCP) |
| config/production.yaml | 303 | 修改 (v1.3) |
| tests/unit/test_production_fixes.py | 236 | 新增 |
| tests/integration/test_integration.py | 227 | 新增 |
| write-service/tests/ (15 文件) | ~1,500 | 新增 (v1.6) |
| search-service/tests/ (14 新文件) | ~2,000 | 新增 (v1.6) |
| cron-scheduler/tests/ (8 文件) | ~600 | 新增 (v1.6) |
| flink-pipeline/src/test/ (5 文件) | ~500 | 新增 (v1.6) |
| bitmap-filter-service/src/test/ (5 文件) | ~700 | 新增 (v1.6) |
| tests/e2e/ (4 文件) | ~200 | 新增 (v1.6) |

**总计**: 17 文件 (v1.3) + 60 测试文件 (v1.6) + 9 文件修复 (v1.6.1), ~8,500 行

---

## 七、质量评分

| 维度 | v1.2 | v1.3 | 关键改进 |
|------|------|------|---------|
| 可靠性 | ★★☆☆☆ | ★★★★☆ | 重试/Watchdog/事务写入 |
| 安全性 | ★★☆☆☆ | ★★★★☆ | SHA256 认证/配置服务 |
| 可观测性 | ★★★☆☆ | ★★★★☆ | 实时指标/Trace/11 新 metric |
| 测试覆盖 | ★★☆☆☆ | ★★★★★ | 336 test cases 全通过 / 56 业务场景 / 5 服务全覆盖 |
| 运维就绪 | ★★★☆☆ | ★★★★☆ | 优雅关闭/动态阈值/自动恢复 |

---

## 八、后续规划

| 优先级 | 项目 | 说明 |
|--------|------|------|
| P2 | HDRHistogram 精确 P99 | 当前用 avg × 2.5 近似 |
| P2 | Outbox 模式替代补偿日志 | 写入最终一致性增强 |
| P2 | 完整 OpenTelemetry SDK | 当前仅 trace_id, 无 span 树 |
| P3 | 补偿日志自动重放 Worker | 当前仅记录, 需人工处理 |
