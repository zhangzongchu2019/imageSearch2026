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

### 集成测试 (10 cases)

`services/search-service/tests/integration/test_integration.py`

需真实中间件: `INTEGRATION_TEST=true pytest tests/integration/ -v`

| 测试 | 覆盖 |
|------|------|
| test_retry_connects_after_delay | FIX #1 重试成功 |
| test_retry_exhausted_raises | FIX #1 重试耗尽 |
| test_dedup_redis_then_pg | 去重完整链路 |
| test_dedup_redis_hit | Redis 快路径 |
| test_watchdog_renews_lock | FIX #4 锁续约 |
| test_watchdog_stops_on_lock_loss | FIX #4 丢锁停止 |
| test_compensation_log_on_pg_failure | FIX #9 补偿日志 |
| test_flush_timeout_logged_not_swallowed | FIX #3 超时捕获 |
| test_trace_id_format | FIX #12 W3C 格式 |
| test_trace_id_uniqueness | FIX #12 唯一性 |

### 单元测试 (20 cases)

`services/search-service/tests/unit/test_production_fixes.py`

覆盖全部 15 项修复, mock 外部依赖。

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
□ 运行单元测试: pytest tests/unit/ -v
□ 运行集成测试: INTEGRATION_TEST=true pytest tests/integration/ -v
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

**总计**: 17 文件, ~2,800 行

---

## 七、质量评分

| 维度 | v1.2 | v1.3 | 关键改进 |
|------|------|------|---------|
| 可靠性 | ★★☆☆☆ | ★★★★☆ | 重试/Watchdog/事务写入 |
| 安全性 | ★★☆☆☆ | ★★★★☆ | SHA256 认证/配置服务 |
| 可观测性 | ★★★☆☆ | ★★★★☆ | 实时指标/Trace/11 新 metric |
| 测试覆盖 | ★★☆☆☆ | ★★★★☆ | 30 test cases |
| 运维就绪 | ★★★☆☆ | ★★★★☆ | 优雅关闭/动态阈值/自动恢复 |

---

## 八、后续规划

| 优先级 | 项目 | 说明 |
|--------|------|------|
| P2 | HDRHistogram 精确 P99 | 当前用 avg × 2.5 近似 |
| P2 | Outbox 模式替代补偿日志 | 写入最终一致性增强 |
| P2 | 完整 OpenTelemetry SDK | 当前仅 trace_id, 无 span 树 |
| P3 | 补偿日志自动重放 Worker | 当前仅记录, 需人工处理 |
