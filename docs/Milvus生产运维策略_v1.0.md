# Milvus 生产运维策略 v1.0

> 版本: v1.0 | 日期: 2026-04-03 | 环境: 4×4090D / 2TB RAM / Milvus v2.5.4 standalone

## 1. 架构概览

```
Client → Milvus Standalone (19530)
              ├── etcd (2379)     — 元数据存储
              ├── MinIO (9000)    — 向量段持久化
              └── Index Node      — 索引构建 (内嵌, 1 slot)
```

### 当前规模
| 指标 | 值 |
|------|-----|
| Collection | global_images_hot |
| 总记录数 | ~20,000,000 |
| 向量维度 | 256 (ViT-L-14 768→256 投影) |
| 索引 | HNSW (M=24, efConstruction=200) + 10 INVERTED |
| 分区 | p_202604 (按月), p_999999 (常青) |

## 2. 已知风险与规避策略

### 2.1 etcd 请求风暴 (最高风险)

**现象**: Milvus 突然退出, 日志显示 `disconnected from etcd, process will exit`

**根因**: 大量写入 → 产生大量 segment → 每个 segment 触发 11 个 indexing task → task scheduler 每秒轮询 etcd → etcd apply 延迟从 100ms 飙到 25s → session lease 超时 → 全组件退出

**规避**:

```yaml
# etcd 容器配置 (docker-compose.yml)
ETCD_HEARTBEAT_INTERVAL: "500"     # 默认 100ms → 500ms
ETCD_ELECTION_TIMEOUT: "5000"      # 默认 1000ms → 5000ms
ETCD_MAX_REQUEST_BYTES: "10485760" # 默认 1.5MB → 10MB
```

```yaml
# milvus.yaml
common:
  sessionTTL: 120          # 默认 30s → 120s
etcd:
  requestTimeout: 60000    # 默认 10s → 60s
```

**监控指标**:
- `etcd_server_slow_apply_total` > 0 → 预警
- Milvus task_num > 50 → 暂停写入
- `etcd_server_proposals_failed_total` 增长 → 严重

### 2.2 批量导入策略

**禁止**: 在有索引的情况下大批量 upsert (>50 万条)

**正确流程**:
```
1. coll.release()           # 释放 collection
2. coll.drop_index(...)     # drop 所有 11 个索引
3. 分批 upsert (100万/批, 无 flush, 批间等 10s)
4. 重建索引 (HNSW 先建, 标量索引用 _async=True)
5. coll.load()              # 加载 collection
```

**批量写入参数**:
| 参数 | 推荐值 | 说明 |
|------|--------|------|
| upsert batch_size | 2,000 | 避免单次 RPC 过大 |
| 每批记录数 | 1,000,000 | 内存 < 64GB cgroup 限制 |
| 批间等待 | 10s | 无索引时无需等 |
| flush | 不主动调用 | 让 Milvus auto-flush |

### 2.3 索引构建注意事项

**HNSW 构建**:
- 2000 万 × 256 维, M=24, efConstruction=200 → 约 **18 分钟**
- 构建期间 CPU 高负载, 可能影响 etcd session
- 建议: 单独建 HNSW, 完成后再建标量索引

**标量索引**:
- 10 个 INVERTED 索引, 每个约 1-3 分钟
- 用 `_async=True` 提交, 后台排队建

**索引构建后验证**:
```python
from pymilvus import utility
progress = utility.index_building_progress('global_images_hot', 'global_vec')
# {'total_rows': 20131226, 'indexed_rows': 20131226, 'state': 'Finished'}
```

### 2.4 分区管理

**自动轮转**: `cron-scheduler` 每月 1 号创建新分区, 删除 18 个月前的旧分区

**手动操作**:
```python
# 创建新月分区
coll.create_partition("p_202605")

# 查看分区
for p in coll.partitions:
    print(f"{p.name}: {p.num_entities:,}")
```

**注意**: drop partition 前必须先 release

## 3. 日常监控

### 3.1 监控页面
- Pipeline Monitor: `http://<server-ip>:9800`
- Grafana: `http://<server-ip>:3002`
- Milvus metrics: `http://<server-ip>:9091/metrics`

### 3.2 关键检查命令

```bash
# Milvus 状态
docker inspect imgsrch-milvus --format '{{.State.Status}}'

# 数据量
python3 -c "
from pymilvus import connections, Collection
connections.connect('default', host='localhost', port=19530)
coll = Collection('global_images_hot')
print(f'Entities: {coll.num_entities:,}')
print(f'Indexes: {len(coll.indexes)}')
"

# etcd 健康
docker logs imgsrch-etcd 2>&1 | grep -c "apply request took too long"

# task 队列
docker logs imgsrch-milvus 2>&1 | grep "task num" | tail -1
```

### 3.3 紧急恢复

**Milvus 崩溃恢复**:
```bash
# 1. 重启 etcd (先)
docker restart imgsrch-etcd
sleep 5
# 2. 重启 Milvus
docker restart imgsrch-milvus
sleep 30
# 3. 验证
python3 -c "from pymilvus import connections; connections.connect('default', host='localhost', port=19530); print('OK')"
```

**etcd 数据丢失恢复**:
- etcd 数据丢失 = Milvus collection 元数据丢失
- 向量数据在 MinIO 中保留, 但需要重新导入
- 从备份 JSONL 恢复: `/data/imgsrch/backup_20260403/`

## 4. 容量规划

### 4.1 当前资源使用 (2000 万条)
| 组件 | 磁盘 | 内存 |
|------|------|------|
| Milvus HNSW | ~20 GB | ~20 GB |
| MinIO segments | ~35 GB | — |
| PostgreSQL | ~5 GB | ~2 GB |
| **合计** | ~60 GB | ~22 GB |

### 4.2 扩容到 1 亿条
| 组件 | 磁盘 | 内存 |
|------|------|------|
| Milvus HNSW | ~100 GB | ~100 GB |
| MinIO | ~175 GB | — |
| PostgreSQL | ~25 GB | ~10 GB |
| **合计** | ~300 GB | ~110 GB |

### 4.3 写入吞吐
| 配置 | 吞吐 | 日处理量 (9h 峰值) |
|------|------|---------------------|
| 单卡 ViT-L-14 (无 FashionSigLIP) | 78 img/s | 252 万 |
| 单卡 ViT-L-14 + FashionSigLIP 批量 | ~60 img/s | 194 万 |
| 4 卡独立进程 | ~240 img/s | 780 万 |

## 5. etcd 崩溃深度分析 (2026-04-04)

### 5.1 崩溃机制

```
Milvus standalone: 所有组件 (proxy, dataCoord, indexNode, queryNode) 在同一进程
                                          ↓
批量写入 50 万条 → 31 个 segment → 11 索引/segment = 341 个 indexing task
                                          ↓
indexNode 建 HNSW 索引 → CPU 100% + 大量磁盘 IO
                                          ↓
etcd WAL fsync 被 CPU/IO 争抢 → 响应延迟从 1ms → 10~25s
                                          ↓
Milvus session keepAlive 超时 → "disconnected from etcd" → 全组件退出
```

### 5.2 生产在线写入 vs 批量导入

| 指标 | 生产在线 (500万/天) | 批量导入 (50万/批) |
|------|-------------------|-------------------|
| 写入速率 | 174 QPS (均匀) | 8000 QPS (瞬时) |
| 新 segment/分钟 | 0.7 | 31 (瞬间) |
| indexing task 积压 | 1-5 个 | 341 个 |
| etcd 轮询 QPS | 1-5 | 341 |
| **风险** | **✅ 安全** | **❌ 崩溃** |

### 5.3 流量突增压力测试 (理论)

| 场景 | 写入 QPS | etcd 轮询 QPS | 风险 |
|------|---------|--------------|------|
| 日常 500万/天 | 174 | 1 | ✅ 安全 |
| 日活 +50% | 260 | 2 | ✅ 安全 |
| 某时段 +100% | 347 | 2 | ✅ 安全 |
| 某时段 +300% | 694 | 5 | ✅ 安全 |

**结论**: 生产在线写入即使流量暴增 300%, etcd 压力极低 (5 QPS), 完全安全。
崩溃只发生在批量运维操作 (大批量导入/索引重建)。

### 5.4 运维操作红线

| 操作 | 安全条件 | 红线 |
|------|---------|------|
| 在线单条写入 | 任意速率 | 无限制 |
| 批量 upsert | 必须先 drop 索引 | **禁止带索引批量写入 >10 万条** |
| 重建索引 | HNSW 单独建, 标量 async | **禁止同时建多个 HNSW 索引** |
| 分区轮转 | 凌晨低峰 | **禁止在高峰期做 compaction** |
| 重启 Milvus | 先确认 task_num | **如 task_num >50, 先 drop 索引再重启** |
| 重建 etcd | docker restart, 不要 rm | **禁止 docker rm etcd (丢数据)** |

## 6. 生产环境升级路径

### 阶段 1: 当前 Standalone 优化 ✅
- etcd 超时参数调优
- 批量导入 drop 索引策略
- 监控预警

### 阶段 2: 分布式部署 (计划中)

**目标**: indexNode 独立 → 索引构建不影响 etcd

```
                    ┌─── etcd × 3 (独立集群, RAM disk WAL)
                    │
Client → Proxy ─────┼─── DataNode × 2 (写入)
                    │
                    ├─── IndexNode × 2-4 (独立容器, 不争抢 CPU/IO)
                    │
                    ├─── QueryNode × 2 (搜索)
                    │
                    └─── MinIO (持久化)
```

**关键改进**:
- indexNode 在独立容器, 建索引时 CPU 100% 不影响其他组件
- etcd WAL 放 RAM disk (/dev/shm), 消除磁盘 IO 抖动
- 3 节点 etcd 集群, 容忍单节点故障

### 阶段 3: 索引优化 (计划中)

| 当前 (11 个) | 优化后 (5-6 个) | 去掉的字段 |
|-------------|----------------|-----------|
| HNSW | HNSW | 保留 |
| category_l1 | category_l1 | 保留 (高频过滤) |
| tags | tags | 保留 (高频过滤) |
| ts_month | ts_month | 保留 (分区过滤) |
| is_evergreen | is_evergreen | 保留 (常青标记) |
| category_l2 | — | 去掉 (低频, brute-force 过滤) |
| category_l3 | — | 去掉 |
| color_code | — | 去掉 |
| material_code | — | 去掉 |
| style_code | — | 去掉 |
| season_code | — | 去掉 |

segment 优化: maxSize 1024MB → 4096MB, 减少 segment 数量 ~75%

### 阶段 4: Milvus Cloud / Zilliz Cloud (远期)
- 托管服务, 免运维
- 自动扩缩容
- 适合 1 亿+ 规模

---

## 7. 避坑指南 (2026-04-06 多Collection迁移实战总结)

> 基于 22M VIP + 20M SVIP + 10M 常青 多Collection架构迁移过程中的实际踩坑

### 坑 1: `create_index(_async=True)` 检测不到完成状态

**现象**: 调用 `create_index(field, params, _async=True)` 后，通过 `collection.indexes` 轮询，该字段迟迟不出现。增量构建器反复超时重提交。

**原因**: `_async=True` 返回 future 对象，需要手动 `.result()` 等待。而 `collection.indexes` 返回的是已注册的索引定义，异步提交后注册时机不确定。

**解决**: 标量索引用**同步模式**，阻塞直到所有 segment 建完:

```python
# ✅ 正确: 同步模式, 实测每个标量索引 22M 行约 45 秒
coll.create_index("category_l1", {"index_type": "INVERTED"}, timeout=3600)

# ❌ 错误: 异步 + 轮询 indexes 列表 (可能永远等不到)
coll.create_index("category_l1", {"index_type": "INVERTED"}, _async=True)
while "category_l1" not in {idx.field_name for idx in coll.indexes}:
    time.sleep(30)
```

### 坑 2: 批量提交标量索引导致 etcd 超时崩溃

**现象**: 一次性提交 10+ 个 `create_index(_async=True)` → 全部排队 → IndexNode slot=0 → 大量 task 轮询 etcd → etcd 心跳超时 → Milvus 自杀退出

**根因**: standalone 模式 IndexNode 只有 **1 个 slot**。批量异步提交 = 所有 task 挤压在队列中，task scheduler 每秒轮询 etcd 状态，压力线性叠加。

**解决**: **逐个同步提交，等完再提下一个**:

```python
SCALAR_FIELDS = ['category_l1', 'category_l2', ...]
for field in SCALAR_FIELDS:
    coll.create_index(field, {"index_type": "INVERTED"}, timeout=3600)
    time.sleep(5)  # 给 etcd 喘口气
```

### 坑 3: HNSW vs INVERTED 资源需求差异巨大

**实测数据** (22M 行, standalone 1 slot):

| 索引类型 | 字段 | 耗时 | 内存峰值 | 说明 |
|---------|------|------|---------|------|
| HNSW (M=24, ef=200) | global_vec (768d) | 2-4 小时 | 60-80GB | CPU 100% 持续 |
| INVERTED | category_l1 (varchar) | ~45 秒 | 2-4GB | 快速完成 |
| INVERTED | is_evergreen (bool) | ~45 秒 | 1-2GB | 最轻量 |

**建议**: 先建 HNSW → 等完成 → 再逐个建标量索引 → 最后 load

### 坑 4: 迁移时新 Collection 的 img_999999 (常青) 数据写入后 num_entities=0

**现象**: 写入完成，flush 也执行了，但 `collection.num_entities` 返回 0。

**原因**: Milvus 的 `num_entities` 需要等 growing segment seal + flush + compaction 完成后才反映。尤其是小批量写入 (<100万条) 时，growing segment 可能不会自动 seal。

**解决**: 写入后调用 `collection.flush()` + 等待 30 秒 + 再查 `num_entities`。

### 坑 5: standalone 反复崩溃 — 根因是 compaction 风暴

**现象**: Milvus 日志反复出现 `"total delete entities is too much"`，然后 etcd 超时自杀。

**原因**: 迁移过程中大量 upsert/delete 产生 tombstone → 触发 compaction → compaction + indexing 争抢唯一的 worker slot → etcd 请求积压。

**缓解**: 迁移完成后等 10-15 分钟让 compaction 自然消化，再开始建索引。

### 坑 6: 增量索引构建器进程意外退出

**现象**: nohup 启动的 Python 脚本运行一段时间后进程消失。

**原因**: 
1. pymilvus 连接超时时未捕获 `ConnectionRefusedError`
2. Milvus 崩溃时 `create_index` 抛出未捕获异常
3. Python 主线程异常未被 try/except 覆盖

**解决**: 所有 pymilvus 操作必须包裹在 try/except 中，内置 Milvus 健康检查 + 自动重启逻辑。

### 坑 7: GPU 推理必须用 gpu_worker 容器

**规则**: sandbox 容器的 CUDA 驱动不可用，GPU 推理**只能在 `gpu_worker` 容器中执行**。

### 坑 8: gpu_worker 容器内 localhost 连不到 Milvus

**现象**: 推理成功 (220K 张图)，写入 Milvus 时 `Fail connecting to server on localhost:19530`。两次都在写入阶段失败。

**原因**: gpu_worker 容器内的 `localhost` 是容器自身，不是宿主机。Milvus 运行在另一个容器 `imgsrch-milvus` 中。

**解决**:
1. 脚本加自动检测: 先试 localhost，失败尝试 `imgsrch-milvus`
2. 环境变量: `MILVUS_HOST=imgsrch-milvus`
3. 紧急修复（已运行进程）: 在容器内用 socat 转发
   ```bash
   docker exec -d gpu_worker socat TCP-LISTEN:19530,fork,reuseaddr TCP:imgsrch-milvus:19530
   ```

**预防**: 所有容器化脚本的服务地址必须用环境变量，默认值用容器名而非 localhost。

### 关键数字速查 (更新)

| 指标 | 数值 | 备注 |
|------|------|------|
| INVERTED 索引 (同步) | ~45s / 字段 / 22M行 | standalone 1 slot |
| HNSW 索引 | ~2-4h / 22M行 768d | M=24, ef=200 |
| 10 个标量索引合计 | ~8-10 分钟 | 逐个同步提交 |
| 批量写入 | ~8000 行/秒 | batch_size=5000 |
| 22M 行迁移 | ~45 分钟 | 含 flush |
| Milvus 重启 | ~30-45 秒 | standalone |
| etcd 心跳超时 | 30s (调优后) | 默认 10s |
| standalone IndexNode slot | 1 | 分布式可 4+ |

### 坑 9: standalone 64GB 内存限制导致多 Collection 无法同时 load

**现象**: 查询验证尝试 load img_202604_vip (22M) 时报错:
```
OOM if load, memUsage=61634MB, predictMemUsage=64600MB, totalMem=65536MB
```

**原因**: Milvus standalone 容器内存限制 64GB (cgroup), 单个 22M 行 Collection 的 HNSW 索引 + segment 需要 ~60GB, 无法同时 load 第二个。

**临时方案**: 逐个 load/release, 不同时 load 多个大 Collection。
**正式方案**: 扩容到 256GB, 或分布式部署 (3 QueryNode × 128GB = 384GB)。

### 坑 10: gpu_worker 容器内 socat 端口转发需要覆盖所有服务

**服务列表**: Milvus(19530), PostgreSQL(5432), Kafka(9092/29092), Redis(6379)
```bash
docker exec -d gpu_worker socat TCP-LISTEN:19530,fork,reuseaddr TCP:imgsrch-milvus:19530
docker exec -d gpu_worker socat TCP-LISTEN:5432,fork,reuseaddr TCP:172.21.0.1:5432
docker exec -d gpu_worker socat TCP-LISTEN:9092,fork,reuseaddr TCP:172.21.0.1:9092
docker exec -d gpu_worker socat TCP-LISTEN:6379,fork,reuseaddr TCP:172.21.0.1:6379
```

### 坑 11: standalone → distributed 迁移 etcd session 不兼容

**现象**: 停掉 standalone, 用相同 etcd+MinIO 启动分布式组件 (rootcoord/datacoord/...), 所有 Coordinator 报错:
```
find no available rootcoord, check rootcoord state
async dial failed, wait for retry... context deadline exceeded
bad resolver state
```

**根因**: standalone 模式下所有组件在同一进程内, etcd 中只注册一个 session。分布式模式下每个组件是独立进程, 通过 etcd 服务发现互相连接。两者的 etcd key 结构不同:
- standalone: `by-dev/session/standalone` (单 key)
- distributed: `by-dev/session/rootcoord`, `by-dev/session/datacoord` 等 (多 key)

standalone 残留的 session 干扰了分布式组件的注册发现。

**解决方案** (待验证):
1. **方案 A**: 迁移前清理 etcd session key, 保留 collection 元数据
   ```bash
   etcdctl del --prefix by-dev/session/
   # 保留: by-dev/meta/, by-dev/snapshots/ 等
   ```
2. **方案 B**: 使用全新 rootPath (如 `by-dev-cluster`), 分布式组件不读旧 session
   - 但 collection 元数据也在旧 rootPath 下, 需要迁移或重建
3. **方案 C**: 官方 Milvus Operator (K8s 部署), 内置迁移逻辑

**教训**: "相同 etcd+MinIO 可兼容" 指的是**数据层**(segment/index 在 MinIO 中), 但 **session 层**不兼容。分布式迁移不是简单的换启动命令。

**当前状态**: 已回退到 standalone (256GB 内存), 93.7M 数据完整。分布式部署需要专门规划迁移步骤。

### 坑 11 修正: 真正的根因是 MQ_TYPE 而非 etcd session

**更正**: 之前认为 etcd session 不兼容, 实际根因是 Milvus 分布式组件默认连 Pulsar:
```
[dependency/factory.go:86] ["try to init mq"] [standalone=false] [mqType=pulsar]
```

standalone 模式默认用内嵌 RocksMQ, 分布式模式默认用 Pulsar。我们用的是 Kafka, 必须显式设置:
```
-e MQ_TYPE=kafka
-e KAFKA_BROKER_LIST=kafka:29092
```

**结论**: standalone → distributed 使用相同 etcd+MinIO, 数据确实自动兼容。关键是 MQ_TYPE 必须正确设置。

### 坑 12: Docker 分布式部署端口冲突

53100/55100 等端口可能被占用 (Docker proxy 残留)。建议:
- 使用 55000-56000 段 (Docker 分布式)
- 或直接复用 standalone 原端口 19530 (停掉 standalone 后)

### 坑 13: inference-service 和 batch_import 使用不同模型

**现象**: 搜索结果相似度极低 (0.09), 同一张图在线提取的向量和库里存的完全不同。

**根因**: inference-service 环境变量 `CLIP_MODEL_PATH` 指向了 ViT-B-32 (354MB), 而数据导入用的是 ViT-L-14 (1.7GB)。两个模型的向量空间完全不同。

**排查方法**:
```python
# 同一张图, 对比在线提取 vs 库内存储的向量余弦相似度
cosine = np.dot(vec_inf, vec_db) / (np.linalg.norm(vec_inf) * np.linalg.norm(vec_db))
# 如果 < 0.5 → 模型不一致
```

**解决**: 所有服务统一使用 gpu_worker 中的 ViT-L-14:
```
CLIP_MODEL_PATH=/data/imgsrch/models/ViT-L-14.pt
INFERENCE_URL=http://172.21.0.8:8090  # gpu_worker IP
```

**预防**: 在 CLAUDE.md 和部署文档中明确记录模型路径, 所有服务从同一配置读取。

## 8. 搜索精度基线测试 (2026-04-08)

### 测试条件
- 查询图: 短袖T恤模特上身图 (京东商品图)
- 搜索库: img_202603_svip (20M, L2品类已补全)
- 模型: ViT-L-14 256维 + COSINE + 品类加权

### Top-8 结果

| # | 图片 | 分数 | 品类 | 精准 |
|---|------|------|------|------|
| 1 | 长袖运动套装 Adidas | 0.856 | 服装/上装 | ⚠️ |
| 2 | 红色短袖T恤 | 0.848 | 服装/上装 | ✅ |
| 3 | 消防保暖内衣长袖 | 0.843 | 服装/上装 | ⚠️ |
| 4 | 同色短袖T恤 | 0.840 | 服装/运动服 | ✅ |
| 5 | 蓝色长袖T恤 | 0.839 | 服装/运动服 | ⚠️ |
| 6 | 同色Polo T恤 | 0.834 | 服装/运动服 | ✅ |
| 7 | 长袖白色T恤 | 0.831 | 服装/运动服 | ⚠️ |
| 8 | 长袖白色T恤 | 0.83x | 服装/运动服 | ⚠️ |

### 基线指标
- **L1 品类准确率: 100%** (8/8 服装类)
- **Top-8 精准率: 37.5%** (3/8 短袖匹配)
- **Top-5 精准率: 40%** (2/5)
- 分数区间: 0.83-0.86 (极窄, 0.026)

### 分析
- 向量搜索能区分品类 (服装 vs 箱包 vs 鞋帽) ✅
- 无法区分细粒度属性 (短袖 vs 长袖) ⚠️
- 短袖和长袖在 256 维空间中距离几乎相同

### 优化方向
- Task 21 属性过滤: 识别"短袖"标签, 过滤长袖 → 精准率预期 80%+
- Task 20 更多数据: 增加短袖T恤原始图 → 提升召回

## 9. 搜索排序需求 (2026-04-08)

### 排序优先级 (从高到低)
1. **原图高度相似** — 几乎一样的图片, score > 0.95
2. **型号一致 + 语义一致** — 如同一双鞋的不同视角, score > 0.90 + 同 L2 品类
3. **系列型号 + 外观相同** — 同系列不同色号, score > 0.85 + 同 L2 + 相似标签
4. **颜色材质一致 + 外形相似** — 不同品牌但同类型, score > 0.80 + 同 L1 + 颜色/材质标签匹配

### 实现方案
当前: 向量 COSINE 分数 + L1/L2 品类加权 (+0.05/+0.03)
待做: 
- 标签匹配加权 (颜色/材质/风格 tags 重叠度)
- 分段排序: score > 0.95 的直接排前面, 然后按品类+标签综合评分
