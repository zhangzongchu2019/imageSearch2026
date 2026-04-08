# 项目规则

## 输出规范
- **所有回应消息必须带上日期时间**，格式: `[YYYY-MM-DD HH:MM]`，放在回应开头或关键信息处。
- 批量任务必须记录详细日志（参数、耗时、失败 URL 等）。

## 版本号
- 四位版本号: X.Y.Z.W，当前 0.3.0.0，分支 isv3-0401

## 环境
- 服务器 2TB RAM / 4×4090D GPU
- GPU 推理必须用 `gpu_worker` 容器（sandbox 的 CUDA 驱动掉了）
- Milvus standalone 连接 etcd 3 节点集群
