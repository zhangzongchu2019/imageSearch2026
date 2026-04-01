# imageSearch2026 开发沙箱

基于 Docker 的一体化开发环境，内置 CUDA 12.6 + Python 3.11 + Node 20 + Java 21 + code-server (浏览器 VS Code) + Claude Code CLI。

## 快速开始

```bash
cd sandbox

# 1. 构建镜像
./sandbox.sh build

# 2. 启动沙箱 (含 code-server)
./sandbox.sh start

# 3. 浏览器访问 VS Code
#    https://<宿主机IP>:65082  (需先 ./sandbox.sh nginx)
#    密码: sandbox2026

# 4. 进入终端 / 启动 Claude Code
./sandbox.sh enter
./sandbox.sh claude
```

## 补充基础设施

沙箱复用宿主机已有的 PG / Redis / MinIO，另需启动: Kafka、Milvus、etcd、ClickHouse、Prometheus、Grafana。

```bash
# 启动全部 (基础设施 + 沙箱)
./sandbox.sh start-all

# 仅基础设施
./sandbox.sh infra-up
./sandbox.sh infra-down
```

## 常用命令

| 命令 | 说明 |
|------|------|
| `./sandbox.sh build` | 构建沙箱镜像 |
| `./sandbox.sh start` | 启动沙箱 |
| `./sandbox.sh start-all` | 启动基础设施 + 沙箱 |
| `./sandbox.sh enter` | 进入沙箱终端 |
| `./sandbox.sh claude` | 进入沙箱并启动 Claude Code |
| `./sandbox.sh stop` | 停止沙箱 |
| `./sandbox.sh stop-all` | 停止全部 |
| `./sandbox.sh destroy` | 销毁全部 (含数据卷) |
| `./sandbox.sh status` | 查看状态 |
| `./sandbox.sh logs` | 查看日志 |
| `./sandbox.sh passwd` | 修改 code-server 密码 |
| `./sandbox.sh nginx` | 安装 Nginx 反向代理 |

## 端口映射

| 服务 | 端口 |
|------|------|
| code-server (Nginx) | 65082 (HTTPS) |
| code-server (直连) | 8443 |
| PostgreSQL | 5432 (宿主机) |
| Redis | 6379 (宿主机) |
| Kafka | 9092 / 29092 |
| Milvus | 19530 |
| ClickHouse | 8123 |
| Prometheus | 9099 |
| Grafana | 3002 |

## 文件说明

- `Dockerfile` — 沙箱镜像定义
- `docker-compose.sandbox.yml` — 沙箱容器编排
- `docker-compose.infra.yml` — 补充基础设施编排
- `sandbox.sh` — 管理脚本
- `entrypoint.sh` — 容器入口
- `nginx-sandbox.conf` — Nginx SSL 反向代理配置
- `requirements-sandbox.txt` — Python 统一依赖
