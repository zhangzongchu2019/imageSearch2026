#!/usr/bin/env bash
# ============================================================
# 沙箱管理脚本
# 用法:
#   ./sandbox.sh build     - 构建沙箱镜像
#   ./sandbox.sh start     - 启动沙箱 (code-server 自动启动)
#   ./sandbox.sh start-all - 启动补充基础设施 + 沙箱
#   ./sandbox.sh enter     - 进入沙箱终端
#   ./sandbox.sh claude    - 进入沙箱并启动 Claude Code
#   ./sandbox.sh stop      - 停止沙箱
#   ./sandbox.sh stop-all  - 停止沙箱 + 补充基础设施
#   ./sandbox.sh destroy   - 销毁全部 (含数据卷)
#   ./sandbox.sh status    - 查看状态
#   ./sandbox.sh logs      - 查看日志
#   ./sandbox.sh passwd    - 修改 code-server 密码
#   ./sandbox.sh nginx     - 安装 Nginx 反向代理配置
#   ./sandbox.sh infra-up  - 仅启动补充基础设施
#   ./sandbox.sh infra-down- 仅停止补充基础设施
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_SANDBOX="$SCRIPT_DIR/docker-compose.sandbox.yml"
COMPOSE_INFRA="$SCRIPT_DIR/docker-compose.infra.yml"
CONTAINER_NAME="imgsrch-sandbox"
NGINX_CONF="$SCRIPT_DIR/nginx-sandbox.conf"
NGINX_PORT=65082

export HOST_UID=$(id -u)
export HOST_GID=$(id -g)
export DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)

cmd_build() {
    echo ">>> 构建沙箱镜像..."
    docker compose -f "$COMPOSE_SANDBOX" build
    echo ">>> 构建完成"
}

cmd_infra_up() {
    echo ">>> 启动补充基础设施 (Kafka/Milvus/etcd/ClickHouse/监控)..."
    sudo docker compose -f "$COMPOSE_INFRA" up -d
    echo ">>> 等待服务就绪..."
    # 等 Kafka 和 Milvus 健康
    local retries=0
    while [ $retries -lt 30 ]; do
        if sudo docker inspect imgsrch-kafka --format '{{.State.Health.Status}}' 2>/dev/null | grep -q healthy && \
           sudo docker inspect imgsrch-milvus --format '{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; then
            echo ">>> 基础设施全部就绪"
            return 0
        fi
        retries=$((retries + 1))
        echo "    等待中... ($retries/30)"
        sleep 5
    done
    echo ">>> 警告: 部分服务可能还未就绪，请用 ./sandbox.sh status 检查"
}

cmd_infra_down() {
    echo ">>> 停止补充基础设施..."
    sudo docker compose -f "$COMPOSE_INFRA" down
    echo ">>> 已停止"
}

cmd_start() {
    echo ">>> 启动沙箱..."
    sudo docker compose -f "$COMPOSE_SANDBOX" up -d
    echo ""
    echo "==========================================="
    echo "  沙箱已启动!"
    echo ""
    echo "  VS Code (Nginx): https://<IP>:${NGINX_PORT}"
    echo "  密码: sandbox2026"
    echo ""
    echo "  如未配置 Nginx: ./sandbox.sh nginx"
    echo ""
    echo "  其他访问方式:"
    echo "    ./sandbox.sh enter   - 终端"
    echo "    ./sandbox.sh claude  - Claude Code"
    echo "==========================================="
}

cmd_start_all() {
    cmd_infra_up
    cmd_start
}

cmd_enter() {
    if ! sudo docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        cmd_start
    fi
    sudo docker exec -it -u dev -w /workspace "$CONTAINER_NAME" bash
}

cmd_claude() {
    if ! sudo docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        cmd_start
    fi
    sudo docker exec -it -u dev -w /workspace "$CONTAINER_NAME" claude
}

cmd_stop() {
    echo ">>> 停止沙箱..."
    sudo docker compose -f "$COMPOSE_SANDBOX" down
    echo ">>> 已停止"
}

cmd_stop_all() {
    cmd_stop
    cmd_infra_down
}

cmd_destroy() {
    echo ">>> 销毁全部..."
    sudo docker compose -f "$COMPOSE_SANDBOX" down -v
    sudo docker compose -f "$COMPOSE_INFRA" down -v
    sudo docker rmi imgsrch-sandbox:latest 2>/dev/null || true
    echo ">>> 已销毁 (含数据卷)"
}

cmd_status() {
    echo ">>> 已有基础设施 (PG/Redis/MinIO):"
    sudo docker ps --filter "name=server-" --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo ""
    echo ">>> 补充基础设施 (Kafka/Milvus/etcd/ClickHouse/监控):"
    sudo docker ps --filter "name=imgsrch-" --filter "name=imgsrch-kafka" --filter "name=imgsrch-milvus" --filter "name=imgsrch-etcd" --filter "name=imgsrch-clickhouse" --filter "name=imgsrch-prometheus" --filter "name=imgsrch-grafana" --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -v sandbox || echo "  (未启动)"
    echo ""
    echo ">>> 沙箱容器:"
    sudo docker ps -a --filter "name=${CONTAINER_NAME}" --format "  {{.Names}}\t{{.Status}}" || echo "  (未启动)"
    echo ""
    echo ">>> Nginx 状态:"
    if [ -f /etc/nginx/sites-enabled/sandbox ]; then
        echo "  已安装, 端口 ${NGINX_PORT}"
        curl -sk -o /dev/null -w "  code-server: HTTP %{http_code}\n" "https://127.0.0.1:${NGINX_PORT}/" 2>/dev/null || echo "  code-server 未响应"
    else
        echo "  未安装, 执行 ./sandbox.sh nginx"
    fi
    echo ""
    echo ">>> 关键端口:"
    ss -tlnp 2>/dev/null | grep -E ':(5432|6379|9092|19530|8123|8443|9099|3002|65082)\b' | awk '{printf "  %s\n", $4}' | sort -t: -k2 -n
}

cmd_logs() {
    sudo docker compose -f "$COMPOSE_SANDBOX" logs -f --tail 50
}

cmd_passwd() {
    local new_pass="${2:-}"
    if [ -z "$new_pass" ]; then
        read -sp "输入新密码: " new_pass
        echo ""
    fi
    sudo docker exec -u dev "$CONTAINER_NAME" bash -c \
        "sed -i 's/^password:.*/password: ${new_pass}/' /home/dev/.config/code-server/config.yaml"
    sudo docker exec -u dev "$CONTAINER_NAME" bash -c "pkill -USR1 -f code-server || true"
    echo ">>> 密码已更新，刷新浏览器即可"
}

cmd_nginx() {
    echo ">>> 安装 Nginx 反向代理配置..."
    if [ ! -f "$NGINX_CONF" ]; then
        echo "错误: 找不到 $NGINX_CONF"
        exit 1
    fi
    sudo cp "$NGINX_CONF" /etc/nginx/sites-enabled/sandbox
    echo ">>> 检测 Nginx 配置..."
    sudo nginx -t
    echo ">>> 重载 Nginx..."
    sudo systemctl reload nginx
    echo ""
    echo "==========================================="
    echo "  Nginx 代理已就绪!"
    echo "  访问: https://$(hostname -I | awk '{print $1}'):${NGINX_PORT}"
    echo "  密码: sandbox2026"
    echo "==========================================="
}

case "${1:-help}" in
    build)      cmd_build ;;
    start)      cmd_start ;;
    start-all)  cmd_start_all ;;
    enter)      cmd_enter ;;
    claude)     cmd_claude ;;
    stop)       cmd_stop ;;
    stop-all)   cmd_stop_all ;;
    destroy)    cmd_destroy ;;
    status)     cmd_status ;;
    logs)       cmd_logs ;;
    passwd)     cmd_passwd "$@" ;;
    nginx)      cmd_nginx ;;
    infra-up)   cmd_infra_up ;;
    infra-down) cmd_infra_down ;;
    *)
        echo "用法: $0 {build|start|start-all|enter|claude|stop|stop-all|destroy|status|logs|passwd|nginx|infra-up|infra-down}"
        exit 1
        ;;
esac
