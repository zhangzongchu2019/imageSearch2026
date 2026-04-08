#!/usr/bin/env bash
# ============================================================
# 全量服务部署脚本 — 以图搜商品系统
# 按依赖顺序将所有服务部署到 K3s/K8s 集群
# 版本: 0.3.0.0 (isv3-0401)
#
# 用法:
#   ./deploy-all.sh                # 部署全部 (Phase 1-4)
#   ./deploy-all.sh --phase 1      # 只部署基础设施
#   ./deploy-all.sh --phase 2      # 只部署 Milvus
#   ./deploy-all.sh --phase 3      # 只部署业务服务
#   ./deploy-all.sh --phase 4      # 只部署监控
#   ./deploy-all.sh --status       # 查看所有服务状态
#   ./deploy-all.sh --delete       # 删除所有部署 (危险)
#
# 前置条件:
#   K3s 需要配置 NodePort 范围以支持 52xxx 端口:
#   ExecStart=/usr/local/bin/k3s server --service-node-port-range=52000-52399
#   详见: PORT_MAP.md
# ============================================================
set -euo pipefail

# ── 颜色输出 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFESTS_DIR="${SCRIPT_DIR}/k8s-manifests"
LOG_FILE="${SCRIPT_DIR}/deploy.log"

# kubectl 命令 (K3s 内置或独立安装)
KUBECTL="sudo k3s kubectl"
if command -v kubectl &>/dev/null && kubectl cluster-info &>/dev/null 2>&1; then
    KUBECTL="kubectl"
fi

# ── 日志函数 ──
log()  { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${GREEN}[INFO]${NC}  $*" | tee -a "$LOG_FILE"; }
warn() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${YELLOW}[WARN]${NC}  $*" | tee -a "$LOG_FILE"; }
err()  { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"; }
info() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${CYAN}[STEP]${NC}  $*" | tee -a "$LOG_FILE"; }

die() { err "$*"; exit 1; }

# ============================================================
# 等待资源就绪
# ============================================================
wait_for_ready() {
    # 用法: wait_for_ready <类型> <名称> <命名空间> <超时秒数>
    local kind="$1" name="$2" ns="$3" timeout="${4:-300}"

    info "等待 ${kind}/${name} 就绪 (超时: ${timeout}s)..."

    local start_time
    start_time=$(date +%s)

    while true; do
        local elapsed=$(( $(date +%s) - start_time ))
        if [[ $elapsed -ge $timeout ]]; then
            err "${kind}/${name} 在 ${timeout}s 内未就绪"
            $KUBECTL get pods -n "$ns" -l "app=${name}" 2>/dev/null || true
            return 1
        fi

        case "$kind" in
            statefulset)
                local ready
                ready=$($KUBECTL get statefulset "$name" -n "$ns" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
                local desired
                desired=$($KUBECTL get statefulset "$name" -n "$ns" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "1")
                if [[ "$ready" == "$desired" ]] && [[ "$ready" -ge 1 ]]; then
                    log "${kind}/${name} 就绪 (${ready}/${desired}) ✓"
                    return 0
                fi
                ;;
            deployment)
                local avail
                avail=$($KUBECTL get deployment "$name" -n "$ns" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo "0")
                if [[ "${avail:-0}" -ge 1 ]]; then
                    log "${kind}/${name} 就绪 (available: ${avail}) ✓"
                    return 0
                fi
                ;;
            *)
                warn "未知资源类型: $kind"
                return 1
                ;;
        esac

        sleep 5
    done
}

# ============================================================
# Phase 0: 创建命名空间和公共配置
# ============================================================
deploy_phase0() {
    info "═══ Phase 0: 创建命名空间和公共配置 ═══"

    $KUBECTL apply -f "${MANIFESTS_DIR}/namespace.yaml"
    log "命名空间创建完成 ✓"
}

# ============================================================
# Phase 1: 基础设施 (etcd, MinIO, PostgreSQL, Redis, Kafka)
# ============================================================
deploy_phase1() {
    info "═══ Phase 1: 部署基础设施 ═══"

    local start_time
    start_time=$(date +%s)

    # 并行部署所有基础设施 (它们之间无依赖)
    log "部署 PostgreSQL..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/postgres.yaml"

    log "部署 Redis..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/redis.yaml"

    log "部署 Kafka (KRaft)..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/kafka.yaml"

    log "部署 MinIO..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/minio.yaml"

    log "部署 ClickHouse..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/clickhouse.yaml"

    # 等待所有基础设施就绪
    wait_for_ready statefulset postgres imgsrch-infra 300
    wait_for_ready statefulset redis imgsrch-infra 120
    wait_for_ready statefulset kafka imgsrch-infra 180
    wait_for_ready statefulset minio imgsrch-infra 180
    wait_for_ready statefulset clickhouse imgsrch-infra 120

    local elapsed=$(( $(date +%s) - start_time ))
    log "Phase 1 完成 ✓ (耗时: ${elapsed}s)"
}

# ============================================================
# Phase 2: Milvus 分布式集群
# ============================================================
deploy_phase2() {
    info "═══ Phase 2: 部署 Milvus 集群 ═══"

    local start_time
    start_time=$(date +%s)

    log "部署 Milvus etcd 集群..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/milvus-etcd.yaml"
    wait_for_ready statefulset milvus-etcd imgsrch-infra 180

    log "部署 Milvus 各组件..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/milvus.yaml"

    # 等待 Milvus proxy 就绪
    wait_for_ready deployment milvus-proxy imgsrch-infra 300

    local elapsed=$(( $(date +%s) - start_time ))
    log "Phase 2 完成 ✓ (耗时: ${elapsed}s)"
}

# ============================================================
# Phase 3: 业务服务
# ============================================================
deploy_phase3() {
    info "═══ Phase 3: 部署业务服务 ═══"

    local start_time
    start_time=$(date +%s)

    # bitmap-filter 先部署 (search-service 依赖它)
    log "部署 bitmap-filter-service..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/bitmap-filter.yaml"
    wait_for_ready statefulset bitmap-filter imgsrch-app 180

    # inference-service 需要 GPU
    log "部署 inference-service (GPU)..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/inference.yaml"
    # inference 启动慢 (加载模型), 给更长超时
    wait_for_ready deployment inference-service imgsrch-app 600

    # search/write/cron 可以并行
    log "部署 search-service..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/search-service.yaml"

    log "部署 write-service..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/write-service.yaml"

    log "部署 cron-scheduler..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/cron-scheduler.yaml"

    log "部署 web-console..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/web-console.yaml"

    wait_for_ready deployment search-service imgsrch-app 180
    wait_for_ready deployment write-service imgsrch-app 120
    wait_for_ready deployment cron-scheduler imgsrch-app 120
    wait_for_ready deployment web-console imgsrch-app 120

    # 部署 Ingress 规则
    log "部署 Ingress..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/ingress.yaml"

    local elapsed=$(( $(date +%s) - start_time ))
    log "Phase 3 完成 ✓ (耗时: ${elapsed}s)"
}

# ============================================================
# Phase 4: 监控
# ============================================================
deploy_phase4() {
    info "═══ Phase 4: 部署监控系统 ═══"

    local start_time
    start_time=$(date +%s)

    log "部署 Prometheus..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/prometheus.yaml"

    log "部署 Grafana..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/grafana.yaml"

    log "部署 Pipeline Monitor..."
    $KUBECTL apply -f "${MANIFESTS_DIR}/pipeline-monitor.yaml"

    wait_for_ready statefulset prometheus imgsrch-monitor 180
    wait_for_ready deployment grafana imgsrch-monitor 120
    wait_for_ready deployment pipeline-monitor imgsrch-monitor 120

    local elapsed=$(( $(date +%s) - start_time ))
    log "Phase 4 完成 ✓ (耗时: ${elapsed}s)"
}

# ============================================================
# 查看部署状态
# ============================================================
show_status() {
    info "=== 部署状态总览 ==="
    echo ""

    for ns in imgsrch-infra imgsrch-app imgsrch-monitor; do
        echo -e "${CYAN}╔══ 命名空间: ${ns} ══╗${NC}"

        echo -e "${YELLOW}  Pods:${NC}"
        $KUBECTL get pods -n "$ns" -o wide 2>/dev/null | sed 's/^/    /' || echo "    (无)"

        echo -e "${YELLOW}  Services:${NC}"
        $KUBECTL get svc -n "$ns" 2>/dev/null | sed 's/^/    /' || echo "    (无)"

        echo -e "${YELLOW}  PVC:${NC}"
        $KUBECTL get pvc -n "$ns" 2>/dev/null | sed 's/^/    /' || echo "    (无)"

        echo ""
    done

    echo -e "${CYAN}╔══ Ingress ══╗${NC}"
    $KUBECTL get ingress -A 2>/dev/null | sed 's/^/    /' || echo "    (无)"

    echo ""
    echo -e "${CYAN}╔══ GPU 资源使用 ══╗${NC}"
    $KUBECTL describe nodes | grep -A3 "nvidia.com/gpu" 2>/dev/null | sed 's/^/    /' || echo "    (无 GPU 信息)"
}

# ============================================================
# 删除所有部署
# ============================================================
delete_all() {
    warn "即将删除所有 imgsrch 命名空间及其资源！"
    echo -n "确认删除? (输入 DELETE 确认): "
    read -r confirm
    if [[ "$confirm" != "DELETE" ]]; then
        log "取消删除"
        return 0
    fi

    info "删除所有部署..."
    for ns in imgsrch-monitor imgsrch-app imgsrch-infra; do
        log "删除命名空间: ${ns}"
        $KUBECTL delete namespace "$ns" --timeout=120s 2>/dev/null || true
    done
    log "所有部署已删除 ✓"
}

# ============================================================
# 主入口
# ============================================================
main() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   以图搜商品系统 — K8s 全量部署脚本     ║${NC}"
    echo -e "${CYAN}║   版本: 0.3.0.0   分支: isv3-0401       ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════╝${NC}"
    echo ""

    # 检查 manifests 目录
    if [[ ! -d "$MANIFESTS_DIR" ]]; then
        die "manifest 目录不存在: ${MANIFESTS_DIR}"
    fi

    # 检查 NodePort 范围是否支持 52xxx
    local port_range
    port_range=$($KUBECTL get pods -n kube-system -l component=kube-apiserver -o jsonpath='{.items[0].spec.containers[0].command}' 2>/dev/null || true)
    if [[ -n "$port_range" ]] && ! echo "$port_range" | grep -q "52000"; then
        warn "注意: K3s 需要配置 --service-node-port-range=52000-52399 以支持 52xxx NodePort"
        warn "参考: /workspace/deploy/k3s/PORT_MAP.md"
    fi

    local phase=""
    local action="deploy"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --phase)
                phase="${2:?--phase 需要指定阶段号 (1-4)}"
                shift 2
                ;;
            --status)
                action="status"
                shift
                ;;
            --delete)
                action="delete"
                shift
                ;;
            --help|-h)
                echo "用法: $0 [选项]"
                echo ""
                echo "选项:"
                echo "  (无参数)       部署所有服务 (Phase 1-4)"
                echo "  --phase N      只部署指定阶段 (1=基础设施, 2=Milvus, 3=业务, 4=监控)"
                echo "  --status       查看部署状态"
                echo "  --delete       删除所有部署"
                exit 0
                ;;
            *)
                die "未知参数: $1"
                ;;
        esac
    done

    case "$action" in
        status)
            show_status
            return 0
            ;;
        delete)
            delete_all
            return 0
            ;;
    esac

    # 记录部署开始时间
    local total_start
    total_start=$(date +%s)

    # Phase 0 总是执行
    deploy_phase0

    if [[ -z "$phase" ]]; then
        # 部署全部
        deploy_phase1
        deploy_phase2
        deploy_phase3
        deploy_phase4
    else
        case "$phase" in
            1) deploy_phase1 ;;
            2) deploy_phase2 ;;
            3) deploy_phase3 ;;
            4) deploy_phase4 ;;
            *) die "无效的 phase: $phase (有效值: 1-4)" ;;
        esac
    fi

    local total_elapsed=$(( $(date +%s) - total_start ))
    echo ""
    log "═══════════════════════════════════════════"
    log "部署完成! 总耗时: ${total_elapsed}s"
    log "═══════════════════════════════════════════"
    echo ""
    info "NodePort 端口映射 (宿主机访问):"
    echo "  基础设施:"
    echo "    52000  MinIO API          (容器内 9000)"
    echo "    52001  MinIO Console      (容器内 9001)"
    echo "    52010  etcd client        (容器内 2379)"
    echo "    52020  PostgreSQL         (容器内 5432)"
    echo "    52030  Redis              (容器内 6379)"
    echo "    52040  Kafka              (容器内 9092)"
    echo "    52050  ClickHouse HTTP    (容器内 8123)"
    echo "    52051  ClickHouse TCP     (容器内 9000)"
    echo "  Milvus:"
    echo "    52100  Milvus Proxy gRPC  (容器内 19530)"
    echo "    52101  Milvus metrics     (容器内 9091)"
    echo "  业务服务:"
    echo "    52200  inference-service   (容器内 8090)"
    echo "    52210  search-service      (容器内 8080)"
    echo "    52220  write-service       (容器内 8081)"
    echo "    52230  cron-scheduler      (容器内 8082)"
    echo "    52240  bitmap-filter gRPC  (容器内 50051)"
    echo "    52250  web-console         (容器内 3000)"
    echo "  监控:"
    echo "    52300  Prometheus          (容器内 9090)"
    echo "    52310  Grafana             (容器内 3000)"
    echo "    52320  pipeline-monitor    (容器内 9800)"
    echo ""
    info "查看状态: ./deploy-all.sh --status"
}

main "$@"
