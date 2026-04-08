#!/usr/bin/env bash
# ============================================================
# K3s 一键部署脚本 — 以图搜商品系统
# 适用环境: Ubuntu 24 / 2TB RAM / 4×4090D GPU / Docker 29+
# 版本: 0.3.0.0 (isv3-0401)
#
# 用法:
#   ./install.sh init                # 初始化第一台 master
#   ./install.sh join-master <IP>    # 加入为 master 节点
#   ./install.sh join-worker <IP>    # 加入为 worker 节点
#   ./install.sh status              # 查看集群状态
#   ./install.sh uninstall           # 卸载 K3s
# ============================================================
set -euo pipefail

# ── 颜色输出 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/install.log"
K3S_VERSION="v1.30.2+k3s1"
NVIDIA_DEVICE_PLUGIN_VERSION="v0.16.1"

# ── 日志函数 ──
log()  { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${GREEN}[INFO]${NC}  $*" | tee -a "$LOG_FILE"; }
warn() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${YELLOW}[WARN]${NC}  $*" | tee -a "$LOG_FILE"; }
err()  { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${RED}[ERROR]${NC} $*" | tee -a "$LOG_FILE"; }
info() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${CYAN}[STEP]${NC}  $*" | tee -a "$LOG_FILE"; }

die() { err "$*"; exit 1; }

# ── sudo 权限检查 ──
check_sudo() {
    if [[ $EUID -ne 0 ]]; then
        if ! sudo -v 2>/dev/null; then
            die "此脚本需要 root 或 sudo 权限，请使用 sudo 运行"
        fi
    fi
}

# ============================================================
# 先决条件检查
# ============================================================
check_prerequisites() {
    info "检查先决条件..."

    # 检查 Ubuntu 版本
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        if [[ "$ID" != "ubuntu" ]]; then
            warn "当前系统为 $ID，脚本针对 Ubuntu 优化，其他发行版可能存在兼容性问题"
        fi
        if [[ "${VERSION_ID:-}" == "24."* ]]; then
            log "Ubuntu 版本: $VERSION_ID ✓"
        else
            warn "Ubuntu 版本为 ${VERSION_ID:-unknown}，建议使用 24.x"
        fi
    else
        warn "无法检测 OS 版本"
    fi

    # 检查 Docker
    if command -v docker &>/dev/null; then
        local docker_ver
        docker_ver=$(docker --version 2>/dev/null | grep -oP '[\d.]+' | head -1)
        log "Docker 版本: $docker_ver ✓"
    else
        warn "Docker 未安装，K3s 将使用内置 containerd（推荐）"
    fi

    # 检查 NVIDIA 驱动
    if command -v nvidia-smi &>/dev/null; then
        local gpu_count
        gpu_count=$(nvidia-smi -L 2>/dev/null | wc -l)
        local driver_ver
        driver_ver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1)
        log "NVIDIA 驱动: $driver_ver, GPU 数量: $gpu_count ✓"
    else
        warn "nvidia-smi 未找到，GPU 功能将不可用"
    fi

    # 检查 nvidia-container-toolkit
    if command -v nvidia-ctk &>/dev/null; then
        log "NVIDIA Container Toolkit 已安装 ✓"
    else
        warn "NVIDIA Container Toolkit 未安装，将尝试自动安装"
    fi

    # 检查端口冲突 (K3s 核心端口)
    local ports=(6443 10250 8472 51820)
    for port in "${ports[@]}"; do
        if ss -tlnp 2>/dev/null | grep -q ":${port} "; then
            warn "端口 $port 已被占用，可能导致 K3s 启动失败"
        fi
    done

    # 检查内存
    local total_mem_gb
    total_mem_gb=$(awk '/MemTotal/ {printf "%.0f", $2/1024/1024}' /proc/meminfo)
    log "系统内存: ${total_mem_gb}GB"

    # 检查 swap (K3s 建议关闭)
    local swap_total
    swap_total=$(awk '/SwapTotal/ {print $2}' /proc/meminfo)
    if [[ "$swap_total" -gt 0 ]]; then
        warn "检测到 swap 已开启 (${swap_total}kB)，K3s 建议关闭 swap"
    fi

    log "先决条件检查完成"
}

# ============================================================
# 安装 NVIDIA Container Toolkit
# ============================================================
install_nvidia_toolkit() {
    if command -v nvidia-ctk &>/dev/null; then
        log "NVIDIA Container Toolkit 已存在，跳过安装"
        return 0
    fi

    info "安装 NVIDIA Container Toolkit..."

    # 添加 NVIDIA 仓库
    local distro
    distro=$(. /etc/os-release; echo "${ID}${VERSION_ID}" | tr -d '.')
    curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
        | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg 2>/dev/null

    curl -fsSL "https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list" \
        | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' \
        | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list > /dev/null

    sudo apt-get update -qq
    sudo apt-get install -y -qq nvidia-container-toolkit

    log "NVIDIA Container Toolkit 安装完成 ✓"
}

# ============================================================
# 配置 K3s containerd 使用 NVIDIA runtime
# ============================================================
configure_nvidia_containerd() {
    info "配置 K3s containerd NVIDIA runtime..."

    local config_dir="/var/lib/rancher/k3s/agent/etc/containerd"
    sudo mkdir -p "$config_dir"

    # 生成 containerd 配置模板
    sudo tee "${config_dir}/config.toml.tmpl" > /dev/null <<'TOML'
version = 2

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runc.options]
  SystemdCgroup = true

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia]
  privileged_without_host_devices = false
  runtime_engine = ""
  runtime_root = ""
  runtime_type = "io.containerd.runc.v2"

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.nvidia.options]
  BinaryName = "/usr/bin/nvidia-container-runtime"
  SystemdCgroup = true
TOML

    # 设置默认 runtime 为 nvidia (使所有容器都能访问 GPU)
    # 注意: 实际 GPU 调度由 NVIDIA device plugin 控制
    log "K3s containerd NVIDIA runtime 配置完成 ✓"
}

# ============================================================
# 安装 K3s — 第一台 master (init)
# ============================================================
install_k3s_init() {
    info "初始化 K3s 集群 (第一台 master)..."

    check_prerequisites
    install_nvidia_toolkit
    configure_nvidia_containerd

    # K3s 安装参数
    # --docker: 不使用，K3s 自带 containerd 更轻量
    # --disable=traefik: 后续使用自定义 ingress
    # --write-kubeconfig-mode=644: 方便非 root 用户读取
    # --cluster-init: 启用 embedded etcd (HA 模式)
    # --tls-san: 添加额外 SAN 以支持外部访问
    local node_ip
    node_ip=$(hostname -I | awk '{print $1}')

    curl -sfL https://get.k3s.io | \
        INSTALL_K3S_VERSION="$K3S_VERSION" \
        K3S_KUBECONFIG_MODE="644" \
        INSTALL_K3S_EXEC="server \
            --cluster-init \
            --disable=traefik \
            --tls-san=${node_ip} \
            --node-label=gpu=true \
            --kubelet-arg=max-pods=250 \
            --kube-apiserver-arg=default-not-ready-toleration-seconds=30 \
            --kube-apiserver-arg=default-unreachable-toleration-seconds=30 \
        " sh -

    # 等待 K3s 就绪
    info "等待 K3s 启动..."
    local retries=0
    while ! sudo k3s kubectl get nodes &>/dev/null; do
        retries=$((retries + 1))
        if [[ $retries -ge 60 ]]; then
            die "K3s 启动超时 (60s)"
        fi
        sleep 1
    done

    # 获取 join token
    local token
    token=$(sudo cat /var/lib/rancher/k3s/server/node-token)
    log "K3s master 初始化成功 ✓"
    log "Node IP: ${node_ip}"
    log "Join Token: ${token}"
    echo ""
    info "其他节点加入命令:"
    echo -e "  ${CYAN}# 加入为 master:${NC}"
    echo -e "  ${GREEN}./install.sh join-master ${node_ip}${NC}"
    echo -e "  ${CYAN}# 加入为 worker:${NC}"
    echo -e "  ${GREEN}./install.sh join-worker ${node_ip}${NC}"
    echo ""

    # 保存 token 到文件，方便其他节点读取
    echo "$token" > "${SCRIPT_DIR}/.node-token"
    echo "$node_ip" > "${SCRIPT_DIR}/.master-ip"
    chmod 600 "${SCRIPT_DIR}/.node-token"

    # 设置 kubeconfig
    setup_kubeconfig

    # 给当前节点打 GPU 标签
    label_gpu_node

    # 安装 Helm
    install_helm

    # 安装 NVIDIA device plugin
    install_nvidia_device_plugin

    log "K3s 集群初始化全部完成 ✓"
}

# ============================================================
# 加入为 master 节点
# ============================================================
join_as_master() {
    local master_ip="${1:?用法: ./install.sh join-master <MASTER_IP>}"

    info "加入 K3s 集群为 master 节点 (master: ${master_ip})..."

    check_prerequisites
    install_nvidia_toolkit
    configure_nvidia_containerd

    # 获取 token
    local token
    if [[ -f "${SCRIPT_DIR}/.node-token" ]]; then
        token=$(cat "${SCRIPT_DIR}/.node-token")
    else
        echo -n "请输入 Join Token (从第一台 master 获取): "
        read -r token
    fi

    local node_ip
    node_ip=$(hostname -I | awk '{print $1}')

    curl -sfL https://get.k3s.io | \
        INSTALL_K3S_VERSION="$K3S_VERSION" \
        K3S_KUBECONFIG_MODE="644" \
        K3S_TOKEN="$token" \
        INSTALL_K3S_EXEC="server \
            --server=https://${master_ip}:6443 \
            --disable=traefik \
            --tls-san=${node_ip} \
            --node-label=gpu=true \
            --kubelet-arg=max-pods=250 \
        " sh -

    # 等待节点就绪
    info "等待节点加入集群..."
    sleep 10

    setup_kubeconfig
    label_gpu_node

    log "Master 节点加入集群成功 ✓ (IP: ${node_ip})"
}

# ============================================================
# 加入为 worker 节点
# ============================================================
join_as_worker() {
    local master_ip="${1:?用法: ./install.sh join-worker <MASTER_IP>}"

    info "加入 K3s 集群为 worker 节点 (master: ${master_ip})..."

    check_prerequisites
    install_nvidia_toolkit
    configure_nvidia_containerd

    # 获取 token
    local token
    if [[ -f "${SCRIPT_DIR}/.node-token" ]]; then
        token=$(cat "${SCRIPT_DIR}/.node-token")
    else
        echo -n "请输入 Join Token (从第一台 master 获取): "
        read -r token
    fi

    curl -sfL https://get.k3s.io | \
        INSTALL_K3S_VERSION="$K3S_VERSION" \
        K3S_TOKEN="$token" \
        K3S_URL="https://${master_ip}:6443" \
        INSTALL_K3S_EXEC="agent \
            --node-label=gpu=true \
            --kubelet-arg=max-pods=250 \
        " sh -

    local node_ip
    node_ip=$(hostname -I | awk '{print $1}')
    log "Worker 节点加入集群成功 ✓ (IP: ${node_ip})"
}

# ============================================================
# 辅助函数
# ============================================================

setup_kubeconfig() {
    info "配置 kubeconfig..."
    mkdir -p "${HOME}/.kube"
    if [[ -f /etc/rancher/k3s/k3s.yaml ]]; then
        sudo cp /etc/rancher/k3s/k3s.yaml "${HOME}/.kube/config"
        sudo chown "$(id -u):$(id -g)" "${HOME}/.kube/config"
        chmod 600 "${HOME}/.kube/config"
        export KUBECONFIG="${HOME}/.kube/config"
        log "kubeconfig 已写入 ~/.kube/config ✓"
    fi
}

label_gpu_node() {
    # 给当前节点打 GPU 标签和 taint
    local node_name
    node_name=$(hostname)
    if sudo k3s kubectl get node "$node_name" &>/dev/null; then
        sudo k3s kubectl label node "$node_name" gpu=true --overwrite 2>/dev/null || true
        log "GPU 标签已添加到节点 ${node_name} ✓"
    fi
}

install_helm() {
    if command -v helm &>/dev/null; then
        log "Helm 已安装: $(helm version --short) ✓"
        return 0
    fi

    info "安装 Helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    log "Helm 安装完成: $(helm version --short) ✓"
}

install_nvidia_device_plugin() {
    info "安装 NVIDIA Device Plugin (${NVIDIA_DEVICE_PLUGIN_VERSION})..."

    # 检查是否已安装
    if sudo k3s kubectl get daemonset -n kube-system nvidia-device-plugin-daemonset &>/dev/null 2>&1; then
        log "NVIDIA Device Plugin 已存在，跳过"
        return 0
    fi

    sudo k3s kubectl apply -f "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/${NVIDIA_DEVICE_PLUGIN_VERSION}/deployments/static/nvidia-device-plugin.yml"

    # 等待 DaemonSet 就绪
    info "等待 NVIDIA Device Plugin 就绪..."
    local retries=0
    while [[ $retries -lt 120 ]]; do
        local ready
        ready=$(sudo k3s kubectl get daemonset -n kube-system nvidia-device-plugin-daemonset -o jsonpath='{.status.numberReady}' 2>/dev/null || echo "0")
        if [[ "$ready" -ge 1 ]]; then
            break
        fi
        retries=$((retries + 1))
        sleep 2
    done

    log "NVIDIA Device Plugin 安装完成 ✓"
}

# ============================================================
# 查看集群状态
# ============================================================
show_status() {
    info "=== K3s 集群状态 ==="
    echo ""

    if ! command -v k3s &>/dev/null; then
        die "K3s 未安装"
    fi

    echo -e "${CYAN}── 节点状态 ──${NC}"
    sudo k3s kubectl get nodes -o wide 2>/dev/null || warn "无法获取节点信息"
    echo ""

    echo -e "${CYAN}── GPU 资源 ──${NC}"
    sudo k3s kubectl describe nodes | grep -A5 "nvidia.com/gpu" 2>/dev/null || warn "未检测到 GPU 资源"
    echo ""

    echo -e "${CYAN}── 系统 Pod ──${NC}"
    sudo k3s kubectl get pods -n kube-system -o wide 2>/dev/null || true
    echo ""

    echo -e "${CYAN}── 业务命名空间 ──${NC}"
    for ns in imgsrch-infra imgsrch-app imgsrch-monitor; do
        echo -e "\n${YELLOW}namespace: ${ns}${NC}"
        sudo k3s kubectl get pods -n "$ns" -o wide 2>/dev/null || echo "  (命名空间不存在或无 Pod)"
    done
    echo ""

    echo -e "${CYAN}── K3s 版本 ──${NC}"
    sudo k3s --version 2>/dev/null || true

    echo -e "${CYAN}── Helm 版本 ──${NC}"
    helm version --short 2>/dev/null || echo "  Helm 未安装"
}

# ============================================================
# 卸载 K3s
# ============================================================
uninstall_k3s() {
    warn "即将卸载 K3s，所有集群数据将丢失！"
    echo -n "确认卸载? (输入 YES 确认): "
    read -r confirm
    if [[ "$confirm" != "YES" ]]; then
        log "取消卸载"
        return 0
    fi

    info "卸载 K3s..."

    # 先尝试 server 卸载脚本，再尝试 agent 卸载脚本
    if [[ -f /usr/local/bin/k3s-uninstall.sh ]]; then
        sudo /usr/local/bin/k3s-uninstall.sh
        log "K3s server 已卸载 ✓"
    elif [[ -f /usr/local/bin/k3s-agent-uninstall.sh ]]; then
        sudo /usr/local/bin/k3s-agent-uninstall.sh
        log "K3s agent 已卸载 ✓"
    else
        warn "未找到 K3s 卸载脚本，可能未安装"
    fi

    # 清理 kubeconfig
    rm -f "${HOME}/.kube/config"
    rm -f "${SCRIPT_DIR}/.node-token"
    rm -f "${SCRIPT_DIR}/.master-ip"

    log "卸载完成 ✓"
}

# ============================================================
# 主入口
# ============================================================
main() {
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   以图搜商品系统 — K3s 集群部署脚本     ║${NC}"
    echo -e "${CYAN}║   版本: 0.3.0.0   分支: isv3-0401       ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════╝${NC}"
    echo ""

    local cmd="${1:-help}"

    case "$cmd" in
        init)
            check_sudo
            install_k3s_init
            ;;
        join-master)
            check_sudo
            join_as_master "${2:-}"
            ;;
        join-worker)
            check_sudo
            join_as_worker "${2:-}"
            ;;
        status)
            show_status
            ;;
        uninstall)
            check_sudo
            uninstall_k3s
            ;;
        help|--help|-h)
            echo "用法: $0 <命令> [参数]"
            echo ""
            echo "命令:"
            echo "  init              初始化第一台 master 节点"
            echo "  join-master <IP>  作为 master 加入集群 (IP 为第一台 master 地址)"
            echo "  join-worker <IP>  作为 worker 加入集群 (IP 为 master 地址)"
            echo "  status            查看集群状态"
            echo "  uninstall         卸载 K3s"
            echo ""
            echo "部署流程 (4台服务器):"
            echo "  Node 1:  ./install.sh init"
            echo "  Node 2:  ./install.sh join-master <Node1-IP>"
            echo "  Node 3:  ./install.sh join-master <Node1-IP>"
            echo "  Node 4:  ./install.sh join-worker <Node1-IP>"
            echo ""
            echo "集群就绪后，执行 deploy-all.sh 部署所有服务"
            ;;
        *)
            die "未知命令: $cmd (使用 --help 查看帮助)"
            ;;
    esac
}

main "$@"
