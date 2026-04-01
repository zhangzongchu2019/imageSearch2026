#!/usr/bin/env bash
# ============================================================
# 沙箱入口: 启动 code-server + 保持容器运行
# ============================================================
set -e

echo "============================================"
echo "  imageSearch2026 开发沙箱"
echo "============================================"
echo ""
echo "  VS Code (浏览器):  http://$(hostname -I | awk '{print $1}'):8443"
echo "  密码:              sandbox2026"
echo ""
echo "  终端内使用 Claude Code:"
echo "    claude            # 启动 Claude Code CLI"
echo ""
echo "============================================"

# 启动 code-server (后台)
# 绑定 127.0.0.1 防止公网暴露; 如需远程访问改为 0.0.0.0
code-server --bind-addr 127.0.0.1:8443 --disable-workspace-trust /workspace &

# 如果传入了其他命令则执行，否则保持运行
if [ $# -gt 0 ]; then
    exec "$@"
else
    # 保持容器前台运行
    wait
fi
