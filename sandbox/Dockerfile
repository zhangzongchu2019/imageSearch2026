# ============================================================
# Claude Code 开发沙箱 · imageSearch2026
# 基础镜像: Ubuntu 24.04 + Python 3.11 + Node 20 + Java 21
# 内置 code-server (浏览器 VS Code) + Claude Code CLI
# ============================================================
FROM nvidia/cuda:12.6.3-cudnn-devel-ubuntu24.04

ARG DEBIAN_FRONTEND=noninteractive

# ── CUDA 环境变量 ──
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENV PATH="/usr/local/cuda/bin:${PATH}"
ENV LD_LIBRARY_PATH="/usr/local/cuda/lib64:${LD_LIBRARY_PATH}"

# ── 系统依赖 ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    # 基础工具
    curl wget git make jq unzip vim less \
    ca-certificates gnupg lsb-release software-properties-common \
    # Java 21
    openjdk-21-jdk-headless \
    # 构建工具
    build-essential pkg-config \
    # gRPC / Protobuf
    protobuf-compiler \
    # 网络调试
    iputils-ping dnsutils net-tools \
    && rm -rf /var/lib/apt/lists/*

# ── Python 3.11 (通过 deadsnakes PPA) ──
RUN add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    python3.11 python3.11-venv python3.11-dev python3-pip && \
    rm -rf /var/lib/apt/lists/*

# ── 设置 Python 默认版本 ──
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# ── Node.js 20 LTS ──
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# ── Maven 3.9 ──
ARG MAVEN_VERSION=3.9.14
RUN curl -fsSL https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    | tar xz -C /opt && \
    ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn

# ── Docker CLI (静态二进制, 避免 Docker apt 源连接问题) ──
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.5.1.tgz \
    | tar xz --strip-components=1 -C /usr/local/bin docker/docker && \
    chmod +x /usr/local/bin/docker

# ── grpcurl (gRPC 调试) ──
RUN curl -fsSL https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_amd64.deb -o /tmp/grpcurl.deb && \
    dpkg -i /tmp/grpcurl.deb && rm /tmp/grpcurl.deb

# ── code-server (浏览器 VS Code) ──
ARG CODE_SERVER_VERSION=4.96.4
RUN curl -fsSL https://github.com/coder/code-server/releases/download/v${CODE_SERVER_VERSION}/code-server_${CODE_SERVER_VERSION}_amd64.deb -o /tmp/code-server.deb && \
    dpkg -i /tmp/code-server.deb && rm /tmp/code-server.deb

# ── 创建开发用户 (UID/GID 与宿主机一致) ──
ARG HOST_UID=1000
ARG HOST_GID=1000
ARG DOCKER_GID=988
RUN set -e; \
    # 清理占用目标 UID/GID 的已有用户和组
    existing_user=$(getent passwd ${HOST_UID} | cut -d: -f1 || true); \
    [ -n "$existing_user" ] && userdel -r "$existing_user" 2>/dev/null || true; \
    existing_group=$(getent group ${HOST_GID} | cut -d: -f1 || true); \
    [ -n "$existing_group" ] && groupdel "$existing_group" 2>/dev/null || true; \
    # 创建 dev 用户
    groupadd -g ${HOST_GID} dev; \
    useradd -m -u ${HOST_UID} -g dev -s /bin/bash dev; \
    # 创建 docker 组 (匹配宿主机 docker.sock GID)
    docker_group=$(getent group ${DOCKER_GID} | cut -d: -f1 || true); \
    [ -z "$docker_group" ] && groupadd -g ${DOCKER_GID} docker && docker_group=docker; \
    usermod -aG "$docker_group" dev

# ── Python 依赖 (全局安装项目所需包) ──
COPY requirements-sandbox.txt /tmp/requirements-sandbox.txt
RUN pip install --break-system-packages --no-cache-dir -r /tmp/requirements-sandbox.txt

# ── Claude Code CLI + VS Code 扩展 ──
RUN npm install -g @anthropic-ai/claude-code

# ── 工作目录 ──
WORKDIR /workspace

# ── 切换到 dev 用户 ──
USER dev

# ── code-server 配置 ──
RUN mkdir -p /home/dev/.config/code-server && \
    cat > /home/dev/.config/code-server/config.yaml << 'EOF'
bind-addr: 127.0.0.1:8443
auth: password
password: sandbox2026
cert: false
EOF

# ── 安装 VS Code 扩展 ──
RUN code-server --install-extension ms-python.python && \
    code-server --install-extension redhat.java && \
    code-server --install-extension dbaeumer.vscode-eslint && \
    code-server --install-extension esbenp.prettier-vscode

# ── 用户级配置 ──
RUN mkdir -p /home/dev/.claude && \
    echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> /home/dev/.bashrc && \
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> /home/dev/.bashrc && \
    echo 'export PS1="\[\033[1;36m\][sandbox]\[\033[0m\] \w \$ "' >> /home/dev/.bashrc && \
    echo 'cd /workspace' >> /home/dev/.bashrc

# ── 入口脚本 ──
COPY --chown=dev:dev entrypoint.sh /home/dev/entrypoint.sh
RUN chmod +x /home/dev/entrypoint.sh

EXPOSE 8443

ENTRYPOINT ["/home/dev/entrypoint.sh"]
