FROM docker.io/debian:bookworm-slim AS builder

WORKDIR /vector

COPY vector.tar.gz ./

RUN tar -xvf vector.tar.gz --strip-components=2

RUN mkdir -p /var/lib/vector

FROM harbor.zhejianglab.com/zjnc-tech/cuda:12.1.0-devel-ubuntu22.04

LABEL org.opencontainers.image.url="https://vector.dev"
LABEL org.opencontainers.image.source="https://github.com/vectordotdev/vector"
LABEL org.opencontainers.image.documentation="https://vector.dev/docs"

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    systemd \
    wget \
    gnupg \
    curl \
    liblldpctl-dev \
    && rm -rf /var/lib/apt/lists/*

# 添加 NVIDIA 的 CUDA 软件仓库（从 Ubuntu 20.04 版本获取 DCGM，适用于多数系统）
RUN wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.0-1_all.deb \
    && dpkg -i cuda-keyring_1.0-1_all.deb \
    && rm cuda-keyring_1.0-1_all.deb

RUN apt-get update && apt-get install -y datacenter-gpu-manager

# 设置环境变量以包含 DCGM 工具
ENV PATH="/usr/local/dcgm/bin:${PATH}"

COPY --from=builder /vector/bin/* /usr/local/bin/
COPY --from=builder /vector/config/vector.yaml /etc/vector/vector.yaml
COPY --from=builder /var/lib/vector /var/lib/vector

# Smoke test
RUN vector --version && dcgmi -v || true

ENTRYPOINT ["/usr/local/bin/vector"]