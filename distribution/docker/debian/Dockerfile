# -------- Stage 1: Build vector from tar --------
FROM docker.io/debian:bookworm-slim AS builder

WORKDIR /vector
COPY vector.tar.gz ./

RUN tar -xvf vector.tar.gz --strip-components=2 \
    && mkdir -p /var/lib/vector

# -------- Stage 2: Runtime with CUDA + DCGM --------
FROM nvcr.io/nvidia/cuda:12.3.2-base-ubuntu22.04

LABEL org.opencontainers.image.url="https://vector.dev"
LABEL org.opencontainers.image.source="https://github.com/vectordotdev/vector"
LABEL org.opencontainers.image.documentation="https://vector.dev/docs"

# 安装依赖 + lldpctl
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates \
      tzdata \
      wget \
      gnupg \
      curl \
      liblldpctl-dev && \
    rm -rf /var/lib/apt/lists/*

# 添加 NVIDIA DCGM 仓库并安装 DCGM
RUN wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.0-1_all.deb \
    && dpkg -i cuda-keyring_1.0-1_all.deb \
    && rm cuda-keyring_1.0-1_all.deb \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends datacenter-gpu-manager \
    && rm -rf /var/lib/apt/lists/*

# 设置 DCGM 环境变量
ENV PATH="/usr/local/dcgm/bin:${PATH}" \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility \
    NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DISABLE_REQUIRE=true

# 复制 Vector 二进制和配置
COPY --from=builder /vector/bin/* /usr/local/bin/
COPY --from=builder /vector/config/vector.yaml /etc/vector/vector.yaml
COPY --from=builder /vector/data /var/lib/vector

# 删除不必要的文件，缩减镜像体积
RUN rm -rf /usr/share/doc /usr/share/man /usr/share/info /usr/share/locale /var/cache/apt/*

# 简单测试
RUN vector --version && dcgmi -v || true

ENTRYPOINT ["/usr/local/bin/vector"]