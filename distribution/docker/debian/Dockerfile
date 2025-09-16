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

# 安装依赖 + lldpctl + systemd
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates \
      tzdata \
      wget \
      gnupg \
      curl \
      liblldpctl-dev \
      systemd && \
    rm -rf /var/lib/apt/lists/*

# 添加 NVIDIA DCGM 仓库并安装 DCGM，然后进行精简
RUN wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.0-1_all.deb \
    && dpkg -i cuda-keyring_1.0-1_all.deb \
    && rm cuda-keyring_1.0-1_all.deb \
    && apt-get -qq update \
    && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y --no-install-recommends \
      datacenter-gpu-manager \
    && apt-get -qq -y clean \
    && apt-get -qq -y autoclean \
    && apt-get -qq autoremove -y \
    && rm -rfd /usr/local/dcgm/bindings \
              /usr/local/dcgm/sdk_samples \
              /usr/share/nvidia-validation-suite \
    && rm -rf /usr/lib/x86_64-linux-gnu/libdcgm_cublas_proxy*.so \
    && rm -rf /usr/local/dcgm/scripts \
    && rm -f /usr/include/*.h \
             /usr/bin/DcgmProfTesterKernels.ptx \
             /usr/bin/dcgmproftester* \
    && rm -rf /var/cache/debconf/* \
              /var/lib/apt/lists/* \
              /var/log/* \
              /tmp/* \
              /var/tmp/* \
    && rm -rf /usr/share/doc \
              /usr/share/man \
    && ldconfig

# 设置 DCGM 环境变量
ENV PATH="/usr/local/dcgm/bin:${PATH}" \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility \
    NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DISABLE_REQUIRE=true

# 复制 Vector 二进制和配置
COPY --from=builder /vector/bin/* /usr/local/bin/
COPY --from=builder /vector/config/vector.yaml /etc/vector/vector.yaml

# 简单测试
RUN vector --version && dcgmi -v || true

ENTRYPOINT ["/usr/local/bin/vector"]