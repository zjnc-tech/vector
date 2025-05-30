FROM ghcr.io/cross-rs/x86_64-unknown-linux-gnu:0.2.5

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    wget \
    gnupg \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN sed -i 's/https:\/\/mirrors.aliyun.com/http:\/\/mirrors.cloud.aliyuncs.com/g' /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    liblldpctl-dev \
    pkg-config \
    build-essential \
    libc6-dev \
    libclang-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/cross/bootstrap-ubuntu.sh scripts/environment/install-protoc.sh /
RUN /bootstrap-ubuntu.sh && bash /install-protoc.sh

# 安装 NVIDIA CUDA 网络存储库密钥和源
RUN wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.0-1_all.deb \
    && dpkg -i cuda-keyring_1.0-1_all.deb \
    && rm cuda-keyring_1.0-1_all.deb

RUN apt-get update

# 安装 DCGM datacenter-gpu-manager 包
RUN apt-get install -y datacenter-gpu-manager

ENV PATH="/usr/local/dcgm/bin:${PATH}"

RUN dcgmi -v