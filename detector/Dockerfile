# ====================================================================
# Official DeepStream 9.1 Base Image (dGPU / Multiarch)
# ====================================================================
FROM nvcr.io/nvidia/deepstream:9.1-triton-multiarch

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies, native Kafka libraries, and OpenCV UI prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps wget curl net-tools iputils-ping  dnsutils vim-tiny less jq ca-certificates \
    python3-pip \
    python3-dev \
    build-essential \
    cmake \
    librdkafka-dev \
    libgl1 \
    libglib2.0-0 \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip, setuptools, and wheel to ensure smooth wheel building
# RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel --break-system-packages

# Install kafka-python separately
RUN pip3 install --no-cache-dir kafka-python --break-system-packages

# Install ultralytics and associated model tools
RUN pip3 install --no-cache-dir ultralytics onnx numpy --break-system-packages

# Ensure proper video driver capabilities for runtime hardware acceleration
ENV NVIDIA_DRIVER_CAPABILITIES=$NVIDIA_DRIVER_CAPABILITIES,video

COPY detector/app/ .

# Set working directory inside the container
WORKDIR /opt/app

# Default command or entrypoint
CMD ["/bin/bash"]