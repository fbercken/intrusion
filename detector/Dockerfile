# =====================================================================
# Official DeepStream 9.1 Base Image (dGPU / Multiarch)
# =====================================================================
FROM nvcr.io/nvidia/deepstream:9.1-triton-multiarch

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies and native Kafka libraries (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    build-essential \
    librdkafka-dev \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies:
# - kafka-python (Official Apache Kafka client)
# - ultralytics (Framework supporting YOLO26 model exporting & training)
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
    kafka-python \
    ultralytics \
    onnx \
    numpy

# Ensure proper video driver capabilities for runtime hardware acceleration
ENV NVIDIA_DRIVER_CAPABILITIES=$NVIDIA_DRIVER_CAPABILITIES,video

# Set working directory inside the container
WORKDIR /opt/app

# Copy custom application scripts, DeepStream config files, or YOLO weights here
# COPY . /opt/app/

# Default command or entrypoint
CMD ["/bin/bash"]