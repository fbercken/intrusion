FROM nvcr.io/nvidia/deepstream:9.1-samples-multiarch

# 1. Install Python + build tools
RUN apt-get update && apt-get install -y \
    python3-gi python3-dev python3-gst-1.0 python3-pip python3-venv \
    cmake g++ build-essential libglib2.0-dev libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev libgirepository1.0-dev git \
    && rm -rf /var/lib/apt/lists/*

# 2. Create Python venv (required by NVIDIA for Python 3.12)
RUN python3 -m venv /opt/pyds --system-site-packages
ENV PATH="/opt/pyds/bin:$PATH"

# 3. Install Python packages
RUN pip install --no-cache-dir numpy opencv-python-headless

# 4. Build & install DeepStream Python bindings (pyds)
WORKDIR /tmp
RUN git clone --depth 1 https://github.com/NVIDIA-AI-IOT/deepstream_python_apps.git
WORKDIR /tmp/deepstream_python_apps/bindings
RUN pip install --no-cache-dir build pybind11 && \
    git submodule update --init && \
    python3 -m build && \
    pip install dist/pyds-*.whl

# 5. Build YOLO custom parser
WORKDIR /tmp
RUN git clone --depth 1 https://github.com/marcoslucianops/DeepStream-Yolo.git
WORKDIR /tmp/DeepStream-Yolo
ENV CUDA_VER=13.1
RUN make -C nvdsinfer_custom_impl_Yolo

# 6. Environment paths
ENV PYTHONPATH="/opt/pyds/lib/python3.12/site-packages:/tmp/deepstream_python_apps/apps:$PYTHONPATH"
ENV LD_LIBRARY_PATH="/tmp/DeepStream-Yolo/nvdsinfer_custom_impl_Yolo:/opt/nvidia/deepstream/deepstream/lib:$LD_LIBRARY_PATH"

WORKDIR /app
CMD ["/bin/bash"]
