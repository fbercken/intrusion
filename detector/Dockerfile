FROM nvcr.io/nvidia/deepstream:9.1-triton-multiarch

WORKDIR /app

# Install system utilities and cv2 prerequisites
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

ENV NVIDIA_DRIVER_CAPABILITIES=${NVIDIA_DRIVER_CAPABILITIES},video


ENTRYPOINT ["python3", "video_archiver.py"]
