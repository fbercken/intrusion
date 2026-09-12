FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    procps wget curl net-tools iputils-ping  dnsutils :wqvim-tiny less jq ca-certificates \
    gcc \
    python3-dev \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY archiver/requirements.txt .

RUN pip install --upgrade pip wheel setuptools \
    && pip install --no-cache-dir -r requirements.txt

COPY archiver/app/ .


ENTRYPOINT ["python3", "video_archiver.py"]
