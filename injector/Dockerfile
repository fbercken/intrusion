FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    procps wget net-tools iputils-ping  dnsutils vim-tiny less jq ca-certificates \
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

COPY injector/requirements.txt .

RUN pip install --upgrade pip wheel setuptools \
    && pip install --no-cache-dir -r requirements.txt

COPY injector/app/ .


CMD ["/bin/bash"]
