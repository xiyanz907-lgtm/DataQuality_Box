FROM apache/airflow:2.8.1-python3.11

# 镜像元数据
LABEL maintainer="yuhong.cai@westwell-lab.com"
LABEL version="1.0"
LABEL description="Airflow for Autonomous Driving Test"

USER root

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    vim \
    net-tools \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 复制依赖文件
COPY requirements.txt /tmp/requirements.txt

# 安装Python依赖
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# 设置工作目录
WORKDIR /opt/airflow

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1