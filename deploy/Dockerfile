FROM apache/airflow:2.10.4-python3.10

USER root

# Airflow 2.10.4 基于 Debian Bookworm，使用 DEB822 格式源文件
# 替换为阿里云镜像加速（兼容 Bookworm 的 sources.list.d/debian.sources）
RUN rm -f /etc/apt/sources.list.d/pgdg.list \
  && if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
       sed -i 's|http://deb.debian.org/debian|http://mirrors.aliyun.com/debian|g' /etc/apt/sources.list.d/debian.sources \
       && sed -i 's|http://deb.debian.org/debian-security|http://mirrors.aliyun.com/debian-security|g' /etc/apt/sources.list.d/debian.sources; \
     elif [ -f /etc/apt/sources.list ]; then \
       sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list \
       && sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list; \
     fi \
  && apt-get update \
  && apt-get remove -y libpq5 libpq-dev || true \
  && apt-get install -y --no-install-recommends \
         default-libmysqlclient-dev \
         libpq-dev \
         gcc \
         g++ \
         python3-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    -r /tmp/requirements.txt
