FROM apache/airflow:2.7.1-python3.10

USER root

# 先清理已安装的 PostgreSQL 包，再重装
RUN rm -f /etc/apt/sources.list.d/pgdg.list \
  && sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list \
  && sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list \
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
RUN pip install --no-cache-dir --user \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    -r /tmp/requirements.txt
