FROM apache/airflow:2.7.1-python3.10
USER root

# 安装编译 MySQL 和 PostgreSQL 驱动所需的系统库
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-libmysqlclient-dev \
         libpq-dev \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# 安装 Python 库
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt