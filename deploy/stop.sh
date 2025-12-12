#!/bin/bash

echo "停止Airflow服务..."
docker compose down || docker-compose down

read -p "是否删除所有数据（包括数据库）？[y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "删除所有数据..."
    docker compose down -v || docker-compose down -v
    sudo rm -rf logs/*
    echo "✓ 数据已清除"
else
    echo "✓ 服务已停止，数据保留"
fi