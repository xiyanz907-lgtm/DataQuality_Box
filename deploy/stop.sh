#!/bin/bash

# 确保在deploy目录下执行
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "停止Airflow服务..."
docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true

read -p "是否删除所有数据（包括数据库）？[y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "删除所有数据..."
    docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true
    
    # 删除logs目录（在父目录）
    if [ -d "../logs" ]; then
        sudo rm -rf ../logs/* 2>/dev/null || true
        echo "✓ 日志已清除"
    fi
    
    # 删除postgres数据卷（在deploy目录）
    if [ -d "./pg_data" ]; then
        sudo rm -rf ./pg_data/* 2>/dev/null || true
        echo "✓ 数据库数据已清除"
    fi
    
    echo "✓ 所有数据已清除"
else
    echo "✓ 服务已停止，数据保留"
fi