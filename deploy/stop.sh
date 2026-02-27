#!/bin/bash

# 确保在deploy目录下执行
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 支持 -f 参数强制删除数据（非交互模式）
FORCE_CLEAN=false
while getopts "f" opt; do
    case $opt in
        f) FORCE_CLEAN=true ;;
        *) ;;
    esac
done

CLEAN_DATA=false

if [ "$FORCE_CLEAN" = true ]; then
    CLEAN_DATA=true
else
    read -p "是否删除所有数据（包括数据库）？[y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        CLEAN_DATA=true
    fi
fi

echo "停止Airflow服务..."
if [ "$CLEAN_DATA" = true ]; then
    # 停止服务并删除 named volumes（如果有的话）
    docker compose down -v 2>/dev/null || docker-compose down -v 2>/dev/null || true

    echo "删除所有数据..."

    # 删除logs目录（在父目录）
    if [ -d "../logs" ]; then
        sudo rm -rf ../logs
        mkdir -p ../logs
        echo "✓ 日志已清除"
    fi

    # 删除postgres数据（bind mount 在 deploy 目录）
    if [ -d "./pg_data" ]; then
        sudo rm -rf ./pg_data
        mkdir -p ./pg_data
        echo "✓ 数据库数据已清除"
    fi

    echo "✓ 所有数据已清除"
else
    docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true
    echo "✓ 服务已停止，数据保留"
fi
