#!/bin/bash

set -e

echo "=========================================="
echo "   Airflow 镜像构建脚本"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 镜像配置
IMAGE_NAME="airflow-custom"
IMAGE_TAG="2.7.1"
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"

# 检查Docker
echo -e "${YELLOW}[1/3]${NC} 检查Docker环境..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker未安装${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker环境正常${NC}"

# 检查必要文件
echo -e "${YELLOW}[2/3]${NC} 检查必要文件..."
if [ ! -f Dockerfile ]; then
    echo -e "${RED}❌ Dockerfile不存在${NC}"
    exit 1
fi

if [ ! -f requirements.txt ]; then
    echo -e "${RED}❌ requirements.txt不存在${NC}"
    exit 1
fi
echo -e "${GREEN}✓ 必要文件检查完成${NC}"

# 构建镜像
echo -e "${YELLOW}[3/3]${NC} 构建Docker镜像..."
docker build -t ${FULL_IMAGE_NAME} .
echo -e "${GREEN}✓ 镜像构建完成${NC}"

echo ""
echo "=========================================="
echo -e "${GREEN}✅ 构建完成！${NC}"
echo "=========================================="
echo ""
echo "📦 镜像信息:"
echo "   名称: ${FULL_IMAGE_NAME}"
echo "   大小: $(docker images ${FULL_IMAGE_NAME} --format "{{.Size}}")"
echo ""
echo "📝 导出镜像命令:"
echo "   docker save ${FULL_IMAGE_NAME} -o airflow-custom.tar"
echo ""
echo "📝 推送到私有仓库命令 (可选):"
echo "   docker tag ${FULL_IMAGE_NAME} your-registry.com/${FULL_IMAGE_NAME}"
echo "   docker push your-registry.com/${FULL_IMAGE_NAME}"
echo ""
echo "=========================================="
