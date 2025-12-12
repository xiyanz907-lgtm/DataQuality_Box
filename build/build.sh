#!/bin/bash

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=========================================="
echo "   Airflow 镜像构建脚本"
echo -e "==========================================${NC}"

# 配置变量
IMAGE_NAME="airflow-cactus"
IMAGE_TAG="latest"
REGISTRY=""  # 如果有私有镜像仓库，在这里填写，例如: registry.company.com

# 解析命令行参数
while getopts "t:r:h" opt; do
  case $opt in
    t)
      IMAGE_TAG="$OPTARG"
      ;;
    r)
      REGISTRY="$OPTARG"
      ;;
    h)
      echo "用法: ./build.sh [-t tag] [-r registry]"
      echo "  -t: 指定镜像标签 (默认: latest)"
      echo "  -r: 指定镜像仓库地址 (可选)"
      echo ""
      echo "示例:"
      echo "  ./build.sh                           # 构建 airflow-custom:latest"
      echo "  ./build.sh -t v1.0                   # 构建 airflow-custom:v1.0"
      echo "  ./build.sh -t v1.0 -r registry.io    # 构建并推送到仓库"
      exit 0
      ;;
    \?)
      echo -e "${RED}无效参数: -$OPTARG${NC}" >&2
      exit 1
      ;;
  esac
done

# 构建完整的镜像名称
if [ -n "$REGISTRY" ]; then
    FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
else
    FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo -e "${YELLOW}[1/4]${NC} 检查Docker环境..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker未安装${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker环境正常${NC}"

echo -e "${YELLOW}[2/4]${NC} 检查构建文件..."
if [ ! -f "Dockerfile" ]; then
    echo -e "${RED}❌ Dockerfile不存在${NC}"
    exit 1
fi
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}❌ requirements.txt不存在${NC}"
    exit 1
fi
echo -e "${GREEN}✓ 构建文件完整${NC}"

echo -e "${YELLOW}[3/4]${NC} 开始构建镜像: ${FULL_IMAGE_NAME}"
echo "构建时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 构建镜像
docker build \
    --build-arg BUILDTIME=$(date '+%Y-%m-%d %H:%M:%S') \
    --tag ${FULL_IMAGE_NAME} \
    --tag ${IMAGE_NAME}:latest \
    .

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ 镜像构建成功${NC}"
else
    echo -e "${RED}❌ 镜像构建失败${NC}"
    exit 1
fi

# 显示镜像信息
echo ""
echo -e "${YELLOW}[4/4]${NC} 镜像信息:"
docker images ${IMAGE_NAME} --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"

# 推送到镜像仓库（如果指定了）
if [ -n "$REGISTRY" ]; then
    echo ""
    read -p "是否推送镜像到仓库 ${REGISTRY}? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}正在推送镜像...${NC}"
        docker push ${FULL_IMAGE_NAME}
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ 镜像推送成功${NC}"
        else
            echo -e "${RED}❌ 镜像推送失败${NC}"
            exit 1
        fi
    fi
fi

echo ""
echo -e "${GREEN}=========================================="
echo "   构建完成！"
echo -e "==========================================${NC}"
echo ""
echo "镜像名称: ${FULL_IMAGE_NAME}"
echo ""
echo "后续步骤:"
echo "  1. 测试镜像: docker run --rm ${FULL_IMAGE_NAME} airflow version"
echo "  2. 部署服务: cd ../deploy && ./deploy.sh"
echo ""