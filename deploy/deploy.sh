#!/bin/bash

set -e

echo "=========================================="
echo "   Airflow 生产环境部署脚本"
echo "   (仅部署，不构建镜像)"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查Docker
echo -e "${YELLOW}[1/6]${NC} 检查Docker环境..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker未安装${NC}"
    exit 1
fi

if ! docker compose version &> /dev/null && ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose未安装${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker环境正常${NC}"

# 检查镜像是否存在
echo -e "${YELLOW}[2/6]${NC} 检查Docker镜像..."
IMAGE_NAME="airflow-custom:2.7.1"

if ! docker images ${IMAGE_NAME} --format "{{.Repository}}:{{.Tag}}" | grep -q "^${IMAGE_NAME}$"; then
    echo -e "${RED}❌ 镜像 ${IMAGE_NAME} 不存在${NC}"
    echo ""
    echo "请先导入镜像："
    echo "  方式1 - 从tar文件导入:"
    echo "    docker load -i airflow-custom.tar"
    echo ""
    echo "  方式2 - 从私有仓库拉取:"
    echo "    docker pull your-registry.com/${IMAGE_NAME}"
    echo "    docker tag your-registry.com/${IMAGE_NAME} ${IMAGE_NAME}"
    echo ""
    exit 1
fi
echo -e "${GREEN}✓ 镜像 ${IMAGE_NAME} 已存在${NC}"

# 检查.env文件
echo -e "${YELLOW}[3/6]${NC} 检查配置文件..."
if [ ! -f .env ]; then
    echo -e "${RED}❌ .env文件不存在${NC}"
    echo "请创建.env文件并配置必要的环境变量"
    exit 1
fi

# 加载环境变量
source .env
echo -e "${GREEN}✓ 配置文件检查完成${NC}"

# 创建必要目录
echo -e "${YELLOW}[4/6]${NC} 创建必要目录..."
mkdir -p ../dags ../logs ../plugins ../config ../incoming_files ../data
mkdir -p ./pg_data

# 设置权限
echo -e "${YELLOW}[5/6]${NC} 设置目录权限..."
if ! grep -q "^AIRFLOW_UID=" .env; then
    echo "AIRFLOW_UID=$(id -u)" >> .env
fi
sudo chown -R $(id -u):0 ../dags ../logs ../plugins
echo -e "${GREEN}✓ 权限设置完成${NC}"

# 停止旧服务
echo "停止旧服务（如果存在）..."
docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true

# 启动服务
echo -e "${YELLOW}[6/6]${NC} 启动Airflow服务..."
docker compose up -d || docker-compose up -d

# 等待服务就绪
echo ""
echo "等待服务启动..."
echo -n "进度: "

for i in {1..30}; do
    if docker compose exec -T airflow curl -sf http://localhost:8080/health &> /dev/null || \
       docker-compose exec -T airflow curl -sf http://localhost:8080/health &> /dev/null; then
        echo -e "\n${GREEN}✓ Airflow服务启动成功！${NC}"
        break
    fi
    echo -n "█"
    sleep 2
    
    if [ $i -eq 30 ]; then
        echo -e "\n${RED}❌ 服务启动超时${NC}"
        echo "请检查日志: docker compose logs"
        exit 1
    fi
done

echo ""
echo "=========================================="
echo -e "${GREEN}✅ 部署完成！${NC}"
echo "=========================================="
echo ""
echo "📊 Airflow Web UI:  http://localhost:8081"
echo "👤 用户名:          ${AIRFLOW_ADMIN_USER}"
echo "🔑 密码:            ${AIRFLOW_ADMIN_PASSWORD}"
echo ""
echo "📝 常用命令:"
echo "  - 查看日志:       docker compose logs -f"
echo "  - 查看特定服务:   docker compose logs -f airflow"
echo "  - 停止服务:       docker compose down"
echo "  - 重启服务:       docker compose restart"
echo "  - 查看状态:       docker compose ps"
echo ""
echo "📂 DAG目录:         ../dags/"
echo "   将你的DAG文件放到此目录即可自动加载"
echo ""
echo "=========================================="
