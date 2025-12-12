#!/bin/bash

set -e

echo "=========================================="
echo "   Airflow 生产环境一键部署脚本"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查Docker
echo -e "${YELLOW}[1/8]${NC} 检查Docker环境..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker未安装${NC}"
    exit 1
fi

if ! docker compose version &> /dev/null && ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose未安装${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker环境正常${NC}"

# 检查.env文件
echo -e "${YELLOW}[2/8]${NC} 检查配置文件..."
if [ ! -f .env ]; then
    echo -e "${RED}❌ .env文件不存在${NC}"
    echo "请创建.env文件并配置必要的环境变量"
    exit 1
fi

# 加载环境变量
source .env

# 生成密钥（如果需要）
if [ -z "$AIRFLOW_SECRET_KEY" ] || [ "$AIRFLOW_SECRET_KEY" = "generate_this_with_deploy_script" ]; then
    echo -e "${YELLOW}[3/8]${NC} 生成Airflow密钥..."
    SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))" 2>/dev/null || openssl rand -base64 32)
    sed -i "s/AIRFLOW_SECRET_KEY=.*/AIRFLOW_SECRET_KEY=$SECRET_KEY/" .env
    echo -e "${GREEN}✓ 密钥已生成${NC}"
else
    echo -e "${YELLOW}[3/8]${NC} 使用已有密钥"
fi

# 创建必要目录
echo -e "${YELLOW}[4/8]${NC} 创建必要目录..."
mkdir -p dags logs plugins config

# 设置权限
echo -e "${YELLOW}[5/8]${NC} 设置目录权限..."
echo "AIRFLOW_UID=$(id -u)" >> .env
sudo chown -R $(id -u):0 dags logs plugins
echo -e "${GREEN}✓ 权限设置完成${NC}"

# 停止旧服务
echo -e "${YELLOW}[6/8]${NC} 停止旧服务..."
docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true
echo -e "${GREEN}✓ 旧服务已停止${NC}"

# 构建镜像
echo -e "${YELLOW}[7/8]${NC} 构建Docker镜像..."
docker compose build || docker-compose build
echo -e "${GREEN}✓ 镜像构建完成${NC}"

# 启动服务
echo -e "${YELLOW}[8/8]${NC} 启动Airflow服务..."
docker compose up -d || docker-compose up -d

# 等待服务就绪
echo ""
echo "等待服务启动..."
echo -n "进度: "

for i in {1..30}; do
    if docker exec airflow-webserver curl -sf http://localhost:8080/health &> /dev/null; then
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
echo "📊 Airflow Web UI:  http://localhost:8080"
echo "👤 用户名:          admin"
echo "🔑 密码:            admin"
echo ""
echo "📝 常用命令:"
echo "  - 查看日志:       docker compose logs -f"
echo "  - 查看特定服务:   docker compose logs -f airflow-scheduler"
echo "  - 停止服务:       docker compose down"
echo "  - 重启服务:       docker compose restart"
echo "  - 查看状态:       docker compose ps"
echo ""
echo "📂 DAG目录:         ./dags/"
echo "   将你的DAG文件放到此目录即可自动加载"
echo ""
echo "=========================================="