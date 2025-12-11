#!/usr/bin/env bash
# 伪代码：
# 1) 设置健壮模式并定位项目根目录
# 2) 清理 __pycache__ 及常见临时目录
# 3) 构建镜像 port-quality-box:v1.0
# 4) 导出镜像为 tar 包
# 5) 准备 release 目录，拷贝代码、配置、模板、文档、镜像包
# 6) 打包 release 为 port_quality_box_release.zip

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

RELEASE_DIR="$ROOT_DIR/release"
IMAGE_TAG="port-quality-box:v1.0"
IMAGE_TAR="$RELEASE_DIR/port-quality-box-v1.0.tar"
ZIP_NAME="$ROOT_DIR/port_quality_box_release.zip"

# 旧逻辑（仅依赖 zip）：若缺少 zip 会直接退出
# if ! command -v zip >/dev/null 2>&1; then
#   echo "[ERROR] zip 未安装，请先安装：sudo apt-get install -y zip"
#   exit 127
# fi

HAS_ZIP=true
if ! command -v zip >/dev/null 2>&1; then
  HAS_ZIP=false
  if ! command -v python3 >/dev/null 2>&1; then
    echo "[ERROR] 未找到 zip 或 python3，请至少安装其中一个用于打包"
    exit 127
  fi
fi

echo "[1/6] 清理 __pycache__ 和临时数据..."
# 避免碰触可能有权限限制的数据目录
find "$ROOT_DIR" \
  -path "$ROOT_DIR/pg_data" -prune -o \
  -path "$ROOT_DIR/mysql_ngen_data" -prune -o \
  -name "__pycache__" -type d -print0 \
  | xargs -0 rm -rf 2>/dev/null || true
rm -rf "$ROOT_DIR/tmp" "$ROOT_DIR/.tmp" "$ROOT_DIR/temp" "$RELEASE_DIR" "$ZIP_NAME" 2>/dev/null || true

echo "[2/6] 构建镜像 $IMAGE_TAG ..."
docker build -t "$IMAGE_TAG" "$ROOT_DIR"

echo "[3/6] 导出镜像为 tar 包..."
mkdir -p "$RELEASE_DIR"
docker save "$IMAGE_TAG" -o "$IMAGE_TAR"

echo "[4/6] 拷贝代码与配置到 release/ ..."
cp -r "$ROOT_DIR/dags" "$RELEASE_DIR/"
cp -r "$ROOT_DIR/plugins" "$RELEASE_DIR/"
cp "$ROOT_DIR/docker-compose.prod.yaml" "$RELEASE_DIR/"
cp "$ROOT_DIR/.env.template" "$RELEASE_DIR/"
cp "$ROOT_DIR/README_DEPLOY.md" "$RELEASE_DIR/"
cp "$ROOT_DIR/build_release.sh" "$RELEASE_DIR/"

echo "[5/6] 生成压缩包 $ZIP_NAME ..."
cd "$ROOT_DIR"
if [ "$HAS_ZIP" = true ]; then
  zip -r "$ZIP_NAME" "$(basename "$RELEASE_DIR")"
else
  python3 - <<'PY' "$RELEASE_DIR" "$ZIP_NAME"
import os
import sys
import zipfile

release_dir = os.path.abspath(sys.argv[1])
zip_path = os.path.abspath(sys.argv[2])
root_base = os.path.dirname(release_dir)

with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    for base, _, files in os.walk(release_dir):
        for name in files:
            path = os.path.join(base, name)
            arc = os.path.relpath(path, root_base)
            zf.write(path, arc)
PY
fi

echo "[6/6] 完成！release 内容位于 $RELEASE_DIR ，压缩包为 $ZIP_NAME"

