#!/usr/bin/env bash
# tick-stock-panel — 容器开发环境入口(由 docker-compose.dev.yml 调用)
#
# 为什么不直接跑 ./dev.sh:
#   uv.lock 把每个包的下载地址锁死为 files.pythonhosted.org(官方源),
#   `uv sync --frozen` 按锁文件地址直连官方 PyPI 下载, UV_DEFAULT_INDEX 等
#   镜像源环境变量对 frozen 安装不生效 —— 国内网络下极慢(实测 <200 KB/s)。
#
# 解法: `uv export --frozen` 导出锁定的版本清单(含 sha256 hash),
# 经国内镜像源安装(uv pip install 会做 hash 校验, 与锁文件内容完全一致,
# 不改变任何锁定版本), 之后交给 ./dev.sh 正常启动 —— 其内部的
# `uv sync --frozen` 检查依赖已全部就位, 无需再联网下载。
#
# 前端 pnpm-lock 不锁定 registry 地址, dev.sh 的 pnpm install 直接走
# npmmirror(compose 注入 npm_config_registry), 无需特殊处理。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m'

info() { echo -e "${BLUE}[dev-container]${NC} $*"; }

# 与 dev.sh 同源的 BACKEND_EXTRAS 解析: compose 注入的环境变量优先,
# 未导出时回退读 .env(与 dev.sh 的 read_dotenv_value 同一节仅取一行)。
if [ -z "${BACKEND_EXTRAS+x}" ] && [ -f "$ROOT/.env" ]; then
  BACKEND_EXTRAS="$(awk -v wanted="BACKEND_EXTRAS" '
    $0 ~ "^[[:space:]]*" wanted "[[:space:]]*=" {
      sub(/^[^=]*=/, ""); sub(/[[:space:]]+#.*$/, "");
      gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print; exit
    }' "$ROOT/.env")"
fi

EXTRA_ARGS=()
if [ -n "${BACKEND_EXTRAS:-}" ]; then
  for extra in $BACKEND_EXTRAS; do
    EXTRA_ARGS+=(--extra "$extra")
  done
  info "后端 extras: $BACKEND_EXTRAS"
fi

cd "$ROOT/backend"

# venv 解释器不存在则创建(named volume 首次挂载会留下空 .venv 目录,
# 不能只判目录存在); 已存在时 uv venv 仅确认复用, 不覆盖
if [ ! -x .venv/bin/python ]; then
  info "创建虚拟环境 .venv ..."
  uv venv
fi

info "按 uv.lock 经国内镜像源同步后端依赖(sha256 校验, 首次约 1-3 分钟)..."
uv export --frozen --no-emit-project \
  "${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}" -o /tmp/backend-pins.txt
uv pip install --python .venv/bin/python -r /tmp/backend-pins.txt
info "后端依赖就绪"

# 前端: 无条件 pnpm install(幂等)。不能依赖 dev.sh 的目录存在性判断 ——
# named volume 首次挂载会创建空 node_modules 目录, 导致 dev.sh 误判已安装。
# pnpm-lock 不锁定 registry 地址, 直接走 compose 注入的 npmmirror。
cd "$ROOT/frontend"
info "同步前端依赖(npmmirror)..."
pnpm install
info "前端依赖就绪"

exec "$ROOT/dev.sh"
