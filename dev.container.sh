#!/usr/bin/env bash
# tick-stock-panel — 容器化开发环境一键入口
#
# 用法:
#   ./dev.container.sh               # 构建镜像并启动(含 ClickHouse, 前台, Ctrl-C 停止)
#   ./dev.container.sh -d            # 后台启动, 用 ./dev.container.sh logs -f 看日志
#   ./dev.container.sh down          # 停止并移除容器(缓存与数据保留)
#   ./dev.container.sh logs -f       # 跟踪日志
#   ./dev.container.sh ck            # 进 ClickHouse client(库 tsp, 账号见 .env)
#   ./dev.container.sh bash          # 进容器调试(其他命令原样透传)
#
# 文档: docs/dev-container.md

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.dev.yml"
CACHE_DIR="$ROOT/.docker-cache"

BLUE='\033[0;34m'
NC='\033[0m'

info() { echo -e "${BLUE}[dev-container]${NC} $*"; }

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "错误: 未安装 docker" >&2
    echo "       安装方式: https://docs.docker.com/engine/install/" >&2
    exit 1
  fi
  if ! docker compose version >/dev/null 2>&1; then
    echo "错误: 缺少 docker compose 插件(v2)" >&2
    echo "       安装方式: https://docs.docker.com/compose/install/" >&2
    exit 1
  fi
}

require_docker

# 读 .env 中单个键(与 dev.container.entry.sh 同样的宽松解析: 允许行尾注释)
read_dotenv_value() {
  local key="$1" fallback="$2" value=""
  if [ -f "$ROOT/.env" ]; then
    value="$(awk -v wanted="$key" '
      $0 ~ "^[[:space:]]*" wanted "[[:space:]]*=" {
        sub(/^[^=]*=/, ""); sub(/[[:space:]]+#.*$/, "");
        gsub(/^[[:space:]]+|[[:space:]]+$/, ""); gsub(/^["'"'"']|["'"'"']$/, ""); print; exit
      }' "$ROOT/.env")"
  fi
  echo "${value:-$fallback}"
}

CK_HTTP_PORT="$(read_dotenv_value CLICKHOUSE_HTTP_PORT 8123)"

# 预建缓存目录, 避免首个实例以 root 属主自动创建后宿主机用户操作不便
mkdir -p "$CACHE_DIR/pnpm-store" "$CACHE_DIR/npm-cache" "$CACHE_DIR/uv-cache"
# CK 数据目录(容器内 /var/lib/clickhouse); 预先建好可避免首次启动权限/挂载报错
mkdir -p "$ROOT/ckdata"

# 宿主端口被别的 ClickHouse(例如 systemd clickhouse-server)占用时, 容器端口绑定会失败。
# 只提示不阻断, 由用户决定停旧实例或在 .env 换端口。
port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    [ -n "$(ss -ltnH "sport = :$port" 2>/dev/null)" ]
  elif command -v netstat >/dev/null 2>&1; then
    netstat -ltn 2>/dev/null | grep -q ":$port "
  else
    return 1
  fi
}

warn_ck_port() {
  # 本项目 CK 容器本身占用该端口是正常情况(重复启动/重启), 不算冲突
  if docker ps --filter "publish=${CK_HTTP_PORT}" --format '{{.Names}}' 2>/dev/null \
    | grep -qx 'tsp-dev-clickhouse'; then
    return 0
  fi
  if port_in_use "$CK_HTTP_PORT"; then
    echo "警告: 宿主 ${CK_HTTP_PORT} 端口已被占用(可能是系统装的 ClickHouse)。" >&2
    echo "      停掉旧实例: sudo systemctl disable --now clickhouse-server" >&2
    echo "      或换端口:   .env 里设 CLICKHOUSE_HTTP_PORT=18123 并把 CLICKHOUSE_URL 改成 http://localhost:18123" >&2
  fi
}

# up 类操作加 --build(工具链镜像基本不变, 有层缓存时秒级);
# 其他子命令(down/logs/exec/ps...)原样透传给 docker compose。
case "${1:-up}" in
  up|start|""|-[a-zA-Z]*)
    # up 及其旗标(-d 等)统一走启动分支; 其余子命令(down/logs/exec/ps...)原样透传
    if [ "${1:-up}" = "-d" ] || [ "${1:-up}" = "--detach" ]; then
      info "后台启动, 查看日志: ./dev.container.sh logs -f"
    else
      info "启动容器化开发环境(首次会构建镜像并安装依赖, 走国内镜像源)..."
      info "后端   http://localhost:${PORT:-3018}"
      info "前端   http://localhost:3011"
      info "CK     http://localhost:${CK_HTTP_PORT} (账号见 .env, 默认 tsp/tsp123456)"
      info "Ctrl-C 停止 | 进容器调试: docker exec -it tsp-dev bash"
    fi
    warn_ck_port
    echo
    exec docker compose -f "$COMPOSE_FILE" up --build "$@"
    ;;
  ck)
    # CK 容器内已注入账号/默认库, 直接复用, 避免脚本重复维护密码
    shift
    exec docker compose -f "$COMPOSE_FILE" exec -T clickhouse \
      sh -c 'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DATABASE" "$@"' sh "$@"
    ;;
  bash|sh)
    shift
    exec docker compose -f "$COMPOSE_FILE" exec dev bash "$@"
    ;;
  *)
    exec docker compose -f "$COMPOSE_FILE" "$@"
    ;;
esac
