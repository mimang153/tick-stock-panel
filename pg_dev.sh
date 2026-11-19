#!/usr/bin/env bash
# tick-stock-panel — 复用宿主机已有镜像的 PostgreSQL 开发容器运维入口（DES-005）
#
# 用法:
#   ./pg_dev.sh status          # 查看容器与连接地址
#   ./pg_dev.sh ping            # 在容器内执行 pg_isready
#   ./pg_dev.sh check           # 容器状态 + 连接检查（部署前用）
#   ./pg_dev.sh up              # 幂等创建/启动容器（用宿主机已有镜像）
#   ./pg_dev.sh url             # 打印源码使用的 PAPER_PG_URL
#   ./pg_dev.sh url-container   # 打印容器内使用的 PAPER_PG_URL_CONTAINER
#   ./pg_dev.sh init            # 建库 tsp_dev + 用户 tsp/123456（幂等）
#   ./pg_dev.sh psql ...        # 在容器内以 tsp 用户连 tsp_dev 执行 psql
#   ./pg_dev.sh down            # 停止容器（不删数据卷）
#
# 约定: 不由本项目 compose 创建 PG；`up` 只在容器不存在时用宿主机已有镜像
# postgres:14-alpine 创建，已存在则只启动/复用，不重建。数据落在命名卷
# tsp-pgdata（容器删除也不丢库）。库/账号由 `init` 幂等创建，应用不建库。
# 变量名从 .env 读取（可覆盖）: PG_CONTAINER / PG_IMAGE / PG_PORT /
# PG_DATABASE / PG_USER / PG_PASSWORD / PAPER_PG_URL / PAPER_PG_URL_CONTAINER。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BLUE='\033[0;34m'
NC='\033[0m'
info() { echo -e "${BLUE}[pg_dev]${NC} $*"; }
die() { echo -e "${BLUE}[pg_dev]${NC} 错误: $*" >&2; exit 1; }

read_dotenv_value() {
  local key="$1" fallback="$2" value=""
  if [ -f "$ROOT/.env" ]; then
    value="$(awk -v wanted="$key" '
      $0 ~ "^[[:space:]]*" wanted "[[:space:]]*=" {
        sub(/^[^=]*=/, ""); sub(/[[:space:]]+#.*$/, "");
        gsub(/^[[:space:]]+|[[:space:]]+$/, ""); gsub(/^"|"$/, ""); print; exit
      }' "$ROOT/.env")"
  fi
  echo "${value:-$fallback}"
}

require_docker() {
  command -v docker >/dev/null 2>&1 || die "未安装 docker"
  docker info >/dev/null 2>&1 || die "docker daemon 不可用"
}

PG_CONTAINER="${PG_CONTAINER:-$(read_dotenv_value PG_CONTAINER 'tsp-pg-dev')}"
PG_IMAGE="${PG_IMAGE:-$(read_dotenv_value PG_IMAGE 'postgres:14-alpine')}"
PG_PORT="${PG_PORT:-$(read_dotenv_value PG_PORT 5432)}"
PG_DATABASE="${PG_DATABASE:-$(read_dotenv_value PG_DATABASE 'tsp_dev')}"
PG_USER="${PG_USER:-$(read_dotenv_value PG_USER 'tsp')}"
PG_PASSWORD="${PG_PASSWORD:-$(read_dotenv_value PG_PASSWORD '123456')}"
PG_VOLUME="${PG_VOLUME:-$(read_dotenv_value PG_VOLUME 'tsp-pgdata')}"
PAPER_PG_URL="${PAPER_PG_URL:-$(read_dotenv_value PAPER_PG_URL "postgresql://${PG_USER}:${PG_PASSWORD}@localhost:${PG_PORT}/${PG_DATABASE}")}"
PAPER_PG_URL_CONTAINER="${PAPER_PG_URL_CONTAINER:-$(read_dotenv_value PAPER_PG_URL_CONTAINER "postgresql://${PG_USER}:${PG_PASSWORD}@host.docker.internal:${PG_PORT}/${PG_DATABASE}")}"

container_exists() {
  docker inspect "$1" >/dev/null 2>&1
}

container_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

detect_pg_image() {
  docker image inspect "$PG_IMAGE" >/dev/null 2>&1 && return 0
  # 未配置镜像不存在时，尝试按 postgres 前缀自动发现一个已有镜像（不拉取）。
  local found
  found="$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -E '(^|/)postgres:' | head -n 1 || true)"
  [ -n "$found" ] || return 1
  PG_IMAGE="$found"
  info "自动发现镜像: $PG_IMAGE"
}

pg_exec() {
  # 以 postgres 超级用户在容器内执行 psql（-c 或 stdin）
  docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    psql -U postgres -d postgres "$@"
}

pg_exec_tsp() {
  docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    psql -U "$PG_USER" -d "$PG_DATABASE" "$@"
}

wait_ready() {
  local i
  for i in $(seq 1 30); do
    if docker exec "$PG_CONTAINER" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

pg_up() {
  require_docker
  detect_pg_image || die "未发现 PostgreSQL 镜像；请在 .env 配置 PG_IMAGE"
  if container_exists "$PG_CONTAINER"; then
    if ! container_running "$PG_CONTAINER"; then
      info "启动已有容器 $PG_CONTAINER"
      docker start "$PG_CONTAINER" >/dev/null
    else
      info "容器 $PG_CONTAINER 已在运行"
    fi
  else
    info "用镜像 $PG_IMAGE 创建容器 $PG_CONTAINER（端口 $PG_PORT，卷 $PG_VOLUME）"
    docker run -d \
      --name "$PG_CONTAINER" \
      --restart unless-stopped \
      -p "${PG_PORT}:5432" \
      -e POSTGRES_PASSWORD="$PG_PASSWORD" \
      -v "${PG_VOLUME}:/var/lib/postgresql/data" \
      "$PG_IMAGE" >/dev/null
  fi
  wait_ready || die "容器已启动但 pg_isready 失败；检查镜像/端口/已有容器配置"
  info "PostgreSQL 就绪: $PAPER_PG_URL"
}

pg_init() {
  require_docker
  container_running "$PG_CONTAINER" || die "PG 容器 $PG_CONTAINER 未运行，请先 ./pg_dev.sh up"
  # 幂等：角色不存在则建；库不存在则建（属主 = tsp）。
  pg_exec -tAc "SELECT 1 FROM pg_roles WHERE rolname='${PG_USER}'" | grep -q 1 \
    || pg_exec -c "CREATE ROLE ${PG_USER} LOGIN PASSWORD '${PG_PASSWORD}'"
  pg_exec -tAc "SELECT 1 FROM pg_database WHERE datname='${PG_DATABASE}'" | grep -q 1 \
    || pg_exec -c "CREATE DATABASE ${PG_DATABASE} OWNER ${PG_USER}"
  # 已存在角色/库时同步口令与属主，保证 .env 凭据生效。
  pg_exec -c "ALTER ROLE ${PG_USER} WITH LOGIN PASSWORD '${PG_PASSWORD}'" >/dev/null
  pg_exec -c "ALTER DATABASE ${PG_DATABASE} OWNER TO ${PG_USER}" >/dev/null
  info "库/账号就绪: ${PG_DATABASE} / ${PG_USER}"
}

status() {
  require_docker
  info "容器:       $PG_CONTAINER"
  if container_exists "$PG_CONTAINER"; then
    info "运行状态:   $(docker inspect -f '{{.State.Status}}' "$PG_CONTAINER")"
  else
    info "运行状态:   不存在"
  fi
  info "镜像:       $PG_IMAGE"
  info "数据卷:     $PG_VOLUME"
  info "库/账号:    $PG_DATABASE / $PG_USER"
  info "宿主 URL:   $PAPER_PG_URL"
  info "容器 URL:   $PAPER_PG_URL_CONTAINER"
}

ping_pg() {
  require_docker
  container_running "$PG_CONTAINER" || die "PG 容器 $PG_CONTAINER 未运行，请先 ./pg_dev.sh up"
  docker exec "$PG_CONTAINER" pg_isready -U postgres -d postgres
}

case "${1:-status}" in
  up)          pg_up ;;
  init)        pg_init ;;
  status)      status ;;
  ping)        ping_pg ;;
  check)       status; ping_pg; pg_exec_tsp -tAc 'SELECT 1' >/dev/null && info "tsp 用户连接 OK" ;;
  url)         echo "$PAPER_PG_URL" ;;
  url-container) echo "$PAPER_PG_URL_CONTAINER" ;;
  psql)        shift; pg_exec_tsp "$@" ;;
  down)
    require_docker
    container_exists "$PG_CONTAINER" || die "容器 $PG_CONTAINER 不存在"
    docker stop "$PG_CONTAINER" >/dev/null && info "已停止 $PG_CONTAINER（数据卷 $PG_VOLUME 保留）"
    ;;
  *)
    echo "用法: $0 {up|init|status|ping|check|url|url-container|psql ...|down}" >&2
    exit 2
    ;;
esac
