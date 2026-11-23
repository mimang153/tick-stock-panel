#!/usr/bin/env bash
# tick-stock-panel — 复用宿主机 Redis 容器的缓存运维入口
#
# 用法:
#   ./redis-cache.sh status          # 查看容器与连接地址
#   ./redis-cache.sh ping            # 在宿主容器内执行 redis-cli ping
#   ./redis-cache.sh check           # 同时做容器状态与 PING 检查（部署前用）
#   ./redis-cache.sh up              # 幂等启动/复用 .env 指定的宿主机 Redis 容器
#   ./redis-cache.sh url             # 打印源码/容器内使用的 CACHE_REDIS_URL
#   ./redis-cache.sh config          # 查看 maxmemory/淘汰策略/持久化配置
#   ./redis-cache.sh aof on|off      # 在线开/关 AOF（已批准的协调态权威配置，无需重启）
#   ./redis-cache.sh flush           # 只清本项目 tickflow:cache:* 键（需 --yes）
#   ./redis-cache.sh exec ...        # 在宿主 Redis 容器里执行 redis-cli 命令
#
# 约定: 不由本项目 compose 创建 Redis；`up` 仅在容器不存在时用宿主机已有镜像创建，
# 已存在则只启动/复用，不重建、不改配置。
# 变量名可从 .env 读取: REDIS_CONTAINER / REDIS_IMAGE / REDIS_PORT / REDIS_DB /
# REDIS_PASSWORD / REDIS_MAXMEMORY / REDIS_MAXMEMORY_POLICY / CACHE_REDIS_URL /
# CACHE_REDIS_URL_CONTAINER / CACHE_REDIS_PREFIX。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BLUE='\033[0;34m'
NC='\033[0m'
info() { echo -e "${BLUE}[redis-cache]${NC} $*"; }
die() { echo -e "${BLUE}[redis-cache]${NC} 错误: $*" >&2; exit 1; }

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

REDIS_CONTAINER="${REDIS_CONTAINER:-$(read_dotenv_value REDIS_CONTAINER '')}"
REDIS_IMAGE="${REDIS_IMAGE:-$(read_dotenv_value REDIS_IMAGE 'redis:8-alpine')}"
REDIS_PORT="${REDIS_PORT:-$(read_dotenv_value REDIS_PORT 6379)}"
REDIS_DB="${REDIS_DB:-$(read_dotenv_value REDIS_DB 0)}"
REDIS_PASSWORD="${REDIS_PASSWORD:-$(read_dotenv_value REDIS_PASSWORD '')}"
REDIS_MAXMEMORY="${REDIS_MAXMEMORY:-$(read_dotenv_value REDIS_MAXMEMORY '256mb')}"
REDIS_MAXMEMORY_POLICY="${REDIS_MAXMEMORY_POLICY:-$(read_dotenv_value REDIS_MAXMEMORY_POLICY 'noeviction')}"
CACHE_REDIS_URL="${CACHE_REDIS_URL:-$(read_dotenv_value CACHE_REDIS_URL "redis://localhost:${REDIS_PORT}/${REDIS_DB}")}"
CACHE_REDIS_URL_CONTAINER="${CACHE_REDIS_URL_CONTAINER:-$(read_dotenv_value CACHE_REDIS_URL_CONTAINER "redis://host.docker.internal:${REDIS_PORT}/${REDIS_DB}")}"
CACHE_REDIS_PREFIX="${CACHE_REDIS_PREFIX:-$(read_dotenv_value CACHE_REDIS_PREFIX 'tickflow:cache:')}"

resolve_container() {
  if [ -n "$REDIS_CONTAINER" ]; then
    docker ps -a --format '{{.Names}}' | grep -qx "$REDIS_CONTAINER" && {
      echo "$REDIS_CONTAINER"
      return 0
    }
    die ".env 配置的 REDIS_CONTAINER=$REDIS_CONTAINER 不存在"
  fi
  # 未显式配置时按镜像名自动发现一个已有 Redis 容器（不创建、不修改）。
  local found
  found="$(docker ps -a --filter 'ancestor=redis' --format '{{.Names}}' | head -n 1 || true)"
  [ -n "$found" ] || die "未发现 Redis 容器；请在 .env 配置 REDIS_CONTAINER"
  echo "$found"
}

container_exists() {
  docker inspect "$1" >/dev/null 2>&1
}

container_running() {
  docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -qx true
}

detect_redis_image() {
  [ -n "$REDIS_IMAGE" ] && return 0
  for image in redis:8-alpine redis:7-alpine redis:6-alpine; do
    if docker image inspect "$image" >/dev/null 2>&1; then
      REDIS_IMAGE="$image"
      return 0
    fi
  done
  return 1
}

wait_for_ping() {
  local container="$1" attempt
  for attempt in $(seq 1 30); do
    if redis_exec "$container" ping 2>/dev/null | tr -d '\r' | grep -qx PONG; then
      return 0
    fi
    sleep 1
  done
  return 1
}

redis_up() {
  require_docker
  [ -n "$REDIS_CONTAINER" ] || die "请先配置 REDIS_CONTAINER（如 tsp-requer）"
  detect_redis_image || die "未发现 Redis 镜像；请在 .env 配置 REDIS_IMAGE"

  if container_exists "$REDIS_CONTAINER"; then
    if ! container_running "$REDIS_CONTAINER"; then
      info "启动已有容器 $REDIS_CONTAINER"
      docker start "$REDIS_CONTAINER" >/dev/null
    else
      info "容器 $REDIS_CONTAINER 已在运行"
    fi
  else
    info "用镜像 $REDIS_IMAGE 创建容器 $REDIS_CONTAINER（端口 $REDIS_PORT）"
    docker run -d \
      --name "$REDIS_CONTAINER" \
      --restart unless-stopped \
      -p "${REDIS_PORT}:6379" \
      -e REDISCLI_AUTH="$REDIS_PASSWORD" \
      "$REDIS_IMAGE" \
      redis-server \
      --requirepass "$REDIS_PASSWORD" \
      --save '' \
      --appendonly yes \
      --appendfsync everysec \
      --maxmemory "$REDIS_MAXMEMORY" \
      --maxmemory-policy "$REDIS_MAXMEMORY_POLICY" \
      >/dev/null
  fi

  wait_for_ping "$REDIS_CONTAINER" || die "容器已启动但 PING 失败；检查密码/端口/已有容器配置"
  info "Redis 就绪: $CACHE_REDIS_URL"
}

redis_exec() {
  local container="$1"; shift
  if [ -n "$REDIS_PASSWORD" ]; then
    docker exec -e REDISCLI_AUTH="$REDIS_PASSWORD" "$container" \
      redis-cli -p "$REDIS_PORT" -n "$REDIS_DB" "$@"
  else
    docker exec "$container" redis-cli -p "$REDIS_PORT" -n "$REDIS_DB" "$@"
  fi
}

status() {
  require_docker
  local container
  container="$(resolve_container)"
  info "容器:       $container"
  info "运行状态:   $(docker inspect -f '{{.State.Status}}' "$container")"
  info "宿主 URL:   $CACHE_REDIS_URL"
  info "容器 URL:   $CACHE_REDIS_URL_CONTAINER"
  info "键前缀:     $CACHE_REDIS_PREFIX"
}

ping_redis() {
  require_docker
  local container reply
  container="$(resolve_container)"
  container_running "$container" || die "Redis 容器 $container 未运行，请先在宿主机启动它"
  reply="$(redis_exec "$container" ping | tr -d '\r')"
  [ "$reply" = "PONG" ] || die "Redis PING 返回 $reply（检查 REDIS_PASSWORD / REDIS_DB / 端口）"
  info "Redis PING: $reply"
}

case "${1:-status}" in
  up)
    redis_up
    ;;
  status)
    status
    ;;
  ping)
    ping_redis
    ;;
  check)
    status
    ping_redis
    ;;
  url)
    echo "CACHE_REDIS_URL=$CACHE_REDIS_URL"
    echo "CACHE_REDIS_URL_CONTAINER=$CACHE_REDIS_URL_CONTAINER"
    ;;
  config)
    require_docker
    container="$(resolve_container)"
    container_running "$container" || die "Redis 容器 $container 未运行"
    info "当前策略（DES-004 §2.3 已批准）: maxmemory=$REDIS_MAXMEMORY；maxmemory-policy=$REDIS_MAXMEMORY_POLICY；appendonly=yes everysec（协调态权威）"
    redis_exec "$container" config get maxmemory maxmemory-policy appendonly appendfsync save
    ;;
  aof)
    require_docker
    mode="${2:-}"
    case "$mode" in
      on)
        container="$(resolve_container)"
        container_running "$container" || die "Redis 容器 $container 未运行"
        info "在线开启 AOF（appendfsync everysec），无需重启"
        redis_exec "$container" config set appendonly yes
        redis_exec "$container" config set appendfsync everysec
        redis_exec "$container" config get appendonly appendfsync
        ;;
      off)
        container="$(resolve_container)"
        container_running "$container" || die "Redis 容器 $container 未运行"
        info "关闭 AOF（仅回退场景；协调态将失去持久化保证）"
        redis_exec "$container" config set appendonly no
        redis_exec "$container" config get appendonly
        ;;
      *)
        die "用法: ./redis-cache.sh aof on|off"
        ;;
    esac
    ;;
  flush)
    [ "${2:-}" = "--yes" ] || die "清缓存只删除 '$CACHE_REDIS_PREFIX*'，确认请执行: ./redis-cache.sh flush --yes"
    require_docker
    container="$(resolve_container)"
    container_running "$container" || die "Redis 容器 $container 未运行"
    # --scan 避免 KEYS 阻塞；xargs -r 兼容空结果。
    if [ -n "$REDIS_PASSWORD" ]; then
      docker exec -e REDISCLI_AUTH="$REDIS_PASSWORD" "$container" sh -c \
        'redis-cli -p "$1" -n "$2" --scan --pattern "$3*" | xargs -r redis-cli -p "$1" -n "$2" del' \
        sh "$REDIS_PORT" "$REDIS_DB" "$CACHE_REDIS_PREFIX"
    else
      docker exec "$container" sh -c \
        'redis-cli -p "$1" -n "$2" --scan --pattern "$3*" | xargs -r redis-cli -p "$1" -n "$2" del' \
        sh "$REDIS_PORT" "$REDIS_DB" "$CACHE_REDIS_PREFIX"
    fi
    info "已清除 $CACHE_REDIS_PREFIX*"
    ;;
  exec)
    require_docker
    shift
    container="$(resolve_container)"
    container_running "$container" || die "Redis 容器 $container 未运行"
    redis_exec "$container" "$@"
    ;;
  *)
    echo "用法: ./redis-cache.sh {up|status|ping|check|url|config|flush --yes|exec ...}" >&2
    exit 2
    ;;
esac
