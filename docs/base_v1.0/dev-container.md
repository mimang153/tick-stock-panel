# 容器化开发环境（源码挂载 + 热更新 + 国内镜像源）

在容器中运行完整开发环境：**宿主机源码目录挂载进容器**，前后端热更新调试；依赖下载缓存持久化在宿主机，npm / PyPI 全部走国内镜像源。存储层 ClickHouse 也由 compose 以容器形式一起启动，宿主机无需安装 CK。

与现有部署方式的关系：

| 方式 | 代码位置 | 适用场景 |
| --- | --- | --- |
| 本文档（Dockerfile.dev + compose.dev） | 宿主机挂载进容器 | **日常开发调试**，改代码无需重建 |
| [deployment.md 方式 A/B](./deployment.md)（Dockerfile） | 烘焙进镜像 | 生产/日常自托管运行 |
| [deployment.md 方式 D](./deployment.md)（dev.sh 宿主机直跑） | 宿主机 | 宿主机已有 Python/Node 环境 |

两套 Docker 与 compose 互不影响，可并存。

## 前置要求

- Docker Engine + docker compose v2 插件
- 仅此而已 —— Python、Node、pnpm、uv 都在工具链镜像内，宿主机无需安装；ClickHouse 由 compose 的 `clickhouse` 服务提供，宿主机也无需安装

## 快速开始

```bash
./dev.container.sh
```

首次启动流程：构建工具链镜像（走国内源）→ 启动 ClickHouse 容器并等其健康 → 容器内安装依赖（pnpm/uv，走国内源并缓存到宿主机）→ 启动前后端。之后访问：

- 前端 <http://localhost:3011>（Vite dev server，`/api` 自动代理到后端）
- 后端 <http://localhost:3018>（FastAPI）
- ClickHouse <http://localhost:8123>（默认账号 `tsp` / `tsp123456`，库 `tsp`）

`Ctrl-C` 同时停止前后端（复用仓库自带 `./dev.sh` 的退出逻辑）。

> 宿主 8123 已被系统装的 ClickHouse 占用时（`systemctl status clickhouse-server` 为 active），容器端口绑定会失败。先停旧实例 `sudo systemctl disable --now clickhouse-server`；或保留旧实例，在 `.env` 设 `CLICKHOUSE_HTTP_PORT=18123` 并把 `CLICKHOUSE_URL` 改成 `http://localhost:18123`。

## 常用命令

```bash
./dev.container.sh -d            # 后台启动
./dev.container.sh logs -f       # 跟踪日志
./dev.container.sh logs -f clickhouse   # 只看 ClickHouse 日志
./dev.container.sh down          # 停止并移除容器（缓存与数据保留）
./dev.container.sh bash          # 进入 dev 容器
./dev.container.sh ck            # 进入 ClickHouse client（复用容器内账号，默认库连接）

# 容器内跑测试 / 检查（与 CONTRIBUTING.md 的验证矩阵一致）
docker exec -it tsp-dev bash -c "cd backend && uv run pytest tests/path/to/test_x.py -q"
docker exec -it tsp-dev bash -c "cd backend && uv run ruff check app/path.py"
docker exec -it tsp-dev bash -c "cd frontend && pnpm build"

# 只起 CK（例如只想跑存储层测试）
docker compose -f docker-compose.dev.yml up -d clickhouse
./dev.container.sh ck --query "SELECT count() FROM tsp.instruments"
```

## 架构与数据流

```text
宿主机                                    容器 (python:3.11-slim + Node 20 + pnpm 9 + uv)
──────────────                           ──────────────────────────────────────────────
源码仓库  ────────挂载──────▶  /app
├─ dev.container.entry.sh                 入口: 镜像源预装依赖 → 交给 dev.sh 启动
├─ backend/.venv        ←named volume    uvicorn --reload  :3018（改 .py 自动重载）
├─ frontend/node_modules←named volume    vite dev          :3011（改 .tsx HMR）
├─ data/  ────────挂载──────▶ /app/data   用户数据持久化（Parquet、配置、Key）
└─ .docker-cache/ ─挂载────▶ /root/...    依赖下载缓存（跨容器重建复用）
    ├─ pnpm-store/  → /root/.local/share/pnpm/store
    ├─ npm-cache/   → /root/.npm
    └─ uv-cache/    → /root/.cache/uv

另行: ckdata/ ──挂载──▶ /var/lib/clickhouse   CK 容器数据（tsp-dev-clickhouse）
      独立于 data/，不挂进 dev 容器，因此"清空数据"、备份打包、升级搬迁都不会碰它
```

启动顺序：`clickhouse` 容器（`/ping` 健康后）→ `dev.container.entry.sh`（后端依赖经镜像源预装 + 前端 pnpm install）→ `dev.sh`（依赖检查秒级通过 → uvicorn --reload + vite dev）。后端启动即连 CK 且 fail-closed，所以 `depends_on` 用 `service_healthy` 而非仅"容器已启动"。

### 为什么 `node_modules` / `.venv` 不直接挂宿主机

pnpm 通过硬链接从 store 生成 `node_modules`，跨 bind mount 的硬链接会失败（EXDEV）；Python venv 的二进制也与容器环境绑定。因此安装产物留在容器内 named volume（Linux 原生链接），而**包内容仓库（store）和下载缓存持久化在宿主机**——即使容器和依赖卷全部删除重建，`pnpm install` / `uv sync` 也只做链接操作，不重新下载。

## 镜像源配置

| 环节 | 镜像源 | 配置位置 |
| --- | --- | --- |
| 构建镜像：apt / uv / pnpm 安装 | 阿里云 Debian / 清华 PyPI / npmmirror | `Dockerfile.dev` 构建参数，默认启用 |
| 运行期 `pnpm install` | `registry.npmmirror.com` | compose 注入 `npm_config_registry`；pnpm-lock 不锁定 registry 地址，直接生效 |
| 后端依赖安装 | 清华主源 + 阿里云兜底 | `dev.container.entry.sh` 使用 `uv pip install -r`（见下方说明） |

### 后端依赖为什么走 `uv export` + `uv pip install`（绕开 uv.lock 的官方源地址）

`uv.lock` 把每个包的下载地址锁死为 `files.pythonhosted.org`（官方 PyPI），`uv sync --frozen` 会按锁文件里的 URL 直连官方源下载，`UV_DEFAULT_INDEX` 等镜像源环境变量**对 frozen 安装不生效**（这是 uv 的已知行为）——国内网络下实测只有约 200 KB/s。

因此容器入口脚本 `dev.container.entry.sh` 的做法是：

1. `uv export --frozen` 导出锁定的版本清单（保留 sha256 hash）
2. `uv pip install -r` 经清华/阿里镜像安装（uv 校验 hash，与锁文件内容完全一致，不改变任何锁定版本）
3. 交给 `./dev.sh` 启动——其内部的 `uv sync --frozen` 检查依赖已就位，无需再联网

> 不采用"在镜像源上重新 `uv lock`"的方案：那会把整个锁文件的 URL 重写成镜像地址，污染仓库并破坏 CI。

海外网络环境可在项目根目录 `.env` 中设 `USE_CN_MIRROR=0`，构建切回官方源（运行期镜像源为独立配置，可按需修改 `docker-compose.dev.yml` 的 environment 段）。

## 配置项（.env）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `BACKEND_EXTRAS` | `dev backtest` | 后端依赖 extras；`dev` 提供 pytest/ruff（容器内跑测试必需），老 CPU（无 AVX2）用 `dev legacy-cpu backtest`；设为空则只装必需依赖 |
| `PORT` | `3018` | 后端对外端口（前端固定 3011） |
| `USE_CN_MIRROR` | `1` | 构建期是否使用国内镜像源 |
| `NODE_VERSION` | `20.18.1` | 工具链镜像内的 Node 版本 |
| `CLICKHOUSE_HTTP_PORT` | `8123` | CK 容器映射到宿主机的 HTTP 端口（宿主端口冲突时改，如 18123） |
| `CLICKHOUSE_URL` | `http://localhost:8123` | 源码直跑连的地址；dev 容器内由 compose 改写为 `http://clickhouse:8123` |
| `CLICKHOUSE_DATABASE` | `tsp` | 库名；测试由 `tests/conftest.py` 强制为 `tsp_test` |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `tsp` / `tsp123456` | 同时用于创建 CK 用户与应用连接，两处必须一致 |

其余配置（`TICKFLOW_API_KEY`、AI Key、`AUTH_PASSWORD` 等）与 [configuration.md](./configuration.md) 一致，`.env` 存在于仓库根目录即被 `dev.sh` 自动读取。

## 边界与注意事项

- **数据持久化**：`DATA_DIR` 固定为 `/app/data` 并挂载宿主机 `data/`；`down` 重建容器不丢数据。切勿删除宿主机 `data/` 目录。
- **CK 数据持久化**：CK 容器数据在宿主机 `ckdata/`，`down` 重建容器不丢；备份/迁移只拷这一目录（含 `system` 等内部表）。
- **CK 初始化只跑一次**：`deploy/clickhouse/init/*.sql` 仅在 `/var/lib/clickhouse` 为空时执行。改了初始化脚本要重新初始化时，先停容器并清空 `ckdata/`（会丢失全部 CK 数据）。
- **CK 账号**：默认 `tsp` / `tsp123456`，由 compose 的 `CLICKHOUSE_USER/PASSWORD` 创建，初始化脚本负责建 `tsp`、`tsp_test` 库并授权；改密码后需同时清空 `ckdata/` 重新初始化（或在 CK 内 `ALTER USER`）。
- **首次密码**：`AUTH_PASSWORD` 仅在尚未设置密码时生效（与生产部署同规则），Key 存于 `data/user_data/secrets.json`。
- **自定义数据源**：跑在宿主机的数据源服务在容器内用 `host.docker.internal` 访问（compose 已配置 `extra_hosts` 映射）。
- **端口占用**：宿主机 3018/3011 被占用时容器启动失败；`PORT` 可改后端端口，前端端口如需调整修改 compose 的 `3011:3011` 映射。
- **文件属主**：容器内以 root 运行，首次启动产生的 `data/`、`.docker-cache/` 内文件属主为 root；预建目录可缓解，如需完全避免属主问题可在 compose 中加 `user: "${UID}:${GID}"`（需同步处理 venv 卷属主，非必需不做默认）。
- **性能**：Windows / macOS 的 Docker Desktop 挂载宿主机文件系统较慢，热更新延迟会明显高于 Linux 原生挂载；Linux 宿主机为最佳体验（本方案在 Linux 上验证）。
- **缓存清理**：`rm -rf .docker-cache/` 只会损失下载缓存（下次重新下载），不影响正确性；依赖卷可 `docker volume rm tsp-dev-backend-venv tsp-dev-frontend-node-modules` 后重启重建。
