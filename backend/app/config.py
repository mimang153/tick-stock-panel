"""全局配置 — 从环境变量 / .env 读取。"""

from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ── 运行环境检测 ──────────────────────────────────────────
# PyInstaller 打包后: __file__ 指向临时解压目录 _MEIPASS, 不能作为路径基准。
# 此时:
#   - 只读资源 (tiers.yaml / 前端 dist) 放在 _MEIPASS 内
#   - 可写用户数据 (data_dir) 放在可执行文件旁的用户目录
# 源码模式按项目根解析；官方 Docker 镜像显式标记其扁平 /app 布局。
_IS_FROZEN = getattr(sys, "frozen", False)
_DOCKER_MOUNTINFO = Path("/proc/self/mountinfo")
logger = logging.getLogger(__name__)


def _docker_root() -> Path | None:
    if not _IS_FROZEN and os.environ.get("TSP_DOCKER") == "1":
        return Path(__file__).resolve().parent.parent
    return None


def _docker_data_mounts() -> list[tuple[Path, str]]:
    # ismount() can miss bind mounts on the same filesystem; use Linux mountinfo.
    try:
        lines = _DOCKER_MOUNTINFO.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    mounts = []
    for line in lines:
        before, separator, after = line.partition(" - ")
        fields, filesystem = before.split(), after.split()
        if not separator or len(fields) < 5 or len(filesystem) < 3:
            continue
        point = Path(re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4]))
        if point.is_absolute():
            mounts.append((point, filesystem[0]))
    return mounts


def _resolve_data_dir(path: Path) -> Path:
    docker_root = _docker_root()
    if docker_root is None:
        return path if path.is_absolute() else (_PROJECT_ROOT / path).resolve()

    mounts = _docker_data_mounts()

    def mounted(directory: Path) -> bool:
        matches = [(point, fs) for point, fs in mounts if directory.resolve().is_relative_to(point)]
        if not matches:
            return False
        # A nested tmpfs can hide a durable parent mount.
        point, filesystem = max(matches, key=lambda item: len(item[0].parts))
        return point != Path("/") and filesystem not in {"tmpfs", "ramfs"}

    if not path.is_absolute():
        legacy = (_PROJECT_ROOT / path).resolve()
        target = (docker_root / path).resolve()
        if legacy != target:
            if mounted(legacy):
                logger.warning("Docker 相对 DATA_DIR 沿用已有挂载目录 %s", legacy)
                return legacy
            try:
                has_legacy_data = next(legacy.iterdir(), None) is not None
            except FileNotFoundError:
                has_legacy_data = False
            except OSError as exc:
                raise RuntimeError(f"无法检查 Docker 旧数据目录 {legacy}，未切换数据目录") from exc
            if has_legacy_data:
                # RuntimeError avoids Pydantic echoing settings (including keys).
                raise RuntimeError(
                    f"Docker 旧数据目录 {legacy} 中仍有文件，停止切换到 {target}。"
                    "请先备份旧容器数据并迁入持久化目录；"
                    f"如需沿用旧目录，请明确设置 DATA_DIR={legacy} 并挂载该目录。"
                    "程序不会自动迁移或覆盖文件。"
                )
        path = target
    if not mounted(path):
        logger.warning(
            "Docker 数据目录 %s 未检测到持久化挂载（或无法读取挂载信息）；"
            "请将宿主机数据目录挂载到此路径，避免升级重建容器后丢失 Key 和数据。",
            path,
        )
    return path


def _user_data_root() -> Path:
    """桌面版用户数据根目录。

    定位策略 (按优先级):
      1. 环境变量 DATA_DIR (pydantic-settings 自动注入到 settings.data_dir, 不在此处理)
      2. 打包桌面版:
         - macOS: ~/Library/Application Support/TSP —— 必须在 .app 包外。
           升级用新 TSP.app 替换旧包时 Finder 会把整个旧包移入废纸篓,
           数据放包内 (≤0.3.6 的 exe 同级 data) 等于每次覆盖安装都被清空;
           App Support 是 mac 用户数据惯例位置, 替换 .app 永不触碰,
           也规避向 /Applications 包内写入的权限问题。包内旧数据由
           recover_desktop_data() 在后端启动前恢复旧配置。
         - Windows/Linux: exe 同级的 data/ 子目录 (<安装目录>/data/)
           —— 与程序同处一个总目录 (用户选择的安装目录), 视觉直观, 便于备份/迁移。
      3. 非 frozen: 源码使用项目根 data/；官方 Docker 使用应用目录下的 data/

    为什么 Windows 不用 platformdirs 默认 (%LOCALAPPDATA%) 作为主路径:
      - 落在 C 盘系统目录, 用户不易察觉, 占系统盘空间
      - 用户期望「数据跟随程序」(便于备份/迁移)
    为什么放 {app}/data (exe 旁的 data/) 而非 {app} 外的兄弟目录:
      - 用户体验: 用户选了安装目录, 自然期望「程序和数据都在这」, 单一总目录更直观。
      - 数据安全: Inno Setup 覆盖安装(升级)时只往 {app} 写新程序文件, 不会清空
        目录里不在安装清单上的运行时文件 (data/ 即此类), 故覆盖安装不丢数据。
        (注意: 卸载时需在 .iss 中豁免 data/, 见 packaging/tsp.iss 的 [UninstallDelete]。)
    旧版本恢复由 app.services.data_dir_upgrade 处理，已有配置优先并保留原配置备份。
    """
    # 打包桌面版
    if _IS_FROZEN:
        # macOS: 数据放 .app 包外 (升级替换 App 不丢数据), 位置为 mac 惯例路径
        if sys.platform == "darwin":
            return Path.home() / "Library" / "Application Support" / "TSP"
        # Windows/Linux: exe 同级的 data/ 子目录 (与程序同一总目录, 覆盖安装不丢数据)
        exe_dir = Path(sys.executable).resolve().parent
        return exe_dir / "data"

    return (_docker_root() or _PROJECT_ROOT) / "data"


def _resource_root() -> Path:
    """只读资源根目录。

    frozen: PyInstaller 解压目录 (_MEIPASS)
    非 frozen: 项目根目录 (源码树)
    """
    if _IS_FROZEN:
        # sys._MEIPASS 是 PyInstaller 注入的解压根
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    return Path(__file__).resolve().parent.parent.parent


def _project_root() -> Path:
    """项目根目录 (非 frozen 用)。"""
    return Path(__file__).resolve().parent.parent.parent


_PROJECT_ROOT = _project_root()
_RESOURCE_ROOT = _resource_root()
_ENV_FILE = Path(
    os.environ.get(
        "TICKFLOW_ENV_FILE",
        str(_RESOURCE_ROOT / ".env") if not _IS_FROZEN else ".env",
    )
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # TickFlow
    tickflow_api_key: str = Field(default="", description="留空启用 free 模式")

    # AI
    ai_provider: str = "openai_compat"
    # OpenAI 系请求协议: chat_completions (兼容面最广) | responses (OpenAI 官方新协议)。
    ai_protocol: str = "chat_completions"
    ai_base_url: str = "https://llm.runninghub.ai/v1"
    ai_api_key: str = ""
    ai_model: str = "openai/gpt-6-astra-saver"
    ai_codex_command: str = "codex"
    ai_codex_reasoning_effort: str = ""
    # 默认浏览器风格 UA,绕过 Cloudflare 等 CDN/WAF 的 Bot 拦截(Issue #8)。
    # 用户可在 AI 设置页按需修改。
    ai_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    # AI 输出上限 (max_tokens) 与输入上下文窗口上限 (约 token)。
    # 任务级 max_tokens 会被钳制到 ai_max_output_tokens; 输入估算超出上下文窗口时给出明确报错。
    # 钳制仅封顶不放大: 现有任务最多请求 4500, 上调默认值只影响用户自行调高任务上限的场景。
    # 上下文 128000 对齐当前主流模型底线 (GPT/Claude/GLM/DeepSeek/Kimi 均 ≥128k); 可在 AI 设置里调整。
    ai_max_output_tokens: int = 16384
    ai_context_window: int = 128000
    # AI 助手工具轮次检查点: 连续 N 轮工具调用未完成时弹「继续/停止」卡; 0=不检查。
    ai_round_checkpoint: int = 100

    # Server
    host: str = "0.0.0.0"
    port: int = 3018
    log_level: str = "INFO"
    backtest_range_guard: bool = False
    backtest_matrix_disk_cache_enabled: bool = True
    backtest_matrix_cache_max_mb: int = 512
    backtest_matrix_cache_prewarm: bool = True
    backtest_matrix_cache_prewarm_years: int = 5

    # polars collect 并发闸 — polars 共享执行器在多线程并发 collect 下存在死锁
    # (上游 #24448/#25754 同族), 限流并发是社区验证的缓解手段。background 限额
    # 保证预热/增量等后台计算不占满闸位饿死页面读请求。
    polars_collect_permits: int = 4
    polars_collect_background_permits: int = 2

    # 后端自愈看门狗 — 探测 collect 闸与全局写锁, 连续失败即退出交由
    # supervisor 拉起 (见 app/watchdog.py)。误伤防护靠保守阈值。
    watchdog_enabled: bool = True
    watchdog_interval_s: float = 30.0
    watchdog_probe_timeout_s: float = 15.0
    watchdog_failure_threshold: int = 2

    # 策略批量执行 (run_all / 策略页全量跑) 的并发 worker 上限。实测 2026-09-07:
    # polars eager 操作内部已多线程并行, 外层再并发 4 worker 属超订, 41 策略
    # 299.6s 慢于串行 — 默认 1 (串行)。保留开关供配合 POLARS_MAX_THREADS 调优实验。
    strategy_run_all_workers: int = 1

    # run_all 渐进式返回: HTTP 同步等待时限 (秒)。策略按历史耗时升序执行,
    # 到点后已算完的随响应返回, 未算完的转后台继续算并逐个写入策略缓存,
    # 前端轮询 cached-summary 点亮卡片。0 = 关闭 (整段阻塞, 旧行为)。
    strategy_run_all_first_return_s: float = 15.0

    # 同步路由 (def) 线程池容量: Starlette/anyio 默认 40。看板自动刷新 +
    # 多标签页 + 长计算路由并存时 40 容易排队, 调大仅提高上限 (按需创建)。
    api_sync_thread_pool: int = 100

    # Auth — 首次启动时预置访问密码(明文, 仅用于初始化, 详见 services/auth.bootstrap_from_env)
    # 公网服务器部署时免去 SSH 端口转发设密码的麻烦。写入 auth.json(哈希)后即不再读取。
    auth_password: str = ""

    # Data — 桌面版按平台约定；源码使用项目根 data/；Docker 使用 /app/data。
    # (均可被环境变量 DATA_DIR 覆盖, pydantic-settings 自动注入)
    data_dir: Path = _user_data_root()
    # 挖掘产物共享根 — 配置后 artifact/候选库落跨实例可见目录；未配置时沿用 data/ 兼容布局。
    mining_artifact_root: Path | None = None
    # 产物存储后端 — shared-root 用共享卷；s3 用 S3 兼容对象存储。
    mining_artifact_store: Literal["shared-root", "s3"] = "shared-root"
    mining_artifact_s3_bucket: str = ""
    mining_artifact_s3_prefix: str = "mining-artifacts"
    mining_artifact_s3_endpoint_url: str | None = None

    # ClickHouse 存储层 — 方案与包络见 ../../docs/v1.0.1/design/DES-001-clickhouse-storage-design.md §4.8/§4.9。
    # CK 由 docker compose 以 clickhouse/clickhouse-server:24.8.8-alpine 启动:
    #   源码直跑 → 连宿主机映射端口 http://localhost:8123
    #   应用容器 → compose 注入 CLICKHOUSE_URL=http://clickhouse:8123(服务名)
    # 库名生产 tsp / 测试 tsp_test; tsp 用户按库授权, 应用不建库(由容器初始化脚本预建)。
    clickhouse_url: str = "http://localhost:8123"
    clickhouse_database: str = "tsp"
    clickhouse_user: str = "tsp"
    # 口令只从 CLICKHOUSE_PASSWORD 环境变量/compose 注入, 源码不内置明文默认值
    # (compose 与 .env.example 提供本地开发默认, 应用自身不持有凭据)
    clickhouse_password: str = ""
    ch_query_timeout_s: int = 60
    # 写缓冲阈值: 满足任一条件即批量 INSERT (§4.2)
    ch_write_flush_interval_ms: int = 2000
    ch_write_flush_rows: int = 5000

    # 缓存后端 — S4 起默认 redis（跨 worker 共享 + generation 计数器）；
    # memory 仅作离线/测试兼容，测试 conftest 显式强制以保证隔离。
    cache_backend: Literal["memory", "redis"] = "redis"
    cache_redis_url: str = "redis://localhost:6379/0"
    cache_redis_prefix: str = "tickflow:cache:"
    cache_redis_socket_timeout_s: float = 1.0

    # SSE 事件总线后端 — S4 起默认 redis（跨 worker 广播）；memory 仅离线/测试兼容。
    event_bus_backend: Literal["memory", "redis"] = "redis"
    event_bus_redis_url: str = "redis://localhost:6379/0"
    event_bus_redis_channel: str = "tickflow:events"
    event_bus_redis_socket_timeout_s: float = 1.0

    # 多实例协调状态 — S4 起默认 redis（跨 worker 取消标志、限流、启动单飞）；
    # memory 仅作离线/测试兼容。
    coordination_backend: Literal["memory", "redis"] = "redis"
    coordination_redis_url: str = "redis://localhost:6379/0"
    coordination_redis_prefix: str = "tickflow:coord:"
    coordination_redis_socket_timeout_s: float = 1.0

    # mining run 状态机后端 — S4 起默认 redis（方案 C，state 权威在 Redis，
    # CK 审计）；memory 文件版仅作离线/测试兼容。
    mining_store_backend: Literal["memory", "redis"] = "redis"

    # uvicorn worker 数（Docker 由 API_WORKERS 注入，默认 2）。多 worker 下
    # mining 文件版依赖进程内 flock，无法跨实例互斥，必须强制 redis 后端（DES-003 D-05）。
    api_workers: int = 1

    # 模拟盘账户事务层（DES-005）— PostgreSQL 承载资金状态，事务写入组保证多 worker
    # 账目原子。由 pg_dev.sh 用宿主机镜像启动容器（库 tsp_dev / 用户 tsp）。
    # 源码直跑连 localhost:5432；容器内 compose 注入 PAPER_PG_URL_CONTAINER。
    paper_pg_url: str = "postgresql://tsp:123456@localhost:5432/tsp_dev"

    # tiers.yaml 路径 — frozen: 资源目录内; 非 frozen: 项目根目录
    tiers_yaml: Path = _RESOURCE_ROOT / "tiers.yaml" if _IS_FROZEN else _PROJECT_ROOT / "tiers.yaml"

    # 静态文件(前端 dist) — frozen: 资源目录的 static/; 非 frozen: frontend/dist
    static_dir: Path = (
        _RESOURCE_ROOT / "static" if _IS_FROZEN else (_PROJECT_ROOT / "frontend" / "dist")
    )

    @model_validator(mode="after")
    def _resolve_paths(self) -> Settings:  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
        """Docker 相对路径按应用目录解析；源码、桌面和绝对路径契约不变。"""
        self.data_dir = _resolve_data_dir(self.data_dir)
        if self.mining_artifact_root is not None:
            self.mining_artifact_root = _resolve_data_dir(self.mining_artifact_root)
        if self.mining_store_backend == "redis" and self.mining_artifact_root is None:
            raise ValueError("MINING_ARTIFACT_ROOT 必须在 MINING_STORE_BACKEND=redis 时配置")
        if self.api_workers < 1:
            raise ValueError("API_WORKERS must be >= 1")
        # 多 worker 生产禁用 memory 文件版 mining：flock 是进程内锁，跨 worker 会重复执行。
        if self.api_workers > 1 and self.mining_store_backend == "memory":
            raise ValueError(
                "MINING_STORE_BACKEND=memory 不允许多 worker（API_WORKERS>1），请改用 redis"
            )
        if self.mining_artifact_store == "s3" and not self.mining_artifact_s3_bucket:
            raise ValueError("MINING_ARTIFACT_S3_BUCKET 必须在 MINING_ARTIFACT_STORE=s3 时配置")
        if self.backtest_matrix_cache_max_mb <= 0:
            raise ValueError("backtest_matrix_cache_max_mb must be positive")
        if self.backtest_matrix_cache_prewarm_years <= 0:
            raise ValueError("backtest_matrix_cache_prewarm_years must be positive")
        if self.ai_max_output_tokens <= 0:
            raise ValueError("ai_max_output_tokens must be positive")
        if self.ai_context_window <= 0:
            raise ValueError("ai_context_window must be positive")
        if self.polars_collect_permits < 2:
            raise ValueError("polars_collect_permits must be >= 2")
        if not 1 <= self.polars_collect_background_permits < self.polars_collect_permits:
            raise ValueError(
                "polars_collect_background_permits must be in [1, polars_collect_permits)"
            )
        if self.watchdog_interval_s <= 0 or self.watchdog_probe_timeout_s <= 0:
            raise ValueError("watchdog intervals must be positive")
        if self.watchdog_failure_threshold < 1:
            raise ValueError("watchdog_failure_threshold must be >= 1")
        if self.strategy_run_all_workers < 1:
            raise ValueError("strategy_run_all_workers must be >= 1")
        if self.strategy_run_all_first_return_s < 0:
            raise ValueError("strategy_run_all_first_return_s must be >= 0")
        if self.ch_query_timeout_s <= 0:
            raise ValueError("ch_query_timeout_s must be positive")
        if self.ch_write_flush_interval_ms <= 0:
            raise ValueError("ch_write_flush_interval_ms must be positive")
        if self.ch_write_flush_rows <= 0:
            raise ValueError("ch_write_flush_rows must be positive")
        return self

    @property
    def use_free_mode(self) -> bool:
        """是否走 Free 模式。优先看 secrets.json,其次看 .env。"""
        from app import secrets_store

        return not secrets_store.get_tickflow_key()


settings = Settings()
