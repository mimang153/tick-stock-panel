"""FastAPI 入口。"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from app import __version__
from app.api import (
    abnormal,
    alerts,
    analysis,
    backtest,
    config_backup,
    data,
    definitions,
    events,
    ext_data,
    ext_pages,
    factors,
    financials,
    guardian,
    import_center,
    indices,
    intraday,
    kline,
    lab,
    lots,
    market_recap,
    mining,
    monitor_rules,
    overview,
    pages_admin,
    paper,
    pipeline,
    position_policies,
    regime,
    rps,
    screener,
    sector_rotation,
    signals,
    stock_analysis,
    strategy,
    strategy_pool,
    task_registry,
    watchlist,
)
from app.api import auth as auth_api
from app.api import settings as settings_api
from app.api import upgrade_guard as upgrade_guard_api
from app.api.routes import router as core_router
from app.config import settings
from app.data_providers.base import ProviderRequestError
from app.enriched_generation import EnrichedGenerationUnavailableError
from app.extensions.loader import (
    configure_backend_extensions,
    current_extension_context,
    start_backend_extensions,
)
from app.frontend_static import FrontendStaticFiles, frontend_file_response
from app.jobs import daily_pipeline
from app.services.daily_health import DailyIntegrityError
from app.services.heavy_job_limiter import HeavyJobLimitTimeoutError
from app.services.matrix_prewarm_owner import MatrixCachePrewarmOwner
from app.services.mining_process_lock import MiningProcessLock, MiningProcessLockError
from app.services.quote_service import QuoteService
from app.services.shared_work import WorkBusyError
from app.startup_diagnostics import startup_phase
from app.tickflow import client as tf_client
from app.tickflow.policy import detect_capabilities
from app.tickflow.repository import DataStore, KlineRepository

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# 追加文件日志: uvicorn (含 --reload 开发模式) 默认只有 StreamHandler, 同步/管道等
# 运行时日志仅出现在 dev 终端, 关掉或滚屏后即丢失, 排查「同步后日志没落」时无处可查。
# 落盘到 data/logs/backend.log 与桌面版 (desktop.py:_setup_logging → logs/desktop.log)
# 行为对齐, 事后可查。桌面版 (frozen) 已由 desktop.py 写 desktop.log, 此处跳过避免
# 重复落盘; 大小切割与旧位置迁移见 app/logfile.py。
if not getattr(sys, "frozen", False):
    from app.logfile import attach_rotating_log

    attach_rotating_log("backend.log", max_mb=10, backup_count=3)


@dataclass
class _LifespanState:
    """_application_lifespan 交付给关闭阶段的最小状态。"""

    repo: KlineRepository
    matrix_prewarm_owner: MatrixCachePrewarmOwner


def _lifespan_early() -> None:
    """同步路由线程池容量 + 用 AUTH_PASSWORD 初始化密码 (可失败降级)。"""
    try:
        import anyio.to_thread

        anyio.to_thread.current_default_thread_limiter().total_tokens = (
            settings.api_sync_thread_pool
        )
        logger.info("sync route thread pool: %d", settings.api_sync_thread_pool)
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("thread pool resize failed (ignored): %s", e)
    try:
        with startup_phase(logger, "auth bootstrap"):
            from app.services import auth as auth_service

            auth_service.bootstrap_from_env()
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("auth bootstrap failed: %s", e)


def _lifespan_data_core(app: FastAPI) -> tuple[DataStore, KlineRepository]:
    """存储层/repository 初始化 (fail-closed: CK 连不上则启动失败)。"""
    with startup_phase(logger, "data store initialization"):
        store = DataStore()
    with startup_phase(logger, "clickhouse storage initialization"):
        from app.storage.runtime import init_storage

        storage = init_storage()
    app.state.storage = storage
    with startup_phase(logger, "coordination state rebuild"):
        from app.config import settings as _settings
        from app.services.coordination import coordination

        # 重建是昂贵且跨 worker 重复无意义的启动钩子：只让一个 worker 执行。
        if _settings.coordination_backend == "redis" and coordination.once(
            "startup:coordination_rebuild"
        ):
            from app.services.coordination_audit import rebuild_coordination

            counts = rebuild_coordination()
            logger.info("协调态重建完成: %s", counts)
        if _settings.mining_store_backend == "redis" and coordination.once(
            "startup:mining_rebuild"
        ):
            from app.services.mining_audit import rebuild_mining_runs

            mining_counts = rebuild_mining_runs()
            logger.info("mining run 重建完成: %s", mining_counts)
    with startup_phase(logger, "repository initialization"):
        repo = KlineRepository(store, storage=storage.store)
    app.state.datastore = store
    app.state.repo = repo
    return store, repo


def _lifespan_seed_factors(store: DataStore) -> int:
    """种子信号 + 自定义因子注册; 单个失败只跳过, 不阻断启动。"""
    # 种子迁移必须赶在 refresh_cache 之前: 后者编译信号表达式, 晚种会让
    # 本次启动的看板/梯队引用的 signal_limit_* 列缺失, 直到下次刷新才自愈。
    seeded_signals = 0
    try:
        with startup_phase(logger, "signal seeding"):
            from app.strategy import custom_signals as _cs_seed

            seeded_signals = _cs_seed.seed_if_empty(store.data_dir)
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("signal seeding failed (ignored): %s", e)
    with startup_phase(logger, "custom factor module import"):
        from app.factors.store import load_runtime_registry

    try:
        with startup_phase(logger, "custom factor registration"):
            loaded_factors = load_runtime_registry(store.data_dir)
        if loaded_factors:
            logger.info("custom factors loaded: %s", len(loaded_factors))
    except Exception as exc:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("custom factors load failed: %s", exc)
    return seeded_signals


def _lifespan_mining_guardian(app: FastAPI, store: DataStore, repo: KlineRepository) -> None:
    """挖掘/守护任务恢复 + enriched generation 固定 + 缓存后台预热。"""
    from app.services.coordination import coordination

    with startup_phase(logger, "mining manager initialization"):
        from app.services.mining_jobs import get_run_store
        from app.services.mining_manager import MiningJobManager

        mining_manager = MiningJobManager(store.data_dir)
    # degraded 实例不得改写其他 owner 的 live run manifests；
    # 恢复只由一个 worker 做，避免多 worker 互把对方 ACTIVE run 标 interrupted。
    with startup_phase(logger, "mining run recovery"):
        recovered_mining_runs = (
            mining_manager.recover_interrupted()
            if app.state.mining_lock_acquired and coordination.once("startup:mining_recover")
            else 0
        )
    app.state.mining_manager = mining_manager
    if recovered_mining_runs:
        logger.warning("recovered %d interrupted mining runs", recovered_mining_runs)
    with startup_phase(logger, "guardian manager initialization"):
        guardian_manager = MiningJobManager(
            store.data_dir,
            kind="guardian",
            store=get_run_store(store.data_dir, namespace="guardian"),
        )
    with startup_phase(logger, "guardian run recovery"):
        recovered_guardian_runs = (
            guardian_manager.recover_interrupted()
            if coordination.once("startup:guardian_recover")
            else 0
        )
    app.state.guardian_manager = guardian_manager
    if recovered_guardian_runs:
        logger.warning("recovered %d interrupted guardian runs", recovered_guardian_runs)
    # 接受回测请求前固定 managed generation, 避免首批并发 worker 各自创建版本；
    # 只让一个 worker 固定共享版本，其余 worker 直接复用已固定的结果。
    if settings.backtest_matrix_disk_cache_enabled and coordination.once(
        "startup:matrix_generation"
    ):
        try:
            with startup_phase(logger, "enriched generation initialization"):
                repo.get_matrix_data_generation("stock")
        except EnrichedGenerationUnavailableError as exc:
            logger.warning("enriched generation requires a full rebuild: %s", exc)
    app.state.indicators_ready = False
    repo._on_warmup_done = lambda: setattr(app.state, "indicators_ready", True)
    # enriched 重计算推后台, instruments/index/ETF 同步; 应用立即 ready
    repo.refresh_cache(background=True)


def _lifespan_capabilities(app: FastAPI, store: DataStore, repo: KlineRepository):
    """自定义源加载 + 能力探测 + 扩展冻结 + 升级守卫; 返回能力集。"""
    try:
        from app.data_providers import custom as custom_sources

        custom_sources.load_all()
        logger.info("custom data sources loaded: %d", len(custom_sources.list_sources()))
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("custom data sources init failed: %s", e)
    # 自定义源必须先注册, 能力探测才能补充其数据集能力。
    capset = detect_capabilities()
    app.state.capabilities = capset
    logger.info("ready; %d capabilities active", len(capset.all()))
    try:
        from app.data_providers.capabilities import resolve_capability_sources

        for row in resolve_capability_sources():
            logger.info(
                "  能力路由 | %-8s ← %-12s %s",
                row["label"],
                row["source_display"],
                "可用" if row["usable"] else "不可用",
            )
    except Exception as e:  # noqa: BLE001 - 仅日志, 失败不影响启动
        logger.warning("能力路由日志输出失败: %s", e)
    from app.extensions.packages import start_managed_extensions

    start_managed_extensions(
        app,
        current_extension_context(data_dir=store.data_dir, repository=repo),
    )
    from app.services.upgrade_guard import UpgradeGuard

    upgrade_guard = UpgradeGuard(store.data_dir)
    app.state.upgrade_guard = upgrade_guard
    upgrade_guard.start_background_scan()
    return capset


def _lifespan_quote_monitor(app: FastAPI, repo: KlineRepository) -> None:
    """全局行情服务 + 策略监控 + 五档盘口服务。"""
    qs = QuoteService()
    app.state.quote_service = qs
    qs.set_repo(repo)
    qs.boot_check()
    # QuoteService 需要访问 strategy_monitor 等单例, 先创建再注入 app.state
    from app.strategy.monitor import StrategyMonitorService

    strategy_monitor = StrategyMonitorService()
    app.state.strategy_monitor = strategy_monitor
    qs.set_app_state(app.state)
    from app.services.depth_service import DepthService

    depth_service = DepthService()
    depth_service.set_repo(repo)
    depth_service.set_app_state(app.state)
    app.state.depth_service = depth_service


def _lifespan_schedulers(app: FastAPI, store: DataStore, repo: KlineRepository, capset) -> None:
    """调度器 + depth sealed 补跑 + 盘中分钟刷新 + 停机缺口自检。

    后台线程只在 leader worker 启动（避免多进程重复调度）；但服务对象与
    app.state 在所有 worker 都挂载，API 读取时非 leader 只是「未启动」状态。
    """
    from app.services.coordination import coordination

    leader = coordination.once("startup:schedulers")
    try:
        daily_pipeline.set_app_state(app.state)
        app.state.scheduler = daily_pipeline.start_scheduler(repo, capset) if leader else None
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("scheduler not started: %s", e)
        app.state.scheduler = None
    try:
        depth_service = app.state.depth_service
        depth_service.boot_check()
        if leader:
            depth_service.start_polling()
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("depth_service init failed: %s", e)
    try:
        from app.services.minute_refresh import MinuteRefreshService

        minute_refresh = MinuteRefreshService(repo)
        minute_refresh.set_app_state(app.state)
        app.state.minute_refresh = minute_refresh
        if leader:
            minute_refresh.start()
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("minute_refresh init failed: %s", e)
    if leader:
        try:
            import threading

            from app.services.data_integrity import boot_integrity_check

            timer = threading.Timer(30.0, boot_integrity_check, args=(app.state,))
            timer.daemon = True  # 不阻塞进程退出
            timer.start()
        except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("integrity boot check scheduling failed: %s", e)


async def _lifespan_background(
    app: FastAPI, store: DataStore, repo: KlineRepository, capset
) -> None:
    """发布标记自愈 + 企业微信机器人 + 扩展表/财务/拉取调度 + 看门狗。"""
    try:
        from app.enriched_generation import recover_stale_publications

        recovered_publications = recover_stale_publications(store.data_dir)
        if recovered_publications:
            logger.warning(
                "已恢复上次中断的 enriched 发布标记: %s", ", ".join(recovered_publications)
            )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("enriched 发布标记自愈失败 (不影响启动): %s", e)
    try:
        from app.services.wecom_bot_service import WecomBotService

        wecom_bot_service = WecomBotService()
        wecom_bot_service.set_app_state(app.state)
        app.state.wecom_bot_service = wecom_bot_service
        wecom_bot_service.boot_check()
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("wecom_bot_service init failed: %s", e)
    # 内置扩展表须在 pull_scheduler.refresh 之前创建, 否则新部署定时拉取读不到预设。
    try:
        from app.services.ext_presets import ensure_builtin_presets

        await ensure_builtin_presets(store.data_dir)
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("内置扩展表初始化失败 (不影响启动): %s", e)
    from app.services.coordination import coordination

    leader = coordination.once("startup:background")
    from app.services.ext_pull import pull_scheduler

    if leader:
        pull_scheduler.start(store.data_dir)
        pull_scheduler.refresh(store.data_dir)
    app.state.pull_scheduler = pull_scheduler
    from app.services.financial_sync import financial_scheduler

    if leader:
        financial_scheduler.start(store.data_dir, capset)
    app.state.financial_scheduler = financial_scheduler
    from app.watchdog import start_watchdog

    app.state.watchdog = start_watchdog(app.state, repo)


def _lifespan_strategy_engine(
    app: FastAPI, store: DataStore, repo: KlineRepository, seeded_signals: int
):
    """策略引擎创建 + 去内置化迁移 + 旧策略归档; 返回引擎与筛选器。"""
    from app.services.screener import ScreenerService
    from app.strategy import config as strategy_config
    from app.strategy.engine import StrategyEngine

    screener_svc = ScreenerService(repo)
    etf_screener_svc = ScreenerService(repo, asset_type="etf")
    # 去内置化补全(仅升级用户): 原内置策略按 ID 补回 custom, 已自行迁移的不覆盖;
    # 必须在 StrategyEngine 创建前完成, 否则首载拿不到刚补回的文件。
    seeded_strategies = 0
    try:
        from app.strategy.strategy_seeds import seed_missing_builtins

        seeded_strategies = seed_missing_builtins(store.data_dir)
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("strategy seeding failed (ignored): %s", e)
    strategy_dirs = [
        Path(__file__).resolve().parent / "strategy" / "research",
        store.data_dir / "strategies" / "custom",
        store.data_dir / "strategies" / "ai",
        store.data_dir / "strategies" / "composite",
    ]
    strategy_engine = StrategyEngine(
        strategy_dirs=strategy_dirs,
        override_loader=lambda sid: strategy_config.load_override(store.data_dir, sid),
    )
    # 信号目录补种 + 加载失败策略自动归档出扫描目录并重载
    legacy_migration: dict = {}
    try:
        from app.strategy.legacy_migrate import archive_unloadable_strategies

        if seeded_signals:
            legacy_migration["seeded_signals"] = seeded_signals
            logger.info("legacy migrate: seeded %d default signals", seeded_signals)
        if seeded_strategies:
            legacy_migration["seeded_strategies"] = seeded_strategies
            logger.info("legacy migrate: seeded %d builtin strategies", seeded_strategies)
        archived = archive_unloadable_strategies(strategy_engine.load_errors())
        if archived:
            strategy_engine.reload()
            legacy_migration["archived_files"] = archived
            logger.info("legacy migrate: archived %d unloadable files", len(archived))
        if legacy_migration:
            app.state.legacy_migration = legacy_migration
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("legacy migrate failed (ignored): %s", e)
    app.state.strategy_engine = strategy_engine
    logger.info("strategy engine loaded: %d strategies", len(strategy_engine.list_strategies()))
    return strategy_engine, screener_svc, etf_screener_svc


def _lifespan_matrix_prewarm(
    app: FastAPI, repo: KlineRepository, strategy_engine
) -> MatrixCachePrewarmOwner:
    """回测矩阵预热调度器; 预热在后台 heavy 槽内执行。"""
    matrix_prewarm_owner = MatrixCachePrewarmOwner()

    def _schedule_matrix_cache_prewarm() -> None:
        if (
            not settings.backtest_matrix_disk_cache_enabled
            or not settings.backtest_matrix_cache_prewarm
        ):
            return

        def _prewarm() -> None:
            from app.backtest.engine import BacktestEngine
            from app.backtest.matrix import MatrixPrewarmCancelledError
            from app.backtest.strategy import prewarm_matrix_cache
            from app.services.heavy_job_limiter import (
                HeavyJobCancelledError,
                shared_heavy_job_limiter,
            )

            try:
                latest = repo.latest_enriched_date("stock")
                if latest is None:
                    logger.info("matrix cache prewarm skipped: no stock enriched data")
                    return
                with shared_heavy_job_limiter.slot(
                    "exclusive", cancel_event=matrix_prewarm_owner.cancel_event
                ):
                    result = prewarm_matrix_cache(
                        BacktestEngine(repo),
                        strategy_engine,
                        asset_type="stock",
                        latest_date=latest,
                        years=settings.backtest_matrix_cache_prewarm_years,
                        cancel_event=matrix_prewarm_owner.cancel_event,
                    )
                logger.info("matrix cache prewarm done: %s", result)
            except (HeavyJobCancelledError, MatrixPrewarmCancelledError):
                logger.info("matrix cache prewarm cancelled")
            except Exception:
                logger.exception("matrix cache prewarm failed")

        if not matrix_prewarm_owner.schedule(_prewarm):
            logger.info("matrix cache prewarm already running or shutting down, skip")

    repo._on_refresh_done = _schedule_matrix_cache_prewarm
    if repo.enriched_ready:
        _schedule_matrix_cache_prewarm()
    return matrix_prewarm_owner


def _lifespan_monitoring(
    app: FastAPI,
    store: DataStore,
    repo: KlineRepository,
    strategy_engine,
    screener_svc,
    etf_screener_svc,
) -> None:
    """监控规则引擎 + 策略迁移 + 模拟盘绑定 + 后端扩展启动钩子。"""
    from app.services import preferences
    from app.services.sector_monitor import SectorMonitorService
    from app.strategy import monitor_rules as mr_store
    from app.strategy.monitor import MonitorRuleEngine

    monitor_engine = MonitorRuleEngine()
    sector_monitor_service = SectorMonitorService(repo)
    monitor_engine.set_strategy_engine(strategy_engine)
    monitor_engine.set_data_dir(store.data_dir)
    monitor_engine.set_sector_monitor_service(sector_monitor_service)
    monitor_engine.set_history_loader(screener_svc._load_enriched_history)
    monitor_engine.set_history_loader_etf(etf_screener_svc._load_enriched_history)
    try:
        if preferences.get_strategy_monitor_enabled():
            ids = preferences.get_strategy_monitor_ids()
            if ids:
                names = {s["id"]: s["name"] for s in strategy_engine.list_strategies()}
                mr_store.migrate_strategy_monitors(store.data_dir, ids, names)
                logger.info("strategy monitor migrated: %d strategies", len(ids))
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("strategy monitor migration failed: %s", e)
    try:
        rules = mr_store.load_all(store.data_dir)
        monitor_engine.set_rules(rules)
        logger.info("monitor engine loaded: %d rules", monitor_engine.rule_count)
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("monitor engine load failed: %s", e)
    try:
        from app.strategy import paper_watch

        synced = paper_watch.sync(store.data_dir, monitor_engine, strategy_engine)
        logger.info("paper watch synced at startup: %d strategies", len(synced["watching"]))
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("paper watch sync failed (ignored): %s", e)
    app.state.monitor_engine = monitor_engine
    app.state.sector_monitor_service = sector_monitor_service
    extension_registry = app.state.extension_registry
    start_backend_extensions(
        current_extension_context(data_dir=store.data_dir, repository=repo),
        extension_registry,
    )


async def _lifespan_shutdown(app: FastAPI, state: _LifespanState) -> None:
    """关闭阶段: 释放后台线程/调度器/存储, 顺序与启动相反。"""
    repo = state.repo
    matrix_prewarm_owner = state.matrix_prewarm_owner
    repo._on_refresh_done = None
    wd = getattr(app.state, "watchdog", None)
    if wd:
        await wd.stop()
    if not matrix_prewarm_owner.shutdown(timeout=5.0):
        logger.warning("matrix cache prewarm did not stop within 5 seconds")
    mmanager = getattr(app.state, "mining_manager", None)
    if mmanager:
        mmanager.shutdown()
    gmanager = getattr(app.state, "guardian_manager", None)
    if gmanager:
        gmanager.shutdown()
    if app.state.scheduler:
        app.state.scheduler.shutdown(wait=False)
    ps = getattr(app.state, "pull_scheduler", None)
    if ps:
        ps.stop()
    fsc = getattr(app.state, "financial_scheduler", None)
    if fsc:
        fsc.stop()
    qs = getattr(app.state, "quote_service", None)  # type: ignore[assignment]
    if qs:
        qs.stop()
    dsvc = getattr(app.state, "depth_service", None)
    if dsvc:
        dsvc.stop_polling()
    wbot = getattr(app.state, "wecom_bot_service", None)
    if wbot:
        wbot.stop()
    mrs = getattr(app.state, "minute_refresh", None)
    if mrs:
        mrs.stop()
    # webhook/系统通知投递线程池: 放弃排队投递并关闭, 避免通知静默丢失在 daemon 线程里
    from app.services.quote_service import shutdown_delivery_executors

    shutdown_delivery_executors()
    # 事件总线订阅线程先退出, 再关闭存储层 (与启动顺序相反)
    from app.services.events import bus

    bus.close()
    # 存储层最后收尾: 先把写缓冲 flush 完再断连, 避免丢已接受的写入
    from app.storage.runtime import shutdown_storage

    shutdown_storage()
    logger.info("shutdown")


@asynccontextmanager
async def _application_lifespan(app: FastAPI):
    logger.info(
        "Tick Stock Panel v%s starting (mode=%s)",
        __version__,
        tf_client.current_mode(),
    )
    _lifespan_early()

    store, repo = _lifespan_data_core(app)
    seeded_signals = _lifespan_seed_factors(store)
    _lifespan_mining_guardian(app, store, repo)

    capset = _lifespan_capabilities(app, store, repo)
    _lifespan_quote_monitor(app, repo)

    _lifespan_schedulers(app, store, repo, capset)

    await _lifespan_background(app, store, repo, capset)

    strategy_engine, screener_svc, etf_screener_svc = _lifespan_strategy_engine(
        app, store, repo, seeded_signals
    )

    matrix_prewarm_owner = _lifespan_matrix_prewarm(app, repo, strategy_engine)

    _lifespan_monitoring(app, store, repo, strategy_engine, screener_svc, etf_screener_svc)
    try:
        yield
    finally:
        await _lifespan_shutdown(app, _LifespanState(repo, matrix_prewarm_owner))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 方案 C（Redis 状态机）下 run 状态由 Redis Lua 权威串行化, 取消走协调层,
    # flock 不再参与 mining 门控 —— 直接以"已获得"语义启动, 不再抢本机文件锁。
    # 默认 memory 文件版仍需 flock 单写者保护, 保留原降级 / 强制释放语义。
    distributed_mining = settings.mining_store_backend == "redis"
    mining_process_lock = MiningProcessLock(settings.data_dir)
    lock_acquired = True
    if not distributed_mining:
        try:
            mining_process_lock.acquire()
        except MiningProcessLockError:
            # 撞锁不再让整个后端起不来 (典型: 旧实例挂死在 shutdown 持锁不放):
            # 面板以降级模式启动 — 挖掘启动入口返回 409, 用户可在挖掘页查看
            # 持有者并 (满足陈旧判定时) 强制释放, 之后自动恢复。
            lock_acquired = False
            logger.warning(
                "挖掘锁被其他进程占用, 面板以降级模式启动 (挖掘功能暂不可用; "
                "可在挖掘页查看锁状态并强制释放)"
            )
    else:
        logger.info("分布式挖掘模式 (redis): flock 门控已出局")
    app.state.mining_process_lock = mining_process_lock
    app.state.mining_lock_acquired = lock_acquired
    try:
        async with _application_lifespan(app):
            yield
    finally:
        # 用 lock.acquired 而非启动时的 flag: 强制释放接管后同样要在退出时释放
        if mining_process_lock.acquired:
            mining_process_lock.release()


app = FastAPI(
    title="Tick Stock Panel",
    version=__version__,
    description="A 股策略 + 回测面板 — TickFlow 适配",
    lifespan=lifespan,
)


@app.exception_handler(WorkBusyError)
@app.exception_handler(HeavyJobLimitTimeoutError)
async def shared_work_busy_handler(request: Request, exc: WorkBusyError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={"detail": str(exc), "code": "work_busy"},
        headers={"Retry-After": "3"},
    )


# CORS: 允许局域网访问 (自托管场景, 放开所有来源)
# 注: allow_credentials=True 与 allow_origins=['*'] 不能共存 (浏览器规范),
# 本项目认证走 header (API Key), 不依赖 cookie, 故关闭 credentials 换取通配来源。
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ================================================================
# 访问认证中间件
# ================================================================
# 拦截所有 /api/ 请求, 三种状态:
#   1. 未设密码 + 本机/内网 → 放行(让本机用户访问面板 + 调 /api/auth/setup 设密码)
#   2. 未设密码 + 公网       → 拒绝(403, 防裸奔也防抢占; 引导本机设密码)
#   3. 已设密码              → 检查 session, 无效则 401(前端跳登录)
# 白名单: /api/auth/* (设密码/登录本身)、/health 等探活。
_AUTH_WHITELIST_PREFIX = ("/api/auth/",)
_AUTH_WHITELIST_EXACT = (
    "/health",
    "/api/health",
    "/openapi.json",
    "/api/openapi.json",
    "/docs",
    "/redoc",
    # SSE 事件流: EventSource 带不了 Authorization 头, 凭证即 query 里的
    # 一次性票据, 端点内校验 (api/events.py); POST /api/events/ticket 不在
    # 白名单, 仍走网关 Bearer 通道
    "/api/events",
)


@app.middleware("http")
async def workload_middleware(request: Request, call_next):
    import time

    from app.services.workload import workload

    group = next(
        (
            name
            for name in ("screener", "kline", "overview", "watchlist", "backtest")
            if request.url.path == f"/api/{name}" or request.url.path.startswith(f"/api/{name}/")
        ),
        None,
    )
    if group is None:
        return await call_next(request)
    started = time.perf_counter()
    try:
        return await call_next(request)
    finally:
        workload.record(f"http.{group}", "request", elapsed_s=time.perf_counter() - started)


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    # 仅 /api/ 走认证; 静态资源(前端页面/assets)放行, 由前端处理跳转
    if not path.startswith("/api/"):
        return await call_next(request)
    # CORS 预检不带凭据, 直接放行 (CORSMiddleware 在外层应答)
    if request.method == "OPTIONS":
        return await call_next(request)
    # 白名单放行(设密码/登录/探活本身不拦)
    if path.startswith(_AUTH_WHITELIST_PREFIX) or path in _AUTH_WHITELIST_EXACT:
        return await call_next(request)

    # ── API Token 通道 (外部调用方; 与密码会话并行, 见 open-platform-plan §4) ──
    authz = request.headers.get("authorization", "")
    if authz.startswith("Bearer "):
        from app.services import api_gateway

        # 网络门禁先行: 默认仅本机/内网可调 Token 通道 (API_TOKEN_LOCAL_ONLY=0 放开);
        # 先于 evaluate 拦截 → 不消耗限流额度, 公网探测者也拿不到 Token 有效性信号
        gate = api_gateway.token_network_verdict(auth_api._client_ip(request))
        if gate is not None:
            return JSONResponse(
                status_code=gate["status"],
                content={"detail": gate["detail"]},
            )
        verdict = api_gateway.evaluate(
            settings.data_dir,
            request.method,
            path,
            authz[len("Bearer ") :].strip(),
        )
        if verdict["status"] is not None:
            return JSONResponse(
                status_code=verdict["status"],
                content={"detail": verdict["detail"]},
                headers=verdict["headers"],
            )
        response = await call_next(request)
        for k, v in verdict["headers"].items():
            response.headers[k] = v
        return response

    from app.services import auth as auth_service

    # 情况 1+2: 未设密码
    if not auth_service.is_configured():
        # 本机/内网 → 放行(服务器主人可访问, 并去 /login 设密码)
        if auth_api._is_local_network(auth_api._client_ip(request)):
            return await call_next(request)
        # 公网 → 拒绝。不裸奔, 也不给公网设密码的机会(防抢占)
        return JSONResponse(
            status_code=403,
            content={
                "detail": "面板尚未初始化访问密码,请通过 SSH/本机浏览器访问以设置密码",
                "code": "NOT_INITIALIZED",
            },
        )

    # 情况 3: 已设密码, 检查会话
    token = request.cookies.get(auth_api.COOKIE_NAME)
    if token and auth_service.is_valid_session(token):
        return await call_next(request)
    # 未登录: 401(前端跳登录页)
    return JSONResponse(status_code=401, content={"detail": "未登录或会话已过期"})


# 路由
app.include_router(core_router)
app.include_router(auth_api.router)
app.include_router(kline.router)
app.include_router(watchlist.router)
app.include_router(screener.router)
app.include_router(backtest.router)
app.include_router(factors.router)
app.include_router(definitions.router)
app.include_router(mining.router)
app.include_router(guardian.router)
app.include_router(lab.router)
app.include_router(intraday.router)
app.include_router(indices.router)
app.include_router(overview.router)
app.include_router(pages_admin.router)
app.include_router(paper.router)
app.include_router(abnormal.router)
app.include_router(regime.router)
app.include_router(analysis.router)
app.include_router(pipeline.router)
app.include_router(position_policies.router)
app.include_router(data.router)
app.include_router(ext_data.router)
app.include_router(ext_pages.router)
app.include_router(task_registry.router)
app.include_router(financials.router)
app.include_router(stock_analysis.router)
app.include_router(market_recap.router)
app.include_router(settings_api.router)
app.include_router(upgrade_guard_api.router)
app.include_router(strategy.router)
app.include_router(strategy_pool.router)
app.include_router(signals.router)
app.include_router(monitor_rules.router)
app.include_router(config_backup.router)
app.include_router(import_center.router)
app.include_router(lots.router)
app.include_router(alerts.router)
app.include_router(events.router)
app.include_router(rps.router)
app.include_router(sector_rotation.router)

# 二次开发路由与小粒度策略在所有核心路由后注册, 禁止覆盖核心路径。
extension_registry, extension_load_errors = configure_backend_extensions(app)
app.state.extension_registry = extension_registry
app.state.extension_load_errors = extension_load_errors


# 能力门控异常 → 403(而非默认 500)
# 业务代码用 capset.require(Cap.X) 断言能力,缺失时抛 CapabilityDenied;
# 若不注册 handler 会冒泡成 500 Internal Server Error,对前端不友好且语义错误。

from app.tickflow.capabilities import (  # noqa: E402 - 延迟导入避免启动期循环依赖
    CapabilityDenied,
)


@app.get("/api/openapi.json", include_in_schema=False)
async def openapi_contract_view(tier: str = "a"):
    """Tier A 契约视图: 只保留对外开放 (Token 可达) 的端点 = 稳定承诺面。

    开放清单的权威来源是 api_gateway 的规则表 — 规则表即契约, 单源维护。
    二开方以此生成客户端; 未出现在此视图的端点属内部实现, 随时变化。
    """
    spec = app.openapi()
    if tier == "a":
        from app.services import api_gateway

        kept_paths: dict = {}
        for path, ops in spec.get("paths", {}).items():
            kept_ops = {}
            for method, op in ops.items():
                if method in (
                    "get",
                    "post",
                    "put",
                    "delete",
                    "patch",
                ) and api_gateway.required_scope(
                    method.upper(),
                    path,
                ):
                    kept_ops[method] = op  # noqa: PERF403 - PERF403: 循环体含复杂逻辑, 推导式不适用
            if kept_ops:
                kept_paths[path] = kept_ops
        spec["paths"] = kept_paths
        spec["x-tier"] = "a"
    return JSONResponse(spec)


@app.exception_handler(CapabilityDenied)
async def capability_denied_handler(request: Request, exc: CapabilityDenied) -> JSONResponse:
    return JSONResponse(
        status_code=403,
        content={"detail": str(exc), "suggestion": exc.suggestion},
    )


@app.exception_handler(DailyIntegrityError)
async def daily_integrity_error_handler(request: Request, exc: DailyIntegrityError) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc), "code": "daily_integrity"})


@app.exception_handler(ProviderRequestError)
async def provider_request_error_handler(
    request: Request, exc: ProviderRequestError
) -> JSONResponse:
    return JSONResponse(status_code=502, content={"detail": str(exc), "code": "provider_request"})


# 生产期静态文件(前端 dist)
def _register_static_routes(target: FastAPI, static_dir: Path) -> None:
    """挂载前端构建产物: /assets 走静态服务, 根级文件直出, 其余回退 SPA。

    根级静态文件 (favicon.svg / donate-wechat.jpg 等) 必须原样返回。此前所有
    未匹配路径都回退 index.html, 浏览器把 HTML 当图标解析失败, Docker 与桌面
    安装包的浏览器页签因此没有图标。
    """
    # 旧客户端可能缓存了 text/plain 的 JS/CSS。使用版本化资源地址，连带
    # 相对路径动态 import 的分块一起绕过旧缓存；保留 /assets 兼容旧链接。
    asset_base = f"/_static/{__version__}/assets"
    if (static_dir / "assets").exists():
        assets = FrontendStaticFiles(directory=static_dir / "assets")
        target.mount("/assets", assets, name="assets")
        target.mount(asset_base, assets, name="versioned-assets")

    @target.get("/{full_path:path}", include_in_schema=False)
    def spa_fallback(full_path: str):
        # 只直出 dist 根目录文件 (单段路径); index.html 仍走下方 no-store 分支。
        if full_path and "/" not in full_path and full_path not in (".", ".."):
            asset = static_dir / full_path
            if full_path != "index.html" and asset.is_file():
                return frontend_file_response(asset)
        index = static_dir / "index.html"
        if index.exists():
            markup = index.read_text(encoding="utf-8")
            for quote in ('"', "'"):
                markup = markup.replace(f"{quote}/assets/", f"{quote}{asset_base}/")
            return HTMLResponse(
                markup,
                headers={"Cache-Control": "no-store, must-revalidate"},
            )
        return {"error": "frontend not built"}


_static = Path(settings.static_dir)
if _static.exists():
    _register_static_routes(app, _static)
