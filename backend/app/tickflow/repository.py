"""Repository 层(§7.4)。

数据分层:
  - DuckDB 视图: 冷查询(统计、元数据、用户自定义SQL)
  - Polars 缓存: 热路径(enriched 最新日 ~5500行 + instruments ~5500行)
  - Polars scan_parquet: 分钟K/历史日K (predicate pushdown)

缓存生命周期:
  - startup 时不加载(数据可能为空)
  - pipeline 完成后调用 refresh_cache()
  - 服务层通过 get_enriched_latest() / get_instruments() 获取缓存
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from datetime import date, datetime
from datetime import time as dt_time
from pathlib import Path

import polars as pl

from app.config import settings
from app.enriched_generation import (
    EnrichedGenerationSnapshot,
    EnrichedGenerationUnavailableError,
    EnrichedPublication,
    bump_enriched_generation,
    enriched_publication_incomplete,
    get_enriched_generation,
    get_enriched_generation_snapshot,
)
from app.market_time import CN_TZ, cn_today
from app.parquet import scan_enriched_parquet, scan_parquet_compat
from app.polars_guard import guarded_collect
from app.services.minute_adjust import apply_minute_adjustment, minute_basis_is_raw
from app.startup_diagnostics import startup_phase

__all__ = [
    "CN_TZ",
    "_ENRICHED_WARMUP_MAX_ATTEMPTS",
    "_ENRICHED_WARMUP_PUBLISH_WAIT_SECONDS",
    "_LIVE_AGG_WINDOW_BARS",
    "Callable",
    "DataStore",
    "EnrichedGenerationSnapshot",
    "EnrichedGenerationUnavailableError",
    "EnrichedPublication",
    "KlineRepository",
    "OrderedDict",
    "Path",
    "_AssetRefreshMixin",
    "_ComputeScanMixin",
    "_DailyMixin",
    "_LiveAggMixin",
    "_MinuteMixin",
    "_MinuteQueryMixin",
    "_QueryMixin",
    "_RefreshMixin",
    "_WriteMixin",
    "_last_available_rows",
    "_live_agg_window_start",
    "apply_minute_adjustment",
    "bump_enriched_generation",
    "cn_today",
    "daily_dirname",
    "date",
    "datetime",
    "dt_time",
    "enriched_dirname",
    "enriched_publication_incomplete",
    "get_enriched_generation",
    "get_enriched_generation_snapshot",
    "guarded_collect",
    "logger",
    "logging",
    "minute_basis_is_raw",
    "pl",
    "replace_with_retry",
    "scan_enriched_parquet",
    "scan_parquet_compat",
    "settings",
    "startup_phase",
    "threading",
    "time",
]


logger = logging.getLogger(__name__)

# GH #480: 后台 warmup 因世代漂移被丢弃时的有界重建上限。compute 窗口长达
# 数十秒, 启动期与 quote 实时刷新的 commit 撞车后若不重建, 历史缓存长期
# 为空, K 线接口逐请求现算指标。仅 warmup 线程用, 同步/懒加载路径单次。
_ENRICHED_WARMUP_MAX_ATTEMPTS = 3
_ENRICHED_WARMUP_PUBLISH_WAIT_SECONDS = 5.0


def replace_with_retry(src: Path, dst: Path, *, attempts: int = 10, delay_s: float = 0.5) -> None:
    """os.replace 的 Windows 读锁重试版。

    分区 parquet 的读端 (polars scan_parquet / DuckDB read_parquet 视图) 在扫描进行
    期间持有句柄; Windows 不允许替换"仍被读端打开"的目标文件 (PermissionError,
    WinError 5), Linux 的 inode 交换语义则无此限制。读端扫描通常亚秒级完成,
    短退避重试即可穿过并发读窗口; attempts 次仍被占用则原样抛出, 由上层记录失败。
    """
    last: PermissionError | None = None
    for i in range(attempts):
        try:
            src.replace(dst)
            if i:
                logger.info("parquet replace succeeded after %d blocked attempt(s): %s", i, dst)
            return
        except PermissionError as e:
            last = e
            if i == 0:
                logger.warning(
                    "parquet replace blocked by concurrent reader, retrying (total <= %.1fs): %s",
                    attempts * delay_s,
                    dst,
                )
            if i < attempts - 1:
                time.sleep(delay_s)
    raise last  # type: ignore[misc]  # attempts >= 1 时 last 必已赋值


def enriched_dirname(asset_type: str) -> str:
    """asset_type → enriched parquet 目录名。ETF 走独立目录, 其余(stock)用日K enriched。"""
    return {
        "stock": "kline_daily_enriched",
        "etf": "kline_etf_enriched",
        "index": "kline_index_enriched",
    }[asset_type]


def daily_dirname(asset_type: str) -> str:
    return {"stock": "kline_daily", "etf": "kline_etf_daily", "index": "kline_index_daily"}[
        asset_type
    ]


# 盘中递推状态的最长窗口 (交易日): MA60 部分和 tail(59)、60 日动量 tail(60)
_LIVE_AGG_WINDOW_BARS = 60


def _live_agg_window_start(dates: pl.Series, latest: date, calendar_start: date) -> date:
    """盘中递推历史窗口起点: 自然日起点与「最近 60 个交易日」起点取较早者。

    自然日 90 天通常含 62~65 个交易日, 但春节/国庆长假前后只有 57~59 个,
    tail(59)/tail(60) 会取到残缺窗口 (与 get_enriched_history 按交易日计数同理)。
    """
    trading = dates.filter(dates <= latest).unique().sort()
    if trading.len() >= _LIVE_AGG_WINDOW_BARS:
        return min(calendar_start, trading[-_LIVE_AGG_WINDOW_BARS])
    return calendar_start


def _last_available_rows(df: pl.DataFrame, cutoff: date) -> pl.DataFrame:
    """从已按 symbol/date 排序的数据中取每只标的最后一条有效状态。"""
    if df.is_empty():
        return df
    return df.filter(pl.col("date") <= cutoff).group_by("symbol", maintain_order=True).last()


class DataStore:
    """唯一的存储入口 — 进程启动时创建。"""

    def __init__(self, data_dir: Path | None = None) -> None:
        self.data_dir = Path(data_dir or settings.data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 一次性数据迁移: 旧桌面版把数据放在 exe 同级的兄弟目录 TickFlowStockPanel_Data/,
        # 新版改为 {app}/data/。老用户首次启动时自动把旧数据搬过来, 无感升级。
        with startup_phase(logger, "legacy data migration"):
            self._migrate_legacy_data_dir()

        # 关键子目录(§7.2)
        for sub in (
            "kline_daily",
            "kline_daily_enriched",
            "kline_index_daily",
            "kline_index_enriched",
            "kline_etf_daily",
            "kline_etf_enriched",
            "kline_etf_minute",
            "kline_minute",
            "adj_factor",
            "adj_factor_etf",
            "financials",
            "instruments",
            "instruments_index",
            "instruments_etf",
            "instruments_ext",
            "kline_ext",
            "pools",
            "backtest_results",
            "ai_cache",
            "user_data",
            "depth5",
        ):
            (self.data_dir / sub).mkdir(parents=True, exist_ok=True)

        # 财务数据子目录
        for sub in ("metrics", "income", "balance_sheet", "cash_flow", "shares"):
            (self.data_dir / "financials" / sub).mkdir(parents=True, exist_ok=True)

        # DuckDB 已移除 (B5.1)：所有数据读写入口统一走 app.storage CK 门面。

    def _migrate_legacy_data_dir(self) -> None:
        """Compatibility entry for frozen workers and callers outside desktop.main."""
        from app.services.data_dir_upgrade import recover_desktop_data

        recover_desktop_data(self.data_dir)


from app.tickflow._repo_asset_refresh_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _AssetRefreshMixin,
)
from app.tickflow._repo_compute_scan_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _ComputeScanMixin,
)
from app.tickflow._repo_daily_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _DailyMixin,
)
from app.tickflow._repo_liveagg_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _LiveAggMixin,
)
from app.tickflow._repo_minute_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _MinuteMixin,
)
from app.tickflow._repo_minute_query_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _MinuteQueryMixin,
)
from app.tickflow._repo_query_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _QueryMixin,
)
from app.tickflow._repo_refresh_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _RefreshMixin,
)
from app.tickflow._repo_write_mixin import (  # noqa: E402 - 延迟导入避免 mixin 与根模块循环依赖
    _WriteMixin,
)


class KlineRepository(
    _RefreshMixin,
    _LiveAggMixin,
    _AssetRefreshMixin,
    _QueryMixin,
    _DailyMixin,
    _MinuteMixin,
    _ComputeScanMixin,
    _MinuteQueryMixin,
    _WriteMixin,
):
    """日 K / 分钟 K 的读写入口。"""

    def __init__(self, store: DataStore, storage=None) -> None:
        self.store = store
        # ClickHouse 存储门面：构造注入，或由 _ck_store() 回退进程 runtime。
        self.storage = storage
        # DuckDB 读并发: cursor() 即同一内存库实例的独立连接 (MVCC 快照隔离,
        # 1.5.3 实测并发读+换视图零异常)。用信号量限制同时进行的扫描数, 取代
        # 原全局互斥 —— 同步期的长查询 (全视图扫描) 不再把交互读堵到超时。
        # DDL (视图重建) 互斥: 并发 CREATE OR REPLACE VIEW 同名视图会抛
        # Catalog write-write conflict (1.5.3 实测), 必须串行; 与读不互斥。
        # 根连接运行期只作连接工厂, 不直接执行 SQL (跨线程共用一个
        # Connection 对象没有契约保证, cursor 各自独立)
        # 序列化 parquet 分区的读-改-写: 实时轮询线程、手动 refresh、盘后管道
        # 可能并发 merge/flush 同一分区文件, 无锁会互相覆盖丢数据
        self._write_lock = threading.Lock()
        # 自选/详情定向补齐的最新行。只供展示回退，不能混入全市场当日策略快照。
        self._prepared_daily_lock = threading.Lock()
        self._prepared_daily: OrderedDict = OrderedDict()

        # ---- Polars 缓存 ----
        self._enriched_cache: pl.DataFrame | None = None  # 最新一天 (~5500行)
        self._enriched_cache_date: date | None = None
        self._live_agg_cache: pl.DataFrame | None = None  # 预计算聚合表 (~5500行)
        self._live_agg_cache_date: date | None = None
        self._live_agg_check_date: date | None = None  # 上次跨日校验时的 today (快路径节流)
        self._instruments_cache: pl.DataFrame | None = None
        self._name_map_cache: dict[str, str] | None = None
        self._historical_shares_cache: pl.DataFrame | None = None
        self._historical_shares_mtime_ns: int | None = None
        # 完整 enriched 历史 (含所有指标, 供 filter_history 策略使用)
        self._enriched_history_cache: pl.DataFrame | None = None  # ~100万行
        self._enriched_history_start: date | None = None
        self._enriched_history_generation: str | None = None
        self._enriched_history_snapshot: tuple[pl.DataFrame, EnrichedGenerationSnapshot] | None = (
            None
        )
        self._index_instruments_cache: pl.DataFrame | None = None
        self._etf_enriched_cache: pl.DataFrame | None = None
        self._etf_enriched_cache_date: date | None = None
        self._etf_live_agg_cache: pl.DataFrame | None = None
        self._etf_live_agg_cache_date: date | None = None
        self._etf_instruments_cache: pl.DataFrame | None = None
        # symbol 集合 memo (随对应 instruments 缓存失效): 供每请求资产分流用
        self._index_symbol_set_cache: set[str] | None = None
        self._etf_symbol_set_cache: set[str] | None = None
        self._index_enriched_cache: pl.DataFrame | None = None
        self._index_enriched_cache_date: date | None = None

        # ---- enriched 后台预热 ----
        # 启动时 compute_indicators (107万行, 低配机 50s+) 移出 lifespan 关键路径,
        # 推到 daemon 线程异步完成。预热期间 get_enriched_latest / get_live_agg
        # 返回空表 (上层优雅降级), 不触发同步重算 (否则会抵消异步化收益)。
        self._enriched_warming: bool = False
        self._warmup_thread: threading.Thread | None = None
        self._warmup_lock = threading.Lock()
        # 预热完成后的回调 (lifespan 注入, 用于设置 app.state.indicators_ready)
        self._on_warmup_done: Callable[[], None] | None = None
        # parquet/instruments 同步刷新完成后的轻量回调；用于调度派生缓存预热。
        self._on_refresh_done: Callable[[], None] | None = None

        # parquet glob 路径
        self._enriched_glob = str(store.data_dir / "kline_daily_enriched" / "**" / "*.parquet")
        self._index_enriched_glob = str(
            store.data_dir / "kline_index_enriched" / "**" / "*.parquet"
        )
        self._etf_enriched_glob = str(store.data_dir / "kline_etf_enriched" / "**" / "*.parquet")
        self._minute_glob = str(store.data_dir / "kline_minute" / "**" / "*.parquet")
        self._etf_minute_glob = str(store.data_dir / "kline_etf_minute" / "**" / "*.parquet")
        self._inst_glob = str(store.data_dir / "instruments" / "**" / "*.parquet")
        self._index_inst_glob = str(store.data_dir / "instruments_index" / "**" / "*.parquet")
        self._etf_inst_glob = str(store.data_dir / "instruments_etf" / "**" / "*.parquet")

    def _warmup_enriched(self) -> None:
        """仅后台预热线程调用: 世代漂移导致丢弃时以新世代有界重建。

        warmup 的 compute 窗口长达数十秒, 启动期与 quote 实时刷新的 commit
        撞车概率高; 丢弃后不重建会把缓存长期留冷, K 线接口逐请求现算指标
        (GH #480)。每轮重新捕获世代、各自仍 fail-closed; 发布未完成时短暂
        等待再试; 超过上限则放弃并保持冷缓存降级 (非无限重读)。
        """
        for attempt in range(_ENRICHED_WARMUP_MAX_ATTEMPTS):
            if attempt and enriched_publication_incomplete(self.store.data_dir, "stock"):
                time.sleep(_ENRICHED_WARMUP_PUBLISH_WAIT_SECONDS)
            if not self._refresh_enriched():
                return
        logger.warning(
            "enriched 历史缓存预热因持续换代被丢弃, %d 轮后放弃 (保持冷缓存降级)",
            _ENRICHED_WARMUP_MAX_ATTEMPTS,
        )
