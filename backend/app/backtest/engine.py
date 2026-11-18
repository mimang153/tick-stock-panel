"""回测引擎 — 共享数据加载 + 撮合 + 统计计算。

纯 Polars/NumPy 实现，不依赖 pandas/vectorbt。
"""

from __future__ import annotations  # noqa: I001 - I001: 延迟导入块无法排序到文件顶部

import hashlib
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, Literal

import numpy as np
import polars as pl
import pyarrow as pa

from app.backtest.matrix import (
    MarketDataMatrix,
    MarketMatrix,
    MatrixCacheProfile,
    build_market_matrix,
    load_market_data_matrix_from_parquet,
)
from app.config import settings
from app.enriched_generation import EnrichedGenerationUnavailableError
from app.parquet import scan_enriched_parquet
from app.tickflow.repository import KlineRepository
import contextlib

logger = logging.getLogger(__name__)


# 公开符号集合：拆分子模块后仍从本模块导出（含测试 monkeypatch 入口）。
__all__ = [
    "EnrichedGenerationUnavailableError",
    "KlineRepository",
    "MarketDataMatrix",
    "MatrixCacheProfile",
    "_IndependentMatrixRun",
    "_LegacyIndependentRun",
    "_LegacyPortfolioRun",
    "_MatrixPortfolioRun",
    "_panel_signal_arrays",
    "_simulate_panel_symbol_trades",
    "build_market_matrix",
    "contextlib",
    "load_market_data_matrix_from_parquet",
    "pa",
]


def history_load_start(
    engine, start: date, asset_type: str, bars: int, *, minimum_days: int = 120
) -> date:
    """保留既有最小预取范围; 长假或分区稀疏时按真实交易日向前取足。"""
    fallback = start - timedelta(days=max(minimum_days, int(bars * 1.6)))
    data_dir = getattr(getattr(getattr(engine, "repo", None), "store", None), "data_dir", None)
    if bars <= 1 or data_dir is None:
        return fallback
    from app.services.mining_preflight import enriched_partition_dates

    dates = enriched_partition_dates(data_dir, asset_type, end=start - timedelta(days=1))
    return min(fallback, dates[max(0, len(dates) - bars)]) if dates else fallback


def _matrix_entry_score(matrix: MarketMatrix, time_id: int, asset_id: int) -> float:
    source_time = int(matrix.entry_signal_time[time_id, asset_id])
    if source_time < 0:
        return 0.0
    return float(matrix.score[source_time, asset_id])


def _apply_position_policy(
    config: MatcherConfig,
    candidates: list[tuple[str, float]],
    recent_returns: dict[str, np.ndarray],
    trades: list[TradeRecord],
    cash: float,
    equity: float,
    peak: float,
    open_positions: int,
    execution_stats: dict,
) -> tuple[np.ndarray, float]:
    """自定义仓位策略 → (候选权重, 总预算缩放)。矩阵/legacy 两路径共用。

    fail-closed 兜底: compute 抛错或输出非法时当日回退等权、scale=1, 并计入
    execution_stats["position_policy_fallback"] 供结果统计暴露 — 不中断整个回测,
    也绝不静默给出未经校验的仓位 (normalize_result 内强校验)。
    """
    n = len(candidates)
    fallback = (np.repeat(1.0 / n, n), 1.0)
    compute = config.position_policy_compute
    if n == 0 or compute is None:
        return fallback
    from app.backtest.position_sizing import (  # 按需导入: 默认路径零加载成本
        Candidate,
        PortfolioState,
        PositionSizingContext,
        TradeSnapshot,
        normalize_result,
    )

    try:
        snapshots = tuple(
            TradeSnapshot(
                symbol=str(t.symbol),
                pnl_pct=float(t.pnl_pct),
                duration=int(t.duration),
                exit_reason=str(t.exit_reason),
            )
            for t in trades[-50:]
        )
        drawdown = (equity - peak) / peak if peak > 0 else 0.0
        ctx = PositionSizingContext(
            selected=tuple(Candidate(symbol=s, score=float(v)) for s, v in candidates),
            recent_returns={s: [float(x) for x in r] for s, r in recent_returns.items()},
            closed_trades=snapshots,
            portfolio=PortfolioState(
                cash=float(cash),
                equity=float(equity),
                drawdown=drawdown,
                open_positions=int(open_positions),
            ),
            params=dict(config.position_policy_params or {}),
        )
        weights, scale = normalize_result(compute(ctx), n)  # type: ignore[arg-type]
        return np.asarray(weights, dtype=np.float64), scale
    except Exception:  # noqa: BLE001 — 用户代码运行期异常 → 当日兜底等权
        execution_stats["position_policy_fallback"] = (
            execution_stats.get("position_policy_fallback", 0) + 1
        )
        return fallback


# ================================================================
# 数据结构
# ================================================================


@dataclass
class MatcherConfig:
    # matching 为向后兼容入口: 仅传 matching 时, entry_fill/exit_fill 都取 matching 的值。
    # 显式传入 entry_fill/exit_fill 时以二者为准 (允许建仓/清仓口径不同)。
    matching: Literal["close_t", "open_t+1"] = "close_t"
    entry_fill: Literal["close_t", "open_t+1"] | None = None
    exit_fill: Literal["close_t", "open_t+1", "signal_next_minute"] | None = None
    # 成本模型: 优先使用拆分口径 (佣金双边 + 印花税仅卖出 + 滑点双边)。
    # 未设 commission_pct 时回退到 fees_pct 作为双边佣金 (向后兼容, 无印花税)。
    fees_pct: float = 0.0002
    commission_pct: float | None = None
    stamp_tax_pct: float | None = None
    slippage_bps: float = 5.0
    stop_loss_pct: float | None = None
    take_profit_pct: float | None = None
    trailing_stop_pct: float | None = None
    trailing_take_profit_activate_pct: float | None = None
    trailing_take_profit_drawdown_pct: float | None = None
    max_hold_days: int | None = None
    max_positions: int = 10
    max_exposure_pct: float = 1.0
    score_min: float | None = None
    score_max: float | None = None
    initial_capital: float = 1_000_000.0
    position_sizing: Literal["equal", "score_weight", "policy"] = "equal"
    # 自定义仓位策略 (position_sizing="policy"): 运行期由 strategy 层解析注入,
    # 引擎不接触注册表/磁盘 — compute 为纯函数, params 为 META 默认值合并请求值。
    # 这两个字段不参与任何缓存键/序列化。
    position_policy_compute: Callable[..., object] | None = None
    position_policy_params: dict | None = None
    # 分钟K精确成交: 开启后, 信号触发日的成交价用当日分钟K优化
    # (有参考线→穿越价, 无参考线→VWAP)。数据缺失时降级为日K口径。
    minute_fill: bool = False
    # 回测资产类型: 分钟K按资产类型分开存储, 精确成交据此路由分钟分区 (不凭代码格式猜测)。
    asset_type: str = "stock"

    def __post_init__(self) -> None:
        # 解析最终口径: 优先 entry_fill/exit_fill, 否则回退到 matching (向后兼容)。
        if self.entry_fill is None:
            self.entry_fill = self.matching
        if self.exit_fill is None:
            self.exit_fill = self.matching

    def _commission_pct(self) -> float:
        # commission_pct 显式给出时优先, 否则回退 fees_pct (向后兼容双边佣金)。
        return self.commission_pct if self.commission_pct is not None else self.fees_pct

    def buy_cost_pct(self) -> float:
        # 买入腿: 佣金 + 滑点。
        return self._commission_pct() + self.slippage_bps / 10000.0

    def sell_cost_pct(self) -> float:
        # 卖出腿: 佣金 + 印花税 + 滑点。印花税未设时为 0 (向后兼容)。
        stamp = self.stamp_tax_pct if self.stamp_tax_pct is not None else 0.0
        return self._commission_pct() + stamp + self.slippage_bps / 10000.0


@dataclass
class TradeRecord:
    symbol: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    pnl_pct: float
    duration: int
    exit_reason: str  # "signal" | "stop_loss" | "take_profit" | "trailing_stop" | "trailing_take_profit" | "max_hold" | "end"
    # 退出优先级 (高→低): pending_exit(历史挂单) > 风控(止损/移动止损/移动止盈) > signal(卖点) > max_hold(到期) > end
    name: str = ""
    shares: float = 0.0
    lots: float = 0.0
    position_pct: float = 0.0
    entry_value: float = 0.0
    exit_value: float = 0.0
    pnl_amount: float = 0.0
    entry_score: float | None = None
    entry_signal_date: date | str | None = None
    exit_signal_date: date | str | None = None
    blocked_exit_days: int = 0
    # 触发买入/卖出的具体信号列名 (如 signal_ma_golden_5_20 / csg_xxx);
    # 仅当该腿由信号触发时填充, 止损/止盈/到期等非信号退出时 exit_signal_id 为 None。
    entry_signal_id: str | None = None
    exit_signal_id: str | None = None


@dataclass
class SimResult:
    equity_curve: list[dict]  # [{date, value}]
    drawdown_curve: list[dict]  # [{date, value}]
    trades: list[TradeRecord]
    per_symbol_stats: list[dict]
    stats: dict


@dataclass(frozen=True)
class SimulationOptions:
    """Controls expensive result materialization without changing matching semantics."""

    include_monte_carlo: bool = True
    include_curves: bool = True
    include_trades: bool = True
    include_per_symbol_stats: bool = True
    include_return_distribution: bool = True


def _resolve_signal_id(panel: pl.DataFrame, idx: int, signal_ids: list[str] | None) -> str | None:
    """在触发行 idx 上, 从候选信号里找出 panel 列为 True 的那个, 返回其列名。

    多个信号同时为 True 时返回第一个匹配的 (信号 OR 关系, 回测只记录其一即可)。
    signal_ids 元素可能带 signal_/csg_ 前缀, 也可能是裸名 (如 "ma_golden_5_20")。
    """
    if not signal_ids:
        return None
    for sid in signal_ids:
        col = sid if (sid.startswith("signal_") or sid.startswith("csg_")) else f"signal_{sid}"
        if col not in panel.columns:
            continue
        try:
            if bool(panel[col][idx]):
                return col
        except (IndexError, TypeError):
            continue
    return None


# ================================================================
# PanelCache — 避免重复 scan_parquet + compute_all
# ================================================================


class _CacheEntry:
    __slots__ = ("df", "ts")

    def __init__(self, df: pl.DataFrame, ts: float):
        self.df = df
        self.ts = ts


class _InFlight:
    """同 key 正在计算的占位: leader 算完通过 done 唤醒所有跟随者复用结果。"""

    __slots__ = ("df", "done", "error")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.df: pl.DataFrame | None = None
        self.error: BaseException | None = None


class PanelCache:
    """LRU + TTL 数据面板缓存。"""

    def __init__(self, max_size: int = 2, ttl_seconds: int = 180):
        self._cache: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._ttl = ttl_seconds
        # 跨请求单例, SSE 回测在各自 daemon 线程并发访问 OrderedDict。
        # 无锁的 move_to_end/del/popitem check-then-act 会抛 "OrderedDict mutated"。
        # 用实例锁守护所有 OrderedDict 变更; compute_fn (重扫盘) 放锁外避免串行化。
        self._lock = threading.Lock()
        # single-flight: 同 key 只让一个线程 compute, 其余等其结果复用。
        # 否则优化器/walk-forward 的 max_workers 个线程冷启动同时 miss, 会并行加载 N 份同一面板。
        self._inflight: dict[str, _InFlight] = {}
        # 轻量遥测: 累加真实扫盘耗时与命中/复用次数, 用于量化 IO 占比 (是否值得进一步优化)。
        # compute_seconds 只计 leader 的实际 compute_fn 耗时, follower 复用不计 —— 反映真实 IO。
        self._compute_seconds = 0.0
        self._compute_count = 0  # 实际扫盘次数
        self._hit_count = 0  # 缓存命中 (未扫盘) 次数
        self._reuse_count = 0  # single-flight 跟随者复用次数

    def get_or_compute(
        self,
        symbols: list[str] | None,
        start: date,
        end: date,
        columns: list[str] | None,
        compute_fn,
        asset_type: str = "stock",
        generation: str | None = None,
    ) -> pl.DataFrame:
        key = self._make_key(symbols, start, end, columns, asset_type, generation)
        now = time.monotonic()

        with self._lock:
            entry = self._cache.get(key)
            if entry is not None:
                if now - entry.ts < self._ttl:
                    self._cache.move_to_end(key)
                    self._hit_count += 1
                    return entry.df
                del self._cache[key]  # 过期, 丢弃后重算
            # single-flight: 同 key 若已有线程在算, 登记为跟随者; 否则本线程当 leader。
            flight = self._inflight.get(key)
            leader = flight is None
            if leader:
                flight = _InFlight()
                self._inflight[key] = flight

        if not leader:
            # 跟随者: 等 leader 算完直接复用, 不重复 compute (消除缓存踩踏)。
            assert flight is not None  # 非 leader 必已在上方命中 inflight
            flight.done.wait()
            with self._lock:
                self._reuse_count += 1
            if flight.error is not None:
                raise flight.error
            return flight.df

        # leader: compute 放锁外 (不同 key 仍可并发, 保留原设计优点)。
        assert flight is not None  # leader 分支已在上方新建 inflight
        t_compute = time.perf_counter()
        try:
            df = compute_fn(symbols, start, end, columns, asset_type)
        except BaseException as e:
            # 失败不缓存: 摘除 inflight 让后续线程重试, 并把异常透传给已在等的跟随者。
            with self._lock:
                self._compute_seconds += time.perf_counter() - t_compute  # 失败也花了 IO, 计入
                self._compute_count += 1
                self._inflight.pop(key, None)
            flight.error = e
            flight.done.set()
            raise
        with self._lock:
            self._compute_seconds += time.perf_counter() - t_compute
            self._compute_count += 1
            self._cache[key] = _CacheEntry(df=df, ts=now)
            if len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
            self._inflight.pop(key, None)
        flight.df = df
        flight.done.set()
        return df

    def stats(self) -> dict:
        """遥测快照: 累计扫盘耗时/次数与命中/复用次数。首尾快照取差即区间内 IO 开销。"""
        with self._lock:
            return {
                "compute_seconds": round(self._compute_seconds, 4),
                "compute_count": self._compute_count,
                "hit_count": self._hit_count,
                "reuse_count": self._reuse_count,
            }

    def invalidate(self) -> None:
        with self._lock:
            self._cache.clear()

    @staticmethod
    def _make_key(
        symbols: list[str] | None,
        start: date,
        end: date,
        columns: list[str] | None,
        asset_type: str = "stock",
        generation: str | None = None,
    ) -> str:
        if symbols is None:
            h = "all"
        else:
            h = hashlib.md5(",".join(sorted(symbols)).encode()).hexdigest()[:12]
        cols = (
            "all"
            if columns is None
            else hashlib.md5(",".join(sorted(columns)).encode()).hexdigest()[:8]
        )
        return f"{asset_type}:{generation or 'unmanaged'}:{h}:{start}:{end}:{cols}"


# 等待进行中 enriched 发布的上限与轮询间隔。孤儿标记由 get_enriched_generation
# 在读取时直接自愈, 因此这里等到的 EnrichedGenerationUnavailableError 意味着
# 发布方确实存活 —— 对回测/优化这类长任务, 有界等待优于立即失败。仅用于
# worker 任务路径 (矩阵加载), 实时热路径不得调用 data_generation_await。
_GENERATION_WAIT_TIMEOUT_S = 300.0
_GENERATION_POLL_S = 1.0


# ================================================================
# BacktestEngine
# ================================================================


def _max_drawdown_from_pnls(pnls: np.ndarray, initial_capital: float) -> float:
    """用逐笔收益序列近似最大回撤；pnls 非空由调用方保证。"""
    equity = initial_capital
    peak = initial_capital
    max_dd = 0.0
    for p in pnls:
        equity *= 1 + p
        peak = max(peak, equity)
        dd = (equity - peak) / peak
        max_dd = min(max_dd, dd)
    return max_dd


def _read_enriched_panel_rows(
    enriched_glob: str,
    symbols: list[str] | None,
    start: date,
    end: date,
    columns: list[str] | None,
    asset_type: str,
) -> pl.DataFrame:
    """按 CK 唯一读源读取 enriched 面板，存储不可用时回退本地分区。"""
    try:
        from app.storage.runtime import get_storage

        runtime = get_storage()
        if runtime is not None:
            table = {
                "stock": "kline_daily_enriched",
                "etf": "kline_etf_enriched",
                "index": "kline_index_enriched",
            }.get(asset_type, "kline_daily_enriched")
            df = runtime.store.read_daily(
                table, symbols=symbols, start=start, end=end, columns=columns
            )
            return df.sort(["symbol", "date"])
        lf = scan_enriched_parquet(enriched_glob)
        if symbols is not None:
            lf = lf.filter(pl.col("symbol").is_in(symbols))
        if columns is not None:
            available = set(lf.collect_schema().names())
            selected = [c for c in columns if c in available]
            if "symbol" not in selected and "symbol" in available:
                selected.insert(0, "symbol")
            if "date" not in selected and "date" in available:
                selected.insert(1, "date")
            lf = lf.select(selected)
        return (
            lf.filter((pl.col("date") >= start) & (pl.col("date") <= end))
            .sort(["symbol", "date"])
            .collect(streaming=True)
        )
    except Exception as e:  # noqa: BLE001 - 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("backtest load panel failed: %s", e)
        return pl.DataFrame()


def _independent_candidate_empty_result(
    n_candidates: int, execution_stats: dict[str, int]
) -> SimResult:
    """无可成交样本时的独立候选空结果 (保持既有 stats 结构)。"""
    return SimResult(
        equity_curve=[],
        drawdown_curve=[],
        trades=[],
        per_symbol_stats=[],
        stats={
            "mode": "full",
            "full_kind": "candidate_execution",
            "error": "no executable trades",
            "n_candidates": int(n_candidates),
            "n_trades": 0,
            "execution": execution_stats,
        },
    )


def _independent_candidate_curves(
    trades: list[TradeRecord], include_curves: bool
) -> tuple[list[dict], list[dict], list[float], list[float]]:
    """按退出日聚合样本收益, 构造样本收益曲线 (非账户净值)。"""
    daily_returns: dict[str, list[float]] = {}
    for t in trades:
        daily_returns.setdefault(str(t.exit_date)[:10], []).append(float(t.pnl_pct))
    equity_curve: list[dict] = []
    drawdown_curve: list[dict] = []
    equity_values: list[float] = []
    daily_avg: list[float] = []
    equity = 1.0
    peak = 1.0
    for d_str in sorted(daily_returns):
        values = daily_returns[d_str]
        avg_ret = float(np.mean(values)) if values else 0.0
        daily_avg.append(avg_ret)
        equity *= 1 + avg_ret
        peak = max(peak, equity)
        dd = (equity - peak) / peak if peak > 0 else 0.0
        equity_value = round(float(equity), 4)
        equity_values.append(equity_value)
        if include_curves:
            equity_curve.append({"date": d_str, "value": equity_value, "positions": len(values)})
            drawdown_curve.append({"date": d_str, "value": round(float(dd), 4)})
    return equity_curve, drawdown_curve, equity_values, daily_avg


def _independent_candidate_stats(
    pnls: np.ndarray,
    durations: np.ndarray,
    daily: np.ndarray,
    equity_values: list[float],
    n_candidates: int,
    n_trades: int,
    n_days: int,
    execution_stats: dict[str, int],
) -> dict:
    """独立候选统计口径: 样本收益聚合 + 样本收益曲线派生指标。"""
    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]
    avg_win = float(np.mean(wins)) if len(wins) else 0.0
    avg_loss = abs(float(np.mean(losses))) if len(losses) else 0.0
    values = np.array(equity_values, dtype=float)
    total_return = float(values[-1] - 1.0) if len(values) else 0.0
    peaks = np.maximum.accumulate(values) if len(values) else np.array([])
    drawdowns = values / peaks - 1 if len(values) else np.array([])
    max_drawdown = float(drawdowns.min()) if len(drawdowns) else 0.0  # type: ignore[call-overload]
    _std = float(np.std(daily, ddof=1)) if len(daily) > 1 else 0.0
    sharpe = float(np.mean(daily) / _std * np.sqrt(252)) if _std > 0 else 0.0
    sortino = BacktestEngine._sortino_ratio(daily)
    return {
        "mode": "full",
        "full_kind": "candidate_execution",
        "n_candidates": int(n_candidates),
        "n_trades": n_trades,
        "n_days": n_days,
        "avg_daily_candidates": round(float(n_trades / max(n_days, 1)), 1),
        "avg_return": round(float(np.mean(pnls)), 4),
        "median_return": round(float(np.median(pnls)), 4),
        "win_rate": round(float(len(wins) / len(pnls)), 4) if len(pnls) else 0.0,
        "profit_factor": round(float(avg_win / avg_loss), 2) if avg_loss > 0 else None,
        "best": round(float(np.max(pnls)), 4),
        "worst": round(float(np.min(pnls)), 4),
        "avg_duration": round(float(np.mean(durations)), 1) if len(durations) else 0.0,
        "total_return": round(float(total_return), 4),
        "max_drawdown": round(float(max_drawdown), 4),
        "sharpe": round(float(sharpe), 2),
        "sortino": round(float(sortino), 2) if sortino is not None else None,
        "execution": execution_stats,
    }


def _resolve_matrix_field_columns(feature_plan, data_dir) -> tuple[set, frozenset]:
    """校验扩展时序字段后返回 (parquet 解析列, 现场注入的扩展列)。

    快照模式/字符串字段/未注册名在此拦截, 防止回看历史时引入未来数据。
    """
    from app.factors.ext_factors import numeric_timeseries_ext_columns

    field_columns = (
        set(feature_plan.base_columns)
        | set(feature_plan.instrument_columns)
        | set(feature_plan.matrix_columns)
    )
    ext_wanted = frozenset(c for c in field_columns if c.startswith("ext_"))
    if ext_wanted:
        invalid_ext = ext_wanted - numeric_timeseries_ext_columns(data_dir)
        if invalid_ext:
            raise ValueError(
                f"扩展字段不可用于矩阵回测 (字段已失效，或不是时序模式的数值字段): "
                f"{sorted(invalid_ext)}。请在策略设置中移除或替换相关评分因子、叠加条件；"
                "若策略源码直接引用该字段，请同步修改源码，或恢复对应的时序扩展数据。"
            )
        field_columns = field_columns - ext_wanted
    return field_columns, ext_wanted


def _matrix_cache_settings(cache_profile, data_dir, field_columns) -> tuple:
    """共享缓存根目录只保留非 ext 列, 避免残留扩展列拖垮整个缓存。"""
    cache_root = (
        data_dir / ".backtest_matrix_cache" if settings.backtest_matrix_disk_cache_enabled else None
    )
    cache_fields = (
        cache_profile.field_columns if cache_profile is not None else frozenset(field_columns)
    )
    cache_fields = frozenset(name for name in cache_fields if not name.startswith("ext_"))
    cache_max_bytes = (
        cache_profile.max_disk_bytes
        if cache_profile is not None
        else settings.backtest_matrix_cache_max_mb * 1024 * 1024
    )
    return cache_root, cache_fields, cache_max_bytes


@dataclass
class _PanelSignalArrays:
    dates: np.ndarray
    symbols: np.ndarray
    ent: np.ndarray
    ext: np.ndarray
    entry_prices: np.ndarray
    exit_prices: np.ndarray
    close_prices: np.ndarray


# ---- 高复杂度方法保留在原模块（享受 per-file-ignores），子模块通过 mixin 复用 ----


class _IndependentMatrixRunRiskMixin:
    """保留原文件的高复杂度撮合方法；子模块类通过继承复用。"""

    if TYPE_CHECKING:

        def _valid_price(self, *args: Any, **kwargs: Any) -> Any: ...
        def _try_close(self, *args: Any, **kwargs: Any) -> Any: ...

        _count: Any
        config: Any

        def _signal_date(self, *args: Any, **kwargs: Any) -> Any: ...
        def _risk_exit(self, *args: Any, **kwargs: Any) -> Any: ...
        def _refill(self, *args: Any, **kwargs: Any) -> Any: ...
        def _present(self, *args: Any, **kwargs: Any) -> Any: ...

        matrix: Any

        def _make_position(self, *args: Any, **kwargs: Any) -> Any: ...

        entry_prices: Any

        def _can_buy(self, *args: Any, **kwargs: Any) -> Any: ...

    def _run_entry(self, time_id: int, asset_id: int) -> None:  # noqa: C901 - 入场多条件风控串联，拆分会改变判定顺序
        m = self.matrix
        ok, blocked = self._can_buy(time_id, asset_id)
        if not ok:
            self._count(blocked)
            return
        score = _matrix_entry_score(m, time_id, asset_id)
        if self.config.score_min is not None and score < self.config.score_min:
            self._count("buy_score_filter")
            return
        if self.config.score_max is not None and score > self.config.score_max:
            self._count("buy_score_filter")
            return
        future_times = [f for f in range(time_id + 1, m.shape[0]) if self._present(f, asset_id)]
        if not future_times:
            self._count("sell_no_future")
            return
        entry_price = self._refill(
            time_id, asset_id, "buy", float(self.entry_prices[time_id, asset_id])
        )
        entry_date = m.timestamp_labels[time_id][:10]
        pos = self._make_position(time_id, asset_id, entry_price, score, entry_date)
        closed = False
        for future in future_times:
            pos["hold_days"] += 1  # type: ignore[operator]
            date_text = m.timestamp_labels[future][:10]
            reason, override = self._risk_exit(pos, future, asset_id)
            if reason and self._try_close(pos, future, asset_id, reason, date_text, override):
                closed = True
                break
            reason = None
            signal_date = date_text
            if pos.get("pending_exit_reason"):
                reason = str(pos["pending_exit_reason"])
                signal_date = str(pos.get("pending_exit_signal_date") or date_text)
            elif m.exit[future, asset_id]:
                reason = "signal"
                signal_date = self._signal_date(
                    int(m.exit_signal_time[future, asset_id]), date_text
                )
            elif (
                self.config.max_hold_days is not None
                and pos["hold_days"] >= self.config.max_hold_days  # type: ignore[operator]
            ):
                reason = "max_hold"
            elif future == future_times[-1]:
                reason = "end"
            if reason and self._try_close(pos, future, asset_id, reason, signal_date):
                closed = True
                break
            high_price = float(m.high[future, asset_id])
            if self._valid_price(high_price):
                pos["max_high"] = max(float(pos["max_high"]), high_price)  # type: ignore[arg-type]
        if not closed and future_times:
            last_time = future_times[-1]
            # 期末仍被递延的持仓按末日价格强制落账, blocked_exit_days 保留受阻痕迹。
            self._try_close(
                pos,
                last_time,
                asset_id,
                str(pos.get("pending_exit_reason") or "end"),
                m.timestamp_labels[last_time][:10],
                force=True,
            )


class _LegacyIndependentRunRiskMixin:
    """保留原文件的高复杂度撮合方法；子模块类通过继承复用。"""

    if TYPE_CHECKING:
        open_prices: Any
        low_prices: Any
        high_prices: Any
        config: Any

        def _valid_price(self, *args: Any, **kwargs: Any) -> Any: ...

    def _risk_exit(self, pos: dict, idx: int) -> tuple[str | None, float | None]:  # noqa: C901 - 单笔退出多档风控顺序判定，拆开易漏分支
        if pos.get("pending_exit_reason") or pos.get("entry_idx") == idx:
            return None, None
        entry_price = float(pos["entry_price"])
        if entry_price <= 0:
            return None, None
        open_price = float(self.open_prices[idx])
        low_price = float(self.low_prices[idx])
        high_price = float(self.high_prices[idx])
        peak_price = float(pos.get("max_high", entry_price))
        risk_lines: list[tuple[float, str]] = []
        cfg = self.config
        if cfg.stop_loss_pct is not None:
            risk_lines.append((entry_price * (1 - abs(cfg.stop_loss_pct)), "stop_loss"))
        if cfg.trailing_stop_pct is not None and peak_price > 0:
            risk_lines.append((peak_price * (1 - abs(cfg.trailing_stop_pct)), "trailing_stop"))
        activate = getattr(cfg, "trailing_take_profit_activate_pct", None)
        drawdown = getattr(cfg, "trailing_take_profit_drawdown_pct", None)
        if (
            activate is not None
            and drawdown is not None
            and peak_price > entry_price
            and peak_price / entry_price - 1 >= abs(float(activate))
        ):
            risk_lines.append((peak_price * (1 - abs(float(drawdown))), "trailing_take_profit"))
        risk_lines = [(line, reason) for line, reason in risk_lines if self._valid_price(line)]
        if risk_lines:
            stop_price, reason = max(risk_lines, key=lambda item: item[0])
            if self._valid_price(open_price) and open_price <= stop_price:
                return reason, open_price
            if self._valid_price(low_price) and low_price <= stop_price:
                return reason, stop_price
        tp_pct = getattr(cfg, "take_profit_pct", None)
        if tp_pct is not None:
            tp_line = entry_price * (1 + abs(float(tp_pct)))
            if self._valid_price(tp_line):
                if self._valid_price(open_price) and open_price >= tp_line:
                    return "take_profit", open_price
                if self._valid_price(high_price) and high_price >= tp_line:
                    return "take_profit", tp_line
        return None, None


class _MatrixPortfolioRunRiskMixin:
    """保留原文件的高复杂度撮合方法；子模块类通过继承复用。"""

    if TYPE_CHECKING:
        matrix: Any
        config: Any
        positions: Any

        def _valid_price(self, *args: Any, **kwargs: Any) -> Any: ...
        def _try_sell(self, *args: Any, **kwargs: Any) -> Any: ...

    def _risk_exit(self, time_id: int, date_text: str, sold_today: set[int]) -> None:  # noqa: C901 - 组合风控退出多条件，保持与原实现一致的判定顺序
        m = self.matrix
        cfg = self.config
        for asset_id in list(self.positions):
            pos = self.positions.get(asset_id)
            if pos is None or pos.get("pending_exit_reason") or pos["entry_date"] == date_text:
                continue
            if not m.tradable[time_id, asset_id] or pos["entry_price"] <= 0:
                continue
            open_price = float(m.open[time_id, asset_id])
            low_price = float(m.low[time_id, asset_id])
            high_price = float(m.high[time_id, asset_id])
            entry_price = float(pos["entry_price"])
            peak_price = float(pos["max_high"])
            risk_lines: list[tuple[float, str]] = []
            if cfg.stop_loss_pct is not None:
                risk_lines.append((entry_price * (1 - abs(cfg.stop_loss_pct)), "stop_loss"))
            if cfg.trailing_stop_pct is not None:
                risk_lines.append((peak_price * (1 - abs(cfg.trailing_stop_pct)), "trailing_stop"))
            activate = cfg.trailing_take_profit_activate_pct
            drawdown = cfg.trailing_take_profit_drawdown_pct
            if activate is not None and drawdown is not None and peak_price > entry_price:  # noqa: SIM102 - 独立前置条件
                if peak_price / entry_price - 1 >= abs(float(activate)):
                    risk_lines.append(
                        (peak_price * (1 - abs(float(drawdown))), "trailing_take_profit")
                    )
            valid_lines = [(line, reason) for line, reason in risk_lines if self._valid_price(line)]
            if valid_lines:
                stop_price, reason = max(valid_lines, key=lambda item: item[0])
                override = None
                if self._valid_price(open_price) and open_price <= stop_price:
                    override = open_price
                elif self._valid_price(low_price) and low_price <= stop_price:
                    override = stop_price
                if override is not None:
                    self._try_sell(time_id, asset_id, reason, date_text, sold_today, override)
                    continue
            if cfg.take_profit_pct is not None:
                take_profit = entry_price * (1 + abs(float(cfg.take_profit_pct)))
                if self._valid_price(open_price) and open_price >= take_profit:
                    self._try_sell(
                        time_id, asset_id, "take_profit", date_text, sold_today, open_price
                    )
                elif self._valid_price(high_price) and high_price >= take_profit:
                    self._try_sell(
                        time_id, asset_id, "take_profit", date_text, sold_today, take_profit
                    )


class _LegacyPortfolioRunRiskMixin:
    """保留原文件的高复杂度撮合方法；子模块类通过继承复用。"""

    if TYPE_CHECKING:
        config: Any
        positions: Any
        open_prices: Any
        low_prices: Any
        high_prices: Any

        def _valid_price(self, *args: Any, **kwargs: Any) -> Any: ...
        def _try_sell(self, *args: Any, **kwargs: Any) -> Any: ...

    def _risk_exits(self, d_str: str, row_by_symbol: dict[str, int], sold_today: set[str]) -> None:  # noqa: C901 - 多标的退出批量判定，拆分会改变成交/计数顺序
        cfg = self.config
        for sym in list(self.positions.keys()):
            pos = self.positions.get(sym)
            if pos is None or pos.get("pending_exit_reason"):
                continue
            if pos.get("entry_date") == d_str:
                continue
            idx = row_by_symbol.get(sym)
            if idx is None or pos["entry_price"] <= 0:
                continue
            open_price = float(self.open_prices[idx])
            low_price = float(self.low_prices[idx])
            high_price = float(self.high_prices[idx])
            entry_price = float(pos["entry_price"])
            peak_price = float(pos.get("max_high", entry_price))
            risk_lines: list[tuple[float, str]] = []
            if cfg.stop_loss_pct is not None:
                risk_lines.append((entry_price * (1 - abs(cfg.stop_loss_pct)), "stop_loss"))
            if cfg.trailing_stop_pct is not None and peak_price > 0:
                risk_lines.append((peak_price * (1 - abs(cfg.trailing_stop_pct)), "trailing_stop"))
            activate = getattr(cfg, "trailing_take_profit_activate_pct", None)
            drawdown = getattr(cfg, "trailing_take_profit_drawdown_pct", None)
            if (
                activate is not None
                and drawdown is not None
                and peak_price > entry_price
                and peak_price / entry_price - 1 >= abs(float(activate))
            ):
                risk_lines.append((peak_price * (1 - abs(float(drawdown))), "trailing_take_profit"))
            risk_lines = [(line, reason) for line, reason in risk_lines if self._valid_price(line)]
            if risk_lines:
                stop_price, reason = max(risk_lines, key=lambda item: item[0])
                override = None
                if self._valid_price(open_price) and open_price <= stop_price:
                    override = open_price
                elif self._valid_price(low_price) and low_price <= stop_price:
                    override = stop_price
                if override is not None:
                    self._try_sell(sym, idx, reason, d_str, sold_today, override)
                    continue
            tp_pct = getattr(cfg, "take_profit_pct", None)
            if tp_pct is not None:
                tp_line = entry_price * (1 + abs(float(tp_pct)))
                if self._valid_price(tp_line):
                    if self._valid_price(open_price) and open_price >= tp_line:
                        self._try_sell(sym, idx, "take_profit", d_str, sold_today, open_price)
                    elif self._valid_price(high_price) and high_price >= tp_line:
                        self._try_sell(sym, idx, "take_profit", d_str, sold_today, tp_line)


# ---- 拆分子模块（公开类仍由本模块导出）----
from .engine_backtest_data import (  # noqa: E402 - 末尾导入，子模块回引本模块符号
    BacktestDataMixin,
)
from .engine_backtest_sim import (  # noqa: E402 - 末尾导入，子模块回引本模块符号
    BacktestSimMixin,
)
from .engine_backtest_stats import (  # noqa: E402 - 末尾导入，子模块回引本模块符号
    BacktestStatsMixin,
)
from .engine_runs_independent import (  # noqa: E402 - 末尾导入，子模块回引本模块符号
    _IndependentMatrixRun,
    _panel_signal_arrays,
    _simulate_panel_symbol_trades,
)
from .engine_runs_independent_legacy import (  # noqa: E402 - 末尾导入，子模块回引本模块符号
    _LegacyIndependentRun,
)
from .engine_runs_portfolio import (  # noqa: E402 - 末尾导入，子模块回引本模块符号
    _MatrixPortfolioRun,
)
from .engine_runs_portfolio_legacy import (  # noqa: E402 - 末尾导入，子模块回引本模块符号
    _LegacyPortfolioRun,
)


class BacktestEngine(BacktestDataMixin, BacktestSimMixin, BacktestStatsMixin):
    """回测引擎 — 数据加载 + 撮合模拟 + 统计计算。

    实现按职责拆到同目录 mixin 子模块；本类保留公开符号与组合入口。
    """
