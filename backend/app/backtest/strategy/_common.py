"""策略回测服务 — 复用 StrategyDef 体系做全周期回测。

核心优化: 向量化 filter_fn，不逐日调用 StrategyEngine.run()。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

import numpy as np
import polars as pl

from app.backtest.engine import (
    SimulationOptions,
)
from app.backtest.matrix import (
    MarketDataMatrix,
    MatrixComputeCache,
    PanelMatrixEncoding,
)
from app.config import settings
from app.indicators.pipeline import (
    ENRICHED_STORAGE_COLS,
    INDICATOR_COLUMNS,
)
from app.strategy.engine import StrategyDef

logger = logging.getLogger("app.backtest.strategy")

BENCHMARK_SYMBOL = "000001.SH"
_EXECUTION_COLUMNS = frozenset(
    {
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "name",
        "score",
        "signal_limit_up",
        "signal_limit_down",
    }
)
_LIMIT_BASE_COLUMNS = frozenset({"raw_close", "raw_high", "raw_low"})
_INSTRUMENT_COLUMNS = frozenset({"name", "total_shares", "float_shares"})


def validate_minute_backtest_coverage(
    strategy: StrategyDef,
    repo,
    start: date,
    asset_type: str,
) -> None:
    """页面与 AI 工具共用分钟回测前置检查，不改变逐日回放的缺分区语义。"""
    if strategy.execution_backend != "minute_filter":
        return
    if asset_type != "stock":
        raise ValueError("分钟策略回测当前仅支持 A 股 (stock)")
    if repo is None:
        raise ValueError("行情仓库未注入，无法检查分钟K覆盖范围")
    earliest = repo.earliest_minute_date()
    if earliest is None or start < earliest:
        have = f"最早到 {earliest}, " if earliest else ""
        raise ValueError(
            f"本地分钟K{have}无法覆盖回测起始日 {start}。"
            "请先用「扩展分钟K历史」拉取更多数据, 或缩小回测区间"
        )


@dataclass(frozen=True)
class FeaturePlan:
    required_features: frozenset[str]
    required_signals: frozenset[str]
    warmup_bars: int


@dataclass(frozen=True)
class ResolvedFeaturePlan:
    base_columns: frozenset[str]
    intermediate_columns: frozenset[str]
    indicator_columns: frozenset[str]
    signal_columns: frozenset[str]
    matrix_columns: frozenset[str]
    instrument_columns: frozenset[str]
    warmup_bars: int
    full_feature_fallback: bool = False
    execution_backend: str = "polars_expr"
    # 财务因子列不落 enriched 存储, 由 engine 在加载口按公告日门控附加。
    fundamental_columns: frozenset[str] = frozenset()


def _merge_resolved_feature_plans(
    plans: list[ResolvedFeaturePlan],
) -> ResolvedFeaturePlan:
    if not plans:
        raise ValueError("cannot merge an empty feature plan list")
    backends = {plan.execution_backend for plan in plans}
    if len(backends) != 1:
        raise ValueError("shared preparation requires a single execution backend")

    def _union(field: str) -> frozenset[str]:
        merged: set[str] = set()
        for plan in plans:
            merged.update(getattr(plan, field))
        return frozenset(merged)

    return ResolvedFeaturePlan(
        base_columns=_union("base_columns"),
        intermediate_columns=_union("intermediate_columns"),
        indicator_columns=_union("indicator_columns"),
        signal_columns=_union("signal_columns"),
        matrix_columns=_union("matrix_columns"),
        instrument_columns=_union("instrument_columns"),
        warmup_bars=max(plan.warmup_bars for plan in plans),
        full_feature_fallback=any(plan.full_feature_fallback for plan in plans),
        execution_backend=backends.pop(),
        fundamental_columns=_union("fundamental_columns"),
    )


# 叠加条件 → numpy 元素级运算符 (与 custom_signals._OP_BUILDERS 同集;
# matrix 路径在 float 数组上评估, NaN 一律视为不命中)
_OVERLAY_NP_OPS = {
    ">": np.greater,
    ">=": np.greater_equal,
    "<": np.less,
    "<=": np.less_equal,
    "==": np.equal,
    "!=": np.not_equal,
}


def _overlay_numeric_fields(overrides: dict) -> set[str]:
    """叠加条件 (overlay_filter) 引用的数值字段集合, 含 field: 右值引用。

    matrix 路径专用: 矩阵 fields 是 float 数组, 字符串扩展列进不了矩阵 —
    引用字符串字段时抛 ValueError (保存期校验已拦, 这里是运行期双保险)。
    """
    conditions = (overrides or {}).get("overlay_filter")
    if not isinstance(conditions, list) or not conditions:
        return set()
    from app.factors.ext_factors import ext_string_fields

    string_fields = ext_string_fields()
    fields: set[str] = set()
    for i, c in enumerate(conditions):
        if not isinstance(c, dict):
            raise ValueError(f"第 {i + 1} 个叠加条件格式错误")
        left = str(c.get("left", ""))
        if left in string_fields:
            raise ValueError(f"第 {i + 1} 个叠加条件: matrix_native 策略不支持字符串字段 {left!r}")
        fields.add(left)
        right = c.get("right")
        if isinstance(right, str) and right.startswith("field:"):
            col = right[len("field:") :]
            if col in string_fields:
                raise ValueError(
                    f"第 {i + 1} 个叠加条件: matrix_native 策略不支持字符串字段引用 {col!r}"
                )
            fields.add(col)
    return fields


def _normalize_signal_name(signal: str) -> str:
    if signal.startswith(("signal_", "csg_")):
        return signal
    return f"signal_{signal}"


def _filter_dependencies(strategy: StrategyDef, params: dict) -> tuple[set[str], bool]:
    if strategy.filter_history_fn:
        return set(strategy.required_features), bool(strategy.required_features)
    if not strategy.filter_fn:
        return set(), True
    try:
        expr = strategy.filter_fn(pl.DataFrame(), params)
        if expr is None:
            return set(), True
        return set(expr.meta.root_names()), True
    except Exception as exc:  # noqa: BLE001 - 依赖解析失败回退声明的 required_features, 不中断回测
        logger.warning("strategy filter dependency resolution failed: %s", exc)
        return set(strategy.required_features), bool(strategy.required_features)


def _basic_filter_dependencies(config: dict) -> set[str]:
    if not config or not config.get("enabled", True):
        return set()
    dependencies = {"symbol", "close"}
    if any(config.get(key) is not None for key in ("amount_min", "amount_max")):
        dependencies.add("amount")
    if any(config.get(key) is not None for key in ("turnover_min", "turnover_max")):
        dependencies.add("turnover_rate")
    if any(config.get(key) is not None for key in ("market_cap_min", "market_cap_max")):
        dependencies.add("total_shares")
    if any(config.get(key) is not None for key in ("float_cap_min", "float_cap_max")):
        dependencies.add("float_shares")
    if config.get("exclude_st"):
        dependencies.add("name")
    return dependencies


_SHARE_CAP_FILTER_KEYS = (
    "market_cap_min",
    "market_cap_max",
    "float_cap_min",
    "float_cap_max",
)

# 换手率界同样依赖股本派生字段 (turnover_rate ← float_shares):
# 非股票资产 (etf/index) 没有股本数据, 若保留非 None 的换手率界,
# _basic_filter_dependencies 会解析出 turnover_rate 字段需求,
# 矩阵缓存档构建时因无 float_shares 而失败 (matrix turnover_rate requires
# float_shares)。与市值界同一族问题, 必须一并中和。
_TURNOVER_FILTER_KEYS = (
    "turnover_min",
    "turnover_max",
)

# 股票专属的价格界与板块过滤对非股票资产同样不可满足 (#215):
# ETF 单价普遍 0.5~7 元, 会被 price_min=3 整列误杀; boards 按股票代码
# 前缀匹配, ETF 代码不属于任何板块 → 掩码全 False, 静默零信号。
_STOCK_ONLY_FILTER_KEYS = (
    *_SHARE_CAP_FILTER_KEYS,
    *_TURNOVER_FILTER_KEYS,
    "price_min",
    "price_max",
    "boards",
)


def _basic_filter_for_asset(basic_filter: dict, asset_type: str) -> dict:
    """非股票资产没有股本数据 (etf/index 维表只有 symbol/name), 市值、流通
    市值与换手率界对它们既无意义也不可满足: 依赖解析与运行期过滤前先置
    None。价格界 (price_min/max) 与板块过滤 (boards) 是股票专属口径, 对
    ETF 同样不可满足, 一并中和, 否则入场候选在运行期被静默清零 (#215)。

    置 None 后: 依赖解析不再产出 total_shares/float_shares/turnover_rate
    需求; polars 侧有列守卫 (engine._basic_filter_expr), 矩阵侧
    _optional_field 对缺失字段返回全 NaN 且 _apply_bound 跳过全 NaN 界。
    """
    if asset_type == "stock" or not basic_filter:
        return basic_filter
    sanitized = dict(basic_filter)
    for key in _STOCK_ONLY_FILTER_KEYS:
        sanitized[key] = None
    return sanitized


def _resolve_base_columns(features: set[str]) -> frozenset[str]:
    storage = set(ENRICHED_STORAGE_COLS)
    base = {"symbol", "date"} | (features & storage)
    close_indicators = set(INDICATOR_COLUMNS) - {
        "atr_14",
        "amplitude",
        "kdj_k",
        "kdj_d",
        "kdj_j",
        "vol_ma5",
        "vol_ma10",
        "vol_ratio_5d",
    }
    if features & close_indicators:
        base.add("close")
    if features & {"atr_14", "amplitude", "kdj_k", "kdj_d", "kdj_j"}:
        base.update({"high", "low", "close"})
    if features & {"vol_ma5", "vol_ma10", "vol_ratio_5d"}:
        base.add("volume")
    base.update({"open", "high", "low", "close", "volume"})
    return frozenset(base & storage)


@dataclass
class StrategyBacktestConfig:
    strategy_id: str
    symbols: list[str] | None
    start: date
    end: date
    params: dict | None = None
    overrides: dict | None = None
    # matching 为向后兼容入口; 显式传 entry_fill/exit_fill 时以二者为准。
    matching: Literal["close_t", "open_t+1"] = "open_t+1"
    entry_fill: Literal["close_t", "open_t+1"] | None = None
    exit_fill: Literal["close_t", "open_t+1", "signal_next_minute"] | None = None
    fees_pct: float = 0.0002
    commission_pct: float | None = None
    stamp_tax_pct: float | None = None
    slippage_bps: float = 5.0
    max_positions: int = 10
    max_exposure_pct: float = 1.0
    initial_capital: float = 1_000_000.0
    position_sizing: Literal["equal", "score_weight", "policy"] = "equal"
    # 自定义仓位策略: 注册表 id + 请求级参数覆盖 (合并 META 默认值后注入引擎)
    position_policy: str | None = None
    position_policy_params: dict | None = None
    mode: Literal["position", "full"] = "position"
    asset_type: str = "stock"
    holding_days: int = 5
    # 分钟K精确成交: 开启后用当日分钟K确定穿越价/VWAP (需 Pro+ 分钟K能力)
    minute_fill: bool = False
    # 市场环境过滤: {"states": ["strong",...], "min_score": 60}。
    # 强制 T-1: regime[T-1] 决定 entry[T](防未来函数)。None=不过滤。
    regime_filter: dict | None = None

    def __post_init__(self) -> None:
        if self.entry_fill is None:
            self.entry_fill = self.matching
        if self.exit_fill is None:
            self.exit_fill = self.matching


def _resolve_position_policy(
    config: StrategyBacktestConfig,
) -> tuple[Callable[..., object] | None, dict | None]:
    """position_sizing="policy" 时解析用户仓位策略 → (compute, 合并参数)。

    每次回测解析一次 (注册表有目录签名缓存); 策略不存在/不可用直接抛
    ValueError — fail-closed 拒绝运行, 不静默回退等权 (见引擎侧兜底注释)。
    """
    if config.position_sizing != "policy" or not config.position_policy:
        return None, None
    from app.backtest import position_policies  # 按需导入: 默认路径零加载成本

    policy = position_policies.get_policy(settings.data_dir, config.position_policy)
    if policy is None:
        raise ValueError(f"仓位策略不存在或不可用: {config.position_policy}")
    defaults = {
        str(p.get("id")): p.get("default")
        for p in policy.params
        if isinstance(p, dict) and p.get("id")
    }
    merged = {**defaults, **(config.position_policy_params or {})}
    return policy.compute, merged


@dataclass
class StrategyBacktestResult:
    run_id: str
    config: dict
    stats: dict = field(default_factory=dict)
    equity_curve: list[dict] = field(default_factory=list)
    drawdown_curve: list[dict] = field(default_factory=list)
    benchmark_curve: list[dict] = field(default_factory=list)
    trades: list[dict] = field(default_factory=list)
    per_symbol_stats: list[dict] = field(default_factory=list)
    strategy_info: dict = field(default_factory=dict)
    factor_attribution: dict | None = None
    elapsed_ms: float = 0.0
    error: str | None = None
    evidence: dict | None = None


@dataclass(frozen=True)
class BacktestResultPolicy:
    """Explicit result contract for full backtests and lightweight optimizer trials."""

    required_stats: frozenset[str] | None = None
    include_monte_carlo: bool = True
    include_curves: bool = True
    include_trades: bool = True
    include_per_symbol_stats: bool = True
    include_return_distribution: bool = True
    include_benchmark: bool = True
    include_strategy_info: bool = True

    @classmethod
    def optimizer_trial(cls, objective: str) -> BacktestResultPolicy:
        return cls(
            # n_trades 恒保留: 引擎侧本就产出的计数 (include_trades 只裁交易列表),
            # 优化器交易数门槛 (guards.trades_ok) 依赖它, 缺席会被 fail-closed 误杀。
            required_stats=frozenset({str(objective), "n_trades"}),
            include_monte_carlo=str(objective).startswith("mc_maxdd_"),
            include_curves=False,
            include_trades=False,
            include_per_symbol_stats=False,
            include_return_distribution=False,
            include_benchmark=False,
            include_strategy_info=False,
        )

    def simulation_options(self) -> SimulationOptions:
        return SimulationOptions(
            include_monte_carlo=self.include_monte_carlo,
            include_curves=self.include_curves,
            include_trades=self.include_trades,
            include_per_symbol_stats=self.include_per_symbol_stats,
            include_return_distribution=self.include_return_distribution,
        )

    def select_stats(self, stats: dict) -> dict:
        if self.required_stats is None:
            return stats
        diagnostic = {
            "error",
            "timing_ms",
            "execution",
            "selection",
            "execution_backend",
            "shared_market_data",
            "shared_market_data_bytes",
            "shared_prepare_timing_ms",
            "matrix_data_cache_hit",
            "matrix_compute_cache",
            "market_matrix_shape",
            "market_matrix_bytes",
            "panel_rows",
            "panel_columns",
            "feature_columns",
            "full_feature_fallback",
        }
        keep = set(self.required_stats) | diagnostic
        return {key: value for key, value in stats.items() if key in keep}


def _factor_attribution_summary(
    snapshot: pl.DataFrame,
    trades: list,
) -> dict | None:
    """v1 因子归因: 入场信号日因子快照 x 成交盈亏, 对比盈利/亏损单因子均值。

    snapshot 来自 _apply_score 物化的候选行 (与评分同一条计算管线), 模拟结束后
    按 (symbol, 信号日) 关联成交。快照缺失、无可关联行或因子列全空时返回 None,
    归因失败不影响回测主结果。
    """
    factor_cols = [c for c in snapshot.columns if c not in ("symbol", "date")]
    if not factor_cols or not trades:
        return None
    normalized = snapshot.with_columns(pl.col("date").cast(pl.Utf8).str.slice(0, 10).alias("date"))
    symbols: list[str] = []
    days: list[str] = []
    pnls: list[float] = []
    for trade in trades:
        day = trade.entry_signal_date or trade.entry_date
        if day is None:
            continue
        symbols.append(trade.symbol)
        days.append(str(day)[:10])
        pnls.append(float(trade.pnl_pct))
    if not symbols:
        return None
    frame = pl.DataFrame({"symbol": symbols, "date": days, "pnl_pct": pnls})
    joined = frame.join(normalized, on=["symbol", "date"], how="left")
    win = joined.filter(pl.col("pnl_pct") > 0)
    lose = joined.filter(pl.col("pnl_pct") <= 0)
    factors: list[dict] = []
    for col in factor_cols:
        win_vals = win.get_column(col).drop_nulls().cast(pl.Float64)
        lose_vals = lose.get_column(col).drop_nulls().cast(pl.Float64)
        if win_vals.is_empty() and lose_vals.is_empty():
            continue
        factors.append(
            {
                "factor": col,
                "win_mean": round(float(win_vals.mean()), 6) if not win_vals.is_empty() else None,
                "lose_mean": round(float(lose_vals.mean()), 6)
                if not lose_vals.is_empty()
                else None,
                "win_n": int(win_vals.len()),
                "lose_n": int(lose_vals.len()),
            }
        )
    if not factors:
        return None
    return {"factors": factors, "n_win": win.height, "n_lose": lose.height}


@dataclass(frozen=True)
class PreparedMatrixBacktest:
    """Job-scoped immutable market data reused by every optimizer trial."""

    signature: tuple
    market_data: MarketDataMatrix
    feature_width: int
    load_start: date
    load_end: date
    sim_end: date
    entry_time_mask: np.ndarray
    exit_time_mask: np.ndarray
    start_id: int
    stop_id: int
    reference_price: np.ndarray | None
    prepare_timing_ms: dict[str, float]
    compute_cache: MatrixComputeCache


def _panel_backend(execution_backend: str) -> bool:
    """该执行后端走 polars 面板路径 (run 的 else 分支): polars_expr 与
    python_history_legacy (动态 filter_history) 等一切非矩阵/复合/分钟后端。"""
    return execution_backend not in ("matrix_native", "composite", "minute_filter")


@dataclass(frozen=True)
class PreparedPanelBacktest:
    """polars_expr 参数网格的共享特征面板 (P2a) — 一次加载, 全部试算复用。

    面板列是全部参数组依赖计划的并集; run 逐组做子集校验 (fail-closed),
    缺列直接报错而不是静默回退加载 — 静默回退会掩盖计划漂移, 且优化器
    正在拿"部分组快、部分组慢"的混合结果, 不如整批显式失败。"""

    signature: tuple
    panel: pl.DataFrame
    columns: frozenset[str]
    load_start: date
    load_end: date
    sim_end: date
    # sim 半边的参数无关矩阵编码 (P2a-2); None = 未启用 (回退逐组建矩阵)
    sim_encoding: PanelMatrixEncoding | None = None


@dataclass
class _RunState:
    """run 跨阶段共享的局部变量容器；只做数据搬运，不改变任何业务语义。"""

    config: StrategyBacktestConfig
    t0: float
    run_id: str
    result_policy: BacktestResultPolicy
    progress_cb: Callable[[dict], None] | None
    cancel_event: threading.Event | None
    prepared: PreparedMatrixBacktest | None
    prepared_panel: PreparedPanelBacktest | None
    evidence: Any
    factor_snapshot: dict
    err: Callable[[str], StrategyBacktestResult]
    # 策略与参数
    s: Any = None
    params: dict = field(default_factory=dict)
    overrides: dict = field(default_factory=dict)
    basic_filter: dict = field(default_factory=dict)
    entry_signals: list = field(default_factory=list)
    exit_signals: list = field(default_factory=list)
    overlay_conditions: Any = None
    stop_loss: Any = None
    take_profit: Any = None
    trailing_stop: Any = None
    trailing_take_profit_activate: Any = None
    trailing_take_profit_drawdown: Any = None
    max_hold_days: Any = None
    score_min: Any = None
    score_max: Any = None
    full_horizon_days: int = 0
    # 特征计划
    feature_plan: Any = None
    composite_children_resolved: Any = None
    # 数据加载
    timing_ms: dict = field(default_factory=dict)
    matrix_data_cache_hit: bool = False
    matrix_data_cache_status: str = "none"
    matrix_data_cache_timing_ms: Any = None
    load_start: Any = None
    load_end: Any = None
    sim_end: Any = None
    panel: Any = None
    formal_range: Any = None
    market_data: Any = None
    feature_width: int = 0
    # 撮合与信号
    matcher_config: Any = None
    t_signal: float = 0.0
    signal_matrix: Any = None
    market_matrix: Any = None
    selection_stats: dict = field(default_factory=dict)
    raw_candidates: int = 0
    panel_rows: int = 0
    panel_columns: int = 0
    entry_time_mask: Any = None
    exit_time_mask: Any = None
    start_id: int = 0
    stop_id: int = 0
    reference_price: Any = None
    candidate_mask: Any = None
    entry_mask: Any = None
    raw_exit_mask: Any = None
    sim_columns: Any = None
    sim_panel: Any = None
    sim_entry_mask: Any = None
    sim_exit_mask: Any = None
    # 结果
    result: Any = None
    benchmark_curve: list = field(default_factory=list)
    strategy_info: dict = field(default_factory=dict)
    # 分钟回放分支
    replay: Any = None
    axis_dates: list = field(default_factory=list)
    axis_symbols: list = field(default_factory=list)
    trigger_times: dict = field(default_factory=dict)
    dropped_axis_hits: int = 0
    signals: Any = None
    minute_sim_panel: Any = None
    entry_price_override: Any = None
