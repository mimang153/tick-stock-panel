"""Internal split module for app.backtest.factor: _common."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from app.factors.registry import factor_columns_view as _factor_columns_view

logger = logging.getLogger("app.backtest.factor")


FACTOR_COLUMNS: list[dict] = _factor_columns_view()


FACTOR_WARMUP_DAYS = 120


FACTOR_METHODOLOGY_VERSION = "factor_v2"


_DAILY_FORWARD_HORIZONS = (1, 3, 5)


@dataclass
class FactorConfig:
    factor_name: str
    symbols: list[str] | None
    start: date
    end: date
    n_groups: int = 5
    rebalance: Literal["daily", "weekly", "monthly"] = "monthly"
    weight: Literal["equal", "factor_weight"] = "equal"
    fees_pct: float = 0.0002
    slippage_bps: float = 5.0
    asset_type: str = "stock"
    commission_pct: float | None = None
    stamp_tax_pct: float | None = None


@dataclass
class GroupStats:
    group: int
    label: str
    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe: float
    win_rate: float


@dataclass
class FactorResult:
    run_id: str
    config: dict
    # IC 分析
    ic_mean: float | None = None
    ic_std: float | None = None
    ir: float | None = None
    ic_win_rate: float | None = None
    ic_series: list[dict] = field(default_factory=list)
    # 分层
    group_stats: list[dict] = field(default_factory=list)
    group_nav: list[dict] = field(default_factory=list)
    # 多空
    long_short_stats: dict = field(default_factory=dict)
    long_short_nav: list[dict] = field(default_factory=list)
    # 元信息
    elapsed_ms: float = 0.0
    n_symbols: int = 0
    n_dates: int = 0
    error: str | None = None
    # factor_v2 兼容扩展字段必须追加在旧字段之后, 保留位置参数语义。
    methodology_version: str = FACTOR_METHODOLOGY_VERSION
    coverage: float | None = None
    turnover: float | None = None
    long_short_sharpe: float | None = None
    yearly_ic: list[dict] = field(default_factory=list)
    ic_decay: list[dict] = field(default_factory=list)
    regime_stats: list[dict] = field(default_factory=list)


@dataclass
class FactorBatchConfig:
    factor_names: list[str]
    symbols: list[str] | None
    start: date
    end: date
    n_groups: int = 5
    rebalance: Literal["daily", "weekly", "monthly"] = "monthly"
    weight: Literal["equal", "factor_weight"] = "equal"
    fees_pct: float = 0.0002
    slippage_bps: float = 5.0
    asset_type: str = "stock"
    commission_pct: float | None = None
    stamp_tax_pct: float | None = None


@dataclass
class FactorBatchItem:
    factor_name: str
    label: str
    group: str
    ic_mean: float | None = None
    ir: float | None = None
    ic_win_rate: float | None = None
    long_short_return: float | None = None
    long_short_max_drawdown: float | None = None
    n_symbols: int = 0
    n_dates: int = 0
    elapsed_ms: float = 0.0
    error: str | None = None
    methodology_version: str = FACTOR_METHODOLOGY_VERSION
    coverage: float | None = None
    turnover: float | None = None
    long_short_sharpe: float | None = None
    yearly_ic: list[dict] = field(default_factory=list)
    ic_decay: list[dict] = field(default_factory=list)
    regime_stats: list[dict] = field(default_factory=list)
    # metrics_v2 (P3): NW HAC t 值 (滞后=1, 日频 1 日前瞻) 与 BH-FDR q 值; 样本不足为 None
    t_naive: float | None = None
    t_newey_west: float | None = None
    nw_lag: int | None = None
    p_value: float | None = None
    q_value: float | None = None


@dataclass
class FactorBatchResult:
    run_id: str
    config: dict
    results: list[FactorBatchItem] = field(default_factory=list)
    elapsed_ms: float = 0.0
    n_symbols: int = 0
    n_dates: int = 0
    error: str | None = None
