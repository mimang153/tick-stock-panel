"""Internal split module for app.backtest.mining_runtime: _common."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

import polars as pl

from app.backtest.mining import FactorMetric, MiningRequest
from app.backtest.strategy import BacktestResultPolicy

ProgressCallback = Callable[[dict[str, Any]], None]


CancelCheck = Callable[[], bool] | Any


_PROFILE_NAMES = frozenset({"exploratory", "balanced", "strict"})


_MINING_MATRIX_CACHE_BYTES = 32 * 1024 * 1024


_RESULT_POLICY = BacktestResultPolicy(
    required_stats=frozenset({"total_return", "sharpe", "max_drawdown", "n_trades"}),
    include_monte_carlo=False,
    include_curves=False,
    include_trades=False,
    include_per_symbol_stats=False,
    include_return_distribution=False,
    include_benchmark=False,
    include_strategy_info=False,
)


_SNAPSHOT_MAX_ATTEMPTS = 3


_SNAPSHOT_PUBLISH_WAIT_SECONDS = 5.0


_REGIME_FILTERS: dict[str, dict[str, list[str]]] = {
    "strong": {"states": ["strong", "lean_strong"]},
    "range": {"states": ["range"]},
    "weak": {"states": ["lean_weak", "weak"]},
}


_WEIGHT_RUNGS: tuple[float, ...] = (0.5, 1.0, 1.5, 2.0)


_REFINE_TOP_N = 1  # 只精调最终冠军


_REFINE_MAX_VARIANTS = 64  # 4^(n-1), n≤4


_REFINE_MAX_TRIALS = 96  # 精调总回测预算 (与 MiningBudget.max_trials 同量级)


_REFINE_DEGRADATION_LIMIT = 0.2  # stage2 全窗退化超过 20% 放弃精调结果


class MiningRuntimeCancelledError(RuntimeError):
    pass


@dataclass(frozen=True)
class RuntimeRequest:
    run_id: str
    factor_names: tuple[str, ...]
    strategy_ids: tuple[str, ...]
    symbols: list[str] | None
    asset_type: Literal["stock", "etf"]
    start: date
    end: date
    profile: Literal["exploratory", "balanced", "strict"]
    forward_horizon: int
    commission_pct: float
    stamp_tax_pct: float
    slippage_bps: float
    correlation_threshold: float
    max_finalists: int
    require_regime: bool
    mining_request: MiningRequest


class TrainingMetricProvider:
    """Compute fold-local metrics and retain only compact call telemetry."""

    def __init__(self, target_column: str) -> None:
        self.target_column = target_column
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        train: pl.DataFrame,
        factor_names: Sequence[str],
    ) -> tuple[FactorMetric, ...]:
        from ._artifacts import _factor_metric
        from ._refine import _date_labels

        metrics = tuple(
            _factor_metric(train, factor_name, self.target_column) for factor_name in factor_names
        )
        labels = _date_labels(train)
        self.calls.append(
            {
                "start": labels[0] if labels else None,
                "end": labels[-1] if labels else None,
                "rows": train.height,
                "metrics": metrics,
            }
        )
        return metrics


_SYMBOL_BATCH_SIZE = 512
