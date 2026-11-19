"""Internal split module for app.backtest.mining: _common."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, Protocol, cast

import polars as pl

logger = logging.getLogger("app.backtest.mining")


MAX_MINING_FACTORS = 48


MAX_EXISTING_STRATEGIES = 8


MAX_COMBINATION_SIZE = 4


MAX_BEAM_WIDTH = 32


MAX_FINALISTS = 8


MAX_REAL_TRIALS = 256


GATE_MIN_VALID_FOLDS = 2


GATE_MIN_POSITIVE_FOLD_RATIO = 2.0 / 3.0


GATE_MIN_OOS_SHARPE = 0.5


GATE_MAX_DRAWDOWN = -0.25


GATE_MIN_TRADES = 60


MiningProfile = Literal["exploratory", "balanced", "strict"]


class JsonDataclassMixin:
    """Provide JSON output without coupling pure models to Pydantic."""

    def to_json(self) -> str:
        return json.dumps(asdict(cast(Any, self)), ensure_ascii=False, allow_nan=False)


@dataclass(frozen=True)
class MiningBudget(JsonDataclassMixin):
    max_factors: int = MAX_MINING_FACTORS
    max_existing_strategies: int = MAX_EXISTING_STRATEGIES
    max_combination_size: int = MAX_COMBINATION_SIZE
    beam_width: int = 16
    max_proxy_trials: int = 256
    max_trials: int = 96

    def __post_init__(self) -> None:
        if not 1 <= self.max_factors <= MAX_MINING_FACTORS:
            raise ValueError(f"max_factors must be between 1 and {MAX_MINING_FACTORS}")
        if not 0 <= self.max_existing_strategies <= MAX_EXISTING_STRATEGIES:
            raise ValueError(
                f"max_existing_strategies must be between 0 and {MAX_EXISTING_STRATEGIES}"
            )
        if not 1 <= self.max_combination_size <= MAX_COMBINATION_SIZE:
            raise ValueError(f"max_combination_size must be between 1 and {MAX_COMBINATION_SIZE}")
        if not 1 <= self.beam_width <= MAX_BEAM_WIDTH:
            raise ValueError(f"beam_width must be between 1 and {MAX_BEAM_WIDTH}")
        if self.max_proxy_trials <= 0:
            raise ValueError("max_proxy_trials must be positive")
        if not 1 <= self.max_trials <= MAX_REAL_TRIALS:
            raise ValueError(f"max_trials must be between 1 and {MAX_REAL_TRIALS}")

    @classmethod
    def exploratory(cls) -> MiningBudget:
        return cls(
            max_combination_size=3,
            beam_width=8,
            max_proxy_trials=96,
            max_trials=32,
        )

    @classmethod
    def balanced(cls) -> MiningBudget:
        return cls()

    @classmethod
    def strict(cls) -> MiningBudget:
        return cls(beam_width=32, max_proxy_trials=512, max_trials=256)


@dataclass(frozen=True)
class NestedValidationConfig(JsonDataclassMixin):
    outer_train_bars: int = 504
    outer_test_bars: int = 126
    outer_step_bars: int = 63
    inner_train_bars: int = 252
    inner_test_bars: int = 63
    inner_step_bars: int = 63
    purge_bars: int = 30
    embargo_bars: int = 5
    min_train_bars: int = 126

    def __post_init__(self) -> None:
        positive = {
            "outer_train_bars": self.outer_train_bars,
            "outer_test_bars": self.outer_test_bars,
            "outer_step_bars": self.outer_step_bars,
            "inner_train_bars": self.inner_train_bars,
            "inner_test_bars": self.inner_test_bars,
            "inner_step_bars": self.inner_step_bars,
            "min_train_bars": self.min_train_bars,
        }
        invalid = [name for name, value in positive.items() if value <= 0]
        if invalid:
            raise ValueError(f"nested validation bars must be positive: {invalid}")
        if self.purge_bars < 0 or self.embargo_bars < 0:
            raise ValueError("purge_bars and embargo_bars must not be negative")
        if self.outer_train_bars < self.min_train_bars:
            raise ValueError("outer_train_bars is smaller than min_train_bars")
        if self.inner_train_bars < self.min_train_bars:
            raise ValueError("inner_train_bars is smaller than min_train_bars")

    @classmethod
    def exploratory(cls) -> NestedValidationConfig:
        return cls(
            outer_train_bars=126,
            outer_test_bars=63,
            outer_step_bars=63,
            inner_train_bars=63,
            inner_test_bars=21,
            inner_step_bars=21,
            purge_bars=30,
            embargo_bars=5,
            min_train_bars=63,
        )

    @classmethod
    def balanced(cls) -> NestedValidationConfig:
        return cls()

    @classmethod
    def strict(cls) -> NestedValidationConfig:
        return cls(
            outer_train_bars=756,
            outer_test_bars=126,
            outer_step_bars=126,
            inner_train_bars=504,
            inner_test_bars=63,
            inner_step_bars=63,
            purge_bars=30,
            embargo_bars=5,
            min_train_bars=126,
        )


def validation_config_for_profile(profile: str) -> NestedValidationConfig:
    if profile not in {"exploratory", "balanced", "strict"}:
        raise ValueError(f"unknown mining profile: {profile}")
    return getattr(NestedValidationConfig, profile)()


def required_outer_folds(profile: str) -> int:
    validation_config_for_profile(profile)
    return 1 if profile == "exploratory" else 3


def required_trading_bars(
    config: NestedValidationConfig,
    outer_folds: int,
) -> int:
    if outer_folds <= 0:
        raise ValueError("outer_folds must be positive")
    return (
        config.outer_train_bars
        + config.purge_bars
        + config.outer_test_bars
        + (outer_folds - 1) * config.outer_step_bars
    )


def nested_fold_count(
    trading_bars: int,
    config: NestedValidationConfig,
) -> int:
    if trading_bars < 0:
        raise ValueError("trading_bars must not be negative")
    one_fold_bars = required_trading_bars(config, 1)
    if trading_bars < one_fold_bars:
        return 0
    return 1 + (trading_bars - one_fold_bars) // config.outer_step_bars


@dataclass(frozen=True)
class CandidateGateResult(JsonDataclassMixin):
    qualified: bool
    reasons: tuple[str, ...]


def evaluate_candidate_gate(
    *,
    confidence: str | None,
    valid_folds: int | None,
    positive_fold_ratio: float | None,
    sharpe: float | None,
    max_drawdown: float | None,
    n_trades: int | None,
    worst_fold_sharpe: float | None = None,
) -> CandidateGateResult:
    """Check the documented promotion thresholds against real candidate evidence.

    worst_fold_sharpe 是 mining-v3 起的稳健维度: strict 档 (confidence=high) 要求
    最差外层折 Sharpe > 0, 防"均值好看、某段崩溃"的组合登顶。None 表示 v2 历史
    工件无此列 — 历史候选按其产出时的口径放行, 不追溯执行新门槛。
    """
    reasons: list[str] = []
    if confidence == "low":
        reasons.append("exploratory results can only be saved as pending candidates")
    if valid_folds is None or valid_folds < GATE_MIN_VALID_FOLDS:
        reasons.append(
            "requires at least "
            f"{GATE_MIN_VALID_FOLDS} valid outer folds (got "
            f"{'none' if valid_folds is None else valid_folds})"
        )
    if positive_fold_ratio is None or positive_fold_ratio < GATE_MIN_POSITIVE_FOLD_RATIO:
        reasons.append(
            f"requires a positive-return fold ratio of at least {GATE_MIN_POSITIVE_FOLD_RATIO:.2f}"
        )
    if sharpe is None or sharpe < GATE_MIN_OOS_SHARPE:
        reasons.append(f"requires an OOS Sharpe of at least {GATE_MIN_OOS_SHARPE}")
    if confidence == "high" and worst_fold_sharpe is not None and worst_fold_sharpe <= 0.0:
        reasons.append(
            f"strict profile requires a positive worst-fold Sharpe (got {worst_fold_sharpe:.3f})"
        )
    if max_drawdown is None or max_drawdown < GATE_MAX_DRAWDOWN:
        reasons.append(f"requires a max drawdown no worse than {abs(GATE_MAX_DRAWDOWN):.0%}")
    if n_trades is None or n_trades < GATE_MIN_TRADES:
        reasons.append(f"requires at least {GATE_MIN_TRADES} OOS trades")
    return CandidateGateResult(qualified=not reasons, reasons=tuple(reasons))


@dataclass(frozen=True)
class MiningRequest(JsonDataclassMixin):
    factor_names: tuple[str, ...]
    existing_strategy_ids: tuple[str, ...] = ()
    correlation_threshold: float = 0.8
    date_column: str = "date"
    target_column: str = "_next_return"
    budget: MiningBudget = field(default_factory=MiningBudget.balanced)
    validation: NestedValidationConfig = field(default_factory=NestedValidationConfig.balanced)
    profile: Literal["exploratory", "balanced", "strict"] = "balanced"

    def __post_init__(self) -> None:
        if not self.factor_names:
            raise ValueError("factor_names must not be empty")
        if len(set(self.factor_names)) != len(self.factor_names):
            raise ValueError("factor_names must not contain duplicates")
        if len(self.factor_names) > self.budget.max_factors:
            raise ValueError(
                f"factor count {len(self.factor_names)} exceeds budget {self.budget.max_factors}"
            )
        if len(set(self.existing_strategy_ids)) != len(self.existing_strategy_ids):
            raise ValueError("existing_strategy_ids must not contain duplicates")
        if len(self.existing_strategy_ids) > self.budget.max_existing_strategies:
            raise ValueError(
                f"existing strategy count {len(self.existing_strategy_ids)} exceeds "
                f"budget {self.budget.max_existing_strategies}"
            )
        if not 0.0 < self.correlation_threshold <= 1.0:
            raise ValueError("correlation_threshold must be in (0, 1]")
        if not self.date_column or not self.target_column:
            raise ValueError("date_column and target_column must not be empty")

    @classmethod
    def for_profile(
        cls,
        profile: Literal["exploratory", "balanced", "strict"],
        factor_names: Sequence[str],
        existing_strategy_ids: Sequence[str] = (),
        **kwargs: Any,
    ) -> MiningRequest:
        if profile not in {"exploratory", "balanced", "strict"}:
            raise ValueError(f"unknown mining profile: {profile}")
        return cls(
            factor_names=tuple(factor_names),
            existing_strategy_ids=tuple(existing_strategy_ids),
            budget=getattr(MiningBudget, profile)(),
            validation=getattr(NestedValidationConfig, profile)(),
            profile=profile,
            **kwargs,
        )


@dataclass(frozen=True)
class FactorMetric(JsonDataclassMixin):
    factor_id: str
    composite_score: float
    ir: float
    coverage: float
    turnover: float
    rank_ic: float = 0.0


@dataclass(frozen=True)
class CorrelationResult(JsonDataclassMixin):
    factor_names: tuple[str, ...]
    matrix: tuple[tuple[float, ...], ...]
    pair_counts: tuple[tuple[int, ...], ...]
    elapsed_ms: float
    n_dates: int
    n_rows: int
    timing_ms: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class FactorExclusion(JsonDataclassMixin):
    factor_id: str
    reason: str
    representative: str
    rho: float


@dataclass(frozen=True)
class PruneResult(JsonDataclassMixin):
    selected: tuple[str, ...]
    excluded: tuple[FactorExclusion, ...]


@dataclass(frozen=True)
class MiningCandidate(JsonDataclassMixin):
    candidate_id: str
    kind: Literal["factor_rank", "existing_strategy"]
    factor_names: tuple[str, ...] = ()
    weights: tuple[float, ...] = ()
    directions: tuple[int, ...] = ()
    strategy_id: str | None = None
    proxy_rank_ic: float = 0.0
    proxy_ir: float = 0.0
    observations: int = 0
    dates: int = 0

    def definition(self) -> dict[str, Any]:
        if self.kind == "existing_strategy":
            return {"kind": self.kind, "strategy_id": self.strategy_id}
        return {
            "kind": self.kind,
            "factor_names": list(self.factor_names),
            "scoring": dict(zip(self.factor_names, self.weights, strict=True)),
            "directions": {
                factor_id: "high" if direction > 0 else "low"
                for factor_id, direction in zip(
                    self.factor_names,
                    self.directions,
                    strict=True,
                )
            },
        }


@dataclass(frozen=True)
class BeamSearchResult(JsonDataclassMixin):
    candidates: tuple[MiningCandidate, ...]
    trials_used: int
    cancelled: bool
    budget_exhausted: bool
    elapsed_ms: float


@dataclass(frozen=True)
class ValidationFold(JsonDataclassMixin):
    level: Literal["outer", "inner"]
    outer_index: int
    inner_index: int | None
    train_labels: tuple[str, ...]
    purge_labels: tuple[str, ...]
    test_labels: tuple[str, ...]
    embargo_labels: tuple[str, ...]

    @property
    def train_start(self) -> str:
        return self.train_labels[0]

    @property
    def train_end(self) -> str:
        return self.train_labels[-1]

    @property
    def test_start(self) -> str:
        return self.test_labels[0]

    @property
    def test_end(self) -> str:
        return self.test_labels[-1]


@dataclass(frozen=True)
class NestedFold(JsonDataclassMixin):
    outer: ValidationFold
    inner: tuple[ValidationFold, ...]


@dataclass(frozen=True)
class CandidateEvaluation(JsonDataclassMixin):
    score: float | None
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class FoldMiningResult(JsonDataclassMixin):
    outer_index: int
    selected_factors: tuple[str, ...]
    candidates: tuple[MiningCandidate, ...]
    selected_candidate_id: str | None
    inner_score: float | None
    outer_evaluation: CandidateEvaluation | None
    error: str | None = None
    benchmark_evaluations: tuple[tuple[str, CandidateEvaluation], ...] = ()
    cross_evaluations: tuple[tuple[str, CandidateEvaluation], ...] = ()


@dataclass(frozen=True)
class MiningResult(JsonDataclassMixin):
    request: MiningRequest
    folds: tuple[FoldMiningResult, ...]
    proxy_trials_used: int
    trials_used: int
    cancelled: bool
    elapsed_ms: float


class CandidateEvaluator(Protocol):
    def evaluate_candidate(
        self,
        train: pl.DataFrame,
        test: pl.DataFrame,
        definition: Mapping[str, Any],
    ) -> CandidateEvaluation | Mapping[str, Any] | float: ...


class FactorMetricProvider(Protocol):
    def __call__(
        self,
        train: pl.DataFrame,
        factor_names: Sequence[str],
    ) -> Sequence[FactorMetric]: ...


CancelCheck = Callable[[], bool] | Any
