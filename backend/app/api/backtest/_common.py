"""backtest API 共享定义 _common（由 backtest.py 机械拆分）。"""

from __future__ import annotations

import logging
import sys as _sys
from dataclasses import asdict
from datetime import date, timedelta
from typing import Any, Literal, cast

from fastapi import HTTPException, Request
from pydantic import BaseModel, Field

from app.config import settings

_api_pkg = _sys.modules["app.api.backtest"]

logger = logging.getLogger("app.api.backtest")


FACTOR_DEFAULT_DAYS = 180


STRATEGY_DEFAULT_DAYS = 365 * 3


BACKTEST_MAX_SERVER_DAYS = 186


FACTOR_MAX_SYMBOLS = 1000


BACKTEST_SERVER_GUARD_MESSAGE = (
    "当前服务器内存约 1.8GB，回测区间最多支持 6 个月；"
    "更长周期容易触发 OOM，建议在 8GB 以上内存环境或本机运行。"
)


def _get_engine(request: Request):
    """获取或创建 BacktestEngine (单例，PanelCache 跨请求生效)。"""
    from app.backtest.engine import BacktestEngine

    engine = getattr(request.app.state, "backtest_engine", None)
    if engine is None:
        engine = BacktestEngine(request.app.state.repo)
        request.app.state.backtest_engine = engine
    return engine


def _resolve_start(req: BaseModel, end: date, default_days: int) -> date:
    """未传 start 使用默认区间；显式传 null/空值表示全部历史。"""
    start = cast(Any, req).start
    if start is not None:
        return start
    if "start" in req.model_fields_set:
        return date(1900, 1, 1)
    return end - timedelta(days=default_days)


def _guard_server_backtest_range(start: date, end: date):
    """服务器端回测区间上限守卫，超限直接返回 400。"""
    if not settings.backtest_range_guard:
        return
    days = (end - start).days + 1
    if days > BACKTEST_MAX_SERVER_DAYS:
        raise HTTPException(status_code=400, detail=BACKTEST_SERVER_GUARD_MESSAGE)


class BacktestRequest(BaseModel):
    symbols: list[str] = Field(..., min_length=1)
    start: date | None = None
    end: date | None = None
    entries: list[str] = []
    exits: list[str] = []
    stop_loss_pct: float | None = None
    max_hold_days: int | None = None
    fees_pct: float = 0.0002
    slippage_bps: float = 5
    matching: Literal["close_t", "open_t+1"] = "close_t"
    asset_type: str = "stock"


class FactorColumnsResponse(BaseModel):
    columns: list[dict]


class FactorBacktestRequest(BaseModel):
    factor_name: str = Field(..., min_length=1, max_length=64)
    symbols: list[str] | None = None
    start: date | None = None
    end: date | None = None
    n_groups: int = Field(5, ge=2, le=10)
    rebalance: Literal["daily", "weekly", "monthly"] = "monthly"
    weight: Literal["equal", "factor_weight"] = "equal"
    fees_pct: float = 0.0002
    slippage_bps: float = 5.0
    asset_type: str = "stock"


class FactorBatchRequest(BaseModel):
    factor_names: list[str] = Field(..., min_length=1, max_length=96)  # 目录 77 + 自定义余量
    symbols: list[str] | None = None
    start: date | None = None
    end: date | None = None
    n_groups: int = Field(5, ge=2, le=10)
    rebalance: Literal["daily", "weekly", "monthly"] = "monthly"
    weight: Literal["equal", "factor_weight"] = "equal"
    fees_pct: float = 0.0002
    slippage_bps: float = 5.0
    asset_type: str = "stock"


class CandidateCreateRequest(BaseModel):
    kind: Literal["factor", "strategy"]
    name: str = Field(..., min_length=1, max_length=80)
    source_id: str = Field(..., min_length=1, max_length=120)
    config: dict = Field(default_factory=dict)
    metrics: dict = Field(default_factory=dict)
    data_as_of: date | None = None
    status: Literal["pending", "validated", "rejected"] = "pending"


class CandidateUpdateRequest(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=80)
    status: Literal["pending", "validated", "rejected"] | None = None


def _candidate_store():
    """返回候选因子的持久化存储实例。"""
    from app.backtest.candidates import CandidateStore

    return CandidateStore(settings.data_dir)


def _raise_candidate_error(exc: Exception) -> None:
    """把候选存储异常映射为统一的 HTTP 错误。"""
    from app.backtest.candidates import CandidateValidationError

    status_code = 400 if isinstance(exc, CandidateValidationError) else 500
    raise HTTPException(status_code=status_code, detail=str(exc)) from exc


class StrategyBacktestRequest(BaseModel):
    strategy_id: str
    symbols: list[str] | None = None
    start: date | None = None
    end: date | None = None
    params: dict | None = None
    overrides: dict | None = None
    # matching 向后兼容; 显式传 entry_fill/exit_fill 时以二者为准。
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
    # 自定义仓位策略: 注册表 id + 请求级参数覆盖 (不存在时 400, 不静默回退)
    position_policy: str | None = None
    position_policy_params: dict[str, float | int | bool | str] | None = None
    mode: Literal["position", "full"] = "position"
    holding_days: int = 5
    asset_type: str = "stock"
    minute_fill: bool = False
    regime_filter: dict | None = None


def _guard_minute_strategy_backtest(
    request: Request,
    strategy_id: str,
    start: date,
    asset_type: str,
) -> None:
    """分钟策略回测入口守卫: 仅 A 股 + 本地分钟K覆盖检查 (fail-fast)。"""
    engine = getattr(request.app.state, "strategy_engine", None)
    if engine is None:
        return
    try:
        s = engine.get(strategy_id)
    except ValueError:
        return
    if s is None or s.execution_backend != "minute_filter":
        return
    from app.backtest.strategy import validate_minute_backtest_coverage

    try:
        validate_minute_backtest_coverage(
            s,
            request.app.state.repo,
            start,
            asset_type,
        )
    except ValueError as exc:
        raise HTTPException(400, detail=str(exc)) from exc


def _daily_data_revisions() -> dict[str, str]:
    """读取当日数据修订标记，用于后台任务过期判定。"""
    from app.services.daily_health import ASSET_LABELS, revision

    return {asset: revision(settings.data_dir, asset) for asset in ASSET_LABELS}


def _stamp_daily_data(result, revisions=None) -> dict:
    """把当日数据修订标记写入回测结果。"""
    payload = result if isinstance(result, dict) else asdict(result)
    return {
        **payload,
        "daily_data_revisions": revisions
        if revisions is not None
        else _api_pkg._daily_data_revisions(),
    }
