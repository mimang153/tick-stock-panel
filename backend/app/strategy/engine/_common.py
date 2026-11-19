"""StrategyEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import contextlib
import importlib.util
import logging
import sys
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from app.config import settings
from app.strategy.scoring import (
    SCORING_DIRECTION_LOW,
    effective_scoring,
    effective_scoring_directions,
    materialize_scoring_columns,
    scoring_dependencies,
    scoring_value_expr,
    scoring_warmup_bars,
)

"""策略引擎 — 加载、执行、评分。

职责: 从文件系统加载策略 Python 模块，执行两阶段过滤(基础+策略)，
     通用评分排序。
不知道: AI、API、前端、配置持久化、回测。
"""


logger = logging.getLogger("app.strategy.engine")


DEFAULT_BASIC_FILTER: dict = {
    "price_min": 3,
    "price_max": 300,
    "market_cap_min": 10e8,
    "float_cap_min": None,
    "float_cap_max": None,
    "amount_min": 0.2e8,
    "amount_max": None,
    "turnover_min": None,
    "turnover_max": None,
    "exclude_st": True,
    "exclude_new_days": 30,
    "boards": ["沪主板", "深主板", "创业板", "科创板", "北交所"],
}


MAX_COMPOSITE_CHILDREN = 8


def _normalize_param_defs(params: Any) -> list[dict]:
    """把 META["params"] 归一化为标准 list[dict] (每项含 id/label/type/default).

    支持的输入格式:
    - list[dict] (标准): 保持, 补齐缺失的 id/label/type/default 字段
    - dict ({"lookback": 20} 或 {"lookback": {"default": 20, "type": "int"}}):
      按 key 作参数 id 转换
    - list[str] (["lookback", "threshold"]): 每项作 id, default=None
    - 其他类型 / 不可识别项: 丢弃并 warning 记录; 整体异常则返回空 list (降级而非崩溃)

    保证下游 {p["id"]: p["default"] for p in params} 永远不会因格式问题抛 TypeError.
    """
    if params is None:
        return []

    # dict 格式: {"lookback": 20} 或 {"lookback": {"default": 20, "type": "int"}}
    if isinstance(params, dict):
        items: list[dict] = []
        for key, val in params.items():
            if not isinstance(key, str) or not key:
                continue
            item = {"id": key, **val} if isinstance(val, dict) else {"id": key, "default": val}
            items.append(item)
        return [_normalize_param_item(item) for item in items]

    # 期望是 list/tuple, 其他类型直接降级
    if not isinstance(params, list | tuple):
        logger.warning("strategy params 非标准格式 (%s), 已降级为空 list", type(params).__name__)
        return []

    result: list[dict] = []
    for i, p in enumerate(params):
        if isinstance(p, str):
            result.append({"id": p, "default": None})
        elif isinstance(p, dict):
            item = _normalize_param_item(p)
            if item:  # 缺 id 等异常项 _normalize_param_item 返回空 dict, 丢弃
                result.append(item)
        else:
            logger.warning("strategy params[%d] 不可识别 (%s), 已丢弃", i, type(p).__name__)
    return result


def _normalize_param_item(item: dict) -> dict:
    """补齐单个参数定义的默认字段, 保证 id/label/type/default 都存在."""
    norm = dict(item)
    if "id" not in norm or not norm["id"]:
        logger.warning("strategy param 定义缺少 id, 已丢弃: %s", item)
        return {}
    norm.setdefault("label", str(norm["id"]))
    norm.setdefault("type", "float")
    norm.setdefault("default", None)
    return norm


def _parse_composite_children(raw: Any) -> CompositeSpec:
    """解析 META["children"] 为 CompositeSpec。

    每项形如 {"strategy_id": "xxx", "weight": 0.4}。仅做结构和权重校验:
    - 非空 list, 每项含合法 strategy_id 与非负 weight
    - 数量 <= MAX_COMPOSITE_CHILDREN (超出在加载期拒绝, 避免信号计算成本爆炸)
    子策略的存在性/非嵌套/asset_types 一致性由 _load_all 两阶段校验保证。
    """
    if not isinstance(raw, list) or not raw:
        raise ValueError("composite strategy META['children'] must be a non-empty list")
    if len(raw) > MAX_COMPOSITE_CHILDREN:
        raise ValueError(
            f"composite strategy children count {len(raw)} exceeds limit {MAX_COMPOSITE_CHILDREN}"
        )
    children: list[CompositeChild] = []
    seen: set[str] = set()
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"composite children[{i}] must be a dict")
        cid = item.get("strategy_id")
        if not isinstance(cid, str) or not cid:
            raise ValueError(f"composite children[{i}] missing non-empty 'strategy_id'")
        if cid in seen:
            raise ValueError(f"composite children[{i}] duplicate strategy_id {cid!r}")
        seen.add(cid)
        weight = item.get("weight", 1.0)
        try:
            weight = float(weight)
        except (TypeError, ValueError) as e:
            raise ValueError(f"composite children[{i}] weight must be a number") from e
        if weight < 0:
            raise ValueError(f"composite children[{i}] weight must be >= 0")
        children.append(CompositeChild(strategy_id=cid, weight=weight))
    return CompositeSpec(children=tuple(children))


@dataclass
class StrategyDataContext:
    """一次策略调用所需的标准数据上下文。"""

    asset_type: str
    timeframe: str
    as_of: date
    current: pl.DataFrame | None = None
    history: pl.DataFrame | None = None
    # 仅 1m 分支: 策略声明 META["daily_history_bars"] 时注入的日线 enriched 窗口,
    # 供分钟策略叠加日线维度条件 (如 N 日内涨停过); 未声明时为 None。
    daily_history: pl.DataFrame | None = None
    market: Any | None = None
    cache_key: str | None = None
    # enriched 数据换代号 (repo.get_matrix_data_generation, 详见 enriched_generation):
    # 装配方填入, 消费方用作共享矩阵缓存的失效指纹 — 数据重建换代号变, 缓存自动失效。
    data_generation: str | None = None


@dataclass(frozen=True)
class CompositeChild:
    """叠加策略的一个子策略引用。"""

    strategy_id: str
    weight: float


@dataclass(frozen=True)
class CompositeSpec:
    """叠加策略的子策略声明（无业务代码，仅引用与权重）。

    引用合法性与一致性由 _load_all 两阶段校验保证：子策略必须存在、
    非嵌套、asset_types 一致、数量 ≤ MAX_COMPOSITE_CHILDREN。
    """

    children: tuple[CompositeChild, ...]


@dataclass
class StrategyDef:
    """加载后的策略定义（只读数据 + filter 函数引用）"""

    meta: dict
    basic_filter: dict
    entry_signals: list[str]
    exit_signals: list[str]
    stop_loss: float | None
    trailing_stop: float | None
    trailing_take_profit_activate: float | None
    trailing_take_profit_drawdown: float | None
    max_hold_days: int | None
    filter_fn: Callable[[pl.DataFrame, dict], pl.Expr] | None
    filter_history_fn: Callable[[pl.DataFrame, dict], pl.DataFrame] | None
    lookback_days: int
    source: str  # "custom" | "ai" | "composite"
    required_features: frozenset[str] = field(default_factory=frozenset)
    file_path: Path | None = None
    execution_backend: str = "polars_expr"
    matrix_strategy: Any | None = None
    composite: CompositeSpec | None = None  # 仅 backend=="composite" 时非空
    # 仅 backend=="minute_filter" 时非空: 输入为当日分钟K窗口, 输出为命中标的行
    filter_minute_history_fn: Callable[[pl.DataFrame, dict], pl.DataFrame] | None = None
    # 仅 minute_filter: META["daily_history_bars"] 声明需要的日线历史窗口 (0=不需要;
    # >0 时 filter_minute_history 必须接受 daily 关键字, 引擎注入 context.daily_history)
    minute_daily_bars: int = 0
    # 模块 SCORING 常量的因子键 (custom_factor_* 排名策略用; 策略包闭包消费)
    scoring_columns: tuple[str, ...] = ()
    # 模块 TAKE_PROFIT 常量（正收益比例, 如 0.10 = +10% 止盈）; 与 STOP_LOSS 对称,
    # 供回测/实盘风控读取（此前遗漏, 模块常量不生效）
    take_profit: float | None = None


@dataclass
class StrategyResult:
    """策略执行结果"""

    as_of: date
    strategy_id: str
    rows: list[dict] = field(default_factory=list)
    total: int = 0
    elapsed_ms: float = 0.0
    scores: dict[str, float] = field(default_factory=dict)
    entry_signal_hits: list[dict] = field(default_factory=list)
    exit_signal_hits: list[dict] = field(default_factory=list)


@dataclass
class _RealtimeMatrixEntry:
    fingerprint: tuple[Any, ...]
    buffer: Any


def _resolve_required_features(meta: dict, mod, filter_history_fn, path: Path) -> frozenset[str]:
    """显式声明 (META/module REQUIRED_FEATURES) 优先; 缺失且存在动态
    filter_history 时, AST 推导列依赖兜底 — 推导不可证明则保持为空,
    回测层继续走全量指标计算 (fail-closed)。

    推导范围是整个策略文件 + 同目录 `_*.py` 依赖文件, 不只是 filter_history
    函数体: 列引用在模块级辅助函数或依赖文件里硬编码时, 只看函数体会漏列,
    回测帧缺列直接 ColumnNotFoundError。范围放宽只会多载列, 不会漏。"""
    declared = frozenset(meta.get("required_features", []) or []) | frozenset(
        getattr(mod, "REQUIRED_FEATURES", []) or []
    )
    if declared or filter_history_fn is None:
        return declared
    from app.strategy.feature_deps import infer_features_from_sources

    family = [path, *sorted(p for p in path.parent.glob("_*.py") if p != path)]
    try:
        sources = [p.read_text(encoding="utf-8") for p in family]
    except OSError as e:
        logger.debug(
            "strategy %s: 依赖源码读取失败, 保持全量兜底: %s", meta.get("id", "<unknown>"), e
        )
        return declared
    inferred = infer_features_from_sources(sources)
    if inferred is None:
        return declared
    logger.info(
        "strategy %s: 依赖源码 AST 推导出 %d 个依赖列, 免除回测全量兜底",
        meta.get("id", "<unknown>"),
        len(inferred),
    )
    return inferred


def _sanitize(rows: list[dict]) -> list[dict]:
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, float) and (v != v or abs(v) == float("inf")):
                r[k] = None
    return rows


__all__ = [
    "DEFAULT_BASIC_FILTER",
    "MAX_COMPOSITE_CHILDREN",
    "SCORING_DIRECTION_LOW",
    "Any",
    "Callable",
    "CompositeChild",
    "CompositeSpec",
    "Mapping",
    "Path",
    "StrategyDataContext",
    "StrategyDef",
    "StrategyResult",
    "ThreadPoolExecutor",
    "_RealtimeMatrixEntry",
    "_normalize_param_defs",
    "_normalize_param_item",
    "_parse_composite_children",
    "_resolve_required_features",
    "_sanitize",
    "contextlib",
    "dataclass",
    "date",
    "effective_scoring",
    "effective_scoring_directions",
    "field",
    "importlib",
    "logger",
    "logging",
    "materialize_scoring_columns",
    "np",
    "pl",
    "replace",
    "scoring_dependencies",
    "scoring_value_expr",
    "scoring_warmup_bars",
    "settings",
    "sys",
    "threading",
    "time",
]
