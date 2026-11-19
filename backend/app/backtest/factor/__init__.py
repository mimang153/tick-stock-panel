"""因子回测服务 — IC/IR 分析 + 分层回测 + 多空组合。

纯 Polars 向量化实现，无 pandas 依赖。
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal

import numpy as np
import polars as pl

from app.backtest import stats_v2
from app.backtest.engine import BacktestEngine, history_load_start
from app.backtest.fundamentals import (
    FUNDAMENTAL_FACTOR_NAMES,
    attach_fundamental_factors,
    load_fundamental_snapshot,
)
from app.factors.registry import factor_calculation_order, factor_warmup_bars, get_factor
from app.factors.registry import factor_columns_view as _factor_columns_view
from app.strategy.scoring import VIRTUAL_SCORING_DEPENDENCIES as DERIVED_FACTOR_DEPENDENCIES
from app.strategy.scoring import materialize_scoring_columns

from ._common import (
    _DAILY_FORWARD_HORIZONS,
    FACTOR_COLUMNS,
    FACTOR_METHODOLOGY_VERSION,
    FACTOR_WARMUP_DAYS,
    FactorBatchConfig,
    FactorBatchItem,
    FactorBatchResult,
    FactorConfig,
    FactorResult,
    GroupStats,
    logger,
)
from ._service import FactorBacktestService

__all__ = [
    "DERIVED_FACTOR_DEPENDENCIES",
    "FACTOR_COLUMNS",
    "FACTOR_METHODOLOGY_VERSION",
    "FACTOR_WARMUP_DAYS",
    "FUNDAMENTAL_FACTOR_NAMES",
    "_DAILY_FORWARD_HORIZONS",
    "Any",
    "BacktestEngine",
    "FactorBacktestService",
    "FactorBatchConfig",
    "FactorBatchItem",
    "FactorBatchResult",
    "FactorConfig",
    "FactorResult",
    "GroupStats",
    "Literal",
    "Mapping",
    "Path",
    "_factor_columns_view",
    "attach_fundamental_factors",
    "dataclass",
    "date",
    "factor_calculation_order",
    "factor_warmup_bars",
    "field",
    "get_factor",
    "history_load_start",
    "load_fundamental_snapshot",
    "logger",
    "logging",
    "materialize_scoring_columns",
    "np",
    "pairwise",
    "pl",
    "stats_v2",
    "time",
    "uuid",
]
