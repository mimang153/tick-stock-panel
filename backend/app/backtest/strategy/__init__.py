"""原 app.backtest.strategy 模块对外入口：重导出全部顶层名字（含原模块级导入名）。"""

import hashlib
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable as Callable
from dataclasses import dataclass as dataclass
from dataclasses import field as field
from dataclasses import replace as replace
from datetime import date as date
from datetime import timedelta as timedelta
from pathlib import Path as Path
from typing import Literal as Literal

import numpy as np
import polars as pl

from app.backtest.engine import BacktestEngine as BacktestEngine
from app.backtest.engine import MatcherConfig as MatcherConfig
from app.backtest.engine import SimResult as SimResult
from app.backtest.engine import SimulationOptions as SimulationOptions
from app.backtest.engine import history_load_start as history_load_start
from app.backtest.fundamentals import FUNDAMENTAL_FACTOR_NAMES as FUNDAMENTAL_FACTOR_NAMES
from app.backtest.matrix import MarketDataMatrix as MarketDataMatrix
from app.backtest.matrix import MatrixCacheProfile as MatrixCacheProfile
from app.backtest.matrix import MatrixComputeCache as MatrixComputeCache
from app.backtest.matrix import MatrixPipelineConfig as MatrixPipelineConfig
from app.backtest.matrix import MatrixPrewarmCancelledError as MatrixPrewarmCancelledError
from app.backtest.matrix import MatrixStrategyPipeline as MatrixStrategyPipeline
from app.backtest.matrix import PanelMatrixEncoding as PanelMatrixEncoding
from app.backtest.matrix import SignalMatrix as SignalMatrix
from app.backtest.matrix import apply_time_masks as apply_time_masks
from app.backtest.matrix import build_market_data_matrix as build_market_data_matrix
from app.backtest.matrix import build_market_matrix as build_market_matrix
from app.backtest.matrix import (
    build_market_matrix_from_encoding as build_market_matrix_from_encoding,
)
from app.backtest.matrix import build_market_matrix_from_signals as build_market_matrix_from_signals
from app.backtest.matrix import encode_panel_matrix as encode_panel_matrix
from app.backtest.matrix import slice_market_data_matrix as slice_market_data_matrix
from app.backtest.matrix import slice_signal_matrix as slice_signal_matrix
from app.backtest.minute_replay import MinuteSignalReplayer as MinuteSignalReplayer
from app.backtest.minute_replay import minute_panel_start as minute_panel_start
from app.backtest.minute_replay import minute_replay_feature_plan as minute_replay_feature_plan
from app.backtest.minute_trigger import build_minute_entry_reference as build_minute_entry_reference
from app.backtest.minute_trigger import (
    unsupported_minute_exit_signals as unsupported_minute_exit_signals,
)
from app.config import settings as settings
from app.indicators.pipeline import ENRICHED_STORAGE_COLS as ENRICHED_STORAGE_COLS
from app.indicators.pipeline import INDICATOR_COLUMNS as INDICATOR_COLUMNS
from app.indicators.pipeline import LIMIT_SIGNAL_OUTPUTS as LIMIT_SIGNAL_OUTPUTS
from app.indicators.pipeline import get_signal_dependencies as get_signal_dependencies
from app.strategy.engine import StrategyDataContext as StrategyDataContext
from app.strategy.engine import StrategyDef as StrategyDef
from app.strategy.engine import StrategyEngine as StrategyEngine
from app.strategy.scoring import SCORING_DIRECTION_LOW as SCORING_DIRECTION_LOW
from app.strategy.scoring import effective_scoring as effective_scoring
from app.strategy.scoring import effective_scoring_directions as effective_scoring_directions
from app.strategy.scoring import materialize_scoring_columns as materialize_scoring_columns
from app.strategy.scoring import scoring_dependencies as scoring_dependencies
from app.strategy.scoring import scoring_value_expr as scoring_value_expr
from app.strategy.scoring import scoring_warmup_bars as scoring_warmup_bars

from ._common import _EXECUTION_COLUMNS as _EXECUTION_COLUMNS
from ._common import _INSTRUMENT_COLUMNS as _INSTRUMENT_COLUMNS
from ._common import _LIMIT_BASE_COLUMNS as _LIMIT_BASE_COLUMNS
from ._common import _OVERLAY_NP_OPS as _OVERLAY_NP_OPS
from ._common import _SHARE_CAP_FILTER_KEYS as _SHARE_CAP_FILTER_KEYS
from ._common import _STOCK_ONLY_FILTER_KEYS as _STOCK_ONLY_FILTER_KEYS
from ._common import _TURNOVER_FILTER_KEYS as _TURNOVER_FILTER_KEYS
from ._common import BENCHMARK_SYMBOL as BENCHMARK_SYMBOL
from ._common import BacktestResultPolicy as BacktestResultPolicy
from ._common import FeaturePlan as FeaturePlan
from ._common import PreparedMatrixBacktest as PreparedMatrixBacktest
from ._common import PreparedPanelBacktest as PreparedPanelBacktest
from ._common import ResolvedFeaturePlan as ResolvedFeaturePlan
from ._common import StrategyBacktestConfig as StrategyBacktestConfig
from ._common import StrategyBacktestResult as StrategyBacktestResult
from ._common import _basic_filter_dependencies as _basic_filter_dependencies
from ._common import _basic_filter_for_asset as _basic_filter_for_asset
from ._common import _factor_attribution_summary as _factor_attribution_summary
from ._common import _filter_dependencies as _filter_dependencies
from ._common import _merge_resolved_feature_plans as _merge_resolved_feature_plans
from ._common import _normalize_signal_name as _normalize_signal_name
from ._common import _overlay_numeric_fields as _overlay_numeric_fields
from ._common import _panel_backend as _panel_backend
from ._common import _resolve_base_columns as _resolve_base_columns
from ._common import _resolve_position_policy as _resolve_position_policy
from ._common import _RunState as _RunState
from ._common import logger as logger
from ._common import validate_minute_backtest_coverage as validate_minute_backtest_coverage
from ._resolver import StrategyDependencyResolver as StrategyDependencyResolver
from ._resolver import _collect_matrix_profile_plans as _collect_matrix_profile_plans
from ._resolver import build_matrix_cache_profile as build_matrix_cache_profile
from ._resolver import prewarm_matrix_cache as prewarm_matrix_cache
from ._service import StrategyBacktestService as StrategyBacktestService

__all__ = [
    "BENCHMARK_SYMBOL",
    "ENRICHED_STORAGE_COLS",
    "FUNDAMENTAL_FACTOR_NAMES",
    "INDICATOR_COLUMNS",
    "LIMIT_SIGNAL_OUTPUTS",
    "SCORING_DIRECTION_LOW",
    "_EXECUTION_COLUMNS",
    "_INSTRUMENT_COLUMNS",
    "_LIMIT_BASE_COLUMNS",
    "_OVERLAY_NP_OPS",
    "_SHARE_CAP_FILTER_KEYS",
    "_STOCK_ONLY_FILTER_KEYS",
    "_TURNOVER_FILTER_KEYS",
    "BacktestEngine",
    "BacktestResultPolicy",
    "Callable",
    "FeaturePlan",
    "Literal",
    "MarketDataMatrix",
    "MatcherConfig",
    "MatrixCacheProfile",
    "MatrixComputeCache",
    "MatrixPipelineConfig",
    "MatrixPrewarmCancelledError",
    "MatrixStrategyPipeline",
    "MinuteSignalReplayer",
    "PanelMatrixEncoding",
    "Path",
    "PreparedMatrixBacktest",
    "PreparedPanelBacktest",
    "ResolvedFeaturePlan",
    "SignalMatrix",
    "SimResult",
    "SimulationOptions",
    "StrategyBacktestConfig",
    "StrategyBacktestResult",
    "StrategyBacktestService",
    "StrategyDataContext",
    "StrategyDef",
    "StrategyDependencyResolver",
    "StrategyEngine",
    "_RunState",
    "_basic_filter_dependencies",
    "_basic_filter_for_asset",
    "_collect_matrix_profile_plans",
    "_factor_attribution_summary",
    "_filter_dependencies",
    "_merge_resolved_feature_plans",
    "_normalize_signal_name",
    "_overlay_numeric_fields",
    "_panel_backend",
    "_resolve_base_columns",
    "_resolve_position_policy",
    "apply_time_masks",
    "build_market_data_matrix",
    "build_market_matrix",
    "build_market_matrix_from_encoding",
    "build_market_matrix_from_signals",
    "build_matrix_cache_profile",
    "build_minute_entry_reference",
    "dataclass",
    "date",
    "effective_scoring",
    "effective_scoring_directions",
    "encode_panel_matrix",
    "field",
    "get_signal_dependencies",
    "hashlib",
    "history_load_start",
    "json",
    "logger",
    "logging",
    "materialize_scoring_columns",
    "minute_panel_start",
    "minute_replay_feature_plan",
    "np",
    "pl",
    "prewarm_matrix_cache",
    "replace",
    "scoring_dependencies",
    "scoring_value_expr",
    "scoring_warmup_bars",
    "settings",
    "slice_market_data_matrix",
    "slice_signal_matrix",
    "threading",
    "time",
    "timedelta",
    "unsupported_minute_exit_signals",
    "uuid",
    "validate_minute_backtest_coverage",
]
