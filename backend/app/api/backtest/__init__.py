"""backtest API —— 由单文件 backtest.py 机械拆分而来。

模块布局：
- _common.py：常量、请求模型与基础 helper；
- _job_runtime.py：后台任务生命周期管理与流式回测准备逻辑；
- _optimize_runtime.py：稳健优化与步进优化的任务/事件处理；
- _routes_core.py：状态、信号/因子/候选/策略回测与 SSE 路由；
- _routes_optimize.py：稳健优化、步进优化的 preview/stream/cancel 路由。

行为保持：
- 子路由按原文件装饰器顺序 include，最终路由 path 与顺序逐字不变；
- 原模块内的导入名与顶层定义全部在此重导出，兼容 from ... import ...；
- 被测试 monkeypatch 的模块级名字通过包属性动态解析，patch 仍然生效。
"""

from __future__ import annotations

import asyncio as asyncio
import hashlib as hashlib
import json as json
import logging as logging
import threading as threading
import time as time
import weakref as weakref
from dataclasses import asdict as asdict
from datetime import date as date
from datetime import timedelta as timedelta
from typing import Literal as Literal

from fastapi import APIRouter
from fastapi import HTTPException as HTTPException
from fastapi import Request as Request
from fastapi.responses import StreamingResponse as StreamingResponse
from pydantic import BaseModel as BaseModel
from pydantic import Field as Field

from app.api.auth import COOKIE_NAME as COOKIE_NAME
from app.config import settings as settings
from app.enriched_generation import (
    EnrichedGenerationUnavailableError as EnrichedGenerationUnavailableError,
)
from app.market_time import cn_today as cn_today
from app.services.backtest import BacktestConfig as BacktestConfig
from app.services.backtest import BacktestService as BacktestService
from app.services.backtest import VectorbtUnavailable as VectorbtUnavailable
from app.services.daily_health import DailyIntegrityError as DailyIntegrityError
from app.services.workload import admit_request as admit_request
from app.services.workload import workload as workload

from ._common import BACKTEST_MAX_SERVER_DAYS as BACKTEST_MAX_SERVER_DAYS
from ._common import BACKTEST_SERVER_GUARD_MESSAGE as BACKTEST_SERVER_GUARD_MESSAGE
from ._common import FACTOR_DEFAULT_DAYS as FACTOR_DEFAULT_DAYS
from ._common import FACTOR_MAX_SYMBOLS as FACTOR_MAX_SYMBOLS
from ._common import STRATEGY_DEFAULT_DAYS as STRATEGY_DEFAULT_DAYS
from ._common import BacktestRequest as BacktestRequest
from ._common import CandidateCreateRequest as CandidateCreateRequest
from ._common import CandidateUpdateRequest as CandidateUpdateRequest
from ._common import FactorBacktestRequest as FactorBacktestRequest
from ._common import FactorBatchRequest as FactorBatchRequest
from ._common import FactorColumnsResponse as FactorColumnsResponse
from ._common import StrategyBacktestRequest as StrategyBacktestRequest
from ._common import _candidate_store as _candidate_store
from ._common import _daily_data_revisions as _daily_data_revisions
from ._common import _get_engine as _get_engine
from ._common import _guard_minute_strategy_backtest as _guard_minute_strategy_backtest
from ._common import _guard_server_backtest_range as _guard_server_backtest_range
from ._common import _raise_candidate_error as _raise_candidate_error
from ._common import _resolve_start as _resolve_start
from ._common import _stamp_daily_data as _stamp_daily_data
from ._common import logger as logger
from ._job_runtime import _JOB_TTL as _JOB_TTL
from ._job_runtime import _MAX_ACTIVE_JOBS as _MAX_ACTIVE_JOBS
from ._job_runtime import _MAX_FINISHED_JOBS as _MAX_FINISHED_JOBS
from ._job_runtime import _acquire_or_evict_job as _acquire_or_evict_job
from ._job_runtime import _BacktestJob as _BacktestJob
from ._job_runtime import _claim_job_start as _claim_job_start
from ._job_runtime import _cleanup_stale_jobs as _cleanup_stale_jobs
from ._job_runtime import _discard_unstarted_job as _discard_unstarted_job
from ._job_runtime import _drop_job as _drop_job
from ._job_runtime import _finish_job as _finish_job
from ._job_runtime import _jobs_lock as _jobs_lock
from ._job_runtime import _make_job_key as _make_job_key
from ._job_runtime import _prepare_strategy_stream_job as _prepare_strategy_stream_job
from ._job_runtime import _running_jobs as _running_jobs
from ._job_runtime import _start_job_thread as _start_job_thread
from ._job_runtime import _start_strategy_stream_backtest as _start_strategy_stream_backtest
from ._job_runtime import _strategy_stream_events as _strategy_stream_events
from ._job_runtime import _strategy_stream_preflight_error as _strategy_stream_preflight_error
from ._optimize_runtime import _OPT_BT_FIELDS as _OPT_BT_FIELDS
from ._optimize_runtime import _acquire_robust_stream_job as _acquire_robust_stream_job
from ._optimize_runtime import _build_robust_optimize_config as _build_robust_optimize_config
from ._optimize_runtime import _iter_robust_job_events as _iter_robust_job_events
from ._optimize_runtime import _json_safe as _json_safe
from ._optimize_runtime import _make_opt_job_key as _make_opt_job_key
from ._optimize_runtime import _make_wf_job_key as _make_wf_job_key
from ._optimize_runtime import _opt_backtest_kwargs as _opt_backtest_kwargs
from ._optimize_runtime import _parse_robust_job_inputs as _parse_robust_job_inputs
from ._optimize_runtime import _prepare_robust_stream_context as _prepare_robust_stream_context
from ._optimize_runtime import _robust_optimize_stream_events as _robust_optimize_stream_events
from ._optimize_runtime import _run_robust_optimize_task as _run_robust_optimize_task
from ._optimize_runtime import _start_robust_optimize_job as _start_robust_optimize_job
from ._optimize_runtime import _validate_robust_stream_params as _validate_robust_stream_params
from ._optimize_runtime import _validate_walkforward_objective as _validate_walkforward_objective
from ._optimize_runtime import _walkforward_acquire_job as _walkforward_acquire_job
from ._optimize_runtime import _walkforward_build_config as _walkforward_build_config
from ._optimize_runtime import _walkforward_launch_worker as _walkforward_launch_worker
from ._optimize_runtime import _walkforward_parse_grid as _walkforward_parse_grid
from ._optimize_runtime import _walkforward_poll_events as _walkforward_poll_events
from ._optimize_runtime import _walkforward_resolve_dates as _walkforward_resolve_dates
from ._optimize_runtime import _walkforward_start_job as _walkforward_start_job
from ._routes_core import candidate_create as candidate_create
from ._routes_core import candidate_delete as candidate_delete
from ._routes_core import candidate_update as candidate_update
from ._routes_core import candidates_list as candidates_list
from ._routes_core import factor_batch as factor_batch
from ._routes_core import factor_columns as factor_columns
from ._routes_core import factor_run as factor_run
from ._routes_core import router as _sub__routes_core
from ._routes_core import run as run
from ._routes_core import status as status
from ._routes_core import strategy_cancel as strategy_cancel
from ._routes_core import strategy_run as strategy_run
from ._routes_core import strategy_run_evidence as strategy_run_evidence
from ._routes_core import strategy_stream as strategy_stream
from ._routes_optimize import optimize_robust_cancel as optimize_robust_cancel
from ._routes_optimize import optimize_robust_preview as optimize_robust_preview
from ._routes_optimize import optimize_robust_stream as optimize_robust_stream
from ._routes_optimize import router as _sub__routes_optimize
from ._routes_optimize import walkforward_cancel as walkforward_cancel
from ._routes_optimize import walkforward_preview as walkforward_preview
from ._routes_optimize import walkforward_stream as walkforward_stream

router = APIRouter(tags=["backtest"])

router.include_router(_sub__routes_core)
router.include_router(_sub__routes_optimize)

__all__ = [
    "BACKTEST_MAX_SERVER_DAYS",
    "BACKTEST_SERVER_GUARD_MESSAGE",
    "COOKIE_NAME",
    "FACTOR_DEFAULT_DAYS",
    "FACTOR_MAX_SYMBOLS",
    "STRATEGY_DEFAULT_DAYS",
    "_JOB_TTL",
    "_MAX_ACTIVE_JOBS",
    "_MAX_FINISHED_JOBS",
    "_OPT_BT_FIELDS",
    "APIRouter",
    "BacktestConfig",
    "BacktestRequest",
    "BacktestService",
    "BaseModel",
    "CandidateCreateRequest",
    "CandidateUpdateRequest",
    "DailyIntegrityError",
    "EnrichedGenerationUnavailableError",
    "FactorBacktestRequest",
    "FactorBatchRequest",
    "FactorColumnsResponse",
    "Field",
    "HTTPException",
    "Literal",
    "Request",
    "StrategyBacktestRequest",
    "StreamingResponse",
    "VectorbtUnavailable",
    "_BacktestJob",
    "_acquire_or_evict_job",
    "_acquire_robust_stream_job",
    "_build_robust_optimize_config",
    "_candidate_store",
    "_claim_job_start",
    "_cleanup_stale_jobs",
    "_daily_data_revisions",
    "_discard_unstarted_job",
    "_drop_job",
    "_finish_job",
    "_get_engine",
    "_guard_minute_strategy_backtest",
    "_guard_server_backtest_range",
    "_iter_robust_job_events",
    "_jobs_lock",
    "_json_safe",
    "_make_job_key",
    "_make_opt_job_key",
    "_make_wf_job_key",
    "_opt_backtest_kwargs",
    "_parse_robust_job_inputs",
    "_prepare_robust_stream_context",
    "_prepare_strategy_stream_job",
    "_raise_candidate_error",
    "_resolve_start",
    "_robust_optimize_stream_events",
    "_run_robust_optimize_task",
    "_running_jobs",
    "_stamp_daily_data",
    "_start_job_thread",
    "_start_robust_optimize_job",
    "_start_strategy_stream_backtest",
    "_strategy_stream_events",
    "_strategy_stream_preflight_error",
    "_validate_robust_stream_params",
    "_validate_walkforward_objective",
    "_walkforward_acquire_job",
    "_walkforward_build_config",
    "_walkforward_launch_worker",
    "_walkforward_parse_grid",
    "_walkforward_poll_events",
    "_walkforward_resolve_dates",
    "_walkforward_start_job",
    "admit_request",
    "asdict",
    "asyncio",
    "candidate_create",
    "candidate_delete",
    "candidate_update",
    "candidates_list",
    "cn_today",
    "date",
    "factor_batch",
    "factor_columns",
    "factor_run",
    "hashlib",
    "json",
    "logger",
    "logging",
    "optimize_robust_cancel",
    "optimize_robust_preview",
    "optimize_robust_stream",
    "router",
    "run",
    "settings",
    "status",
    "strategy_cancel",
    "strategy_run",
    "strategy_run_evidence",
    "strategy_stream",
    "threading",
    "time",
    "timedelta",
    "walkforward_cancel",
    "walkforward_preview",
    "walkforward_stream",
    "weakref",
    "workload",
]
