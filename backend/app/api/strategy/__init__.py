"""strategy API —— 由单文件 strategy.py 机械拆分而来。

模块布局：
- _common.py：请求模型、元信息规范化、代码保存与 bundle helper；
- _routes_core.py：策略列表/运行/配置/来源与 bundle 路由；
- _routes_ai_build.py：AI 生成/迭代/构建/发布与代码保存路由。

行为保持：
- 子路由按原文件装饰器顺序 include，最终路由 path 与顺序逐字不变；
- 原模块内的导入名与顶层定义全部在此重导出，兼容 from ... import ...；
- 被测试 monkeypatch 的模块级名字通过包属性动态解析，patch 仍然生效。
"""

from __future__ import annotations

import ast as ast
import contextlib as contextlib
import json as json
import logging as logging
import math as math
import re as re
from dataclasses import asdict as asdict
from datetime import date as date
from pathlib import Path as Path
from typing import Any as Any
from typing import Literal as Literal

from fastapi import APIRouter
from fastapi import File as File
from fastapi import Form as Form
from fastapi import HTTPException as HTTPException
from fastapi import Request as Request
from fastapi import UploadFile as UploadFile
from fastapi.responses import StreamingResponse as StreamingResponse
from pydantic import BaseModel as BaseModel
from pydantic import Field as Field
from starlette.concurrency import run_in_threadpool as run_in_threadpool

from app.backtest.minute_trigger import MINUTE_EXIT_TRIGGER_SIGNALS as MINUTE_EXIT_TRIGGER_SIGNALS
from app.services import strategy_pool as strategy_pool
from app.services.ndjson_heartbeat import with_heartbeat as with_heartbeat
from app.strategy import config as strategy_config
from app.strategy.ai_generator import AIStrategyGenerator as AIStrategyGenerator
from app.strategy.ai_generator import _strategy_execution_backend as _strategy_execution_backend
from app.strategy.ai_generator import find_meta_assignment as find_meta_assignment
from app.strategy.engine import StrategyDef as StrategyDef
from app.strategy.engine import StrategyEngine as StrategyEngine
from app.strategy.monitor import StrategyMonitorService as StrategyMonitorService
from app.strategy.prompt_builder import build_step1 as build_step1
from app.strategy.prompt_builder import build_step2 as build_step2
from app.strategy.scoring import SCORING_DIRECTIONS as SCORING_DIRECTIONS
from app.strategy.scoring import effective_scoring as effective_scoring
from app.strategy.scoring import effective_scoring_directions as effective_scoring_directions

from ._common import _BUNDLE_FILE as _BUNDLE_FILE
from ._common import _OVERLAY_UNSUPPORTED_BACKENDS as _OVERLAY_UNSUPPORTED_BACKENDS
from ._common import AIGenerateRequest as AIGenerateRequest
from ._common import AIIterateRequest as AIIterateRequest
from ._common import AISaveRequest as AISaveRequest
from ._common import BuildRequest as BuildRequest
from ._common import CompositeChildItem as CompositeChildItem
from ._common import MonitorStartRequest as MonitorStartRequest
from ._common import RunAllRequest as RunAllRequest
from ._common import RunRequest as RunRequest
from ._common import SaveConfigRequest as SaveConfigRequest
from ._common import StrategyCodeSaveRequest as StrategyCodeSaveRequest
from ._common import StrategyCodeValidateRequest as StrategyCodeValidateRequest
from ._common import StrategyCompositeSaveRequest as StrategyCompositeSaveRequest
from ._common import _apply_strategy_bundle as _apply_strategy_bundle
from ._common import _build_execution_backend as _build_execution_backend
from ._common import _build_prompt as _build_prompt
from ._common import _bundle_service as _bundle_service
from ._common import _child_meta as _child_meta
from ._common import _cleanup_deleted_strategy as _cleanup_deleted_strategy
from ._common import _composite_children as _composite_children
from ._common import _data_dir as _data_dir
from ._common import _find_meta_dict as _find_meta_dict
from ._common import _get_engine as _get_engine
from ._common import _get_monitor as _get_monitor
from ._common import _get_public_strategy as _get_public_strategy
from ._common import _insert_meta_field as _insert_meta_field
from ._common import _invalidate_strategy_runtime as _invalidate_strategy_runtime
from ._common import _missing_custom_signals as _missing_custom_signals
from ._common import _normalize_build_result as _normalize_build_result
from ._common import _normalize_strategy_meta as _normalize_strategy_meta
from ._common import _overlay_changed as _overlay_changed
from ._common import _prepare_strategy_code as _prepare_strategy_code
from ._common import _prepare_strategy_code_for_save as _prepare_strategy_code_for_save
from ._common import _py_string as _py_string
from ._common import _reload_and_verify_strategy as _reload_and_verify_strategy
from ._common import _render_composite_code as _render_composite_code
from ._common import _resolve_strategy_save_target as _resolve_strategy_save_target
from ._common import _restore_strategy_file as _restore_strategy_file
from ._common import _risk_fields as _risk_fields
from ._common import _safe as _safe
from ._common import _save_composite_strategy as _save_composite_strategy
from ._common import _save_strategy_code as _save_strategy_code
from ._common import _set_meta_bool_field as _set_meta_bool_field
from ._common import _set_meta_string_field as _set_meta_string_field
from ._common import _strategy_detail as _strategy_detail
from ._common import _strip_defaults as _strip_defaults
from ._common import _target_dir as _target_dir
from ._common import _validate_overlay_filter as _validate_overlay_filter
from ._common import _validate_scoring_config as _validate_scoring_config
from ._common import _validate_strategy_id as _validate_strategy_id
from ._common import logger as logger
from ._common import save_strategy_code_core as save_strategy_code_core
from ._routes_ai_build import ai_generate as ai_generate
from ._routes_ai_build import ai_iterate as ai_iterate
from ._routes_ai_build import ai_save as ai_save
from ._routes_ai_build import ai_status as ai_status
from ._routes_ai_build import ai_test as ai_test
from ._routes_ai_build import apply_strategy_bundle as apply_strategy_bundle
from ._routes_ai_build import build_strategy as build_strategy
from ._routes_ai_build import build_strategy_stream as build_strategy_stream
from ._routes_ai_build import delete_strategy as delete_strategy
from ._routes_ai_build import export_strategy_bundle as export_strategy_bundle
from ._routes_ai_build import get_strategy_source as get_strategy_source
from ._routes_ai_build import inspect_strategy_bundle as inspect_strategy_bundle
from ._routes_ai_build import publish_ai_strategy as publish_ai_strategy
from ._routes_ai_build import reload_strategies as reload_strategies
from ._routes_ai_build import router as _sub__routes_ai_build
from ._routes_ai_build import save_composite_strategy as save_composite_strategy
from ._routes_ai_build import save_strategy_code as save_strategy_code
from ._routes_ai_build import validate_strategy_code as validate_strategy_code
from ._routes_core import get_strategy as get_strategy
from ._routes_core import list_strategies as list_strategies
from ._routes_core import patch_config as patch_config
from ._routes_core import reset_config as reset_config
from ._routes_core import router as _sub__routes_core
from ._routes_core import run_all as run_all
from ._routes_core import run_strategy as run_strategy
from ._routes_core import save_config as save_config

router = APIRouter(tags=["strategies"])

router.include_router(_sub__routes_core)
router.include_router(_sub__routes_ai_build)

__all__ = [
    "MINUTE_EXIT_TRIGGER_SIGNALS",
    "SCORING_DIRECTIONS",
    "_BUNDLE_FILE",
    "_OVERLAY_UNSUPPORTED_BACKENDS",
    "AIGenerateRequest",
    "AIIterateRequest",
    "AISaveRequest",
    "AIStrategyGenerator",
    "APIRouter",
    "Any",
    "BaseModel",
    "BuildRequest",
    "CompositeChildItem",
    "Field",
    "File",
    "Form",
    "HTTPException",
    "Literal",
    "MonitorStartRequest",
    "Path",
    "Request",
    "RunAllRequest",
    "RunRequest",
    "SaveConfigRequest",
    "StrategyCodeSaveRequest",
    "StrategyCodeValidateRequest",
    "StrategyCompositeSaveRequest",
    "StrategyDef",
    "StrategyEngine",
    "StrategyMonitorService",
    "StreamingResponse",
    "UploadFile",
    "_apply_strategy_bundle",
    "_build_execution_backend",
    "_build_prompt",
    "_bundle_service",
    "_child_meta",
    "_cleanup_deleted_strategy",
    "_composite_children",
    "_data_dir",
    "_find_meta_dict",
    "_get_engine",
    "_get_monitor",
    "_get_public_strategy",
    "_insert_meta_field",
    "_invalidate_strategy_runtime",
    "_missing_custom_signals",
    "_normalize_build_result",
    "_normalize_strategy_meta",
    "_overlay_changed",
    "_prepare_strategy_code",
    "_prepare_strategy_code_for_save",
    "_py_string",
    "_reload_and_verify_strategy",
    "_render_composite_code",
    "_resolve_strategy_save_target",
    "_restore_strategy_file",
    "_risk_fields",
    "_safe",
    "_save_composite_strategy",
    "_save_strategy_code",
    "_set_meta_bool_field",
    "_set_meta_string_field",
    "_strategy_detail",
    "_strategy_execution_backend",
    "_strip_defaults",
    "_target_dir",
    "_validate_overlay_filter",
    "_validate_scoring_config",
    "_validate_strategy_id",
    "ai_generate",
    "ai_iterate",
    "ai_save",
    "ai_status",
    "ai_test",
    "apply_strategy_bundle",
    "asdict",
    "ast",
    "build_step1",
    "build_step2",
    "build_strategy",
    "build_strategy_stream",
    "contextlib",
    "date",
    "delete_strategy",
    "effective_scoring",
    "effective_scoring_directions",
    "export_strategy_bundle",
    "find_meta_assignment",
    "get_strategy",
    "get_strategy_source",
    "inspect_strategy_bundle",
    "json",
    "list_strategies",
    "logger",
    "logging",
    "math",
    "patch_config",
    "publish_ai_strategy",
    "re",
    "reload_strategies",
    "reset_config",
    "router",
    "run_all",
    "run_in_threadpool",
    "run_strategy",
    "save_composite_strategy",
    "save_config",
    "save_strategy_code",
    "save_strategy_code_core",
    "strategy_config",
    "strategy_pool",
    "validate_strategy_code",
    "with_heartbeat",
]
