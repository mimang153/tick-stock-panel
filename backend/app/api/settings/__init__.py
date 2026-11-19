"""settings API —— 由单文件 settings.py 机械拆分而来。

模块布局：
- _common.py：共享常量、Pydantic 请求模型与跨分组内部 helper；
- _routes_core.py：设置总览、端点切换、TickFlow Key 与 AI 设置相关路由；
- _routes_preferences.py：偏好读写、数据源/插件与 UI 布局相关路由；
- _routes_notifications.py：通知/Webhook/机器人与定时任务相关路由；
- _routes_system.py：端点探测、批处理参数、令牌与音效包相关路由。

行为保持：
- 子路由按原文件装饰器顺序 include，最终路由 path 与顺序逐字不变；
- 原模块内的导入名与顶层定义全部在此重导出，兼容 from ... import ...；
- 被测试 monkeypatch 的模块级名字通过包属性动态解析，patch 仍然生效。

兼容性说明：
- app.api.routes 从本模块导入 _sync_financial_scheduler_caps，此处直接重导出；
- 路由分组只改变代码物理位置，不改变路由函数签名、依赖与返回结构；
- 共享的可变状态（如 _endpoints_cache）仍由本包持有，避免复制出第二份状态；
- 新增路由必须放回对应分组的末尾，以保持与历史版本一致的路径顺序。
"""

from __future__ import annotations

import logging as logging
import time as time
from pathlib import Path as Path
from typing import Literal as Literal

from fastapi import APIRouter
from fastapi import File as File
from fastapi import HTTPException as HTTPException
from fastapi import Request as Request
from fastapi import UploadFile as UploadFile
from fastapi.responses import FileResponse as FileResponse
from pydantic import BaseModel as BaseModel
from pydantic import ConfigDict as ConfigDict
from pydantic import Field as Field

from app import secrets_store as secrets_store
from app.data_providers.custom.config import MAX_TIMEOUT as MAX_TIMEOUT
from app.tickflow import client as tf_client
from app.tickflow.policy import detect_capabilities as detect_capabilities
from app.tickflow.policy import extras_caps as extras_caps
from app.tickflow.policy import missing_caps as missing_caps
from app.tickflow.policy import probe_log as probe_log
from app.tickflow.policy import tier_label as tier_label

from ._common import _FALLBACK_ENDPOINTS as _FALLBACK_ENDPOINTS
from ._common import _SOUND_PACK_DIR as _SOUND_PACK_DIR
from ._common import _SOUND_PACK_EXTS as _SOUND_PACK_EXTS
from ._common import _SOUND_PACK_MAX_BYTES as _SOUND_PACK_MAX_BYTES
from ._common import _SOUND_PACK_MEDIA_TYPES as _SOUND_PACK_MEDIA_TYPES
from ._common import DEFAULT_PAID_ENDPOINT as DEFAULT_PAID_ENDPOINT
from ._common import ENDPOINTS_TTL as ENDPOINTS_TTL
from ._common import ENDPOINTS_URL as ENDPOINTS_URL
from ._common import AiSettingsIn as AiSettingsIn
from ._common import AiTestIn as AiTestIn
from ._common import ApiTokenCreateIn as ApiTokenCreateIn
from ._common import AssistantAutoApproveIn as AssistantAutoApproveIn
from ._common import AssistantMaskIn as AssistantMaskIn
from ._common import AuthConfigIn as AuthConfigIn
from ._common import BacktestWorkerRssLimitIn as BacktestWorkerRssLimitIn
from ._common import BatchSymbolsIn as BatchSymbolsIn
from ._common import CustomSourceIn as CustomSourceIn
from ._common import CustomSourceTestIn as CustomSourceTestIn
from ._common import CustomWebhookPrefsIn as CustomWebhookPrefsIn
from ._common import DailyBatchCompressPrefs as DailyBatchCompressPrefs
from ._common import DashboardLayoutIn as DashboardLayoutIn
from ._common import DataProvidersIn as DataProvidersIn
from ._common import DatasetConfigIn as DatasetConfigIn
from ._common import DatasetFieldMapItem as DatasetFieldMapItem
from ._common import DataSourceJobTimeoutPrefs as DataSourceJobTimeoutPrefs
from ._common import DepthFinalizeTimeIn as DepthFinalizeTimeIn
from ._common import DepthPollingIntervalIn as DepthPollingIntervalIn
from ._common import EmailSmtpPrefsIn as EmailSmtpPrefsIn
from ._common import EmotionScoreConfigIn as EmotionScoreConfigIn
from ._common import EnrichedBatchSizeIn as EnrichedBatchSizeIn
from ._common import FeishuWebhookPrefsIn as FeishuWebhookPrefsIn
from ._common import IndexDailyBatchSizeIn as IndexDailyBatchSizeIn
from ._common import LimitLadderMonitorIn as LimitLadderMonitorIn
from ._common import MainlineFilterIn as MainlineFilterIn
from ._common import MarketRecapPromptIn as MarketRecapPromptIn
from ._common import MiningSchedulePrefs as MiningSchedulePrefs
from ._common import MinuteBatchCompressPrefs as MinuteBatchCompressPrefs
from ._common import MinuteSyncPrefs as MinuteSyncPrefs
from ._common import NavHiddenIn as NavHiddenIn
from ._common import NavLayoutIn as NavLayoutIn
from ._common import NavOrderIn as NavOrderIn
from ._common import PipelineIndexSymbolsIn as PipelineIndexSymbolsIn
from ._common import PipelinePullTypesIn as PipelinePullTypesIn
from ._common import PipelineRegimeEnabledIn as PipelineRegimeEnabledIn
from ._common import PipelineScheduleIn as PipelineScheduleIn
from ._common import PluginKeyIn as PluginKeyIn
from ._common import QuoteIntervalIn as QuoteIntervalIn
from ._common import RealtimeMonitorConfigIn as RealtimeMonitorConfigIn
from ._common import RealtimeQuoteScopePrefs as RealtimeQuoteScopePrefs
from ._common import RealtimeQuotesPrefs as RealtimeQuotesPrefs
from ._common import RegimeBatchParamsIn as RegimeBatchParamsIn
from ._common import ReviewPushIn as ReviewPushIn
from ._common import ReviewScheduleIn as ReviewScheduleIn
from ._common import SwitchEndpointIn as SwitchEndpointIn
from ._common import SystemNotifyPrefsIn as SystemNotifyPrefsIn
from ._common import TestEndpointIn as TestEndpointIn
from ._common import TickflowKeyIn as TickflowKeyIn
from ._common import UiPrefsIn as UiPrefsIn
from ._common import WatchlistGroupsInNavPrefs as WatchlistGroupsInNavPrefs
from ._common import WebhookDefaultChannelsIn as WebhookDefaultChannelsIn
from ._common import WebhookEnabledDefaultIn as WebhookEnabledDefaultIn
from ._common import WebhookTestIn as WebhookTestIn
from ._common import WecomBotPrefsIn as WecomBotPrefsIn
from ._common import WecomBotToggleIn as WecomBotToggleIn
from ._common import WecomWebhookPrefsIn as WecomWebhookPrefsIn
from ._common import _ai_settings_status as _ai_settings_status
from ._common import _apply_ai_settings_updates as _apply_ai_settings_updates
from ._common import _apply_realtime_prefs as _apply_realtime_prefs
from ._common import _check_realtime_enable_gates as _check_realtime_enable_gates
from ._common import _endpoints_cache as _endpoints_cache
from ._common import _force_disable_realtime_quotes as _force_disable_realtime_quotes
from ._common import _http_ping as _http_ping
from ._common import _minute_history_days as _minute_history_days
from ._common import _probe_and_sync_capabilities as _probe_and_sync_capabilities
from ._common import _realtime_allowed as _realtime_allowed
from ._common import _safe_pack_path as _safe_pack_path
from ._common import _sound_pack_dir as _sound_pack_dir
from ._common import _sync_depth_polling as _sync_depth_polling
from ._common import _sync_financial_scheduler_caps as _sync_financial_scheduler_caps
from ._common import logger as logger
from ._routes_core import clear_ai_settings as clear_ai_settings
from ._routes_core import clear_tickflow_key as clear_tickflow_key
from ._routes_core import complete_onboarding as complete_onboarding
from ._routes_core import get_settings as get_settings
from ._routes_core import list_sponsor_models as list_sponsor_models
from ._routes_core import router as _sub__routes_core
from ._routes_core import save_ai_settings as save_ai_settings
from ._routes_core import save_tickflow_key as save_tickflow_key
from ._routes_core import switch_endpoint as switch_endpoint
from ._routes_core import test_ai_settings as test_ai_settings
from ._routes_notifications import get_emotion_score_config as get_emotion_score_config
from ._routes_notifications import get_quote_interval as get_quote_interval
from ._routes_notifications import router as _sub__routes_notifications
from ._routes_notifications import test_webhook as test_webhook
from ._routes_notifications import toggle_wecom_bot as toggle_wecom_bot
from ._routes_notifications import update_assistant_auto_approve as update_assistant_auto_approve
from ._routes_notifications import update_assistant_mask as update_assistant_mask
from ._routes_notifications import update_custom_webhook as update_custom_webhook
from ._routes_notifications import update_email_smtp as update_email_smtp
from ._routes_notifications import update_emotion_score_config as update_emotion_score_config
from ._routes_notifications import update_feishu_webhook as update_feishu_webhook
from ._routes_notifications import update_mainline_filter as update_mainline_filter
from ._routes_notifications import update_pipeline_index_symbols as update_pipeline_index_symbols
from ._routes_notifications import update_pipeline_pull_types as update_pipeline_pull_types
from ._routes_notifications import update_pipeline_regime_enabled as update_pipeline_regime_enabled
from ._routes_notifications import update_quote_interval as update_quote_interval
from ._routes_notifications import update_realtime_monitor_config as update_realtime_monitor_config
from ._routes_notifications import update_realtime_quote_scope as update_realtime_quote_scope
from ._routes_notifications import update_realtime_quotes as update_realtime_quotes
from ._routes_notifications import update_regime_batch_params as update_regime_batch_params
from ._routes_notifications import update_system_notify as update_system_notify
from ._routes_notifications import update_watchlist_groups_in_nav as update_watchlist_groups_in_nav
from ._routes_notifications import (
    update_webhook_default_channels as update_webhook_default_channels,
)
from ._routes_notifications import update_webhook_enabled_default as update_webhook_enabled_default
from ._routes_notifications import update_wecom_bot as update_wecom_bot
from ._routes_notifications import update_wecom_webhook as update_wecom_webhook
from ._routes_preferences import clear_plugin_key as clear_plugin_key
from ._routes_preferences import delete_data_source as delete_data_source
from ._routes_preferences import get_capability_matrix as get_capability_matrix
from ._routes_preferences import get_data_source as get_data_source
from ._routes_preferences import get_preferences as get_preferences
from ._routes_preferences import get_screener_result_columns as get_screener_result_columns
from ._routes_preferences import get_watchlist_columns as get_watchlist_columns
from ._routes_preferences import install_plugin as install_plugin
from ._routes_preferences import list_data_sources as list_data_sources
from ._routes_preferences import minute_refresh_status as minute_refresh_status
from ._routes_preferences import reload_data_sources as reload_data_sources
from ._routes_preferences import router as _sub__routes_preferences
from ._routes_preferences import save_data_source as save_data_source
from ._routes_preferences import save_plugin_key as save_plugin_key
from ._routes_preferences import test_data_source as test_data_source
from ._routes_preferences import uninstall_plugin as uninstall_plugin
from ._routes_preferences import update_daily_batch_compress as update_daily_batch_compress
from ._routes_preferences import update_dashboard_layout as update_dashboard_layout
from ._routes_preferences import update_data_providers as update_data_providers
from ._routes_preferences import update_data_source_job_timeouts as update_data_source_job_timeouts
from ._routes_preferences import update_mining_schedule as update_mining_schedule
from ._routes_preferences import update_minute_batch_compress as update_minute_batch_compress
from ._routes_preferences import update_minute_sync as update_minute_sync
from ._routes_preferences import update_nav_hidden as update_nav_hidden
from ._routes_preferences import update_nav_layout as update_nav_layout
from ._routes_preferences import update_nav_order as update_nav_order
from ._routes_preferences import update_screener_result_columns as update_screener_result_columns
from ._routes_preferences import update_ui_prefs as update_ui_prefs
from ._routes_preferences import update_watchlist_columns as update_watchlist_columns
from ._routes_system import api_tokens_create as api_tokens_create
from ._routes_system import api_tokens_list as api_tokens_list
from ._routes_system import api_tokens_revoke as api_tokens_revoke
from ._routes_system import get_market_recap_prompt as get_market_recap_prompt
from ._routes_system import list_endpoints as list_endpoints
from ._routes_system import router as _sub__routes_system
from ._routes_system import run_limit_ladder_fix as run_limit_ladder_fix
from ._routes_system import sound_packs_delete as sound_packs_delete
from ._routes_system import sound_packs_file as sound_packs_file
from ._routes_system import sound_packs_list as sound_packs_list
from ._routes_system import sound_packs_upload as sound_packs_upload
from ._routes_system import test_endpoint as test_endpoint
from ._routes_system import update_backtest_worker_rss_limit as update_backtest_worker_rss_limit
from ._routes_system import update_daily_batch_symbols as update_daily_batch_symbols
from ._routes_system import update_depth_finalize_time as update_depth_finalize_time
from ._routes_system import update_depth_polling_interval as update_depth_polling_interval
from ._routes_system import update_enriched_batch_size as update_enriched_batch_size
from ._routes_system import update_index_daily_batch_size as update_index_daily_batch_size
from ._routes_system import update_instruments_schedule as update_instruments_schedule
from ._routes_system import update_limit_ladder_monitor as update_limit_ladder_monitor
from ._routes_system import update_market_recap_prompt as update_market_recap_prompt
from ._routes_system import update_minute_batch_symbols as update_minute_batch_symbols
from ._routes_system import update_pipeline_schedule as update_pipeline_schedule
from ._routes_system import update_review_push as update_review_push
from ._routes_system import update_review_schedule as update_review_schedule

router = APIRouter(tags=["settings"])

router.include_router(_sub__routes_core)
router.include_router(_sub__routes_preferences)
router.include_router(_sub__routes_notifications)
router.include_router(_sub__routes_system)

__all__ = [
    "DEFAULT_PAID_ENDPOINT",
    "ENDPOINTS_TTL",
    "ENDPOINTS_URL",
    "MAX_TIMEOUT",
    "_FALLBACK_ENDPOINTS",
    "_SOUND_PACK_DIR",
    "_SOUND_PACK_EXTS",
    "_SOUND_PACK_MAX_BYTES",
    "_SOUND_PACK_MEDIA_TYPES",
    "APIRouter",
    "AiSettingsIn",
    "AiTestIn",
    "ApiTokenCreateIn",
    "AssistantAutoApproveIn",
    "AssistantMaskIn",
    "AuthConfigIn",
    "BacktestWorkerRssLimitIn",
    "BaseModel",
    "BatchSymbolsIn",
    "ConfigDict",
    "CustomSourceIn",
    "CustomSourceTestIn",
    "CustomWebhookPrefsIn",
    "DailyBatchCompressPrefs",
    "DashboardLayoutIn",
    "DataProvidersIn",
    "DataSourceJobTimeoutPrefs",
    "DatasetConfigIn",
    "DatasetFieldMapItem",
    "DepthFinalizeTimeIn",
    "DepthPollingIntervalIn",
    "EmailSmtpPrefsIn",
    "EmotionScoreConfigIn",
    "EnrichedBatchSizeIn",
    "FeishuWebhookPrefsIn",
    "Field",
    "File",
    "FileResponse",
    "HTTPException",
    "IndexDailyBatchSizeIn",
    "LimitLadderMonitorIn",
    "Literal",
    "MainlineFilterIn",
    "MarketRecapPromptIn",
    "MiningSchedulePrefs",
    "MinuteBatchCompressPrefs",
    "MinuteSyncPrefs",
    "NavHiddenIn",
    "NavLayoutIn",
    "NavOrderIn",
    "Path",
    "PipelineIndexSymbolsIn",
    "PipelinePullTypesIn",
    "PipelineRegimeEnabledIn",
    "PipelineScheduleIn",
    "PluginKeyIn",
    "QuoteIntervalIn",
    "RealtimeMonitorConfigIn",
    "RealtimeQuoteScopePrefs",
    "RealtimeQuotesPrefs",
    "RegimeBatchParamsIn",
    "Request",
    "ReviewPushIn",
    "ReviewScheduleIn",
    "SwitchEndpointIn",
    "SystemNotifyPrefsIn",
    "TestEndpointIn",
    "TickflowKeyIn",
    "UiPrefsIn",
    "UploadFile",
    "WatchlistGroupsInNavPrefs",
    "WebhookDefaultChannelsIn",
    "WebhookEnabledDefaultIn",
    "WebhookTestIn",
    "WecomBotPrefsIn",
    "WecomBotToggleIn",
    "WecomWebhookPrefsIn",
    "_ai_settings_status",
    "_apply_ai_settings_updates",
    "_apply_realtime_prefs",
    "_check_realtime_enable_gates",
    "_endpoints_cache",
    "_force_disable_realtime_quotes",
    "_http_ping",
    "_minute_history_days",
    "_probe_and_sync_capabilities",
    "_realtime_allowed",
    "_safe_pack_path",
    "_sound_pack_dir",
    "_sync_depth_polling",
    "_sync_financial_scheduler_caps",
    "api_tokens_create",
    "api_tokens_list",
    "api_tokens_revoke",
    "clear_ai_settings",
    "clear_plugin_key",
    "clear_tickflow_key",
    "complete_onboarding",
    "delete_data_source",
    "detect_capabilities",
    "extras_caps",
    "get_capability_matrix",
    "get_data_source",
    "get_emotion_score_config",
    "get_market_recap_prompt",
    "get_preferences",
    "get_quote_interval",
    "get_screener_result_columns",
    "get_settings",
    "get_watchlist_columns",
    "install_plugin",
    "list_data_sources",
    "list_endpoints",
    "list_sponsor_models",
    "logger",
    "logging",
    "minute_refresh_status",
    "missing_caps",
    "probe_log",
    "reload_data_sources",
    "router",
    "run_limit_ladder_fix",
    "save_ai_settings",
    "save_data_source",
    "save_plugin_key",
    "save_tickflow_key",
    "secrets_store",
    "sound_packs_delete",
    "sound_packs_file",
    "sound_packs_list",
    "sound_packs_upload",
    "switch_endpoint",
    "test_ai_settings",
    "test_data_source",
    "test_endpoint",
    "test_webhook",
    "tf_client",
    "tier_label",
    "time",
    "toggle_wecom_bot",
    "uninstall_plugin",
    "update_assistant_auto_approve",
    "update_assistant_mask",
    "update_backtest_worker_rss_limit",
    "update_custom_webhook",
    "update_daily_batch_compress",
    "update_daily_batch_symbols",
    "update_dashboard_layout",
    "update_data_providers",
    "update_data_source_job_timeouts",
    "update_depth_finalize_time",
    "update_depth_polling_interval",
    "update_email_smtp",
    "update_emotion_score_config",
    "update_enriched_batch_size",
    "update_feishu_webhook",
    "update_index_daily_batch_size",
    "update_instruments_schedule",
    "update_limit_ladder_monitor",
    "update_mainline_filter",
    "update_market_recap_prompt",
    "update_mining_schedule",
    "update_minute_batch_compress",
    "update_minute_batch_symbols",
    "update_minute_sync",
    "update_nav_hidden",
    "update_nav_layout",
    "update_nav_order",
    "update_pipeline_index_symbols",
    "update_pipeline_pull_types",
    "update_pipeline_regime_enabled",
    "update_pipeline_schedule",
    "update_quote_interval",
    "update_realtime_monitor_config",
    "update_realtime_quote_scope",
    "update_realtime_quotes",
    "update_regime_batch_params",
    "update_review_push",
    "update_review_schedule",
    "update_screener_result_columns",
    "update_system_notify",
    "update_ui_prefs",
    "update_watchlist_columns",
    "update_watchlist_groups_in_nav",
    "update_webhook_default_channels",
    "update_webhook_enabled_default",
    "update_wecom_bot",
    "update_wecom_webhook",
]
