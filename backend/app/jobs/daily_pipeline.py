"""盘后管道 + 盘前维表同步。

调度:
  09:10 盘前 — 同步个股维表 instruments (全量覆盖)
  15:35 盘后 — 日K同步 + 增量除权因子 + enriched 计算 + 刷新视图
  (默认 15:35: 盘后固定价 15:30 终止 + 供应商日线定稿缓冲, 见 preferences)

盘后同步策略:
  日 K: QuoteService 交易时段已实时落盘 → 有数据时跳过 batch,首次拉 1 年区间
  除权因子: 从已有数据最新日期的下一天开始增量获取,避免重复拉取和计算
"""
# ruff: noqa: F401  # 根导入供子模块经 _root.<name> 延迟引用（保持 monkeypatch 可观察性）

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.config import settings  # 测试按模块属性 monkeypatch data_dir
from app.indicators.pipeline import filter_halt_days, run_pipeline
from app.market_time import cn_today
from app.services import index_sync, instrument_sync, kline_sync
from app.services import preferences as _prefs
from app.tickflow.capabilities import Cap, CapabilitySet
from app.tickflow.pools import get_pool
from app.tickflow.repository import KlineRepository

from .daily_pipeline_jobs import (
    _push_phase_change_alert,
    _recompute_enriched_for_affected,
    _refresh_single_view,
    _refresh_views,
    _resolve_minute_symbols,
    _run_tracked,
    _scheduled_pipeline_task,
    run_adj_factor_sync,
    warn_if_enriched_too_thin,
)

# ── 子模块 re-export（保持既有 import 路径不变）──
from .daily_pipeline_prepare import (
    PipelineStageError,
    _daily_chunk_progress,
    _invalidate,
    _noop,
    _prune_partial_enriched_partitions,
    _prune_stale_price_partitions,
    _resolve_universe,
    _run_daily_override,
    _run_prepare,
    _run_scan_integrity,
    run_instruments_sync,
)
from .daily_pipeline_review import (
    _maybe_push_review,
    _register_review_job,
    _run_scheduled_review,
    _sched_depth_finalize,
    _sched_instruments_task,
    _sched_pipeline_task,
    _stream_review_with_retry,
    _weekday_cron,
)
from .daily_pipeline_run import (
    _run_build_result,
    _run_compute_mainline,
    _run_compute_regime,
    _run_paper_settle,
    _run_post_repair_check,
    _run_sync_etf,
    _run_sync_index_etf,
    _run_sync_minute,
    capset_has_daily,
    run_now,
)
from .daily_pipeline_scheduler import (
    _sched_reprobe_capabilities,
    start_scheduler,
)
from .daily_pipeline_sync import (
    _run_compute_enriched,
    _run_daily_gapfill,
    _run_daily_initial,
    _run_daily_live,
    _run_detect_lagging,
    _run_enriched_prepare,
    _run_prune_enriched,
    _run_sync_adj,
    _run_sync_daily,
    _run_sync_index,
)

logger = logging.getLogger(__name__)

ProgressCb = Callable[..., None]


@dataclass
class _RunState:
    """run_now 跨阶段共享状态; 阶段 helper 就地读写, 保持原编排顺序与语义。"""

    repo: KlineRepository
    capset: CapabilitySet
    emit: ProgressCb
    override_start_date: date | None
    override_symbols: list[str] | None
    skipped: list[str] = field(default_factory=list)
    stage_errors: list[str] = field(default_factory=list)
    today: Any = None
    latest_daily: Any = None
    today_exists: bool = False
    pull_a_share: bool = True
    universe: list[str] = field(default_factory=list)
    integrity_issues: list = field(default_factory=list)
    stale_day: Any = None
    etf_stale_day: Any = None
    index_stale_day: Any = None
    daily_range_start: Any = None
    new_daily_days: int = 0
    repair_start: Any = None
    lagging_symbols: list[str] = field(default_factory=list)
    written_adj: int = 0
    affected_symbols: list[str] = field(default_factory=list)
    written_enriched: int = 0
    enriched_total_days: int = 0
    written_index_daily: int = 0
    written_etf_daily: int = 0
    index_count: int = 0
    etf_count: int = 0
    etf_adj_symbols: int = 0
    written_minute: int = 0
    regime_days: int = 0
    mainline_rows: int = 0
    paper_summary: dict = field(default_factory=dict)


# ================================================================
# 定时复盘 (AI 大盘复盘报告)
# ================================================================

REVIEW_JOB_ID = "scheduled_review"


# app_state 延迟引用(start_scheduler 在 lifespan 早期调用, app.state 可能还没就绪)
_app_state_ref = None


def set_app_state(app_state) -> None:
    """lifespan 注册 app.state 引用, 供 scheduled job 访问 depth_service 等单例。"""
    global _app_state_ref
    _app_state_ref = app_state


def _get_app_state():
    return _app_state_ref
