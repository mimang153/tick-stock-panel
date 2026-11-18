"""日 K 同步服务(§7.7 Step 1)。

调度器在 capability 允许下,把符号集合的日 K 批量同步到本地 Parquet。
策略:
  - 日 K 仅使用 `kline.daily.batch`
  - 除权因子仅使用 `adj_factor`
"""

from __future__ import annotations

import contextlib as contextlib
import hashlib as hashlib
import json as json
import logging as logging
import shutil as shutil
import time as time
import uuid as uuid
from collections.abc import Callable as Callable
from datetime import date as date
from datetime import datetime as datetime
from datetime import timedelta as timedelta
from pathlib import Path as Path
from typing import NamedTuple as NamedTuple

import polars as pl

from app.data_providers.base import (
    AssetType as AssetType,
)
from app.data_providers.base import (
    ProviderRequestError as ProviderRequestError,
)
from app.data_providers.minute_normalizer import (
    CANONICAL_MINUTE_COLS as CANONICAL_MINUTE_COLS,
)
from app.data_providers.minute_normalizer import (
    _compact_klines_to_df as _compact_klines_to_df,
)
from app.data_providers.minute_normalizer import (
    _datetime_to_ms as _datetime_to_ms,
)
from app.data_providers.minute_normalizer import (
    _enforce_minute_beijing_wallclock as _enforce_minute_beijing_wallclock,
)
from app.data_providers.minute_normalizer import (
    _normalize_minute as _normalize_minute,
)
from app.data_providers.minute_normalizer import (
    _timestamp_to_beijing_datetime as _timestamp_to_beijing_datetime,
)
from app.data_providers.minute_stream import iter_minute_frames as iter_minute_frames
from app.indicators.pipeline import filter_halt_days as filter_halt_days
from app.market_time import CN_TZ as CN_TZ
from app.market_time import cn_now as cn_now
from app.market_time import cn_today as cn_today
from app.parquet import (
    scan_daily_parquet as scan_daily_parquet,
)
from app.parquet import (
    scan_parquet_compat as scan_parquet_compat,
)
from app.polars_guard import guarded_collect as guarded_collect
from app.services import minute_adjust as minute_adjust
from app.services import preferences as preferences
from app.services.shared_work import SharedWork as SharedWork
from app.services.shared_work import WorkBusyError as WorkBusyError
from app.tickflow.capabilities import Cap as Cap
from app.tickflow.capabilities import CapabilitySet as CapabilitySet
from app.tickflow.client import get_client as get_client
from app.tickflow.rate_limits import (
    chunked as chunked,
)
from app.tickflow.rate_limits import (
    resolve_limit as resolve_limit,
)
from app.tickflow.rate_limits import (
    sleep_between_batches as sleep_between_batches,
)
from app.tickflow.repository import (
    KlineRepository as KlineRepository,
)
from app.tickflow.repository import (
    replace_with_retry as replace_with_retry,
)
from app.tickflow.request_guard import (
    SDKCallTimeoutError as SDKCallTimeoutError,
)
from app.tickflow.request_guard import (
    _sdk_call_with_deadline as _sdk_call_with_deadline,
)

# --- re-export：保持 import 路径与 monkeypatch 可观察性 ---
from .kline_adj import (
    _adj_custom_sync as _adj_custom_sync,
)
from .kline_adj import (
    _adj_fetch_tickflow as _adj_fetch_tickflow,
)
from .kline_adj import (
    _align_adj_factor_frame as _align_adj_factor_frame,
)
from .kline_adj import (
    _fetch_adj_factor_rows as _fetch_adj_factor_rows,
)
from .kline_adj import (
    _merge_adj_factor_store as _merge_adj_factor_store,
)
from .kline_adj import (
    _mirror_adj_to_ck as _mirror_adj_to_ck,
)
from .kline_adj import (
    _normalize_adj_factor as _normalize_adj_factor,
)
from .kline_adj import (
    fetch_adj_factor_single as fetch_adj_factor_single,
)
from .kline_adj import (
    sync_adj_factor as sync_adj_factor,
)
from .kline_daily import (
    DailySourceUnavailableError as DailySourceUnavailableError,
)
from .kline_daily import (
    _fetch_batch_klines as _fetch_batch_klines,
)
from .kline_daily import (
    _instruments_symbol_index as _instruments_symbol_index,
)
from .kline_daily import (
    _make_daily_finalizer as _make_daily_finalizer,
)
from .kline_daily import (
    _normalize_bare_symbols as _normalize_bare_symbols,
)
from .kline_daily import (
    _normalize_daily as _normalize_daily,
)
from .kline_daily import (
    _persist_daily_chunks as _persist_daily_chunks,
)
from .kline_daily import (
    _sweep_stale_daily_staging as _sweep_stale_daily_staging,
)
from .kline_daily import (
    _sync_daily_custom as _sync_daily_custom,
)
from .kline_daily import (
    iter_custom_daily_frames as iter_custom_daily_frames,
)
from .kline_daily import (
    iter_daily_frames as iter_daily_frames,
)
from .kline_daily import (
    sync_and_persist_daily_batch as sync_and_persist_daily_batch,
)
from .kline_daily import (
    sync_daily_batch as sync_daily_batch,
)
from .kline_daily import (
    sync_daily_by_quotes as sync_daily_by_quotes,
)
from .kline_gate import (
    _minute_gate as _minute_gate,
)
from .kline_gate import (
    _minute_split_by_asset as _minute_split_by_asset,
)
from .kline_gate import (
    _minute_sync_window as _minute_sync_window,
)
from .kline_gate import (
    _minute_window as _minute_window,
)
from .kline_gate import (
    sync_and_persist_minute as sync_and_persist_minute,
)
from .kline_intraday import (
    _fetch_intraday_chunk as _fetch_intraday_chunk,
)
from .kline_intraday import (
    _resolve_full_minute_provider as _resolve_full_minute_provider,
)
from .kline_intraday import (
    fetch_intraday_custom_batch as fetch_intraday_custom_batch,
)
from .kline_intraday import (
    fetch_intraday_custom_latest as fetch_intraday_custom_latest,
)
from .kline_intraday import (
    fetch_intraday_full_market_burst as fetch_intraday_full_market_burst,
)
from .kline_intraday import (
    fetch_intraday_monitor_batch as fetch_intraday_monitor_batch,
)
from .kline_intraday import (
    fetch_intraday_universe_increment as fetch_intraday_universe_increment,
)
from .kline_intraday import (
    intraday_monitor_support as intraday_monitor_support,
)
from .kline_minute import (
    MinuteSourceStamp as MinuteSourceStamp,
)
from .kline_minute import (
    _as_beijing as _as_beijing,
)
from .kline_minute import (
    _call_custom_minute as _call_custom_minute,
)
from .kline_minute import (
    _cleanup_null_datetime_minute as _cleanup_null_datetime_minute,
)
from .kline_minute import (
    _earliest_minute_datetime as _earliest_minute_datetime,
)
from .kline_minute import (
    _fetch_minute_single as _fetch_minute_single,
)
from .kline_minute import (
    _latest_minute_datetime as _latest_minute_datetime,
)
from .kline_minute import (
    _migrate_symbol_to_date_partition as _migrate_symbol_to_date_partition,
)
from .kline_minute import (
    _minute_bounds as _minute_bounds,
)
from .kline_minute import (
    _mirror_minute_to_ck as _mirror_minute_to_ck,
)
from .kline_minute import (
    _read_minute_day as _read_minute_day,
)
from .kline_minute import (
    _resolve_minute_provider as _resolve_minute_provider,
)
from .kline_minute import (
    _selected_minute_provider_name as _selected_minute_provider_name,
)
from .kline_minute import (
    _stream_custom_minutes as _stream_custom_minutes,
)
from .kline_minute import (
    _try_custom_minute as _try_custom_minute,
)
from .kline_minute import (
    _wrap_minute_callback as _wrap_minute_callback,
)
from .kline_minute import (
    _write_minute_partition as _write_minute_partition,
)
from .kline_minute import (
    fetch_minute_single as fetch_minute_single,
)
from .kline_minute import (
    minute_source_stamp as minute_source_stamp,
)
from .kline_minute import (
    sync_minute_batch as sync_minute_batch,
)

logger = logging.getLogger(__name__)

_single_minute_work = SharedWork(
    "minute.single",
    max_pending=128,
    max_entries=512,
    max_bytes=16 * 1024 * 1024,
    size_of=lambda frame: frame.estimated_size(),
)


def _log_cap_unavailable(cap_id: str, action: str) -> None:
    """门禁未通过时记一条带生效源的日志。

    原实现是静默 return —— 切换数据源后「什么都没发生」且无迹可循。
    带源标签后可一眼看出是路由到不可用源, 还是 TickFlow 档位不足。
    """
    try:
        from app.data_providers.capabilities import capability_gate

        _, label = capability_gate(cap_id)
    except Exception:  # noqa: BLE001 — 仅日志, 失败不应影响主流程
        label = cap_id
    logger.info("%s 跳过: %s 不可用", action, label)


def _atomic_write_parquet(df: pl.DataFrame, out) -> None:
    """先写临时文件再原子替换, 避免进程中断留下损坏的 parquet。

    与 repository._atomic_write_parquet 同语义。adj_factor 的 all.parquet 是全市场
    单文件、每次「读→concat→原地写」, 直接 write_parquet(out) 在进程被 kill
    (dev.sh 清端口用 kill -9)、reap 超时或断电时会留下半截文件, 之后复权视图
    scan_parquet 整条链路报错、enriched 全市场重算不出。临时文件后缀 .tmp 不匹配
    *.parquet glob, 不会被扫描误读。Windows 下目标正被并发读取时由
    replace_with_retry 短退避穿过。
    """
    # 唯一后缀: 两个写入者共用固定名 tmp 并发写同一文件会交错损坏 (被 rename
    # 定版后分区报废)。崩溃残留的孤儿 tmp 不匹配 *.parquet glob, 读侧无影响。
    tmp = out.with_name(f"{out.name}.{uuid.uuid4().hex[:8]}.tmp")
    df.write_parquet(tmp)
    replace_with_retry(tmp, out)


# 标准列(无论 SDK 返回什么形状,我们把它规范成这套)
CANONICAL_DAILY_COLS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]


# ---- sync_and_persist_daily_batch 分段 helper ----


# 除权事件明细列 (可选, 缺省为 null): 等差显示投影 (原始价 - Σ未来每股分红,
# 分红按其后送转比例缩放) 与官方金额全精度因子链重建使用。
# 存量 all.parquet 只有规范三列, 合并用 how="diagonal" 自动补 null;
# 读取方 (_apply_adj_factor / minute_adjust) 只按规范三列消费, 不受影响。
ADJ_DETAIL_COLS = ("dividend", "bonus", "allot", "allot_price", "prev_close")


# ---- sync_adj_factor 分段 helper ----


# ===== 分钟 K 同步 =====


# ---- _try_custom_minute 分段 helper ----


# ---- sync_and_persist_minute 分段 helper ----
