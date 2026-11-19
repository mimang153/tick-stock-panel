"""原 app.services.quote_service 模块对外入口：重导出全部顶层名字。

拆分后 QuoteService 由 _part1/_part2/_part3 三个 mixin 组合而成，_common 提供
SSE 订阅者与共享常量，_service 负责组装与注入。本模块保留原有导入路径，任何
调用方 `from app.services.quote_service import X` 都无需感知内部分包。
"""

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import ClassVar

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.parquet import scan_daily_parquet
from app.polars_guard import guarded_collect
from app.services.index_const import CORE_INDEX_SYMBOLS
from app.strategy.intraday_signals import IntradaySignalEvaluator
from app.strategy.monitor import format_alert_quote

from ._common import (
    _FINAL_BOUNDARY,
    _FINAL_CONFIRM_SLACK_MS,
    _FINAL_DEADLINE,
    _LAST_FETCH_WRITE_INTERVAL_MS,
    _SYSNOTIFY_EXECUTOR,
    _WEBHOOK_EXECUTOR,
    SOURCE_LABELS,
    QuoteSubscriber,
    _body_with_quote,
    _deliver_system_notify,
    _last_fetch_written_at_ms,
    _monitor_name_map,
    _persist_last_fetch,
    logger,
    shutdown_delivery_executors,
)
from ._part1 import QuoteServiceMixin1
from ._part2 import QuoteServiceMixin2
from ._part3 import QuoteServiceMixin3
from ._service import QuoteService

__all__ = [
    "CN_TZ",
    "CORE_INDEX_SYMBOLS",
    "SOURCE_LABELS",
    "_FINAL_BOUNDARY",
    "_FINAL_CONFIRM_SLACK_MS",
    "_FINAL_DEADLINE",
    "_LAST_FETCH_WRITE_INTERVAL_MS",
    "_SYSNOTIFY_EXECUTOR",
    "_WEBHOOK_EXECUTOR",
    "ClassVar",
    "IntradaySignalEvaluator",
    "Path",
    "QuoteService",
    "QuoteServiceMixin1",
    "QuoteServiceMixin2",
    "QuoteServiceMixin3",
    "QuoteSubscriber",
    "ThreadPoolExecutor",
    "_body_with_quote",
    "_deliver_system_notify",
    "_last_fetch_written_at_ms",
    "_monitor_name_map",
    "_persist_last_fetch",
    "asyncio",
    "cn_now",
    "cn_today",
    "contextmanager",
    "date",
    "datetime",
    "dt_time",
    "format_alert_quote",
    "guarded_collect",
    "logger",
    "logging",
    "pl",
    "scan_daily_parquet",
    "shutdown_delivery_executors",
    "suppress",
    "threading",
    "time",
    "timedelta",
]
