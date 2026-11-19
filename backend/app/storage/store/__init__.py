"""原 app.storage.store 模块对外入口。

重导出拆分前的全部顶层名字（含模块级导入名），保持 ``app.storage.store.X``
这一导入路径与 ``ClickHouseStore`` 对外行为完全兼容。
"""

import base64
import json
import logging
import threading
import uuid
from collections.abc import Sequence as Sequence
from datetime import date as date
from datetime import datetime as datetime
from datetime import timedelta as timedelta

import polars as pl

from app.storage.clickhouse import ClickHouseClient as ClickHouseClient
from app.storage.protocol import ADJ_FACTOR_TABLES as ADJ_FACTOR_TABLES
from app.storage.protocol import DAILY_TABLES as DAILY_TABLES
from app.storage.protocol import FINANCIAL_TABLES as FINANCIAL_TABLES
from app.storage.protocol import INSTRUMENT_TABLES as INSTRUMENT_TABLES
from app.storage.protocol import MINUTE_TABLES as MINUTE_TABLES
from app.storage.protocol import DedupeStrategy as DedupeStrategy
from app.storage.schema import TABLE_NAMES as TABLE_NAMES
from app.storage.schema import TABLE_READ_COLUMNS as TABLE_READ_COLUMNS

from ._common import ARGMAX_SPAN_DAYS as ARGMAX_SPAN_DAYS
from ._common import logger as logger
from ._store import ClickHouseStore as ClickHouseStore

__all__ = [
    "ADJ_FACTOR_TABLES",
    "ARGMAX_SPAN_DAYS",
    "DAILY_TABLES",
    "FINANCIAL_TABLES",
    "INSTRUMENT_TABLES",
    "MINUTE_TABLES",
    "TABLE_NAMES",
    "TABLE_READ_COLUMNS",
    "ClickHouseClient",
    "ClickHouseStore",
    "DedupeStrategy",
    "Sequence",
    "base64",
    "date",
    "datetime",
    "json",
    "logger",
    "logging",
    "pl",
    "threading",
    "timedelta",
    "uuid",
]
