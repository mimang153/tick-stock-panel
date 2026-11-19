"""Internal provider schema column lists.

这些常量是 provider 与存储层共享的标准化列契约：顺序即写入 parquet 的列顺序，
新增列只能追加，避免历史分区读取时列错位。
"""

from __future__ import annotations

DAILY_COLUMNS = [
    "symbol",
    "asset_type",
    "source",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "pre_close",
    "change_pct",
]

ADJ_FACTOR_COLUMNS = ["symbol", "asset_type", "source", "trade_date", "ex_factor"]

INSTRUMENT_COLUMNS = [
    "symbol",
    "name",
    "exchange",
    "asset_type",
    "source",
    "listing_date",
    "status",
]

MINUTE_COLUMNS = [
    "symbol",
    "asset_type",
    "source",
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "freq",
]
