"""存储门面接口（设计文档 §2 / §4.2）。

业务层只依赖这里的 Protocol：读返回 Polars DataFrame，写由实现决定是直写还是进缓冲。
表名、SQL、去重策略都是实现细节，上层不得拼 SQL，也不得自行拼表名。

B2 范围：主干六类日K + 除权因子 + 标的维表。分钟/财务/盘口/扩展/回测等
数据集在 B3 追加同类方法。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Literal, Protocol, runtime_checkable

import polars as pl

#: 去重策略：``final`` 走 CK 的 FINAL，``argmax`` 走 argMax 子查询，``auto`` 由实现按数据量选择
DedupeStrategy = Literal["auto", "final", "argmax"]

#: 主干日K数据集（设计文档 §3.1）。写入按 ReplacingMergeTree 语义 upsert，无需先读后写。
DAILY_TABLES: frozenset[str] = frozenset(
    {
        "kline_daily",
        "kline_daily_enriched",
        "kline_index_daily",
        "kline_index_enriched",
        "kline_etf_daily",
        "kline_etf_enriched",
    }
)

#: 除权因子表：股票与 ETF 分开存
ADJ_FACTOR_TABLES: frozenset[str] = frozenset({"adj_factor", "adj_factor_etf"})

#: 分钟K表（股票 / ETF 分开存）
MINUTE_TABLES: frozenset[str] = frozenset({"kline_minute", "kline_etf_minute"})

#: 财务五表（动态列走 Map）
FINANCIAL_TABLES: frozenset[str] = frozenset(
    {
        "financials_metrics",
        "financials_income",
        "financials_balance_sheet",
        "financials_cash_flow",
        "financials_shares",
    }
)

#: 资产类型 → 标的维表
INSTRUMENT_TABLES: dict[str, str] = {
    "stock": "instruments",
    "index": "instruments_index",
    "etf": "instruments_etf",
}


@runtime_checkable
class StorageProtocol(Protocol):
    """存储门面。实现须保证读回顺序为 ``(symbol, date)`` 升序。"""

    # ── 日K ────────────────────────────────────────────────────
    def read_daily(
        self,
        table: str,
        *,
        symbols: Sequence[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        columns: Sequence[str] | None = None,
        dedupe: DedupeStrategy = "auto",
    ) -> pl.DataFrame:
        """按标的/日期区间读日K；``start``/``end`` 两端闭区间。"""
        ...

    def write_daily(self, table: str, df: pl.DataFrame) -> None:
        """写入日K（同 key 后写覆盖先写）。"""
        ...

    def delete_daily(self, table: str, *, day: date, symbols: Sequence[str]) -> None:
        """删除某日指定标的的行；写入前清理残留用（同步可见）。"""
        ...

    def delete_daily_date(self, table: str, *, day: date) -> None:
        """删除某日整个分区；enriched 分区自愈用（同步可见）。"""
        ...

    def latest_daily_date(self, table: str) -> date | None:
        """该数据集最新交易日；空表返回 None（不返回 1970-01-01）。"""
        ...

    def list_daily_dates(
        self,
        table: str,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> list[date]:
        """该数据集存在数据的交易日（升序）。"""
        ...

    # ── 分钟K ──────────────────────────────────────────────────
    def read_minute(
        self,
        table: str,
        *,
        symbols: Sequence[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        columns: Sequence[str] | None = None,
        dedupe: DedupeStrategy = "auto",
    ) -> pl.DataFrame:
        """按标的/时间区间读分钟K；日期列是 ``datetime``（北京墙钟）。"""
        ...

    def write_minute(self, table: str, df: pl.DataFrame) -> None:
        """写入分钟K。"""
        ...

    def minute_bounds(
        self,
        table: str,
        *,
        symbols: Sequence[str] | None = None,
    ) -> tuple[datetime | None, datetime | None]:
        """返回 (最早, 最新) 分钟K时间；空表返回 (None, None)。"""
        ...

    def list_minute_dates(
        self,
        table: str,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> list[date]:
        """存在的分钟K交易日（升序）。"""
        ...

    # ── 除权因子 ────────────────────────────────────────────────
    def read_adj_factor(
        self,
        *,
        symbols: Sequence[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        columns: Sequence[str] | None = None,
        etf: bool = False,
        dedupe: DedupeStrategy = "auto",
    ) -> pl.DataFrame:
        """除权事件；日期列是 ``trade_date``。"""
        ...

    def write_adj_factor(self, df: pl.DataFrame, *, etf: bool = False) -> None:
        """写入除权事件。"""
        ...

    # ── 标的维表 ────────────────────────────────────────────────
    def read_instruments(self, asset_type: str = "stock") -> pl.DataFrame:
        """读标的维表（stock/index/etf）。"""
        ...

    def write_instruments(self, asset_type: str, df: pl.DataFrame) -> None:
        """写入标的维表（整表快照 upsert）。"""
        ...

    # ── 通用写入（写缓冲统一入口）───────────────────────────────
    def write(self, table: str, df: pl.DataFrame) -> None:
        """按逻辑表名写入任意已知表；表名合法性由实现校验。"""
        ...

    # ── 修补契约（DES-002）─────────────────────────────────────
    def begin_repair(self, dataset: str, day: date, reason: str, actor: str) -> str:
        """创建修补单号；审计/快照共享该 ID。"""
        ...

    def append_repair_snapshot(
        self, repair_id: str, dataset: str, day: date, rows: pl.DataFrame, actor: str
    ) -> None:
        """写入修补前完整旧行。"""
        ...

    def append_repair_audit(
        self, repair_id: str, dataset: str, day: date, diffs: pl.DataFrame, actor: str
    ) -> None:
        """写入修补前后差异行。"""
        ...

    def set_repair_status(self, repair_id: str, status: str, actor: str) -> None:
        """更新修补单状态（started/committed/failed）。"""
        ...

    def read_repair_snapshot(self, repair_id: str) -> pl.DataFrame:
        """读取某次修补的旧行快照。"""
        ...

    def rollback_repair(self, repair_id: str, actor: str) -> None:
        """按快照回滚某次修补。"""
        ...

    def begin_generation(self, asset_type: str, actor: str, changed_from: date | None) -> str:
        """创建 enriched publishing generation。"""
        ...

    def heartbeat_generation(self, asset_type: str, generation_id: str, actor: str) -> None:
        """刷新 generation 心跳。"""
        ...

    def commit_generation(self, asset_type: str, generation_id: str, actor: str) -> None:
        """将 generation 标记为 ready。"""
        ...

    def fail_generation(self, asset_type: str, generation_id: str, actor: str) -> None:
        """将 generation 标记为 failed。"""
        ...

    def current_generation(self, asset_type: str) -> dict:
        """读取当前 ready generation；空表返回空 dict。"""
        ...
