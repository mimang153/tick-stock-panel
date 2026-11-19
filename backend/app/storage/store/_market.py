from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

import polars as pl

from app.storage.protocol import ADJ_FACTOR_TABLES, DAILY_TABLES, MINUTE_TABLES, DedupeStrategy
from app.storage.schema import TABLE_READ_COLUMNS


class ClickHouseMarketDataMixin:
    """日K/复权/乐器/分钟/screener 数据读写。"""

    # 以下属性/方法由组装类 ClickHouseStore / CoreMixin 提供
    _client: Any
    _table: Any
    _conditions: Any
    _instrument_table: Any
    _read_range: Any
    _require: Any
    write: Any

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
        self._require(table, DAILY_TABLES)
        return self._read_range(
            table,
            date_column="date",
            symbols=symbols,
            start=start,
            end=end,
            columns=columns,
            dedupe=dedupe,
        )

    def write_daily(self, table: str, df: pl.DataFrame) -> None:
        self._require(table, DAILY_TABLES)
        self.write(table, df)

    def delete_daily(self, table: str, *, day: date, symbols: Sequence[str]) -> None:
        """删除某日指定标的的行。

        用 ``ALTER TABLE ... DELETE`` + ``mutations_sync=2``：lightweight DELETE 不保证
        写完即可见，而修补流程紧接着要重新写入并读回（验收 §4.4）。
        """
        self._require(table, DAILY_TABLES)
        targets = list(symbols)
        if not targets:
            return
        self._client.execute(
            f"ALTER TABLE {self._table(table)} DELETE "
            "WHERE date = {day:Date} AND symbol IN {symbols:Array(String)}",
            {"day": day, "symbols": targets},
            settings={"mutations_sync": 2},
        )

    def delete_daily_date(self, table: str, *, day: date) -> None:
        """删除某日整个分区（覆盖不全/收盘价过期分区的自愈删除）。"""
        self._require(table, DAILY_TABLES)
        self._client.execute(
            f"ALTER TABLE {self._table(table)} DELETE WHERE date = {{day:Date}}",
            {"day": day},
            settings={"mutations_sync": 2},
        )

    def latest_daily_date(self, table: str) -> date | None:
        self._require(table, DAILY_TABLES)
        df = self._client.query(
            f"SELECT max(date) AS latest, count() AS n FROM {self._table(table)}",
        )
        if df["n"][0] == 0:
            return None
        return df["latest"][0]

    def list_daily_dates(
        self,
        table: str,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> list[date]:
        """该数据集存在数据的交易日（升序）。

        供增量管道判断"日K有、enriched 没有"的日期集合，替代枚举 Parquet
        ``date=`` 目录。
        """
        self._require(table, DAILY_TABLES)
        conditions, params = self._conditions("date", None, start, end)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        df = self._client.query(
            f"SELECT DISTINCT date FROM {self._table(table)}{where} ORDER BY date",
            params,
        )
        return df["date"].to_list()

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
        table = "adj_factor_etf" if etf else "adj_factor"
        self._require(table, ADJ_FACTOR_TABLES)
        return self._read_range(
            table,
            date_column="trade_date",
            symbols=symbols,
            start=start,
            end=end,
            columns=columns,
            dedupe=dedupe,
        )

    def write_adj_factor(self, df: pl.DataFrame, *, etf: bool = False) -> None:
        self.write("adj_factor_etf" if etf else "adj_factor", df)

    # ── 标的维表 ────────────────────────────────────────────────
    def read_instruments(self, asset_type: str = "stock") -> pl.DataFrame:
        table = self._instrument_table(asset_type)
        columns = ", ".join(TABLE_READ_COLUMNS[table])
        return self._client.query(
            f"SELECT {columns} FROM {self._table(table)} FINAL ORDER BY symbol",
        )

    def write_instruments(self, asset_type: str, df: pl.DataFrame) -> None:
        self.write(self._instrument_table(asset_type), df)

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
        self._require(table, MINUTE_TABLES)
        return self._read_range(
            table,
            date_column="datetime",
            symbols=symbols,
            start=start,
            end=end,
            columns=columns,
            dedupe=dedupe,
            value_type="DateTime64(6)",
        )

    def write_minute(self, table: str, df: pl.DataFrame) -> None:
        self._require(table, MINUTE_TABLES)
        self.write(table, df)

    def latest_minute_datetime(self, table: str) -> datetime | None:
        self._require(table, MINUTE_TABLES)
        df = self._client.query(
            f"SELECT max(datetime) AS latest, count() AS n FROM {self._table(table)}",
        )
        if df["n"][0] == 0:
            return None
        return df["latest"][0]

    def minute_bounds(
        self,
        table: str,
        *,
        symbols: Sequence[str] | None = None,
    ) -> tuple[datetime | None, datetime | None]:
        """返回 (最早, 最新) 分钟K时间；空表返回 (None, None)。

        替代对 Parquet ``date=`` 目录的枚举与 DuckDB ``min/max(CAST(datetime))``
        查询（分钟读写切 CK 后目录不再是事实来源）。
        """
        self._require(table, MINUTE_TABLES)
        conditions, params = self._conditions("datetime", symbols, None, None, "DateTime64(6)")
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        df = self._client.query(
            "SELECT min(datetime) AS lo, max(datetime) AS hi, count() AS n "
            f"FROM {self._table(table)}{where}",
            params,
        )
        if df["n"][0] == 0:
            return None, None
        return df["lo"][0], df["hi"][0]

    def list_minute_dates(
        self,
        table: str,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> list[date]:
        """存在的分钟K交易日（升序）。"""
        self._require(table, MINUTE_TABLES)
        conditions, params = self._conditions("datetime", None, None, None, "DateTime64(6)")
        if start is not None:
            conditions.append("toDate(datetime) >= {dstart:Date}")
            params["dstart"] = start
        if end is not None:
            conditions.append("toDate(datetime) <= {dend:Date}")
            params["dend"] = end
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        df = self._client.query(
            f"SELECT DISTINCT toDate(datetime) AS d FROM {self._table(table)}{where} ORDER BY d",
            params,
        )
        return df["d"].to_list()

    # ── 联合视图（T3.7）────────────────────────────────────────
    def screener_base(
        self,
        day: date,
        *,
        symbols: Sequence[str] | None = None,
    ) -> pl.DataFrame:
        """单日 enriched ⋈ 标的维表宽表（CK 视图 ``v_screener_base``）。

        省掉调用方从两表各读一次再内存 join；维表缺失时字段为 null，不丢行。
        """
        conditions = ["e.date = {day:Date}"]
        params: dict[str, object] = {"day": day}
        if symbols is not None:
            conditions.append("e.symbol IN {syms:Array(String)}")
            params["syms"] = list(symbols)
        return self._client.query(
            f"SELECT * FROM {self._client.table('v_screener_base')} AS e "
            f"WHERE {' AND '.join(conditions)}",
            params,
        )
