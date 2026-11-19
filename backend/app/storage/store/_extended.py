from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

import polars as pl

from app.storage.protocol import (
    ADJ_FACTOR_TABLES,
    DAILY_TABLES,
    FINANCIAL_TABLES,
    INSTRUMENT_TABLES,
    MINUTE_TABLES,
)


class ClickHouseExtendedDataMixin:
    """财务、深度五档、扩展时序与矩阵缓存。"""

    # 以下属性/方法由组装类 ClickHouseStore / CoreMixin / MarketMixin 提供
    _client: Any
    _table: Any
    _conditions: Any
    _require: Any
    _read_range: Any
    _expand_maps: Any
    _wide_to_maps: Any
    table_columns: Any
    read_instruments: Any
    write: Any

    # ── 财务（动态列 Map，§4.1 策略 A）──────────────────────────
    def read_financials(
        self,
        table: str,
        *,
        symbols: Sequence[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> pl.DataFrame:
        """读财务并展开成宽表：数值字段来自 ``values``，文本字段来自 ``raw``。"""
        self._require(table, FINANCIAL_TABLES)
        conditions, params = self._conditions("period_end", symbols, start, end)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        df = self._client.query(
            "SELECT symbol, period_end, "
            "mapKeys(values) AS _vk, mapValues(values) AS _vv, "
            "mapKeys(raw) AS _rk, mapValues(raw) AS _rv "
            f"FROM {self._table(table)} FINAL{where} ORDER BY symbol, period_end",
            params,
        )
        return self._expand_maps(df, ("symbol", "period_end"))

    def financials_stats(self, table: str) -> tuple[int, int]:
        """返回 (行数, 标的数)；供数据页/财务状态接口做轻量统计。"""
        self._require(table, FINANCIAL_TABLES)
        df = self._client.query(
            f"SELECT count() AS rows, uniqExact(symbol) AS syms FROM {self._table(table)} FINAL",
        )
        return int(df["rows"][0] or 0), int(df["syms"][0] or 0)

    def table_stats(self, table: str, *, date_column: str = "date") -> dict:
        """数据状态页的行数/日期范围/标的数/交易日数统计（CK 唯一读源）。

        替代原 DuckDB 视图上的 ``SELECT count/min/max/uniq`` 聚合。
        ``date_column`` 供 adj_factor 等以 trade_date 为日期轴的表指定。
        返回键与 api/data._safe_aggregate 保持一致，空表返回空 dict。
        """
        allowed = (
            DAILY_TABLES
            | ADJ_FACTOR_TABLES
            | MINUTE_TABLES
            | FINANCIAL_TABLES
            | frozenset(INSTRUMENT_TABLES.values())
        )
        self._require(table, allowed)
        cols = set(self.table_columns(table))
        # 空表时 min(date) 在 CK 24.8 返回 Nothing 类型，Arrow 不支持：
        # 用 ifNull + 类型化占位把聚合结果钉到 Nullable(Date)/UInt64。
        date_expr = (
            f"ifNull(toDate(min({date_column})), toDate('1970-01-01'))"
            if date_column in cols
            else "toDate('1970-01-01')"
        )
        date_max_expr = (
            f"ifNull(toDate(max({date_column})), toDate('1970-01-01'))"
            if date_column in cols
            else "toDate('1970-01-01')"
        )
        days_expr = f"toUInt64(uniqExact({date_column}))" if date_column in cols else "toUInt64(0)"
        sym_expr = "toUInt64(uniqExact(symbol))" if "symbol" in cols else "toUInt64(0)"
        df = self._client.query(
            f"SELECT count() AS rows, {date_expr} AS earliest, {date_max_expr} AS latest, "
            f"{sym_expr} AS symbols, {days_expr} AS days FROM {self._table(table)} FINAL",
        )
        if df.is_empty() or not df["rows"][0]:
            return {}
        earliest = df["earliest"][0]
        latest = df["latest"][0]
        # 1970 占位表示无日期（空表 or 该表本就没有日期轴）
        if str(earliest).startswith("1970-01-01"):
            earliest = None
        if str(latest).startswith("1970-01-01"):
            latest = None
        return {
            "rows": int(df["rows"][0]),
            "earliest_date": str(earliest) if earliest is not None else None,
            "latest_date": str(latest) if latest is not None else None,
            "symbols_covered": int(df["symbols"][0] or 0),
            "trading_days": int(df["days"][0] or 0),
        }

    def instrument_stats(self, asset_type: str = "stock") -> dict:
        """维表统计（行数/标的数/命名数/最新 as_of）；无 as_of 的指数/ETF 表兼容返回 None。"""
        df = self.read_instruments(asset_type)
        if df.is_empty():
            return {}
        named = 0
        if "name" in df.columns:
            named = int(
                df.filter(
                    pl.col("name").is_not_null()
                    & (pl.col("name").cast(pl.Utf8).str.strip_chars() != "")
                ).height
            )
        latest = None
        # 只有 stock 维表有真实 as_of；index/etf 表该列是 CK Date 默认 1970，
        # 与旧 instruments_index/etf 视图统计口径一致（固定返回 None）。
        if asset_type == "stock" and "as_of" in df.columns and df["as_of"].len():
            v = df["as_of"].max()
            latest = str(v) if v and not str(v).startswith("1970-01-01") else None
        return {
            "rows": df.height,
            "symbols_covered": int(df["symbol"].n_unique()) if "symbol" in df.columns else 0,
            "named": named,
            "latest_as_of": latest,
        }

    def write_financials(self, table: str, df: pl.DataFrame) -> None:
        """宽表 → Map 行写入；数值列进 ``values``，其余进 ``raw``。"""
        self._require(table, FINANCIAL_TABLES)
        keys = ("symbol", "period_end")
        rows = self._wide_to_maps(df, keys)
        self._client.insert_rows(
            self._table(table),
            rows,
            column_names=[*keys, "values", "raw"],
        )

    # ── 五档盘口 ────────────────────────────────────────────────
    def read_depth5(
        self,
        *,
        symbols: Sequence[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> pl.DataFrame:
        return self._read_range(
            "depth5",
            date_column="date",
            symbols=symbols,
            start=start,
            end=end,
            columns=None,
            dedupe="final",
        )

    def write_depth5(self, df: pl.DataFrame) -> None:
        self.write("depth5", df)

    def depth5_has(self, day: date) -> bool:
        """该交易日是否已有五档盘口。用于"是否已定版"判定，主键裁剪成本极低。"""
        df = self._client.query(
            f"SELECT count() AS n FROM {self._table('depth5')} WHERE date = {{day:Date}}",
            {"day": day},
        )
        return int(df["n"][0] or 0) > 0

    # ── 扩展数据（多扩展共用一张表，带 ext_id）──────────────────
    def read_ext_timeseries(
        self,
        ext_id: str,
        *,
        symbols: Sequence[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> pl.DataFrame:
        conditions, params = self._conditions("date", symbols, start, end)
        conditions.insert(0, "ext_id = {ext_id:String}")
        params["ext_id"] = ext_id
        df = self._client.query(
            "SELECT ext_id, symbol, date, time_value, "
            "mapKeys(values) AS _vk, mapValues(values) AS _vv, "
            "mapKeys(raw) AS _rk, mapValues(raw) AS _rv "
            f"FROM {self._table('ext_timeseries')} FINAL WHERE {' AND '.join(conditions)} "
            "ORDER BY symbol, date, time_value",
            params,
        )
        return self._expand_maps(df, ("ext_id", "symbol", "date", "time_value"))

    def list_ext_dates(self, ext_id: str) -> list[date]:
        """扩展时序表已覆盖的日期（升序）。"""
        df = self._client.query(
            f"SELECT DISTINCT date FROM {self._table('ext_timeseries')} FINAL "
            "WHERE ext_id = {ext_id:String} ORDER BY date",
            {"ext_id": ext_id},
        )
        return [d for d in df["date"].to_list() if d is not None]

    def write_ext_timeseries(self, df: pl.DataFrame) -> None:
        keys = ("ext_id", "symbol", "date", "time_value")
        rows = self._wide_to_maps(df, keys)
        self._client.insert_rows(
            self._table("ext_timeseries"),
            rows,
            column_names=[*keys, "values", "raw"],
        )

    def read_ext_instruments(self, ext_id: str) -> pl.DataFrame:
        df = self._client.query(
            "SELECT ext_id, symbol, code, "
            "mapKeys(values) AS _vk, mapValues(values) AS _vv, "
            "mapKeys(raw) AS _rk, mapValues(raw) AS _rv "
            f"FROM {self._table('ext_instruments')} FINAL "
            "WHERE ext_id = {ext_id:String} ORDER BY symbol",
            {"ext_id": ext_id},
        )
        return self._expand_maps(df, ("ext_id", "symbol", "code"))

    def write_ext_instruments(self, df: pl.DataFrame) -> None:
        keys = ("ext_id", "symbol", "code")
        rows = self._wide_to_maps(df, keys)
        self._client.insert_rows(
            self._table("ext_instruments"),
            rows,
            column_names=[*keys, "values", "raw"],
        )

    # ── 回测矩阵缓存 ────────────────────────────────────────────
    def get_matrix_cache(self, key: str) -> bytes | None:
        df = self._client.query(
            f"SELECT payload FROM {self._table('matrix_cache')} FINAL "
            "WHERE key = {key:String} LIMIT 1",
            {"key": key},
        )
        if df.is_empty():
            return None
        # payload 走 base64：Arrow 的 utf8 通道会把二进制当字符串，直接存会损坏字节
        return base64.b64decode(df["payload"][0])

    def list_matrix_cache(
        self,
        prefix: str,
        *,
        manifest_suffix: str = ":manifest",
    ) -> dict[str, tuple[str, int]]:
        """返回 ``{主 key: (manifest 文本, 矩阵本体字节数)}``。

        只读 manifest 行（小 payload），不拉取矩阵本体，避免为了判别命中而下载几百 MB。
        manifest 行内容为 ``{"manifest": <原文本>, "body_size": <字节数>}``。
        """
        df = self._client.query(
            f"SELECT key, payload FROM {self._table('matrix_cache')} FINAL "
            "WHERE key LIKE {p:String}",
            {"p": f"{prefix}%{manifest_suffix}"},
        )
        suffix_len = len(manifest_suffix)
        result: dict[str, tuple[str, int]] = {}
        for row in df.to_dicts():
            try:
                meta = json.loads(base64.b64decode(row["payload"]).decode("utf-8"))
            except (ValueError, json.JSONDecodeError):
                continue
            result[row["key"][:-suffix_len]] = (
                str(meta.get("manifest", "")),
                int(meta.get("body_size", 0) or 0),
            )
        return result

    def put_matrix_cache(self, key: str, payload: bytes) -> None:
        now = datetime.now()
        self.write(
            "matrix_cache",
            pl.DataFrame(
                {
                    "key": [key],
                    "payload": [base64.b64encode(payload).decode("ascii")],
                    "size_bytes": [len(payload)],
                    "created_at": [now],
                    "accessed_at": [now],
                },
                schema={
                    "key": pl.Utf8,
                    "payload": pl.Utf8,
                    "size_bytes": pl.UInt64,
                    "created_at": pl.Datetime("us"),
                    "accessed_at": pl.Datetime("us"),
                },
            ),
        )

    def touch_matrix_cache(self, key: str) -> None:
        """刷新 accessed_at，让 TTL 反映"最近被复用"。

        走服务端 INSERT SELECT：payload 不经网络往返；版本列是 accessed_at，
        新行必然胜出。
        """
        table = self._table("matrix_cache")
        self._client.execute(
            f"INSERT INTO {table} (key, payload, size_bytes, created_at, accessed_at) "
            f"SELECT key, payload, size_bytes, created_at, now64(3) FROM {table} FINAL "
            "WHERE key = {key:String}",
            {"key": key},
        )

    def delete_matrix_cache(self, keys: Sequence[str]) -> None:
        if not keys:
            return
        self._client.execute(
            f"ALTER TABLE {self._table('matrix_cache')} DELETE WHERE key IN {{keys:Array(String)}}",
            {"keys": list(keys)},
            settings={"mutations_sync": 2},
        )

    def matrix_cache_bytes(self) -> int:
        df = self._client.query(
            f"SELECT sum(size_bytes) AS total FROM {self._table('matrix_cache')} FINAL",
        )
        return int(df["total"][0] or 0)
