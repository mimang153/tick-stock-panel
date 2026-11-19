"""ClickHouse 仓储实现（设计文档 §4.2 / §4.3 / §4.10）。

职责边界：

- 把"读写语义"翻译成 SQL，业务层看不到 SQL、看不到表名、看不到去重策略
- 写：同 key 直接 INSERT，由 ``ReplacingMergeTree(updated_at)`` 收敛；
  不再需要"读旧分区再合并"或 ``.tmp`` 原子写（§4.2）
- 读：默认 ``FINAL``；全市场大跨度扫描改用 ``argMax`` 子查询（§4.3 / §4.10），
  两条路径结果必须一致（验收 §4.3 交叉验证）
- 表名与列名都过白名单校验，不做字符串拼接式 SQL 注入
"""

from __future__ import annotations

import json
import threading
from collections.abc import Sequence
from datetime import date, datetime, timedelta

import polars as pl

from app.storage.clickhouse import ClickHouseClient
from app.storage.protocol import INSTRUMENT_TABLES, DedupeStrategy
from app.storage.schema import TABLE_NAMES, TABLE_READ_COLUMNS

from ._common import ARGMAX_SPAN_DAYS, logger


class ClickHouseStoreCoreMixin:
    """连接、底层读写与表/列工具。"""

    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client
        self._columns: dict[str, tuple[str, ...]] = {}
        self._lock = threading.Lock()

    @property
    def client(self) -> ClickHouseClient:
        return self._client

    # ── 通用写入 ────────────────────────────────────────────────
    def _next_updated_at(self) -> datetime:
        with self._lock:
            now = datetime.now()
            value = now.replace(microsecond=now.microsecond // 1000 * 1000)
            last = getattr(self, "_last_updated_at", None)
            if last is not None and value <= last:
                value = last + timedelta(milliseconds=1)
            self._last_updated_at = value
            return value

    def write(self, table: str, df: pl.DataFrame) -> None:
        """写入任意已知表；未知列丢弃并告警（CK 不接受表里没有的列）。"""
        if df.is_empty():
            return
        if table not in TABLE_NAMES:
            raise ValueError(f"未知表 {table!r}")
        known = set(self.table_columns(table))
        keep = [c for c in df.columns if c in known]
        dropped = [c for c in df.columns if c not in known]
        if dropped:
            logger.warning("写入 %s 时丢弃未知列 %s", table, dropped)
        if not keep:
            return
        payload = df.select(keep)
        if "updated_at" in keep and "updated_at" not in df.columns:
            payload = payload.with_columns(pl.lit(self._next_updated_at()).alias("updated_at"))
        self._client.insert(self._table(table), payload)

    def table_columns_typed(self, table: str) -> list[tuple[str, str]]:
        """表的 (列名, CK 类型) 列表，直接 DESCRIBE（不进列名缓存）。

        供 schema 展示 API 返回精确类型，取代原 DuckDB DESCRIBE 视图。
        """
        if table not in TABLE_NAMES:
            raise ValueError(f"未知表 {table!r}")
        df = self._client.query(f"DESCRIBE TABLE {self._table(table)}")
        return list(zip(df["name"].to_list(), df["type"].to_list(), strict=False))

    def table_columns(self, table: str) -> tuple[str, ...]:
        """表的实际列（用于清洗写入列、校验请求列），按表缓存。"""
        if table not in TABLE_NAMES:
            raise ValueError(f"未知表 {table!r}")
        cached = self._columns.get(table)
        if cached is not None:
            return cached
        with self._lock:
            cached = self._columns.get(table)
            if cached is None:
                df = self._client.query(f"DESCRIBE TABLE {self._table(table)}")
                cached = tuple(df["name"].to_list())
                self._columns[table] = cached
        return cached

    # ── 内部 ────────────────────────────────────────────────────
    def _read_range(
        self,
        table: str,
        *,
        date_column: str,
        symbols: Sequence[str] | None,
        start: date | datetime | None,
        end: date | datetime | None,
        columns: Sequence[str] | None,
        dedupe: DedupeStrategy,
        value_type: str = "Date",
    ) -> pl.DataFrame:
        selected = self._selected_columns(table, columns)
        conditions, params = self._conditions(date_column, symbols, start, end, value_type)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        order = f"ORDER BY symbol, {date_column}"
        target = self._table(table)

        strategy = self._resolve_dedupe(dedupe, symbols, start, end)
        if strategy == "argmax" and "updated_at" in self.table_columns(table):
            aggregates = [
                f"argMax({column}, updated_at) AS {column}"
                for column in selected
                if column not in ("symbol", date_column)
            ]
            projection = ", ".join(["symbol", date_column, *aggregates])
            df = self._client.query(
                f"SELECT {projection} FROM {target}{where} GROUP BY symbol, {date_column} {order}",
                params,
            )
            # GROUP BY 的键只用于收敛，调用方没请求就不对外暴露
            return df.select(list(selected))

        sql = f"SELECT {', '.join(selected)} FROM {target} FINAL{where} {order}"
        return self._client.query(sql, params)

    def _selected_columns(self, table: str, columns: Sequence[str] | None) -> list[str]:
        if columns is None:
            return list(TABLE_READ_COLUMNS[table])
        if not columns:
            raise ValueError("columns 不能为空列表；要全列请传 None")
        allowed = set(self.table_columns(table))
        unknown = [c for c in columns if c not in allowed]
        if unknown:
            raise ValueError(f"{table} 不存在列 {unknown}")
        return list(columns)

    @staticmethod
    def _conditions(
        date_column: str,
        symbols: Sequence[str] | None,
        start: date | None,
        end: date | None,
        value_type: str = "Date",
    ) -> tuple[list[str], dict]:
        conditions: list[str] = []
        params: dict = {}
        if symbols is not None:
            if not symbols:
                # 空标的集合语义是"没有数据"，不能退化成全表扫描
                return ["0"], {}
            conditions.append("symbol IN {symbols:Array(String)}")
            params["symbols"] = list(symbols)
        if start is not None:
            conditions.append(f"{date_column} >= {{start:{value_type}}}")
            params["start"] = start
        if end is not None:
            conditions.append(f"{date_column} <= {{end:{value_type}}}")
            params["end"] = end
        return conditions, params

    # ── Map 展开 / 收拢 ─────────────────────────────────────────
    @staticmethod
    def _expand_maps(df: pl.DataFrame, key_columns: Sequence[str]) -> pl.DataFrame:
        """把 ``values``/``raw`` 两个 Map 展开成宽表列（列名有序，便于快照比对）。"""
        keys = list(key_columns)
        if df.is_empty():
            return df.select(keys)
        base = df.select(keys)
        for keys_col, values_col in (("_vk", "_vv"), ("_rk", "_rv")):
            wide = ClickHouseStoreCoreMixin._pivot_map(df, keys, keys_col, values_col)
            if wide is None:
                continue
            overlap = [c for c in wide.columns if c in base.columns and c not in keys]
            if overlap:
                wide = wide.drop(overlap)  # 数值列优先，文本列让位
            base = base.join(wide, on=keys, how="left")
        return base.sort(keys)

    @staticmethod
    def _pivot_map(
        df: pl.DataFrame,
        keys: Sequence[str],
        keys_col: str,
        values_col: str,
    ) -> pl.DataFrame | None:
        long = (
            df.select(
                pl.col(keys),
                pl.col(keys_col).alias("_key"),
                pl.col(values_col).alias("_val"),
            )
            .explode(["_key", "_val"], empty_as_null=True)
            .filter(pl.col("_key").is_not_null())
        )
        if long.is_empty():
            return None
        wide = long.pivot(index=list(keys), on="_key", values="_val", aggregate_function="first")
        fields = sorted(c for c in wide.columns if c not in keys)
        return wide.select([*keys, *fields])

    def _wide_to_maps(self, df: pl.DataFrame, key_columns: Sequence[str]) -> list[list]:
        """宽表 → CK Map 行：数值列进 ``values``，其余（文本/日期/布尔）转字符串进 ``raw``。"""
        keys = list(key_columns)
        missing = [c for c in keys if c not in df.columns]
        if missing:
            raise ValueError(f"缺少键列 {missing}")
        value_columns: list[str] = []
        raw_columns: list[str] = []
        for name, dtype in df.schema.items():
            if name in keys or name == "updated_at":
                continue
            (value_columns if dtype.is_numeric() else raw_columns).append(name)

        rows: list[list] = []
        for row in df.iter_rows(named=True):
            values = {c: float(row[c]) for c in value_columns if row[c] is not None}
            raw = {c: str(row[c]) for c in raw_columns if row[c] is not None}
            rows.append([row[k] for k in keys] + [values, raw])
        return rows

    # ── 修补契约（DES-002）─────────────────────────────────────
    @staticmethod
    def _row_json(row: dict) -> str:
        return json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)

    @staticmethod
    def _resolve_dedupe(
        dedupe: DedupeStrategy,
        symbols: Sequence[str] | None,
        start: date | None,
        end: date | None,
    ) -> str:
        if dedupe != "auto":
            return dedupe
        if symbols is not None:
            return "final"
        span = (end - start).days if start is not None and end is not None else None
        if span is None or span > ARGMAX_SPAN_DAYS:
            return "argmax"
        return "final"

    def _instrument_table(self, asset_type: str) -> str:
        try:
            return INSTRUMENT_TABLES[asset_type]
        except KeyError as exc:
            raise ValueError(f"未知资产类型 {asset_type!r}") from exc

    def _require(self, table: str, allowed: frozenset[str]) -> None:
        if table not in allowed:
            raise ValueError(f"{table!r} 不是该接口允许的表：{sorted(allowed)}")

    def _table(self, name: str) -> str:
        if name not in TABLE_NAMES:
            raise ValueError(f"未知表 {name!r}")
        return self._client.table(name)
