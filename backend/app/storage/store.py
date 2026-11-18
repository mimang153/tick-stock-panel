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

import base64
import json
import logging
import threading
from collections.abc import Sequence
from datetime import date, datetime

import polars as pl

from app.storage.clickhouse import ClickHouseClient
from app.storage.protocol import (
    ADJ_FACTOR_TABLES,
    DAILY_TABLES,
    FINANCIAL_TABLES,
    INSTRUMENT_TABLES,
    MINUTE_TABLES,
    DedupeStrategy,
)
from app.storage.schema import TABLE_NAMES, TABLE_READ_COLUMNS

logger = logging.getLogger(__name__)

# 全市场大跨度扫描改用 argMax（§4.3：千万级行时 FINAL 代价过高）。
# 有 symbol 过滤时单票区间是连续块，FINAL 更快，保持默认。
ARGMAX_SPAN_DAYS = 180


class ClickHouseStore:
    """``StorageProtocol`` 的 ClickHouse 实现。"""

    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client
        self._columns: dict[str, tuple[str, ...]] = {}
        self._lock = threading.Lock()

    @property
    def client(self) -> ClickHouseClient:
        return self._client

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

    # ── 派生结果：市场主线历史（§13.3①）──────────────────────────
    _MAINLINE_COLUMNS = (
        "date",
        "kind",
        "member",
        "limit_up_count",
        "ge2_count",
        "max_boards",
        "boards_sum",
        "rungs_filled",
        "leader_symbol",
        "score",
        "rank",
    )

    def read_mainline(self) -> pl.DataFrame:
        """读全部主线历史 (FINAL 去重), 无数据返回空帧。"""
        cols = ", ".join(self._MAINLINE_COLUMNS)
        df = self._client.query(
            f"SELECT {cols} FROM {self._table('market_mainline')} FINAL ORDER BY date, kind, rank"
        )
        return df

    def replace_mainline(self, *, kind: str, start: date, end: date, rows: pl.DataFrame) -> None:
        """按 (kind, 日期范围) 覆盖: 先 mutation 删除, 再写新行 (写完即可见)。"""
        self._client.execute(
            f"ALTER TABLE {self._table('market_mainline')} DELETE "
            "WHERE kind = {kind:String} AND date BETWEEN {start:Date} AND {end:Date}",
            {"kind": kind, "start": start, "end": end},
            settings={"mutations_sync": 2},
        )
        if rows.is_empty():
            return
        cols = [c for c in self._MAINLINE_COLUMNS if c in rows.columns]
        self.write("market_mainline", rows.select(cols))

    # ── 回测结果（§3.3）─────────────────────────────────────────
    _RUN_COLUMNS = (
        "run_id",
        "created_at",
        "strategy_id",
        "config_json",
        "stats_json",
        "equity_curve",
        "extra_json",
        "n_trades",
        "elapsed_ms",
        "error",
    )

    def write_backtest_run(self, run: dict) -> None:
        if not run.get("run_id"):
            raise ValueError("run_id 必填")
        columns = [c for c in self._RUN_COLUMNS if c in run]
        schema = {
            "run_id": pl.Utf8,
            "created_at": pl.Datetime("us"),
            "strategy_id": pl.Utf8,
            "config_json": pl.Utf8,
            "stats_json": pl.Utf8,
            "equity_curve": pl.Utf8,
            "extra_json": pl.Utf8,
            "n_trades": pl.UInt32,
            "elapsed_ms": pl.Float64,
            "error": pl.Utf8,
        }
        df = pl.DataFrame({c: [run[c]] for c in columns}, schema={c: schema[c] for c in columns})
        self.write("backtest_runs", df)

    def read_backtest_run(self, run_id: str) -> dict | None:
        df = self._client.query(
            f"SELECT {', '.join(self._RUN_COLUMNS)} FROM {self._table('backtest_runs')} FINAL "
            "WHERE run_id = {run_id:String} LIMIT 1",
            {"run_id": run_id},
        )
        return None if df.is_empty() else df.row(0, named=True)

    def list_backtest_runs(self, *, limit: int = 50, offset: int = 0) -> pl.DataFrame:
        if limit <= 0 or offset < 0:
            raise ValueError("limit 必须为正、offset 不得为负")
        return self._client.query(
            "SELECT run_id, created_at, strategy_id, n_trades, elapsed_ms, error "
            f"FROM {self._table('backtest_runs')} FINAL "
            "ORDER BY created_at DESC, run_id DESC "
            "LIMIT {limit:UInt32} OFFSET {offset:UInt32}",
            {"limit": limit, "offset": offset},
        )

    def write_backtest_trades(self, df: pl.DataFrame) -> None:
        """写入成交明细。

        ``extra`` 是 Map 列，Arrow 无法表达，本批次不写（缺失即空 Map），
        需要时由调用方改用 ``insert_rows``。
        """
        if df.is_empty():
            return
        if "extra" in df.columns:
            logger.warning("backtest_trades.extra 是 Map 列，本批次跳过该列")
            df = df.drop("extra")
        self.write("backtest_trades", df)

    def read_backtest_trades(self, run_id: str) -> pl.DataFrame:
        columns = [c for c in self.table_columns("backtest_trades") if c != "updated_at"]
        return self._client.query(
            f"SELECT {', '.join(columns)} FROM {self._table('backtest_trades')} FINAL "
            "WHERE run_id = {run_id:String} ORDER BY seq",
            {"run_id": run_id},
        )

    # ── 通用写入 ────────────────────────────────────────────────
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
        self._client.insert(self._table(table), df.select(keep))

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
            wide = ClickHouseStore._pivot_map(df, keys, keys_col, values_col)
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
