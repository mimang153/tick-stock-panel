from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from ._common import logger


class ClickHouseMetadataMixin:
    """主线、regime 与回测记录。"""

    # 以下属性/方法由组装类 ClickHouseStore / CoreMixin 提供
    _client: Any
    _table: Any
    write: Any
    table_columns: Any

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

    # ── 市场环境时序（RC-12）─────────────────────────────────────
    _REGIME_COLUMNS = ("date", "payload_json")

    def read_regime(self) -> pl.DataFrame:
        """整行 payload 按 date 去重读回，供 regime 在线路径消费。"""
        cols = ", ".join(self._REGIME_COLUMNS)
        return self._client.query(
            f"SELECT {cols} FROM {self._table('regime_history')} FINAL ORDER BY date"
        )

    def replace_regime(self, *, start: date, end: date, rows: pl.DataFrame) -> None:
        """按日期范围覆盖 regime 时序：先同步 mutation 删除，再写新行。"""
        self._client.execute(
            f"ALTER TABLE {self._table('regime_history')} DELETE "
            "WHERE date BETWEEN {start:Date} AND {end:Date}",
            {"start": start, "end": end},
            settings={"mutations_sync": 2},
        )
        if rows.is_empty():
            return
        cols = [c for c in self._REGIME_COLUMNS if c in rows.columns]
        self.write("regime_history", rows.select(cols))

    def upsert_regime(self, rows: pl.DataFrame) -> None:
        """按日期集合覆盖：只删待写日期，保留区间内未重算的历史行。"""
        if rows.is_empty():
            return
        days = rows["date"].unique().to_list()
        self._client.execute(
            f"ALTER TABLE {self._table('regime_history')} DELETE "
            "WHERE date IN {days:Array(Date)}",
            {"days": days},
            settings={"mutations_sync": 2},
        )
        cols = [c for c in self._REGIME_COLUMNS if c in rows.columns]
        self.write("regime_history", rows.select(cols))

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
