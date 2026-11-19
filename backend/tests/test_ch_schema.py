"""ClickHouse schema 契约（L1，不需要真 CK）。

守的是"共用实例安全"和"建表口径"两件事：
- 只能建表（IF NOT EXISTS），不得出现 DROP/SYSTEM 等跨库危险语句
- 表名必须经前缀拼接，测试隔离才成立
- 时区/Nullable/projection setting 与设计文档一致
"""

from __future__ import annotations

import re

import polars as pl
import pytest

from app.storage import schema

# 设计文档 §3 表清单（instruments_ext / kline_ext 已废弃，不建表）
EXPECTED_TABLES = {
    "schema_version",
    "kline_daily",
    "kline_daily_enriched",
    "kline_index_daily",
    "kline_index_enriched",
    "kline_etf_daily",
    "kline_etf_enriched",
    "kline_minute",
    "kline_etf_minute",
    "adj_factor",
    "adj_factor_etf",
    "instruments",
    "instruments_index",
    "instruments_etf",
    "depth5",
    "financials_metrics",
    "financials_income",
    "financials_balance_sheet",
    "financials_cash_flow",
    "financials_shares",
    "ext_timeseries",
    "ext_instruments",
    "matrix_cache",
    "market_mainline",
    "regime_history",
    "ck_repair_audit",
    "ck_repair_snapshot",
    "ck_enriched_generation",
    "ck_coord_job_state",
    "ck_coord_cancel_request",
    "ck_coord_limiter_lease",
    "ck_mining_run_audit",
    "backtest_runs",
    "backtest_trades",
    "sync_state",
}


class _FakeClient:
    """只记录语句与写入，不连 CK。"""

    def __init__(self, table_prefix: str = "", recorded_version: int = 0) -> None:
        self.table_prefix = table_prefix
        self.statements: list[str] = []
        self.inserted: list[tuple[str, pl.DataFrame]] = []
        self._recorded_version = recorded_version

    def table(self, name: str) -> str:
        return f"{self.table_prefix}{name}"

    def execute(self, sql: str, parameters=None, *, settings=None) -> None:
        self.statements.append(sql)

    def query(self, sql: str, parameters=None, *, settings=None) -> pl.DataFrame:
        return pl.DataFrame({"version": pl.Series([self._recorded_version], dtype=pl.UInt32)})

    def insert(self, table: str, df: pl.DataFrame) -> None:
        self.inserted.append((table, df))


def _table_statements(prefix: str = "t_ab12cd34_") -> list[str]:
    """只取建表 DDL：build_statements 末尾还会追加联合视图（CREATE OR REPLACE VIEW）。"""
    return [s for s in schema.build_statements(prefix) if s.startswith("CREATE TABLE")]


def _view_statements(prefix: str = "t_ab12cd34_") -> list[str]:
    return [s for s in schema.build_statements(prefix) if s.startswith("CREATE OR REPLACE VIEW")]


def test_schema_covers_design_tables() -> None:
    assert set(schema.TABLE_NAMES) == EXPECTED_TABLES


def test_statements_are_idempotent_and_safe() -> None:
    for statement in _table_statements():
        assert statement.startswith("CREATE TABLE IF NOT EXISTS t_ab12cd34_"), statement
        upper = statement.upper()
        assert "DROP " not in upper
        assert "SYSTEM " not in upper
        assert "CREATE DATABASE" not in upper
    # 联合视图必须可重复执行（CREATE OR REPLACE），且同样不含破坏性语句
    for statement in _view_statements():
        assert statement.startswith("CREATE OR REPLACE VIEW t_ab12cd34_"), statement
        upper = statement.upper()
        assert "DROP " not in upper
        assert "SYSTEM " not in upper
        assert "CREATE DATABASE" not in upper


def test_every_table_name_is_prefixed() -> None:
    statements = _table_statements()

    assert len(statements) == len(schema.TABLE_NAMES)
    for name, statement in zip(schema.TABLE_NAMES, statements, strict=True):
        assert f"t_ab12cd34_{name} (" in statement


def test_projection_rebuild_setting_only_on_wide_tables() -> None:
    statements = dict(zip(schema.TABLE_NAMES, _table_statements(), strict=True))
    setting = "deduplicate_merge_projection_mode = 'rebuild'"

    for name in ("kline_daily", "kline_daily_enriched", "kline_minute", "ext_timeseries"):
        assert setting in statements[name], name
    for name in ("schema_version", "instruments", "matrix_cache", "sync_state"):
        assert setting not in statements[name], name


def test_timezone_and_nullable_contract() -> None:
    statements = dict(zip(schema.TABLE_NAMES, _table_statements(), strict=True))

    # 分钟线是北京墙钟 DateTime64(6)；日线日期是 Date，不能被写成 DateTime
    assert "datetime DateTime64(6, 'Asia/Shanghai')" in statements["kline_minute"]
    assert "datetime DateTime64(6, 'Asia/Shanghai')" in statements["kline_etf_minute"]
    assert "date   Date" in statements["kline_daily"]
    # 涨跌停价缺失必须是 NULL，不能退化成 0
    assert "limit_up Nullable(Float64)" in statements["instruments"]
    assert "sealed_up Nullable(Bool)" in statements["depth5"]
    # 动态列走 Map（§4.1 策略 A）；DDL 里列名对齐会带多空格，按空白归一后比对
    compact = {name: " ".join(sql.split()) for name, sql in statements.items()}
    assert "values Map(String, Float64)" in compact["financials_metrics"]
    assert "raw Map(String, String)" in compact["ext_timeseries"]


def test_no_uint16_or_bare_datetime_columns() -> None:
    """Arrow 读端的旧版 CK 兼容依赖这两条约束（clickhouse.py 的 date32 还原）。

    CK < 25 的 Arrow 把 Date 编成 UInt16、DateTime 编成 UInt32，读端无从区分，
    因此约定：schema 不使用 UInt16（用 UInt32/Int64），时间列一律 DateTime64 而非 DateTime。
    违反约定会让日期读成整数，或把整数误读成日期。
    """
    for name, statement in zip(schema.TABLE_NAMES, _table_statements(), strict=True):
        assert not re.search(r"\bUInt16\b", statement), name
        assert not re.search(r"\bDateTime\b", statement), name  # \b 不会命中 DateTime64


def _ddl_columns(statement: str) -> list[str]:
    """从 DDL 里取出列名（跳过 Map(...) 内部的逗号）。"""
    body: list[str] = []
    started = False
    for line in statement.splitlines():
        if not started:
            started = line.rstrip().endswith("(")
            continue
        if line.startswith(")"):
            break
        body.append(line)

    parts: list[str] = []
    depth, current = 0, ""
    for char in " ".join(body):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
            continue
        current += char
    parts.append(current)
    return [part.strip().split()[0] for part in parts if part.strip()]


def test_read_projection_exists_in_ddl_and_hides_updated_at() -> None:
    """读取投影必须与 DDL 列一致：多一列会漏到 API 响应，少一列直接查错。"""
    statements = dict(zip(schema.TABLE_NAMES, _table_statements(), strict=True))

    for table, columns in schema.TABLE_READ_COLUMNS.items():
        assert table in schema.TABLE_NAMES, table
        ddl_columns = set(_ddl_columns(statements[table]))
        assert set(columns) <= ddl_columns, (table, set(columns) - ddl_columns)
        assert "updated_at" not in columns, table
        assert "updated_at" in ddl_columns, table


def test_ensure_schema_creates_tables_and_records_version() -> None:
    client = _FakeClient(table_prefix="t_ab12cd34_")

    version = schema.ensure_schema(client)  # type: ignore[arg-type]

    assert version == schema.SCHEMA_VERSION
    tables = [s for s in client.statements if s.startswith("CREATE TABLE")]
    views = [s for s in client.statements if s.startswith("CREATE OR REPLACE VIEW")]
    assert len(tables) == len(schema.TABLE_NAMES)
    # 联合视图在 enriched 与 instruments 都建好后追加
    assert len(views) == len(_view_statements())
    # 版本登记只写一行
    assert len(client.inserted) == 1
    table, df = client.inserted[0]
    assert table == "t_ab12cd34_schema_version"
    assert df["version"].to_list() == [schema.SCHEMA_VERSION]
    assert df["note"].to_list() == [schema.SCHEMA_NOTE]


def test_ensure_schema_is_idempotent_when_version_current() -> None:
    client = _FakeClient(recorded_version=schema.SCHEMA_VERSION)

    assert schema.ensure_schema(client) == schema.SCHEMA_VERSION  # type: ignore[arg-type]
    assert client.inserted == []


def test_ensure_schema_refuses_newer_schema() -> None:
    """库比代码新时 fail-closed，避免旧代码写新表结构。"""
    client = _FakeClient(recorded_version=schema.SCHEMA_VERSION + 1)

    with pytest.raises(RuntimeError, match="高于代码支持"):
        schema.ensure_schema(client)  # type: ignore[arg-type]
