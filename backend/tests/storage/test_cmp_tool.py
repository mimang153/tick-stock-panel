"""双栈对照工具的端到端验证（L2）：CK 写入 → 与 Parquet 比对 → 差异可定位。"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

from app.storage.store import ClickHouseStore

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import cmp_stacks

_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "code": pl.Utf8,
    "exchange": pl.Utf8,
    "region": pl.Utf8,
    "type": pl.Utf8,
    "listing_date": pl.Utf8,
    "total_shares": pl.Float64,
    "float_shares": pl.Float64,
    "tick_size": pl.Float64,
    "limit_up": pl.Float64,
    "limit_down": pl.Float64,
    "as_of": pl.Date,
}


def _instruments_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "name": ["贵州茅台", "平安银行"],
            "code": ["600519", "000001"],
            "exchange": ["SH", "SZ"],
            "region": ["CN", "CN"],
            "type": ["stock", "stock"],
            "listing_date": ["2001-08-27", "1991-04-03"],
            "total_shares": [1.256e9, 1.94e10],
            "float_shares": [1.256e9, 1.94e10],
            "tick_size": [0.01, 0.01],
            "limit_up": [None, 12.0],
            "limit_down": [None, None],
            "as_of": [date(2024, 1, 2)] * 2,
        },
        schema=_SCHEMA,
    )


def _write_parquet(data_dir: Path, df: pl.DataFrame) -> None:
    target = data_dir / "instruments"
    target.mkdir(parents=True, exist_ok=True)
    df.write_parquet(target / "instruments.parquet")


def test_compare_dataset_reports_no_diff_when_stacks_match(
    store: ClickHouseStore,
    tmp_path: Path,
) -> None:
    df = _instruments_frame()
    store.write_instruments(
        "stock", df.with_columns(pl.lit(datetime(2024, 1, 2, 16)).alias("updated_at"))
    )
    _write_parquet(tmp_path, df)

    result = cmp_stacks.compare_dataset("instruments", store, tmp_path)

    assert result["status"] == "ok"
    assert result["ok"] is True
    assert result["parquet_rows"] == 2
    assert result["clickhouse_rows"] == 2


def test_compare_dataset_locates_field_diff(store: ClickHouseStore, tmp_path: Path) -> None:
    df = _instruments_frame()
    store.write_instruments(
        "stock", df.with_columns(pl.lit(datetime(2024, 1, 2, 16)).alias("updated_at"))
    )
    _write_parquet(tmp_path, df.with_columns(pl.lit(99.0).alias("tick_size")))

    result = cmp_stacks.compare_dataset("instruments", store, tmp_path)

    assert result["status"] == "diff"
    assert set(result["column_diffs"]) == {"tick_size"}
    assert result["samples"]["tick_size"][0]["tick_size"] == 99.0


def test_compare_dataset_flags_empty_clickhouse(store: ClickHouseStore, tmp_path: Path) -> None:
    _write_parquet(tmp_path, _instruments_frame())

    result = cmp_stacks.compare_dataset("instruments", store, tmp_path)

    assert result["status"] == "ck_empty"
    assert result["ok"] is False


def test_compare_dataset_skips_missing_parquet(store: ClickHouseStore, tmp_path: Path) -> None:
    result = cmp_stacks.compare_dataset("kline_daily", store, tmp_path)

    assert result["status"] == "no_parquet"
    assert result["ok"] is True
