"""双栈对照工具的差异判定（L1，不需要真 CK）。"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import cmp_stacks

KEYS = ["symbol", "date"]
VALUES = ["close", "name"]


def _frame(rows: list[tuple[str, date, float, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "date": [r[1] for r in rows],
            "close": [r[2] for r in rows],
            "name": [r[3] for r in rows],
        },
    )


BASE = [
    ("600519.SH", date(2024, 1, 2), 10.0, "贵州茅台"),
    ("000001.SZ", date(2024, 1, 2), 11.0, "平安银行"),
]


def test_identical_frames_have_no_diff() -> None:
    result = cmp_stacks.compare_frames(_frame(BASE), _frame(BASE), keys=KEYS, value_columns=VALUES)

    assert result["ok"] is True
    assert result["only_in_parquet"] == 0
    assert result["only_in_clickhouse"] == 0
    assert result["column_diffs"] == {}


def test_missing_row_is_reported_by_side() -> None:
    result = cmp_stacks.compare_frames(
        _frame(BASE),
        _frame(BASE[:1]),
        keys=KEYS,
        value_columns=VALUES,
    )

    assert result["ok"] is False
    assert result["only_in_parquet"] == 1
    assert result["only_in_clickhouse"] == 0


def test_extra_row_is_reported_by_side() -> None:
    extra = [*BASE, ("300750.SZ", date(2024, 1, 2), 9.0, "宁德时代")]

    result = cmp_stacks.compare_frames(
        _frame(BASE),
        _frame(extra),
        keys=KEYS,
        value_columns=VALUES,
    )

    assert result["only_in_clickhouse"] == 1
    assert result["only_in_parquet"] == 0


def test_numeric_tolerance_and_text_diff() -> None:
    close_ok = cmp_stacks.compare_frames(
        _frame(BASE),
        _frame([(*BASE[0][:2], 10.0 + 1e-12, BASE[0][3]), BASE[1]]),
        keys=KEYS,
        value_columns=["close"],
    )
    assert close_ok["ok"] is True

    text_diff = cmp_stacks.compare_frames(
        _frame(BASE),
        _frame([(*BASE[0][:2], 10.0, "贵州茅台A"), BASE[1]]),
        keys=KEYS,
        value_columns=["name"],
    )
    assert text_diff["ok"] is False
    assert text_diff["column_diffs"] == {"name": 1}
    assert text_diff["samples"]["name"][0]["name"] == "贵州茅台"
    assert text_diff["samples"]["name"][0]["name_right"] == "贵州茅台A"


def test_null_handling() -> None:
    left = pl.DataFrame({"symbol": ["A"], "date": [date(2024, 1, 2)], "close": [None]})
    same_null = pl.DataFrame({"symbol": ["A"], "date": [date(2024, 1, 2)], "close": [None]})
    right_value = pl.DataFrame({"symbol": ["A"], "date": [date(2024, 1, 2)], "close": [0.0]})

    assert (
        cmp_stacks.compare_frames(
            left,
            same_null,
            keys=KEYS,
            value_columns=["close"],
        )["ok"]
        is True
    )
    diff = cmp_stacks.compare_frames(left, right_value, keys=KEYS, value_columns=["close"])
    assert diff["column_diffs"] == {"close": 1}


def test_missing_column_raises() -> None:
    with pytest.raises(ValueError, match="缺少列"):
        cmp_stacks.compare_frames(
            _frame(BASE),
            _frame(BASE).drop("name"),
            keys=KEYS,
            value_columns=VALUES,
        )


def _write_day(root: Path, day: str, rows: list[tuple[str, date, float, str]]) -> None:
    target = root / "kline_daily" / f"date={day}"
    target.mkdir(parents=True, exist_ok=True)
    _frame(rows).write_parquet(target / "part.parquet")


def test_load_parquet_only_reads_requested_partitions(tmp_path: Path) -> None:
    _write_day(tmp_path, "2024-01-02", BASE)
    _write_day(tmp_path, "2024-01-03", [("600519.SH", date(2024, 1, 3), 12.0, "贵州茅台")])
    _write_day(tmp_path, "2024-01-04", [("600519.SH", date(2024, 1, 4), 13.0, "贵州茅台")])

    df = cmp_stacks.load_parquet(
        "kline_daily",
        tmp_path,
        start=date(2024, 1, 3),
        end=date(2024, 1, 3),
    )

    assert df is not None
    assert df["date"].to_list() == [date(2024, 1, 3)]
    assert cmp_stacks.load_parquet("kline_daily", tmp_path / "missing") is None


def test_load_parquet_filters_symbols(tmp_path: Path) -> None:
    _write_day(tmp_path, "2024-01-02", BASE)

    df = cmp_stacks.load_parquet("kline_daily", tmp_path, symbols=["000001.SZ"])

    assert df is not None
    assert df["symbol"].to_list() == ["000001.SZ"]


def test_load_parquet_single_file_layout(tmp_path: Path) -> None:
    target = tmp_path / "adj_factor"
    target.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 3)],
            "ex_factor": [2.1],
            "dividend": [0.0],
            "bonus": [0.0],
            "allot": [0.0],
            "allot_price": [0.0],
            "prev_close": [0.0],
        },
    ).write_parquet(target / "all.parquet")

    df = cmp_stacks.load_parquet("adj_factor", tmp_path, start=date(2024, 1, 1))

    assert df is not None
    assert df["trade_date"].to_list() == [date(2024, 6, 3)]


def test_dataset_registry_covers_daily_and_dimension_tables() -> None:
    assert {"kline_daily", "kline_daily_enriched", "instruments", "adj_factor"} <= set(
        cmp_stacks.DATASETS,
    )
