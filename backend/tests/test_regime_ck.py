"""regime 派生缓存的 CK 在线读源（RC-12）。"""

from __future__ import annotations

from datetime import date

import polars as pl

from app.services import regime_builder


def _rows(dates: list[date], scores: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": dates,
            "state": ["strong" if s >= 50 else "weak" for s in scores],
            "score": scores,
        }
    )


def test_regime_upsert_and_load_via_ck(tmp_path, ck_runtime) -> None:
    """在线路径 upsert/load 只走 CK，本地 parquet 不再产生。"""
    regime_builder.upsert_regime_history(
        tmp_path, _rows([date(2026, 1, 1), date(2026, 1, 2)], [80.0, 20.0])
    )
    loaded = regime_builder.load_regime_history(tmp_path).sort("date")
    assert loaded["score"].to_list() == [80.0, 20.0]
    assert loaded["state"].to_list() == ["strong", "weak"]
    assert not regime_builder.regime_path(tmp_path).exists()


def test_regime_upsert_preserves_unrelated_dates(tmp_path, ck_runtime) -> None:
    """按日期集合覆盖不得误删区间内未重算的历史行。"""
    regime_builder.upsert_regime_history(
        tmp_path,
        _rows([date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)], [1.0, 2.0, 3.0]),
    )
    regime_builder.upsert_regime_history(
        tmp_path, _rows([date(2026, 1, 1), date(2026, 1, 3)], [11.0, 33.0])
    )
    loaded = regime_builder.load_regime_history(tmp_path).sort("date")
    assert loaded["score"].to_list() == [11.0, 2.0, 33.0]


def test_regime_replace_range_removes_absent_days(tmp_path, ck_runtime) -> None:
    """历史修正：范围内不再存在的日期必须从 CK 删除。"""
    regime_builder.upsert_regime_history(
        tmp_path,
        _rows([date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)], [1.0, 2.0, 3.0]),
    )
    regime_builder.replace_regime_range(
        tmp_path, date(2026, 1, 2), date(2026, 1, 3), _rows([date(2026, 1, 2)], [22.0])
    )
    loaded = regime_builder.load_regime_history(tmp_path).sort("date")
    assert loaded["date"].to_list() == [date(2026, 1, 1), date(2026, 1, 2)]
    assert loaded["score"].to_list() == [1.0, 22.0]


def test_enriched_date_set_reads_ck(tmp_path, ck_runtime) -> None:
    """CK-only 下 regime 增量起点不再依赖本地 enriched 分区目录。"""
    from app.tickflow.repository import DataStore, KlineRepository

    repo = KlineRepository(DataStore(tmp_path), storage=ck_runtime)
    ck_runtime.write_daily(
        "kline_daily_enriched",
        pl.DataFrame({"symbol": ["000001"], "date": [date(2026, 1, 2)], "close": [10.0]}),
    )
    assert regime_builder.enriched_date_set(repo) == {date(2026, 1, 2)}
