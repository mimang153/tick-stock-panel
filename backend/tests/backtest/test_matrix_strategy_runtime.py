"""矩阵策略/缓存运行时测试 (从 test_matrix_strategy.py 拆分, 断言语义完全保留)。"""

from __future__ import annotations

import gc
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest

from app.backtest import matrix as matrix_module
from app.backtest.matrix import (
    MatrixPipelineConfig,
    MatrixStrategyPipeline,
    RealtimeMarketDataMatrix,
    apply_time_masks,
    build_market_data_matrix,
    build_matrix_score,
    load_market_data_matrix_from_parquet,
    make_signal_matrix,
    slice_signal_matrix,
    validate_signal_matrix,
)
from app.backtest.strategy import build_matrix_cache_profile
from app.strategy.engine import StrategyDataContext, StrategyEngine

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def _migrated_signal_definitions(monkeypatch):
    """注入迁移自内置的信号定义 (信号列定义驱动, 不依赖运行目录)。"""
    from tests.signal_seeds import install_pipeline_caches

    install_pipeline_caches(monkeypatch)


def test_managed_source_generation_skips_file_walk_and_invalidates_explicitly(tmp_path):
    market_root = tmp_path / "kline_daily_enriched"
    current = date(2024, 3, 5)
    partition = market_root / f"date={current.isoformat()}"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "date": [current],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "volume": [1_000.0],
        }
    ).write_parquet(partition / "part.parquet")
    cache_root = tmp_path / "matrix_cache"

    first = load_market_data_matrix_from_parquet(
        market_root,
        current,
        current,
        field_columns=set(),
        cache_root=cache_root,
        source_generation="generation-a",
    )
    repeated = load_market_data_matrix_from_parquet(
        market_root,
        current,
        current,
        field_columns=set(),
        cache_root=cache_root,
        source_generation="generation-a",
    )
    changed = load_market_data_matrix_from_parquet(
        market_root,
        current,
        current,
        field_columns=set(),
        cache_root=cache_root,
        source_generation="generation-b",
    )

    assert first.cache_status == "built"
    assert repeated.cache_status == "exact"
    assert repeated.cache_path == first.cache_path
    assert changed.cache_status == "built"
    assert changed.cache_path != first.cache_path
    del first, repeated
    gc.collect()
    assert len(list(cache_root.glob("v*-*"))) == 1


def test_registered_builtin_matrix_strategies_share_one_cache_profile():
    engine = StrategyEngine(
        strategy_dirs=[REPO_ROOT / "backend" / "tests" / "fixtures" / "strategies"]
    )
    profile = build_matrix_cache_profile(engine, "stock")
    strategies = tuple(
        s for s in engine.strategy_definitions() if s.execution_backend != "minute_filter"
    )

    # 25 个 matrix 策略夹具 (研究模板在 research 目录, 不在夹具目录)
    assert len(strategies) == 25
    assert all(strategy.execution_backend == "matrix_native" for strategy in strategies)
    assert profile.warmup_bars > 0
    assert profile.forward_bars == max(int(strategy.max_hold_days or 0) for strategy in strategies)
    assert {"open", "high", "low", "close", "volume"}.issubset(profile.field_columns)


def test_chunked_matrix_score_matches_previous_full_matrix_formula():
    row_count = 4
    asset_count = 600
    symbols = [f"{asset_id:06d}.SZ" for asset_id in range(asset_count)]
    rows = []
    rng = np.random.default_rng(20260715)
    for time_id in range(row_count):
        for asset_id, symbol in enumerate(symbols):
            rows.append(
                {
                    "symbol": symbol,
                    "date": date(2024, 1, 1) + timedelta(days=time_id),
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "volume": 1_000.0,
                    "feature_a": float(rng.normal()),
                    "feature_b": float((asset_id % 7) - 3),
                }
            )
    market = build_market_data_matrix(
        pl.DataFrame(rows),
        field_columns={"feature_a", "feature_b"},
    )
    universe = rng.random(market.shape) > 0.2
    weights = {"feature_a": 0.75, "feature_b": 0.25}

    expected = np.zeros(market.shape, dtype=np.float32)
    all_finite = universe.copy()
    total_weight = sum(weights.values())
    for name, weight in weights.items():
        values = market.field(name)
        finite = universe & np.isfinite(values)
        all_finite &= np.isfinite(values)
        row_min = np.min(np.where(finite, values, np.inf), axis=1)
        row_max = np.max(np.where(finite, values, -np.inf), axis=1)
        row_range = row_max - row_min
        normalized = np.zeros(market.shape, dtype=np.float32)
        varying = finite & np.isfinite(row_range[:, None]) & (row_range[:, None] > 0)
        equal = finite & ~varying
        np.divide(
            values - row_min[:, None],
            row_range[:, None],
            out=normalized,
            where=varying,
        )
        normalized[equal] = np.float32(0.5)
        expected += normalized * np.float32(weight / total_weight)
    expected *= np.float32(100.0)
    expected[~universe | ~all_finite] = 0.0

    actual = build_matrix_score(
        market,
        universe,
        weights,
        "score",
        True,
        fallback=np.zeros(market.shape, dtype=np.float32),
    )
    np.testing.assert_array_equal(actual, expected)

    inverted = build_matrix_score(
        market,
        universe,
        weights,
        "score",
        True,
        fallback=np.zeros(market.shape, dtype=np.float32),
        directions={"feature_a": "low", "feature_b": "low"},
    )
    np.testing.assert_allclose(
        inverted[universe],
        np.float32(100.0) - expected[universe],
        rtol=1e-6,
        atol=1e-6,
    )


def test_signal_slice_is_zero_copy_and_masking_only_allocates_final_flags():
    entry = np.ones((5, 2), dtype=np.uint8)
    codes = np.arange(10, dtype=np.int16).reshape(5, 2)
    score = np.arange(10, dtype=np.float32).reshape(5, 2)
    signals = make_signal_matrix(
        (5, 2),
        entry=entry,
        exit=entry,
        score=score,
        entry_signal_code=codes,
        exit_signal_code=codes,
    )
    sliced = slice_signal_matrix(signals, 1, 4)
    assert np.shares_memory(sliced.entry, signals.entry)
    assert np.shares_memory(sliced.score, signals.score)

    masked = apply_time_masks(
        sliced,
        np.array([True, False, True]),
        np.array([False, True, True]),
    )
    assert np.shares_memory(masked.score, signals.score)
    assert masked.entry.tolist() == [[1, 1], [0, 0], [1, 1]]
    assert masked.exit.tolist() == [[0, 0], [1, 1], [1, 1]]
    assert masked.entry_signal_code[1].tolist() == [-1, -1]
    assert masked.exit_signal_code[0].tolist() == [-1, -1]
    assert not masked.entry.flags.writeable
    assert signals.entry.tolist() == [[1, 1]] * 5


def test_matrix_pipeline_applies_basic_filter_and_candidate_scoring():
    panel = pl.DataFrame(
        {
            "symbol": ["000001.SZ", "600000.SH"],
            "name": ["A", "B"],
            "date": [date(2024, 1, 1)] * 2,
            "open": [10.0, 20.0],
            "high": [10.0, 20.0],
            "low": [10.0, 20.0],
            "close": [10.0, 20.0],
            "volume": [100.0, 100.0],
            "amount": [50.0, 500.0],
        }
    )
    market = build_market_data_matrix(panel, field_columns={"amount"})

    class AllEntries:
        def required_fields(self):
            return frozenset({"close"})

        def required_warmup_bars(self, params):
            return 1

        def compute_signals(self, market, params):
            return make_signal_matrix(market.shape, entry=np.ones(market.shape, dtype=np.uint8))

    signals = MatrixStrategyPipeline().run(
        AllEntries(),
        market,
        {},
        MatrixPipelineConfig(
            basic_filter={"enabled": True, "amount_min": 100.0},
            scoring={"close": 1.0},
            order_by="score",
            descending=True,
        ),
    )

    assert signals.entry.tolist() == [[0, 1]]
    assert signals.score.tolist() == [[0.0, 50.0]]


def test_signal_matrix_validation_rejects_mutable_strategy_output():
    signals = make_signal_matrix((2, 1))
    mutable_entry = signals.entry.copy()
    invalid = type(signals)(
        entry=mutable_entry,
        exit=signals.exit,
        score=signals.score,
        entry_signal_code=signals.entry_signal_code,
        exit_signal_code=signals.exit_signal_code,
    )
    with pytest.raises(ValueError, match="read-only"):
        validate_signal_matrix(invalid, (2, 1))


def test_matrix_pipeline_applies_asset_pool_before_cross_sectional_scoring():
    panel = pl.DataFrame(
        {
            "symbol": ["000001.SZ", "600000.SH"],
            "name": ["A", "B"],
            "date": [date(2024, 1, 1)] * 2,
            "open": [10.0, 20.0],
            "high": [10.0, 20.0],
            "low": [10.0, 20.0],
            "close": [10.0, 20.0],
            "volume": [100.0, 100.0],
        }
    )
    market = build_market_data_matrix(panel)

    class AllEntries:
        def required_fields(self):
            return frozenset({"close"})

        def required_warmup_bars(self, params):
            return 1

        def compute_signals(self, market, params):
            return make_signal_matrix(market.shape, entry=np.ones(market.shape, dtype=np.uint8))

    signals = MatrixStrategyPipeline().run(
        AllEntries(),
        market,
        {},
        MatrixPipelineConfig(
            basic_filter={"enabled": False},
            scoring={"close": 1.0},
            order_by="score",
            descending=True,
            asset_mask=np.array([False, True]),
        ),
    )

    assert signals.entry.tolist() == [[0, 1]]
    assert signals.score.tolist() == [[0.0, 50.0]]


def test_realtime_market_matrix_overwrites_last_row_and_appends_new_bar():
    panel = pl.DataFrame(
        {
            "symbol": ["000001.SZ", "600000.SH"] * 2,
            "name": ["A", "B"] * 2,
            "date": [date(2024, 1, 1)] * 2 + [date(2024, 1, 2)] * 2,
            "open": [10.0, 20.0, 10.5, 20.5],
            "high": [10.2, 20.2, 10.7, 20.7],
            "low": [9.8, 19.8, 10.3, 20.3],
            "close": [10.1, 20.1, 10.6, 20.6],
            "volume": [1_000.0, 2_000.0, 1_100.0, 2_100.0],
            "amount": [10_100.0, 40_200.0, 11_660.0, 43_260.0],
        }
    )
    buffer = RealtimeMarketDataMatrix(panel, field_columns={"amount"})

    same_bar = panel.filter(pl.col("date") == date(2024, 1, 2)).with_columns(
        (pl.col("close") + 5.0).alias("close")
    )
    buffer.update(same_bar)
    snapshot = buffer.snapshot()
    assert snapshot.shape == (2, 2)
    assert snapshot.close[-1].tolist() == pytest.approx(same_bar.sort("symbol")["close"].to_list())
    assert buffer.build_count == 1
    assert buffer.update_count == 1
    assert not snapshot.close.flags.writeable

    next_bar = same_bar.with_columns(pl.lit(date(2024, 1, 3)).alias("date"))
    buffer.update(next_bar)
    assert buffer.snapshot().shape == (3, 2)
    assert buffer.build_count == 1
    assert buffer.update_count == 2


def test_strategy_engine_runs_matrix_strategy_without_legacy_filter():
    start = date(2024, 1, 1)
    rows = []
    for offset in range(65):
        values = (
            ("000001.SZ", 10.0 + offset * 0.05),
            ("600000.SH", 20.0 + offset * 0.03),
        )
        for symbol, close in values:
            rows.append(
                {
                    "symbol": symbol,
                    "name": symbol,
                    "date": start + timedelta(days=offset),
                    "open": close,
                    "high": close * 1.01,
                    "low": close * 0.99,
                    "close": close,
                    "volume": 1_000_000.0,
                    "amount": 100_000_000.0,
                    "total_shares": 1_000_000_000.0,
                    "float_shares": 800_000_000.0,
                }
            )
    history = pl.DataFrame(rows)
    target = start + timedelta(days=64)
    engine = StrategyEngine(
        strategy_dirs=[REPO_ROOT / "backend" / "tests" / "fixtures" / "strategies"],
    )
    strategy = engine.get("macd_golden")
    assert strategy.filter_fn is None

    result = engine.run(
        "macd_golden",
        StrategyDataContext(
            asset_type="stock",
            timeframe="1d",
            as_of=target,
            current=history.filter(pl.col("date") == target),
            history=history,
        ),
        pool=["600000.SH"],
        params={"require_macd_golden": False, "use_volume_filter": False},
        overrides={"basic_filter": {"enabled": False}},
    )

    assert [row["symbol"] for row in result.rows] == ["600000.SH"]
    assert result.total == 1


def test_strategy_engine_run_all_builds_one_shared_matrix():
    start = date(2024, 1, 1)
    rows = []
    for offset in range(65):
        close = 10.0 + offset * 0.05
        rows.append(
            {
                "symbol": "000001.SZ",
                "name": "A",
                "date": start + timedelta(days=offset),
                "open": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 1_000_000.0,
                "amount": 100_000_000.0,
                "total_shares": 1_000_000_000.0,
                "float_shares": 800_000_000.0,
            }
        )
    history = pl.DataFrame(rows)
    target = start + timedelta(days=64)
    engine = StrategyEngine(
        strategy_dirs=[REPO_ROOT / "backend" / "tests" / "fixtures" / "strategies"],
    )
    original = engine.get("macd_golden")
    engine._strategies["macd_copy"] = replace(
        original,
        meta={**original.meta, "id": "macd_copy"},
    )
    params = {"require_macd_golden": False, "use_volume_filter": False}
    overrides = {"basic_filter": {"enabled": False}}

    with patch(
        "app.backtest.matrix.build_market_data_matrix",
        wraps=matrix_module.build_market_data_matrix,
    ) as build:
        results = engine.run_all(
            StrategyDataContext(
                asset_type="stock",
                timeframe="1d",
                as_of=target,
                current=history.filter(pl.col("date") == target),
                history=history,
            ),
            strategy_ids=["macd_golden", "macd_copy"],
            params_map={"macd_golden": params, "macd_copy": params},
            overrides_map={"macd_golden": overrides, "macd_copy": overrides},
        )

    assert build.call_count == 1
    assert results["macd_golden"].total == 1
    assert results["macd_copy"].total == 1
