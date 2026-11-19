"""指数资产路由 — repository 层测试。"""

import datetime as _dt

import polars as pl
import pytest

from app.tickflow.repository import DataStore, KlineRepository


@pytest.fixture()
def repo(tmp_path, ck_runtime):
    return KlineRepository(DataStore(tmp_path), storage=ck_runtime)


def _write_index_instruments(repo, rows):
    repo.save_index_instruments(pl.DataFrame(rows))


def test_name_map_includes_index(repo):
    _write_index_instruments(
        repo,
        {
            "symbol": ["000001.SH"],
            "name": ["上证指数"],
            "code": ["000001"],
            "asset_type": ["index"],
        },
    )
    names = repo.get_name_map(["000001.SH", "600000.SH"])
    assert names.get("000001.SH") == "上证指数"
    assert "600000.SH" not in names  # 未收录不造名


def test_name_map_stock_beats_index(repo):
    """同名 symbol 同时出现在股票/指数维表时, 股票名称优先。"""
    _write_index_instruments(
        repo,
        {
            "symbol": ["600000.SH"],
            "name": ["某指数"],
            "code": ["600000"],
            "asset_type": ["index"],
        },
    )
    repo.save_instruments(
        pl.DataFrame(
            {
                "symbol": ["600000.SH"],
                "name": ["浦发银行"],
                "code": ["600000"],
                "exchange": ["SH"],
                "region": ["CN"],
                "type": ["stock"],
                "listing_date": [None],
                "total_shares": [None],
                "float_shares": [None],
                "tick_size": [None],
                "limit_up": [None],
                "limit_down": [None],
                "as_of": [_dt.date(2026, 7, 25)],
            }
        )
    )
    assert repo.get_name_map(["600000.SH"]).get("600000.SH") == "浦发银行"


def _write_stock_instruments(repo, symbols, names):
    repo.save_instruments(
        pl.DataFrame(
            {
                "symbol": symbols,
                "name": names,
                "code": [s[:6] for s in symbols],
                "exchange": ["SH"] * len(symbols),
                "region": ["CN"] * len(symbols),
                "type": ["stock"] * len(symbols),
                "listing_date": [None] * len(symbols),
                "total_shares": [None] * len(symbols),
                "float_shares": [None] * len(symbols),
                "tick_size": [None] * len(symbols),
                "limit_up": [None] * len(symbols),
                "limit_down": [None] * len(symbols),
                "as_of": [_dt.date(2026, 8, 14)] * len(symbols),
            }
        )
    )


def test_name_map_partial_query_does_not_poison_cache(repo):
    """带 symbols 的部分查询不能把残缺映射写入缓存 (自选新加股票无名称的回归).

    旧 bug: 首次 get_name_map(["600000.SH"]) 把只含 600000 的映射缓存住,
    之后自选加入 000001.SZ 再查名称命中残缺缓存 → name=None。
    """
    _write_stock_instruments(repo, ["600000.SH", "000001.SZ"], ["浦发银行", "平安银行"])
    first = repo.get_name_map(["600000.SH"])
    assert first == {"600000.SH": "浦发银行"}
    # 缓存必须是全量: 后续其他 symbols 查询仍能命中
    second = repo.get_name_map(["000001.SZ"])
    assert second == {"000001.SZ": "平安银行"}
    full = repo.get_name_map()
    assert full == {"600000.SH": "浦发银行", "000001.SZ": "平安银行"}


def test_name_map_cache_invalidated_on_instruments_refresh(repo):
    """维表刷新后缓存必须失效: 新收录的股票能立刻查到名称。"""
    _write_stock_instruments(repo, ["600000.SH"], ["浦发银行"])
    assert repo.get_name_map(["600000.SH"]) == {"600000.SH": "浦发银行"}
    _write_stock_instruments(repo, ["600000.SH", "301999.SZ"], ["浦发银行", "新股股份"])
    assert repo.get_name_map(["301999.SZ"]) == {"301999.SZ": "新股股份"}


def _write_index_enriched(repo, dates_rows):
    for rows in dates_rows.values():
        repo.storage.write_daily("kline_index_enriched", pl.DataFrame(rows))


def _index_rows(ds, close=3000.0):
    return [
        {
            "symbol": "000001.SH",
            "date": _dt.date.fromisoformat(ds),
            "open": close - 10,
            "high": close + 20,
            "low": close - 20,
            "close": close,
            "volume": 1_000_000,
            "amount": 1e9,
        }
    ]


def test_get_enriched_latest_asset_index(repo):
    _write_index_enriched(
        repo,
        {
            "2026-07-23": _index_rows("2026-07-23", 2990.0),
            "2026-07-24": _index_rows("2026-07-24", 3000.0),
        },
    )
    df, dt = repo.get_enriched_latest_asset("index")
    assert str(dt) == "2026-07-24"
    assert df["symbol"].to_list() == ["000001.SH"]
    assert "ma5" in df.columns or "rsi_14" in df.columns  # 重算产出指标列


def test_get_enriched_latest_asset_index_cold_no_refresh(repo):
    df, dt = repo.get_enriched_latest_asset("index", refresh=False)
    assert df.is_empty() and dt is None


def test_flush_live_enriched_asset_index_updates_cache(repo):
    df = pl.DataFrame(
        [
            {
                "symbol": "000001.SH",
                "date": _dt.date(2026, 7, 25),
                "open": 3000.0,
                "high": 3010.0,
                "low": 2990.0,
                "close": 3005.0,
                "volume": 1_000_000,
                "amount": 1e9,
                "ma5": 3001.0,
                "rsi_14": 55.0,
            }
        ]
    )
    repo.flush_live_enriched_asset("index", df)
    cached, dt = repo.get_enriched_latest_asset("index", refresh=False)
    assert str(dt) == "2026-07-25"
    assert cached["close"].to_list() == [3005.0]
    stored = repo.storage.read_daily(
        "kline_index_enriched", start=_dt.date(2026, 7, 25), end=_dt.date(2026, 7, 25)
    )
    assert stored["close"].to_list() == [3005.0]
    # RC-11: 指数 enriched live 写也在 CK 上留下 ready generation
    current = repo.storage.current_generation("index")
    assert current and current["state"] == "ready" and current["generation_id"]


def _merge_row(symbol, close):
    return {
        "symbol": symbol,
        "date": _dt.date(2026, 7, 25),
        "open": close - 5,
        "high": close + 5,
        "low": close - 6,
        "close": close,
        "volume": 1_000,
        "amount": 1e6,
    }


def test_merge_live_enriched_asset_index_merges_cache(repo):
    """merge 路径: 两次合并缓存取并集 (不 NameError, 不丢已有缓存)。"""
    repo.merge_live_enriched_asset("index", pl.DataFrame([_merge_row("000001.SH", 3000.0)]))
    repo.merge_live_enriched_asset("index", pl.DataFrame([_merge_row("000300.SH", 4000.0)]))
    cached, dt = repo.get_enriched_latest_asset("index", refresh=False)
    assert str(dt) == "2026-07-25"
    assert set(cached["symbol"].to_list()) == {"000001.SH", "000300.SH"}
