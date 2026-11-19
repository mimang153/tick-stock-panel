from __future__ import annotations

import calendar
import itertools
from datetime import date, datetime, timedelta
from typing import Any, cast

import polars as pl
import pytest

from app.data_providers.base import ProviderRequestError
from app.plugins.fuyao import client as fc
from app.plugins.fuyao import provider as fp
from app.plugins.fuyao.provider import FuyaoProvider

# ---- helpers: 历史K线 / dump (与原文件同语义) ----


def _sh_ms(d: date) -> int:
    """交易日 → 扶摇口径 ms(该日上海零点), 与 provider._ms_of_date 同式。"""
    return (calendar.timegm(d.timetuple()) - 28_800) * 1000


def _bar(d: date, close: float, volume: float = 1_612_611, open_: float | None = None):
    """historical/dump 原始行(价格元, volume 股)。"""
    return {
        "date_ms": _sh_ms(d),
        "open_price": open_ if open_ is not None else round(close * 0.99, 2),
        "high_price": round(close * 1.01, 2),
        "low_price": round(close * 0.98, 2),
        "close_price": close,
        "volume": volume,
        "turnover": close * volume,
    }


def _dump_bar(sym: str, d: date, close: float) -> dict:
    """daily dump 行(11 列形状, 含 thscode/adjusted)。"""
    return {
        "thscode": sym,
        "currency": "CNY",
        "interval": "1d",
        "adjusted": "none",
        **_bar(d, close),
    }


class _FakeHistClient:
    """单标的历史K线假客户端: 按窗口过滤预置 bars, 记录调用参数。"""

    def __init__(self, bars_by_symbol: dict | None = None, error_syms: tuple = ()):
        self.bars = bars_by_symbol or {}
        self.errors = set(error_syms)
        self.calls: list[dict] = []

    def historical_kline(self, thscode, start_ms, end_ms, adjust="none"):
        self.calls.append({"thscode": thscode, "start": start_ms, "end": end_ms, "adjust": adjust})
        if thscode in self.errors:
            raise fc.FuyaoError("扶摇接口错误 code=4001: 频率超限")
        return [b for b in self.bars.get(thscode, []) if start_ms <= b["date_ms"] <= end_ms]

    def historical_kline_batch(self, thscodes, start_ms, end_ms):
        # 旧用例专门覆盖整包与旧接口兜底;批量成功契约由独立用例覆盖。
        raise fc.FuyaoError("测试旧接口回退")

    def dump_download_url(self, dump_kind):
        raise fc.FuyaoError("测试环境无 dump")

    def close(self):
        pass


def _hist_provider(monkeypatch, fake: _FakeHistClient, allow_dumps: bool = False) -> FuyaoProvider:
    p = FuyaoProvider()
    monkeypatch.setattr(fp, "fuyao_client", type("M", (), {"FuyaoClient": lambda **kw: fake}))
    monkeypatch.setattr(fp, "get_api_key", lambda: "test-key")
    monkeypatch.setattr(fp, "_HIST_INTERVAL_S", 0.0)
    monkeypatch.setattr(fp, "_HIST_SMALL_SYMBOL_LIMIT", 0)
    if not allow_dumps:
        # API 路径测试必须与开发机真实 dump 缓存隔离; dump 测试显式传 allow_dumps=True。
        p._ensure_daily_big_dump = lambda start_d: None  # type: ignore[assignment]
        p._daily_dump_info = lambda: None  # type: ignore[assignment]
    return p


def _daily10_dump(rows: list[dict]) -> pl.DataFrame:
    """11 列 daily-k-10d dump 形状。"""
    return pl.DataFrame(
        rows,
        schema={
            "thscode": pl.String,
            "currency": pl.String,
            "interval": pl.String,
            "adjusted": pl.String,
            "date_ms": pl.Int64,
            "open_price": pl.Float64,
            "high_price": pl.Float64,
            "low_price": pl.Float64,
            "close_price": pl.Float64,
            "volume": pl.Float64,
            "turnover": pl.Float64,
        },
    )


def _adj_dump(events: list[tuple]) -> pl.DataFrame:
    """8 列 adjustment-factors dump 形状。events: (thscode, ex_date, D, S, AR, AP)。"""
    rows = [
        {
            "thscode": s,
            "ticker": s.split(".")[0],
            "ex_date_ms": _sh_ms(d),
            "dividend_per_share": dv,
            "per_share_bonus": bn,
            "allotment_ratio": ar,
            "allotment_price": ap,
            "currency": "CNY",
        }
        for s, d, dv, bn, ar, ap in events
    ]
    return pl.DataFrame(
        rows,
        schema={
            "thscode": pl.String,
            "ticker": pl.String,
            "ex_date_ms": pl.Int64,
            "dividend_per_share": pl.Float64,
            "per_share_bonus": pl.Float64,
            "allotment_ratio": pl.Float64,
            "allotment_price": pl.Float64,
            "currency": pl.String,
        },
    )


def _adj_provider(monkeypatch, events: list[tuple], bars_by_symbol: dict) -> FuyaoProvider:
    p = _hist_provider(monkeypatch, _FakeHistClient(bars_by_symbol))
    p._dump_memo[fp._ADJ_DUMP_KIND] = _adj_dump(events)
    return p


def _bigdump_provider(
    monkeypatch, tmp_path, big_rows: list[dict], ten_memo: pl.DataFrame | None = None
):
    """缓存目录重定向到 tmp_path 并放置 daily_k__<release>.parquet; 单标的接口若被调用会被断言暴露。"""
    provider = _hist_provider(monkeypatch, _FakeHistClient({}), allow_dumps=True)
    monkeypatch.setattr(fp, "_cache_dir", lambda: tmp_path)
    _daily10_dump(big_rows).write_parquet(tmp_path / "daily_k__20260101.parquet")
    if ten_memo is not None:
        provider._dump_memo[fp._DAILY10_DUMP_KIND] = ten_memo
    return provider


# =====================================================================
# daily / adj_factor (2026-08 接入)
# =====================================================================


# ---- daily: 单标的接口路径 ----


def test_daily_api_units_volume_shares_to_lots(monkeypatch):
    """核心口径: 原始价 + volume 股→手 floor(/100) + 上海零点时区。"""
    bars = [
        _bar(date(2026, 8, 26), 11.10),
        _bar(date(2026, 8, 27), 11.05),
        _bar(date(2026, 8, 28), 11.65),
    ]
    provider = _hist_provider(monkeypatch, _FakeHistClient({"000001.SZ": bars}))
    # 窗口跨度 28 天 > _RECENT_DUMP_DAYS → 走单标的接口
    df = provider.get_daily(["000001.SZ"], datetime(2026, 8, 1), datetime(2026, 8, 28))
    assert df.height == 3
    assert df.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    assert df.schema["date"] == pl.Date
    assert df["date"].to_list() == [date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28)]
    assert df["volume"].to_list() == [16126.0] * 3  # 1,612,611 股 → 16126 手
    assert df["close"].to_list() == [11.10, 11.05, 11.65]
    assert df["amount"].to_list() == [b["turnover"] for b in bars]


def test_daily_api_adjust_locked_to_none(monkeypatch):
    """adjust=none 锁定: 服务端默认 forward, 官方前复权序列不可用(事件间逐日漂移)。"""
    provider = _hist_provider(
        monkeypatch, _FakeHistClient({"000001.SZ": [_bar(date(2026, 8, 27), 11.05)]})
    )
    provider.get_daily(["000001.SZ"], datetime(2026, 8, 1), datetime(2026, 8, 28))
    fake = cast(Any, provider._get_client())
    assert fake.calls and all(c["adjust"] == "none" for c in fake.calls)


def test_daily_api_deep_window_splits_into_10y_chunks(monkeypatch):
    """超 10 年窗口自动分片(25.6 年 → 3 片), 分片连续不重叠且各 ≤10 年。"""
    provider = _hist_provider(monkeypatch, _FakeHistClient({"000001.SZ": []}))
    provider.get_daily(["000001.SZ"], datetime(2001, 1, 1), datetime(2026, 8, 28))
    calls = cast(Any, provider._get_client()).calls
    assert len(calls) == 3
    for c in calls:
        assert c["end"] - c["start"] < fp._HIST_MAX_SPAN_MS
    for a, b in itertools.pairwise(calls):
        assert b["start"] == a["end"] + 1  # 连续且不重叠


def test_daily_api_failure_after_batch_fallback_is_explicit(monkeypatch):
    """批量与旧接口均失败时不得把缺失的股票视为成功。"""
    bars = {"000001.SZ": [_bar(date(2026, 8, 27), 11.05)]}
    provider = _hist_provider(monkeypatch, _FakeHistClient(bars, error_syms=("600519.SH",)))
    with pytest.raises(ProviderRequestError, match=r"600519\.SH"):
        provider.get_daily(["600519.SH", "000001.SZ"], datetime(2026, 8, 1), datetime(2026, 8, 28))


def test_iter_daily_api_is_bounded_by_symbol_batch(monkeypatch):
    """历史 API 每批只产出固定 symbol 集合,不积累全市场 frames。"""
    bars = {
        "000001.SZ": [_bar(date(2018, 1, 2), 10.0)],
        "000002.SZ": [_bar(date(2018, 1, 2), 11.0)],
        "000003.SZ": [_bar(date(2018, 1, 2), 12.0)],
    }
    provider = _hist_provider(monkeypatch, _FakeHistClient(bars))
    monkeypatch.setattr(fp, "_HIST_SYMBOL_BATCH", 2)
    chunks = list(provider.iter_daily(list(bars), datetime(2018, 1, 1), datetime(2018, 1, 3)))
    assert [set(df["symbol"].to_list()) for df in chunks] == [
        {"000001.SZ", "000002.SZ"},
        {"000003.SZ"},
    ]


def test_daily_empty_symbols_or_non_stock_returns_empty(monkeypatch):
    provider = _hist_provider(monkeypatch, _FakeHistClient({}))
    assert provider.get_daily([], None, None).is_empty()
    assert provider.get_daily(
        ["510300.SH"], datetime(2026, 8, 1), datetime(2026, 8, 28), asset_type="etf"
    ).is_empty()


# ---- daily: 10d dump 路径 ----


def _recent_daily_dump() -> pl.DataFrame:
    rows = []
    for d in [date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28)]:
        b = _bar(d, 11.0 + hash(d) % 3, volume=97_570_170)
        rows.append(
            {"thscode": "000001.SZ", "currency": "CNY", "interval": "1d", "adjusted": "none", **b}
        )
        rows.append(
            {
                "thscode": "600519.SH",
                "currency": "CNY",
                "interval": "1d",
                "adjusted": "none",
                **_bar(d, 1480.0, volume=1_234_500),
            }
        )
    # 停牌行(open=high=0)应被过滤
    rows.append(
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 27), 11.0, open_=0.0),
        }
    )
    rows[-1]["high_price"] = 0.0
    return _daily10_dump(rows)


def test_daily_recent_window_uses_dump_without_api_calls(monkeypatch):
    """近端窗口: 一次 dump 覆盖全部标的, 不打单标的接口; volume 股→手; 停牌行过滤。"""
    provider = _hist_provider(monkeypatch, _FakeHistClient({}))
    provider._dump_memo[fp._DAILY10_DUMP_KIND] = _recent_daily_dump()
    progress = []
    df = provider.get_daily(
        ["000001.SZ"],
        datetime(2026, 8, 26),
        datetime(2026, 8, 28),
        on_chunk_done=lambda c, t: progress.append((c, t)),
    )
    assert cast(Any, provider._get_client()).calls == []  # 未走单标的接口
    assert progress == [(1, 1)]
    assert set(df["symbol"].to_list()) == {"000001.SZ"}  # 未请求的标的不混入
    assert df["volume"].max() == 975_701.0  # 97,570,170 股 → 975,701 手
    assert date(2026, 8, 26) in df["date"].to_list()
    # 停牌行(open=high=0)被 filter_halt_days 剔除, 08-27 仅保留正常行
    assert df.filter(pl.col("date") == date(2026, 8, 27)).height == 1


def test_daily_dump_stale_falls_back_to_api(monkeypatch):
    """dump 末端落后且 end 是工作日 → 视为有缺口, 回退单标的接口。"""
    bars = {"000001.SZ": [_bar(date(2026, 8, 27), 11.05)]}
    provider = _hist_provider(monkeypatch, _FakeHistClient(bars))
    provider._dump_memo[fp._DAILY10_DUMP_KIND] = _recent_daily_dump()  # max=08-28
    provider.get_daily(["000001.SZ"], datetime(2026, 8, 24), datetime(2026, 9, 4))  # 09-04 周五
    assert cast(Any, provider._get_client()).calls != []


@pytest.mark.parametrize("deep_window", [False, True])
def test_daily_dump_refreshes_cached_release_for_new_trading_days(
    monkeypatch, tmp_path, deep_window
):
    """常驻进程跨日后须检查新 dump, 不能一直拿旧缓存转逐票历史接口。"""

    class DailyDumpClient(_FakeHistClient):
        release = "20261027"

        def __init__(self):
            super().__init__()
            self.downloads = []

        def dump_download_url(self, dump_kind):
            return {"presigned_url": f"https://example.test/releases/{self.release}/daily.parquet"}

        def download_dump(self, dump_kind, dest):
            self.downloads.append(self.release)
            last = date(2026, 10, 27) if self.release == "20261027" else date(2026, 10, 29)
            rows = [
                _dump_bar("000001.SZ", day, close)
                for day, close in [
                    (date(2026, 10, 27), 10.0),
                    (date(2026, 10, 28), 11.0),
                    (date(2026, 10, 29), 12.0),
                ]
                if day <= last
            ]
            _daily10_dump(rows).write_parquet(dest)
            return dest

    client = DailyDumpClient()
    provider = _hist_provider(monkeypatch, client, allow_dumps=deep_window)
    monkeypatch.setattr(fp, "_cache_dir", lambda: tmp_path)
    if deep_window:
        _daily10_dump(
            [
                _dump_bar("000001.SZ", date(2026, 10, 12), 9.0),
                _dump_bar("000001.SZ", date(2026, 10, 27), 10.0),
            ]
        ).write_parquet(tmp_path / "daily_k__20261027.parquet")

    first = provider.get_daily(["000001.SZ"], datetime(2026, 10, 27), datetime(2026, 10, 27))
    assert first["date"].to_list() == [date(2026, 10, 27)]
    client.release = "20261029"
    refreshed = provider.get_daily(
        ["000001.SZ"], datetime(2026, 10, 12 if deep_window else 27), datetime(2026, 10, 29)
    )
    expected_days = ([date(2026, 10, 12)] if deep_window else []) + [
        date(2026, 10, 27),
        date(2026, 10, 28),
        date(2026, 10, 29),
    ]
    assert refreshed["date"].to_list() == expected_days
    assert refreshed["close"].to_list() == ([9.0] if deep_window else []) + [10.0, 11.0, 12.0]
    assert client.calls == []
    assert client.downloads == ["20261027", "20261029"]


def test_daily_dump_weekend_end_covered(monkeypatch):
    """end 为周末且紧随 dump 末端 → 自然缺口, 仍走 dump。"""
    provider = _hist_provider(monkeypatch, _FakeHistClient({}))
    provider._dump_memo[fp._DAILY10_DUMP_KIND] = _recent_daily_dump()  # max=周五 08-28
    df = provider.get_daily(["000001.SZ"], datetime(2026, 8, 26), datetime(2026, 8, 30))  # 周日
    assert cast(Any, provider._get_client()).calls == []
    assert not df.is_empty()


def test_daily_dump_rejects_non_raw_adjustment(monkeypatch):
    """防御: dump 变为复权口径(adjusted != none)时拒绝输出, 不污染原始K线库。"""
    dump = _recent_daily_dump().with_columns(pl.lit("forward").alias("adjusted"))
    provider = _hist_provider(monkeypatch, _FakeHistClient({}))
    provider._dump_memo[fp._DAILY10_DUMP_KIND] = dump
    assert provider.get_daily(
        ["000001.SZ"], datetime(2026, 8, 26), datetime(2026, 8, 28)
    ).is_empty()


def test_daily_old_window_bypasses_dump(monkeypatch):
    """深窗口且 dump 不可用 → 单标的接口兜底。"""
    provider = _hist_provider(monkeypatch, _FakeHistClient({"000001.SZ": []}))
    df = provider.get_daily(["000001.SZ"], datetime(2020, 1, 1), datetime(2026, 8, 28))
    assert df.is_empty()
    assert cast(Any, provider._get_client()).calls  # 走了单标的接口


# ---- daily: 10 年全量 dump 档 ----


def _bigdump_provider_v2(
    monkeypatch, tmp_path, big_rows: list[dict], ten_memo: pl.DataFrame | None = None
):
    """缓存目录重定向到 tmp_path 并放置 daily_k__<release>.parquet; 单标的接口若被调用会被断言暴露。"""
    provider = _hist_provider(monkeypatch, _FakeHistClient({}), allow_dumps=True)
    monkeypatch.setattr(fp, "_cache_dir", lambda: tmp_path)
    _daily10_dump(big_rows).write_parquet(tmp_path / "daily_k__20260101.parquet")
    if ten_memo is not None:
        provider._dump_memo[fp._DAILY10_DUMP_KIND] = ten_memo
    return provider


def test_daily_deep_window_uses_big_dump(monkeypatch, tmp_path):
    """深窗口走 10 年 dump: 不打单标的接口, volume 股→手, 未请求标的不混入。"""
    rows = [
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 1, 5), 11.0, volume=97_570_170),
        },
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 6, 12), 11.7, volume=1_234_500),
        },
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 14), 11.4, volume=1_234_500),
        },
        {
            "thscode": "600519.SH",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 14), 1480.0, volume=1_234_500),
        },
    ]
    provider = _bigdump_provider_v2(monkeypatch, tmp_path, rows)
    progress = []
    df = provider.get_daily(
        ["000001.SZ"],
        datetime(2026, 1, 5),
        datetime(2026, 8, 14),
        on_chunk_done=lambda c, t: progress.append((c, t)),
    )
    assert cast(Any, provider._get_client()).calls == []
    assert progress == [(1, 1)]
    assert set(df["symbol"].to_list()) == {"000001.SZ"}
    assert df["date"].to_list() == [date(2026, 1, 5), date(2026, 6, 12), date(2026, 8, 14)]
    assert df["volume"].to_list() == [975_701.0, 12_345.0, 12_345.0]


def test_iter_daily_big_dump_reads_bounded_record_batches(monkeypatch, tmp_path):
    """多年 dump 分批读取,不经 LazyFrame collect() 物化整份文件。"""
    rows = [
        _dump_bar("000001.SZ", date(2026, 1, 1) + timedelta(days=i), 10.0 + i) for i in range(5)
    ]
    provider = _bigdump_provider_v2(monkeypatch, tmp_path, rows)
    monkeypatch.setattr(fp, "_DAILY_DUMP_BATCH_ROWS", 2)
    chunks = list(provider.iter_daily(["000001.SZ"], datetime(2026, 1, 1), datetime(2026, 1, 5)))
    assert [chunk.height for chunk in chunks] == [2, 2, 1]
    assert [d for chunk in chunks for d in chunk["date"].to_list()] == [
        date(2026, 1, 1),
        date(2026, 1, 2),
        date(2026, 1, 3),
        date(2026, 1, 4),
        date(2026, 1, 5),
    ]


def test_iter_daily_splits_older_history_from_big_dump(monkeypatch, tmp_path):
    """请求跨 dump 起点时,只把 dump 外那段回退到历史 API。"""
    provider = _bigdump_provider_v2(
        monkeypatch,
        tmp_path,
        [_dump_bar("000001.SZ", date(2020, 1, 2), 10.0)],
    )
    provider._client = _FakeHistClient({"000001.SZ": [_bar(date(2019, 12, 31), 9.0)]})
    rows = pl.concat(
        list(provider.iter_daily(["000001.SZ"], datetime(2019, 12, 30), datetime(2020, 1, 2)))
    )
    assert rows["date"].to_list() == [date(2019, 12, 31), date(2020, 1, 2)]
    assert len(cast(Any, provider._get_client()).calls) == 1


def test_daily_big_dump_tail_filled_by_10d(monkeypatch, tmp_path):
    """大 dump 末端缺口(dmax 旧)由 10d dump 补尾, 两段拼接无缝。"""
    big_rows = [
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 10), 10.9),
        },
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 14), 11.0),
        },
    ]
    ten_rows = [
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 15), 11.2),
        },
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 28), 11.65),
        },
    ]
    provider = _bigdump_provider_v2(
        monkeypatch, tmp_path, big_rows, ten_memo=_daily10_dump(ten_rows)
    )
    df = provider.get_daily(["000001.SZ"], datetime(2026, 8, 10), datetime(2026, 8, 28))
    assert df["date"].to_list() == [
        date(2026, 8, 10),
        date(2026, 8, 14),
        date(2026, 8, 15),
        date(2026, 8, 28),
    ]
    assert df["close"].to_list() == [10.9, 11.0, 11.2, 11.65]


def test_daily_big_dump_weekend_end_needs_no_tail(monkeypatch, tmp_path):
    """多年 dump 覆盖到周五时,紧邻周末不应回退逐标的 API。"""
    provider = _bigdump_provider_v2(
        monkeypatch,
        tmp_path,
        [
            _dump_bar("000001.SZ", date(2026, 8, 1), 10.9),
            _dump_bar("000001.SZ", date(2026, 8, 28), 11.65),
        ],
    )

    df = provider.get_daily(["000001.SZ"], datetime(2026, 8, 1), datetime(2026, 8, 30))

    assert df["date"].to_list() == [date(2026, 8, 1), date(2026, 8, 28)]
    assert cast(Any, provider._get_client()).calls == []


def test_daily_big_dump_midgap_falls_back(monkeypatch, tmp_path):
    """大 dump 与 10d dump 之间有中段缺口 → 整体回退, 不拼缺口数据。"""
    big_rows = [
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 1), 10.9),
        },
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 5), 11.0),
        },
    ]
    # 10d 从 08-27 起, 与大 dump 末端 08-05 之间有中段缺口
    ten_rows = [
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2026, 8, 27), 11.5),
        },
    ]
    provider = _bigdump_provider_v2(
        monkeypatch, tmp_path, big_rows, ten_memo=_daily10_dump(ten_rows)
    )
    df = provider.get_daily(["000001.SZ"], datetime(2026, 8, 1), datetime(2026, 8, 28))
    assert df.is_empty()  # 兜底走单标的接口(fake 为空) → 不返回缺口拼接数据


def test_daily_big_dump_start_before_dmin_falls_back(monkeypatch, tmp_path):
    """窗口早于大 dump 起点(>10 年) → 单标的接口兜底。"""
    rows = [
        {
            "thscode": "000001.SZ",
            "currency": "CNY",
            "interval": "1d",
            "adjusted": "none",
            **_bar(date(2020, 1, 2), 10.0),
        },
    ]
    provider = _bigdump_provider_v2(monkeypatch, tmp_path, rows)
    # 缓存不覆盖起点 → 会尝试拉最新 release; 模拟"最新 release 就是这份缓存文件"
    provider._ensure_dump_path = lambda kind, prefix: tmp_path / "daily_k__20260101.parquet"  # type: ignore[assignment]
    bars = {"000001.SZ": [_bar(date(2015, 6, 1), 9.0)]}
    provider._dump_memo.clear()
    monkeypatch.setattr(
        fp, "fuyao_client", type("M", (), {"FuyaoClient": lambda **kw: _FakeHistClient(bars)})
    )
    df = provider.get_daily(["000001.SZ"], datetime(2015, 1, 1), datetime(2015, 12, 31))
    assert df.height == 1 and df["close"][0] == 9.0  # 单标的兜底成功


def test_tail_ok_weekend_tolerance():
    """末端容忍: 周末/节假日的自然缺口(≤3 天且 end 是周末)不算缺失。"""
    assert fp._tail_ok(date(2026, 8, 28), date(2026, 8, 28)) is True
    assert fp._tail_ok(date(2026, 8, 29), date(2026, 8, 28)) is True  # 周六
    assert fp._tail_ok(date(2026, 8, 30), date(2026, 8, 28)) is True  # 周日
    assert fp._tail_ok(date(2026, 8, 31), date(2026, 8, 28)) is False  # 周一(可能有行情)
    assert fp._tail_ok(date(2026, 9, 5), date(2026, 8, 28)) is False  # 隔了一周
