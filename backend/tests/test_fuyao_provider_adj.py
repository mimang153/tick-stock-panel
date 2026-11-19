from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta

import polars as pl
import pytest

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


_ADJ_OUT_COLS = [
    "symbol",
    "trade_date",
    "ex_factor",
    "dividend",
    "bonus",
    "allot",
    "allot_price",
    "prev_close",
]


class _RecordingHttp:
    def __init__(self, payload):
        self.payload = payload
        self.calls: list[tuple] = []

    def get(self, path, params=None):
        self.calls.append((path, params))
        resp = type("R", (), {"json": lambda self_: self.payload, "status_code": 200})()
        return resp

    def close(self):
        pass


# =====================================================================
# adj_factor 推导/配价、客户端、时区与试拉
# =====================================================================


# ---- adj_factor: 推导 ----

# get_adj_factors 输出契约: 规范三列 + 事件明细列 (等差显示/全精度链重建用)
_ADJ_OUT_COLS = [
    "symbol",
    "trade_date",
    "ex_factor",
    "dividend",
    "bonus",
    "allot",
    "allot_price",
    "prev_close",
]


def test_adj_dividend_factor_derivation(monkeypatch):
    """纯分红: P=32.8, D=0.68 → ref=32.12 → factor=32.8/32.12 (与 tickflow 对拍值一致)。"""
    events = [("600519.SH", date(2026, 6, 12), 0.68, 0.0, 0.0, 0.0)]
    bars = {"600519.SH": [_bar(date(2026, 6, 11), 32.8), _bar(date(2026, 6, 12), 32.0)]}
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["600519.SH"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["trade_date"] == date(2026, 6, 12)
    assert row["ex_factor"] == pytest.approx(32.8 / 32.12)


def test_adj_output_keeps_event_details(monkeypatch):
    """扩表: 输出保留事件明细 (每股分红/送转/配股/前收盘), 供等差显示与全精度链重建。"""
    events = [("600519.SH", date(2026, 6, 12), 0.68, 0.0, 0.0, 0.0)]
    bars = {"600519.SH": [_bar(date(2026, 6, 11), 32.8), _bar(date(2026, 6, 12), 32.0)]}
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["600519.SH"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    row = df.row(0, named=True)
    assert row["dividend"] == pytest.approx(0.68)
    assert row["bonus"] == 0.0
    assert row["allot"] == 0.0
    assert row["allot_price"] == 0.0
    assert row["prev_close"] == pytest.approx(32.8)


def test_adj_bonus_and_allotment_formula(monkeypatch):
    """送转+配股混合: P=5.23, AR=0.4, AP=3.36 → ref=(5.23+1.344)/1.4=4.6957→4.70(half-up)。"""
    events = [("300176.SZ", date(2026, 8, 21), 0.0, 0.0, 0.4, 3.36)]
    bars = {"300176.SZ": [_bar(date(2026, 8, 20), 5.23), _bar(date(2026, 8, 21), 4.68)]}
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["300176.SZ"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    assert df.height == 1
    assert df["ex_factor"][0] == pytest.approx(5.23 / 4.70)


def test_adj_half_up_rounding_not_bankers(monkeypatch):
    """舍入口径: x=2.625 → half-up 2.63 (银行家舍入会给 2.62, 对拍实证偏离)。"""
    events = [("600000.SH", date(2026, 6, 12), 8.0, 0.0, 0.0, 0.0)]
    bars = {"600000.SH": [_bar(date(2026, 6, 11), 10.625), _bar(date(2026, 6, 12), 2.70)]}
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["600000.SH"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    assert df["ex_factor"][0] == pytest.approx(10.625 / 2.63)


def test_adj_price_limit_selfcheck_drops_impossible_factor(monkeypatch):
    """涨跌停自检: 复权后除权日涨幅超板限的事件剔除, 不让坏因子落库。"""
    events = [("600000.SH", date(2026, 6, 12), 8.0, 0.0, 0.0, 0.0)]
    bars = {"600000.SH": [_bar(date(2026, 6, 11), 10.625), _bar(date(2026, 6, 12), 4.05)]}  # +54%
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["600000.SH"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    assert df.is_empty()


def test_adj_merges_same_day_components_before_derivation(monkeypatch):
    """同日拆行(分红行+送股行)必须先合并再推导。"""
    events = [
        ("000812.SZ", date(2026, 6, 12), 0.3, 0.0, 0.0, 0.0),
        ("000812.SZ", date(2026, 6, 12), 0.0, 0.1, 0.0, 0.0),
    ]
    bars = {"000812.SZ": [_bar(date(2026, 6, 11), 10.0), _bar(date(2026, 6, 12), 9.0)]}
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["000812.SZ"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    assert df.height == 1  # 合并成一个事件
    assert df["ex_factor"][0] == pytest.approx(10.0 / 8.82)  # ref=(10-0.3)/1.1=8.8181→8.82


def test_adj_filters_zero_rows_and_missing_allot_price(monkeypatch):
    """全零行与配股价缺失行过滤, 不参与推导。"""
    events = [
        ("000001.SZ", date(2026, 6, 12), 0.0, 0.0, 0.0, 0.0),  # 全零
        ("000002.SZ", date(2026, 6, 12), 0.0, 0.0, 0.3, 0.0),  # 配股无价
        ("600519.SH", date(2026, 6, 12), 0.68, 0.0, 0.0, 0.0),  # 正常
    ]
    bars = {
        "600519.SH": [_bar(date(2026, 6, 11), 32.8), _bar(date(2026, 6, 12), 32.0)],
        "000001.SZ": [_bar(date(2026, 6, 11), 10.0), _bar(date(2026, 6, 12), 10.0)],
        "000002.SZ": [_bar(date(2026, 6, 11), 8.0), _bar(date(2026, 6, 12), 8.0)],
    }
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["000001.SZ", "000002.SZ", "600519.SH"], None, None)
    assert df["symbol"].unique().to_list() == ["600519.SH"]


def test_adj_skips_future_announced_events(monkeypatch):
    """未来已公告事件无前收盘, 跳过留给滚动增量窗口。"""
    future = date.today() + timedelta(days=5)
    events = [("600519.SH", future, 0.68, 0.0, 0.0, 0.0)]
    provider = _adj_provider(monkeypatch, events, {"600519.SH": []})
    df = provider.get_adj_factors(["600519.SH"], datetime(2026, 1, 1), None)
    assert df.is_empty()


def test_adj_window_filter(monkeypatch):
    events = [
        ("600519.SH", date(2026, 1, 10), 0.5, 0.0, 0.0, 0.0),
        ("600519.SH", date(2026, 6, 12), 0.68, 0.0, 0.0, 0.0),
    ]
    bars = {
        "600519.SH": [
            _bar(date(2026, 1, 9), 30.0),
            _bar(date(2026, 1, 10), 30.0),
            _bar(date(2026, 6, 11), 32.8),
            _bar(date(2026, 6, 12), 32.0),
        ]
    }
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["600519.SH"], datetime(2026, 3, 1), datetime(2026, 12, 31))
    assert df["trade_date"].to_list() == [date(2026, 6, 12)]


def test_adj_output_schema_sorted_and_deduped(monkeypatch):
    events = [
        ("000001.SZ", date(2026, 6, 12), 0.36, 0.0, 0.0, 0.0),
        ("600519.SH", date(2026, 6, 12), 0.68, 0.0, 0.0, 0.0),
        ("600519.SH", date(2025, 6, 20), 0.5, 0.0, 0.0, 0.0),
    ]
    bars = {
        "600519.SH": [
            _bar(date(2025, 6, 19), 31.0),
            _bar(date(2025, 6, 20), 31.0),
            _bar(date(2026, 6, 11), 32.8),
            _bar(date(2026, 6, 12), 32.0),
        ],
        "000001.SZ": [_bar(date(2026, 6, 11), 11.7), _bar(date(2026, 6, 12), 11.5)],
    }
    provider = _adj_provider(monkeypatch, events, bars)
    df = provider.get_adj_factors(["000001.SZ", "600519.SH"], None, None)
    assert df.columns == _ADJ_OUT_COLS
    assert df.schema["trade_date"] == pl.Date and df.schema["ex_factor"] == pl.Float64
    assert df.height == 3
    assert df.sort(["symbol", "trade_date"]).equals(df)  # 已排序
    assert df["ex_factor"].min() > 1.0  # 分红因子必 >1


def test_adj_etf_and_empty_symbols_return_empty(monkeypatch):
    provider = _adj_provider(monkeypatch, [], {})
    empty = provider.get_adj_factors([], None, None)
    assert empty.is_empty() and empty.columns == _ADJ_OUT_COLS
    etf = provider.get_adj_factors(["510300.SH"], None, None, asset_type="etf")
    assert etf.is_empty()


def test_adj_dump_unavailable_returns_empty(monkeypatch):
    """dump 加载失败软返回空(不阻断管道), 不抛异常。"""
    provider = _hist_provider(monkeypatch, _FakeHistClient({}))

    def _boom(dump_kind, cache_prefix):
        raise fc.FuyaoError("dump 下载网络失败")

    provider._ensure_dump = _boom  # type: ignore[assignment]
    df = provider.get_adj_factors(["600519.SH"], None, None)
    assert df.is_empty() and df.columns == _ADJ_OUT_COLS


def test_adj_progress_callback_per_symbol(monkeypatch):
    events = [
        ("000001.SZ", date(2026, 6, 12), 0.36, 0.0, 0.0, 0.0),
        ("600519.SH", date(2026, 6, 12), 0.68, 0.0, 0.0, 0.0),
    ]
    bars = {
        "600519.SH": [_bar(date(2026, 6, 11), 32.8), _bar(date(2026, 6, 12), 32.0)],
        "000001.SZ": [_bar(date(2026, 6, 11), 11.7), _bar(date(2026, 6, 12), 11.5)],
    }
    provider = _adj_provider(monkeypatch, events, bars)
    progress = []
    provider.get_adj_factors(
        ["600519.SH", "000001.SZ"], None, None, on_chunk_done=lambda c, t: progress.append((c, t))
    )
    assert progress == [(1, 2), (2, 2)]


# ---- adj_factor: 本地 dump 配价(2026-08 优化) ----


def test_adj_closes_from_local_dump_no_http(monkeypatch, tmp_path):
    """前收盘从本地日K dump 一次取齐: 零单标的请求, 因子值与接口路径同公式。"""
    events = [("000001.SZ", date(2026, 6, 12), 0.3, 0.0, 0.0, 0.0)]
    big_rows = [
        _dump_bar("000001.SZ", date(2026, 5, 10), 10.2),  # 覆盖窗口起点(first_ex-30d)
        _dump_bar("000001.SZ", date(2026, 6, 11), 10.625),
        _dump_bar("000001.SZ", date(2026, 6, 12), 9.6),
    ]
    provider = _bigdump_provider(monkeypatch, tmp_path, big_rows)
    provider._dump_memo[fp._ADJ_DUMP_KIND] = _adj_dump(events)
    progress = []
    df = provider.get_adj_factors(
        ["000001.SZ"],
        datetime(2026, 1, 1),
        datetime(2026, 12, 31),
        on_chunk_done=lambda c, t: progress.append((c, t)),
    )
    assert provider._get_client().calls == []  # 零 HTTP
    assert progress == [(1, 1)]
    assert df.height == 1
    assert df["trade_date"][0] == date(2026, 6, 12)
    ref = fp._ref_price(10.625, 0.3, 0.0, 0.0, 0.0)
    assert ref is not None
    assert df["ex_factor"][0] == pytest.approx(10.625 / ref)


def test_adj_missing_symbol_in_dump_falls_back_to_http(monkeypatch, tmp_path):
    """dump 缺价的标的回退单标的接口; dump 内标的仍零请求。"""
    events = [
        ("000001.SZ", date(2026, 6, 12), 0.3, 0.0, 0.0, 0.0),
        ("600519.SH", date(2026, 6, 12), 1.0, 0.0, 0.0, 0.0),
    ]
    big_rows = [
        _dump_bar("000001.SZ", date(2026, 5, 10), 10.2),
        _dump_bar("000001.SZ", date(2026, 6, 11), 10.625),
        _dump_bar("000001.SZ", date(2026, 6, 12), 9.6),
    ]
    provider = _bigdump_provider(monkeypatch, tmp_path, big_rows)
    provider._dump_memo[fp._ADJ_DUMP_KIND] = _adj_dump(events)
    provider._get_client().bars["600519.SH"] = [
        _bar(date(2026, 6, 11), 1500.0),
        _bar(date(2026, 6, 12), 1400.0),
    ]
    df = provider.get_adj_factors(
        ["000001.SZ", "600519.SH"], datetime(2026, 1, 1), datetime(2026, 12, 31)
    )
    assert [c["thscode"] for c in provider._get_client().calls] == ["600519.SH"]
    assert set(df["symbol"].to_list()) == {"000001.SZ", "600519.SH"}


def test_adj_ancient_event_does_not_drag_universe_to_http(monkeypatch, tmp_path):
    """一只远古事件(早于 dump 覆盖起点)只拖累自己; dump 覆盖内的标的零请求。

    回归: 配价曾是"全有或全无" — 全市场最早事件早于 dump 起点时整轮放弃
    dump, 全部标的逐只回补历史K线 (全市场一次 ~15 分钟)。
    """
    events = [
        ("000001.SZ", date(2026, 6, 12), 0.3, 0.0, 0.0, 0.0),
        ("600519.SH", date(2005, 6, 30), 1.0, 0.0, 0.0, 0.0),  # 早于 dump 起点
    ]
    big_rows = [
        _dump_bar("000001.SZ", date(2026, 5, 10), 10.2),
        _dump_bar("000001.SZ", date(2026, 6, 11), 10.625),
        _dump_bar("000001.SZ", date(2026, 6, 12), 9.6),
    ]
    provider = _bigdump_provider(monkeypatch, tmp_path, big_rows)
    provider._dump_memo[fp._ADJ_DUMP_KIND] = _adj_dump(events)
    provider._get_client().bars["600519.SH"] = [
        _bar(date(2005, 6, 29), 50.0),
        _bar(date(2005, 6, 30), 45.0),
    ]
    df = provider.get_adj_factors(["000001.SZ", "600519.SH"], None, None)
    assert [c["thscode"] for c in provider._get_client().calls] == ["600519.SH"]
    assert "000001.SZ" in set(df["symbol"].to_list())  # dump 路径产出未被连累


def test_adj_ten_day_dump_overlays_fresh_ex_close(monkeypatch, tmp_path):
    """大 dump 缺除权日收盘(发布滞后)时由 10d dump 叠加补, 涨跌停自检仍生效。"""
    events = [("000001.SZ", date(2026, 8, 28), 8.0, 0.0, 0.0, 0.0)]  # 异常大额分红
    big_rows = [
        _dump_bar("000001.SZ", date(2026, 7, 25), 10.0),  # 覆盖窗口起点
        _dump_bar("000001.SZ", date(2026, 8, 27), 10.625),  # 大 dump 只到除权前一日
    ]
    ten_rows = [_dump_bar("000001.SZ", date(2026, 8, 28), 4.05)]  # +54% 超涨跌停
    provider = _bigdump_provider(monkeypatch, tmp_path, big_rows, ten_memo=_daily10_dump(ten_rows))
    provider._dump_memo[fp._ADJ_DUMP_KIND] = _adj_dump(events)
    df = provider.get_adj_factors(["000001.SZ"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    assert df.is_empty()  # 自检剔除(无叠加时 ex 日收盘缺失, 该行会被保留)


def test_adj_local_pricing_corrupt_dump_falls_back(monkeypatch, tmp_path):
    """本地配价读盘异常不致命: 整轮回退单标的接口。"""
    events = [("000001.SZ", date(2026, 6, 12), 0.3, 0.0, 0.0, 0.0)]
    big_rows = [
        _dump_bar("000001.SZ", date(2026, 5, 10), 10.2),
        _dump_bar("000001.SZ", date(2026, 6, 11), 10.625),
        _dump_bar("000001.SZ", date(2026, 6, 12), 9.6),
    ]
    provider = _bigdump_provider(monkeypatch, tmp_path, big_rows)
    provider._dump_memo[fp._ADJ_DUMP_KIND] = _adj_dump(events)

    def _boom(_lf):  # 签名对齐被替换的 _closes_scan
        raise OSError("disk error")

    provider._closes_scan = _boom  # type: ignore[assignment]
    provider._get_client().bars["000001.SZ"] = [
        _bar(date(2026, 6, 11), 10.625),
        _bar(date(2026, 6, 12), 9.6),
    ]
    df = provider.get_adj_factors(["000001.SZ"], datetime(2026, 1, 1), datetime(2026, 12, 31))
    assert df.height == 1
    assert provider._get_client().calls  # 回退了接口


# ---- 客户端: 历史日K / dump ----


class _DumpRecordingHttp:
    def __init__(self, payload):
        self.payload = payload
        self.calls: list[tuple] = []

    def get(self, path, params=None):
        self.calls.append((path, params))
        resp = type("R", (), {"json": lambda self_: self.payload, "status_code": 200})()
        return resp

    def close(self):
        pass


def test_client_historical_kline_sends_params(monkeypatch):
    http = _DumpRecordingHttp({"code": 0, "data": {"item": [_bar(date(2026, 8, 28), 11.65)]}})
    monkeypatch.setattr(fc.httpx, "Client", lambda **kw: http)
    c = fc.FuyaoClient(api_key="k")
    rows = c.historical_kline("000001.SZ", 1, 2, adjust="none")
    assert len(rows) == 1
    path, params = http.calls[0]
    assert path == "/api/a-share/prices/historical"
    assert params == {
        "thscode": "000001.SZ",
        "interval": "1d",
        "adjust": "none",
        "start": 1,
        "end": 2,
    }


def test_client_historical_kline_missing_item(monkeypatch):
    http = _DumpRecordingHttp({"code": 0, "data": {}})
    monkeypatch.setattr(fc.httpx, "Client", lambda **kw: http)
    c = fc.FuyaoClient(api_key="k")
    assert c.historical_kline("000001.SZ", 1, 2) == []


def test_client_dump_download_url(monkeypatch):
    info = {
        "presigned_url": "https://o.thsi.cn/.../releases/20260828/x.parquet?sig=1",
        "presigned_url_expires_at": "2026-08-29T12:00:00+08:00",
        "expires_in_seconds": 300,
    }
    http = _DumpRecordingHttp({"code": 0, "data": info})
    monkeypatch.setattr(fc.httpx, "Client", lambda **kw: http)
    c = fc.FuyaoClient(api_key="k")
    assert c.dump_download_url("adjustment-factors") == info
    assert http.calls[0][0] == "/api/dump/market-dumps/adjustment-factors/download-url"


def test_client_download_dump_atomic_and_key_never_leaked(monkeypatch, tmp_path):
    """/S3 预签名下载不带 X-api-key; 成功后原子改名, .part 清理。"""
    http = _DumpRecordingHttp(
        {
            "code": 0,
            "data": {
                "presigned_url": "https://o.thsi.cn/fuyao/market-dump/adj/releases/20260828/a.parquet?sig=1"
            },
        }
    )
    monkeypatch.setattr(fc.httpx, "Client", lambda **kw: http)

    captured: dict = {}

    class _Resp:
        status_code = 200

        def iter_bytes(self, n):
            yield b"parquet-bytes-"

    class _Ctx:
        def __enter__(self):
            return _Resp()

        def __exit__(self, *a):
            return False

    def _stream(method, url, **kw):
        captured["method"], captured["url"], captured["kw"] = method, url, kw
        return _Ctx()

    monkeypatch.setattr(fc.httpx, "stream", _stream)
    dest = tmp_path / "adj__20260828.parquet"
    fc.FuyaoClient(api_key="k").download_dump("adjustment-factors", dest)
    assert captured["method"] == "GET" and "o.thsi.cn" in captured["url"]
    assert "headers" not in captured["kw"] or "X-api-key" not in (
        captured["kw"].get("headers") or {}
    )
    assert dest.read_bytes() == b"parquet-bytes-"
    assert not list(tmp_path.glob("*.part"))


def test_client_download_dump_http_error(monkeypatch, tmp_path):
    http = _DumpRecordingHttp({"code": 0, "data": {"presigned_url": "https://o.thsi.cn/x.parquet"}})
    monkeypatch.setattr(fc.httpx, "Client", lambda **kw: http)

    class _Resp:
        status_code = 403

    class _Ctx:
        def __enter__(self):
            return _Resp()

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(fc.httpx, "stream", lambda m, u, **kw: _Ctx())
    dest = tmp_path / "x.parquet"
    with pytest.raises(fc.FuyaoError, match="403"):
        fc.FuyaoClient(api_key="k").download_dump("adjustment-factors", dest)
    assert not dest.exists() and not list(tmp_path.glob("*.part"))


# ---- 时区与 release 解析 ----


def test_shanghai_midnight_ms_roundtrip():
    """上海零点戳 ↔ 日期: +8h 换算不得偏移一天。"""
    d = date(2026, 6, 12)
    assert fp._date_of_ms(_sh_ms(d)) == d
    assert fp._ms_of_date(d) == _sh_ms(d)


def test_release_of_extracts_from_presigned_url():
    url = "https://o.thsi.cn/x/market-dump/adj/releases/20260828/a.parquet?sig=1"
    assert fp._release_of(url) == "20260828"
    assert fp._release_of("https://no-release-here/x") == "unknown"


# ---- 设置页试拉: daily / adj_factor ----


def test_test_dataset_daily_preview(monkeypatch):
    # 实现窗口为 now-30天..now, 数据日期必须相对今天生成; 硬编码日期会随时间
    # 滑出窗口 (2026-09-27 起原硬编码 8/27 的版本必然失败)。
    d1 = date.today() - timedelta(days=2)
    d2 = date.today() - timedelta(days=1)
    bars = {"000001.SZ": [_bar(d1, 11.05), _bar(d2, 11.65)]}
    provider = _hist_provider(monkeypatch, _FakeHistClient(bars))
    out = provider.test_dataset("daily", ["000001.SZ"])
    assert out["provider"] == "fuyao" and out["dataset"] == "daily"
    assert out["rows"] == 2
    assert out["preview"][0]["date"] == d1.isoformat()  # date → ISO 字符串


def test_test_dataset_adj_factor_preview(monkeypatch):
    events = [("600519.SH", date(2026, 6, 12), 0.68, 0.0, 0.0, 0.0)]
    bars = {"600519.SH": [_bar(date(2026, 6, 11), 32.8), _bar(date(2026, 6, 12), 32.0)]}
    provider = _adj_provider(monkeypatch, events, bars)
    out = provider.test_dataset("adj_factor", ["600519.SH"])
    assert out["rows"] == 1
    assert out["preview"][0]["trade_date"] == "2026-06-12"
    assert out["preview"][0]["ex_factor"] == pytest.approx(32.8 / 32.12)
