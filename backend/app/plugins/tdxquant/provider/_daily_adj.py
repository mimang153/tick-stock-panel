"""TdxQuantProvider 日K + 除权因子 mixin: count 模式取数与交易所公式推导。"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta
from typing import Any, cast

import polars as pl

from app.plugins.tdxquant.client import TdxQuantError

from ._common import (
    _ADJ_BACKDAYS,
    _ADJ_FUTURE_DAYS,
    _ADJ_SCHEMA,
    _DAILY_SCHEMA,
    _HIST_INTERVAL_S,
    _HIST_MAX_COUNT,
    _bars_for_window,
    _compact_date,
    _kline_rows,
    _price_limit,
    _ref_price,
    _to_float,
    logger,
)


class TdxQuantProviderDailyAdj:
    """日K与除权因子: 原始价锁定、volume 股→手、amount 万元→元。"""

    # 以下成员由 TdxQuantProviderBase / 组装类提供: mixin 拆分后 mypy 不跨兄弟类解析属性
    _get_client: Any

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        chunks = [
            df
            for df in self.iter_daily(
                symbols,
                start_time=start_time,
                end_time=end_time,
                asset_type=asset_type,
                on_chunk_done=on_chunk_done,
            )
            if not df.is_empty()
        ]
        return (
            pl.concat(chunks, how="diagonal_relaxed")
            if chunks
            else pl.DataFrame(schema=_DAILY_SCHEMA)
        )

    def iter_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> Iterator[pl.DataFrame]:
        """逐标的 count 模式拉日K → 内部契约(原始价, volume 手, amount 元)。

        ETF/指数日K未实测覆盖, asset_type 仅接受 stock, 其余空手而归(上层回退)。
        """
        if not symbols or asset_type != "stock":
            if on_chunk_done:
                on_chunk_done(0, 0)
            return
        client = self._get_client()
        start_d = start_time.date() if start_time else None
        end_d = end_time.date() if end_time else date.today()
        span = (end_d - start_d).days if start_d else _HIST_MAX_COUNT * 1.4
        count = _bars_for_window(int(span))
        for i, symbol in enumerate(symbols):
            try:
                raw = client.his_dats(symbol, end_d, count)
            except TdxQuantError as e:
                logger.warning("TdxQuant 日K %s 拉取失败: %s", symbol, e)
                raw = {}
            rows = _kline_rows(symbol, raw, start_d, end_d)
            if (
                rows
                and rows[0]["date"] is not None
                and start_d is not None
                and rows[0]["date"] > start_d
            ):
                logger.info(
                    "TdxQuant 日K %s: count=%d 仅覆盖到 %s(窗口起点 %s 之前的数据不在返回内)",
                    symbol,
                    count,
                    rows[0]["date"],
                    start_d,
                )
            yield pl.DataFrame(rows, schema=_DAILY_SCHEMA)
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
            time.sleep(_HIST_INTERVAL_S)

    # ---- adj_factor ----
    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """除权因子 → 内部契约(symbol/trade_date/ex_factor, 单事件比值非累积)
        + 事件明细列(dividend/bonus/allot/allot_price/prev_close, 每股口径)。

        数据链: divid_factors 事件接口(每 10 股口径 → ÷10 转每股) → 同一标的
        一次 count 模式日K取齐窗口内收盘价 → 交易所公式推导 → 涨跌停自检剔除。
        未来已公告事件无前收盘, 自然跳过, 滚动增量窗口补上。
        """
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=_ADJ_SCHEMA)
        client = self._get_client()
        start_d = start_time.date() if start_time else None
        end_d = end_time.date() if end_time else date.today()

        out_rows: list[dict] = []
        for i, symbol in enumerate(symbols):
            try:
                events = client.divid_factors(
                    symbol, start_d, end_d + timedelta(days=_ADJ_FUTURE_DAYS)
                )
            except TdxQuantError as e:
                logger.warning("TdxQuant 除权事件 %s 拉取失败: %s", symbol, e)
                # 失败不能当作「该股无除权」；由任务层标记失败，整轮不写入
                # 不完整因子，也避免权限/额度异常后继续请求其余数千只。
                raise
            events = [ev for ev in events if _compact_date(ev.get("Date")) is not None]
            if events:
                events.sort(key=lambda ev: str(ev.get("Date")))
                first_ex = cast(date, _compact_date(events[0]["Date"]))
                closes = self._close_map(
                    client, symbol, first_ex - timedelta(days=_ADJ_BACKDAYS), end_d
                )
                out_rows.extend(self._factor_rows(symbol, events, closes, start_d, end_d))
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
            time.sleep(_HIST_INTERVAL_S)
        if not out_rows:
            return pl.DataFrame(schema=_ADJ_SCHEMA)
        return (
            pl.DataFrame(out_rows, schema=_ADJ_SCHEMA)
            .unique(subset=["symbol", "trade_date"], keep="last")
            .sort(["symbol", "trade_date"])
        )

    def _close_map(self, client, symbol: str, start_d: date, end_d: date) -> dict[date, float]:
        """一次 count 模式日K → {交易日: 原始收盘价}(推导前收盘与自检共用)。"""
        try:
            raw = client.his_dats(symbol, end_d, _bars_for_window((end_d - start_d).days))
        except TdxQuantError as e:
            logger.warning("TdxQuant 除权配价 %s 日K失败: %s", symbol, e)
            raise
        out: dict[date, float] = {}
        dates = raw.get("Date") or []
        closes = raw.get("Close") or []
        for i, compact in enumerate(dates):
            d = _compact_date(compact)
            close = _to_float(closes[i]) if i < len(closes) else None
            if d is not None and close is not None:
                out[d] = close
        return out

    def _factor_rows(
        self,
        symbol: str,
        events: list[dict],
        closes: dict[date, float],
        start_d: date | None,
        end_d: date,
    ) -> list[dict]:
        days = sorted(closes)
        rows: list[dict] = []
        for ev in events:
            exd = _compact_date(ev.get("Date"))
            if exd is None or exd > end_d + timedelta(days=_ADJ_FUTURE_DAYS):
                continue
            if start_d is not None and exd < start_d:
                continue
            # 每 10 股口径 → 每股 (Bonus=10派X元, ShareBonus=10送Y股, Allotment=10配Z股)
            dividend = (_to_float(ev.get("Bonus")) or 0.0) / 10.0
            bonus = (_to_float(ev.get("ShareBonus")) or 0.0) / 10.0
            allot = (_to_float(ev.get("Allotment")) or 0.0) / 10.0
            allot_price = _to_float(ev.get("AllotPrice"))
            prev_days = [d for d in days if d < exd]
            if not prev_days:
                continue  # 日K窗口未覆盖(远古事件或未来已公告事件)
            p = closes[prev_days[-1]]
            ref = _ref_price(p, dividend, bonus, allot, allot_price or 0.0)
            if ref is None or ref <= 0:
                logger.warning(
                    "TdxQuant 除权因子: %s %s 参考价无法计算(P=%s D=%s S=%s AR=%s AP=%s), 跳过",
                    symbol,
                    exd,
                    p,
                    dividend,
                    bonus,
                    allot,
                    allot_price,
                )
                continue
            factor = p / ref
            ex_days = [d for d in days if d >= exd]
            if ex_days:  # 涨跌停自检: 错误因子会令复权后除权日涨跌幅超出板限
                ret = closes[ex_days[0]] / (p / factor) - 1.0
                if abs(ret) > _price_limit(symbol) + 0.02:
                    logger.warning(
                        "TdxQuant 除权因子自检剔除 %s %s: 复权后除权日涨跌幅 %.1f%% 超出涨跌停",
                        symbol,
                        exd,
                        ret * 100,
                    )
                    continue
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": exd,
                    "ex_factor": factor,
                    "dividend": dividend,
                    "bonus": bonus,
                    "allot": allot,
                    "allot_price": allot_price,
                    "prev_close": p,
                }
            )
        return rows
