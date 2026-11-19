"""FuyaoProvider 除权因子 mixin: 事件 dump + 本地日K配价 + 交易所公式推导。"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any

import polars as pl

from app.plugins.fuyao.client import FuyaoError

from ._common import (
    _ADJ_DUMP_KIND,
    _DAILY10_DUMP_KIND,
    _PREV_CLOSE_BACKDAYS,
    _SH_MS,
    _date_of_ms,
    _price_limit,
    _ref_price,
    _to_float,
    logger,
)

_fp = sys.modules[__package__]


class FuyaoProviderAdj:
    """除权因子: 单事件比值(非累积), 明细列透传, 涨跌停自检剔除异常。"""

    # 下列成员由 FuyaoProvider 组装类中的 Base / Daily mixin 提供（分包后 mypy 不可见）。
    _dump_memo: Any  # dump 帧进程内 memo
    _ensure_dump: Any  # 小体量 dump 整读
    _ensure_dump_path: Any  # 大 dump 落盘路径
    _ensure_daily_big_dump: Any  # 10 年全量日K dump
    _historical_bars: Any  # 逐标的日K兜底

    @staticmethod
    def _adj_factor_float(x) -> float | None:
        return None if x is None else float(x)

    def _adj_closes_for_symbol(
        self, sym: str, first_ex: date, last_ex: date, closes_by_sym: dict
    ) -> tuple[dict | None, int]:
        closes = closes_by_sym.get(sym)
        if not closes or min(closes) > first_ex:
            # dump 无该标的 / 覆盖不到首个事件前 → 单标的接口兜底
            closes = self._fetch_closes(
                sym, first_ex - timedelta(days=_PREV_CLOSE_BACKDAYS), last_ex
            )
            time.sleep(_fp._HIST_INTERVAL_S)
            return closes, 1
        return closes, 0

    def _derive_adj_row(self, sym: str, ev: dict, days: list[date], closes: dict) -> dict | None:
        exd: date = ev["ex_date"]
        prev_days = [d for d in days if d < exd]
        if not prev_days:
            return None  # 日K窗口未覆盖的远古事件(如 10 年分片边界之外)
        p = closes[prev_days[-1]]
        ref = _ref_price(p, ev["dividend"], ev["bonus"], ev["allot"], ev["allot_price"])
        if ref is None or ref <= 0:
            logger.warning(
                "扶摇除权因子: %s %s 参考价无法计算(P=%s D=%s S=%s AR=%s AP=%s), 跳过",
                sym,
                exd,
                p,
                ev["dividend"],
                ev["bonus"],
                ev["allot"],
                ev["allot_price"],
            )
            return None
        factor = p / ref
        ex_days = [d for d in days if d >= exd]
        if ex_days:  # 涨跌停自检: 错误因子会令复权后除权日涨跌幅超出板限
            ret = closes[ex_days[0]] / (p / factor) - 1.0
            if abs(ret) > _price_limit(sym) + 0.02:
                logger.warning(
                    "扶摇除权因子自检剔除 %s %s: 复权后除权日涨跌幅 %.1f%% 超出涨跌停",
                    sym,
                    exd,
                    ret * 100,
                )
                return None
        f = self._adj_factor_float
        return {
            "symbol": sym,
            "trade_date": exd,
            "ex_factor": factor,
            "dividend": f(ev["dividend"]),
            "bonus": f(ev["bonus"]),
            "allot": f(ev["allot"]),
            "allot_price": f(ev["allot_price"]),
            "prev_close": f(p),
        }

    @staticmethod
    def _finalize_adj_rows(out_rows: list[dict], schema: dict) -> pl.DataFrame:
        if not out_rows:
            return pl.DataFrame(schema=schema)
        return (
            pl.DataFrame(out_rows, schema=schema)
            .unique(subset=["symbol", "trade_date"], keep="last")
            .sort(["symbol", "trade_date"])
        )

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """A 股除权因子 → 内部契约(symbol/trade_date/ex_factor, 单事件比值非累积)
        + 事件明细列(dividend/bonus/allot/allot_price/prev_close, 可空):
        供等差显示投影(Σ未来每股分红)与官方金额全精度因子链重建使用,
        明细取自事件 dump 原始成分, 不再在推导后丢弃。

        数据链: adjustment-factors 全量事件 dump → 清洗(时区 +8h / 滤全零 / 同日成分合并)
        → 前收盘价从本地日K dump 一次取齐(缺价标的回退单标的接口) → 交易所公式推导
        → 涨跌停自检剔除异常。
        未来已公告事件无前收盘, 留给滚动增量窗口(15 天)自然补上。
        """
        schema = {
            "symbol": pl.String,
            "trade_date": pl.Date,
            "ex_factor": pl.Float64,
            "dividend": pl.Float64,
            "bonus": pl.Float64,
            "allot": pl.Float64,
            "allot_price": pl.Float64,
            "prev_close": pl.Float64,
        }
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=schema)
        try:
            events = self._load_adj_events(set(symbols), start_time, end_time)
        except FuyaoError as e:
            logger.warning("扶摇除权因子 dump 加载失败: %s", e)
            return pl.DataFrame(schema=schema)
        if events.is_empty():
            return pl.DataFrame(schema=schema)

        out_rows: list[dict] = []
        syms = sorted(events["symbol"].unique().to_list())
        # 前收盘价优先从本地日K dump 一次取齐(全市场配价从逐标的 ~13 分钟降为秒级),
        # 大 dump 不可用或个别标的缺价时回退单标的接口(节流保留)。
        bounds = events.group_by("symbol").agg(
            pl.col("ex_date").min().alias("first_ex"),
            pl.col("ex_date").max().alias("last_ex"),
        )
        closes_source = self._closes_from_dumps(bounds)
        closes_by_sym = closes_source or {}
        fallbacks = 0
        for i, sym in enumerate(syms):
            evs = events.filter(pl.col("symbol") == sym).sort("ex_date")
            first_ex: date = evs["ex_date"][0]
            last_ex: date = evs["ex_date"][-1]
            closes, used_fallback = self._adj_closes_for_symbol(
                sym, first_ex, last_ex, closes_by_sym
            )
            fallbacks += used_fallback
            if not closes:
                logger.warning("扶摇除权因子: %s 原始日K为空, 跳过其 %d 个事件", sym, evs.height)
                if on_chunk_done:
                    on_chunk_done(i + 1, len(syms))
                continue
            days = sorted(closes)
            for ev in evs.iter_rows(named=True):
                row = self._derive_adj_row(sym, ev, days, closes)
                if row is not None:
                    out_rows.append(row)
            if on_chunk_done:
                on_chunk_done(i + 1, len(syms))
        if closes_source is not None:
            logger.info(
                "扶摇除权因子: 本地 dump 配价 %d/%d 标的, 回退接口 %d 标的",
                len(closes_source),
                len(syms),
                fallbacks,
            )
        return self._finalize_adj_rows(out_rows, schema)

    def _load_adj_events(
        self,
        symset: set[str],
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> pl.DataFrame:
        """事件 dump → 清洗 → 窗口过滤。返回 symbol/ex_date/dividend/bonus/allot/allot_price。

        清洗规则(对拍实证, 见 2026-08 验证记录):
        - ex_date_ms 为上海零点戳, +8h 转日期;
        - 全零事件行(疑似特殊事件, dump 未给成分)过滤;
        - 同日拆行(如分红/送转各一行)按成分合并后推导, 顺序不可反;
        - 配股但配股价缺失 → 无法推导, 过滤。
        """
        today = datetime.now().date()
        df = self._ensure_dump(_ADJ_DUMP_KIND, "adj_factors").rename({"thscode": "symbol"})
        df = (
            df.with_columns(
                pl.from_epoch(pl.col("ex_date_ms") + _SH_MS, time_unit="ms")
                .dt.date()
                .alias("ex_date"),
                pl.col("dividend_per_share").fill_null(0.0),
                pl.col("per_share_bonus").fill_null(0.0),
                pl.col("allotment_ratio").fill_null(0.0),
                pl.col("allotment_price").fill_null(0.0),
            )
            .filter(
                (pl.col("ex_date") <= today)  # 未来已公告事件无前收盘, 留给滚动增量
                & (
                    (pl.col("dividend_per_share") != 0)
                    | (pl.col("per_share_bonus") != 0)
                    | (pl.col("allotment_ratio") != 0)
                )
            )
            .group_by("symbol", "ex_date")
            .agg(
                pl.col("dividend_per_share").sum().alias("dividend"),
                pl.col("per_share_bonus").sum().alias("bonus"),
                pl.col("allotment_ratio").sum().alias("allot"),
                pl.col("allotment_price").max().alias("allot_price"),  # 同日拆行共享配股价
            )
            .filter(~((pl.col("allot") > 0) & (pl.col("allot_price") <= 0)))
            .filter(pl.col("symbol").is_in(sorted(symset)))
        )
        if start_time is not None:
            df = df.filter(pl.col("ex_date") >= start_time.date())
        if end_time is not None:
            df = df.filter(pl.col("ex_date") <= end_time.date())
        return df.select("symbol", "ex_date", "dividend", "bonus", "allot", "allot_price")

    def _fetch_closes(self, symbol: str, start_d: date, end_d: date) -> dict[date, float]:
        rows = self._historical_bars(symbol, start_d, end_d)
        out: dict[date, float] = {}
        for r in rows:
            d = _date_of_ms(r.get("date_ms"))
            c = _to_float(r.get("close_price"))
            if d is not None and c is not None:
                out[d] = c
        return out

    def _closes_from_dumps(self, bounds: pl.DataFrame) -> dict[str, dict[date, float]] | None:
        """从本地日K dump 一次取齐全部事件标的的收盘价。

        每标的开窗 [first_ex-30d, last_ex](与单标的接口同窗): 10 年大 dump 为
        主体, 10d dump 叠加补末端新鲜度(除权日当天的自检需要 ex 日收盘)。
        返回 symbol → {date: close}; 大 dump 不可用/读盘失败 → None 整轮回退。
        覆盖不到首个事件的个别标的不在此 dict 中(或首价晚于 first_ex),
        由 get_adj_factors 逐只回退单标的接口 — 按需, 不全有或全无。
        """
        try:
            path = self._ensure_daily_big_dump(None)
            if path is None:
                return None
            sources = [self._closes_scan(pl.scan_parquet(path))]
            ten = self._dump_memo.get(_DAILY10_DUMP_KIND)
            if ten is not None:
                sources.append(self._closes_scan(ten.lazy()))
            else:
                try:
                    p10 = self._ensure_dump_path(_DAILY10_DUMP_KIND, "daily_k_10d")
                except FuyaoError as e:
                    logger.info("扶摇 10d dump 不可用, 配价仅用 10 年 dump: %s", e)
                else:
                    sources.append(self._closes_scan(pl.scan_parquet(p10)))
            win = bounds.select(
                "symbol",
                (pl.col("first_ex") - pl.duration(days=_PREV_CLOSE_BACKDAYS)).alias("lo"),
                "last_ex",
            )
            # concat 顺序 = 叠加优先级: 同 (symbol, date) 时 10d dump(更新鲜)覆盖大 dump
            df = (
                pl.concat(sources, how="vertical_relaxed")
                .join(win.lazy(), on="symbol", how="inner")
                .filter((pl.col("date") >= pl.col("lo")) & (pl.col("date") <= pl.col("last_ex")))
                .unique(subset=["symbol", "date"], keep="last", maintain_order=True)
                .collect()
            )
            return {
                f["symbol"][0]: dict(zip(f["date"].to_list(), f["close"].to_list(), strict=True))
                for f in df.partition_by("symbol")
            }
        except Exception as e:  # noqa: BLE001 - 本地配价缓存损坏不致命, 回退逐标的接口
            logger.warning("扶摇除权因子本地配价失败, 回退单标的接口: %s", e)
            return None

    @staticmethod
    def _closes_scan(lf: pl.LazyFrame) -> pl.LazyFrame:
        """dump 行 → (symbol, date, close) lazy 投影; adjusted 列存在时锁 none。"""
        if "adjusted" in lf.collect_schema().names():
            lf = lf.filter(pl.col("adjusted") == "none")
        return lf.select(
            pl.col("thscode").alias("symbol"),
            pl.from_epoch(pl.col("date_ms").cast(pl.Int64) + _SH_MS, time_unit="ms")
            .dt.date()
            .alias("date"),
            pl.col("close_price").cast(pl.Float64).alias("close"),
        ).filter(pl.col("close").is_not_null())
