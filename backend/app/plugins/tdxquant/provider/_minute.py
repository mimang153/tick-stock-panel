"""TdxQuantProvider 分钟/全量分钟 mixin: 1m 历史、修复轮与稳态增量轮。"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any

import polars as pl

from app.plugins.tdxquant.client import TdxQuantAuthError, TdxQuantError

from ._common import (
    _BEIJING,
    _MINUTE_BARS_PER_DAY,
    _MINUTE_OUTPUT_ROWS,
    _MINUTE_OUTPUT_SYMBOLS,
    _MINUTE_SCHEMA,
    _minute_count_for_window,
    _minute_rows,
    _warn_minute_unparsed,
    logger,
)

_fp = sys.modules[__package__]


class TdxQuantProviderMinute:
    """分钟 K 与全量分钟: 鉴权熔断共用基础 mixin 的状态。"""

    # 由 TdxQuantProviderBase / 组装类提供: mixin 拆分后 mypy 不跨兄弟类解析属性
    _get_client: Any
    _auth_cooldown_active: Any
    _trip_auth_breaker: Any
    _is_blank_error: Any
    _AUTH_COOLDOWN_S: Any
    _AUTH_BLANK_STREAK: Any

    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """分钟K → 内部契约(symbol/datetime 北京墙钟 naive/ohlcv/volume 手/amount 元)。

        period=1m count 模式单标的拉取(实测 240 根/日, count≤9600 正常);
        freq 仅支持 1m(上游有 5m 等档位但项目契约按 1m 路由), 其余空手而归还上层回退;
        ETF/指数分钟未实测覆盖, asset_type 仅接受 stock。
        """
        chunks = list(
            self.iter_minute(
                symbols,
                start_time=start_time,
                end_time=end_time,
                asset_type=asset_type,
                freq=freq,
                on_chunk_done=on_chunk_done,
            )
        )
        parts = [df for df in chunks if not df.is_empty()]
        return (
            pl.concat(parts, how="diagonal_relaxed")
            if parts
            else pl.DataFrame(schema=_MINUTE_SCHEMA)
        )

    def iter_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> Iterator[pl.DataFrame]:
        if not symbols or asset_type != "stock" or freq != "1m":
            if on_chunk_done:
                on_chunk_done(0, 0)
            return
        client = self._get_client()
        # 同步窗口来自 cn_now/单股 API, 可能带时区; 原生接口和返回列均用北京墙钟。
        if start_time is not None and start_time.tzinfo is not None:
            start_time = start_time.astimezone(_BEIJING).replace(tzinfo=None)
        end_dt = end_time or _fp.datetime.now(_BEIJING)
        if end_dt.tzinfo is not None:
            end_dt = end_dt.astimezone(_BEIJING).replace(tzinfo=None)
        count = _minute_count_for_window(start_time, end_dt)
        parts: list[pl.DataFrame] = []
        pending_rows = 0
        for i, symbol in enumerate(symbols):
            # 请求失败应中止历史同步并标记失败, 不得作为无数据继续遍历全市场。
            raw = client.his_dats(symbol, end_dt, count, period="1m")
            rows = _minute_rows(symbol, raw, start_time, end_dt)
            if not rows and raw.get("Date") and not _minute_rows(symbol, raw, None, None):
                _warn_minute_unparsed(symbol, raw)  # 去掉窗口过滤仍 0 行才是解析失败
            part = pl.DataFrame(rows, schema=_MINUTE_SCHEMA)
            if parts and pending_rows + part.height > _MINUTE_OUTPUT_ROWS:
                yield pl.concat(parts, how="diagonal_relaxed")
                parts, pending_rows = [], 0
            if not part.is_empty():
                parts.append(part)
                pending_rows += part.height
            if len(parts) >= _MINUTE_OUTPUT_SYMBOLS or pending_rows >= _MINUTE_OUTPUT_ROWS:
                yield pl.concat(parts, how="diagonal_relaxed")
                parts, pending_rows = [], 0
            if on_chunk_done:
                on_chunk_done(i + 1, len(symbols))
            time.sleep(_fp._MINUTE_INTERVAL_S)
        if parts:
            yield pl.concat(parts, how="diagonal_relaxed")

    def get_intraday_batch(
        self,
        symbols: list[str],
        count: int = 300,
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        """全量分钟修复轮: 当日窗口逐标的拉取, 只保留今日 1m 行。

        count 模式尾部天然带前一日余量, 按 today_only 过滤; 单标的请求
        240+count 余量根。全市场一轮 5579 次调用(含节流约 10 分钟量级),
        由 minute_refresh 以修复轮节奏调度。
        """
        if not symbols or asset_type != "stock":
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        if self._auth_cooldown_active():
            logger.warning(
                "TdxQuant 修复轮跳过: 凭据疑似失效熔断中 (最多 %d 分钟), 请重新保存 Key",
                int(self._AUTH_COOLDOWN_S // 60),
            )
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        client = self._get_client()
        now = _fp.datetime.now()
        today = now.date()
        fetch = max(_MINUTE_BARS_PER_DAY, min(count, 480)) + _MINUTE_BARS_PER_DAY
        out: list[pl.DataFrame] = []
        blank_streak = 0
        failures = 0
        first_error = ""
        for symbol in symbols:
            try:
                raw = client.his_dats(symbol, now, fetch, period="1m")
            except TdxQuantAuthError as e:
                self._trip_auth_breaker(1, str(e))
                break
            except TdxQuantError as e:
                failures += 1
                first_error = first_error or str(e)
                if self._is_blank_error(e):
                    blank_streak += 1
                    if blank_streak >= self._AUTH_BLANK_STREAK:
                        self._trip_auth_breaker(blank_streak, first_error)
                        break
                else:
                    blank_streak = 0
                logger.debug("TdxQuant 修复轮 %s 拉取失败: %s", symbol, e)
                continue
            blank_streak = 0
            rows = _minute_rows(symbol, raw, None, None, today_only=today)
            if rows:
                out.append(pl.DataFrame(rows, schema=_MINUTE_SCHEMA))
            elif raw.get("Date") and not _minute_rows(symbol, raw, None, None):
                _warn_minute_unparsed(symbol, raw)  # 盘前返回全为前一日属正常, 仅解析失败才告警
            time.sleep(_fp._MINUTE_INTERVAL_S)
        if failures:
            logger.warning(
                "TdxQuant 修复轮: %d/%d 只标的失败 (首个错误: %s)",
                failures,
                len(symbols),
                first_error,
            )
        return (
            pl.concat(out, how="diagonal_relaxed") if out else pl.DataFrame(schema=_MINUTE_SCHEMA)
        )

    def _intraday_targets(self, client: Any, symbols: list[str] | None) -> list[str]:
        """增量轮标的解析: 显式列表优先, 否则取全市场股票池; 枚举失败返回空。"""
        targets = list(symbols) if symbols else []
        if not targets:
            try:
                targets = client.stock_universe()
            except TdxQuantError as e:
                logger.warning("TdxQuant 增量轮股票池枚举失败: %s", e)
                return []
        return targets

    def get_intraday_latest(self, symbols: list[str] | None = None, count: int = 3) -> pl.DataFrame:
        """全量分钟稳态增量轮: 每只标的最新 count 根 1m(symbols 缺省 = 全市场股票池)。

        与修复轮同为逐标的 count 调用(无批量端点), 差别在根数少、负载轻;
        失败标的跳过, 空轮由调用方按空处理。
        """
        if self._auth_cooldown_active():
            logger.warning(
                "TdxQuant 增量轮跳过: 凭据疑似失效熔断中 (最多 %d 分钟), 请重新保存 Key",
                int(self._AUTH_COOLDOWN_S // 60),
            )
            return pl.DataFrame(schema=_MINUTE_SCHEMA)
        client = self._get_client()
        targets = self._intraday_targets(client, symbols)
        count = max(1, min(int(count), 30))
        now = _fp.datetime.now()
        out: list[pl.DataFrame] = []
        blank_streak = 0
        failures = 0
        first_error = ""
        for j, symbol in enumerate(targets):
            try:
                raw = client.his_dats(symbol, now, count, period="1m")
            except TdxQuantAuthError as e:
                self._trip_auth_breaker(1, str(e))
                break
            except TdxQuantError as e:
                failures += 1
                first_error = first_error or str(e)
                if self._is_blank_error(e):
                    blank_streak += 1
                    if blank_streak >= self._AUTH_BLANK_STREAK:
                        self._trip_auth_breaker(blank_streak, first_error)
                        break
                else:
                    blank_streak = 0
                logger.debug("TdxQuant 增量轮 %s 拉取失败: %s", symbol, e)
                continue
            blank_streak = 0
            rows = _minute_rows(symbol, raw, None, None)
            if rows:
                out.append(pl.DataFrame(rows, schema=_MINUTE_SCHEMA))
            elif raw.get("Date"):
                _warn_minute_unparsed(symbol, raw)
            if j % 1000 == 999:
                logger.info("TdxQuant 增量轮进度: %d/%d", j + 1, len(targets))
            time.sleep(_fp._MINUTE_INTERVAL_S)
        if failures:
            logger.warning(
                "TdxQuant 增量轮: %d/%d 只标的失败 (首个错误: %s)",
                failures,
                len(targets),
                first_error,
            )
        return (
            pl.concat(out, how="diagonal_relaxed") if out else pl.DataFrame(schema=_MINUTE_SCHEMA)
        )
