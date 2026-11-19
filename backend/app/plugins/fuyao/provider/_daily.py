"""FuyaoProvider 日K mixin: 批量接口 / 10d dump / 10 年 dump 的分流与回退。"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from app.data_providers.base import ProviderRequestError
from app.data_providers.normalizer import DAILY_COLS, normalize_daily
from app.indicators.pipeline import filter_halt_days
from app.plugins.fuyao.client import (
    HISTORICAL_BATCH_LIMIT,
    FuyaoError,
    FuyaoUnknownSymbolError,
)

from ._common import (
    _DAILY10_DUMP_KIND,
    _DAILY_DUMP_COLUMNS,
    _DAILY_DUMP_KIND,
    _HIST_MAX_SPAN_MS,
    _RECENT_DUMP_DAYS,
    _SH_MS,
    _dump_covers,
    _dump_date_range,
    _kline_rows,
    _ms_of_date,
    _tail_ok,
    logger,
)

_fp = sys.modules[__package__]


class FuyaoProviderDaily:
    """日K: 小查询走批量接口, 大窗口走 dump, 缺口逐标的兜底。"""

    # 下列成员由 FuyaoProvider 组装类中的 Base mixin 提供（分包后 mypy 不可见）。
    name: Any  # provider 名称, Base 类属性
    _get_client: Any  # 客户端工厂
    _ensure_dump: Any  # 小体量 dump 整读
    _ensure_dump_path: Any  # 大 dump 落盘路径
    _daily10_for_window: Any  # 近端 10d 日K dump

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """A 股日K → 内部契约。原始价(adjust=none 锁定)、volume 股→手(floor /100)。

        - ≤10 只: 优先股票批量历史接口,每次最多10只,避免小查询下载全市场整包;
        - 更大范围的近端窗口(跨度 ≤12 天): daily-k-10d dump(约 1MB);
        - 更大范围的深窗口: daily-k 10 年全量 dump(约 172MB, 缓存覆盖起点即复用, 不追新 release),
          末端缺口由 10d dump 补尾 — 全市场深回填从"逐票 34 分钟"降为"一次下载+秒级筛选";
        - dump 缺口或不可用时走批量接口;漏标的或批量失败回退单标的 historical,
          10 年自动分片、节流和进度回调保留;请求均失败时明确报错,不发布部分结果。
        """
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
        return pl.concat(chunks, how="diagonal_relaxed") if chunks else pl.DataFrame()

    @staticmethod
    def _daily_iter_args(
        symbols: list[str], start_time: datetime | None, end_time: datetime | None, asset_type: str
    ) -> tuple[list[str], date, date, set[str], bool] | None:
        if not symbols or asset_type != "stock":
            return None
        deduped = list(dict.fromkeys(symbols))
        end_dt = end_time or datetime.now()
        start_dt = start_time or (end_dt - timedelta(days=365))
        start_d, end_d = start_dt.date(), end_dt.date()
        return deduped, start_d, end_d, set(deduped), len(deduped) <= _fp._HIST_SMALL_SYMBOL_LIMIT

    def _recent_dump_frame(
        self, start_d: date, end_d: date, symset: set[str]
    ) -> pl.DataFrame | None:
        try:
            dump = self._daily10_for_window(start_d, end_d)
            if _dump_covers(dump, start_d, end_d):
                return self._daily_from_dump(dump, symset, start_d, end_d)
        except FuyaoError as e:
            logger.warning("扶摇 10d dump 不可用: %s", e)
        return None

    def _plan_daily_sources(
        self,
        start_d: date,
        end_d: date,
        symset: set[str],
        prefer_api: bool,
    ) -> tuple[list[tuple[str, date, date]], object]:
        dump_info = None if prefer_api else self._daily_dump_info()
        sources: list[tuple[str, date, date]] = []
        if not dump_info:
            return [("api", start_d, end_d)], None
        _, dump_min, dump_max = dump_info
        if start_d < dump_min:
            sources.append(("api", start_d, min(end_d, dump_min - timedelta(days=1))))
        overlap_start, overlap_end = max(start_d, dump_min), min(end_d, dump_max)
        if overlap_start <= overlap_end:
            sources.append(("dump", overlap_start, overlap_end))
        tail_start = max(start_d, dump_max + timedelta(days=1))
        if tail_start <= end_d and not _tail_ok(end_d, dump_max):
            try:
                ten = self._daily10_for_window(tail_start, end_d)
                ten_dates = pl.from_epoch(
                    ten["date_ms"].cast(pl.Int64) + _SH_MS, time_unit="ms"
                ).dt.date()
                ten_min, ten_max = ten_dates.min(), ten_dates.max()
            except (FuyaoError, KeyError):
                ten_min = ten_max = None
            if (
                ten_min is not None
                and ten_min <= dump_max + timedelta(days=1)
                and _tail_ok(end_d, ten_max)
            ):
                sources.append(("10d", tail_start, end_d))
            else:
                # 多年 dump 与请求终点之间存在不可验证的缺口,不能返回半段数据。
                sources = [("api", start_d, end_d)]
        return sources, dump_info

    @staticmethod
    def _daily_source_total(sources, dump_info, symbol_count: int) -> int:
        dump_batch_count = 0
        if dump_info:
            dump_rows = pq.ParquetFile(dump_info[0]).metadata.num_rows
            dump_batch_count = max(
                1, (dump_rows + _fp._DAILY_DUMP_BATCH_ROWS - 1) // _fp._DAILY_DUMP_BATCH_ROWS
            )
        api_batch_count = (symbol_count + _fp._HIST_SYMBOL_BATCH - 1) // _fp._HIST_SYMBOL_BATCH
        return sum(
            dump_batch_count if kind == "dump" else 1 if kind == "10d" else api_batch_count
            for kind, _, _ in sources
        )

    def _iter_daily_source(
        self, kind, source_start, source_end, dump_info, symbols: list[str], symset: set[str]
    ) -> Iterator[pl.DataFrame]:
        if source_start > source_end:
            return
        if kind == "dump":
            yield from self._iter_big_dump(dump_info[0], symset, source_start, source_end)
            return
        if kind == "10d":
            ten = self._ensure_dump(_DAILY10_DUMP_KIND, "daily_k_10d")
            yield self._daily_from_dump(ten, symset, source_start, source_end)
            return
        batches = [
            symbols[i : i + _fp._HIST_SYMBOL_BATCH]
            for i in range(0, len(symbols), _fp._HIST_SYMBOL_BATCH)
        ]
        for batch in batches:
            rows = self._historical_batch_rows(batch, source_start, source_end)
            yield normalize_daily(rows, source=self.name)

    def iter_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> Iterator[pl.DataFrame]:
        """分批产出日K,供历史同步逐批落盘,避免全市场结果累积在内存。"""
        args = self._daily_iter_args(symbols, start_time, end_time, asset_type)
        if args is None:
            return
        symbols, start_d, end_d, symset, prefer_api = args

        if not prefer_api and (end_d - start_d).days <= _RECENT_DUMP_DAYS:
            df = self._recent_dump_frame(start_d, end_d, symset)
            if df is not None:
                if on_chunk_done:
                    on_chunk_done(1, 1)
                if not df.is_empty():
                    yield df
                return

        sources, dump_info = self._plan_daily_sources(start_d, end_d, symset, prefer_api)
        total = self._daily_source_total(sources, dump_info, len(symbols))
        done = 0
        for kind, source_start, source_end in sources:
            for df in self._iter_daily_source(
                kind, source_start, source_end, dump_info, symbols, symset
            ):
                done += 1
                if on_chunk_done:
                    on_chunk_done(done, total)
                if not df.is_empty():
                    yield df

    def _daily_dump_info(self) -> tuple[Path, date, date] | None:
        """返回多年 dump 的路径和覆盖范围,不把大文件读入进程内存。"""
        for candidate in sorted(_fp._cache_dir().glob("daily_k__*.parquet"), reverse=True):
            try:
                dmin, dmax = _dump_date_range(candidate)
            except Exception:  # noqa: BLE001 - 候选dump文件损坏, 跳过换下一个
                continue
            if dmin is not None and dmax is not None:
                return candidate, dmin, dmax
        try:
            path = self._ensure_dump_path(_DAILY_DUMP_KIND, "daily_k")
            dmin, dmax = _dump_date_range(path)
        except FuyaoError as e:
            logger.warning("扶摇 10 年 dump 不可用, 回退单标的接口: %s", e)
            return None
        return (path, dmin, dmax) if dmin is not None and dmax is not None else None

    def _daily_from_dump(
        self, dump: pl.DataFrame, symset: set[str], start_d: date, end_d: date
    ) -> pl.DataFrame:
        df = dump.with_columns(
            pl.from_epoch(pl.col("date_ms") + _SH_MS, time_unit="ms").dt.date().alias("date")
        )
        return self._map_daily_dump(df, symset, start_d, end_d)

    def _iter_big_dump(
        self, path: Path, symset: set[str], start_d: date, end_d: date
    ) -> Iterator[pl.DataFrame]:
        """按固定 record batch 读取多年 dump,不做单次全量 collect。"""
        parquet = pq.ParquetFile(path)
        columns = [name for name in _DAILY_DUMP_COLUMNS if name in parquet.schema.names]
        for batch in parquet.iter_batches(
            batch_size=_fp._DAILY_DUMP_BATCH_ROWS,
            columns=columns,
        ):
            raw = pl.from_arrow(batch)
            if raw.is_empty() or "date_ms" not in raw.columns or "thscode" not in raw.columns:
                yield pl.DataFrame()
                continue
            start_ms, end_ms = _ms_of_date(start_d), _ms_of_date(end_d)
            raw = raw.filter(
                (pl.col("date_ms") >= start_ms)
                & (pl.col("date_ms") <= end_ms)
                & pl.col("thscode").is_in(sorted(symset))
            )
            if raw.is_empty():
                yield pl.DataFrame()
                continue
            raw = raw.with_columns(
                pl.from_epoch(pl.col("date_ms") + _SH_MS, time_unit="ms").dt.date().alias("date")
            )
            yield self._map_daily_dump(raw, symset, start_d, end_d)

    def _map_daily_dump(
        self, df: pl.DataFrame, symset: set[str], start_d: date, end_d: date
    ) -> pl.DataFrame:
        """dump 原始行 → 内部日K契约(股→手、字段重命名、停牌过滤)。"""
        if "adjusted" in df.columns:
            df = df.filter(pl.col("adjusted") == "none")  # 防御: dump 变为复权口径时拒绝落库
        out = (
            df.filter(
                (pl.col("date") >= start_d)
                & (pl.col("date") <= end_d)
                & pl.col("thscode").is_in(sorted(symset))
            )
            .with_columns(
                (pl.col("volume") / 100.0).floor().alias("volume"),  # 股 → 手
                pl.col("thscode").alias("symbol"),
            )
            .rename(
                {
                    "open_price": "open",
                    "high_price": "high",
                    "low_price": "low",
                    "close_price": "close",
                    "turnover": "amount",
                }
            )
        )
        out = filter_halt_days(out)
        cols = [c for c in DAILY_COLS if c in out.columns]
        return out.select(cols).sort(["symbol", "date"]) if not out.is_empty() else out.select(cols)

    def _historical_batch_rows(self, symbols: list[str], start_d: date, end_d: date) -> list[dict]:
        """每组至多10只、每片至多10年;只为缺组回退,合法空组不重复拉取。"""
        client = self._get_client()
        out: list[dict] = []
        unknown_symbols: set[str] = set()
        start, end = _ms_of_date(start_d), _ms_of_date(end_d)
        while start <= end:
            chunk_end = min(end, start + _HIST_MAX_SPAN_MS - 1)
            for offset in range(0, len(symbols), HISTORICAL_BATCH_LIMIT):
                batch = [
                    s
                    for s in symbols[offset : offset + HISTORICAL_BATCH_LIMIT]
                    if s not in unknown_symbols
                ]
                if not batch:
                    continue
                try:
                    grouped = client.historical_kline_batch(batch, start, chunk_end)
                except FuyaoError as err:
                    logger.warning("扶摇批量日K不可用,回退单标的接口 (%d 只): %s", len(batch), err)
                    grouped = {}
                time.sleep(_fp._HIST_INTERVAL_S)
                for symbol in batch:
                    if symbol in grouped:
                        bars = grouped[symbol]
                    else:
                        try:
                            bars = client.historical_kline(symbol, start, chunk_end, adjust="none")
                        except FuyaoError as err:
                            if isinstance(err, FuyaoUnknownSymbolError) and err.symbol == symbol:
                                unknown_symbols.add(symbol)
                                logger.warning(
                                    "扶摇历史日K未覆盖 %s,本轮跳过并保留本地历史", symbol
                                )
                                time.sleep(_fp._HIST_INTERVAL_S)
                                continue
                            # 同步层会丢弃暂存帧,避免一部分标的成功后误报整轮完成。
                            raise ProviderRequestError(f"扶摇日K补查失败 {symbol}: {err}") from err
                        time.sleep(_fp._HIST_INTERVAL_S)
                    out.extend(_kline_rows(symbol, bars))
            start = chunk_end + 1
        return out

    def _historical_bars(self, symbol: str, start_d: date, end_d: date) -> list[dict]:
        """按 ≤10 年窗口分片拉取单标的原始日K。中途失败软返回已得行, 不抛出。"""
        out: list[dict] = []
        s = _ms_of_date(start_d)
        e = _ms_of_date(end_d)
        while s <= e:
            chunk_end = min(e, s + _HIST_MAX_SPAN_MS - 1)
            try:
                out.extend(self._get_client().historical_kline(symbol, s, chunk_end, adjust="none"))
            except FuyaoError as err:
                logger.warning("扶摇日K拉取失败 %s [%s ~ %s]: %s", symbol, start_d, end_d, err)
                break
            if s + _HIST_MAX_SPAN_MS <= e:
                time.sleep(_fp._HIST_INTERVAL_S)
            s = chunk_end + 1
        return out
