"""KlineRepository mixin: _ComputeScanMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    date,
    guarded_collect,
    logger,
    pl,
    scan_enriched_parquet,
)


class _ComputeScanMixin(_RepoMixinBase):
    # ================================================================
    # Polars 查询内部方法
    # ================================================================

    def _read_daily_from_ck(
        self,
        table: str,
        symbols: list[str],
        start: date,
        end: date,
        columns: list[str] | None,
        sort_cols: list[str],
    ) -> pl.DataFrame | None:
        """CK 优先读取日K；无门面/查不到数据时返回 None 走本地兼容路径。"""
        store = self._ck_store()
        if store is None:
            return None
        try:
            df = store.read_daily(table, symbols=symbols, start=start, end=end)
        except Exception as e:  # noqa: BLE001 - 读 CK 失败要降级到本地, 不能让页面报错
            logger.warning("CK 日K读取失败(%s): %s", table, e)
            return None
        if df.is_empty():
            return None
        df = df.sort(sort_cols)
        if columns:
            df = df.select([c for c in columns if c in df.columns])
        return df

    def _compute_enriched_range(self, df: pl.DataFrame) -> pl.DataFrame:
        """对14列enriched数据即时计算完整指标+信号。输入应含足够预热行数。"""
        from app.indicators.pipeline import (
            compute_indicators,
            compute_limit_signals,
            compute_signals,
            filter_halt_days,
        )

        if df.is_empty() or df.height < 2:
            return df
        # 兜底过滤历史脏数据中的停牌日 (close 可能被填充为前收盘价)
        df = filter_halt_days(df)
        if df.is_empty() or df.height < 2:
            return df
        try:
            df = compute_indicators(df)
            df = compute_signals(df)
            instruments = self.get_instruments()
            df = compute_limit_signals(
                df,
                instruments,
                historical_shares=self.get_historical_shares(),
            )
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("on-demand compute failed: %s", e)
        logger.debug(
            "on-demand enriched computed: rows=%d columns=%d has_deviate=%s has_metadata=%s",
            len(df),
            len(df.columns),
            "deviate_3d" in df.columns,
            "float_shares" in df.columns,
        )
        return df

    def _compute_index_enriched_range(self, df: pl.DataFrame) -> pl.DataFrame:
        """指数只计算通用技术指标和通用信号，跳过涨跌停/股本/市值逻辑。"""
        from app.indicators.pipeline import compute_indicators, compute_signals

        if df.is_empty() or df.height < 2:
            return df
        try:
            df = compute_indicators(df)
            df = compute_signals(df)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("index on-demand compute failed: %s", e)
        return df

    def _filter_cached(
        self, cached: pl.DataFrame, symbol: str, columns: list[str] | None
    ) -> pl.DataFrame:
        df = cached.filter(pl.col("symbol") == symbol)
        if columns and not df.is_empty():
            existing = [c for c in columns if c in df.columns]
            df = df.select(existing)
        return df

    def _filter_cached_batch(
        self, cached: pl.DataFrame, symbols: list[str], columns: list[str] | None
    ) -> pl.DataFrame:
        df = cached.filter(pl.col("symbol").is_in(symbols))
        if columns and not df.is_empty():
            existing = [c for c in columns if c in df.columns]
            df = df.select(existing)
        return df.sort(["symbol", "date"])

    def _scan_daily_symbol(
        self, symbol: str, start: date, end: date, columns: list[str] | None
    ) -> pl.DataFrame:
        ck = self._read_daily_from_ck(
            "kline_daily_enriched", [symbol], start, end, columns, ["date"]
        )
        if ck is not None:
            return ck
        try:
            lf = (
                scan_enriched_parquet(
                    self._enriched_glob, cast_options=pl.ScanCastOptions(integer_cast="allow-float")
                )
                .filter(
                    (pl.col("symbol") == symbol)
                    & (pl.col("date") >= start)
                    & (pl.col("date") <= end)
                )
                .sort("date")
            )
            if columns:
                schema_names = lf.collect_schema().names()
                existing = [c for c in columns if c in schema_names]
                lf = lf.select(existing)
            return guarded_collect(lf)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("日K查询失败: %s", e)
            return pl.DataFrame()

    def _scan_daily_batch(
        self, symbols: list[str], start: date, end: date, columns: list[str] | None
    ) -> pl.DataFrame:
        ck = self._read_daily_from_ck(
            "kline_daily_enriched", symbols, start, end, columns, ["symbol", "date"]
        )
        if ck is not None:
            return ck
        try:
            lf = (
                scan_enriched_parquet(
                    self._enriched_glob, cast_options=pl.ScanCastOptions(integer_cast="allow-float")
                )
                .filter(
                    (pl.col("symbol").is_in(symbols))
                    & (pl.col("date") >= start)
                    & (pl.col("date") <= end)
                )
                .sort(["symbol", "date"])
            )
            if columns:
                schema_names = lf.collect_schema().names()
                existing = [c for c in columns if c in schema_names]
                lf = lf.select(existing)
            return guarded_collect(lf)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("日K批量查询失败: %s", e)
            return pl.DataFrame()

    def _scan_index_daily_symbol(
        self, symbol: str, start: date, end: date, columns: list[str] | None
    ) -> pl.DataFrame:
        ck = self._read_daily_from_ck(
            "kline_index_enriched", [symbol], start, end, columns, ["date"]
        )
        if ck is not None:
            return ck
        try:
            lf = (
                scan_enriched_parquet(
                    self._index_enriched_glob,
                    cast_options=pl.ScanCastOptions(integer_cast="allow-float"),
                )
                .filter(
                    (pl.col("symbol") == symbol)
                    & (pl.col("date") >= start)
                    & (pl.col("date") <= end)
                )
                .sort("date")
            )
            if columns:
                schema_names = lf.collect_schema().names()
                existing = [c for c in columns if c in schema_names]
                lf = lf.select(existing)
            return guarded_collect(lf)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("指数日K查询失败: %s", e)
            return pl.DataFrame()

    def _scan_etf_daily_symbol(
        self, symbol: str, start: date, end: date, columns: list[str] | None
    ) -> pl.DataFrame:
        ck = self._read_daily_from_ck("kline_etf_enriched", [symbol], start, end, columns, ["date"])
        if ck is not None:
            return ck
        try:
            lf = (
                scan_enriched_parquet(
                    self._etf_enriched_glob,
                    cast_options=pl.ScanCastOptions(integer_cast="allow-float"),
                )
                .filter(
                    (pl.col("symbol") == symbol)
                    & (pl.col("date") >= start)
                    & (pl.col("date") <= end)
                )
                .sort("date")
            )
            if columns:
                schema_names = lf.collect_schema().names()
                existing = [c for c in columns if c in schema_names]
                lf = lf.select(existing)
            return guarded_collect(lf)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("ETF 日K查询跳过: %s", e)
            return pl.DataFrame()

    def _merge_cached_and_scan(
        self,
        cached: pl.DataFrame,
        cache_date: date,
        symbol: str,
        start: date,
        end: date,
        columns: list[str] | None,
    ) -> pl.DataFrame:
        """合并缓存部分 + scan 历史部分。

        历史部分用 strict < cache_date, 避免与缓存重复。
        两部分 schema 可能不一致 (增量 vs 全量), concat 前对齐列。
        """
        hist = self._scan_daily_symbol(symbol, start, cache_date, columns)
        cached_part = self._filter_cached(cached, symbol, columns)
        if hist.is_empty():
            return cached_part
        if cached_part.is_empty():
            return hist
        # 去重: 历史部分可能包含 cache_date, 去掉后再合并
        hist = hist.filter(pl.col("date") < cache_date)
        # 对齐列: 取交集, 统一类型
        common_cols = [c for c in hist.columns if c in cached_part.columns]
        hist = hist.select(common_cols)
        cached_part = cached_part.select(common_cols)
        # 统一类型: 历史可能是 Float64, 缓存可能是 Int64, 统一为 cast
        for c in common_cols:
            if hist[c].dtype != cached_part[c].dtype:
                # 统一到更宽的类型
                target = hist[c].dtype if hist.height > cached_part.height else cached_part[c].dtype
                hist = hist.with_columns(pl.col(c).cast(target))
                cached_part = cached_part.with_columns(pl.col(c).cast(target))
        return pl.concat([hist, cached_part])
