"""KlineRepository mixin: _DailyMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    date,
    pl,
)


class _DailyMixin(_RepoMixinBase):
    # ================================================================
    # 热路径: Polars 查询 (Chart / Screener / Signals / Intraday)
    # ================================================================

    def get_daily(  # noqa: C901 - C901: 分支为业务多阶段编排, 拆分会割裂上下文; 单测已覆盖
        self,
        symbol: str,
        start: date,
        end: date,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """单股日K查询 — 从14列parquet读取后即时计算指标。"""
        from datetime import timedelta

        self._assert_daily_usable(symbols=[symbol], end=end)

        # 快路径: 请求的列全是 parquet 直接存储的列 (如迷你蜡烛图只要 OHLCV) →
        # scan + 列下推直接返回, 跳过 warmup(150天) 与 _compute_enriched_range 全套指标计算。
        # 仍用 enriched_latest 缓存覆盖最新日 (盘中更准), 只保留请求列。
        # 试探 scan 仅读请求的列; 缺列时回退到下方完整计算路径 (代价仅一次轻量 scan)。
        if columns:
            df = self._scan_daily_symbol(symbol, start, end, columns)
            if not df.is_empty() and all(c in df.columns for c in columns):
                cached, cache_date = self.get_enriched_latest(symbols=[symbol], end=end)
                if cached is not None and not cached.is_empty() and cache_date:  # noqa: SIM102 - SIM102: 嵌套if表达独立前置条件, 合并损可读性
                    if start <= cache_date <= end:
                        cached_part = self._filter_cached(cached, symbol, columns)
                        if not cached_part.is_empty():
                            df = df.filter(pl.col("date") != cache_date)
                            common_cols = [c for c in df.columns if c in cached_part.columns]
                            df = pl.concat(
                                [df.select(common_cols), cached_part.select(common_cols)]
                            )
                return df

        # 扩展范围用于指标预热 (MA60 需要 ~60 交易日 ≈ 120 日历日)
        warmup_start = start - timedelta(days=150)

        # 优先复用预计算 enriched 历史缓存 (300 天全指标, 与回测引擎同源):
        # 个股对话框打开时本接口每个行情 tick 被调一次, 逐请求 150 天扫描 + 全套
        # 指标重算是热路径上最大的重复计算。缓存最新日可能不含当日实时行,
        # 由下方 get_enriched_latest 覆盖逻辑补齐; 覆盖不足时回退单股计算路径。
        df = pl.DataFrame()
        hist = self._enriched_history_cache
        if hist is not None and not hist.is_empty() and "date" in hist.columns:
            hist_min = self._enriched_history_start
            hist_max = hist["date"].max()
            if hist_min is not None and hist_min <= start and hist_max >= start:
                df = hist.filter(
                    (pl.col("symbol") == symbol)
                    & (pl.col("date") >= start)
                    & (pl.col("date") <= end)
                )
        if df.is_empty():
            # 扫描14列 parquet
            df = self._scan_daily_symbol(symbol, warmup_start, end, None)
            if not df.is_empty():
                df = self._compute_enriched_range(df)

        # 尝试用缓存数据覆盖最新日 (盘中更准确)
        cached, cache_date = self.get_enriched_latest(symbols=[symbol], end=end)
        if not df.is_empty() and cached is not None and not cached.is_empty() and cache_date:  # noqa: SIM102 - SIM102: 嵌套if表达独立前置条件, 合并损可读性
            if start <= cache_date <= end:
                cached_part = self._filter_cached(cached, symbol, None)
                if not cached_part.is_empty():
                    df = df.filter(pl.col("date") != cache_date)
                    common_cols = [c for c in df.columns if c in cached_part.columns]
                    df = pl.concat([df.select(common_cols), cached_part.select(common_cols)])

        # 裁剪到请求范围
        if not df.is_empty():
            df = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))

        if columns and not df.is_empty():
            existing = [c for c in columns if c in df.columns]
            df = df.select(existing)

        return df

    def get_daily_batch(
        self,
        symbols: list[str],
        start: date,
        end: date,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """批量日K查询。"""
        self._assert_daily_usable(symbols=symbols, end=end)
        cached, cache_date = self.get_enriched_latest(symbols=symbols, end=end)
        if cached is not None and not cached.is_empty() and cache_date:  # noqa: SIM102 - SIM102: 嵌套if表达独立前置条件, 合并损可读性
            if start >= cache_date:
                return self._filter_cached_batch(cached, symbols, columns)

        # 复用 300 天预计算 enriched 历史缓存 (与单股 get_daily 同源同判据):
        # 自选迷你蜡烛窗口 (days≤60) 完全落在缓存内, 免整树 glob 扫描;
        # 覆盖不足 (窗口早于缓存起点 / symbol 不在缓存) 时回退扫描路径。
        df = pl.DataFrame()
        hist = self._enriched_history_cache
        if hist is not None and not hist.is_empty() and "date" in hist.columns:
            hist_min = self._enriched_history_start
            hist_max = hist["date"].max()
            if hist_min is not None and hist_min <= start and hist_max >= start:
                df = hist.filter(
                    pl.col("symbol").is_in(symbols)
                    & (pl.col("date") >= start)
                    & (pl.col("date") <= end)
                )

        if not df.is_empty():
            # 用实时缓存覆盖最新日 (与单股 get_daily 的覆盖逻辑同口径)
            if (
                cached is not None
                and not cached.is_empty()
                and cache_date
                and start <= cache_date <= end
            ):
                cached_part = self._filter_cached_batch(cached, symbols, None)
                if not cached_part.is_empty():
                    df = df.filter(pl.col("date") != cache_date)
                    common_cols = [c for c in df.columns if c in cached_part.columns]
                    df = pl.concat([df.select(common_cols), cached_part.select(common_cols)])
            if columns and not df.is_empty():
                existing = [c for c in columns if c in df.columns]
                df = df.select(existing)
            if not df.is_empty():
                return df.sort(["symbol", "date"])

        # 回退 scan_parquet
        return self._scan_daily_batch(symbols, start, end, columns)

    def get_index_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """指数日K查询 — 从独立指数 enriched parquet 读取后即时计算通用指标。"""
        from datetime import timedelta

        self._assert_daily_usable("index", symbols=[symbol], end=end)

        # 快路径: 若请求的列全部是 parquet 直接存储的列 (如迷你蜡烛图只要 OHLCV),
        # 直接 scan + 列下推返回, 跳过 warmup(150天) 与 _compute_index_enriched_range 全套指标计算。
        # 试探 scan 仅读请求的列, 缺列时回退到下方完整计算路径 (代价仅一次轻量 scan)。
        if columns:
            df = self._scan_index_daily_symbol(symbol, start, end, columns)
            if not df.is_empty() and all(c in df.columns for c in columns):
                return df

        warmup_start = start - timedelta(days=150)
        df = self._scan_index_daily_symbol(symbol, warmup_start, end, None)
        if not df.is_empty():
            df = self._compute_index_enriched_range(df)
            df = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        if columns and not df.is_empty():
            existing = [c for c in columns if c in df.columns]
            df = df.select(existing)
        return df

    def get_etf_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """ETF 日K查询 — 优先读独立 ETF enriched，兼容旧版 index enriched 中的 ETF。"""
        from datetime import timedelta

        self._assert_daily_usable("etf", symbols=[symbol], end=end)

        # 快路径: 独立 ETF 数据 + 请求列全是 parquet 存储列 → 直接 scan 列下推, 跳过 warmup + compute。
        # 无独立 ETF 数据 (旧版存入 index enriched) 时 df 为空, 自然回退到下方完整路径。
        if columns:
            df = self._scan_etf_daily_symbol(symbol, start, end, columns)
            if not df.is_empty() and all(c in df.columns for c in columns):
                return df

        warmup_start = start - timedelta(days=150)
        df = self._scan_etf_daily_symbol(symbol, warmup_start, end, None)
        if df.is_empty():
            # 旧版 ETF 曾存入 kline_index_enriched；没有独立数据时回退读取。
            df = self._scan_index_daily_symbol(symbol, warmup_start, end, None)
        if not df.is_empty():
            df = self._compute_index_enriched_range(df)
            df = df.filter((pl.col("date") >= start) & (pl.col("date") <= end))
        if columns and not df.is_empty():
            existing = [c for c in columns if c in df.columns]
            df = df.select(existing)
        return df

    def get_daily_asset(
        self,
        asset_type: str,
        symbol: str,
        start: date,
        end: date,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        if asset_type == "stock":
            return self.get_daily(symbol, start, end, columns)
        if asset_type == "index":
            return self.get_index_daily(symbol, start, end, columns)
        if asset_type == "etf":
            return self.get_etf_daily(symbol, start, end, columns)
        return pl.DataFrame()
