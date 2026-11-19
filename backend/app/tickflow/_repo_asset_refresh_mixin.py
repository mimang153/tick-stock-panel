"""KlineRepository mixin: _AssetRefreshMixin.

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
    scan_parquet_compat,
)


class _AssetRefreshMixin(_RepoMixinBase):
    def _refresh_etf_enriched(self) -> None:
        """从 ETF enriched parquet 加载最新日到内存缓存。"""
        try:
            enriched_dir = self.store.data_dir / "kline_etf_enriched"
            dates = (
                sorted(
                    p.name[5:]
                    for p in enriched_dir.glob("date=*")
                    if p.is_dir() and p.name.startswith("date=")
                )
                if enriched_dir.exists()
                else []
            )
            if not dates:
                self._etf_enriched_cache = None
                self._etf_enriched_cache_date = None
                return
            latest = date.fromisoformat(dates[-1])
            target_parquet = enriched_dir / f"date={dates[-1]}" / "part.parquet"
            df_latest = pl.read_parquet(target_parquet)
            if df_latest.is_empty():
                return

            from datetime import timedelta

            from app.indicators.pipeline import compute_indicators, compute_signals

            start_full = latest - timedelta(days=300)
            read_cols = [
                c
                for c in [
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "quote_ts",
                    "raw_close",
                    "raw_high",
                    "raw_low",
                ]
                if c in df_latest.columns
            ]
            df_hist = guarded_collect(
                scan_enriched_parquet(
                    self._etf_enriched_glob,
                    cast_options=pl.ScanCastOptions(integer_cast="allow-float"),
                )
                .filter(pl.col("date") >= start_full)
                .select(read_cols)
                .sort(["symbol", "date"]),
                priority="background",
            )
            if df_hist.is_empty():
                self._etf_enriched_cache = df_latest.sort(["symbol"])
            else:
                df_full = compute_signals(compute_indicators(df_hist))
                self._etf_enriched_cache = df_full.filter(pl.col("date") == latest).sort(["symbol"])
            self._etf_enriched_cache_date = latest
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("ETF enriched 缓存刷新跳过: %s", e)

    def _refresh_index_enriched(self) -> None:
        """从 CK 指数 enriched 加载最新日并重算通用指标（在线）/ 本地 parquet 兼容。

        磁盘窄表无指标列, 必须读近 300 天重算, 否则监控信号规则无列可评估。
        指数无复权需求, 不读 raw_close/raw_high/raw_low。
        """
        from datetime import timedelta

        store = getattr(self, "storage", None)
        if store is None:
            self._refresh_index_enriched_local()
            return
        try:
            dates = store.list_daily_dates("kline_index_enriched")
            if not dates:
                self._index_enriched_cache = None
                self._index_enriched_cache_date = None
                return
            latest = dates[-1]
            df_hist = store.read_daily(
                "kline_index_enriched",
                start=latest - timedelta(days=300),
                columns=["symbol", "date", "open", "high", "low", "close", "volume", "amount"],
            ).sort(["symbol", "date"])
            if df_hist.is_empty():
                return
            df_full = self._compute_index_enriched_range(df_hist)
            self._index_enriched_cache = df_full.filter(pl.col("date") == latest).sort(["symbol"])
            self._index_enriched_cache_date = latest
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("指数 enriched 缓存刷新跳过: %s", e)

    def _refresh_index_enriched_local(self) -> None:
        """离线兼容：从本地 parquet 分区刷新指数 enriched 内存缓存。"""
        try:
            enriched_dir = self.store.data_dir / "kline_index_enriched"
            dates = (
                sorted(
                    p.name[5:]
                    for p in enriched_dir.glob("date=*")
                    if p.is_dir() and p.name.startswith("date=")
                )
                if enriched_dir.exists()
                else []
            )
            if not dates:
                self._index_enriched_cache = None
                self._index_enriched_cache_date = None
                return
            latest = date.fromisoformat(dates[-1])
            target_parquet = enriched_dir / f"date={dates[-1]}" / "part.parquet"
            df_latest = pl.read_parquet(target_parquet)
            if df_latest.is_empty():
                return

            from datetime import timedelta

            start_full = latest - timedelta(days=300)
            read_cols = [
                c
                for c in [
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "quote_ts",
                ]
                if c in df_latest.columns
            ]
            df_hist = guarded_collect(
                scan_enriched_parquet(
                    self._index_enriched_glob,
                    cast_options=pl.ScanCastOptions(integer_cast="allow-float"),
                )
                .filter(pl.col("date") >= start_full)
                .select(read_cols)
                .sort(["symbol", "date"]),
                priority="background",
            )
            if df_hist.is_empty():
                self._index_enriched_cache = df_latest.sort(["symbol"])
            else:
                df_full = self._compute_index_enriched_range(df_hist)
                self._index_enriched_cache = df_full.filter(pl.col("date") == latest).sort(
                    ["symbol"]
                )
            self._index_enriched_cache_date = latest
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("指数 enriched 缓存刷新跳过: %s", e)

    def _refresh_instruments(self) -> None:
        """加载 instruments 到内存；在线路径只读 CK 维表。"""
        store = getattr(self, "storage", None)
        if store is not None:
            try:
                df = store.read_instruments("stock")
                if not df.is_empty():
                    if "as_of" in df.columns:
                        df = df.sort("as_of", descending=True, nulls_last=True).unique(
                            subset=["symbol"], keep="first", maintain_order=True
                        )
                    self._instruments_cache = df
                    self._name_map_cache = None
            except Exception as e:  # noqa: BLE001 - CK 不可用时不静默回退到本地旧数据
                logger.warning("instruments CK 缓存刷新失败: %s", e)
            return
        try:
            files = sorted((self.store.data_dir / "instruments").rglob("*.parquet"))
            if not files:
                return
            # 先合并各文件的字段，旧文件缺少 as_of 时也能使用新文件的日期。
            df = guarded_collect(
                pl.concat([scan_parquet_compat(path) for path in files], how="diagonal_relaxed"),
                priority="background",
            )
            if not df.is_empty():
                # 旧 part.parquet 与当前 instruments.parquet 可能共存。
                # 缓存必须每标的一行，按元数据日期保留最新整行。
                if "as_of" in df.columns:
                    df = df.sort("as_of", descending=True, nulls_last=True, maintain_order=True)
                df = df.unique(subset=["symbol"], keep="first", maintain_order=True)
                self._instruments_cache = df
                self._name_map_cache = None
                logger.info("instruments 缓存已加载: %d 只", len(df))
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("instruments 缓存刷新失败: %s", e)

    def _refresh_index_instruments(self) -> None:
        """加载指数 instruments 到内存；在线路径只读 CK 维表。"""
        store = getattr(self, "storage", None)
        if store is not None:
            try:
                df = store.read_instruments("index")
                if not df.is_empty():
                    self._index_instruments_cache = df
                    self._index_symbol_set_cache = None
                    self._name_map_cache = None
            except Exception as e:  # noqa: BLE001 - CK 不可用时不静默回退到本地旧数据
                logger.debug("index instruments CK 缓存刷新跳过: %s", e)
            return
        try:
            df = guarded_collect(scan_parquet_compat(self._index_inst_glob), priority="background")
            if not df.is_empty():
                self._index_instruments_cache = df
                self._index_symbol_set_cache = None
                self._name_map_cache = None
                logger.info("index instruments 缓存已加载: %d 只", len(df))
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("index instruments 缓存刷新跳过: %s", e)

    def _refresh_etf_instruments_from_ck(self, store) -> None:
        parts: list[pl.DataFrame] = []
        try:
            df = store.read_instruments("etf")
            if not df.is_empty():
                parts.append(df)
        except Exception as e:  # noqa: BLE001 - CK 不可用时不静默回退到本地旧数据
            logger.debug("etf instruments CK 缓存刷新跳过(new): %s", e)
        try:
            legacy = store.read_instruments("index")
            if not legacy.is_empty() and "asset_type" in legacy.columns:
                legacy = legacy.filter(pl.col("asset_type") == "etf")
                if not legacy.is_empty():
                    parts.append(legacy)
        except Exception as e:  # noqa: BLE001 - CK 旧表可能尚未建立
            logger.debug("etf instruments CK legacy fallback skipped: %s", e)
        if parts:
            df_all = (
                pl.concat(parts, how="diagonal_relaxed")
                .unique(subset=["symbol"], keep="last")
                .sort("symbol")
            )
            self._etf_instruments_cache = df_all
            self._etf_symbol_set_cache = None
            self._name_map_cache = None

    def _refresh_etf_instruments(self) -> None:
        """加载 ETF instruments 到内存；在线路径只读 CK 维表。"""
        store = getattr(self, "storage", None)
        if store is not None:
            self._refresh_etf_instruments_from_ck(store)
            return
        parts: list[pl.DataFrame] = []
        try:
            df = guarded_collect(scan_parquet_compat(self._etf_inst_glob), priority="background")
            if not df.is_empty():
                parts.append(df)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("etf instruments 缓存刷新跳过(new): %s", e)
        try:
            legacy = self.get_index_instruments()
            if not legacy.is_empty() and "asset_type" in legacy.columns:
                legacy = legacy.filter(pl.col("asset_type") == "etf")
                if not legacy.is_empty():
                    parts.append(legacy)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("etf instruments legacy fallback skipped: %s", e)
        if parts:
            df_all = (
                pl.concat(parts, how="diagonal_relaxed")
                .unique(subset=["symbol"], keep="last")
                .sort("symbol")
            )
            self._etf_instruments_cache = df_all
            self._etf_symbol_set_cache = None
            self._name_map_cache = None
            logger.info("ETF instruments 缓存已加载: %d 只", len(df_all))
