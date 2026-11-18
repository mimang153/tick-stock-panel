from __future__ import annotations

import app.backtest.engine as _root
from app.backtest.engine import (
    _GENERATION_WAIT_TIMEOUT_S,
    EnrichedGenerationUnavailableError,
    KlineRepository,
    MarketDataMatrix,
    MatrixCacheProfile,
    PanelCache,
    _matrix_cache_settings,
    _read_enriched_panel_rows,
    _resolve_matrix_field_columns,
    date,
    logger,
    pa,
    pl,
    threading,
    time,
)


class BacktestDataMixin:
    """BacktestEngine 混入方法组 — 由 engine.py 组合。"""

    def __init__(self, repo: KlineRepository) -> None:
        self.repo = repo
        self._cache = PanelCache()
        self._evidence = None

    # ── 数据加载 ──────────────────────────────────────

    def data_generation(self, asset_type: str = "stock") -> str | None:
        from app.services.daily_health import assert_usable

        root = getattr(getattr(self.repo, "store", None), "data_dir", None)
        if root is not None:
            assert_usable(root, asset_type)
        loader = getattr(self.repo, "get_matrix_data_generation", None)
        return loader(asset_type) if callable(loader) else None

    def data_generation_await(
        self,
        asset_type: str = "stock",
        *,
        cancel_event: threading.Event | None = None,
        timeout_s: float = _GENERATION_WAIT_TIMEOUT_S,
    ) -> str | None:
        """获取 generation; 发布进行中时在超时窗口内轮询, 可被取消事件打断。"""
        deadline = time.monotonic() + timeout_s
        from app.services.daily_health import DailyIntegrityError

        while True:
            try:
                return self.data_generation(asset_type)
            except DailyIntegrityError:
                raise
            except EnrichedGenerationUnavailableError:
                if cancel_event is not None and cancel_event.is_set():
                    raise
                if time.monotonic() >= deadline:
                    raise
                time.sleep(_root._GENERATION_POLL_S)

    def assert_data_generation(
        self,
        asset_type: str,
        expected: str | None,
    ) -> None:
        if expected is None:
            return
        current = self.data_generation(asset_type)
        if current != expected:
            raise EnrichedGenerationUnavailableError(
                "enriched data changed while the snapshot was being read"
            )

    def _range_cache_generation(
        self, asset_type: str, end: date, expected: str | None
    ) -> str | None:
        loader = getattr(self.repo, "get_enriched_generation_snapshot", None)
        if not callable(loader) or expected is None:
            return expected
        snapshot = loader(asset_type)
        if snapshot.generation != expected:
            raise EnrichedGenerationUnavailableError(
                "enriched data changed while the snapshot was being read"
            )
        return snapshot.through(end)

    def load_panel(
        self,
        symbols: list[str] | None,
        start: date,
        end: date,
        columns: list[str] | None = None,
        asset_type: str = "stock",
        *,
        expected_generation: str | None = None,
    ) -> pl.DataFrame:
        """加载 enriched 数据面板，带缓存。asset_type='etf' 时读 ETF enriched。"""
        # 快照世代路径也给一次重试 (issue #517): 盘中刷新每轮换新 generation,
        # 读后校验失败时重读「现役世代」而不是死等已作废的旧世代。
        attempts = 2
        expected = expected_generation
        for attempt in range(attempts):
            generation = expected if expected is not None else self.data_generation(asset_type)
            try:
                # Cache identity follows the consumed prefix. The global generation
                # still guards the read, including in-progress/partial publications.
                cache_generation = self._range_cache_generation(asset_type, end, generation)
                panel = self._cache.get_or_compute(
                    symbols,
                    start,
                    end,
                    columns,
                    self._load_panel_inner,
                    asset_type=asset_type,
                    generation=cache_generation,
                )
                self.assert_data_generation(asset_type, generation)
            except EnrichedGenerationUnavailableError:
                if attempt + 1 >= attempts:
                    raise
                expected = None
                continue
            return panel
        raise EnrichedGenerationUnavailableError("unable to read a stable enriched data snapshot")

    def load_panel_for_backtest(
        self,
        symbols: list[str] | None,
        start: date,
        end: date,
        feature_plan,
        asset_type: str = "stock",
        *,
        evidence=None,
    ) -> pl.DataFrame:
        """按解析后的依赖加载窄基础列并计算回测所需特征。"""
        df = self.load_panel(
            symbols,
            start,
            end,
            columns=sorted(feature_plan.base_columns),
            asset_type=asset_type,
        )
        if df.is_empty():
            return df

        from app.backtest.fundamentals import (
            load_fundamental_snapshot,
        )

        fundamental_names = sorted(
            getattr(feature_plan, "fundamental_columns", frozenset()) or frozenset()
        )
        financials = (
            load_fundamental_snapshot(
                self.repo.store.data_dir if self.repo is not None else None,
            )
            if fundamental_names
            else None
        )
        instruments = (
            self.repo.get_instruments_asset(asset_type) if self.repo is not None else pl.DataFrame()
        )
        historical_shares = (
            self.repo.get_historical_shares()
            if asset_type == "stock" and self.repo is not None and not instruments.is_empty()
            else None
        )
        if evidence is not None:
            from app.factors.ext_factors import attach_ext_columns

            # 扩展时序值也要固定在本次原始快照内；检查期间不重新读取磁盘。
            df = attach_ext_columns(df, include_snapshot=False, data_dir=self.repo.store.data_dir)

        def prepare(raw):
            return self._prepare_panel_features(
                raw,
                feature_plan,
                instruments,
                historical_shares,
                financials,
                attach_extensions=evidence is None,
            )

        if evidence is not None:
            evidence.capture_panel(
                df,
                prepare,
                {
                    "instruments": instruments,
                    "historical_shares": historical_shares,
                    "financial_announcements": financials,
                },
            )
        return prepare(df)

    @staticmethod
    def _prepare_panel_features(
        df,
        feature_plan,
        instruments,
        historical_shares,
        financials,
        *,
        attach_extensions=True,
    ):
        from app.backtest.fundamentals import attach_fundamental_factors
        from app.indicators.pipeline import (
            compute_indicators,
            compute_limit_signals,
            compute_signals,
        )

        fundamental_names = sorted(getattr(feature_plan, "fundamental_columns", ()) or ())
        if fundamental_names:
            df = attach_fundamental_factors(df, financials, fundamental_names)
        matrix_native = feature_plan.execution_backend == "matrix_native"
        if not matrix_native:
            df = compute_indicators(df, needed=set(feature_plan.indicator_columns))
            df = compute_signals(
                df,
                needed=set(feature_plan.signal_columns),
                **({"attach_extensions": False} if not attach_extensions else {}),
            )
        if not instruments.is_empty():
            df = compute_limit_signals(
                df,
                instruments,
                needed={"signal_limit_up", "signal_limit_down"}
                if matrix_native
                else set(feature_plan.signal_columns),
                historical_shares=historical_shares,
            )
            join_cols = ["symbol"] if "symbol" in instruments.columns else []
            join_cols.extend(
                col
                for col in sorted(feature_plan.instrument_columns)
                if col in instruments.columns and col not in df.columns
            )
            if len(join_cols) > 1:
                df = df.join(
                    instruments.select(join_cols).unique(subset=["symbol"]),
                    on="symbol",
                    how="left",
                )

        required_columns = set(feature_plan.base_columns) | set(feature_plan.instrument_columns)
        required_columns |= set(feature_plan.signal_columns)
        if not matrix_native:
            required_columns |= set(feature_plan.indicator_columns)
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"回测字段依赖未生成: {sorted(missing_columns)}")

        float_cols = [c for c in df.columns if df[c].dtype.is_float()]
        if float_cols:
            df = df.with_columns(
                [
                    pl.when(pl.col(c).is_nan() | pl.col(c).is_infinite())
                    .then(None)
                    .otherwise(pl.col(c))
                    .alias(c)
                    for c in float_cols
                ]
            )
        return df

    def _attach_matrix_extra_fields(self, market, feature_plan, ext_wanted):
        """矩阵加载后按公告日门控附加财务/扩展字段。"""
        from app.backtest.fundamentals import attach_matrix_fundamental_fields

        fundamental_names = sorted(
            getattr(feature_plan, "fundamental_columns", frozenset()) or frozenset()
        )
        if fundamental_names:
            market = attach_matrix_fundamental_fields(
                market,
                self.repo.store.data_dir if self.repo is not None else None,
                fundamental_names,
                evidence=self._evidence,
            )
        if ext_wanted:
            from app.factors.ext_factors import attach_matrix_ext_fields

            market = attach_matrix_ext_fields(
                market,
                self.repo.store.data_dir if self.repo is not None else None,
                ext_wanted,
            )
        return market

    def load_market_data_matrix_for_backtest(
        self,
        symbols: list[str] | None,
        start: date,
        end: date,
        feature_plan,
        asset_type: str = "stock",
        *,
        cache_profile: MatrixCacheProfile | None = None,
        coverage_start: date | None = None,
        coverage_end: date | None = None,
        expected_generation: str | None = None,
        cancel_event: threading.Event | None = None,
    ) -> MarketDataMatrix:
        """Load a matrix-native backtest directly from projected parquet batches."""
        if feature_plan.execution_backend != "matrix_native":
            raise ValueError("direct market matrix loading requires matrix_native backend")
        from app.tickflow.repository import enriched_dirname

        parquet_root = self.repo.store.data_dir / enriched_dirname(asset_type)
        instruments = self.repo.get_instruments_asset(asset_type)
        if self._evidence is not None:
            self._evidence.record_frame("instruments", instruments)
        field_columns, ext_wanted = _resolve_matrix_field_columns(
            feature_plan, self.repo.store.data_dir
        )
        cache_root, cache_fields, cache_max_bytes = _matrix_cache_settings(
            cache_profile, self.repo.store.data_dir, field_columns
        )
        source_generation = (
            expected_generation
            if expected_generation is not None
            else self.data_generation_await(asset_type, cancel_event=cancel_event)
        )
        # 快照世代路径也给第二次机会 (issue #517): 循环内撞换代时下方 except
        # 已用 data_generation_await 刷新世代, 这里只需不再限定单次尝试。
        attempts = 2
        for attempt in range(attempts):
            try:
                cache_generation = self._range_cache_generation(
                    asset_type,
                    coverage_end or end,
                    source_generation,
                )
                market = _root.load_market_data_matrix_from_parquet(
                    parquet_root,
                    start,
                    end,
                    field_columns=field_columns,
                    symbols=symbols,
                    instruments=instruments,
                    cache_root=cache_root,
                    coverage_start=coverage_start,
                    coverage_end=coverage_end,
                    cache_field_columns=cache_fields,
                    cache_max_bytes=cache_max_bytes,
                    profile_generation=(
                        cache_profile.generation if cache_profile is not None else "request"
                    ),
                    source_generation=cache_generation,
                    cancel_event=cancel_event,
                )
                self.assert_data_generation(asset_type, source_generation)
                return self._attach_matrix_extra_fields(market, feature_plan, ext_wanted)
            except EnrichedGenerationUnavailableError:
                if attempt + 1 >= attempts:
                    raise
                source_generation = self.data_generation_await(
                    asset_type, cancel_event=cancel_event
                )
            except pa.ArrowException as exc:
                raise ValueError(f"direct market matrix parquet scan failed: {exc}") from exc
        raise EnrichedGenerationUnavailableError("unable to read a stable enriched matrix snapshot")

    def cache_stats(self) -> dict:
        """暴露 PanelCache 遥测快照 (扫盘耗时/次数/命中/复用), 供上层量化 IO 占比。"""
        return self._cache.stats()

    def clear_panel_cache(self) -> None:
        self._cache.invalidate()

    def _load_panel_inner(
        self,
        symbols: list[str] | None,
        start: date,
        end: date,
        columns: list[str] | None = None,
        asset_type: str = "stock",
    ) -> pl.DataFrame:
        t0 = time.perf_counter()

        # 近期区间优先复用 repository 的预计算 enriched 历史缓存 (仅 stock: 该缓存为股票专用)。
        try:
            if (
                columns is None
                and asset_type == "stock"
                and self.repo is not None
                and hasattr(self.repo, "get_enriched_range")
            ):
                cached = self.repo.get_enriched_range(start, end, symbols=symbols, columns=columns)
                if cached is not None and not cached.is_empty():
                    elapsed = (time.perf_counter() - t0) * 1000
                    logger.info(
                        "load_panel(cache): %.0fms, %d rows, %d columns",
                        elapsed,
                        len(cached),
                        len(cached.columns),
                    )
                    return cached
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.debug("backtest load panel cache miss: %s", e)

        from app.tickflow.repository import enriched_dirname

        enriched_glob = str(
            self.repo.store.data_dir / enriched_dirname(asset_type) / "**" / "*.parquet"
        )

        df = _read_enriched_panel_rows(enriched_glob, symbols, start, end, columns, asset_type)
        if df.is_empty():
            return df

        if columns is not None:
            elapsed = (time.perf_counter() - t0) * 1000
            logger.info(
                "load_panel: %.0fms, %d rows, %d columns", elapsed, len(df), len(df.columns)
            )
            return df

        from app.indicators.pipeline import compute_all

        # 按 asset_type 取维表: ETF 回测须用 ETF 维表, 否则名称 JOIN 失败(全 null)、
        # 涨停信号算在错误的 instruments 上。
        instruments = self.repo.get_instruments_asset(asset_type)
        df = compute_all(df, instruments=instruments)
        if not instruments.is_empty() and "name" not in df.columns:
            inst_cols = [c for c in ["symbol", "name"] if c in instruments.columns]
            if len(inst_cols) == 2:
                df = df.join(
                    instruments.select(inst_cols).unique(subset=["symbol"]),
                    on="symbol",
                    how="left",
                )

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info("load_panel: %.0fms, %d rows", elapsed, len(df))
        return df
