"""KlineRepository mixin: _RefreshMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    EnrichedGenerationUnavailableError,
    _last_available_rows,
    date,
    guarded_collect,
    logger,
    pl,
    scan_enriched_parquet,
    threading,
    time,
)


class _RefreshMixin(_RepoMixinBase):
    def _ck_store(self):
        """CK 门面：优先构造注入的 storage，回退进程 runtime；无则返回 None。"""
        store = getattr(self, "storage", None)
        if store is not None:
            return store
        from app.storage.runtime import get_storage

        runtime = get_storage()
        return runtime.store if runtime is not None else None

    def refresh_cache(self, background: bool = False) -> None:
        """刷新 Polars 缓存。在 pipeline 完成后、服务启动时调用。

        background=True (启动时): instruments/index/ETF 同步刷新 (毫秒级),
        enriched 的重计算 (compute_indicators, 107万行) 推到 daemon 线程,
        不阻塞 FastAPI lifespan。预热期间上层走空表降级。
        background=False (盘后管道/手动刷新): 全部同步, 保证数据即时一致。
        """
        started = time.perf_counter()
        logger.info("cache refresh start (background=%s)", background)

        step = time.perf_counter()
        logger.info("cache refresh step start: instruments")
        self._refresh_instruments()
        logger.info("cache refresh step done: instruments (%.2fs)", time.perf_counter() - step)

        step = time.perf_counter()
        logger.info("cache refresh step start: index instruments")
        self._refresh_index_instruments()
        logger.info(
            "cache refresh step done: index instruments (%.2fs)", time.perf_counter() - step
        )

        step = time.perf_counter()
        logger.info("cache refresh step start: ETF instruments")
        self._refresh_etf_instruments()
        logger.info("cache refresh step done: ETF instruments (%.2fs)", time.perf_counter() - step)

        # ETF enriched 只失效不重建: 下次访问时按新数据懒加载,
        # 避免自选无 ETF 的用户在管道后白付全量重算成本
        self._etf_enriched_cache = None
        self._etf_enriched_cache_date = None
        # 指数 enriched 同样只失效不重建 (懒加载)
        self._index_enriched_cache = None
        self._index_enriched_cache_date = None

        if background:
            logger.info("cache refresh: enriched 推后台线程预热")
            self._start_enriched_warmup()
        else:
            step = time.perf_counter()
            logger.info("cache refresh step start: enriched")
            self._refresh_enriched()
            logger.info("cache refresh step done: enriched (%.2fs)", time.perf_counter() - step)
            self._notify_refresh_done()

        logger.info("cache refresh done (%.2fs)", time.perf_counter() - started)

    def _start_enriched_warmup(self) -> None:
        """启动后台 daemon 线程预热 enriched 缓存 (compute_indicators)。

        仿 QuoteService 的线程模式: 设 warming 标志 → 起 daemon → 完成后清标志 +
        触发回调。重复调用时若已有线程在跑则跳过 (避免重复预热)。
        预热走 _warmup_enriched: 世代漂移导致丢弃时有界重建 (GH #480),
        同步/懒加载路径仍走单次 _refresh_enriched, 不阻塞 HTTP。
        """
        with self._warmup_lock:
            if self._enriched_warming:
                logger.info("enriched warmup already in progress, skip")
                return
            self._enriched_warming = True

        def _warmup() -> None:
            t0 = time.perf_counter()
            try:
                logger.info("enriched warmup thread started")
                self._warmup_enriched()
                logger.info("enriched warmup thread done (%.1fs)", time.perf_counter() - t0)
                self._notify_refresh_done()
            except Exception:  # noqa: BLE001 - 后台线程异常必须记录后继续，不能静默死亡
                logger.exception("enriched warmup thread failed")
            finally:
                with self._warmup_lock:
                    self._enriched_warming = False
                cb = self._on_warmup_done
                if cb is not None:
                    try:
                        cb()
                    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                        logger.warning("enriched warmup callback failed", exc_info=True)

        self._warmup_thread = threading.Thread(
            target=_warmup,
            name="enriched-warmup",
            daemon=True,
        )
        self._warmup_thread.start()

    def _notify_refresh_done(self) -> None:
        callback = self._on_refresh_done
        if callback is None:
            return
        try:
            callback()
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("repository refresh callback failed", exc_info=True)

    @property
    def enriched_ready(self) -> bool:
        """enriched 缓存是否已就绪 (非 None 且不在后台预热中)。"""
        return (
            not self._enriched_warming
            and self._enriched_cache is not None
            and self._live_agg_cache is not None
        )

    def clear_cache(self) -> None:
        """清空所有 Polars 内存缓存。

        与 refresh_cache 的区别: refresh_cache 在磁盘无数据时会提前 return,
        导致内存里的旧缓存残留 (clear 数据后看板仍显示旧数据的根因)。
        本方法无条件清空, 供清除数据/重置场景调用。
        """
        with self._prepared_daily_lock:
            self._prepared_daily.clear()
        self._enriched_cache = None
        self._enriched_cache_date = None
        self._enriched_history_cache = None
        self._enriched_history_start = None
        self._enriched_history_generation = None
        self._enriched_history_snapshot = None
        self._live_agg_cache = None
        self._live_agg_cache_date = None
        self._live_agg_check_date = None
        self._instruments_cache = None
        self._index_instruments_cache = None
        self._etf_enriched_cache = None
        self._etf_enriched_cache_date = None
        self._etf_live_agg_cache = None
        self._etf_live_agg_cache_date = None
        self._etf_instruments_cache = None
        self._index_symbol_set_cache = None
        self._etf_symbol_set_cache = None
        self._name_map_cache = None
        self._index_enriched_cache = None
        self._index_enriched_cache_date = None

    def _refresh_enriched(self) -> bool:
        """刷新 enriched 内存缓存。返回 True 表示因数据换代被丢弃、可重试。

        返回 False 覆盖两种情形: 成功建成, 或非换代失败 (不重试, 保持降级)。
        现有调用方均不读返回值, 行为不变; 仅后台 warmup 线程用返回值做有界重建。
        """
        from app.services.heavy_job_limiter import shared_heavy_job_limiter

        with shared_heavy_job_limiter.slot("exclusive"):
            try:
                self._refresh_enriched_impl()
            except EnrichedGenerationUnavailableError as e:
                logger.warning("enriched 历史缓存因数据换代被丢弃 (可重试): %s", e)
                return True
            except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("enriched 缓存刷新失败: %s", e)
                return False
        return False

    def _refresh_resolve_latest(self, started):
        refresh_snapshot = self.get_enriched_generation_snapshot("stock")
        refresh_generation = refresh_snapshot.generation
        logger.info("enriched refresh start")

        step = time.perf_counter()
        logger.info("enriched refresh step start: latest date")
        latest = self._latest_enriched_date_duckdb()
        logger.info(
            "enriched refresh step done: latest date=%s (%.2fs)",
            latest,
            time.perf_counter() - step,
        )
        if not latest:
            # 磁盘已无数据: 必须清空内存缓存, 否则旧数据会残留
            # (清数据后看板仍显示旧数据的根因)
            self.clear_cache()
            logger.info(
                "enriched refresh skipped: no latest date (%.2fs)",
                time.perf_counter() - started,
            )
            return None, None, None
        return latest, refresh_snapshot, refresh_generation

    def _refresh_read_latest(self, latest, started):
        enriched_dir = self.store.data_dir / "kline_daily_enriched"
        ds = latest.isoformat() if hasattr(latest, "isoformat") else str(latest)
        target_parquet = enriched_dir / f"date={ds}" / "part.parquet"

        if not target_parquet.exists():
            parquet_latest = max(
                (
                    item.name.removeprefix("date=")
                    for item in enriched_dir.glob("date=*")
                    if item.is_dir()
                ),
                default=None,
            )
            if parquet_latest is not None and ds > parquet_latest:
                logger.warning(
                    "enriched CK latest %s newer than local parquet latest %s; hot cache stays stale",
                    ds,
                    parquet_latest,
                )
            logger.info(
                "enriched refresh skipped: %s not found (%.2fs)",
                target_parquet,
                time.perf_counter() - started,
            )
            return None

        step = time.perf_counter()
        logger.info("enriched refresh step start: read latest parquet %s", target_parquet)
        df_latest = pl.read_parquet(target_parquet)
        logger.info(
            "enriched refresh step done: read latest parquet rows=%d (%.2fs)",
            len(df_latest),
            time.perf_counter() - step,
        )
        if df_latest.is_empty():
            logger.info(
                "enriched refresh skipped: latest parquet empty (%.2fs)",
                time.perf_counter() - started,
            )
            return None
        return df_latest

    def _refresh_collect_history(self, latest, df_latest):
        from datetime import timedelta

        # Step 2: 读近 300 天 14 列数据 → compute → filter(latest) → 缓存
        # 300 日历天 ≈ 210 交易日, 覆盖 filter_history 最大 lookback(90) + warmup(60)
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
        lf = (
            scan_enriched_parquet(self._enriched_glob)
            .filter(pl.col("date") >= start_full)
            .sort(["symbol", "date"])
        )
        step = time.perf_counter()
        logger.info("enriched refresh step start: collect history from %s", start_full)
        df_hist = guarded_collect(lf.select(read_cols), priority="background")
        logger.info(
            "enriched refresh step done: collect history rows=%d (%.2fs)",
            len(df_hist),
            time.perf_counter() - step,
        )
        return df_hist

    def _refresh_compute_history(self, df_hist):
        from app.indicators.pipeline import compute_enriched_history_window

        instruments = (
            self._instruments_cache if self._instruments_cache is not None else pl.DataFrame()
        )
        # 分批计算并关联元数据, 保留完整历史, 限制宽表临时副本。
        step = time.perf_counter()
        logger.info("enriched refresh step start: compute window (batched)")
        df_full = compute_enriched_history_window(
            df_hist,
            self.store.data_dir,
            instruments=instruments,
            historical_shares=(
                self.get_historical_shares()
                if instruments is not None and not instruments.is_empty()
                else None
            ),
            include_instrument_metadata=True,
        )
        del df_hist
        logger.info(
            "enriched refresh step done: compute window rows=%d (%.2fs)",
            len(df_full),
            time.perf_counter() - step,
        )
        return df_full

    def _refresh_publish_history(
        self, df_full, latest, refresh_snapshot, refresh_generation, started
    ):
        # 缓存完整历史 (含指标+必要基础信息) 供 filter_history/backtest 直接复用
        if self.get_matrix_data_generation("stock") != refresh_generation:
            raise EnrichedGenerationUnavailableError(
                "enriched data changed while refreshing its history cache"
            )
        self._enriched_history_cache = df_full
        self._enriched_history_start = df_full["date"].min()
        self._enriched_history_generation = refresh_generation
        self._enriched_history_snapshot = (df_full, refresh_snapshot)
        logger.info(
            "enriched 历史缓存: %d rows, %s ~ %s",
            len(df_full),
            self._enriched_history_start,
            latest,
        )

        # 只取最新一天作为 enriched_cache
        df_today = df_full.filter(pl.col("date") == latest)
        if not df_today.is_empty():
            self._enriched_cache = df_today
            self._enriched_cache_date = latest
            # 构建盘中递推基准: 若最新分区是今天的实时盘中数据,
            # 递推状态必须停在上一交易日, 不能把今天作为“昨日”。
            step = time.perf_counter()
            logger.info("enriched refresh step start: build live agg")
            self._build_live_agg(self._live_agg_baseline_date(latest))
            logger.info(
                "enriched refresh step done: build live agg (%.2fs)",
                time.perf_counter() - step,
            )
            repaired_today = self._restore_missing_latest_rows(
                latest,
                df_today,
                df_full,
            )
            if len(repaired_today) > len(df_today):
                df_full = pl.concat(
                    [df_full.filter(pl.col("date") != latest), repaired_today],
                    how="diagonal_relaxed",
                ).sort(["symbol", "date"])
                self._enriched_history_cache = df_full
                self._enriched_history_snapshot = (df_full, refresh_snapshot)
                self._enriched_cache = repaired_today
                df_today = repaired_today
            logger.info("enriched 缓存已计算: %d 只, 日期 %s (即时计算)", len(df_today), latest)
            logger.info("enriched refresh done (%.2fs)", time.perf_counter() - started)
            return True
        return False

    def _refresh_enriched_impl(self) -> None:
        """从 parquet 加载 enriched 最新日到内存 + 构建聚合表。

        enriched parquet 仅存 14 列基础数据。启动时读入历史数据并即时计算完整指标，
        将结果缓存在内存中供各服务使用。

        优化: 扩大历史读取范围, 同时缓存完整历史 (含指标), 供 filter_history 策略直接复用。
        """
        try:
            started = time.perf_counter()
            latest, refresh_snapshot, refresh_generation = self._refresh_resolve_latest(started)
            if latest is None:
                return

            # Step 1: 直接读最新日期的分区文件 (仅 14 列)
            df_latest = self._refresh_read_latest(latest, started)
            if df_latest is None:
                return

            try:
                df_hist = self._refresh_collect_history(latest, df_latest)
                if not df_hist.is_empty():
                    df_full = self._refresh_compute_history(df_hist)
                    if self._refresh_publish_history(
                        df_full, latest, refresh_snapshot, refresh_generation, started
                    ):
                        return
            except EnrichedGenerationUnavailableError:
                raise
            except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                logger.warning("enriched 即时计算失败, 使用原始 14 列缓存: %s", e)

            # 降级: 直接使用 14 列数据 + 构建 live_agg
            self._enriched_cache = df_latest
            self._enriched_cache_date = latest
            step = time.perf_counter()
            logger.info("enriched refresh fallback step start: build live agg")
            self._build_live_agg(self._live_agg_baseline_date(latest))
            logger.info(
                "enriched refresh fallback step done: build live agg (%.2fs)",
                time.perf_counter() - step,
            )

            logger.info("enriched 缓存已加载: %d 只, 日期 %s", len(df_latest), latest)
            logger.info("enriched refresh done fallback (%.2fs)", time.perf_counter() - started)
        except EnrichedGenerationUnavailableError:  # noqa: TRY203 - TRY203: raise 在 except 内为显式重抛
            # 换代丢弃上抛给 _refresh_enriched: 后台 warmup 可重试, 同步路径记 warning 降级
            raise

    def _restore_missing_latest_rows(
        self,
        latest: date,
        df_today: pl.DataFrame,
        history: pl.DataFrame,
    ) -> pl.DataFrame:
        """用同日原始日K补齐旧实时快照漏写的正常成交股票。"""
        if self._live_agg_cache is None or self._live_agg_cache.is_empty():
            return df_today
        daily_path = (
            self.store.data_dir / "kline_daily" / f"date={latest.isoformat()}" / "part.parquet"
        )
        if not daily_path.exists():
            return df_today

        try:
            from app.indicators.pipeline import compute_enriched_today, filter_halt_days

            daily = filter_halt_days(pl.read_parquet(daily_path))
            missing = daily.join(
                df_today.select("symbol").unique(),
                on="symbol",
                how="anti",
            )
            if missing.is_empty():
                return df_today

            missing_symbols = missing.select("symbol").unique()
            previous = history.filter(pl.col("date") < latest).join(
                missing_symbols,
                on="symbol",
                how="semi",
            )
            if not previous.is_empty():
                previous = _last_available_rows(previous, latest)
            recovered = compute_enriched_today(
                self._live_agg_cache,
                previous,
                missing,
                self.get_instruments(),
            )
            if recovered.is_empty():
                return df_today
            recovered = self._with_instrument_metadata("stock", recovered)
            result = (
                pl.concat(
                    [df_today, recovered],
                    how="diagonal_relaxed",
                )
                .unique(subset=["symbol", "date"], keep="last")
                .sort("symbol")
            )
            logger.info(
                "enriched latest cache restored from daily: date=%s, rows=%d",
                latest,
                len(result) - len(df_today),
            )
            return result
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("enriched latest cache restore skipped: %s", e)
            return df_today
