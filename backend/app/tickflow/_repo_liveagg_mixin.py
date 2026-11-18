"""KlineRepository mixin: _LiveAggMixin.

由 app.tickflow.repository 组合；模块级 helper 保持在 repository，
导入路径与 monkeypatch 目标不变。
"""

from __future__ import annotations

from app.tickflow._repo_mixin_base import _RepoMixinBase
from app.tickflow.repository import (
    _last_available_rows,
    _live_agg_window_start,
    cn_today,
    date,
    guarded_collect,
    logger,
    pl,
    scan_enriched_parquet,
    time,
)


class _LiveAggMixin(_RepoMixinBase):
    def _live_agg_source(self, latest, start_60d):
        # 优先使用已有的历史缓存 (避免重复 scan_parquet + compute_indicators)
        if self._enriched_history_cache is not None and not self._enriched_history_cache.is_empty():
            hist_all = self._enriched_history_cache
            if "date" in hist_all.columns and hist_all["date"].min() <= start_60d:
                # 从历史缓存中提取所需列 (历史缓存已有指标列)
                base_cols = [
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "raw_close",
                    "raw_high",
                    "raw_low",
                    "consecutive_limit_ups",
                    "consecutive_limit_downs",
                ]
                needed = [c for c in base_cols if c in hist_all.columns]
                step = time.perf_counter()
                logger.info("live agg step start: slice history cache")
                window_start = _live_agg_window_start(hist_all["date"], latest, start_60d)
                df_hist = (
                    hist_all.select(needed)
                    .filter((pl.col("date") >= window_start) & (pl.col("date") <= latest))
                    .sort(["symbol", "date"])
                )
                logger.info(
                    "live agg step done: slice history cache rows=%d (%.2fs)",
                    len(df_hist),
                    time.perf_counter() - step,
                )

                state_cols = [
                    "symbol",
                    "ema5",
                    "ema10",
                    "ema20",
                    "ema30",
                    "ema60",
                    "macd_dea",
                    "kdj_k",
                    "kdj_d",
                    "atr_14",
                    "close",
                    "high",
                    "low",
                    "annual_vol_20d",
                ]
                existing_state = [c for c in state_cols if c in hist_all.columns]
                state_source = _last_available_rows(
                    hist_all.select("date", *existing_state),
                    latest,
                )
                agg_a = state_source.select(existing_state)
                # ema5~ema60 / macd_dea 等状态取自完整历史缓存; _ema12/_ema26 与 RSI 也必须
                # 同源暖机, 只用 90 天窗口递推会与 macd_dea、盘后全量口径不一致
                ewm_history = hist_all.select("symbol", "date", "close")
                return df_hist, agg_a, ewm_history
            df_hist = pl.DataFrame()
            agg_a = pl.DataFrame()
        else:
            # 降级: 读 parquet + compute_indicators
            df_hist, agg_a = self._build_live_agg_from_parquet(latest, start_60d)
        return df_hist, agg_a, None

    def _live_agg_ema_rsi(self, ewm_source, agg_a, latest):
        from app.indicators.pipeline import _ema_alpha

        # 单独计算 _ema12 / _ema26 (compute_indicators 内部会 drop 掉)
        step = time.perf_counter()
        logger.info("live agg step start: ema state")
        df_ema = _last_available_rows(
            ewm_source.sort(["symbol", "date"])
            .with_columns(
                [
                    pl.col("close")
                    .ewm_mean(alpha=_ema_alpha(12), adjust=False)
                    .over("symbol")
                    .alias("_ema12"),
                    pl.col("close")
                    .ewm_mean(alpha=_ema_alpha(26), adjust=False)
                    .over("symbol")
                    .alias("_ema26"),
                ]
            )
            .select("symbol", "date", "_ema12", "_ema26"),
            latest,
        ).select("symbol", "_ema12", "_ema26")

        agg_a = agg_a.join(df_ema, on="symbol", how="inner")
        logger.info("live agg step done: ema state (%.2fs)", time.perf_counter() - step)

        # 单独计算 RSI 状态列 (compute_indicators 内部会 drop 掉)
        step = time.perf_counter()
        logger.info("live agg step start: rsi state")
        df_rsi_base = ewm_source.sort(["symbol", "date"]).with_columns(
            pl.col("close").diff().over("symbol").alias("_daily_delta")
        )
        gain = pl.when(pl.col("_daily_delta") > 0).then(pl.col("_daily_delta")).otherwise(0.0)
        loss = pl.when(pl.col("_daily_delta") < 0).then(-pl.col("_daily_delta")).otherwise(0.0)
        rsi_exprs = []
        for n in (6, 14, 24):
            a = 1.0 / n
            rsi_exprs.append(
                gain.ewm_mean(alpha=a, adjust=False).over("symbol").alias(f"_rsi_avg_gain_{n}")
            )
            rsi_exprs.append(
                loss.ewm_mean(alpha=a, adjust=False).over("symbol").alias(f"_rsi_avg_loss_{n}")
            )
        df_rsi = _last_available_rows(
            df_rsi_base.with_columns(rsi_exprs),
            latest,
        ).select(
            "symbol",
            *[f"_rsi_avg_gain_{n}" for n in (6, 14, 24)],
            *[f"_rsi_avg_loss_{n}" for n in (6, 14, 24)],
        )
        agg_a = agg_a.join(df_rsi, on="symbol", how="inner")
        logger.info("live agg step done: rsi state (%.2fs)", time.perf_counter() - step)
        return agg_a

    def _live_agg_extra_states(self, df_hist, agg_a, latest, start_60d):
        # 前复权因子: adj_factor = close(复权) / raw_close(原始)
        if "raw_close" in df_hist.columns:
            step = time.perf_counter()
            logger.info("live agg step start: adj factor state")
            adj_factor_df = _last_available_rows(
                df_hist.select("symbol", "date", "close", "raw_close"),
                latest,
            ).select("symbol", (pl.col("close") / pl.col("raw_close")).alias("_adj_factor"))
            agg_a = agg_a.join(adj_factor_df, on="symbol", how="left")
            if "_adj_factor" in agg_a.columns:
                agg_a = agg_a.with_columns(pl.col("_adj_factor").fill_null(1.0))
            logger.info("live agg step done: adj factor state (%.2fs)", time.perf_counter() - step)

        # annual_vol_20d 递推状态: 最近 19 天日收益率的部分和 / 平方和
        step = time.perf_counter()
        logger.info("live agg step start: annual vol state")
        df_daily_pct = df_hist.sort(["symbol", "date"]).with_columns(
            pl.col("close").pct_change().over("symbol").alias("_daily_pct")
        )
        df_vol = df_daily_pct.group_by("symbol").agg(
            [
                pl.col("_daily_pct").tail(19).sum().alias("_vol_19d_pct_sum"),
                (pl.col("_daily_pct") ** 2).tail(19).sum().alias("_vol_19d_pct_sq_sum"),
            ]
        )
        agg_a = agg_a.join(df_vol, on="symbol", how="left")
        logger.info("live agg step done: annual vol state (%.2fs)", time.perf_counter() - step)

        # 昨日连板数: 使用每只股票最后一个有效交易日状态 (用于增量计算同向 +1)
        step = time.perf_counter()
        logger.info("live agg step start: consecutive state")
        consec_cols = [
            c
            for c in ["symbol", "consecutive_limit_ups", "consecutive_limit_downs"]
            if c in df_hist.columns
        ]
        consec_source = df_hist
        if len(consec_cols) != 3:
            lf = (
                scan_enriched_parquet(self._enriched_glob)
                .filter((pl.col("date") >= start_60d) & (pl.col("date") <= latest))
                .sort(["symbol", "date"])
            )
            consec_cols = [
                c
                for c in ["symbol", "consecutive_limit_ups", "consecutive_limit_downs"]
                if c in lf.collect_schema().names()
            ]
            consec_source = guarded_collect(lf.select("date", *consec_cols), priority="background")
        if len(consec_cols) == 3:
            consec_df = _last_available_rows(
                consec_source.select("date", *consec_cols),
                latest,
            )
            if not consec_df.is_empty():
                consec = consec_df.select(
                    "symbol",
                    pl.col("consecutive_limit_ups").alias("_prev_consec_up"),
                    pl.col("consecutive_limit_downs").alias("_prev_consec_down"),
                )
                agg_a = agg_a.join(consec, on="symbol", how="left")
        logger.info("live agg step done: consecutive state (%.2fs)", time.perf_counter() - step)
        return agg_a

    def _live_agg_rolling(self, df_hist):
        # B类: 按 symbol 分组聚合 — 窗口统计
        step = time.perf_counter()
        logger.info("live agg step start: rolling windows")
        agg_b = (
            df_hist.sort(["symbol", "date"])
            .group_by("symbol")
            .agg(
                [
                    pl.col("close").tail(4).sum().alias("_ma5_partial_sum"),
                    pl.col("close").tail(9).sum().alias("_ma10_partial_sum"),
                    pl.col("close").tail(19).sum().alias("_ma20_partial_sum"),
                    pl.col("close").tail(29).sum().alias("_ma30_partial_sum"),
                    pl.col("close").tail(59).sum().alias("_ma60_partial_sum"),
                    pl.col("close").tail(19).sum().alias("_boll_partial_sum"),
                    (pl.col("close").tail(19) ** 2).sum().alias("_boll_partial_sq_sum"),
                    # 60 日极值为收盘价口径 (与 compute_indicators / 回测矩阵 high_60d 一致)
                    pl.col("close").tail(59).max().alias("_high_59d"),
                    pl.col("close").tail(59).min().alias("_low_59d"),
                    # 异动偏离 deviate_3d 用 (与 5d/10d/30d 同语义: 尾部第 N 个收盘)
                    pl.col("close").tail(3).first().alias("_close_3d_ago"),
                    pl.col("close").tail(5).first().alias("_close_5d_ago"),
                    pl.col("close").tail(10).first().alias("_close_10d_ago"),
                    pl.col("close").tail(20).first().alias("_close_20d_ago"),
                    pl.col("close").tail(30).first().alias("_close_30d_ago"),
                    pl.col("close").tail(60).first().alias("_close_60d_ago"),
                    pl.col("volume").tail(4).sum().alias("_vol_ma5_partial_sum"),
                    pl.col("volume").tail(9).sum().alias("_vol_ma10_partial_sum"),
                    # 标准量比分母: 前5日成交量之和(不含当天), 用于 vol_ratio_5d
                    pl.col("volume").tail(5).sum().alias("_vol_ma5_prev_sum"),
                    pl.col("low").tail(8).min().alias("_kdj_8d_low"),
                    pl.col("high").tail(8).max().alias("_kdj_8d_high"),
                    # 窗口内实际 K 线根数 (≤ 窗口天数): compute_enriched_today 据此把
                    # 历史不足的窗口指标置空, 与全量 rolling(窗口满才出值) 同口径
                    pl.len().alias("_window_len"),
                ]
            )
        )
        logger.info("live agg step done: rolling windows (%.2fs)", time.perf_counter() - step)
        return agg_b

    def _build_live_agg(self, latest: date) -> None:
        """从 OHLCV 即时计算递推状态 + 窗口聚合, 构建盘中实时聚合表。

        优化: 优先使用 _enriched_history_cache (启动时已计算), 避免重复 compute_indicators。
        """
        from datetime import timedelta

        started = time.perf_counter()
        logger.info("live agg build start: latest=%s", latest)
        start_60d = latest - timedelta(days=90)  # 日历90天 ≈ 60个交易日
        # EMA12/26、RSI 递推状态的暖机来源; None = 与窗口切片 df_hist 同源 (降级路径)
        df_hist, agg_a, ewm_history = self._live_agg_source(latest, start_60d)

        if df_hist.is_empty():
            self._live_agg_cache = pl.DataFrame()
            self._live_agg_cache_date = None
            logger.info(
                "live agg build skipped: empty history (%.2fs)", time.perf_counter() - started
            )
            return

        if agg_a.is_empty():
            self._live_agg_cache = pl.DataFrame()
            self._live_agg_cache_date = None
            logger.info(
                "live agg build skipped: empty state (%.2fs)", time.perf_counter() - started
            )
            return

        ewm_source = df_hist if ewm_history is None else ewm_history
        agg_a = self._live_agg_ema_rsi(ewm_source, agg_a, latest)
        agg_a = self._live_agg_extra_states(df_hist, agg_a, latest, start_60d)

        agg_b = self._live_agg_rolling(df_hist)
        self._live_agg_cache = agg_a.join(agg_b, on="symbol", how="inner")
        self._live_agg_cache_date = latest
        logger.info(
            "live agg build done: rows=%d (%.2fs)",
            len(self._live_agg_cache),
            time.perf_counter() - started,
        )

    def _live_agg_baseline_date(self, latest: date) -> date:
        """盘中递推基准日期。当天实时分区存在时使用上一可用交易日。"""
        if latest != cn_today():
            return latest
        try:
            store = self._ck_store()
            if store is not None:
                dates = store.list_daily_dates("kline_daily_enriched")
                prior = [d for d in dates if d < latest]
                if prior:
                    return max(prior)
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            pass
        return latest

    def _build_live_agg_from_parquet(
        self, latest: date, start_60d: date
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """降级路径: 从 parquet 读取数据并计算指标 (当 _enriched_history_cache 不可用时)。"""
        from datetime import timedelta

        from app.indicators.pipeline import compute_indicators

        # 多读一段自然日, 再按交易日计数确定窗口起点 (长假前后 90 个自然日不足 60 个交易日)
        lf = (
            scan_enriched_parquet(self._enriched_glob)
            .filter(pl.col("date") >= start_60d - timedelta(days=60))
            .filter(pl.col("date") <= latest)
            .sort(["symbol", "date"])
        )

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
                "raw_close",
                "raw_high",
                "raw_low",
                "consecutive_limit_ups",
                "consecutive_limit_downs",
            ]
            if c in lf.collect_schema().names()
        ]
        df_hist = guarded_collect(lf.select(read_cols), priority="background")

        if df_hist.is_empty():
            return df_hist, pl.DataFrame()
        window_start = _live_agg_window_start(df_hist["date"], latest, start_60d)
        df_hist = df_hist.filter(pl.col("date") >= window_start)

        df_with_indicators = compute_indicators(df_hist)

        state_cols = [
            "symbol",
            "ema5",
            "ema10",
            "ema20",
            "ema30",
            "ema60",
            "macd_dea",
            "kdj_k",
            "kdj_d",
            "atr_14",
            "close",
            "high",
            "low",
            "annual_vol_20d",
        ]
        existing_state = [c for c in state_cols if c in df_with_indicators.columns]
        agg_a = _last_available_rows(
            df_with_indicators.select("date", *existing_state),
            latest,
        ).select(existing_state)

        return df_hist, agg_a
