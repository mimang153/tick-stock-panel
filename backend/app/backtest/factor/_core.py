"""Internal split module for app.backtest.factor: _core."""

from __future__ import annotations

import time
import uuid
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from app.backtest import stats_v2
from app.backtest.engine import history_load_start
from app.backtest.fundamentals import (
    FUNDAMENTAL_FACTOR_NAMES,
    attach_fundamental_factors,
    load_fundamental_snapshot,
)
from app.factors.registry import factor_calculation_order, factor_warmup_bars, get_factor
from app.factors.registry import factor_columns_view as _factor_columns_view
from app.strategy.scoring import VIRTUAL_SCORING_DEPENDENCIES as DERIVED_FACTOR_DEPENDENCIES
from app.strategy.scoring import materialize_scoring_columns

from ._common import (
    _DAILY_FORWARD_HORIZONS,
    FACTOR_WARMUP_DAYS,
    FactorBatchConfig,
    FactorBatchItem,
    FactorBatchResult,
    FactorConfig,
    FactorResult,
    logger,
)
from ._stats import FactorStatsMixin


class FactorCoreMixin:
    # 以下属性由组装类/其它 mixin 提供，仅作类型声明（不创建运行时属性）。
    _SNAPSHOT_MAX_ATTEMPTS: Any
    _SNAPSHOT_RETRY_SLEEP_S: Any
    _batch_config_to_dict: Any
    engine: Any
    _calc_regime_stats: Any
    _add_groups: Any
    _calc_group_nav: Any
    _calc_group_stats: Any
    _calc_turnover: Any
    _calc_long_short: Any
    _round_nav: Any

    def _with_snapshot_retry(self, body):
        from app.enriched_generation import EnrichedGenerationUnavailableError

        for attempt in range(self._SNAPSHOT_MAX_ATTEMPTS):
            if attempt:
                time.sleep(self._SNAPSHOT_RETRY_SLEEP_S)
            try:
                return body()
            except EnrichedGenerationUnavailableError:
                if attempt + 1 >= self._SNAPSHOT_MAX_ATTEMPTS:
                    raise

    def run(
        self,
        config: FactorConfig,
        *,
        regime_by_date: Mapping[object, Any] | None = None,
    ) -> FactorResult:
        from app.services.heavy_job_limiter import shared_heavy_job_limiter

        with shared_heavy_job_limiter.slot("exclusive"):
            return self._with_snapshot_retry(
                lambda: self._run(config, regime_by_date=regime_by_date)
            )

    def _run(
        self,
        config: FactorConfig,
        *,
        regime_by_date: Mapping[object, Any] | None = None,
    ) -> FactorResult:
        t0 = time.perf_counter()
        run_id = uuid.uuid4().hex[:10]
        generation = self._data_generation(config.asset_type)
        panel = self._load_factor_panel(
            config,
            [config.factor_name],
            expected_generation=generation,
        )
        if panel.is_empty():
            return self._error_result(config, run_id, t0, "无数据, 请检查日期范围或先运行盘后管道")

        if config.factor_name in FUNDAMENTAL_FACTOR_NAMES and self._fundamentals_missing():
            return self._error_result(
                config,
                run_id,
                t0,
                "本地没有财务数据: 请先在数据页同步财务数据后再使用财务因子",
            )

        trading_dates = self._global_trading_dates(config)
        self._assert_data_generation(config.asset_type, generation)
        panel = self._attach_shared_next_return(
            panel,
            config,
            trading_dates=trading_dates,
        )
        evaluate_kwargs = {"regime_by_date": regime_by_date} if regime_by_date is not None else {}
        return self._evaluate_panel(
            panel,
            config,
            run_id,
            t0,
            market_trading_dates=trading_dates,
            **evaluate_kwargs,
        )

    def run_batch(
        self,
        config: FactorBatchConfig,
        *,
        regime_by_date: Mapping[object, Any] | None = None,
    ) -> FactorBatchResult:
        from app.services.heavy_job_limiter import shared_heavy_job_limiter

        with shared_heavy_job_limiter.slot("exclusive"):
            return self._with_snapshot_retry(
                lambda: self._run_batch(config, regime_by_date=regime_by_date)
            )

    def _prepare_batch_panel(
        self,
        config: FactorBatchConfig,
        factor_names: list[str],
        run_id: str,
        result_config: dict,
        t0: float,
    ) -> tuple[pl.DataFrame, list[date] | None] | FactorBatchResult:
        generation = self._data_generation(config.asset_type)
        panel = self._load_factor_panel(
            config,
            factor_names,
            expected_generation=generation,
        )
        if panel.is_empty():
            return FactorBatchResult(
                run_id=run_id,
                config=result_config,
                error="无数据, 请检查日期范围或先运行盘后管道",
                elapsed_ms=round((time.perf_counter() - t0) * 1000, 1),
            )

        # P1: 预计算共享下期收益 (仅依赖 close/date/symbol), 避免每个因子重复 shift/调仓日 JOIN。
        trading_dates = self._global_trading_dates(config)
        self._assert_data_generation(config.asset_type, generation)
        panel = self._attach_shared_next_return(panel, config, trading_dates=trading_dates)
        return panel, trading_dates

    @staticmethod
    def _batch_factor_config(config: FactorBatchConfig, factor_name: str) -> FactorConfig:
        return FactorConfig(
            factor_name=factor_name,
            symbols=config.symbols,
            start=config.start,
            end=config.end,
            n_groups=config.n_groups,
            rebalance=config.rebalance,
            weight=config.weight,
            fees_pct=config.fees_pct,
            commission_pct=config.commission_pct,
            stamp_tax_pct=config.stamp_tax_pct,
            slippage_bps=config.slippage_bps,
            asset_type=config.asset_type,
        )

    @staticmethod
    def _batch_item_from_result(
        factor_name: str, meta: Mapping[str, Any], result: FactorResult
    ) -> FactorBatchItem:
        long_short = result.long_short_stats
        # metrics_v2: 由 IC 序列推导 NW HAC t 值与 p 值 (日频 1 日前瞻 → 滞后 1)
        ic_values = [row.get("ic") for row in result.ic_series]
        t_naive = stats_v2.naive_t(ic_values)
        nw = stats_v2.newey_west_t(ic_values, lag=1)
        return FactorBatchItem(
            factor_name=factor_name,
            label=str(meta.get("label", factor_name)),
            group=str(meta.get("group", "")),
            ic_mean=result.ic_mean,
            ir=result.ir,
            ic_win_rate=result.ic_win_rate,
            long_short_return=long_short.get("total_return"),
            long_short_max_drawdown=long_short.get("max_drawdown"),
            methodology_version=result.methodology_version,
            coverage=result.coverage,
            turnover=result.turnover,
            long_short_sharpe=result.long_short_sharpe,
            yearly_ic=result.yearly_ic,
            ic_decay=result.ic_decay,
            regime_stats=result.regime_stats,
            n_symbols=result.n_symbols,
            n_dates=result.n_dates,
            elapsed_ms=result.elapsed_ms,
            error=result.error,
            t_naive=t_naive,
            t_newey_west=nw[0] if nw else None,
            nw_lag=1 if nw else None,
            p_value=(
                stats_v2.normal_two_sided_p(nw[0])
                if nw
                else stats_v2.normal_two_sided_p(t_naive)
                if t_naive is not None
                else None
            ),
        )

    @staticmethod
    def _finalize_batch_result(
        run_id: str, result_config: dict, items: list[FactorBatchItem], t0: float
    ) -> FactorBatchResult:
        n_symbols = max((item.n_symbols for item in items), default=0)
        n_dates = max((item.n_dates for item in items), default=0)
        # metrics_v2: 批内 BH-FDR q 值 (m = 可检验因子数, 计算失败项不计)
        q_values = stats_v2.bh_fdr_qvalues([item.p_value for item in items])
        for item, q_value in zip(items, q_values, strict=True):
            item.q_value = q_value
        return FactorBatchResult(
            run_id=run_id,
            config=result_config,
            results=items,
            elapsed_ms=round((time.perf_counter() - t0) * 1000, 1),
            n_symbols=n_symbols,
            n_dates=n_dates,
        )

    def _run_batch(
        self,
        config: FactorBatchConfig,
        *,
        regime_by_date: Mapping[object, Any] | None = None,
    ) -> FactorBatchResult:
        """在同一份 Panel 上依次评估多个因子, 避免重复读取和计算指标。"""
        t0 = time.perf_counter()
        run_id = uuid.uuid4().hex[:10]
        factor_names = list(dict.fromkeys(config.factor_names))
        result_config = self._batch_config_to_dict(config, factor_names)
        if not factor_names:
            return FactorBatchResult(run_id=run_id, config=result_config, error="至少选择一个因子")

        prepared = self._prepare_batch_panel(config, factor_names, run_id, result_config, t0)
        if isinstance(prepared, FactorBatchResult):
            return prepared
        panel, trading_dates = prepared

        metadata = {item["id"]: item for item in _factor_columns_view()}
        fundamentals_missing = (
            any(name in FUNDAMENTAL_FACTOR_NAMES for name in factor_names)
            and self._fundamentals_missing()
        )
        items: list[FactorBatchItem] = []
        for factor_name in factor_names:
            item_t0 = time.perf_counter()
            meta = metadata.get(factor_name, {})
            if factor_name in FUNDAMENTAL_FACTOR_NAMES and fundamentals_missing:
                items.append(
                    FactorBatchItem(
                        factor_name=factor_name,
                        label=str(meta.get("label", factor_name)),
                        group=str(meta.get("group", "")),
                        elapsed_ms=round((time.perf_counter() - item_t0) * 1000, 1),
                        error="本地没有财务数据: 请先在数据页同步财务数据后再使用财务因子",
                    )
                )
                continue
            try:
                evaluate_kwargs = (
                    {"regime_by_date": regime_by_date} if regime_by_date is not None else {}
                )
                result = self._evaluate_panel(
                    panel,
                    self._batch_factor_config(config, factor_name),
                    f"{run_id}-{len(items) + 1}",
                    item_t0,
                    market_trading_dates=trading_dates,
                    **evaluate_kwargs,
                )
                items.append(self._batch_item_from_result(factor_name, meta, result))
            except Exception as exc:  # noqa: BLE001 - 单因子失败不能中止整个筛选批次
                logger.exception("factor batch item failed: %s", factor_name)
                items.append(
                    FactorBatchItem(
                        factor_name=factor_name,
                        label=str(meta.get("label", factor_name)),
                        group=str(meta.get("group", "")),
                        elapsed_ms=round((time.perf_counter() - item_t0) * 1000, 1),
                        error=str(exc),
                    )
                )
        return self._finalize_batch_result(run_id, result_config, items, t0)

    def _data_generation(self, asset_type: str) -> str | None:
        loader = getattr(self.engine, "data_generation", None)
        return loader(asset_type) if callable(loader) else None

    def _fundamentals_missing(self) -> bool:
        return load_fundamental_snapshot(self._fundamentals_data_dir()) is None

    def _fundamentals_data_dir(self) -> Path | None:
        repo = getattr(self.engine, "repo", None)
        return getattr(getattr(repo, "store", None), "data_dir", None)

    def _assert_data_generation(
        self,
        asset_type: str,
        expected: str | None,
    ) -> None:
        verifier = getattr(self.engine, "assert_data_generation", None)
        if callable(verifier):
            verifier(asset_type, expected)

    def _load_factor_panel(
        self,
        config: FactorConfig | FactorBatchConfig,
        factor_names: list[str],
        *,
        expected_generation: str | None = None,
        warmup_bars: int = 1,
    ) -> pl.DataFrame:
        panel_columns = [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "turnover_rate",
        ]
        from app.indicators.pipeline import ENRICHED_STORAGE_COLS

        required = set(factor_calculation_order(factor_names))
        panel_columns.extend(sorted((required & set(ENRICHED_STORAGE_COLS)) - set(panel_columns)))
        load_start = self._factor_load_start(config, factor_names, warmup_bars=warmup_bars)

        load_kwargs = {
            "columns": panel_columns,
            "asset_type": config.asset_type,
        }
        if expected_generation is not None:
            load_kwargs["expected_generation"] = expected_generation
        panel = self.engine.load_panel(
            config.symbols,
            load_start,
            config.end,
            **load_kwargs,
        )
        if panel.is_empty():
            return panel

        fundamental_names = sorted(required & FUNDAMENTAL_FACTOR_NAMES)
        if fundamental_names:
            # 点时财务因子: 公告日门控, 无数据标的保持 null (不参与该日截面)。
            panel = attach_fundamental_factors(
                panel,
                load_fundamental_snapshot(self._fundamentals_data_dir()),
                fundamental_names,
            )
        missing = set(factor_names) - set(panel.columns)
        if missing:
            panel = self._compute_missing_factors(
                panel, missing, data_dir=self._fundamentals_data_dir()
            )
        return panel

    def _factor_load_start(self, config, factor_names, *, warmup_bars: int = 1) -> date:
        warmup_bars = max(warmup_bars, factor_warmup_bars(factor_names))
        if set(factor_names) == {"turnover_rate"} and warmup_bars == 1:
            return config.start
        return history_load_start(
            self.engine,
            config.start,
            config.asset_type,
            warmup_bars,
            minimum_days=FACTOR_WARMUP_DAYS,
        )

    def _global_trading_dates(
        self,
        config: FactorConfig | FactorBatchConfig,
    ) -> list[date] | None:
        """Read the market date axis from enriched partitions, independent of symbols."""
        repo = getattr(self.engine, "repo", None)
        data_dir = getattr(getattr(repo, "store", None), "data_dir", None)
        if data_dir is None:
            return None
        from app.tickflow.repository import enriched_dirname

        values: list[date] = []
        root = data_dir / enriched_dirname(config.asset_type)
        for partition in root.glob("date=*"):
            try:
                value = date.fromisoformat(partition.name.removeprefix("date="))
            except ValueError:
                continue
            if value <= config.end and (partition / "part.parquet").is_file():
                values.append(value)
        ordered = sorted(set(values))
        formal = [value for value in ordered if value >= config.start]
        predecessor = next(
            (value for value in reversed(ordered) if value < config.start),
            None,
        )
        if predecessor is not None:
            formal.insert(0, predecessor)
        return formal or None

    @staticmethod
    def _attach_daily_forward_returns(
        base: pl.DataFrame,
        all_dates: list[date],
        date_dtype: pl.DataType,
        forward_columns: list[str],
    ) -> pl.DataFrame:
        """Attach all configured daily forward returns to the prepared price base."""
        for horizon, return_column in zip(
            _DAILY_FORWARD_HORIZONS,
            forward_columns,
            strict=True,
        ):
            if len(all_dates) <= horizon:
                base = base.with_columns(pl.lit(None).cast(pl.Float64).alias(return_column))
                continue
            target_column = f"_target_date_{horizon}d"
            target_close_column = f"_target_close_{horizon}d"
            date_map = pl.DataFrame(
                {
                    "date": all_dates[:-horizon],
                    target_column: all_dates[horizon:],
                }
            ).with_columns(
                pl.col("date").cast(date_dtype),
                pl.col(target_column).cast(date_dtype),
            )
            price_lookup = base.select(
                "symbol",
                pl.col("date").alias(target_column),
                pl.col("close").alias(target_close_column),
            )
            base = (
                base.join(date_map, on="date", how="left")
                .join(price_lookup, on=["symbol", target_column], how="left")
                .with_columns(
                    pl.when(pl.col(target_close_column).is_not_null())
                    .then(pl.col(target_close_column) / pl.col("close") - 1.0)
                    .otherwise(None)
                    .cast(pl.Float64)
                    .alias(return_column)
                )
                .drop([target_column, target_close_column])
            )
        return base

    @staticmethod
    def _attach_shared_next_return(
        panel: pl.DataFrame,
        config: FactorConfig | FactorBatchConfig,
        *,
        trading_dates: list[date] | None = None,
    ) -> pl.DataFrame:
        """Prepare returns once on the complete price axis before factor filtering."""
        forward_columns = [f"_forward_return_{horizon}d" for horizon in _DAILY_FORWARD_HORIZONS]
        prepared_columns = ["_next_return", *forward_columns]
        if all(column in panel.columns for column in prepared_columns):
            return panel
        existing_columns = [column for column in prepared_columns if column in panel.columns]
        if existing_columns:
            panel = panel.drop(existing_columns)

        base = (
            panel.filter((pl.col("date") >= config.start) & (pl.col("date") <= config.end))
            .filter(pl.col("close").is_not_null() & (pl.col("close") > 0))
            .select(["symbol", "date", "close"])
            .unique(subset=["symbol", "date"], keep="last")
            .sort(["symbol", "date"])
        )
        if base.is_empty():
            return panel.with_columns(
                [pl.lit(None).cast(pl.Float64).alias(column) for column in forward_columns]
                + [pl.lit(None).cast(pl.Float64).alias("_next_return")]
            )

        all_dates = sorted(
            value
            for value in (
                trading_dates if trading_dates is not None else base["date"].unique().to_list()
            )
            if config.start <= value <= config.end
        )
        date_dtype = base.schema["date"]
        base = FactorCoreMixin._attach_daily_forward_returns(
            base,
            all_dates,
            date_dtype,
            forward_columns,
        )

        if config.rebalance == "daily":
            base = base.with_columns(pl.col("_forward_return_1d").alias("_next_return"))
        else:
            base = FactorStatsMixin._calc_period_return(base, config.rebalance)

        return panel.join(
            base.select(["symbol", "date", "_next_return", *forward_columns]),
            on=["symbol", "date"],
            how="left",
        )

    def _prepare_eval_panel(
        self,
        source_panel: pl.DataFrame,
        config: FactorConfig,
        run_id: str,
        t0: float,
    ) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]] | FactorResult:
        if "close" not in source_panel.columns:
            return self._error_result(config, run_id, t0, "enriched 数据缺少收盘价 close")
        if "_next_return" not in source_panel.columns:
            source_panel = self._attach_shared_next_return(source_panel, config)

        factor_col = config.factor_name
        return_columns = [
            column
            for column in (
                "_next_return",
                *(f"_forward_return_{horizon}d" for horizon in _DAILY_FORWARD_HORIZONS),
            )
            if column in source_panel.columns
        ]
        price_panel = (
            source_panel.select(["symbol", "date", "close", factor_col, *return_columns])
            .filter((pl.col("date") >= config.start) & (pl.col("date") <= config.end))
            .filter(pl.col("close").is_not_null() & (pl.col("close") > 0))
        )
        panel = price_panel.filter(
            pl.col(factor_col).is_not_null() & pl.col(factor_col).is_finite()
        )
        if panel.is_empty():
            return self._error_result(config, run_id, t0, "过滤后无有效数据")

        panel = panel.sort(["symbol", "date"])
        total_price_rows = price_panel.height
        meta = {
            "coverage": panel.height / total_price_rows if total_price_rows else None,
            "n_symbols": panel["symbol"].n_unique(),
            "n_dates": panel["date"].n_unique(),
        }
        return panel, price_panel, meta

    def _calc_ic_summary(
        self,
        panel: pl.DataFrame,
        price_panel: pl.DataFrame,
        config: FactorConfig,
        regime_by_date: Mapping[object, Any] | None,
        market_trading_dates: list[date] | None,
    ) -> dict[str, Any]:
        factor_col = config.factor_name
        ic_df = self._calc_ic(panel, factor_col)
        valid_ic_df = ic_df.filter(pl.col("ic").is_not_null() & pl.col("ic").is_finite())
        ic_series = [
            {"date": str(row["date"]), "ic": round(float(row["ic"]), 4)}
            for row in valid_ic_df.iter_rows(named=True)
        ]
        ic_values = valid_ic_df["ic"].to_numpy() if not valid_ic_df.is_empty() else np.array([])
        ic_mean = float(np.mean(ic_values)) if ic_values.size else None
        ic_std = float(np.std(ic_values)) if ic_values.size else None
        ir = (ic_mean / ic_std) if (ic_mean is not None and ic_std and ic_std > 1e-8) else None
        return {
            "ic_series": ic_series,
            "ic_mean": ic_mean,
            "ic_std": ic_std,
            "ir": ir,
            "ic_win_rate": float(np.mean(ic_values > 0)) if ic_values.size else None,
            "yearly_ic": self._calc_yearly_ic(valid_ic_df),
            "ic_decay": self._calc_ic_decay(panel, factor_col),
            "regime_stats": self._calc_regime_stats(
                valid_ic_df,
                price_panel,
                regime_by_date,
                config.start,
                config.end,
                market_trading_dates=market_trading_dates,
            ),
        }

    def _evaluate_panel(
        self,
        source_panel: pl.DataFrame,
        config: FactorConfig,
        run_id: str,
        t0: float,
        *,
        regime_by_date: Mapping[object, Any] | None = None,
        market_trading_dates: list[date] | None = None,
    ) -> FactorResult:
        factor_col = config.factor_name
        if factor_col not in source_panel.columns:
            return self._error_result(
                config,
                run_id,
                t0,
                f"因子列 '{factor_col}' 不存在于 enriched 数据中, 且无法从基础行情计算",
            )
        prepared = self._prepare_eval_panel(source_panel, config, run_id, t0)
        if isinstance(prepared, FactorResult):
            return prepared
        panel, price_panel, meta = prepared

        ic = self._calc_ic_summary(panel, price_panel, config, regime_by_date, market_trading_dates)
        panel = self._add_groups(panel, factor_col, config.n_groups)
        group_nav = self._calc_group_nav(panel, config)
        group_stats = self._calc_group_stats(group_nav, config.start, config.end, config.rebalance)
        turnover = self._calc_turnover(panel, config)
        long_short_nav, long_short_stats = self._calc_long_short(group_nav, config)
        elapsed = (time.perf_counter() - t0) * 1000
        return FactorResult(
            run_id=run_id,
            config=FactorStatsMixin._config_to_dict(config),
            ic_mean=round(ic["ic_mean"], 4) if ic["ic_mean"] is not None else None,
            ic_std=round(ic["ic_std"], 4) if ic["ic_std"] is not None else None,
            ir=round(ic["ir"], 4) if ic["ir"] is not None else None,
            ic_win_rate=round(ic["ic_win_rate"], 4) if ic["ic_win_rate"] is not None else None,
            ic_series=ic["ic_series"],
            group_stats=group_stats,
            group_nav=self._round_nav(group_nav),
            long_short_stats=long_short_stats,
            long_short_nav=long_short_nav,
            coverage=round(meta["coverage"], 4) if meta["coverage"] is not None else None,
            turnover=round(turnover, 4) if turnover is not None else None,
            long_short_sharpe=long_short_stats.get("sharpe"),
            yearly_ic=ic["yearly_ic"],
            ic_decay=ic["ic_decay"],
            regime_stats=ic["regime_stats"],
            elapsed_ms=round(elapsed, 1),
            n_symbols=meta["n_symbols"],
            n_dates=meta["n_dates"],
        )

    @staticmethod
    def _compute_missing_factors(
        panel: pl.DataFrame,
        factor_cols: set[str],
        *,
        assume_sorted: bool = False,
        data_dir: Path | None = None,
    ) -> pl.DataFrame:
        required = {"symbol", "date", "open", "high", "low", "close", "volume"}
        if not required.issubset(panel.columns):
            missing = sorted(required - set(panel.columns))
            logger.warning(
                "factors %s cannot be computed, missing columns: %s", factor_cols, missing
            )
            return panel

        # 扩展表因子 (ext_ base 条目) = 外部物化列, 指标补算管线不认识;
        # 请求的因子集合命中时在此按 (symbol, date) 时序对齐注入 (与
        # compute_signals 同一原语, 历史帧不含快照 → 无未来函数)。
        from app.factors import ext_factors

        ordered = factor_calculation_order(factor_cols)
        if any(name.startswith("ext_") for name in ordered):
            panel = ext_factors.attach_ext_columns(panel, include_snapshot=False, data_dir=data_dir)

        from app.indicators.pipeline import compute_indicators

        # custom/composite 因子走注册表→DSL/组合 的同一条物化路径 (P3, 与策略评分共用);
        # 其底层依赖 (如 change_pct/ma20) 先经内置补算路径物化, 再做 DSL/组合物化。
        registry_names = {
            name
            for name in ordered
            if (spec := get_factor(name)) is not None and spec.kind in ("custom", "composite")
        }
        factor_cols = set(ordered) - registry_names - set(panel.columns)

        derived = factor_cols & set(DERIVED_FACTOR_DEPENDENCIES)
        indicator_columns = factor_cols - derived
        for factor_name in derived:
            indicator_columns.update(DERIVED_FACTOR_DEPENDENCIES[factor_name])
        panel = compute_indicators(
            panel,
            needed=indicator_columns,
            assume_sorted=assume_sorted,
        )
        panel = FactorCoreMixin._compute_derived_factors(panel, derived)
        return materialize_scoring_columns(panel, registry_names) if registry_names else panel

    @staticmethod
    def _compute_derived_factors(panel: pl.DataFrame, factor_cols: set[str]) -> pl.DataFrame:
        return materialize_scoring_columns(panel, factor_cols)

    @staticmethod
    def _error_result(
        config: FactorConfig,
        run_id: str,
        started_at: float,
        message: str,
    ) -> FactorResult:
        return FactorResult(
            run_id=run_id,
            config=FactorStatsMixin._config_to_dict(config),
            error=message,
            elapsed_ms=round((time.perf_counter() - started_at) * 1000, 1),
        )

    @staticmethod
    def _calc_ic(panel: pl.DataFrame, factor_col: str) -> pl.DataFrame:
        """计算截面 Rank IC (因子值 rank vs 下期收益 rank 的相关系数)。"""
        return (
            panel.filter(pl.col("_next_return").is_not_null())
            .group_by("date")
            .agg(
                pl.corr(
                    pl.col(factor_col).rank(method="average"),
                    pl.col("_next_return").rank(method="average"),
                ).alias("ic")
            )
            .sort("date")
        )

    @staticmethod
    def _calc_yearly_ic(ic_df: pl.DataFrame) -> list[dict]:
        if ic_df.is_empty():
            return []
        yearly = (
            ic_df.with_columns(pl.col("date").dt.year().alias("_year"))
            .group_by("_year")
            .agg(
                pl.col("ic").mean().alias("ic_mean"),
                pl.col("ic").std(ddof=0).alias("ic_std"),
                (pl.col("ic") > 0).mean().alias("win_rate"),
                pl.len().alias("n_dates"),
            )
            .sort("_year")
        )
        result: list[dict] = []
        for row in yearly.iter_rows(named=True):
            mean = float(row["ic_mean"])
            std = float(row["ic_std"] or 0.0)
            result.append(
                {
                    "year": int(row["_year"]),
                    "ic_mean": round(mean, 4),
                    "ir": round(mean / std, 4) if std > 1e-8 else None,
                    "win_rate": round(float(row["win_rate"]), 4),
                    "n_dates": int(row["n_dates"]),
                }
            )
        return result

    @staticmethod
    def _calc_ic_decay(panel: pl.DataFrame, factor_col: str) -> list[dict]:
        columns = [
            (horizon, f"_forward_return_{horizon}d")
            for horizon in _DAILY_FORWARD_HORIZONS
            if f"_forward_return_{horizon}d" in panel.columns
        ]
        if not columns:
            return []
        decay_df = (
            panel.group_by("date")
            .agg(
                [
                    pl.corr(
                        pl.col(factor_col).rank(method="average"),
                        pl.col(return_column).rank(method="average"),
                    ).alias(f"ic_{horizon}d")
                    for horizon, return_column in columns
                ]
            )
            .sort("date")
        )
        result: list[dict] = []
        for horizon, _ in columns:
            column = f"ic_{horizon}d"
            values = decay_df.filter(pl.col(column).is_not_null() & pl.col(column).is_finite())[
                column
            ]
            result.append(
                {
                    "horizon": horizon,
                    "ic_mean": round(float(values.mean()), 4) if len(values) else None,
                    "n_dates": len(values),
                }
            )
        return result
