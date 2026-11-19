"""Internal split module for app.backtest.factor: _stats."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from itertools import pairwise
from typing import Any

import numpy as np
import polars as pl

from ._common import FactorBatchConfig, FactorConfig


class FactorStatsMixin:
    @staticmethod
    def _summarize_regime_groups(
        grouped: dict[str, dict[str, list[float]]],
    ) -> list[dict]:
        order = {"strong": 0, "range": 1, "weak": 2}
        result: list[dict] = []
        for state in sorted(grouped, key=lambda value: (order.get(value, 99), value)):
            values = np.asarray(grouped[state]["ics"], dtype=np.float64)
            scores = np.asarray(grouped[state]["scores"], dtype=np.float64)
            mean = float(np.mean(values)) if values.size else None
            std = float(np.std(values)) if values.size else None
            result.append(
                {
                    "state": state,
                    "ic_mean": round(mean, 4) if mean is not None else None,
                    "ir": (
                        round(mean / std, 4)
                        if mean is not None and std is not None and std > 1e-8
                        else None
                    ),
                    "win_rate": round(float(np.mean(values > 0)), 4) if values.size else None,
                    "mean_score": round(float(np.mean(scores)), 2),
                    "n_dates": len(grouped[state]["dates"]),
                    "n_ic_dates": int(values.size),
                }
            )
        return result

    @staticmethod
    def _calc_regime_stats(
        ic_df: pl.DataFrame,
        price_panel: pl.DataFrame,
        regime_by_date: Mapping[object, Any] | None,
        required_start: date,
        required_end: date,
        *,
        market_trading_dates: list[date] | None = None,
    ) -> list[dict]:
        if not regime_by_date:
            return []

        from app.backtest.regime_alignment import (
            align_regime_t_minus_one,
            three_level_regime,
        )

        formal_labels = tuple(
            str(value)[:10] for value in sorted(price_panel["date"].unique().to_list())
        )
        if market_trading_dates:
            labels = tuple(
                str(value)[:10] for value in market_trading_dates if value <= required_end
            )
        else:
            predecessor = max(
                (
                    str(value)[:10]
                    for value in regime_by_date
                    if str(value)[:10] < str(required_start)
                ),
                default=None,
            )
            labels = (predecessor, *formal_labels) if predecessor is not None else formal_labels
        aligned = align_regime_t_minus_one(
            labels,
            regime_by_date,
            required_start,
            required_end,
            # 统计场景: 数据边界即正式首日 (如「全部」/「1年」范围起点=本地数据首日) 时,
            # 首日无 T-1 环境属正常, 跳过首日不参与环境分组即可, 不阻断回测;
            # 与策略回测 clamp_formal_start_for_regime 的「首日让渡」同口径。
            # 内部缺口 (次日 T-1 缺环境) 仍 fail-closed 报错。
            first_day_boundary_ok=True,
        )
        ic_by_date = {
            str(row["date"])[:10]: float(row["ic"]) for row in ic_df.iter_rows(named=True)
        }
        grouped: dict[str, dict[str, list[Any]]] = {}
        for label, point in zip(labels, aligned, strict=True):
            if label not in formal_labels or point is None:
                continue
            state, score = point
            bucket = grouped.setdefault(
                three_level_regime(state),
                {"scores": [], "ics": [], "dates": []},
            )
            bucket["scores"].append(score)
            bucket["dates"].append(label)
            if label in ic_by_date:
                bucket["ics"].append(ic_by_date[label])

        return FactorStatsMixin._summarize_regime_groups(grouped)

    @staticmethod
    def _get_rebalance_dates(panel: pl.DataFrame, rebalance: str) -> set:
        all_dates = sorted(panel["date"].unique().to_list())
        if rebalance == "weekly":
            rebalance_dates: set = set()
            seen_weeks: set[tuple[int, int]] = set()
            for current_date in all_dates:
                normalized_date = (
                    current_date
                    if hasattr(current_date, "isocalendar")
                    else date.fromisoformat(str(current_date)[:10])
                )
                iso_year, iso_week, _ = normalized_date.isocalendar()
                week = (iso_year, iso_week)
                if week not in seen_weeks:
                    seen_weeks.add(week)
                    rebalance_dates.add(current_date)
        else:
            seen_months: set[str] = set()
            rebalance_dates = set()
            for d in sorted(all_dates):
                m = str(d)[:7]
                if m not in seen_months:
                    seen_months.add(m)
                    rebalance_dates.add(d)
        return rebalance_dates

    @staticmethod
    def _calc_period_return(panel: pl.DataFrame, rebalance: str) -> pl.DataFrame:
        """计算到下个调仓日的收益。

        weekly: 下个周调仓日 close / 今日 close - 1
        monthly: 下个月调仓日 close / 今日 close - 1
        只在调仓日标记行有效，其他行为 null。
        """
        rebalance_dates = FactorStatsMixin._get_rebalance_dates(panel, rebalance)

        if not rebalance_dates:
            panel = panel.with_columns(pl.lit(None).cast(pl.Float64).alias("_next_return"))
            return panel

        # 对每个调仓日，找到下一个调仓日 (仅在 unique 日期上做, 成本极低)
        sorted_rebalance = sorted(rebalance_dates)
        reb_dates: list = []
        next_dates: list = []
        for i, d in enumerate(sorted_rebalance):
            if i + 1 < len(sorted_rebalance):
                reb_dates.append(d)
                next_dates.append(sorted_rebalance[i + 1])
            # 最后一个调仓日没有下一个，不计算收益

        if not reb_dates:
            panel = panel.with_columns(pl.lit(None).cast(pl.Float64).alias("_next_return"))
            return panel

        panel = panel.sort(["symbol", "date"])
        date_dtype = panel.schema["date"]

        # 调仓日 → 下一调仓日 的映射表 (向量化 JOIN, 替代 Python 逐行 price_map 循环)
        rebal_df = pl.DataFrame({"date": reb_dates, "_next_reb_date": next_dates}).with_columns(
            pl.col("date").cast(date_dtype),
            pl.col("_next_reb_date").cast(date_dtype),
        )

        # (symbol, 下一调仓日) → 该日 close 的查找表 (等价于原 price_map, 重复取 last)
        price_lookup = panel.select(
            pl.col("symbol"),
            pl.col("date").alias("_next_reb_date"),
            pl.col("close").alias("_next_close"),
        ).unique(subset=["symbol", "_next_reb_date"], keep="last")

        # 只在调仓日标记行有效: 下一调仓日该股 close / 当日 close - 1; 缺价或非调仓日为 null
        panel = (
            panel.join(rebal_df, on="date", how="left")
            .join(price_lookup, on=["symbol", "_next_reb_date"], how="left")
            .with_columns(
                pl.when(
                    pl.col("_next_reb_date").is_not_null()
                    & pl.col("_next_close").is_not_null()
                    & (pl.col("close") > 0)
                )
                .then(pl.col("_next_close") / pl.col("close") - 1.0)
                .otherwise(None)
                .cast(pl.Float64)
                .alias("_next_return")
            )
            .drop(["_next_reb_date", "_next_close"])
            .sort(["symbol", "date"])
        )
        return panel

    @staticmethod
    def _add_groups(panel: pl.DataFrame, factor_col: str, n_groups: int) -> pl.DataFrame:
        """Tie-aware cross-sectional buckets; equal factor values never split."""
        return (
            panel.with_columns(
                pl.col(factor_col).rank(method="average").over("date").alias("_factor_rank"),
                pl.len().over("date").alias("_factor_count"),
            )
            .with_columns(
                (
                    pl.lit("Q")
                    + (
                        ((pl.col("_factor_rank") - 1.0) * n_groups / pl.col("_factor_count"))
                        .floor()
                        .cast(pl.Int64)
                        + 1
                    )
                    .clip(1, n_groups)
                    .cast(pl.Utf8)
                ).alias("_group"),
                (pl.col("_factor_rank") - (pl.col("_factor_count") + 1) / 2.0)
                .abs()
                .add(0.5)
                .alias("_factor_strength"),
            )
            .drop(["_factor_rank", "_factor_count"])
        )

    @staticmethod
    def _group_sort_key(group: str) -> int:
        if group.startswith("Q"):
            try:
                return int(group[1:])
            except ValueError:
                pass
        return 0

    @staticmethod
    def _round_nav(group_nav: list[dict]) -> list[dict]:
        return [
            {key: value if key == "date" else round(float(value), 4) for key, value in row.items()}
            for row in group_nav
        ]

    @staticmethod
    def _round_trip_cost(config: FactorConfig) -> float:
        commission = config.commission_pct if config.commission_pct is not None else config.fees_pct
        stamp_tax = config.stamp_tax_pct or 0.0
        slippage = config.slippage_bps / 10_000.0
        return 2.0 * commission + stamp_tax + 2.0 * slippage

    @staticmethod
    def _calc_group_nav(panel: pl.DataFrame, config: FactorConfig) -> list[dict]:
        """Calculate group NAV with the configured cross-sectional weighting."""
        eligible = panel.filter(
            pl.col("_next_return").is_not_null() & pl.col("_group").is_not_null()
        )
        if config.weight == "factor_weight":
            group_ret = (
                eligible.with_columns(
                    pl.when(pl.col("_factor_strength") > 0)
                    .then(pl.col("_factor_strength"))
                    .otherwise(1.0)
                    .alias("_weight")
                )
                .group_by(["date", "_group"])
                .agg(
                    (
                        (pl.col("_next_return") * pl.col("_weight")).sum() / pl.col("_weight").sum()
                    ).alias("group_return")
                )
            )
        else:
            group_ret = eligible.group_by(["date", "_group"]).agg(
                pl.col("_next_return").mean().alias("group_return")
            )
        group_ret = group_ret.with_columns(
            (pl.col("group_return") - FactorStatsMixin._round_trip_cost(config)).alias(
                "group_return"
            )
        )

        pivot = group_ret.pivot(
            index="date",
            on="_group",
            values="group_return",
        ).sort("date")
        if pivot.is_empty():
            return []

        group_cols = sorted(
            [column for column in pivot.columns if column != "date"],
            key=FactorStatsMixin._group_sort_key,
        )
        nav_df = pivot.with_columns(
            [
                (1.0 + pl.col(column).fill_null(0.0)).cum_prod().alias(column)
                for column in group_cols
            ]
        )
        result: list[dict] = []
        for row in nav_df.iter_rows(named=True):
            entry: dict = {"date": str(row["date"])[:10]}
            for column in group_cols:
                entry[column] = float(row[column])
            result.append(entry)
        return result

    @staticmethod
    def _calc_turnover(panel: pl.DataFrame, config: FactorConfig) -> float | None:
        eligible = panel.filter(
            pl.col("_next_return").is_not_null() & pl.col("_group").is_not_null()
        )
        if eligible.is_empty():
            return None
        groups = sorted(
            eligible["_group"].unique().to_list(),
            key=FactorStatsMixin._group_sort_key,
        )
        if not groups:
            return None
        top_group = groups[-1]
        weights = eligible.filter(pl.col("_group") == top_group)
        if config.weight == "factor_weight":
            weights = weights.with_columns(
                pl.when(pl.col("_factor_strength") > 0)
                .then(pl.col("_factor_strength"))
                .otherwise(1.0)
                .alias("_raw_weight")
            )
        else:
            weights = weights.with_columns(pl.lit(1.0).alias("_raw_weight"))
        weights = weights.with_columns(
            (pl.col("_raw_weight") / pl.col("_raw_weight").sum().over("date")).alias("_weight")
        )

        by_date: dict[Any, dict[str, float]] = {}
        for row in weights.select(["date", "symbol", "_weight"]).iter_rows(named=True):
            by_date.setdefault(row["date"], {})[str(row["symbol"])] = float(row["_weight"])
        ordered_dates = sorted(by_date)
        if len(ordered_dates) < 2:
            return 0.0
        turnovers: list[float] = []
        for previous_date, current_date in pairwise(ordered_dates):
            previous = by_date[previous_date]
            current = by_date[current_date]
            symbols = previous.keys() | current.keys()
            turnovers.append(
                0.5
                * sum(
                    abs(current.get(symbol, 0.0) - previous.get(symbol, 0.0)) for symbol in symbols
                )
            )
        return float(np.mean(turnovers))

    @staticmethod
    def _calc_group_stats(
        group_nav: list[dict],
        start: date,
        end: date,
        rebalance: str = "monthly",
    ) -> list[dict]:
        if not group_nav:
            return []

        group_cols = sorted(
            [k for k in group_nav[0] if k != "date"],
            key=FactorStatsMixin._group_sort_key,
        )
        n_days = max((end - start).days, 1)
        years = n_days / 365.25
        # 夏普 — 年化系数必须匹配 group_nav 的调仓频率 (每个净值点 = 一个调仓周期收益);
        # 周/月频收益若乘 √252 会把 Sharpe 高估 √(252/期数) 倍 (月频 ≈4.6x, 周频 ≈2.2x)。
        _ann = {"daily": 252, "weekly": 52, "monthly": 12}.get(rebalance, 252)

        stats = []
        for i, c in enumerate(group_cols):
            values = [r[c] for r in group_nav if r.get(c) is not None]
            if not values:
                continue
            arr = np.asarray(values, dtype=np.float64)
            last = float(arr[-1])
            total_return = last - 1.0
            annual_return = last ** (1 / max(years, 0.01)) - 1 if last > 0 else 0.0

            # 最大回撤 (向量化): 峰值 = max(1.0, 历史最高), 与原 peak 初值 1.0 的逐行 max 一致
            peak = np.maximum(np.maximum.accumulate(arr), 1.0)
            max_dd = float(np.min((arr - peak) / peak))

            # 周期收益序列 (向量化): nav[t]/nav[t-1] - 1, 仅保留 nav[t-1] > 0 的样本
            prev = arr[:-1]
            with np.errstate(divide="ignore", invalid="ignore"):
                rets = arr[1:] / prev - 1.0
            rets = rets[prev > 0]
            if rets.size:
                # 样本标准差 (ddof=1), 与 engine 的 Sharpe 口径一致
                std = float(np.std(rets, ddof=1))
                sharpe = float(np.mean(rets) / std) * np.sqrt(_ann) if std > 0 else 0.0
                win_rate = float(np.mean(rets > 0))
            else:
                sharpe = 0.0
                win_rate = 0.0

            stats.append(
                {
                    "group": i + 1,
                    "label": c,
                    "total_return": round(total_return, 4),
                    "annual_return": round(annual_return, 4),
                    "max_drawdown": round(max_dd, 4),
                    "sharpe": round(sharpe, 2),
                    "win_rate": round(win_rate, 4),
                }
            )

        return stats

    @staticmethod
    def _calc_long_short(
        group_nav: list[dict],
        config: FactorConfig,
    ) -> tuple[list[dict], dict]:
        """多空组合: 做多最高组 + 做空最低组。"""
        if not group_nav:
            return [], {}

        group_cols = sorted(
            [k for k in group_nav[0] if k != "date"],
            key=FactorStatsMixin._group_sort_key,
        )
        if len(group_cols) < 2:
            return [], {}

        top_col = group_cols[-1]  # Q5 (最高)
        bottom_col = group_cols[0]  # Q1 (最低)

        # 向量化: 各组净值 (null 视为 1.0), 前置 1.0 作为初值 prev_top/prev_bot,
        # 等价于原逐行 prev_top/prev_bot 初始 1.0 的累乘逻辑。
        top = np.array(
            [r[top_col] if r.get(top_col) is not None else 1.0 for r in group_nav],
            dtype=np.float64,
        )
        bot = np.array(
            [r[bottom_col] if r.get(bottom_col) is not None else 1.0 for r in group_nav],
            dtype=np.float64,
        )
        prev_top = np.concatenate(([1.0], top[:-1]))
        prev_bot = np.concatenate(([1.0], bot[:-1]))

        # 分组收益; prev <= 0 时按原逻辑置 0 (做多 top, 做空 bottom = 取反)
        with np.errstate(divide="ignore", invalid="ignore"):
            top_ret = np.where(prev_top > 0, top / prev_top - 1.0, 0.0)
            bot_ret = np.where(prev_bot > 0, bot / prev_bot - 1.0, 0.0)
        ls_ret = (top_ret - bot_ret) / 2.0 - FactorStatsMixin._round_trip_cost(
            config
        )  # 50/50 理论多空两腿
        ls_value = np.cumprod(1.0 + ls_ret)

        # 最大回撤: 峰值 = max(1.0, 历史最高), 与原 peak 初值 1.0 一致
        peak = np.maximum(np.maximum.accumulate(ls_value), 1.0)
        max_dd = float(np.min((ls_value - peak) / peak))

        ann_factor = {"daily": 252, "weekly": 52, "monthly": 12}.get(
            config.rebalance,
            252,
        )
        ls_std = float(np.std(ls_ret, ddof=1))  # 样本标准差, 与 engine 口径一致
        sharpe = float(np.mean(ls_ret) / ls_std) * np.sqrt(ann_factor) if ls_std > 1e-8 else 0.0
        ls_nav = [
            {"date": group_nav[k]["date"], "value": round(float(ls_value[k]), 4)}
            for k in range(len(group_nav))
        ]
        ls_stats = {
            "total_return": round(float(ls_value[-1]) - 1.0, 4),
            "max_drawdown": round(max_dd, 4),
            "sharpe": round(sharpe, 4),
            "top_group": top_col,
            "bottom_group": bottom_col,
            "portfolio_type": "theoretical_factor_spread",
            "executable_short": False,
        }

        return ls_nav, ls_stats

    @staticmethod
    def _config_to_dict(c: FactorConfig) -> dict:
        return {
            "factor_name": c.factor_name,
            "symbols": c.symbols,
            "start": str(c.start),
            "end": str(c.end),
            "n_groups": c.n_groups,
            "rebalance": c.rebalance,
            "weight": c.weight,
            "fees_pct": c.fees_pct,
            "commission_pct": c.commission_pct,
            "stamp_tax_pct": c.stamp_tax_pct,
            "slippage_bps": c.slippage_bps,
            "asset_type": c.asset_type,
        }

    @staticmethod
    def _batch_config_to_dict(c: FactorBatchConfig, factor_names: list[str]) -> dict:
        return {
            "factor_names": factor_names,
            "symbols": c.symbols,
            "start": str(c.start),
            "end": str(c.end),
            "n_groups": c.n_groups,
            "rebalance": c.rebalance,
            "weight": c.weight,
            "fees_pct": c.fees_pct,
            "commission_pct": c.commission_pct,
            "stamp_tax_pct": c.stamp_tax_pct,
            "slippage_bps": c.slippage_bps,
            "asset_type": c.asset_type,
        }
