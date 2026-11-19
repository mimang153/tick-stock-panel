"""StrategyEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl

import app.strategy.engine as _engine_pkg
from app.strategy.scoring import (
    SCORING_DIRECTION_LOW,
    materialize_scoring_columns,
    scoring_value_expr,
)


class _StrategyEngineMixin4:
    @staticmethod
    def _join_basic_columns(df: pl.DataFrame, current: pl.DataFrame) -> pl.DataFrame:
        """把 enriched 快照列按 symbol 联到分钟策略输出上, 只补 df 缺失的列。"""
        cols = [
            c
            for c in _engine_pkg.StrategyEngine.MINUTE_JOIN_COLUMNS
            if c in current.columns and c not in df.columns
        ]
        if not cols:
            return df
        extra = current.select(["symbol", *cols]).unique(subset=["symbol"], keep="last")
        return df.join(extra, on="symbol", how="left")

    # ================================================================
    # 内部: 评分
    # ================================================================

    @staticmethod
    def _apply_scoring(
        df: pl.DataFrame,
        weights: dict,
        directions: Mapping[str, str] | None = None,
    ) -> pl.DataFrame:
        """通用评分: min-max 归一化 → 加权求和 → 0~100 分"""
        if not weights:
            return df

        executable = [
            (str(col), value, weight)
            for col, weight in weights.items()
            if weight and (value := scoring_value_expr(df.columns, str(col))) is not None
        ]
        total_weight = sum(weight for _, _, weight in executable)
        if total_weight <= 0:
            return df

        score_parts: list[pl.Expr] = []
        for name, value, weight in executable:
            w = weight / total_weight
            col_min = value.min()
            col_range = value.max() - col_min
            normalized = (
                pl.when(col_range > 0).then((value - col_min) / col_range).otherwise(pl.lit(0.5))
            )
            if (directions or {}).get(name) == SCORING_DIRECTION_LOW:
                normalized = 1.0 - normalized
            score_parts.append(normalized * w)

        if not score_parts:
            return df

        score_expr = score_parts[0]
        for part in score_parts[1:]:
            score_expr = score_expr + part
        return df.with_columns((score_expr * 100).alias("score"))

    @staticmethod
    def _materialize_scoring_frames(
        current: pl.DataFrame | None,
        history: pl.DataFrame | None,
        scoring: Mapping[str, Any],
    ) -> tuple[pl.DataFrame | None, pl.DataFrame | None]:
        names = [str(name) for name, weight in scoring.items() if weight]
        if not names:
            return current, history
        if history is None or history.is_empty():
            return (
                materialize_scoring_columns(current, names) if current is not None else None,
                history,
            )

        scored_history = materialize_scoring_columns(history, names)
        if current is None or current.is_empty():
            return current, scored_history
        join_keys = [
            key
            for key in ("symbol", "date", "datetime")
            if key in current.columns and key in scored_history.columns
        ]
        added = [
            name for name in names if name not in current.columns and name in scored_history.columns
        ]
        if not join_keys or not added:
            return materialize_scoring_columns(current, names), scored_history
        values = scored_history.select([*join_keys, *added]).unique(subset=join_keys, keep="last")
        return current.join(values, on=join_keys, how="left"), scored_history
