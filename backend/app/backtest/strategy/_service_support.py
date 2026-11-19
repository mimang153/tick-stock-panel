from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl

from app.backtest.engine import BacktestEngine
from app.backtest.matrix import MarketDataMatrix
from app.strategy.engine import StrategyDef, StrategyEngine
from app.strategy.scoring import (
    SCORING_DIRECTION_LOW,
    effective_scoring,
    effective_scoring_directions,
    materialize_scoring_columns,
    scoring_value_expr,
)

from ._common import (
    _OVERLAY_NP_OPS,
    BENCHMARK_SYMBOL,
    StrategyBacktestConfig,
    _normalize_signal_name,
    logger,
)


class StrategyBacktestSupportMixin:
    """共享状态初始化、掩码/评分/序列化等支撑方法。"""

    def __init__(
        self,
        engine: BacktestEngine,
        strategy_engine: StrategyEngine,
    ) -> None:
        self.engine = engine
        self.strategy_engine = strategy_engine

    # ── 向量化信号生成 ──

    @staticmethod
    def _date_range_mask(panel: pl.DataFrame, start: date, end: date) -> pl.Series:
        return (
            panel.select(((pl.col("date") >= start) & (pl.col("date") <= end)).alias("_range"))[
                "_range"
            ]
            .fill_null(False)
            .cast(pl.Boolean)
        )

    @staticmethod
    def _matrix_date_range_mask(
        timestamp_labels: tuple[str, ...],
        start: date,
        end: date,
    ) -> np.ndarray:
        start_text = str(start)
        end_text = str(end)
        return np.fromiter(
            (start_text <= label[:10] <= end_text for label in timestamp_labels),
            dtype=bool,
            count=len(timestamp_labels),
        )

    @staticmethod
    def _clamp_regime_formal_start(
        config: StrategyBacktestConfig, labels: tuple[str, ...] | list[str]
    ) -> StrategyBacktestConfig:
        """环境过滤下正式起点=面板首日 (无前驱交易日) 时, 顺延到第二个交易日。

        数据边界即正式起点 (如「全部」范围) 时, T-1 环境校验会 fail-closed 拒绝;
        首日降级为预热后, 其环境即成为次日的 T-1, 仅损失 1 个正式交易日。
        """
        from app.backtest.regime_alignment import clamp_formal_start_for_regime

        shifted = clamp_formal_start_for_regime(labels, config.start, config.regime_filter)
        if shifted is not None and shifted != config.start:
            return replace(config, start=shifted)
        return config

    @staticmethod
    def _build_regime_mask(
        timestamp_labels: tuple[str, ...],
        regime_filter: dict | None,
        data_dir: Path | None,
        *,
        required_start: date | None = None,
        required_end: date | None = None,
        evidence=None,
    ) -> np.ndarray | None:
        """构造逐日 T-1 regime mask, 保留历史静态入口兼容调用方。"""
        if not regime_filter:
            return None
        allowed_states = set(regime_filter.get("states") or [])
        min_score = regime_filter.get("min_score")
        if not allowed_states and min_score is None:
            return None
        if data_dir is None:
            raise ValueError("市场环境过滤不可用: 未找到环境数据目录")

        from app.backtest.regime_alignment import build_regime_filter_mask
        from app.services import regime_builder

        regime_df = regime_builder.load_regime_history(data_dir)
        if evidence is not None:
            evidence.record_frame("regime_history", regime_df)
        regime_by_date = {
            row["date"]: {
                "state": row.get("state", ""),
                "score": row.get("score", 0),
            }
            for row in regime_df.iter_rows(named=True)
            if row.get("date") is not None
        }
        return build_regime_filter_mask(
            timestamp_labels,
            regime_filter,
            regime_by_date,
            required_start=required_start,
            required_end=required_end,
        )

    def _evaluate_panel_decisions(
        self,
        panel,
        strategy,
        params,
        overrides,
        basic_filter,
        entry_signals,
        exit_signals,
        *,
        factor_snapshot=None,
    ):
        """生产回测与时间切片检查共用信号管线，日期/成交延迟在其后应用。"""
        basic_mask = pl.Series("_basic", [True] * len(panel), dtype=pl.Boolean)
        if basic_filter and basic_filter.get("enabled", True):
            expr = StrategyEngine._basic_filter_expr(panel, basic_filter)
            if expr is not None:
                basic_mask = (
                    panel.select(expr.alias("_basic"))["_basic"].fill_null(False).cast(pl.Boolean)
                )
        candidate = basic_mask & self._build_candidate_filter_mask(panel, strategy, params)
        panel = self._apply_score(
            panel,
            strategy,
            overrides,
            universe_mask=candidate,
            factor_snapshot=factor_snapshot,
        )
        entry = self._build_entry_mask_from_candidate(panel, candidate, strategy, entry_signals)
        overlay = StrategyEngine._effective_overlay(overrides)
        if overlay:
            mask, error = self._overlay_panel_mask(panel, overlay)
            if error is not None:
                raise ValueError(error)
            entry = entry & mask
        exits = self._build_signal_mask(panel, exit_signals, "_exit")
        return panel, candidate, entry, exits

    @staticmethod
    def _decision_frame(panel, candidate, entry, exits):
        names = [
            name
            for name, dtype in panel.schema.items()
            if name in {"symbol", "date"} or dtype.is_numeric() or dtype == pl.Boolean
        ]
        return panel.select(names).with_columns(
            candidate.rename("_audit_candidate"),
            entry.rename("_audit_entry"),
            exits.rename("_audit_exit"),
        )

    def _build_candidate_filter_mask(
        self,
        panel: pl.DataFrame,
        s: StrategyDef,
        params: dict,
    ) -> pl.Series:
        """生成策略候选层 mask。filter_history/filter 决定候选池, 不包含 entry_signals。"""
        false_mask = pl.Series("_candidate_filter", [False] * len(panel), dtype=pl.Boolean)
        true_mask = pl.Series("_candidate_filter", [True] * len(panel), dtype=pl.Boolean)

        history_failed = False
        # 优先: filter_history_fn 策略 (涨停/反包等多日形态, 与策略路径共用同一逻辑)
        if s.filter_history_fn:
            try:
                hit_df = s.filter_history_fn(panel, params)
                if hit_df is None or hit_df.is_empty():
                    return false_mask
                # 命中行 (symbol,date) → 转 panel 等长布尔 mask
                hits = hit_df.select(["symbol", "date"]).unique()
                marked = panel.select(["symbol", "date"]).join(
                    hits.with_columns(pl.lit(True).alias("_hit")),
                    on=["symbol", "date"],
                    how="left",
                )
                return marked["_hit"].fill_null(False).cast(pl.Boolean)
            except Exception as e:  # noqa: BLE001 - 历史过滤不可用回退 filter_fn
                history_failed = True
                logger.warning("strategy filter_history_fn failed: %s", e)
                # 失败则回退到 filter_fn (若存在)

        # 策略 filter_fn: 候选层 (filter_history 不可用或失败时)
        if s.filter_fn:
            try:
                expr = s.filter_fn(panel, params)
                if expr is not None:
                    result = panel.select(expr.alias("_candidate_filter"))
                    if not result.is_empty():
                        return result["_candidate_filter"].fill_null(False).cast(pl.Boolean)
            except Exception as e:  # noqa: BLE001 - 单策略过滤异常返回空掩码, 隔离失败
                logger.warning("strategy filter_fn failed: %s", e)
                return false_mask

        if history_failed:
            return false_mask

        # 没有策略候选层时, 由 entry_signals 直接决定买点。
        return true_mask

    @staticmethod
    def _overlay_panel_mask(
        panel: pl.DataFrame, conditions: list[dict]
    ) -> tuple[pl.Series | None, str | None]:
        """叠加条件 → panel 行布尔掩码 (polars 路径)。出错返回 (None, errmsg)。

        复用引擎侧同一套编译/物化/fail-closed 语义: 注册表因子列缺失时先物化,
        物化后仍缺列 (扩展表被删等) 报可读错误而不是静默放宽。
        """
        from app.strategy import custom_signals

        try:
            expr = custom_signals.build_overlay_expr(conditions)
        except ValueError as e:
            return None, f"叠加条件编译失败: {e}"
        if expr is None:
            return None, None
        try:
            enriched = custom_signals.materialize_factor_columns(panel, {"__overlay__": expr})
            mask = enriched.select(expr.fill_null(False).alias("__overlay__"))["__overlay__"]
            return mask.cast(pl.Boolean), None
        except Exception as e:  # noqa: BLE001
            missing = custom_signals.overlay_missing_columns(conditions, panel.columns)
            detail = f"，缺失列: {', '.join(missing)}" if missing else f": {e}"
            return None, f"叠加条件评估失败{detail}"

    @staticmethod
    def _shift_time(arr: np.ndarray, days: int) -> np.ndarray:
        """沿时间轴取 N 个交易日前的值 (前 days 行置 NaN, 与 polars shift 同语义)。

        NaN 在比较结果里一律不命中 (调用方统一 AND ~isnan), 即越界/缺数据
        fail-closed, 与 polars 路径 fill_null(False) 对齐。
        """
        if days <= 0:
            return arr
        out = np.full(arr.shape, np.nan, dtype=float)
        if days < arr.shape[0]:
            out[days:] = arr[:-days]
        return out

    @classmethod
    def _overlay_matrix_mask(
        cls, market_data: MarketDataMatrix, conditions: list[dict]
    ) -> tuple[np.ndarray | None, str | None]:
        """叠加条件 → (T,A) 布尔掩码 (matrix 路径)。出错返回 (None, errmsg)。

        在 market_data.fields (float 数组) 上元素级评估; NaN (缺数据日) 一律
        不命中, 与 polars 路径 fill_null(False) 同语义。leftDays/rightDays
        沿时间轴回看 N 个交易日 (越界前置 NaN → 不命中)。字符串字段在保存期
        已对 matrix_native 策略拒绝, 这里对 contains 再兜底拦截。
        """
        n_times = len(market_data.timestamp_labels)
        n_assets = len(market_data.symbols)
        mask = np.ones((n_times, n_assets), dtype=bool)
        for i, c in enumerate(conditions):
            op = c.get("op")
            fn = _OVERLAY_NP_OPS.get(cast(str, op))
            if fn is None:
                return None, f"第 {i + 1} 个叠加条件: matrix 路径不支持运算符 {op!r}"
            left = str(c.get("left", ""))
            if left not in market_data.fields:
                return None, (
                    f"叠加条件引用的列不在回测矩阵中: {left} (请检查扩展数据拉取/因子计算是否可用)"
                )
            left_days = int(c.get("leftDays", 0) or 0)
            right_days = int(c.get("rightDays", 0) or 0)
            left_arr = cls._shift_time(np.asarray(market_data.fields[left], dtype=float), left_days)
            right = c.get("right")
            if isinstance(right, str) and right.startswith("field:"):
                col = right[len("field:") :]
                if col not in market_data.fields:
                    return None, f"叠加条件引用的列不在回测矩阵中: {col}"
                right_arr = cls._shift_time(
                    np.asarray(market_data.fields[col], dtype=float), right_days
                )
                cond = fn(left_arr, right_arr) & ~np.isnan(left_arr) & ~np.isnan(right_arr)
            else:
                try:
                    value = float(right)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    return None, f"第 {i + 1} 个叠加条件: 右值必须是数字 (matrix 路径)"
                cond = fn(left_arr, value) & ~np.isnan(left_arr)
            mask &= cond
        return mask, None

    def _build_entry_mask_from_candidate(
        self,
        panel: pl.DataFrame,
        candidate_mask: pl.Series,
        s: StrategyDef,
        entry_signals: list[str],
    ) -> pl.Series:
        """向量化生成买入掩码：候选层 AND 买点层；无买点时只用策略候选层。"""
        signal_mask = self._build_signal_mask(panel, entry_signals, "_entry_signal")
        if entry_signals:
            return candidate_mask & signal_mask
        if s.filter_history_fn or s.filter_fn:
            return candidate_mask
        return pl.Series("_entry", [False] * len(panel), dtype=pl.Boolean)

    def _build_entry_mask(
        self,
        panel: pl.DataFrame,
        s: StrategyDef,
        params: dict,
        entry_signals: list[str],
    ) -> pl.Series:
        """兼容旧调用: 候选层 AND 买点层。"""
        candidate_mask = self._build_candidate_filter_mask(panel, s, params)
        return self._build_entry_mask_from_candidate(panel, candidate_mask, s, entry_signals)

    @staticmethod
    def _build_signal_mask(panel: pl.DataFrame, signals: list[str], name: str) -> pl.Series:
        """向量化合并信号列，多个信号 OR。支持内置 signal_ 与自定义 csg_ 前缀。"""
        masks: list[pl.Series] = []
        for sig in signals:
            # csg_ (自定义信号) 直接用；否则按 signal_ 解析
            col = sig if (sig.startswith("signal_") or sig.startswith("csg_")) else f"signal_{sig}"
            if col in panel.columns:
                masks.append(panel[col].fill_null(False).cast(pl.Boolean))

        if not masks:
            return pl.Series(name, [False] * len(panel), dtype=pl.Boolean)

        combined = masks[0]
        for m in masks[1:]:
            combined = combined | m
        return combined

    def _build_benchmark_curve(self, start: date, end: date) -> list[dict]:
        try:
            df = self.engine.repo.get_index_daily(
                BENCHMARK_SYMBOL, start, end, columns=["date", "close"]
            )
        except Exception as e:  # noqa: BLE001 - 基准数据不可用时返回空曲线, 不中断回测
            logger.warning("load benchmark %s failed: %s", BENCHMARK_SYMBOL, e)
            return []

        evidence = getattr(self.engine, "_evidence", None)
        if evidence is not None:
            evidence.record_frame("benchmark", df)

        if df.is_empty() or "close" not in df.columns:
            return []

        df = df.filter(pl.col("close").is_not_null() & (pl.col("close") > 0)).sort("date")
        if df.is_empty():
            return []

        return [
            {
                "date": str(row["date"])[:10],
                "value": round(float(row["close"]), 4),
                "close": round(float(row["close"]), 4),
                "name": "上证指数",
                "symbol": BENCHMARK_SYMBOL,
            }
            for row in df.iter_rows(named=True)
            if row["close"] is not None
        ]

    # ── 工具 ──

    @staticmethod
    def _effective_basic_filter(s: StrategyDef, overrides: dict) -> dict:
        basic_filter = dict(s.basic_filter or {})
        override_filter = overrides.get("basic_filter")
        if isinstance(override_filter, dict):
            basic_filter.update(override_filter)
        return basic_filter

    @staticmethod
    def _effective_signals(overrides: dict, key: str, default: list[str]) -> list[str]:
        value = overrides.get(key)
        if isinstance(value, list):
            return [str(v) for v in value if v]
        return list(default or [])

    @classmethod
    def _has_matrix_signal_override(cls, strategy: StrategyDef, overrides: dict) -> bool:
        """Allow legacy persisted defaults, but reject a real Matrix signal replacement."""
        for key, default in (
            ("entry_signals", strategy.entry_signals),
            ("exit_signals", strategy.exit_signals),
        ):
            if key not in overrides:
                continue
            actual = cls._effective_signals(overrides, key, default)
            expected = [_normalize_signal_name(str(signal)) for signal in (default or [])]
            normalized_actual = [_normalize_signal_name(signal) for signal in actual]
            if normalized_actual != expected:
                return True
        return False

    @staticmethod
    def _override_value(overrides: dict, key: str, default):
        if key in overrides:
            return overrides.get(key)
        return default

    @staticmethod
    def _normalize_pct(value, min_value: float, max_value: float) -> float | None:
        if value is None or value == "":
            return None
        try:
            pct = abs(float(value))
        except (TypeError, ValueError):
            return None
        return min(max(pct, min_value), max_value)

    @staticmethod
    def _normalize_score_range(min_value, max_value) -> tuple[float | None, float | None]:
        def _bound(value) -> float | None:
            if value is None or value == "":
                return None
            try:
                score = float(value)
            except (TypeError, ValueError):
                return None
            if not np.isfinite(score):
                return None
            return min(max(score, 0.0), 100.0)

        score_min = _bound(min_value)
        score_max = _bound(max_value)
        if score_min is not None and score_max is not None and score_min > score_max:
            score_min, score_max = score_max, score_min
        return score_min, score_max

    @staticmethod
    def _normalize_params(params: dict, s: StrategyDef) -> dict:
        normalized = dict(params)
        for param in s.meta.get("params", []):
            pid = param.get("id")
            if not pid:
                continue
            value = normalized.get(pid, param.get("default"))
            p_type = param.get("type")
            if p_type in {"float", "int"}:
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    num = float(param.get("default", 0) or 0)
                if param.get("min") is not None:
                    num = max(num, float(param["min"]))
                if param.get("max") is not None:
                    num = min(num, float(param["max"]))
                normalized[pid] = int(num) if p_type == "int" else num
            elif p_type == "select" and param.get("options"):
                normalized[pid] = value if value in param["options"] else param.get("default")
            elif p_type == "bool":
                if isinstance(value, bool):
                    normalized[pid] = value
                elif isinstance(value, str):
                    normalized[pid] = value.lower() == "true"
                else:
                    normalized[pid] = bool(param.get("default", False))
            else:
                normalized[pid] = value
        return normalized

    @staticmethod
    def _trade_to_dict(t) -> dict:
        return {
            "symbol": t.symbol,
            "name": t.name,
            "entry_date": str(t.entry_date),
            "exit_date": str(t.exit_date),
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "pnl_pct": t.pnl_pct,
            "duration": t.duration,
            "exit_reason": t.exit_reason,
            "shares": t.shares,
            "lots": t.lots,
            "position_pct": t.position_pct,
            "entry_value": t.entry_value,
            "exit_value": t.exit_value,
            "pnl_amount": t.pnl_amount,
            "entry_score": getattr(t, "entry_score", None),
            "entry_signal_date": str(t.entry_signal_date)
            if getattr(t, "entry_signal_date", None) is not None
            else None,
            "exit_signal_date": str(t.exit_signal_date)
            if getattr(t, "exit_signal_date", None) is not None
            else None,
            "blocked_exit_days": getattr(t, "blocked_exit_days", 0),
            "entry_signal_id": getattr(t, "entry_signal_id", None),
            "exit_signal_id": getattr(t, "exit_signal_id", None),
        }

    @staticmethod
    def _config_to_dict(c: StrategyBacktestConfig) -> dict:
        score_min, score_max = StrategyBacktestSupportMixin._normalize_score_range(
            (c.overrides or {}).get("score_min"),
            (c.overrides or {}).get("score_max"),
        )
        return {
            "strategy_id": c.strategy_id,
            "symbols": c.symbols,
            "start": str(c.start),
            "end": str(c.end),
            "params": c.params,
            "overrides": c.overrides,
            "score_min": score_min,
            "score_max": score_max,
            "matching": c.matching,
            "entry_fill": c.entry_fill,
            "exit_fill": c.exit_fill,
            "timing_mode": (
                "strict" if c.entry_fill == "open_t+1" and c.exit_fill == "open_t+1" else "custom"
            ),
            "fees_pct": c.fees_pct,
            "commission_pct": c.commission_pct,
            "stamp_tax_pct": c.stamp_tax_pct,
            "slippage_bps": c.slippage_bps,
            "max_positions": c.max_positions,
            "max_exposure_pct": c.max_exposure_pct,
            "initial_capital": c.initial_capital,
            "position_sizing": c.position_sizing,
            "position_policy": c.position_policy,
            "position_policy_params": c.position_policy_params,
            "asset_type": c.asset_type,
            "mode": c.mode,
            "holding_days": c.holding_days,
            "minute_fill": c.minute_fill,
            "regime_filter": c.regime_filter,
        }

    @staticmethod
    def _capture_factor_snapshot(
        work: pl.DataFrame,
        scoring: dict,
        has_universe: bool,
        factor_snapshot: dict | None,
    ) -> None:
        """在临时因子列被丢弃前截取候选行快照, 与评分共用物化结果。"""
        if factor_snapshot is None:
            return
        snapshot_cols = ["symbol", "date"] + [name for name in scoring if name in work.columns]
        if len(snapshot_cols) <= 2:
            return
        frame = work
        if has_universe:
            frame = frame.filter(pl.col("_score_universe"))
        factor_snapshot["frame"] = frame.select(snapshot_cols)

    @staticmethod
    def _value_in_universe(value: pl.Expr, has_universe: bool) -> pl.Expr:
        if has_universe:
            return pl.when(pl.col("_score_universe")).then(value).otherwise(None)
        return value

    @staticmethod
    def _drop_temporary(df: pl.DataFrame, temporary_scoring_columns: list[str]) -> pl.DataFrame:
        temporary = [
            name for name in ["_score_universe", *temporary_scoring_columns] if name in df.columns
        ]
        return df.drop(temporary) if temporary else df

    @staticmethod
    def _apply_weighted_score(
        work: pl.DataFrame,
        scoring: dict,
        directions: dict,
        has_universe: bool,
        temporary_scoring_columns: list[str],
    ) -> pl.DataFrame | None:
        """按权重归一化各因子列并合成 score; 无可执行因子时返回 None。"""
        if not scoring:
            return None
        executable = [
            (str(col), value, weight)
            for col, weight in scoring.items()
            if weight and (value := scoring_value_expr(work.columns, str(col))) is not None
        ]
        total_weight = sum(weight for _, _, weight in executable)
        if total_weight <= 0:
            return None
        score_parts: list[pl.Expr] = []
        for name, score_value, weight in executable:
            w = weight / total_weight
            value = StrategyBacktestSupportMixin._value_in_universe(score_value, has_universe)
            col_min = value.min().over("date")
            col_max = value.max().over("date")
            col_range = col_max - col_min
            normalized = (
                pl.when(col_range > 0)
                .then((score_value - col_min) / col_range)
                .otherwise(pl.lit(0.5))
            )
            if directions.get(name) == SCORING_DIRECTION_LOW:
                normalized = 1.0 - normalized
            if has_universe:
                normalized = pl.when(pl.col("_score_universe")).then(normalized).otherwise(0.0)
            score_parts.append(normalized * w)
        if not score_parts:
            return None
        score_expr = score_parts[0]
        for part in score_parts[1:]:
            score_expr = score_expr + part
        return StrategyBacktestSupportMixin._drop_temporary(
            work.with_columns((score_expr * 100).fill_null(0).alias("score")),
            temporary_scoring_columns,
        )

    @staticmethod
    def _apply_order_by_score(
        work: pl.DataFrame,
        s: StrategyDef,
        has_universe: bool,
        temporary_scoring_columns: list[str],
    ) -> pl.DataFrame | None:
        """无可执行评分因子时按 order_by 兜底排序。"""
        order_by = s.meta.get("order_by")
        if not (order_by and order_by != "score" and order_by in work.columns):
            return None
        direction = 1 if s.meta.get("descending", True) else -1
        score_expr = pl.col(order_by).fill_null(0) * direction
        if has_universe:
            score_expr = pl.when(pl.col("_score_universe")).then(score_expr).otherwise(0.0)
        return StrategyBacktestSupportMixin._drop_temporary(
            work.with_columns(score_expr.alias("score")), temporary_scoring_columns
        )

    @staticmethod
    def _apply_score(
        panel: pl.DataFrame,
        s: StrategyDef,
        overrides: dict | None,
        universe_mask: pl.Series | None = None,
        factor_snapshot: dict | None = None,
    ) -> pl.DataFrame:
        scoring = effective_scoring(s.meta.get("scoring"), overrides)
        directions = effective_scoring_directions(overrides)

        work = materialize_scoring_columns(panel, scoring.keys())
        temporary_scoring_columns = [
            name for name in scoring if name not in panel.columns and name in work.columns
        ]
        has_universe = universe_mask is not None and len(universe_mask) == len(panel)
        if universe_mask is not None and has_universe:
            work = work.with_columns(universe_mask.rename("_score_universe"))

        # 因子归因快照: 在临时因子列被 _finish 丢弃前, 截取候选行的
        # (symbol, date, 因子值)。与评分共用同一份物化结果, 无第二次计算。
        StrategyBacktestSupportMixin._capture_factor_snapshot(
            work, scoring, has_universe, factor_snapshot
        )
        weighted = StrategyBacktestSupportMixin._apply_weighted_score(
            work, scoring, directions, has_universe, temporary_scoring_columns
        )
        if weighted is not None:
            return weighted
        ordered = StrategyBacktestSupportMixin._apply_order_by_score(
            work, s, has_universe, temporary_scoring_columns
        )
        if ordered is not None:
            return ordered
        return StrategyBacktestSupportMixin._drop_temporary(
            work.with_columns(pl.lit(0.0).alias("score")), temporary_scoring_columns
        )
