"""StrategyEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date
from typing import Any, cast

import polars as pl

from app.config import settings
from app.strategy.scoring import (
    effective_scoring,
    effective_scoring_directions,
    scoring_warmup_bars,
)

from ._common import (
    StrategyDataContext,
    StrategyDef,
    StrategyResult,
    _RealtimeMatrixEntry,
    _sanitize,
    logger,
)


class _StrategyEngineMixin2:
    # 以下成员由组装类 StrategyEngine 的其它 mixin / __init__ 提供，
    # 在本类中仅作类型声明以满足 mypy 的分片检查（运行时无赋值副作用）。
    get: Any
    validate_context: Any
    resolve_params: Any
    _matrix_history_bars: Any
    _matrix_field_columns: Any
    _realtime_matrix_lock: Any
    _realtime_matrices: Any
    _run_matrix_strategy: Any
    _run_composite_strategy: Any
    _inject_intraday_signal_columns: Any
    _join_basic_columns: Any
    _apply_basic_filter: Any
    _apply_scoring: Any
    _result_limit: Any
    _materialize_scoring_frames: Any
    _strategies: Any
    build_shared_matrix: Any

    def _collect_realtime_matrix_spec(
        self,
        context: StrategyDataContext,
        strategy_ids: list[str],
        params_map: dict[str, dict],
        overrides_map: dict[str, dict],
    ) -> tuple[list[str], set[str], int]:
        field_columns: set[str] = set()
        max_warmup = 1
        matrix_ids: list[str] = []
        for strategy_id in strategy_ids:
            strategy = self.get(strategy_id)
            self.validate_context(strategy, context)
            if strategy.execution_backend != "matrix_native":
                continue
            params = self.resolve_params(
                strategy,
                params_map.get(strategy_id),
                overrides_map.get(strategy_id),
            )
            matrix_ids.append(strategy_id)
            max_warmup = max(
                max_warmup,
                self._matrix_history_bars(strategy, params),
                scoring_warmup_bars(
                    effective_scoring(
                        strategy.meta.get("scoring"),
                        overrides_map.get(strategy_id),
                    )
                ),
            )
            field_columns.update(
                self._matrix_field_columns(
                    strategy,
                    overrides_map.get(strategy_id),
                    params,
                )
            )
        return matrix_ids, field_columns, max_warmup

    def prepare_realtime_matrix(
        self,
        context: StrategyDataContext,
        strategy_ids: list[str],
        *,
        params_map: dict[str, dict] | None = None,
        overrides_map: dict[str, dict] | None = None,
    ):
        """Build once, then update only the latest live bar for matrix strategies."""
        from app.backtest.matrix import RealtimeMarketDataMatrix

        current = context.current
        if current is None:
            raise ValueError("realtime matrix context requires current data")
        if current.is_empty() or not strategy_ids:
            return None
        params_map = params_map or {}
        overrides_map = overrides_map or {}
        matrix_ids, field_columns, max_warmup = self._collect_realtime_matrix_spec(
            context,
            strategy_ids,
            params_map,
            overrides_map,
        )
        if not matrix_ids:
            return None

        timestamp_col = "datetime" if "datetime" in current.columns else "date"
        if timestamp_col not in current.columns:
            raise ValueError("realtime matrix current data requires date or datetime")
        latest_value = current[timestamp_col].max()
        as_of = latest_value.date() if hasattr(latest_value, "date") else latest_value
        if not isinstance(as_of, date):
            raise ValueError("realtime matrix timestamp cannot be converted to date")
        symbols = tuple(current["symbol"].cast(pl.Utf8).unique().sort().to_list())
        fingerprint = (
            context.asset_type,
            context.timeframe,
            context.data_generation,
            tuple(sorted(field_columns)),
            max_warmup,
            symbols,
        )

        with self._realtime_matrix_lock:
            cache_key = context.cache_key or f"{context.asset_type}:{context.timeframe}"
            entry = self._realtime_matrices.get(cache_key)
            if entry is not None and entry.fingerprint == fingerprint:
                try:
                    entry.buffer.update(current)
                    return entry.buffer.snapshot()
                except ValueError as exc:
                    logger.info("realtime matrix %s invalidated: %s", cache_key, exc)

            history = context.history
            if history is None:
                raise ValueError("matrix strategy realtime context requires history data")
            if history is None or history.is_empty():
                raise ValueError("matrix strategy realtime history is empty")
            if timestamp_col in history.columns:
                history = history.filter(pl.col(timestamp_col) != latest_value)
            elif "date" in history.columns:
                history = history.filter(pl.col("date") != as_of)
            panel = pl.concat([history, current], how="diagonal_relaxed")
            previous_builds = entry.buffer.build_count if entry is not None else 0
            buffer = RealtimeMarketDataMatrix(
                panel,
                field_columns=field_columns,
                build_count=previous_builds + 1,
            )
            self._realtime_matrices[cache_key] = _RealtimeMatrixEntry(
                fingerprint=fingerprint,
                buffer=buffer,
            )
            return buffer.snapshot()

    def realtime_matrix_stats(self, cache_key: str) -> dict[str, int]:
        with self._realtime_matrix_lock:
            entry = self._realtime_matrices.get(cache_key)
            if entry is None:
                return {"generation": 0, "build_count": 0, "update_count": 0}
            return {
                "generation": int(entry.buffer.generation),
                "build_count": int(entry.buffer.build_count),
                "update_count": int(entry.buffer.update_count),
            }

    # ================================================================
    # 执行
    # ================================================================

    def _run_special_backend(
        self,
        strategy_id: str,
        s: StrategyDef,
        as_of: date,
        pool: list[str] | None,
        params: dict,
        overrides: dict,
        context: StrategyDataContext,
        started_at: float,
    ) -> StrategyResult | None:
        """matrix_native/composite 后端在主链之前短路返回，其余返回 None。"""
        if s.execution_backend == "matrix_native":
            return self._run_matrix_strategy(
                strategy_id,
                s,
                as_of,
                pool=pool,
                params=params,
                overrides=overrides,
                context=context,
                started_at=started_at,
            )
        if s.execution_backend == "composite":
            return self._run_composite_strategy(
                strategy_id,
                s,
                context,
                pool=pool,
                params=params,
                overrides=overrides,
                started_at=started_at,
            )
        return None

    def _collect_exit_hits(
        self,
        current: pl.DataFrame | None,
        history: pl.DataFrame | None,
        as_of: date,
        pool: list[str] | None,
        exit_signals: list[str],
    ) -> list[dict]:
        """在过滤前的目标日期帧上收集卖出信号命中。"""
        signal_df = current if current is not None else history
        if signal_df is None:
            signal_df = pl.DataFrame()
        if not signal_df.is_empty() and "date" in signal_df.columns:
            signal_df = signal_df.filter(pl.col("date") == as_of)
        if pool and not signal_df.is_empty():
            signal_df = signal_df.filter(pl.col("symbol").is_in(pool))
        return self._collect_signal_hits(signal_df, exit_signals)

    def _run_minute_filter_frame(
        self,
        strategy_id: str,
        s: StrategyDef,
        params: dict,
        context: StrategyDataContext,
        current: pl.DataFrame | None,
        history: pl.DataFrame | None,
        as_of: date,
        exit_signal_hits: list[dict],
    ) -> tuple[pl.DataFrame | None, StrategyResult | None]:
        """分钟策略: 在当日分钟K窗口上过滤，返回 (df, 提前返回的结果)。"""
        # 分钟策略: 读取调用方注入的当日分钟K窗口。无 date 列, 不按 as_of 过滤,
        # 每个命中行自带最后K线时间戳 (last_datetime)。
        if history is None:
            raise ValueError(f"strategy {strategy_id} requires minute history data")
        if history.is_empty():
            return None, StrategyResult(
                as_of=as_of,
                strategy_id=strategy_id,
                exit_signal_hits=exit_signal_hits,
            )
        # 盘中信号列注入(csgi_): 实盘扫描与分钟回测共用本路径 — 与监控评估
        # 同一特征构造器, 单点注入保证三处口径一致。
        history = self._inject_intraday_signal_columns(history)
        missing_csgi = [
            name
            for name in s.required_features
            if name.startswith("csgi_") and name not in history.columns
        ]
        if missing_csgi:
            raise ValueError(
                "策略引用了未定义的盘中信号: "
                + ", ".join(sorted(missing_csgi))
                + " — 请先在「自定义信号」中创建(timeframe=intraday)后再运行"
            )
        if s.minute_daily_bars > 0:
            df = cast(Any, s.filter_minute_history_fn)(history, params, daily=context.daily_history)
        else:
            df = cast(Any, s.filter_minute_history_fn)(history, params)
        # 基础过滤/展示列 (name/total_shares/change_pct 等) 来自 enriched 快照,
        # 在命中结果上事后联表, 避免把 enriched 列铺到全市场分钟行上。
        if current is not None and not current.is_empty():
            df = self._join_basic_columns(df, current)
        return df, None

    def _run_history_frame(
        self,
        strategy_id: str,
        s: StrategyDef,
        params: dict,
        history: pl.DataFrame | None,
        as_of: date,
        exit_signal_hits: list[dict],
    ) -> tuple[pl.DataFrame | None, StrategyResult | None]:
        """历史策略: 在调用方注入的历史窗口上过滤，返回 (df, 提前返回的结果)。"""
        if history is None:
            raise ValueError(f"strategy {strategy_id} requires history data")
        df = history
        if df.is_empty():
            return None, StrategyResult(
                as_of=as_of,
                strategy_id=strategy_id,
                exit_signal_hits=exit_signal_hits,
            )
        # 自定义信号前置校验: REQUIRED_FEATURES 引用的 csg_ 列未注入时,
        # 给出明确指引, 而不是让策略代码抛 polars 缺列错 (500)。
        # 盘中单日路径不在此校验 (该路径对带偏移信号本就优雅降级)。
        missing_csg = [
            name
            for name in s.required_features
            if name.startswith("csg_") and name not in df.columns
        ]
        if missing_csg:
            raise ValueError(
                "策略引用了未定义的自定义信号: "
                + ", ".join(sorted(missing_csg))
                + " — 请先在「自定义信号」管理中创建对应信号后再运行"
            )
        missing_csgi = [name for name in s.required_features if name.startswith("csgi_")]
        if missing_csgi:
            raise ValueError(
                "盘中信号仅可用于分钟策略(timeframes=['1m']), 日线策略不支持: "
                + ", ".join(sorted(missing_csgi))
            )
        df = cast(Any, s.filter_history_fn)(df, params)
        if "date" in df.columns:
            df = df.filter(pl.col("date") == as_of)
        return df, None

    def _select_strategy_frame(
        self,
        strategy_id: str,
        s: StrategyDef,
        params: dict,
        context: StrategyDataContext,
        current: pl.DataFrame | None,
        history: pl.DataFrame | None,
        as_of: date,
        exit_signal_hits: list[dict],
    ) -> tuple[pl.DataFrame | None, StrategyResult | None]:
        """按后端选择候选帧: 普通策略只读目标日期，历史策略读取历史窗口。"""
        if s.execution_backend == "minute_filter":
            return self._run_minute_filter_frame(
                strategy_id, s, params, context, current, history, as_of, exit_signal_hits
            )
        if s.filter_history_fn:
            return self._run_history_frame(strategy_id, s, params, history, as_of, exit_signal_hits)
        if current is None:
            raise ValueError(f"strategy {strategy_id} requires current data")
        return current, None

    def _filter_candidates(
        self,
        df: pl.DataFrame,
        s: StrategyDef,
        params: dict,
        overrides: dict,
        pool: list[str] | None,
    ) -> pl.DataFrame:
        """Stage 1 基础过滤 + Pool 过滤 + Stage 2 策略过滤。"""
        # 基础过滤: 策略默认 basic_filter 兜底, 用户 override 优先覆盖。
        # 这样策略文件里写的 exclude_st/price_min 等默认值即使前端没保存也能生效。
        bf = dict(s.basic_filter) if s.basic_filter else {}
        if overrides and overrides.get("basic_filter"):
            bf.update(overrides["basic_filter"])
        # Stage 1: 基础过滤（enabled 默认开启; 显式 enabled=false 才跳过）
        if bf and bf.get("enabled", True):
            df = self._apply_basic_filter(df, bf)
        # Pool 过滤
        if pool:
            df = df.filter(pl.col("symbol").is_in(pool))
        # Stage 2: 策略过滤
        if s.filter_fn:
            expr = s.filter_fn(df, params)
            df = df.filter(expr)
        return df

    def _apply_overlay_filters(
        self,
        df: pl.DataFrame,
        overlay_conditions: list[dict] | None,
        history: pl.DataFrame | None,
        as_of: date,
    ) -> pl.DataFrame:
        """Stage 2.5: 叠加条件硬过滤；含「前 N 日」偏移时在完整历史窗口上求值。"""
        if not overlay_conditions:
            return df
        from app.strategy import custom_signals

        try:
            overlay_expr = custom_signals.build_overlay_expr(overlay_conditions)
        except ValueError as e:
            raise ValueError(f"叠加条件编译失败: {e}") from e
        if overlay_expr is None:
            return df
        overlay_days = custom_signals.overlay_max_days(overlay_conditions)
        if overlay_days > 0:
            # 「前 N 日」偏移需要更早的行来 shift: 候选帧只是当日切片, 改在
            # 完整历史窗口上求值 as_of 行掩码, 再按 symbol 交集回候选行。
            if history is None or history.is_empty():
                raise ValueError(
                    "叠加条件含「前 N 日」偏移, 需要历史数据窗口; 当前调用未提供 history"
                )
            src = history
        else:
            src = df
        # engine.run 不保证任意注册表因子列在帧上 → 先按条件依赖物化
        src = custom_signals.materialize_factor_columns(src, {"__overlay__": overlay_expr})
        missing = custom_signals.overlay_missing_columns(overlay_conditions, src.columns)
        if missing:
            raise ValueError(
                "叠加条件引用的列不存在: "
                + ", ".join(missing)
                + " — 请检查扩展数据拉取/因子计算是否可用"
            )
        if overlay_days > 0:
            # 掩码必须在完整帧上算 (先 filter 到 as_of 会截断 shift 所需的历史行),
            # 再取 as_of 行读掩码 — 与 matrix 路径同一顺序。
            masked = src.with_columns(overlay_expr.fill_null(False).alias("__overlay__"))
            allowed = (
                masked.filter(pl.col("date") == as_of)
                .filter(pl.col("__overlay__"))["symbol"]
                .cast(pl.Utf8)
                .unique()
                .to_list()
            )
            return df.filter(pl.col("symbol").cast(pl.Utf8).is_in(allowed))
        return df.filter(overlay_expr.fill_null(False))

    def _finalize_run_result(
        self,
        df: pl.DataFrame,
        scoring: dict,
        scoring_directions: dict,
        s: StrategyDef,
        strategy_id: str,
        as_of: date,
        t0: float,
        overrides: dict,
        entry_signals: list[str],
        exit_signal_hits: list[dict],
    ) -> StrategyResult:
        """Stage 3 评分、信号命中、排序截断，构造最终返回结果。"""
        df = self._apply_scoring(df, scoring, scoring_directions)
        entry_signal_hits = self._collect_signal_hits(df, entry_signals)
        if not entry_signals and (
            s.filter_history_fn or s.filter_fn or s.execution_backend == "minute_filter"
        ):
            entry_signal_hits = [
                {"symbol": str(symbol), "signals": []}
                for symbol in df["symbol"].cast(pl.Utf8).unique().to_list()
            ]

        # 排序 + 限制
        limit = self._result_limit(s, overrides)
        order_desc = s.meta.get("descending", True)
        if "score" in df.columns:
            df = df.sort("score", descending=order_desc)
        elif s.meta.get("order_by") and s.meta["order_by"] != "score":
            ob = s.meta["order_by"]
            if ob in df.columns:
                df = df.sort(ob, descending=order_desc)
        if limit is not None:
            df = df.head(limit)

        # 输出
        rows = _sanitize(df.to_dicts())
        elapsed = (time.perf_counter() - t0) * 1000

        scores: dict[str, float] = {}
        if "score" in df.columns:
            for r in df.iter_rows(named=True):
                scores[r["symbol"]] = float(r.get("score") or 0)

        return StrategyResult(
            as_of=as_of,
            strategy_id=strategy_id,
            rows=rows,
            total=len(rows),
            elapsed_ms=elapsed,
            scores=scores,
            entry_signal_hits=entry_signal_hits,
            exit_signal_hits=exit_signal_hits,
        )

    def run(
        self,
        strategy_id: str,
        context: StrategyDataContext,
        pool: list[str] | None = None,
        params: dict | None = None,
        overrides: dict | None = None,
    ) -> StrategyResult:
        """执行策略: 基础过滤 → 策略过滤 → 评分排序

        Args:
            strategy_id:        策略 ID
            context:            调用级行情、资产和周期上下文
            pool:               限定股票池
            params:             本次执行显式传入的策略参数
            overrides:          用户覆盖配置 (params/basic_filter/scoring/stop_loss 等)
        """
        t0 = time.perf_counter()

        s = self.get(strategy_id)
        self.validate_context(s, context)
        as_of = context.as_of
        overrides = overrides or {}
        params = self.resolve_params(s, params, overrides)
        entry_signals = self._effective_signals(overrides, "entry_signals", s.entry_signals)
        exit_signals = self._effective_signals(overrides, "exit_signals", s.exit_signals)
        overlay_conditions = self._effective_overlay(overrides)
        # 叠加条件运行期守卫 (保存期已拦, 手改 override 文件时的双保险)
        if overlay_conditions and s.execution_backend in ("composite", "minute_filter"):
            raise ValueError(f"strategy {strategy_id}: 叠加条件不支持 {s.execution_backend} 策略")

        early = self._run_special_backend(
            strategy_id, s, as_of, pool, params, overrides, context, t0
        )
        if early is not None:
            return early

        scoring = effective_scoring(s.meta.get("scoring"), overrides)
        scoring_directions = effective_scoring_directions(overrides)
        score_frames = (context.current, context.history, scoring)
        current, history = self._materialize_scoring_frames(*score_frames)

        exit_signal_hits = self._collect_exit_hits(current, history, as_of, pool, exit_signals)

        # 普通策略只读目标日期；历史策略读取调用方注入的历史窗口。
        df, early = self._select_strategy_frame(
            strategy_id, s, params, context, current, history, as_of, exit_signal_hits
        )
        if early is not None:
            return early
        assert df is not None

        if df.is_empty():
            return StrategyResult(
                as_of=as_of,
                strategy_id=strategy_id,
                exit_signal_hits=exit_signal_hits,
            )

        df = self._filter_candidates(df, s, params, overrides, pool)

        # Stage 2.5: 叠加条件 (每策略 overlay 硬过滤) — 策略过滤之后、评分之前:
        # 评分 min-max 归一化与 limit 截断都在过滤后的候选上进行; exit_signal_hits
        # 已在 :942 过滤前的帧上收集, 已持仓的卖出信号不受影响。
        df = self._apply_overlay_filters(df, overlay_conditions, history, as_of)

        return self._finalize_run_result(
            df,
            scoring,
            scoring_directions,
            s,
            strategy_id,
            as_of,
            t0,
            overrides,
            entry_signals,
            exit_signal_hits,
        )

    @staticmethod
    def _effective_signals(overrides: dict, key: str, default: list[str]) -> list[str]:
        value = overrides.get(key)
        if isinstance(value, list):
            return [str(signal) for signal in value if signal]
        return list(default or [])

    @staticmethod
    def _effective_overlay(overrides: dict) -> list[dict] | None:
        """叠加条件 override; 归一为非空条件列表或 None (None/[] = 未配置)。"""
        value = (overrides or {}).get("overlay_filter")
        if isinstance(value, list) and value:
            return value
        return None

    @staticmethod
    def _collect_signal_hits(df: pl.DataFrame, signals: list[str]) -> list[dict]:
        if df.is_empty() or not signals or "symbol" not in df.columns:
            return []
        resolved = [
            signal if signal.startswith(("signal_", "csg_")) else f"signal_{signal}"
            for signal in signals
        ]
        available = [
            (signal, column)
            for signal, column in zip(signals, resolved, strict=True)
            if column in df.columns
        ]
        if not available:
            return []
        hit_df = df.filter(
            pl.any_horizontal(pl.col(column).fill_null(False) for _, column in available)
        )
        return [
            {
                "symbol": str(row["symbol"]),
                "signals": [signal for signal, column in available if row.get(column)],
            }
            for row in hit_df.iter_rows(named=True)
        ]

    def run_all(
        self,
        context: StrategyDataContext,
        params_map: dict | None = None,
        overrides_map: dict | None = None,
        *,
        strategy_ids: list[str] | None = None,
        parallel: bool = True,
    ) -> dict[str, StrategyResult]:
        """批量执行策略；当前数据、历史和矩阵均来自同一个调用上下文。

        parallel=True 时用有界线程池并发执行: 策略对 context 是只读纯函数
        (polars 计算释放 GIL), 并发不改变结果, 逐策略耗时日志不变。composite
        子策略的递归 run_all 以 parallel=False 调用, 保证嵌套时线程总数仍
        不超过 worker 上限, 不随叠加层数放大。
        """
        if context.current is None:
            raise ValueError("strategy run_all context requires current data")
        df = context.current
        params_map = params_map or {}
        overrides_map = overrides_map or {}
        selected_ids = list(self._strategies) if strategy_ids is None else strategy_ids
        selected = [(sid, self.get(sid)) for sid in selected_ids]
        for _, strategy in selected:
            self.validate_context(strategy, context)

        history_strats = [
            (sid, strategy)
            for sid, strategy in selected
            if strategy.filter_history_fn
            or strategy.execution_backend in ("matrix_native", "minute_filter")
        ]
        shared_history = context.history
        if history_strats and shared_history is None:
            raise ValueError("selected strategies require history data")

        shared_matrix = context.market
        if shared_matrix is None:
            shared_matrix = self.build_shared_matrix(context, selected, params_map, overrides_map)

        results: dict[str, StrategyResult] = {}

        def _execute(sid: str) -> tuple[str, StrategyResult]:
            started = time.perf_counter()
            result = self.run(
                sid,
                replace(
                    context,
                    current=df,
                    history=shared_history,
                    market=shared_matrix,
                ),
                params=params_map.get(sid),
                overrides=overrides_map.get(sid),
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            # >=1s 打 INFO 供热点归因 (哪些策略吃掉了 run_all 的大头), 其余 DEBUG 防噪。
            log_fn = logger.info if elapsed_ms >= 1000 else logger.debug
            log_fn(
                "run_all: strategy %s took %.0fms (total=%d)",
                sid,
                elapsed_ms,
                result.total,
            )
            return sid, result

        workers = min(settings.strategy_run_all_workers, len(selected))
        if parallel and workers > 1:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="strategy-run") as pool:
                futures = [pool.submit(_execute, sid) for sid, _ in selected]
                # 按原顺序收集: 首个失败策略的异常语义与串行执行一致。
                for future in futures:
                    sid, result = future.result()
                    results[sid] = result
        else:
            for sid, _ in selected:
                sid, result = _execute(sid)
                results[sid] = result

        return results
