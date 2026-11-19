"""StrategyEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl

import app.strategy.engine as _engine_pkg
from app.strategy.scoring import (
    effective_scoring,
    effective_scoring_directions,
    scoring_dependencies,
)

from ._common import (
    StrategyDataContext,
    StrategyDef,
    StrategyResult,
    _parse_composite_children,
    _sanitize,
    logger,
)


def _basic_numeric_filters(df: pl.DataFrame, bf: dict) -> list[pl.Expr]:
    """构建价格/市值/成交额/换手率/ST 基础过滤表达式。"""
    exprs: list[pl.Expr] = []
    if bf.get("price_min") is not None:
        exprs.append(pl.col("close") >= bf["price_min"])
    if bf.get("price_max") is not None:
        exprs.append(pl.col("close") <= bf["price_max"])
    if bf.get("market_cap_min") is not None and "total_shares" in df.columns:
        exprs.append(pl.col("close") * pl.col("total_shares") >= bf["market_cap_min"])
    if bf.get("market_cap_max") is not None and "total_shares" in df.columns:
        exprs.append(pl.col("close") * pl.col("total_shares") <= bf["market_cap_max"])
    # 流通市值
    if bf.get("float_cap_min") is not None and "float_shares" in df.columns:
        exprs.append(pl.col("close") * pl.col("float_shares") >= bf["float_cap_min"])
    if bf.get("float_cap_max") is not None and "float_shares" in df.columns:
        exprs.append(pl.col("close") * pl.col("float_shares") <= bf["float_cap_max"])
    if bf.get("amount_min") is not None:
        exprs.append(pl.col("amount") >= bf["amount_min"])
    if bf.get("amount_max") is not None:
        exprs.append(pl.col("amount") <= bf["amount_max"])
    # 换手率
    if bf.get("turnover_min") is not None and "turnover_rate" in df.columns:
        exprs.append(pl.col("turnover_rate") >= bf["turnover_min"])
    if bf.get("turnover_max") is not None and "turnover_rate" in df.columns:
        exprs.append(pl.col("turnover_rate") <= bf["turnover_max"])
    if bf.get("exclude_st") and "name" in df.columns:
        exprs.append(~pl.col("name").str.contains("(?i)ST|\\*ST|退"))
    return exprs


def _board_filter_expr(boards) -> pl.Expr | None:
    """构建板块过滤表达式；无有效板块返回 None。"""
    if not boards or not isinstance(boards, list):
        return None
    board_exprs: list[pl.Expr] = []
    for b in boards:
        if b == "沪主板":
            board_exprs.append(pl.col("symbol").str.starts_with("60"))
        elif b == "深主板":
            board_exprs.append(
                pl.col("symbol").str.starts_with("00") | pl.col("symbol").str.starts_with("001")
            )
        elif b == "创业板":
            board_exprs.append(
                pl.col("symbol").str.starts_with("300")
                | pl.col("symbol").str.starts_with("301")
                | pl.col("symbol").str.starts_with("302")
            )
        elif b == "科创板":
            board_exprs.append(
                pl.col("symbol").str.starts_with("688") | pl.col("symbol").str.starts_with("689")
            )
        elif b == "北交所":
            board_exprs.append(pl.col("symbol").str.contains(r"\.BJ$"))
    return pl.any_horizontal(board_exprs) if board_exprs else None


class _StrategyEngineMixin3:
    # 以下成员由组装类 StrategyEngine 的其它 mixin / __init__ 提供，
    # 在本类中仅作类型声明以满足 mypy 的分片检查（运行时无赋值副作用）。
    _realtime_matrix_lock: Any
    _shared_matrix_cache: Any
    _effective_overlay: Any
    _result_limit: Any
    _override_loader: Any
    run_all: Any
    _strategy_dirs: Any

    def build_shared_matrix(
        self,
        context: StrategyDataContext,
        selected: list[tuple[str, StrategyDef]],
        params_map: dict | None = None,
        overrides_map: dict | None = None,
    ):
        """按所选策略的字段并集构建市场数据矩阵; 无矩阵策略或无历史时返回 None。

        渐进式 run_all (逐策略执行) 也用它一次建好并集矩阵后放入 context.market,
        避免每个 matrix_native 策略重复构建同一份大矩阵 (全市场历史, 秒级)。
        """
        params_map = params_map or {}
        overrides_map = overrides_map or {}
        matrix_strats = [
            (sid, strategy)
            for sid, strategy in selected
            if strategy.execution_backend == "matrix_native"
        ]
        history = context.history
        if not matrix_strats or history is None or history.is_empty():
            return None

        from app.backtest.matrix import build_market_data_matrix

        field_columns: set[str] = set()
        for sid, strategy in matrix_strats:
            field_columns.update(
                self._matrix_field_columns(
                    strategy,
                    overrides_map.get(sid),
                    params_map.get(sid),
                )
            )

        # 纯函数缓存: 矩阵只由 (history 窗口, 字段并集) 决定且数组只读, 同指纹
        # 直接复用。指纹含 enriched generation (数据换代自动失效) + 窗口范围
        # (不同 lookback 的 history 不可混用)。generation 未知 (分钟/ETF/测试
        # Fake 数据) 时 fail-closed 不缓存, 保持每次重建的旧行为。
        generation = getattr(context, "data_generation", None)
        fingerprint: tuple | None = None
        if generation and "date" in history.columns:
            window = (
                str(history["date"].min()),
                str(history["date"].max()),
                history.height,
            )
            fingerprint = (
                context.asset_type,
                str(context.as_of),
                tuple(sorted(field_columns)),
                window,
                generation,
            )
            with self._realtime_matrix_lock:
                cached_matrix = self._shared_matrix_cache.get(fingerprint)
            if cached_matrix is not None:
                logger.info("run_all: shared matrix cache hit (fields=%d)", len(field_columns))
                return cached_matrix

        matrix_t0 = time.perf_counter()
        matrix = build_market_data_matrix(history, field_columns=field_columns)
        logger.info(
            "run_all: shared matrix built in %.0fms (fields=%d)",
            (time.perf_counter() - matrix_t0) * 1000,
            len(field_columns),
        )
        if fingerprint is not None:
            with self._realtime_matrix_lock:
                # 只保留最近一份: 字段并集随策略池变化, 旧矩阵价值低且占内存
                self._shared_matrix_cache.clear()
                self._shared_matrix_cache[fingerprint] = matrix
        return matrix

    @staticmethod
    def _matrix_history_bars(strategy: StrategyDef, params: dict) -> int:
        from app.factors.registry import factor_warmup_bars

        required = int(cast(Any, strategy.matrix_strategy).required_warmup_bars(params)) + 1
        fields = getattr(strategy.matrix_strategy, "required_fields_for_params", None)
        return max(required, factor_warmup_bars(fields(params))) if callable(fields) else required

    @staticmethod
    def _matrix_field_columns(
        strategy: StrategyDef,
        overrides: dict | None = None,
        params: dict | None = None,
    ) -> set[str]:
        fields = set(cast(Any, strategy.matrix_strategy).required_fields())
        # 参数评分字段 (如挖掘策略的因子组合) 需展开为实际数据依赖,
        # 与 backtest._resolve_matrix_native 保持同一语义, 否则虚拟因子
        # (limit_up_count_* -> consecutive_limit_ups) 在矩阵里缺字段。
        parameter_fields = getattr(
            strategy.matrix_strategy,
            "required_fields_for_params",
            None,
        )
        if callable(parameter_fields):
            fields.update(
                scoring_dependencies({str(name): 1.0 for name in parameter_fields(params or {})})
            )
        basic_filter = dict(strategy.basic_filter or {})
        if (overrides or {}).get("basic_filter"):
            basic_filter.update((overrides or {})["basic_filter"])
        for prefix, field_name in (
            ("market_cap", "total_shares"),
            ("float_cap", "float_shares"),
            ("amount", "amount"),
            ("turnover", "turnover_rate"),
        ):
            if (
                basic_filter.get(f"{prefix}_min") is not None
                or basic_filter.get(f"{prefix}_max") is not None
            ):
                fields.add(field_name)
        scoring = effective_scoring(strategy.meta.get("scoring"), overrides)
        fields.update(scoring_dependencies(scoring))
        order_by = strategy.meta.get("order_by")
        if order_by and order_by != "score":
            fields.add(str(order_by))
        return fields

    def _prepare_matrix_panels(
        self,
        strategy_id: str,
        strategy: StrategyDef,
        context: StrategyDataContext,
        as_of: date,
        overrides: dict,
        params: dict,
    ) -> tuple[pl.DataFrame | None, Any | None, StrategyResult | None]:
        """准备矩阵 panel 与 market，返回 (source_panel, market, 提前返回的结果)。"""
        from app.backtest.matrix import build_market_data_matrix

        source_panel = context.history
        market = context.market
        if market is None:
            if source_panel is None:
                raise ValueError(f"matrix strategy {strategy_id} requires history data")
            if source_panel is None or source_panel.is_empty():
                return source_panel, market, StrategyResult(as_of=as_of, strategy_id=strategy_id)
            market = build_market_data_matrix(
                source_panel,
                field_columns=self._matrix_field_columns(strategy, overrides, params),
            )

        if source_panel is None or source_panel.is_empty():
            source_panel = context.current
        if source_panel is None or source_panel.is_empty():
            return source_panel, market, StrategyResult(as_of=as_of, strategy_id=strategy_id)
        return source_panel, market, None

    def _compute_matrix_signals(
        self,
        strategy: StrategyDef,
        context: StrategyDataContext,
        overrides: dict,
        params: dict,
        pool: list[str] | None,
        market: Any,
    ) -> tuple[Any, np.ndarray | None]:
        """构建过滤/评分配置并运行矩阵流水线，返回 (signals, asset_mask)。"""
        from app.backtest.matrix import MatrixPipelineConfig, MatrixStrategyPipeline

        basic_filter = dict(strategy.basic_filter or {})
        if overrides.get("basic_filter"):
            basic_filter.update(overrides["basic_filter"])
        # 策略扫描的运行期过滤同样要按资产类型中和股票专属键 (boards/价格界),
        # 否则 ETF 候选在矩阵掩码阶段被静默清零 (#215); 函数级导入避免
        # engine ↔ backtest.strategy 的模块级循环依赖 (与上方 matrix 导入同模式)
        from app.backtest.strategy import _basic_filter_for_asset

        basic_filter = _basic_filter_for_asset(basic_filter, context.asset_type)
        scoring = effective_scoring(strategy.meta.get("scoring"), overrides)
        asset_mask = None
        if pool:
            pool_set = set(pool)
            asset_mask = np.fromiter(
                (symbol in pool_set for symbol in market.symbols),
                dtype=bool,
                count=len(market.symbols),
            )

        signals = MatrixStrategyPipeline().run(
            cast(Any, strategy.matrix_strategy),
            market,
            params,
            MatrixPipelineConfig(
                basic_filter=basic_filter,
                scoring=scoring,
                scoring_directions=effective_scoring_directions(overrides),
                order_by=strategy.meta.get("order_by"),
                descending=bool(strategy.meta.get("descending", True)),
                asset_mask=asset_mask,
            ),
        )
        return signals, asset_mask

    def _apply_matrix_overlay(
        self,
        overrides: dict,
        target_frame: pl.DataFrame,
        source_panel: pl.DataFrame | None,
        market: Any,
        entry_active: np.ndarray,
        as_of: date,
    ) -> np.ndarray:
        """叠加条件只掩码 entry；含「前 N 日」偏移时在完整 panel 上求值。"""
        overlay_conditions = self._effective_overlay(overrides)
        if not overlay_conditions:
            return entry_active
        from app.strategy import custom_signals

        try:
            overlay_expr = custom_signals.build_overlay_expr(overlay_conditions)
        except ValueError as e:
            raise ValueError(f"叠加条件编译失败: {e}") from e
        if overlay_expr is None:
            return entry_active
        overlay_days = custom_signals.overlay_max_days(overlay_conditions)
        # 「前 N 日」偏移需要更早的行来 shift: 在完整 source_panel 上求值,
        # 再取 as_of 行读掩码; 无偏移时维持现状 (只评估当日切片)。
        eval_frame = target_frame
        if overlay_days > 0:
            if source_panel is None or source_panel.is_empty():
                raise ValueError("叠加条件含「前 N 日」偏移, 需要历史数据窗口")
            eval_frame = source_panel
        eval_frame = custom_signals.materialize_factor_columns(
            eval_frame, {"__overlay__": overlay_expr}
        )
        missing = custom_signals.overlay_missing_columns(overlay_conditions, eval_frame.columns)
        if missing:
            raise ValueError(
                "叠加条件引用的列不存在: "
                + ", ".join(missing)
                + " — 请检查扩展数据拉取/因子计算是否可用"
            )
        if overlay_days > 0:
            masked = eval_frame.with_columns(overlay_expr.fill_null(False).alias("__overlay__"))
            keep_frame = self._matrix_target_frame(masked, as_of)
            keep = set(keep_frame.filter(pl.col("__overlay__"))["symbol"].cast(pl.Utf8).to_list())
        else:
            keep = set(
                eval_frame.filter(overlay_expr.fill_null(False))["symbol"].cast(pl.Utf8).to_list()
            )
        allowed = np.fromiter(
            (str(symbol) in keep for symbol in market.symbols),
            dtype=bool,
            count=len(market.symbols),
        )
        return np.where(allowed, entry_active, 0)

    def _rank_matrix_result(
        self,
        strategy_id: str,
        strategy: StrategyDef,
        overrides: dict,
        target_frame: pl.DataFrame,
        signals: Any,
        target_time: int,
        market: Any,
        selected_assets: np.ndarray,
        as_of: date,
        started_at: float,
        entry_signal_hits: list[dict],
        exit_signal_hits: list[dict],
    ) -> StrategyResult:
        """按 score 排序截断命中行并构造矩阵策略结果。"""
        row_by_symbol = {str(row["symbol"]): row for row in target_frame.iter_rows(named=True)}
        ranked: list[tuple[float, dict]] = []
        for asset_id in selected_assets:
            symbol = market.symbols[int(asset_id)]
            row = row_by_symbol.get(symbol)
            if row is None:
                continue
            score = float(signals.score[target_time, int(asset_id)])
            ranked.append((score, {**row, "score": score}))
        ranked.sort(
            key=lambda item: item[0],
            reverse=bool(strategy.meta.get("descending", True)),
        )
        limit = self._result_limit(strategy, overrides)
        selected_rows = ranked if limit is None else ranked[:limit]
        rows = _sanitize([row for _, row in selected_rows])
        scores = {str(row["symbol"]): float(row.get("score") or 0.0) for row in rows}
        return StrategyResult(
            as_of=as_of,
            strategy_id=strategy_id,
            rows=rows,
            total=len(rows),
            elapsed_ms=(time.perf_counter() - started_at) * 1000,
            scores=scores,
            entry_signal_hits=entry_signal_hits,
            exit_signal_hits=exit_signal_hits,
        )

    def _run_matrix_strategy(
        self,
        strategy_id: str,
        strategy: StrategyDef,
        as_of: date,
        *,
        pool: list[str] | None,
        params: dict,
        overrides: dict,
        context: StrategyDataContext,
        started_at: float,
    ) -> StrategyResult:
        source_panel, market, early = self._prepare_matrix_panels(
            strategy_id, strategy, context, as_of, overrides, params
        )
        if early is not None:
            return early
        signals, asset_mask = self._compute_matrix_signals(
            strategy, context, overrides, params, pool, market
        )
        target_ids = [
            time_id
            for time_id, label in enumerate(cast(Any, market).timestamp_labels)
            if label[:10] == str(as_of)
        ]
        if not target_ids:
            return StrategyResult(as_of=as_of, strategy_id=strategy_id)
        target_time = target_ids[-1]
        target_frame = self._matrix_target_frame(source_panel, as_of)
        entry_active = signals.entry[target_time]
        exit_active = signals.exit[target_time]
        if asset_mask is not None:
            entry_active = entry_active & asset_mask
            exit_active = exit_active & asset_mask
        # 叠加条件 (每策略 overlay 硬过滤): 在带完整 enriched 列 (ext_*/因子) 的
        # target_frame 上评估, 按符号掩码 entry (只挡入场, exit/持仓卖出不动)。
        # 与 polars 路径 Stage 2.5 同一套条件编译与 fail-closed 语义。
        entry_active = self._apply_matrix_overlay(
            overrides, target_frame, source_panel, market, entry_active, as_of
        )
        entry_signal_hits = self._matrix_signal_hits(
            entry_active,
            signals.entry_signal_code[target_time],
            signals.entry_signal_ids,
            cast(Any, market).symbols,
        )
        exit_signal_hits = self._matrix_signal_hits(
            exit_active,
            signals.exit_signal_code[target_time],
            signals.exit_signal_ids,
            cast(Any, market).symbols,
        )
        selected_assets = np.flatnonzero(entry_active != 0)
        if selected_assets.size == 0:
            return StrategyResult(
                as_of=as_of,
                strategy_id=strategy_id,
                elapsed_ms=(time.perf_counter() - started_at) * 1000,
                entry_signal_hits=entry_signal_hits,
                exit_signal_hits=exit_signal_hits,
            )

        return self._rank_matrix_result(
            strategy_id,
            strategy,
            overrides,
            target_frame,
            signals,
            target_time,
            market,
            selected_assets,
            as_of,
            started_at,
            entry_signal_hits,
            exit_signal_hits,
        )

    def _composite_child_overrides(self, child_ids: list[str], overrides: dict) -> dict[str, dict]:
        """加载各子策略保存的 override 并叠加 composite 统一的 basic_filter。"""
        # 子策略 override: 先加载各自保存的用户配置(参数/评分/信号等),
        # 再叠加 composite 统一的 basic_filter(计划 §3.3, 保证候选池一致)。
        # 这样 composite 内跑子策略与单独跑子策略使用同一口径。
        shared_basic_filter = overrides.get("basic_filter")
        overrides_map: dict[str, dict] = {}
        for cid in child_ids:
            child_override: dict = {}
            if self._override_loader is not None:
                try:
                    loaded = self._override_loader(cid)
                    if isinstance(loaded, dict):
                        child_override = dict(loaded)
                except Exception:  # noqa: BLE001
                    pass
            if shared_basic_filter:
                child_override["basic_filter"] = shared_basic_filter
            overrides_map[cid] = child_override
        return overrides_map

    def _composite_build_result(
        self,
        strategy_id: str,
        strategy: StrategyDef,
        context: StrategyDataContext,
        overrides: dict,
        ordered_results: list[StrategyResult],
        merged: Any,
        started_at: float,
    ) -> StrategyResult:
        """从合并分数与各子结果展示行构造叠加策略最终结果。"""
        # 构造展示行: 按 symbol 从各子结果取首个命中的行(含 name/价格等展示字段),
        # 融合 score。子策略间 schema 可能不同, 保留首个命中子的字段即可。
        row_by_symbol: dict[str, dict] = {}
        for res in ordered_results:
            for row in res.rows:
                sym = str(row.get("symbol"))
                if sym and sym not in row_by_symbol and sym in merged.scores:
                    row_by_symbol[sym] = row

        order_desc = bool(strategy.meta.get("descending", True))
        ranked_symbols = sorted(
            merged.scores.keys(),
            key=lambda s: merged.scores[s],
            reverse=order_desc,
        )
        limit = self._result_limit(strategy, overrides)
        if limit is not None:
            ranked_symbols = ranked_symbols[:limit]

        rows = _sanitize(
            [
                {**row_by_symbol[sym], "score": merged.scores[sym]}
                for sym in ranked_symbols
                if sym in row_by_symbol
            ]
        )
        scores = {str(row["symbol"]): float(row.get("score") or 0.0) for row in rows}

        return StrategyResult(
            as_of=context.as_of,
            strategy_id=strategy_id,
            rows=rows,
            total=len(rows),
            elapsed_ms=(time.perf_counter() - started_at) * 1000,
            scores=scores,
        )

    def _run_composite_strategy(
        self,
        strategy_id: str,
        strategy: StrategyDef,
        context: StrategyDataContext,
        *,
        pool: list[str] | None = None,
        params: dict | None = None,
        overrides: dict | None = None,
        started_at: float,
    ) -> StrategyResult:
        """叠加策略: 调度各子策略(共享 context)→ 合并结果。

        复用 run_all 共享 current/history/market, 避免各子策略重复加载数据。
        子策略必须已在加载期通过两阶段引用校验(存在/非嵌套/asset_types 一致)。
        """
        from app.strategy import composite as composite_mod

        assert strategy.composite is not None
        overrides = overrides or {}

        # 权重: override.children 优先(META 固化值的轻量覆盖), 否则用 META 声明。
        override_children = overrides.get("children")
        if isinstance(override_children, list) and override_children:
            spec = _parse_composite_children(override_children)
            children = spec.children
        else:
            children = strategy.composite.children

        child_ids = [c.strategy_id for c in children]
        child_weights = [c.weight for c in children]
        merge_mode = str((params or {}).get("merge_mode") or "union")
        min_confirm = int((params or {}).get("min_confirm") or 0)

        overrides_map = self._composite_child_overrides(child_ids, overrides)

        # 共享 context 跑所有子策略。run_all 内部对 matrix_native 子策略会
        # 合并 field_columns 构建超集矩阵, 一次加载。
        child_results = self.run_all(
            context,
            params_map={},
            overrides_map=overrides_map,
            strategy_ids=child_ids,
            # 嵌套调用串行: 父级 worker 已并发, 子级再开池会使线程总数随叠加
            # 层数放大 (4×4×...), 超出并发闸与核数的合理范围。
            parallel=False,
        )
        ordered_results = [child_results[cid] for cid in child_ids]

        merged = composite_mod.merge_results(
            ordered_results,
            child_weights,
            merge_mode,
            min_confirm,
            as_of=context.as_of,
            strategy_id=strategy_id,
        )

        return self._composite_build_result(
            strategy_id, strategy, context, overrides, ordered_results, merged, started_at
        )

    @staticmethod
    def _matrix_signal_hits(
        active: np.ndarray,
        codes: np.ndarray,
        signal_ids: tuple[str, ...],
        symbols: tuple[str, ...],
    ) -> list[dict]:
        hits = []
        for asset_id in np.flatnonzero(active != 0):
            code = int(codes[int(asset_id)])
            signals = [signal_ids[code]] if 0 <= code < len(signal_ids) else []
            hits.append({"symbol": symbols[int(asset_id)], "signals": signals})
        return hits

    @staticmethod
    def _matrix_target_frame(panel: pl.DataFrame, as_of: date) -> pl.DataFrame:
        if "datetime" in panel.columns:
            target = panel.filter(pl.col("datetime").cast(pl.Date) == as_of)
            if target.is_empty():
                return target
            latest = target["datetime"].max()
            target = target.filter(pl.col("datetime") == latest)
        elif "date" in panel.columns:
            target = panel.filter(pl.col("date") == as_of)
        else:
            return panel.head(0)
        return target.unique(subset=["symbol"], keep="last")

    # ================================================================
    # 内部: 基础过滤
    # ================================================================

    @staticmethod
    def _basic_filter_expr(df: pl.DataFrame, bf: dict) -> pl.Expr | None:
        """构建基础过滤表达式。回测可复用为买入候选 mask，不删除行情行。"""
        exprs = _basic_numeric_filters(df, bf)
        board_expr = _board_filter_expr(bf.get("boards"))
        if board_expr is not None:
            exprs.append(board_expr)
        return pl.all_horizontal(exprs) if exprs else None

    @staticmethod
    def _apply_basic_filter(df: pl.DataFrame, bf: dict) -> pl.DataFrame:
        """Stage 1: 基础参数过滤"""
        expr = _engine_pkg.StrategyEngine._basic_filter_expr(df, bf)
        if expr is not None:
            return df.filter(expr)
        return df

    # 分钟策略命中行需要从事后联表补齐的 enriched 列: 基础过滤引用 + 前端展示。
    # close 不在列 — 分钟策略输出的 close 是最后一根分钟K收盘价, 优先于日线快照。
    MINUTE_JOIN_COLUMNS: tuple[str, ...] = (
        "name",
        "total_shares",
        "float_shares",
        "amount",
        "turnover_rate",
        "change_pct",
        "pre_close",
    )

    def _user_data_dir(self) -> Path | None:
        """从策略目录推导 data_dir(…/strategies/custom → data_dir)。推不出则跳过注入。"""
        for d in self._strategy_dirs:
            if d.name == "custom" and d.parent.name == "strategies":
                return d.parent.parent
        return None

    def _inject_intraday_signal_columns(self, minute_df: pl.DataFrame) -> pl.DataFrame:
        """向当日分钟K帧注入自定义盘中信号列(csgi_, 当日条件上升沿)。

        单点注入: 实盘分钟扫描与分钟回测 worker 共用本方法, 特征计算与
        监控评估同源(intraday_features), 保证口径一致。无定义/帧为空时原样返回。
        """
        if minute_df is None or minute_df.is_empty() or "datetime" not in minute_df.columns:
            return minute_df
        data_dir = self._user_data_dir()
        if data_dir is None:
            return minute_df
        try:
            from app.strategy import custom_signals
            from app.strategy.intraday_features import build_feature_frame

            definitions = custom_signals.load_intraday_all(data_dir)
            if not definitions:
                return minute_df
            exprs = custom_signals.build_intraday_expressions(definitions)
            if not exprs:
                return minute_df
            frame = build_feature_frame(minute_df)
            if frame.is_empty():
                return minute_df
            evaluated = custom_signals.apply_intraday_edges(frame, exprs).select(
                ["symbol", "datetime", *exprs.keys()]
            )
            return minute_df.join(evaluated, on=["symbol", "datetime"], how="left").with_columns(
                [pl.col(name).fill_null(False).cast(pl.Boolean).alias(name) for name in exprs]
            )
        except Exception as e:  # noqa: BLE001 - 盘中信号注入失败降级, 由 required_features 兜底
            # 注入失败不阻断策略执行: 未注入列会由 required_features 校验兜底报错
            logger.warning("intraday signal inject failed: %s", e)
            return minute_df
