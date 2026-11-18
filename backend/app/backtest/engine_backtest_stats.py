from __future__ import annotations

import app.backtest.engine as _root
from app.backtest.engine import (
    SimResult,
    SimulationOptions,
    TradeRecord,
    _independent_candidate_curves,
    _independent_candidate_empty_result,
    _independent_candidate_stats,
    _max_drawdown_from_pnls,
    date,
    np,
    pl,
    time,
)


class BacktestStatsMixin:
    """BacktestEngine 混入方法组 — 由 engine.py 组合。"""

    @staticmethod
    def _build_curves(
        trades: list[TradeRecord],
        all_dates: np.ndarray,
        initial_capital: float,
    ) -> tuple[list[dict], list[dict]]:
        """从交易记录构建日频净值曲线和回撤曲线。

        资金模型: 每笔交易等权分配 (1/N_capital)，N_capital = 同时持仓数上限。
        简化版: 按出场日归集所有已平仓交易的平均收益作为当日组合收益。
        """
        if not trades or len(all_dates) == 0:
            return [], []

        # 按出场日归集 pnl
        exit_pnl: dict[str, list[float]] = {}
        for t in trades:
            d_str = str(t.exit_date)
            exit_pnl.setdefault(d_str, []).append(t.pnl_pct)

        equity = initial_capital
        peak = initial_capital
        curve: list[dict] = []
        dd_curve: list[dict] = []

        for d in all_dates:
            d_str = str(d.item() if hasattr(d, "item") else d)
            pnls = exit_pnl.get(d_str, [])
            # 当日组合收益 = 该日所有出场交易的平均收益
            daily_ret = float(np.mean(pnls)) if pnls else 0.0
            equity *= 1 + daily_ret
            peak = max(peak, equity)
            dd = (equity - peak) / peak if peak > 0 else 0.0
            curve.append({"date": d_str[:10], "value": round(equity, 2)})
            dd_curve.append({"date": d_str[:10], "value": round(dd, 4)})

        return curve, dd_curve

    # ── 统计计算 ──────────────────────────────────────

    @staticmethod
    def _sortino_ratio(returns: np.ndarray, periods_per_year: int = 252) -> float | None:
        """Sortino 比率: 用下行偏差 (仅惩罚负收益) 替代总标准差, 年化。

        下行偏差 = sqrt(mean(min(r, 0)^2)), MAR=0 的目标半方差 (对全部样本求均, 非仅负样本)。
        无下行波动 (无亏损) 时 Sortino 未定义, 返回 None (与 profit_factor 的 None 约定一致,
        不虚报 0 或 inf)。样本不足 (<2) 返回 0.0 (与 sharpe 的退化约定一致)。
        """
        returns = returns[np.isfinite(returns)]  # 剔除 inf/nan, 防止污染均值/序列化出非法 JSON
        if len(returns) < 2:
            return 0.0
        mean = float(np.mean(returns))
        downside = np.minimum(returns, 0.0)
        downside_dev = float(np.sqrt(np.mean(downside**2)))
        if downside_dev <= 0:
            return None
        return mean / downside_dev * float(np.sqrt(periods_per_year))

    @staticmethod
    def _mc_drawdown_percentiles(pnls: np.ndarray, n_sims: int = 1000) -> dict:
        """自助重抽样交易序列, 估计最大回撤的分布 — 回答"仅因成交顺序运气, 回撤能有多坏"。

        对每笔收益有放回重抽样 n_sims 次, 各自算最大回撤, 取分位:
        - mc_maxdd_p50: 中位场景最大回撤
        - mc_maxdd_p95: 95% 置信最坏场景 (= 分布 5 分位, 更负)

        固定种子保证可复现/可测。样本 <3 无统计意义, 返回 None。
        大样本 (如 full 模式数千笔) 时按 2M 单元上限压降模拟次数, 防止瞬时数组 OOM。
        """
        pnls = pnls[np.isfinite(pnls)]  # 剔除 inf/nan, 否则 cumprod 传播 nan 导致分位为 nan
        # 防御: 单笔 pnl <= -100% 时 (1+pnl) <= 0 会让 cumprod 符号翻转/得非正净值, 回撤失真。
        # 回测有止损, 实际不会发生; 兜底 clip 到 -99.99% 保证 (1+pnl) 恒正。
        pnls = np.clip(pnls, -0.9999, None)
        n = len(pnls)
        if n < 3:
            return {"mc_maxdd_p50": None, "mc_maxdd_p95": None}
        # 内存护栏: samples/equity/peak/dd 各占 eff_sims*n*8B, 控总单元 <= 2M (~64MB 峰值)
        eff_sims = min(n_sims, max(200, 2_000_000 // n))
        rng = np.random.default_rng(42)
        samples = rng.choice(pnls, size=(eff_sims, n), replace=True)
        equity = np.cumprod(1.0 + samples, axis=1)
        peak = np.maximum.accumulate(equity, axis=1)
        dd = (equity - peak) / peak
        maxdds = dd.min(axis=1)
        return {
            "mc_maxdd_p50": round(float(np.percentile(maxdds, 50)), 4),
            "mc_maxdd_p95": round(float(np.percentile(maxdds, 5)), 4),
        }

    @staticmethod
    def _per_trade_block(pnls: np.ndarray, durations: np.ndarray) -> dict:
        """per-trade 明细字段: best/worst/median_pnl/avg_holding_days。"""
        pnls = pnls[np.isfinite(pnls)]  # 剔除 inf/nan, 防 best/worst 出非法值
        durations = durations[np.isfinite(durations)] if len(durations) else durations
        if not len(pnls):
            return {"best": 0.0, "worst": 0.0, "median_pnl": 0.0, "avg_holding_days": 0.0}
        return {
            "best": round(float(np.max(pnls)), 4),
            "worst": round(float(np.min(pnls)), 4),
            "median_pnl": round(float(np.median(pnls)), 4),
            "avg_holding_days": round(float(np.mean(durations)), 1) if len(durations) else 0.0,
        }

    @staticmethod
    def _calc_stats(
        trades: list[TradeRecord],
        initial_capital: float,
        start: date,
        end: date,
        *,
        include_monte_carlo: bool = True,
    ) -> dict:
        if not trades:
            return {"total_return": 0, "n_trades": 0}

        pnls = np.array([t.pnl_pct for t in trades])
        n_trades = len(trades)

        # 从净值曲线推算总收益 (等权组合)
        cumulative = 1.0
        for p in pnls:
            cumulative *= 1 + p
        # 修正: 等权组合的总收益不等于各笔复乘，用曲线终点更准
        # 但这里作为简化，用各笔复乘作为近似
        total_return = cumulative - 1.0

        # 年化
        n_days = max((end - start).days, 1)
        years = n_days / 365.25
        if total_return > -1.0 and years > 0:
            annual_return = (1 + total_return) ** (1 / years) - 1
        else:
            annual_return = total_return

        # 胜率
        wins = pnls[pnls > 0]
        losses = pnls[pnls <= 0]
        win_rate = len(wins) / n_trades

        # 盈亏比
        avg_win = float(np.mean(wins)) if len(wins) > 0 else 0.0
        avg_loss = abs(float(np.mean(losses))) if len(losses) > 0 else 0.0
        profit_factor = (
            avg_win / avg_loss if avg_loss > 0 else (float("inf") if avg_win > 0 else 0.0)
        )

        # 最大回撤 — 用交易序列近似
        max_dd = _max_drawdown_from_pnls(pnls, initial_capital)
        # 夏普 — 用交易收益标准差近似
        # 样本标准差 (ddof=1): 与主流回测库惯例一致; 单样本 std=nan → 守卫为 0
        _std = float(np.std(pnls, ddof=1)) if len(pnls) > 1 else 0.0
        sharpe = float(np.mean(pnls) / _std) * np.sqrt(252) if _std > 0 else 0.0

        # Sortino: 刻意沿用本函数 sharpe 的逐笔收益 x sqrt(252) 基准。逐笔年化非严格正确,
        # 但保证同一函数内 sharpe/sortino 口径一致可比 (内部一致 > 局部绝对)。仅惩罚下行波动。
        sortino = _root.BacktestEngine._sortino_ratio(pnls)

        # Calmar
        calmar = annual_return / abs(max_dd) if abs(max_dd) > 0.001 else 0.0

        durations = np.array([t.duration for t in trades], dtype=float)
        stats = {
            "total_return": round(float(total_return), 4),
            "annual_return": round(float(annual_return), 4),
            "max_drawdown": round(float(max_dd), 4),
            "sharpe": round(float(sharpe), 2),
            "sortino": round(float(sortino), 2) if sortino is not None else None,
            "calmar": round(float(calmar), 2),
            "win_rate": round(float(win_rate), 4),
            "profit_factor": round(float(profit_factor), 2) if np.isfinite(profit_factor) else None,
            "n_trades": n_trades,
            "avg_pnl": round(float(np.mean(pnls)), 4),
            "avg_win": round(avg_win, 4),
            "avg_loss": round(avg_loss, 4),
            **_root.BacktestEngine._per_trade_block(pnls, durations),
        }
        if include_monte_carlo:
            stats.update(_root.BacktestEngine._mc_drawdown_percentiles(pnls))
        return stats

    @staticmethod
    def _calc_per_symbol(trades: list[TradeRecord]) -> list[dict]:
        if not trades:
            return []
        by_sym: dict[str, dict] = {}
        for t in trades:
            s = t.symbol
            d = by_sym.setdefault(
                s,
                {
                    "symbol": s,
                    "n_trades": 0,
                    "total_return": 1.0,
                    "best": -999.0,
                    "worst": 999.0,
                    "wins": 0,
                    "pnls": [],
                },
            )
            d["n_trades"] += 1
            d["pnls"].append(t.pnl_pct)
            d["total_return"] *= 1 + t.pnl_pct
            d["best"] = max(d["best"], t.pnl_pct)
            d["worst"] = min(d["worst"], t.pnl_pct)
            if t.pnl_pct > 0:
                d["wins"] += 1

        result = []
        for d in by_sym.values():
            result.append(  # noqa: PERF401 - PERF401: 循环内含多行处理/异常, 改写推导式损可读性
                {
                    "symbol": d["symbol"],
                    "n_trades": d["n_trades"],
                    "total_return": round(d["total_return"] - 1.0, 4),
                    "win_rate": round(d["wins"] / d["n_trades"], 4) if d["n_trades"] > 0 else 0.0,
                    "best": round(d["best"], 4),
                    "worst": round(d["worst"], 4),
                }
            )
        return sorted(result, key=lambda x: x["total_return"], reverse=True)

    @staticmethod
    def _calc_independent_candidate_result(
        trades: list[TradeRecord],
        n_candidates: int,
        execution_stats: dict[str, int],
        *,
        options: SimulationOptions | None = None,
    ) -> SimResult:
        """全量独立候选统计：按每个候选样本的实际执行收益聚合。"""
        options = options or SimulationOptions()
        if not trades:
            return _independent_candidate_empty_result(n_candidates, execution_stats)

        pnls = np.array([t.pnl_pct for t in trades], dtype=float)
        durations = np.array([t.duration for t in trades], dtype=float)
        equity_curve, drawdown_curve, equity_values, daily_avg = _independent_candidate_curves(
            trades, options.include_curves
        )

        statistics_started = time.perf_counter()
        daily = np.array(daily_avg, dtype=float)
        stats = _independent_candidate_stats(
            pnls,
            durations,
            daily,
            equity_values,
            n_candidates,
            len(trades),
            len(set(str(t.exit_date)[:10] for t in trades)),
            execution_stats,
        )
        if options.include_return_distribution:
            lo, hi, nbins = -0.20, 0.20, 20
            clipped = np.clip(pnls, lo, hi)
            counts, edges = np.histogram(clipped, bins=nbins, range=(lo, hi))
            stats["return_distribution"] = [
                {
                    "range": f"{(edges[i] * 100):+.0f}~{(edges[i + 1] * 100):+.0f}%",
                    "count": int(counts[i]),
                    "ratio": round(float(counts[i] / pnls.size), 4) if pnls.size else 0.0,
                }
                for i in range(nbins)
            ]
        if options.include_monte_carlo:
            stats.update(_root.BacktestEngine._mc_drawdown_percentiles(pnls))
        stats["statistics_ms"] = round(
            (time.perf_counter() - statistics_started) * 1000,
            1,
        )

        return SimResult(
            equity_curve=equity_curve if options.include_curves else [],
            drawdown_curve=drawdown_curve if options.include_curves else [],
            trades=trades if options.include_trades else [],
            per_symbol_stats=(
                _root.BacktestEngine._calc_per_symbol(trades)
                if options.include_per_symbol_stats
                else []
            ),
            stats=stats,
        )

    @staticmethod
    def _calc_portfolio_stats(
        equity_curve: list[dict],
        trades: list[TradeRecord],
        initial_capital: float,
        *,
        include_monte_carlo: bool = True,
    ) -> dict:
        equity_values = [float(row["value"]) for row in equity_curve]
        exposure_values = [float(row.get("exposure", 0.0)) for row in equity_curve]
        return _root.BacktestEngine._calc_portfolio_stats_from_values(
            equity_values,
            exposure_values,
            trades,
            initial_capital,
            include_monte_carlo=include_monte_carlo,
        )

    @staticmethod
    def _calc_portfolio_stats_from_values(
        equity_values: list[float],
        exposure_values: list[float],
        trades: list[TradeRecord],
        initial_capital: float,
        *,
        include_monte_carlo: bool = True,
    ) -> dict:
        if not equity_values:
            return {"total_return": 0, "n_trades": 0}
        final_equity = float(equity_values[-1])
        total_return = final_equity / initial_capital - 1 if initial_capital > 0 else 0.0
        values = np.array(equity_values, dtype=float)
        daily = values[1:] / values[:-1] - 1 if len(values) > 1 else np.array([])
        annual_return = (
            (1 + total_return) ** (252 / max(len(equity_values), 1)) - 1
            if total_return > -1
            else total_return
        )
        peaks = np.maximum.accumulate(values)
        drawdowns = values / peaks - 1
        max_drawdown = float(drawdowns.min()) if len(drawdowns) else 0.0  # type: ignore[call-overload]
        # 样本标准差 (ddof=1), 与主流回测库惯例一致
        _std = float(np.std(daily, ddof=1)) if len(daily) > 1 else 0.0
        sharpe = float(np.mean(daily) / _std * np.sqrt(252)) if _std > 0 else 0.0
        sortino = _root.BacktestEngine._sortino_ratio(daily)
        pnls = np.array([t.pnl_pct for t in trades], dtype=float) if trades else np.array([])
        durations = np.array([t.duration for t in trades], dtype=float) if trades else np.array([])
        exposures = np.array(exposure_values, dtype=float)
        wins = pnls[pnls > 0]
        losses = pnls[pnls <= 0]
        avg_win = float(np.mean(wins)) if len(wins) else 0.0
        avg_loss = abs(float(np.mean(losses))) if len(losses) else 0.0
        stats = {
            "total_return": round(float(total_return), 4),
            "annual_return": round(float(annual_return), 4),
            "max_drawdown": round(float(max_drawdown), 4),
            "sharpe": round(float(sharpe), 2),
            "sortino": round(float(sortino), 2) if sortino is not None else None,
            "calmar": round(float(annual_return / abs(max_drawdown)), 2)
            if abs(max_drawdown) > 0.001
            else 0.0,
            "win_rate": round(float(len(wins) / len(pnls)), 4) if len(pnls) else 0.0,
            "profit_factor": round(float(avg_win / avg_loss), 2) if avg_loss > 0 else None,
            "n_trades": len(trades),
            "avg_pnl": round(float(np.mean(pnls)), 4) if len(pnls) else 0.0,
            "avg_win": round(avg_win, 4),
            "avg_loss": round(avg_loss, 4),
            **_root.BacktestEngine._per_trade_block(pnls, durations),
            "final_equity": round(final_equity, 2),
            "initial_capital": round(float(initial_capital), 2),
            "avg_exposure": round(float(np.mean(exposures)), 4) if len(exposures) else 0.0,
            "max_exposure": round(float(np.max(exposures)), 4) if len(exposures) else 0.0,
        }
        if include_monte_carlo:
            stats.update(_root.BacktestEngine._mc_drawdown_percentiles(pnls))
        return stats

    @staticmethod
    def _date_str(value) -> str:
        value = value.item() if hasattr(value, "item") else value
        return str(value)[:10]

    @staticmethod
    def _empty_result() -> SimResult:
        return SimResult(
            equity_curve=[],
            drawdown_curve=[],
            trades=[],
            per_symbol_stats=[],
            stats={"error": "no data or no signals"},
        )

    # ── 截面工具 (因子回测用) ─────────────────────────

    @staticmethod
    def cross_section_rank(panel: pl.DataFrame, col: str) -> pl.DataFrame:
        return panel.with_columns(
            pl.col(col).rank(method="random").over("date").alias(f"{col}_rank")
        )

    @staticmethod
    def cross_section_qcut(panel: pl.DataFrame, col: str, n_groups: int) -> pl.DataFrame:
        return panel.with_columns(
            pl.col(col)
            .qcut(n_groups, labels=[f"Q{i + 1}" for i in range(n_groups)])
            .over("date")
            .alias("_group")
        )
