from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl

from app.backtest.engine import SimResult


class StrategyBacktestSimMixin:
    """完整成交曲线与统计。"""

    # 由组装类/其他 mixin 提供, 拆分后在此显式声明以对 mypy 可见。
    _build_benchmark_curve: Any
    engine: Any

    # ── 全量模拟 (策略能力统计, 不建组合不算净值) ──

    @staticmethod
    def _full_sim_curves(daily: pl.DataFrame) -> tuple[list[dict], list[dict], float]:
        """按日复利累计收益, 构造净值/回撤曲线并返回最终净值。"""
        # 累计超额曲线: 每日复利平均收益 (基准归零, 故 equity 即累计策略收益)
        equity_curve: list[dict] = []
        equity = 1.0
        peak = 1.0
        drawdown_curve: list[dict] = []
        for row in daily.iter_rows(named=True):
            ret = float(row["avg_ret"] or 0.0)
            equity *= 1 + ret
            peak = max(peak, equity)
            dd = (equity - peak) / peak if peak > 0 else 0.0
            d_str = str(row["date"])[:10]
            equity_curve.append(
                {
                    "date": d_str,
                    "value": round(equity, 4),
                    "positions": int(row["n_cand"]),
                }
            )
            drawdown_curve.append({"date": d_str, "value": round(dd, 4)})
        return equity_curve, drawdown_curve, equity

    def _full_sim_benchmark(self, daily: pl.DataFrame) -> float:
        """同期上证收益 (用 benchmark close 算), 失败按 0 处理。"""
        benchmark_curve = self._build_benchmark_curve(daily["date"].min(), daily["date"].max())
        benchmark_return = 0.0
        if benchmark_curve:
            closes = [b["close"] for b in benchmark_curve if b.get("close")]
            if len(closes) >= 2 and closes[0] > 0:
                benchmark_return = closes[-1] / closes[0] - 1
        return benchmark_return

    @staticmethod
    def _full_sim_distribution(fwd) -> list[dict]:
        """收益分布直方图: 按 [-20%, +20%] 分 21 档, 超出归入首尾档。"""
        lo, hi, nbins = -0.20, 0.20, 20
        clipped = np.clip(fwd, lo, hi)
        counts, edges = np.histogram(clipped, bins=nbins, range=(lo, hi))
        return [
            {
                "range": f"{(edges[i] * 100):+.0f}~{(edges[i + 1] * 100):+.0f}%",
                "count": int(counts[i]),
                "ratio": round(float(counts[i] / fwd.size), 4) if fwd.size else 0.0,
            }
            for i in range(nbins)
        ]

    @staticmethod
    def _full_sim_stats(
        fwd,
        wins,
        losses,
        daily: pl.DataFrame,
        avg_win: float,
        avg_loss: float,
        total_return: float,
        max_dd: float,
        sharpe: float,
        benchmark_return: float,
        dist: list[dict],
    ) -> dict:
        """汇总全量模拟统计指标。"""
        return {
            "mode": "full",
            "n_candidates": int(fwd.size),
            "n_days": int(daily.height),
            "avg_daily_candidates": round(float(daily["n_cand"].mean()), 1),
            "avg_return": round(float(fwd.mean()), 4),
            "median_return": round(float(np.median(fwd)), 4),
            "win_rate": round(float(wins.size / fwd.size), 4) if fwd.size else 0.0,
            "profit_factor": round(avg_win / avg_loss, 2) if avg_loss > 0 else None,
            "best": round(float(fwd.max()), 4),
            "worst": round(float(fwd.min()), 4),
            "total_return": round(float(total_return), 4),
            "max_drawdown": round(float(max_dd), 4),
            "sharpe": round(sharpe, 2),
            "benchmark_return": round(float(benchmark_return), 4),
            "excess": round(float(total_return - benchmark_return), 4),
            "return_distribution": dist,
        }

    def _run_full_simulation(
        self,
        panel: pl.DataFrame,
        entry_mask: pl.Series,
        holding_days: int,
    ) -> SimResult:
        """对 entry_mask 命中的全部候选, 算持有 N 天后的前瞻收益统计。

        不受 max_positions/资金约束, 反映策略能力本身。
        equity_curve 复用为"累计日均超额收益曲线"(基准归零)。
        """
        n = holding_days if holding_days and holding_days > 0 else 5

        df = panel.with_columns(
            [
                entry_mask.cast(pl.Boolean).alias("_is_candidate"),
                (pl.col("close").shift(-n).over("symbol") / pl.col("close") - 1).alias(
                    "_fwd_return"
                ),
            ]
        ).filter(
            pl.col("_is_candidate")
            & pl.col("_fwd_return").is_not_null()
            & pl.col("_fwd_return").is_not_nan()
        )

        if df.is_empty():
            return self.engine._empty_result()

        fwd = df["_fwd_return"].to_numpy()
        wins = fwd[fwd > 0]
        losses = fwd[fwd <= 0]
        avg_win = float(wins.mean()) if wins.size else 0.0
        avg_loss = abs(float(losses.mean())) if losses.size else 0.0

        # 按日聚合: 当日候选的平均前瞻收益
        daily = (
            df.group_by("date")
            .agg(
                pl.col("_fwd_return").mean().alias("avg_ret"),
                pl.col("_fwd_return").count().alias("n_cand"),
            )
            .sort("date")
        )

        equity_curve, drawdown_curve, equity = self._full_sim_curves(daily)
        benchmark_return = self._full_sim_benchmark(daily)
        total_return = equity - 1.0
        max_dd = min((d["value"] for d in drawdown_curve), default=0.0)

        # 日收益序列算 Sharpe (年化)
        daily_rets = daily["avg_ret"].to_numpy()
        sharpe = (
            float(daily_rets.mean() / daily_rets.std() * np.sqrt(252))
            if daily_rets.size > 1 and daily_rets.std() > 0
            else 0.0
        )

        dist = self._full_sim_distribution(fwd)
        stats = self._full_sim_stats(
            fwd,
            wins,
            losses,
            daily,
            avg_win,
            avg_loss,
            total_return,
            max_dd,
            sharpe,
            benchmark_return,
            dist,
        )
        return SimResult(
            equity_curve=equity_curve,
            drawdown_curve=drawdown_curve,
            trades=[],
            per_symbol_stats=[],
            stats=stats,
        )
