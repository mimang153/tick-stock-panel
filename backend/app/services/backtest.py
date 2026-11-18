"""回测服务(§6.7)。

包 vectorbt — 全项目唯一一处出现 pandas。
"""

from __future__ import annotations

import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd
import polars as pl

from app.tickflow.repository import KlineRepository

logger = logging.getLogger(__name__)

# 旧信号回测的指标 warmup 日历窗口 (#201): 与 backtest.factor.FACTOR_WARMUP_DAYS
# 同源 (120 交易日 → 保守取日历日), 覆盖 MA60/MACD/BOLL 等最长回看
_WARMUP_CALENDAR_DAYS = 120 * 1.6

# vectorbt 是 optional extras(见 pyproject.toml).未装时只有 backtest 不可用,其他功能正常.
_vbt = None
_vbt_unavailable_reason: str | None = None


class VectorbtUnavailable(RuntimeError):  # noqa: N818 - N818: 异常名与既有API对齐
    """vectorbt 未安装 — 提示用户 `uv sync --extra backtest`."""


def _get_vbt():
    global _vbt, _vbt_unavailable_reason
    if _vbt is not None:
        return _vbt
    if _vbt_unavailable_reason is not None:
        raise VectorbtUnavailable(_vbt_unavailable_reason)
    try:
        import vectorbt as vbt

        _vbt = vbt
        return _vbt
    except ImportError as e:
        _vbt_unavailable_reason = (
            "vectorbt 未安装 — 它是回测的可选依赖.macOS Intel 用户先 `brew install cmake` "
            "然后 `uv sync --extra backtest`"
        )
        logger.warning("vectorbt unavailable: %s", e)
        raise VectorbtUnavailable(_vbt_unavailable_reason) from e


def is_available() -> bool:
    """供 API 层快速检测."""
    try:
        _get_vbt()
        return True
    except VectorbtUnavailable:
        return False


SignalKind = Literal[
    "macd_golden",
    "macd_dead",
    "ma_golden_5_20",
    "ma_dead_5_20",
    "ma_golden_20_60",
    "ma20_breakout",
    "ma20_breakdown",
    "n_day_high",
    "n_day_low",
    "boll_breakout_upper",
    "boll_breakdown_lower",
    "volume_surge",
    "rsi_oversold",
    "rsi_overbought",
    "stop_loss",
    "trailing_stop",
    "max_hold",
]


@dataclass
class BacktestConfig:
    symbols: list[str]
    start: date
    end: date
    # 买入信号(任一触发即买)
    entries: list[str] = field(default_factory=list)
    # 卖出信号(任一触发即卖)
    exits: list[str] = field(default_factory=list)
    # 其他参数
    stop_loss_pct: float | None = None  # 例 -0.05 = -5%
    max_hold_days: int | None = None
    fees_pct: float = 0.0002  # 万二佣金
    slippage_bps: float = 5  # 5 bps
    # 撮合
    matching: Literal["close_t", "open_t+1"] = "close_t"
    rsi_oversold_threshold: float = 30
    rsi_overbought_threshold: float = 70
    asset_type: str = "stock"


@dataclass
class BacktestResult:
    run_id: str
    config: dict
    stats: dict
    equity_curve: list[dict]  # [{date, value}]
    trades: list[dict]  # [{symbol, entry_date, exit_date, pnl_pct, ...}]
    per_symbol_stats: list[dict]  # 每只股票的统计


# enriched 表里的信号列名映射
_SIGNAL_COLS: dict[SignalKind, str] = {
    "macd_golden": "signal_macd_golden",
    "macd_dead": "signal_macd_dead",
    "ma_golden_5_20": "signal_ma_golden_5_20",
    "ma_dead_5_20": "signal_ma_dead_5_20",
    "ma_golden_20_60": "signal_ma_golden_20_60",
    "ma20_breakout": "signal_ma20_breakout",
    "ma20_breakdown": "signal_ma20_breakdown",
    "n_day_high": "signal_n_day_high",
    "n_day_low": "signal_n_day_low",
    "boll_breakout_upper": "signal_boll_breakout_upper",
    "boll_breakdown_lower": "signal_boll_breakdown_lower",
    "volume_surge": "signal_volume_surge",
}


def _build_max_hold_exits(entries: pd.DataFrame, max_hold_days: int) -> pd.DataFrame:
    """为每个入场信号在 max_hold_days 个交易日后生成一个强制退出信号。

    返回与 entries 同形状的布尔矩阵, 仅在「入场位之后第 max_hold_days 个交易日」置
    True(不含入场位本身), 供调用方与用户 exits 做 OR。

    两处易错点(见 #198):
    - 必须从全 False 起步。若用 `entries.copy()` 起步会把入场位当成退出位,
      导致入场当日即被强制平仓。
    - 用单步定位写入 `iloc[row, col_loc]`。链式 `iloc[row][col] = True` 写入的是
      临时行副本, 在 pandas Copy-on-Write 语义下不会落到原矩阵(pandas 3.x 直接报错),
      强制退出信号会静默丢失。
    """
    out = pd.DataFrame(False, index=entries.index, columns=entries.columns)
    n = len(entries)
    for col in entries.columns:
        col_loc = out.columns.get_loc(col)
        entry_rows = np.where(entries[col].to_numpy())[0]
        for i in entry_rows:
            end_i = min(int(i) + max_hold_days, n - 1)
            if end_i > i:
                out.iloc[end_i, col_loc] = True
    return out


class BacktestService:
    def __init__(self, repo: KlineRepository) -> None:
        self.repo = repo

    def _load_panel(
        self,
        symbols: list[str],
        start: date,
        end: date,
        asset_type: str = "stock",
    ) -> pd.DataFrame:
        """加载 [date × symbol] 价格面板 — Polars scan_parquet + 即时计算指标。

        **全项目唯一从 Polars 转 pandas 的边界**(§7.4 / ADR-19)。
        asset_type='etf' 时读 ETF enriched。
        """
        try:
            from app.storage.runtime import require_storage
            from app.tickflow.repository import enriched_dirname

            # 指标 warmup (#201): MA/MACD/RSI/BOLL 需要区间前的历史窗口,
            # 直接按 [start,end] 过滤后 compute_all 会让区间头部的指标失真。
            # 与挖掘侧同款公式 (mining_runtime: warmup = max(120, bars*1.6)),
            # 此处指标最长回看约 120 交易日, 取保守日历日窗口; 数据不足时
            # 自然退化 (有多少算多少)。计算完成后裁回 [start,end]。
            warmup_start = start - timedelta(days=_WARMUP_CALENDAR_DAYS)
            df = (
                require_storage()
                .store.read_daily(
                    enriched_dirname(asset_type),
                    symbols=list(symbols),
                    start=warmup_start,
                    end=end,
                )
                .sort(["date", "symbol"])
            )
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("backtest load failed: %s", e)
            return pd.DataFrame()

        if df.is_empty():
            return pd.DataFrame()

        # 即时计算指标 + 信号
        from app.indicators.pipeline import compute_all

        df = compute_all(df)
        df = df.filter(pl.col("date") >= start)

        # 选择需要的列
        needed_cols = [
            "date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "rsi_14",
            "signal_macd_golden",
            "signal_macd_dead",
            "signal_ma_golden_5_20",
            "signal_ma_dead_5_20",
            "signal_ma_golden_20_60",
            "signal_ma20_breakout",
            "signal_ma20_breakdown",
            "signal_n_day_high",
            "signal_n_day_low",
            "signal_boll_breakout_upper",
            "signal_boll_breakdown_lower",
            "signal_volume_surge",
        ]
        existing = [c for c in needed_cols if c in df.columns]
        df = df.select(existing)

        # to_pandas 边界
        return df.to_pandas(use_pyarrow_extension_array=False)

    def _build_signal_matrix(
        self,
        panel: pd.DataFrame,
        kinds: list[str],
        config: BacktestConfig,
    ) -> pd.DataFrame:
        """从面板构造 [date × symbol] 的布尔信号矩阵。"""
        if not kinds or panel.empty:
            return pd.DataFrame()

        # pivot 成 [date × symbol] 形式
        result = None
        for kind in kinds:
            mat = None
            if kind in _SIGNAL_COLS:
                col = _SIGNAL_COLS[kind]
                mat = (
                    panel.pivot(index="date", columns="symbol", values=col)
                    .fillna(False)
                    .astype(bool)
                )
            elif kind == "rsi_oversold":
                mat = (
                    panel.pivot(index="date", columns="symbol", values="rsi_14")
                    < config.rsi_oversold_threshold
                )
            elif kind == "rsi_overbought":
                mat = (
                    panel.pivot(index="date", columns="symbol", values="rsi_14")
                    > config.rsi_overbought_threshold
                )
            # stop_loss / trailing / max_hold 通过 vectorbt 参数处理,不参与信号矩阵

            if mat is not None:
                result = mat if result is None else (result | mat)
        return result if result is not None else pd.DataFrame()

    def run(self, config: BacktestConfig) -> BacktestResult:
        from app.services.heavy_job_limiter import shared_heavy_job_limiter

        with shared_heavy_job_limiter.slot("exclusive"):
            return self._run(config)

    # ---- _run 分段 helper (语义等价) ----

    def _error_result(self, run_id, config, error):
        return BacktestResult(
            run_id=run_id,
            config=_config_to_dict(config),
            stats={"error": error},
            equity_curve=[],
            trades=[],
            per_symbol_stats=[],
        )

    def _prepare_signals(self, panel, config, close):
        # 信号矩阵
        entries = self._build_signal_matrix(panel, config.entries, config)
        exits = self._build_signal_matrix(panel, config.exits, config)

        # 对齐 index/columns
        if not entries.empty:
            entries = entries.reindex_like(close).fillna(False).astype(bool)
        else:
            entries = pd.DataFrame(False, index=close.index, columns=close.columns)
        if not exits.empty:
            exits = exits.reindex_like(close).fillna(False).astype(bool)
        else:
            exits = pd.DataFrame(False, index=close.index, columns=close.columns)

        if not entries.any().any():
            return None

        # T+1 适配:vectorbt 默认信号当根 K 撮合
        # close_t 撮合:维持默认
        # open_t+1 撮合:shift 信号 1 根 + 用 open 作为价
        if config.matching == "open_t+1":
            entries = entries.shift(1).fillna(False).astype(bool)
            exits = exits.shift(1).fillna(False).astype(bool)
            price = panel.pivot(index="date", columns="symbol", values="open")
        else:
            price = close
        return entries, exits, price

    def _run_portfolio(self, config, close, entries, exits, price, vbt):
        try:
            pf_kwargs = dict(
                close=close,
                entries=entries,
                exits=exits,
                price=price,
                fees=config.fees_pct,
                slippage=config.slippage_bps / 10000.0,
                freq="1D",
            )
            if config.stop_loss_pct is not None:
                pf_kwargs["sl_stop"] = abs(config.stop_loss_pct)
            if config.max_hold_days is not None:
                # vectorbt 没有内置 max-hold;用时间退出近似:入场后第 max_hold_days
                # 个交易日强制 exit, 与用户 exits 做 OR(保留原有信号退出)。
                forced_exits = _build_max_hold_exits(entries, config.max_hold_days)
                pf_kwargs["exits"] = (exits | forced_exits).astype(bool)

            pf = vbt.Portfolio.from_signals(**pf_kwargs)
        except Exception as e:
            logger.exception("vectorbt backtest failed")
            return None, str(e)
        return pf, None

    def _extract_results(self, pf):
        # 提取结果
        try:
            stats_series = pf.stats(silence_warnings=True)
            if isinstance(stats_series, pd.DataFrame):
                # 多列时取 agg
                stats_dict = stats_series.mean(numeric_only=True).to_dict()
            else:
                stats_dict = stats_series.to_dict()
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            stats_dict = {}

        # 净值曲线(组合平均)
        equity = pf.value().mean(axis=1) if isinstance(pf.value(), pd.DataFrame) else pf.value()
        equity_curve = [
            {"date": str(idx.date() if hasattr(idx, "date") else idx), "value": float(v)}
            for idx, v in equity.items()
            if pd.notna(v)
        ]

        # 交易记录
        try:
            trades_df = pf.trades.records_readable
            trades = trades_df.to_dict(orient="records") if not trades_df.empty else []
            # 字段名美化
            trades = [
                {
                    "symbol": t.get("Column", t.get("Symbol", "")),
                    "entry_date": str(t.get("Entry Timestamp", t.get("Entry Date", ""))),
                    "exit_date": str(t.get("Exit Timestamp", t.get("Exit Date", ""))),
                    "entry_price": float(t.get("Avg Entry Price", t.get("Avg. Entry Price", 0))),
                    "exit_price": float(t.get("Avg Exit Price", t.get("Avg. Exit Price", 0))),
                    "pnl_pct": float(t.get("Return", t.get("PnL %", 0))),
                    "duration": str(t.get("Duration", "")),
                }
                for t in trades
            ]
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            trades = []

        # 每标的统计
        per_symbol = []
        try:
            total_ret = pf.total_return()
            if isinstance(total_ret, pd.Series):
                for sym, ret in total_ret.items():
                    if pd.notna(ret):
                        per_symbol.append({"symbol": sym, "total_return": float(ret)})
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            pass
        return stats_dict, equity_curve, trades, per_symbol

    def _run(self, config: BacktestConfig) -> BacktestResult:
        vbt = _get_vbt()
        run_id = uuid.uuid4().hex[:10]

        panel = self._load_panel(config.symbols, config.start, config.end, config.asset_type)
        if panel.empty:
            return self._error_result(run_id, config, "no data")

        # 价格面板
        close = panel.pivot(index="date", columns="symbol", values="close")

        signals = self._prepare_signals(panel, config, close)
        if signals is None:
            return self._error_result(run_id, config, "no buy signals")
        entries, exits, price = signals

        # 跑回测
        pf, error = self._run_portfolio(config, close, entries, exits, price, vbt)
        if pf is None:
            return self._error_result(run_id, config, error)

        stats_dict, equity_curve, trades, per_symbol = self._extract_results(pf)
        result = BacktestResult(
            run_id=run_id,
            config=_config_to_dict(config),
            stats={k: _json_safe(v) for k, v in stats_dict.items()},
            equity_curve=equity_curve,
            trades=trades,
            per_symbol_stats=per_symbol,
        )

        # 落盘
        self._persist(result)
        return result

    def _persist(self, result: BacktestResult) -> None:
        """落库：CK ``backtest_runs`` + ``backtest_trades``（唯一存储）。"""
        import json

        from app.storage.runtime import get_storage

        runtime = get_storage()
        if runtime is None:
            # 存储未初始化（离线脚本/单测）：不落盘，避免写出无人读取的 Parquet
            logger.debug("backtest 结果未落库: 存储层未初始化 run_id=%s", result.run_id)
            return
        store = runtime.store
        store.write_backtest_run(
            {
                "run_id": result.run_id,
                "created_at": datetime.now(),
                "strategy_id": str(result.config.get("strategy_id") or ""),
                "config_json": json.dumps(_json_safe(result.config), ensure_ascii=False),
                "stats_json": json.dumps(_json_safe(result.stats), ensure_ascii=False),
                "equity_curve": json.dumps(_json_safe(result.equity_curve), ensure_ascii=False),
                "extra_json": json.dumps(
                    _json_safe({"per_symbol_stats": result.per_symbol_stats}),
                    ensure_ascii=False,
                ),
                "n_trades": len(result.trades),
            }
        )
        trades = _trades_to_frame(result)
        if not trades.is_empty():
            store.write_backtest_trades(trades)

    def get_result(self, run_id: str) -> BacktestResult | None:
        """从 CK 读回完整结果；不存在返回 None。"""
        import json

        from app.storage.runtime import get_storage

        runtime = get_storage()
        if runtime is None:
            return None
        store = runtime.store
        run = store.read_backtest_run(run_id)
        if run is None:
            return None
        trades_df = store.read_backtest_trades(run_id)
        extra = json.loads(run.get("extra_json") or "{}")
        return BacktestResult(
            run_id=run["run_id"],
            config=json.loads(run.get("config_json") or "{}"),
            stats=json.loads(run.get("stats_json") or "{}"),
            equity_curve=json.loads(run.get("equity_curve") or "[]"),
            trades=trades_df.to_dicts() if not trades_df.is_empty() else [],
            per_symbol_stats=extra.get("per_symbol_stats", []),
        )


def _config_to_dict(c: BacktestConfig) -> dict:
    return {
        "symbols": c.symbols,
        "start": str(c.start),
        "end": str(c.end),
        "entries": c.entries,
        "exits": c.exits,
        "stop_loss_pct": c.stop_loss_pct,
        "max_hold_days": c.max_hold_days,
        "fees_pct": c.fees_pct,
        "slippage_bps": c.slippage_bps,
        "matching": c.matching,
    }


_TRADE_COLUMNS = (
    "symbol",
    "entry_signal_date",
    "entry_date",
    "entry_price",
    "exit_signal_date",
    "exit_date",
    "exit_price",
    "pnl_pct",
    "duration_days",
    "exit_reason",
)


def _as_date(value) -> date | None:
    """成交日期归一：date / datetime / ISO 字符串 / 空值。"""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _trades_to_frame(result: BacktestResult) -> pl.DataFrame:
    """trades(list[dict]) → CK ``backtest_trades`` 宽表（按显式 schema 归一类型）。"""
    if not result.trades:
        return pl.DataFrame()
    rows = []
    for seq, trade in enumerate(result.trades):
        row = {"run_id": result.run_id, "seq": seq}
        for name in _TRADE_COLUMNS:
            value = trade.get(name)
            if name.endswith("_date"):
                value = _as_date(value)
            elif name in ("entry_price", "exit_price", "pnl_pct"):
                value = None if value is None else float(value)
            elif name == "duration_days":
                value = None if value is None else int(value)
            elif name in ("symbol", "exit_reason"):
                value = None if value is None else str(value)
            row[name] = value
        rows.append(row)
    return pl.DataFrame(
        rows,
        schema={
            "run_id": pl.Utf8,
            "seq": pl.UInt32,
            "symbol": pl.Utf8,
            "entry_signal_date": pl.Date,
            "entry_date": pl.Date,
            "entry_price": pl.Float64,
            "exit_signal_date": pl.Date,
            "exit_date": pl.Date,
            "exit_price": pl.Float64,
            "pnl_pct": pl.Float64,
            "duration_days": pl.Int32,
            "exit_reason": pl.Utf8,
        },
    )


def _json_safe(v):
    # 非有限浮点 (inf / NaN) 必须先于原生标量分支拦下: Starlette 的 JSONResponse 用
    # json.dumps(allow_nan=False) 渲染, 漏一个就是整个响应 500。pf.stats() 经
    # pandas Series.to_dict() 出来时 numpy 标量已被装箱成原生 float (全胜时
    # Profit Factor = inf, 零波动时 Sharpe = NaN), 两条分支都要覆盖。
    if isinstance(v, float | np.floating) and not math.isfinite(float(v)):
        return None
    if isinstance(v, int | float | str | bool) or v is None:
        return v
    if isinstance(v, np.floating | np.integer):
        return float(v)
    if isinstance(v, dict):
        # 递归而非 str(): 嵌套结构 (per_symbol_stats/equity_curve 行) 需保持对象形态,
        # 否则 json.dumps 会把 Python repr 再包一层, 读回时变成字符串
        return {str(k): _json_safe(val) for k, val in v.items()}
    if isinstance(v, list | tuple):
        return [_json_safe(item) for item in v]
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)
