"""enriched 表计算流水线(§7.5 / §7.7 Step 2)。

存储层 (enriched parquet):
  仅存储基础行情窄表 (14 列), 指标和信号由各服务即时计算。

  存储列: symbol, date, OHLCV(前复权), volume, amount,
          raw_close, raw_high, raw_low, turnover_rate,
          consecutive_limit_ups, consecutive_limit_downs

设计:
  - 100% Polars 表达式(SQL 窗口无法表达递归 EMA)
  - 每只标的独立计算(`.over("symbol")`)
  - 有 adj_factor 时先应用前复权再算指标;无因子时直接用 raw
  - streaming collect 控制内存
"""
# ruff: noqa: F401  # 根导入供子模块经 _root.<name> 延迟引用（保持 monkeypatch 可观察性）

from __future__ import annotations

import logging
import shutil
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import date
from datetime import timedelta as _timedelta
from pathlib import Path

import polars as pl

from app.config import settings
from app.enriched_generation import (
    CKEnrichedPublication,
    EnrichedPublication,
    enriched_publication_incomplete,
)
from app.market_time import cn_today
from app.parquet import scan_enriched_parquet  # 仅用于全量重建的本地暂存文件
from app.polars_guard import guarded_collect
from app.price_limits import (
    no_limit_window_days,
    no_limit_window_margin_days,
    parse_listing_date,
    polars_is_risk_warning_name,
    polars_limit_price,
    polars_price_limit_pct,
)
from app.share_capital import apply_historical_float_shares, load_share_history

from .pipeline_enriched import (
    _as_date,
    _bench_key_expr,
    _bench_rt_pct_of,
    _pick_benchmark_pairs,
    _select_storage_cols,
    _upsert_enriched_day,
    attach_deviation_columns,
    attach_deviation_columns_today,
    bench_rt_pct_for,
    benchmark_momentum_today,
    compute_enriched,
    filter_halt_days,
    load_benchmark_momentum,
)

# ── 子模块 re-export（保持既有 import 路径不变）──
from .pipeline_indicators_core import (
    _apply_adj_factor,
    _ci_ema_exprs,
    _ci_ma_exprs,
    _ci_p1_rest_exprs,
    _ci_pass1,
    _ci_pass2,
    _ci_pass3,
    _ci_pass4,
    _ci_pass5,
    _ema_alpha,
    _math_half_up,
    _resolve_needed,
    compute_indicators,
)
from .pipeline_rebuild import (
    _adaptive_sym_batch,
    _compute_storage_batches,
    _sweep_stale_staging,
    _total_ram_bytes,
    _validate_repair_batch,
    compute_enriched_history_window,
    recompute_daily_symbols,
    run_pipeline,
)
from .pipeline_recompute import (
    _daily_read_columns,
    _load_factors,
    _load_recent_history,
    _rpi_full,
    _rpi_full_batch,
    _rpi_full_batch_write,
    _rpi_full_merge,
    _rpi_full_prepare,
    _rpi_incremental,
    _rpi_incremental_new_dates,
    _rpi_incremental_symbols,
    _rpi_load_inputs,
    _run_pipeline_impl,
    compute_enriched_single,
)
from .pipeline_signals import (
    _attach_no_limit_window,
    _limit_cleanup,
    _limit_consecutive,
    _limit_prepare_instruments,
    _limit_price_effective,
    _limit_price_foundations,
    _limit_turnover,
    _needed_references_limit_prices,
    _split_custom_exprs_by_limit_deps,
    compute_all,
    compute_limit_signals,
    compute_signals,
    get_signal_dependencies,
)
from .pipeline_today import (
    _cet_cleanup,
    _cet_finalize,
    _cet_oscillators,
    _cet_prepare,
    _cet_prev_fields,
    _cet_trend,
    _cet_volume_momentum,
    _cet_window_gate,
    _clst_basis,
    _clst_cleanup,
    _clst_consecutive,
    _clst_effective_prices,
    _clst_prepare_instruments,
    _clst_turnover,
    _compute_limit_signals_today,
    compute_enriched_today,
)

logger = logging.getLogger(__name__)


def _daily_store():
    """enriched 生成的数据源：ClickHouse（设计文档 §8.4）。

    读路径切 CK 后不再有 Parquet 回退：存储层未初始化直接报错，避免"静默读到空
    数据、算出一份空 enriched"这种最难排查的降级。
    """
    from app.storage.runtime import require_storage

    return require_storage().store


# ── 自定义信号缓存 ─────────────────────────────────────
# 从 data/user_data/custom_signals/*.json 加载并编译为 Polars 表达式。
# 两套表达式分别用于全量路径 (allow_shift=True, 支持日期偏移条件)
# 和盘中增量热路径 (allow_shift=False, 跳过偏移条件)。
# 模块级缓存：首次调用时加载，invalidate_custom_signals() 后下次重载。
# 增量路径每秒级执行, 若不缓存则每轮 glob + 读所有 JSON + 重编译表达式。
_custom_signal_exprs: dict[str, pl.Expr] | None = None
_custom_signal_exprs_today: dict[str, pl.Expr] | None = None
_custom_prev_fields_cache: set[str] | None = None


def _get_custom_signal_exprs() -> dict[str, pl.Expr]:
    """懒加载自定义信号表达式（带模块级缓存，allow_shift=True）。"""
    global _custom_signal_exprs
    if _custom_signal_exprs is None:
        from app.strategy import custom_signals

        try:
            sigs = custom_signals.load_all(settings.data_dir)
            _custom_signal_exprs = custom_signals.build_expressions(sigs)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("custom signals load failed: %s", e)
            _custom_signal_exprs = {}
    return _custom_signal_exprs


def _get_custom_signal_exprs_today() -> dict[str, pl.Expr]:
    """当日(实时)路径的自定义信号表达式 (模块级缓存)。

    与全量版的差异: 偏移=1 的字段改读 ``_prev_{field}`` 昨日值 join 列
    (compute_enriched_today 预先 join, 字段集见 _custom_prev_fields_today);
    偏移 >1 的信号无法在单日快照求值, 跳过并告警。
    """
    global _custom_signal_exprs_today, _custom_prev_fields_cache
    if _custom_signal_exprs_today is None or _custom_prev_fields_cache is None:
        from app.strategy import custom_signals

        try:
            sigs = custom_signals.load_all(settings.data_dir)
            prev_fields = custom_signals.prev_day_fields(sigs)
            _custom_prev_fields_cache = prev_fields
            _custom_signal_exprs_today = custom_signals.build_expressions_prev(sigs, prev_fields)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("custom signals load failed (today): %s", e)
            _custom_signal_exprs_today = {}
            _custom_prev_fields_cache = set()
    return _custom_signal_exprs_today


def _custom_prev_fields_today() -> set[str]:
    """当日路径需要 join 昨日值的字段集合 (与表达式缓存同源同失效)。"""
    _get_custom_signal_exprs_today()
    return _custom_prev_fields_cache or set()


def invalidate_custom_signals() -> None:
    """失效自定义信号缓存（保存/删除信号后调用，下次计算重新加载）。"""
    global _custom_signal_exprs, _custom_signal_exprs_today, _custom_prev_fields_cache
    _custom_signal_exprs = None
    _custom_signal_exprs_today = None
    _custom_prev_fields_cache = None


# enriched parquet 仅存储的列 (14 列)
ENRICHED_STORAGE_COLS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",  # 前复权
    "volume",
    "amount",
    "raw_close",
    "raw_high",
    "raw_low",  # 不复权原始价
    "turnover_rate",  # 依赖当时的 float_shares, 不可回推
    "consecutive_limit_ups",  # 递推状态, 需从历史 cum_sum
    "consecutive_limit_downs",
    "quote_ts",  # 行情时间戳(ms): 盘后校验/量比折算/跨天完整性
]

# 长任务等待实时发布让出的上限。实时行情每轮持有发布几毫秒到几百毫秒,
# 60s 足以跨过任意一轮; 超过则说明确有其他长任务在跑, 继续等没有意义。
_PUBLICATION_CLAIM_WAIT_S = 60.0


# ================================================================
# enriched 完整列清单 (存储 + 运行时计算)
# 供 AI 审查代码时参考: 策略/筛选/回测 可直接使用以下列名。
# 分类: 存储列 → 指标列 → 信号列 → JOIN 列
# ================================================================
ENRICHED_COLUMNS: dict[str, str] = {
    # ── 存储列 (parquet 持久化) ──────────────────────────
    "symbol": "股票代码",
    "date": "交易日期",
    "open": "前复权开盘价",
    "high": "前复权最高价",
    "low": "前复权最低价",
    "close": "前复权收盘价",
    "volume": "成交量",
    "amount": "成交额",
    "raw_close": "原始收盘价(未复权)",
    "raw_high": "原始最高价(未复权)",
    "raw_low": "原始最低价(未复权)",
    "turnover_rate": "换手率",
    "consecutive_limit_ups": "连板数",
    "consecutive_limit_downs": "连跌数",
    # ── 基础指标 ─────────────────────────────────────────
    "prev_close": "前收盘价",
    "change_pct": "日涨跌幅(小数, 如 0.05 = 5%)",
    "change_amount": "日涨跌额",
    "amplitude": "日振幅 (最高-最低)/昨收",
    # ── 均线 MA ──────────────────────────────────────────
    "ma5": "5日简单均线",
    "ma10": "10日简单均线",
    "ma20": "20日简单均线",
    "ma30": "30日简单均线",
    "ma60": "60日简单均线(季线)",
    # ── 指数均线 EMA ─────────────────────────────────────
    "ema5": "5日指数均线",
    "ema10": "10日指数均线",
    "ema20": "20日指数均线",
    "ema30": "30日指数均线",
    "ema60": "60日指数均线",
    # ── MACD ─────────────────────────────────────────────
    "macd_dif": "MACD DIF线(快线-慢线)",
    "macd_dea": "MACD DEA线(信号线)",
    "macd_hist": "MACD柱状图 (DIF-DEA)×2",
    # ── 布林带 BOLL ──────────────────────────────────────
    "boll_upper": "布林带上轨 MA20+2σ",
    "boll_lower": "布林带下轨 MA20-2σ",
    # ── KDJ ──────────────────────────────────────────────
    "kdj_k": "KDJ K值",
    "kdj_d": "KDJ D值",
    "kdj_j": "KDJ J值 (3K-2D)",
    # ── ATR ──────────────────────────────────────────────
    "atr_14": "14日平均真实波幅",
    # ── 量价 ─────────────────────────────────────────────
    "vol_ma5": "5日成交均量",
    "vol_ma10": "10日成交均量",
    "vol_ratio_5d": "量比 (成交量/5日均量)",
    # ── 极值 ─────────────────────────────────────────────
    "high_60d": "60日最高价",
    "low_60d": "60日最低价",
    # ── 动量 ─────────────────────────────────────────────
    "momentum_5d": "5日动量(涨跌幅小数)",
    "momentum_10d": "10日动量",
    "momentum_20d": "20日动量",
    "momentum_30d": "30日动量",
    "momentum_60d": "60日动量",
    # ── 异动偏离 (运行时由 repository 附着, 不落盘) ────────
    "deviate_3d": "3日涨跌幅偏离值(vs对应指数, 小数)",
    "deviate_10d": "10日涨跌幅偏离值",
    "deviate_30d": "30日涨跌幅偏离值",
    # ── 波动率 ───────────────────────────────────────────
    "annual_vol_20d": "20日年化波动率",
    # ── RSI ──────────────────────────────────────────────
    "rsi_6": "6日相对强弱指标",
    "rsi_14": "14日相对强弱指标",
    "rsi_24": "24日相对强弱指标",
    # ── 涨跌停判定价 (bool 信号列已迁移为自定义信号定义, 见 user_data/custom_signals) ──
    "limit_up_price": "涨停判定价 (生效价-0.005容差; raw_close>=即涨停)",
    "limit_down_price": "跌停判定价 (生效价+0.005容差; raw_close<=即跌停)",
    # ── JOIN 列 (由 repository 从 instruments 表补充) ───
    "name": "股票名称 (来自 instruments)",
    "total_shares": "总股本 (来自 instruments)",
    "float_shares": "流通股本 (来自 instruments)",
}

# 仅供 AI/开发者快速索引: 按类别的列名列表
ENRICHED_COLUMNS_BY_CATEGORY: dict[str, list[str]] = {
    "storage": [k for k in ENRICHED_COLUMNS if k in ENRICHED_STORAGE_COLS],
    "basic": ["prev_close", "change_pct", "change_amount", "amplitude"],
    "ma": ["ma5", "ma10", "ma20", "ma30", "ma60"],
    "ema": ["ema5", "ema10", "ema20", "ema30", "ema60"],
    "macd": ["macd_dif", "macd_dea", "macd_hist"],
    "boll": ["boll_upper", "boll_lower"],
    "kdj": ["kdj_k", "kdj_d", "kdj_j"],
    "atr": ["atr_14"],
    "volume": ["vol_ma5", "vol_ma10", "vol_ratio_5d"],
    "extremes": ["high_60d", "low_60d"],
    "momentum": ["momentum_5d", "momentum_10d", "momentum_20d", "momentum_30d", "momentum_60d"],
    "deviation": ["deviate_3d", "deviate_10d", "deviate_30d"],
    "volatility": ["annual_vol_20d"],
    "rsi": ["rsi_6", "rsi_14", "rsi_24"],
    "signals": [k for k in ENRICHED_COLUMNS if k.startswith("signal_")],
    "join": ["name", "total_shares", "float_shares"],
}


# ================================================================
# 技术指标计算 (从 OHLCV 计算)
# ================================================================

# ── compute_indicators 的列依赖关系 (供 needed 裁剪时求闭包) ────────────
# target -> 其计算所依赖的中间/指标列 (仅列出依赖非原始 OHLCV 的项)
_INDICATOR_DEPS: dict[str, set[str]] = {
    "macd_dif": {"_ema12", "_ema26"},
    "boll_upper": {"ma20", "_boll_std"},
    "boll_lower": {"ma20", "_boll_std"},
    "macd_dea": {"macd_dif"},
    "macd_hist": {"macd_dif", "macd_dea"},
    "kdj_k": {"_kdj_ln", "_kdj_hn"},
    "kdj_d": {"kdj_k"},
    "kdj_j": {"kdj_k", "kdj_d"},
    "atr_14": {"_tr"},
    "vol_ratio_5d": {"_vol_ma5"},
    "annual_vol_20d": {"_daily_pct"},
    "rsi_6": {"_delta", "_gain", "_loss"},
    "rsi_14": {"_delta", "_gain", "_loss"},
    "rsi_24": {"_delta", "_gain", "_loss"},
}

# compute_indicators 可产出的全部指标/临时列 (needed=None 时即为此全集, 行为不变)
_ALL_INDICATOR_COLS: frozenset[str] = frozenset(
    {
        "prev_close",
        "ma5",
        "ma10",
        "ma20",
        "ma30",
        "ma60",
        "ema5",
        "ema10",
        "ema20",
        "ema30",
        "ema60",
        "_ema12",
        "_ema26",
        "_boll_std",
        "_kdj_ln",
        "_kdj_hn",
        "_tr",
        "vol_ma5",
        "vol_ma10",
        "_vol_ma5",
        "high_60d",
        "low_60d",
        "macd_dif",
        "boll_upper",
        "boll_lower",
        "macd_dea",
        "macd_hist",
        "kdj_k",
        "kdj_d",
        "kdj_j",
        "atr_14",
        "vol_ratio_5d",
        "momentum_5d",
        "momentum_10d",
        "momentum_20d",
        "momentum_30d",
        "momentum_60d",
        "change_pct",
        "change_amount",
        "amplitude",
        "_daily_pct",
        "annual_vol_20d",
        "rsi_6",
        "rsi_14",
        "rsi_24",
    }
)


# 内置信号表达式已全部迁移为 data/user_data/custom_signals/*.json 定义
# (列名沿用 signal_* 前缀, 由 custom_signals 注入管线计算); 本模块不再
# 硬编码任何信号布尔表达式 —— 依赖关系见 get_signal_dependencies()。

# compute_limit_signals 的可请求输出 (涨跌停判定价 + 连板状态 + 换手率)。
# 涨跌停判定价 = 交易所生效价 ∓0.005 容差 (raw_close >= limit_up_price 即涨停),
# 供迁移后的 signal_limit_* 自定义信号条件引用。
LIMIT_SIGNAL_OUTPUTS: frozenset[str] = frozenset(
    {
        "limit_up_price",
        "limit_down_price",
        "consecutive_limit_ups",
        "consecutive_limit_downs",
        "turnover_rate",
    }
)

_LIMIT_PRICE_COLUMNS = frozenset({"limit_up_price", "limit_down_price"})

INDICATOR_COLUMNS: frozenset[str] = frozenset(
    col for col in _ALL_INDICATOR_COLS if not col.startswith("_")
)


_ENRICHED_TABLE = "kline_daily_enriched"


# ================================================================
# 异动偏离列 (deviate_3d/10d/30d)
#
# N 日涨跌幅偏离值 = 个股 N 日累计涨跌幅 - 对应指数同期涨跌幅,
# 是交易所「异常波动 / 严重异常波动」规则的量化口径 (如主板 3日±20%,
# 10日+100%, 30日+200%)。不属于 compute_indicators 的纯函数范围
# (需要指数数据), 因此在 repository 读取路径上附着, 不随 parquet 落盘。
# ================================================================

DEVIATION_WINDOWS: tuple[int, ...] = (3, 10, 30)

# 各板块基准指数 (偏离值规则的「对应指数」, 按交易所官方口径): 优先首选, 缺失时回退
# - 沪主板:   上证A指 → 上证指数 (两者差异可忽略)
# - 科创板:   科创50 (上交所《交易规则》2026修订 6.12 指定基准) → 上证A指
# - 深主板:   深证A指 → 深证成指 (深交所投教口径)
# - 创业板:   创业板综合指数 → 深证A指 (深交所投教口径)
# - 北交所:   北证50 → 上证指数 (北交所《交易规则》5.4.4)
_BENCHMARK_PREFERENCE: dict[str, list[str]] = {
    "SH": ["000002.SH", "000001.SH"],
    "STAR": ["000688.SH", "000002.SH"],
    "SZ": ["399107.SZ", "399001.SZ"],
    "GEM": ["399102.SZ", "399107.SZ"],
    "BJ": ["899050.BJ", "000001.SH"],
}

# 偏离值计算需要的全部基准指数 (quote_service 并入实时显式拉取, 不依赖监控规则)
BENCHMARK_INDEX_SYMBOLS: frozenset[str] = frozenset(
    sym for cands in _BENCHMARK_PREFERENCE.values() for sym in cands
)

# 全部板块基准键 (SH/STAR/SZ/GEM/BJ)
BENCH_KEYS: tuple[str, ...] = tuple(_BENCHMARK_PREFERENCE)

_benchmark_cache: dict[str, tuple[float, pl.DataFrame | None]] = {}
_BENCHMARK_CACHE_TTL = 600.0


# ================================================================
# 全量重建流式暂存 + 自适应批次 (#208/#174)
#
# 旧全量模式把所有批次结果累积在内存 date_buffers 直到统一写盘:
# 延长历史后 (5年 × 5500 只 ≈ 800 万行) 「全表驻留 + 单批宽表」双双
# 超出小内存机器上限, 重建必然 OOM。现改为:
#   - 每批结果立即写暂存文件 (enriched 树外的隐藏目录 —— polars/duckdb
#     的 **/*.parquet glob 均会匹配点目录, 树内暂存会被业务读取扫到),
#     最后按日期分块流式合并、逐分区原子替换;
#   - 批次大小按单批目标行数自适应收缩 (指标/信号全部 over("symbol")
#     分组, symbol 级分批不改变计算结果, 只约束单批宽表峰值)。
# 任一时刻峰值内存 = 单批计算 + 单个日期块合并, 与总历史长度无关。
# ================================================================

_STAGING_ROOT = Path(".staging") / "enriched_rebuild"
_STALE_STAGING_MAX_AGE_S = 24 * 3600
_RAM_LARGE_BYTES = 8 * 1024**3  # ≥8GB 视为内存充裕, 批次保持用户设置
_BATCH_TARGET_ROWS = 150_000  # 小内存单批目标行数 (宽表 ~60-80MB)
_BATCH_MIN_SYMBOLS = 50
_MERGE_DATE_CHUNKS = 15  # 最终合并按日期切 15 块流式执行
