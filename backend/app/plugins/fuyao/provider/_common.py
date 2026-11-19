"""扶摇(同花顺金融数据 API)内置数据源 provider。

方法签名对齐 custom.GenericHTTPProvider(service 分流点按这套签名调用),
注入 custom loader 注册表后, 各 service 无需改动即可路由到本 provider。

实现数据集:
  - realtime     A 股全市场快照 (分页)
  - daily        A 股原始日K; 少量标的优先批量,全市场走 10d/10 年 dump,
                 dump 缺口由批量接口补齐,漏标的或请求失败回退单标的接口
  - adj_factor   A 股除权因子; adjustment-factors 事件 dump + 自家原始日K前收盘,
                 按交易所公式推导单事件比值, 涨跌停自检
  - financial    财务五表(股本除外): 三表多期序列 + 指标单期, 字段映射为 TickFlow
                 canonical 列名, 扶摇独有字段原名透传为扩展列; bps 由估值 pb_mrq
                 反推; shares 无上游接口恒空
未声明 minute → provider_has_dataset 为 False, 自动回退 tickflow。

单位与口径 (CONTRIBUTING §3.1, 不可凭字段名推断):
  - 扶摇 price_change_ratio_pct 为百分数数值 (1.74 = +1.74%), 本项目 realtime
    change_pct 契约为小数制 (0.0174 = 1.74%) → 此处显式 / 100。
  - 扶摇 volume 单位为股, 本项目日K/实时契约均为手 → 统一 floor(股/100)。
  - turnover 单位元, 与内部一致, 直接透传。
  - 日K取数 adjust=none 锁定: 官方 forward 序列事件间有逐日漂移(2026-08 实测),
    项目内前复权一律由 indicators.pipeline 用本地因子计算。
  - ex_factor 为单事件比值(非累积), 累积链由 pipeline._apply_adj_factor 构建。

拆包说明: 本模块持有原 provider.py 的全部模块级名字(常量/公开函数/工具函数)。
被测试 monkeypatch 的名字(get_api_key / fuyao_client / _cache_dir / _HIST_* 等)
在调用处通过 _fp(包对象)动态解析, 保证 patch 对分包后的各 mixin 同样生效。
"""

from __future__ import annotations

import calendar
import contextlib
import logging
import math
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from app.plugins.fuyao import client as fuyao_client  # noqa: F401 — 由包重导出, 测试 patch 目标
from app.plugins.fuyao.client import HISTORICAL_BATCH_LIMIT, FuyaoError

# 包对象自身: 所有可被 monkeypatch 的模块级名字都从它动态取, 避免分包后引用失效。
_fp = sys.modules[__package__]

# 日志名保持与原单文件模块一致, 不随拆包改变日志过滤行为。
logger = logging.getLogger("app.plugins.fuyao.provider")

# 只声明真实提供的数据集; 其余数据集 provider_has_dataset 返回 False → 回退 tickflow
_DATASETS = ("realtime", "daily", "adj_factor", "financial")

API_KEY_ENV = "FUYAO_API_KEY"
SECRETS_FIELD = "fuyao_api_key"  # UI 配置的 Key 存 secrets.json, 优先级高于 .env

# 扶摇 *ms 时间字段为北京时间零点(= UTC 前一日 16:00), +8h 后按 UTC 解析即得交易日
_SH_MS = 28_800_000
_HIST_MAX_SPAN_MS = 3650 * 86_400_000  # historical 单次窗口上限 10 年, 超出由本层分片
_HIST_INTERVAL_S = 0.12  # 单标的请求节流(实测 200+ 连发未触发 4001 限频)
_FINANCIAL_HISTORY_PERIODS = 8  # 财务首装全量历史: 最近 8 期季报(约 2 年)
_VALUATION_BATCH = 100  # 估值/价格快照端点单次上限 100 只
# 项目财务表名 → 扶摇报表端点名
_STATEMENT_ENDPOINTS = {
    "income": "income-statements",
    "balance_sheet": "balance-sheets",
    "cash_flow": "cash-flow-statements",
}
_ADJ_DUMP_KIND = "adjustment-factors"
_DAILY10_DUMP_KIND = "daily-k-10d"
_DAILY_DUMP_KIND = "daily-k"  # 10 年全量日K dump(约 172MB), 深窗口一次下载覆盖全市场
_RECENT_DUMP_DAYS = 12  # 窗口跨度 ≤ 此天数时优先走 10d dump(覆盖 ≈10 个交易日)
_PREV_CLOSE_BACKDAYS = 30  # 推导因子时向前找"除权日前收盘"的回看天数(容忍长期停牌)
_DAILY_DUMP_BATCH_ROWS = 100_000
_HIST_SYMBOL_BATCH = 50
_HIST_SMALL_SYMBOL_LIMIT = HISTORICAL_BATCH_LIMIT
_DAILY_DUMP_COLUMNS = [
    "thscode",
    "adjusted",
    "date_ms",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "turnover",
]


def get_api_key() -> str:
    from app import secrets_store

    return secrets_store.get_env_backed_secret(SECRETS_FIELD, API_KEY_ENV)


def availability() -> tuple[bool, str]:
    """loader 启动自检: API Key 已配置(secrets.json 或 .env)才注册为可切换数据源。不抛异常。"""
    if _fp.get_api_key():
        return True, "ok"
    # 状态行会拼在「未配置」标签之后, 文案不再重复"未配置"字样
    return False, f"缺少 API Key(可在下方输入框直接填写,或配置环境变量 {API_KEY_ENV})"


def probe_api_key(api_key: str) -> tuple[bool, str]:
    """用候选 Key 实探一次快照接口(先探后存, 对齐 /tickflow-key 语义)。不落盘。"""
    client = None
    try:
        client = _fp.fuyao_client.FuyaoClient(api_key=api_key, timeout=10.0)
        client.snapshot_page(limit=1)
        return True, "ok"
    except FuyaoError as e:
        return False, f"Key 无效或网络失败: {e}"
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()


@dataclass
class _FuyaoConfig:
    """轻量 config shim, 让 custom loader 的 provider_has_dataset 能识别本 provider。"""

    name: str = "fuyao"
    display_name: str = "fuyao"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first(row: dict, *names: str):
    """按优先级取第一个非 None 字段。实测字段名与官方文档示例不一致, 两者兼容。"""
    for n in names:
        if row.get(n) is not None:
            return row.get(n)
    return None


def _date_of_ms(value) -> date | None:
    """扶摇 *ms(上海零点) → 交易日。None/非法值返回 None, 不伪造。"""
    if value is None:
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp((ms + _SH_MS) // 1000, tz=UTC).date()


def _ms_of_date(d: date) -> int:
    """交易日 → 扶摇 start/end 入参口径的 ms(该日上海零点的 epoch ms, 不依赖本机时区)。"""
    return (calendar.timegm(d.timetuple()) - 28_800) * 1000


def _iso_of_ms(value) -> str | None:
    """扶摇 *ms → ISO 日期字符串(项目财务表 period_end/announce_date 的存储口径)。"""
    d = _date_of_ms(value)
    return d.isoformat() if d is not None else None


def _report_quarter(fiscal_period) -> int | None:
    """fiscal_period(Q1..Q4/FY) → 指标接口 report 参数的季号 N(1..4)。"""
    if not fiscal_period:
        return None
    text = str(fiscal_period).strip().upper()
    if text == "FY":
        return 4
    try:
        return int(text.lstrip("Q"))
    except ValueError:
        return None


def _ref_price(
    prev_close: float, dividend: float, bonus: float, allot: float, allot_price: float
) -> float | None:
    """交易所除权参考价: (P - D + AR·AP) / (1 + S + AR), 四舍五入(half-up)保留 2 位。

    half-up 是交易所口径; 银行家舍入会让约半数事件在第 2 位小数上偏离(对拍实证)。
    """
    denom = 1.0 + bonus + allot
    if denom <= 0:
        return None
    x = (prev_close - dividend + allot * allot_price) / denom
    return math.floor(x * 100 + 0.5) / 100


def _price_limit(symbol: str) -> float:
    """按代码前缀给涨跌停幅度(自检容差用): 创业板/科创板 20%, 北交所 30%, 主板 10%。"""
    code = symbol.split(".")[0]
    if code.startswith(("300", "301", "302", "688", "689")):
        return 0.20
    if code.startswith(("8", "4", "92")):
        return 0.30
    return 0.10


def _release_of(url: str) -> str:
    """从预签名 URL 提取 release 版本号(releases/<date>/ 路径), 提不到返回 unknown。"""
    m = re.search(r"releases/(\d+)/", url or "")
    return m.group(1) if m else "unknown"


def _cache_dir() -> Path:
    from app.config import settings

    d = settings.data_dir / "cache" / "fuyao"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _kline_rows(symbol: str, bars: list[dict]) -> list[dict]:
    """historical/dump 原始行(价格元, volume 股) → 内部行(volume 手)。"""
    out = []
    for b in bars:
        v = _to_float(b.get("volume"))
        out.append(
            {
                "symbol": symbol,
                "date": _date_of_ms(b.get("date_ms")),
                "open": _to_float(b.get("open_price")),
                "high": _to_float(b.get("high_price")),
                "low": _to_float(b.get("low_price")),
                "close": _to_float(b.get("close_price")),
                "volume": math.floor(v / 100.0) if v is not None else None,
                "amount": _to_float(b.get("turnover")),
            }
        )
    return out


def _tail_ok(end_d: date, covered_max: date) -> bool:
    """请求终点是否被覆盖到 covered_max: 周末/节假日的自然缺口(≤3 天)不算缺失。"""
    if end_d <= covered_max:
        return True
    return (end_d - covered_max).days <= 3 and end_d.weekday() >= 5


def _dump_covers(dump: pl.DataFrame, start_d: date, end_d: date) -> bool:
    """dump 日期范围是否覆盖请求窗口: 起点必须落在 dump 内; 终点允许周末自然缺口。"""
    if dump.is_empty() or "date_ms" not in dump.columns:
        return False
    dates = pl.from_epoch(dump["date_ms"].cast(pl.Int64) + _SH_MS, time_unit="ms").dt.date()
    dmin, dmax = dates.min(), dates.max()
    return start_d >= dmin and _tail_ok(end_d, dmax)


def _dump_date_range(path: Path) -> tuple[date | None, date | None]:
    """lazy 读 parquet 的 date_ms 边界(走元数据/少量行组, 不整读大文件)。"""
    row = (
        pl.scan_parquet(path)
        .select(
            pl.from_epoch(pl.col("date_ms").min() + _SH_MS, time_unit="ms").dt.date().alias("dmin"),
            pl.from_epoch(pl.col("date_ms").max() + _SH_MS, time_unit="ms").dt.date().alias("dmax"),
        )
        .collect()
    )
    return row["dmin"][0], row["dmax"][0]


def _map_snapshot_row(row: dict, fetched_ms: int, *, volume_to_hand: bool = True) -> dict | None:
    """扶摇快照行 → 内部 realtime record。字段缺失时按依赖推导, 不伪造数据。

    实测字段(2026-08): high_price / low_price / prev_price;
    官方文档示例: highest_price / lowest_price / prev_close_price。两者都取。
    volume_to_hand: A 股快照 volume 为股 → 手; 指数快照无此口径, 直接透传。
    """
    symbol = row.get("thscode")
    if not symbol:
        return None
    last = _to_float(row.get("last_price"))
    prev = _to_float(_first(row, "prev_price", "prev_close_price"))

    # 百分数 (1.74 = +1.74%) → 小数制 (0.0174), 契约见模块 docstring
    pct = _to_float(row.get("price_change_ratio_pct"))
    change_pct = pct / 100.0 if pct is not None else None

    change_amount = _to_float(row.get("price_change"))
    if change_amount is None and last is not None and prev is not None:
        change_amount = last - prev
    if change_pct is None and change_amount is not None and prev not in (None, 0):
        # 与 quote_service 的推导同口径: 小数制, 不乘 100
        change_pct = change_amount / prev

    volume = _to_float(row.get("volume"))

    return {
        "symbol": symbol,
        "name": row.get("name"),  # 快照无名称, 由下游维表关联
        "last_price": last,
        "prev_close": prev,
        "open": _to_float(row.get("open_price")),
        "high": _to_float(_first(row, "high_price", "highest_price")),
        "low": _to_float(_first(row, "low_price", "lowest_price")),
        "volume": math.floor(volume / 100.0) if (volume is not None and volume_to_hand) else volume,
        "amount": _to_float(row.get("turnover")),
        "change_pct": change_pct,
        "change_amount": change_amount,
        "amplitude": None,  # 快照未提供, 不启发式计算
        "turnover_rate": None,  # 需股本口径 (§3.4), 交给 enriched 管道用历史股本计算
        "timestamp": fetched_ms,
        "session": None,
    }
