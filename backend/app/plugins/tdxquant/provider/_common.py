"""TdxQuant(通达信量化数据服务)内置数据源 provider。

方法签名对齐 custom.GenericHTTPProvider(service 分流点按这套签名调用),
插件被 loader 扫描注册后, 各 service 无需改动即可路由。

实现数据集:
  - realtime    A 股全市场快照(五市场股票池, 600 只/批)
  - depth5      五档盘口(快照 Buyp/Buyv/Sellp/Sellv)
  - daily       A 股日K, 原始价(dividend=none), count 模式单标的拉取
  - adj_factor  除权因子: 事件接口 + 本地K线配价, 交易所公式推导, 涨跌停自检
  - minute      分钟K(period=1m count 模式, 单标的 240 根/日, 原始价)
  - full_minute 全量分钟: 修复轮 get_intraday_batch(当日窗口) +
                稳态增量 get_intraday_latest(每股最新 count 根)。
未声明 financial → provider_has_dataset 为 False, 回退 tickflow。

单位与口径 (CONTRIBUTING §3, 不可凭字段名推断; 2026-10 实测):
  - 快照 Volume 已是手, 直接透传; Amount 为万元 → ×10000 转元。
  - 快照无涨跌幅字段 → change_pct = (Now-LastClose)/LastClose, 小数制。
  - 日K Volume 为股 → floor(/100) 转手; Amount 为万元 → ×10000 转元。
  - 分钟K与日K同源同口径 (实测茅台全天 240 根 sum(Volume)=3511100 ≈ 日K
    3511051 股): Volume 股 → floor(/100) 转手; Amount 万元 → ×10000 转元;
    datetime = Date+Time 拼北京墙钟 naive (契约 §分钟K)。Time 列实测可为
    int/float 裸值或缺前导零 (2026-10 #509 整批 0 行事故), 按 int 归一后再拼;
    原生返回可能整体包在 "Value" 键内 (SDK get_market_data 有同款防御)。
  - 日K取数 dividend="none" 锁定原始价, 前复权由 indicators.pipeline 用本地因子计算。
  - 除权事件 Bonus/ShareBonus/Allotment 均为每 10 股口径 → ÷10 转每股再进公式。
  - ex_factor 为单事件比值(非累积), 累积链由 pipeline._apply_adj_factor 构建。

拆包说明: 本模块持有原 provider.py 的全部模块级名字(常量/公开函数/工具函数)。
被测试 monkeypatch 的名字(datetime / get_api_key / _module_available /
_MINUTE_INTERVAL_S / _UNPARSED_MINUTE_SAMPLES)在调用处通过 _fp(包对象)动态解析,
保证 patch 对分包后的各 mixin 同样生效。
"""

from __future__ import annotations

import logging
import math
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

# 包对象自身: 所有可被 monkeypatch 的模块级名字都从它动态取, 避免分包后引用失效。
_fp: Any = sys.modules[__package__]

# 日志名保持与原单文件模块一致(测试 caplog 按此名字过滤)。
logger = logging.getLogger("app.plugins.tdxquant.provider")

# 只声明真实提供的数据集; 其余数据集 provider_has_dataset 返回 False → 回退 tickflow
_DATASETS = ("realtime", "depth5", "daily", "adj_factor", "minute", "full_minute")

API_KEY_ENV = "TDXQUANT_API_KEY"
SECRETS_FIELD = "tdxquant_api_key"  # UI 配置的 Key 存 secrets.json, 优先级高于 .env

_SNAPSHOT_BATCH = 600  # 实测 600 只/批正常返回
_HIST_INTERVAL_S = 0.08  # 单标的 K 线/事件请求节流
_HIST_MAX_COUNT = 2600  # count 模式单次上限(≈10.6 年日K, 项目深窗口即 10 年)
_ADJ_BACKDAYS = 30  # 推导因子时向前找"除权日前收盘"的回看天数(容忍长期停牌)
_ADJ_FUTURE_DAYS = 15  # 未来已公告事件窗口: 无前收盘自然跳过, 滚动增量轮补上
_BEIJING = ZoneInfo("Asia/Shanghai")

# 内部指数代码 → TDX 快照代码(实测 2026-10: 上证指数走 TDX 999999 别名)
_INDEX_CODE_MAP = {"000001.SH": "999999.SH"}

_DAILY_SCHEMA = {
    "symbol": pl.String,
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
}
_ADJ_SCHEMA = {
    "symbol": pl.String,
    "trade_date": pl.Date,
    "ex_factor": pl.Float64,
    "dividend": pl.Float64,
    "bonus": pl.Float64,
    "allot": pl.Float64,
    "allot_price": pl.Float64,
    "prev_close": pl.Float64,
}
_MINUTE_SCHEMA = {
    "symbol": pl.String,
    "datetime": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "amount": pl.Float64,
}
_MINUTE_BARS_PER_DAY = 240  # A 股 1m 一交易日根数(含收盘集合竞价分钟)
_MINUTE_MAX_COUNT = 9600  # 实测单次 count=9600(40 交易日)正常返回
_MINUTE_INTERVAL_S = 0.08  # 单标的分钟请求节流(与日K同档)
_MINUTE_OUTPUT_ROWS = 100_000
_MINUTE_OUTPUT_SYMBOLS = 50  # 输出合并控制写放大,上游仍按自己的单票接口取数


def get_api_key() -> str:
    from app import secrets_store

    return secrets_store.get_env_backed_secret(SECRETS_FIELD, API_KEY_ENV)


def _module_available() -> bool:
    try:
        import importlib.util

        return importlib.util.find_spec("tdxaidata") is not None
    except Exception:  # noqa: BLE001 — find_spec 对损坏安装也会抛
        return False


def availability() -> tuple[bool, str]:
    """loader 启动自检: tdxaidata 已安装 + Key 已配置才注册。不抛异常。"""
    if not _fp._module_available():
        return False, "tdxaidata 未安装(默认依赖, 请检查 uv sync)"
    if not _fp.get_api_key():
        return False, f"缺少 API Key(可在下方输入框直接填写,或配置环境变量 {API_KEY_ENV})"
    return True, "ok"


def probe_api_key(api_key: str) -> tuple[bool, str]:
    """候选 Key 探测: 格式校验 + 子进程连通性实探。

    TDX 会话按 IP 复用(同 IP 活跃好 Key 会话时错误 Key 也能拉到日K, 实测),
    "无效 Key 拒绝"不可靠 — 格式明显不对或服务端明确拒绝时不保存;
    通过连通性不代表所有接口权限均可用, 卡片可一键试拉复核。
    """
    key = (api_key or "").strip()
    if not key.startswith("TDX-") or len(key) < 12:
        return False, "Key 格式应为 TDX- 开头(通达信商城创建的数据服务 Key)"
    from app.plugins.tdxquant import probe

    return probe.run(key)


@dataclass
class _TdxQuantConfig:
    """轻量 config shim, 让 custom loader 的 provider_has_dataset 能识别本 provider。"""

    name: str = "tdx"
    display_name: str = "TdxQuant"
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


def _compact_date(value) -> date | None:
    """原生 Date 'YYYYMMDD'(或 DataFrame 归一后的 ISO 'YYYY-MM-DD…')→ date。

    非法值返回 None, 不伪造。
    """
    try:
        text = str(value).strip()
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            text = text[:10].replace("-", "")
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except (TypeError, ValueError):
        return None


def _snapshot_ms(refresh_time, fallback_ms: int) -> int:
    """RefreshTime 'HHMMSS'(服务端北京墙钟) + 今日北京日期 → epoch ms。

    解析失败退回本地时间; A 股时段无跨日歧义。
    """
    try:
        text = str(refresh_time).strip().zfill(6)
        hh, mm, ss = int(text[:2]), int(text[2:4]), int(text[4:6])
        if hh > 23 or mm > 59 or ss > 59:
            raise ValueError(text)
        now_bj = _fp.datetime.now(_BEIJING)
        stamp = datetime(now_bj.year, now_bj.month, now_bj.day, hh, mm, ss, tzinfo=_BEIJING)
        return int(stamp.timestamp() * 1000)
    except (TypeError, ValueError):
        return fallback_ms


def _map_snapshot_row(symbol: str, row: dict, fallback_ms: int) -> dict | None:
    """TDX 快照行 → 内部 realtime record。字段缺失按口径推导, 不伪造数据。"""
    last = _to_float(row.get("Now"))
    prev = _to_float(row.get("LastClose"))
    if last is None and prev is None:
        return None

    change_amount = last - prev if (last is not None and prev is not None) else None
    # 与 quote_service 推导同口径: 小数制, 不乘 100
    change_pct = change_amount / prev if (change_amount is not None and prev) else None

    volume = _to_float(row.get("Volume"))  # 快照已为手, 零换算
    amount = _to_float(row.get("Amount"))  # 万元 → 元

    return {
        "symbol": symbol,
        "name": None,  # 快照无名称, 由下游维表关联
        "last_price": last,
        "prev_close": prev,
        "open": _to_float(row.get("Open")),
        "high": _to_float(row.get("Max")),
        "low": _to_float(row.get("Min")),
        "volume": math.floor(volume) if volume is not None else None,
        "amount": amount * 10_000.0 if amount is not None else None,
        "change_pct": change_pct,
        "change_amount": change_amount,
        "amplitude": None,  # 快照未提供, 不启发式计算
        "turnover_rate": None,  # 需股本口径 (§3.4), 交给 enriched 管道
        "timestamp": _snapshot_ms(row.get("RefreshTime"), fallback_ms),
        "session": None,
    }


def _ref_price(
    prev_close: float, dividend: float, bonus: float, allot: float, allot_price: float
) -> float | None:
    """交易所除权参考价: (P - D + AR·AP) / (1 + S + AR), 四舍五入(half-up)保留 2 位。"""
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


def _unwrap_value(raw: dict) -> dict:
    """部分原生返回把数据整体包在 "Value" 键里 (SDK get_market_data 有同款防御),
    解包后才是 Date/Open/… 列式结构; 无包装原样返回。"""
    if isinstance(raw, dict):
        inner = raw.get("Value")
        if isinstance(inner, dict):
            return inner
    return raw


def _kline_rows(symbol: str, raw: dict, start_d: date | None, end_d: date) -> list[dict]:
    """原生 count 模式 K 线 dict → 内部行(窗口过滤, volume 股→手, amount 万元→元)。"""
    raw = _unwrap_value(raw)

    def _col(name: str, i: int) -> float | None:
        values = raw.get(name)
        return _to_float(values[i]) if values and i < len(values) else None

    dates = raw.get("Date") or []
    out: list[dict] = []
    for i, compact in enumerate(dates):
        d = _compact_date(compact)
        if d is None or d > end_d:
            continue
        if start_d is not None and d < start_d:
            continue
        volume = _col("Volume", i)
        amount = _col("Amount", i)
        out.append(
            {
                "symbol": symbol,
                "date": d,
                "open": _col("Open", i),
                "high": _col("High", i),
                "low": _col("Low", i),
                "close": _col("Close", i),
                "volume": math.floor(volume / 100.0) if volume is not None else None,
                "amount": amount * 10_000.0 if amount is not None else None,
            }
        )
    return out


def _bars_for_window(span_days: int) -> int:
    """窗口自然日跨度 → count 模式请求根数(交易日≈自然日×5/7, 加停牌冗余)。"""
    return max(30, min(_HIST_MAX_COUNT, int(span_days * 5 / 7) + 20))


def _hhmmss_digits(value) -> str | None:
    """Time 列值 → 6 位 HHMMSS 数字串; 无法归一返回 None。

    原生层实测混出 int/float 裸值 (93100) 与缺前导零的 5 位串 (#509), SDK
    自身按 int 归一 (get_market_data 的 "%06d" 拼法); 6 位取前 6、5 位
    左补零; 4 位及以下按 HHMM 补 "00" — 分钟K 9:30-15:00 时段内 HHMM 是
    唯一能落到交易时刻的读法。全零视为无时间 (日K行混入)。
    """
    if value is None:
        return None
    if isinstance(value, float):
        value = int(value)  # 93100.0 → 93100
    text = str(value).strip()
    if not text or not text.isdigit():
        return None
    if len(text) > 6:
        text = text[:6]
    elif len(text) == 5:
        text = text.zfill(6)
    elif len(text) < 6:
        text = text.zfill(4) + "00"  # HHMM → HHMMSS
    return None if text == "000000" else text


def _minute_dt(compact_date, hhmmss) -> datetime | None:
    """Date 'YYYYMMDD' + Time (数值或 'HHMMSS' 串) → 北京墙钟 naive; 非法返回 None。"""
    d = _compact_date(compact_date)
    if d is None:
        return None
    t = _hhmmss_digits(hhmmss)
    if t is None:
        return None
    try:
        return datetime(d.year, d.month, d.day, int(t[:2]), int(t[2:4]), int(t[4:6]))
    except ValueError:
        return None


# 全市场分钟一轮 5579 标的, "返回非空但解析 0 行" 的诊断样本只打前几个防刷屏
_UNPARSED_MINUTE_SAMPLES = 0


def _warn_minute_unparsed(symbol: str, raw: dict) -> None:
    """返回非空但整批解析 0 行 = 格式漂移最直接的信号 (#509 曾完全静默)。

    进程内限前 3 个标的; 采样键名与每列前两个值, 供反馈定位。
    计数从包对象读写, 测试可直接复位 tp._UNPARSED_MINUTE_SAMPLES。
    """
    samples = _fp._UNPARSED_MINUTE_SAMPLES
    if samples >= 3:
        return
    _fp._UNPARSED_MINUTE_SAMPLES = samples + 1
    sample: dict[str, object] = {}
    for key, values in list(raw.items())[:8]:
        if isinstance(values, list | tuple):
            sample[key] = [repr(v) for v in values[:2]]
        else:
            sample[key] = repr(values)[:40]
    logger.warning(
        "TdxQuant 分钟K %s 返回 %d 行但解析为 0 行(疑似返回结构漂移), 字段样本: %s — 请附此日志反馈",
        symbol,
        len(raw.get("Date") or ()),
        sample,
    )


def _minute_rows(
    symbol: str,
    raw: dict,
    start_dt: datetime | None,
    end_dt: datetime | None,
    today_only: date | None = None,
) -> list[dict]:
    """原生 1m K 线 dict → 内部分钟行(datetime 北京墙钟, volume 股→手, amount 万元→元)。

    today_only 非空时仅保留该交易日的行(修复轮用: count 模式尾部会带前一日余量)。
    """
    raw = _unwrap_value(raw)
    dates = raw.get("Date") or []
    times = raw.get("Time") or []
    out: list[dict] = []
    for i, compact in enumerate(dates):
        dt = _minute_dt(compact, times[i] if i < len(times) else None)
        if dt is None:
            continue
        if today_only is not None and dt.date() != today_only:
            continue
        if start_dt is not None and dt < start_dt:
            continue
        if end_dt is not None and dt > end_dt:
            continue

        def _col(name: str, idx: int = i) -> float | None:
            values = raw.get(name)
            return _to_float(values[idx]) if values and idx < len(values) else None

        close = _col("Close")
        if close is None:
            continue  # 停牌/无成交分钟, 不伪造
        volume = _col("Volume")
        amount = _col("Amount")
        out.append(
            {
                "symbol": symbol,
                "datetime": dt,
                "open": _col("Open"),
                "high": _col("High"),
                "low": _col("Low"),
                "close": close,
                "volume": float(math.floor(volume / 100.0)) if volume is not None else None,
                "amount": amount * 10_000.0 if amount is not None else None,
            }
        )
    return out


def _minute_count_for_window(start_dt: datetime | None, end_dt: datetime) -> int:
    """窗口 → 单次 count 请求根数(交易日×240 + 两日余量, 封顶 _MINUTE_MAX_COUNT)。"""
    span_days = (end_dt.date() - start_dt.date()).days if start_dt else 30
    return max(240, min(_MINUTE_MAX_COUNT, int(span_days * 5 / 7 * _MINUTE_BARS_PER_DAY) + 480))
