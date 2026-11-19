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

原单文件已拆为 package: 模块级定义集中在 _common, 大类按能力分 mixin。
外部导入路径 app.plugins.tdxquant.provider 保持不变(plugin.yaml entry/check、
loader getattr 及测试 monkeypatch 均指向本包对象)。
"""

from ._base import TdxQuantProviderBase
from ._common import (  # noqa: F401 — 对外可见名与测试 patch 目标的显式重导出
    _ADJ_BACKDAYS,
    _ADJ_FUTURE_DAYS,
    _ADJ_SCHEMA,
    _BEIJING,
    _DAILY_SCHEMA,
    _DATASETS,
    _HIST_INTERVAL_S,
    _HIST_MAX_COUNT,
    _INDEX_CODE_MAP,
    _MINUTE_BARS_PER_DAY,
    _MINUTE_INTERVAL_S,
    _MINUTE_MAX_COUNT,
    _MINUTE_OUTPUT_ROWS,
    _MINUTE_OUTPUT_SYMBOLS,
    _MINUTE_SCHEMA,
    _SNAPSHOT_BATCH,
    _UNPARSED_MINUTE_SAMPLES,
    API_KEY_ENV,
    SECRETS_FIELD,
    _bars_for_window,
    _compact_date,
    _hhmmss_digits,
    _kline_rows,
    _map_snapshot_row,
    _minute_count_for_window,
    _minute_dt,
    _minute_rows,
    _module_available,
    _price_limit,
    _ref_price,
    _snapshot_ms,
    _TdxQuantConfig,
    _to_float,
    _warn_minute_unparsed,
    availability,
    datetime,
    get_api_key,
    logger,
    probe_api_key,
)
from ._daily_adj import TdxQuantProviderDailyAdj
from ._depth import TdxQuantProviderDepth
from ._minute import TdxQuantProviderMinute
from ._test import TdxQuantProviderTest


class TdxQuantProvider(
    TdxQuantProviderBase,
    TdxQuantProviderDailyAdj,
    TdxQuantProviderMinute,
    TdxQuantProviderDepth,
    TdxQuantProviderTest,
):
    """tdx 数据源。realtime = 五市场全 A 快照批拉; depth5 复用同一快照的五档数组。"""


__all__ = [
    "API_KEY_ENV",
    "TdxQuantProvider",
    "availability",
    "get_api_key",
    "probe_api_key",
]
