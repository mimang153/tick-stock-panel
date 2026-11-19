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

原单文件已拆为 package: 模块级定义集中在 _common, 大类按能力分 mixin。
外部导入路径 app.plugins.fuyao.provider 保持不变(plugin.yaml entry/check、
loader getattr 及测试 monkeypatch 均指向本包对象)。
"""

from ._adj import FuyaoProviderAdj
from ._base import FuyaoProviderBase
from ._common import (  # noqa: F401 — 对外可见名与测试 patch 目标的显式重导出
    _ADJ_DUMP_KIND,
    _DAILY10_DUMP_KIND,
    _DAILY_DUMP_BATCH_ROWS,
    _DAILY_DUMP_COLUMNS,
    _DAILY_DUMP_KIND,
    _DATASETS,
    _FINANCIAL_HISTORY_PERIODS,
    _HIST_INTERVAL_S,
    _HIST_MAX_SPAN_MS,
    _HIST_SMALL_SYMBOL_LIMIT,
    _HIST_SYMBOL_BATCH,
    _PREV_CLOSE_BACKDAYS,
    _RECENT_DUMP_DAYS,
    _SH_MS,
    _STATEMENT_ENDPOINTS,
    _VALUATION_BATCH,
    API_KEY_ENV,
    SECRETS_FIELD,
    _cache_dir,
    _date_of_ms,
    _dump_covers,
    _dump_date_range,
    _first,
    _FuyaoConfig,
    _iso_of_ms,
    _kline_rows,
    _map_snapshot_row,
    _ms_of_date,
    _price_limit,
    _ref_price,
    _release_of,
    _report_quarter,
    _tail_ok,
    _to_float,
    availability,
    fuyao_client,
    get_api_key,
    logger,
    probe_api_key,
)
from ._daily import FuyaoProviderDaily
from ._financial import FuyaoProviderFinancial
from ._test import FuyaoProviderTest


class FuyaoProvider(
    FuyaoProviderBase,
    FuyaoProviderDaily,
    FuyaoProviderAdj,
    FuyaoProviderFinancial,
    FuyaoProviderTest,
):
    """扶摇数据源。realtime = A 股全市场快照(quote_service 全市场模式轮询调用)。"""


__all__ = [
    "API_KEY_ENV",
    "SECRETS_FIELD",
    "FuyaoProvider",
    "availability",
    "get_api_key",
    "logger",
    "probe_api_key",
]
