"""assistant tools — schemas 分组实现。

由 app.custom.assistant.tools 组合并 re-export；公开导入路径不变。

本模块集中定义本地工具的 JSON Schema（函数签名供 LLM 工具调用）：

- ``_schema``: 把名称/描述/属性/必填项组装为统一的 function schema 字典；
- ``_local_tool_schemas*``: 按 quotes / market / strategy_query / signal_strategy /
  strategy_actions / data 分组返回工具 schema，供助手按场景裁剪上下文。

入参是静态描述字符串或字段清单，出参是 ``list[dict[str, Any]]``，不含 IO 与外部依赖。
"""

from __future__ import annotations

from app.custom.assistant.tools import (
    Any,
)


def _schema(
    name: str, description: str, properties: dict[str, Any], required: list[str]
) -> dict[str, Any]:
    """把名称/描述/参数属性/必填项组装为统一的 OpenAI function schema 字典。"""
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
        },
    }


def _local_tool_schemas() -> list[dict[str, Any]]:
    """汇总全部本地工具 schema（quotes/market/strategy/signal/data 六组）供助手注册。"""
    from app.strategy.intraday_features import INTRADAY_FEATURES

    intraday_fields = "/".join(INTRADAY_FEATURES)
    return [
        *_local_tool_schemas_quotes(),
        *_local_tool_schemas_market(),
        *_local_tool_schemas_strategy_query(),
        *_local_tool_schemas_signal_strategy(intraday_fields),
        *_local_tool_schemas_strategy_actions(),
        *_local_tool_schemas_data(),
    ]


def _local_tool_schemas_quotes() -> list[dict[str, Any]]:
    """行情域工具 schema：单/多标的实时快照、K线、分时与财务等查询。"""
    return [
        _schema(
            "get_stock_quote",
            "查询一只或多只股票(≤50)的实时行情快照: 最新价、涨跌幅、成交额、换手率、振幅等。",
            {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "证券代码数组, 如 ['600519.SH', '300750.SZ']",
                },
            },
            ["symbols"],
        ),
        _schema(
            "get_stock_daily",
            "查询个股日线行情(近 N 个交易日, ≤500): OHLCV、涨跌幅、换手率与常用均线。",
            {
                "symbol": {"type": "string", "description": "证券代码, 如 600519.SH"},
                "days": {"type": "integer", "description": "返回最近 N 个交易日 (10-500, 默认 60)"},
            },
            ["symbol"],
        ),
        _schema(
            "get_stock_analysis",
            "个股分析: 基于 120 日日线计算 11 类关键价位(支撑/压力/枢轴/布林等)与文字摘要, 附最新行情快照。",
            {"symbol": {"type": "string", "description": "证券代码"}},
            ["symbol"],
        ),
        _schema(
            "get_financials",
            "查询个股财务报表数据(本地已同步的最近几个报告期)。table: metrics(核心指标)/income(利润)/balance_sheet(资产负债)/cash_flow(现金流)/shares(股本)。",
            {
                "symbol": {"type": "string", "description": "证券代码"},
                "table": {
                    "type": "string",
                    "enum": ["metrics", "income", "balance_sheet", "cash_flow", "shares"],
                    "description": "报表类型, 默认 metrics",
                },
                "periods": {"type": "integer", "description": "最近 N 个报告期 (1-8, 默认 4)"},
            },
            ["symbol"],
        ),
        _schema(
            "get_watchlist",
            "查询用户的自选列表(含备注), 并合并最新价与今日涨跌幅, 用于回答「我的自选表现如何」。",
            {},
            [],
        ),
        _schema(
            "get_market_overview",
            "市场总览(看板首页口径): 指数、涨跌家数、成交额、涨停/连板梯队、涨跌幅分布、均线趋势占比、情绪雷达与领涨领跌/活跃榜。无参数。",
            {},
            [],
        ),
        _schema(
            "get_indices",
            "查询指数实时行情表(上证/深证/创业板/科创50 等), 返回点位与涨跌幅。",
            {},
            [],
        ),
    ]


def _local_tool_schemas_market() -> list[dict[str, Any]]:
    """市场域工具 schema：大盘概览、指数、板块轮动与市场状态查询。"""
    return [
        _schema(
            "get_sector_rotation",
            "盘中板块轮动(概念/行业分析页口径): 活跃板块榜、板块切换事件与资金流向排名。",
            {
                "kind": {
                    "type": "string",
                    "enum": ["concept", "industry"],
                    "description": "板块维度, 默认 concept",
                },
                "top": {"type": "integer", "description": "榜单数量 (10-50, 默认 20)"},
            },
            [],
        ),
        _schema(
            "get_regime",
            "市场环境(regime)时序: 最近 N 日的强弱状态、情绪分、连板高度、晋级率/封板率与情绪周期阶段。",
            {"days": {"type": "integer", "description": "最近 N 个交易日 (1-120, 默认 30)"}},
            [],
        ),
        _schema(
            "get_abnormal",
            "异动监控: intraday=盘中异动(涨停/炸板/跌停/新高新低/放量), overview=交易所异动规则偏离总览。",
            {
                "kind": {
                    "type": "string",
                    "enum": ["intraday", "overview"],
                    "description": "默认 intraday",
                },
                "limit": {"type": "integer", "description": "返回行数上限 (10-200, 默认 50)"},
            },
            [],
        ),
        _schema(
            "get_lots",
            "查询用户的持仓提醒(lots): 持仓数量、成本、目标/止损幅度, 合并最新价与今日涨跌幅。",
            {},
            [],
        ),
        _schema(
            "list_signals",
            "检索用户自定义信号的目录(信号库页): 名称、表达式、分组与启用状态。",
            {},
            [],
        ),
    ]


def _local_tool_schemas_strategy_query() -> list[dict[str, Any]]:
    """策略查询域工具 schema：策略列表、参数与历史表现等只读查询。"""
    return [
        _schema(
            "run_strategy",
            "执行已保存策略并返回命中列表(≤30 行)。默认按策略声明选择资产/周期，分钟策略读取最新分钟分区。"
            "这是单次筛选，持续分钟策略提醒尚未接通；日线监控需另行配置。strategy_id 可先用 list_strategies 查询。",
            {
                "strategy_id": {"type": "string", "description": "策略 id"},
                "asset_type": {
                    "type": "string",
                    "enum": ["stock", "etf"],
                    "description": "默认按策略声明选择",
                },
                "timeframe": {
                    "type": "string",
                    "enum": ["1d", "1m"],
                    "description": "默认按策略声明选择",
                },
                "params": {"type": "object", "description": "本次运行的参数覆盖，不改已保存参数"},
            },
            ["strategy_id"],
        ),
        _schema(
            "get_factor_values",
            "查询某因子在最新交易日的全市场取值排名(Top N), 附股票名称与今日涨跌幅。factor 用因子 id, 可先用 list_factors 查询。"
            "默认已排除上市不足 60 天的次新股 (历史不足会让 RSI/MACD 等递推指标失真, 新股 RSI 恒为 0, "
            "不过滤会把整个榜单挤成新股); 返回值里的 excluded_new_listings 说明排除了多少只。",
            {
                "factor": {"type": "string", "description": "因子 id"},
                "top": {"type": "integer", "description": "返回前 N 名 (5-50, 默认 30)"},
                "direction": {
                    "type": "string",
                    "enum": ["desc", "asc"],
                    "description": "排序方向, 默认 desc(从大到小)",
                },
                "min_listed_days": {
                    "type": "integer",
                    "description": "排除上市不足 N 个自然日的次新股 (默认 60, 传 0 关闭过滤)",
                },
            },
            ["factor"],
        ),
    ]


def _local_tool_schemas_signal_strategy(intraday_fields: str) -> list[dict[str, Any]]:
    """信号/策略域工具 schema；intraday_fields 为可用的分时字段枚举说明。"""
    return [
        _schema(
            "create_signal_strategy",
            "在**信号库**创建一个自定义信号 (动作工具, 须确认后执行)。"
            "只写一条声明式条件条目, **不创建策略** —— 用户要「策略」时必须用 create_strategy。"
            "把交易思路翻译成声明式条件: 白名单字段 + 比较运算符, 多条件之间为 AND。"
            "字段如 close/open/high/low/volume/change_pct/turnover_rate/ma5/ma10/ma20/ma60/"
            "macd_dif/macd_dea/macd_hist/boll_upper/boll_lower/kdj_k/kdj_d/kdj_j/rsi_6/rsi_14/"
            "vol_ratio_5d/high_60d/low_60d/momentum_5d 等; 上穿/下穿用天数偏移表达"
            "(如 close>field:ma20 且 1 日前 close<=field:ma20 即金叉)。"
            f"timeframe=intraday 时仅使用 {intraday_fields}，"
            "额外支持 cross_up/cross_down，不支持日期偏移。"
            "任意分钟滚动公式请用 create_strategy 创建 minute_filter 策略。",
            {
                "name": {"type": "string", "description": "信号名称, 简洁中文 (如 放量突破20日线)"},
                "kind": {
                    "type": "string",
                    "enum": ["entry", "exit", "both"],
                    "description": "entry=入场信号, exit=出场信号, both=两者",
                },
                "timeframe": {
                    "type": "string",
                    "enum": ["daily", "intraday"],
                    "description": "默认 daily；盘中信号用 intraday",
                },
                "min_bars": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 240,
                    "description": "盘中信号最少已完成分钟根数",
                },
                "conditions": {
                    "type": "array",
                    "maxItems": 8,
                    "description": "条件列表 (1-8 条, 之间 AND)",
                    "items": {
                        "type": "object",
                        "properties": {
                            "left": {"type": "string", "description": "左字段名 (白名单内)"},
                            "op": {
                                "type": "string",
                                "enum": [
                                    ">",
                                    ">=",
                                    "<",
                                    "<=",
                                    "==",
                                    "!=",
                                    "cross_up",
                                    "cross_down",
                                ],
                                "description": "cross_up/cross_down 仅用于盘中信号",
                            },
                            "right": {
                                "type": "string",
                                "description": "数字 (如 1.5) 或字段引用 'field:字段名'",
                            },
                            "leftDays": {
                                "type": "integer",
                                "description": "左字段取 N 个交易日前 (0-60, 默认 0)",
                            },
                            "rightDays": {
                                "type": "integer",
                                "description": "右字段取 N 个交易日前 (仅 right 为字段时有效, 默认 0)",
                            },
                        },
                        "required": ["left", "op", "right"],
                    },
                },
            },
            ["name", "kind", "conditions"],
        ),
    ]


def _local_tool_schemas_strategy_actions() -> list[dict[str, Any]]:
    """策略动作域工具 schema：模拟交易下单、撤仓与止损参数配置。"""
    return [
        _schema(
            "create_strategy",
            "创建 AI 策略 (动作工具)。两步用法, 必须按顺序: "
            "① 先调用 create_strategy(show_guide=true) 获取策略编写规范 "
            "(META 字段/信号函数契约/示例代码), 不要跳过这步凭印象写; "
            "② 严格按规范写出完整策略 Python 源码后, 调用 create_strategy(name, description, code) 保存。"
            "支持分钟策略 minute_filter（timeframes=['1m']，filter_minute_history）与日线策略，"
            "适用于声明式信号表达不了的复杂逻辑 (多因子评分、自定义列、跨日时序)。"
            "简单思路优先用 create_signal_strategy。保存后为 research_only 草稿态, 需用户在策略页发布才生效。",
            {
                "show_guide": {
                    "type": "boolean",
                    "description": "true = 只返回策略编写规范, 不保存 (第一步必用)",
                },
                "name": {"type": "string", "description": "策略中文名, 简洁 (如 强势反包)"},
                "description": {"type": "string", "description": "一句话说明策略选股逻辑"},
                "code": {
                    "type": "string",
                    "description": "完整策略 Python 源码 (含 META 字典), 上限 60000 字符",
                },
            },
            [],
        ),
        _schema(
            "add_to_watchlist",
            "把一只标的加入用户自选列表(动作工具, 须用户在确认卡上确认后才执行)。",
            {
                "symbol": {"type": "string", "description": "证券代码, 如 600519.SH"},
                "note": {"type": "string", "description": "备注 (可选, ≤80 字)"},
            },
            ["symbol"],
        ),
        _schema(
            "check_data_coverage",
            "检查本地数据完整性: 日线/指标(enriched)/分钟K的覆盖区间与新旧、财务表覆盖、"
            "正在运行的同步任务, 返回问题清单 issues(为空即数据完整)。"
            "发现『数据停留在旧交易日』『指标落后日线』『无财务数据』等缺口时, 建议用户补全。",
            {"symbol": {"type": "string", "description": "可选: 顺带检查单只标的的日线/财务覆盖"}},
            [],
        ),
    ]


def _local_tool_schemas_data() -> list[dict[str, Any]]:
    """数据域工具 schema：数据覆盖检查、字段范围与可用性查询。"""
    return [
        _schema(
            "list_ext_tables",
            "列出用户配置的扩展数据表(二开扩展内容): 每张表的模式(快照/时序)、字段、说明"
            "与最新数据日期。用户问到扩展表/自定义数据/上传的数据时先调它。",
            {},
            [],
        ),
        _schema(
            "query_ext_table",
            "读取一张扩展数据表的行数据。table 为表 id(先用 list_ext_tables 查); "
            "filter=字段:值1|值2 等值过滤(值间 OR); sort=字段 或 字段:desc 排序; "
            "时序表支持 start_date/end_date(YYYY-MM-DD)或 date 单日。limit 默认 20, 上限 100。",
            {
                "table": {"type": "string", "description": "扩展表 id"},
                "filter": {"type": "string", "description": "等值过滤: 字段:值1|值2"},
                "sort": {"type": "string", "description": "排序字段, 降序加 :desc"},
                "date": {"type": "string", "description": "时序表单日 YYYY-MM-DD"},
                "start_date": {"type": "string", "description": "时序表起始日期(含)"},
                "end_date": {"type": "string", "description": "时序表结束日期(含)"},
                "limit": {"type": "integer", "description": "返回行数上限 (默认 20, 最大 100)"},
            },
            ["table"],
        ),
        _schema(
            "get_sync_status",
            "查询数据同步任务进度: 活跃任务的阶段/百分比/消息、最近 5 个任务、财务同步状态。"
            "sync_data 触发后用它轮询(建议间隔数秒, 不要高频轮询)。",
            {},
            [],
        ),
        _schema(
            "sync_data",
            "数据补全(动作工具, 须用户在确认卡上确认后才执行; 后台执行, 立即返回)。"
            "kind=pipeline 盘后管道(日K/指标/复权/名单, 含已启用的分钟K, 补最新交易日); "
            "kind=financials 财务表(table 可选 metrics/income/balance_sheet/cash_flow/shares/all); "
            "kind=minute_extend 扩展分钟K历史(days 30-1095, 默认 365, 用于回测深度)。",
            {
                "kind": {
                    "type": "string",
                    "enum": ["pipeline", "financials", "minute_extend"],
                    "description": "补全类型",
                },
                "table": {
                    "type": "string",
                    "enum": ["metrics", "income", "balance_sheet", "cash_flow", "shares", "all"],
                    "description": "仅 kind=financials: 同步哪张表, 默认 all",
                },
                "days": {
                    "type": "integer",
                    "description": "仅 kind=minute_extend: 目标天数 (30-1095, 默认 365)",
                },
            },
            ["kind"],
        ),
    ]
