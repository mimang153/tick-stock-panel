"""ClickHouse schema —— 建表 DDL 与版本号集中于此（设计文档 §3 / §4.12）。

约定：

- 建表语句必须幂等（``CREATE TABLE IF NOT EXISTS``），启动时整批顺序执行
- 需要 projection 的主表额外带 ``deduplicate_merge_projection_mode = 'rebuild'``：
  CK 26.10 上 ReplacingMergeTree 加 projection 不带该 setting 会直接报
  ``SUPPORT_IS_DISABLED``（§4.8），因此按"可能要加 projection 的主表"统一带上；
  维表/状态表/缓存表可省略
- 数据库（``tsp`` / ``tsp_test``）由部署预建，应用不执行 ``CREATE DATABASE``：
  共用实例上 ``tsp`` 用户按库授权，应用侧不应有建库/删库能力（§4.8）
- 表名一律经 ``ClickHouseClient.table()`` 生成，测试靠前缀隔离（§4.7）
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import polars as pl

from app.storage.clickhouse import ClickHouseClient

# 版本号只在 DDL 语义变化时 +1；幂等建表本身覆盖"新增表"
SCHEMA_VERSION = 3
SCHEMA_NOTE = "v3: 修补快照 30 天 / mining 审计 90 天 TTL（DES-002 §9 / DES-003 D-02）"

_PROJECTION_SETTINGS = "SETTINGS deduplicate_merge_projection_mode = 'rebuild'"

# 可能被按日横截面查询的主表（§4.10 兜底方案），建表时必须预留 projection 能力
_PROJECTION_TABLES = frozenset(
    {
        "kline_daily",
        "kline_daily_enriched",
        "kline_index_daily",
        "kline_index_enriched",
        "kline_etf_daily",
        "kline_etf_enriched",
        "kline_minute",
        "kline_etf_minute",
        "adj_factor",
        "adj_factor_etf",
        "depth5",
        "financials_metrics",
        "financials_income",
        "financials_balance_sheet",
        "financials_cash_flow",
        "financials_shares",
        "ext_timeseries",
    }
)

_TABLE_DDL: dict[str, str] = {
    # 版本登记表：只记"应用过哪个版本"，不记历史
    "schema_version": """CREATE TABLE IF NOT EXISTS {table} (
  version    UInt32,
  applied_at DateTime64(3) DEFAULT now64(3),
  note       String
) ENGINE = MergeTree ORDER BY version""",
    # ── 日 K ────────────────────────────────────────────────────
    # quote_ts 必须是 Nullable：历史同步帧根本没有该列，实测 2025-11-19 分区
    # kline_daily_enriched 的 quote_ts 为 5571/5571 全 NULL，非空列会让历史写入直接失败。
    "kline_daily": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  date   Date,
  open   Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  quote_ts Nullable(Int64),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date)""",
    "kline_daily_enriched": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, date Date,
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  raw_close Float64, raw_high Float64, raw_low Float64,
  turnover_rate Float64,
  consecutive_limit_ups UInt32, consecutive_limit_downs UInt32,
  quote_ts Nullable(Int64),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date)""",
    "kline_index_daily": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  date   Date,
  open   Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date)""",
    # 指数/ETF enriched 是窄表（无换手率、连板列）
    "kline_index_enriched": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, date Date,
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  raw_close Float64, raw_high Float64, raw_low Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date)""",
    "kline_etf_daily": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  date   Date,
  open   Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date)""",
    "kline_etf_enriched": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, date Date,
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  raw_close Float64, raw_high Float64, raw_low Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date)""",
    # ── 分钟 K ──────────────────────────────────────────────────
    # 分区按月：全市场 1 年约 3 亿行（§3.2）。datetime 是北京墙钟，写入时由
    # ClickHouseClient 的 Arrow 边界补 Asia/Shanghai，读取时还原为 naive。
    "kline_minute": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  datetime DateTime64(6, 'Asia/Shanghai'),
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(datetime)
ORDER BY (symbol, datetime)""",
    "kline_etf_minute": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  datetime DateTime64(6, 'Asia/Shanghai'),
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(datetime)
ORDER BY (symbol, datetime)""",
    # ── 除权因子 ────────────────────────────────────────────────
    "adj_factor": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, trade_date Date,
  ex_factor Float64, dividend Float64, bonus Float64,
  allot Float64, allot_price Float64, prev_close Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(trade_date)
ORDER BY (symbol, trade_date)""",
    "adj_factor_etf": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, trade_date Date,
  ex_factor Float64, dividend Float64, bonus Float64,
  allot Float64, allot_price Float64, prev_close Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(trade_date)
ORDER BY (symbol, trade_date)""",
    # ── 标的维表 ────────────────────────────────────────────────
    # listing_date 原表就是 String，不在这里改成 Date，避免引入新的解析口径
    "instruments": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, name String, code String, exchange String,
  region String, type String, listing_date String,
  total_shares Nullable(Float64), float_shares Nullable(Float64), tick_size Nullable(Float64),
  limit_up Nullable(Float64), limit_down Nullable(Float64),
  as_of Date,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY symbol""",
    "instruments_index": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, name String, code String, exchange String,
  region String, type String, listing_date String,
  total_shares Nullable(Float64), float_shares Nullable(Float64), tick_size Nullable(Float64),
  limit_up Nullable(Float64), limit_down Nullable(Float64),
  as_of Date,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY symbol""",
    "instruments_etf": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String, name String, code String, exchange String,
  region String, type String, listing_date String,
  total_shares Nullable(Float64), float_shares Nullable(Float64), tick_size Nullable(Float64),
  limit_up Nullable(Float64), limit_down Nullable(Float64),
  as_of Date, asset_type String,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY symbol""",
    # ── 五档盘口（收盘定版旁路）──────────────────────────────────
    "depth5": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  date Date,
  sealed_up Nullable(Bool), sealed_down Nullable(Bool),
  ask1_vol Nullable(Int64), bid1_vol Nullable(Int64),
  status String,
  fetched_at Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date)""",
    # ── 财务五表（动态列，§4.1 策略 A）───────────────────────────
    # 数值字段进 values Map，文本/日期串进 raw Map，加字段零 DDL。
    # 股本表的期间列实测就是 period_end（app/share_capital.py 依赖该列名）。
    "financials_metrics": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  period_end Date,
  values Map(String, Float64),
  raw Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(period_end)
ORDER BY (symbol, period_end)""",
    "financials_income": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  period_end Date,
  values Map(String, Float64),
  raw Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(period_end)
ORDER BY (symbol, period_end)""",
    "financials_balance_sheet": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  period_end Date,
  values Map(String, Float64),
  raw Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(period_end)
ORDER BY (symbol, period_end)""",
    "financials_cash_flow": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  period_end Date,
  values Map(String, Float64),
  raw Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(period_end)
ORDER BY (symbol, period_end)""",
    "financials_shares": """CREATE TABLE IF NOT EXISTS {table} (
  symbol String,
  period_end Date,
  values Map(String, Float64),
  raw Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(period_end)
ORDER BY (symbol, period_end)""",
    # ── 扩展数据（多扩展共用一张，带 ext_id）─────────────────────
    # 去重键对齐既有 [symbol, date, time_field]：time_value 进排序键而非分区键，
    # 保证同一扩展同一天的多盘数据落同一分区。
    "ext_timeseries": """CREATE TABLE IF NOT EXISTS {table} (
  ext_id      LowCardinality(String),
  symbol      String,
  date        Date,
  time_value  String,
  values      Map(String, Float64),
  raw         Map(String, String),
  updated_at  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (ext_id, symbol, date, time_value)""",
    "ext_instruments": """CREATE TABLE IF NOT EXISTS {table} (
  ext_id     LowCardinality(String),
  symbol     String,
  code       String,
  values     Map(String, Float64),
  raw        Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY (ext_id, symbol)""",
    # ── 回测矩阵缓存（原 cache/factor_columns/<sha256>.parquet）──
    # TTL 用 accessed_at：内容寻址缓存会被反复复用，按创建时间淘汰会误删热数据。
    # TTL 表达式必须返回 Date/DateTime：24.8 不接受 DateTime64(3)（BAD_TTL_EXPRESSION），
    # 因此先 toDate 再按天淘汰。
    "matrix_cache": """CREATE TABLE IF NOT EXISTS {table} (
  key         String,
  payload     String,
  size_bytes  UInt64,
  created_at  DateTime64(3) DEFAULT now64(3),
  accessed_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(accessed_at)
PARTITION BY tuple()
ORDER BY key
TTL toDate(accessed_at) + INTERVAL 30 DAY""",
    # ── 回测结果（§3.3）─────────────────────────────────────────
    "backtest_runs": """CREATE TABLE IF NOT EXISTS {table} (
  run_id       String,
  created_at   DateTime64(3) DEFAULT now64(3),
  strategy_id  String,
  config_json  String,
  stats_json   String,
  equity_curve String,
  extra_json   String,
  n_trades     UInt32,
  elapsed_ms   Float64,
  error        Nullable(String),
  updated_at   DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(created_at)
ORDER BY run_id""",
    "backtest_trades": """CREATE TABLE IF NOT EXISTS {table} (
  run_id String,
  seq    UInt32,
  symbol String,
  entry_signal_date Nullable(Date),
  entry_date        Nullable(Date),
  entry_price       Nullable(Float64),
  exit_signal_date  Nullable(Date),
  exit_date         Nullable(Date),
  exit_price        Nullable(Float64),
  pnl_pct       Nullable(Float64),
  duration_days Nullable(Int32),
  exit_reason   Nullable(String),
  extra         Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY (run_id, seq)""",
    # ── 同步状态（替代 preferences.json 的 last_sync 与文件发布标记）──
    "sync_state": """CREATE TABLE IF NOT EXISTS {table} (
  task        LowCardinality(String),
  status      LowCardinality(String),
  last_ok_at  Nullable(DateTime64(3)),
  last_error  Nullable(String),
  updated_at  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY task""",
    # ── 派生结果缓存（§13.3①）：市场主线历史 ──────────────────────
    # 结构化宽表而非 blob: 读侧按 (date, kind) 过滤/排序, 需要列裁剪。
    # 覆盖写用 delete WHERE (date,kind) + INSERT (mutations_sync=2 保证写完即可见),
    # ReplacingMergeTree(updated_at) 负责兜底同键去重。
    "market_mainline": """CREATE TABLE IF NOT EXISTS {table} (
  date              Date,
  kind              LowCardinality(String),
  member            String,
  limit_up_count    UInt32,
  ge2_count         UInt32,
  max_boards        UInt32,
  boards_sum        UInt32,
  rungs_filled      UInt32,
  leader_symbol     String,
  score             Float64,
  rank              UInt32,
  updated_at        DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY (date, kind, member)""",
    # ── 派生结果缓存（RC-12）：市场环境时序 ──────────────────────────
    # 列随评分模型迭代扩展，用 payload_json 存整行避免频繁 DDL；date 独立列
    # 供按日覆盖/删除与去重。本地 parquet 仅作离线兼容，在线读源为 CK。
    "regime_history": """CREATE TABLE IF NOT EXISTS {table} (
  date              Date,
  payload_json      String,
  updated_at        DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY date""",
    # ── 修补契约（DES-002）：审计/快照/generation 元数据 ──────────────
    "ck_repair_audit": """CREATE TABLE IF NOT EXISTS {table} (
  id             UUID DEFAULT generateUUIDv4(),
  repair_id      String,
  dataset        LowCardinality(String),
  day            Date,
  symbol         String,
  change_type    LowCardinality(String),
  before_json    Nullable(String),
  after_json     Nullable(String),
  reason         LowCardinality(String),
  status         LowCardinality(String),
  created_by     LowCardinality(String),
  created_at     DateTime64(3) DEFAULT now64(3),
  updated_by     LowCardinality(String),
  updated_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (dataset, day, repair_id, symbol, id)""",
    "ck_repair_snapshot": """CREATE TABLE IF NOT EXISTS {table} (
  id             UUID DEFAULT generateUUIDv4(),
  repair_id      String,
  dataset        LowCardinality(String),
  day            Date,
  symbol         String,
  payload_json   String,
  created_by     LowCardinality(String),
  created_at     DateTime64(3) DEFAULT now64(3),
  updated_by     LowCardinality(String),
  updated_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (dataset, day, repair_id, symbol, id)
TTL toDate(created_at) + INTERVAL 30 DAY""",
    "ck_enriched_generation": """CREATE TABLE IF NOT EXISTS {table} (
  id             UUID DEFAULT generateUUIDv4(),
  asset_type     LowCardinality(String),
  generation_id  String,
  state          LowCardinality(String),
  changed_from   Nullable(Date),
  heartbeat_at   DateTime64(3),
  owner_id       LowCardinality(String) DEFAULT '',
  fencing_token UInt64 DEFAULT 0,
  lease_expires_at Nullable(DateTime64(3)),
  committed_at   Nullable(DateTime64(3)),
  created_by     LowCardinality(String),
  created_at     DateTime64(3) DEFAULT now64(3),
  updated_by     LowCardinality(String),
  updated_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY asset_type
ORDER BY (asset_type, generation_id)""",
    # ── 协调态审计（DES-004 §2.3 / 方案 B）：Redis 协调态丢失后的 CK 重建源 ──
    "ck_coord_job_state": """CREATE TABLE IF NOT EXISTS {table} (
  id          UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  job_id      String COMMENT '任务唯一标识',
  state_json  String COMMENT '任务完整状态快照JSON(id/status/stage/progress/result/error等)',
  status      LowCardinality(String) COMMENT '任务状态快照,便于过滤',
  created_at  DateTime64(3) COMMENT '首次写入时间',
  created_by  LowCardinality(String) COMMENT '首次写入者标识',
  updated_at  DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作ReplacingMergeTree版本列',
  updated_by  LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY job_id
COMMENT '任务状态审计:每个job保留最新快照,Redis JobState丢失后据此重建'""",
    "ck_coord_cancel_request": """CREATE TABLE IF NOT EXISTS {table} (
  id           UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  job_id       String COMMENT '被取消的任务标识',
  requested_at DateTime64(3) COMMENT '取消请求发起时间',
  actor        LowCardinality(String) COMMENT '取消请求发起者标识',
  consumed     UInt8 COMMENT '是否已消费:1=已消费不再恢复,0=待恢复',
  created_at   DateTime64(3) COMMENT '审计记录创建时间',
  created_by   LowCardinality(String) COMMENT '审计记录创建者标识',
  updated_at   DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作版本列',
  updated_by   LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY job_id
COMMENT '取消请求审计:每个job保留最新取消请求,Redis CancelFlag丢失后据此重建'""",
    "ck_coord_limiter_lease": """CREATE TABLE IF NOT EXISTS {table} (
  id           UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  lease_id     String COMMENT '租约唯一标识,由limiter在acquire时生成',
  owner_id     String COMMENT '租约持有者标识(进程/任务)',
  kind         LowCardinality(String) COMMENT '任务类别:normal/mining/exclusive',
  weight       UInt32 COMMENT '占用容量权重',
  acquired_at  DateTime64(3) COMMENT '获取租约时间',
  released_at  Nullable(DateTime64(3)) COMMENT '释放租约时间,未释放为NULL',
  created_at   DateTime64(3) COMMENT '审计记录创建时间',
  created_by   LowCardinality(String) COMMENT '审计记录创建者标识',
  updated_at   DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作版本列',
  updated_by   LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY lease_id
COMMENT '容量租约审计:记录acquire/release生命周期,Redis LimiterCounter丢失后按未释放租约重建'""",
    "ck_mining_run_audit": """CREATE TABLE IF NOT EXISTS {table} (
  id             UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  event_id       String COMMENT '审计事件唯一ID,用于outbox幂等',
  namespace      LowCardinality(String) COMMENT '命名空间:mining/guardian',
  run_id         String COMMENT '运行标识',
  from_status    LowCardinality(String) COMMENT '转换前状态',
  to_status      LowCardinality(String) COMMENT '转换后状态',
  manifest_json  String COMMENT '转换后的manifest完整快照JSON',
  error          Nullable(String) COMMENT '转换附带错误信息',
  created_at     DateTime64(3) COMMENT '审计记录写入时间',
  created_by     LowCardinality(String) COMMENT '写入者标识',
  updated_at     DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作版本列',
  updated_by     LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY namespace
ORDER BY (namespace, run_id, event_id)
# TTL 表达式先 toDate：CK 24.8+ 不接受高精度时间类型直接做 TTL（BAD_TTL_EXPRESSION）。
# D-02：事件/审计保留 90 天，不小于协调态重建窗口。
TTL toDate(created_at) + INTERVAL 90 DAY
COMMENT 'mining运行审计:记录每次状态转换的快照,Redis状态机丢失后据此重建run'""",
    "ck_mining_artifact": """CREATE TABLE IF NOT EXISTS {table} (
  id             UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  namespace      LowCardinality(String) COMMENT '命名空间:mining/guardian',
  run_id         String COMMENT '运行标识',
  artifact_name  LowCardinality(String) COMMENT '产物名称',
  content_addr   String COMMENT '内容寻址标识(sha256)',
  uri            String COMMENT '跨实例可解析的产物URI',
  row_count      UInt64 DEFAULT 0 COMMENT '产物行数',
  size_bytes     UInt64 DEFAULT 0 COMMENT '产物字节数',
  schema_version UInt32 DEFAULT 1 COMMENT '产物schema版本',
  created_at     DateTime64(3) COMMENT '登记时间',
  created_by     LowCardinality(String) COMMENT '登记者标识',
  updated_at     DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作版本列',
  updated_by     LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY namespace
ORDER BY (namespace, run_id, artifact_name)
COMMENT 'mining产物元数据:内容寻址URI,产物字节不落CK'""",
}

# 建表顺序即 DDL 声明顺序（无外键依赖，保持稳定即可）
TABLE_NAMES: tuple[str, ...] = tuple(_TABLE_DDL)

# 读取投影：与既有 Parquet 存储列一一对应，**不含** `updated_at`。
# 多出来的列会顺着 DataFrame 流到 API 响应里，破坏"页面无感"，所以这里显式定死；
# 需要 quote_ts 之类的列时由调用方在 columns 里显式请求。
TABLE_READ_COLUMNS: dict[str, tuple[str, ...]] = {
    "kline_daily": ("symbol", "date", "open", "high", "low", "close", "volume", "amount"),
    "kline_index_daily": ("symbol", "date", "open", "high", "low", "close", "volume", "amount"),
    "kline_etf_daily": ("symbol", "date", "open", "high", "low", "close", "volume", "amount"),
    "kline_daily_enriched": (
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "raw_close",
        "raw_high",
        "raw_low",
        "turnover_rate",
        "consecutive_limit_ups",
        "consecutive_limit_downs",
        "quote_ts",
    ),
    "kline_index_enriched": (
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "raw_close",
        "raw_high",
        "raw_low",
    ),
    "kline_etf_enriched": (
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "raw_close",
        "raw_high",
        "raw_low",
    ),
    "kline_minute": (
        "symbol",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ),
    "kline_etf_minute": (
        "symbol",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ),
    "depth5": (
        "symbol",
        "date",
        "sealed_up",
        "sealed_down",
        "ask1_vol",
        "bid1_vol",
        "status",
        "fetched_at",
    ),
    "adj_factor": (
        "symbol",
        "trade_date",
        "ex_factor",
        "dividend",
        "bonus",
        "allot",
        "allot_price",
        "prev_close",
    ),
    "adj_factor_etf": (
        "symbol",
        "trade_date",
        "ex_factor",
        "dividend",
        "bonus",
        "allot",
        "allot_price",
        "prev_close",
    ),
    "instruments": (
        "symbol",
        "name",
        "code",
        "exchange",
        "region",
        "type",
        "listing_date",
        "total_shares",
        "float_shares",
        "tick_size",
        "limit_up",
        "limit_down",
        "as_of",
    ),
    "instruments_index": (
        "symbol",
        "name",
        "code",
        "exchange",
        "region",
        "type",
        "listing_date",
        "total_shares",
        "float_shares",
        "tick_size",
        "limit_up",
        "limit_down",
        "as_of",
    ),
    "instruments_etf": (
        "symbol",
        "name",
        "code",
        "exchange",
        "region",
        "type",
        "listing_date",
        "total_shares",
        "float_shares",
        "tick_size",
        "limit_up",
        "limit_down",
        "as_of",
        "asset_type",
    ),
}


def build_statements(
    table_prefix: str = "",
    *,
    tables: Sequence[str] | None = None,
) -> list[str]:
    """生成建表语句；``table_prefix`` 供测试隔离，``tables`` 可只建子集。"""
    names = list(_TABLE_DDL) if tables is None else list(tables)
    unknown = [n for n in names if n not in _TABLE_DDL]
    if unknown:
        raise ValueError(f"未知表 {unknown}")
    statements = []
    for name in names:
        ddl = _TABLE_DDL[name]
        statement = ddl.format(table=f"{table_prefix}{name}")
        if name in _PROJECTION_TABLES:
            statement = f"{statement}\n{_PROJECTION_SETTINGS}"
        statements.append(statement)
    return statements


def _migration_v2_instruments_nullable(client: ClickHouseClient) -> None:
    """v1 → v2: 股本/交易规则列改为 Nullable。

    旧表为非 Nullable Float64, provider 未提供的股本被写成 0.0, enriched 的
    turnover_rate 随之变 0, 依赖换手率/流通市值的策略筛选结果失真。
    """
    for name in ("instruments", "instruments_index", "instruments_etf"):
        client.execute(
            f"ALTER TABLE {client.table(name)} "
            "MODIFY COLUMN total_shares Nullable(Float64), "
            "MODIFY COLUMN float_shares Nullable(Float64), "
            "MODIFY COLUMN tick_size Nullable(Float64)"
        )


def _migration_v3_retention_ttl(client: ClickHouseClient) -> None:
    """v2 → v3: 修补快照 30 天、mining 审计 90 天 TTL（DES-002 §9 / DES-003 D-02）。

    ``CREATE TABLE IF NOT EXISTS`` 不会修改已存在表的 TTL，存量库必须显式 ALTER。
    ensure_schema(tables=...) 只建传入的子集（如仅 instruments），迁移只 ALTER 实际存在的表。
    TTL 表达式返回 Date，兼容 CK 24.8+ 的 BAD_TTL_EXPRESSION 约束。
    """
    for logical, days in (("ck_repair_snapshot", 30), ("ck_mining_run_audit", 90)):
        existing = client.query(
            "SELECT count() AS n FROM system.tables WHERE database = currentDatabase() "
            "AND name = {name:String}",
            {"name": client.table(logical)},
        )
        # 简化测试桩的 query 可能不返回 n 列（只记录语句、不连 CK）；此时照常发 ALTER。
        if "n" in existing.columns and (existing.is_empty() or int(existing["n"][0]) == 0):
            continue
        client.execute(
            f"ALTER TABLE {client.table(logical)} "
            f"MODIFY TTL toDate(created_at) + INTERVAL {days} DAY"
        )


_MIGRATIONS: dict[int, Callable[[ClickHouseClient], None]] = {
    2: _migration_v2_instruments_nullable,
    3: _migration_v3_retention_ttl,
}


def ensure_schema(client: ClickHouseClient, *, tables: Sequence[str] | None = None) -> int:
    """幂等建表并登记版本，返回当前 schema 版本。

    ``tables`` 只建子集时（测试用），仍需带上 ``schema_version`` 才能记版本。
    版本升级按序执行增量迁移（CREATE IF NOT EXISTS 不会变更已存在的表）。
    """
    names = list(_TABLE_DDL) if tables is None else [*tables]
    if "schema_version" not in names:
        names = ["schema_version", *names]
    for statement in build_statements(client.table_prefix, tables=names):
        client.execute(statement)

    recorded = _recorded_version(client)
    if recorded > SCHEMA_VERSION:
        raise RuntimeError(
            f"ClickHouse schema 版本 {recorded} 高于代码支持的 {SCHEMA_VERSION}；"
            "拒绝启动，避免旧代码写新库造成口径错乱"
        )
    if recorded < SCHEMA_VERSION:
        for version in range(recorded + 1, SCHEMA_VERSION + 1):
            migration = _MIGRATIONS.get(version)
            if migration is not None:
                migration(client)
        client.insert(
            client.table("schema_version"),
            pl.DataFrame({"version": [SCHEMA_VERSION], "note": [SCHEMA_NOTE]}),
        )
        return SCHEMA_VERSION
    return recorded


def _recorded_version(client: ClickHouseClient) -> int:
    table = client.table("schema_version")
    df = client.query(f"SELECT max(version) AS version FROM {table}")
    if df.is_empty() or df["version"][0] is None:
        return 0
    return int(df["version"][0])
