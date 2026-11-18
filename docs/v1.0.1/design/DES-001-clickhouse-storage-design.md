# ClickHouse 存储层方案（草案）

> 文档类型：设计 | 编号：DES-001 | 状态：基线 | 文档版本：v1.0 | 最后更新：2026-11-18
> 负责人：— | 关联：DEP-001、REQ-002
> 归档来源：base_v1.0 基线文档，按 DOC-STANDARD v1.0 归类命名；内容未改。

> 状态：**方案 + 实施中**。B1 基础设施已落地（`backend/app/storage/`：连接封装 + 建表 DDL + 类型/时区边界）；
> B2 的日K/enriched/标的维表**读路径已切 CK**（详见 §8.4），enriched 生成管道与其余数据集仍在 Parquet。
> 目标：用 ClickHouse 完全替换现有 Parquet + DuckDB 存储层，**页面功能与交互完全不变**。
> 配套文档：[DEP-001-clickhouse-acceptance.md](../deploy/DEP-001-clickhouse-acceptance.md)（测试与验收方案）

## 1. 背景与目标

### 现状

| 组件 | 角色 |
| --- | --- |
| Parquet 文件 | 唯一持久化载体（`data/` 下 22 类目录，Hive 式按日分区） |
| DuckDB（`:memory:`） | 仅作 Parquet 的 SQL 查询引擎，**不是数据库**，无 `.duckdb` 文件 |
| Polars | 实际读写与计算引擎（`scan_parquet` + 进程内 DataFrame 缓存） |

### 目标

1. 行情/财务/盘口等**结构化时序数据**全部落 ClickHouse
2. 数据**不迁移**，切换后从数据源重新同步，旧 Parquet 直接弃用
3. 不留兼容双实现、不留运行时开关；回退靠 git 分支
4. 页面零感知：HTTP API、SSE、响应结构、页面交互一律不变

### 不替换范围

- `data/user_data/` 下 JSON（自选、策略定义、监控规则、偏好、`secrets.json`）
- `data/logs/`、`data/job_store/`
- 策略/回测的**计算结果**改为 CK（回测结果见 §3.3、矩阵缓存见 §3.2），但策略定义等仍是文件

### 对照基准（不参与改造）

仓库外的 `tick-stock-panel-origin` 目录保留一套**旧栈可运行环境**（Parquet + DuckDB，容器 `tsp-origin`，端口 4018/4011），代码与数据均不随本次改造变动，专门用于双栈对照校准（验收文档 §16）。它不接 CK、不作开发环境。

## 2. 总体结构

```text
FastAPI 服务
   ├─ services / api / backtest / indicators      ← 业务层（不改契约）
   ├─ storage 门面（Repository 接口）              ← 本次新增的统一收口层
   │    ├─ clickhouse 读取（SQL → Arrow → Polars）
   │    └─ 写入缓冲（2s / 5000 行批量 flush）
   ├─ CacheBackend（接口）                         ← 进程内存实现，预留 Redis
   └─ ClickHouse（单节点容器，HTTP 8123）
```

**关键约束**：业务代码只依赖 storage 门面与 `CacheBackend`，不直接持有 CK 连接、不直接拼 SQL。

## 3. 表清单与字段映射

### 3.1 命名约定

| 原 Parquet 目录 | CK 表名 |
| --- | --- |
| `kline_daily/` | `kline_daily` |
| `kline_daily_enriched/` | `kline_daily_enriched` |
| `kline_index_daily/` | `kline_index_daily` |
| `kline_index_enriched/` | `kline_index_enriched` |
| `kline_etf_daily/` | `kline_etf_daily` |
| `kline_etf_enriched/` | `kline_etf_enriched` |
| `kline_minute/` | `kline_minute` |
| `kline_etf_minute/` | `kline_etf_minute` |
| `adj_factor/` | `adj_factor` |
| `adj_factor_etf/` | `adj_factor_etf` |
| `financials/{metrics,income,balance_sheet,cash_flow,shares}/` | `financials_metrics` 等 5 张 |
| `instruments/`、`instruments_index/`、`instruments_etf/` | 同名 3 张 |
| `depth5/` | `depth5` |
| `ext_data/{id}/timeseries/` | `ext_timeseries`（多扩展共用一张，带 `ext_id`），DDL 见 §3.2 |
| `ext_data/{id}/instruments` | `ext_instruments`（带 `ext_id`） |
| `instruments_ext/`、`kline_ext/` | **已废弃**：现行实现走 `ext_data/`，这两个目录只是旧路径兼容（追溯见 §8.2） |
| `cache/factor_columns/`（回测矩阵缓存） | `matrix_cache` |
| `backtest_results/`（回测运行结果） | `backtest_runs` + `backtest_trades`（见 §3.3） |
| 同步完成状态（原 `user_data/preferences.json` 的 `last_sync`） | `sync_state`（见 §3.4） |
| 视图 `kline_daily_all` 等联合视图 | CK 视图（`UNION ALL`）或查询期拼装，见 §4.6 |

**不进 ClickHouse 的目录**：

| 目录 | 去向 | 理由 |
| --- | --- | --- |
| `pools/` | `CacheBackend`（现为进程内存，将来可换 Redis） | 是远程接口结果的缓存，非本地权威数据 |
| `ai_cache/` | `CacheBackend`（同上） | AI 对话/生成结果缓存，可重建 |
| `screener_results/` | **已删除**（2026-11-15，追溯记录见 §8） | 预留但无任何写入方，删除避免"看似有数据实际空着"的误导 |
| `user_data/`、`logs/`、`job_store/` | 保持文件 | 配置与运行日志，非时序数据 |
| `strategies/`、`research/` | 保持文件 | 用户策略脚本与研究报告产物，非时序行情数据 |

### 3.2 字段级映射（依据真实 Parquet schema 提取）

类型映射总表：

| Polars | ClickHouse |
| --- | --- |
| `Utf8` | `String`（低基数列可 `LowCardinality(String)`） |
| `Float64` | `Float64` |
| `Int64` | `Int64` |
| `UInt32` | `UInt32` |
| `Boolean` | `Bool`（可空时 `Nullable(Bool)`） |
| `Date` | `Date` |
| `Datetime("us")`（北京墙钟 naive） | `DateTime64(6, 'Asia/Shanghai')` |
| `Null` 列 | `Nullable(...)` |

> **建表注意（26.10 实测）**：若该表后续要加 projection（§4.10），建表时必须带 `SETTINGS deduplicate_merge_projection_mode = 'rebuild'`，否则 `ADD PROJECTION` 直接报 `SUPPORT_IS_DISABLED`。
> 下列 DDL 按"可能需要 projection 的主表"统一带上该 setting；小表可省略。

**`kline_daily`**（实测 241 个分区，字段一致）

```sql
CREATE TABLE kline_daily (
  symbol String,
  date   Date,
  open   Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  quote_ts Nullable(Int64),          -- 实时快照帧才带, 历史同步没有该列
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date);
```

> **`quote_ts` 必须可空（实测修正）**：历史同步帧根本没有该列；实测 `data/kline_daily_enriched/date=2025-11-19` 的 `quote_ts` 为 **5571/5571 全 NULL**。写成非空 `Int64` 会让历史写入全部失败。

**`kline_daily_enriched`**（14 列存储列，实测一致）

```sql
CREATE TABLE kline_daily_enriched (
  symbol String, date Date,
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  raw_close Float64, raw_high Float64, raw_low Float64,
  turnover_rate Float64,
  consecutive_limit_ups UInt32, consecutive_limit_downs UInt32,
  quote_ts Nullable(Int64),         -- 见上方说明: 历史行为 NULL, 不能是非空列
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date);
```

> 注：`ENRICHED_STORAGE_COLS` 共 15 列（含 `symbol/date`），其余 68 列指标为**运行时计算**，不落库。

**`kline_index_daily`**：结构同 `kline_daily`。

**`kline_index_enriched` / `kline_etf_enriched`**（窄表，实测 11 列）

```sql
CREATE TABLE kline_index_enriched (
  symbol String, date Date,
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  raw_close Float64, raw_high Float64, raw_low Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date);
```

`kline_etf_daily` / `kline_etf_enriched` 同构。

**`kline_minute` / `kline_etf_minute`**

```sql
CREATE TABLE kline_minute (
  symbol String,
  datetime DateTime64(6, 'Asia/Shanghai'),   -- 北京墙钟, 口径见 §5
  open Float64, high Float64, low Float64, close Float64,
  volume Float64, amount Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(datetime)
ORDER BY (symbol, datetime);
```

> 分钟数据量大（全市场 1 年约 3 亿行），**分区按月**；写入必须走批量缓冲。
> 时区契约：现有数据为"北京墙钟 naive datetime"，落 CK 时统一 `Asia/Shanghai`，读取时按墙钟还原，不做二次换算。

**`adj_factor` / `adj_factor_etf`**（实测）

```sql
CREATE TABLE adj_factor (
  symbol String, trade_date Date,
  ex_factor Float64, dividend Float64, bonus Float64,
  allot Float64, allot_price Float64, prev_close Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(trade_date)
ORDER BY (symbol, trade_date);
```

**`instruments`**（实测）

```sql
CREATE TABLE instruments (
  symbol String, name String, code String, exchange String,
  region String, type String, listing_date String,   -- 原表即为 String
  total_shares Float64, float_shares Float64, tick_size Float64,
  limit_up Nullable(Float64), limit_down Nullable(Float64),
  as_of Date,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY symbol;
```

`instruments_index` / `instruments_etf`（额外 `asset_type`）/ `instruments_ext` 同构，均 `PARTITION BY tuple()`（小表不分区）。

**`depth5`**（定版旁路，实测字段来自 `depth_service.py`）

```sql
CREATE TABLE depth5 (
  symbol String,
  date Date,
  sealed_up Nullable(Bool), sealed_down Nullable(Bool),
  ask1_vol Nullable(Int64), bid1_vol Nullable(Int64),
  status String,
  fetched_at Float64,
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(date)
ORDER BY (symbol, date);
```

**`financials_*`（5 张，动态列）**——见 §4.1 策略 A。

**`ext_timeseries`（扩展时序数据，动态列）**

扩展数据由 `data/data_sources/*.yaml` 声明字段（`fields` + `mode: snapshot|timeseries`），列随扩展配置变化，按 §4.1 策略 A 用 `Map` 承载：

```sql
CREATE TABLE ext_timeseries (
  ext_id      LowCardinality(String),   -- 扩展配置 id
  symbol      String,
  date        Date,
  time_value  String,                   -- time_field 原值(日内多盘需精确时刻), 日频为空串
  values      Map(String, Float64),     -- 数值型扩展字段
  raw         Map(String, String),      -- 非数值字段(文本/日期串)
  updated_at  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (ext_id, symbol, date, time_value);
```

> 去重键对齐现有 `[symbol, date, time_field]` 语义；`time_value` 参与排序键而非分区键，保证同一扩展同一天的多盘数据落同一分区。

**`ext_instruments`（扩展标的表）**

```sql
CREATE TABLE ext_instruments (
  ext_id     LowCardinality(String),
  symbol     String,
  code       String,
  values     Map(String, Float64),
  raw        Map(String, String),
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY (ext_id, symbol);
```

**`financials_shares` 的期间字段**：**已核实为 `period_end`** —— `app/share_capital.py` 的 `load_share_history` 明确要求 `{symbol, period_end, float_shares}` 三列，故与其余 4 张同构，按 `ORDER BY (symbol, period_end)` 落地（见 `storage/schema.py`）。

**`matrix_cache`（回测矩阵磁盘缓存，原 `data/cache/factor_columns/<sha256>.parquet`）**

```sql
CREATE TABLE matrix_cache (
  key String,                    -- 内容寻址 sha256
  payload String,                -- Arrow IPC 字节的 base64（Arrow 的 utf8 通道存二进制会损坏字节）
  size_bytes UInt64,
  created_at DateTime64(3) DEFAULT now64(3),
  accessed_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(accessed_at)   -- 版本列用 accessed_at：touch 时新行才能胜出
PARTITION BY tuple()
ORDER BY key
TTL accessed_at + INTERVAL 30 DAY;
```

> **TTL 用 `accessed_at` 而非 `created_at`**：矩阵缓存是内容寻址的复用型缓存，同一份矩阵可能被反复读取；用创建时间做 TTL 会把"创建早但一直复用"的热数据误删，反而击穿缓存。
>
> **版本列也必须是 `accessed_at`（实现期修正）**：`touch` 用服务端 `INSERT ... SELECT payload, now64(3)` 刷新访问时间，若版本列是 `created_at`，新旧两行版本相同，`FINAL` 收敛结果不确定，刷新可能被旧行覆盖。
> 容量上限（原 `backtest_matrix_disk_cache_max_mb`）由**应用层统计 + LRU 删除**控制；CK 的 TTL 只做兜底清理（访问后 30 天未再读即淘汰）。

### 3.3 回测结果表（新增）

现有实现只落一份 summary（`run_id` / `stats_json` / `n_trades`），完整结果（`config` / `equity_curve` / `trades` / `per_symbol_stats` / `factor_attribution` / `evidence`）只在内存。改存 CK 后拆为两张表：

**`backtest_runs`（一次回测一行）**

```sql
CREATE TABLE backtest_runs (
  run_id       String,
  created_at   DateTime64(3) DEFAULT now64(3),
  strategy_id  String,                  -- 策略标识(从 config 提取, 便于按策略列表)
  config_json  String,                  -- 完整回测配置(JSON)
  stats_json   String,                  -- 全部统计指标(JSON)
  equity_curve String,                  -- 资金曲线(JSON 数组, 项目不多按需解析)
  extra_json   String,                  -- per_symbol_stats / factor_attribution / evidence
  n_trades     UInt32,
  elapsed_ms   Float64,
  error        Nullable(String),
  updated_at   DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(created_at)
ORDER BY run_id;
```

**`backtest_trades`（成交明细，结构化便于后续分析）**

```sql
CREATE TABLE backtest_trades (
  run_id String,
  seq    UInt32,                        -- 成交序号(保持原顺序)
  symbol String,
  entry_signal_date Nullable(Date),
  entry_date        Nullable(Date),
  entry_price       Nullable(Float64),
  exit_signal_date  Nullable(Date),
  exit_date         Nullable(Date),
  exit_price        Nullable(Float64),
  pnl_pct      Nullable(Float64),
  duration_days Nullable(Int32),
  exit_reason  Nullable(String),
  extra        Map(String, String),      -- 其它动态字段(JSON 值)
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY (run_id, seq);
```

设计理由：

- `run_id` 是天然主键，`ReplacingMergeTree` 天然支持"同一 run 重算覆盖"
- 常用列（`strategy_id` / `n_trades` / `created_at`）提升为原生列，支撑"回测历史列表"分页排序，不必解析 JSON
- 成交明细结构化后，"某策略历史成交分布""按退出原因统计"这类分析可直接 SQL，不必加载整份结果
- `equity_curve` 留在 JSON：点数多但只用于单次回测结果页绘制，拆表收益低

### 3.4 同步状态表 `sync_state`（替代 preferences 文件与 publishing 标记）

同步/修复类任务需要"是否真的完成了"的可信记录。原来有两处实现：`user_data/preferences.json` 的 `last_sync`（时间）与 enriched 的 `EnrichedPublication` publishing 标记文件。**全部改为一张小表**：

```sql
CREATE TABLE sync_state (
  task        LowCardinality(String),   -- kline_sync / financial_sync / adj_factor / repair ...
  status      LowCardinality(String),   -- ok / running / failed
  last_ok_at  Nullable(DateTime64(3)),
  last_error  Nullable(String),
  updated_at  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY task;
```

- **规模**：一种任务一行，全表约 10 行；不记历史、不记每次运行
- **写入**：三条 `INSERT` 覆盖同一行（`running` → `ok` / `failed`），**用"插新版本行"代替 UPDATE，避免 mutation**
- **崩溃处理**：启动时把残留 `status='running'` 的记录改写为 `failed, last_error='上次同步中断'`，**不做自动重跑**
- **读取**：页面同步状态 = `SELECT task, status, last_ok_at FROM sync_state FINAL`，一次查询
- **明确不做**：运行历史、区间（scope）字段、自动重试、定期清理

> 这条设计取代了两个旧机制：`preferences.json` 里的同步时间字段、以及 `EnrichedPublication` 的 JSON publishing 标记文件（追溯见 §8.3）。

## 4. 关键设计决策
### 4.1 动态列数据集的处理（财务 5 表 + 扩展数据）

这两类表的列随数据源返回变化（财务五表字段由 SDK 决定；扩展数据由 `data_sources/*.yaml` 的列声明决定），无法固化为静态 schema。

**策略 A（推荐）：`Map(String, Float64)` + 元数据表**

```sql
CREATE TABLE financials_metrics (
  symbol String,
  period_end Date,
  values Map(String, Float64),      -- 动态财务字段
  raw Map(String, String),          -- 非数值字段(公告日期/类型等)兜底
  updated_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYear(period_end)
ORDER BY (symbol, period_end);
```

- 读取时 `mapGet(values, 'roe')` 或在 Arrow 层展开成列，对业务层呈现与原 DataFrame 一致的宽表
- 优点：加字段零 DDL；缺点：单字段查询要扫 Map，但财务数据量小（百万级），可接受
- 边界：`period_end` / `symbol` 为固定提升列

**备选策略 B**：写入前按实际列做 `ALTER TABLE ADD COLUMN` 动态扩充——字段多时 DDL 频繁，且列顺序不稳定，**不推荐**。

### 4.2 写入语义映射（Parquet upsert → CK）

| 现有行为 | CK 实现 |
| --- | --- |
| `append_daily`（按日分区 merge-upsert） | `INSERT`，`ReplacingMergeTree` 按 `(symbol,date)` 保留最新 `updated_at` |
| `_optimistic_upsert_partition`（读-合并-重写整日分区） | 简化为**直接 INSERT 覆盖行**，不再需要"读旧分区再合并" |
| `flush_live_daily_asset`（覆写当天分区） | 同 key 重复 INSERT，查询侧 `FINAL` 收敛 |
| `replace_daily_rows`（删除部分 symbol 后写） | `ALTER TABLE ... DELETE WHERE date=? AND symbol IN (...)` + `mutations_sync=2`；修补流程写完立刻要读回，lightweight DELETE 不保证同步可见，故用同步 mutation |
| `_atomic_write_parquet`（.tmp + os.replace） | 不再需要：CK 的 INSERT 天然原子 |
| `EnrichedPublication` 发布标记 / 僵尸恢复 | **删除**。职责拆分：缓存失效的 `generation` 归 `CacheBackend`（§4.4），任务完成状态归 `sync_state` 表（§3.4）。前提：同一次发布合并为**一条 INSERT**（见下方硬约束）。**现状（2026-11-16）**：读路径已切 CK，但 enriched 仍逐日分区写 Parquet（B4 才收敛），发布对象仍承担"分区原子替换 + 孤儿标记 + generation 换新"，因此**暂不删除**；本阶段只给它加了 CK 镜像钩子（`write_parquet` 落盘后同步 `store.write_daily`），否则新指标在 CK 侧永远不可见。退场条件 = B5 删除 Parquet 写入 + 满足下方"一次发布 = 一条 INSERT" |

**写入缓冲**（新增）：所有写请求先进进程内队列，满足 `2s` 或 `5000 行` 任一条件即批量 `INSERT`；关闭服务时 flush。批量参数可配。

**写入模式：两类数据两条路径**

| 数据类别 | 路径 | 完成判定 | 崩溃后果 |
| --- | --- | --- | --- |
| 盘中实时/当日热数据（实时快照、live agg、depth5 定版、当日 enriched） | **缓存优先 + 异步落库**（写内存缓存 → bump generation → 进写缓冲） | 不写 `sync_state`；页面读缓存即时可见 | 丢 ≤ 1 个 flush 窗口的数据，下一轮行情覆盖/补拉即可 |
| 历史批量同步/修复（日K 全量、财务、除权、分钟回补、`repair_daily`） | **落库优先**（`INSERT` 成功 → 更新缓存 → 写 `sync_state=ok`） | 只有数据真正写入 CK 后才置 `ok` | 重启后读 CK 得到完整数据；`status='running'` 会被标为中断，用户可重试 |

**硬约束：一次发布 = 一条 INSERT**

同一批（同一 generation）的 enriched 数据必须合并成单条 `INSERT` 落库，不允许按日期拆成多条依次写。这样 CK 的语句级原子性足以替代原 `publishing` 标记防"撕裂"的作用；违反此约束则必须回退到"文件标记"方案（已废弃，见 §8.3）。

### 4.3 查询去重口径

- 统一走 `FINAL`（`SELECT ... FROM kline_daily FINAL WHERE ...`）
- 对超大数据量扫描（全市场多年日K，千万级）改用 `argMax` 子查询：
  ```sql
  SELECT symbol, date, argMax(close, updated_at) AS close ...
  FROM kline_daily WHERE date BETWEEN ? AND ? GROUP BY symbol, date
  ```
  由 storage 层按"是否需要去重/数据量级"自动选择策略，业务无感
- **注意**：CK 的合并是后台异步的，`FINAL` 只保证查询时可见最新版本，不保证物理去重；这正好等价于现有"读时合并"语义

### 4.4 缓存抽象（CacheBackend）—— 介质可切换

**核心要求**：缓存介质可替换（进程内存 ↔ Redis ↔ …），业务代码只依赖接口。

```python
class CacheBackend(Protocol):
    # 值类型统一用 Arrow/Polars，介质无关
    def get(self, key: str) -> pl.DataFrame | None: ...
    def set(self, key: str, df: pl.DataFrame, *, ttl_s: int | None = None) -> None: ...
    def get_json(self, key: str) -> dict | None: ...
    def set_json(self, key: str, value: dict, *, ttl_s: int | None = None) -> None: ...
    def invalidate(self, prefix: str) -> None: ...
    def generation(self, name: str) -> int: ...   # 现有 generation 快照机制
    def bump(self, name: str) -> int: ...
    def clear(self) -> None: ...
```

**介质实现**

| 实现 | 场景 | 序列化 |
| --- | --- | --- |
| `MemoryCacheBackend` | 当前默认（单进程） | 直接持有 Polars DataFrame / dict，零序列化 |
| `RedisCacheBackend` | 将来多进程 / 多实例 | **Arrow IPC**（DataFrame）+ JSON（dict），禁用 pickle |

选择方式：`CACHE_BACKEND=memory|redis`（+ `CACHE_REDIS_URL`），与存储层解耦，可独立切换。

**缓存内容与命名空间**

| 命名空间 | 内容 | 介质建议 | TTL |
| --- | --- | --- | --- |
| `enriched:*` | enriched 最新日全量、区间历史 | 内存（热路径，逐秒读） | 跟随 `generation` 失效，不设 TTL |
| `live:*` | 当日实时聚合结果 | 内存 | 同 `generation` |
| `instruments:*` | 标的清单、名称映射、历史股本 | 内存（+ Redis 可共享） | 同 `generation` |
| `pool:*` | 标的池（CSI300/中证500/上证50/全A） | 内存 →（将来）Redis | 1 天 |
| `ai:*` | AI 对话/生成结果 | 内存 →（将来）Redis | 按用途（对话 24h / 生成 7d） |

- **接口契约**：`generation/bump` 必须保证"bump 后所有 `generation(name)` 小于新值的条目视为失效"，切 Redis 后语义不变
- 页面实时性依赖缓存命中率，因此 **CK 之上必须有缓存层**，不做"每次查询直连 CK"
- 跨进程一致性：切 Redis 后 `bump` 需通过 Redis 计数器实现（而非进程内变量），保证多实例同时失效

### 4.5 screener 自定义 SQL 校验（单用户简化版）

- 仅允许单条 `SELECT`
- 禁止 `;` 多语句、`INTO OUTFILE`、`SYSTEM`、`ALTER/DROP/INSERT` 等写操作
- 禁止远程表函数：`url(`、`file(`、`s3(`、`remote(`、`mysql(`、`jdbc(`
- 保留现有标识符转义工具 `db_safe.py`
- 不建 CK 只读用户、不做权限细分（单用户场景）

### 4.6 联合视图（`kline_daily_all` 等）

现有 `UNION ALL BY NAME` 视图（stock/index/etf 合并）在 CK 侧两种做法：

| 方案 | 说明 |
| --- | --- |
| CK 视图 | `CREATE VIEW kline_daily_all AS SELECT *, 'stock' AS asset_type, ... UNION ALL ...`，一次性建好 |
| 查询期拼装 | storage 层按需拼 `UNION ALL` |

**推荐 CK 视图**：与现有 API/服务代码调用方式最接近，改造面最小。

### 4.7 测试策略

- pytest fixture：每测试生成唯一**表名前缀**（如 `t_ab12cd_`），在独立 database 内建同名带前缀表，结束统一 `DROP TABLE`
- 仓储层统一收口前缀（业务代码不感知）
- CI 需 CK 容器（compose profile 或 testcontainers）
- 现有 60+ 存储测试按"临时数据"重写（不依赖历史 Parquet 文件）

### 4.8 部署形态（镜像容器）

CK 由 compose 以官方镜像 `clickhouse/clickhouse-server:24.8.8-alpine` 启动，与面板同网络，**不依赖宿主机安装 ClickHouse**。后端启动即连 CK 且 fail-closed，因此 `depends_on` 用 `condition: service_healthy`。

| 项 | 值 |
| --- | --- |
| 镜像 | `clickhouse/clickhouse-server:24.8.8-alpine` |
| 服务名 / 容器名 | `clickhouse` / `tsp-dev-clickhouse`（dev） · `tsp-clickhouse`（生产） |
| 账号 | `tsp` / `tsp123456`（由 `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` 首启创建） |
| 库 | `tsp`（生产）、`tsp_test`（测试），由 `deploy/clickhouse/init/01-databases.sql` 预建并授权 |
| 监听 | 容器内 `8123`(HTTP) / `9000`(native)；宿主端口按需映射（dev 默认 8123，可用 `CLICKHOUSE_HTTP_PORT` 改） |
| 内存上限 | 3.5 GB（`deploy/clickhouse/config.d/40-tsp-container.xml`）+ 容器 `mem_limit: 4g`。1.5 GiB 实测在"回填 130 万行 + 并发同步 + `uniqExact` 聚合"时触发 `MEMORY_LIMIT_EXCEEDED`，故上调；机器内存 < 8 GB 时按文档下调 |
| 数据目录 | 宿主机 `./ckdata` → `/var/lib/clickhouse` |
| 时区 | `Asia/Shanghai` |

**连接方式**

```text
应用容器（compose 内） → http://clickhouse:8123   （服务名，compose 注入 CLICKHOUSE_URL）
源码直跑 / 宿主调试   → http://localhost:8123    （端口映射，CLICKHOUSE_HTTP_PORT 可改）
```

**初始化与幂等**

- `/docker-entrypoint-initdb.d` 只在数据目录为空时执行；反复 `up` / `down` 不会重建库
- 改初始化脚本或账号密码后要重新初始化：停容器并清空 `./ckdata/`（会丢失 CK 全部数据）
- 应用仍不执行 `CREATE DATABASE`（见 §4.12），库一律由部署侧预建

**单节点约束**：不做集群/副本；数据卷落宿主机，重建容器不丢数据。

**镜像版本与 DDL**：24.8.8 支持 `Map(String, Float64)`、`LowCardinality`、`FINAL`、`DateTime64(3)`、`PARTITION BY tuple()`，以及 `SETTINGS deduplicate_merge_projection_mode = 'rebuild'`（`schema.py` 为可加 projection 的主表统一带上；该 setting 在 24.8 上仅作前置声明，不改变建表结果）。升级镜像前先跑 `uv run pytest tests/test_ch_schema.py -q` 验证建表。

**旧版 Arrow 类型兼容（24.8 实测）**：CK < 25 的 Arrow 输出把 `Date` 编成 `UInt16`（自 1970 天数）、`DateTime` 编成 `UInt32`（秒），而 `Date32` / `DateTime64` 正常输出。读端没有开关可关，因此 `ClickHouseClient` 在首次查询前用一次轻量探测确认编码，命中时把 `uint16` 列按位还原成 `date32`（`_restore_legacy_date_columns`）。这依赖两条 schema 约束——**不使用 `UInt16` 列、时间列一律 `DateTime64` 而非 `DateTime`**——由 `tests/test_ch_schema.py::test_no_uint16_or_bare_datetime_columns` 守卫。另：24.8 的 TTL 表达式不接受 `DateTime64` 结果，`matrix_cache` 的 TTL 写成 `toDate(accessed_at) + INTERVAL 30 DAY`。

**从宿主实例迁移（旧形态）**

旧部署直接复用 systemd 的 `clickhouse-server`。改用容器时：

1. `docker compose -f docker-compose.dev.yml up -d clickhouse` 起容器实例
2. 停旧服务释放 8123：`sudo systemctl disable --now clickhouse-server`；继续保留则用 `CLICKHOUSE_HTTP_PORT=18123`
3. `.env` 的 `CLICKHOUSE_URL` 改为 `http://localhost:8123`（源码直跑时用；compose 内由环境变量覆盖为服务名）
4. 历史数据如需保留，从旧实例导出后用 `clickhouse-client` 导入（不提供自动迁移）

**与其它项目共用实例的历史约束**：容器化后本项目的 CK 不再与 `stock` / `zvt_*` 等库共用；测试仍在 `tsp_test` 内建带前缀表，禁止 `DROP DATABASE` 与 `SYSTEM` 级全局操作，该约束继续有效。

### 4.9 配置项（新增）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `CLICKHOUSE_URL` | `http://host.docker.internal:8123` | HTTP 端点（宿主 CK，见 §4.8） |
| `CLICKHOUSE_DATABASE` | `tsp` | 库名（测试用 `tsp_test`） |
| `CLICKHOUSE_USER` / `PASSWORD` | `tsp` / 见 `.env` | 专用用户，仅限内网网段 |
| `CH_WRITE_FLUSH_INTERVAL_MS` | `2000` | 写缓冲时间阈值 |
| `CH_WRITE_FLUSH_ROWS` | `5000` | 写缓冲行数阈值 |
| `CH_QUERY_TIMEOUT_S` | `60` | 查询超时 |
| `CACHE_BACKEND` | `memory` | 缓存介质：`memory` / `redis` |
| `CACHE_REDIS_URL` | 空 | `CACHE_BACKEND=redis` 时的连接串（当前不实现，仅占位） |

### 4.10 主键与查询模式（性能关键）

现有页面的查询可归为两类，对排序键的需求相反：

| 查询类型 | 典型场景 | 理想排序键 |
| --- | --- | --- |
| **单标的历史区间** | K线图、单票指标、回测面板（按 symbol 分组聚合） | `(symbol, date)` |
| **按日横截面扫描** | 市场概览、板块轮动、选股器、涨跌停/连板统计 | `(date, symbol)` |

**决策：主表统一 `ORDER BY (symbol, date)`**，理由：

- 回测面板是 CPU/IO 最重、最频繁的路径（每 symbol 一段连续日期），主键必须让单标的区间读是**连续块扫描**
- 横截面查询虽会扫到"全年所有 symbol"，但仍受分区（年）裁剪，且这些页面大多是**单日或小窗口**（如"今日涨停"），实际扫描量可控

**横截面性能的兜底方案（按优先级）**：

1. **Projection**（CK 24.3+ 稳定）：为 `(date, symbol)` 建投影，CK 自动路由；`ALTER TABLE kline_daily ADD PROJECTION by_date (SELECT * ORDER BY (date, symbol))`
2. Projection 不可用或代价过高时，用**专用预聚合表**（如 `market_daily_agg`）由写入路径维护
3. 最后手段：storage 层对横截面查询显式走 `argMax` 子查询 + 列裁剪

> 决策落地时不做"两套主键"的表，避免写入翻倍；先按 projection 方案验证，实测不达标再上预聚合表。

### 4.11 并发模型（旧原语取舍）

| 现有原语 | 位置 | CK 化后 |
| --- | --- | --- |
| `Semaphore(TICK_DUCKDB_READ_CONCURRENCY)` 读并发限流 | `repository.py:320-330` | **保留**，改为客户端并发上限（防打爆 CK `max_concurrent_queries`），默认值重新标定 |
| 每请求 `cursor()` 派生独立连接 | `repository.py:405` | **删除**：HTTP 无状态，`clickhouse-connect` 内部连接池自动复用 |
| DDL 锁（`_ddl_lock`） | `repository.py:345` | **简化**：仅在启动期 schema 初始化时串行执行一次，运行期不再需要 |
| `_write_lock`（写串行化） | `repository.py` | **由写缓冲队列取代**：所有写进入同一队列，天然串行 |
| `replace_with_retry`（Windows 读锁重试） | `repository.py:54` | **删除**：无文件替换动作 |
| MVCC 读快照（cursor 隔离） | 全文多处 | **不需要**：CK 每次查询读已提交数据；跨查询一致性由 `updated_at` 版本 + 缓存 generation 保障 |

### 4.12 启动、Schema 版本与降级

**启动序列**

```text
1. 连接 CK（重试退避：最多 60s，每 2s 一次）
2. 目标库必须已存在：`tsp` / `tsp_test` 由部署预建，应用**不执行** `CREATE DATABASE`（共用实例上 `tsp` 用户按库授权，不应给应用建库/删库能力）
3. 读 schema_version 表（不存在则创建）
4. 逐条执行幂等 DDL（CREATE TABLE IF NOT EXISTS / CREATE VIEW OR REPLACE）
5. 版本落后时执行迁移脚本（按版本号顺序，事务性靠幂等 DDL 保证）
6. 建立写入缓冲、预热缓存，开始对外服务
```

**`schema_version` 表**

```sql
CREATE TABLE schema_version (
  version    UInt32,
  applied_at DateTime64(3) DEFAULT now64(3),
  note       String
) ENGINE = MergeTree ORDER BY version;
```

DDL 与迁移脚本集中在 `storage/schema.py`（版本号 + 语句列表），禁止散落在业务代码里。

**故障语义（必须明确，避免"页面无感"变成"静默错误"）**

| 场景 | 行为 |
| --- | --- |
| 启动时 CK 不可达（重试 60s 后） | **启动失败**并打印明确错误（fail-closed），不进入半可用状态 |
| 运行期查询失败 | API 返回 5xx + 明确错误信息；页面显示错误提示，**不回退为空数据**（空数据会让用户误判为"没有行情"） |
| 运行期写入失败 | 写缓冲重试 3 次后丢弃该批并记录 error 日志 + `/api/health` 置 degraded |
| 缓存命中 | 不受 CK 故障影响，仍可服务已缓存内容（如 enriched 最新日） |
| `/api/health` | 增加 CK `ping` 与写入队列积压指标 |

## 5. 类型与语义对齐检查清单

替换时必须逐项确认"行为等价"，这是页面无感的真正保障：

| 项 | 现有实现 | CK 实现要点 |
| --- | --- | --- |
| 涨跌幅口径 | 小数制（0.05 = 5%） | 存储层不做任何单位换算 |
| 复权 | 前复权存 `close`，原始价存 `raw_*` | 同上，禁止在 SQL 里做复权变换 |
| 时区 | 北京时间，分钟为北京墙钟 naive | `DateTime64(6,'Asia/Shanghai')`，读写均不二次换算 |
| 交易日 vs 自然日 | 交易日历在业务层 | CK 只存日期，不做日历推算 |
| 空值 | Float64 可空 / UInt32 非空 | 严格映射 `Nullable`，避免 0 与 NULL 混淆 |
| 排序 | 各 API 自行 `sort` | storage 层返回顺序与现有实现一致（按 `symbol,date` 升序） |
| 分页 | API 层 limit/offset | SQL `LIMIT/OFFSET` 语义一致 |

## 6. 风险与待定项

| 编号 | 事项 | 说明 |
| --- | --- | --- |
| R1 | 全市场分钟数据体量 | 3 亿行级别，需验证写入吞吐与查询延迟；必要时分钟表单独提高压缩/分区粒度 |
| R2 | `DELETE` mutation 异步 | `mutations_sync=2` 会同步等待，高频删除场景需评估开销 |
| R3 | `FINAL` 性能 | 大范围扫描时 `FINAL` 代价高，必须有 `argMax` 备选路径 |
| R4 | 动态列（财务/扩展）查询表达力 | `Map` 查询不如原生列，需确认财务因子链路（`backtest/fundamentals.py`）改造量 |
| R5 | 内存缓存与 CK 一致性 | 进程内缓存失效时机需重新校准，避免"CK 已更新、缓存仍旧" |
| R6 | 横截面查询性能 | 主键 `(symbol, date)` 对"按日扫全市场"不利，需靠 projection 或预聚合表兜底（§4.10），实测不达标要提前调整 |
| R7 | CK 故障时的用户体验 | 必须 fail-closed + 明确报错，不能静默返回空数据（§4.12） |
| D1 | 周边数据归属 | **已定**：`pools`/`ai_cache` 走 `CacheBackend`（内存→Redis）；`backtest_results` 入 CK（`backtest_runs`+`backtest_trades`）；`screener_results` 已删除（追溯见 §8） |
| D2 | 批次划分（B1~B5） | **待定**，后续专门讨论 |
| D3 | B2 完成标准（是否含一次完整回测） | **待定**，后续专门讨论 |
| D4 | 缓存介质切换时机 | 当前 `MemoryCacheBackend`；Redis 实现留接口，按需接入 |
| D5 | 同步状态与完成语义 | **已定**：用 `sync_state` 小表（§3.4），**不再使用任何文件标记**（`preferences.json` 的同步时间、`EnrichedPublication` 的 publishing 标记均废弃） |
| D6 | 写入模式 | **已定**：实时类缓存优先 + 异步落库；批量同步/修复类落库优先（§4.2） |

## 7. 附：与现有代码的对应关系

| 现有文件 | 改造后 |
| --- | --- |
| `backend/app/tickflow/repository.py`（DataStore + KlineRepository，2853 行） | 拆为 `storage/` 门面 + `clickhouse` 实现；DataStore 只保留目录/配置职责或删除 |
| `backend/app/parquet.py`（schema 常量 + `scan_*_parquet`） | 转为列类型常量，供 DDL 与 Arrow 转换复用；`scan_*` 删除 |
| `backend/app/db_safe.py` | 保留（标识符转义） |
| `backend/app/services/screener.py`（隔离 DuckDB 连接） | 改走 storage 层 SQL 执行 + §4.5 校验 |
| 35 个直接 `read_parquet/scan_parquet` 的 service/backtest/indicators 文件 | 全部收敛到 storage 门面 |
| `backend/app/factors/materialize_cache.py`（磁盘 parquet 缓存） | 改走 `matrix_cache` 表 |
| `backend/app/tickflow/pools.py`（标的池 parquet 缓存） | 改走 `CacheBackend`（`pool:*` 命名空间，TTL 1 天） |
| `backend/app/services/backtest.py`（`_persist` 写 summary parquet） | 改走 `backtest_runs` + `backtest_trades` 表 |
| AI 结果缓存（当前无实现） | 新增时走 `CacheBackend`（`ai:*` 命名空间） |
| `backend/app/tickflow/repository.py:130-158` 中的 `screener_results` 目录创建 | 删除（追溯见 §8） |
| `backend/app/api/data.py:580`、`:726-731` 中的 `screener_results` 磁盘统计 | 删除（追溯见 §8） |
| `backend/app/enriched_generation.py:396` `EnrichedPublication`（发布标记/心跳/僵尸恢复） | 删除文件形态；`generation` 迁入 `CacheBackend`（追溯见 §8.3） |
| `backend/app/services/preferences.py` 的 `last_sync` 同步时间 | 改存 `sync_state` 表（追溯见 §8.3） |

## 8. 变更与追溯记录

记录本次存储层重构中"删除/替换"的既有设计，便于将来回溯为什么改、怎么恢复。

### 8.1 `screener_results/` 目录（2026-11-15 删除）

| 项 | 内容 |
| --- | --- |
| 原位置 | `DataStore.__init__` 的 22 个关键子目录之一（`repository.py:130-158`）；`api/data.py` 的磁盘占用统计列表 |
| 原设计意图 | 预留给"筛选器（screener）运行结果落盘"（从命名与 `backtest_results` 的对称性推断） |
| 实际状态 | **全库无任何写入代码**——grep `screener_results` 仅命中"目录创建"与"磁盘统计"两处，筛选结果实际只走 API 响应，不落盘 |
| 删除原因 | 空目录参与磁盘统计会误导用户以为有数据；存储层重构时并入清理范围 |
| 恢复方式 | 若将来要做"筛选结果历史"，参照 §3.3 `backtest_runs` 设计同构表（`screener_runs` + 可选明细表），走 CK 而非文件 |
| 影响面 | 无（无读写方）；仅需同步移除目录创建与统计引用 |

### 8.2 `instruments_ext/` + `kline_ext/` 旧路径（2026-11-15 标记废弃）

| 项 | 内容 |
| --- | --- |
| 原位置 | `repository.py:145-146` 目录创建；`:202-205` 视图注册；`api/ext_data.py:380-392`、`:1726-1736` 旧路径兼容读取 |
| 原设计意图 | 扩展数据的早期存储形态：`instruments_ext/` 存扩展标的表、`kline_ext/` 存扩展时序 |
| 实际状态 | 现行实现已迁移到 `data/ext_data/{id}/`（每扩展独立目录，支持多扩展并存）；`api/ext_data.py` 明确把这两个目录标注为"兼容旧路径"，仅在新目录不存在时才回退读取 |
| 处理方式 | CK 化时**不再建同名表**，改为 `ext_timeseries` + `ext_instruments`（带 `ext_id`）；旧目录读取分支一并删除 |
| 恢复方式 | 若需支持老数据回读，可临时把旧目录映射为只读导入任务，导入到 `ext_timeseries` 后删除 |
| 影响面 | 仅影响"很久以前导入过扩展数据且从未升级"的本地数据（当前环境这两个目录为空） |

### 8.3 文件型"发布标记 + 同步时间"（2026-11-15 废弃）

| 项 | 内容 |
| --- | --- |
| 原位置 | `backend/app/enriched_generation.py:396` `EnrichedPublication`（publishing 标记 + 心跳 + `recover` 僵尸恢复）；`backend/app/tickflow/repository.py:2603-2690` commit/abandon 调用；`backend/app/services/preferences.py` 的 `last_sync` 时间（落 `data/user_data/preferences.json`） |
| 原设计意图 | ① enriched 数据按交易日**逐分区**写，写入期间读者可能读到"半新半旧" → 用文件标记让读者 fail-closed；② 进程崩溃后靠心跳超时判定僵尸并自愈；③ 同步完成时间给页面展示 |
| 实际状态 | 有效机制，但强耦合文件系统；CK 化的 INSERT 具备语句级原子性后，"防撕裂"的原始诉求大幅减弱 |
| 处理方式 | **全部删除文件形态**：`generation` 语义迁入 `CacheBackend`（缓存失效仍需它），同步状态迁入 `sync_state` 表（§3.4）。CK 侧以"一次发布 = 一条 INSERT"承接原子性（§4.2 硬约束） |
| 恢复方式 | 若未来出现"必须跨多条 INSERT 才能完成一次发布"的场景，需要重新引入两阶段提交（可在 `sync_state` 增加 `preparing` 状态），而不是回退到文件标记 |
| 影响面 | 页面同步状态改读 `sync_state`（一次 `SELECT`）；enriched 缓存失效改由 `CacheBackend.bump` 驱动；桌面版/多进程场景下不再有文件锁竞争 |

### 8.4 过渡期双写（2026-11-16）

| 项 | 内容 |
| --- | --- |
| 原位置 | `repository.py` 的 6 个日K写入口（`_write_daily_partition` / `merge_live_daily_asset` / `merge_live_enriched_asset` / `flush_live_daily_asset` / `flush_live_enriched_asset` / `replace_daily_rows`）与 `kline_sync.sync_adj_factor` |
| 原意图 | 日K/除权只写 Parquet 分区，页面从 DuckDB 视图读 |
| 过渡做法 | 上述入口在写完 Parquet 后**再镜像写一次 CK**（`_mirror_daily_to_ck` / `_mirror_adj_to_ck`）；CK 未初始化时跳过并保留 Parquet 链路 |
| 为什么允许双写 | B2 的读路径仍靠 DuckDB 视图，若只切写会让页面立刻读不到数据；先让 CK 积累可校验的真实数据，再一次性切读，才能保证每一步都可运行、可回滚 |
| 与 §4.2「不留双实现」的关系 | §4.2 禁止的是**长期**双实现。本节是明确的过渡态，Phase 2（读切 CK）完成后必须删除 Parquet 写入与本记录，届时 `_mirror_daily_to_ck` 的成功路径变为唯一写路径且改为 fail-closed |
| 回填策略 | 不迁移历史数据（§1 目标 2）。CK 从双写启用时刻起积累，Phase 2 切读前按需重新同步历史区间 |
| 验证 | `tests/storage/test_daily_mirror.py`：6 张表写入/收敛/列裁剪/修补删除、无 CK 时降级 |
| 现状（2026-11-16 第三段） | **读路径已切 CK**：日K/enriched/index/ETF 的仓储读点（四个 `_scan_*`、`_refresh_enriched_impl` 最新日+300 天窗口、`_restore_missing_latest_rows`、`_build_live_agg_from_storage`、`_refresh_etf/index_enriched`、`_latest_enriched_date`、`symbols_lagging`）全部走 `ClickHouseStore`；实测 enriched 启动缓存 3.1s / 5571 只 / 109 万行历史，与 Parquet 同日 close 逐值一致 |
| 现状（2026-11-16 第四段） | **生成侧与分钟读也切 CK**：`indicators/pipeline.py` 的日K/除权/维表/instruments/基准指数全部走门面，日期集合用 `list_daily_dates`；`EnrichedPublication.write_parquet` 增加 CK 镜像（否则新指标在 CK 不可见）；分钟四个读方法与日期边界走 `read_minute` / `minute_bounds` / `list_minute_dates`，`_write_minute_partition` 落盘后镜像 `incoming` |
| 完成标志 | 删除 Parquet 写入、删除本记录、`grep -rn "read_parquet" app/` 为 0（B4/B5）。读切完成后 Parquet 写入只服务尚未迁移的散点读取方，CK 双写失败按 error 级日志暴露 |

### 8.5 记录规范

- 后续若再删除/替换既有目录、表、缓存键，均在 §8 追加小节：**原位置 / 原意图 / 实际状态 / 删除原因 / 恢复方式 / 影响面**
- 每条记录带日期，正文其它章节只留结论并指向本节，避免文档正文被历史细节打断
