# ClickHouse 修补契约设计

> 文档类型：设计 | 编号：DES-002 | 状态：主体已落地（P0~P4），尾巴待验收 | 文档版本：v1.6 | 最后更新：2026-11-19
> 负责人：— | 关联：REQ-002、ISS-002、DES-001、DEP-001、ISS-005（未闭环项汇总）

> 状态说明（2026-11-19 更新）：P0~P4 已于 2026-11-18~19 落地——`daily/enriched` 在显式 storage 下的在线读写（修补、校验、universe、常规写入、live merge/flush、状态统计与 generation）已 CK-only，本地 Parquet 仅留离线兼容（REQ-002 §13.16、ISS-002 处理进展）。
> 仍属待办：`pipeline_recompute` 常规 enriched 编排、派生缓存与离线迁移工具保留本地 Parquet 路径（ISS-003 §3.3）；B4/B5 尾巴与 B6 人工验收见 ISS-005。
> §2.3 保留 2026-11-18 首次原型因 34 个测试失败回退的原因，作为设计依据，不代表当前实现状态。

## 1. 背景与目标

### 1.1 背景

DES-001 的目标是让 ClickHouse 成为结构化行情数据的唯一持久化载体，不保留运行时双栈开关。当前 B1~B3 主体功能已落地，B5 也已把分钟、盘口、复权因子、财务表、维表 `instruments` 的在线读写收敛为 CK-only。

但 `daily/enriched` 仍保留本地 Parquet + CK 双写。原因是修补体系仍依赖本地分区：

- `replace_daily_rows` 依赖本地分区的备份文件、指纹校验和原子写；
- `daily_repair_nontrading_symbols` 依赖本地分区确认零成交/旧快照；
- `daily_repair_universe` 依赖本地历史分区和本地维表发现回补清单；
- `replace_enriched_rows` 与 `enriched_generation` 的 publication 标记依赖本地文件；
- `repair_daily.py` 的修补结果通过本地分区传给下游。

2026-11-18 曾实现 daily 修补 CK 分支原型：从 CK 读当日数据、删除后写回，并把非交易确认和 universe 发现改为 CK 读。结果在精确修补、历史发现、完整性 API 和 S9 场景共 34 个用例失败。原因不是业务规则错误，而是这些测试与本地备份、指纹、publication marker、staging 文件深度绑定。

### 1.2 目标

1. 定义 CK 原生的修补契约，替代本地文件备份/指纹/publication 标记。
2. 把 `daily/enriched` 在线写路径切换为 CK-only。
3. 保留 `repair_daily.py` 的 staging 文件作为临时输入，但不再把本地分区作为最终存储。
4. 页面 API、SSE、响应结构和交互保持不变。
5. 不引入运行时双栈开关；回退依靠 git 分支和 CK 快照/审计记录。

### 1.3 不做什么

- 不迁移旧 Parquet 数据；切换后从数据源重新同步。
- 不改变外部 HTTP API 契约。
- 不把 `data/user_data/` 下 JSON 文件迁 CK。
- 不在生产代码中保留“CK/Parquet 双写开关”。
- 不为了兼容测试而在生产路径保留本地分区写入。

## 2. 现状与问题

### 2.1 当前本地依赖

| 能力 | 当前实现 | 问题 |
| --- | --- | --- |
| 修补备份 | `shutil.copy2` 备份 `part.parquet` | CK 更新没有文件可备份 |
| 指纹校验 | 本地文件 `mtime_ns/size` 冲突检测 | CK 用 ReplacingMergeTree，不是文件级并发 |
| 非交易确认 | 扫描本地分区的 `volume/amount/quote_ts` | 在线数据只在 CK 时无法确认 |
| 历史 universe | 扫描本地前后分区和维表 | 需要 CK 按日期范围/列查询 |
| enriched publication | 本地 `.matrix_generation_*.json` | CK-only 后 marker 文件失去权威数据源 |
| 测试断言 | 大量断言 `part.parquet`、backup 文件、marker 文件 | 切 CK 后契约失配 |

### 2.2 原型失败原因

原型把以下函数直接改成 CK 读写：

- `replace_daily_rows`
- `daily_repair_nontrading_symbols`
- `daily_repair_universe`
- `validate_daily_repair_target`

失败集中在：

- `backend/tests/test_precise_daily_repair.py`
- `backend/tests/test_daily_repair_history_discovery.py`
- `backend/tests/test_data_integrity_api.py`
- `backend/tests/scenarios/test_s9_integrity.py`

失败断言验证的是本地文件系统语义，而不是 CK 数据语义。例如：

- 备份文件是否存在；
- 备份字节是否一致；
- 本地分区 glob 是否发现旧文件；
- publication marker 是否从 `publishing` 变为 `ready`。

### 2.3 结论

不能直接替换读写函数。必须先定义 CK 原生修补契约：

1. 审计表：记录修补前后差异；
2. 快照表：保存修补前完整旧数据，用于回滚；
3. generation 表：承载 enriched 发布状态；
4. 测试契约：从文件断言迁移到 CK 断言；
5. 最后才切生产写路径。

## 3. 总体方案

### 3.1 权威数据源

```text
上游数据源 / staging 文件（临时输入）
        ↓ 校验
ClickHouse 业务表（唯一权威存储）
        ↓
CK 修补审计 / 快照 / generation 元数据表
```

staging 文件继续作为修补任务的临时输入，但不再作为页面读取源，也不再作为最终分区存储。

### 3.2 写入原则

- 在线路径：`CK only`。
- 离线脚本：可显式读写 staging 文件，但结果通过 CK 门面落库。
- 无 `storage` 注入的兼容测试：可保留本地 parquet 路径，仅用于离线工具测试。
- 显式注入 `storage` 的在线测试：必须只验证 CK。
- 不允许通过配置开关在 CK/Parquet 之间切换生产写入目标。

### 3.3 数据写入顺序

一次 CK 修补按以下顺序执行：

```text
begin_repair
  → 读取修补前数据
  → 写 ck_repair_snapshot（旧数据全量）
  → 计算修补后数据
  → 写 ck_repair_audit（差异行 before/after）
  → 删除 CK 中受影响行
  → 写入修补后行
  → commit_repair
```

失败时写 `failed` 状态，并以快照作为回滚依据。

## 4. 数据模型

以下表名在本文中使用 `ck_` 前缀，便于与业务事实表区分；实现时按 DES-001 的命名规范可在 DDL 阶段最终确认。

三张元数据表统一包含 `id`、`created_by`、`created_at`、`updated_by`、`updated_at`。ClickHouse 不提供强制唯一约束，`id` 由 `generateUUIDv4()` 生成并作为技术主键；`ORDER BY` 表达物理稀疏主索引，不替代业务去重键。

### 4.1 修补审计表 `ck_repair_audit`

用途：记录“改了什么”，用于追溯和对账。

```sql
CREATE TABLE IF NOT EXISTS ck_repair_audit
(
    id             UUID DEFAULT generateUUIDv4(), -- 技术主键
    repair_id      String,                        -- 业务修补单号
    dataset        LowCardinality(String),
    day            Date,
    symbol         String,
    change_type    LowCardinality(String), -- insert / update / delete / rollback
    before_json    Nullable(String),
    after_json     Nullable(String),
    reason         LowCardinality(String),
    status         LowCardinality(String), -- started / committed / failed
    created_by     LowCardinality(String),
    created_at     DateTime64(3) DEFAULT now64(3),
    updated_by     LowCardinality(String),
    updated_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (dataset, day, repair_id, symbol, id);
```

说明：

- 审计表是追加写，不做 in-place update。
- `id` 为技术主键，由 `generateUUIDv4()` 生成；ClickHouse 不强制唯一约束，写入方不得复用 `id`。
- `created_by/created_at` 记录首次写入人和时间；`updated_by/updated_at` 记录最近一次状态变更人和时间。追加写时两者相同。
- JSON 字段存单行数据的序列化结果，字段顺序由存储层固定。
- `symbol='*'` 表示整日修补，仍需在差异行里记录真实 symbol。
- `status` 用于识别中断任务：长时间 `started` 视为异常。

### 4.2 修补快照表 `ck_repair_snapshot`

用途：保存修补前的完整旧数据，作为回滚源。

```sql
CREATE TABLE IF NOT EXISTS ck_repair_snapshot
(
    id             UUID DEFAULT generateUUIDv4(), -- 技术主键
    repair_id      String,                        -- 业务修补单号
    dataset        LowCardinality(String),
    day            Date,
    symbol         String,
    payload_json   String,
    created_by     LowCardinality(String),
    created_at     DateTime64(3) DEFAULT now64(3),
    updated_by     LowCardinality(String),
    updated_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (dataset, day, repair_id, symbol, id);
```

说明：

- 快照是修补前受影响范围的完整旧行，不是仅差异行。
- `id` 为技术主键；`repair_id` 关联同一次修补的审计记录。
- `created_by/created_at` 记录快照采集人和时间；`updated_by/updated_at` 记录最近一次维护人和时间。
- 回滚按 `repair_id` 读取快照，删除当前数据后写回快照。
- 需要设置保留期，例如 30 天；过期清理任务独立实现。
- 快照写入成功前，不允许删除业务表中的旧数据。

### 4.3 enriched generation 表 `ck_enriched_generation`

用途：替代本地 `publishing` / `.matrix_generation_*.json` 标记，承载 enriched 发布状态和缓存失效依据。

```sql
CREATE TABLE IF NOT EXISTS ck_enriched_generation
(
    id             UUID DEFAULT generateUUIDv4(), -- 技术主键
    asset_type     LowCardinality(String), -- stock / etf / index
    generation_id  String,                 -- 业务 generation 单号
    state          LowCardinality(String), -- publishing / ready / failed
    changed_from   Nullable(Date),
    heartbeat_at   DateTime64(3),
    committed_at   Nullable(DateTime64(3)),
    created_by     LowCardinality(String),
    created_at     DateTime64(3) DEFAULT now64(3),
    updated_by     LowCardinality(String),
    updated_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY asset_type
ORDER BY (asset_type, generation_id);
```

说明：

- 查询时使用 `FINAL` 取每个 `generation_id` 的最新版本。
- `id` 为每次状态写入的技术主键；`generation_id` 是业务 generation 单号，`ORDER BY (asset_type, generation_id)` 作为 ReplacingMergeTree 的归并键，不能把 `id` 放进归并键，否则同一 generation 的多次状态写入无法互相替换。
- `created_by/created_at` 记录 generation 创建人和时间；`updated_by/updated_at` 记录最近一次心跳/提交/失败写入人和时间，`updated_at` 同时作为版本号。
- 当前 generation 定义为 `state='ready'` 且 `updated_at` 最大的一行。
- `heartbeat_at` 超时后，新的写者可以接管并写新的 generation。
- 缓存层把当前 `generation_id` 纳入 enriched 缓存键；generation 变化即缓存失效。
- 不在 enriched 事实表中增加 `generation_id` 列；事实表已有 `updated_at`，`changed_from` 足以驱动缓存范围失效。

## 5. 模块设计

### 5.1 存储层接口

以下为拟新增接口，当前尚未实现。

```python
def begin_repair(dataset: str, day: date, reason: str, actor: str) -> str: ...
def append_repair_snapshot(repair_id: str, dataset: str, day: date, rows: pl.DataFrame, actor: str) -> None: ...
def append_repair_audit(repair_id: str, dataset: str, day: date, diffs: pl.DataFrame, actor: str) -> None: ...
def set_repair_status(repair_id: str, status: str, actor: str) -> None: ...
def read_repair_snapshot(repair_id: str) -> pl.DataFrame: ...
def rollback_repair(repair_id: str, actor: str) -> None: ...

def begin_generation(asset_type: str, actor: str, changed_from: date | None) -> str: ...
def heartbeat_generation(asset_type: str, generation_id: str, actor: str) -> None: ...
def commit_generation(asset_type: str, generation_id: str, actor: str) -> None: ...
def fail_generation(asset_type: str, generation_id: str, actor: str) -> None: ...
def current_generation(asset_type: str) -> dict: ...
```

### 5.2 仓储层变化

`KlineRepository` 在显式持有 `storage` 时走 CK 权威路径：

- `replace_daily_rows`：CK 读旧数据 → 写快照/审计 → 删除 → 写入 → 提交。
- `replace_enriched_rows`：同样走 CK 权威路径，并更新 generation。
- `daily_repair_nontrading_symbols`：从 CK 读 `symbol/volume/amount/quote_ts`。
- `daily_repair_universe`：从 CK 按日期读历史 symbol，并结合 CK 维表。
- `validate_daily_repair_target`：CK 路径只校验表名和日期范围，不再校验本地路径。
- `_write_daily_partition`：上述契约和测试迁移完成后，改为 CK-only。

无显式 `storage` 时仅用于离线工具/兼容测试，可继续保留本地 Parquet 路径。

### 5.3 repair_daily.py

`repair_daily.py` 的职责调整为：

```text
拉取/生成 staging 文件
  → 校验 staging
  → 调用 repository 的 CK 修补方法
  → 记录审计/快照/generation
```

staging 文件继续保留，因为它解决的是“修补输入如何临时落地”的问题，不是“最终数据存在哪里”的问题。

### 5.4 enriched generation

enriched 生成流程调整为：

```text
begin_generation(publishing)
  → 写 enriched CK 表
  → 更新 changed_from
  → commit_generation(ready)
  → 缓存层读取 current_generation 失效
```

失败时：

```text
fail_generation(failed)
  → 保留上一个 ready generation
  → 不清除已写入但标记为 failed 的数据，等下次生成覆盖
```

## 6. 备选方案与取舍

### 6.1 继续保留本地文件备份

不采纳。
它会让 CK-only always 有本地旁路，违背 DES-001 的“不留兼容双实现”。

### 6.2 只做审计表，不做快照表

不采纳。
审计表能看出改了什么，但不能可靠恢复。误修补仍需要回滚能力。

### 6.3 审计和快照合并为一张表

不采纳。
审计需要轻量查询，快照需要完整旧值。合并会让常见审计查询扫描大量 JSON 快照，成本和语义都差。

### 6.4 在 enriched 事实表增加 generation_id 列

当前不采纳。
改用独立 generation 表 + `changed_from`，避免改所有 enriched 表的读列契约。若后续需要行级 generation 过滤，再单独评审。

## 7. 风险

1. **CK 无跨表事务**：审计、快照、事实表写入不能原子提交。用 `status` 和对账任务补偿。
2. **快照膨胀**：修补频繁时快照表增长快，需要保留期和清理任务。
3. **generation 竞争**：多进程同时 publishing 时，需要用 `updated_at` 版本和心跳超时接管。
4. **测试迁移成本**：当前修补测试深度绑定本地文件，必须先迁契约。
5. **离线工具兼容**：`repair_daily` 的 staging 仍存在，容易误认为仍在双写；需要在代码注释和文档中明确 staging 不是 storage。

## 8. 验收 / 测试点

### 8.1 首批迁移的 8 个契约测试

以下用例从本地文件断言迁移到 CK 断言：

1. `backend/tests/storage/test_daily_mirror.py::test_replace_daily_rows_mirrors_delete_and_write`
2. `backend/tests/scenarios/test_s9_integrity.py::test_s9_integrity_scan_repair`
3. `backend/tests/test_precise_daily_repair.py::test_repair_fetches_only_bad_symbol_and_day_and_preserves_other_rows`
4. `backend/tests/test_precise_daily_repair.py::test_empty_source_removes_only_confirmed_nontrading_residue_with_snapshot`
5. `backend/tests/test_precise_daily_repair.py::test_mixed_real_repair_and_nontrading_cleanup_preserve_healthy_rows`
6. `backend/tests/test_daily_repair_history_discovery.py::test_discovery_registers_old_partition_and_retry_keeps_narrow_scope`
7. `backend/tests/test_data_integrity_api.py::test_api_repair_reports_old_history_and_next_attempt_repairs_it`
8. `backend/tests/test_enriched_range_invalidation.py::test_matrix_cache_respects_coverage_and_historical_corrections`

### 8.2 阶段划分

| 阶段 | 内容 | 退出条件 |
| --- | --- | --- |
| P0 设计 | 评审本文 | 表名/字段/流程确认 |
| P1 能力 | 建表、协议接口、ClickHouseStore 实现 | 存储层单测通过 |
| P2 契约迁移 | 迁移上述 8 个测试 | 8 个用例以 CK 断言通过 |
| P3 生产切换 | `replace_daily_rows` / `replace_enriched_rows` / `_write_daily_partition` 切 CK-only | 全量回归通过，无本地分区写入 |
| P4 清理 | 删除本地 backup/fingerprint/marker/glob 依赖 | B4/B5 grep 收敛，文档更新 |

### 8.3 完成标准

- `daily/enriched` 在线路径不再写 `part.parquet`。
- 修补动作总能对应到 `ck_repair_audit` 记录。
- 修补前数据总能通过 `ck_repair_snapshot` 回滚。
- enriched 缓存失效由 `ck_enriched_generation` 驱动。
- 全量测试通过，且不生成 `backend/MagicMock/`。
- REQ-002 / ISS-002 / DEP-002 状态同步更新。

## 9. 待决事项

- 快照保留期具体取 30 天还是更长。
- `ck_` 前缀是否保留，还是与 DES-001 现有业务表命名统一为无前缀。
- 审计 JSON 字段是否使用 `String` 还是 `Map(String, String)`。
- generation 心跳超时阈值。
- `repair_daily.py` 的 staging 目录是否纳入统一清理策略。

## 10. 实现状态（2026-11-18）

- **P0 设计**：本文已形成完整契约，包含标准审计字段（`id` / `created_by` / `created_at` / `updated_by` / `updated_at`）。
- **P1 能力已完成**：
  - `backend/app/storage/schema.py` 已注册 `ck_repair_audit`、`ck_repair_snapshot`、`ck_enriched_generation` 三张表。
  - `StorageProtocol` / `ClickHouseStore` 已新增修补审计、快照回滚和 generation 生命周期接口。
  - `backend/tests/storage/test_repair_contract.py` 覆盖快照回滚、标准审计字段和 generation 生命周期，定向测试 `3 passed`。
  - `tests/test_ch_schema.py` 已纳入三张新表的 schema 契约。
- **P2a 已完成（CK 只读复核）**：`data_integrity` 新增 `inspect_daily_frame` 与 `scan_recent_integrity_ck`；`integrity_check` / `repair_daily` 在显式 `storage` 下用 CK 数据做扫描、已知问题复核和修补后校验；本地路径仍作为无 storage 时的兼容实现。
- **P2b 已完成（8 个优先契约测试）**：§8.1 的 8 个用例已全部迁移到 CK 数据 / 审计 / 快照断言并通过。日K修补相关两个文件 `67 passed`，其余优先契约文件 `41 passed`；另把 `test_integrity_self_check.py` 的 backup 用例迁为快照断言，完整性相关测试 `70 passed`。
- **P3 已完成（显式 storage 下在线写路径 CK-only）**：
  - `replace_daily_rows` 走 CK 权威路径：读取整日数据保留其他股票、写快照与审计、删除目标行后写回，并用写入前重读替代本地 fingerprint 做并发校验。
  - `validate_daily_repair_target` / `daily_repair_universe` / `daily_repair_nontrading_symbols` 从 CK 表与维表发现、确认修补范围；重算前校验也改读 CK 原始日K。
  - `replace_enriched_rows` 与 `_write_daily_partition`、live merge/flush 在显式 `storage` 下只写 CK 事实表；股票/ETF enriched 写入同步登记 `ck_enriched_generation` 的 `publishing / heartbeat / ready / failed`。指数 enriched 暂无 CK 表，保留本地兼容。
  - 状态页轻量统计、完整性报告状态与矩阵缓存 generation 均改为 CK 优先；`get_matrix_data_generation` 消费 `current_generation`。
  - `ClickHouseStore.write` 为未显式给 `updated_at` 的写入生成严格递增的毫秒版本，避免同毫秒替换结果不确定。
- **P4 主体完成，离线兼容保留**：显式 `storage` 路径不再依赖本地 backup/fingerprint/marker/glob；无 `storage` 的离线工具/单测仍保留 parquet 兼容路径（DES-002 §3.2 约定）。`SCHEMA_VERSION` 无需因三张元数据表升级，新增表由幂等 `ensure_schema` 覆盖。
- 剩余：`pipeline_recompute` 的常规 enriched 编排与派生缓存（pools/regime/mainline/mining/factors）仍写本地 parquet/marker，离线工具同理；指数 enriched 表尚未纳入 CK。
- 验证：定向测试全绿；全量回归 `4820 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors。改动文件 Ruff 全绿，`git diff --check` 通过。
