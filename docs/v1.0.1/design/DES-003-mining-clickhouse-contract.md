# mining CK 化契约设计（MN-01：run 状态 / 事件 / 摘要）

> 文档类型：设计 | 编号：DES-003 | 状态：决策已拍板并部分落地，MN-02/03 待处理 | 文档版本：v1.3 | 最后更新：2026-11-19
> 负责人：待分配 | 关联：ISS-004（MN-01~MN-05）、DES-002、DES-004（D-01 决策依据）、ISS-003 §7.5、REQ-002、ISS-005（未闭环项汇总）

## 0. 文档状态与边界（先读）

**决策状态（2026-11-19）**：D-01 已拍板方案 C 并落地——run 状态机权威在 Redis（`RedisMiningRunStore`，Lua 原子状态机），CK 承担审计与历史查询；跨实例取消经 `coordination.cancel_flags` 验证闭环（ISS-004 MN-01、DIST-04）。这不等于全文已实现：**MN-02（artifact 归属）/ MN-03（claim 与指纹）仍为明确待处理项**，本文定义的 CK 表/读写路径按"已决策 + 部分实现"理解，不得假定 artifact/claim 已迁移。

范围（MN-01）：把 run 的 **manifest（状态机）、compact summary、事件流、周度 claim 与数据指纹** 的权威副本迁到 CK，使多实例可查询状态、事件不丢、claim 幂等。

不在本文范围（见 ISS-004 对应条目）：
- MN-02：四个结果 Parquet artifact 的归属（内容寻址 / CK 行 / 对象存储）；
- MN-04：挖掘锁 `.mining_process.lock` 与跨实例取消（锁是进程本地 flock，**不迁 CK**）；
- MN-05：API 与前端工作台的历史 run 兼容读取。

本文只定义 run 元数据层；§5 给出 artifact 表的预留形态以避免后续改表，但实现属 MN-02。

## 1. 现状盘点（迁移源）

来源：`backend/app/services/mining_jobs.py` 的 `MiningRunStore`（`:124`）、`mining_runtime.py`、`mining_schedule.py`、`mining_candidates.py`。当前一个 run 目录含 `manifest.json` / `summary.json` / `events.jsonl` 及 4 个 Parquet。

### 1.1 manifest.json（权威状态机）

字段（`mining_jobs.py:151-165`）：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `schema_version` | int | 当前 `_SCHEMA_VERSION=1`（`:49`） |
| `run_id` | str | 主键；周度任务即 `weekly-<iso_year>-W<week:02d>` |
| `status` | str | 9 态状态机（§1.4） |
| `request` | object | 原始请求（含因子/策略范围、参数、confidence 等） |
| `data_fingerprint` | str | 数据指纹 digest，防漂移 |
| `run_signature` | str | 请求规范化签名，用于去重 |
| `artifacts` | object | `{name: 相对路径}`，四个产物登记 |
| `created_at` / `updated_at` / `started_at` / `finished_at` / `cancellation_requested_at` | float | Unix 秒，部分可空 |
| `error` | str \| null | 失败原因（截断 2000 字） |

namespace 区分：`MiningRunStore(namespace="mining")` 与 guardian 用独立 namespace（`main.py:169,185`），根路径 `research/<namespace>/runs`。

### 1.2 compact summary

字段（`mining_runtime.py:680-705`，运行时追加 `progress`，`mining_manager.py:228`）：
`status, factor_count, selected_factor_count, candidate_count, valid_fold_count, skipped_fold_count, confidence, budget_exhausted, elapsed_ms, data_as_of, algorithm_version, methodology_version, refinement, proxy_trials_used, trials_used, panel_rows, panel_scans, matrix_bytes, matrix_compute_cache_peak_bytes, phase_ms, phase_peak_rss_bytes, artifacts[]`，以及运行中的 `progress`。

特点：**列会随评分模型迭代扩展**（v3 新增 `oos_worst_sharpe` 已是先例），旧 run 可能缺列，读取端需容忍缺字段。

### 1.3 事件流 events.jsonl

每条 `{id, timestamp, type, payload}`（`mining_jobs.py:288-293`）。约束：`MAX_EVENTS=256`、payload ≤16KB（`:47-48`）、id 单调、读取 `read_events(after_id=0)` 返回尾部最近 256 条。事件是**追加日志**，不覆盖。

### 1.4 状态机

`RUN_STATUSES`（`mining_jobs.py:30-42`）：`queued, running, cancelling, succeeded, succeeded_with_budget_exhausted, failed, cancelled, interrupted, skipped_prerequisite`。
`ACTIVE_RUN_STATUSES = {queued, running, cancelling}`；终态集为该集合补集。合法转换表 `_ALLOWED_TRANSITIONS`（`:54-84`），非法转换抛 `InvalidMiningStatusTransitionError`（`:95`）。`transition_status` 在全局 `_STORE_LOCK`（`:52`）下校验并更新时间戳。

### 1.5 周度 claim 与数据指纹

- claim：`schedule_claim(day) -> "weekly-<iso_year>-W<week:02d>"`（`mining_schedule.py:170-173`），**直接作为 run_id**；去重靠 `store.get(claim)` 判存在（`:199-202`）。
- `data_fingerprint`：`build_data_fingerprint`（`:75-123`）以 `version("weekly-mining-data-v2") + asset_type + generation + latest_enriched_date + enriched/instruments/regime digest + algorithm_version("mining-v3") + methodology_version + implementation + strategies + factors` 规范化后 sha256。用于 worker 入口防漂移（`mining_runtime.py:471,479`）与晋升校验（`mining_candidates.py:458`）。
- `run_signature`：`compute_run_signature`（`mining_jobs.py:106-121`），`find_by_signature` 用于幂等复用（`:350`）。

## 2. 设计原则

1. **单一权威**：CK 化后在线读写只认 CK，本地 run 目录降级为离线兼容（与 DES-002 的 daily/enriched CK-only 同构）；不做双写分叉。
2. **状态与产物分离**：run 元数据（小、需并发一致）进 CK；大对象 artifact（MN-02）另定归属，不在本批把宽表塞进 CK 行。
3. **状态机语义不变**：合法转换表、时间戳更新、终态不可转出等规则从 `MiningRunStore` 原样迁移，CK 只换存储，不改语义；前端 API 契约不变。
4. **幂等靠唯一键**：claim 即 run_id，CK `ORDER BY (namespace, run_id)` 保证同一逻辑 run 单行；`find_by_signature` 用 `run_signature` 查询。
5. **schema 演进容忍**：summary 存 `payload_json`，列扩展不改 DDL；读取端容忍缺字段（v3 `oos_worst_sharpe` 先例）。
6. **fail-closed**：CK 不可达时显式报错，不静默回退本地文件造成多实例状态分叉（与 RC-08 协调层决策一致）。

## 3. CK 表契约

命名沿用 `ck_` 前缀（与 `ck_repair_audit` / `ck_enriched_generation` 一致），DDL 遵循 `schema.py` 风格（幂等 `CREATE TABLE IF NOT EXISTS`、`updated_at` 版本列、元数据表 `PARTITION BY tuple()` 可省 projection）。所有表统一带五个审计列（`id` UUID 主键 + `created_at` / `created_by` / `updated_at` / `updated_by`），遵循 `tsp-rule/clickhouse.mdc`「表设计」规范；业务去重键由 `ORDER BY` 表达，不与行主键冲突（事件序号因此命名为 `event_id`）。每张表带表级 `COMMENT`（说明用途与粒度）、每字段带列级 `COMMENT`（说明业务含义 / 单位 / 取值），遵循同规范的表 / 字段注释完整性要求；CK 24.8 实测支持两种注释语法。

### 3.1 `ck_mining_run`（run 主表）

```sql
CREATE TABLE IF NOT EXISTS {table} (
  id              UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  namespace       LowCardinality(String) COMMENT '命名空间:mining/guardian,隔离不同挖掘业务',
  run_id          String COMMENT '运行唯一标识;周度任务为weekly-YYYY-Www',
  status          LowCardinality(String) COMMENT '运行状态:9态,合法转换按_ALLOWED_TRANSITIONS校验',
  schema_version  UInt32 COMMENT 'manifest结构版本号',
  request_json    String COMMENT '原始挖掘请求JSON(因子/策略范围、参数、confidence)',
  summary_json    String COMMENT '压缩摘要JSON(统计/进度/版本),列扩展不改DDL',
  data_fingerprint String COMMENT '数据指纹sha256,检测输入数据漂移',
  run_signature   String COMMENT '请求规范化签名,用于幂等去重',
  error           Nullable(String) COMMENT '失败原因(截断2000字),成功为NULL',
  artifact_names  Array(String) COMMENT '产物名称数组(factors/correlation/candidates/folds)',
  created_at      DateTime64(3) COMMENT 'run创建时间',
  started_at      Nullable(DateTime64(3)) COMMENT '进入running的时间,未开始为NULL',
  finished_at     Nullable(DateTime64(3)) COMMENT '进入终态的时间,未结束为NULL',
  cancellation_requested_at Nullable(DateTime64(3)) COMMENT '请求取消的时间,未请求为NULL',
  created_by      LowCardinality(String) COMMENT '创建者标识(用户或服务)',
  updated_at      DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作ReplacingMergeTree版本列',
  updated_by      LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY namespace
ORDER BY (namespace, run_id)
COMMENT '挖掘运行主表:每个run一行,承载manifest状态机与压缩摘要的权威状态'
```

设计要点：
- `request` / `summary` 用 JSON 字符串整存，避免列随算法版本扩展而改 DDL；`artifact_names` 仅存名字数组，路径由 §5 的产物表/内容寻址推导。
- 时间戳由本地 float 秒转 `DateTime64(3)`；迁移兼容见 §6。
- `status` 用 `LowCardinality`，9 态枚举值写入前按 `_ALLOWED_TRANSITIONS` 校验。
- 并发：`ReplacingMergeTree` 以 `updated_at` 决定最终版本；写入用严格递增版本（参照 DES-002 `ClickHouseStore.write` 的毫秒版本生成），避免同毫秒覆盖不确定。

### 3.2 `ck_mining_run_event`（事件追加日志）

```sql
CREATE TABLE IF NOT EXISTS {table} (
  id           UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  namespace    LowCardinality(String) COMMENT '命名空间:与主表一致',
  run_id       String COMMENT '所属run标识,关联ck_mining_run.run_id',
  event_id     UInt32 COMMENT '事件序号(run内单调递增),由store分配',
  timestamp    DateTime64(3) COMMENT '事件发生时刻',
  type         LowCardinality(String) COMMENT '事件类型(progress/queued/running/终态等)',
  payload_json String COMMENT '事件负载JSON,上限16KB',
  created_at   DateTime64(3) COMMENT '事件入库时间',
  created_by   LowCardinality(String) COMMENT '事件写入者标识',
  updated_at   DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间',
  updated_by   LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = MergeTree
PARTITION BY namespace
ORDER BY (namespace, run_id, event_id)
TTL toDate(timestamp) + INTERVAL 90 DAY
COMMENT '挖掘运行事件流:每个run的追加日志,保留全量后按event_id倒序查最近256条'
```

设计要点：
- `id` 为行主键（UUID）；事件序号另用 `event_id UInt32`，由 store 分配（沿用现有自增），避免占用主键名。
- `MergeTree` 追加语义，不覆盖；`timestamp` 与 `created_at` 分别承载事件时刻与入库时刻。
- `payload_json` 上限 16KB 在写入侧校验，与现有 `MAX_EVENT_PAYLOAD` 一致。
- 现有 256 条截断是文件大小优化，CK 下改为**保留全量 + 查询侧限制**：`read_events(after_id, limit=256)` 用 `WHERE event_id > after_id ORDER BY event_id DESC LIMIT 256` 复刻原语义；TTL 90 天控制总量。TTL 值待评审确认（见 §8）。

### 3.3 `ck_mining_claim`（claim 与指纹幂等）

```sql
CREATE TABLE IF NOT EXISTS {table} (
  id               UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  claim            String COMMENT '周度唯一标识weekly-YYYY-Www,作去重键',
  run_id           String COMMENT '该claim对应的run标识,关联ck_mining_run.run_id',
  data_fingerprint String COMMENT '当时的输入数据指纹,用于漂移校验',
  run_signature    String COMMENT '请求规范化签名,算法升级后变化以拒绝复用旧结果',
  outcome          LowCardinality(String) COMMENT '认领结果:claimed/skipped_prerequisite',
  created_at       DateTime64(3) COMMENT 'claim创建时间',
  created_by       LowCardinality(String) COMMENT '认领者标识',
  updated_at       DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作版本列',
  updated_by       LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY claim
COMMENT '周度挖掘认领表:同一ISO周仅触发一次,记录数据指纹与认领结果'
```

设计要点：claim（`weekly-YYYY-Www`）作唯一键，`outcome` 记录即使前置不满足也占用该周 claim（现有语义：`_record_skipped_prerequisite` 仍建 run）。`data_fingerprint` 落表供跨实例漂移校验；算法版本升级后指纹变化，旧 claim 不复用（与 `mining.md` §87 一致）。

### 3.4 `ck_mining_artifact`（产物登记，MN-02 预留）

```sql
CREATE TABLE IF NOT EXISTS {table} (
  id            UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  namespace     LowCardinality(String) COMMENT '命名空间:与主表一致',
  run_id        String COMMENT '所属run标识,关联ck_mining_run.run_id',
  artifact_name LowCardinality(String) COMMENT '产物名:factors/correlation/candidates/folds',
  content_addr  String COMMENT '内容寻址键(sha256),指向实际产物存储',
  row_count     UInt64 COMMENT '产物行数',
  size_bytes    UInt64 COMMENT '产物字节数',
  schema_version UInt32 COMMENT '产物schema版本号',
  created_at    DateTime64(3) COMMENT '产物登记时间',
  created_by    LowCardinality(String) COMMENT '产物写入者标识',
  updated_at    DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间,兼作版本列',
  updated_by    LowCardinality(String) COMMENT '最后更新者标识'
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY namespace
ORDER BY (namespace, run_id, artifact_name)
COMMENT '挖掘产物登记表:每个run的四类结果产物的内容寻址元数据(MN-02落地)'
```

本文只定表形态，具体产物落地（CK 行 vs 内容寻址 parquet）与四类 schema 由 MN-02 决策；`content_addr` 预留内容寻址，避免把宽表行写进 CK。

## 4. store 方法 → CK 操作映射

`MiningRunStore` 公开方法（`mining_jobs.py`）逐个给出 CK 化后的行为契约：

| 方法 | CK 操作 | 语义保持要点 |
| --- | --- | --- |
| `create(request, data_fingerprint, run_id=None)` | `ck_mining_run` 插入一行（status=queued） | run_id 唯一；已存在时返回既有（幂等），不覆盖 |
| `get(run_id)` | `SELECT ... WHERE namespace=? AND run_id=? FINAL` | 缺字段补默认，归一化同 `:459-498` |
| `transition_status(run_id, status, error=None)` | 读当前 → 校验 `_ALLOWED_TRANSITIONS` → 更新 status + 时间戳 | 校验必须在读版本后进行；非法转换抛错；用 CAS/事务防并发覆盖 |
| `write_summary / read_summary` | 更新 / 读 `summary_json` | 整包替换；运行中 progress 追加 |
| `register_artifact(run_id, name, path)` | `ck_mining_artifact` upsert + 更新 `artifact_names` | 与 MN-02 产物表联动 |
| `append_event(run_id, type, payload)` | `ck_mining_run_event` INSERT，`event_id` 取 `max(event_id)+1`，`id` 由 UUID 默认生成 | payload ≤16KB；`event_id` 单调 |
| `read_events(run_id, after_id)` | `WHERE event_id > after_id ORDER BY event_id DESC LIMIT 256` | 复刻尾部 256 条语义 |
| `list_runs(limit, statuses)` / `find_by_signature` | `SELECT ... ORDER BY created_at DESC LIMIT n` / `WHERE run_signature=?` | 排序与过滤与现有一致 |
| `recover_interrupted()` | `WHERE status IN ACTIVE → interrupted` 批量更新 | 启动恢复语义不变 |
| `schedule_claim` / 指纹校验 | `ck_mining_claim` 查 / upsert | claim 幂等，指纹漂移拒绝复用 |

### 4.1 关键并发点：transition_status

状态机合法性与并发安全是本次迁移的核心风险。契约要求：
- 用单条原子操作完成"读当前状态 → 校验合法 → 写新状态 + 时间戳"，避免读后写之间的竞态；
- CK 不提供行级 CAS，需在写入层用 `updated_at` 版本做乐观并发（写入带期望版本，冲突则重试），或把状态转换收敛到单实例写入者（依赖 MN-04 锁）；
- **决策待定**：乐观并发 vs 单写者，见 §8。这是评审必须拍板的第一项。

## 5. 读写路径与兼容策略

- **在线路径**：`MiningJobManager` 的 `_store` 在显式 `storage` 且 `CK_MINING=1`（或复用 `CACHE_BACKEND` 之外的独立开关）时注入 `CKMiningRunStore`；无 storage 的离线工具保留 `MiningRunStore` 文件实现（同 DES-002 §3.2 兼容模式）。
- **worker 子进程**：`mining_runtime.py` 的 `run_mining_runtime` 仍写本地 artifact 与 summary（spawn 子进程通过 `storage_prefix` 访问 CK，见 `backtest/worker.py:196`）；主进程 manager 负责把 run 状态 / 事件 / 摘要落 CK。MN-01 阶段 artifact 不迁，worker 路径不变。
- **前端**：API 层 `app/api/mining.py` 读取契约（字段名、排序、分页）保持不变，仅底层 store 换 CK；MN-05 再处理历史本地 run 的只读兼容。
- **generation 联动**：run 依赖的 enriched/regime 数据 generation 变化应使进行中 run 可感知（与数据指纹配合），不额外引入新 generation 命名空间。

## 6. 迁移与回滚

1. **双读校验期**（可选）：新 run 同时写本地与 CK，读以 CK 为准、定期比对；差异告警。
2. **存量回填**：脚本扫描 `research/<namespace>/runs/*/manifest.json` + `summary.json` + `events.jsonl`，解析后批量写 CK；float 时间戳转 `DateTime64(3)`，缺字段补默认。
3. **切换**：在线读写切 CK 后，本地 run 目录不再写入；保留只读直到 MN-05 完成历史兼容。
4. **回滚**：CK 不可达时 fail-closed（不回退文件）；回滚需恢复旧版本并停止 CK 写入，本地目录作为数据源重新接管。

## 7. 分批落地顺序

| 批次 | 内容 | 门禁 |
| --- | --- | --- |
| MN-01a | DDL 注册（4 表）+ schema 测试 + `CKMiningRunStore` 骨架 | `test_ch_schema.py` 通过，DDL 幂等 |
| MN-01b | manifest/summary 读写 + 状态机 + claim/指纹 | 状态机全路径测试（合法/非法/并发），find_by_signature 幂等 |
| MN-01c | 事件流读写 + `recover_interrupted` + list_runs | 事件单调、256 截断语义、启动恢复测试 |
| MN-01d | 存量回填脚本 + 兼容模式接通 | 回填一致性校验，定向全绿 |

全量回归与 pre-commit 放在 MN-01 整体验收前统一跑（用户已要求全量回归推迟到后续批次）。

## 8. 待决事项（评审必须回答）

| 编号 | 问题 | 影响 | 建议 |
| --- | --- | --- | --- |
| D-01 | `transition_status` 的并发方案？ | 决定 §4.1 实现复杂度与锁依赖 | **已拍板（2026-11-19）：方案 C（Redis Lua 状态机 + CK 审计）**，依据 [DES-004](DES-004-distributed-architecture-roadmap.md) §5；flock 出局，B 仅备选，否决 MySQL；§4.1 需按方案 C 重写（状态权威改 Redis，CK 仅审计） |
| D-02 | 事件表 TTL 保留期（90 天？） | 影响存储成本与历史可查范围 | 按产品对历史事件的保留要求定 |
| D-03 | artifact 进 CK 行还是内容寻址（MN-02） | 决定 `ck_mining_artifact` 是否存 payload | 倾向内容寻址，CK 只存元数据 |
| D-04 | 候选库 `research_candidates.json` 是否同期迁 CK | backlink 回写（`mining_candidates.py:844-872`）跨存储 | 建议与 MN-02 合并评估，不在 MN-01 处理 |
| D-05 | 是否设 `CK_MINING` 独立开关 | 灰度与回滚粒度 | 建议独立于 `CACHE_BACKEND`，便于按模块灰度 |

## 9. 验收标准

1. 四张 CK 表 DDL 幂等、schema 测试覆盖；`ck_mining_run` 以 `(namespace, run_id)` 唯一。
2. `MiningRunStore` 全部公开方法在 CK 后端下有等价实现，语义与本地版一致（状态机、事件单调、256 截断、幂等去重）。
3. 两个实例共享 CK 时：状态转换不丢、事件可见、同一 ISO 周只 claim 一次、指纹漂移被拒。
4. 存量 run 回填后 CK 与本地文件解析结果一致。
5. CK 不可达 fail-closed，不静默回退本地造成分叉。
6. 定向测试全绿、pre-commit 全门禁通过；全量回归在后续批次统一执行。
