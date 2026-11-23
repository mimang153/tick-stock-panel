# 用户状态分布式迁移设计（S6：用户状态迁入 PG/Redis/CK）

> 文档类型：设计 | 编号：DES-006 | 状态：设计中（2026-11-20 决策拍板） | 文档版本：v1.0 | 最后更新：2026-11-20
> 负责人：待分配 | 关联：DES-004（§2.3/§5 S5）、DES-005、ISS-003、ISS-005、tsp-rule/pgsql.mdc、tsp-rule/redis.mdc、tsp-rule/clickhouse.mdc、tsp-rule/docker-compose-standards.mdc

## 0. 文档状态与边界（先读）

本文回答一个问题：**如何把当前 `/app/data` 中的用户配置和本地产物迁到 PG/Redis/CK，使宿主机只保留日志挂载与策略/插件共享目录，同时不改变现有 API 契约。**

本文基于 2026-11-20 已确认决策：

| 编号 | 决策 |
| --- | --- |
| D-01 | 不引入 S3/MinIO；保留一个共享目录，仅用于 `strategies/` 与 `plugins/` |
| D-02 | Redis 开启 AOF + `noeviction`，承载会话、热缓存、任务当前态与 progress |
| D-03 | Redis 整体丢失后允许全部用户重新登录 |
| D-04 | 所有运行形态强制 PG/Redis；不保留运行期 file backend |
| D-05 | 取消 `secrets.json` 外部编辑语义，统一走 API/UI |
| D-06 | 任务 progress 持久化纳入本期 |

不在本文范围：

- 行情 parquet 历史镜像清理（CK 已是权威，parquet 保留为离线兼容/回填源，单独治理）；
- API 路由/字段/事件结构调整；
- 前端交互重构；
- 回测算法和行情口径变化；
- 新增 S3/MinIO 服务。

本文是设计文档；实施 PR 必须遵守 `tsp-rule/*.mdc`。其中 PG 表设计按 `tsp-rule/pgsql.mdc`，Redis 访问按 `tsp-rule/redis.mdc`，CK 表设计按 `tsp-rule/clickhouse.mdc`，compose 调整按 `tsp-rule/docker-compose-standards.mdc`。

## 1. tsp-rule 合规设计约束

### 1.1 PostgreSQL

依据 `tsp-rule/pgsql.mdc`：

1. 每张业务表必须有 `id TEXT PRIMARY KEY`，值为 UUID4 hex；业务唯一键用 `UNIQUE` 表达，不得直接作为主键。
2. 每张表必须有 `created_by` / `created_at` / `updated_by` / `updated_at` 四审计字段，均为 `NOT NULL`，时间类型统一 `TIMESTAMPTZ`。
3. 每张表必须有表注释，每个字段必须有字段注释。
4. 业务代码不得直接持有 PG 连接对象或跨模块拼 SQL；统一通过 `app/state` 门面访问。
5. DDL 变更必须版本化，禁止手工改库；一次性导入必须有全量导入脚本、一致性校验和回滚路径。
6. 一次业务状态变更 = 一个事务；UPSERT 用 `ON CONFLICT ... DO UPDATE` 并更新审计字段。
7. 金额/数量类型约束本期不涉及资金状态；如后续扩展，按 `NUMERIC`/`BIGINT` 执行。

> 规则适用范围说明：`tsp-rule/pgsql.mdc` 当前描述 PG 用于资金类 OLTP 状态。本文扩展 PG 到“用户配置类 OLTP 状态”。实施时必须同步修订该规则的描述，但不得降低上述库表/事务/注释约束。

### 1.2 Redis

依据 `tsp-rule/redis.mdc`：

1. 业务代码只依赖 `CacheBackend` 接口或 `UserStateCache` 包装门面，不直接持有 Redis 客户端。
2. dict 值使用 JSON 序列化，DataFrame 使用 Arrow IPC；严禁 pickle。
3. 命名空间即 generation 作用域；配置类缓存通过 `generation()` / `bump()` 做跨进程失效。
4. Redis 承载可重建的短态：会话、热缓存、版本号、任务 progress；PG/CK 仍是用户配置与历史的持久权威。
5. Redis 失联或数据丢失时按设计降级，不允许进入半可用状态。

> AOF/noeviction 是部署侧运行前提，业务代码不依赖 Redis 永不丢数据。D-03 明确：Redis 整体丢失后用户重新登录。

### 1.3 ClickHouse

依据 `tsp-rule/clickhouse.mdc`：

1. 每张表必须包含五审计列：`id UUID DEFAULT generateUUIDv4()`、`created_at`、`created_by`、`updated_at`、`updated_by`。
2. 每张表和每个字段必须有注释。
3. 使用 `ReplacingMergeTree(updated_at)` 承接 upsert，`ORDER BY` 表达业务去重键。
4. 业务代码不直接拼 CK SQL；通过存储门面访问。
5. 新增表进入 `schema.py::_TABLE_DDL`，并 bump `SCHEMA_VERSION`；读最新态用 `FINAL` 或 `argMax`。

### 1.4 Docker Compose

依据 `tsp-rule/docker-compose-standards.mdc`：

- 不写 `version:` 字段；
- 2 空格缩进；
- 不使用 `links:`；
- 镜像显式 tag；
- 服务保留资源限制；
- 显式声明持久化挂载与 tmpfs，避免把用户状态写入容器可写层。

## 2. 目标架构

### 2.1 最终挂载形态

```text
./data/logs:/app/data/logs
./data/shared:/app/data/shared
```

`./data/shared` 只允许包含：

```text
shared/
  strategies/
    custom/
    ai/
    composite/
  plugins/
    <package_name>/
```

临时目录使用 tmpfs：

```text
/app/data/.runtime
/app/data/.staging
/app/data/.cache
```

### 2.2 数据归属总表

| 数据 | 权威/落点 | Redis 角色 | CK 角色 |
| --- | --- | --- | --- |
| 登录密码哈希 | PG | 短 TTL configured 缓存 | 不存 |
| 登录会话 | Redis 短态，允许丢失 | 会话缓存，TTL | 不存 |
| API token 元数据/哈希 | PG | verify 结果短缓存、last_used 节流 | 不存 |
| secrets | PG 加密存储 | 不缓存明文 | 配置变更审计 |
| preferences | PG | JSON 缓存 + generation 失效 | 配置变更审计 |
| capabilities | PG last-known-good | 5 分钟短缓存 | 配置变更审计 |
| watchlist / groups | PG | version + 失效通知 | 不存 |
| daily_integrity 当前状态 | PG | version + 失效通知 | 状态变更历史 |
| integrity_report 当前报告 | PG | 不缓存正文 | 历史报告 |
| repair backoff | PG | 6h TTL 判断缓存 | 不存 |
| job 当前态/progress | Redis 热态；CK 可重建历史 | 主读写热态 | `ck_coord_job_state` 历史 |
| strategies 源码 | shared 目录 | 不存 | 不存 |
| plugins 包文件 | shared 目录 | 不存 | 不存 |
| data_sources 配置 | PG | 配置缓存 | 配置变更审计 |
| ext_pages 配置/bundle | PG 元数据 + `user_blob` | 不存 | 配置变更审计 |
| sound_packs | PG 元数据 + `user_blob` | 不存 | 不存 |
| integrity_backups | PG 元数据 + `user_blob` | 不存 | 不存 |
| mining artifacts | PG `user_blob`；CK 元数据 | 不存 | `ck_mining_artifact` |
| 日志 | `./data/logs` | 不存 | 可选采集，非本期 |

### 2.3 分层

```text
API / Service
    ↓
app/state 门面
    ├─ PgStateStore        # PG 持久状态
    ├─ UserStateCache      # CacheBackend 包装，Redis 短态
    ├─ UserBlobStore       # PG BYTEA 二进制对象
    ├─ SharedFileStore     # strategies/plugins 文件门面
    └─ StateAuditStore     # CK 历史/审计
```

业务代码只调用门面函数，例如：

```text
get_preferences()
save_preferences(payload, actor)
record_integrity_issues(...)
assert_daily_usable(...)
get_job_progress(job_id)
put_blob(bytes) -> hash
```

禁止在 `services/`、`api/`、`strategy/` 中直接 `psycopg.connect()`、`redis.Redis()` 或拼 SQL。

## 3. 配置与部署设计

### 3.1 新增配置

```text
USER_STATE_PG_SCHEMA=tsp_user_state
USER_STATE_REDIS_URL=redis://:123456@host.docker.internal:6379/3
USER_STATE_REDIS_PREFIX=tsp:user:
SHARED_DATA_ROOT=/app/data/shared
RUNTIME_DATA_ROOT=/app/data/.runtime
USER_BLOB_MAX_BYTES=67108864
JOB_LIVE_TTL_DAYS=7
USER_STATE_ENCRYPTION_KEY=<base64:32bytes>
MINING_ARTIFACT_STORE=pgblob
MINING_ARTIFACT_ROOT=/app/data/.staging/mining-artifacts
```

### 3.2 Redis 服务端要求

```text
appendonly yes
appendfsync everysec
maxmemory-policy noeviction
```

应用启动时只做提示性检查；如果 Redis 未开启 AOF，记录 warning，但不阻塞开发启动。生产部署必须在部署清单中保证。

### 3.3 Compose 约束

- 删除 `./data:/app/data`；
- 保留 `./data/logs:/app/data/logs`；
- 新增 `./data/shared:/app/data/shared`；
- 增加 tmpfs 挂载；
- `DATA_DIR=/app/data` 保持不变；
- 源码挂载 `.:/app` 保持不变，显式子挂载覆盖 `/app/data/logs` 与 `/app/data/shared`。

## 4. PG 数据模型

所有 PG 表统一审计字段 DDL 片段：

```sql
id          TEXT PRIMARY KEY,                         -- 主键，UUID4 hex
created_by  TEXT NOT NULL,                            -- 创建人/操作来源
created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),       -- 创建时间
updated_by  TEXT NOT NULL,                            -- 更新人/操作来源
updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()        -- 更新时间
```

统一审计字段注释：

```sql
COMMENT ON COLUMN <table>.id IS '主键，UUID4 hex';
COMMENT ON COLUMN <table>.created_by IS '创建人/操作来源：api/scheduler/pipeline/system/migration';
COMMENT ON COLUMN <table>.created_at IS '创建时间，UTC；由数据库默认值生成';
COMMENT ON COLUMN <table>.updated_by IS '更新人/操作来源：api/scheduler/pipeline/system/migration';
COMMENT ON COLUMN <table>.updated_at IS '更新时间，UTC；每次 UPSERT 更新';
```

下表中“公共审计字段”表示必须按上述片段和注释补齐。

### 4.1 迁移版本表

```sql
CREATE TABLE tsp_user_state.schema_migrations (
  id          TEXT PRIMARY KEY,
  version     INTEGER NOT NULL UNIQUE,
  name        TEXT NOT NULL,
  applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by  TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by  TEXT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE tsp_user_state.schema_migrations IS '用户状态库版本登记表；记录已应用的 DDL 迁移版本';
COMMENT ON COLUMN tsp_user_state.schema_migrations.version IS '迁移版本号，单调递增';
COMMENT ON COLUMN tsp_user_state.schema_migrations.name IS '迁移名称，便于审计';
COMMENT ON COLUMN tsp_user_state.schema_migrations.applied_at IS '迁移应用时间';
COMMENT ON COLUMN tsp_user_state.schema_migrations.id IS '主键，UUID4 hex';
COMMENT ON COLUMN tsp_user_state.schema_migrations.created_by IS '创建人/操作来源：api/scheduler/pipeline/system/migration';
COMMENT ON COLUMN tsp_user_state.schema_migrations.created_at IS '创建时间，UTC；由数据库默认值生成';
COMMENT ON COLUMN tsp_user_state.schema_migrations.updated_by IS '更新人/操作来源：api/scheduler/pipeline/system/migration';
COMMENT ON COLUMN tsp_user_state.schema_migrations.updated_at IS '更新时间，UTC；每次 UPSERT 更新';
```

### 4.2 认证、Token、密钥、偏好、能力

以下所有表均追加 §4 公共审计字段，并在实施 DDL 中逐字段写 `COMMENT ON COLUMN`；表中列出的是业务字段与业务注释要点。

| 表 | 用途 | 业务字段（注释要点） | 约束 |
| --- | --- | --- | --- |
| `user_auth` | 单实例访问密码哈希 | `singleton_key`（默认 `default`）、`password_hash`（PBKDF2-SHA256，不存明文）、`password_salt`（hex）、`hash_algo`、`iterations` | `UNIQUE(singleton_key)` |
| `api_token` | Token 元数据与哈希 | `token_id`（对外 ID）、`name`、`token_hash`（SHA-256）、`scopes`（JSONB）、`revoked`、`last_used_at`（Redis 节流后落库） | `UNIQUE(token_id)`、`UNIQUE(token_hash)` |
| `user_secret` | 加密密钥配置 | `secret_key`（如 `ai.openai.key`）、`value_encrypted`（AES-GCM 密文含 nonce/tag） | `UNIQUE(secret_key)` |
| `user_preference` | 偏好与数据源路由 | `singleton_key`、`payload`（JSONB）、`version`（递增版本号） | `UNIQUE(singleton_key)` |
| `capability_cache` | 能力探测 last-known-good | `singleton_key`、`schema_version`（当前 6）、`payload`（JSONB）、`probe_log`（TEXT[]）、`missing_caps`（TEXT[]）、`invalid_key` | `UNIQUE(singleton_key)` |

会话不建 PG 表，放 Redis；Redis 丢失后重新登录。

API Token 撤销 SQL：

```sql
UPDATE tsp_user_state.api_token
SET revoked = TRUE, updated_by = $2, updated_at = now()
WHERE token_id = $1;
```

密钥约束：

- `USER_STATE_ENCRYPTION_KEY` 为 base64 编码的 32 字节密钥，算法 AES-GCM；
- API 返回一律脱敏；
- Redis 不缓存明文 secret。

偏好写入使用事务 + `SELECT ... FOR UPDATE`，禁止 read-modify-write 覆盖。

### 4.7 自选、完整性、配置对象

| 表 | 用途 | 业务字段（注释要点） | 约束 |
| --- | --- | --- | --- |
| `watchlist_item` | 自选标的 | `symbol`、`note`、`position`、`added_at` | `UNIQUE(symbol)` |
| `watchlist_group` | 自选分组 | `group_key`、`name`、`color`、`position` | `UNIQUE(group_key)` |
| `watchlist_membership` | 自选与分组关系 | `symbol`、`group_id` | `UNIQUE(symbol, group_id)` |
| `daily_integrity` | 日K完整性闸门当前态 | `asset_type`、`day`、`symbol`、`kind`、`reason`、`needs_raw`、`needs_enriched`、`revision` | `UNIQUE(asset_type, day, symbol, kind)` |
| `integrity_revision` | 按资产族的完整性版本 | `asset_type`、`revision` | `UNIQUE(asset_type)` |
| `integrity_report` | 当前完整性报告 | `report_id`、`checked_at`、`status`、`report`（JSONB） | `UNIQUE(report_id)` |
| `repair_backoff` | 自动修补冷却 | `start_date`（DATE）、`last_attempt_at` | `UNIQUE(start_date)` |
| `data_source_config` | 数据源 YAML 配置正文 | `config_name`、`kind`、`content` | `UNIQUE(config_name)` |
| `user_blob` | 内容寻址二进制对象 | `content_hash`（SHA-256）、`payload`（BYTEA）、`size_bytes`、`mime_type` | `UNIQUE(content_hash)` |
| `ext_page` | 扩展页面配置 | `page_id`、`config`（JSONB）、`bundle_hash` | `UNIQUE(page_id)` |
| `sound_pack` | 告警音元数据 | `pack_name`、`blob_hash`、`size_bytes`、`ext` | `UNIQUE(pack_name)` |
| `integrity_backup` | 修补备份元数据 | `backup_id`、`manifest`（JSONB）、`blob_hashes`（TEXT[]） | `UNIQUE(backup_id)` |
| `state_migration_audit` | 一次性迁移审计 | `migration_id`、`source_path`、`target_ref`、`status`、`checksum`、`detail` | `UNIQUE(migration_id)` |

`user_blob` 承载 ext_pages bundle、sound_packs 音频、integrity_backups、mining artifacts；`pgblob://{content_hash}` 为统一 URI。

## 5. Redis 设计

### 5.1 访问约束

业务代码通过 `UserStateCache` 门面访问 Redis；门面内部依赖 `CacheBackend`：

```text
UserStateCache
  ├─ get_json(key) / set_json(key, value, ttl_s)
  ├─ invalidate(prefix)
  ├─ generation(name) / bump(name)
  └─ namespace/prefix 管理
```

约束：

- 不直接创建 Redis client；
- dict/JSON 使用 JSON 序列化；
- DataFrame 使用 Arrow IPC；
- 禁止 pickle；
- 所有 key 以 `tsp:user:` 作为统一前缀。

### 5.2 Key 与 TTL

| Key | 类型 | TTL | 说明 |
| --- | --- | --- | --- |
| `tsp:user:session:{token_hash}` | JSON | 沿用现有会话 TTL | 会话短态，丢失后重新登录 |
| `tsp:user:token:last_used:{token_id}` | JSON | 24h | 最近使用节流，落 PG |
| `tsp:user:pref:cache` | JSON | 10min | preferences 热缓存 |
| `tsp:user:cap:{provider}:v6` | JSON | 5min | capability 短缓存 |
| `tsp:user:watch:version` | int | 7d | 自选版本号 |
| `tsp:user:integrity:version:{asset_type}` | int | 7d | 完整性版本号 |
| `tsp:user:repair:backoff:{start_date}` | JSON | 6h | 修补冷却判断缓存 |
| `tsp:user:job:{job_id}:state` | JSON | 完成后 7d | 任务当前状态 |
| `tsp:user:job:{job_id}:progress` | JSON | 完成后 7d | 任务进度 |

### 5.3 generation / 失效

命名空间：

```text
pref
cap
watch
integrity
token
config
```

写入后：

```text
save_preferences  -> bump("pref")     -> publish config-change
save_watchlist    -> bump("watch")    -> publish watchlist-change
save_integrity    -> bump("integrity")-> publish integrity-change
revoke_token      -> bump("token")    -> publish token-change
```

所有 worker 监听 Pub/Sub；收到消息后清进程内缓存并重新读 PG。Pub/Sub 丢消息由 TTL + generation 兜底。

### 5.4 会话

登录：

```text
生成 session token
set_json("session:{sha256(token)}", payload, ttl_s=现有会话时长)
```

鉴权：

```text
get_json("session:{sha256(token)}")
无值 -> 401
```

不做 PG 会话表；Redis 丢失与 D-03 保持一致。

### 5.5 任务状态

Redis 保存 live state：

```json
{
  "job_id": "...",
  "status": "running",
  "stage": "scan",
  "created_at": "...",
  "started_at": "...",
  "finished_at": null,
  "error": null
}
```

progress：

```json
{
  "current": 12,
  "total": 100,
  "message": "scanning",
  "updated_at": "..."
}
```

CK `ck_coord_job_state` 保留历史。Redis 丢失后：

```text
扫 CK 中 pending/running
标记为 interrupted
写回 CK
通知前端刷新
```

## 6. ClickHouse 设计

### 6.1 公共审计列

每张新 CK 表都使用：

```sql
id         UUID DEFAULT generateUUIDv4(),
created_at DateTime64(3),
created_by LowCardinality(String),
updated_at DateTime64(3) DEFAULT now64(3),
updated_by LowCardinality(String)
```

并补齐注释：

```sql
COMMENT ON COLUMN <table>.id IS '行主键，UUID';
COMMENT ON COLUMN <table>.created_at IS '创建时间，UTC';
COMMENT ON COLUMN <table>.created_by IS '创建人/操作来源';
COMMENT ON COLUMN <table>.updated_at IS '更新时间，UTC；ReplacingMergeTree 版本列';
COMMENT ON COLUMN <table>.updated_by IS '更新人/操作来源';
```

### 6.2 完整性事件表

```sql
CREATE TABLE tsp.ck_integrity_event (
  id         UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  event_id   String COMMENT '审计事件唯一 ID，用于 outbox 幂等',
  asset_type LowCardinality(String) COMMENT '资产类型：stock/etf/index',
  day        Date COMMENT '完整性状态对应交易日',
  symbol     String COMMENT '标的代码；资产级问题为空字符串',
  kind       LowCardinality(String) COMMENT '问题类型：missing/recompute 等',
  action     LowCardinality(String) COMMENT '状态动作：record/complete/clear',
  detail     String COMMENT '动作详情 JSON 或文本',
  created_at DateTime64(3) COMMENT '审计记录创建时间',
  created_by LowCardinality(String) COMMENT '操作来源：api/scheduler/pipeline/system',
  updated_at DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间，兼作版本列',
  updated_by LowCardinality(String) COMMENT '最后更新来源'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(day)
ORDER BY (asset_type, day, symbol, event_id)
COMMENT '完整性状态变更历史事件；一行表示一次资产/标的/日的状态动作';
```

### 6.3 完整性报告历史

```sql
CREATE TABLE tsp.ck_integrity_report_history (
  id          UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  report_id   String COMMENT '自检报告业务 ID，修补凭证',
  checked_at  DateTime64(3) COMMENT '自检完成时间',
  status      LowCardinality(String) COMMENT '报告状态：clean/issues',
  report_json String COMMENT '报告完整 JSON 字符串',
  created_at  DateTime64(3) COMMENT '审计记录创建时间',
  created_by  LowCardinality(String) COMMENT '操作来源',
  updated_at  DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间，兼作版本列',
  updated_by  LowCardinality(String) COMMENT '最后更新来源'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY report_id
COMMENT '完整性自检报告历史；当前报告以 PG 为准，本表用于审计与趋势';
```

### 6.4 配置变更审计

```sql
CREATE TABLE tsp.ck_config_change_audit (
  id         UUID DEFAULT generateUUIDv4() COMMENT '行主键',
  event_id   String COMMENT '审计事件唯一 ID，用于幂等',
  domain     LowCardinality(String) COMMENT '配置域：pref/token/watchlist/capability',
  action     LowCardinality(String) COMMENT '动作：create/update/delete/revoke/import',
  actor      LowCardinality(String) COMMENT '操作来源：api/scheduler/migration/system',
  detail     String COMMENT '变更摘要 JSON，禁止记录明文密钥',
  created_at DateTime64(3) COMMENT '审计记录创建时间',
  created_by LowCardinality(String) COMMENT '操作来源',
  updated_at DateTime64(3) DEFAULT now64(3) COMMENT '最后更新时间，兼作版本列',
  updated_by LowCardinality(String) COMMENT '最后更新来源'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (domain, event_id)
COMMENT '用户配置类状态变更审计；不承载当前态，仅用于追责与回放';
```

### 6.5 Schema 版本

当前 `SCHEMA_VERSION = 4`。本期新增 CK 表，实施时：

```text
SCHEMA_VERSION = 5
_MIGRATIONS[5] = 创建 ck_integrity_event / ck_integrity_report_history / ck_config_change_audit
```

部署顺序：

```text
先升代码 -> 再启动应用 -> 自动执行迁移
```

不允许数据库版本高于代码版本时启动。

## 7. 文件与对象设计

### 7.1 shared 目录

仅两个根目录：

```text
/app/data/shared/strategies
/app/data/shared/plugins
```

实施约束：

- `main.py` / `backtest/worker.py` 的 `sys.path` 指向 shared strategies；
- 插件 loader 根路径指向 shared plugins；
- AI 生成策略写 shared strategies；
- 插件安装写 shared plugins；
- 其他目录不得写 shared；
- shared 下文件写入使用原子替换；
- 多 worker 共享目录只依赖“文件即代码”加载语义，不承担用户配置权威。

### 7.2 PG blob

`user_blob` 承载：

- ext_pages bundle；
- sound_packs 音频；
- integrity_backups；
- mining artifacts。

限制：

```text
USER_BLOB_MAX_BYTES = 64 MiB
```

超过上限：

- 明确报错；
- 不静默截断；
- 不写半对象；
- 由调用方决定压缩或未来切对象存储。

URI：

```text
pgblob://{content_hash}
```

`artifact_store.py` 扩展：

```text
MINING_ARTIFACT_STORE: shared-root | s3 | pgblob
```

`pgblob` 模式：

1. 本地临时文件写 `/app/data/.staging/mining-artifacts`；
2. 计算 SHA-256；
3. 写 `user_blob`；
4. 返回 `pgblob://{hash}`；
5. CK `ck_mining_artifact.uri` 保存 URI；
6. 读取统一走 `resolve_artifact_uri()`。

## 8. 模块改造清单

| 模块 | 现状 | 改造后 | 兼容注意 |
| --- | --- | --- | --- |
| `services/auth.py` | `auth.json` + 内存 session | PG `user_auth` + Redis session | `is_configured()` 短 TTL 缓存 |
| `services/api_tokens.py` | `api_tokens.json` | PG `api_token` + Redis 节流 | token 哈希不返回 |
| `secrets_store.py` | `secrets.json` 外部可编辑 | PG `user_secret` + API/UI | 新增 secrets API，返回脱敏 |
| `preferences/_state.py` | `preferences.json` | PG `user_preference` + Redis cache | version 替代文件 hash |
| `tickflow/policy.py` | `capabilities.json` | PG `capability_cache` + Redis TTL | schema_version=6 保留 |
| `services/watchlist.py` | parquet/json | PG 三表 + Redis version | 顺序用事务重排 |
| `services/daily_health.py` | `daily_integrity_state.json` | PG `daily_integrity` + Redis version | `assert_usable()` 同步读 PG/短缓存 |
| `services/integrity_check.py` | `integrity_report.json` + 本地备份 | PG `integrity_report` + CK 历史 + blob | report_id 凭证不变 |
| `services/data_integrity.py` | backoff json | PG `repair_backoff` + Redis TTL | 6h 语义不变 |
| `services/pipeline_jobs.py` | `job_store/*.json` | Redis live + CK history | progress 需持久化 |
| `api/strategy/*`、`main.py` | `data/strategies` | shared strategies | 文件即代码不变 |
| `data_providers/custom/*` | `data/data_sources`、`data/plugins` | 数据源 PG，插件 shared | loader 路径改门面 |
| `api/ext_pages.py` | 本地 page.json + bundle.js | PG `ext_page` + blob | 导入信任模型不变 |
| `api/analysis.py` | 本地菜单配置 | PG 表或 user_preference 子文档 | API 契约不变 |
| `api/settings/_routes_system.py` | 本地音频文件 | PG `sound_pack` + blob | 文件下载改代理读 |
| `storage/artifact_store.py` | shared-root/s3 | 增加 pgblob | 读取统一 resolve |
| `services/config_backup.py` | 按文件路径分类 | 导出 PG + Redis 白名单 + blob 清单 + shared | 快照格式 bump v2 |

## 9. 启动流程

```text
1. 加载 settings，校验 PG/Redis 配置
2. 初始化 PG pool，生命周期绑定 FastAPI lifespan
3. 执行 tsp_user_state.schema_migrations
4. 初始化 CK schema（沿用现有流程，版本 5）
5. 初始化 CacheBackend / UserStateCache
6. 校验 shared/strategies 与 shared/plugins 可读写
7. 读取 PG preferences/capabilities/auth 到进程缓存
8. 从 CK 恢复 interrupted jobs，写 Redis live state
9. 启动 API 与后台线程
```

失败策略：

- PG 不可达：启动失败；
- Redis 不可达：启动失败；
- CK 不可达：沿用现有 fail-closed；
- shared 目录不可写：启动失败；
- blob 超限：业务请求失败，明确错误码与日志。

## 10. 迁移工具

新增：

```text
backend/scripts/migrate_user_state.py
```

命令：

```bash
python scripts/migrate_user_state.py --source-dir /app/data --dry-run
python scripts/migrate_user_state.py --source-dir /app/data --apply
python scripts/migrate_user_state.py --source-dir /app/data --verify
```

迁移顺序：

```text
1. auth.json -> user_auth
2. api_tokens.json -> api_token
3. secrets.json -> user_secret（加密）
4. preferences.json -> user_preference
5. capabilities.json -> capability_cache
6. watchlist.parquet / watchlist_groups.json -> watchlist 三表
7. daily_integrity_state.json -> daily_integrity / integrity_revision
8. integrity_report.json -> integrity_report + CK 历史
9. .integrity_repair_backoff.json -> repair_backoff
10. job_store/*.json -> Redis live + CK history
11. data_sources/*.yaml -> data_source_config
12. ext_pages / sound_packs / integrity_backups / mining artifacts -> user_blob + 元数据表
13. strategies/ -> shared/strategies
14. plugins/ -> shared/plugins
15. 写 state_migration_audit + CK 配置审计
```

幂等要求：

- 每张 PG 表按业务唯一键 UPSERT；
- blob 按 `content_hash` 去重；
- Redis key 写入前检查 generation；
- checksum 一致则 skip；
- checksum 不一致则 fail，不覆盖；
- `--verify` 对比数量、checksum、关键字段抽样。

迁移期保留旧文件只读；验证通过后才允许清理。

## 11. 备份与恢复

`config_backup.py` 快照格式升级到 v2：

```json
{
  "format": "v2",
  "pg_tables": {},
  "blob_hashes": [],
  "shared_files": [],
  "redis_keys": {},
  "ck_history": []
}
```

规则：

- session 不备份；
- Redis 热缓存不备份；
- job 当前态不备份，历史从 CK；
- user_blob 必须备份；
- shared strategies/plugins 必须备份；
- 恢复后 bump 所有 generation 并发布 config-change。

## 12. 测试设计

### 12.1 单元测试

- state repository CRUD；
- AES-GCM secret 加解密；
- token 哈希与脱敏；
- blob hash 去重；
- migration dry-run / apply / verify；
- Redis generation bump。

### 12.2 集成测试

- PG/Redis 不可达启动失败；
- 多 worker 并发 preferences 写入无丢失更新；
- 多 worker watchlist 重排一致；
- token 撤销即时生效；
- integrity fail-closed；
- job progress 跨 worker 可见；
- Redis 清空后重新登录；
- CK 历史任务恢复为 interrupted。

### 12.3 迁移测试

用 fixture 构造旧 `data/` 目录：

```text
auth.json
api_tokens.json
secrets.json
preferences.json
capabilities.json
watchlist.parquet
watchlist_groups.json
daily_integrity_state.json
integrity_report.json
job_store/
strategies/
plugins/
data_sources/
ext_pages/
sound_packs/
```

验证：

- `--dry-run` 不写入任何目标；
- `--apply` 可重复执行；
- `--verify` 计数/hash/字段一致；
- 失败时不覆盖已有目标数据。

### 12.4 回归测试

- `bash run_tests.sh -q`；
- `cmp_api_all_pages.py` 双栈对照；
- ruff / ruff format / mypy；
- DEBUG 日志检查；
- compose 重建后持久化检查。

## 13. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| PG 规则适用范围变化 | 实施 PR 同步修订 `tsp-rule/pgsql.mdc`，不降低约束 |
| Redis 被误用为持久权威 | 通过 UserStateCache 门面限制；持久权威只放 PG/CK |
| secrets 泄露 | AES-GCM、API 脱敏、Redis 不缓存明文 |
| blob 膨胀 | 64MiB 上限、内容寻址去重、超限显式失败 |
| 多 worker 缓存不一致 | generation + Pub/Sub + TTL 三重失效 |
| 迁移覆盖新数据 | checksum 不一致 fail，不覆盖 |
| shared 目录被滥用 | 只挂 strategies/plugins，代码审查路径白名单 |
| 桌面版无 PG/Redis | 决策 D-04 要求强制依赖；安装包需提供或明确启动失败 |

## 14. 实施阶段

| 阶段 | 内容 | 交付 |
| --- | --- | --- |
| P0 | `app/state` 门面、PG pool、CacheBackend 包装、schema_migrations | 基础设施 |
| P1 | auth / token / secrets / preferences / capabilities | 认证配置迁移 |
| P2 | watchlist / integrity / repair backoff | 热状态一致性 |
| P3 | job state / progress / CK history | 任务状态迁移 |
| P4 | strategies/plugins shared、data_sources PG、blob store | 文件与二进制迁移 |
| P5 | 迁移工具、备份恢复、compose 切换 | 可切换版本 |
| P6 | 多 worker 测试、双栈对照、旧数据清理 | 验收完成 |

## 15. 验收标准

1. compose 只保留 `logs` 与 `shared` 两个宿主机 bind mount；
2. 删除旧 `./data` 非 logs/shared 内容后服务正常启动；
3. PG/Redis 不可达时启动失败并输出明确配置提示；
4. 用户密码、token、偏好、自选、完整性状态、任务历史不丢；
5. Redis 清空后用户重新登录，其他状态不丢；
6. 多 worker 配置/自选/完整性读写一致；
7. secrets 全部通过 API/UI 配置，不再读 `secrets.json`；
8. strategies/plugins 从 shared 目录加载；
9. mining artifacts 通过 `pgblob://` 可读写；
10. 备份快照可恢复到新 PG/Redis 环境；
11. PG/CK 表均满足对应 tsp-rule 的 id、审计字段、注释要求；
12. 双栈接口对照不新增未解释的 API 契约差异。
