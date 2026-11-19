# 模拟盘账户事务层设计（S3：引入 PostgreSQL）

> 文档类型：设计 | 编号：DES-005 | 状态：已实现（2026-11-19 决策拍板并同日落地） | 文档版本：v1.0 | 最后更新：2026-11-19
> 负责人：待分配 | 关联：ISS-004（DIST-05、DIST-06 均已完成）、DES-004（§2.4 否决已修订、§4 S3/S4 均已解锁）、tsp-rule/pgsql.mdc（库表设计约束）、deployment.md（单 worker 约束已解除）

## 0. 文档状态与边界（先读）

**本文决策已于 2026-11-19 拍板并同日落地。** PG 事务层、5 个 paper 模块存储改造、12 个测试文件重写均已完成，账户事实源为 PG；前端零改动。账户不再是 S4 单 worker 的前置项；S4 其余前置（启动钩子单飞、四后端切 redis、多 worker 门禁）也已于同日完成，见 ISS-004 DIST-05。并发成交的重算已移入 account 行锁内，避免跨 worker 旧快照覆盖现金。

已批准决策（2026-11-19）：

| 项 | 决策 |
| --- | --- |
| 事务层 | **引入关系库**，推翻 DES-004 §2.4 原"否决 MySQL/关系库"决策（已同步修订） |
| 选型 | **PostgreSQL**（`psycopg[binary]`） |
| CK 审计 | **非强制**——关系库事务日志 + 备份即权威，不默认双写 CK |
| NAV / auto_rules | **一并迁入** PG（无历史数据，直接建库） |
| 历史数据 | **不存在历史数据，无任何历史负担**——不做迁入脚本、不做 file 后端兼容、不做双后端对照、不做回滚路径 |
| 前端 | **保持原样，一切后端改动不影响前端**——API 路由/契约/字段/事件结构完全不变 |
| 库表规范 | 每表必须有 `id` 主键 + 创建人/创建时间/更新人/更新时间四审计字段 + 表注释 + 字段注释；约束见 `tsp-rule/pgsql.mdc` |

本文回答的问题：**如何用 PG 事务层支持多 worker 同时写模拟盘账户，保证账目正确，工作量多大。**

不在本文范围：
- S4 其他前置（启动钩子跨 worker 单飞、全 worker 切 redis 后端、多 worker 门禁）——见 ISS-004 DIST-05；
- 账户业务语义变更（撮合规则、费率、T+1、除权）——只搬存储层，不改口径；
- 任何前端改动——前端保持原样，后端改动对前端透明。

## 1. 背景：为什么要引入事务层

### 1.1 S3 评估结论

账户是**资金状态**，多 worker 并发写会账目分叉（P0），不能像 RC-06 `DataStatusCache` 那样以"结果一致"软取舍放行（ISS-004 DIST-06）。原决策是保留单写者；本次评审决定改为引入关系库事务层，以解除 S4 的账户前置。

### 1.2 现状写路径（改造源）

来源：`backend/app/strategy/paper.py` / `paper_positions.py` / `paper_fill.py` / `paper_orders.py` / `paper_auto.py` / `api/paper.py`。

事实源设计只**部分**成立：`fills.jsonl` 是重建依据，但读路径直接信任物化 `account.json` / `positions.json`（`paper_positions.py:45`、`paper.py:258`），不是每次读都重放。

一次成交的写入组（`paper_fill.py:96-109`）共 5 步，**连进程内都不是整体原子**：append fills（普通 `open("a")`，`paper_positions.py:163`，仅靠 `PAPER_LOCK` 防交错）→ 重写 order → `replay_positions()` 算 cash（纯函数 `paper_positions.py:57`）→ 重写 account.cash → 重写 positions。

四条写路径：`api/paper.py` API、行情轮询（`quote_service.py:1609`，1s~6s/账户）、盘后结算（`daily_pipeline_run.py:238`）、自定义执行（`custom/paper_execution/service.py`）。`PAPER_LOCK` 是进程内 `RLock`（`paper.py:152`），无跨进程锁/版本/CAS。

> 上述文件写路径是要被 PG 事务替换的**改造对象**，不是需要兼容的历史数据——项目不存在需迁移的历史账户数据，无需读入/迁移任何旧文件。

### 1.3 DES-004 §2.4 否决的修订

原否决"不引入 MySQL/关系库"的前提之一是"账户无事务需求"——S3 调研发现该前提与代码事实不符（写入组正是真实 OLTP 事务）。本次评审正式修订 DES-004 §2.4：从"否决关系库"改为"账户事务层引入 PG"。修订与本文决策同步生效。

## 2. 选型：PostgreSQL

PG/MySQL 都提供事务 + 行锁 + 崩溃恢复，远比"Redis Stream + Lua + CK 审计"的手工分布式事务方案干净。评审选定 **PG**：

- `JSONB` 原生二进制 + GIN 索引，fill（12+ 字段）/order（20+ 字段）的 dict 可直接存，减少列定义；
- `SELECT ... FOR UPDATE` 行锁 + 默认 `READ COMMITTED` 隔离即满足本场景（见 §4），无需更高隔离级别的额外重试；
- `INSERT ... ON CONFLICT DO UPDATE` UPSERT 干净，窗口函数便于重放；
- Python 侧 `psycopg[binary]` 成熟。

> 关于隔离级别：PG 支持 `SERIALIZABLE`（比 MySQL 默认更强），但本设计用 account 行锁串行化同账户写即可消除并发分叉，默认 `READ COMMITTED` 已足够，不引入 `SERIALIZABLE` 的 `40001 serialization failure` 重试复杂度。若将来出现跨账户一致性需求再评估升级。

## 3. 库表设计

**全部表遵循 `tsp-rule/pgsql.mdc` 库表设计约束**：每张表有 `id` 主键 + `created_by`/`created_at`/`updated_by`/`updated_at` 四审计字段 + 表注释 + 字段注释。审计字段取值约定：`created_by`/`updated_by` 记录操作来源主体（`api` / `scheduler` / `paper_watch` / `pipeline` / `system`），当前单密码模式无用户身份，不强行造 user_id；审计时间取当前操作时间（`now()` 或应用层注入）。

### 3.1 账户表 `paper_account`

```sql
CREATE TABLE paper_account (
  id                 TEXT PRIMARY KEY,                    -- 主键：UUID4 hex
  account_id         TEXT NOT NULL UNIQUE,                -- 业务账户 ID（对外契约不变）
  name               TEXT NOT NULL,
  initial_cash       NUMERIC(18,2) NOT NULL,
  cash               NUMERIC(18,2) NOT NULL,
  commission_pct     NUMERIC(10,6) NOT NULL,
  stamp_tax_pct      NUMERIC(10,6) NOT NULL,
  slippage_bps       INTEGER NOT NULL,
  queue_limit_orders BOOLEAN NOT NULL,
  status             TEXT NOT NULL,                      -- active / frozen
  version            BIGINT NOT NULL DEFAULT 0,          -- 内部版本号，随事务递增，不对外暴露
  created_by         TEXT NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL,
  updated_by         TEXT NOT NULL,
  updated_at         TIMESTAMPTZ NOT NULL
);
COMMENT ON TABLE paper_account IS '模拟盘账户静态配置与当前现金；资金状态，事务写入组在事务内定版 cash';
COMMENT ON COLUMN paper_account.id IS '主键，UUID4 hex';
COMMENT ON COLUMN paper_account.account_id IS '业务账户 ID，对外契约不变';
COMMENT ON COLUMN paper_account.name IS '账户展示名';
COMMENT ON COLUMN paper_account.initial_cash IS '初始现金';
COMMENT ON COLUMN paper_account.cash IS '当前可用现金，由事务写入组定版';
COMMENT ON COLUMN paper_account.commission_pct IS '佣金费率';
COMMENT ON COLUMN paper_account.stamp_tax_pct IS '印花税率';
COMMENT ON COLUMN paper_account.slippage_bps IS '滑点，基点';
COMMENT ON COLUMN paper_account.queue_limit_orders IS '是否限定涨跌停排队';
COMMENT ON COLUMN paper_account.status IS '账户状态：active/frozen';
COMMENT ON COLUMN paper_account.version IS '内部版本号，随事务递增，暂不对外暴露 ETag';
COMMENT ON COLUMN paper_account.created_by IS '创建人/操作来源';
COMMENT ON COLUMN paper_account.created_at IS '创建时间';
COMMENT ON COLUMN paper_account.updated_by IS '更新人/操作来源';
COMMENT ON COLUMN paper_account.updated_at IS '更新时间';
```

### 3.2 成交台账 `paper_fill`（append-only）

```sql
CREATE TABLE paper_fill (
  id         TEXT PRIMARY KEY,                            -- 主键：UUID4 hex
  account_id TEXT NOT NULL,                               -- 所属账户
  seq        BIGINT NOT NULL,                              -- 账户内递增序号（事务内 MAX(seq)+1 分配）
  ts         TIMESTAMPTZ NOT NULL,                         -- 成交时间
  date       DATE NOT NULL,                               -- 交易日
  order_id   TEXT NOT NULL,                               -- 关联订单 ID
  payload    JSONB NOT NULL,                              -- 完整 fill dict 原样落库（重放数据源）
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_paper_fill_account_seq UNIQUE (account_id, seq)
);
CREATE INDEX paper_fill_account_ts ON paper_fill (account_id, ts);
COMMENT ON TABLE paper_fill IS '模拟盘成交台账，append-only 事件溯源；事务写入组内唯一新增行，不可更新/删除';
COMMENT ON COLUMN paper_fill.id IS '主键，UUID4 hex';
COMMENT ON COLUMN paper_fill.account_id IS '所属账户 ID';
COMMENT ON COLUMN paper_fill.seq IS '账户内递增序号，事务内行锁保护下 MAX(seq)+1 分配，与 account_id 组成唯一约束';
COMMENT ON COLUMN paper_fill.ts IS '成交时间戳';
COMMENT ON COLUMN paper_fill.date IS '交易日';
COMMENT ON COLUMN paper_fill.order_id IS '关联订单 ID';
COMMENT ON COLUMN paper_fill.payload IS '完整 fill 字段 JSON，重放唯一数据源，保持对外事件结构不变';
COMMENT ON COLUMN paper_fill.created_by IS '创建人/操作来源';
COMMENT ON COLUMN paper_fill.created_at IS '创建时间';
COMMENT ON COLUMN paper_fill.updated_by IS '更新人/操作来源';
COMMENT ON COLUMN paper_fill.updated_at IS '更新时间';
```

### 3.3 订单表 `paper_order`

```sql
CREATE TABLE paper_order (
  id         TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  order_id   TEXT NOT NULL,                               -- 业务订单 ID
  status     TEXT NOT NULL,                               -- 订单状态
  payload    JSONB NOT NULL,                              -- 完整 order dict
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_paper_order_account_order UNIQUE (account_id, order_id)
);
COMMENT ON TABLE paper_order IS '模拟盘订单状态；事务写入组内更新 status/payload';
COMMENT ON COLUMN paper_order.id IS '主键，UUID4 hex';
COMMENT ON COLUMN paper_order.account_id IS '所属账户 ID';
COMMENT ON COLUMN paper_order.order_id IS '业务订单 ID，与 account_id 组成唯一约束';
COMMENT ON COLUMN paper_order.status IS '订单状态（pending/filled/cancelled 等）';
COMMENT ON COLUMN paper_order.payload IS '完整订单字段 JSON';
COMMENT ON COLUMN paper_order.created_by IS '创建人/操作来源';
COMMENT ON COLUMN paper_order.created_at IS '创建时间';
COMMENT ON COLUMN paper_order.updated_by IS '更新人/操作来源';
COMMENT ON COLUMN paper_order.updated_at IS '更新时间';
```

### 3.4 持仓表 `paper_position`

```sql
CREATE TABLE paper_position (
  id         TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  symbol     TEXT NOT NULL,                               -- 标的代码
  asset_type TEXT NOT NULL,                               -- 资产类型
  qty        BIGINT NOT NULL,                             -- 总持仓
  avg_cost   NUMERIC(18,6) NOT NULL,                      -- 平均成本
  available  BIGINT NOT NULL,                             -- 可用持仓
  lots       JSONB NOT NULL,                              -- FIFO lot 明细
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_paper_position_account_symbol UNIQUE (account_id, symbol)
);
COMMENT ON TABLE paper_position IS '模拟盘物化持仓，可由 paper_fill 重放；事务写入组内 UPSERT';
COMMENT ON COLUMN paper_position.id IS '主键，UUID4 hex';
COMMENT ON COLUMN paper_position.account_id IS '所属账户 ID';
COMMENT ON COLUMN paper_position.symbol IS '标的代码，与 account_id 组成唯一约束';
COMMENT ON COLUMN paper_position.asset_type IS '资产类型（stock/etf 等）';
COMMENT ON COLUMN paper_position.qty IS '总持仓数量';
COMMENT ON COLUMN paper_position.avg_cost IS '平均成本价';
COMMENT ON COLUMN paper_position.available IS '可用持仓数量（T+1 后可售）';
COMMENT ON COLUMN paper_position.lots IS 'FIFO lot 明细 JSON';
COMMENT ON COLUMN paper_position.created_by IS '创建人/操作来源';
COMMENT ON COLUMN paper_position.created_at IS '创建时间';
COMMENT ON COLUMN paper_position.updated_by IS '更新人/操作来源';
COMMENT ON COLUMN paper_position.updated_at IS '更新时间';
```

### 3.5 自动规则表 `paper_auto_rule`

```sql
CREATE TABLE paper_auto_rule (
  id         TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  rule_id    TEXT NOT NULL,                               -- 业务规则 ID
  payload    JSONB NOT NULL,                              -- 规则配置 dict
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_paper_auto_rule_account_rule UNIQUE (account_id, rule_id)
);
COMMENT ON TABLE paper_auto_rule IS '模拟盘自动交易规则；直接建库，无历史数据';
COMMENT ON COLUMN paper_auto_rule.id IS '主键，UUID4 hex';
COMMENT ON COLUMN paper_auto_rule.account_id IS '所属账户 ID';
COMMENT ON COLUMN paper_auto_rule.rule_id IS '业务规则 ID，与 account_id 组成唯一约束';
COMMENT ON COLUMN paper_auto_rule.payload IS '规则配置 JSON';
COMMENT ON COLUMN paper_auto_rule.created_by IS '创建人/操作来源';
COMMENT ON COLUMN paper_auto_rule.created_at IS '创建时间';
COMMENT ON COLUMN paper_auto_rule.updated_by IS '更新人/操作来源';
COMMENT ON COLUMN paper_auto_rule.updated_at IS '更新时间';
```

### 3.6 净值表 `paper_nav`

```sql
CREATE TABLE paper_nav (
  id         TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  day        DATE NOT NULL,                               -- 净值日期
  payload    JSONB NOT NULL,                              -- 当日净值快照 dict
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  CONSTRAINT uq_paper_nav_account_day UNIQUE (account_id, day)
);
COMMENT ON TABLE paper_nav IS '模拟盘盘后净值历史；直接建库，无历史数据';
COMMENT ON COLUMN paper_nav.id IS '主键，UUID4 hex';
COMMENT ON COLUMN paper_nav.account_id IS '所属账户 ID';
COMMENT ON COLUMN paper_nav.day IS '净值日期，与 account_id 组成唯一约束';
COMMENT ON COLUMN paper_nav.payload IS '当日净值快照 JSON';
COMMENT ON COLUMN paper_nav.created_by IS '创建人/操作来源';
COMMENT ON COLUMN paper_nav.created_at IS '创建时间';
COMMENT ON COLUMN paper_nav.updated_by IS '更新人/操作来源';
COMMENT ON COLUMN paper_nav.updated_at IS '更新时间';
```

`payload` 保留现有 dict 结构（fill 字段见 `paper_fill.py:177-190`），**这是为了对外事件/API 结构不变（前端零改动），不是为兼容旧文件**。业务唯一键用 `UNIQUE` 约束而非主键（主键统一为 `id`）。

## 4. 写入组（一次成交 = 一个事务）

当前 `_record_fill` 的 5 步非原子写入改为单事务，隔离级别默认 `READ COMMITTED`，每张写入表都填四审计字段：

```text
BEGIN;  -- READ COMMITTED（默认）
  SELECT * FROM paper_account WHERE account_id = ? FOR UPDATE;          -- account 粒度行锁
  SELECT payload FROM paper_fill WHERE account_id = ? ORDER BY seq;     -- 事务快照内读台账（重放源 = payload）
  -- 计算 seq = MAX(seq)+1（行锁内，同账户串行，无并发冲突）
  -- Python 调 replay_positions(fills, acc) → (positions, cash)         -- 纯函数本体不动
  INSERT INTO paper_fill (id, account_id, seq, ..., payload, created_by, created_at, updated_by, updated_at)
    VALUES (...);                                                       -- 记成交
  UPDATE paper_order SET status='filled', payload=?, updated_by=?, updated_at=? ...;
  UPDATE paper_account SET cash=?, version=version+1, updated_by=?, updated_at=? ...;
  INSERT INTO paper_position ... ON CONFLICT (account_id, symbol)
    DO UPDATE SET qty=..., lots=..., updated_by=..., updated_at=...;
  INSERT INTO paper_nav ... ON CONFLICT (account_id, day) DO UPDATE ...（如影响当日净值）;
COMMIT;
```

要点：
- **隔离级别 `READ COMMITTED` + account 行锁**：不同账户并行撮合、同账户串行，由引擎保证；不引入 `SERIALIZABLE` 的 40001 重试。
- **seq 在行锁内分配**：`MAX(seq)+1` 在账户行锁保护下计算，同账户串行无冲突，避免全局 sequence 跳号。
- **重放数据源 = `payload`**：`paper_fill.payload` 是重放唯一数据源，`ts/date/order_id` 等结构化列仅供查询/展示；`replay_positions()` 读 payload 还原 dict，保证对外事件结构不变。
- **重放必须在事务内**：cash/positions 是 fills 派生值，必须在同一快照内算并写，否则两 worker 仍会算出不同结果。
- **`replay_positions()` 本体完全不改**（`paper_positions.py:57`，已接收 fills list 参数），只把数据源从文件换成事务内 payload——这是改动可控的关键。
- **崩溃自动回滚**：事务原子性消除"fills 写了但 cash 没改"的中间态。
- **`version` 是内部字段**：随事务递增，不对外暴露 ETag；前端与 API 契约不变。

## 5. CK 审计（评审结论：非强制，默认关闭）

关系库本身是账户持久化权威（事务日志 + 备份），不像 Redis 那样"丢得起"。评审决定**不默认写 CK 审计**：`paper_fill` 表 + 备份即权威审计副本。若将来有合规要求"独立审计副本"，可按 `ck_mining_run_audit` 同构补一张 `ck_paper_fill_audit`（五审计列 + `PARTITION BY account_id` / `ORDER BY (account_id, seq)`），作为独立增强项，不影响本设计主线。

## 6. 存储改造与前端零改动

- **不再保留 `file` 后端**：无历史数据、无兼容负担，账户读写入口直接改为 PG 实现（`paper_store.py`），不引入 `paper_store_backend` 开关、不做 `FilePaperStore`、不做双后端对照。
- 改造范围：`paper.py` / `paper_positions.py` / `paper_fill.py` / `paper_orders.py` / `paper_auto.py` 的读写入口改为走 PG 存储层，业务逻辑保持。
- 读映射：`get_account` → `SELECT`；`load_positions` → 读 `paper_position`，缺行时从 `paper_fill.payload` 重放；`load_fills` → `SELECT payload ... ORDER BY seq`；NAV / auto_rules 同理。
- `api/paper.py` 写路由经服务层走 PG 存储层，**路由签名、请求/响应字段、事件结构完全不变**；`version` 不对外暴露。
- **前端保持原样**：本次所有后端改动对前端透明；不新增"维护态"等前端概念，不修改任何前端文件。

## 7. 全新部署（无历史数据、无迁移）

评审确认项目**不存在需迁移的历史账户数据**，因此：

- 不做迁入脚本、不做双写期、不做回滚路径、不做 file 后端兼容；
- `paper_*` 六表在 PG 中直接建空库（DDL 见 §3），账户由用户从头创建；
- 部署时只需保证 `paper_*` 表已建（schema 迁移脚本建表，见 §9）；
- 无"停机维护窗口"需求——也就不需要前端维护态或后台写路径暂停（原 §1.2 的四条写路径在切换时不存在历史数据，改造后统一走 PG 事务）。

这大幅简化迁移复杂度：从"文件→库"迁移降为"空库初始化"。

## 8. 方案对比（为什么选关系库）

| 维度 | A 保留单写者 | B1 Redis Stream + Lua + CK | B3 关系库事务（本文，已选） |
| --- | --- | --- | --- |
| 原子性 | 进程内 `PAPER_LOCK` | Lua + 手工锁 + 审计滞后窗口 | 数据库事务原生保证 |
| 崩溃恢复 | 无跨进程场景 | CK 重建链路 + 锁 TTL 兜底 | redo/undo 自动回滚 |
| 新增组件 | 无 | 无（复用 Redis/CK） | 新增 PG |
| 代码改动 | 无 | 大 | 中（4~5 模块 + store 层） |
| 正确性风险 | 单 worker 下无 | 高（手工分布式事务） | 低（成熟引擎） |
| 对 S4 | 阻塞 | 解除账户项 | 解除账户项 |

## 9. 工作量评估

| 模块 | 工作 | 人天 |
| --- | --- | --- |
| 依赖 / config / docker-compose / 部署文档 | 加 PG 服务 + DSN | 0.5–1 |
| Schema DDL + 注释（6 表，空库建表脚本） | 含 id/四审计字段/表字段注释/唯一约束 | 1–1.5 |
| `paper_store.py`（PG） | CRUD + 事务写入组 + 行锁 + seq 分配 | 2–3 |
| 改造 5 个 paper 模块的读写入口 | `paper.py` / `paper_positions.py` / `paper_fill.py` / `paper_orders.py` / `paper_auto.py` | 2–3 |
| API 层 + 启动钩子适配 | `api/paper.py` 走 PG 存储层（签名不变）、`main.py` 建表入口 | 0.5–1 |
| 测试 | 现有 36 用例跑 PG + **新增多进程写冲突测试** + 重放/回滚测试 | 2–3 |
| 文档 | DES-004 §2.4 修订（已完成）+ 本文定稿 + ISS-004 状态 | 0.5 |

**合计约 8–12 人天**（核心代码 5–7 人天）。因无历史迁移/双写/回滚/双后端对照，较原估算下调。

## 10. 风险与缓解

| 风险 | 描述 | 缓解 |
| --- | --- | --- |
| 重放正确性 | 从 `paper_fill.payload` 重放必须算出正确 cash/positions | 单测覆盖重放结果与预期一致；`replay_positions()` 本体不改 |
| 事务内持锁跑 Python 重放 | 锁持有 = 1 次查询 + Python 计算 + 数次写，高成交账户需压测 | 压测；必要时改 SQL 窗口函数（保留 Python 版作对照） |
| 多组件运维 | CK+Redis+PG 三套的部署/备份/监控 | 复用现有 compose 模式；评估自托管轻量定位 |
| 审计字段取值 | 单密码模式无用户身份，`created_by/updated_by` 记操作来源 | 按 §3 约定取值；将来引入用户体系再扩展 |
| 前端零改动约束 | 后端换存储但对外契约必须完全不变 | API 路由/字段/事件结构不改；`version` 不暴露；不新增前端概念 |

## 11. 验收标准

1. 6 张表 DDL 满足 `tsp-rule/pgsql.mdc`：id 主键 + 四审计字段 + 表注释 + 字段注释；
2. 现有 36 个 paper 用例在 PG 后端全绿，重放结果正确；
3. 新增多进程并发写同一账户测试：无丢成交、无重复成交、现金与持仓一致；
4. 崩溃注入：事务中途失败后账户状态完整回滚；
5. 空库初始化：`paper_*` 表建表脚本可执行，账户可从零创建；
6. 前端零改动：API 路由/字段/事件结构与改造前逐项一致，无前端文件变更；
7. `git diff --check` 与相关质量门禁通过；全量回归按项目门禁安排执行。

## 12. 决策记录（2026-11-19 拍板）

1. 引入关系库，推翻 DES-004 §2.4 原否决（已同步修订）；
2. 选型 **PostgreSQL**；
3. CK 审计**非强制**，默认关闭；
4. NAV / auto_rules **一并迁入**（直接建库，无历史数据）；
5. **无历史数据 / 无历史负担**：不做迁入、双写、回滚、file 后端兼容、双后端对照；
6. 库表统一 `id` 主键 + 四审计字段 + 表/字段注释，约束登记于 `tsp-rule/pgsql.mdc`；
7. **前端保持原样**：后端改动不影响前端，API 契约与事件结构不变；
8. 事务隔离级别默认 `READ COMMITTED` + account 行锁，不引入 `SERIALIZABLE` 重试。

## 13. 实现落地记录（2026-11-19）

| 项 | 落地情况 |
| --- | --- |
| PG 基础设施 | `pg_dev.sh` 复用宿主镜像启动 `tsp-pg-dev`（`tsp_dev`/`tsp`/`123456`），接入 `dev.container.sh`；`.env.example` 与 compose 注入 `PAPER_PG_URL(_CONTAINER)`；`psycopg[binary]` 入依赖并同步 `uv.lock` |
| 存储层 | `app/storage/paper_pg.py`：6 表 DDL/注释、advisory lock 跨进程串行建表、`transaction()`、账户/订单/台账/持仓/NAV/规则 CRUD、`record_fill_transaction()` 事务写入组 |
| 模块改造 | `paper.py` / `paper_auto.py` / `paper_fill.py` / `paper_orders.py` / `paper_positions.py` 全量走 PG；`data_dir` 签名保留兼容调用方，实际按 `account_id` 隔离；成交事件时间戳改由 `fill.ts` 解析（PG 台账 `seq` 为递增序号） |
| 测试 | 12 个 paper 测试文件重写，唯一 `account_id` + teardown 清理；新增多进程并发 seq 唯一、事务回滚、重放一致性测试 |
| 验证 | paper 定向测试全绿；全量回归最终 `4898 passed / 0 failed`（期间 `ch_runtime` redis 污染、`abnormal_moves` 缓存口径差异已通过 conftest 锁定四后端 memory 修复）；ruff 无新增告警；`git diff --check` 通过 |
| 配置备份 | `config_backup` 的 `paper` 类别改为从 PG 导出账户快照（`paper/accounts/{id}/snapshot.json` 虚拟条目），导入按账户整体事务恢复；默认已存在则跳过，`allow_overwrite` 才替换；ZIP/manifest/API 契约与前端零改动 |
| 测试环境 | `tests/conftest.py` 在任何 `app.*` 导入前强制 `CACHE_BACKEND=memory`，消除开发机 `.env` 的 redis 配置导致的共享缓存跨用例污染（曾致全量回归 8 例失败） |
| 前端 | 无前端文件改动，API 路由/字段/事件结构不变 |
