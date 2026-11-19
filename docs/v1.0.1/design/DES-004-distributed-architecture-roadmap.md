# 分布式架构规划：边界、权威归属与迁移顺序

> 文档类型：设计 | 编号：DES-004 | 状态：已批准（2026-11-19 决策拍板，实施中） | 文档版本：v1.1 | 最后更新：2026-11-19
> 负责人：待分配 | 关联：ISS-004、DES-001、DES-002、DES-003、DES-005（账户事务层评估，待评审）、ISS-003、REQ-002、RC-02、RC-05~RC-08

## 0. 文档状态与目标

**本文的架构决策已于 2026-11-19 拍板批准（§2 四条边界 + §5 D-01 选方案 C），具体实施按 §4 分阶段推进——文中标注"待定/未迁"的组件尚未完成迁移，不得按已实现能力调用。** S2 的 mining 状态机 + 跨 worker 取消协调 + 方案 C 路径 flock 出局已于 2026-11-19 完成；默认 memory 文件版仍保留 flock 作离线兼容。目标是把项目从"单节点、单机锁、进程内状态"渐进演进到分布式系统，给出统一的方向和判断规则。

本文回答四个边界问题（§2），给出组件权威归属（§3）、迁移顺序（§4），并把 mining 的状态并发方案（DES-003 D-01）放进整体决策树（§5）。~~不引入 MySQL~~ **原否决已于 2026-11-19 修订：账户事务层引入 PostgreSQL**（§2.4 与 DES-005），CK 继续作为分析型数据权威，Redis 升格为协调态权威。

## 1. 现状盘点（已分布式就绪 vs 未就绪）

| 组件 | 当前实现 | 分布式就绪 | 证据 |
| --- | --- | --- | --- |
| 缓存层 | `CacheBackend` 可切 `RedisCacheBackend` | 是 | RC-01；RC-05 strategy 缓存 Hash+Lua |
| SSE 事件 | `EventBus` 可切 Redis Pub/Sub | 是 | RC-07 |
| 任务协调 | 取消标志 / 限流 / 任务状态可共享 | 是 | RC-08；`coordination.py` 三套后端契约 |
| 行情 / 回测数据 | CK（`ck_daily` / `ck_backtest_*`） | 是 | DES-001、DES-002 |
| mining run 状态 | 本机文件 + flock（默认兼容）/ Redis Lua 状态机（方案 C） | 方案 C 已实现，跨 worker 取消与 flock 出局 2026-11-19 完成 | ISS-004 MN-01~MN-05、DES-003、DIST-03/04 |
| 模拟盘账户 | PostgreSQL 事务层（6 表 + account 行锁） | 是（DES-005 2026-11-19） | DES-005 §13 |
| API 层 | `uvicorn --workers ${API_WORKERS}`（默认 2） | 是（S4 2026-11-19 解锁） | `deployment.md` 多 worker 段 |

结论：基础设施层、mining、模拟盘账户、API 多 worker 四处卡点均已打通（2026-11-19）；剩余为 S5 跨主机评估（可选，独立立项）。

## 2. 四个边界问题的决策

### 2.1 分布式边界：同一主机多进程起步，不堵死跨主机

**决策（已批准 2026-11-19）：先做同一主机多进程，接口与锁实现预留跨主机替换点。**

依据：
- 当前基础设施全部是单实例形态：单 CK 容器、单 Redis 容器、本地 `data/` 目录。跨主机意味着集群化与共享存储，是数量级改动。
- 同主机多进程已能覆盖真实需求：API 多 worker、mining 与 API 分离、后台任务独立进程。
- 锁方案上，同机多进程用 Redis 单实例锁即可（Redis 在同机，网络分区风险极低）；跨主机才需要 Redlock / etcd。先按同机设计，将来换锁实现不动上层契约。

代价：本地 `data/` 下的文件类状态（账户台账、flock 锁）在多进程下仍需处理写冲突——账户台账已决定迁 PostgreSQL（DES-005），flock 在方案 C 路径已出局。

### 2.2 API 层：进程内状态迁出（S4 已解锁多 worker）

**决策（已批准 2026-11-19）：先把进程内状态清单化逐个迁出，全部迁完后开多 worker 只是配置改动。同日已达成解锁条件**——账户迁 PG（DES-005）、四后端默认 redis、启动钩子经 `coordination.once()` 单飞；Dockerfile `API_WORKERS` 默认 2。早期"短期维持 `--workers 1`"是阶段性起点，现已解除。

"进程内状态"指只存在于单个 worker 内存、其他 worker 看不到的变量。当前清单：

| 进程内状态 | 位置 | 迁出状态 |
| --- | --- | --- |
| 策略结果缓存内存镜像 | `strategy_cache.py:50` | Redis 模式已实现（RC-05），memory 模式保留 |
| 数据状态快照 | `data_status_cache.py:16` | 决策保留本地（RC-06），接受各 worker 各自刷新 |
| mining 任务线程表 / 取消事件 | `mining_manager.py` `_threads`/`_cancel_events` | 2026-11-19 已迁：取消事件经 `_CoordinatedCancelEvent` 叠加 `coordination.cancel_flags` 跨 worker 传播，线程表降级为本地执行句柄（权威为共享 store + 协调层标志） |
| 重任务限额本地计数 | `heavy_job_limiter.py` | 部分由 RC-08 覆盖，需复核 |
| SSE 内存总线 | `events.py` | Redis Pub/Sub 已实现（RC-07） |
| 取消标志 / 限流 / 任务状态 | `coordination.py` | Redis 后端已实现（RC-08） |

多 worker 的风险是具体的：用户点"开始挖掘"落到 worker A，刷新页面落到 worker B，B 的线程表为空 → 显示"没有任务"；取消请求落到第三个 worker → 找不到任务。所以不是性能问题，是状态不一致导致错误行为。

**解锁条件（已满足 2026-11-19）**：上表"未迁"项已迁出——mining 线程表降级为本地执行句柄（权威在共享 store）、模拟盘账户迁 PG 事务层；限流本地计数经 `COORDINATION_BACKEND=redis` 的全局计数覆盖；SSE/策略缓存 Redis 模式已实现。启动钩子的跨 worker 重复执行由 `coordination.once()` 解决。`--workers N` 已生效（`API_WORKERS`）。

### 2.3 Redis 定位：升格为协调态权威，开 AOF，限定权威范围

**决策（已批准 2026-11-19，含 RC-02「不持久化」决策修订）：Redis 从"纯缓存"升格为"可重建的协调态权威"，开启 AOF（`appendfsync everysec`），但只放可重建的协调态，不放不可丢失的业务数据。**

- **Redis 权威（协调态）**：锁、任务状态、限流计数、取消标志、mining 状态机、strategy 缓存。这些丢得起——从 CK 审计或重算可重建，AOF 只用于减少重启恢复时间，不作为持久化保证。
- **CK 权威（业务数据）**：行情、回测结果、修补审计、generation、mining 审计日志。
- **事件溯源（账户）**：模拟盘台账 append-only，持仓/现金由重放推导。

代价与前置条件：
- RC-02 的"缓存不持久化"决策已修订：新容器默认 `--appendonly yes --appendfsync everysec`；已有容器用 `./redis-cache.sh aof on` 在线开启（无需重启）；部署文档已更新（2026-11-19 完成）。
- "从 CK 审计重建 Redis 协调态"已按方案 B 实现（2026-11-19）：
  - 新增三张 CK 审计表 `ck_coord_job_state` / `ck_coord_cancel_request` / `ck_coord_limiter_lease`（五审计列 + 表/字段注释规范）；
  - 新增 `app/services/coordination_audit.py`：业务层写入点（job 落盘 `pipeline_jobs._write_file`、取消 `request_cancel`、限流 `HeavyJobLimiter.acquire/release` 生成 lease_id）落 CK 审计；`rebuild_coordination()` 从三表 FINAL 读最新，回填 Redis JobState / CancelFlag / LimiterCounter；
  - `LimiterCounterBackend` 协议新增 `restore(used)` 用于覆盖式重建；
  - 启动钩子接入 `main.py` storage 初始化后，仅 `COORDINATION_BACKEND=redis` 执行；`enabled()` 保证 memory 模式与未初始化 CK 时零影响；
  - 定向测试 `test_coordination_audit.py` 6 passed（真实 CK + 假 coordination 后端）；相关回归 80 passed（pre-commit 全门禁通过；全量回归按用户要求推迟）。
  - 设计取舍：取消标志无 `clear()` 生命周期，重建时按 `requested_at > now-1800s`（与 Redis TTL 一致）过滤过期请求；租约按 `released_at IS NULL` 求和恢复计数。

### 2.4 一致性要求：分级——协调态强一致，业务数据最终一致

**决策（已批准 2026-11-19，关系库否决同日修订）：接受最终一致，但分三级明确"哪些立刻可见、哪些允许延迟、哪些可重放"。**

| 级别 | 数据 | 传播方式 |
| --- | --- | --- |
| 强一致（立刻可见） | mining 状态机、锁、限流计数、取消标志 | Redis 原子操作（Lua / WATCH） |
| 最终一致（秒级延迟） | CK 审计日志、历史 run 查询、报表统计 | 异步写入，读侧接受短暂滞后 |
| 可重放（写入即追加） | 模拟盘账户余额 / 持仓 | 事件溯源，台账追加后读侧重放 |

代价：前端要处理"状态已变但审计未落"的短暂窗口（run 显示 succeeded 但事件列表未刷出），用 SSE 或轮询兜底——SSE 基础设施已有，改造量可控。

**原否决（已修订）**：~~不引入 MySQL/关系库做事务层~~。该否决的前提之一"账户无事务需求"被 S3 调研证伪——账户写入组（一次成交改 fills/order/cash/positions）正是真实 OLTP 事务需求（ISS-004 DIST-06）。

> **修订决策（2026-11-19 拍板，已批准）**：引入 **PostgreSQL** 作模拟盘账户事务层，解除 S4 的账户前置。选型（PG）、非强制 CK 审计、NAV/auto_rules 直接建库、无历史数据/无迁移负担（不做迁入/双写/回滚/file 兼容）、前端零改动、库表约束（id 主键 + 四审计字段 + 表/字段注释）见 [DES-005](DES-005-paper-account-transaction-store.md)（已批准，待实现）与 `tsp-rule/pgsql.mdc`。CK 继续作为分析型数据权威，本修订不改变 §2.3 的 Redis/CK 职责边界。

## 3. 组件权威归属总表

| 数据 / 状态 | 权威存储 | 是否持久化 | 一致性级别 | 备注 |
| --- | --- | --- | --- | --- |
| 日线 / enriched / 指数 | CK | 是 | 最终一致 | DES-002 已完成 |
| 回测结果 / 交易记录 | CK | 是 | 最终一致 | append-only 历史，非事务负载 |
| generation / 修补审计 | CK | 是 | 最终一致 | 异步审计 |
| strategy 结果缓存 | Redis Hash | 否（可重算） | 强一致 | RC-05 已实现 |
| SSE 事件 | Redis Pub/Sub | 否 | 强一致（推送） | RC-07 已实现 |
| 取消 / 限流 / 任务状态 | Redis（权威）+ CK 审计 | AOF + CK | 强一致 | RC-08 已实现；CK 审计与重建 2026-11-19 已实现（§2.3 方案 B） |
| mining run 状态 / 事件 / 摘要 | Redis（权威，Lua 原子）+ CK 审计 | AOF + CK | 强一致 | 方案 C 已实现 2026-11-19（`RedisMiningRunStore` + `mining_audit`），默认 memory 文件版保留兼容 |
| mining 产物 artifact | 待定（MN-02） | — | 最终一致 | 倾向内容寻址 + CK 元数据 |
| 模拟盘账户台账 | PostgreSQL 事务层（DES-005，已实现） | 否 | 可重放 | 2026-11-19 决策修订并同日落地：引入 PG（6 表 + 事务 + account 行锁）支持多 worker 写，原"保留单写者"已推翻；账户项不再是 S4 前置，其余前置见 DES-005、ISS-004 DIST-06/DIST-05 |
| 挖掘锁 | 方案 C：无锁（Redis Lua 串行化）/ memory 文件版：本地 flock | 否 | 强一致 | 2026-11-19：redis 模式 flock 出局；memory 文件版保留 flock 单写者兼容 |

## 4. 迁移顺序

| 阶段 | 内容 | 解锁条件 / 门禁 |
| --- | --- | --- |
| S0 规划冻结 | 本文评审；DES-003 D-01 拍板；RC-02 AOF 决策修订 | 本文与 DES-003 评审通过 |
| S1 基础设施补齐（已完成 2026-11-19） | Redis 开 AOF（`redis-cache.sh` + 部署文档）；三张 CK 协调审计表 + `coordination_audit` 重建流程；启动钩子 | 定向 6 passed + 回归 80 passed；pre-commit 全绿；全量回归待后续批次 |
| S2 mining 分布式化（状态机 + 取消协调 + flock 出局 + 历史兼容验证完成 2026-11-19） | 已实现 `RedisMiningRunStore`（Lua 原子状态机 + 事件/摘要/claim，工厂 `get_run_store` 按 `mining_store_backend` 切换）+ `mining_audit`（CK 审计 + 重建）；取消事件经 `_CoordinatedCancelEvent.is_set()` 叠加 `coordination.cancel_flags` 跨 worker 传播，`main.lifespan`/`ensure_mining_lock` 在 redis 模式下不再抢 flock（DIST-03/MN-04/方案 C 路径）；MN-05 历史 run 读取归一化 + API 投影已验证（残留缺口登记待处理）；**待完成**：MN-02 产物 | 状态机 7 + 审计 3 + 跨实例取消 5 + 历史兼容 5 + mining 相关回归 77 passed；memory 文件版保留 flock 属已登记兼容取舍 |
| S3 模拟盘账户（决策修订 2026-11-19，**已实现**） | ~~保留单写者~~ → **引入 PostgreSQL 事务层**：账户写入组是真实 OLTP 事务，多 worker 并发写会账目分叉（P0）；选 PG 事务 + account 行锁保证原子，6 张表直接建库（无历史数据、无迁移负担），CK 审计非强制，NAV/auto_rules 一并建表，前端零改动。库表遵循 `tsp-rule/pgsql.mdc`；同日完成 5 个 paper 模块全量迁移 + 12 个测试重写（见 [DES-005](DES-005-paper-account-transaction-store.md) §13） | 已实现；账户项不再是 S4 前置 |
| S4 API 多 worker（**已解锁 2026-11-19**） | 账户 PG 事务层 + 四后端默认 redis（cache/SSE/coordination/mining）+ `coordination.once()` 启动钩子单飞 + Dockerfile `API_WORKERS`（默认 2）；成交重算移入 account 行锁内避免旧快照覆盖。服务对象仍挂所有 worker 的 `app.state`，非 leader 为「未启动」状态 | `test_coordination_once` + `test_paper_pg::test_concurrent_fills_cash_not_overwritten`（真实多进程）+ `test_paper_multiworker`（两 API 实例）全绿；全量回归通过；`deployment.md` 单 worker 约束已解除。已登记取舍：RC-06 各 worker 本地刷新、memory 文件版 mining 保留 flock |
| S5 跨主机评估（可选） | 若需要跨主机，锁换 Redlock/etcd，共享 `data/` | 独立立项，不在本规划实施范围 |

排期建议：S0→S1→S2→S3→S4 顺序推进，S5 仅在真实跨主机需求出现时启动。每阶段独立回归，不跨阶段累积变更。

## 5. mining 状态并发方案决策树（DES-003 D-01 的上层依据）

DES-003 §8 D-01 问："`transition_status` 用乐观并发还是单写者？"在分布式目标下，这个问题要重新框定：**flock 是本机锁，跨主机/多进程分布式场景下不可用**，所以候选只剩 Redis 系方案。

### 5.1 候选方案

| 方案 | 本质 | 脑裂/双写 | 复杂度 | 分布式适配 |
| --- | --- | --- | --- | --- |
| A 单写者 + flock | 本机文件锁 | 无（单机内） | 最低 | **不适用分布式目标** |
| B Redis 锁 + 状态留 CK + fencing | 用锁保护另一套存储的状态 | fencing 在 CK 无 CAS 落不了地 | 最高 | 名义可行，落地打折 |
| C Redis Lua 状态机 + CK 审计 | 状态权威在 Redis，Lua 原子串行化 | 无（Lua 单线程原子） | 中 | **最干净** |
| D etcd/ZK 租约锁 | CP 共识 + revision fencing | 无 | 最高（新组件） | 正确性天花板 |

### 5.2 分布式锁单独比较（客观结论）

若只准选"分布式锁"：
- 单实例 Redis 锁（`SET NX PX` + Lua 释放 + 续约）是已有 Redis 下的现实上限；
- Redlock 需 5 独立节点，项目当前单 Redis 容器，不匹配；
- Redis 锁 + fencing 需要资源端 CAS，CK 不支持，落不了地；
- etcd 正确性最高但需全新基础设施。

### 5.3 已批准决策（2026-11-19）

- **D-01 选方案 C（Redis Lua 状态机 + CK 审计）**：把并发问题消解在 Redis 单线程内，没有跨存储事务，是三个 Redis 方案里最干净的。
- **前置条件**：Redis 开 AOF（§2.3）；状态丢失后从 CK 审计重建；重写 mining 相关测试的存储假设。
- ~~**不引入 MySQL**（§2.4）：项目无 OLTP 负载，账户是事件溯源、回测是 append-only 历史，没有事务需求能摊薄引入成本。~~ → **已修订（2026-11-19）**：S3 发现账户写入组是真实 OLTP 事务，引入 PostgreSQL 作账户事务层（DES-005）；本 D-01 方案 C 结论不受影响。
- **B 仅作备选**：仅在"状态必须强持久化在 CK、不能接受 Redis 作权威"时退到 B，并接受 fencing 复杂度。

### 5.4 Redis 锁的可观测性约束（若最终选 B 或分布式锁）

Redis 锁的 value 必须携带 holder 诊断信息：`{token, schema_version, pid, started_at, role, host, process_started_at}`（沿用 `mining_process_lock.py:104-114` 的 `holder_payload` 字段），配合 psutil 核验进程存活 / 创建时间。注意 lsof 的"OS 铁证"无法在 Redis 锁上复刻——value 是写入方自报，接管逻辑需多一层不确定性。这是选方案 C 而非 B 的又一理由。

## 6. 风险与开放问题

| 风险 | 描述 | 缓解 |
| --- | --- | --- |
| 渐进式长期停留在中间态 | 部分组件分布式、部分单机，整体不一致 | 按 §4 顺序推进，每阶段有明确解锁条件与验收 |
| Redis 升格权威后的单点 | Redis 挂则协调态不可用 | AOF + 从 CK 重建流程；单节点部署下风险可控 |
| API 多 worker 解锁过晚 | 进程内状态迟迟迁不完 | §2.2 清单化跟踪，每迁一项更新 ISS-004 |
| mining 存储假设翻转 | 方案 C 需重写大量测试 | 在 S2 集中处理，不与其他改动混批 |
| 跨主机需求提前出现 | S5 未规划实施 | 独立立项，本文只保留接口预留原则 |

## 7. 验收标准

1. §2 四条决策经评审确认或明确修改；RC-02 AOF 决策修订记录归档。
2. §3 每个组件的权威存储、一致性级别明确，无"无主"状态。
3. §4 各阶段按解锁条件推进，S1~S4 每阶段有回归证据。
4. DES-003 D-01 依据本文 §5 拍板，结论回填 DES-003 与 ISS-004。
5. API 多 worker 解锁前，§2.2 清单中"未迁"项全部迁出或有明确的保留决策（如 RC-06）。
