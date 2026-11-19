# 剩余未处理问题：mining CK 化、状态化缓存与 W5 验收

> 文档类型：问题 | 编号：ISS-004 | 状态：待评审 | 文档版本：v1.1 | 最后更新：2026-11-19
> 负责人：待分配 | 关联：ISS-002、ISS-003、REQ-002、DEP-002、DES-001、DES-002、DES-003、DES-004
> 基线来源：2026-11-19 的代码、文档与实跑结果

用途：把 ISS-003 的 W0~W4 与 W5 前置闭环之后仍未处理的问题集中记录，明确范围、依赖、排期和验收标准。本文不重复 ISS-002 的 CH-01~CH-13 原始清单，也不重复 ISS-003 已闭环的 RC-01~RC-15，只对其中“明确未实现 / 未验收 / 待决策”的尾巴做二次归集，便于后续逐项认领与复核。

## 1. 背景与当前基线

### 1.1 已完成（本文不再展开）

- DES-002 P0~P4 主体完成：显式 `storage` 下 daily/enriched 在线读写、修补、校验、universe、live merge/flush、状态统计与矩阵 generation 均已 CK 化。
- ISS-003 W0~W4 闭环：Redis `CacheBackend` 落地；SSE EventBus 可切 Redis Pub/Sub；任务取消 / 限流 / 状态可跨实例共享；`pipeline_recompute` 与指数 enriched 在线写入收口 CK；generation marker 在线依赖清理完成；派生缓存归属表落盘（regime CK、mainline CK、sector / factors 本地热缓存、mining 作业产物）。
- W4 唯一红灯修复：`ext_factors._frame_cache` 内容寻址键缺少 owner 校验导致跨 `data_dir` 误命中，命中时增加 `cached[0] == root_id` 后红灯清零；`RedisLimiterCounter` 的 WATCH 冲突重试加上限 `_MAX_CAS_RETRIES=16`。
- 全量基线（2026-11-19 S4 解锁后）：四后端 conftest 锁 memory 下 `4898 passed / 13 skipped / 1 xfailed / 0 failed`（历史 4848 基线已被后续批次递增）。
- W5 前置：RC-14 应用层 Redis 冒烟 `4/4 passed`、RC-15 双模式性能标定报告归档，两项脚本均过 pre-commit 全门禁。
- RC-05 已实现：strategy 缓存 Redis Hash + Lua 原子合并（`strategy_cache_redis.py`），定向 15 passed、pre-commit 全绿（全量回归按用户要求推迟）。RC-06 已闭环：DataStatusCache 保留本地的决策写入 `deployment.md`。
- 规划产出：DES-003 mining CK 表契约设计稿（v1.2，待评审）；DES-004 分布式架构规划稿（v1.0，待评审，含四条边界决策与 mining D-01 决策树）。

### 1.2 本文范围

| 类别 | 范围 | 来源 |
| --- | --- | --- |
| 分布式架构规划 | 四条边界决策、组件权威归属、迁移顺序、mining 并发决策树 | DES-004（规划稿，待评审） |
| mining CK 化 | run 状态、事件、摘要、结果 artifact、快照指纹、周度 claim 从本地文件迁 CK | ISS-003 §7.5 归属结论“mining → 作业产物，CK 化转入后续 ISSUE” |
| 状态化缓存 | `strategy_cache.json` 状态化（已实现）；`DataStatusCache` 本地化（已登记） | RC-05、RC-06 |
| W5 人工 / 环境验收 | 三天观察、回滚演练、S3/S4 与矩阵签字、页面双栈对照、满量性能、前端构建 | ISS-002 CH-04~CH-09 |
| 文档与工程债 | 状态同步、工作区提交、flaky 测试、redis 门禁口径 | CH-10 与 2026-11-19 工作区实况 |

## 2. 未处理问题清单

优先级按 CONTRIBUTING §11 的阻断口径：`P1` 核心流程 / 正确性，`P2` 明确功能或兼容性问题，`P3` 维护性。`阻断发布`列中“是”表示该项未闭环前不能把 REQ-002 / v1.0.1 判定为完成。

### 2.1 mining CK 化（MN-01~MN-05）

| 编号 | 优先级 | 阻断发布 | 问题 | 现状 / 证据 | 下一步 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| MN-01 | P1 | 是 | mining run 状态、事件与摘要 Redis 化（状态机部分已实现） | 2026-11-19 方案 C 已落地：`RedisMiningRunStore`（Lua 原子状态机 + 事件自增/256 截断 + 摘要 + claim）经 `get_run_store` 工厂按 `mining_store_backend=redis` 切换；`mining_audit` 写 CK 审计 + 启动重建；默认 memory 文件版兼容 | 已验证：状态机 7 + 审计 3 + 回归 32 passed、pre-commit 全绿；flock 出局 / DIST-03 取消协调 / MN-05 历史兼容验证已完成；**待**：MN-02 产物 | DES-004 §5 已拍板方案 C；DES-003 表设计（运行时改审计表） |
| MN-02 | P1 | 是 | 四个结果 Parquet artifact、候选库仍本地落盘 | `folds.parquet`、`candidates.parquet`、correlation 等产物与 `mining_candidates.py` 候选库走本地文件 | 判定大对象归属：CK 存元数据 + 内容寻址产物指针，或专用对象存储；明确保留期与清理策略 | MN-01；数据指纹口径 |
| MN-03 | P1 | 是 | 周度 claim 与数据指纹幂等状态在本地 | 同一 ISO 周确定性 claim 与 `data_fingerprint` 依赖本地运行状态，多实例可能重复 claim 或用旧口径复用结果 | 把 claim 与指纹映射到 CK 唯一键；升级算法版本时按指纹失效旧结果 | MN-01；`mining.md` §85~§87 |
| MN-04 | P1 | 否 | 挖掘锁与任务协调仍依赖进程 / 文件锁（方案 C 路径已出局） | 2026-11-19：`RedisMiningRunStore` 下 flock 已从启动门控出局（`main.lifespan` 仅 memory 模式抢锁；`ensure_mining_lock` 仅 memory 模式触锁）；取消信号经 `_CoordinatedCancelEvent.is_set()` 叠加 `coordination.cancel_flags` 跨实例可见；`_threads` / `_cancel_events` 降级为本地执行句柄，不再是权威（权威为共享 store 状态 + 协调层标志） | 定向 5 passed（跨实例取消传播 / 无本地线程仍写标志 / 远程取消收尾 / redis 模式绕过 flock）；**待**：memory 文件版仍保留 flock（离线兼容，已登记）；MN-03 周度 claim 的跨进程原子性复核 | RC-07/RC-08；MN-01；DES-004 §2.1/§5 |
| MN-05 | P2 | 是 | 历史 run 兼容读取已验证，残留缺口登记待处理 | 2026-11-19：`MiningRunStore` 读取时归一化既有契约经 `test_mining_jobs.py::test_historical_manifest_defaults_and_startup_recovery` + 新增 `test_mining_historical_compat.py` 5 用例验证（老 manifest 补默认字段/签名重算/artifact 过滤、老 events+summary 读取、缺文件降级、API `_project_run` 投影不崩）；相关回归 32 passed、ruff 全绿。前端 SSE + 2s 轮询接回、不直读 manifest.artifacts。**已知限制（本批只记录不修）**：① `artifacts` 缺失/非法会被归一化为 `{}`，`/result` 要求四件齐全 → 老 run 结果读取 500；② 目录名不匹配 `_RUN_ID_PATTERN` 的老 run 在 list/find/recover 中不可见、直接 get 抛错；③ events 缺 id/非递增/JSON 损坏、summary 非 dict 均 fail-closed；④ manifest 内 run_id 被目录名覆盖；⑤ 时间戳缺失时 `created_at/updated_at=None`，与前端 `string` 声明不符；⑥ Redis 后端不读文件系统历史 run | 已验证的读取归一化路径闭环；**待**（单列批次）：老 run `/result` 与 promote/publish 兼容策略、非法目录名迁移/只读方案、缺字段时间戳契约对齐、Redis 切换时文件历史迁移；不与 DIST-03 混批 | MN-01~MN-04 |

### 2.2 状态化缓存与设计落地（RC-05、RC-06）

| 编号 | 优先级 | 阻断发布 | 问题 | 现状 / 证据 | 下一步 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| RC-05 | P1 | 否 | `strategy_cache.json` 多实例状态化（已实现，待全量回归） | 新增 `strategy_cache_redis.py`：Hash + Lua 原子合并（同日并集 / 跨日重置 / version 递增），镜像以 version 判失效；仅 `CACHE_BACKEND=redis` 启用，文件逻辑零改动 | 定向 15 passed、pre-commit 全绿；全量回归按用户要求推迟到后续批次 | RC-01 已完成；状态语义按 ISS-003 §7.2 |
| RC-06 | P2 | 否 | `DataStatusCache` 多实例影响（已登记，无需代码） | ISS-003 §7.2 决策保留进程内单飞；已在 `deployment.md`「可选 Redis 缓存层」登记多实例下各自刷新 / 重复统计的取舍 | 闭环；仅当多实例成为正式形态且需消除重复开销时，再按原租约方案实现全局单飞 | — |

### 2.3 W5 人工 / 环境验收（承接 ISS-002 CH-04~CH-09）

| 编号 | 优先级 | 阻断发布 | 问题 | 现状 / 证据 | 下一步 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| CH-04 | 中 | 是 | 三天观察未闭环 | `data/ch_acceptance_observe.jsonl` 日志止于 2026-11-18，要求 11-20 复查 | 检查跨日连续性与 uptime 回落，补观察结论 | W4 冻结版本 |
| CH-05 | 中 | 是 | 回滚演练未执行 | DEP-002 记录无 `master` 基线、`git checkout` 有风险，回滚耗时未实测 | 由项目方确认回滚基线，演练并记录重新同步耗时 | 项目方确认基线 |
| CH-06 | 中 | 是 | S3/S4 未完整自动化，数据集矩阵未全量签字 | DEP-002 标为“现有测试背书”，矩阵仅 S2 的 3/4 有明确对照 | 补复权四方对照和 kill→重启→failed 场景，按 DEP-001 §5 补齐 13 类矩阵 | CK 测试环境 |
| CH-07 | 中 | 是 | 页面级双栈对照未执行 | DEP-003 执行表为空，`执行用例数：__ / 12` | 按 DEP-003 执行 12 个用例，归档差异与截图 | CH-09 构建环境 |
| CH-08 | 中 | 是 | 满量性能标定未完成 | 分钟仅 370 行、财务表 0 行；RC-15 报告为本机非满量数据 | 积累真实数据后按 DEP-001 §9.1 同口径重测并回填阈值表 | 部署环境真实数据 |
| CH-09 | 中 | 否 | 前端构建未验证 | 本机无 `pnpm`，`pnpm build` 未执行 | 在有 pnpm 的环境执行并记录结果 | 构建环境 |

### 2.4 文档与工程债

| 编号 | 优先级 | 阻断发布 | 问题 | 现状 / 证据 | 下一步 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| DOC-01 | P2 | 是 | 文档状态与真实状态不一致（CH-10） | REQ-002 / DES-001 / DES-002 / DES-003 / DES-005 / ISS-002 / ISS-003 状态与基线已于 2026-11-19 回填（当前 `4898 passed / 0 failed`）；历史失败数为阶段实测，保留不改 | 剩余 DEP-002 结论与 CH-04~CH-09 验收结果待补；未闭环项汇总见 ISS-005 | W4/W5 结论冻结 |
| ENG-01 | P2 | 是 | 工作区改动尚未提交 | 2026-11-19 `git status` 显示大量已改 / 未跟踪文件，分支 `feature/custom_tag_v0.3.37` | 按 CONTRIBUTING §10/§13 拆分 PR，DCO 签署提交；提交前跑 `git diff --check` 与质量门禁 | 本文评审 |
| ENG-02 | P3 | 否 | `test_upgrade_guard.py::test_live_backup_follows_reverts` 偶发失败 | 单跑通过，连续 3 次整文件通过；全量中偶发 1 次失败，判断为 mtime 精度 / 文件系统时序 flaky，与本次改动无关 | 观察复现频率；若再现，改用显式时序同步或时间冻结断言 | — |
| ENG-03 | P3 | 否 | redis 全量测试不作为常规门禁，覆盖口径需登记 | redis 全量极慢且部分测试硬编码内存协调语义；仅核心契约 `test_redis_cache.py`、`test_cache_backend_integration.py`、`test_events_redis.py`、`test_coordination_redis.py` 共 `15 passed` | 在 DEP-002 / 发布记录写明门禁口径与剩余风险，避免被误读为 redis 全量已验证 | RC-14/RC-15 已归档 |
| ENG-04 | P3 | 否 | 历史代码质量门禁欠账（HEAD 既有，非本批改动引入） | `git show HEAD` 证实：`mining_jobs.py`(注释率 4.9%)、`mining_manager.py`(2.3%) 低于 5% 阈值；`mining_runtime.py`/`mining_manager.py`/`mining_jobs.py` 存在既有 mypy 错误；`mining_manager.py:197` 的 `except Exception as exc` 触发 ruff `BLE001`。按 CONTRIBUTING §1.3 未在功能 PR 中重构无关代码 | 单列工程债 PR 集中处理，不与分布式改造混批；处理时仍需遵守"不新增豁免、不改阈值"（CONVENTIONS.md） | — |
| ENG-05 | P3 | 否 | 本机测试环境配置与门禁口径不一致（测试侧已修复，DEP-002 待记录） | 根因是 `.env:11 CACHE_BACKEND=redis` 与内存期望冲突，**已在 `tests/conftest.py` 强制四后端 memory 修复**（全量 4898 / 0 failed）；`backend/.venv` 为 root 空目录、`.pytest_cache`/`.ruff_cache` 权限拒绝经 `.venv-pytest` 绕开 | 在 DEP-002 记录"定向用 `.venv-pytest` + 四后端 memory（conftest 强制）、redis 路径由 test_*_redis.py 单独覆盖"的实测口径 | ENG-03 |

### 2.5 分布式架构规划（DIST-01~DIST-04）

> 目标形态见 [DES-004](../design/DES-004-distributed-architecture-roadmap.md)。本组是规划落地项，按 S0~S4 阶段推进，不一次性实施。

| 编号 | 优先级 | 阻断发布 | 问题 | 现状 / 证据 | 下一步 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| DIST-01 | P1 | 否 | 分布式架构四条边界决策（已批准，含一处同日修订） | DES-004 §2 四条 + §5 D-01 已于 2026-11-19 拍板：同主机多进程起步、API 单 worker 起步、Redis 升格权威（开 AOF）、一致性分级、D-01 选方案 C；原"否决 MySQL/关系库"同日修订为"账户事务层引入 PG"（DES-005） | 已批准；RC-02「不持久化」决策修订随 DIST-02 实施归档；按 DES-004 §4 的 S1~S4 推进 | DIST-06 |
| DIST-02 | P1 | 否 | Redis 升格协调态权威的前置设施（已实现） | 2026-11-19：Redis AOF 已启用（新容器默认 + `aof on/off` 命令）；三张 CK 协调审计表 + `coordination_audit.py` 重建流程 + 启动钩子已落地；RC-02 决策已修订 | 闭环；定向 6 passed + 回归 80 passed、pre-commit 全绿；全量回归待后续批次 | DIST-01 已批准 |
| DIST-03 | P1 | 否 | API 进程内状态迁出（mining 项已迁出，多 worker 已解锁） | 2026-11-19：mining 取消事件经 `_CoordinatedCancelEvent.is_set()` 叠加 `coordination.cancel_flags`，可在不同 worker 间传播；`cancel()` 即使本 worker 无线程也写协调标志；`_threads`/`_cancel_events` 仅为本地执行句柄，权威为共享 store 状态。`deployment.md` 单 worker 约束已随 DIST-05 解除 | 已验证 5 passed；DES-004 §2.2 清单剩余项已迁出，`--workers N` 由 `API_WORKERS` 启用（见 DIST-05） | DIST-02；MN-04；DIST-05 已完成 |
| DIST-04 | P2 | 否 | mining 状态并发方案 D-01（方案 C 已落地，flock 在方案 C 路径出局） | 2026-11-19：Redis Lua 状态机 + CK 审计；`main.lifespan` 在 `mining_store_backend=redis` 时不抢 flock，`ensure_mining_lock` 直接放行，调度 claim 仍走共享 store 防重 | 状态机并发 7 passed + 审计 3 passed + flock 出局 5 passed；memory 文件版保留 flock 属已登记兼容取舍 | DIST-02 已完成；DES-003 D-01 |
| DIST-05 | P1 | 否 | S4 API 多 worker 解锁（**已解除 2026-11-19**） | 账户事务层（DIST-06）已解除账户根因；本批补齐其余前置：① **启动钩子单飞**——`coordination.once()`（Redis SET NX / memory 进程内去重）包住 coordination/mining 重建、mining/guardian 恢复、调度器、扩展拉取、财务调度、矩阵 generation；服务对象仍挂所有 worker `app.state`；② **四后端默认 redis**——cache/event_bus/coordination/mining_store，compose 与 `.env.example` 同步；③ **Dockerfile `API_WORKERS`（默认 2）**，`deployment.md` 单 worker 约束已解除；④ **多 worker 门禁**已入 CONTRIBUTING 验证矩阵。另修复了事务外重放导致的跨 worker 旧快照覆盖（成交 cash 分叉），重算已移入 account 行锁内 | 已验证：`test_coordination_once`（含 redis SET NX 跨实例）、真实多进程成交流水一致性、两 API 实例并发写、全量回归全绿；已登记取舍：RC-06 各 worker 本地刷新、memory 文件版 mining 保留 flock | DIST-06 已完成；DES-004 §4 S4；CONTRIBUTING 验证矩阵 |
| DIST-06 | P1 | 否 | S3 模拟盘账户多实例（决策修订：引入 PostgreSQL 事务层，原"保留单写者"已推翻；**同日已实现**） | 2026-11-19 评估：事实源是 append-only `fills.jsonl`，但读路径直接信任物化 `account.json`/`positions.json`（`paper_positions.py:45`、`paper.py:258`），非每次重放；写入组（追加 fills + 重写 order/account/positions，`paper_fill.py:96-109`）连进程内都不整体原子，仅靠 `PAPER_LOCK`（`paper.py:152`）协调；四条写路径（API / 行情轮询 `quote_service.py:1609` / 盘后结算 `daily_pipeline_run.py:238` / 自定义执行 `custom/paper_execution/service.py`）均无跨进程锁/版本/CAS。**与 RC-06 的本质区别**：`DataStatusCache` 结果一致可软取舍；账户是资金状态，多 worker 并发写会账目分叉（P0）。**同日评审决策改为引入 PG**（推翻保留单写者），并**同日落地**：PG 事务 + account 行锁保证写入组原子，6 表直接建库（无历史数据、无迁移负担），CK 审计非强制，NAV/auto_rules 一并建表，前端零改动，库表遵循 `tsp-rule/pgsql.mdc` | **已实现**（DES-005 §13）：6 表 schema + 事务写入组 + 5 个 paper 模块全量迁移 + 12 个测试重写，paper 定向全绿；账户不再是 S4 前置。单 worker 硬约束的其余根因见 DIST-05 | DIST-05；DES-005；DES-004 §4 S3、§2.4 已修订 |

## 3. 分组处理方案与验收标准

### 3.1 mining CK 化

- 先定契约再迁移：MN-01 的契约见 [DES-003](../design/DES-003-mining-clickhouse-contract.md)（状态：决策已拍板并部分落地，非待评审）。**D-01 并发策略已于 2026-11-19 拍板方案 C**（Redis Lua 原子状态机 + CK 审计，见 DIST-01/DIST-04），不再是待决项；DES-003 §8 记录该决策过程。artifact/claim 的 CK 归属（MN-02/MN-03）仍待处理。
- 区分“权威状态”与“大对象产物”：状态、事件、claim、指纹必须有 CK 权威副本；结果 Parquet 按内容指纹寻址，避免把大矩阵塞进 CK 行。
- 多实例边界：**单机阶段** OS 文件锁是“谁持有挖掘权”的唯一事实，协调层只做取消与状态可见；**分布式目标下 flock 出局**，按 DES-004 §5 拍板的新方案重构状态机与并发控制，降级 / 恢复 / 强制接管的语义不得丢失（`mining.md` §109~§117）。
- 验收（口径修订 2026-11-19）：run 状态/事件/摘要**权威在 Redis**（`RedisMiningRunStore`），CK 作审计与历史查询——"新 run 在 CK 可查询"应理解为审计副本可查；杀进程重启后可重连；同一 ISO 周跨实例只 claim 一次（MN-03 待落地）；历史本地 run 仍可读；挖掘相关回归全绿。status/事件/摘要的 Redis 权威路径已验证（MN-01），claim/artifact 待处理。

### 3.2 状态化缓存

- RC-05 已实现（2026-11-19）：新增 `backend/app/services/strategy_cache_redis.py`，用 Hash 按 `data_dir` 摘要分键、Lua 在 Redis 端原子完成“读旧值 → 同日并集合并 / 跨日重置 → 写新值 → version 递增”；进程内镜像以 version 判失效，命中时只做一次 HGET。`strategy_cache.py` 仅在 `CACHE_BACKEND=redis` 且装配 `RedisCacheBackend` 时分发到该 store，文件后端逻辑零改动。`RedisCacheBackend` 新增 `client()` / `entry_prefix()` 暴露底层句柄。
  - 定向验证：`test_strategy_cache_redis.py` / `test_strategy_cache.py` / `test_strategy_cache_memory.py` 共 `15 passed`（Redis 用例连真实实例，不可达时跳过）；pre-commit 全门禁通过。
  - 待补：全量回归（用户要求推迟到后续批次）；真实数据量下 Lua 合并耗时与内存放大压测（此前提出的大 key 风险，按 strategy 拆 field 已规避整包搬运，仍需实测确认）。
- RC-06 已闭环（2026-11-19）：不实现代码，决策与多实例影响已写入 `docs/base_v1.0/deployment.md`「可选 Redis 缓存层」章节（各自刷新 / 重复统计是已登记取舍、结果一致、不需重启清缓存）。若部署形态变化，重新评估而不是默认继承当前结论。

### 3.3 W5 人工验收

- 以 W4 冻结版本为唯一基线；观察期跨自然日不足则顺延，只记录事实，不提前改判 DEP-001 的“有条件通过”。
- CH-04~CH-08 逐项留证（日志、演练记录、对照报告、性能报告）；CH-09 在可用环境补构建；完成后再按 DEP-001 §11 六条 gating 签字。

### 3.4 文档与工程

- 状态同步放在最后做，以实测数据为准；先同步本文，再回填 ISS-002 / DEP-002 / REQ-002 / DES-001。
- 提交按主题拆分，不把 mining CK 化、W5 验收记录与本文之外的代码改动混在一个 PR。
- ENG-02/ENG-03 不阻断发布，但需在验收记录中保留“未验证范围”说明。

### 3.5 分布式架构规划

- DES-004 §2 四条边界决策（DIST-01）已于 2026-11-19 拍板，作为后续分布式改造的上层依据；S1/S2 已据此推进。
- DIST-02（Redis AOF + CK 重建）是 Redis 升格权威的前置设施，已在 mining 状态机迁移前完成。
- DIST-03（进程内状态迁出）与 DIST-04（D-01 拍板）已就 mining 项落地；DES-004 §2.2 清单已全部迁出，API 多 worker 于 2026-11-19 解锁（DIST-05），`API_WORKERS` 默认 2。
- 多 worker 下 `recover_interrupted` 误标其他 worker ACTIVE run 的风险已由 `coordination.once()` 单飞消除：mining/guardian 恢复只由一个 worker 执行。剩余复选用例（四后端 redis 下任务/限流/缓存/SSE）已入 CONTRIBUTING 验证矩阵。
- ~~不引入 MySQL~~ 已于 2026-11-19 修订：S3 发现账户写入组是真实 OLTP 事务，引入 PostgreSQL 作账户事务层（DES-005、DES-004 §2.4 已修订）；回测仍是 append-only 历史，不受影响。

## 4. 建议排期

| 阶段 | 目标 | 包含事项 | 门禁 |
| --- | --- | --- | --- |
| P0 决策与冻结 | 认领问题、冻结 W4 基线 | MN-01~MN-05 契约设计；DES-004 四条决策评审；DOC-01 同步；ENG-01 提交拆分 | 本文与 DES-004 评审通过；基线 commit 明确 |
| P1 状态化缓存 | 补齐设计已定项 | RC-05 实现 + 并发测试（已完成）；RC-06 决策登记（已完成）；补跑全量回归 | 定向 15 passed、pre-commit 全绿；全量回归待跑 |
| P2 人工验收 | 完成 W5 | CH-04~CH-09 | 六条 gating 逐项签字或明确不顺延原因 |
| P3 分布式前置 | Redis 升格权威的基础设施 | DIST-01 决策确认；DIST-02 AOF + CK 重建流程；DIST-04 D-01 拍板 | AOF 生效、重建流程测试通过、D-01 回填 |
| P4 mining 分布式化 | run 状态 / 事件 / 摘要按 D-01 迁移，再迁 artifact、claim、任务表 | MN-01→MN-04→MN-02→MN-05；DIST-03 进程内状态迁出 | 老 run 兼容回归；跨进程任务可见 / 取消可用；全绿 |
| P5 API 多 worker（已完成 2026-11-19） | 解锁横向扩展 | DES-004 §2.2 清单全部迁出；`API_WORKERS` 默认 2；启动钩子单飞 | 多 worker 定向测试 + 全量 4898 passed / 0 failed，见 DIST-05 |
| P6 收口 | 发布判断 | 全部阻断项复核 | 仅剩建议项；发布结论有证据 |

排期为建议窗口，不替代项目方对外部依赖（Redis 容器、pnpm 环境、回滚基线、真实数据量）的确认。

## 5. 退出标准

1. MN-01~MN-05 有明确结论：已迁移、明确保留本地并说明影响、或转入新 ISSUE；不允许“无主” mining 状态留在生产路径。
2. RC-05 实现并通过并发测试；RC-06 决策已登记。
3. CH-04~CH-09 逐项有验收证据或明确的不可执行原因。
4. DOC-01 同步完成，REQ-002 / DEP-001 / DEP-002 / DES-001 与实测状态一致。
5. ENG-01 完成 DCO 提交，`git diff --check` 与质量门禁通过。
6. ENG-02/ENG-03 在验收记录中标注为已知限制。
7. DES-004 §2 四条边界决策已评审确认；RC-02 AOF 决策修订归档；DIST-02 重建流程测试通过。
8. DES-003 D-01 依据 DES-004 §5 拍板并回填（方案 C，2026-11-19）；DES-004 §2.2 清单“未迁”项已全部迁出，API 多 worker 已解锁（DIST-05），`API_WORKERS` 默认 2。

## 6. 风险与阻塞

| 风险 | 描述 | 缓解 |
| --- | --- | --- |
| mining CK 化范围过大 | run 状态、artifact、claim、锁涉及多个模块，一次性替换易破坏重连 / 恢复语义 | 按 MN-01→MN-04→MN-02→MN-05 顺序分批，每批有兼容回归 |
| 多实例需求未定 | `DataStatusCache` 与挖掘锁是否要求跨实例尚未产品确认 | 不实现未确认的大改；按 RC-06 先登记现状 |
| W5 依赖外部条件 | 三天观察、回滚基线、双栈对照、真实数据量均需项目方 / 部署环境 | 以事实记录为主，不足则顺延，不改判验收结论 |
| 文档与代码不同步 | 历史 ISSUE 已出现“状态列与实测不一致” | 状态同步统一放到最后，以本报告实测数据回填 |
| 未提交改动丢失 | 大量工作区改动承载已验证结论 | 拆分 PR 后用 DCO 提交，提交前后复核 `git status` / `git diff` |
| 渐进式长期停留中间态 | 部分组件分布式、部分单机，整体不一致 | 按 DES-004 §4 的 S0~S4 顺序推进，每阶段有解锁条件与验收 |
| Redis 升格权威后单点 | Redis 挂则协调态不可用 | AOF + DES-004 §2.3 的 CK 重建流程；单节点下风险可控 |
| mining 存储假设翻转 | 方案 C（若拍板）需重写 mining 相关测试 | 在 P4 集中处理，不与其他改动混批 |
| 混合后端下 mining 取消轮询对 Redis 的隐式依赖 | DIST-03 的 `_CoordinatedCancelEvent.is_set()` 在 worker 父进程 100ms 轮询里调用 `coordination.cancel_flags.is_set()`。当 `COORDINATION_BACKEND=redis` 而 `MINING_STORE_BACKEND=memory` 时，文件版 mining 本应离线自洽，但取消轮询会打 Redis；`RedisCancelFlags.is_set` 在 Redis 不可用时抛 `CoordinationUnavailableError`，可能中断 `worker` pump / `_mark_running` / `_finish_success` 并把正常 run 误标 failed。方案 C 正常部署（两者皆 redis）不受额外影响——store 本身已依赖 Redis | 后续让复合事件仅在 `mining_store_backend=redis` 时查协调层，或提供 fail-open 本地回退；本批按"先记录、后处理"不改动 |
