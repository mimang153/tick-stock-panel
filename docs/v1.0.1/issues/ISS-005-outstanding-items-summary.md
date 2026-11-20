# v1.0.1 未闭环问题与需求汇总

> 文档类型：问题汇总 | 编号：ISS-005 | 状态：活跃清单 | 文档版本：v1.2 | 最后更新：2026-11-20
> 负责人：待分配 | 来源：ISS-001~004、DES-001~005、REQ-001~002 的盘点结果
> 用途：**单一入口**登记三个目录里仍未处理/待验收/状态待决的项；已闭环项不在此重复登记。处理完成后把对应行状态改为已完成并注明去处。

## 0. 盘点口径

- 盘点范围：`docs/v1.0.1/issues/`（ISS-001~004）、`docs/v1.0.1/design/`（DES-001~005）、`docs/v1.0.1/requirements/`（REQ-001~002）共 11 份文档。
- 唯一编号条目约 88 项 + 无编号风险/待办约 15 项；其中已完成约 35 项，其余为本清单登记的未闭环项。
- **历史实测数字**（4820/4848/4898 passed、163→0 failed）是各阶段过程证据，保留在原文，不作为当前状态。当前全量基线为 `4923 passed / 13 skipped / 1 xfailed / 0 failed`（2026-11-20 本轮 DIST-05~14 + DIST-13 fencing 接入后重跑，四后端 conftest 锁 memory）；`pre-commit run --all-files` 仍因存量 mypy 错误与本机缺 `pnpm` 未通过（改动文件 ruff/mypy/门禁通过）。
- **2026-11-20 代码核对 correction（已补齐）**：§7.1 中 DES-002 §9 与 DES-003 D-02/D-05 原记"均已落地"，逐项读码核对发现 30 天快照 TTL、repair CAS、私有 staging owner、D-02 审计 90 天 TTL、D-05 多 worker 强制 redis 五项缺失。本轮已全部补齐并定向验证；§4/§7.1.1/§7.1.2 状态同步为"已落地"。
- **2026-11-20 二次深度核对（分布式端到端，已补齐）**：核对发现 §7.1.1/§7.1.2 三项"已落地"实为部分落地、另有四类跨实例状态本地化违反与三类故障恢复缺口，登记 DIST-05~14。本轮已补齐：DIST-05/06/07/08/09/10/11/12/13 闭环；DIST-14（无接管调度器）经用户决策本轮不做、登记残余风险，需要时独立立项。§7.1.1/§7.1.2 状态同步更正。
- 分类：`A 环境/人工验收`、`B 可做代码与产物`、`C 工程与质量债`、`D 设计待定/产品决策`、`E 文档滞后（已修正/待修正）`。

## 1. A 类：环境 / 人工验收（阻断发布，需外部条件）

| 编号 | 来源 | 事项 | 为什么未闭环 | 前置条件 |
|---|---|---|---|---|
| CH-04（暂缓） | ISS-002/004 | 三天跨日观察未闭环 | 2026-11-20 用户决定本轮暂不做；日志仍为 351 行，末条 `11-18T01:35:15+08:00` | 本轮不纳入发布判定；如恢复观察，再按 DEP-002 门槛复查 |
| CH-05（不纳入范围） | ISS-002/004 | 回滚演练未执行 | 2026-11-20 用户决定本轮暂不考虑回滚问题；无基线，禁止擅自执行 | 本轮不作为发布阻断；后续如需回滚能力再另行立项 |
| CH-06（已闭环） | ISS-002/004 | S3/S4 未完整自动化、数据集矩阵未全量签字 | 已补齐 S3 `adjust` 双响应/`/api/adj-factor` 与 S4 进程级 kill→重启→failed→重跑自动化；S3/S4 定向 32 passed，13 类矩阵近似集合 + 场景 85 passed | 无；记录见 DEP-002 §追加 |
| CH-07 | ISS-002/004 | 页面级双栈对照未执行 | DEP-003 执行表为空，`执行用例数 __ / 12` | 部署页面可用（CH-09 已闭环） |
| CH-08 | ISS-002/004 | 满量性能标定未完成 | 分钟仅 370 行、财务 0 行，未达 DEP-001 §9.1 规模 | 部署环境真实数据 |
| CH-09（已闭环） | ISS-002/004 | 前端构建未验证 | 2026-11-20 用 corepack `pnpm@9.10.0` 在可写副本执行 `tsc -b && vite build` 成功：2923 modules transformed，仅 chunk-size/browserslist 警告 | 无；原 root 属主 `frontend/node_modules` 未改动 |

## 2. B 类：可做代码与产物（阻断发布，可排期）

| 编号 | 来源 | 事项 | 现状 |
|---|---|---|---|
| MN-02（主体已落地） | ISS-004 / DES-003 | mining 四个 Parquet artifact、候选库归属 | 已接入 `MINING_ARTIFACT_ROOT`、内容哈希、共享 URI、`s3/shared-root` 适配器与 `ck_mining_artifact` 元数据表；待全量回归与部署验收 |
| MN-03（主体已落地） | ISS-004 / DES-003 | 周度 claim 与数据指纹 | 已拆分数据/实现指纹，实现指纹不进签名；签名声明、产物注册 Redis 原子化，审计 outbox/barrier 已落地；待全量回归与部署验收 |
| MN-05（代码已闭环，待全量回归） | ISS-004 | 历史读取归一化的 6 类残留缺口 | 6 类均已实现：标准产物推断、`run_id` 别名、mtime 时间兜底、非法历史目录名只读兼容、events/summary 损坏降级、Redis 后端只读回退本地文件 run；写路径保持严格，待全量回归确认 |
| T4.2~T4.8 | REQ-002 §6 | B4 回测/挖掘、同步/写入、展示/监控、因子、API 层、选股器、标的池的散点收敛 | T4.8 标的池缓存已收敛（CacheBackend 权威、parquet 离线回退），其余主路径已切，DoD（grep 归零）未达成 |
| T5.1~T5.6 | REQ-002 §7 | B5 删 DuckDB/Parquet 依赖、清 publishing 标记、删遗留路径、测试迁移清零、文档、全量基线 | 在线读写主体完成，剩余本地路径与测试债务 |
| T6.1~T6.7 | REQ-002 §8 | B6 场景 S1~S9、性能标定、真实数据抽样、回滚演练、三天观察、验收记录、页面级双栈对照 | 前置冒烟已记录，正式验收未开始 |
| DIST-01（已闭环） | DES-002 §9 | 30 天快照 TTL + repair 状态 CAS | 已落地：`ck_repair_snapshot` DDL+v3 迁移加 30 天 TTL；`set_repair_status()` 增加按 `repair_id` 最新状态的状态机 CAS（终态不可覆写）；定向测试覆盖 |
| DIST-02（已闭环） | DES-002 §9 | 私有 staging + owner 清理 | 已落地：staging 用本次 `backup_id` 唯一目录 + `.owner` 标记，`finally` 校验 owner 才清理，防同名/重试误删；repair_daily/integrity 59 passed |
| DIST-03（已闭环） | DES-003 D-02 | 审计表 90 天 TTL | 已落地：`ck_mining_run_audit` DDL+v3 迁移加 90 天 TTL（CK 无独立 `ck_mining_events`，审计表即重建源）；schema 测试通过 |
| DIST-04（已闭环） | DES-003 D-05 | 多 worker 强制 redis 后端 | 已落地：新增 `API_WORKERS` 配置，validator 拒绝 `api_workers>1` + `mining_store_backend=memory`；测试进程锁 `API_WORKERS=1`；配置测试通过 |
| B4/B5 DoD（待重定义） | REQ-002 §6/§7 | grep 归零 / 裸 DataStore 清零 | 二次核对确认与架构原则冲突：剩余 parquet 调用多为 CK 优先回退、上游 dump、产物、暂存、取证、用户 SQL 引擎；本轮完成 ETF 因子 CK 优先、删 `screener_results` 残留。DoD 口径需走 REQ-002 §12 变更控制，见 REQ-002 B4/B5 收敛结论 |
| DIST-05（已闭环，后台接管除外） | DES-004 / §7.1.1 | generation 抢占原子化 + fencing 覆盖数据写入 | 已落地：`begin_generation_leased` 用 `coordination.lock(generation_lease:{asset})` 串行 SELECT→INSERT；CK enriched 写入改用 leased generation + 唯一 owner，迟到 commit 因 token 失配被拒。**仍无后台自动接管**：过期租约由下个任务启动时抢占，CK 孤儿 publishing 只检测（读不阻塞） |
| DIST-06（已闭环） | DES-002 §9 | repair 状态 CAS 原子化 | 已落地：`set_repair_status` 用 `coordination.lock(repair_status:{id})` 串行化 read-then-write，跨 worker 不再各写终态；42 passed |
| DIST-07（已闭环） | DES-003 D-01 | 签名 claim→create 竞态闭环 + 悬挂签名修复 | 已落地：manager 用 `coordination.lock(mining_claim:{sig})` 包住 claim→create（并发双 run 窗口消除）；`clear_dangling_signatures()` 随启动 recover 清理悬挂映射；32 passed |
| DIST-08（已闭环） | §7.1.0 总原则 | `last_runs` 缓存 inf TTL | 已落地：TTL inf → 有界 30s（`_LAST_RUNS_TTL`），数据源本就共享，跨 worker 最终一致 |
| DIST-09（已闭环） | §7.1.0 总原则 | `/api/data/clear` 跨实例同步 | 已落地：`job_store.clear()` 同步 `coordination.job_state.clear()`；取消标志按 TTL 自愈。**CK 数据清库仍是运维独立操作**，clear API 只清本地 |
| DIST-10（已闭环） | §7.1.0 总原则 | `sync_state.updated_at` 跨实例单调 | 已落地：`_write` 用 `coordination.lock(sync_state:{task})` 串行化，updated_at 以 CK 历史 max 为下界 +1ms，FINAL 收敛确定 |
| DIST-11（已闭环） | §7.1.0 总原则 | `strategy_cache` 多 worker 强制 redis | 已落地：validator 拒绝 `api_workers>1` + `cache_backend=memory`，与 mining 校验对齐 |
| DIST-12（已闭环） | DES-004 | mining recover owner 存活判定 | 已落地：run 转 running 写 `owner_pid`/`owner_host`；recover 对 running run 用 `os.kill(pid,0)` 判存活（跨主机保守跳过），不再误标 live run |
| DIST-13（已闭环） | DES-004 | coordination fencing token 接入被保护资源 | 已落地：`lock()` yield token（调用方 `if not acquired` 语义不变）；新增 `coordination.fence(key, token)`——写前比对 Redis 锁当前值是否仍是自己的 token，失配即拒。`apply_ck_repair()` 在写快照/审计/事实表前调用 fence，租约被接管时拦住迟到写入。generation 沿用 CK 侧 `_assert_lease`。测试覆盖 token 失配被拒；47 passed |
| DIST-14（本轮不做，用户决策 2026-11-20） | DES-004 | repair 任务级跨 worker 接管 | 现状：任务状态跨实例可见、崩溃后标 failed（`_reap_orphans`），但无自动接管重跑调度器。**残余风险**：后台任务被 kill 后需人工介入或等下次定时调度重跑；单 worker 或定时兜底场景风险低。多实例自愈场景需要时独立立项（等同 DES-004 S5 级别），本轮不实现 |

## 3. C 类：工程与质量债（非阻断，可延后）

| 编号 | 来源 | 事项 | 现状 |
|---|---|---|---|
| ENG-01 | ISS-004 | 工作区改动尚未提交 | 大量 tracked 改动 + 未跟踪文件，待拆 PR / DCO 提交 |
| ENG-02 | ISS-004 | `test_upgrade_guard` 偶发 flaky | mtime 精度/文件系统时序，观察复现频率 |
| ENG-03 | ISS-004 | redis 全量测试不作为常规门禁，口径待登记 | 仅核心 redis 契约测试进门禁；DEP-002 待写明 |
| ENG-04 | ISS-004 | 历史质量门禁欠账（HEAD 既有） | mining 三文件注释率/既有 mypy 错误/BLE001，单列工程债 PR |
| ENG-05 | ISS-004 | 本机测试环境与门禁口径登记 | 测试侧已用 conftest 锁 memory 修复，DEP-002 待记录口径 |
| PC-14 | ISS-001 | gitleaks 真实扫描未跑成 | 环境允许后补跑 |
| PC-15 | ISS-001 | zip 超 512KB 且未跟踪 | 加 .gitignore 或排除 |
| PC-16 | ISS-001 | 存量 noqa 逐步换真标注 | 后续按 PR 处理 |
| PC-17~PC-21 | ISS-001 | ignore 行号、AST 检查、全局搜索、缓存锁版本、提交 message 规约 | 注意事项类，随提交流程落实 |
| TR-05/07/08/09 | ISS-001 | 长函数、双实现、魔法数字、docstring/why 注释 | 未修复 |
| TR-10/11 | ISS-001 | CK 唯一读源未收口、测试裸 `DataStore(tmp_path)` | 部分随 B4/B5 处理中 |
| TR-14/16/17 | ISS-001 | 弱口令下限/cookie secure、无 RBAC/长度约束、suggestion 未分页/CORS `*` | 公网部署前评估 |
| TR-21 | ISS-001 | SAST/SCA/DAST 配置 | 不在代码内，人工核对 |
| TR-19 | ISS-001 | 容器 non-root 与精确版本钉死 | HEALTHCHECK 已加，这两项仍待定 |
| TR-06/22 | ISS-001 | 嵌套深度人工核对 / 前端规则零改动 | 分别需 PR review / 不适用 |
| REQ-001 | REQ-001 | `strategy.py` 3127 行豁免 | 阶段 1~5 完成，该文件大文件拆分仍豁免 |

## 4. D 类：设计待定 / 产品决策（非阻断，待评审）

| 编号 | 来源 | 事项 | 现状 |
|---|---|---|---|
| DES-001 D2/D3,R1~R6 | DES-001 | 存储设计待定决策与风险项 | 保持待定，按阶段评审 |
| DES-002 §9（已落地） | DES-002 | 五项待定决策 | `ck_` 前缀保留、`String`+`schema_version`、600s generation 租约/fencing/自动接管均已落地；2026-11-20 补齐 30 天快照 TTL（DDL+v3 迁移）、repair 状态 CAS、私有 staging owner 清理。见 §7.1.1 |
| DES-003 D-02~D-05（已落地） | DES-003 | mining CK 化合同待定项 | 指纹分层、签名原子去重、共享产物根/内容哈希、审计 outbox/barrier、`ck_mining_artifact` 表与 `s3/shared-root`、redis 后端强制 `MINING_ARTIFACT_ROOT` 已落地；2026-11-20 补齐 D-02 审计 90 天 TTL、D-05 多 worker 强制 redis。见 §7.1.2 |
| DES-004 S5 | DES-004 | 跨主机多机评估 | 可选，独立立项；跨主机前需共享存储与租约/fencing |
| CH-12（已闭环） | ISS-002 | `v_screener_base` 无业务调用 | 已从 B3 DoD 移除；2026-11-20 已删除视图 DDL 与 `store.screener_base()`（见 §7.1.3） |
| CH-13 | ISS-002 | 历史对照报告未解释差异 | 重新生成或标注历史中间产物 |
| RC-06（已闭环） | ISS-003/004 | DataStatusCache 各 worker 本地刷新 | 决策已登记，仅部署形态变化时重评 |

> 2026-11-20 PH-1 决策与落地：以上已确认项的主体代码已落地并通过定向测试与 pre-commit，包括 CK artifact 表与对象存储适配器；尚未重跑全量回归，部署验收仍待外部条件。后续变更走 REQ-002 §12 变更控制，数据指纹口径不再单独列为 D 类未决项。

## 5. E 类：文档状态（本次已处理 / 待处理）

本次（2026-11-19）已修正的状态矛盾：

- `DES-002` 头部“草案/待实现”→主体已落地 P0~P4；`DES-003` 头部“待评审未实现”→分布式决策主体已落地，外部验收与部署条件待补。
- `REQ-002` 头部“草案”→主体已实现尾巴未闭环；§13.16 补当前基线 4898 与 ISS-005 指针。
- `ISS-002` CH-10 证据、当前状态、CH-11 待决、建议顺序段加指针；`ISS-003` RC-02 AOF 决策、§8 结论；`ISS-004` DOC-01/ENG-05 基线、§3.1 D-01、退出标准第 8 条。
- `DES-005` §13 基线 4881→4898；`DES-001`/`dev-container.md` 四后端默认 redis；`DES-004` §2.1/§2.2、S4；`deployment.md`、`CONTRIBUTING.md`、`REQ-001` 相关状态。

仍待处理（需外部验收结果，非本次可改）：

- `DEP-002`：结论段需回填当前基线、redis 门禁口径（ENG-03/05）与 CH-04~CH-08 验收结果。
- `DEP-001`：维持“有条件通过”，待 CH-04~CH-08 完成后改判。

## 6. 当前全量门禁基线

```
4923 passed / 13 skipped / 1 xfailed / 0 failed
```

> 该数字是 2026-11-20 本轮 DIST-05~14 分布式缺口补齐 + DIST-13 fencing 接入后的全量回归结果（改造前基线 4898）。`pre-commit run --all-files` 因存量 mypy 错误（多个未改动文件）与本机缺 `pnpm` 未通过；改动文件 ruff/mypy/门禁单独通过，提交/发布前仍需清除存量门禁欠账。

- 四后端（cache/event_bus/coordination/mining_store）由 `tests/conftest.py` 强制 memory 保证隔离；redis 路径由 `test_*_redis.py` 与 `test_coordination_once.py` 单独覆盖。
- PG 容器 `tsp-pg-dev` 需运行（DES-005 账户测试）；CK 测试库 `tsp_test`。

## 7. 排期计划（2026-11-19 制定）

> **阶段沟通编号**：用 `PH-0` ~ `PH-5`（Phase）指代下列六个阶段，沟通时直接说编号即可。
> 该编号不与优先级 `P0~P3`、DES-004 架构阶段 `S0~S5`、REQ-002 批次 `B1~B6`、ISS-003 批次 `W0~W5` 冲突。

| 编号 | 名称 | 日期 | 一句话目标 |
|---|---|---|---|
| **PH-0** | 版本冻结 | 11-19~11-20 | 提交当前成果，CH-04 三天观察出结论 |
| **PH-1** | 决策与解锁 | 11-23~11-27 | D 类三项拍板，CH-05/06/09 闭环 |
| **PH-2** | 产物迁移 | 11-30~12-04 | MN-02/03/05 与 CH-07 闭环 |
| **PH-3** | B5 清理 | 12-07~12-11 | B4/B5 DoD 达成，测试债务清零 |
| **PH-4** | 正式验收 | 12-14~12-18 | B6 验收记录 + CH-08 满量性能 |
| **PH-5** | 冻结发布 | 12-21~12-23 | DEP-001/002 回填，REQ-002 改判 |

### 7.0 PH-0 执行细则（11-19~11-20，2 工作日）

**目标**：把工作区成果拆成可审查的 PR 提交，形成冻结版本 tag。CH-04 三天观察按 2026-11-20 用户决定本轮暂缓，不再作为 PH-0 并行目标。

#### 7.0.1 ENG-01：拆 PR 提交

当前工作区（2026-11-19 实测）：`feature/custom_tag_v0.3.37` 分支，111 个 tracked 修改 + 39 个未跟踪文件，相对 HEAD `4ca69fad`。按 CONTRIBUTING §10「一个 PR 聚焦一个问题」拆为 4 个 PR，**按依赖顺序**提交：

| PR | 主题 | 主要文件（示意） | 验证命令 | 依赖 |
|---|---|---|---|---|
| PR-1 | 基础设施与质量门禁 | `pg_dev.sh`、`redis-cache.sh`、`dev.container.sh`、`Dockerfile`、两个 compose、`.env.example`、`pyproject.toml`、`CONVENTIONS.md`、`.pre-commit-config.yaml`、`tsp-rule/pgsql.mdc` | `bash -n *.sh`；两个 `docker compose config -q`（需 `.env`）；`git diff --check` | — |
| PR-2 | CK 修补与验收（DES-002） | `DES-002/003` 等设计文档、`ISS-002/003`、`REQ-002`、storage/tests 下 CK 相关（`test_repair_contract.py`、`test_repository_ck_first.py`、`test_generation_ck_snapshot.py`、`test_regime_ck.py`）、`deploy/clickhouse/config.d/41-tsp-dev-memory.xml` | `bash run_tests.sh -q tests/storage tests/test_ch_*.py` | PR-1 |
| PR-3 | Redis 协调与 mining 状态机（RC/DIST/MN-01） | `coordination.py`、`coordination_audit.py`、`mining_audit.py`、`mining_run_store_redis.py`、`strategy_cache_redis.py` + 对应 `test_*_redis.py`/`test_coordination_once.py`/`test_mining_*.py`、`scripts/rc14_*`/`rc15_*` | `bash run_tests.sh -q tests/test_*redis*.py tests/test_coordination_once.py tests/test_mining_*.py` | PR-2 |
| PR-4 | DES-005 账户事务层 + S4 多 worker + 文档收口 | `app/storage/paper_pg.py`、`app/strategy/paper*.py`、`tests/test_paper_*.py`、`app/main.py`、`app/config.py`、`DES-004/005`、`ISS-004/005`、`deployment.md`、`CONTRIBUTING.md`、`docs/agent/` | `bash run_tests.sh -q`（全量 4898/0 failed）；`git diff --check` | PR-3 |

> 提交动作必须遵守 CONTRIBUTING §13/§14：**不自动提交**，先征得用户确认；每个 commit 用 `git commit -s` 签 DCO；`Signed-off-by` 姓名/邮箱与 Git 身份一致。

**每个 PR 提交前的固定自检**（§10 + §14.5）：

1. `git diff --check` 通过；`git status` 确认无 `data/`、密钥、日志、`.env`、`*.parquet`、`user_data/` 混入（当前 tracked 改动中仅 `DEP-002` 命中关键词，为合法文档）。
2. 按 CONVENTIONS 跑 ruff/mypy（改动文件）与定向 pytest；PR-4 需全量 4898/0 failed。
3. PR 描述按 §10 八项填写（问题/根因/方案/兼容性/性能/验证/界面/风险回滚），验证结果写**实际执行**的命令与数字。
4. 39 个未跟踪文件须逐个归类：上表已覆盖 30 个（源码/测试/文档/配置）；剩余 `docs/agent/` 等需在 PR-4 确认是否属于交付物，不属于则加入 `.gitignore` 或留在工作区不提交。

**冻结 tag**：PR-4 合入后打 `v1.0.1-ph0-frozen`（或项目既有 tag 命名），作为 PH-1~PH-5 验收的基准版本。

#### 7.0.2 CH-04：三天观察复查（11-20）

| 步骤 | 动作 | 产出 |
|---|---|---|
| 1 | 读取 `data/ch_acceptance_observe.jsonl`，检查日志连续性（覆盖 11-18~11-20、跨交易日、无断档） | 连续性结论 |
| 2 | 对照 DEP-001 §11 的 uptime 门槛，统计观察期 uptime 回落情况 | uptime 数据 |
| 3 | 把结论写入 DEP-002 观察记录段；若满足门槛则 CH-04 改判完成，否则登记例外 | CH-04 状态更新 |

CH-04 只依赖已有观察日志和冻结版本，不依赖 PH-1 的代码改动，可与 ENG-01 并行。

2026-11-20 复查结论：`data/ch_acceptance_observe.jsonl` 仍为 351 行，末条 `2026-11-18T01:35:15+08:00`，未覆盖 11-19/11-20；6 次 uptime 回落均为秒级抖动，但不满足连续三天观察窗口。**CH-04 仍不可闭环**，需继续采集到 11-20 同一时刻后重新执行上表三步。

#### 7.0.3 PH-0 完成标准（M0）

- [ ] 4 个 PR 按序提交并 DCO 签署，`git status` 工作区干净（或仅剩明确不提交的文件）
- [ ] 冻结版本 tag 已打
- [ ] CH-04 观察结论落 DEP-002
- [ ] 全量门禁仍 4923/0 failed

### 7.1 PH-1 决策记录（2026-11-20）

以下决策按“做一个分布式系统”原则确认，作为 MN-02/MN-03、B4/B5 剩余清理与后续验收的执行口径。主体代码已于 2026-11-20 落地并通过定向门禁；2026-11-20 逐项读码核对发现的五项缺口已补齐，§7.1.1/§7.1.2 状态均为"已落地"，其余外部验收按 PH-2/PH-4 推进，变更走 REQ-002 §12 变更控制。

#### 7.1.0 总原则

**任何跨实例需要看到的状态，不得放在本地文件、进程内存或本地锁中；本地文件只能是临时输入、离线兼容或可重建缓存。**

按 DES-004 已批准边界：同主机多进程起步，Redis 为协调态权威，CK 为业务数据/审计权威，PG 为账户事务权威；跨主机前必须补齐共享存储与租约/fencing，走 DES-004 S5 独立立项。该原则约束本节所有决策，任何与之冲突的旧口径均以本节为准。

#### 7.1.1 DES-002 §9 五项待定项

| 项 | 决策 | 落地状态（2026-11-20 读码核对） |
|---|---|---|
| 快照保留期 | **30 天 TTL** | ✅ 已落地（2026-11-20 补）：`ck_repair_snapshot` DDL + v3 迁移加 30 天 TTL；`set_repair_status()` 的 CAS 已用 `coordination.lock(repair_status:{id})` 串行化 read-then-write（DIST-06），跨 worker 不再各写终态。过期快照不承诺可回滚 |
| `ck_` 前缀 | **保留** | ✅ 已落地：三张元数据表仍用 `ck_` 前缀 |
| 审计 JSON 字段类型 | **保持 `String` + `schema_version`** | ✅ 已落地：字段保持 `String`，未引入 `Map` |
| generation 心跳超时 | **600s 租约 + 自动接管** | ⚠️ 大部分落地（2026-11-20 补）：字段与 `_LEASE_SECONDS=600` 在、错 owner/token 被拒已测；抢占已用 `coordination.lock` 串行化不再重复（DIST-05），CK enriched 写入改用 leased generation + 唯一 owner，迟到 commit 因 token 失配被拒。**无后台自动接管循环**：过期租约由下个任务启动时抢占，CK 孤儿只检测（读不阻塞） |
| staging 清理策略 | **按 `repair_id` 私有化** | ✅ 已落地（2026-11-20 补）：staging 用本次唯一 `backup_id` 私有目录并写 `.owner` 标记，`finally` 校验 owner 后才 `rmtree`。CK `repair_id` 在更底层 `apply_ck_repair()` 内生成且不向上传递，故以等价的备份批次 id 作为私有 owner；效果一致（防同名/重试误删） |

补充约束：修补审计、快照、事实表写入无跨表事务，需 outbox/对账补偿；generation commit 与 `cache.bump("enriched")` 非原子，缓存失效必须可由 CK generation 幂等重建，不能依赖调用顺序。

执行边界：决策口径不变，5 项均已落地（2026-11-20 补齐 TTL/CAS/staging owner）；generation 租约/fencing 与 repair CAS 均已实现。不做提前大范围重构，后续仅外部验收与对象存储适配。

#### 7.1.2 DES-003 数据指纹与 mining CK 合同

| 编号 | 决策 | 落地状态（2026-11-20 读码核对） |
|---|---|---|
| 数据指纹口径 | **拆分“数据指纹 + 实现指纹”，且实现指纹不进签名** | ✅ 已落地（2026-11-20 补）：指纹拆分在 `_build_data_fingerprint_once()`；claim→create 已用 `coordination.lock(mining_claim:{sig})` 串行化，悬挂签名由 `clear_dangling_signatures()` 随启动 recover 清理（DIST-07） |
| D-02 事件 TTL | **90 天，且不小于协调态重建窗口** | ✅ 已落地（2026-11-20 补）：`ck_mining_run_audit` DDL + v3 迁移加 `TTL toDate(created_at)+INTERVAL 90 DAY`。当前实现事件存 Redis、审计存 CK（`ck_mining_run_audit` 即 Redis 状态机重建源），无独立 `ck_mining_events` 表，TTL 落在审计表 |
| D-03 artifact 归属 | **内容寻址 + 共享 URI，CK 只存元数据** | ✅ 已落地：`content_addr` sha256、`store_artifact_uri()` 返回 `file://`/`s3://`、`ck_mining_artifact` 只存元数据 |
| D-04 候选库迁移 | **随 artifact 一起共享，不保留本地独占副本** | ✅ 已落地（依赖部署共享存储）：候选库随 artifact 走共享根/对象存储 |
| D-05 `CK_MINING` 开关 | **不新增独立开关，多 worker 强制 redis 后端** | ✅ 已落地（2026-11-20 补）：新增 `API_WORKERS` 配置，validator 拒绝 `api_workers>1` + `mining_store_backend=memory`；测试进程锁 `API_WORKERS=1`。无独立 CK_MINING 开关，默认 redis + 强制 `MINING_ARTIFACT_ROOT` |

补充约束：`find_by_signature → create` 必须 Redis `SETNX(signature → run_id)` 或 Lua 原子化；`register_artifact` 改 Lua/原子更新，避免并发丢更新；Redis 状态转换与 CK 审计之间用 outbox 或可等待的 rebuild barrier 闭环，避免“状态已变、审计未落”。

#### 7.1.3 CH-12 `v_screener_base` 处理

- 决策：**从 B3 DoD 移除 `v_screener_base` 调用要求**。
- 依据：`backend/app` 中无业务调用；5 处慢路径已由其他 CK 读法闭环，该视图属冗余。
- 落地动作：2026-11-20 已删除视图 DDL 与 `store.screener_base()` 方法，并同步更新 ISS-002 / REQ-002 文字；无需数据迁移。
- 兼容性：不触及在线读路径，不需要数据迁移；若后续重新引入 screener 慢路径，按新需求重新评审。

### 7.2 排期原则与关键路径

- **先冻结、再验收**：ENG-01（拆 PR 提交）放在 PH-0，让后续 CH-04~CH-08 的验收对象是一个确定的版本；环境验收与代码工作并行，但发布判定必须等两者都闭环。
- **决策已解锁**：D 类的 DES-002 §9（派生缓存 schema）、数据指纹口径、CH-12 已按“跨实例状态不得本地化”原则确认并写入 §7.1；MN-02/MN-03 与 B4/B5 remaining 可据此推进，后续变更走 REQ-002 §12 变更控制。
- **阻断优先**：P1 阻断发布项（MN-02/03/05、CH-04~CH-08）进主线排期；C 类工程债滚动安排、不阻断。
- **不硬迁深 E 类**：REQ-002 §13.9 明确深 E 类测试债务须与 B5 删 Parquet 写入同步设计，排期上不单独提前。

关键依赖链：

```text
[PH-0] ENG-01 拆 PR 提交 → 版本冻结
          │
[PH-1] D 类决策(按分布式原则确认：DES-002 §9 / 数据指纹 / CH-12)
          ├──→ MN-02/MN-03 ──→ MN-05 ──────────────┐
          └──→ B4 收尾 ──→ B5 清理 ──→ B6 验收 ─────┤
[PH-1] CH-09(pnpm) ──→ CH-07 双栈对照 ─────────────┤
[PH-0] CH-04(11-20 复查) ───────────────────────────┤
[PH-1] CH-05(基线确认) ──→ 回滚演练 ─────────────────┤
[PH-1] CH-06(CK 环境) ──→ 13 类矩阵签字 ─────────────┤
[PH-2] CH-08 真实数据积累（周期长，与代码并行）──────┤
                                                   ↓
[PH-5] ENG-03/05 门禁口径 → DOC-01 回填 DEP-001/002 → 发布判定
```

### 7.3 分周计划（工作日）

| 编号 | 阶段 | 日期 | 工作日 | 事项（编号） | 依赖 | 产出 / 验收 |
|---|---|---|---|---|---|---|
| **PH-0** | 冻结 | 11-19~11-20 | 2 | ENG-01 拆 PR 提交当前成果（DES-005 / S4 / 文档收口）；CH-04 于 11-20 复查三天观察 | 本文评审（已具备） | 冻结版本 tag；CH-04 观察结论 |
| **PH-1** | 决策与解锁 | 11-23~11-27 | 5 | D 类三项按分布式原则确认并落地（DES-002 §9、数据指纹口径、CH-12）；CH-09 已闭环；CH-05 待项目方确认回滚基线并演练；CH-06 待 CK 环境补 S3/S4 自动化与 13 类矩阵；B4 读点收敛待排 | PH-0；项目方 / CK 环境 | 决策/代码/CH-09 已落地；CH-05/06 闭环；B4 读点进一步收敛 |
| **PH-2** | 产物迁移 | 11-30~12-04 | 5 | MN-02（artifact 归属 + 保留期）、MN-03（claim/指纹 CK 唯一键）、MN-05（6 类残留缺口）；CH-07 页面双栈 12 用例；CH-08 开始真实数据积累 | PH-1 决策；CH-09 | MN-02/03/05 闭环；CH-07 报告归档；CH-08 数据开始积累 |
| **PH-3** | B5 清理 | 12-07~12-11 | 5 | T5.1~T5.6（删 DuckDB/Parquet、清 publishing、遗留路径、测试迁移清零、文档、全量基线）；TR-10/11 随 B4/B5 闭环 | MN-02/03 完成；B4 DoD 归零 | 测试债务清零；B5 DoD 达成；全量仍 0 failed |
| **PH-4** | 正式验收 | 12-14~12-18 | 5 | T6.1~T6.7（S1~S9、性能标定、真实数据抽样、回滚、三天观察、验收记录、页面对照）；CH-08 满量性能标定；T6.5 三天挂机与 T6.1~T6.4 并行 | B5 完成；CH-04~CH-07 闭环；CH-08 数据到位 | B6 验收记录；CH-08 阈值表回填 |
| **PH-5** | 冻结发布 | 12-21~12-23 | 3 | ENG-03/05 在 DEP-002 登记门禁口径；DOC-01 回填 DEP-001/DEP-002 并改判；更新 ISS-005 状态 | CH-04~CH-08 全部闭环 | REQ-002 改判完成；v1.0.1 发布判定 |

### 7.4 C 类工程债（滚动，不阻断）

| 窗口 | 事项 | 说明 |
|---|---|---|
| PH-2 空档 | ENG-02（flaky 观察）、ENG-04（mining 三文件质量欠账单列 PR） | 与 MN 系列同文件域，避免重复改动 |
| PH-3 空档 | PC-14（gitleaks 补跑）、PC-15（zip 排除）、PC-16~21 | 提交流程相关，随 ENG-01 后续 PR 落实 |
| PH-4 空档 | TR-05/07/08/09/14/16/17/19/21、REQ-001 `strategy.py` 拆分 | 大文件重构/安全评估，单列 PR，不与发布混批 |
| PH-5 后 | DES-004 S5 跨主机评估 | 可选，独立立项 |

### 7.5 里程碑与发布判定

| 里程碑 | 对应阶段 | 日期 | 判定条件 |
|---|---|---|---|
| M0 版本冻结 | PH-0 | 11-20 | ENG-01 完成，CH-04 有结论 |
| M1 决策解锁 | PH-1 | 11-27 | D 类三项拍板，CH-05/06/09 闭环 |
| M2 产物迁移完成 | PH-2 | 12-04 | MN-02/03/05 闭环，CH-07 闭环 |
| M3 B5 完成 | PH-3 | 12-11 | B4/B5 DoD 达成，测试债务清零 |
| M4 验收完成 | PH-4 | 12-18 | CH-04~CH-08 全部闭环，B6 记录完成 |
| M5 发布判定 | PH-5 | 12-23 | DEP-001/002 回填，REQ-002 改判完成 |

**发布判定的硬条件**（缺一不可）：P1 阻断项 MN-02/03/05 闭环；CH-04~CH-08 全部闭环；B4/B5/B6 DoD 达成；全量门禁 0 failed；DEP-001 从"有条件通过"改判。

### 7.6 风险与缓冲

| 风险 | 影响 | 缓冲安排 |
|---|---|---|
| 项目方回滚基线确认延迟 | CH-05 卡住，进而 M4 延后 | PH-1 即启动确认；若 11-27 未定，M4 顺延但文档/代码工作不停 |
| CH-08 真实数据积累不足 | 满量性能无法按 §9.1 标定 | PH-2 即开始积累；PH-4 若仍未到位，DEP-001 维持"有条件通过"并登记例外 |
| pnpm 环境不可用 | CH-09 曾阻塞 CH-07 | 已解决：`corepack pnpm@9.10.0` 在可写副本完成构建；后续在 CI/部署环境复跑即可 |
| D 类决策反复 | MN-02/03 与 B5 返工 | PH-1 一次性拍板并冻结；后续变更走 REQ-002 §12 变更控制 |
| 深 E 类测试债务比预期大 | B5 延期 | 按 REQ-002 §11 缓冲：T5.4 允许分两次合入（先 C/D 类再 B 类删除） |

> 人天估算：PH-1~PH-5 合计约 31 人天（B4 剩余 ~3、B5 ~8、B6 ~7、MN 系列 ~8、CH/ENG/DOC ~5），均为单人线性估算；PH-4 的三天观察为挂机，不占人天。负责人待分配，本排期不含评审等待。

## 8. 外部依赖调整（2026-11-20 用户确认）

| 编号 | 调整结论 | 后续处理 |
|---|---|---|
| CH-04 | 本轮暂不做三天观察 | 不再作为本轮发布阻断；如恢复观察，再按 DEP-002 门槛重新采集和出结论 |
| CH-05 | 本轮暂不考虑回滚问题 | 不作为本轮发布阻断；未确认基线前禁止回滚演练，后续如需回滚能力另行立项 |
| CH-06 | 当前环境 CK 可直接作为测试环境 | 已补齐 S3/S4 严格口径自动化并完成实测（32 + 85 passed），记录见 DEP-002；CH-06 闭环 |

调整后本轮剩余发布验收项为 CH-07 页面双栈对照、CH-08 满量性能；CH-04/CH-05 从阻断清单移出但保留历史记录。
