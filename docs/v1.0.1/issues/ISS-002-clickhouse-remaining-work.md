# ClickHouse 需求后续问题清单

> 文档类型：问题 | 编号：ISS-002 | 状态：评审中 | 文档版本：v1.5 | 最后更新：2026-11-20
> 负责人：— | 关联：REQ-002、DES-002、ISS-003、DEP-001、DEP-002、DEP-003、REQ-001

用途：把 REQ-002 当前未闭环问题集中记录，便于后续逐项处理和验收复核。本文以 2026-11-18 的代码、文档和实跑结果为准。

## 现象

REQ-002 的主体功能已经实现并入库：分支 `feature/custom_tag_v0.3.37`，ClickHouse 已作为主要读源接入，B1~B3 的主体链路可用。2026-11-18 已恢复全量测试基线：`4817 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors。

但按 REQ-002 的 WBS DoD 和 DEP-001 的验收口径，需求仍未完全完成：

- B4 未完成：`backend/app` 内 `read_parquet` / `scan_parquet` 尚未归零。
- B5 部分完成：DuckDB 基础设施已从 repository 删除，但 Parquet scan 函数、DataStore、双写镜像和测试迁移仍在。
- B6 未闭环：验收结论为“有条件通过”，3 天观察、回滚演练、S3/S4 完整自动化、页面级双栈对照、满量性能标定均未补齐。
- 文档状态滞后：REQ-002 仍记录“T6 尚未开始”“163 failed”“全程未提交”，与当前状态不一致。

## 复现与核验步骤

### 1. 环境与配置

```bash
cd <repo>
docker compose -f docker-compose.dev.yml config -q
docker compose -f docker-compose.dev.yml ps
curl -sS http://localhost:8123/ping
```

2026-11-18 实测：compose 配置合法，ClickHouse 容器 healthy，`/ping` 返回 `Ok`。

### 2. 存储与场景测试

```bash
bash run_tests.sh -q tests/storage tests/scenarios -p no:randomly
```

2026-11-18 实测：`58 passed`。

### 3. 全量后端测试

```bash
bash run_tests.sh -q --tb=no -p no:randomly
```

2026-11-18 连续两次结果不一致：

- 第一次：`2 failed / 4633 passed / 13 skipped / 1 xfailed / 179 errors`。
- 第二次：`4 failed / 4256 passed / 13 skipped / 1 xfailed / 554 errors`。

抽样定位：

- ERROR 集中在 `ck_runtime` / `ensure_schema` 初始化阶段，ClickHouse 报 `MEMORY_LIMIT_EXCEEDED`，当前上限 `3.20 GiB`。
- `tests/test_main_mining_lifespan.py` 和 `tests/test_extension_packages.py` 的失败同样来自该内存上限。
- `tests/test_quote_service_boot_check.py::test_logs_and_does_not_start_when_tier_disallows` 因 `data/user_data/preferences.json.tmp` 权限不足报 `PermissionError`。

### 4. 静态残留扫描

```bash
cd backend
grep -rn "read_parquet(" app --include=*.py | wc -l
grep -rn "scan_parquet(" app --include=*.py | wc -l
grep -rn "scan_parquet_compat(" app --include=*.py | wc -l
grep -rnE "scan_(daily|enriched)_parquet\(" app --include=*.py | wc -l
grep -rn "DataStore(tmp_path" tests --include=*.py | wc -l
grep -rnE "read_parquet\(|scan_parquet\(" tests --include=*.py | wc -l
```

2026-11-18 实测：生产代码分别约 44、5、16、22 处；测试代码分别约 67、139 处。计数包含少量 helper 定义和注释，但说明 WBS 的“grep 归零”远未达成。

### 5. 前端构建

```bash
cd frontend
pnpm build
```

2026-11-18 实测：当前环境未安装 `pnpm`，本轮未能执行，结果未验证。

## 影响

- 不能据此宣布 REQ-002 完成，也不能把 DEP-001 的六条 gating 视为全部满足。
- 全量测试结果不稳定，无法给出可作为发布依据的绿色基线。
- B4/B5 未清零意味着本地 Parquet 与 CK 双写、回退路径并存，后续排查数据一致性时仍有两套状态需要比对。
- REQ-002 交接记录滞后，后续开发者容易按旧数字和旧结论判断进度。
- 页面级双栈对照、回滚演练和三天观察未闭环，验收风险仍由人工承担。

## 根因

1. REQ-002 §1 的范围裁剪把 L3.5/L3.6/L4/L5 测试基线、既有测试迁移和验收记录后置，导致代码实现先行，DoD 未同步收敛。
2. 派生结果缓存是否建 CK 表曾长期未定；部分读点因此保留本地文件或 CacheBackend 方案。
3. 深 E 类测试与“删 Parquet 写入”强耦合，在 B5 完成前不适合逐例硬迁。
4. 本机 ClickHouse 内存上限和 `data/user_data` 目录权限阻塞全量测试，属于环境问题，但已影响验收证据。
5. 代码已随 REQ-001 整改提交并重构拆分，文档未同步更新最终状态。

## 问题清单

优先级中“高”表示当前阻断 REQ-002 完成判定；“中/低”表示需排期但不单独阻断主体功能使用。表中问题的实际处理状态以“处理进展”节为准。

| 编号 | 优先级 | 阻断验收 | 问题 | 证据 | 下一步 |
|---|---|---|---|---|---|
| CH-01 | 高 | 是 | 全量测试受 CK 内存上限阻塞，结果不稳定 | 两轮全量分别为 179 / 554 errors；错误为 `MEMORY_LIMIT_EXCEEDED`，上限 3.20 GiB | 提高 CK 可用内存或隔离 fixture 并发；修复后连续跑三次，记录稳定清单 |
| CH-02 | 高 | 是 | B4 `read_parquet/scan_parquet` 未归零 | 生产代码约 44/5/16/22 处相关调用 | 逐组判定迁移或保留；若裁剪范围，必须改写 REQ-002 B4 DoD |
| CH-03 | 高 | 是 | B5 清理未完成 | `parquet.py` scan 函数仍在；DataStore 仍在；CK/Parquet 双写镜像约 46 处标记；测试侧 `DataStore(tmp_path)` 约 67 处 | 先定派生缓存 schema，再删本地读点和双写；测试按 D 类模式迁移 |
| CH-04 | 中 | 是 | 三天观察未闭环 | DEP-002 记录观察进行中，日志止于 2026-11-18；要求 11-20 复查 | 检查 `data/ch_acceptance_observe.jsonl` 的跨日连续性与 uptime 回落，补结论 |
| CH-05 | 中 | 是 | 回滚演练未执行 | DEP-002 记录无 `master` 基线、`git checkout` 风险；回滚耗时未实测 | 由项目方确认回滚基线，再做演练并记录重新同步耗时 |
| CH-06 | 中 | 是 | S3/S4 未完整自动化；数据集矩阵未全量签字 | DEP-002 将 S3/S4 标为“现有测试背书”；矩阵仅 S2 的 3/4 有明确对照 | 补复权四方对照与 kill→重启→failed 场景；按 DEP-001 §5 补齐 13 类矩阵 |
| CH-07 | 中 | 是 | 页面级双栈对照未执行 | DEP-003 执行表为空，`执行用例数：__ / 12`；未见 `TC*` 截图或报告 | 按 DEP-003 执行 12 个用例，归档差异和截图 |
| CH-08 | 中 | 是 | 满量性能标定未完成 | 分钟仅 370 行、财务表 0 行；未达 DEP-001 §9.1 的数据规模 | 积累真实数据后按同口径重测并回填阈值表 |
| CH-09 | 中 | 否 | 前端构建未验证 | 本机无 `pnpm` | 在有 pnpm 的环境执行 `pnpm build`，记录结果 |
| CH-10 | 中 | 是 | 文档状态与真实状态不一致 | REQ-002 / DES-001 / DES-002 / DES-003 状态已回填（2026-11-19）；DEP-001“有条件通过”结论未变 | 已回填；剩余 DEP-002 结论与 CH-04~CH-08 验收结果待补（见 ISS-005） |
| CH-11 | 低 | 否 | 健康端点路径不一致 | WBS T1.6 写 `/api/health`，代码实际注册 `/health`；`/api/health` 只在白名单出现 | 确认是否需加别名；若不需要，统一文档口径 |
| CH-12（已确认） | 低 | 否 | `v_screener_base` 只有定义未见业务调用 | `store.screener_base()` 存在，但 `backend/app` 内无调用点 | 2026-11-20 PH-1 按分布式原则确认：从 B3 DoD 移除，PH-3 删除视图 DDL 与 store 方法（见 ISS-005 §7.1.3） |
| CH-13 | 低 | 否 | 历史对照报告存在未解释差异 | `backend/reports/cmp_b2_kline_daily_enriched.json` 仍为 `diff` / `ok=false` | 重新生成或标注为已解释的历史中间产物 |

### 处理进展（2026-11-18）

- **CH-01 已处理**：根因不是单纯内存不足，而是测试把带 `t_` 前缀的表写进正式库 `tsp`（dev compose 注入 `CLICKHOUSE_DATABASE=tsp`，`os.environ.setdefault` 未能覆盖）。已把 `tests/conftest.py` 改为强制写入 `tsp_test`；清理 `tsp` 中 17,267 张测试残留表和 `tsp_test` 残留表；开发 compose 单独挂 `41-tsp-dev-memory.xml`，容器上限 5g、CK 上限 4.5 GB。全量测试从 `2 failed/179 errors`、`4 failed/554 errors` 恢复为 `4817 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors。
- **CH-11 已处理**：`routes_api` 同时注册 `/health` 与 `/api/health`，`tests/test_memory_guards.py` 参数化覆盖两个路径。
- **CH-12 已确认（2026-11-20 PH-1）**：`v_screener_base` 无业务调用且慢路径已由其他 CK 读法闭环，从 B3 DoD 移除；PH-3 B5 清理阶段删除视图 DDL 与 `store.screener_base()` 方法，不触及在线读路径。
- **CH-02 / CH-03 首批完成**：`KlineRepository` 的股票/指数/ETF 日 K 扫描、分钟 K 四种读法改为 CK 优先，查不到时保留本地兼容路径；新增 `tests/test_repository_ck_first.py` 断言 CK 值优先；`test_symbol_data_concurrency.py` 改为显式注入 `ck_runtime`，避免全局 runtime 泄漏造成顺序依赖。
- **B5 第二批完成（核心数据写路径 CK-only）**：
  - `minute_shared_pull.pull()` 与 `minute_refresh._write_round_data()` 不再调用 `_write_minute_partition`，改走 `store.write_minute()`；缓存键加入 `data_dir.resolve()` 以避免跨测试/跨仓库串用。
  - `depth_service._persist()` 删除本地 `depth5/date=*/part.parquet` 原子写，只写 CK `depth5` 表。
  - `kline_adj._merge_adj_factor_store()`、`financial_sync._write_table()` 在有存储门面时只写 CK；离线脚本/单测无门面时仍保留 `all.parquet` / `part.parquet` 兼容路径。
  - 迁移 21 个受影响的分钟/财务测试到 CK 造数与断言；全量回归后不再生成 `backend/MagicMock/` 临时目录。
- **B5 第三批完成（维表在线读写 CK-only）**：`KlineRepository.save_instruments/save_index_instruments/save_etf_instruments` 在显式注入 `storage` 时只写 CK，无门面时才写本地 parquet；`_refresh_instruments/_refresh_index_instruments/_refresh_etf_instruments` 同样只用显式 `storage` 读 CK，避免全局 runtime 泄漏造成顺序依赖。全量回归 `4817 passed / 13 skipped / 1 xfailed`。
- **修补体系 CK 化评估（2026-11-18）**：已完成依赖盘点与分阶段方案：daily修补依赖本地分区的指纹/备份/非交易确认/历史 universe，`replace_daily_rows`、`daily_repair_nontrading_symbols`、`daily_repair_universe`、`replace_enriched_rows` 都需要 CK 权威语义。实现 daily CK 分支原型后，精确修补、历史发现、完整性 API 和 S9 共 34 个用例失败，根因是 staging/backup/publication 与本地分区深度绑定。原型已回退，全量恢复 `4817 passed / 13 skipped / 1 xfailed`。结论：daily/enriched CK-only **须实现**，不能简单替换读写函数；按 DES-002 先定义 CK 修补契约（审计/回滚快照/generation）并迁移修补测试，再切换生产写路径。
- **DES-002 P1 已完成**：三张 CK 元数据表、StorageProtocol/ClickHouseStore 接口、快照回滚和 generation 生命周期已实现；`tests/storage/test_repair_contract.py` 定向 `3 passed`，`tests/test_ch_schema.py` 已纳入新表。
- **DES-002 P2a 已完成**：显式 `storage` 下完整性扫描、修补复核和修补后校验改走 CK；本地路径保持兼容。修补相关定向测试 `88 passed`，存储层修补契约 `3 passed`。
- **DES-002 P2b 已完成**：DES-002 §8.1 列出的 8 个优先契约测试已全部迁移到 CK 数据 / 审计 / 快照断言并通过；日K修补两个文件 `67 passed`，其余优先契约文件 `41 passed`，完整性相关测试 `70 passed`（含额外迁移的快照用例）。
- **DES-002 P3 已完成**：显式 `storage` 下修补校验、universe 发现、非交易确认、`replace_daily_rows`、`replace_enriched_rows`、`_write_daily_partition`、live merge/flush 全部走 CK 权威表；状态统计与矩阵缓存 generation 改为 CK 优先。`ClickHouseStore.write` 为未显式给 `updated_at` 的写入生成严格递增毫秒版本，避免替换不确定。
- **DES-002 P4 主体完成**：显式 `storage` 路径不再依赖本地 backup/fingerprint/marker/glob；无 `storage` 的离线兼容路径按 DES-002 §3.2 保留。`SCHEMA_VERSION` 无需因三张元数据表升级。剩余：`pipeline_recompute` 常规 enriched 编排与派生缓存（regime/mainline/mining/factors）仍写本地 parquet/marker；指数 enriched 表尚未纳入 CK。
- **Redis 缓存层已落地**：`CacheBackend` 增加 `RedisCacheBackend`（Arrow IPC/JSON、TTL、Redis generation 计数器）；`CACHE_BACKEND=redis` 可切换，Redis 由宿主机已有容器提供，本项目 compose 不新建服务。`pool:*` 与 `benchmark:momentum:v1` 已接入 CacheBackend，enriched generation bump 同步递增 Redis generation。部署检查用 `./redis-cache.sh check`。
- **B5 主体完成**：daily/enriched 在显式 `storage` 下的在线读写已收敛为 CK-only（含修补与 live 路径），本地 parquet 仅保留离线兼容。派生缓存与 `repair_daily`/`minute_adjust` 离线工具仍保留本地 Parquet 路径。
- **CH-10 部分完成**：本文和 REQ-002 已回填最新测试基线；DEP-001 的“有条件通过”结论未变，因为 CH-04~CH-08 仍未闭环。
- **未处理**：CH-04~CH-08 需跨交易日观察、项目方确认回滚基线、人工执行页面对照和满量性能标定；CH-09 因本机无 `pnpm` 未验证；CH-12/CH-13 待产品/维护者决策。

## 解决方案与建议顺序

> 本节是当时的规划顺序，保留作过程记录。CH-01 已处理、CH-02/CH-03 首批完成、CH-10 已回填；当前仍未闭环的 CH-04~CH-09、CH-12/CH-13 统一见 [ISS-005](ISS-005-outstanding-items-summary.md)。

1. **先恢复可验证基线**：处理 CH-01 的 CK 内存上限和目录权限，连续三次全量测试得到稳定失败清单；CH-09 在可用环境补 `pnpm build`。
2. **再定范围**：对 CH-02/CH-03 逐组决定“迁移 / 保留并说明理由 / 范围裁剪”。范围裁剪必须同步修改 REQ-002 DoD，不能只改状态列。
3. **清理代码与测试**：按 `ck_runtime` + 生产写函数双写 + 语义边界标注的既有模式，推进 Parquet 读点收敛和 `DataStore(tmp_path)` 测试迁移。
4. **补齐验收**：完成 CH-04~CH-08，再按 DEP-001 §11 六条 gating 逐项签字。
5. **同步文档**：最后更新 CH-10，把本文问题状态回填到 REQ-002 §13 和 DEP-002 结论，避免再次出现交付状态失真。

## 当前状态

- 代码状态：`feature/custom_tag_v0.3.37`；工作区仍有未提交改动（ENG-01 待拆 PR 提交，见 ISS-004/ISS-005）。
- 服务状态：ClickHouse 容器 healthy，`/ping` = `Ok`，生产/开发 compose config 均合法；`tsp_test` 已清空重建，`tsp` 恢复为 28 张正式表。
- 定向验证：日K/enriched 修补、写入、generation、完整性 API/S9 与场景测试全绿；改动文件 Ruff 全绿，`git diff --check` 通过。
- 全量验证：当前基线 `4898 passed / 13 skipped / 1 xfailed`，0 failed（四后端 conftest 锁 memory；历史 4820 为阶段实测，见 ISS-004 §1.1）。
- 前端构建：仍未执行，原因是当前环境没有 `pnpm`。
- 总体判断：REQ-002 **主体功能已实现且测试基线已恢复**，但验收条件仍未全部满足；CH-04~CH-09 处理完并同步文档后，才能改判为完成。

## 待决事项

- CK 修补契约：DES-002 P0~P4 主体已完成；daily/enriched 显式 storage 在线读写已 CK-only，剩余尾巴见 ISS-003 §3.3。
- B5 剩余本地 Parquet 路径的归属：daily/enriched 在线路径已收敛；`pipeline_recompute` 常规编排、派生缓存与离线工具的后续归属见 ISS-003 §3.3。
- Redis / CacheBackend / 多实例协调问题：统一在 ISS-003 记录和排期，本文不再展开。
- W5 人工 / 环境验收（CH-04~CH-09）、状态化缓存、mining CK 化与工作区提交等后续项：统一在 [ISS-004](ISS-004-remaining-followups-and-acceptance.md) 记录和排期。
- `market_level` 扩展是否支持 timeseries；如支持，需要为无 symbol 行设计 CK 存储。
- 回滚演练使用哪个基线分支替代文档中的 `master`。
- ~~`/api/health` 是否作为真实路由别名提供，还是仅修正文档~~ → **已处理**：`routes_api` 同时注册 `/health` 与 `/api/health`，`test_memory_guards` 参数化覆盖两个路径（CH-11）。
- ~~`v_screener_base` 是否纳入后续 screener 慢路径，或从 B3 DoD 中移除~~ → **已确认**：从 B3 DoD 移除，PH-3 删除视图 DDL 与 store 方法（CH-12，ISS-005 §7.1.3）。
