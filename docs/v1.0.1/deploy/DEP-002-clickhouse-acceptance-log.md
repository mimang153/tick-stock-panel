# ClickHouse 迁移验收记录

> 文档类型：部署 | 编号：DEP-002 | 状态：基线 | 文档版本：v1.4 | 最后更新：2026-11-18
> 负责人：— | 关联：DEP-001、REQ-002、ISS-002、DES-002
> 归档来源：base_v1.0 基线文档，按 DOC-STANDARD v1.0 归类命名；内容未改。

> 模板来源：`./DEP-001-clickhouse-acceptance.md` §15。按批次追加，没有记录的批次视为未验收。

## B6 验收与试运行（2026-11-17，进行中）

- **执行人**：CodeBuddy（AI 辅助），环境 `tsp-dev` + `tsp-dev-clickhouse`（CK 24.8.8）+ `tsp-origin`（旧栈 oracle）
- **代码范围**：`feature/custom_tag_v0.3.37` 分支，全程未提交（工作区 126 项改动）；涉及 B5.1 收尾（DuckDB 依赖清零）+ B6 验收资产
- **测试结果**：`pytest tests -q` → 4655 passed / 165 failed / 7 skipped / 1 xfailed。新增 `tests/scenarios` 5 例全绿；`tests/storage` 54 例全绿。165 failed 为既有深 E 类测试债务（与 B5 删 Parquet 写入强耦合），本批次无新增失败项能归因于本批次改动。
- **数据集矩阵**：通过 3/4（§S2 对照）：`kline_daily`、`kline_daily_enriched`、`adj_factor` 逐值零差异；`instruments` 有差异但已判定为预期（见下）。

### 场景测试（§7）

| 场景 | 状态 | 证据 / 说明 |
|---|---|---|
| S1 首次使用全流程 | ✅ | 新增 `tests/scenarios/test_s1_first_use.py`：空库→造数→status 可观测→instruments 搜索→kline/daily 行数→watchlist→CK 落库 |
| S2 指标口径交叉 | ✅（部分） | `scripts/cmp_stacks.py` 实测 4 数据集；3 个零差异，`instruments` 差异已判定为预期（见"差异判定"）。origin API 逐值对照 + 手算四方比对未做完整自动化 |
| S3 复权链路 | ✅（2026-11-20 补齐） | `/api/kline/daily` 已支持 `adjust=forward|none`，新增 `/api/adj-factor`；`tests/scenarios/test_s3_adjustment_chain.py` 真 CK 三响应逐行对账通过 |
| S4 同步中断恢复 | ✅（2026-11-20 补齐） | `tests/scenarios/test_s4_sync_interruption.py` 子进程 `os._exit()` 模拟 kill，启动恢复后 `/api/sync/status` failed 且 error 含“中断”，重跑行数完整无重复 |
| S5 实时写入可见性 | ✅（现有测试背书） | `test_quote_chain.py::test_three_assets_share_publication...` 完整覆盖 provider→缓存→三读端点→落盘 |
| S6 回测全链路 | ✅（现有测试背书） | `tests/backtest/test_composite_backtest_e2e.py` 覆盖策略→回测→trades/曲线 |
| S7 扩展数据端到端 | ✅ | 新增 `tests/scenarios/test_s7_ext_data.py`：建配置→ingest→rows（CK 读）→删除→404/空→主页面不受影响 |
| S8 并发与一致性 | ✅ | 新增 `tests/scenarios/test_s8_concurrency.py`：8 线程 HTTP 并发 + 16 次并发 store 查询，无 500/无脏读。**发现并修复 P1 缺陷**（见下） |
| S9 完整性检查与修复 | ✅ | 新增 `tests/scenarios/test_s9_integrity.py`：CK 双写造数→坏分区→自检精确定位日期→修复→复查 clean |

**本批次发现并修复的 P1 缺陷**：`ClickHouseClient` 默认 session 模式下，同一 client 并发查询抛 `ProgrammingError: concurrent queries within the same session`。真实 FastAPI 多线程并发访问页面会稳定 500。修复：`_build_client` 设 `autogenerate_session_id=False`（`session_timezone` 作为每次查询 settings 下发，时区口径不变）。验证：S8 并发测试通过 + `tests/storage` 54 例（含 §4.6 时区专项）全绿。

### 性能冒烟（§9.1，实测 2026-11-17 11:07 CST）

| 场景 | 实测 | 阈值 | 判定 |
|---|---|---|---|
| 单票日K 10 年 | 18.1ms / 242 行 | < 300ms | ✅ |
| 全市场单日横截面 | 16.3ms / 5571 行 | < 5s | ✅ |
| 全市场日K 1 年 | 1050ms / 133.2 万行 / RSS 315MB | < 15s / < 2G | ✅ |
| 单票分钟查 1 月 | 5.0ms / 370 行 | < 2s | ⚠️ 分钟表现存仅 ~1100 行，非 5000 万满量，未能满量标定 |
| 财务宽表 | 3.6ms / 0 行 | < 3s | ⚠️ 财务表为空，未能满量标定 |

数据文件：`data/ch_perf_smoke.json`。分钟/财务满量标定待真实数据积累后补测（非阻塞，已远超当前规模阈值）。

### 双栈对照差异判定（§16.4）

| 数据集 | 结论 | 判定依据 |
|---|---|---|
| kline_daily / kline_daily_enriched / adj_factor | 无差异 | Parquet 与 CK 逐值比对，仅旧栈=0、仅新栈=0、差异列={} |
| instruments | **预期差异（可解释）** | 行数一致（5892）。`as_of` 全部不同（CK=11-17 / Parquet=11-15）：`instrument_sync` 同步只写 CK，本地 parquet 是遗留镜像、非权威读源，写时点不同所致。`name` 7 处差异均为 `XD`（除权除息）前缀动态标注，属数据源时点差；`listing_date`/`shares` 少量差异同源。读路径已切 CK，本地 parquet 不再更新，差异合理。 |

报告：`data/cmp_s2_*.json`。本批次修复 `scripts/cmp_stacks.py` 报告写出的 date JSON 序列化问题（P3，`default=str`）。

### 连续观察（§11，T6.5）

- 采集器 `scripts/ch_acceptance_observe.py` 已后台启动，每 5 分钟记录 CK 健康/6 表行数/schema 版本/完整性条目/uptime 到 `data/ch_acceptance_observe.jsonl`，持续 3 天。
- 首条：CK status=ok、failed_batches=0、schema_version=2、integrity_entries=0、6 表行数正常。
- **跨交易日/跨重启判定**：观察尚未满 3 天（进行中），需在 11-20 复查 JSONL 的 uptime 回落（重启）与各日行数连续性后闭环。

### 回滚演练（§11 第 7 条，T6.4）

- **未执行破坏性回滚**：当前分支 `feature/custom_tag_v0.3.37` 有 126 项未提交改动（用户要求不提交），`git checkout` 会丢失工作区，不可执行。
- **仓库无 `master` 分支**（`git rev-parse master` 失败），验收文档约定的 `git checkout master` 路径不存在——这是回滚演练的真实阻塞项，需项目方确认回滚基线分支。
- **替代证据**：`tsp-origin` 容器（旧栈 Parquet+DuckDB 代码）已成功启动且 `/health` = ok（v0.3.37），证明旧栈路径可运行。回滚耗时（旧数据弃用后需重新同步）未实测。

### 真实数据源抽样（§13.2，T6.3）

- B6 同步冒烟以真实 TickFlow 数据源拉取 2026-11-16 全市场：日K 5572 行、指数 599 行、enriched 11144 行写回 CK 并读回一致。属真实数据源抽样验收。

### 缺陷汇总

- P1：0 项遗留（本批次发现 1 项已修复：CK client 并发）
- P2：1 项（回滚基线分支缺失 —— 流程阻塞，非代码缺陷）
- P3：1 项已修复（cmp_stacks date 序列化）

### 结论

**有条件通过**：核心数据链路（读路径 CK 唯一、双写镜像、口径一致、并发安全）验证通过；条件为 ① 连续 3 天观察在 11-20 复查闭环；② 回滚基线分支由项目方确认后补演练；③ S3/S4 的完整自动化场景（现有测试已分段背书）与页面级双栈对照（T6.7 手工清单）按需补齐。全量失败为既有测试债务，不构成本批次回归（严格对比见下）。

---

## 追加：P1 修复的严格回归对比（2026-11-17）

对 `autogenerate_session_id=False`（P1 并发修复）做两次全量对照，不比数量比清单：

- **A（含修复）**：163 failed / 4657 passed
- **B（临时回退修复，去掉 `autogenerate_session_id=False`）**：161 failed / 4659 passed
- 复现方式：两次全量 `pytest tests -q --tb=no`，用 `grep '^FAILED' | sed 's/ - .*//' | sort` 提取清单后 `diff`；临时清单与日志在核对后已清理，未入库。
- **diff 结果**：
  - `B 独有`：`test_s8_concurrent_store_queries` —— 唯一有因果关系的用例，回退必失败、含修复必通过，**证明 P1 修复有效**。
  - `A 独有`（3 例）：`test_ext_config_load_all_cache::test_upsert_edit_invalidates_cache`、`test_sector_rotation::test_series_matrix_aligns_with_timeline_and_sectors`、`test_task_registry::test_orphan_running_pruned_on_read` —— 两版单独重跑时失败用例随机轮换（回退版挂 sector_rotation，修复版挂 task_registry），确认为**时序/状态污染波动用例**，与 session 修复无因果关系。
- **结论**：P1 修复零回归，已恢复修复版（`clickhouse.py:280`）。失败数在 161~165 间波动源于上述时序用例，非稳定指标。

## 追加：全量失败分类与清理排期（2026-11-17）

基于稳定清单 A（163 例 / 49 文件）按根因分类：

| 类别 | 规模 | 根因 | 处置策略 | 建议批次 |
|---|---|---|---|---|
| **A 类 深 E** | 82 例 / 17 文件 | 测试用 `DataStore(tmp_path)` 造数但无 `ck_runtime`，读路径切 CK 后"存储层未初始化"/空数据断言失败。含 TOP：`daily_repair_history_discovery`(30)、`enriched_partial_partition`(20)、`minute_gaps`(16)、`minute_source_guard`(6) | 统一改造：`DataStore(tmp_path)` → `KlineRepository(DataStore, storage=ck_runtime)`，造数经 `store.write_*` 或双写生产写函数（验收 §14.1 D 类）。同根因批量处理，一次 fixture 改造清一片 | T5.4-1（优先，2~3 人天，清 ~82 例） |
| **B 类 迁移不彻底** | 5 例 / 4 文件 | 已有 `ck_runtime` 但仍失败：`adj_factor_standalone_sync`、`enriched_full_rebuild`、`ext_granularity`、`sector_rotation`（后者为波动） | 逐个排查：确认是迁移遗漏（修代码/断言）还是顺序波动（加隔离） | T5.4-2（0.5 人天） |
| **C 类 混合** | 76 例 / 28 文件 | 多样：① 快照过期（`test_ch_schema` 缺 `market_mainline`）；② 财务/分钟深 E 变体（assistant_actions、custom_factor_mining 依赖本地财务数据）；③ 真实时钟依赖（`kline_sync_timezone` 期望固定日期却拿到今天）；④ 状态污染波动 | 按子类分批：快照过期当天修；深 E 变体随 T5.4-1 摸排；时钟依赖改 monkeypatch 固定"今天"；波动用例加隔离 | T5.4-3（1~2 人天，视摸排结果） |

**排期原则**：
1. **A 类先行**（ROI 最高）：82 例同根因，一次 fixture 模式改造清一半失败，且直接对齐 §14.1 迁移模式。
2. **快照/时钟类随手清**：`test_ch_schema`、时间依赖用例改动小、风险低，穿插处理。
3. **波动用例单独治理**：`sector_rotation`、`task_registry`、`ext_config_load_all_cache` 等顺序依赖用例，统一加 fixture 隔离（与 A 类改造共用 `ck_runtime` reset 机制）。
4. **完成标志**（验收 §14.3）：`DataStore(tmp_path)` 裸用归零、skip 清零、全量无顺序波动（连续 3 次全量失败清单一致）。

**不做的事**：不为追求"全绿"而 skip/xfail 掩盖失败（验收 §14 明确禁止）；不在 B6 验收批次内动这些测试（避免与验收结论耦合），作为独立 T5.4 后续批次执行。

### T5.4-1 试点执行结果（2026-11-17）

**试点文件**：`tests/test_daily_repair_history_discovery.py`（30 例，原全 failed）。

- **根因**：`recompute_daily_symbols` 经 `_daily_store()→require_storage()` 读 CK 原始日K重算，测试只写本地 parquet → `RuntimeError: 存储层尚未初始化`。
- **改造**（已验证模板）：
  1. `env(..., ck_runtime)` 加 fixture；`KlineRepository(DataStore(tmp_path), storage=ck_runtime)`。
  2. 造数加 `write_raw(day, df)` helper：写本地 parquet 分区 + `ck_runtime.write_daily(raw_dir, df)`。**关键**：测试内部每处额外写本地分区（earlier 日/TODAY 坏分区/null-date/改 close）都必须同步 CK，否则 CK 与本地不一致导致定位失败。
  3. 语义边界：`test_missing_row_date`（行内 null date）在 CK 分区键下不可达（CK 存不进 null date），改为验证仍有效的部分——自检读本地能识别 null-date 并定位源分区；修复阶段由等价用例用 CK 可表达坏值覆盖。**不造假，如实标注边界**。
- **结果**：30/30 通过；全量 `163 → 133 failed`、`4657 → 4687 passed`，零新增回归。

**精确失败分类**（工具 `scripts/classify_failures.py`，逐文件独立进程重跑 133 个失败 node）：

| 类别 | 数量 | 含义 | 处置 |
|---|---|---|---|
| STABLE 稳定失败 | 112 | 单独跑仍失败，真实迁移债务 | 需逐个 CK 迁移改造 |
| FLAKY 波动 | 21 | 单独跑通过，全量并跑时状态污染/顺序依赖 | 加 fixture 隔离（较简单），不属迁移债务 |

这修正了先前"A 类 82 例一次 fixture 改造清一片"的乐观估算：**实际是 112 例稳定债务、根因异质**（接口漂移如 `_prune_partial_enriched_partitions` 签名从 2 参变 3 参、CK read_minute/read_daily/ext_timeseries 读空、CK 无法表达的 corrupt/null 语义边界），不能用单一模板批量处理，需按文件逐个看真实失败栈。

**剩余工作量（实测修正）**：
- 112 stable：按文件分批，TOP `enriched_partial_partition`(20，接口重写)、`minute_gaps`(16，CK read_minute+语义边界)最难；其余 76 例分布 30+ 文件。试点模式（ck_runtime + 双写 + 语义边界标注）可复用，但每文件需针对性验证。预估 6~10 人天（原估 2~3 人天偏乐观）。
- 21 flaky：统一加 fixture 隔离，预估 0.5~1 人天。
- `scripts/classify_failures.py` 作为清理进度度量工具保留（只读诊断）。

### T5.4 方向 2 执行结果（2026-11-17，低垂果实 + flaky 隔离）

目标：先清快照过期与状态污染类，快速压缩失败数。改 8 个文件：

| 文件 | 改动 | 结果 |
|---|---|---|
| `test_ch_schema.py` | 快照加 `market_mainline` | 10 例全绿（原 1 failed） |
| `test_minute_history_repair.py` | fixture 加 ck_runtime + repo 注入；因 CK 镜像补 OHLC 列，断言从整表 equals 改按业务列 select 比较 | 8 例全绿 |
| `test_minute_source_guard.py` | autouse fixture 加 ck_runtime 隔离全局 runtime；2 repo 注入 | 9 例全绿 |
| `test_minute_partition_merge.py` | autouse fixture 加 ck_runtime | 5 例全绿 |
| `test_minute_provider_stream.py` | 1 测试加 ck_runtime + repo 注入 | 18 例全绿 |
| `test_tdxquant_adj_response.py` | 2 测试加 ck_runtime（伪 repo 走全局 get_storage） | 18 例全绿 |
| `test_resolve_universe_index.py` | repo fixture 加 ck_runtime；`_write_instruments` 同步写 CK instruments（load_instruments 读 CK） | 3 例全绿 |
| `test_precise_daily_repair.py` | env 加 ck_runtime + 坏值同步 CK | 34 通过，3 深水区失败保留（见下） |

**验证**：全量 `133 → 112 failed`、`4687 → 4708 passed`（-21 失败、+21 通过），零新增回归。7 个文件失败全清，`test_precise_daily_repair` 仅剩 3 例深水区失败。所有改动文件 ruff 全绿。

**治理模式**：flaky 根因是全局 `storage.runtime` 进程单例被跨测试泄漏——`require_storage()`/`get_storage()` 的测试若未建 ck_runtime，会读到其他用例残留的 runtime（错误 CK 前缀/未初始化）。统一解：fixture 加 `ck_runtime`（自动 init + 隔离随机表前缀）；repo 需注入则用 `KlineRepository(DataStore(tmp_path), storage=ck_runtime)`；CK 镜像回读补齐列时按业务列比较而非整表 equals。

**剩余 3 例深水区（`test_precise_daily_repair`）**：`test_one_corrupt_file_falls_back...`（corrupt parquet 文件语义 CK 不可表达）、`test_recompute_uses_full_raw_history...`（turnover_rate 口径差异）、`test_empty_recomputed_market_result...`。这些是真实 stable 债务，CK 不可表达语义需按试点 null-date 方式标注边界，留待 T5.4 深水区批次，未用 skip/xfail 掩盖。

### T5.4 深水区执行结果（2026-11-17）

| 文件 | 改动 | 结果 |
|---|---|---|
| `test_enriched_partial_partition.py` | 20 例整体重写：`_prune_partial_enriched_partitions` 已改成 CK store API（list_daily_dates/read_daily/delete_daily_date），测试从"操作本地目录"改为"CK store 造数 + 查 CK 断言"；enriched 补 `raw_close` 避免被 stale 检查误删；"缺列/损坏文件/多文件"等 CK 不可表达场景用 monkeypatch `read_daily` 抛错验证"读取失败时保留分区"语义（非造假） | 20 例全绿 |
| `test_minute_gaps.py` | 20 例重写：`scan_minute_gaps` 仍枚举本地 `date=*/part.parquet` 目录定日期，但数据经 `_read_minute_activity` 走 CK read_minute、日线核对走 CK read_daily → 造数改为"本地占位目录 + CK 真实分钟/日K数据"；null/缺列场景用极小活动量等价表达"无法确认零成交" | 20 例全绿 |
| `test_precise_daily_repair.py` | 3→2：`test_empty_recomputed_market_result` 的 `market_mainline.replace_mainline_range` 已优先走 CK，测试本意是验证本地 fallback，monkeypatch `_mainline_store`→None 强制走本地路径 | 35 通过 / 2 失败 |

**验证**：全量 `112 → 74 failed`、`4708 → 4746 passed`（-38 失败、+38 通过），零新增回归。改动文件 ruff 全绿。

**剩余 2 例（`test_precise_daily_repair`）为同一 fixture 语义冲突**：env fixture 把 BAD_DAY 坏值(close=999)写到 CK 以满足"发现历史坏数据"类测试；但 `test_one_corrupt_file_falls_back`（本地文件损坏回退）与 `test_recompute_uses_full_raw_history`（数据源修复）需要 CK 中为好数据，两类需求在同一 fixture 中互斥。需重构为"每个测试自行控制 CK 状态"（拆分 fixture 或按需写 CK），属深水区后续，未用 skip/xfail 掩盖。

**累计清理进度**：`163 → 74 failed`（清 89 例 / -55%），`4657 → 4746 passed`。剩余 74 例为分散在 30+ 文件的 stable 债务，多为同类"本地造数 vs CK 权威读源"适配，按本批建立的三类模式（ck_runtime 隔离 / CK store 重写 / 语义边界标注）可继续逐文件推进，预估剩余 4~7 人天。

### T5.4 第二批执行结果（2026-11-17）

**发现并修复 P1 缺陷（生产代码）**：`app/services/kline_sync.py` 的 `_latest_minute_datetime` / `_earliest_minute_datetime` 误调 `repo.minute_bounds(asset_type)`——但 `minute_bounds` 是 **store** 方法（`storage.store.minute_bounds(table)`），`KlineRepository` 没有该方法。异常被 `except` 吞掉后返回 None，导致分钟增量/向前扩展同步的窗口回退到真实当前时间（而非本地最新/最早分钟 K）。生产环境同样触发（分钟同步窗口错误，可能漏拉或重复）。修复：新增 `_minute_bounds()` 走 `get_storage().store.minute_bounds(kline_etf_minute|kline_minute)`，两个函数改调它。由 `test_etf_minute_persistence` + `test_kline_sync_timezone` 暴露并验证（10 例通过）。

| 文件 | 改动 | 结果 |
|---|---|---|
| `test_main_mining_lifespan.py` | mock `KlineRepository` 的 lambda 补 `*a,**kw`（main.py 新增 storage 参数） | 5 例全绿 |
| `test_kline_sync_timezone.py` | `_capture_minute_window` 改写 CK 分钟数据供 `_minute_bounds` 读（配合 P1 修复） | 5 例全绿 |
| `test_ext_rows_projection.py` | fixture 加 ck_runtime + 写 `ext_timeseries`/instruments；snapshot fallback 用例禁用 CK 门面 | 14 例全绿 |
| `test_assistant_financials_order.py` | `_write_metrics` 同步写 CK `financials_metrics` | 2 例全绿 |
| `test_assistant_auto_approve.py` | 3 用例写 CK instruments（`_listing_dates` 走 `load_instruments` CK） | 28 例全绿 |
| `test_assistant_actions.py` | coverage 用例写 CK kline_daily/enriched/minute + financials_metrics | 1 例全绿 |
| `test_assistant_ext_tables.py` | `_make_timeseries_table` 同步写 CK `ext_timeseries` | 7 例全绿 |
| `test_etf_minute_persistence.py` | market fixture 加 ck_runtime + repo 注入；minute window 用例双写 CK（暴露 P1） | 5 例全绿 |

**验证**：全量 `74 → 56 failed`、`4746 → 4764 passed`（-18），零新增回归（净清 18 例；个别 flaky 用例在其他位置重现，属已知状态污染）。改动测试文件 ruff 全绿；`kline_sync.py` 的 ruff 告警均为既有代码（B023/C901/F841，行号远离本次改动），未引入新告警。

**累计**：`163 → 56 failed`（清 107 例 / -66%），`4657 → 4764 passed`。本批额外交付 P1 bug 修复（分钟同步窗口）。剩余 56 例继续按三类模式清理。

### T5.4 第三批执行结果（2026-11-17）

| 文件 | 改动 | 结果 |
|---|---|---|
| `test_ext_time_field_latest_session.py` | `_write_three_sessions` 逐盘同步写 CK `ext_timeseries`（time_value=ts）；3 用例注入 ck_runtime | 5 例全绿 |
| `test_screener_coverage_warnings.py` | `_make_partitions` 从全局 runtime 写 CK enriched 表（enriched_history_days 走 list_daily_dates，忽略 data_dir）；"目录不存在返回0"改为查未造数的 etf 资产 | 10 例全绿 |
| `test_ext_market_level.py` | client fixture 加 ck_runtime（ingest 写/rows 读走 CK） | 4/5 通过；1 例 `market_level timeseries` 仍读空待确认（见下） |

**验证**：全量 `56 → 48 failed`、`4764 → 4772 passed`（-8），零新增回归。改动文件 ruff 全绿。

**待确认（非阻塞）**：`test_ext_market_level::test_market_level_ingest_and_rows_roundtrip` —— market_level（无 symbol 列）的 timeseries 数据 ingest 后 `/rows` 读空。CK `ext_timeseries` 主键含 symbol，market_level 行无法入库；`_read_ext_dataframe` timeseries 分支又强制读 CK（snapshot 分支有 market_level 本地回退）。这是**产品契约边界**：market_level 是否支持 timeseries 需产品方确认（若支持则需为 market_level 设计无 symbol 的 CK 存储，若不支持则测试应改用 snapshot 模式）。未用 skip/xfail 掩盖。

**累计**：`163 → 48 failed`（清 115 例 / -71%），`4657 → 4772 passed`。剩余 48 例多为 integrity corrupt/损坏文件/symlink 等 CK 不可表达语义边界（集中在 `test_integrity_*`/`test_data_integrity*`/`test_atomic_parquet_writes`），需逐个按"语义等价标注"处理，单个成本高。

## 追加：2026-11-18 全量测试基线恢复（不改变“有条件通过”结论）

- 本轮排查发现此前 `MEMORY_LIMIT_EXCEEDED` 的根因不是 CK 容量本身，而是测试把带 `t_` 前缀的表写进正式库 `tsp`。已强制 pytest 使用 `tsp_test` 并清理 17,267 张残留表，开发 compose 另加内存覆盖。
- 全量回归：`bash run_tests.sh -q --tb=no -p no:randomly` → `4817 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors；B5 第二批后复跑仍为同一结果，且不再生成 `backend/MagicMock/` 临时文件。
- B5 第三批完成：维表 instruments 在线读写改为 CK-only（仅显式注入 storage 时），避免全局 runtime 泄漏；daily/enriched 写入仍保留双写（依赖修补体系），publication 标记、派生缓存和离线工具仍待收敛。
- 2026-11-18 评估 daily 修补 CK 分支原型：34 个修补/完整性用例依赖本地备份、指纹和 publication 标记，原型已回退；全量仍为 `4817 passed / 13 skipped / 1 xfailed`。daily/enriched CK-only **须实现**，按 DES-002 分阶段完成。
- DES-002 P1 已完成：三张 CK 元数据表和存储层修补接口已实现，修补契约定向测试 `3 passed`。
- DES-002 P2a 已完成：显式 storage 下完整性扫描/补丁复核/修补后校验走 CK，修补相关定向测试 `88 passed`。
- DES-002 P2b 已完成：DES-002 §8.1 的 8 个优先契约测试全部迁移并通过；日K修补相关两个文件 `67 passed`，其余优先契约文件 `41 passed`，完整性相关测试 `70 passed`。
- DES-002 P3 已完成：显式 storage 下修补、校验、universe、常规写入、live merge/flush 全部走 CK 权威表；状态统计与矩阵缓存 generation 改为 CK 优先；`ClickHouseStore.write` 生成严格递增行版本。
- DES-002 P4 主体完成：显式 storage 路径移除本地 backup/fingerprint/marker/glob 依赖；无 storage 的离线兼容路径保留。`SCHEMA_VERSION` 无需因三张元数据表升级；`pipeline_recompute` 常规编排与派生缓存仍写本地 marker/parquet，指数 enriched 表尚未纳入 CK。
- Redis 缓存层：`RedisCacheBackend` 已实现，`CACHE_BACKEND=redis` 可切换；复用宿主机已有 Redis 容器（compose 不新建服务），部署检查脚本 `./redis-cache.sh check`。`pool:*`、`benchmark:momentum:v1` 已接入，enriched bump 同步递增 Redis generation。
- 最终验证：定向测试全绿；全量回归 `4820 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors；改动文件 Ruff 全绿，`git diff --check` 通过。
- CH-09 已闭环：2026-11-20 用 `corepack pnpm@9.10.0` 在可写临时副本执行 `tsc -b && vite build` 成功（2923 modules transformed，仅 chunk-size/browserslist 警告）。
- 2026-11-20 分布式主体已落地（2026-11-20 读码核对后更正范围）：generation fencing/lease（600s 自动接管）、mining 指纹分层/原子去重/审计 outbox、共享产物根 `MINING_ARTIFACT_ROOT`（redis 后端未配置则启动失败）已落地；**repair 租约/CAS 与 30 天快照 TTL、D-02 事件表 90 天 TTL 实际未落地，私有 staging 未按 `repair_id`/owner 清理，D-05 多 worker 强制 redis 仅靠默认值无代码拦截**，详见 ISS-005 §7.1.1/§7.1.2 与 B 类 DIST-01~04。定向测试与改动文件 pre-commit 全通过，全量回归与全量 pre-commit（存量 mypy 错误、缺 pnpm）未通过。
- 仍未闭环：页面级双栈对照（CH-07）与满量性能标定（CH-08）。CH-04 三天观察、CH-05 回滚演练按 2026-11-20 用户决定本轮暂缓/不纳入范围；CH-06 环境阻塞已解除并完成实测（见下）；CK artifact 表与对象存储适配器已落地。因此 DEP-002 结论按本轮范围更新见下方 CH-06 记录。

## 追加：CH-06 实测记录（2026-11-20）

- **执行人**：CodeBuddy（AI 辅助）；**环境**：当前 `tsp-dev` 环境 CK，测试库 `tsp_test`，无需另建 CK 测试环境。
- **S3 实现与验证**：`GET /api/kline/daily` 新增 `adjust=forward|none`（默认 forward，保持旧调用方）；新增 `GET /api/adj-factor`。`tests/scenarios/test_s3_adjustment_chain.py` 用真实 CK + TestClient 验证三响应，逐行断言 `forward_close == raw_close / F`，覆盖除权日前一日、当日、后一日。
- **S4 实现与验证**：新增 `GET /api/sync/status`；盘后/除权因子任务接入 `sync_state` 的 running/ok/failed 生命周期。`tests/scenarios/test_s4_sync_interruption.py` 用子进程 `os._exit()` 安全模拟 kill，父进程执行启动恢复后断言 `status=failed`、`error` 含“中断”、中断期目标表 0 行、重跑后行数完整且 `(symbol,date)` 无重复。
- **S3/S4 定向命令**：`bash run_tests.sh -q tests/scenarios/test_s3_adjustment_chain.py tests/scenarios/test_s4_sync_interruption.py tests/test_pipeline_capacity.py tests/test_ch_writer.py tests/storage/test_b3_store.py::test_sync_state_transitions_keep_last_ok tests/storage/test_b3_store.py::test_sync_state_recovers_interrupted` → **32 passed**。
- **13 类矩阵近似集合 + 场景命令**：`bash run_tests.sh -q tests/storage tests/scenarios tests/test_repository_index.py tests/test_shared_matrix_cache.py tests/backtest/test_composite_backtest_e2e.py` → **85 passed**；覆盖 storage 契约、S1/S7/S8/S9 场景、ETF/指数、matrix cache、backtest、sync_state 等矩阵类。
- **结论**：CH-06 已按 DEP-001 原口径补齐 S3 `adjust` 双响应与 S4 进程级 kill→重启→failed→重跑自动化，结合 13 类矩阵近似集合实测，记为**已闭环**；页面双栈对照（CH-07）与满量性能（CH-08）仍是本轮剩余项。
