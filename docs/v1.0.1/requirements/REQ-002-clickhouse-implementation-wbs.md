# ClickHouse 存储层：开发内容分解（WBS）

> 文档类型：需求 | 编号：REQ-002 | 状态：主体已实现，B4/B5 尾巴与 B6 验收未闭环 | 文档版本：v1.2 | 最后更新：2026-11-19
> 负责人：— | 关联：REQ-001、ISS-002、DES-002、ISS-005（未闭环项汇总）
> 归档来源：base_v1.0 基线文档，按 DOC-STANDARD v1.0 归类命名；批次历史与实测数字保留原样。

> 配套：[设计文档](../design/DES-001-clickhouse-storage-design.md) · [验收文档](../deploy/DEP-001-clickhouse-acceptance.md)
> 状态（2026-11-19）：B1/B2/B3 已交付；B4 主路径与 B5 在线读写主体已 CK-only（§13.16）；**B4 归零、B5 剩余本地路径、B6 人工/环境验收仍未完成，需求不改判为完成**。历史 §13.1~§13.15 的失败数递减过程是阶段实测证据，不覆盖为现值；当前基线与未闭环清单见 §13.16 与 ISS-005。
> 估算口径：单人开发、熟悉本代码库的前提下，单位为人天（含自测），不含评审等待。

## 1. 前置约定

| 项 | 约定 |
| --- | --- |
| 分支 | 沿用 `feature/custom_tag_v0.3.37`（**2026-11-16 决策**：不新建 `feat/clickhouse-storage`；切换后该分支必须带 CK 运行，回退仍只靠 git） |
| 回退 | 依赖 git 分支，不留运行时开关（设计文档 §1） |
| 数据 | 不迁移，切换后从数据源重新同步 |
| 提交粒度 | 一个任务一个提交；每批次结束打 tag（`ch-b1`…`ch-b6`）用于二分定位 |
| 禁止事项 | 改造期间不新增"断言 Parquet 文件存在"的测试（验收文档 §14.2） |
| 校准基准 | `tick-stock-panel-origin`（旧栈，端口 4018/4011）保持只读，仅用于双栈对照（验收文档 §16）；不在其中改代码、不接 CK |
| 范围裁剪 | **2026-11-16 决策**：① CI 不接 CK（该 CI 流程已废弃，验收 §11 不再以 CI 绿为 gating）；② 桌面版（PyInstaller）不发布、不为其设计 CK 交付方案；③ "清空数据"接口暂不处理 CK 残留（后续按需实现，届时需同时清 CK 表）；④ 生产 compose 暂不做联调（仅保证 `docker compose config` 合法） |
| 流程决策（2026-11-16 第二轮） | ⑤ **全量改动不中途提交**：整个需求开发完成后一次性提交，期间不做批次 tag（覆盖本文 §1 的"一个任务一个提交 + 批次 tag"约定，改造期间以工作区快照为准）；⑥ 读路径**一次性切换**，不分表渐进；⑦ L3.5/L3.6/L4/L5 测试基线与场景测试**后续排期**，当前以功能开发为先；⑧ 既有测试迁移（§T5.4）在功能开发完成后再逐步进行，迁移期允许 `DataStore(tmp_path)` 用例失败；⑨ 验收记录（§15）按需补充；⑩ 设计文档 D3/D4 待定项与分支回滚风险本次不处理 |

## 2. 里程碑总览

| 里程碑 | 含义 | 对应批次 | 判断标准 | 状态（2026-11-16） |
| --- | --- | --- | --- | --- |
| **M1 底座可用** | CK 起得来、表建好、能写能读 | B1 | `pytest tests/storage -q` 绿；CK 重启数据保留 | ✅ 达成 |
| **M2 主链路通** | 标的+日K+enriched+除权 全走 CK | B2 | 场景 S1/S2/S3 通过；看盘页正常 | ✅ 达成 |
| **M3 全功能通** | 分钟/财务/盘口/扩展/回测/矩阵缓存 全通 | B3 | 数据集矩阵 §5 全绿 | ✅ 达成 |
| **M4 无 Parquet 残留** | 35 个散点全部收敛，DuckDB 依赖删除 | B4+B5 | `grep read_parquet app/` 为 0 | 🟡 B4 进行中（见 §13） |
| **M5 验收通过** | 全场景 + 性能 + 观察期 | B6 | 验收文档 §11 六条 gating 全满足 | ⬜ 未开始 |

## 3. B1 基础设施（M1）

**目标**：CK 容器可跑、schema 可建、客户端可连通、测试可隔离。**不改任何业务代码**（风险为零）。

| ID | 任务 | 主要文件 | 产出 | 依赖 | 估算 |
| --- | --- | --- | --- | --- | --- |
| T1.1 | **CK 服务加入 compose**（设计文档 §4.8 最终形态：`clickhouse/clickhouse-server:24.8.8-alpine` 容器 + `deploy/clickhouse/{init,config.d}`，账号 `tsp/tsp123456`；曾短暂改为复用宿主 CK，已回滚为容器化） | `docker-compose.yml`、`docker-compose.dev.yml`、`deploy/clickhouse/` | 容器可直连 `http://clickhouse:8123` | — | 0.5 |
| T1.2 | CK 客户端封装（连接、超时、重试、Arrow→Polars） | `backend/app/storage/clickhouse.py` | `ClickHouseClient` 类 | T1.1 | 1.5 |
| T1.3 | Schema 定义与初始化（22 表 DDL + `schema_version` + 幂等 ensure） | `backend/app/storage/schema.py` | `ensure_schema()` 可重复执行 | T1.2 | 2 |
| T1.4 | 测试 fixture：表名前缀隔离 + 无 CK 时 skip | `backend/tests/storage/conftest.py` | `store` fixture | T1.3 | 1 |
| T1.5 | 冒烟测试：建表/写入/读回/时区/去重 | `backend/tests/storage/test_smoke.py` | 6~8 个用例 | T1.4 | 1 |
| T1.6 | 健康检查接入 CK ping | `backend/app/api/routes.py`、`app/storage/runtime.py` | `/api/health` 含 CK 状态 | T1.2 | 0.5 |

**DoD**
- [ ] `uv run pytest tests/storage -q` 通过
- [ ] CK 重启后数据保留（宿主实例数据在 `/var/lib/clickhouse`；重启属共用实例操作，未在本批次执行）
- [x] 时区专项（验收 §4.6）第一条通过（容器 `TZ=Asia/Shanghai` 下 09:35 写入→09:35 读回，且库内瞬时值 = 北京 09:35）
- [x] 时区专项第二条通过（`TZ=UTC` 下整套 `tests/storage` 重跑 7 passed，无 ±8h 漂移；尚未固化为自动化子进程用例）
- [x] 前端、现有服务无任何行为变化（本批次未接线到启动流程）
- [x] T1.6 `/api/health` 接入 CK 状态（`storage_health()`；未初始化报 `not_initialized`，接线后报 ok/degraded）

**风险**：DDL 细节（`Map`/`LowCardinality`/分区键）在实测中可能调整 → 预留 1 天返工。

## 4. B2 存储门面与主干数据集（M2）

**目标**：把 instruments / daily / enriched / index / etf / adj_factor 六类数据的读写切到 CK，页面主流程可用。

**进度（2026-11-15）**：T2.1~T2.5、**T2.9** 已完成（`storage/protocol.py`、`store.py`、`writer.py`、`cache.py`、
`sync_state.py`、`runtime.py`、`scripts/cmp_stacks.py`；L2 契约 20 例、写入缓冲 10 例、缓存 7 例、对照工具 14 例）。
T2.6（repository 切换）/ T2.8（首次同步）未开始，阻塞点见下。

| ID | 任务 | 主要文件 | 产出 | 依赖 | 估算 |
| --- | --- | --- | --- | --- | --- |
| T2.1 | `StorageProtocol` 接口定义（读写分组、返回 Polars） | `backend/app/storage/protocol.py` | Protocol | T1.3 | 1 |
| T2.2 | `ClickHouseStore` 读写实现（含 `FINAL`/`argMax` 双策略） | `backend/app/storage/store.py` | 实现类 | T2.1 | 3 |
| T2.3 | 写入缓冲（时间/行数阈值、flush、失败重试、退出 flush） | `backend/app/storage/writer.py` | `BufferedWriter` | T2.2 | 2 |
| T2.4 | `CacheBackend` 接口 + `MemoryCacheBackend`（generation 语义） | `backend/app/storage/cache.py` | 接口 + 实现 | T2.1 | 2 |
| T2.5 | `sync_state` 接入（替代 preferences 时间 + publishing 标记） | `backend/app/storage/sync_state.py`、`enriched_generation.py` | 状态读写；旧标记删除 | T2.2 | 1.5 |
| T2.6 | 六类数据集映射（读写 + 去重 + 复用现有 `_write_*` 语义） | `backend/app/tickflow/repository.py`（改造） | 仓储内部换实现 | T2.2/2.3/2.4 | 4 |
| T2.7 | 契约测试（§4.1~4.4 全部用例） | `backend/tests/storage/test_*.py` | 30+ 用例 | T2.6 | 3 |
| T2.8 | 首次同步跑通（标的→日K→除权）+ K线/看盘页验证 | — | 手工验证记录 | T2.6 | 1 |
| T2.9 | **双栈对照工具**（`scripts/cmp_stacks.py`）+ 主干对照报告 | `scripts/cmp_stacks.py` | 差异报告 `reports/cmp_b2.json` | T2.6 + origin 运行中 | 2 |

**DoD**
- [ ] 场景 S1（部分：标的/日K/自选）、S2（**四方**交叉：dev / CK / origin / 手算）、S3（复权链路）通过
- [ ] `reports/cmp_b2.json` 无未解释差异（dev vs origin，验收文档 §16）
- [ ] 看盘主页、K线图、指数页正常
- [ ] 写入缓冲在进程退出时正确 flush（kill 前数据不丢）
- [ ] 契约测试覆盖 §4.1~4.4 全部条目

**并行机会**：T2.4（缓存抽象）与 T2.2/T2.3 无强依赖，可并行；T2.5 可与 T2.6 交叉进行。

**2026-11-15 进展（用户决策：当前分支 / 策略 B / 不许脚本造数）**

- 启动接线完成：`_application_lifespan` 调用 `init_storage()`（连接 → 建表 → 恢复中断任务 → 写缓冲），
  shutdown 末尾 `shutdown_storage()` 先 flush 再断连；实测服务启动后 `/health` 返回
  `clickhouse.status=ok`，`tsp` 库 26 张表齐备、`schema_version=1`
- 测试策略改为 B：`tests/conftest.py` 强制测试连 `tsp_test`，`tests/storage/conftest.py` 连不上即 `fail`
- 顺带修复测试收集：新增 `backend/tests/__init__.py`，解决 venv 里 `vectorbt` 自带顶层 `tests` 包
  遮蔽 `backend/tests` 导致 **90 个用例 collection error** 的问题
- 全量回归：`4776 passed / 26 failed / 1 error`，对比改动前基线 **无新增失败**
  （26 个失败为 watchlist/quote 既有问题，与本批次无交集）

**2026-11-15 第二段：instruments 已切 CK（T2.6 第一个数据集）**

- 读写：`KlineRepository(storage=...)` 注入门面；`get/save_instruments`、index/etf 三个变体全部走 CK，
  未注入时回落进程级 `runtime`，两者都没有直接报错（不回退 Parquet）
- 视图：`refresh_table_from_frame()` 用 CK 读出的 DataFrame 在 DuckDB 物化**同名表**
  （`instruments` / `instruments_index` / `instruments_etf`），裸 SQL 与 unified 视图零改动
- 写入侧：`instrument_sync.sync_instruments` 写 CK；`_preserve_instrument_fields` 从 CK 读历史补字段
- 直读 parquet 的 5 处调用方全部改走 `load_instruments()`：
  `kline_sync`、`daily_pipeline`、`api/kline`、`api/ext_data`、`custom/assistant/tools`
- 端到端实测（走真实同步，非脚本造数）：`sync_instruments` 拉到 **5892** 只 → 写入 CK →
  `repo.get_instruments()` 读回 5892 行、13 列契约列齐全
- 全量回归：`4719 passed / 81 failed / 3 errors`。**新增 55 处失败全部是"测试直接写
  instruments parquet 再读"的用例**，分布：`test_instrument_sync_share_fields`(12)、
  `backtest/test_matrix_ext_fields`(9)、`backtest/test_worker_process`(8)、`backtest/test_evidence`(8)、
  `test_repository_index`(4)、`test_kline_sync_bare_symbols`(4) 等 —— 属 T5.4 的测试迁移范围

**2026-11-16 第二段：回填 + 双栈对照 + 日期边界切 CK**

- 新增 `backend/scripts/backfill_ck.py`：把 Parquet 日K/除权一次性回填 CK（幂等、可分表、可 `--dry-run`）；
  实测 `kline_daily` 241 分区/1,326,065 行、`kline_daily_enriched` 同量、`kline_index_daily` 144,072 行、`adj_factor` 4,938 行，0 失败
- 双栈对照（T2.9 工具，报告落 `backend/reports/`）：`kline_daily`、`kline_daily_enriched`、`kline_index_daily`
  在 2026-06 区间 **逐值一致**（0 差异行 / 0 差异列），证明 CK 数据与 Parquet 语义等价（含 24.8 的 Date→UInt16 还原）
- 读路径第一块：`earliest_daily_date` / `latest_daily_date` / `latest_enriched_date` 改走 CK 聚合
  （`_daily_bound`），不再扫描 Parquet 分区；实测边界 2025-11-17 ~ 2026-11-13 与旧实现一致
- 双写落盘顺序修正：统一改为"Parquet 成功后再镜像 CK"，消除"CK 有、Parquet 无"的脏数据
  （实测清理了 5 个退市占位标的的 249 行残留）
- CK 数据目录独立为 `./ckdata`（不被"清空数据"/备份/升级搬迁波及），内存上限 1.5 GiB → 3.5 GB + `mem_limit: 4g`
  （1.5 GiB 在回填与聚合并发时触发 `MEMORY_LIMIT_EXCEEDED`）
- 全量回归：`64 failed / 4750 passed`，失败项仍全部是 CK 迁移未收尾的既有用例（`get_instruments` 等依赖 storage 注入）

**2026-11-16：日K/除权过渡期双写（读路径切换的前置，设计文档 §8.4）**

- 写：6 个日K写入口（`_write_daily_partition` / `merge_live_daily_asset` / `merge_live_enriched_asset` /
  `flush_live_daily_asset` / `flush_live_enriched_asset` / `replace_daily_rows`）与 `sync_adj_factor`
  在写 Parquet 之后镜像写 CK；CK 未初始化时降级跳过，既有 Parquet 链路不受影响
- `replace_daily_rows`：CK 侧先 `ALTER TABLE ... DELETE`（`mutations_sync=2`）再写回补数据
- 新增用例：`tests/storage/test_daily_mirror.py`（7 例：写入/收敛/列裁剪/修补删除/降级）
- 回归：`tests/storage` + 日K/除权相关 **89 passed**，失败项与改动前基线一致
- **未做**：读路径仍是 DuckDB 视图（`execute_all/execute_one` 30 处）、enriched 生成仍在读 Parquet、
  `EnrichedPublication` 未删除 —— 这三项合起来才是 T2.6 的完成标志

**2026-11-16 第三段：日K/enriched 读路径一次性切 CK（Phase 2 主体）**

- 读入口收口：新增 `_read_daily_table()` / `_read_enriched()`，四个 `_scan_*` 方法
  （`_scan_daily_symbol` / `_scan_daily_batch` / `_scan_index_daily_symbol` / `_scan_etf_daily_symbol`）
  全部改走 `ClickHouseStore.read_daily`，不再扫 Parquet
- enriched 缓存链路切 CK：`_refresh_enriched_impl`（最新日 + 近 300 天窗口）、
  `_restore_missing_latest_rows`、`_build_live_agg_from_storage`（原 `_build_live_agg_from_parquet`）、
  `_build_live_agg` 连板回退、`_refresh_etf_enriched`、`_refresh_index_enriched`、
  `_latest_enriched_date`（原 `_latest_enriched_date_duckdb`）、`symbols_lagging` 全部改 CK
- 清理：删除 `repository.py` 里的 4 个 enriched/instruments parquet glob 与
  `scan_enriched_parquet` 依赖（分钟 glob 保留，B3.1 范围）
- **修复 `backfill_ck.py` 表名错误**：原写 `kline_index_daily_enriched` / `kline_etf_daily_enriched`
  两个不存在的表，导致指数/ETF enriched 从未被回填（实测 CK 仅 6 行 / Parquet 241 分区）；
  同时新增标的维表（`instruments*`）回填——维表读路径早已切 CK，但 CK 侧为空（Parquet 5892/599 行）
- 回填结果：`kline_index_enriched` 241 分区 / 144,072 行、`instruments` 5892 行、`instruments_index` 599 行
- 实测（真实 CK + 真实数据）：`get_daily` 27 行 / 74 列（含 `limit_up_price`）、
  快路径 0.01s、`get_index_daily` 27 行、300 天窗口 109 万行 0.8s、
  enriched 启动缓存 3.1s（5571 只 × 69 列、历史 109 万行、live_agg 5586 行）、
  `kline_daily_enriched` 与 Parquet 同日 close 逐值一致（差异 0.0）
- `tests/storage` + 5 个 `test_ch_*` + `test_cmp_stacks` 共 102 例通过
- **未做**：enriched 生成管道（`indicators/pipeline.py` 写侧）仍读 Parquet（T4.1）；
  `EnrichedPublication` 未删除（随写路径切换处理）；DuckDB 视图仍给裸 SQL 供数（B4）；
  分钟/财务/盘口/扩展/回测仍是 Parquet（B3）；app 内直接 `read_parquet` 文件数 47（B4）
- **已知既有问题（非本次引入）**：`data/financials/shares` 为空时 `compute_limit_signals`
  对空 instruments 报 `unable to find column "symbol"`，on-demand 计算会丢掉涨跌停信号；
  属 B3.2 财务切片范围

**2026-11-16 第四段：enriched 生成管道切 CK（T2.6 收尾）+ 分钟接线（T3.1）**

- `indicators/pipeline.py` 全部数据源改 CK（不再 scan Parquet）：
  `_daily_store()` 统一取存储门面（未初始化直接报错，不静默算空表）；instruments 走
  `read_instruments`；复权因子走 `read_adj_factor`；全量/增量/局部重算的原始日K走
  `read_daily`；增量模式的历史前缀走 `read_daily("kline_daily_enriched")`；
  基准指数动量 `load_benchmark_momentum` 走 `read_daily("kline_index_daily")`；
  `recompute_daily_symbols` 同样改 CK。日期集合判断改 `list_daily_dates`（差集算"新日期"）
- 存储门面新增三个读能力并同步 `StorageProtocol`：`list_daily_dates`、`minute_bounds`、
  `list_minute_dates`
- **`EnrichedPublication` 增加 CK 镜像**：管道只 `publication.write_parquet` 写 Parquet，
  读路径切 CK 后新增指标在页面永远不可见（实测 `run_pipeline(new_dates_only)` 写入 2 天，
  CK 日期数不增）。镜像放在发布对象的落盘钩子里，覆盖全部 6 个发布点（管道新增/局部/全量
  合并、`replace_enriched_rows`、`_optimistic_upsert_partition`），表名由落盘路径反推
- 分钟（T3.1）：`repository` 的 `get_minute` / `get_minute_batch` / `get_minute_range` /
  `get_minute_by_dates` 与 `latest_minute_date*` / `earliest_minute_date` / `list_minute_dates`
  全部切 CK；`kline_sync._write_minute_partition` 落盘后镜像 `incoming` 行到 CK；
  `_latest/_earliest_minute_datetime` 改 `repo.minute_bounds`；`api/data.py` 分钟统计从
  "数分区目录"改为 CK 交易日集合；删除 `_minute_glob` / `_minute_partition_path` 及
  `guarded_collect` 依赖
- 实测（真实 CK + 造数）：
  - 增量管道补齐 2 个缺失日 → CK `kline_daily_enriched` 日期数 248→250，与新写入口径一致
  - enriched 启动缓存 4.3s / 5571 只 / 109 万行历史；`get_daily` 74 列
  - 分钟造数 2 票 × 3 天：单票/批量/区间/按日期四种读法行数与列正确，
    `latest/earliest/list_minute_dates` 正确；`_write_minute_partition` 写 Parquet 后
    CK 立即可读（镜像生效）；测试数据与 CK 行已清理
  - `tests/storage` 49 例通过
- 回填工具补分钟表：`backfill_ck.py --tables kline_minute` / `kline_etf_minute` 按 `date=`
  分区读、按 500 只 symbol 分块写入（单日百万行量级不会一次性打进 CK）；造数实测
  「1 分区 / 3 行 / 失败 0」→ CK 立即可读
- **未做（下一步）**：财务（T3.2）、盘口（T3.3）、扩展（T3.4）、回测结果与矩阵缓存
  （T3.5/T3.6）、CK 联合视图（T3.7）；`minute_adjust` 的因子读取与 DuckDB 分钟视图仍读
  Parquet（双写期一致，B4 收敛）；`EnrichedPublication` **不删除**——它与 Parquet 分区的
  原子替换/孤儿标记绑定，只有 B5 删除 Parquet 写入后才可退场（详见设计 §8.3）

**2026-11-16 第五段：财务（T3.2）+ 五档盘口（T3.3）+ 扩展数据写入通道（T3.4）**

- T3.2 财务（读切 CK + 写双写）
  - `get_financial_df()` 改 `store.read_financials("financials_<kind>")`（Map 列已展开）；
    所有读取方（`api/financials` 5 个接口、`stock_analyzer`、`financial_analyzer`、
    `custom/assistant/tools`）都经此单点，一并切换
  - `_write_table()` 落 Parquet 后镜像 `store.write_financials`；缺 `period_end` 主键时拒绝写入并告警
  - `_get_symbols()` 改 `load_instruments()`（维表 CK 唯一读源）
  - `api/financials.financial_status` 从"数 Parquet 行"改 `store.financials_stats`
  - `share_capital.load_share_history()` 改走 `get_financial_df("shares")`，
    涨跌停信号的历史股本与财务表同源
  - 存储门面新增 `financials_stats(table)`；回填工具新增 `financials_*` 五表
  - 实测（造数）：宽表写入 → 读回列/数值/日期正确（`revenue=1000.5`、`period_end=2025-09-30`、
    文本列 `source` 走 `raw` 不丢），`financials_stats` 与清理后归零一致
- T3.3 五档盘口（读切 CK + 写双写）
  - `_read_from_parquet` / `_restore_from_parquet` 改 `store.read_depth5(start=end=day)`；
    `_persisted_for_date` 改 `store.depth5_has(day)`；`_persist` 落盘后镜像
    `store.write_depth5`（补 `date` 列）
  - 内存缓存的 `fetched_ts` 与 CK 列 `fetched_at` 映射对齐
  - 回填工具新增 `depth5`（分区名补 `date`）
  - 实测（造数）：`depth5_has` 真/假正确；涨/跌两个方向 `sealed`/封单量映射正确
    （涨→`bid1_vol`、跌→`ask1_vol`）；`_restore_from_parquet` 恢复缓存 2 条且 `fetched_ts` 正确
- T3.4 扩展数据（只开写入通道，读路径**暂不切**）
  - `write_ext_parquet()` 落盘后镜像 `store.write_ext_timeseries`：
    `ext_id`/`date`（快照用落盘日）/`time_value`（`pull.time_field` 或 `time|trade_time|datetime`）
  - 回填工具新增 `ext_timeseries`，按扩展配置遍历 snapshot 与 `timeseries/date=*`
  - 实测：2 个扩展源回填 **11,162 行**（`ext_gn_ths` 5582 / `ext_hy_ths` 5580，
    各 5582/5580 只标的），失败 0
  - **读路径暂不切的三条硬理由**（需决策后再动）：
    ① `market_level` 扩展（无 symbol）无法用主键 `(ext_id, symbol, date, time_value)` 表达；
    ② `values` 是 `Map(String, Float64)`，整数列往返会变成浮点（前端/因子精度漂移）；
    ③ 消费方有 12+ 处（screener / overview / watchlist / rps_rotation / sector_rotation /
    market_overview_builder / ext_pages / ext_factors），属 B4 统一收敛范围
- 验证：`tests/storage` + 盘口/财务 provider + 扩展写入相关共 **133 例通过**

**2026-11-16 第六段：回测结果（T3.5）+ 矩阵缓存（T3.6）+ 联合视图（T3.7）**

> 口径统一：**CK 是唯一读源**，不再保留 Parquet 回退分支；存储未初始化即降级为空并告警（fail-closed）。

- T3.5 回测结果
  - `BacktestService._persist` 改写 `backtest_runs` + `backtest_trades`（`extra_json` 存
    `per_symbol_stats`），删除 `data/backtest_results/*.parquet` 汇总
  - `get_result()` 从空壳改为从 CK 读回完整 `BacktestResult`（config/stats/equity/trades/per_symbol）
  - 修复 `_json_safe` 对 dict/list 直接 `str()` 的双重编码（读回变字符串），改为递归
  - 新增 `_trades_to_frame`：日期/数值按显式 schema 归一（ISO 字符串也能解析）
  - 实测（造数往返）：run 落库、2 条 trades 按 `seq` 有序读回、`equity_curve` 2 点、
    `per_symbol_stats` 对象形态完好，清理后 `get_result` 返回 None
- T3.6 矩阵缓存
  - 缓存从"本地目录即唯一存储"升级为"CK 持久层 + 本地工作区"：
    - 上传：缓存目录 zip → `matrix_cache.payload`（超预算只留本地）；同时写一行
      `<key>:manifest`（`{"manifest": 原文, "body_size": N}`），命中判别只读这一行
    - 索引：`store.list_matrix_cache(prefix)` 返回 `{key: (manifest, body_size)}`，
      `_sync_matrix_cache_index` 只把 manifest 落到本地（几 KB），不下载矩阵本体
    - 命中：`_load_market_data_matrix_cache` 发现 `matrix.bin` 缺失 → 按 key 下载解压
      （zip 成员路径做穿越校验），memmap 仍读真实文件
    - 过期：表级 TTL `toDate(accessed_at)+30d`，与本地 LRU 清理互补
  - 实测：上传 → 索引 → 删本地目录 → 同步只留 manifest → 命中后还原 `matrix.bin` 2048 字节
- T3.7 CK 联合视图
  - 新增视图 `v_screener_base`：`kline_daily_enriched ⋈ instruments`，两侧 `FINAL`
    （否则 ReplacingMergeTree 未合并版本会把行数放大：实测 5571 → 10787）
  - `store.screener_base(day, symbols=...)` 直接查视图，省掉两表各读一次 + 内存 join
  - `screener` 5 处 Parquet 慢路径全部切 CK：`_load_enriched_for_date`、
    `_compute_enriched_full` warmup、`_load_enriched_history`、`load_prior_consecutive`
    （窄读两列）、`_prior_partition_dates`（`list_daily_dates`）
  - `BacktestService._load_panel` 改 `store.read_daily(enriched)`（原来 `scan_enriched_parquet`）
  - 实测：视图 5571 行 = enriched 行数，维表字段 5571/5571 命中，单票过滤正常；
    screener 窗口读 5 天 27864 行、前一日枚举 3 天、连板窄读 5573 行；
    回测面板 38 行 × 20 列（含 rsi_14/signal_macd_golden）
- 已知：`tests/test_screener_jit_turnover.py` 2 例用 `tmp_path` 造 Parquet 且不初始化
  存储，慢路径 fail-closed 后必然失败 —— 属测试待迁移（造数需写 CK）

**2026-11-16 B4 第一批：生成/自愈 + 回测因子 + 展示侧（T4.1 全量、T4.2/T4.4 部分）**

- T4.1 生成与自愈（新增 `store.delete_daily_date`）
  - `daily_pipeline._prune_partial_enriched_partitions` /
    `_prune_stale_price_partitions` 从"枚举 `date=` 目录 + `shutil.rmtree`"改为
    `list_daily_dates` + `read_daily` + `delete_daily_date`
  - `compute_enriched` 的日期集合/计数全部改 `list_daily_dates`，不再 glob 目录
  - **enriched 写侧首次接入 CK**：新增 `_upsert_enriched_day`（按 symbol 覆写或整日替换），
    局部/除权重算/全量重建三条路径都先写 CK，再从 CK 读回整日镜像到 Parquet
    （B5 前保留 Parquet 写侧镜像）；`indicators/pipeline.py` 两处 read-modify-write 消失
  - `extend_history` 标的池改 `load_instruments`；`minute_refresh` 覆盖度探测改
    `store.minute_bounds`
  - 实测：单日 5571 行按 3 只票覆写后行数不变、目标票值更新、其余票不受影响、
    还原后逐位一致
  - 迁移测试：新增 `tests/storage/test_enriched_stale_price_prune.py`（4 例，CK fixture），
    删除旧 Parquet 版
- T4.2 回测与挖掘（部分）
  - `backtest/fundamentals.load_fundamental_snapshot` 改 `read_financials("financials_metrics")`
  - `strategy/market_data.get_ext_timeseries` 改 `read_ext_timeseries`（`time_value→ts` 映射）
  - 待攻：`backtest/matrix.py` 的 `pads.dataset(parquet_root)`（矩阵构建本体，非
    `read_parquet` 关键字，需换 InMemoryDataset/CK 扫描）
- T4.4 展示/监控侧（部分）
  - `auction_benchmark`：交易日枚举 + 单日收盘/开盘读取全改 CK
  - `sector_rotation`：前收（`list_daily_dates` + `read_daily`）+ 当日分钟（`read_minute`）
  - `market_mainline`：ST 名单改 `load_instruments`；涨停行改 `read_daily(enriched)`
  - `regime_builder`：慢路径改 `read_daily(enriched)`
- B4 剩余（下批）
  - `tickflow/repository.py`（34 处，DuckDB 视图主体，随 B5 删除）、`api/ext_data.py`（11）、
    `kline_sync`（6，写入侧 read-modify-write）、`pools`/`watchlist`/`index_sync`/
    `minute_adjust`/`minute_gaps`/`data_integrity`/`ext_data`/`plugins/fuyao`
  - 派生结果缓存（无对应 CK 表）：`market_mainline` 历史、`regime` 历史、
    `market_overview_builder`/`sector_monitor` 的 ext 快照 —— 需先定表结构
**2026-11-16 B4 第二批：写入侧 + ext 消费方**

- 写入侧 read-modify-write 消除
  - `kline_sync._merge_adj_factor_store` → 存量读 `read_adj_factor`、新行 `write_adj_factor`
    （ReplacingMergeTree 天然 upsert），`all.parquet` 仅镜像；存储未初始化时退回本地合并（离线脚本）
  - `kline_sync._write_minute_partition` → 已存在行改 `_read_minute_day`（CK），
    Parquet 从 CK 读回镜像；新增 `_read_minute_day` 带本地兜底
  - `minute_adjust._load_factor_frames` → `read_adj_factor`，缓存签名改行数
- 分钟检视
  - `minute_gaps._daily_active_symbols` / `_read_minute_activity` 改 `read_daily` / `read_minute`
    （表名/日期由入参 Path 反推，调用方零改动）
- ext 消费方
  - `api/ext_data`：`_read_ext_dataframe` 的 timeseries 分支改
    `list_ext_dates` + `read_ext_timeseries`（新增 `_normalize_ext_dates`：去 ext_id、
    `time_value→ts`、date 转字符串补 data_date）；`_date_range`、`_plate_stats`、
    `_prev_daily_close`、维度日内分钟全部改 CK
  - `market_overview_builder._read_ext_rows`（timeseries 分支，缓存签名改 CK 日期）
  - `factors/ext_factors._timeseries_frame`（整表读 + `_ext_date` 由 date 列生成）
  - 新增 `store.list_ext_dates(ext_id)`
- 本批测试新增债务：`test_assistant_ext_tables`(2)、`test_ext_backfill`(1)、
  `test_etf_minute_persistence`(5)、`test_adj_factor_standalone_sync`(6) —— 均因
  `tmp_path` 造 Parquet/fake repo 未接 CK
- 仍未改（B4 第三批）：`tickflow/repository.py`(34)、`api/ext_data` 的 DuckDB
  `DESCRIBE read_parquet`(3 处) 与 snapshot 分支、`api/overview`、`api/data`、
  `sector_monitor`、`sector_rotation` 的 ext snapshot、`ext_pull`、`quote_service`
  因子文件、`pools/watchlist/data_integrity/index_sync`、`matrix.py` 的 pyarrow dataset

- 测试影响（待 B5.4 迁移）：`test_enriched_partial_partition`(20)、
  `test_enriched_full_rebuild`(6)、`test_enriched_generation`(4)、
  `test_enriched_incremental_memory`(3)、`test_screener_jit_turnover`(2)、
  `test_daily_pipeline_minute_gate`(2) —— 均为 `tmp_path` 造 Parquet 或未 `init_storage`
- 遗留：验证时向 `data/kline_minute/date=2020-01-09/part.parquet` 写入过一笔造数，
  容器内 root 权限导致工具侧删除被拒，需手动删除该目录

**T2.6 阻塞点（2026-11-15 实测）**：
1. 运行时凭据缺失——`tsp` 用户密码未落 `.env`，接线后应用按设计 fail-closed 将无法启动（L2 一直用 `default` 账号验证）
2. 影响面实测——`save_instruments/get_instruments` 被 **74 个测试文件**引用、**40 个 app 文件**依赖；切换必须同步把这些测试改成"注入 store / 无 CK 时 skip"，属 T5.4 的 4 天工作量
3. 分支——WBS §1 要求 `feat/clickhouse-storage`，当前工作区在 `feature/custom_tag_v0.3.37` 上，切换后此分支将不可运行（无 CK 数据）

## 5. B3 剩余数据集（M3）

**目标**：分钟、财务、盘口、扩展、回测结果、矩阵缓存全部切 CK，数据集矩阵全绿。

| ID | 任务 | 关键点 | 依赖 | 估算 |
| --- | --- | --- | --- | --- |
| T3.1 | 分钟数据（stock/etf） | 月分区、批量写、时区、跨午休 | B2 | 2 |
| T3.2 | 财务 5 表（动态列 `Map` + 宽表展开） | `mapGet` 适配 `backtest/fundamentals.py` | B2 | 3 |
| T3.3 | `depth5`（sealed 真假涨停） | 三态 Nullable(Bool) 保真 | B2 | 1 |
| T3.4 | 扩展数据 `ext_timeseries`/`ext_instruments` | 多扩展并存、日内多盘 `time_value` | B2 | 2.5 |
| T3.5 | `matrix_cache`（Arrow IPC + LRU + `accessed_at` TTL） | 替代 `factors/materialize_cache.py` | B2 | 2 |
| T3.6 | `backtest_runs` + `backtest_trades` | 替代 `services/backtest.py::_persist` | B2 | 2 |
| T3.7 | 联合视图（`kline_daily_all` 等 4 个） | CK 视图 `UNION ALL` | T3.1 | 1 |
| T3.8 | 契约测试补充（§4.5 动态列）+ 数据集矩阵逐项验证 | — | T3.1~T3.6 | 2.5 |
| T3.9 | **全数据集双栈对照**（分钟/财务/盘口/扩展/回测结果）+ 差异判定 | `scripts/cmp_stacks.py` 扩展 | `reports/cmp_b3.json` | T3.1~T3.6、T2.9 | 2 |

**DoD**
- [ ] 数据集矩阵（验收 §5）13 类全部"写入→读回→API 正常"
- [ ] 分时图、财务页、连板梯队、扩展数据页、回测结果页正常
- [ ] 动态列展开的列顺序稳定

**并行机会**：T3.1~T3.6 **六项互不依赖**，可任意并行；建议按"分钟 → 财务 → 扩展 → 回测 → 矩阵缓存 → 盘口"的顺序（按业务价值排）。

## 6. B4 散点收敛（M4 上半）

**目标**：35 个直接 `read_parquet/scan_parquet` 的文件全部改走门面。这是**最机械但最容易漏**的一批。

**进度快照（截至 2026-11-16，详见文末 §13 交接说明）**

| 任务 | 状态 | 说明 |
| --- | --- | --- |
| T4.1 指标与 enriched 生成 | ✅ 完成 | `daily_pipeline` 自愈、`indicators/pipeline` 写侧 upsert、`extend_history`、`minute_refresh` |
| T4.2 回测与挖掘 | 🟡 部分 | `fundamentals`、`strategy/market_data` 已切；`matrix.py` 的 pyarrow dataset、`mining_candidates`、`ai_generator` 未改 |
| T4.3 同步/写入侧 | 🟡 部分 | `kline_sync`（日K staging 与分钟镜像已 CK 读、复权 upsert）、`minute_adjust`、`minute_gaps` 已切；`ext_pull`、`instrument_sync`、`index_sync`、`repair_daily`、`data_integrity`、`plugins/fuyao` 未改 |
| T4.4 展示/监控侧 | 🟡 部分 | `auction_benchmark`、`sector_rotation`、`market_mainline`、`regime_builder`、`market_overview_builder` 已切；`quote_service`、`sector_monitor` 未改 |
| T4.5 因子链路 | 🟡 部分 | `factors/ext_factors` 时序已切；`materialize_cache` 未接 T3.5 矩阵缓存 |
| T4.6 API 层 | 🟡 部分 | `api/ext_data` 的 timeseries/snapshot 读已切；3 处 DuckDB `DESCRIBE read_parquet`、`api/overview`、`api/data`、`api/paper`、`api/mining` 未改 |
| T4.7 选股器 SQL | ✅ 完成（B3 交付） | `v_screener_base` 联合视图 + 5 处慢路径 |
| T4.8 标的池缓存 | ❌ 未开始 | `tickflow/pools.py` |

> B4 的 DoD（grep 归零）**尚未达成**：当前 `app/` 下仍有 81 处真实调用（其中 34 处在 `tickflow/repository.py`，随 B5 删除）。

| ID | 任务 | 涉及文件（组） | 估算 |
| --- | --- | --- | --- |
| T4.1 | 指标与 enriched 生成 | `indicators/pipeline.py`、`enriched_generation.py`、`services/extend_history.py`、`services/minute_refresh.py` | 2 |
| T4.2 | 回测与挖掘 | `backtest/fundamentals.py`、`backtest/matrix.py`、`backtest/mining_runtime.py`、`strategy/market_data.py`、`strategy/ai_generator.py`、`services/mining_candidates.py` | 3 |
| T4.3 | 同步/写入侧服务 | `kline_sync.py`、`instrument_sync.py`、`financial_sync.py`、`ext_pull.py`、`index_sync.py`、`repair_daily.py`、`minute_adjust.py`、`minute_gaps.py`、`depth_service.py`、`data_integrity.py` | 4 |
| T4.4 | 展示/监控侧服务 | `quote_service.py`、`auction_benchmark.py`、`sector_monitor.py`、`sector_rotation.py`、`market_mainline.py`、`market_overview_builder.py`、`regime_builder.py` | 3 |
| T4.5 | 因子链路 | `factors/ext_factors.py`、`factors/materialize_cache.py`（接入 T3.5） | 1.5 |
| T4.6 | API 层 | `api/data.py`（磁盘统计 → `system.parts`）、`api/ext_data.py`（去 `DESCRIBE`） | 1.5 |
| T4.7 | 选股器 SQL 执行 + 白名单校验 | `services/screener.py`、`db_safe.py` | 1.5 |
| T4.8 | 标的池改走缓存 | `tickflow/pools.py` | 0.5 |

**DoD**
- [ ] `grep -rn "read_parquet\|scan_parquet" backend/app` 为 0
- [ ] 场景 S4~S9 全部通过
- [ ] 页面冒烟 §8 中 5/6/7/8/9/11 项通过

**注意**：每完成一个子组就跑一次全量 `pytest`，避免一次性改完难以定位回归。

## 7. B5 清理收尾（M4 下半）

| ID | 任务 | 内容 | 估算 |
| --- | --- | --- | --- |
| T5.1 | 删除 DuckDB 与 Parquet 依赖 | `pyproject.toml` 去 duckdb；删 `app/parquet.py` 的 scan 函数（保留类型常量）；删 DataStore 目录创建 | 1 |
| T5.2 | 删除 publishing 标记逻辑 | `enriched_generation.py` 精简为 generation；`repository.py` 的 commit/abandon 调用移除 | 1 |
| T5.3 | 删除遗留路径 | `instruments_ext`/`kline_ext` 兼容分支、`screener_results` 目录与统计引用 | 0.5 |
| T5.4 | 测试迁移清零 | 按验收 §14 分类处置，`DataStore(tmp_path)` 归零，skip 清零 | 4 |
| T5.5 | 文档更新 | README/deployment/configuration；两份 CH 文档状态改"已实现"并回填实测值 | 1 |
| T5.6 | 全量基线 | `pytest` + `ruff` + `pnpm build` 全绿，记录用例数 | 0.5 |

**DoD**
- [ ] 验收 §14.3 三条完成标志达成
- [ ] 无 DuckDB 依赖残留（`grep -rn duckdb backend/app` 仅剩 screener 相关注释或无）
- [ ] 冷启动（空库）→ 全量同步 → 页面可用，全流程走通

## 8. B6 验收与试运行（M5）

| ID | 任务 | 依据 | 估算 |
| --- | --- | --- | --- |
| T6.1 | 场景 S1~S9 全跑 | 验收 §7 | 2 |
| T6.2 | 性能冒烟标定并回填阈值表 | 验收 §9.1 | 1 |
| T6.3 | 真实数据源抽样验收（20 只 × 3 月等） | 验收 §13.2 | 1 |
| T6.4 | 回滚演练（记录重新同步耗时） | 验收 §11 第 7 条 | 0.5 |
| T6.5 | 连续 3 天观察（跨交易日、跨重启） | 验收 §11 | 3（挂机） |
| T6.6 | 填写验收记录 | 验收 §15 | 0.5 |
| T6.7 | **页面级双栈对照**（3011 vs 4011 逐项比对）+ 最终对照报告归档 | 验收 §16.5 | 1 |

## 9. 工作量汇总

| 批次 | 估算（人天） | 累计 |
| --- | --- | --- |
| B1 基础设施 | 6.5（+1 缓冲） | 7.5 |
| B2 门面与主干 | 19.5（含双栈对照工具） | 27 |
| B3 剩余数据集 | 18（含对照） | 45 |
| B4 散点收敛 | 17 | 62 |
| B5 清理 | 8 | 70 |
| B6 验收 | 9（含页面级对照） | 79 |

> 79 人天是**线性单人**估算。若 B3 的六项并行、B4 的分组并行，实际日历时间可压缩约 40%。估算不含联调意外与需求变更。
> 相比无对照方案增加约 5 人天，换取"与旧栈逐值对齐"的质量保障——这是硬切、数据不迁移方案里最值得的投入。

## 10. 关键路径

```text
T1.1 → T1.2 → T1.3 → T1.4 ─┬→ T2.1 → T2.2 → T2.6 → T2.7 → B3 全部 → B4 → B5 → B6
                            └→ T2.4（并行）
B3.1(minute) 与 B3.2(financial) 互不依赖，可并行
B4 的 8 组彼此独立，可并行
```

**最长链路**：T1.3（schema）→ T2.2（store）→ T2.6（主干映射）→ T3.2（财务动态列）→ T4.2（回测散点）→ T5.4（测试迁移）。

## 11. 风险缓冲

| 风险 | 缓冲安排 |
| --- | --- |
| DDL 需要反复调整（`Map`/分区/主键） | B1 预留 1 天；B3.2 预留 1 天 |
| projection 方案不达标（R6 横截面性能） | B2 结束前做一次横截面压测，不达标转"预聚合表"方案，预估 +3 天 |
| 动态列改造牵动回测（R4） | B3.2 与 T4.2 之间预留 2 天联调 |
| 测试迁移量超预期（402 个文件） | B5.4 预留 4 天，超支则允许分两次合入（先 C/D 类，再 B 类删除） |
| CK 版本特性差异（projection/TTL 语法） | B1 建表时锁版本号并记录到文档 |

## 12. 变更控制

- 任务范围变更（增/删/改）→ 更新本文表格，并在设计文档 §8 追加追溯记录
- 每批次结束 → 按验收文档 §15 填记录，tag 打点
- 不因"赶进度"跳过 DoD；DoD 未达成时批次不关闭

## 13. 交接说明（2026-11-16，B4 进行中）

> 新会话从这一节开始读即可。它回答三件事：**做到哪了 / 还剩什么 / 有哪些坑**。

### 13.1 一句话现状

B1、B2、B3 已交付并通过验收级验证；**B4 中 T4.1 与 T4.7 已完成，T4.2/T4.3/T4.4/T4.5/T4.6 只完成主路径、T4.8 未开始**，读路径已"CK 唯一读源、无 Parquet 回退"，但 `grep read_parquet` 尚未归零；测试侧积累约 **330 例待迁移用例**（不影响运行，但会让"全量绿"失真）。

关键数字（2026-11-16 实测）：

| 指标 | 数值 |
| --- | --- |
| 全量 pytest | `357 failed, 4456 passed, 7 skipped, 1 xfailed, 2 errors`（171s） |
| 改造前基线（`--ignore=tests/storage`） | `28 failed, 4739 passed` |
| 净新增失败 | **约 329 例**（160 例 `RuntimeError: 存储层尚未初始化`，其余为"读 CK 为空"断言） |
| `tests/storage` | **53 例全绿**（CK 集成，含新增自愈测试） |
| `app/` 内 `read_parquet/scan_parquet` | 81 处真实调用（34 处在 `tickflow/repository.py`，随 B5 删除） |
| 服务健康 | `3018`/`3011` 均返回 401（鉴权正常），启动无异常 |

### 13.2 已完成（按批次）

| 批次 | 内容 | 记录位置 |
| --- | --- | --- |
| B3 收口 | 回测结果落 `backtest_runs`/`backtest_trades`、矩阵缓存"CK 持久层 + 本地工作区"、`v_screener_base` 联合视图、screener 5 处慢路径 | 本文 §4 内嵌记录（第六段） |
| B4 第一批 | `daily_pipeline` 自愈（`list_daily_dates`+`delete_daily_date`）、`indicators/pipeline` 写侧 upsert（enriched 首次真正写入 CK）、`extend_history`、`minute_refresh`、`fundamentals`、`market_data`、`auction_benchmark`、`sector_rotation`、`market_mainline`、`regime_builder` | 本文 §4 内嵌记录（B4 第一批） |
| B4 第二批 | `kline_sync`（复权 upsert、分钟分区读 CK）、`minute_adjust` 因子、`minute_gaps`、`api/ext_data` timeseries 主路径、`market_overview_builder`、`factors/ext_factors`；新增 `store.list_ext_dates` | 本文 §4 内嵌记录（B4 第二批） |
| 本轮另修 | `tests/test_ch_schema.py` 7 例（`v_screener_base` 视图加入后未更新断言）→ 现 10 例全绿 | 本次会话修复 |

### 13.3 剩余工作（按建议顺序）

**① 派生结果缓存（需先定 schema，是 B4 唯一的"设计决策"缺口）**

这些不是数据源，而是"本地派生产物"，改 CK 需要先建表（`market_mainline`、`regime_history`、`screener_results`、`factor_cache` 等），否则只能保留本地文件：

- `services/market_mainline.py:109,259,271`（主线历史读 + 范围替换 upsert）
- `services/regime_builder.py:542`（市场状态历史）
- `services/sector_monitor.py:245`、`services/sector_rotation.py:232`（ext 快照/最新分区）
- `services/quote_service.py:2205`、`services/index_sync.py:343,359`（因子/复权 parquet）
- `factors/materialize_cache.py:151`、`factors/ext_factors.py:281`（快照分支）
- `services/watchlist.py:86,108`、`tickflow/pools.py:62,143`（自选/池缓存）
- `services/market_overview_builder.py:239,242`（ext 快照分支）

**② 数据源/写入侧（直接可改）**

- `services/ext_data.py:694,941,1083`（code→symbol、写入前读旧分区、回填）
- `services/ext_pull.py:182`、`services/repair_daily.py:119`
- `services/data_integrity.py:238,289,296`
- `plugins/fuyao/provider.py:257,364,1153,1163`（插件自带 dump，需判断是"数据源缓存"还是"CK 表"）
- `services/minute_adjust.py:180,251`（日K close 锚点、分钟分区）

**③ API 层**

- `api/ext_data.py:295,1507`（snapshot 分支）、`1684,1766,1797`（DuckDB `DESCRIBE read_parquet`，随 B5 删 DuckDB 一并处理）
- `api/overview.py:72,75`、`api/data.py:484`、`api/paper.py:80`、`api/mining.py:699`

**④ 回测与策略**

- `backtest/matrix.py` 的 `pads.dataset(parquet_root)`（**B4 最大单块**，非 `read_parquet` 关键字，需换 `InMemoryDataset` 或 CK 分批）
- `services/mining_candidates.py:327,354`、`strategy/ai_generator.py`（2）、`strategy/paper.py:914`、`backtest/engine.py`

**⑤ 不建议现在动的**

- `tickflow/repository.py`（34 处，DuckDB 视图主体）→ 随 **B5.1** 整体删除，逐点改是白做
- `services/kline_sync.py:1576,1592`（旧版分区迁移工具，一次性）→ 随 B5.3 删
- `services/config_backup.py:567`（备份包校验读内存字节，不是数据读点）
- `services/kline_sync.py:489`、`repair_daily.py:119` 的 staging 读（写入中间态）

### 13.4 已知问题（按严重度）

1. **测试债务 329 例（最高优先级，但可后置）**
   - 160 例直接抛 `RuntimeError: 存储层尚未初始化`——这些用例用 `DataStore(tmp_path)`/`tmp_path` 造 Parquet，读路径 fail-closed 后必然失败。
   - 其余 170 例是断言"有数据"但 CK 里没有（测试造的 Parquet 不再被读）。
   - 分布 TOP：`test_daily_repair_history_discovery`(30)、`test_enriched_partial_partition`(20)、`test_sector_rotation`(19)、`test_minute_gaps`(16)、`test_instrument_sync_share_fields`(12)、`backtest/test_matrix_ext_fields`(12)、`test_ext_granularity`(11)、`test_quote_demand`(10)、`test_abnormal_moves`(10)、`test_ext_factors`(9)。
   - 处置建议：先"注入 store mock"恢复断言逻辑（快），再对核心链路补 CK 集成测试（慢）；对应 T5.4 的 4 人天。
2. **派生缓存无 CK 表**（见 13.3 ①）：需要产品/技术决策，是 B4 未清零的主因之一。
3. **双写镜像仍在**：enriched / minute / adj_factor / ext 都是"写 CK + 写 Parquet 镜像"。读侧以 CK 为准，Parquet 只是过渡；B5 才能删。若发现两栈不一致，先查镜像写入是否漏（`_mirror_*` 函数）。
4. **兜底口径不统一（有意为之，但要知道）**
   - 核心读路径（screener、回测面板、pipeline、ext API）：`require_storage()` **fail-closed**。
   - 写侧辅助（`kline_sync._read_minute_day`、`_merge_adj_factor_store`）：`get_storage() is None` 时**退回 Parquet**，因为 backfill 离线工具会用临时目录调用。
   - 测试表现因此不一致：前者报 RuntimeError，后者静默读到空/旧数据。
5. **CK 凭据**：`tsp` 用户密码未落 `.env`（L2 阶段一直用 `default` 验证）。真机切换时应用会按 fail-closed 拒绝启动，需先补 `.env`。
6. **遗留脏文件**：`data/kline_minute/date=2020-01-09/part.parquet`（容器 root 写入，工具侧删不掉），需 `docker exec -u root` 手工删。
7. **工作区不可随意 checkout**：按 §1 决策"全量改动不中途提交"，当前 68 个 tracked 改动 + 272 个未跟踪文件都在工作区。做基线对比时必须先 `git diff > /tmp/backup.patch` 再 `git stash push`（**不要用 `-u`**，未跟踪文件太多），跑完立刻 `git stash pop` 并核对文件数（应为 68）。

### 13.5 下次会话开工步骤

```bash
cd /home/zwcad/workplace/tsp/tick-stock-panel/backend

# 1) 服务健康（401 = 正常，说明鉴权在生效）
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3018/api/data/status
docker logs tsp-dev --since 10m 2>&1 | grep -iE "traceback|ImportError|SyntaxError" | head

# 2) 盘点剩余读点（排除 DuckDB 主体与注释）
grep -rn "read_parquet(\|scan_parquet(" app --include=*.py \
  | grep -v "app/parquet.py" | grep -v "app/tickflow/repository.py"

# 3) 当前测试基线（应与此处数字一致；变化说明引入新回归）
docker exec -w /app/backend tsp-dev .venv/bin/python -m pytest tests -q --tb=no -p no:randomly | tail -2
#   期望：310 failed, 4503 passed, 7 skipped, 1 xfailed, 2 errors（第三批会后基线，见 §13.7）

# 4) 存储层契约（必须全绿）
docker exec -w /app/backend tsp-dev .venv/bin/python -m pytest tests/storage -q
#   期望：53 passed

# 5) 改完一批后看"新增失败"
docker exec -w /app/backend tsp-dev .venv/bin/python -m pytest tests -q --tb=line -p no:randomly 2>&1 \
  | grep -E "^FAILED" | sed 's/FAILED //' | cut -d: -f1 | sort | uniq -c | sort -rn | head -20
```

**改动安全判定**：一次改动后，(a) `tests/storage` 仍 53 绿；(b) 全量 failed 数不高于 310（除非是"故意删掉的旧测试"）；(c) 服务重启无 traceback；(d) 涉及表结构时手写一次"写→读回→字段对齐"脚本。

**建议顺序**：13.3 ② → ③（不含 DuckDB 三处）→ ④ matrix.py → ①（先定 schema）→ B5.1 删 repository.py/DuckDB → T5.4 测试迁移。

### 13.6 关键实现约定（改代码前必看）

- 单日读：`store.read_daily(table, start=..., end=..., symbols=..., columns=...)`；日期枚举：`list_daily_dates(table, start=None, end=None)`；整日删：`delete_daily_date(table, day=...)`。
- ReplacingMergeTree 表读一律走 `FINAL`（`store` 内部已处理），否则行数放大（曾出现 5571→10787）。
- 单日写入用 `store.delete_daily(table, day=..., symbols=[...])` + `write_daily` 组合（局部覆写），不要"读旧分区再 concat 写回"。
- 扩展时序：`read_ext_timeseries(ext_id, start, end)` 返回 `ext_id, symbol, date, time_value, <展开列>`；对外需 `_normalize_ext_dates` 口径（去 `ext_id`、`time_value→ts`、`date` 转字符串）。
- 分钟读：`read_minute(table, start=datetime, end=datetime, symbols=..., columns=...)`，时间为北京墙钟 naive。
- 新增 store 方法要同步 `app/storage/protocol.py`（若属核心 daily/minute 契约）与 `tests/storage` 契约测试。

### 13.7 第三批会话记录（2026-11-16 下午）

**已完成：B4 剩余可改文件（§13.3 ②③④ 除 matrix.py 外全部）**

- ② 写入侧/数据源：`ext_data.build_code_lookup`→`load_instruments()`、`ext_pull.since_cursor`→`read_ext_timeseries`、`minute_adjust._daily_close_lookup`→`read_daily`；`data_integrity`（3 处）与 `fuyao provider`（4 处）判定保留本地：前者是"检查本地镜像/staging 文件"语义（E 类，随 B6 数据管理重做），后者是插件对数据源响应的 dump 缓存（非业务数据）。
- ③ API 层：`api/ext_data` snapshot 分支（有 symbol 的快照读 CK ext_timeseries 最新日期，market_level 与无 runtime 回退本地）、`ext_series` intraday 分支统一走 `_read_ext_dataframe`（兼容 `ts` 列）、`api/overview` 复用 `market_overview_builder._read_ext_rows`、`api/data` 财务统计→`store.financials_stats`、`api/paper._last_close`→`read_daily`。
- ④ 回测/策略：`strategy/paper._factor_on`→`read_adj_factor`、`backtest/engine._load_panel_inner` 兜底→`read_daily(enriched)`（无 runtime 回退本地）；`mining_candidates`（2）、`api/mining`（1）、`ai_generator`（白名单字符串，无读点）按约定保留。
- `backtest/engine.py:705`（matrix 入口）随 matrix.py 压轴项一并处理。

**顺带修复的存量 bug（CK 写入侧）**

1. **str 日期写 CK Date 列会抛 TypeError**：`_mirror_ext_to_ck`（str day）、`instrument_sync` as_of、`depth_service` date 三处都在往 Date 列塞 str，错误被 try/except 或 buffered flush 吞掉（日志难察觉）。已全部 `pl.lit(...).cast(pl.Date)`。
2. **镜像 time_value 为 null 会炸 String 列**：`_mirror_ext_to_ck` 补 `.fill_null("")`。
3. **防穿越校验顺序回归**：CK 化后 `_partition_date` 校验被排到 `list_ext_dates` 之后，表为空时非法日期不再 400（安全回归）。已把校验提到任何存储查询之前。
4. **instruments 股本列非 Nullable**：provider 缺股本时被写成 0.0（换手率算成 0）→ **schema v2**（`total_shares/float_shares/tick_size` → `Nullable(Float64)`，带增量迁移）。注：`tsp` 库三张维表的 ALTER 已手工补齐生效（v2 版本号在会话测试期已被泄漏 runtime 的 ensure_schema 写入，迁移体当时未在真库执行；全新库上 ensure_schema 迁移路径已验证正确）。

**测试债务：356 → 310（本批清 46 例）**

迁移模式：`tests/conftest.py` 新增 `ck_runtime` fixture（回收泄漏的全局 runtime → init 带随机前缀的 CK 表 → 用例结束恢复进入前状态）。造数改经 `store.write_*` 或生产写函数（`write_ext_parquet` 自带镜像），断言逻辑不变（验收 §14.1 D 类）。已清零：`test_ext_granularity`(11)、`test_ext_factors`(9)、`test_ext_data_rows_date_guard`(8)、`test_ext_rows_query`(6)、`test_ext_snapshot_at`(1)、`test_ext_backfill`(1)、`test_watchlist_csv`(5)、`test_watchlist_ocr`(1)、`test_minute_adjust`(2)、`test_paper_trading`(1)、`test_ext_config_load_all_cache`(1)、`test_instrument_sync_share_fields`(12)。`tests/storage` 契约 53 例持续全绿。

**新踩的 CK 坑（后续迁移必看）**

- ReplacingMergeTree 以 `updated_at` 定版本，**同毫秒两次写入是平局，FINAL 去重结果不确定**——测试内"seed 后立即再写"必须 `time.sleep(0.005)` 隔开；生产侧若存在同毫秒双写（修复类脚本）需在 `store.write` 显式传版本列（遗留项）。
- CK `Date` 列不接受 str、非 Nullable `String` 不接受 null；polars 读回 `Date` 列是 date 对象，对外口径要 `cast(Utf8)`。
- 断言不要用行位置索引：CK 读按 `ORDER BY symbol` 排序，与本地 parquet 写入顺序不同，一律按 symbol 过滤。
- `store` fixture 化的 runtime 是**带表前缀**的；经真实 app lifespan（TestClient）泄漏的是无前缀 runtime，`ck_runtime` 会先回收再注入，结束恢复原状，不改变后续用例所见状态。

**剩余（下一批）**

- matrix.py 压轴（`pads.dataset(parquet_root)` → CK 分批/InMemoryDataset；分区边界、分区指纹、磁盘 mmap 缓存复用深度耦合本地布局，不能半改；backtest 30 例失败均属此项）。
- 派生结果缓存 schema 决策（§13.3 ①，约 17 处读点）。
- 剩余测试债务 ~310 例，TOP：`test_daily_repair_history_discovery`(30)、`test_enriched_partial_partition`(20)、`test_sector_rotation`(19)、`test_minute_gaps`(16)、`test_quote_demand`(10)、`test_abnormal_moves`(10)、`test_market_mainline`(8)、`test_limit_ladder_prior_trading_day`(8)、`test_ext_data_rows_date_guard` 已清。`test_task_registry::test_orphan_running_pruned_on_read`、`test_pipeline_status::test_jobs_snapshot_*`、`test_ext_granularity::test_series_daily_mapped_date_does_not_expose_future_values` 为 mtime 粒度/schema 语义类偶发，不属于迁移范围。


### 13.8 第四批会话记录（2026-11-16 晚）

**全量基线：356 → 310 → 212 failed**（本批净清约 98 例；`tests/storage` 53 例持续全绿）。

**测试债务（本批清零的文件）**

- `test_market_mainline`(8)、`test_limit_ladder_prior_trading_day`(8)、`test_watchlist_prepare`(8)、`test_live_agg_ewm_warmup`(6)、`test_minute_adjust`(6)、`test_abnormal_moves`(10)、`test_quote_demand`(10)、`test_sector_rotation`(19)、`test_regime_builder`(1 连带)、`backtest/test_matrix_ext_fields`(12)、`backtest/test_worker_process`(8)、`backtest/test_evidence`(8)。
- 迁移配方沿用 §13.7 的 `ck_runtime` fixture；写点统一改经 `store.write_*` 或生产写函数（带镜像），工具函数内 `_CK["store"]` 注入 + 清进程内 TTL 缓存（如 `pipeline._benchmark_cache`、`ext_factors._frame_cache`）。

**matrix.py 压轴项结论：不需整体切换**

- `pads.dataset(parquet_root)` 读的是 **pipeline 落盘的 enriched Parquet 缓存**（B5 之前仍是权威落盘），不是外部数据源；强行换 InMemoryDataset 会破坏分区指纹 / 磁盘 mmap 缓存复用。保持本地读 + CK 矩阵缓存（已实现）是正确架构，待 B5 删除 Parquet 写入时再整体重做。
- 但发现并修复了真实生产缺口：**spawn backtest worker 未初始化 storage**，CK 化后子进程读 instruments/enriched 必报 `require_storage()` 运行时错误。修复：`make_worker_task` 把 `storage_prefix`（仅表前缀，不含凭据）随任务下发，`_worker_entry` 据此自行 `init_storage(client=...)`；生产无前缀时走默认配置。`backtest` 30 例失败全部清零。

**派生结果缓存 schema 决策（§13.3 ①）**

先分类再动手，不盲目建表：

| 读点 | 决策 | 依据 |
|---|---|---|
| `market_mainline`(3 读点) | **已 CK 化**：新增 `market_mainline` 表（结构化宽表，`ORDER BY (date,kind,member)`，ReplacingMergeTree），`store.replace_mainline(kind,start,end,rows)` 用 `mutations_sync=2` 的 DELETE + INSERT 做范围覆盖；服务层读/范围替换/整日 upsert 三处切换，无 runtime 回退本地 | 结构化历史需列裁剪与范围过滤，适合专门表；已全绿 |
| `regime_builder:542`、`sector_monitor:245`、`sector_rotation:232`、`market_overview_builder:239,242` | **复用已有表**：这些是 ext 快照 / 最新分区读点，数据源已是 `ext_timeseries`/`ext_instruments`（B3 已建），只需把服务层 `pl.read_parquet` 换成 `_read_ext_rows`（同 overview 已做的）；不新建表 | 扩展表已能表达快照语义（time_value 区分盘次） |
| `quote_service:2205`、`index_sync:343,359`、`factors/materialize_cache:151`、`ext_factors:281` 快照分支 | **复用 `adj_factor`/`adj_factor_etf` + `factor_cache`**：因子/复权已有 CK 表；materialize_cache 属 `CacheBackend` 范畴（§4.4），按内存/Redis 可替换设计，不在 B4 建表 | 设计文档 §4.4 明确缓存介质可换，不应把缓存固化进 CK 表 |
| `watchlist:86,108`、`tickflow/pools:62,143` | **保留本地/CacheBackend**：自选分组与池成员是用户配置 + 远程接口缓存，设计文档 §3 已划归 `CacheBackend`，随 B5.1 CacheBackend 落地统一处理 | 非派生产物，建表属越界 |

**新踩的坑（补充 §13.7）**

- 测试直接写 `store.write_*` 不会触发生产写函数附带的缓存失效：必须手动 `invalidate_ext_caches(data_dir)` / `_benchmark_cache.clear()` / `_frame_cache.clear()`，否则读到旧值。
- spawn worker 的 storage 前缀必须随 task 下发；只 init 默认 runtime 会读到无前缀表（测试前缀隔离失效）。
- `upsert_mainline_history` 的 CK 版用 `group_by(date,kind)` 逐组 `replace_mainline`，与本地"anti-join 整日覆盖"语义等价。

**剩余债务（下批）**：`test_daily_repair_history_discovery`(30)、`test_enriched_partial_partition`(20)、`test_minute_gaps`(16) 等约 212 例，多为深 E 类（pipeline 重建 / 分区覆盖语义），需与 B5 删 Parquet 写入一并设计，不建议逐例硬迁。`test_atomic_parquet_writes`(3) 是 instrument_sync/financial_sync 切 CK 后 atomic-parquet 中断语义不再适用的遗留，随 B5.3 清理。

### 13.9 第五批会话记录（2026-11-16 深夜）

**全量基线：210 → 199 failed**（净清 11 例；`tests/storage` 53 例持续全绿）。

**本轮清理**

- `test_adj_factor_standalone_sync`(4)、`test_daily_pipeline_minute_gate`(2)、`test_enriched_full_rebuild`(4，剩 2 例 0 行分区语义属 E 类)。
- 迁移动作沿用 `ck_runtime` autouse fixture + 生产写函数双写；`run_adj_factor_sync` 的 `list_daily_dates` 判断改用 CK enriched 单行。

**派生缓存读点（§13.8 决策矩阵）为何本轮暂缓**

- `sector_rotation`/`sector_monitor`/`market_overview_builder` 的 ext 快照读点虽可改走 CK，但 `ext_data._mirror_ext_to_ck` 对 `market_level` 大表不镜像（无 symbol），且 snapshot 模式的"最新快照"语义在 CK 的 `ext_timeseries/time_value` 下需重新定义；当前 B4 阶段本地 Parquet 仍是权威落盘，切读点会让"只写本地未写 CK"的旧数据不可见。风险大于收益，留到 B5 删 Parquet 写入时整体切换。

**深 E 类为何不硬迁（明确边界）**

- `test_daily_repair_history_discovery`(30)：自检 `_scan` 读本地、重算 `recompute_daily_symbols` 读 CK，双写 bad 数据后校验环节 `inspect_invalid_history` 从本地读到已修好的值 → issues 为空 → 断言链错乱。这是本地/CK 混合语义的典型，硬迁会掩盖真实数据一致性问题。
- `test_enriched_partial_partition`(20)：生产函数已从"读本地多 Parquet 文件"改为"读 CK 单表"，`broken.parquet`/`second.parquet`/缺列等容错用例的语义在 CK 里不存在对应概念，逐例改会改变测试本意。
- `test_enriched_incremental_memory`(3)：CK roundtrip 改变列集合（quote_ts）与浮点精度，断言 `assert_frame_equal` 严格比对会失败，需重置精度口径。
- 以上均需与 B5 删 Parquet 写入同步设计，不建议在 B4 单测层逐例硬迁。

**剩余债务分布（199 例）**：TOP `daily_repair_history_discovery`(30)、`enriched_partial_partition`(20)、`minute_gaps`(16)、`minute_history_repair`(7)、其余为分散小文件。绝大多数属"pipeline 重建/分区覆盖"深 E 类，需 B5 整体设计。

### 13.10 B5.1 可行性评估（2026-11-16 深夜）

**结论：B5.1（删 repository.py/DuckDB）当前不可执行，前置依赖未满足。**

实测调用链证据：

1. `api/data.py`、`api/intraday.py`、`api/kline.py`、`api/overview.py`、`services/screener.py` 仍有 **18+ 处** `repo.execute_all`/`execute_one` 直查 DuckDB 视图（`DESCRIBE kline_enriched`、`SELECT ... FROM kline_minute` 等）。这些 API 读路径尚未切 CK，删 DuckDB 会直接破坏它们。
2. `repository.py`（2883 行、120+ 方法）被 **35 个 app 文件 + 61 个测试文件**依赖；DuckDB 视图注册（`financial_sync._refresh_financials_views`、`daily_pipeline._refresh_views`/`_refresh_single_view`、`extend_history._refresh_single_view`）仍被同步流程主动调用，不是空壳。
3. `app/parquet.py` 的 `scan_daily_parquet`/`scan_enriched_parquet` 仍有 **10 处**生产调用（strategy/paper、quote_service、data_integrity、index_sync、kline_sync、engine、pipeline），不能随 T5.1 删除。

**B5.1 的前置条件**（按依赖顺序）：
1. API 层 DuckDB 读点（api/data 18 处、intraday 1、kline 3、overview 2、screener 1）全部切 CK `store` 读；
2. 删各同步流程的 view refresh 调用 + `repository.py` 的 `_register_views`/`_register_unified_views`；
3. `parquet.py` 的 scan 函数随最后调用方迁移后删除；
4. 最后才能删 `repository.py` 的 DuckDB 依赖 → DataStore 精简 → 测试 `DataStore(tmp_path)` 归零。

这是一个跨多文件的连续工程，违反"最小范围"原则，**不应在单会话内硬做**。剩余 197 例测试债务中，深 E 类（pipeline 重建 / 本地-CK 混合语义）也需等这些前置完成后才能成批重写。

**本轮净清 2 例**：`test_limit_ladder_names`(4 例中的 4 个参数化用例，count 口径 4→0)；`test_integrity_symbol_repair`(5) 判定为本地-CK 混合深 E 类，未迁移。全量 199 → 197 failed，`tests/storage` 53 绿。

### 13.11 B5.1 前置完成：API 层 DuckDB 读点切 CK（2026-11-16 深夜）

**目标**：解除 §13.10 记录的 B5.1 阻塞（API 层 18+ 处 DuckDB 视图读点未切 CK）。

**store 新增通用统计方法**（`app/storage/store.py`）：
- `table_stats(table, date_column="date")`：一条 CK 聚合返回 rows/earliest/latest/symbols/trading_days，取代各 `SELECT count/min/max/uniq` 视图聚合。空表返回 `{}`；`adj_factor` 用 `trade_date` 轴；空聚合用 `ifNull(toDate(...), 1970 占位)` 绕过 CK 24.8 Nothing→Arrow 不兼容。
- `instrument_stats(asset_type)`：`read_instruments` 上算 rows/symbols/named/latest_as_of（index/etf 固定 latest=None，与原视图统计口径一致）。
- `table_columns_typed(table)`：DESCRIBE 返回 `(name, type)`，取代 `DESCRIBE <duckdb view>` 给 /schema 端点用。

**api/data.py 迁移**（全部 DuckDB 读点清零）：
- `_safe_aggregate` 走 `_VIEW_TO_CK_TABLE` 映射 + table_stats；`_safe_aggregate_instruments`/index/etf_instruments 走 instrument_stats；etf_daily/adj_factor 的复杂兼容查询简化为权威 CK 表统计（原兼容旧 index 存储分支删除）。
- `_safe_aggregate_daily`/`_safe_aggregate_enriched` 的 symbols 数改 CK instruments；字段数 DESCRIBE 改 table_columns。
- `table_schema` 用 table_columns_typed 返回精确 CK 类型。
- 新增 `_ck_store(repo)`：优先 `repo.storage`（lifespan 注入），回退进程 runtime；都无则按空处理（与原视图缺失同语义）。

**其余 API 读点**：
- `api/intraday` 兜底指数报价、`api/overview` 兜底指数报价：从 `repo.execute_all` 窗口函数改 `store.read_daily(kline_index_daily)` + Python 侧取最近两日（指数数据量小）。
- `api/kline` 清空分钟K的 COUNT：改 `table_stats(kline_minute)`（顺带移除 DuckDB 未消费结果集钉住分区句柄导致 rmtree 静默失败的隐患）。
- `services/screener` 的 `enriched_latest_date` 回退：改 CK `latest_daily_date`。**screener 的用户自定义 SQL 筛选（内存 df 注册到 duckdb）保留**——它是筛选引擎核心能力，无 CK 表可替代，符合 DoD "duckdb 仅剩 screener 注释/内存查询"。

**验证**：新增 `tests/test_api_duckdb_to_ck.py` 7 例（table_stats/instrument_stats/typed columns/聚合函数/schema 端点真实 CK 路径）全绿；data status 43 例 + storage 53 例全绿；全量 198 failed（对比基线失败清单：新增 0 / 消失 0，零回归）；服务重启无 traceback。

**补扫残留 3 处 execute 读点（同批完成）**：
- `services/market_overview_builder._index_quotes`：指数兜底改 CK `read_daily`（同 api/overview）。
- `tickflow/repository._live_agg_baseline_date`：取上一交易日改 CK `list_daily_dates`。
- `services/kline_sync._cleanup_null_datetime_minute`：旧版全-null 分区清理改为直接读本地 parquet 文件（本就是清理磁盘文件，DuckDB 视图计数反而滞后失真）。
- **至此 app 层 `repo.execute_all`/`execute_one` 调用归零**（grep 验证），仅 screener 内存 df 的用户 SQL 筛选保留 duckdb.connect（核心能力，无 CK 替代）。

**B5.1 剩余前置**（下次）：同步流程的 view refresh 调用（financial_sync/daily_pipeline/extend_history 的 `_refresh_*_views`）仍在写 DuckDB 视图，但这些视图现已无读者（app 层 execute 归零）——可直接删 refresh 调用 + `repository.py` 的 `_register_views`/`_register_unified_views`；随后删 repository 的 DuckDB 连接、精简 DataStore、最后删 parquet.py scan 函数（需先迁移其 10 处生产调用方）。

### 13.12 B5.1 视图读点清零（2026-11-17）

**目标**：删除 DuckDB 视图的最后生产读者，为删除视图注册铺路。

**已完成（app 层视图读点归零）**：

- `api/watchlist.py`、`api/screener.py`（2 处）：删除 `db.query("SELECT ... FROM ext_{config_id}")` fallback。该分支仅在扩展配置未注册时命中，读的是本地 parquet 视图（与 CK 主路径数据可能不一致），改为"配置缺失则跳过该列"。删除后 `quote_ident` import 一并清理。
- `api/ext_data._refresh_views`：删除 21 条视图注册 DDL（`CREATE OR REPLACE VIEW`），**保留 `repo.clear_cache()`**（扩展数据变更后清理 enriched 帧缓存，是必要副作用，不可删）。
- `api/ext_data._describe_parquet_columns`：`duckdb.query(DESCRIBE read_parquet)` → `pl.read_parquet_schema` 逐文件取并集（兼容 schema drift）。**保持返回 DuckDB 类型名（VARCHAR/DOUBLE/BIGINT…）**——前端按 type 判数值列，是 API 契约，`test_ext_schema_endpoints` 5 例验证。

**验证**：全量严格对比新增失败 0 / 消失 0；`grep -rn "store.db|.db.query|.db.execute"` app 层仅剩 repository 内部连接维护（视图注册 DDL、引用传递、close）；`execute_all/execute_one` app 层归零。

**B5.1 剩余（下次，需连同专门测试一起删）**：

- 生产流程的无效 refresh 调用仍在：`daily_pipeline`（7 处）、`extend_history`（6）、`index_sync`（5）、`kline_sync`（3）、`repair_daily`/`daily_bootstrap`/`indicators/pipeline`/`api/data`/`api/kline` 各 1。它们调用的 `repo.refresh_view`/`rebuild_views`/`refresh_index_views` 方法本身仍在（视图已无读者，这些调用是纯 DDL 开销）。
- 但 `tests/test_duckdb_read_concurrency.py`（直接测 refresh_view 并发）、`test_repository_index.py`（rebuild_views 后 execute_one 读视图）、`test_quote_chain.py` 等是**针对 DuckDB 功能本身的契约测试**——删除 repository 方法必须同步删除/重写这些测试，不能简单删测试来"通过"。这是 B5.1 最后阶段，需整体设计。
- 之后才能删 `DataStore.__init__` 的 `duckdb.connect` 与 `_register_views`/`_register_unified_views`，精简 DataStore，删 parquet.py scan 函数，最后清理 61 个 `DataStore(tmp_path)` 测试构造。

### 13.13 B5.1 收尾：DuckDB 依赖移除（2026-11-17）

**全量 197 → 176 failed（净清 21 例），0 errors（原 2 errors 归零），tests/storage 53 绿。**

**已完成**：

1. **repository.py 业务读方法 CK 化（8 个）**：`latest_minute_date`/`latest_minute_date_global`/`earliest_daily_date`/`earliest_minute_date`/`latest_daily_date`/`symbols_lagging`/`_latest_enriched_date_duckdb` 全部从 DuckDB `execute_one/all` 改为 CK store（`read_minute`/`latest_minute_datetime`/`list_daily_dates`/`table_stats`/`read_daily`）。新增 `_ck_store()` 门面（优先构造注入 storage，回退进程 runtime）。
2. **删除 repository 全部 DuckDB 基础设施**：`duckdb.connect`、`_register_views`、`_register_unified_views`、`_has_parquet`、`_read_concurrency_env`、`execute_all/one`、`_new_cursor`、`refresh_view`/`refresh_index_views`/`rebuild_views`/`_materialize_instruments_views`，并发锁 `_read_limiter`/`_ddl_lock`/`_cursor_lock`、`self.db` 引用。`DataStore`/`KlineRepository` 保留全部非 DuckDB 职责。
3. **删除生产 view refresh 调用**：daily_pipeline（含 3 个 helper）、extend_history、index_sync、kline_sync、daily_bootstrap、repair_daily、indicators/pipeline、api/data、api/kline。保留 `_refresh_views`/`_refresh_single_view` 空壳以兼容测试 monkeypatch。
4. **删除 `.db.close()`**：worker.py、scripts/benchmark_cache_async.py、3 个测试文件。
5. **CK 镜像方法恢复**：`git checkout` 事故（见下）丢失了 `_write_daily_partition`/`replace_daily_rows`/`flush_live_daily_asset` 的 CK 双写、以及 `save_instruments`，已全部基于测试契约重建，`tests/storage` 53 绿。

**测试处理**：删除 `test_duckdb_read_concurrency.py`（纯 DuckDB 功能测试）与 `test_repository_index.py::test_execute_one_releases_parquet_file`；改写 12 个业务测试里的 stub/mock/setup 引用（`refresh_view`/`rebuild_views`/`execute_one`/`.db.close()` 等）。保留 `test_screener_external_access.py`（screener 用户 SQL 筛选仍用 duckdb.connect，是核心能力）。

**保留的 duckdb 依赖（仅 1 处）**：`services/screener.py` 的 `duckdb.connect` + `con.register(user_df)` — 用户自定义 SQL WHERE 筛选引擎，无 CK 表可替代。

**重要教训（操作风险记录）**：
- 用行号/方法签名脚本批量删除 repository.py 方法时，边界判定误删了 `KlineRepository.__init__` 的 `storage` 参数、`_atomic_write_parquet` 的 `@staticmethod`、以及前几批未提交的 CK 镜像方法；一度用 `git checkout -- repository.py` 恢复，但该操作**回滚到 HEAD、丢失了所有未提交改动**（repo 的 CK 镜像、save_instruments 等），后续基于测试契约逐个重建。
- **结论：本仓库未提交改动很多，禁止用 `git checkout --` 恢复单个文件**。删除大段代码应逐方法用 `replace_in_file` 精确匹配，删后立即 `ast.parse` + 目标测试验证。
- 全量对比脚本按用例名（含参数化）做集合差会产生大量假"新增/消失"，应以 failed/errors 总数 + `tests/storage` 为准。

**剩余**：`parquet.py` 的 `scan_daily_parquet`/`scan_enriched_parquet` 仍有 11 处生产调用（strategy/paper、quote_service、data_integrity、index_sync、kline_sync、engine、pipeline），B5.1 尚未删除 parquet.py；`import duckdb` 仅剩 screener。剩余 176 failed 多为深 E 类（pipeline 重建/本地-CK 混合语义），与 B5 后续阶段强耦合。

### 13.14 B5.1 parquet.py scan 评估 + 测试债务清理（2026-11-17）

**全量 176 → 163 failed，tests/storage 53 绿。**

**parquet.py scan 迁移评估结论：20 处调用（8 文件，含 repository.py 9 处）不适合现在机械切 CK。**

逐站分析（`docs` §13.13 的"11 处"口径不含 repository.py）：
- `tickflow/repository.py` 9 处：内部缓存预热/历史窗口计算，用 `guarded_collect(priority=background)` 做资源控制，CK 读是同步的，语义不同。
- `strategy/paper.py` 3 处 + `data_integrity.py` 1 处 + `kline_sync.py` 1 处：读 `data_dir` 参数指定的本地 parquet（离线结算/文件级反查 include_file_paths/原子合并），无 CK 对应能力。
- `indicators/pipeline.py` 2 处：全量重建的 staging 中间产物（在 enriched 树外，非业务表镜像）。
- `backtest/engine.py` 1 处：已是 CK 分支的 else 本地降级（离线脚本/单测依赖）。
- `quote_service.py` 2 处：文件列表 + guarded_collect 资源控制。

**结论：scan 函数应随 B5「删 Parquet 写入」整体重做，现在切换会破坏离线语义/资源控制/中间产物链路。仅迁移了 `index_sync._load_local_etf_daily`（CK 优先 + 本地回退，7 例验证）。**

**本轮清理的测试债务（13 例）**：`test_minute_pct_zero_reference`(5)、`test_dimension_intraday`(8)。两者同根因：`_dimension_intraday_compute` 日期发现读本地目录（有分区→target ok），但分钟K/前收已切 CK 读（测试只写本地→no_data）。迁移：autouse `ck_runtime` fixture + 造数 helper 双写 CK `kline_minute`/`kline_daily`（date 列从分区日期补）。

**剩余 163 failed**：TOP `daily_repair_history_discovery`(30)、`enriched_partial_partition`(20)、`minute_gaps`(16) 等深 E 类，均与 B5 删 Parquet 写入强耦合。

### 13.15 B6 验收准备实测记录（2026-11-17）

**范围**：冷启动、数据完整性、关键 API 冒烟、同步冒烟（§8 中 T6 的前置，T6.1~T6.7 尚未开始）。环境：容器 `tsp-dev` + `tsp-dev-clickhouse`（CK 24.8.8），workspace 挂载 `/app`，`CLICKHOUSE_URL=http://clickhouse:8123`。全程未提交代码。

**冷启动与数据完整性**：
- CK schema v2，正式表 27 张；清掉 49 张 `dbg_264e13_*` / `mig_d91bdb_*` 迁移遗留表。
- 同步前基线：`kline_daily_enriched` 1,415,893 / `kline_daily` 1,326,387 / `kline_index_enriched` 144,085 / `kline_index_daily` 143,484 / `ext_timeseries` 33,928 / `instruments` 11,429 / `adj_factor` 4,951 / `kline_minute` 1,106 行。
- `/api/data/status` 首请求 `stats_pending: True`（异步后台加载），等 2s 后转 `False`：`index_daily.rows=143478`、`instruments.rows=5892`、`latest_as_of=2026-11-16`；`/api/data/schema/daily` 200，10 字段。

**关键 API 冒烟**（最小 FastAPI app 注入真实 `KlineRepository(store, storage=rt.store)` + 各 service，绕过认证中间件；数据与 service 均为真实实例）：

| 端点 | 首轮 | 补齐 11-16 后 | 备注 |
|---|---|---|---|
| `/api/kline/daily` | 200 / 81.8ms | 200 / 2678.9ms（冷 enriched 缓存） | 23 行，source=enriched |
| `/api/kline/daily/latest` | `DailyIntegrityError` | 200 / 2.6ms | 首轮被 11-16 缺失标记拦截 |
| `/api/kline/minute` | 200 | 200 | rows=0，见下方边界 |
| `/api/overview/market` | `DailyIntegrityError` | 200 / 321.2ms | indices=4、breadth 等字段齐全 |
| `/api/watchlist/enriched` | 200 | 200 | rows=0（自选列表为空） |
| `/api/screener/market-snapshot` | `DailyIntegrityError` | 200 / 137.9ms | as_of=2026-11-16，rows=5572 |
| `/api/sector-rotation` | 200 | 200 | no_data/minute_missing（当日无分钟数据） |
| `/api/ext-data` | 200 | 200 | items=2 |
| `/api/ext-data/ext_gn_ths/rows` | 200 / 37.9ms | 200 / 75.3ms | total=5582，CH `ext_timeseries` 读通 |

首轮 3 例失败根因：本地 `user_data/daily_integrity_state.json` 记录 11-16 股票/指数日K `missing`，`assert_usable` 按设计拦截下游 enriched 计算；查 CK 同日 `count()=0`，确为真实数据缺口而非迁移回归。该门禁读本地 JSON，非本次迁移改动。

**同步冒烟**（`daily_pipeline.run_now(..., override_start_date=2026-11-16)`，耗时 291.5s）：
- 结果 `stage_errors: []`、`integrity_issues: 0`；日K 覆盖 1 天、指数 599 行、enriched 覆盖 251 天、除权因子 +10 只个股。
- CK 写回读回：`kline_daily` 11-16 5,572 行、`kline_index_daily` 599 行、`kline_daily_enriched` 11,144 行；本地 `HEALTH []`（完整性标记自动清除）。
- 证明双写镜像（本地 Parquet + CK）在真实同步链路上生效，补齐后 9/9 端点全绿。

**已知边界与发现**：
- `KlineRepository.get_minute` / `get_minute_batch` / `get_minute_range` 仍走 `pl.scan_parquet`（§13.14 评估后保留的 E 类读点），未切 CK。CK `kline_minute` 表本身可读：`store.read_minute("kline_minute", symbols=["600000.SH"], start/end)` 返回 130 行（11-09）；但 `/api/kline/minute` 读本地分区，本地无 11-09 数据 → 走实时拉取 → rows=0。T6.7 双栈对照需关注分时图与 CK 数据的一致性。
- `watchlist/enriched`、`sector-rotation` 的空结果是数据状态（自选为空 / 当日无分钟数据），非迁移回归。

**后续**：T6.1 场景 S1~S9、T6.2 性能冒烟标定、T6.3 真实数据源抽样、T6.4 回滚演练、T6.5 连续 3 天观察、T6.6 验收记录、T6.7 页面级双栈对照尚未开始。

### 13.16 2026-11-18 后续进展

本节只记录最新状态；历史过程仍保留在上面的交接记录中，详细问题清单见 ISS-002。

- 全量测试基线已恢复：`4817 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors。此前 `MEMORY_LIMIT_EXCEEDED` 的根因是测试把带 `t_` 前缀的表写进正式库 `tsp`；已强制测试写入 `tsp_test` 并清理残留表（详见 ISS-002 CH-01）。
- 开发 compose 新增 `deploy/clickhouse/config.d/41-tsp-dev-memory.xml`，容器上限 5g、CK 上限 4.5 GB；生产 compose 配置不变。
- 仓储日 K / 分钟 K 读路径已改为 CK 优先，查不到时保留本地兼容路径；`KlineRepository.get_minute*` 不再只走本地 Parquet（详见 ISS-002 CH-02/CH-03）。
- `/api/health` 与 `/health` 同时返回 CK 健康状态，WBS T1.6 的路径口径已对齐（详见 ISS-002 CH-11）。
- B5 已把分钟补拉/刷新、盘口、复权因子、财务表、维表 instruments 的在线读写收敛为 CK-only；daily/enriched 在显式 storage 下的在线读写（含修补、校验、universe、常规写入、live merge/flush、状态统计与缓存 generation）已收敛为 CK-only，本地 parquet 仅保留离线兼容。2026-11-18 评估的 daily 修补 CK 分支原型失败后，已按 DES-002 完成 P0~P4 主体落地。`pipeline_recompute` 常规 enriched 编排、派生缓存和离线迁移工具仍保留本地 Parquet 路径（详见 ISS-002、DES-002）。
- B4 的 `read_parquet/scan_parquet` 归零、B5 剩余本地路径清理、B6 的三天观察 / 回滚演练 / 页面级双栈对照 / 满量性能标定仍未完成；需求暂不改判为完成。
- **2026-11-19 当前状态**：全量回归 `4898 passed / 13 skipped / 1 xfailed / 0 failed`（四后端 conftest 锁 memory）；模拟盘账户已迁 PG 事务层（DES-005），S4 多 worker 已解锁（ISS-004 DIST-05）。本需求相关的剩余未闭环项（含 CH-04~CH-09、MN-02/03、B4/B5 尾巴）统一登记在 [ISS-005](ISS-005-outstanding-items-summary.md)。
