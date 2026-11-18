# ClickHouse 存储层：测试与验收方案

> 文档类型：部署 | 编号：DEP-001 | 状态：草案 | 文档版本：v1.0 | 最后更新：2026-11-18
> 负责人：— | 关联：DES-001、DEP-002
> 归档来源：base_v1.0 基线文档，按 DOC-STANDARD v1.0 归类命名；内容未改。

> 配套文档：[DES-001-clickhouse-storage-design.md](../design/DES-001-clickhouse-storage-design.md)（下称"设计文档"）
> 状态：**草案**，与设计文档同步评审。适用阶段：B1 基础设施 → B5 收尾，每批次按本文对应章节验收。

## 1. 验收原则

1. **功能等价优先于性能**：先证明"同样的输入得到同样的输出"，再谈快慢
2. **页面无感是最终标准**：API 契约、响应结构、页面交互不变；错误语义更严格（fail-closed，不静默返回空）
3. **数据不迁移**：测试数据分两档 —— CI 用**小规模确定性造数**，本地/准生产用**真实数据源抽样同步**（§13）
4. **能自动化的绝不留手工**：手工清单只保留"必须用眼看"的部分（渲染、交互、视觉）
5. **双栈对照是主要校准手段**：`origin` 环境（旧栈 Parquet+DuckDB，端口 4018/4011）保留为**运行时基准（oracle）**，同一份数据在两套栈上跑出结果做逐值比对（§16）

## 2. 测试分层与执行命令

| 层级 | 范围 | 是否真 CK | 数据来源 | 命令 | 何时跑 |
| --- | --- | --- | --- | --- | --- |
| L1 单元 | 纯函数、序列化、类型转换、SQL 拼装 | 否 | 内联构造 | `uv run pytest tests/test_ch_*.py -q` | 每次提交 |
| L2 存储契约 | 仓储方法读写语义 | 是 | 造数（`factories.py`） | `uv run pytest tests/storage -q` | 每次提交 |
| L3 服务集成 | 同步服务 → 仓储 → 服务层 | 是 | 造数 | `uv run pytest tests/integration -q` | PR 合并前 |
| **L3.5 API 契约** | **HTTP 请求打 FastAPI app，断言状态码/响应体** | **是** | **造数（同一份）** | `uv run pytest tests/api_contract -q` | **每次提交** |
| **L3.6 SSE 推送** | **流式事件格式、断线重连、背压** | **是** | **造数** | `uv run pytest tests/api_contract/test_sse.py -q` | **每次提交** |
| **L4 场景测试** | **多接口编排，验证业务闭环** | **是** | **造数 + 真实抽样** | `uv run pytest tests/scenarios -q` | **每批次结束** |
| L5 故障注入 | CK 不可用/超时/中断 | 是 | 造数 | `uv run pytest tests/storage/test_faults.py -q` | 每批次结束 |
| L6 页面冒烟 | 渲染与交互 | 是 | 造数或真实 | 手工清单（§7 末） | 每批次结束 |

前端不受存储改造影响，仍执行 `pnpm build` 作为回归门槛。

## 3. 测试环境与 fixture 契约

### 3.1 CK 环境

- 测试用独立 database：`tsp_test`（与运行库 `tsp` 隔离）
- CK 由 compose 的 `clickhouse` 服务提供（`clickhouse/clickhouse-server:24.8.8-alpine`）：容器内 `http://clickhouse:8123`（compose 注入 `CLICKHOUSE_URL`），宿主调试 `http://localhost:8123`
- 账号 `tsp` / `tsp123456`（`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`），库与账号由 `deploy/clickhouse/init` 预建
- **隔离约束**：测试只允许在 `tsp_test` 内建/删带前缀的表；**禁止** `DROP DATABASE`、`SYSTEM` 全局操作、跨库写入
- **无 CK 时测试直接失败**（策略 B，2026-11-15 确认）：不允许 skip、不允许静默通过
- 测试库固定 `tsp_test`（`backend/tests/conftest.py` 强制 `CLICKHOUSE_DATABASE`），连接参数默认取应用配置
- 连接参数可用 `CLICKHOUSE_TEST_URL` 覆盖（默认取 `settings.clickhouse_url`）

### 3.2 表名前缀隔离（设计文档 §4.7）

```python
# tests/storage/conftest.py
@pytest.fixture
def store():
    prefix = f"t_{uuid4().hex[:8]}_"
    s = ClickHouseStore(url=..., database="tsp_test", table_prefix=prefix)
    s.ensure_schema()      # 建带前缀的表
    yield s
    s.drop_all()           # 结束 drop，失败也清理
```

**硬要求**：业务代码不得自行拼表名；所有表名必须经 `store.table("kline_daily")` 生成，否则测试隔离失效。此约束需在代码评审中检查。

> 现状（B2）：`tests/storage/conftest.py` 提供两个 fixture —— `ch` 是带前缀的 `ClickHouseClient`
> （`ensure_schema` + 收尾只 drop 本前缀表），`store` 是 `ClickHouseStore(ch)` 仓储门面。
> 即样本中的 `ClickHouseStore(url=..., database=..., table_prefix=...)`
> 实际写作 `ClickHouseStore(ClickHouseClient(url=..., database=..., table_prefix=...))`。

### 3.3 真实 HTTP 栈（L3.5 / L4 共用）

```python
# tests/api_contract/conftest.py
@pytest.fixture
def client(store):
    app = create_app_for_test(store=store, cache=MemoryCacheBackend())
    with TestClient(app) as c:      # 走真实中间件/路由/序列化，不 mock service
        yield c
```

**关键约束**：只替换"存储与缓存"两个依赖，**不 mock 服务层、不 mock 路由** —— 否则测不到真实链路。数据由 `factories.py` 通过真实仓储写入，再由同步服务/接口读取。

## 4. 存储层契约测试（L2）

每个仓储方法都要有"语义断言"，不能只断言"不报错"。

### 4.1 基础读写

| 用例 | 断言 |
| --- | --- |
| 写入后读回 | 行数、列名、dtype、值完全一致（含浮点精度） |
| 空写入 | `append_*(空 DataFrame)` 不报错、不产生行 |
| 重复写入同一 key | 读回只有一行，且是**后写**的值（`ReplacingMergeTree(updated_at)` 生效） |
| 同日多 symbol 写入 | 按 `(symbol, date)` 唯一，互不覆盖 |
| 跨年写入同一 symbol | 落在不同分区，读回仍按 key 唯一（验证"分区不打断去重"） |

### 4.2 Nullable 与类型

| 用例 | 断言 |
| --- | --- |
| 可空列写 `None` | 读回仍是 `None`，**不是 0**（Float64/UInt32 都要测） |
| `limit_up` 全空（instruments） | 读回 schema 为 `Nullable(Float64)`，非空列 |
| `UInt32` 连板数 | 写入 0 与 NULL 语义区分 |
| 日期/时间 | `Date` 与源一致；`Datetime` 读回后为**北京墙钟 naive**（设计文档 §5） |
| symbol 格式 | `600519.SH` 原样保真，不做大小写/前缀变换 |

### 4.3 查询语义

| 用例 | 断言 |
| --- | --- |
| 区间查询边界 | `[start, end]` **两端闭区间**，与现有 `filter` 语义一致 |
| 排序 | 默认返回按 `(symbol, date)` 升序；显式 `sort` 参数生效 |
| 分页 | `limit/offset` 与现有 SQL 语义一致；`offset` 超过总数返回空而非报错 |
| 列裁剪 | 指定 `columns` 时只返回这些列，顺序与请求一致 |
| 去重口径 | 同 key 多版本时 `FINAL` 与 `argMax` 两条路径**结果一致**（交叉验证） |
| 大范围扫描 | 单票 10 年 + 全市场单日两种查询都不超时（默认 60s 内） |

### 4.4 删除与替换

| 用例 | 断言 |
| --- | --- |
| `replace_daily_rows`（删除部分 symbol 后写） | 目标 symbol 旧行消失、其他 symbol 不变；`mutations_sync=2` 下**读立即可见** |
| 删除不存在的 key | 不报错、无副作用 |
| 删除后重新写入 | 数据恢复，去重仍生效 |

### 4.5 动态列（财务/扩展）

| 用例 | 断言 |
| --- | --- |
| 写 3 个字段，再写含 5 个字段的同 key 记录 | 读回 5 个字段，未提供字段为 NULL |
| 字段全为数值 | 落在 `values` Map；文本/日期串落在 `raw` Map |
| `mapGet` 取单字段 | 与源 DataFrame 对应列逐值一致 |
| 展开为宽表 | storage 层输出的列顺序稳定（按字段定义顺序） |

### 4.6 时区与序列化专项

CK 侧统一 `DateTime64(6, 'Asia/Shanghai')`，但进程时区、容器 TZ、宿主时区可能不一致。这类 bug 极隐蔽，必须专项验证：

| 用例 | 做法 | 断言 |
| --- | --- | --- |
| 进程时区 = Asia/Shanghai | 正常写入分钟数据 | 读回墙钟 = 源数据（09:30 写入即 09:30 读出） |
| 进程时区 = UTC | 同一份数据重跑（`TZ=UTC` 启动测试子进程） | 读回墙钟**与上海时区下完全一致**（不出现 ±8h 漂移） |
| 跨零点边界 | 写入 `15:00:00` 与次日 `09:30:00` | 日期归属正确，不串日 |
| 分钟跨午休 | 11:30 → 13:00 连续数据 | 无补点、无时区偏移 |
| 序列化 | API 返回的 datetime 字符串 | 格式与现有实现一致（ISO，无时区后缀），页面无需改动 |
| `Date` 与 `Datetime` 混用 | 同一响应里同时含两种 | 类型不互相污染（`date` 不被转成 `datetime`） |

## 5. 数据集验收矩阵

每个数据集都要走完"写入 → 读回 → 对应 API 正常"三步。

| 数据集 | 对应功能 | 关键验收点 |
| --- | --- | --- |
| `kline_daily` | K线图、回测、选股 | 单票区间、全市场单日、最新交易日 |
| `kline_daily_enriched` | 看盘主页、指标、策略 | 14 列全保真；最新日加载正确；历史区间可读 |
| `kline_index_daily/_enriched` | 指数页、大盘概览 | 窄表 11 列；与股票表互不污染 |
| `kline_etf_daily/_enriched` | ETF 页 | 同上 |
| `kline_minute` / `kline_etf_minute` | 分时图 | 北京墙钟时刻正确（09:30~15:00）；跨日分区正确 |
| `adj_factor` / `adj_factor_etf` | 复权计算 | `trade_date` 主键；前复权价与原始价关系正确 |
| `instruments` ×3 | 标的列表、搜索、下拉 | 名称/代码/股本/涨跌停价映射正确 |
| `financials_*` ×5 | 财务页、财务因子 | 多期历史保留；同一期多次修正取最新 |
| `depth5` | 连板梯队"真假涨停" | `sealed_up/down` 三态（true/false/null）保真 |
| `ext_timeseries` / `ext_instruments` | 扩展数据页、扩展列 JOIN | 多扩展并存不串；日内多盘取"该时刻或之前最近一盘" |
| `matrix_cache` | 回测矩阵复用 | 命中/未命中行为一致；LRU 淘汰生效 |
| `backtest_runs` / `backtest_trades` | 回测结果页、历史列表 | 结果可查；成交明细完整；重算同 run_id 覆盖 |
| `sync_state` | 同步状态展示 | running/ok/failed 三态正确；重启后残留 running 被标中断 |

## 6. API 接口契约测试（L3.5）

**做法**：真实 HTTP 请求打 FastAPI app（TestClient），真实 CK + 造数，**不 mock 服务与路由**，断言"状态码 + 响应 schema + 数值"。

### 6.1 断言三件套

每个接口测试都必须包含：

```python
r = client.get("/api/kline/daily", params={...})
assert r.status_code == 200
data = r.json()
assert set(data.keys()) == {"symbol", "rows", "meta"}        # ① 响应结构不变
assert data["rows"][0]["close"] == pytest.approx(1234.56)     # ② 数值对得上造数
assert isinstance(data["rows"][0]["date"], str)               # ③ 序列化格式不变
```

### 6.2 接口清单（按功能域）

优先覆盖**读路径**（改造面最大），写路径通过同步服务 API 覆盖。

| 功能域 | 主要接口 | 关键断言 |
| --- | --- | --- |
| 行情 | `/api/kline/daily`、`/api/kline/minute`、`/api/kline/enriched` | 行数、日期区间边界、复权/原始价切换、字段齐全 |
| 概览 | `/api/overview/*`、`/api/intraday/*` | 横截面数字与造数一致；空数据返回空数组而非 500 |
| 标的 | `/api/instruments`、搜索、下拉 | 名称/代码/类型过滤正确；分页总数一致 |
| 自选 | `/api/watchlist/*` | 增删后列表正确；文件存储不受影响 |
| 选股 | `/api/screener/*`（内置 + 自定义 SQL） | 结果集 = SQL 手算结果；危险 SQL 被拒 |
| 财务 | `/api/financials/*` | 多期历史按报告期排序；动态字段展开正确 |
| 扩展数据 | `/api/ext-data/*` | 导入 → 查询 → JOIN 列展开 → 删除 |
| 回测 | `/api/backtest/*` | 结果字段完整；历史列表分页；同 run_id 重跑覆盖 |
| 数据管理 | `/api/data/status`、`/api/data/health` | 统计口径正确（行数/大小）；完整性检查结果符合造数 |
| 同步 | `/api/sync/*`、`/api/financials/sync/*` | 进度推进；完成后 `sync_state=ok` |
| 健康 | `/api/health` | CK ping 正常；degraded 状态可被触发 |

### 6.3 契约守护

- 响应字段集合做**快照比对**（`tests/api_contract/__snapshots__/*.json`）：字段增删必须显式更新快照，防"顺手改字段"破坏页面
- 错误路径同样有断言：无数据 → 200 + 空；参数非法 → 400；CK 故障 → 5xx + 明确错误（不得返回空数据伪装成功）
- **错误响应体结构保真**：状态码 + body 字段（如 `detail` / `message`）+ 中文提示文案的**字段结构**必须与现有实现一致（文案措辞可变，结构不可变），并进快照

### 6.4 SSE 推送契约测试（L3.6）

实时行情、同步进度、回测进度都走 SSE，这是"页面无感"里最容易漏测的部分。

| 用例 | 做法 | 断言 |
| --- | --- | --- |
| 事件格式不变 | 连上 SSE，触发一次数据更新 | 事件类型（`event:`）、数据字段集合、字段类型与改造前一致（快照比对） |
| 首帧即时性 | 连接后立即读第一条 | 不需要业务事件即可收到首帧/heartbeat（与现有行为一致） |
| 断线重连 | 主动断开 → 重连 | 不丢关键事件（重连后能拿到最新状态）；不重复推送已确认事件 |
| 慢客户端背压 | 连接后不读取（模拟卡顿） | 服务端有缓冲上限，超限断开该连接，**不影响其他连接** |
| CK 故障下的 SSE | 断开 CK 后触发推送 | 与现有行为一致（明确报错事件或断开），**不静默发空数据** |
| 长连接稳定性 | 保持连接 5 分钟 + 期间多次写入 | 不断流、不内存增长、`data/` 无残留文件 |

## 7. 场景测试（L4）——多接口组合

单个接口对不代表流程对。以下 7 个场景覆盖"用户真实会走的路径"，每个场景都是**串行调用多个接口 + 跨接口交叉验证**。

### S1 首次使用全流程（空库起步）

```text
空库启动 → GET /api/data/status（各家为 0）
→ POST 同步标的 → GET /api/instruments（数量 > 0）
→ POST 同步日K（固定区间） → GET /api/kline/daily（行数 = 造数）
→ GET /api/sync/status（sync_state = ok，last_ok_at 非空）
→ GET /api/overview（横截面数字正确）
→ POST 加自选 → GET /api/watchlist（含新标的）
```

**断言**：每一步的前置状态可观测；无 500；最终页面所需数据全部可用。

### S2 指标口径四方交叉验证（防"存储换了算法漂了"）

```text
同一交易日、同一标的：
  ① dev 栈：GET dev:3018/api/kline/enriched 的 close / ma5 / turnover_rate
  ② dev 栈：直接查 CK（storage 层）同一行
  ③ origin 栈：GET origin:4018/api/kline/enriched（旧栈 Parquet+DuckDB 作为 oracle）
  ④ 用造数源数据手算期望值
四方必须一致（浮点容差 1e-9；③ 与 ①②④ 不一致时优先怀疑新栈）
```

**为什么必须有**：这是唯一能证明"换存储没换口径"的机制。加上 origin 后，比对从"我算的期望值"升级为"一份真实跑过的参考实现"，能覆盖手算容易漏的口径（复权累计、停牌对齐、除权日边界）。

### S3 复权链路（跨表一致性）

```text
GET /api/kline/daily?adjust=forward  → 前复权价
GET /api/kline/daily?adjust=none     → 原始价
GET /api/adj-factor（除权事件）
断言：前复权价 == 原始价 / 累计复权因子（逐行核对，含除权日前后）
```

### S4 同步中断与恢复

```text
启动同步（大区间） → 中途 kill 后端
→ 重启 → GET /api/sync/status（status = failed，error 含"中断"）
→ 数据校验：不出现"半批数据"（行数 = 0 或完整批）
→ 重新触发同步 → 完成 → 行数完整、无重复行
```

### S5 实时写入可见性（缓存优先路径）

```text
写入实时快照（模拟盘中）→ 立即 GET /api/quote（读缓存，必须拿到新值）
→ 等待 flush 窗口 → 直接查 CK（数据已落库）
→ GET /api/kline/daily（当日行 = 实时值，未被旧数据覆盖）
```

### S6 回测全链路

```text
POST 创建/选择策略 → POST 回测（固定区间）
→ GET 回测结果（stats/曲线/成交明细）
→ GET 回测历史列表（含本次 run）
→ 原参数重跑 → 同 run_id 或新 run（按现有语义）→ 结果覆盖/新增正确
→ GET /api/backtest/{run_id}/trades（明细与 stats.n_trades 一致）
```

### S7 扩展数据端到端

```text
POST 导入 CSV（含 symbol/code 映射）→ GET 扩展数据（行数、字段类型正确）
→ 用扩展列做筛选/JOIN（结果逐值核对）
→ 删除扩展 → GET 返回空，主页面不受影响
```

### S8 并发与一致性

```text
并发 8 线程：4 个持续 GET /api/kline/daily + 2 个持续写入同步 + 2 个 GET /api/overview
断言：无 500、无脏读（读到的行数单调不减、不出现半行）、CK 无 superflous part 告警
```

> S8 用真实多线程打接口，不使用 mock；这是唯一能暴露"缓存与 CK 不一致"的用例。

### S9 数据完整性检查与修复

```text
1. 正常状态：GET /api/data/health → 各项检查通过，缺失项为空
2. 人为删除某日数据（CK DELETE 或跳过写入某交易日）
3. GET /api/data/health → 精确报告缺失日期 + 受影响标的数（不是笼统"数据异常"）
4. 触发修复（POST /api/data/repair 或对应接口）→ sync_state=ok
5. 再次检查 → 通过；行数与删除前一致；无重复行
6. 边界：删除最新交易日 / 删除中间交易日 / 删除单只股票某日
```

**断言**：检查结果"可定位"（日期 + 标的维度），不是只返回一个 boolean。

## 8. 页面冒烟清单（L6，手工）

自动化覆盖不到的渲染与交互，逐项手动确认：

```text
[ ] 1  空库启动：各页面空态正常，无白屏、控制台无报错
[ ] 2  看盘主页：K线、指标切换、成交量、复权切换
[ ] 3  自选/搜索/跳转
[ ] 4  选股器：内置条件 + 自定义 SQL 各一次
[ ] 5  板块/市场概览/涨跌停（重点验横截面 projection 路径）
[ ] 6  连板梯队（sealed 真假涨停）
[ ] 7  财务页
[ ] 8  扩展数据页（导入→展示→删除）
[ ] 9  回测结果页 + 回测历史列表
[ ] 10 实时行情（盘中）：报价刷新、当日 K 线更新
[ ] 11 数据管理页：表大小/行数统计
[ ] 12 重启服务：数据完整、无回退现象
[ ] 13 缓存介质切换（可选）：CACHE_BACKEND=redis 起一次，行为一致
```

## 9. 边界与故障用例（L5）

| 编号 | 场景 | 期望行为 |
| --- | --- | --- |
| F1 | CK 启动时不可达（重试 60s 后） | 服务启动失败，错误信息明确指出 CK 地址（fail-closed） |
| F2 | 运行期查询时 CK 断连 | API 返回 5xx + 明确错误；页面显示错误提示，**不回退空数据** |
| F3 | 写入批次失败（表不存在/磁盘满） | 重试 3 次后丢弃该批、记录 error、`/api/health` 置 degraded |
| F4 | 写缓冲积压（高写入速率） | 队列有上限，超限阻塞或降级并告警；进程内存不持续增长 |
| F5 | 同步中途 kill 进程 | 重启后 `sync_state` 显示"上次同步中断"，数据无撕裂（半批不可见） |
| F6 | 同 key 并发写（两个写线程） | 最终只有一个版本可见（`updated_at` 较新者），不报错 |
| F7 | 查询超时（故意用大范围慢查询） | 达到 `CH_QUERY_TIMEOUT_S` 后取消，返回超时错误而非挂死 |
| F8 | schema 版本落后 | 启动时按顺序执行迁移，成功后 `schema_version` +1 |

### 9.1 大数据量冒烟（规模阈值）

风险 R1（分钟数据体量）必须有具体规模验证，不能只写"不超时"。阈值在 B1 建表后用实测结果标定并回填本表：

| 场景 | 数据规模 | 目标 |
| --- | --- | --- |
| 分钟表写入吞吐 | 100 万行批量灌入 | ≥ 待标定（记实测值，作为基线） |
| 单票分钟查询 | 5000 万行总量下，查单票 1 个月 | < 2s |
| 全市场单日横截面 | 5500 只 × 1 天 | < 5s |
| 单票日K 10 年 | 2500 行 | < 300ms |
| 全市场日K 1 年（回测面板） | 5500 × 242 ≈ 133 万行 | < 15s，且内存不超 2G |
| 财务宽表（动态列展开） | 5000 只 × 40 期 | < 3s |

**判定**：任一场景超阈值 → 记 P1，进 B2/B3 优化（分区、projection、预聚合）后再验；不允许"先放过，后面再说"。

## 10. 缓存层验收

| 用例 | 断言 |
| --- | --- |
| generation 失效 | 写入 enriched 后 `bump` → 旧 generation 的缓存条目**不再被读取**，页面拿到新值 |
| 缓存命中不查库 | 命中时 CK 无查询（用查询日志或 spy 断言） |
| `pool:*` TTL | 未过期不重复拉接口；过期后重新拉取一次 |
| `ai:*` 缓存 | 同输入命中缓存；不同输入互不干扰 |
| 介质可替换 | 同一套测试在 `MemoryCacheBackend` 下通过；接口无 Polars 之外的类型泄漏（为 Redis 实现做准备） |
| 并发安全 | 多线程同时 get/set/bump 不抛异常、不串数据 |

## 11. 通过标准（Gating）

一个批次可进入下一批次，需同时满足：

1. ✅ `uv run pytest tests -q` 全绿（含 L1~L5，包含 API 契约与场景测试）
2. ✅ `uv run ruff check app tests` 无新增告警
3. ✅ `pnpm build` 通过
4. ✅ 本批次涉及的数据集在 §5 矩阵全部通过
5. ✅ 本批次涉及场景（§7）与页面冒烟（§8）对应项完成
6. ✅ 无 P0/P1 缺陷遗留（见 §12）
7. ✅ **回滚演练通过**：`git checkout master` + 重建容器后，旧 Parquet 路径能启动；确认并记录"回滚后需重新同步的耗时"，避免紧急时误判（旧数据已弃用，回滚 ≠ 数据自动恢复）
8. ✅ **双栈对照报告通过**：`reports/cmp_*.json` 显示 dev 与 origin 无未解释差异（§16），差异项均已在验收记录中注明判定依据

全部批次结束后追加：**连续运行 3 天无数据异常**（含跨交易日、跨重启）才认为改造完成。

## 12. 缺陷分级

| 级别 | 定义 | 处置 |
| --- | --- | --- |
| P0 | 数据错误（价格/指标/复权算错）、页面白屏、服务无法启动 | 阻断，立即修复 |
| P1 | 功能不可用（某页面报错、某同步失败）、性能不可接受（页面超 10s） | 阻断进入下一批次 |
| P2 | 交互异常（排序/分页/边界值）、状态显示不准 | 本批次内修复 |
| P3 | 文案、日志、内部命名 | 可延后 |

## 13. 测试数据策略（两档）

### 13.1 CI 档：确定性造数（默认）

小规模、可复现，不依赖网络与数据源 Key：

```text
交易日：固定 2024-01-02 ~ 2024-01-10（9 个交易日）+ 一个跨年边界（2023-12-29）
标的：3 只（600519.SH / 000001.SZ / 510300.SH）
边界样本：
  - 停牌（当日无行）
  - 涨跌停（close == limit_up）
  - 全空字段（limit_up / limit_down / turnover_rate）
  - 除权事件（adj_factor 变动）
  - 分钟数据跨午休（11:30 → 13:00）
  - 重复写入同一 key（验证去重）
```

造数代码 `tests/storage/factories.py`，同一份数据同时用于：契约测试写入、API 断言期望值、场景测试输入 —— 避免三套数据互相漂移。

### 13.2 准生产档：真实数据源抽样（本地，人工触发）

CI 环境没有数据源 Key，真实链路验证放在本地按批次执行一次：

```bash
# 用真实 Key 同步少量标的、短区间，然后跑整套 API/场景测试
TSP_TEST_REAL=1 TICKFLOW_API_KEY=... uv run pytest tests/scenarios -q --real-sample
```

| 抽样范围 | 目的 |
| --- | --- |
| 20 只股票 × 3 个月日K + 除权 | 验证真实数据的字段映射、停牌/新股缺口、复权链路 |
| 3 只股票 × 5 个交易日分钟K | 验证分钟时区、跨午休、分区粒度 |
| 5 只股票 × 2 期财务 | 验证动态列（`Map`）在真实字段集下的展开 |
| 1 份扩展 CSV | 验证扩展数据端到端 |

**要求**：真实抽样跑完后，与造数档的通过项对照，任何"造数通过但真实数据失败"的情况都算 P1 缺陷（说明造数覆盖不足，需补边界样本）。

## 14. 现有测试迁移口径

当前 `backend/tests/` 有 **402 个测试文件**，其中约 60+ 个直接构造 `DataStore(tmp_path)` 做真实文件读写。存储层替换后，这批测试必须分类处置，否则会出现"迁移一半、两边都跑不通"。

### 14.1 分类处置规则

| 类别 | 判定标准 | 处置 |
| --- | --- | --- |
| **A. 纯单元** | 不 import DataStore/KlineRepository，不读写文件 | **原样保留**，零改动 |
| **B. 存储形态测试** | 断言 `.parquet` 文件存在/分区目录结构/`.tmp` 原子写/`os.replace` 重试 | **删除**，由新的契约测试（§4）覆盖同等语义 |
| **C. 存储行为测试** | 构造 `DataStore(tmp_path)` 但断言的是"写入后能读回/去重/并发" | **改写**：fixture 换成 CK `store`，断言逻辑不变 |
| **D. 服务/接口测试** | 依赖上面的存储构造，但断言业务逻辑 | **改写**：仅替换 fixture，其余不动 |
| **E. 文件系统专项** | legacy 数据迁移、备份打包、磁盘统计 | 按设计文档 §8 追溯记录处置（迁移删除、统计改口径） |

### 14.2 执行约束

1. **禁止新增 B 类用例**：改造期间任何新测试不得断言 Parquet 文件存在
2. **迁移按目录分批**：每个批次只迁移本批次涉及数据集的测试，未迁移的允许 `skip` 并标注 `@pytest.mark.skip(reason="storage-migration")`，但**批次结束前必须清空本批次的 skip**
3. **统计口径**：每批次结束时记录 `pytest --collect-only` 的用例总数变化，总数**只允许因 B 类删除而减少**，不允许因改写而减少
4. **禁止 mock 仓储**：C/D 类改写时不得用 mock 替代 CK，必须用真实 `store` fixture（否则测不到真实链路）

### 14.3 迁移完成标志

- `grep -rn "DataStore(tmp_path)" backend/tests` 返回 0 命中
- `grep -rn "read_parquet\|scan_parquet" backend/tests` 仅剩 A 类纯解析用例（如有）
- 上述 skip 标记全部清零

## 15. 验收记录模板

每个批次结束留痕（对应设计文档 §8 的追溯要求），追加到 `./DEP-002-clickhouse-acceptance-log.md`：

```markdown
## B2 主干（2026-11-XX）

- 执行人：
- 代码范围：commit 区间 / 涉及数据集
- 测试结果：pytest 通过 X / 跳过 Y（原因）/ 失败 Z
- 数据集矩阵：通过 n / 共 m（列出未通过项）
- 场景测试：S1 ✅ S2 ✅ S3 ⬜（说明原因）
- 性能冒烟：见 §9.1 实测值表（附测量时间与机器）
- 遗留缺陷：P1 x 项 / P2 y 项（链接 issue）
- 回滚演练：未做 / 已做（耗时记录）
- 结论：**通过 / 有条件通过（列条件）/ 不通过**
```

**规则**：没有记录的批次视为未验收，不得进入下一批次。

## 16. 双栈对照校准（origin 作为 oracle）

`origin` 环境（同仓库旧栈：Parquet + DuckDB，容器 `tsp-origin`，端口 4018/4011）保留一份可运行的参考实现，用于**运行时校准**，而不是仅作代码 diff。这是本次改造最重要的质量保障手段。

### 16.1 两套栈

| 栈 | 地址 | 存储 | 用途 |
| --- | --- | --- | --- |
| dev | `localhost:3018`（API）/ `3011`（页面） | ClickHouse | 待验证的新实现 |
| origin | `localhost:4018`（API）/ `4011`（页面） | Parquet + DuckDB | 参考实现（oracle），代码不变、数据独立 |

两套**数据目录互相独立**（`tick-stock-panel/data` vs `tick-stock-panel-origin/data`），各自同步、互不影响。

### 16.2 三种对照方式

| 方式 | 做法 | 适用 | 确定性 |
| --- | --- | --- | --- |
| **A. 同造数双写**（CI 首选） | 造数脚本同时写入 origin 的 Parquet 目录与 dev 的 CK 测试库 | L2/L3 回归 | 完全确定，可重复 |
| **B. 同源同步对照** | 两边各自从数据源同步**同一固定区间**（收盘后执行，避免盘中漂移） | 真实数据校准 | 较高，但受源侧增量影响，需固定日期边界 |
| **C. 快照导入对照** | 把 origin 已有的真实 Parquet（如现有 338MB 日K/enriched）导入 CK **测试库** `tsp_test` | 真实数据形态验证 | 高（同一份字节） |

> C 不违反"数据不迁移"：迁移的是**测试库中的测试样本**，生产库 `tsp` 仍从零同步。

### 16.3 比对工具与口径

脚本 `scripts/cmp_stacks.py`（B2 阶段实现）：

```bash
uv run python scripts/cmp_stacks.py \
  --dev http://localhost:3018 --origin http://localhost:4018 \
  --endpoints kline,daily,enriched,adj_factor,financials,screener \
  --range 2024-01-02:2024-01-10 --out reports/cmp_b2.json
```

比对口径（逐项输出差异明细，不只给"通过/失败"）：

| 维度 | 判定 |
| --- | --- |
| 行数 | 必须相等 |
| 主键集合 | `(symbol, date)` 集合完全一致（缺行/多行都要列出） |
| 数值列 | 相对误差 ≤ 1e-9；`NaN`/`null` 位置必须一致 |
| 排序 | 返回顺序一致（页面依赖顺序） |
| 字段集合与类型 | 完全一致（含 `date` 是字符串还是数字） |
| 边界 | 区间两端、停牌日、涨跌停日、除权日逐个核对 |

### 16.4 差异判定规则（重要）

**origin 是参考实现，不是真理**。发现差异时按以下顺序判定：

1. 先查造数/同步是否真的一致（数据源侧差异 → 不算缺陷）
2. 再查 origin 是否有已知缺陷（查 issue / 历史测试）—— 已知缺陷不能作为基准
3. 都不成立 → 判为新栈缺陷（P0/P1），并与手算期望值交叉确认
4. 差异结论必须写进验收记录（§15）的"对照差异"栏，注明判定依据

### 16.5 页面级对照（手工）

同 §8 冒烟清单，但**两边同时打开**（`3011` vs `4011`）逐项比对：数值、图表形状、排序、空态文案。重点看：

- K线（含复权切换）
- 看盘主页指标（MA/EMA/MACD/BOLL/KDJ/ATR/RSI）
- 市场概览/板块/涨跌停计数
- 选股器结果条数与列表
- 回测结果页（曲线形状、统计数字、成交明细条数）

### 16.6 局限与注意事项

- origin 用的是**同一份代码的历史版本**，业务逻辑若已演化会存在合理差异（需先确认版本一致：`git -C tick-stock-panel-origin log -1`）
- 两边同步时间不同会导致"最新交易日"不同 → 比对必须显式指定区间，不比对"最新"
- 实时数据（盘中）无法对照（两边轮询时点不同），只做"写入后读回"的一致性验证
- origin 不接 CK、不改代码；它只作为只读基准，任何时候都不用它做开发
