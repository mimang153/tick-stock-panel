# CH-07 页面级双栈接口对照执行记录

> 文档类型：部署 | 编号：DEP-004 | 状态：已执行（待项目方判定） | 文档版本：v1.0 | 最后更新：2026-11-21
> 负责人：待分配 | 关联：DEP-003（CH-07 / T6.7）、ISS-005 §1 CH-07、DES-006
> 归档数据：`../reports/cmp_ch07_api_stacks.json`、`../reports/cmp_ch07_api_all_pages.json`

## 1. 执行目的与环境

按 DEP-003 要求，用自动化脚本对 origin（旧栈，oracle）与 dev（分布式新栈）的只读 GET 接口做结构化对照，验证「换存储/分布式改造没换 API 口径」。

| 项 | origin（oracle） | dev（新栈） |
| --- | --- | --- |
| 地址 | `http://localhost:4018` | `http://localhost:3018` |
| 认证 | 免登录 | 会话 Cookie + Bearer Token |
| 数据源 provider | `fuyao` | `tickflow` |
| 数据日期 | 至 2026-11-19 | 至 2026-11-16 |

执行日期：2026-11-21。对照窗口：`2026-11-12 ~ 2026-11-13`。

## 2. 执行前置处理

1. dev 容器原为 2026-11-20 早期创建，缺少 `USER_STATE_PG_URL` 注入，后端 lifespan 连 `localhost:5432` 失败、服务不可用。经 `docker compose -f docker-compose.dev.yml up -d --force-recreate dev` 重建后注入 `host.docker.internal:5432`，服务恢复。
2. 对照脚本初版把 K线/指数用例的日期参数写成 `start`/`end`，但两栈接口签名均为 `start_date`/`end_date`（FastAPI 忽略未知参数，回落默认 120 天窗口），导致窗口错配的假差异。已修正 `backend/scripts/cmp_api_stacks.py` 与 `cmp_api_all_pages.py` 对应 3 处用例参数名并重跑。

## 3. 执行结果

| 脚本 | 一致 | 差异 | 阻塞 | 报告 |
| --- | --- | --- | --- | --- |
| `cmp_api_stacks.py`（固定 8 用例） | 4 | 4 | 0 | `cmp_ch07_api_stacks.json` |
| `cmp_api_all_pages.py`（全页面 97 用例） | 57 | 40 | 0 | `cmp_ch07_api_all_pages.json` |

全页面 40 个差异按页面分布：全局 9、数据 5、财务 5、自选 3、选股 3、指数 3、看板 2、异动 2、回测 2、设置 3、因子 1、复盘 1、K线 1、个股分析 1。

## 4. 差异归类与排查结论

### 4.1 已确认的环境/数据差异（非 API 契约回归）

- **财务 5 个接口 403**（origin 200 / dev 403）：dev 返回 `capability not available: financial`。根因为两栈 provider 不同：origin 用 `fuyao` 探测到 `financial`/`adj_factor` 能力，dev 用 `tickflow` 只探测到 3 项基础能力（`capability_cache` 中 `missing_caps=[]`，即 provider 本身不提供），非代码缺陷。若要消除需在 dev 切换 provider 或补齐 tickflow 的 financial 能力，属配置决策。
- **数值/行数差异**（K线、指数、异动、复盘、market_snapshot、mining_availability、个股分析等）：两栈数据日期不同（origin 11-19 / dev 11-16）且 provider 不同，同一请求区间底层数据不一致，逐行比对必然有差异。
- **用户态配置差异**（preferences、watchlist、watchlist_groups、strategy_pool、sound_packs、market_recap_prompt、mining_lock、auth_status 等）：两栈各自独立状态，非契约问题。

### 4.2 已确认的 API 契约差异（需评审）

- **指数 `index_info` 字段结构不一致**：`index_info` 由 `_index_info()` 直接返回 `repo.get_index_instruments()` 的一行 dict，代码未做字段裁剪，字段集完全由 instruments 数据源决定。
  - origin：`asset_type`/`code`/`name`/`symbol`（4 字段）
  - dev：`as_of`/`code`/`exchange`/`float_shares`/`limit_down`/`limit_up`/`listing_date`/`name`/`region`/`symbol`/`tick_size`/`total_shares`/`type`（13 字段，无 `asset_type`）
  - 影响：`/api/index/daily`、`/api/index/minute` 的 `index_info`；dev 新增丰富的 instrument 元数据但丢失 `asset_type`。若前端或其他调用方依赖 `asset_type`，需在 dev 侧补齐或按 DEP-003 §1.3 归一后判定为允许差异。此项需产品/架构评审确认是否接受。
- **`regime` 类差异**：`regime_history`/`regime_phases`/`regime_mainline` 用例参数名为 `start`/`end`（与接口签名一致，非脚本问题），差异来自数据窗口未覆盖，属数据差异。

## 5. 判定与后续

- 本轮对照未发现因 DES-006 分布式改造导致的 API 路由/字段契约回归；唯一需评审的契约差异是指数 `index_info` 字段集，且源于 instruments 数据源结构而非本次改造代码。
- CH-07 当前状态：自动化对照已执行并归档，差异已逐项归类。**是否判定闭环需项目方确认**：(1) 接受 provider/数据日期造成的环境差异；(2) 对 `index_info` 字段差异作出接受或补齐决策。
- 后续变更走 REQ-002 §12 变更控制；本报告不改动 DEP-001 的「有条件通过」结论。
