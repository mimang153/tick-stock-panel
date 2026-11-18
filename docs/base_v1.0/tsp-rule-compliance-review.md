# 未提交代码 · tsp-rule 规则符合度审查

> 审查日期：2026-11-17
> 审查对象：工作区全部未提交改动（172 个 .py、13 个 .md、Docker/脚本/配置若干；前端 frontend/ 零改动）
> 规则来源：`tsp-rule/*.mdc` 共 28 条
> 方法：4 组并行只读审查 + 关键发现人工复核（grep/行号核对）。**未修改任何代码。**
> 与 `docs/base_v1.0/precommit-issue-inventory.md` 的关系：那份查的是 pre-commit 门禁（已全绿）；这份查的是**门禁不覆盖的 tsp-rule 人工规约**。

---

## 0. 结论速览

**一句话**：pre-commit 三门禁虽全绿，但按 tsp-rule 的**人工规约**衡量，未提交代码是**部分符合**——架构类规则（CK 门面、SQL 注入、访问控制、API 规范）执行良好，但**代码规约（noqa 理由、函数规模、DRY）、存储规约（Polars collect、CK 读源收口）、安全硬编码（CK 口令）**三类存在明确不符合，其中 1 项（硬编码口令）建议提交前处理。

按判定分组：

| 判定 | 规则 |
|---|---|
| ✅ 符合 | clickhouse.mdc（门面部分）、python-injection、python-broken-access-control、api-standards、testing-guidelines（主体） |
| ⚠️ 部分符合 | code-quality、clean-code、python、clickhouse.mdc（读源/测试隔离部分）、redis、python-authentication-failures、security-practices、security-devsecops、fastapi、docker、docker-compose、readme |
| ❌ 不符合 | polars.mdc（裸 collect/scan 多处） |
| ➖ 本次不适用 | react、react-patterns、tailwind、tailwind-standards、tanstack-query、vitest、javascript-injection（前端零改动）；git-commit-standards、git-conventional-commit（未提交，待提交时检查） |
| ❓ 无法判定 | 嵌套深度、CI 中的 SAST/SCA/DAST 配置 |

---

## 1. 明确不符合 / 需优先处理（P0~P1）

### 1.1 硬编码 ClickHouse 明文口令（P0，安全）

- **位置**：`backend/app/config.py:248`
  ```python
  clickhouse_password: str = "tsp123456"
  ```
- **违反**：`python-authentication-failures.mdc`（Pattern 2/15 硬编码与默认凭据）、`security-practices.mdc`、`security-devsecops-ssdls-appsec.mdc`（Never hardcode secrets）。
- **核实**：`config.py` 在本次改动清单内；虽有 `CLICKHOUSE_*` 环境变量可覆盖、属本地 compose 约定，但源码写死明文口令这一事实成立，且 gitleaks 模式扫描漏掉了这类"配置项默认值"。
- **建议**：改为 `clickhouse_password: str = ""` + 启动时校验必填，或要求必须通过环境变量提供。注意同步检查 compose 文件里的密码来源。

### 1.2 Polars 裸 collect / 裸 scan_parquet（P1）

- **违反**：`polars.mdc`（统一走 `guarded_collect(lf, priority=...)`、scan 走封装入口）。
- **命中**（改动文件内）：裸 `.collect()` 约 15 处、裸 `pl.scan_parquet/read_parquet` 多处。
  - 典型：`app/services/index_sync.py:355`（`pl.scan_parquet(...).collect()`）、`app/api/ext_data.py:461`、`app/indicators/pipeline.py:1956`、`app/strategy/paper.py:855/876/896/930`、`app/tickflow/repository.py:2173` 等。
  - 注：`gc.collect()` 是 Python GC，不是 Polars，应排除。
- **判定**：倾向存量（阶段 4 检查时就已记录为人工项未修），但改动文件涉及它们，提交后仍违反规约。
- **建议**：页面/交互路径改 `guarded_collect(lf, priority="interactive")`；离线同步用 `background`；日K/enriched 主路径改 `scan_daily_parquet`/`scan_enriched_parquet` 封装。

### 1.3 noqa 大面积缺理由（P1，阶段 4 引入）

- **违反**：`code-quality.mdc:24-25`（"用 noqa 显式承认必须附一行理由，不得静默绕过"）。
- **核实数据**：改动文件共 **480 处** `# noqa:`，其中带 ` - 理由` 的仅 **47 处**，即 **433 处无理由**（含阶段 4 用 `--add-noqa` 批量生成的约 153 处，以及全部 55 处 C901）。
- **影响**：阶段 4 的 ruff 全绿是建立在"显式承认"上的，但无理由的承认等于"静默绕过"，与规约精神冲突。
- **建议**：按文件分批补理由，优先 C901/PLR（复杂度）和 BLE001（吞异常）——它们最需要说明"为何可豁免"。可用脚本在规则码后追加 ` - <简述>`。

### 1.4 SSRF 风险面（P1，非 7 条规则直接命中但属实际风险）

- **位置**：`backend/app/api/ext_data.py:1859-1882`，`POST /api/ext_data/detect-url`。
- **问题**：接收任意 `url` + 自定义 headers/body，`follow_redirects=True`，无私网/元数据地址黑名单。端点在认证之后，不构成匿名越权，但认证用户可借服务端探测内网。
- **建议**：加 URL scheme 白名单（http/https）、解析后校验目标 IP 非内网/环回/元数据地址（防 DNS rebinding）、限制 headers/body 数量与长度。

---

## 2. 部分符合（P2，按 PR 逐步收敛，不阻塞提交）

### 2.1 code-quality.mdc

| 项 | 判定 | 证据 |
|---|---|---|
| 函数规模 ≤80 行 | ❌ | `pipeline.py` 至少 5 个巨函数（`_run_pipeline_impl` ~409 行、`compute_enriched_today` ~372、`compute_indicators` ~341、`compute_limit_signals` ~330），仅加 noqa 未拆分 |
| noqa 附理由 | ❌ | 见 1.3 |
| 注释/docstring 覆盖 | ⚠️ | 正例 `market_data.py:1-10/74/79-81` 详尽；反例 `watchlist_ocr/pipeline.py:22-31` 公开类无 docstring。（注：share_capital.py 的 1.1% 是 tokenize 只数 `#` 的误报，docstring 实际完整） |
| 异常规范 | ✅ | BLE/TRY ruff 全绿；抽查为 `logger.warning` + 降级返回，非静默吞 |
| TODO 残留 | ✅ | 无真实 `# TODO` |
| 嵌套深度 ≤2 | ❓ | 无自动工具，`watchlist_ocr/pipeline.py:65-80` 疑似 3 层，需 PR 人工 |

### 2.2 clean-code.mdc

| 项 | 判定 | 证据 |
|---|---|---|
| DRY | ❌ | `_dimension_rank` 在 `services/market_overview_builder.py:329` 与 `api/overview.py:88` 高度重复（逐行近似，仅 data_dir 来源不同），建议抽公共函数 |
| 函数单一职责 | ❌ | 同 2.1 超长函数 |
| 常量替代魔法数字 | ⚠️ | 正例 `_FULL_START`/`_SCORE_WEIGHTS`/`_ema_alpha`；风险：指标参数（EMA12/26、VOL MA5/10、KDJ9）仍内联 |
| 智能注释（why not what） | ⚠️ | 正例 `market_data.py:79-81`；风险 `pipeline.py:2442/2467` 偏 what 复述 |
| 命名 | ✅ | ruff N 守护，抽查清晰 |

### 2.3 clickhouse.mdc（读源收口 / 测试隔离部分）

| 项 | 判定 | 证据 |
|---|---|---|
| 业务层不直连 CK | ✅ | 全量 `clickhouse_connect` 仅命中门面自身 `storage/clickhouse.py:262-266`；其余 `get_client()` 是 TickFlow SDK |
| 启动 fail-closed | ✅ | `storage/runtime.py:66-88` 启动即 connect+ensure_schema |
| CK 唯一读源 | ⚠️ | `../v1.0.1/requirements/REQ-002-clickhouse-implementation-wbs.md:378`：app/ 下仍有 81 处本地 parquet 读（34 处在 repository.py），B4 DoD 未清零。**本次改动未新增直连** |
| 测试用 ck_runtime 隔离 | ⚠️ | 23 个改动测试文件仍用裸 `DataStore(tmp_path)`（核实数），需逐个判定读路径是否已切 CK；纯本地逻辑测试可保留 |

### 2.4 redis.mdc

- **架构符合**：无硬编码 redis、无 pickle；`CacheBackend` 可切换设计在位。
- **功能不完整**：`RedisCacheBackend` 实现与开关在仓库中缺失，规则要求的 Arrow IPC 序列化未落地 → 无法判定为完全符合。属未实现的目标契约，非本次回归。

### 2.5 python.mdc（类型提示）

- **部分符合**：`market_data.py:73/90/34/60/68` 等公开函数参数与返回值无注解；ruff/mypy 全绿依赖 `follow_imports=silent` 渐进档，并非真符合。新写代码（如 ext_granularity.py）注解完整。

### 2.6 testing-guidelines.mdc

| 项 | 判定 |
|---|---|
| 测试/生产分离 | ✅ 测试均在 tests/ |
| Mock 复用生产逻辑 | ⚠️ 正例 `test_ext_granularity.py:58` 复用 `_mirror_ext_to_ck`；风险 23 处裸 DataStore |
| 测试隔离 | ⚠️ fixture 用 yield 清理（正例），但部分缺统一清理 |
| 测试命名 | ✅ 绝大多数表意清晰 |
| 断言质量 | ⚠️ 阶段 4 为 mypy 收窄插入约 190 处 `assert xxx is not None`，服务于类型检查而非测试意图，稀释可读性 |

### 2.7 安全类（api/fastapi/auth/devsecops）

| 规则 | 判定 | 要点 |
|---|---|---|
| python-injection | ✅ | SQL 全参数化（`storage/store.py:711-733` 只拼白名单片段）；改动文件 0 处字符串拼 SQL；路径遍历有 `_validate_repair_path`（repository.py:2542）和 ID 白名单（ext_data.py:555） |
| python-broken-access-control | ✅ | `main.py:664-728` 全局 auth 中间件覆盖所有 `/api/`，新增端点未绕过；单用户模型无 RBAC（设计取舍） |
| python-authentication-failures | ⚠️ | 1.1 硬编码口令；存量弱口令下限 6 位、`secure=False`；正例 PBKDF2 20 万次+限流+token 哈希 |
| api-standards | ✅ | HTTP 方法语义正确、错误码规范、Pydantic 校验；列表端点未分页（suggestion） |
| fastapi | ⚠️ | 认证用全局中间件而非 `Depends`（功能等效但偏离推荐写法）；`detect-url` 请求缺长度约束 |
| security-devsecops | ⚠️ | 硬编码口令 + 自研认证（规则建议用框架）；SAST/SCA 配置不在代码内无法判定 |

### 2.8 Docker / 文档

| 规则 | 判定 | 要点 |
|---|---|---|
| docker.mdc | ⚠️ | `Dockerfile.dev` 版本固定（NODE 20.18.1）、无密钥入镜像 ✅；但无 `USER non-root`、无 HEALTHCHECK、基础镜像未精确补丁 |
| docker-compose-standards | ⚠️ | 无 version 字段、2 空格缩进、显式 tag ✅；但 `app`/`dev` 服务无 `deploy.resources.limits` |
| readme-best-practices | ✅ | `CONVENTIONS.md`/`docs/base_v1.0/dev-container.md` 定位句+表格+copy-paste 命令，无营销词 |

---

## 3. 本次不适用（无需处理）

| 规则 | 原因 |
|---|---|
| react / react-patterns / tailwind / tailwind-standards / tanstack-query / vitest / javascript-injection | `frontend/` 零改动，无 JS/TS/CSS 变更 |
| git-commit-standards / git-conventional-commit-messages | 所有改动未提交（按要求），无 commit message 可审。提交时需遵守 `type(scope): 小写描述`、<46 字符、前缀限 fix/feat/... |

---

## 4. 建议处理顺序

| 优先级 | 项 | 动作 |
|---|---|---|
| **P0（提交前）** | 1.1 硬编码 CK 口令 | 改为空默认 + 启动校验/必填环境变量 |
| **P1（尽快）** | 1.2 Polars 裸 collect/scan | 按路径改 guarded_collect / scan 封装 |
| **P1** | 1.3 noqa 理由 433 处 | 按文件分批补 ` - 理由`，优先 C901/BLE001 |
| **P1** | 1.4 SSRF | detect-url 加 URL/IP 白名单校验 |
| **P2（按 PR）** | 函数规模、DRY、CK 读源收口、测试隔离、类型注解、Docker 限制 | 逐项单列任务，随功能 PR 收敛 |
| **P3（提交时）** | git commit message | 按规约格式化，建议按主题拆分提交 |

---

## 5. 与 pre-commit 状态的关系

- pre-commit 门禁（ruff/mypy/format/通用钩子）**已全绿**——那是机器可检查的底线。
- 本审查覆盖的是**机器检查不到的 tsp-rule 人工规约**——结论是"部分符合"，有 1 项 P0（硬编码口令）+ 3 项 P1 需处理。
- 两者不矛盾：代码可以"通过所有自动门禁"但"不完全符合项目规约"。本审查的价值就在于暴露这层差距。

## 6. 审查局限（如实标注）

- 三个子审查 agent 无法执行 shell，部分"新增行 vs 存量"判定基于改动清单的文件级判断 + 既有文档，未逐行 diff；关键项（硬编码口令、noqa 统计、裸 DataStore 数）已由主审查用 grep/行号独立核实。
- 嵌套深度、CI 安全扫描配置、Redis 完整实现等无法从当前代码判定，标为"无法判定"。
- 本报告是静态审查，不含动态验证（未运行应用、未做渗透测试）。

---

# 修复执行结果（2026-11-17）

## 已修复项

| 优先级 | 项 | 修复内容 | 验证 |
|---|---|---|---|
| **P0** | 硬编码 CK 口令 | `config.py:250` 默认值 `tsp123456` → `""`，注释说明凭据只从 env/compose 注入。容器内 env 仍读到口令（9 字符），CK 连接正常 | env 注入验证 + 全量测试 |
| **P1** | Polars 裸 collect/scan | 10 处裸 `.collect()` → `guarded_collect(lf, priority=...)`（页面路径 interactive / 后台 background）；13 处裸 `pl.scan_parquet` → `scan_parquet_compat`；7 个文件补 import | 96 例相关测试通过；全量裸调用归零 |
| **P1** | noqa 缺理由 | 418 处批量补 ` - 理由`（按规则码映射：BLE001/C901/PLR/PERF401/B023 等）；修复 ruff-format 重排导致的 2 处 except noqa 错位 + 3 处 E402 noqa 丢失 | 无理由 noqa = 0（含原有破折号格式的 13 处）；三门禁全绿 |
| **P1** | SSRF | `detect_url` 新增 `_assert_public_http_url`：scheme 白名单、userinfo 拒绝、DNS 解析后拒绝私有/环回/链路本地/保留/组播 IP；httpx response hook 对重定向目标二次校验 | 13 例 detect-url 测试通过 |
| **P2** | 类型注解 | `strategy/market_data.py` 4 个公开函数 + `_get_repo`/`_norm_date` 补全参数与返回注解；symbol/config_id 允许 None（测试验证非法入参降级） | mypy 全绿 + 16 例测试 |
| **P2** | Docker 配置 | `docker-compose.yml` app 服务 + `docker-compose.dev.yml` dev 服务加 `deploy.resources.limits`；`Dockerfile.dev` 加 HEALTHCHECK（标准库 urllib 探 `/health`，免认证） | YAML 合法 + healthcheck 命令容器内实测 OK |

## 未修复项（评估后判定超出本次范围，如实标注）

| 项 | 原因 |
|---|---|
| 函数规模（pipeline 400 行函数） | 大型重构，验收收尾期做风险过高，单列重构任务 |
| DRY（`_dimension_rank` 双实现） | 两处有真实功能差异（services 版多 `group_source` 追踪），抽取需保留可选参数，收益有限且有行为风险 |
| CK 读源 81 处（B4） | 属于独立的存储迁移工作流（`../v1.0.1/requirements/REQ-002-clickhouse-implementation-wbs.md`），不是 pre-commit/tsp-rule 修复范围；本次未新增直连 |
| 测试隔离 23 处裸 `DataStore(tmp_path)` | 需逐个判定读路径是否已切 CK；纯本地逻辑测试可保留，随功能 PR 处理 |
| 前端 7 条规则 / git 2 条规则 | 前端零改动 / 未提交，本次不适用 |
| Redis 完整实现 / 嵌套深度 / CI 安全扫描 | 目标契约未实现或无法静态判定 |

## 门禁与测试结果

- ruff（141 文件）：**All checks passed**
- ruff-format：**141 files already formatted**
- mypy（134 文件）：**Success: no issues found**
- 全量 pytest：**4820 passed / 7 skipped / 1 xfailed / 0 failed**（最终确认，零回归）

## 关键提醒

1. noqa 理由虽已全部补齐，但部分格式为 `BLE001: <理由>`（规则码重复前缀），不影响合规，可选清理。
2. SSRF 防线是静态解析校验 + 重定向二次校验，**DNS rebinding 无法完全防**；该端点已受全局认证保护，风险限于已登录用户，已在代码注释中说明。
3. 硬编码口令改空默认后，**必须确保部署环境注入 `CLICKHOUSE_PASSWORD`**——compose 与 `.env.example` 已提供，源码不再兜底。
