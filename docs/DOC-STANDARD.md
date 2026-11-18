# 项目文档管理规范（DOC-STANDARD）

> 文档类型：指南 | 编号：DOC-STANDARD | 状态：基线 | 版本：v1.0 | 最后更新：2026-11-18
> 适用：本项目（tick-stock-panel / TickFlow）所有自建文档。
> 本规范自文档版本 **v1.0.1** 起生效；此前的存量文档按 `base_v1.0` 基线归档，不追溯改名。

---

## 1. 目的

统一 AI 辅助开发下**需求、设计、决策、部署、问题、指南**文档的**存放位置、命名、模板、版本与生命周期**，避免文档散落、重名、失控、无法追溯。

> 规划/路线图、质量/测试闸门类内容统一归入**需求**文档（`REQ`），不再单设目录与类型。

## 2. 存放位置（两档并行）

本项目文档分两档存放，以"是否采用新标准"为界：

```
docs/
├── DOC-STANDARD.md        # 本规范（跨版本共享，唯一留在 docs/ 根的自建规范）
├── base_v1.0/             # 【存量档】此前全部文档，原名归档，只读
│   ├── features.md        #   文件名、内容均不改，仅搬位置
│   └── ...（37 份）
├── v1.0.1/                # 【新标准档】本规范生效后的新文档，按类型 + 命名规范存放
│   ├── requirements/      #   需求（What，含规划/质量类内容）→ REQ-xxx
│   ├── design/            #   设计（How：架构/模块/接口/数据模型）→ DES-xxx
│   ├── adr/               #   架构决策记录 → ADR-xxx
│   ├── deploy/            #   部署 / 运维 / 环境 → DEP-xxx
│   ├── issues/            #   问题 / 缺陷 / 风险 → ISS-xxx
│   └── guides/            #   开发 / 使用 / 操作指南 → GUI-xxx
├── v1.0.2/                #   后续版本：从上一版本复制后编辑（见 §6.2）
└── examples/              #   组件联调示例，跟随代码原位
```

**规则**：
- `base_v1.0`：采用本规范之前已存在的文档，**原名、原内容归档，不重命名、不重分类、只读**。它记录"规范化之前的文档原貌"，仅供追溯。
- `v1.0.1` 及以后：新写、大改的文档按本规范命名归类，放入**当前版本目录 + 对应类型子目录**；禁止散落在仓库根或其他目录。
- 文档规范的版本从 `v1.0.1` 起算（与代码版本 `v0.3.37` 等无关）；`base_v1.0` 中的 `v1.0` 表示"基线"，不是代码版本。

## 3. 命名规范

### 3.1 文件名

`<TYPE>-<NNN>-<slug>.md`

| 部分 | 说明 |
|------|------|
| `TYPE` | 类型前缀，见下表 |
| `NNN` | 三位编号，按类型**独立**递增（001、002…） |
| `slug` | 英文 kebab-case 简短描述 |

- 文件名用**英文**（跨平台/避免编码问题）；**内容用中文**。
- 一个主题一个文件；不要把多个主题塞进一个文件。
- 此规则仅适用于 `v1.0.1` 及以后；`base_v1.0` 内保留原文件名。

### 3.2 类型前缀

| 类型 | 前缀 | 目录 | 用途 |
|------|------|------|------|
| 需求 | `REQ` | `requirements/` | 要做什么、验收标准（含规划/质量类内容） |
| 设计 | `DES` | `design/` | 怎么做：架构/模块/接口/数据模型 |
| 决策 | `ADR` | `adr/` | 架构决策及其理由 |
| 部署 | `DEP` | `deploy/` | 环境/部署/回滚/验证 |
| 问题 | `ISS` | `issues/` | 现象/根因/方案/状态 |
| 指南 | `GUI` | `guides/` | 开发/使用/操作步骤 |

**示例**：
- `REQ-001-secondary-dev-requirements.md`
- `DES-001-project-structure.md`
- `ADR-001-sqlite-first-storage.md`
- `ISS-001-token-leak.md`

## 4. 文档头部元信息（每份必备）

`v1.0.1` 及以后的每份文档开头必须有元信息块：

```markdown
> 文档类型：需求 | 编号：REQ-001 | 状态：草案 | 文档版本：v1.0 | 最后更新：2026-11-18
> 负责人：—
> 关联：ADR-001、DES-001
```

| 字段 | 取值 |
|------|------|
| 文档类型 | 需求 / 设计 / 决策 / 部署 / 问题 / 指南 |
| 编号 | 对应文件名前缀+编号（如 REQ-001） |
| 状态 | 草案 / 评审中 / 基线 / 已废弃 |
| 文档版本 | v<主>.<次> |
| 最后更新 | YYYY-MM-DD |
| 负责人 | 姓名或 — |
| 关联 | 其他文档编号 |

## 5. 文档模板（各类型必备要素）

### 5.1 需求（REQ）
背景与目标 → 范围（做什么/不做什么）→ 需求条目（编号 + 描述 + 优先级 + 验收标准）→ 非功能需求 → 约束 → 待决事项。

### 5.2 设计（DES）
背景与目标 → 方案（架构 / 模块 / 接口 / 数据模型）→ 备选方案与取舍 → 风险 → 验收 / 测试点。

### 5.3 决策（ADR）
背景（Context）→ 决策（Decision）→ 备选方案 → 理由 → 影响 / 后果 → 状态。

### 5.4 部署（DEP）
环境要求 → 部署步骤 → 配置项 → 验证 → 回滚。

### 5.5 问题（ISS）
现象 → 复现步骤 → 影响 → 根因 → 解决方案 → 当前状态。

### 5.6 指南（GUI）
目的 → 前置条件 → 操作步骤 → 示例 → 常见问题。

## 6. 版本与生命周期

- **状态流转**：草案 → 评审中 → 基线 → 已废弃。
- **文档版本号**：结构性变更升主版本（v1.0→v2.0）；内容修订升次版本（v1.0→v1.1）。
- **变更**：每次修改同步更新头部"文档版本"和"最后更新"。
- **废弃不删**：标记"已废弃"并保留，便于追溯。

### 6.1 编号 / 文档版本 / 项目版本（三者区分）

| 概念 | 作用 | 规则 |
|------|------|------|
| **文档编号** | 文档的**唯一身份**，用于引用、索引、追踪 | `TYPE-NNN`，创建后**永不改变** |
| **文档版本** | 该文档**内容的修订号** | `v<主>.<次>`，随修改递增，记录在文档头 |
| **代码版本** | **代码发布版本** | `vX.Y.Z`，由 `VERSION` 与 git tag 管理（当前 `v0.3.37`）；与文档规范版本相互独立 |

要点：**编号 ≠ 版本**——编号回答"是哪份文档"，版本回答"是第几版内容"。二者独立，都要有。

### 6.2 版本目录规则（按文档规范版本分文件夹）

- 当前版本目录（如 `v1.0.1/`）可编辑；发布后冻结为只读，新版本从当前复制出新目录（如 `v1.0.2/`），只在新目录编辑。
- 版本间差异用 git diff 比对：`git diff <old-tag> <new-tag> -- docs/v1.0.1`。
- `base_v1.0` 永久只读，不参与版本迭代。

## 7. 引用规范

- 文档间交叉引用用**编号**：如"见 REQ-001 §4.1"、"见 ADR-001"。
- 引用 `base_v1.0` 中的存量文档用相对路径：`../base_v1.0/xxx.md`。
- 引用代码注明文件路径与行号。
- 移动/归档文档时必须同步修复全仓引用，确保无断链。

## 8. AI 生成文档的额外约定

- AI 生成的每份文档**必须**带头部元信息（类型/编号/状态/文档版本/日期）。
- 新增文档**必须**放进当前版本目录的对应类型子目录。
- 修改现有文档必须更新文档版本与日期。
- 不得把临时结论、草稿写进仓库；正式结论才落文档。

## 9. 存量归档与 v1.0.1 落档（已执行 2026-11-18）

### 9.1 base_v1.0 存量归档（原名，30 份）

采用本规范前已存在的文档，**原名归档**到 `docs/base_v1.0/`，文件名与内容不变，仅位置变化。其中 7 份已按本规范落到 v1.0.1（见 9.2），余 30 份保留在 base_v1.0：

| 类别 | 归档文档（原名） |
|------|------------------|
| 使用与配置 | `操作说明书.md`、`features.md`、`configuration.md`、`deployment.md`、`deploy-password.md`、`dev-container.md` |
| 开发扩展 | `secondary-development.md`、`plugin-development.md`、`custom-data-source.md`、`external-data-source-packages.md`、`extension-packages.md`、`page-sdk.md` |
| 策略与挖掘 | `strategy.md`、`strategy-iteration.md`、`market-phase.md`、`mining.md`、`paper-trading-plan.md`、`auto-strategy-lab-plan.md` |
| 架构与设计 | `factor-system-design.md`、`backtest-evidence.md`、`ext-architecture-plan.md`、`open-platform-plan.md`、`tickflow-pro-phase1-probe.md`、`tickflow-pro-shared-rate-limit.md` |
| 规划方案 | `factor-platform-plan.md`、`robust-optimizer-plan.md`、`backend-data-performance-plan.md`、`stock-detail-lightweight-charts-plan.md` |
| 质量治理 | `precommit-issue-inventory.md`、`tsp-rule-compliance-review.md` |

归档时已同步修复全仓引用（`docs/xxx.md` → `docs/base_v1.0/xxx.md`）；`base_v1.0` 内部的相对链接（`./xxx.md`）因同目录关系不变，无需修改。

### 9.2 v1.0.1 首批落档（新标准命名，7 份）

以下 7 份从 base_v1.0 按本规范**重命名 + 分类**落到 `docs/v1.0.1/`，补了元信息头，内容未改：

| 编号 | 文档 | 类型 | 状态 | 来源（base_v1.0 原名） |
|------|------|------|------|------------------------|
| REQ-001 | [代码规模整改计划](v1.0.1/requirements/REQ-001-code-size-remediation-plan.md) | 需求 | 基线 | 2026-11-17-函数与文件行数超标整改计划.md |
| REQ-002 | [ClickHouse 实施 WBS](v1.0.1/requirements/REQ-002-clickhouse-implementation-wbs.md) | 需求 | 草案 | clickhouse-implementation-plan.md |
| DES-001 | [ClickHouse 存储设计](v1.0.1/design/DES-001-clickhouse-storage-design.md) | 设计 | 基线 | clickhouse-storage-design.md |
| DEP-001 | [ClickHouse 验收方案](v1.0.1/deploy/DEP-001-clickhouse-acceptance.md) | 部署 | 草案 | clickhouse-acceptance.md |
| DEP-002 | [ClickHouse 验收记录](v1.0.1/deploy/DEP-002-clickhouse-acceptance-log.md) | 部署 | 基线 | clickhouse-acceptance-log.md |
| DEP-003 | [页面双栈对照用例](v1.0.1/deploy/DEP-003-clickhouse-page-compare-cases.md) | 部署 | 基线 | clickhouse-page-compare-cases.md |
| ISS-001 | [质量治理交接](v1.0.1/issues/ISS-001-quality-issues-handover.md) | 问题 | 基线 | 2026-11-17-处理质量问题交接文档.md |

> 其余 base_v1.0 文档**不重命名、不重分类**；后续大改时再按本规范新建对应编号文档，而非原地改造。

## 10. 写作要求（简洁优先）

- **不过度封装**：不自造术语，不用"体系/机制/闭环/赋能"这类包装词；直接写事实和做法。
- **能删就删**：一句话说清的不写一段；为对称硬凑的小标题、表格、清单直接去掉。
- **不写未落地内容**：没做、不具备条件、纯设想的方案不进正文，放"待决事项"并注明前提。
- **层级宜浅**：标题一般不超过 3 级；单节内容不足 3 行则合并。

## 11. 版本目录索引（`docs/<version>/README.md`，按需建立）

每个新标准版本目录可建 `README.md` 索引：

```markdown
# 文档索引（v1.0.1）

| 编号 | 文档 | 类型 | 状态 | 文档版本 | 最后更新 |
|------|------|------|------|----------|----------|
| DOC-STANDARD | [文档管理规范](DOC-STANDARD.md) | 指南 | 基线 | v1.0 | 2026-11-18 |
| ... | ... | ... | ... | ... | ... |
```

---

*本规范一经采用即为基线；新增/调整类型前缀须同步更新本文件。*
