# 工程规约与质量门禁 Conventions

面向 tick-stock-panel 的**工程规约**和**质量门禁**实操约定。本文件是 [`CONTRIBUTING.md`](CONTRIBUTING.md) 在「代码质量门禁」层面的落地补充；两者冲突时以 `CONTRIBUTING.md` 为准。涉及架构、数据契约、测试矩阵、PR 复审的内容见 `CONTRIBUTING.md`，不在此重复。

## 1. 工具链与责任分层

| 层 | 工具 | 责任 | 触发时机 |
|----|------|------|---------|
| 规约（软） | `tsp-rule/*.mdc` | 约束 AI 与写码方式（技术栈、数据、注释、复杂度约定） | 写码时/开发前 |
| 门禁（硬） | `pre-commit` | 提交时强制代码不得恶化 | `git commit` 时 |
| 门禁后端 | ruff + mypy + tsc + 自定义脚本 | 承载具体检查 | 随 pre-commit 触发 |

代码质量遵循「**软规约先行，硬门禁兜底**」：能自动判定的交给工具强制，需人工判断的交给规约约束并在 PR 复审核对。

## 2. 一次性安装（每个环境必做）

```bash
cd /home/zwcad/workplace/tsp/tick-stock-panel
pip install pre-commit git-cliff   # git-cliff 可选；pre-commit 必需
pre-commit install                  # 关键动作：把钩子写入 .git/hooks
```

> 未执行 `pre-commit install`，下方所有门禁不会自动生效。
> 首次建仓或换机后，运行一次 `pre-commit run --all-files` 观察基线，确认没有「坏钩子」把门禁本身卡死。

## 3. 已启用的门禁清单

配置文件：`.pre-commit-config.yaml`

### 3.1 通用卫生（pre-commit-hooks）
- 行尾/行尾空行、EOF 空行、LF 统一；YAML/JSON/TOML 语法；大小写冲突；合并冲突标记；超大文件（>512KB）；私钥（`detect-private-key`）；`main` 分支禁止直接提交。
- **密钥/token 泄露扫描**：未接入提交门禁（gitleaks 环境依赖 Go，本机安装受宿主 safe-delete 策略限制）；提交侧由 `detect-private-key` 兜底，完整扫描交给 CI。

### 3.2 后端 Python
| 类别 | 规则 | 阈值/说明 |
|------|------|----------|
| Lint | ruff `E/F/I/N/UP/B/SIM/RUF/BLE` | 命名、导入顺序、无副作用、反模式、裸 except 禁止 |
| 复杂度 | ruff `C901` | `max-complexity = 12` |
| 潜在性能 | ruff `PERF` + `TRY400/401/203` | 低效写法、异常规范（须 `raise`/日志，禁裸 print） |
| 函数规模 | ruff `PLR0915` / `PLR0912` | `max-statements = 60`（对应函数体约 80 行）、`max-branches = 20` |
| 函数行数 | 脚本 `check_function_length.py` | AST 统计 `def/async def` 起止行，默认 ≤80 行 |
| 类型 | mypy | 增量，`backend/pyproject.toml` 的 `[tool.mypy]` 渐进档 |
| 格式化 | `ruff-format` | 统一格式 |
| 文件行数 | 脚本 `check_file_length.sh` | 默认 ≤1000 行 |
| 文档覆盖率 | 脚本 `comment_coverage.py` | 文档行=行注释+docstring，默认区间 5%~60%（双向） |
| 调试残留 | 脚本 | 禁 `print(`/`pdb.`/`breakpoint(`/`import ipdb` |

### 3.3 前端 TypeScript/React
- `tsc -b` 类型检查（strict + noUnusedLocals/Params）。当前未引入 eslint/prettier，`package.json` 的 `lint` 脚本无对应依赖，勿误以为已强制。

## 4. 阈值与豁免原则

### 4.1 阈值一览（可调整位置）
| 项 | 当前值 | 调整点 |
|----|--------|--------|
| 圈复杂度 | 12 | `backend/pyproject.toml` → `[tool.ruff.lint.mccabe]` |
| 函数语句/分支 | 60 / 20 | `[tool.ruff.lint.pylint]` |
| 函数行数 | 80 | 钩子内 `FUNC_LINE_LIMIT` 环境变量（脚本 `check_function_length.py`） |
| 文件行数 | 1000 | 钩子内 `FILE_LINE_LIMIT` 环境变量（脚本 `check_file_length.sh`） |
| 文档覆盖率 | 5%~60% | 钩子内 `COMMENT_COVERAGE_MIN/MAX` 环境变量，或 `entry` 前的 `env` |

### 4.2 豁免原则（不删规则，显式承认）
- **未拆分的存量巨型文件**：阶段 4 已完成主要超长文件拆分，目录级/批量 `exclude` 已收敛。
  当前仅 `backend/app/backtest/strategy.py`（3127 行）仍按**单文件**在 `exclude` 与
  `per-file-ignores` 中显式豁免，并在 `./docs/v1.0.1/requirements/REQ-001-code-size-remediation-plan.md`
  登记为待重构技术债；该文件一旦拆分即移除豁免。其余文件一律受全量门禁约束。
- 必须超限的单一函数：
  - ruff 规则（复杂度/语句数）用 `# noqa: C901` / `# noqa: PLR0915 - 理由` 行内显式承认；
  - 函数行数门禁（AST）用 `# func-length-allow: <理由>` 行内显式承认，**理由必填**；
  - 两者都不静默删规则、不静默忽略，且都能被 grep 追踪。
- 文档覆盖率口径：`comment_coverage.py` 把行注释 **和 docstring** 都计为文档行（与 `code-quality.mdc` 强制 docstring 一致），测试文件/scripts 由钩子 `exclude` 豁免；区间窄易抖动时放宽 `COMMENT_COVERAGE_MIN/MAX`，而非删除门禁。

## 5. 日常开发落地流程

```text
1. 写码前   → 遵守 tsp-rule/ 规约（尤其 polars/clickhouse/redis/code-quality）
2. git add  →
3. git commit
   ├─ 通用卫生（含 detect-private-key）
   ├─ ruff --fix（lint + C901/PERF/TRY + PLR0915/PLR0912）
   ├─ ruff-format
   ├─ 函数行数 ≤80（AST）
   ├─ 文件行数 ≤1000
   ├─ 文档覆盖率 5%~60%（区间，双向）
   ├─ mypy（增量）
   └─ tsc --b（前端类型）
```

关键认知：**pre-commit 是增量门禁，只检查本次改动涉及的文件；存量历史问题不阻塞历史提交、不连坐。**

### 提交被拦时的处理
| 被拦原因 | 正确处置 |
|---------|---------|
| ruff 报错 | `ruff --fix` 自动修；不可自动修的改代码 |
| mypy 类型错误 | 补类型注解；禁止乱用 `# type: ignore` |
| 注释覆盖率 LOW/HIGH | 补必要注释 / 删冗余注释 |
| 函数 >80 行 | 拆分函数；确属必要写 `# func-length-allow: <理由>` 显式承认 |
| 文件 >1000 行 | 拆分文件；确属必要走豁免 |
| 密钥泄露 | 完整扫描交 CI；提交侧 `detect-private-key` 命中即移除，绝不能提交 |

## 6. 常用命令

```bash
pre-commit install                  # 一次性启用
pre-commit run --all-files          # 全量跑所有钩子（首次/基线）
pre-commit run ruff                 # 只跑某个钩子
pre-commit run backend-file-length
pre-commit run backend-function-length
pre-commit run backend-comment-coverage
SKIP=<hook-id> git commit ...       # 临时跳过单钩子（慎用，勿养成习惯）
pre-commit autoupdate               # 升级钩子版本（非必要勿跑）

# 后端 pytest（仓库根执行；自建可写 venv，绕开 root 只读的 backend/.venv）
bash run_tests.sh                          # 全量测试
bash run_tests.sh -q tests/test_xxx.py     # 指定文件，pytest 参数透传
PYTEST_VENV=/my/writable/venv bash run_tests.sh -q tests/   # 自定义 venv 路径
```

## 7. 未启用项与后续（避免误添加真拦路规则）

- **bandit**：对存量告警多、需 baseline，适合 CI 选做而非提交门禁；提交侧由 `detect-private-key` + mypy 兜底，完整密钥/安全扫描交 CI。
- **mypy strict 全量**：待 `[tool.mypy]` 渐进档随存量收敛后再上调严格度。
- **eslint/prettier（前端）**：tsconfig 已 strict；如需补需先装依赖并生成配置。
- **pytest 覆盖率**：属测试覆盖，不是注释覆盖；由 CONTRIBUTING 测试矩阵约束，不做提交门禁。

## 8. 与既有文档的衔接

| 文档 | 定位 |
|------|------|
| `AGENTS.md` | AI 开发入口，必读 `CONTRIBUTING.md` |
| `CONTRIBUTING.md` | 架构、数据契约、测试矩阵、PR 合并标准（权威） |
| `tsp-rule/*.mdc` | 技术栈规约，AI 写码参考（软约束）；库表设计约束见 `tsp-rule/pgsql.mdc`（原本文 §9 已迁入） |
| `CONVENTIONS.md` | 本文件：工程规约 + 质量门禁实操（软约束的量化 + 硬门禁落地） |

> 库表设计约束（关系库 id 主键 + 四审计字段 + 表/字段注释）已迁至 [`tsp-rule/pgsql.mdc`](tsp-rule/pgsql.mdc)，按软规约在 PR 复审时人工核对；本文不再重复。
