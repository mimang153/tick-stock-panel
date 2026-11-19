# CodeBuddy 项目规则入口 (Project Rules)

本目录是 CodeBuddy IDE 的项目规则入口。规则内容统一维护在仓库根的真相源文档中，本文件仅作**索引/指路**，避免多份拷贝导致口径漂移。

## 必读文档（按优先级）

开发、审查或修改本仓库前，按以下顺序阅读：

1. **架构与契约（最高权威）**：`CONTRIBUTING.md`
   - 项目架构、数据契约、数据源插件化、缓存与性能要求、测试矩阵、PR 复审与合并标准。
2. **二次开发契约**：`docs/base_v1.0/secondary-development.md`
   - 前端插槽、后端可替换策略、扩展注册、上游升级兼容；区分"已实现能力"与"目标扩展契约"，不得虚构 API。
3. **代码质量门禁**：`CONVENTIONS.md`
   - 软规约（tsp-rule）与硬门禁（pre-commit）的分工、已启用钩子清单、阈值与豁免原则、日常开发落地流程。
4. **AI 开发入口**：`AGENTS.md`
   - 开发前须知与通用规则（先理解调用链、改动范围最小、以实际验证为准）。

## 技术栈规约（AI 写码时遵守）

本仓库按技术栈将规约拆分在 `tsp-rule/*.mdc`，写码前按涉及模块选择阅读：

- `tsp-rule/polars.mdc` — 数据栈规约
- `tsp-rule/clickhouse.mdc` — 存储规约
- `tsp-rule/redis.mdc` — 缓存规约
- `tsp-rule/backend-services.mdc` — APScheduler/SSE 实时流
- `tsp-rule/code-quality.mdc` — 命名/注释/异常/复杂度/控制流（对标 P3C）

## 门禁要点（勿随意改动）

- 提交质量由 **pre-commit** 强制（ruff/mypy/tsc + 文件行数 + 注释覆盖率），见 `CONVENTIONS.md`。
- **阈值与豁免原则**：所有阈值可调点、以及巨型存量文件（backtest/、tickflow/repository.py）的豁免逻辑，都在 `CONVENTIONS.md` §4 定义。修改任何阈值前先读。
- 新代码不得静默删规则；确需超限用 `# noqa` 显式承认并附理由。
