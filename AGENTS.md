# AI 开发入口

修改、调试或审查本仓库前，必须完整阅读并遵循根目录的 [`CONTRIBUTING.md`](CONTRIBUTING.md)。其中定义了项目架构、数据契约、数据源插件化、缓存与性能要求、测试矩阵以及 PR 复审和合并标准。

涉及代码二次开发、前端插槽、后端可替换策略、扩展注册或上游升级兼容时，还必须阅读 [`docs/base_v1.0/secondary-development.md`](docs/base_v1.0/secondary-development.md)。该文档区分当前已实现能力与目标扩展契约；不得根据设计示例虚构尚不存在的 API。

涉及 Pre-Commit、ruff、mypy、lint 等**代码质量门禁**的配置、阈值与豁免原则，还必须阅读 [`CONVENTIONS.md`](CONVENTIONS.md)。该文档是 CONTRIBUTING 在质量门禁层的落地补充，定义了软规约(tsp-rule)与硬门禁(pre-commit)的分工与实操流程；开发、提交或修改门禁配置前先查阅，避免新增被豁免的存量文件或误改阈值。

同时遵守以下规则：

- 先理解调用链和现有测试，再进行修改。
- 保持实现简单、改动范围最小，不处理无关问题。
- 不覆盖工作区已有修改，不虚构测试或审查结果。
- 以实际验证结果作为完成标准。

## 核心代码质量硬约束（写码时始终遵守）

以下为必守约束，由 pre-commit 增量门禁（只检查本次改动文件）强制。写码时一次做对，不依赖事后补救；阈值、豁免与执行细节见 `CONVENTIONS.md` §3/§4，软规约全文见 `tsp-rule/code-quality.mdc`。禁止静默删规则或改阈值。

- **方法/函数 ≤80 行**：超出先拆函数；确需超限须在签名行写 `# func-length-allow: <理由>` 显式承认（理由必填、可 grep）。
- **文件 ≤1000 行**：超出先拆模块（唯一存量豁免是 `app/backtest/strategy.py`，已登记待重构）。
- **注释/docstring 覆盖率 5%~60%**（测试与 scripts 豁免）：文档行 = 行注释 + docstring；低于下限补必要注释，高于上限删冗余注释。
- **圈复杂度 ≤12**（ruff C901）：超出用 `# noqa: C901` 附理由。
- **命名**：snake_case / camelCase / PascalCase / UPPER_SNAKE；禁拼音、缩写与含混单字母（循环索引除外）。
- **异常**：禁裸 `except Exception` 不留痕（须 raise 或记日志后约束范围）、禁 `print(` 报错、禁无用空 `try/except`。
- **docstring**：模块、公开类/函数、API handler、数据转换边界函数必须有，写清「输入约束 → 行为 → 输出/异常」，中文为主。
- **嵌套深度 ≤2 层**（if/try/for 相互嵌套）：超出用 guard clause 或抽 `is_valid(...)` / `_do_x(...)` 子函数，不靠缩进堆层。
