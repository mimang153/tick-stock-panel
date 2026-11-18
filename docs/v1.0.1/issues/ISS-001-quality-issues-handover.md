# 2026-11-17 处理质量问题交接文档

> 文档类型：问题 | 编号：ISS-001 | 状态：基线 | 文档版本：v1.0 | 最后更新：2026-11-18
> 负责人：— | 关联：REQ-001
> 归档来源：base_v1.0 基线文档，按 DOC-STANDARD v1.0 归类命名；内容未改。

范围：仅记录通过 pre-commit 和 tsp-rule 扫描出的问题与注意事项。

相关文档：[pre-commit 问题清单](precommit-issue-inventory.md)、[tsp-rule 合规审查](tsp-rule-compliance-review.md)。

## 总体状态

- pre-commit：ruff 492→0，mypy 568→0，format 141 文件合规，pytest 4820 passed。
- tsp-rule：P0/P1 已处理；P2 多为技术债、设计取舍或需独立重构。
- 当前改动未提交，提交前仍需按 git 规约检查 message。

## 1. pre-commit 扫描结果

### 已修复

| 编号 | 问题 | 处理 |
|---|---|---|
| PC-01~05 | ruff/mypy 新增：BLE001、B905、UP038、I001、类型错误 | 补 noqa 理由、strict 参数、类型注解、assert 收窄 |
| PC-06 | ruff-format 138/141 文件需重排 | 统一 format，语法与全量测试确认无语义变化 |
| PC-07~09 | debug/comment/file-length 钩子 exclude 不全 | 排除 scripts 与两个巨型文件；大文件拆分另立任务 |
| PC-10~11 | 存量 ruff 475 条、mypy 530 条 | 真修 + 显式 noqa/type:ignore，均清零 |
| PC-12 | 8 文件末尾缺换行 | end-of-file-fixer 修复 |
| PC-13 | grep 误报 151 文件尾随空白 | Python 逐字节检测确认实际为 0，无需处理 |

### 待处理 / 注意事项

| 编号 | 问题 | 建议 |
|---|---|---|
| PC-14 | gitleaks 真实扫描未跑成 | 环境允许后补跑 `pre-commit run gitleaks --all-files` |
| PC-15 | `awesome-cursorrules-main.zip` 超 512KB 且未跟踪 | 加 `.gitignore` 或提交时排除 |
| PC-16 | 约 320 处 `type:ignore`、480 处 `noqa` | 已可追溯；后续按 PR 逐步换真标注 |
| PC-17 | format 与行尾 ignore 有循环依赖 | 先 format 稳定，再按最终行号补 ignore |
| PC-18 | 批量 assert 曾插入多行表达式内部 | 批量改测试后跑 AST 语法检查与 pytest |
| PC-19 | 变量重命名漏改尾部引用 | 重命名后全局搜索引用 |
| PC-20 | 宿主 ruff 版本与 pre-commit 缓存版本不一致 | 审查用 pre-commit 缓存锁定版本 |
| PC-21 | 提交 message 需符合 git 规约 | `type(scope):` 小写描述，按主题拆分 |

## 2. tsp-rule 扫描结果

### 已修复 / 部分修复

| 编号 | 问题 | 处理 |
|---|---|---|
| TR-01 | 硬编码 ClickHouse 明文口令 | 改为环境变量/compose 注入，部署须设置 `CLICKHOUSE_PASSWORD` |
| TR-02 | Polars 裸 collect 10 处、裸 scan 13 处 | 改用 `guarded_collect` / `scan_parquet_compat` |
| TR-03 | noqa 大面积缺理由 | 批量补理由，无理由归零 |
| TR-04 | detect-url SSRF 风险 | 加 scheme 白名单、IP 拦截、重定向二次校验 |
| TR-13 | `market_data.py` 公开函数缺注解 | 补参数/返回类型，`symbol/config_id` 允许 `None` |
| TR-19 | `Dockerfile.dev` 无 HEALTHCHECK、non-root | HEALTHCHECK 已加；non-root 与精确补丁待定 |
| TR-20 | compose 无资源限制 | app/dev 增加 cpus/memory limits |

### 未修复 / 需后续

| 编号 | 问题 | 建议 |
|---|---|---|
| TR-05 | `pipeline.py` 5 个 300~400 行函数 | 单列重构任务 |
| TR-07 | `_dimension_rank` 双实现 | 抽取时保留可选参数差异 |
| TR-08 | EMA/VOL/KDJ 魔法数字 | 逐步提取命名常量 |
| TR-09 | 公开类缺 docstring、注释偏 what | 补 docstring，注释改 why |
| TR-10 | CK 唯一读源未收口 | 按 B4 独立迁移处理 |
| TR-11 | 测试仍有裸 `DataStore(tmp_path)` | 逐个判定读路径，纯本地逻辑可保留 |
| TR-12 | Redis 后端与 Arrow IPC 未落地 | 属目标契约，非本次回归 |
| TR-14 | 弱口令下限 6 位、cookie `secure=False` | 自托管场景已有缓解；如需达标另改 |
| TR-16 | 无 RBAC、detect-url 缺长度约束 | 补 Pydantic 约束；RBAC 符合当前单用户设计 |
| TR-17 | suggestion 未分页、CORS `allow_origins=*` | 公网部署前评估并补限制 |

### 无法判定 / 不适用 / 风险接受

| 编号 | 状态 | 说明 |
|---|---|---|
| TR-06 | 无法判定 | 嵌套深度需 PR review 人工核对 |
| TR-15 | 接受风险 | 自研单用户认证；PBKDF2、限流、token 哈希已实现 |
| TR-18 | 注意事项 | SSRF 静态校验不能完全防 DNS rebinding，如需彻底防需连接层 pinned IP |
| TR-21 | 无法判定 | SAST/SCA/DAST 配置不在代码内 |
| TR-22 | 不适用 | 前端规则零改动 |
