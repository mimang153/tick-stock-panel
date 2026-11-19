# Redis 缓存、CK-only 尾巴与多实例问题排期

> 文档类型：问题 | 编号：ISS-003 | 状态：评审中 | 文档版本：v1.4 | 最后更新：2026-11-18
> 负责人：待分配 | 关联：ISS-002、ISS-004、REQ-002、DES-001、DES-002、DEP-002

用途：把 DES-002 主体完成、Redis 缓存层落地之后仍未闭环的问题集中记录，给出可执行排期和验收标准。本文不重复 ISS-002 的 CH-04~CH-13，只处理“缓存介质、CK-only 尾巴、多实例一致性、发布验证”四类后续工作。

## 1. 背景与当前状态

### 1.1 已完成

- DES-002 P0~P4 主体完成：显式 `storage` 下 daily/enriched 在线读写、修补、校验、universe、live merge/flush、状态统计和矩阵 generation 均已 CK 化。
- 全量回归基线：`4820 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors（DES-002 主体完成后执行）。
- Redis 缓存层已实现：
  - `CacheBackend` 增加 `RedisCacheBackend`：DataFrame 用 Arrow IPC，dict 用 JSON，禁用 pickle，支持 TTL 与 Redis generation 计数器。
  - `CACHE_BACKEND=memory|redis` 可切换；当时默认 memory，S4（2026-11-19）起默认 redis，memory 仅作离线/测试兼容。
  - compose 不新建 Redis 服务，改为复用宿主机已有 Redis 容器，容器内通过 `host.docker.internal` 访问。
  - `pool:*`、`benchmark:momentum:v1` 已接入 CacheBackend。
  - `bump_enriched_generation()` 同步递增 Redis `enriched` generation。
  - 部署脚本 `./redis-cache.sh` 提供 `status / ping / check / url / flush --yes / exec`。
- Redis 改动后已跑定向验证：`100 passed`，Ruff / `git diff --check` / `docker compose config` 均通过。

### 1.2 未完成

- Redis 改动后尚未跑新的全量基线；最近一次全量基线早于 Redis 改动。
- 当前 shell 的 `docker ps -a` 未发现 Redis 容器，真实容器名、端口、密码和网络可达性仍需在部署环境确认。
- Redis 只解决了“跨进程/多实例共享缓存与失效一致性”的一部分问题，不替代 CK 权威数据、逐 tick 热缓存、mmap 矩阵缓存和内容寻址因子 parquet。
- 派生缓存 `sector / mining / factors` 仍保留本地文件/进程内实现（regime 已完成 W3 首批 CK 迁移，见 §7.4 归属表）；下一阶段按归属表逐类收口。

## 2. 范围界定

### 2.1 本文要处理

| 类别 | 范围 |
| --- | --- |
| Redis 落地 | 容器发现、连接配置、容量策略、健康端点、真实 Redis 验证 |
| CacheBackend 接入 | 适合共享的缓存从本地文件/进程内字典迁到 CacheBackend |
| CK-only 尾巴 | `pipeline_recompute` 常规 enriched 编排、指数 enriched 表、派生缓存 |
| 多实例一致性 | SSE EventBus、任务状态、取消标志、限流器、单飞状态 |
| 发布验证 | 全量回归、pre-commit/mypy、真实容器冒烟、性能标定 |

### 2.2 本文不处理

- DES-002 已完成的 daily/enriched 在线读写与修补契约，不重复改造。
- ISS-002 已记录的 CH-04~CH-13（三天观察、回滚演练、页面双栈对照、`pnpm build`、CH-12/CH-13 决策等）。
- 逐 tick enriched/live 热缓存、矩阵 mmap、内容寻址因子 parquet：这些是性能型本地缓存，不属于 Redis 迁移目标。
- 离线工具 `repair_daily` / `minute_adjust` 的显式 parquet 输入，离线兼容路径按 DES-002 §3.2 保留。

## 3. 问题清单

### 3.1 Redis 落地与运维（RC-01~RC-04）

| 编号 | 优先级 | 问题 | 影响 | 处理方案 | 验收标准 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| RC-01 | P0 | 部署环境未确认宿主机 Redis 容器的名称、端口、密码和网络 | `CACHE_BACKEND=redis` 启动时可能连接失败；容器内 `localhost` 不可达 | 在 `.env` 明确 `REDIS_CONTAINER / REDIS_PORT / REDIS_DB / REDIS_PASSWORD`；密码写入 `CACHE_REDIS_URL` / `CACHE_REDIS_URL_CONTAINER`；执行 `./redis-cache.sh check` | 宿主机脚本 PING 通过；应用容器内能 PING 同地址；两套 URL 文档与实例一致 | 部署环境提供 Redis 容器访问信息 |
| RC-02 | P0 | 未定义 Redis `maxmemory` / eviction policy / 持久化策略 | 缓存无限增长或错误淘汰 generation 键，导致不可预测的失效 | 原决策「Redis 仅作缓存、不开启 RDB/AOF」已于 2026-11-19 由 DES-004 §2.3 修订：Redis 升格协调态权威，开启 AOF（`appendfsync everysec`），但只放可重建协调态；`maxmemory-policy=allkeys-lru` 与缓存键 TTL 不变 | 运维文档（`deployment.md`）已更新；`redis-cache.sh` 新容器默认 AOF + `aof on/off` 在线命令；协调态从 CK 重建流程见 DES-004 §2.3 | RC-01、DES-004 |
| RC-03 | P1 | `/api/health`（或 `/health`）只反映 CK，不反映缓存后端 | 多实例部署中 Redis 失联但 CK 正常时，健康探针仍为绿 | 健康端点增加缓存探活；内存后端直接 healthy；Redis 后端执行轻量 PING；Redis 失联时按配置决定 degraded/unhealthy | 健康端点可区分 `cache_backend / cache_status / ck_status`；对应测试覆盖 | RC-01 |
| RC-04 | P0 | Redis 改动后未跑全量和质量门禁 | 不能作为发布依据 | 以 `CACHE_BACKEND=memory` 跑全量回归；以 `CACHE_BACKEND=redis` 跑定向缓存/池/generation 测试；执行 pre-commit / mypy / Ruff | 全量 0 failed；mypy/pre-commit 通过或豁免有记录；测试报告归档 | RC-01、RC-02 |

### 3.2 CacheBackend 接入（RC-05~RC-09）

| 编号 | 优先级 | 问题 | 是否直接迁 Redis | 处理方案 | 验收标准 | 依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| RC-05 | P1 | `strategy_cache.json` 是带累积状态的 read-modify-write 文件 | 否 | 已实现 Redis Hash + Lua 原子读改写（详见 ISS-004 §3.2）；CK 表方案未采用 | 多实例并发更新不丢状态；重启/缓存清空后行为符合定义；有并发测试（已满足，全量回归待补） | RC-01 已完成；状态语义按 §7.2 |
| RC-06 | P1 | `DataStatusCache` 是请求级协调器，含单飞刷新后台线程 | 否 | 先决策：单实例继续本地；多实例改为 Redis 分布式锁/状态键，或接受每实例独立刷新 | 决策文档落盘；若实现，跨实例只触发一次后台刷新，状态一致 | 多实例是否在范围内 |
| RC-07 | P2 | SSE `EventBus` 是进程内，多实例事件不会跨进程广播 | 是（Pub/Sub） | 增加 Redis Pub/Sub 或 MQ 后端；保持现有 EventBus 接口；默认内存实现不变 | 两个进程订阅同一频道均能收到事件；断线重连不丢关键事件；有集成测试 | RC-01 |
| RC-08 | P2 | `pipeline_jobs` 的 job_store、取消标志、heavy job limiter 是本地/进程内 | 部分 | 梳理三类状态：历史记录可入 CK；取消标志和限流计数器用 Redis 原子操作 | 跨实例可查询任务状态；取消标志多实例可见；限流计数全局生效；测试覆盖 | RC-01、RC-07 |
| RC-09 | P2 | `ext_factors._frame_cache` 的键含绝对路径，Redis 共享时键不稳定 | 是（需先改键） | 用数据指纹/内容哈希替代绝对路径；确保不同实例相同数据生成相同键 | 两实例同数据命中同一缓存；数据变更后生成新键；有缓存失效测试 | RC-01 |

### 3.3 CK-only 尾巴（RC-10~RC-13）

| 编号 | 优先级 | 问题 | 处理方案 | 验收标准 | 依赖 |
| --- | --- | --- | --- | --- | --- |
| RC-10 | P1 | `pipeline_recompute` 常规 enriched 编排仍写本地 parquet / publication marker | 在显式 `storage` 下最终写入只走 CK；staging 可作为离线交换格式保留但不作为在线读源；publication 用 CK generation | 显式 storage 重算后本地 `kline_*_enriched/date=*/part.parquet` 不再产生；读路径只从 CK/缓存读；相关测试迁移 | DES-002 P3/P4 已完成的 repository 路径 |
| RC-11 | P1 | 指数 enriched 表 `kline_index_enriched` 不在 CK，仍保留本地兼容 | 新增 CK 表注册、DDL、读写与 schema 契约；移除 `_repo_write_mixin.py` 中指数 enriched 的本地分支 | `kline_index_enriched` 在 schema 测试中通过；指数 live/recompute 写 CK；指数 enriched 测试无本地 parquet 断言 | RC-10 |
| RC-12 | P2 | 派生缓存 `regime / sector / mining / factors` 仍写本地 parquet/marker | 按 REQ-002 决策复用 `ext_timeseries` 或新建 CK 表；`market_mainline` 已 CK 化，作为样板 | 各派生缓存有明确归属：在线 CK / 离线 parquet / 本地热缓存；无无主文件；测试验证在线路径不读旧 marker | RC-10、REQ-002 决策 |
| RC-13 | P3 | enriched 文件 generation marker 仍存在，Redis bump 只是旁路 | 在 RC-10~RC-12 完成后，评估是否删除文件 marker 或只保留离线兼容；确认所有在线读路径已消费 CK `current_generation` | 在线读路径不依赖 marker；删除 marker 后全量回归通过；离线工具明确仍需要时保留兼容分支 | RC-10~RC-12 |

### 3.4 发布验证（RC-14~RC-15）

| 编号 | 优先级 | 问题 | 处理方案 | 验收标准 | 依赖 |
| --- | --- | --- | --- | --- | --- |
| RC-14 | P0 | Redis 改动后未做真实容器冒烟 | 启动应用并设 `CACHE_BACKEND=redis`，执行池缓存读、generation bump、flush 后重新取数、健康检查 | 全流程日志无异常；Redis 键前缀只含本项目键；flush 不影响其他业务；有冒烟记录 | RC-01~RC-04 |
| RC-15 | P1 | 满量性能与稳定性仍依赖人工 | 分别跑 memory / redis 两组的满量性能标定；记录 P95/P99、Redis QPS、内存占用、CK 写入延迟 | 性能报告归档；无明显回退；超过阈值的问题转入新 ISSUE | RC-10~RC-14、部署环境可用 |

## 4. 排期

### 4.1 排期假设

- 以 2026-11-18 为起点，按每周 5 个工作日估算。
- 日期为目标窗口，不是上线承诺；外部依赖（Redis 容器访问信息、产品确认、部署环境）未满足时顺延。
- 每个批次只跑相关定向测试；全量回归和 pre-commit/mypy 放在批次门禁或发布门禁，避免每轮都跑全量。
- 负责人字段暂缺，评审后按角色分配：后端、运维、QA、产品。

### 4.2 里程碑总表

| 阶段 | 日期窗口 | 目标 | 包含事项 | 门禁 |
| --- | --- | --- | --- | --- |
| W0 基础验证 | 2026-11-18 ~ 2026-11-20 | Redis 真实容器可用、质量门禁恢复 | RC-01~RC-04、RC-14 | `./redis-cache.sh check` 通过；全量回归 0 failed；mypy/pre-commit 通过 |
| W1 缓存语义收敛 | 2026-11-23 ~ 2026-11-27 | 决定哪些非简单缓存需要状态化改造 | RC-05、RC-06、RC-09；RC-02 运维策略定稿 | 每项有设计结论或实现；并发/共享缓存测试通过 |
| W2 CK-only 主体收口 | 2026-11-30 ~ 2026-12-04 | `pipeline_recompute` 和指数 enriched 收口 | RC-10、RC-11 | 显式 storage 下不再写 enriched 本地 parquet/marker；schema 测试通过；定向测试全绿 |
| W3 多实例与派生缓存 | 2026-12-07 ~ 2026-12-11 | 多实例协调与派生缓存归属明确 | RC-07、RC-08、RC-12 | 跨进程事件/任务测试通过；派生缓存归属表落盘；定向测试全绿 |
| W4 清理与发布准备 | 2026-12-14 ~ 2026-12-18 | 清理 generation marker，完成性能与发布基线 | RC-13、RC-15、RC-14 复测 | 全量回归 0 failed；memory/redis 双模式冒烟通过；性能报告归档 |
| W5 验收观察 | 2026-12-21 ~ 2026-12-25 | 连续观察与验收复核 | ISS-002 CH-04~CH-09 中可由本轮闭环的部分 | DEP-002 判定“有条件通过”是否可升级；不可升级项转入新 ISSUE |

### 4.3 周计划细化

#### W0：2026-11-18 ~ 2026-11-20

| 日期 | 工作项 | 产出 |
| --- | --- | --- |
| 11-18 | 创建本文；确认 Redis 容器信息；补 `.env`；执行 `./redis-cache.sh check` | 容器信息和检查结果 |
| 11-19 | Redis maxmemory/eviction/持久化策略；健康端点缓存状态 | 运维策略、health 字段、测试 |
| 11-20 | memory 模式全量回归；redis 模式定向冒烟；pre-commit/mypy | 质量门禁报告 |

#### W1：2026-11-23 ~ 2026-11-27

| 日期 | 工作项 | 产出 |
| --- | --- | --- |
| 11-23 | `strategy_cache.json` 状态语义梳理与方案设计 | 设计结论：Redis Hash + Lua / CK 表 / 保留本地 |
| 11-24~11-25 | `DataStatusCache` 多实例决策；若需要则实现 Redis 锁/状态键 | 决策文档或实现 |
| 11-26 | `ext_factors._frame_cache` 键规范化 | 稳定缓存键与测试 |
| 11-27 | W1 评审：确认 W2 的 CK-only 收口范围 | 评审记录 |

#### W2：2026-11-30 ~ 2026-12-04

| 日期 | 工作项 | 产出 |
| --- | --- | --- |
| 11-30~12-01 | `pipeline_recompute` 显式 storage 下最终写入 CK-only；staging 保留但退出在线读源 | 代码与测试 |
| 12-02 | 新增 `kline_index_enriched` CK 表和 schema 契约 | schema 测试通过 |
| 12-03 | 指数 enriched live/recompute 切 CK；移除本地兼容分支 | 指数相关测试迁移 |
| 12-04 | W2 定向回归与评审 | 无本地 enriched parquet 写入的证据 |

#### W3：2026-12-07 ~ 2026-12-11

| 日期 | 工作项 | 产出 |
| --- | --- | --- |
| 12-07~12-08 | SSE EventBus Redis Pub/Sub 后端与集成测试 | 跨进程事件测试 |
| 12-09~12-10 | `pipeline_jobs` 状态/取消/限流多实例化 | 跨实例任务状态与取消测试 |
| 12-11 | 派生缓存归属表与迁移第一批 | `regime / sector / mining / factors` 归属明确，至少一类完成迁移 |

#### W4：2026-12-14 ~ 2026-12-18

| 日期 | 工作项 | 产出 |
| --- | --- | --- |
| 12-14 | 在线读路径 generation 依赖复核 | 确认是否可删除 marker |
| 12-15 | enriched 文件 generation marker 清理或保留兼容说明 | 决策与代码 |
| 12-16 | memory / redis 双模式全量回归 | 两份全量报告 |
| 12-17 | 满量性能标定 | 性能报告 |
| 12-18 | 发布基线冻结与文档回填 | DEP-002 / ISS-002 / 本文更新 |

#### W5：2026-12-21 ~ 2026-12-25

- 执行或继续 ISS-002 的 CH-04~CH-09 人工/环境相关验收。
- 回滚演练、页面级双栈对照、连续观察均以 W4 冻结版本为基线。
- 若观察期跨自然日不足，顺延到下一个工作日窗口，并仅记录事实，不提前改判验收结论。

## 5. 验收与退出标准

整体退出需同时满足：

1. Redis 真实容器可发现、可连接、可健康检查；运维策略明确。
2. `CACHE_BACKEND=memory` 与 `CACHE_BACKEND=redis` 均有全量回归或等价覆盖范围，0 failed。
3. 显式 `storage` 下在线路径不再依赖本地 backup / fingerprint / marker / glob；本地 parquet 只在离线工具或明确兼容分支存在。
4. `pipeline_recompute` 常规 enriched 与指数 enriched 写 CK；`kline_index_enriched` 注册并通过 schema 测试。
5. 多实例相关状态（SSE、任务状态、取消、限流、策略状态）均有明确归属：已迁移、明确保留本地且说明影响、或转入新 ISSUE。
6. 派生缓存有归属表：在线 CK / 离线 parquet / 本地热缓存三类之一，不允许“无主”缓存继续留在生产路径。
7. 性能标定无明显回退；Redis 内存占用受控。
8. 本文、ISS-002、DEP-002 状态同步完成。

## 6. 风险与阻塞

| 风险 | 描述 | 缓解 |
| --- | --- | --- |
| Redis 容器信息缺失 | 当前 shell 未发现 Redis 容器，名称/端口/密码未知 | W0 先确认；无法确认则 RC-01 阻塞，其余 Redis 项顺延，但 CK-only 项可并行 |
| Redis generation 重置 | 纯缓存不持久化，Redis 重启后 generation 从 0 开始，可能放过旧缓存条目 | 所有缓存键设 TTL；重启后执行一次 `flush --yes`；运维文档写清 |
| 缓存失效语义不一致 | 内存与 Redis 的 TTL、generation、序列化必须完全一致 | 共用 CacheBackend 契约测试；新增真实 Redis 冒烟 |
| 多实例需求范围不清 | SSE / 任务状态 / 单飞是否要求跨实例未产品确认 | W1 先决策，不实现未确认的大改 |
| 本地缓存误删 | 派生缓存可能被在线读路径隐式依赖 | 先做归属表和读路径搜索，再删文件；每类缓存单独定向测试 |
| 全量回归耗时 | 用户已明确不希望每轮跑全量 | 只在 W0、W4 和发布前跑；其余用定向测试 |

## 7. 进展记录

### 7.1 W0（2026-11-18）已完成

- RC-01 配置完成：宿主机已有 Redis 镜像（`redis:8-alpine`），`.env` 已按环境信息写入 `REDIS_CONTAINER=tsp-requer`、端口、库、密码及两套连接 URL。`redis-cache.sh` 新增幂等 `up` 命令：容器不存在时用本地镜像创建，存在则只启动/复用；待部署环境执行 `./redis-cache.sh up && ./redis-cache.sh check` 后解除真实容器冒烟阻塞。
- RC-02 已提供 `./redis-cache.sh config` 与新建容器默认参数：`REDIS_MAXMEMORY=256mb`、`REDIS_MAXMEMORY_POLICY=allkeys-lru`、关闭 RDB/AOF（此为 W0 当时记录）；**2026-11-19 决策修订为开启 AOF**（Redis 升格为多 worker 权威存储，需持久化），最终口径见本文 §2 表格与 ISS-004 DIST-02，本行保留历史过程不改。
- RC-03 已完成：`/health` 与 `/api/health` 增加 `cache / cache_backend / cache_status / ck_status` 字段；缓存后端提供统一 `ping()`。
- RC-04 已完成：
  - 全量回归 `4831 passed / 13 skipped / 1 xfailed`，0 failed / 0 errors。
  - W0 涉及文件 pre-commit 全绿，含 Ruff、ruff-format、文件/函数行数、注释覆盖率、mypy。
  - 修复 `.pre-commit-config.yaml` 中 `backend-debug-statements` 与 `backend-mypy` 的 `bash -c` 参数传递缺陷：缺少 `$0` 占位会吞掉首个文件并触发 mypy 空参数误报；本次加 `_` 占位，未改阈值和豁免。
- RC-14 真实 Redis 冒烟待执行：部署环境执行 `./redis-cache.sh up` 后再跑 `check / config / flush --yes` 冒烟。

### 7.2 W1 阶段决策

- RC-05（`strategy_cache.json`）：方案选定 **Redis Hash + Lua 原子读改写**，并已于 2026-11-19 实现（详见 ISS-004 §3.2）。理由：该缓存有同日 `today_ever_rows` symbol 级并集语义，CK `ReplacingMergeTree` 仍需要读-改-写，无法天然避免并发覆盖；Redis Lua 可在一次原子操作中完成“读旧值 → 合并 → 写新值 → bump generation”。内存镜像继续保留，但失效判据从文件 `mtime/size` 改为后端版本号；本地 JSON 仅作为无 CacheBackend 时的离线兼容。
- RC-06（`DataStatusCache`）：当前默认单实例部署，暂不迁移到 Redis。保留进程内 daemon 单飞刷新，接受多实例下各自刷新与重复统计；若后续确认多实例为正式部署形态，再按 RC-06 的 Redis 租约方案实现全局单飞。
- RC-09（`ext_factors._frame_cache`）已完成：
  - 帧缓存键从 `(data_dir 绝对路径, config_id, mode)` 改为 `(config_id, mode, 内容签名)`；
  - snapshot 签名由本机 `mtime/size` 改为 parquet 内容 SHA-256 + 文件大小；
  - 缓存值保留写入方逻辑根 ID，仅用于 `invalidate_ext_caches(data_dir)` 失效；
  - 新增跨挂载点相同内容命中同一缓存键测试。
  - 定向验证：`test_ext_factors.py`、`test_factor_registry.py`、`test_factor_store.py` 共 `42 passed`。

### 7.3 W2（2026-11-30）已完成

- RC-10 `pipeline_recompute` 常规 enriched 编排 CK-only：
  - 新增 `CKEnrichedPublication`（`backend/app/enriched_generation.py`）：继承 `EnrichedPublication` 复用发布认领、generation 区间语义与本地 marker 兼容（marker 清理留给 RC-13），仅覆写 `write_parquet`；显式 storage 下分区只写 CK 事实表，不再落本地 enriched parquet。
  - `run_pipeline`（`pipeline_rebuild.py`）在显式 storage 下改用该适配器，并在 CK 上维护 `begin_generation → commit_generation / fail_generation` 生命周期；无 storage 的离线路径保留原 `EnrichedPublication`。
  - `pipeline_recompute.py` 的增量新日期、全量合并、局部重算与 `pipeline_enriched.py::_upsert_enriched_day` 移除本地分区目录预创建（CK 适配器不写文件，离线父类自行建目录）。全量 staging parquet 仍是中间交换格式，但不作为在线读源。
  - 生产读路径 `_kline_jobs.py`、`extend_history.py` 的「覆盖天数」统计改为优先 CK `list_daily_dates`，本地 glob 仅作无 storage 的离线兼容。
  - 测试迁移：`test_enriched_full_rebuild.py`、`test_enriched_incremental_memory.py`、`test_enriched_generation.py`、`test_data_integrity.py` 的断言切 CK，并新增 CK generation `ready` 断言；显式验证 CK 被更新而预置本地 parquet 不被覆盖，证明写入只走 CK。
- RC-11 指数 enriched CK-only：
  - `_repo_write_mixin.py` 移除 `merge_live_enriched_asset` / `flush_live_enriched_asset` 中 `table != "kline_index_enriched"` 本地兼容分支；`_run_ck_enriched_generation` 放宽到 `index`；`_write_daily_partition` 的 generation_asset 增加 `kline_index_enriched → index`。`kline_index_enriched` 表 schema 在 DES-002 已注册并存在，本阶段未改表结构。
  - `_repo_asset_refresh_mixin.py::_refresh_index_enriched` 改为 CK 优先读近 300 天重算通用指标，本地 parquet 读取拆为 `_refresh_index_enriched_local` 仅作离线兼容；`daily_pipeline_sync.py` 指数增量起点改为优先 CK 日期列表。
  - `test_repository_index.py` fixture 切换 `ck_runtime`（完整 schema），数据构造与落盘断言切 CK，并新增 index generation `ready` 断言；指数测试不再有本地 parquet 断言。
- 验证结果：
  - W2 定向回归 15 个测试文件共 `133 passed / 0 failed`（含 enriched 全量重建、增量、generation、完整性自检、指数 live 写、daily mirror、CK 优先仓储等）。
  - 对 15 个改动文件执行 `pre-commit run --files ...` 全绿：ruff、ruff-format、文件/函数行数、注释覆盖率、mypy、调试残留、通用卫生均通过。
  - `recompute_daily_symbols` 按 CONVENTIONS 用 `# func-length-allow` 显式承认（DES-002 已内联 CK generation 生命周期与修补校验分支，拆分会改变发布顺序），理由已随行记录。

### 7.4 W3（2026-12-07）已完成

- RC-07 SSE EventBus 跨实例广播：
  - `backend/app/services/events.py` 抽出 `EventBusBackend` 契约与 `MemoryEventBusBackend` / `RedisEventBusBackend`；`EventBus` 门面与模块级 `bus` 单例保持不变，默认 `EVENT_BUS_BACKEND=memory` 行为与旧版完全一致。
  - Redis 后端每个订阅者用独立 pub/sub 连接，断线退避重连；事件 id 用纳秒时间戳避免多实例毫秒碰撞；`lifespan` 关停时 `bus.close()` 释放连接。
  - 新增 `tests/test_events_redis.py`：两个独立实例共享频道均收到事件、退订后停止投递、scope 原样下发、Redis 故障直接抛错不降级（`15 passed` 含既有 `test_events.py`）。
- RC-08 任务状态、取消与限流多实例可见：
  - 新增 `backend/app/services/coordination.py`：`CancelFlagBackend` / `LimiterCounterBackend` / `JobStateBackend` 三套契约，默认 `Memory*` 空实现；`COORDINATION_BACKEND=redis` 时装配 Redis 取消标志、WATCH+MULTI 原子全局容量计数、Hash+ZSET 任务状态镜像。
  - `pipeline_jobs` 保留本地 `threading.Event` 即时唤醒，跨实例取消改查协调层；任务本地未命中时回退协调层查询，`recent_snapshot` 合并其他实例任务；`HeavyJobLimiter` 接入全局计数（limit 传总容量，因全局计数已含本地占用）。
  - 新增 `tests/test_coordination_redis.py`（跨实例取消、全局容量不超发、任务状态镜像、Redis 故障禁止降级），与既有 `test_heavy_job_limiter.py` / `test_job_stall_and_cancel.py` / `test_pipeline_capacity.py` / `test_job_record_persistence.py` 共 `57 passed`。
  - 决策（用户确认）：Redis 后端不可用时**不降级**为本地语义——`coordination.py` 三类后端所有 Redis 失败（含启动 ping）统一抛 `CoordinationUnavailableError`，消息携带 `COORDINATION_BACKEND/URL` 与原始错误；`events.py` 的 publish/subscribe 失败抛 `EventBusUnavailableError`，不再静默丢事件或伪装订阅成功。仅 `WatchError` 作为并发冲突继续重试，`_listen` 的后台断线重连保留并升级为 `logger.exception` 提示总线不可用。
- RC-12 派生缓存归属与第一批迁移：
  - 新增 CK 表 `regime_history`（date + payload_json，ReplacingMergeTree），`store` 增加 `read_regime` / `replace_regime` / `upsert_regime`，与 `market_mainline` 样板对称。
  - `regime_builder` 的 `load / upsert / replace / refresh_phase_labels` 在线走 CK，离线无 storage 时保留本地 parquet 兼容；修复 W2 遗留隐患：`enriched_date_set` 改从 CK `list_daily_dates` 取 enriched 日期，CK-only 下 `detect_stale_dates` 不再依赖本地 mtime。
  - 新增 `tests/test_regime_ck.py`（upsert/load、不误删区间内未重算日期、范围修正、enriched 日期读 CK），与既有 regime 测试共 `29 passed`。
  - 派生缓存归属表（W3 落盘，后续按此逐类收口）：

    | 缓存 | 归属 | 当前状态 | 后续 |
    | --- | --- | --- | --- |
    | regime | 在线 CK `regime_history` / 离线本地 parquet | W3 已迁移 CK 在线读源 | W4 确认清理离线路径 |
    | market_mainline | 在线 CK `market_mainline` | 已 CK 化（样板） | 保持 |
    | sector | 待定：本地文件+进程内 | 未迁移 | W4 前决定归属，倾向 CK 或本地热缓存 |
    | mining | 待定：本地文件+进程内 | 未迁移 | W4 前决定归属，评估与 `ext_timeseries` 复用 |
    | factors | 本地热缓存 / 内容寻址 parquet | W1 已规范缓存键（RC-09） | 不迁 CK，保持内容寻址适合大矩阵 |

- 验证结果：
  - W3+W2 综合回归 13 个文件共 `146 passed / 0 failed`。
  - 对 W3 改动文件执行 `pre-commit run --files ...` 全绿：ruff、ruff-format、文件/函数行数、注释覆盖率、mypy、调试残留、通用卫生均通过。
  - `store.py` 按 CONVENTIONS 用文件行数门禁单文件显式豁免登记为待重构债（REQ-004 按数据集拆分后移除），理由已随配置记录。

### 7.5 W4（2026-12-14）已完成

- RC-13 generation 依赖复核与 marker 清理：
  - 复核结论：不能裸删 marker；三类在线依赖必须先 CK 化——`get_enriched_generation_snapshot`（无 CK 版，被 backtest/screener/cache 大量调用）、`enriched_publication_incomplete`（无 CK 版）、`CKEnrichedPublication` 继承的 marker 写入。
  - store 新增 `generation_history(asset_type, limit)` 与 `publishing_generations(asset_type)`；`enriched_generation.py` 新增 `_ck_snapshot()` 从 CK ready 历史重建 range 快照（全量重建 changed_from=None 退化为空 ranges，保证最后一个 token=当前 generation 的不变量）。
  - `get_enriched_generation_snapshot` / `enriched_publication_incomplete` / `bump_enriched_generation` 改为 CK 优先、无 storage 回退 marker；CK incomplete 只判定心跳超时的僵死发布，避免误伤调用方自己刚开的 generation。
  - `CKEnrichedPublication` 覆写生命周期（begin/heartbeat/commit/abandon/`_locked_claim`）完全停止读写本地 marker；`recompute_daily_symbols` 显式 storage 下也改用 CK 适配器。`api/data.py` 清库路径保留本地 publication 作为运维互斥兼容（非在线读依赖）。
  - 新增 `test_generation_ck_snapshot.py`；迁移 `test_enriched_generation.py` 的部分写失败断言到 CK generation。离线无 storage 路径仍保留 marker 作为兼容。
- 派生缓存归属收口（W3 归属表落盘后的在线硬依赖修复）：
  - `mining_preflight.enriched_partition_dates`、`mining_schedule._prerequisite_error` 改 CK 优先，修复 CK-only 下挖掘前置检查读本地 enriched/regime 目录失效。
  - `sector_rotation._latest_minute_partition` 改 CK 优先（`latest_minute_datetime`），消除板块轮动对本地分钟分区目录的隐式权威。
  - 归属最终结论：regime → 在线 CK `regime_history` + 离线 parquet 兼容；market_mainline → CK；sector → 本地热缓存（读已 CK 化）；factors → 本地内容寻址热缓存；mining → 作业产物（非派生缓存），CK 化转入后续 ISSUE。
- 测试隔离修复：`ck_runtime` fixture teardown 不再恢复可能泄漏的 prev runtime（先 drop 表再 shutdown），`test_ch_runtime` 加 autouse 隔离；修复 `replace_regime_range` 空帧序列化崩溃。
- W4 收尾：ext 帧缓存跨 data_dir 误命中修复（W1 遗留缺陷，已闭环）：
  - 根因（经探针脚本确认）：W1（RC-09）把 `_frame_cache` 键改为 `(config_id, mode, 内容签名)` 后去掉了 root 路径，但命中时不再校验 owner。不同 `data_dir`、相同 `config_id`、相同分区日期集合的两个测试/实例命中同一帧条目，后者的 `invalidate_ext_caches(自己的 data_dir)` 因 owner 不匹配清不掉，读到前者的旧扩展值。与 matrix 磁盘缓存无关（matrix 缓存字段已剔除 ext 列，ext 是 load 后现场注入）。
  - 修复：`_timeseries_frame` / `_snapshot_frame` 命中缓存时增加 `cached[0] == root_id` 校验，owner 不符则重读并覆盖条目（保留内容寻址共享语义，只保证当前 root 读到自己的数据）。
  - `RedisLimiterCounter.try_acquire/release` 的 WATCH 冲突重试加上限 `_MAX_CAS_RETRIES=16`，避免异常 client 导致无限循环。
- 验证结果：
  - W4 定向 + 扩大回归（generation / enriched / 指数 / regime / mining / sector / matrix ext）共 `113+` 项全绿；`pre-commit run --files` 全绿；`git diff --check` 通过。
  - **memory 模式全量：`4848 passed / 13 skipped / 1 xfailed / 0 failed`**（红灯清零）。
  - redis 模式核心契约定向验证：真实 Redis 容器下 `test_redis_cache.py` / `test_cache_backend_integration.py` / `test_events_redis.py` / `test_coordination_redis.py` 共 `15 passed`；redis 模式全量因大量测试硬编码内存协调语义且 Redis 往返开销高，不作为常规门禁。

### 7.6 W5 前置：RC-14 / RC-15 验收脚本（2026-12-19）已完成

- RC-14 应用层 Redis 冒烟：新增 `backend/scripts/rc14_redis_smoke.py`，进程内以 Redis 缓存装配 runtime，覆盖四项验收：
  - 健康检查 `cache_health()` 报 `redis/ok`；
  - generation bump：旧条目失效且计数递增；
  - 池缓存读：重命名本地 parquet 后仍从 Redis 读到预置列表；
  - flush（`cache.clear`）后条目消失、generation 保留、重新写入可读回。
  - 实测 `4/4 passed`，报告归档 `backend/.artifacts/rc14_redis_smoke.json`（gitignore，不入库）。
  - 用法：`CACHE_BACKEND=redis CACHE_REDIS_URL=... uv run --extra cache-redis python scripts/rc14_redis_smoke.py`。
- RC-15 满量性能标定：新增 `backend/scripts/rc15_perf_baseline.py`，`--mode both --samples 50` 双模式采样，输出 P50/P95/P99、峰值 RSS、Redis used_memory / 近似 QPS；CK 写延迟用随机前缀 Memory 临时表测量、结束即 drop 不污染业务表。
  - 报告归档 `backend/.artifacts/rc15_perf_baseline.json`（gitignore）。实测（本机，非满量交易数据）：
    - 缓存操作 P95：memory 亚毫秒；redis `set_json 0.104ms / set_df 0.277ms / get_json 0.096ms / get_df 0.046ms / bump 0.108ms`。
    - CK 读 P95：memory `4.789ms`、redis `4.474ms`；CK 写 P95：memory `1.215ms`、redis `1.0ms`（两模式一致，验证缓存改造不影响 CK 路径）。
    - 峰值 RSS：memory `126.0MB`、redis `140.3MB`（+14.3MB）；Redis `used_memory 1,693,544B`（受控）、近似 QPS `24,314`（探针窗口短，仅作量级参考）。
  - 应用容器默认 `--out /app/data/...`；本地/宿主机运行需显式 `--out` 到可写目录（仓库根 `data/` 属 root 不可写）。
  - 结论：无明显性能回退，Redis 内存占用受控，满足 RC-15 验收；满量交易规模的绝对数值需在部署环境复测。
- 两个脚本均通过 pre-commit 全门禁（ruff / ruff-format / 函数行数 / mypy / 调试残留等）。

### 7.7 RC-05 实现（2026-11-19）

- 新增 `backend/app/services/strategy_cache_redis.py`：以 `data_dir` 摘要为 Hash 键，Lua 脚本在 Redis 端原子完成“读旧值 → 同日按策略并集合并 / 跨日重置 → 写新值 → version 递增”；进程内镜像以 version 判失效，命中时只做一次 HGET，不搬运整包。`RedisCacheBackend` 暴露 `client()` / `entry_prefix()` 供该 store 使用。
- `strategy_cache.py` 仅在 `CACHE_BACKEND=redis` 且装配 `RedisCacheBackend` 时分发到新 store；默认 memory 模式与离线文件逻辑零改动，保护既有 4848 基线。
- 验证：`test_strategy_cache_redis.py`（同日多策略合并、跨日重置、8 线程并发不丢状态、镜像失效、clear、镜像命中）+ 既有文件后端测试共 `15 passed`；pre-commit 全门禁（ruff / ruff-format / 函数行数 / mypy / 注释覆盖率）全绿。
- 全量回归按用户要求推迟到后续批次；真实数据量下 Lua 合并耗时与内存放大待压测。跟踪转入 ISS-004。

## 8. 当前结论

- Redis 能解决跨进程缓存共享、generation 计数和缓存失效一致性的一部分问题。
- Redis 不能解决 CK 权威数据、逐 tick 热缓存、mmap 矩阵缓存、内容寻址因子 parquet，也不能替代 CK generation 作为在线发布状态。
- W2 已完成 CK-only 主体收口：显式 storage 下常规 enriched 编排与指数 enriched 的在线写入、generation、内存缓存刷新均已走 CK，本地 enriched parquet 不再是任何在线读源（staging/离线兼容除外）。
- W3 已完成多实例一致性与派生缓存首批收口：EventBus 可切 Redis Pub/Sub，任务取消/限流/状态可跨实例共享，regime 派生缓存在线读源切 CK，并落盘四类派生缓存归属表。
- W4 已完成 RC-13 复核与清理：在线读路径全部消费 CK generation、CK 发布停止写本地 marker，派生缓存归属落盘并消除 sector/mining 的本地目录隐式权威；W1 遗留的 ext 帧缓存跨 data_dir 误命中亦已定位修复。
- RC-14（应用层 Redis 冒烟，4/4 passed）与 RC-15（双模式性能标定，报告已归档）均已完成，W4 发布基线的两项前置验证闭环。
- 本文列出的工作按优先级推进：W0/W1/W2/W3/W4 及 W5 前置已闭环（memory 全量当时 `4848 passed / 0 failed`，当前基线 `4898 passed`，见 ISS-004）；RC-05（strategy_cache.json 状态化）已于 2026-11-19 实现（§7.7）；RC-06 决策已登记且 ISS-004 标记闭环。剩余 mining CK 化与 W5 环境相关的 CH-04~CH-09 人工验收、文档/工程债，转入 [ISS-004](ISS-004-remaining-followups-and-acceptance.md)；全部未闭环项的最新汇总见 [ISS-005](ISS-005-outstanding-items-summary.md)。
