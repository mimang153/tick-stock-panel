"""StrategyEngine 分包实现：方法按 mixin 分片，无 super 调用。"""

from __future__ import annotations

import contextlib
import importlib.util
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import app.strategy.engine as _engine_pkg
from app.strategy.scoring import effective_scoring, scoring_warmup_bars

from ._common import (
    DEFAULT_BASIC_FILTER,
    MAX_COMPOSITE_CHILDREN,
    CompositeSpec,
    StrategyDataContext,
    StrategyDef,
    _normalize_param_defs,
    _parse_composite_children,
    _RealtimeMatrixEntry,
    _resolve_required_features,
    logger,
)


class _StrategyEngineMixin1:
    """策略引擎 — 策略加载 + 执行 + 评分"""

    _module_load_lock = threading.RLock()

    # 由组装类 StrategyEngine 的其它 mixin 提供，此处声明供 mypy 解析。
    _effective_overlay: Any
    _matrix_history_bars: Any

    def __init__(
        self,
        strategy_dirs: list[Path] | None = None,
        *,
        override_loader: Callable[[str], dict] | None = None,
    ):
        self._strategies: dict[str, StrategyDef] = {}
        self.definition_revision = 0
        self._load_errors: list[dict] = []  # 加载失败的策略 [{file, error}]
        self._strategy_dirs = strategy_dirs or []
        self._realtime_matrices: dict[str, _RealtimeMatrixEntry] = {}
        self._realtime_matrix_lock = threading.RLock()
        # run_all 共享矩阵进程级缓存: 矩阵 = 纯函数(history, 字段并集), 矩阵数组
        # 构建后只读, 同 generation 同窗口复用免重建 (全市场矩阵构建数秒)。
        # 失效: 指纹含 enriched generation + 窗口范围 (数据/窗口变化自动重建),
        # reload/删除策略时与 _realtime_matrices 一同清空。
        self._shared_matrix_cache: dict[tuple, Any] = {}
        # 可选的 override 加载器: 叠加策略执行时用它查子策略的用户覆盖配置,
        # 保证 composite 内跑子策略与单独跑子策略使用同一口径(CONTRIBUTING §5.1)。
        # None 时(测试/无 data_dir) 子策略用默认参数, 不报错。
        self._override_loader = override_loader
        self._load_all(retain_previous_on_error=False)

    # ================================================================
    # 加载
    # ================================================================

    def _collect_candidates(
        self,
    ) -> tuple[dict[str, StrategyDef], dict[str, Path], list[dict]]:
        """加载所有策略文件，收集候选、路径与加载错误（含重复 id）。"""
        candidates: dict[str, StrategyDef] = {}
        candidate_paths: dict[str, Path] = {}
        errors: list[dict] = []
        duplicate_ids: set[str] = set()
        for d in self._strategy_dirs:
            if not d.exists():
                continue
            for f in sorted(d.glob("*.py")):
                if f.name.startswith("_"):
                    continue
                try:
                    s = self._load_file(f)
                    strategy_id = str(s.meta["id"])
                    if strategy_id in duplicate_ids:
                        errors.append(
                            {
                                "file": str(f),
                                "error": f"duplicate strategy id {strategy_id!r}",
                            }
                        )
                        continue
                    if strategy_id in candidates:
                        previous_path = candidate_paths.pop(strategy_id)
                        candidates.pop(strategy_id)
                        duplicate_ids.add(strategy_id)
                        message = f"duplicate strategy id {strategy_id!r}"
                        errors.extend(
                            [
                                {"file": str(previous_path), "error": message},
                                {"file": str(f), "error": message},
                            ]
                        )
                        continue
                    candidates[strategy_id] = s
                    candidate_paths[strategy_id] = f
                except Exception as e:  # noqa: BLE001 - 单个策略文件加载失败只记录, 继续加载其余策略
                    logger.warning("load strategy %s failed: %s", f.name, e)
                    errors.append({"file": str(f), "error": str(e)})
        return candidates, candidate_paths, errors

    def _load_all(self, *, retain_previous_on_error: bool) -> bool:
        candidates, candidate_paths, errors = self._collect_candidates()

        # 第二阶段: 校验 composite 引用合法性。
        # _load_file 是 staticmethod, 加载单个文件时无法判断 child 是否存在;
        # 此处已拿到全部 candidates, 可做引用、嵌套、asset_types 与数量校验。
        # 孤儿 composite (引用不合法) 被移出 candidates 并记入 errors,
        # 但不触发整体 reload 失败 —— 不波及其他正常策略(插件隔离原则)。
        for sid in list(candidates):
            strategy = candidates[sid]
            if strategy.execution_backend != "composite" or strategy.composite is None:
                continue
            error = self._validate_composite_references(sid, strategy, candidates)
            if error is not None:
                errors.append(
                    {
                        "file": str(strategy.file_path) if strategy.file_path else sid,
                        "error": error,
                    }
                )
                candidates.pop(sid, None)
                candidate_paths.pop(sid, None)

        self._load_errors = errors
        if errors and retain_previous_on_error:
            return False

        self._strategies = candidates
        for strategy_id, strategy in candidates.items():
            logger.debug("loaded strategy: %s (%s)", strategy_id, strategy.source)
        return not errors

    def load_errors(self) -> list[dict]:
        """返回最近一次 _load_all 中加载失败的策略 [{file, error}]。"""
        return list(self._load_errors)

    @staticmethod
    def _validate_composite_references(
        sid: str,
        strategy: StrategyDef,
        candidates: dict[str, StrategyDef],
    ) -> str | None:
        """校验 composite 策略的引用合法性。返回错误描述或 None。

        规则(首版硬约束):
        - 每个 child 必须已加载(candidates 中存在)
        - 禁止 composite 嵌套 composite(子策略必须是叶子)
        - child 的 asset_types / timeframes 必须与父 composite 完全一致
        - 数量 <= MAX_COMPOSITE_CHILDREN
        任一不满足返回错误描述, 由 _load_all 移除该孤儿策略(不波及无辜)。
        """
        assert strategy.composite is not None
        children = strategy.composite.children
        if len(children) > MAX_COMPOSITE_CHILDREN:
            return (
                f"composite strategy {sid} children count {len(children)} "
                f"exceeds limit {MAX_COMPOSITE_CHILDREN}"
            )
        parent_assets = list(strategy.meta.get("asset_types", ["stock"]))
        parent_timeframes = list(strategy.meta.get("timeframes", ["1d"]))
        for child in children:
            child_def = candidates.get(child.strategy_id)
            if child_def is None:
                return f"composite strategy {sid} 引用的子策略 {child.strategy_id!r} 不存在"
            if child_def.execution_backend == "composite":
                return (
                    f"composite strategy {sid} 引用的子策略 {child.strategy_id!r} "
                    f"也是叠加策略; 首版禁止嵌套叠加"
                )
            child_assets = list(child_def.meta.get("asset_types", ["stock"]))
            if not set(parent_assets).issubset(set(child_assets)):
                return (
                    f"composite strategy {sid} 的 asset_types {parent_assets} "
                    f"未被子策略 {child.strategy_id!r} 完全支持(支持 {child_assets})"
                )
            child_timeframes = list(child_def.meta.get("timeframes", ["1d"]))
            if not set(parent_timeframes).issubset(set(child_timeframes)):
                return (
                    f"composite strategy {sid} 的 timeframes {parent_timeframes} "
                    f"未被子策略 {child.strategy_id!r} 完全支持(支持 {child_timeframes})"
                )
        return None

    @staticmethod
    def _validate_source_safety(path: Path) -> frozenset[str]:
        """执行前对策略文件及其 _ 前缀依赖做 AST 安全校验，返回依赖模块名集合。"""
        dependency_paths = [
            candidate for candidate in path.parent.glob("_*.py") if candidate != path
        ]
        dependency_names = frozenset(candidate.stem for candidate in dependency_paths)
        try:
            code = path.read_text(encoding="utf-8")
            from app.strategy.ai_generator import AIStrategyGenerator

            AIStrategyGenerator._validate_safety(
                code,
                extra_allowed_import_modules=dependency_names,
            )
            for dependency_path in dependency_paths:
                AIStrategyGenerator._validate_safety(
                    dependency_path.read_text(encoding="utf-8"),
                    extra_allowed_import_modules=frozenset(
                        {
                            "collections.abc",
                            "types",
                            "typing",
                        }
                    ),
                )
        except ValueError:
            raise
        except SyntaxError as e:
            raise ValueError(f"策略语法错误: 第 {e.lineno} 行, {e.msg}") from e
        except (OSError, UnicodeDecodeError) as e:
            # fail-closed: 文件读不到/读不懂时不得放行到 exec_module,
            # 否则异常输入即可让校验静默跳过 (GH #475 同类逃逸)。
            raise ValueError(f"策略安全校验失败: {e}") from e
        return dependency_names

    @staticmethod
    def _exec_strategy_module(path: Path, dependency_names: frozenset[str]):
        """在模块锁内加载并执行策略模块，失败时恢复 sys.modules；返回模块对象。"""
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec is None or spec.loader is None:
            raise ValueError(f"cannot load module from {path}")
        mod = importlib.util.module_from_spec(spec)
        with _engine_pkg.StrategyEngine._module_load_lock:
            previous_module = sys.modules.get(spec.name)
            sys.modules[spec.name] = mod
            inserted_path = str(path.parent)
            sys.path.insert(0, inserted_path)
            try:
                for dependency_name in dependency_names:
                    sys.modules.pop(dependency_name, None)
                spec.loader.exec_module(mod)
            except Exception:
                if previous_module is None:
                    sys.modules.pop(spec.name, None)
                else:
                    sys.modules[spec.name] = previous_module
                raise
            finally:
                with contextlib.suppress(ValueError):
                    sys.path.remove(inserted_path)
        return mod

    @staticmethod
    def _load_meta(mod, path: Path) -> tuple[dict, str]:
        """归一化 META 默认值、来源、资产/周期白名单与 params；返回 (meta, source)。"""
        meta = dict(getattr(mod, "META", {}) or {})
        meta.setdefault("id", path.stem)
        meta.setdefault("name", path.stem)
        meta.setdefault("description", "")
        meta.setdefault("tags", [])
        meta.setdefault("params", [])
        meta.setdefault("scoring", {})
        meta.setdefault("order_by", "score")
        meta.setdefault("descending", True)
        meta.setdefault("limit", 100)

        # research/ 目录: 挖掘功能的研究模板 (research_only), 对外按 custom 口径。
        source = "custom"
        normalized_path = str(path).replace("\\", "/")
        if "/ai/" in normalized_path:
            source = "ai"
        elif "/composite/" in normalized_path:
            source = "composite"

        meta.setdefault("asset_types", ["stock"])
        meta.setdefault("timeframes", ["1d"])
        for field_name in ("asset_types", "timeframes"):
            values = meta.get(field_name)
            if (
                not isinstance(values, list | tuple)
                or not values
                or any(not isinstance(value, str) or not value for value in values)
            ):
                raise ValueError(f"META[{field_name!r}] must be a non-empty string list")
            meta[field_name] = list(dict.fromkeys(values))

        # 归一化 params 为标准 list[dict]: custom/AI 策略的 META["params"] 可能是
        # dict / list[str] 等非标准格式 (LLM 偶发漂移 / 用户手改), 不归一化的话会在
        # _strategy_detail() 的 {p["id"]: p["default"] for p in params} 处抛 TypeError,
        # 导致整个 /api/strategies 列表 500. 降级为空 list 而非崩溃, 策略仍可见可用.
        meta["params"] = _normalize_param_defs(meta.get("params"))
        return meta, source

    @staticmethod
    def _basic_filter(mod, meta: dict) -> dict:
        """合并默认基础过滤、模块 BASIC_FILTER 与 meta.basic_filter。"""
        bf = {**DEFAULT_BASIC_FILTER}
        strat_bf = getattr(mod, "BASIC_FILTER", None)
        if strat_bf:
            bf.update(strat_bf)
        # meta 里的 basic_filter 也合并（优先级最高）
        meta_bf = meta.get("basic_filter")
        if meta_bf:
            bf.update(meta_bf)
        return bf

    @staticmethod
    def _validate_minute_filter(
        meta: dict,
        filter_minute_history_fn,
        filter_fn,
        filter_history_fn,
        matrix_strategy,
    ) -> int:
        """校验 minute_filter 形态约束，返回可选日线历史窗口。"""
        # 分钟形态策略: 只声明 filter_minute_history; 数据源是本地当日分钟K分区
        # (由 ScreenerService.build_strategy_context 的 1m 分支注入), 因此 timeframes
        # 必须且只能是 ["1m"] — 混入 1d 会让日线 context 走错数据路径。
        if (
            filter_minute_history_fn is None
            or filter_fn is not None
            or filter_history_fn is not None
            or matrix_strategy is not None
        ):
            raise ValueError("minute_filter strategy must declare only filter_minute_history")
        if meta.get("timeframes") != ["1m"]:
            raise ValueError("minute_filter strategy must declare timeframes == ['1m']")
        # 可选日线历史窗口: 声明 daily_history_bars 时 fn 必须接受 daily 关键字,
        # 引擎会把 context.daily_history (enriched 日线窗口) 注入进来。
        minute_daily_bars = int(meta.get("daily_history_bars") or 0)
        if minute_daily_bars < 0 or minute_daily_bars > 250:
            raise ValueError("minute_filter daily_history_bars must be within [0, 250]")
        if minute_daily_bars > 0:
            import inspect

            sig = inspect.signature(filter_minute_history_fn)
            if "daily" not in sig.parameters:
                raise ValueError(
                    "minute_filter daily_history_bars requires "
                    "filter_minute_history to accept a 'daily' keyword"
                )
        return minute_daily_bars

    @staticmethod
    def _validate_backend_constraints(
        meta: dict,
        execution_backend: str,
        filter_fn,
        filter_history_fn,
        filter_minute_history_fn,
        matrix_strategy,
    ) -> tuple[CompositeSpec | None, int]:
        """按 execution_backend 校验声明形态；返回 (composite_spec, minute_daily_bars)。"""
        composite_spec: CompositeSpec | None = None
        minute_daily_bars = 0
        if execution_backend == "matrix_native":
            from app.backtest.matrix import MatrixStrategy

            if matrix_strategy is None:
                raise ValueError("matrix_native strategy must declare MATRIX_STRATEGY")
            if not isinstance(matrix_strategy, MatrixStrategy):
                raise TypeError("MATRIX_STRATEGY must implement MatrixStrategy")
            if filter_fn is not None or filter_history_fn is not None:
                raise ValueError("matrix_native strategy must not declare filter or filter_history")
        elif execution_backend == "polars_expr":
            if filter_fn is None or filter_history_fn is not None:
                raise ValueError("polars_expr strategy must declare only filter")
        elif execution_backend == "composite":
            # 叠加策略是声明式的: 不含业务代码, 仅通过 META["children"] 引用其他策略。
            # 引用合法性(子策略存在/非嵌套/asset_types 一致/数量上限)延后到
            # _load_all 两阶段校验 —— 因为此时注册表尚未加载完, 无法判断 child 是否存在。
            if (
                filter_fn is not None
                or filter_history_fn is not None
                or matrix_strategy is not None
            ):
                raise ValueError(
                    "composite strategy must not declare filter, filter_history or MATRIX_STRATEGY"
                )
            composite_spec = _parse_composite_children(meta.get("children"))
        elif execution_backend == "minute_filter":
            minute_daily_bars = _engine_pkg.StrategyEngine._validate_minute_filter(
                meta, filter_minute_history_fn, filter_fn, filter_history_fn, matrix_strategy
            )
        elif filter_history_fn is None or filter_fn is not None:
            raise ValueError("python_history_legacy strategy must declare only filter_history")
        return composite_spec, minute_daily_bars

    @staticmethod
    def _resolve_execution_config(mod, meta: dict) -> tuple:
        """解析 filter/filter_history 与执行后端，返回执行配置元组。"""
        filter_fn = getattr(mod, "filter", None)
        filter_history_fn = getattr(mod, "filter_history", None)
        filter_minute_history_fn = getattr(mod, "filter_minute_history", None)
        execution_backend = str(
            getattr(
                mod,
                "EXECUTION_BACKEND",
                meta.get(
                    "execution_backend",
                    "python_history_legacy" if filter_history_fn else "polars_expr",
                ),
            )
        )
        valid_backends = {
            "polars_expr",
            "matrix_native",
            "python_history_legacy",
            "composite",
            "minute_filter",
        }
        if execution_backend not in valid_backends:
            raise ValueError(
                f"unsupported execution backend {execution_backend!r}; "
                f"expected one of {sorted(valid_backends)}"
            )

        matrix_strategy = getattr(mod, "MATRIX_STRATEGY", None)
        # 因子排名类策略 (custom_factor_*) 把评分因子写在模块 SCORING 常量里交给
        # MATRIX_STRATEGY 构造, 不进 META.scoring — 依赖闭包 (strategy_bundle) 需要
        # 在加载期拿到它, 与 ENTRY_SIGNALS 等常量同一口径。
        module_scoring = getattr(mod, "SCORING", None)
        scoring_columns: tuple[str, ...] = ()
        if isinstance(module_scoring, dict) and module_scoring:
            scoring_columns = tuple(str(k) for k in module_scoring)
        composite_spec, minute_daily_bars = (
            _engine_pkg.StrategyEngine._validate_backend_constraints(
                meta,
                execution_backend,
                filter_fn,
                filter_history_fn,
                filter_minute_history_fn,
                matrix_strategy,
            )
        )
        return (
            execution_backend,
            filter_fn,
            filter_history_fn,
            filter_minute_history_fn,
            matrix_strategy,
            composite_spec,
            minute_daily_bars,
            scoring_columns,
        )

    @staticmethod
    def _load_file(path: Path) -> StrategyDef:
        """从 Python 文件加载策略定义"""
        # 纵深防御: 执行前再跑一次 AST 安全校验, 防止策略文件被直接篡改
        # 绕过 API 校验后, 在 exec_module 时执行恶意代码。
        dependency_names = _engine_pkg.StrategyEngine._validate_source_safety(path)
        mod = _engine_pkg.StrategyEngine._exec_strategy_module(path, dependency_names)

        meta, source = _engine_pkg.StrategyEngine._load_meta(mod, path)
        bf = _engine_pkg.StrategyEngine._basic_filter(mod, meta)
        (
            execution_backend,
            filter_fn,
            filter_history_fn,
            filter_minute_history_fn,
            matrix_strategy,
            composite_spec,
            minute_daily_bars,
            scoring_columns,
        ) = _engine_pkg.StrategyEngine._resolve_execution_config(mod, meta)

        return StrategyDef(
            meta=meta,
            basic_filter=bf,
            entry_signals=getattr(mod, "ENTRY_SIGNALS", []),
            exit_signals=getattr(mod, "EXIT_SIGNALS", []),
            stop_loss=getattr(mod, "STOP_LOSS", None),
            take_profit=getattr(mod, "TAKE_PROFIT", None),
            trailing_stop=getattr(mod, "TRAILING_STOP", None),
            trailing_take_profit_activate=getattr(mod, "TRAILING_TAKE_PROFIT_ACTIVATE", None),
            trailing_take_profit_drawdown=getattr(mod, "TRAILING_TAKE_PROFIT_DRAWDOWN", None),
            max_hold_days=getattr(mod, "MAX_HOLD_DAYS", None),
            filter_fn=filter_fn,
            filter_history_fn=filter_history_fn,
            required_features=_resolve_required_features(meta, mod, filter_history_fn, path),
            lookback_days=int(getattr(mod, "LOOKBACK_DAYS", meta.get("lookback_days", 1)) or 1),
            source=source,
            file_path=path,
            execution_backend=execution_backend,
            matrix_strategy=matrix_strategy,
            composite=composite_spec,
            filter_minute_history_fn=filter_minute_history_fn,
            minute_daily_bars=minute_daily_bars,
            scoring_columns=scoring_columns,
        )

    def reload(self) -> None:
        """原子热重载；任一策略失败时保留上一版注册表。"""
        if not self._load_all(retain_previous_on_error=True):
            details = "; ".join(f"{item['file']}: {item['error']}" for item in self._load_errors)
            raise ValueError(f"strategy reload failed: {details}")
        with self._realtime_matrix_lock:
            self._realtime_matrices.clear()
            self._shared_matrix_cache.clear()
            self.definition_revision += 1

    # ================================================================
    # 查询
    # ================================================================

    def list_strategies(self, *, include_research: bool = False) -> list[dict]:
        """Return public strategy metadata unless research templates are requested."""
        result = []
        for s in self._strategies.values():
            if s.meta.get("research_only") and not include_research:
                continue
            result.append(
                {
                    **s.meta,
                    "source": s.source,
                    "execution_backend": s.execution_backend,
                }
            )
        return result

    def strategy_definitions(self) -> tuple[StrategyDef, ...]:
        """Return the immutable registry snapshot for framework dependency planning."""
        return tuple(self._strategies.values())

    def get(self, strategy_id: str) -> StrategyDef:
        s = self._strategies.get(strategy_id)
        if not s:
            raise ValueError(f"unknown strategy: {strategy_id}")
        return s

    def has(self, strategy_id: str) -> bool:
        return strategy_id in self._strategies

    def unregister(self, strategy_id: str) -> bool:
        """从运行时注册表移除单个策略, 不重新加载其他策略文件。"""
        if strategy_id not in self._strategies:
            return False
        strategies = dict(self._strategies)
        strategies.pop(strategy_id)
        self._strategies = strategies
        self.definition_revision += 1
        with self._realtime_matrix_lock:
            self._realtime_matrices.clear()
            self._shared_matrix_cache.clear()
        return True

    def find_dependents(self, strategy_id: str) -> list[str]:
        """返回引用了 strategy_id 作为子策略的所有 composite 策略 id。

        供删除校验使用: 删除被引用的子策略会令 composite 加载失败,
        删除前应阻止(fail-closed)或提示用户先解除引用。策略数量通常很小,
        线性遍历注册表即可, 无需维护反向索引。
        """
        dependents: list[str] = []
        for sid, strategy in self._strategies.items():
            if strategy.execution_backend != "composite" or strategy.composite is None:
                continue
            if any(c.strategy_id == strategy_id for c in strategy.composite.children):
                dependents.append(sid)
        return dependents

    @staticmethod
    def validate_context(strategy: StrategyDef, context: StrategyDataContext) -> None:
        asset_types = strategy.meta.get("asset_types", ["stock"])
        if context.asset_type not in asset_types:
            raise ValueError(
                f"strategy {strategy.meta['id']} does not support asset_type "
                f"{context.asset_type!r}; supported={asset_types}"
            )
        timeframes = strategy.meta.get("timeframes", ["1d"])
        if context.timeframe not in timeframes:
            raise ValueError(
                f"strategy {strategy.meta['id']} does not support timeframe "
                f"{context.timeframe!r}; supported={timeframes}"
            )

    @staticmethod
    def resolve_params(
        strategy: StrategyDef,
        params: dict | None = None,
        overrides: dict | None = None,
    ) -> dict:
        """Resolve one parameter source of truth for every strategy consumer."""
        resolved = {
            item["id"]: item.get("default")
            for item in strategy.meta.get("params", [])
            if isinstance(item, dict) and item.get("id")
        }
        saved = (overrides or {}).get("params")
        if isinstance(saved, dict):
            resolved.update(saved)
        if params:
            resolved.update(params)
        return resolved

    @staticmethod
    def _result_limit(strategy: StrategyDef, overrides: dict | None) -> int | None:
        if overrides and "display_limit" in overrides:
            value = overrides.get("display_limit")
            if value in (None, 0):
                return None
            return max(0, int(value))
        value = strategy.meta.get("limit", 100)
        if value in (None, 0):
            return None
        return max(0, int(value))

    def minute_daily_history_bars(self, strategy_ids: list[str]) -> int:
        """1m 分支需要的日线 enriched 窗口大小: 各 minute_filter 策略声明的
        META["daily_history_bars"] 取 max, 未声明 (纯分钟策略) 为 0。"""
        required = 0
        for strategy_id in strategy_ids:
            strategy = self.get(strategy_id)
            if strategy.execution_backend == "minute_filter":
                required = max(required, strategy.minute_daily_bars)
        return required

    def required_history_bars(
        self,
        strategy_ids: list[str],
        *,
        params_map: dict[str, dict] | None = None,
        overrides_map: dict[str, dict] | None = None,
    ) -> int:
        params_map = params_map or {}
        overrides_map = overrides_map or {}
        required = 1
        for strategy_id in strategy_ids:
            strategy = self.get(strategy_id)
            overrides = overrides_map.get(strategy_id) or {}
            # 叠加条件含「前 N 日」偏移时, 历史窗口需覆盖其回看深度 (as_of + N 个交易日)。
            overlay = self._effective_overlay(overrides)
            if overlay:
                from app.strategy import custom_signals as _custom_signals

                overlay_days = _custom_signals.overlay_max_days(overlay)
                if overlay_days > 0:
                    required = max(required, overlay_days + 1)
            scoring = effective_scoring(strategy.meta.get("scoring"), overrides)
            required = max(required, scoring_warmup_bars(scoring))
            if strategy.execution_backend == "matrix_native":
                params = self.resolve_params(
                    strategy,
                    params_map.get(strategy_id),
                    overrides,
                )
                required = max(
                    required,
                    self._matrix_history_bars(strategy, params),
                )
            elif strategy.execution_backend == "composite":
                # composite 预热 = 各子策略预热的 max。
                # 子策略已通过加载期校验(非嵌套叶子), 这里展开一层即可。
                if strategy.composite is None:
                    continue
                child_ids = [c.strategy_id for c in strategy.composite.children]
                required = max(
                    required,
                    self.required_history_bars(child_ids, params_map=params_map),
                )
            elif strategy.filter_history_fn:
                # lookback_days 优先取自解析后的参数(默认值/保存覆盖/本次调用),
                # 静态 LOOKBACK_DAYS 兜底。策略可能只把窗口声明为参数
                # (如 AI 生成策略), 此时 strategy.lookback_days 回退到 1,
                # 不解析参数会低估历史需求 → build_strategy_context 跳过加载 → 运行时报错。
                params = self.resolve_params(
                    strategy,
                    params_map.get(strategy_id),
                    overrides_map.get(strategy_id),
                )
                lookback = int(strategy.lookback_days)
                param_lookback = params.get("lookback_days")
                if isinstance(param_lookback, int | float) and param_lookback > 0:
                    lookback = max(lookback, int(param_lookback))
                required = max(required, lookback)
        return required
