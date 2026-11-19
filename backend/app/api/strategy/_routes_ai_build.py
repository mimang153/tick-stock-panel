"""strategy API 路由分组 _routes_ai_build（由 strategy.py 机械拆分，顺序不变）。"""

from __future__ import annotations

import contextlib
import json

from fastapi import APIRouter, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from starlette.concurrency import run_in_threadpool

from app.services import strategy_pool
from app.strategy.ai_generator import AIStrategyGenerator
from app.strategy.prompt_builder import build_step1

from ._common import (
    _BUNDLE_FILE,
    AIGenerateRequest,
    AIIterateRequest,
    AISaveRequest,
    BuildRequest,
    StrategyCodeSaveRequest,
    StrategyCodeValidateRequest,
    StrategyCompositeSaveRequest,
    _api_pkg,
    _apply_strategy_bundle,
    _build_execution_backend,
    _build_prompt,
    _bundle_service,
    _cleanup_deleted_strategy,
    _data_dir,
    _get_engine,
    _get_public_strategy,
    _invalidate_strategy_runtime,
    _normalize_build_result,
    _prepare_strategy_code,
    _restore_strategy_file,
    _save_composite_strategy,
    _save_strategy_code,
    _set_meta_bool_field,
    _validate_strategy_id,
    logger,
)

router = APIRouter(prefix="/api/strategies")


@router.get("/ai/status")
def ai_status(request: Request):
    """Check whether the selected AI provider is configured."""
    from app import secrets_store
    from app.services.ai_provider import (
        ai_configured,
        current_ai_model,
        current_ai_provider,
        supports_ai_tools,
    )

    has_key = bool(secrets_store.get_ai_key())
    model = current_ai_model()
    provider = current_ai_provider()
    return {
        "configured": ai_configured(provider) and bool(model or provider == "codex_cli"),
        "has_key": has_key,
        "has_model": bool(model),
        "provider": provider,
        "supports_tools": supports_ai_tools(provider),
    }


@router.get("/{strategy_id}/source")
def get_strategy_source(strategy_id: str, request: Request):
    """获取策略源文件内容（用于 AI 修改）"""

    # 先查 StrategyEngine 获取文件路径
    engine = _get_engine(request)
    s = _get_public_strategy(engine, strategy_id)

    path = s.file_path
    if not path or not path.exists():
        raise HTTPException(status_code=404, detail="策略源文件不存在")

    return {"code": path.read_text(encoding="utf-8"), "source": s.source}


@router.get("/{strategy_id}/bundle")
def export_strategy_bundle(strategy_id: str, request: Request):
    """导出策略包 ZIP: 策略 .py + 引用的信号/因子定义 + 用户调参 (依赖闭包)。"""
    import io

    engine = _get_engine(request)
    _get_public_strategy(engine, strategy_id)  # research_only 404 口径与源码端点一致
    from app.config import settings

    try:
        data, filename, _summary = _bundle_service().build_bundle(
            settings.data_dir, engine, strategy_id
        )
    except _bundle_service().StrategyBundleError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/bundle/inspect")
async def inspect_strategy_bundle(file: UploadFile = _BUNDLE_FILE):
    """导入预览: 条目清单 (create/conflict) + 缺失依赖预警, 不落盘。"""
    data = await file.read()
    try:
        return await run_in_threadpool(_bundle_service().inspect_bundle, data)
    except _bundle_service().StrategyBundleError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/bundle/apply")
async def apply_strategy_bundle(
    request: Request, file: UploadFile = _BUNDLE_FILE, allow_overwrite: bool = Form(False)
):
    """导入策略包: 复用配置备份的校验/写盘/导入即冒烟/热重载管线。"""
    data = await file.read()
    return await run_in_threadpool(_apply_strategy_bundle, request, data, allow_overwrite)


@router.post("/ai/test")
async def ai_test(request: Request):
    """Send a small prompt through the selected AI provider."""
    from app.services.ai_provider import current_ai_model, current_ai_provider, generate_ai_text

    try:
        text = await generate_ai_text(
            [{"role": "user", "content": "Reply exactly: OK"}],
            temperature=0,
            max_tokens=8,
            timeout=15,
        )
        return {
            "ok": True,
            "model": current_ai_model() or current_ai_provider(),
            "response": text[:80],
        }
    except Exception as e:  # noqa: BLE001 - AI 构建外部输出不可控, 捕获后返回结构化失败
        return {
            "ok": False,
            "error": str(e) or repr(e),
            "error_type": type(e).__name__,
        }


@router.post("/build")
async def build_strategy(req: BuildRequest, request: Request):
    """两步策略构建。
    step1: name + description + direction + rules → 完整策略
    step2: current_code + instruction → 修改任意部分
    """
    gen = AIStrategyGenerator(
        execution_backend=_build_execution_backend(req), data_dir=_data_dir(request)
    )

    try:
        prompt = _build_prompt(req)
        result = await gen.generate(prompt)
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if req.step == 1:
        result = _normalize_build_result(result, req.strategy_id, req.name, req.description)
    elif req.strategy_id:
        result = _normalize_build_result(result, req.strategy_id)
    return result


@router.post("/build/stream")
async def build_strategy_stream(req: BuildRequest, request: Request):
    try:
        prompt = _build_prompt(req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    async def event_generator():
        gen = AIStrategyGenerator(
            execution_backend=_build_execution_backend(req), data_dir=_data_dir(request)
        )
        chunks: list[str] = []
        yield json.dumps(
            {"type": "meta", "strategy_id": req.strategy_id, "step": req.step}, ensure_ascii=False
        )
        try:
            async for chunk in gen.stream(prompt):
                chunks.append(chunk)
                yield json.dumps({"type": "delta", "content": chunk}, ensure_ascii=False)
            result = gen.validate_code("".join(chunks))
            if gen.needs_structural_repair(result):
                result = await gen.repair_code(result["code"], result["error"])
            if req.step == 1:
                result = _normalize_build_result(result, req.strategy_id, req.name, req.description)
            elif req.strategy_id:
                result = _normalize_build_result(result, req.strategy_id)
            yield json.dumps({"type": "result", **result}, ensure_ascii=False)
        except RuntimeError as e:
            yield json.dumps({"type": "error", "message": str(e)}, ensure_ascii=False)
        except Exception as e:  # noqa: BLE001 - AI 流式生成外部不可控, 捕获后推送错误事件
            yield json.dumps({"type": "error", "message": f"AI生成失败: {e}"}, ensure_ascii=False)

    # 与复盘/个股/财务/轮动的流式端点同口径: 事件行不带换行, 由外层统一补换行。
    # with_heartbeat 插入的 ping 行本身不带换行, 事件行若自带换行, ping 会与下一行粘成一行。
    async def stream_gen():
        async for line in _api_pkg.with_heartbeat(event_generator()):
            yield line + "\n"

    return StreamingResponse(
        stream_gen(),
        media_type="application/x-ndjson",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/ai/generate")
async def ai_generate(req: AIGenerateRequest, request: Request):
    try:
        gen = AIStrategyGenerator(data_dir=_data_dir(request))
        result = await gen.generate(req.prompt)
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI生成失败: {e}") from e
    return result


@router.post("/ai/iterate")
async def ai_iterate(req: AIIterateRequest, request: Request):
    """生成→回测→诊断→修改 的有界闭环 (只读回测, 产物为 ai_ 草稿, 不自动上线)。"""
    from app.services.ai_provider import AIToolProtocolError, supports_ai_tools
    from app.strategy.ai_iterator import AIStrategyIterator

    if not supports_ai_tools():
        raise HTTPException(
            status_code=400,
            detail="当前 AI 接入不支持工具调用，请选择支持工具调用的 AI 接入方式",
        )

    engine = _get_engine(request)
    data_dir = _data_dir(request)
    try:
        prompt = build_step1(
            req.name,
            req.description,
            req.direction,
            req.rules,
            strategy_id="",
            execution_backend=req.execution_backend,
        )
        iterator = AIStrategyIterator(max_rounds=req.max_rounds)
        result = await iterator.iterate(
            prompt,
            engine=engine,
            data_dir=str(data_dir),
            execution_backend=req.execution_backend,
            repo=request.app.state.repo,
        )
    except (ValueError, RuntimeError, AIToolProtocolError) as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception:  # noqa: BLE001 - AI 迭代外部不可控, 详情只进日志不透给客户端 (§8)
        # 内部异常详情只进日志, 不透给客户端 (§8)
        logger.exception("AI 迭代失败")
        raise HTTPException(status_code=500, detail="AI 迭代失败, 请稍后重试") from None
    return result


@router.post("/code/validate")
def validate_strategy_code(req: StrategyCodeValidateRequest, request: Request):
    try:
        prepared = _prepare_strategy_code(req)
        return {"valid": True, "error": None, **prepared}
    except Exception as e:  # noqa: BLE001 - AI 校验外部输出不可控, 捕获后返回结构化失败
        return {"valid": False, "error": str(e), "code": req.code, "meta": {}}


@router.post("/code/save")
def save_strategy_code(req: StrategyCodeSaveRequest, request: Request):
    try:
        return _save_strategy_code(req, request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/composite/save")
def save_composite_strategy(req: StrategyCompositeSaveRequest, request: Request):
    try:
        return _save_composite_strategy(req, request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/ai/save")
async def ai_save(req: AISaveRequest, request: Request):
    try:
        save_req = StrategyCodeSaveRequest(
            code=req.code,
            strategy_id=req.strategy_id,
            target_source="ai",
            mode="create",
            name=req.name,
            description=req.description,
        )
        result = _save_strategy_code(save_req, request, legacy_ai_path=True)
        return {"ok": True, "path": result["path"]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/{strategy_id}/publish")
def publish_ai_strategy(strategy_id: str, request: Request):
    """把 research_only 的 AI 草稿策略翻转为公开(research_only=False)。

    门 = 人的显式动作: 只有 AI 来源且仍处于草稿态的策略才能被发布。
    发布后即进入公开列表、可 run、可监控。
    """
    sid = _validate_strategy_id(strategy_id)
    engine = _get_engine(request)
    try:
        s = engine.get(sid)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=f"策略 {sid} 不存在") from e

    if s.source != "ai":
        raise HTTPException(status_code=400, detail="仅 AI 策略可经发布端点上线")
    if not s.meta.get("research_only"):
        raise HTTPException(status_code=400, detail="该策略已是公开状态")

    path = s.file_path
    if path is None:
        raise HTTPException(status_code=400, detail="策略源文件路径无效, 无法发布")
    previous_code = path.read_text(encoding="utf-8")
    path.write_text(_set_meta_bool_field(previous_code, "research_only", False), encoding="utf-8")

    try:
        engine.reload()
        loaded = engine.get(sid)
        if loaded.meta.get("research_only"):
            raise ValueError("发布后策略仍为草稿态")
    except Exception as e:
        _restore_strategy_file(path, previous_code)
        engine.reload()
        raise HTTPException(status_code=500, detail=f"策略发布失败: {e}") from e

    _invalidate_strategy_runtime(request)
    # 发布即进入公开库, 登记加入时间 (幂等)
    with contextlib.suppress(Exception):
        strategy_pool.register(sid)
    return {"ok": True, "strategy_id": sid}


@router.delete("/{strategy_id}")
def delete_strategy(strategy_id: str, request: Request):
    """删除自定义策略 — 清除源文件、运行时注册和关联状态。"""

    engine = _get_engine(request)
    try:
        s = engine.get(strategy_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=f"策略 {strategy_id} 不存在") from e

    # 研究模板是挖掘功能的内部依赖, 不允许从策略接口删除。
    if s.meta.get("research_only"):
        raise HTTPException(status_code=403, detail="研究模板不可删除")

    # 删除被引用的子策略会令叠加策略加载失败; 删除前 fail-closed 阻止。
    dependents = engine.find_dependents(strategy_id)
    if dependents:
        raise HTTPException(
            status_code=409,
            detail=f"该策略被叠加策略 {dependents} 引用，请先解除引用后再删除",
        )

    path = s.file_path
    data_dir = _data_dir(request)
    if path is None or s.source not in {"custom", "ai", "composite"}:
        raise HTTPException(status_code=400, detail="策略源文件路径无效, 无法删除")

    try:
        allowed_dir = (data_dir / "strategies" / s.source).resolve()
        resolved_path = path.resolve()
    except (OSError, RuntimeError) as e:
        raise HTTPException(status_code=409, detail=f"无法访问策略文件: {e}") from e
    if not resolved_path.is_relative_to(allowed_dir):
        raise HTTPException(status_code=400, detail="策略源文件不在用户策略目录, 拒绝删除")

    try:
        resolved_path.unlink(missing_ok=True)
    except OSError as e:
        reason = e.strerror or str(e)
        raise HTTPException(
            status_code=409,
            detail=(
                f"无法删除策略文件 {resolved_path.name}: {reason}。"
                "请确认数据目录可写; Docker 部署请检查数据卷不是只读挂载。"
            ),
        ) from e

    # 删除只影响当前策略, 全量 reload 会让其他损坏或重复 ID 的文件阻塞本次删除。
    engine.unregister(strategy_id)
    warnings = _cleanup_deleted_strategy(request, strategy_id)
    return {"ok": True, "warnings": warnings}


@router.post("/reload")
def reload_strategies(request: Request):
    engine = _get_engine(request)
    try:
        engine.reload()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    _invalidate_strategy_runtime(request)
    return {"ok": True, "count": len(engine.list_strategies())}
