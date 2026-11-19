"""settings API 路由分组 _routes_core（由 settings.py 机械拆分，顺序不变）。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from app import secrets_store
from app.tickflow import client as tf_client
from app.tickflow.policy import extras_caps, missing_caps, probe_log, tier_label

from ._common import (
    DEFAULT_PAID_ENDPOINT,
    AiSettingsIn,
    AiTestIn,
    SwitchEndpointIn,
    TickflowKeyIn,
    _ai_settings_status,
    _api_pkg,
    _apply_ai_settings_updates,
    _probe_and_sync_capabilities,
    _sync_financial_scheduler_caps,
)

router = APIRouter(prefix="/api/settings")


@router.get("")
def get_settings() -> dict:
    """返回当前配置概况(Key 脱敏)。"""
    from app.config import settings
    from app.services import preferences
    from app.services.ai_provider import (
        ai_configured,
        current_ai_context_window,
        current_ai_max_output_tokens,
        current_ai_model,
        current_ai_protocol,
        current_ai_round_checkpoint,
        current_codex_command,
        current_codex_model,
        current_codex_reasoning_effort,
        current_openai_model,
        current_openai_reasoning_effort,
    )

    key = secrets_store.get_tickflow_key()
    ai_provider = secrets_store.get_ai_config("ai_provider", settings.ai_provider)
    return {
        "mode": tf_client.current_mode(),
        "tickflow_api_key_masked": secrets_store.mask(key),
        "has_tickflow_key": bool(key),
        "tier_label": tier_label(),
        "current_endpoint": tf_client.current_endpoint(),
        "probe_log": probe_log(),
        "missing_caps": missing_caps(),
        "extras_caps": extras_caps(),
        # 首次使用引导
        "onboarding_completed": preferences.get_onboarding_completed(),
        # 配置文件位置 (AI 与数据源 Key 统一存放, 支持外部编辑后热加载)
        "secrets_path": str(secrets_store.file_path()),
        # AI 配置
        "ai_provider": ai_provider,
        "ai_protocol": current_ai_protocol(),
        "ai_base_url": secrets_store.get_ai_config("ai_base_url", settings.ai_base_url),
        "ai_api_key_masked": secrets_store.mask(secrets_store.get_ai_key()),
        "has_ai_key": bool(secrets_store.get_ai_key()),
        "ai_configured": ai_configured(ai_provider),
        "ai_model": current_ai_model(),
        "ai_openai_model": current_openai_model(),
        "ai_reasoning_effort": current_openai_reasoning_effort(),
        "ai_codex_model": current_codex_model(),
        "ai_codex_command": current_codex_command(),
        "ai_codex_reasoning_effort": current_codex_reasoning_effort(),
        "ai_user_agent": secrets_store.get_ai_config("ai_user_agent", settings.ai_user_agent),
        "ai_max_output_tokens": current_ai_max_output_tokens(),
        "ai_context_window": current_ai_context_window(),
        "ai_round_checkpoint": current_ai_round_checkpoint(),
    }


@router.post("/switch_endpoint")
def switch_endpoint(req: SwitchEndpointIn, request: Request) -> dict:
    """切换 TickFlow 端点并立即生效。

    端点切换仅对付费档(starter+,走 api.tickflow.org)有意义;
    none/free 档运行在 free-api 服务器,无付费端点权限,禁止切换。
    """
    # none/free 档没有付费端点权限,禁止切换
    if tf_client.current_mode() != "api_key":
        return {"ok": False, "error": "当前档位无法切换端点,仅付费套餐(Starter+)支持"}

    url = req.url.strip().rstrip("/")
    if not url.startswith("https://"):
        return {"ok": False, "error": "仅支持 HTTPS 端点"}

    # 持久化到 secrets.json
    secrets_store.save({"tickflow_base_url": url})
    # 重置客户端，下次调用自动用新端点
    tf_client.reset_clients()

    return {
        "ok": True,
        "current_endpoint": tf_client.current_endpoint(),
    }


@router.post("/tickflow-key")
def save_tickflow_key(req: TickflowKeyIn, request: Request) -> dict:
    """保存 TickFlow API Key 并立即重新探测能力。

    先探后存(关键改动,修复乱填 key 也会被持久化的问题):
      1. 临时用新 key 探测(付费端点),判定档位
      2. 判定为 none(连单只日K都拿不到)→ key 无效:不存,清除已存的,
         返回 {ok: false, reason: "invalid"},前端提示「Key 无效」
      3. 判定为 free(免费有效 key)→ 存 key,客户端切到 free-api 服务器
      4. 判定为 starter+ → 存 key,切到付费端点(现有逻辑)

    端点联动:从无 key 升级到付费 key 时,残留的 free-api 端点不可用,
    故自动切到默认付费端点(api.tickflow.org);free 档则清除自定义端点。
    """
    from app.tickflow.policy import (
        base_tier_name,
        is_invalid_key,
    )

    key = req.api_key.strip()
    if not key:
        return {"ok": False, "error": "key empty"}

    # ===== 1) 临时存 key + 重置客户端,让探测走付费端点 =====
    secrets_store.save({"tickflow_api_key": key})
    tf_client.reset_clients()

    # 立即重新探测(此时 client 已按档位判定,但首次探测必然走付费端点验证)
    capset = _probe_and_sync_capabilities(request.app.state)

    # ===== 2) 判定为无效 key(连单只日K都拿不到)→ 不存,清除 =====
    if is_invalid_key() or base_tier_name() == "none":
        # 无效 key:清除刚存的,避免乱填被持久化;退回 none 档
        secrets_store.clear("tickflow_api_key", "tickflow_base_url")
        tf_client.reset_clients()
        capset = _api_pkg.detect_capabilities(force=True)
        request.app.state.capabilities = capset
        _sync_financial_scheduler_caps(request.app.state, capset)
        return {
            "ok": False,
            "reason": "invalid",
            "error": "Key 无效或已过期,请检查后重试",
            "mode": "none",
            "tier_label": tier_label(),
            "current_endpoint": tf_client.current_endpoint(),
            "probe_log": [],
            "capabilities_count": len(capset.all()),
        }

    # ===== 3) free 档(免费有效 key)→ 存 key,切到 free-api 服务器 =====
    if base_tier_name() == "free":
        # 免费档运行时走 free-api 服务器,清除付费端点的自定义配置
        secrets_store.clear("tickflow_base_url")
        tf_client.reset_clients()
        return {
            "ok": True,
            "tickflow_api_key_masked": secrets_store.mask(key),
            "mode": "free",
            "tier_label": tier_label(),
            "current_endpoint": tf_client.current_endpoint(),
            "probe_log": [],
            "capabilities_count": len(capset.all()),
        }

    # ===== 4) starter+ 付费档 → 确保走付费端点(现有逻辑) =====
    # 若之前是 none/free(无自定义付费端点),切到默认付费端点
    base = secrets_store.load().get("tickflow_base_url")
    if not base:
        secrets_store.save({"tickflow_base_url": DEFAULT_PAID_ENDPOINT})
    tf_client.reset_clients()

    return {
        "ok": True,
        "tickflow_api_key_masked": secrets_store.mask(key),
        "mode": "api_key",
        "tier_label": tier_label(),
        "current_endpoint": tf_client.current_endpoint(),
        "probe_log": [],
        "capabilities_count": len(capset.all()),
    }


@router.delete("/tickflow-key")
def clear_tickflow_key(request: Request) -> dict:
    """清除 Key,退回无档(none)。

    同时清除 tickflow_base_url(测速切换的自定义端点),使客户端走 free-api
    服务器取历史日K;档位标签为 None(无档)。
    """
    secrets_store.clear("tickflow_api_key", "tickflow_base_url")
    tf_client.reset_clients()

    capset = _probe_and_sync_capabilities(request.app.state)

    return {
        "ok": True,
        "mode": "none",
        "tier_label": tier_label(),
        "current_endpoint": tf_client.current_endpoint(),
        "capabilities_count": len(capset.all()),
    }


@router.post("/onboarding/complete")
def complete_onboarding() -> dict:
    """标记首次使用向导完成。

    写入 preferences.json,前端守卫据此判断是否需要再次展示向导。
    跨设备/清缓存安全 —— 状态落在后端文件,不依赖浏览器本地存储。
    """
    from app.services import preferences

    done = preferences.set_onboarding_completed(True)
    return {"ok": True, "onboarding_completed": done}


@router.post("/ai/test")
async def test_ai_settings(req: AiTestIn) -> dict:
    """用当前表单值发一次最小请求验证连通性, 不保存/不修改任何配置。

    与 POST /ai (保存) 解耦: 测试失败不影响已保存配置, 状态栏也不会把
    未验证的草稿展示为已生效。错误保留 200 包体 (ok=false), 由前端内联
    展示在测试结果徽章, 不触发全局 toast。
    """
    from app.services.ai_provider import (
        RESPONSES_PROTOCOL,
        _format_openai_error,
        _is_openai_transport_error,
        probe_ai_config,
    )

    provider = req.provider or "openai_compat"
    api_key = (req.api_key or "").strip() or (secrets_store.get_ai_key() or "")
    protocol = req.protocol if req.protocol == RESPONSES_PROTOCOL else ""
    try:
        text = await probe_ai_config(
            provider=provider,
            api_key=api_key,
            base_url=req.base_url.strip(),
            model=req.model.strip(),
            protocol=protocol,
        )
    except Exception as exc:  # noqa: BLE001 — 连通性测试: 任何异常都转为结果回报
        detail = str(exc) or repr(exc)
        if _is_openai_transport_error(exc):
            detail = _format_openai_error(exc)
        return {"ok": False, "error": detail, "error_type": type(exc).__name__}
    return {"ok": True, "model": req.model.strip() or provider, "response": text[:80]}


@router.post("/ai")
def save_ai_settings(req: AiSettingsIn) -> dict:
    """保存 AI 配置（全部持久化到 secrets.json）"""
    from app.config import settings

    updates = _apply_ai_settings_updates(req, settings)

    if updates:
        secrets_store.save(updates)

    return _ai_settings_status()


@router.delete("/ai")
def clear_ai_settings() -> dict:
    """一键清空 AI 配置(provider / base_url / api_key / model)。

    保留 ai_user_agent —— 自定义请求头与凭证解耦,清空凭证不影响绕过 CDN 拦截的设置。
    """
    from app.config import settings

    secrets_store.clear(
        "ai_provider",
        "ai_base_url",
        "ai_api_key",
        "ai_model",
        "ai_reasoning_effort",
        "ai_codex_model",
        "ai_codex_command",
        "ai_codex_reasoning_effort",
    )
    secrets_store.clear(
        "ai_provider",
        "ai_protocol",
        "ai_base_url",
        "ai_api_key",
        "ai_model",
        "ai_codex_command",
        "ai_codex_reasoning_effort",
        "ai_max_output_tokens",
        "ai_context_window",
    )
    # 同步重置运行时内存(provider 回默认值,其余置空)
    settings.ai_provider = "openai_compat"
    settings.ai_protocol = "chat_completions"
    settings.ai_base_url = ""
    settings.ai_api_key = ""
    settings.ai_model = ""
    settings.ai_codex_command = "codex"
    settings.ai_codex_reasoning_effort = ""
    settings.ai_max_output_tokens = 16384
    settings.ai_context_window = 128000

    return {"ok": True}


@router.get("/ai/sponsor-models")
async def list_sponsor_models() -> dict:
    """代理获取赞助商(RunningHub)的模型列表。

    其网关按 Origin 头过滤: 浏览器跨域请求只会拿到国产模型子集,
    服务端请求无 Origin 头可取全量, 故由后端代理转发。

    /v1/models 除 id 外还携带 pricing (discountRate/分层单价) 与
    context_length; 精简为 items 下发供前端展示折扣徽章与价格,
    models 保持纯 id 列表向后兼容。
    """
    import httpx

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get("https://llm.runninghub.ai/v1/models")
            res.raise_for_status()
            data = res.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"获取模型列表失败: {exc}") from exc

    def _amount(pricing: dict, dim: str) -> float | None:
        value = (pricing.get(dim) or {}).get("amount")
        return float(value) if isinstance(value, int | float) else None

    def _rate(band: dict | None) -> float | None:
        if not isinstance(band, dict):
            return None
        value = band.get("discountRate")
        return float(value) if isinstance(value, int | float) else None

    items: list[dict] = []
    for raw in data.get("data", []):
        if not isinstance(raw, dict):
            continue
        model_id = raw.get("id")
        if not isinstance(model_id, str) or not model_id:
            continue
        raw_pricing = raw.get("pricing")
        pricing = raw_pricing if isinstance(raw_pricing, dict) else {}
        rate = pricing.get("discountRate")
        context_length = raw.get("context_length")
        # 忙闲时定价: 上游把两档折扣放在 pricing.timePricing.timeBands。
        # 顶层 discountRate 为忙时价, 忙闲时展示需单独透传。
        time_pricing = pricing.get("timePricing")
        time_pricing = time_pricing if isinstance(time_pricing, dict) else {}
        bands = time_pricing.get("timeBands")
        bands = [b for b in bands if isinstance(b, dict)] if isinstance(bands, list) else []
        busy = next((b for b in bands if b.get("code") == "BUSY"), None)
        offpeak = next((b for b in bands if b.get("code") == "OFF_PEAK"), None)
        current_band = time_pricing.get("currentBand")
        current = (
            next((b for b in bands if b.get("code") == current_band), None)
            if isinstance(current_band, str)
            else None
        )
        items.append(
            {
                "id": model_id,
                "context_length": context_length if isinstance(context_length, int) else None,
                "discount_rate": float(rate) if isinstance(rate, int | float) else 1.0,
                "input_price": _amount(pricing, "input"),
                "output_price": _amount(pricing, "output"),
                # 忙闲时 (无 timePricing 的模型为 None, 前端回退单一折扣)
                "busy_discount_rate": _rate(busy),
                "offpeak_discount_rate": _rate(offpeak),
                "offpeak_label": offpeak.get("label")
                if isinstance(offpeak, dict) and isinstance(offpeak.get("label"), str)
                else None,
                "current_band": current_band if isinstance(current_band, str) else None,
                "current_discount_rate": _rate(current),
            }
        )
    items.sort(key=lambda x: x["id"])
    return {"models": [item["id"] for item in items], "items": items}
