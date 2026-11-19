"""AI provider 拆分 part 2（原 app.services.ai_provider）。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import cast
from urllib.parse import urlsplit

import app.services.ai_provider as _self

from ._common import (
    _ANSI_RE,
    _DEEPSEEK_V4_MODELS,
    _LENGTH_FINISH_REASONS,
    _REASONING_EFFORT_REJECT_HINTS,
    _TEMP_REJECT_HINTS,
    _THINKING_BODY_REJECT_HINTS,
    CODEX_CLI_PROVIDER,
    OPENAI_PROVIDER,
    RESPONSES_PROTOCOL,
    Message,
)


async def _run_openai_message_once(
    messages: Sequence[Message],
    *,
    temperature: float | None,
    max_tokens: int | None,
    timeout: float,
    tools: Sequence[dict] | None = None,
):
    """一次 OpenAI create() 调用, 返回完整 message (含 tool_calls, 而非仅 content)。

    _run_openai_once 只取 .content; 工具循环需要 .tool_calls 以回填 role:tool,
    故在此返回完整消息对象。tools 非空时以 tools= 透传, 否则走纯文本路径。
    """
    ai_key = _self.secrets_store.get_ai_key()
    if not ai_key:
        raise RuntimeError("AI API Key 未配置, 请在设置页配置")

    client = _self._openai_client(ai_key, timeout)
    model = _self.current_ai_model()
    req_messages = (
        _self.prepare_tool_messages(
            messages,
            model=model,
            base_url=_self.secrets_store.get_ai_config("ai_base_url", _self.settings.ai_base_url),
        )
        if tools
        else list(messages)
    )
    kwargs = _self._openai_kwargs(temperature=temperature, max_tokens=max_tokens, tools=tools)
    while True:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=req_messages,
                **kwargs,
            )
            break
        except Exception as exc:
            retry_kwargs = _self._openai_retry_kwargs(exc, kwargs)
            if retry_kwargs is not None:
                kwargs = retry_kwargs
                continue
            if _self._is_openai_transport_error(exc):
                raise RuntimeError(_self._format_openai_error(exc)) from exc
            raise
    if not resp.choices:
        return None
    return resp.choices[0].message


async def _stream_openai(
    messages: Sequence[Message],
    *,
    temperature: float | None,
    max_tokens: int | None,
    timeout: float,
    prefer_final_answer: bool,
) -> AsyncIterator[str]:
    ai_key = _self.secrets_store.get_ai_key()
    if not ai_key:
        raise RuntimeError("AI API Key 未配置, 请在设置页配置")

    client = _self._openai_client(ai_key, timeout)
    model = _self.current_ai_model()
    base_url = _self.secrets_store.get_ai_config("ai_base_url", _self.settings.ai_base_url)
    req_messages = list(messages)

    kwargs = _self._openai_kwargs(
        temperature=temperature,
        max_tokens=max_tokens,
        model=model,
        base_url=base_url,
        prefer_final_answer=prefer_final_answer,
    )
    while True:
        try:
            stream = await client.chat.completions.create(
                model=model,
                messages=req_messages,
                **kwargs,
                stream=True,
            )
            break
        except Exception as exc:
            # 流尚未开始 yield, 可安全移除被拒绝的可选参数后重建。
            retry_kwargs = _self._openai_retry_kwargs(exc, kwargs)
            if retry_kwargs is not None:
                kwargs = retry_kwargs
                continue
            if _self._is_openai_transport_error(exc):
                raise RuntimeError(_self._format_openai_error(exc)) from exc
            raise

    try:
        async for piece in _self._iter_openai_text(stream):
            yield piece
    except Exception as exc:
        if _self._is_openai_transport_error(exc):
            raise RuntimeError(_self._format_openai_error(exc)) from exc
        raise


async def _iter_openai_text(stream) -> AsyncIterator[str]:
    """Normalize an OpenAI-compatible stream into complete text deltas.

    Reasoning models may spend the entire completion budget on
    ``reasoning_content`` and finish with HTTP 200 but no user-visible text.
    Treat that response, and any length-truncated partial response, as a
    terminal generation error instead of silently reporting success.
    """
    content_seen = False
    reasoning_seen = False
    finish_reason = ""

    async for chunk in stream:
        choices = getattr(chunk, "choices", None) or []
        if not choices:
            continue
        choice = choices[0]
        reason = getattr(choice, "finish_reason", None)
        if reason:
            finish_reason = str(reason)

        delta = getattr(choice, "delta", None)
        if delta is None:
            continue
        if getattr(delta, "reasoning_content", None):
            reasoning_seen = True
        content = getattr(delta, "content", None)
        if content:
            content_seen = True
            yield content

    if finish_reason in _LENGTH_FINISH_REASONS:
        if reasoning_seen and not content_seen:
            raise RuntimeError(
                "AI 推理达到输出长度上限, 未生成正文; 请提高输出 Token 上限或改用非推理模型"
            )
        raise RuntimeError("AI 输出达到长度上限, 内容不完整; 请提高输出 Token 上限后重试")

    if not content_seen:
        if reasoning_seen:
            raise RuntimeError("AI 仅返回推理内容, 未生成正文; 请检查模型配置或改用非推理模型")
        raise RuntimeError("AI 服务未返回正文内容; 请检查模型配置或稍后重试")


def _openai_client(api_key: str, timeout: float):
    from openai import AsyncOpenAI

    user_agent = (
        _self.secrets_store.get_ai_config("ai_user_agent", "") or _self.settings.ai_user_agent
    )
    return AsyncOpenAI(
        api_key=api_key,
        base_url=_self.normalize_openai_base_url(
            _self.secrets_store.get_ai_config("ai_base_url", _self.settings.ai_base_url)
        ),
        timeout=timeout,
        # SDK 层重试仅覆盖首包前的连接错误/超时/429/5xx, 此时尚未产出任何内容,
        # 重试安全; 首 chunk 之后的断流不在此列, 由上层协议报错处理。
        # 根因: DeepSeek 等上游高峰过载时首包失败率高, max_retries=0 导致一次抖动即终止。
        max_retries=2,
        default_headers={"User-Agent": user_agent},
    )


async def _probe_codex_cli(*, model: str = "", timeout: float = 90.0) -> str:
    """Codex CLI 真实连通性探测: 跑一次最小 exec 验证命令与登录态。

    旧实现只验 `codex --version` — 命令存在但未登录 (auth.json 缺失) 时
    「测试连接」通过、实际调用全 401。探测与正式调用共用 _run_codex_cli,
    鉴权/端点/模型问题在测试阶段即暴露, 失败信息含登录指引。
    """
    if not _self.codex_cli_available():
        raise RuntimeError("Codex CLI 不可用: 未找到可运行的 codex 命令, 请先安装 Codex CLI")
    return await _self._run_codex_cli(
        [{"role": "user", "content": "Reply exactly: OK"}],
        max_tokens=16,
        timeout=timeout,
        model_override=model,
    )


async def probe_ai_config(
    *,
    provider: str,
    api_key: str,
    base_url: str,
    model: str,
    protocol: str = "",
    timeout: float = 20.0,
) -> str:
    """草稿配置一次性连通性探测 — 不读写任何配置状态。

    「测试」必须与「保存」解耦: 旧实现先保存草稿再测试, 测试失败时坏配置
    已经落盘并被状态栏展示为已连接。本函数用表单值直接建临时客户端发一条
    最小消息, 成功与否都不触碰 secrets。

    与正式调用共用 base URL 规范化与协议分支 (chat.completions / responses);
    不传输出上限/温度等可选参数, 规避 max_tokens 与 max_completion_tokens 等
    模型差异。返回模型可见文本 (推理模型可能为空串 — 200 即证明鉴权/端点/
    模型可用); 传输层异常原样抛出, 由调用方格式化后回报。
    """
    if provider == CODEX_CLI_PROVIDER:
        return await _self._probe_codex_cli(model=model)
    if not api_key:
        raise RuntimeError("未配置 API Key")
    if not base_url:
        raise ValueError("请先填写 API 地址")
    if not model:
        raise ValueError("请先填写模型名称")

    from openai import AsyncOpenAI

    user_agent = (
        _self.secrets_store.get_ai_config("ai_user_agent", "") or _self.settings.ai_user_agent
    )
    client = AsyncOpenAI(
        api_key=api_key,
        base_url=_self.normalize_openai_base_url(base_url),
        timeout=timeout,
        max_retries=0,
        default_headers={"User-Agent": user_agent},
    )
    prompt = [{"role": "user", "content": "Reply exactly: OK"}]
    if protocol == RESPONSES_PROTOCOL:
        resp = await client.responses.create(model=model, input=prompt)
        return str(getattr(resp, "output_text", "") or "")
    resp = await client.chat.completions.create(model=model, messages=prompt)
    if not getattr(resp, "choices", None):
        return ""
    return str(resp.choices[0].message.content or "")


# 不同模型可能拒绝 temperature 或 reasoning_effort。这里不靠模型名猜测,
# 只在 400 明确指出对应参数时移除该参数并重试; 每个参数最多移除一次。


def _is_temperature_rejected(exc: Exception) -> bool:
    """True if the upstream 400 is specifically about the temperature param."""
    if getattr(exc, "status_code", None) != 400:
        return False
    text = _self._openai_error_detail(exc) or str(exc)
    return _self._openai_error_param(exc) == "temperature" or any(
        h in text.lower() for h in _TEMP_REJECT_HINTS
    )


def _is_reasoning_effort_rejected(exc: Exception) -> bool:
    """True if the upstream 400 specifically rejects reasoning_effort."""
    if getattr(exc, "status_code", None) != 400:
        return False
    text = _self._openai_error_detail(exc) or str(exc)
    return _self._openai_error_param(exc) == "reasoning_effort" or any(
        h in text.lower() for h in _REASONING_EFFORT_REJECT_HINTS
    )


def _is_thinking_body_rejected(exc: Exception) -> bool:
    """True if the upstream 400 specifically rejects the thinking extra_body."""
    if getattr(exc, "status_code", None) != 400:
        return False
    text = _self._openai_error_detail(exc) or str(exc)
    return _self._openai_error_param(exc) == "thinking" or any(
        h in text.lower() for h in _THINKING_BODY_REJECT_HINTS
    )


def _openai_error_param(exc: Exception) -> str:
    body = getattr(exc, "body", None)
    if not isinstance(body, dict):
        return ""
    error = body.get("error")
    if isinstance(error, dict):
        body = error
    return str(body.get("param") or "").strip().lower()


def _openai_retry_kwargs(exc: Exception, kwargs: dict) -> dict | None:
    """Remove one explicitly rejected optional argument for a bounded retry."""
    retry_kwargs = dict(kwargs)
    if "temperature" in retry_kwargs and _self._is_temperature_rejected(exc):
        retry_kwargs.pop("temperature")
        return retry_kwargs
    if "reasoning_effort" in retry_kwargs and _self._is_reasoning_effort_rejected(exc):
        retry_kwargs.pop("reasoning_effort")
        return retry_kwargs
    if "extra_body" in retry_kwargs and _self._is_thinking_body_rejected(exc):
        # DeepSeek thinking 禁用参数被拒 (模型/API 版本差异): 回退默认思考模式
        # 重试; 报告若因此被推理挤占正文, 由 _iter_openai_text 显式报错。
        retry_kwargs.pop("extra_body")
        return retry_kwargs
    return None


def _openai_kwargs(
    *,
    temperature: float | None,
    max_tokens: int | None,
    model: str = "",
    base_url: str = "",
    prefer_final_answer: bool = False,
    tools: Sequence[dict] | None = None,
) -> dict:
    """Build OpenAI create() kwargs and map supported provider capabilities.

    max_tokens=None 时不传 — 由服务端默认上限管理(推理模型的思考 token 也
    计入该参数预算, 限制会挤占正文, 见 stream_ai_text 文档)。
    """
    kwargs: dict = {}
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    if temperature is not None:
        kwargs["temperature"] = temperature
    if _self.current_ai_provider() == OPENAI_PROVIDER:
        reasoning_effort = _self.current_openai_reasoning_effort()
        if reasoning_effort:
            kwargs["reasoning_effort"] = reasoning_effort
    if (
        prefer_final_answer
        and model.strip().lower() in _DEEPSEEK_V4_MODELS
        and urlsplit(base_url.strip()).hostname == "api.deepseek.com"
    ):
        # DeepSeek V4 defaults to thinking mode. For report-style tasks the
        # hidden reasoning shares max_tokens with the final answer and can
        # exhaust the budget before any visible content is emitted.
        kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
    if tools:
        kwargs["tools"] = list(tools)
    return kwargs


def _is_openai_transport_error(exc: Exception) -> bool:
    try:
        import openai
    except ImportError:
        openai = None

    if openai is not None and isinstance(exc, openai.APIError):
        return True

    try:
        import httpx
    except ImportError:
        return False

    return isinstance(exc, httpx.HTTPError)


def _format_openai_error(exc: Exception) -> str:
    status = getattr(exc, "status_code", None)
    response = getattr(exc, "response", None)
    if status is None and response is not None:
        status = getattr(response, "status_code", None)

    class_name = exc.__class__.__name__
    if "Timeout" in class_name:
        return "AI 服务请求超时, 请稍后重试或检查 AI Base URL / 网络"
    if "Connection" in class_name:
        return "AI 服务连接失败, 请检查 AI Base URL / 网络"

    detail = _self._openai_error_detail(exc)
    status_messages = {
        400: "请求参数无效, 请检查模型名称和上下文长度",
        401: "API Key 无效或无权限, 请检查设置页配置",
        403: "AI 服务拒绝访问, 请检查账号权限或网关配置",
        404: "模型或接口地址不存在, 请检查 AI Base URL 和模型名称",
        408: "AI 服务请求超时, 请稍后重试",
        429: "AI 服务限流或额度不足, 请稍后重试或检查额度",
        500: "AI 服务内部错误, 请稍后重试",
        502: "AI 网关返回错误, 请稍后重试或检查 AI Base URL",
        503: "AI 服务暂时不可用, 请稍后重试",
        504: "AI 上游服务超时, 请稍后重试或检查 AI Base URL / 网络",
    }
    # 优先透出上游真实错误 (如 Moonshot 的 "model not found"), 仅在没有
    # 可读 detail 时才回落到按状态码的通用文案, 避免吞掉排障关键信息。
    message = detail or status_messages.get(cast(int, status)) or "请稍后重试或检查 AI 服务配置"
    if status:
        return f"AI 服务请求失败({status}): {message}"
    return f"AI 服务请求失败: {message}"


def _openai_error_detail(exc: Exception) -> str:
    body = getattr(exc, "body", None)
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict):
            text = error.get("message") or error.get("code") or error.get("type")
            return _self._compact_error_text(str(text or ""))
        if isinstance(error, str):
            return _self._compact_error_text(error)

    response = getattr(exc, "response", None)
    content_type = ""
    text = ""
    if response is not None:
        content_type = response.headers.get("content-type", "").lower()
        try:
            text = response.text
        except Exception:  # noqa: BLE001 - 响应体读取失败时回退 body/exc 文本
            text = ""
    if not text and isinstance(body, str):
        text = body
    if not text:
        text = str(exc)
    if _self._looks_like_html(text, content_type):
        return ""
    return _self._compact_error_text(text)


def _looks_like_html(text: str, content_type: str) -> bool:
    sample = text.lstrip()[:200].lower()
    return (
        "html" in content_type or sample.startswith("<!doctype html") or sample.startswith("<html")
    )


def _compact_error_text(text: str) -> str:
    text = _ANSI_RE.sub("", text)
    text = _self.re.sub(r"<[^>]+>", " ", text)
    text = _self.re.sub(r"\s+", " ", text).strip()
    return text[:500]


# 词边界约束: 裸 "401" 会把输出正文里任意含 401 的数字串 (如模型回复文本)
# 误判成鉴权失败, 附出不相关的 codex login 指引; 词边界只排除数字粘连,
# 真实鉴权报错 ("401 Unauthorized"/"status 401") 不受影响。


def _codex_missing_auth_hint() -> str:
    """auth.json 缺失时的登录指引; 存在则返回空串。

    新版 Codex 凭证可存系统钥匙串 (cli_auth_credentials_store=keyring/auto),
    Codex Desktop 登录不落 auth.json; 隔离 CODEX_HOME 只复制 auth.json, 这类
    登录态复制不到, 用户面对裸 401/超时无从下手。
    """
    try:
        if (_self._codex_home() / "auth.json").exists():
            return ""
    except OSError:
        pass
    return (
        "本机 Codex 登录态不可用: ~/.codex/auth.json 不存在 (新版 Codex 凭证可能存在"
        "系统钥匙串, 隔离运行无法复用)。请在终端执行 codex login, 或 "
        "printenv OPENAI_API_KEY | codex login --with-api-key 生成 auth.json 后重试"
    )
