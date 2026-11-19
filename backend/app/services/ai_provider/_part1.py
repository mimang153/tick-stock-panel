"""AI provider 拆分 part 1（原 app.services.ai_provider）。"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from urllib.parse import urlsplit

import app.services.ai_provider as _self

from ._common import (
    _FOCUS_BLOCKLIST,
    _SUPPORTED_PROTOCOLS,
    _VERSION_SEGMENT_RE,
    CHAT_COMPLETIONS_PROTOCOL,
    CODEX_CLI_PROVIDER,
    CODEX_DEFAULT_COMMAND,
    CODEX_MIN_TIMEOUT_SECONDS,
    CODEX_SUPPORTED_REASONING_EFFORTS,
    OPENAI_COMPAT_PROVIDER,
    OPENAI_DEFAULT_REASONING_EFFORT,
    OPENAI_PROVIDER,
    RESPONSES_PROTOCOL,
    Message,
)


class AIToolProtocolError(RuntimeError):
    """可直接展示的工具协议错误，不包含上游响应或子进程内部输出。"""


# ----------------------------------------------------------------
# 用户 focus 输入规范化。交易建议类表达不会被静默丢弃,而是由统一提示词
# 转换成客观价位、风险和情景分析,避免历史报告显示了 focus、模型却没有收到。
# ----------------------------------------------------------------


def sanitize_focus(focus: str) -> str:
    """规范化 focus 中的首尾空白与连续换行。"""
    if not focus:
        return ""
    text = _self.re.sub(r"\s+", " ", focus).strip()
    return text


def build_focus_instruction(focus: str, *, report_name: str = "分析报告") -> str:
    """构建所有报告共用的关注重点指令。

    有关注点时要求模型在固定报告结构之前先直接回应。若原问题涉及交易
    建议,保留问题语义但要求转换成中立的数据分析,不再无提示地整段丢弃。
    """
    text = _self.sanitize_focus(focus)
    if not text:
        return ""

    lines = [
        "## 用户关注重点(必须优先回应)",
        f"用户关注: {text}",
        f"请在完整{report_name}最前面先输出 `### 0. 🔎 关注重点回应`,"
        "用 2-4 条带具体数据的结论直接回应;随后继续完成既定报告结构,"
        "并在相关章节加深分析。不要只复述问题。",
    ]
    if _FOCUS_BLOCKLIST.search(text):
        lines.append(
            "该关注点含有买卖、仓位、目标价或预测类表达。不得给出相应操作结论;"
            "请将其转换为客观的技术/财务状态、关键价位、风险因素和条件情景后回应。"
        )
    return "\n".join(lines)


def current_ai_provider() -> str:
    return (
        _self.secrets_store.get_ai_config("ai_provider", _self.settings.ai_provider)
        or OPENAI_COMPAT_PROVIDER
    )


def current_ai_protocol() -> str:
    """当前 OpenAI 系请求协议: chat_completions (默认) | responses。

    仅对 openai/openai_compat 两种 provider 有意义; Codex CLI 走本地子进程,
    恒为 chat_completions。非法存储值一律回退默认, 不抛错。
    """
    if _self.is_codex_cli_provider():
        return CHAT_COMPLETIONS_PROTOCOL
    value = (
        str(_self.secrets_store.get_ai_config("ai_protocol", _self.settings.ai_protocol) or "")
        .strip()
        .lower()
    )
    return value if value in _SUPPORTED_PROTOCOLS else CHAT_COMPLETIONS_PROTOCOL


def is_responses_protocol() -> bool:
    return _self.current_ai_protocol() == RESPONSES_PROTOCOL


def current_openai_model() -> str:
    return _self.secrets_store.get_ai_config("ai_model", _self.settings.ai_model)


def current_codex_model() -> str:
    stored = _self.secrets_store.load()
    model = stored.get("ai_codex_model")
    # 旧版本的两种 provider 共用 ai_model。仅在旧配置仍启用 Codex 时回退读取,
    # 避免把正常的 OpenAI-compatible 模型误当作 Codex 模型。
    if model is None and _self.current_ai_provider() == CODEX_CLI_PROVIDER:
        model = stored.get("ai_model")
    return _self.normalize_codex_model(str(model or ""))


def current_ai_model() -> str:
    if _self.current_ai_provider() == CODEX_CLI_PROVIDER:
        return _self.current_codex_model()
    return _self.current_openai_model()


def current_reasoning_context_key() -> str:
    """Bind saved reasoning to its provider, protocol, endpoint and model."""
    identity = [
        _self.current_ai_provider(),
        _self.current_ai_protocol(),
        _self.current_ai_model(),
        _self.normalize_openai_base_url(
            _self.secrets_store.get_ai_config("ai_base_url", _self.settings.ai_base_url)
        ),
    ]
    return _self.hashlib.sha256(_self.json.dumps(identity).encode()).hexdigest()


def prepare_tool_messages(messages: Sequence[dict], *, model: str, base_url: str) -> list[dict]:
    """Keep vendor metadata private and tolerate old DeepSeek text-only history."""
    prepared = []
    context_key = None
    deepseek = urlsplit(base_url).hostname == "api.deepseek.com" or model.lower().rsplit("/", 1)[
        -1
    ].startswith("deepseek")
    for source in messages:
        message = dict(source)
        origin = message.pop("reasoning_context", None)
        if origin is not None:
            context_key = context_key or _self.current_reasoning_context_key()
            if origin != context_key:
                message.pop("reasoning_content", None)
        if (
            deepseek
            and message.get("role") == "assistant"
            and message.get("reasoning_content") is None
        ):
            # Older sessions never saved reasoning; the required field can only
            # be empty for those messages. Never replace a recorded empty string.
            message["reasoning_content"] = ""
        prepared.append(message)
    return prepared


def current_openai_reasoning_effort() -> str:
    stored = _self.secrets_store.load()
    if "ai_reasoning_effort" not in stored:
        return OPENAI_DEFAULT_REASONING_EFFORT
    return str(stored.get("ai_reasoning_effort") or "").strip()


def current_ai_max_output_tokens() -> int:
    """当前 AI 输出上限 (max_tokens): secrets.json 优先, 否则 config 默认。"""
    return _self.secrets_store.get_ai_config_int(
        "ai_max_output_tokens", _self.settings.ai_max_output_tokens
    )


def current_ai_context_window() -> int:
    """当前 AI 输入上下文窗口上限 (约 token): secrets.json 优先, 否则 config 默认。"""
    return _self.secrets_store.get_ai_config_int(
        "ai_context_window", _self.settings.ai_context_window
    )


def current_ai_round_checkpoint() -> int:
    """AI 助手工具轮次检查点 (0=不检查): secrets.json 优先, 否则 config 默认。"""
    return _self.secrets_store.get_ai_config_int(
        "ai_round_checkpoint", _self.settings.ai_round_checkpoint
    )


def _resolve_max_tokens(max_tokens: int | None) -> int | None:
    """显式传入的 max_tokens 钳制到配置输出上限; None 保持 None。

    None = 不传该参数(推理型模型的思考 token 计入 max_tokens 预算,
    显式限制会挤占正文, 语义见 generate_ai_text), 不能映射成配置上限。
    """
    if max_tokens is None:
        return None
    cap = _self.current_ai_max_output_tokens()
    return max(1, min(int(max_tokens), cap))


def _estimate_input_tokens(messages: Sequence[Message]) -> int:
    """粗略估算输入 token 数: 中文按 1 字 1 token, 其余按 4 字符 1 token。"""
    total = 0
    for m in messages:
        text = str(m.get("content") or "") + str(m.get("reasoning_content") or "")
        cjk = sum(1 for ch in text if "一" <= ch <= "鿿")
        total += cjk + (len(text) - cjk) // 4 + 1
    return max(1, total)


def _check_input_budget(messages: Sequence[Message], *, max_tokens: int | None) -> None:
    """输入估算超出上下文窗口时给出明确报错, 避免上游 400 或静默截断。

    token 计数不精确, 仅作安全网: 用中/英文混合估算, 只有明显超窗才拒绝;
    用户可在 AI 设置里调大『上下文窗口』。max_tokens=None(输出放开)时用
    配置上限作输出预留估计。
    """
    context_window = _self.current_ai_context_window()
    if context_window <= 0:
        return
    output_reserve = max_tokens if max_tokens is not None else _self.current_ai_max_output_tokens()
    est = _self._estimate_input_tokens(messages)
    if est + output_reserve > context_window:
        raise ValueError(
            f"输入过长: 估算输入约 {est} tokens, 加上输出预算 {max_tokens} tokens, "
            f"超过上下文窗口 {context_window}。请缩短输入, 或在 AI 设置中调大『上下文窗口』。"
        )


def current_codex_command() -> str:
    return _self.normalize_codex_command(
        _self.secrets_store.get_ai_config("ai_codex_command", _self.settings.ai_codex_command),
        strict=False,
    )


def current_codex_reasoning_effort() -> str:
    return _self.normalize_codex_reasoning_effort(
        _self.secrets_store.get_ai_config(
            "ai_codex_reasoning_effort",
            _self.settings.ai_codex_reasoning_effort,
        )
    )


def is_codex_cli_provider(provider: str | None = None) -> bool:
    return (provider or _self.current_ai_provider()) == CODEX_CLI_PROVIDER


def supports_ai_tools(provider: str | None = None) -> bool:
    """接入适配器是否支持工具协议；具体模型/CLI 的响应仍须运行期校验。"""
    return (provider or _self.current_ai_provider()) in {
        OPENAI_PROVIDER,
        OPENAI_COMPAT_PROVIDER,
        CODEX_CLI_PROVIDER,
    }


def normalize_codex_model(model: str) -> str:
    value = model.strip()
    aliases = {
        "gpt5.5": "gpt-5.5",
        "gpt5.6": "gpt-5.6-sol",
        "gpt5.6-sol": "gpt-5.6-sol",
        "gpt5.6-terra": "gpt-5.6-terra",
        "gpt5.6-luna": "gpt-5.6-luna",
    }
    return aliases.get(value.lower(), value)


def normalize_codex_reasoning_effort(effort: str | None) -> str:
    value = (effort or "").strip().lower()
    return value if value in CODEX_SUPPORTED_REASONING_EFFORTS else ""


def normalize_codex_command(command: str | None, *, strict: bool = True) -> str:
    value = (command or "").strip()
    if not value or value.lower() == CODEX_DEFAULT_COMMAND:
        return CODEX_DEFAULT_COMMAND
    if strict:
        raise ValueError("Codex CLI 仅支持使用默认 codex 命令自动解析, 不支持自定义可执行路径")
    return CODEX_DEFAULT_COMMAND


def normalize_openai_base_url(url: str) -> str:
    """Return the OpenAI-compatible base URL expected by the OpenAI SDK.

    识别 URL 中已有的版本段 (/v1、/v2、/v4 等) 时保持原样 —— 部分 OpenAI 兼容
    服务用非 v1 的版本号 (如智谱 GLM 用 /api/paas/v4), 旧实现无条件补 /v1 会拼成
    不存在的 /api/paas/v4/v1/chat/completions 导致 404。仅在无版本段时才补 /v1。
    """
    base = (url or "").strip().rstrip("/")
    if base.endswith("/chat/completions"):
        base = base[: -len("/chat/completions")].rstrip("/")
    if _VERSION_SEGMENT_RE.search(base):
        return base
    return f"{base}/v1"


def codex_cli_available() -> bool:
    """Codex CLI 是否真正可用: 解析命令后实跑一次 --version。

    仅 which 到二进制不够 — npm 壳缺平台原生二进制时同样存在于 PATH,
    但一运行就报错(如 "Codex CLI not available"), 状态页会误报已连接。
    """
    try:
        base = _self._codex_base_command()
    except RuntimeError:
        return False
    try:
        proc = _self.subprocess.run(
            [*base, "--version"],
            # Windows 桌面包无控制台, 不继承可能无效的标准输入句柄。
            stdin=_self.subprocess.DEVNULL,
            capture_output=True,
            timeout=15,
            check=False,
        )
    except (OSError, _self.subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def ai_configured(provider: str | None = None) -> bool:
    provider = provider or _self.current_ai_provider()
    if _self.is_codex_cli_provider(provider):
        return _self.codex_cli_available()
    return bool(_self.secrets_store.get_ai_key())


async def generate_ai_text(
    messages: Sequence[Message],
    *,
    temperature: float | None = 0.3,
    max_tokens: int | None = 3000,
    timeout: float = 180.0,
) -> str:
    """Return a complete AI response from the currently configured provider.

    max_tokens=None 表示不传该参数(输出上限交给服务端默认) — 推理型模型
    (如 deepseek reasoner 系)的思考 token 计入 max_tokens 预算, 显式限制
    会挤占正文甚至全部吃光(正文 0 字 + finish=length), 长分析类调用应放开。
    显式传入的数值会被钳制到配置的输出上限 (AI 设置可调)。
    """
    max_tokens = _self._resolve_max_tokens(max_tokens)
    _self._check_input_budget(messages, max_tokens=max_tokens)
    if _self.is_codex_cli_provider():
        return await _self._run_codex_cli(
            messages, max_tokens=max_tokens, timeout=max(timeout, CODEX_MIN_TIMEOUT_SECONDS)
        )
    if _self.is_responses_protocol():
        from app.services import ai_responses

        return await ai_responses.run_text(
            messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    return await _self._run_openai_once(
        messages,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )


async def _run_tool_message_once(
    req_messages: list[dict],
    tool_schemas: list[dict],
    *,
    temperature: float | None,
    max_tokens: int | None,
    timeout: float,
):
    if _self.is_codex_cli_provider():
        from app.services import ai_codex_tools

        return await ai_codex_tools.run_message_once(
            req_messages,
            tool_schemas,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    if _self.is_responses_protocol():
        from app.services import ai_responses

        return await ai_responses.run_message_once(
            req_messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            tools=tool_schemas,
        )
    return await _self._run_openai_message_once(
        req_messages,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
        tools=tool_schemas,
    )


async def generate_ai_text_with_tools(
    messages: Sequence[dict],
    tools: Sequence[dict],
    *,
    execute_tool,
    temperature: float | None = 0.3,
    max_tokens: int | None = 3000,
    timeout: float = 180.0,
    max_rounds: int = 4,
) -> list[dict]:
    """通用有界工具循环: 调用 → 执行调用方工具 → role:tool 回填 → 循环。

    返回完整 messages (含 tool 往返)，可扫描 role:tool 重建执行证据。
    轮次耗尽时可能停在工具结果，调用方须检查是否存在最终正文。
    execute_tool 为 async (name, args) -> dict, 返回 {"ok": bool, "result"|"error"},
    工具 schema 与执行回调由调用方注入，不依赖 AI 助手扩展或其工具目录。
    OpenAI 两种协议及 Codex 结构化输出共用此循环；Codex 响应先做结构校验，
    各工具的参数、权限和动作确认仍由调用方的执行回调负责。
    """
    if not _self.supports_ai_tools():
        raise AIToolProtocolError("当前 AI 接入不支持工具调用，请选择支持工具调用的 AI 接入方式")

    max_tokens = _self._resolve_max_tokens(max_tokens)
    if not tools:
        raise ValueError("generate_ai_text_with_tools 需要至少一个工具 schema")

    req_messages: list[dict] = [dict(m) for m in messages]
    tool_schemas = list(tools)

    for _ in range(max_rounds):
        _self._check_input_budget(req_messages, max_tokens=max_tokens)
        message = await _self._run_tool_message_once(
            req_messages,
            tool_schemas,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
        if message is None:
            return req_messages

        tool_calls = getattr(message, "tool_calls", None) or []
        if not tool_calls:
            # 模型不再请求工具, 产出最终文本, 结束循环。
            assistant = {"role": "assistant", "content": message.content or ""}
            reasoning = getattr(message, "reasoning_content", None)
            if reasoning is not None:
                assistant["reasoning_content"] = reasoning
            req_messages.append(assistant)
            return req_messages

        assistant = {
            "role": "assistant",
            "tool_calls": [_self._assistant_tool_call(tc) for tc in tool_calls],
        }
        if message.content:
            assistant["content"] = message.content
        # DeepSeek 思考模式要求工具轮的 reasoning_content 原样回传, 否则下一轮 400。
        reasoning = getattr(message, "reasoning_content", None)
        if reasoning is not None:
            assistant["reasoning_content"] = reasoning
        req_messages.append(assistant)

        for tc in tool_calls:
            tool_result = await execute_tool(
                tc.function.name,
                _self._parse_tool_arguments(tc.function.arguments),
            )
            req_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": _self.json.dumps(tool_result, ensure_ascii=False),
                }
            )

    return req_messages


def _assistant_tool_call(tc) -> dict:
    """SDK 返回的 tool_call 转成回传用的 dict; Gemini 3 的 extra_content
    (thought_signature) 需原样带回, 否则下一轮被上游 400 拒绝。"""
    item = {
        "id": tc.id,
        "type": getattr(tc, "type", "function"),
        "function": {
            "name": tc.function.name,
            "arguments": tc.function.arguments,
        },
    }
    extra = getattr(tc, "extra_content", None)
    if extra:
        item["extra_content"] = extra
    return item


def _parse_tool_arguments(raw: str) -> dict:
    """把 tool call 的 arguments JSON 字符串解析为 dict, 解析失败返回空 dict。"""
    if not raw:
        return {}
    try:
        value = _self.json.loads(raw)
    except (_self.json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


async def stream_ai_text(
    messages: Sequence[Message],
    *,
    temperature: float | None = 0.5,
    max_tokens: int | None = 4000,
    timeout: float = 180.0,
    prefer_final_answer: bool = False,
) -> AsyncIterator[str]:
    """Yield text deltas from the configured provider.

    Codex CLI only exposes the final assistant message for this use case, so it
    yields one complete chunk after the command exits. ``prefer_final_answer``
    lets compatible providers prioritize visible content over hidden reasoning.

    max_tokens=None 表示不限制输出(同 generate_ai_text 的说明)。
    """
    max_tokens = _self._resolve_max_tokens(max_tokens)
    _self._check_input_budget(messages, max_tokens=max_tokens)
    if _self.is_codex_cli_provider():
        yield await _self._run_codex_cli(
            messages, max_tokens=max_tokens, timeout=max(timeout, CODEX_MIN_TIMEOUT_SECONDS)
        )
        return

    if _self.is_responses_protocol():
        from app.services import ai_responses

        async for chunk in ai_responses.stream_text(
            messages,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        ):
            yield chunk
        return

    async for chunk in _self._stream_openai(
        messages,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
        prefer_final_answer=prefer_final_answer,
    ):
        yield chunk


async def _run_openai_once(
    messages: Sequence[Message],
    *,
    temperature: float | None,
    max_tokens: int | None,
    timeout: float,
) -> str:
    message = await _self._run_openai_message_once(
        messages,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )
    if message is None:
        return ""
    return (message.content or "").strip()
