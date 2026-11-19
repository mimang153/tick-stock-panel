"""AI 对话助手扩展（L2 二开，可整目录卸载，不影响核心功能）。

通过对话调用因子/策略目录、数据能力与回测；详见模块 README 与扩展契约文档。
事件流 NDJSON：notice / tool_call / tool_result / action_confirm / delta /
assistant_context / error / done / ping（确认类由前端 POST /actions/{call_id}/decision）。
"""

from __future__ import annotations

from app.extensions import (
    BACKEND_EXTENSION_API_VERSION,
    BackendExtensionRegistrar,
    ExtensionContext,
)

EXTENSION_ID = "assistant.chat"
EXTENSION_API_VERSION = BACKEND_EXTENSION_API_VERSION


def setup(registrar: BackendExtensionRegistrar) -> None:
    from app.custom.assistant.routes import build_router

    registrar.include_router(build_router())


def startup(context: ExtensionContext) -> None:
    # 对话无状态, 无需启动期初始化; 策略引擎与数据目录在请求期从 app.state 获取。
    _ = context
