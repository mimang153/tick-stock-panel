"""AI provider adapter for OpenAI-compatible APIs and local Codex CLI."""

from __future__ import annotations

import logging
import re
from pathlib import Path

# 共享常量
OPENAI_COMPAT_PROVIDER = "openai_compat"
OPENAI_PROVIDER = "openai"
CODEX_CLI_PROVIDER = "codex_cli"
CODEX_DEFAULT_COMMAND = "codex"
CODEX_MIN_TIMEOUT_SECONDS = 600.0
CODEX_SUPPORTED_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}
OPENAI_DEFAULT_REASONING_EFFORT = "high"
CHAT_COMPLETIONS_PROTOCOL = "chat_completions"
RESPONSES_PROTOCOL = "responses"
_SUPPORTED_PROTOCOLS = {CHAT_COMPLETIONS_PROTOCOL, RESPONSES_PROTOCOL}
# Codex 子进程环境白名单: 只透传路径/区域/代理变量, 不泄漏凭据
_CODEX_ENV_ALLOWLIST = (
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "HOME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "APPDATA",
    "LOCALAPPDATA",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "TEMP",
    "TMP",
    "TMPDIR",
    "SHELL",
    "USER",
    "LOGNAME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TZ",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
)
Message = dict[str, str]
logger = logging.getLogger("app.services.ai_provider")
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
# 关注点注入拦截词: 避免 AI 输出具体交易指令 (合规约束)
_FOCUS_BLOCKLIST = re.compile(
    r"买入|卖出|加仓|减仓|轻仓|重仓|半仓|全仓|仓位|止损|止盈|"
    r"操作建议|买卖点|买卖区间|建仓|平仓|清仓|调仓|"
    r"追高|低吸|反包|抄底|逃顶|进攻|防守|"
    r"激进|稳健|保守|目标价|能涨|会跌|预测涨|预测跌|"
    r"荐股|推荐买|推荐卖|值得投资|现在买|可以买|能买|要不要买|买吗|卖吗|"
    r"明日基调|交易计划|下单",
    re.IGNORECASE,
)
_VERSION_SEGMENT_RE = re.compile(r"/v\d+(?:\.\d+)?$", re.IGNORECASE)
# 触发输出截断的 finish_reason: 命中后按不完整回复走续写/重试分支
_LENGTH_FINISH_REASONS = {"length", "max_tokens", "max_output_tokens"}
_TEMP_REJECT_HINTS = ("temperature", "only 1 is allowed")
_REASONING_EFFORT_REJECT_HINTS = ("reasoning_effort", "reasoning effort")
_THINKING_BODY_REJECT_HINTS = ("thinking",)
_DEEPSEEK_V4_MODELS = {"deepseek-v4-flash", "deepseek-v4-pro"}
# Codex 鉴权失败特征: 用于把 CLI 错误映射为可操作的重登录提示
_CODEX_AUTH_FAILURE_RE = re.compile(
    r"\b401\b|\bunauthorized\b|\bnot logged in\b|\bmissing bearer\b|\blogin required\b",
    re.IGNORECASE,
)
# Codex CLI 常见安装落位 (macOS): PATH 缺失时按此顺序探测
_MACOS_SYSTEM_APPLICATIONS = Path("/Applications")
_MACOS_CODEX_BIN_DIRS = (
    Path("/opt/homebrew/bin"),
    Path("/usr/local/bin"),
)
