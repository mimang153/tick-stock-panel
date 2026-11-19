"""AI provider 拆分 part 3（原 app.services.ai_provider）。"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from types import TracebackType
from urllib.parse import urlsplit, urlunsplit

import app.services.ai_provider as _self

from ._common import (
    _ANSI_RE,
    _CODEX_AUTH_FAILURE_RE,
    _CODEX_ENV_ALLOWLIST,
    _MACOS_CODEX_BIN_DIRS,
    CODEX_DEFAULT_COMMAND,
    Message,
    logger,
)


async def _run_codex_cli(
    messages: Sequence[Message],
    *,
    max_tokens: int | None,
    timeout: float,
    model_override: str | None = None,
    output_schema: dict | None = None,
) -> str:
    prompt = _self._codex_prompt(messages, max_tokens=max_tokens)
    _self._sweep_stale_codex_run_dirs()
    run_path = Path(_self.tempfile.mkdtemp(prefix="tickflow-codex-run-"))
    try:
        codex_home_path = run_path / "codex-home"
        workspace_path = run_path / "workspace"
        codex_home_path.mkdir()
        workspace_path.mkdir()
        output_path = codex_home_path / "last-message.txt"
        if model_override is None:
            _self._prepare_codex_home(codex_home_path)
        else:
            _self._prepare_codex_home(codex_home_path, model_override=model_override)

        # 不传 --ephemeral: 老版本 codex(如 0.58)无此参数, 传了直接报
        # unexpected argument; 会话隔离已由一次性临时 CODEX_HOME 保证(跑完即删)。
        args = [
            *_self._codex_base_command(),
            "exec",
            "--sandbox",
            "read-only",
            "--skip-git-repo-check",
            "--color",
            "never",
            "--output-last-message",
            str(output_path),
        ]
        if output_schema is not None:
            schema_path = run_path / "output-schema.json"
            schema_path.write_text(
                _self.json.dumps(output_schema, ensure_ascii=False), encoding="utf-8"
            )
            args.extend(["--output-schema", str(schema_path)])
        model = (_self.current_ai_model() if model_override is None else model_override).strip()
        if model:
            args.extend(["--model", model])
        args.extend(["--cd", str(workspace_path), "-"])

        env = _self._codex_process_env(codex_home_path)

        try:
            returncode, stdout, stderr = await _self.asyncio.to_thread(
                _self._run_codex_process,
                args,
                prompt,
                env,
                timeout,
            )
        except RuntimeError as exc:
            # 调用超时: 无凭证时 codex 会反复重试表现为长时间挂起 — auth.json
            # 缺失时附登录指引, 与 401 失败路径同一提示。
            hint = _self._codex_missing_auth_hint()
            raise RuntimeError(f"{exc}\n{hint}" if hint else str(exc)) from None

        out = _self._clean_process_text(stdout)
        err = _self._clean_process_text(stderr)
        final_message = _self._read_output_file(output_path)
        if returncode != 0:
            detail = err or out or f"exit code {returncode}"
            message = f"Codex CLI 调用失败: {detail[-1200:]}"
            if _CODEX_AUTH_FAILURE_RE.search(detail):
                hint = _self._codex_missing_auth_hint()
                if hint:
                    message = f"{message}\n{hint}"
            raise RuntimeError(message)
        result = final_message or out
        if not result:
            raise RuntimeError("Codex CLI 未返回内容")
        return result
    finally:
        await _self.asyncio.to_thread(_self._remove_tree_best_effort, run_path)


def _run_codex_process(
    args: Sequence[str],
    prompt: str,
    env: dict[str, str],
    timeout: float,
) -> tuple[int, bytes, bytes]:
    started = _self.time.monotonic()
    try:
        proc = _self.subprocess.run(
            list(args),
            input=prompt.encode("utf-8"),
            capture_output=True,
            env=env,
            timeout=timeout,
            check=False,
        )
    except _self.subprocess.TimeoutExpired as exc:
        stderr = _self._clean_process_text(exc.stderr or b"")
        logger.warning(
            "Codex CLI timed out: elapsed_s=%.1f timeout_s=%g prompt_chars=%d "
            "structured=%s retry_indicated=%s auth_failure_indicated=%s",
            _self.time.monotonic() - started,
            timeout,
            len(prompt),
            "--output-schema" in args,
            any(
                hint in stderr.lower()
                for hint in ("reconnecting", "retrying", "stream disconnected")
            ),
            bool(_CODEX_AUTH_FAILURE_RE.search(stderr)),
        )
        raise RuntimeError(
            f"Codex CLI 调用超时({timeout:g} 秒), 本轮生成已终止。"
            "可重试, 或在 AI 设置中降低 Codex 推理强度。"
        ) from exc
    logger.info(
        "Codex CLI completed: elapsed_s=%.1f timeout_s=%g prompt_chars=%d structured=%s returncode=%d",
        _self.time.monotonic() - started,
        timeout,
        len(prompt),
        "--output-schema" in args,
        proc.returncode,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _codex_process_env(codex_home_path: Path) -> dict[str, str]:
    """Pass OS settings and the explicitly configured provider credential to Codex."""
    env: dict[str, str] = {}
    seen: set[str] = set()
    for name in _CODEX_ENV_ALLOWLIST:
        normalized = name.casefold() if _self.os.name == "nt" else name
        if normalized in seen:
            continue
        value = _self.os.environ.get(name)
        if value:
            env[name] = value
            seen.add(normalized)
    provider = _self._active_codex_provider(_self._read_codex_config())
    if provider:
        key = provider[1].get("env_key")
        if isinstance(key, str) and _self.re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            value = _self.os.environ.get(key)
            if value:
                env[key] = value
    env["NO_COLOR"] = "1"
    env["CODEX_HOME"] = str(codex_home_path)
    return env


def _remove_tree_best_effort(path: Path) -> None:
    _self._remove_auth_files(path)
    for attempt in range(4):
        try:
            _self.shutil.rmtree(path, onerror=_self._make_writable_and_retry)
            return
        except FileNotFoundError:
            return
        except OSError:
            if attempt == 3:
                break
            _self.time.sleep(0.2 * (attempt + 1))
    _self._remove_auth_files(path)
    _self.shutil.rmtree(path, ignore_errors=True)
    _self._remove_auth_files(path)


def _remove_auth_files(path: Path) -> None:
    try:
        auth_files = list(path.rglob("auth.json"))
    except OSError:
        return
    for auth_file in auth_files:
        try:
            _self.os.chmod(auth_file, _self.stat.S_IWRITE)
            auth_file.unlink(missing_ok=True)
        except OSError:
            pass


def _make_writable_and_retry(
    func: Callable[[str], object],
    path: str,
    exc_info: tuple[type[BaseException], BaseException, TracebackType],
) -> None:
    try:
        _self.os.chmod(path, _self.stat.S_IWRITE)
        func(path)
    except OSError:
        raise exc_info[1] from None


_stale_sweep_done = False


def _sweep_stale_codex_run_dirs(*, max_age_s: float = 3600.0, force: bool = False) -> None:
    """清理历史进程遗留的 tickflow-codex-run-* 临时目录 (每进程一次)。

    进程退出/请求取消可能中断 finally 清理, 留下空目录壳长期累积 (实测
    每次失败调用都可能漏一个)。新任务开跑前扫一遍 tempdir; 只动超过
    max_age_s 的同前缀目录, 不碰并发进程的活动目录。
    """
    if _self._stale_sweep_done and not force:
        return
    _self._stale_sweep_done = True
    try:
        cutoff = _self.time.time() - max_age_s
        for path in Path(_self.tempfile.gettempdir()).glob("tickflow-codex-run-*"):
            try:
                if path.is_dir() and path.stat().st_mtime < cutoff:
                    _self._remove_tree_best_effort(path)
            except OSError:
                continue
    except OSError:
        pass


def _codex_prompt(messages: Sequence[Message], *, max_tokens: int | None) -> str:
    parts = [
        "You are Tick Stock Panel's local AI provider.",
        "This is a text-generation task. The working directory is intentionally empty.",
        "Use only the user-provided prompt content below; do not inspect or modify local files.",
        "Return only the final requested content; do not include execution logs.",
    ]
    if max_tokens:
        parts.append(f"Keep the final answer within about {max_tokens} output tokens.")
    for message in messages:
        role = message.get("role", "user")
        content = message.get("content", "")
        parts.append(f"\n<{role}>\n{content}\n</{role}>")
    return "\n".join(parts)


def _codex_base_command() -> list[str]:
    command = _self.current_codex_command()
    resolved = _self._resolve_command(command)
    if not resolved:
        raise RuntimeError(f"未找到 Codex CLI 命令: {command}")

    if _self.sys.platform == "win32" and resolved.lower().endswith(".ps1"):
        return ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", resolved]
    return [resolved]


def _resolve_command(command: str) -> str | None:
    if command.lower() != CODEX_DEFAULT_COMMAND:
        return None

    if _self.sys.platform == "win32":
        desktop_codex = _self._resolve_windows_desktop_codex()
        if desktop_codex:
            return desktop_codex

    resolved = _self.shutil.which(command)
    if _self.sys.platform == "win32" and resolved:
        resolved_path = Path(resolved)
        if not resolved_path.suffix:
            cmd_path = resolved_path.with_suffix(".cmd")
            if cmd_path.exists():
                return str(cmd_path)
    if not resolved and _self.sys.platform == "win32" and not command.lower().endswith(".cmd"):
        resolved = _self.shutil.which(f"{command}.cmd")
    if not resolved and _self.sys.platform == "win32":
        resolved = _self._resolve_windows_codex_command(command)
    if not resolved and _self.sys.platform == "darwin":
        resolved = _self._resolve_macos_codex_command()
    return resolved


def _resolve_windows_codex_command(command: str) -> str | None:
    """Find npm-installed Codex when the backend process has a minimal PATH."""
    raw = Path(command)
    if raw.parent != Path("."):
        return None

    names = [command]
    if not raw.suffix:
        names = [f"{command}.cmd", f"{command}.exe", f"{command}.bat", f"{command}.ps1", command]

    dirs: list[Path] = []
    appdata = _self.os.environ.get("APPDATA")
    if appdata:
        dirs.append(Path(appdata) / "npm")
    dirs.append(_self.Path.home() / "AppData" / "Roaming" / "npm")

    for env_name in ("ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"):
        value = _self.os.environ.get(env_name)
        if value:
            dirs.append(Path(value) / "nodejs")

    for directory in dirs:
        for name in names:
            candidate = directory / name
            if candidate.exists():
                return str(candidate)
    return None


# macOS GUI 启动的应用只继承最小 PATH (/usr/bin:/bin:...), npm/brew/官方
# 安装脚本装的 codex 与 ChatGPT.app 内置 CLI 都不在其中 — 与 Windows 的
# _resolve_windows_desktop_codex 对称, 显式探测常见落位。测试可打桩覆盖。


def _resolve_macos_codex_command() -> str | None:
    """Find Codex CLI in common macOS locations when the process PATH misses it."""
    candidates: list[Path] = [_self.Path.home() / ".local" / "bin" / "codex"]
    candidates.extend(bin_dir / "codex" for bin_dir in _MACOS_CODEX_BIN_DIRS)
    # Codex Desktop (ChatGPT.app) 内置 CLI — 与 ~/.codex 登录态配套
    for apps in (_self._MACOS_SYSTEM_APPLICATIONS, _self.Path.home() / "Applications"):
        cli_root = apps / "ChatGPT.app" / "Contents" / "Resources" / "codex-cli"
        candidates.append(cli_root / "CodexCLI.app" / "Contents" / "MacOS" / "codex")
        try:
            if cli_root.is_dir():
                candidates.extend(sorted(cli_root.glob("*.app/Contents/MacOS/codex")))
        except OSError:
            continue
    for candidate in candidates:
        try:
            # 与 _self.shutil.which 同口径校验可执行位: 只查 is_file 会解析到同名
            # 不可执行文件, 运行时报错不直观
            if candidate.is_file() and _self.os.access(candidate, _self.os.X_OK):
                return str(candidate)
        except OSError:
            continue
    return None


def _resolve_windows_desktop_codex() -> str | None:
    """Prefer the Codex Desktop bundled CLI over an older npm shim."""
    local_appdata = _self.os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        return None

    root = Path(local_appdata) / "OpenAI" / "Codex" / "bin"
    if not root.exists():
        return None

    candidates = list(root.glob("*/codex.exe"))
    direct = root / "codex.exe"
    if direct.exists():
        candidates.append(direct)
    if not candidates:
        return None

    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    return str(newest)


def _prepare_codex_home(target: Path, *, model_override: str | None = None) -> None:
    """Create an isolated CODEX_HOME that reuses auth but not fragile config."""
    source = _self._codex_home()
    auth_file = source / "auth.json"
    if auth_file.exists():
        _self.shutil.copy2(auth_file, target / "auth.json")
    _self._write_compatible_codex_config(target / "config.toml", model_override=model_override)


def _codex_home() -> Path:
    return Path(_self.os.environ.get("CODEX_HOME") or _self.Path.home() / ".codex")


def _write_compatible_codex_config(path: Path, *, model_override: str | None = None) -> None:
    config = _self._read_codex_config()
    lines: list[str] = []
    active_provider = _self._active_codex_provider(config)

    if active_provider:
        lines.append(_self._toml_string("model_provider", active_provider[0]))

    openai_base_url = config.get("openai_base_url")
    if isinstance(openai_base_url, str) and openai_base_url:
        lines.append(_self._toml_string("openai_base_url", openai_base_url))

    model = (
        _self.current_ai_model() if model_override is None else model_override
    ) or _self.normalize_codex_model(str(config.get("model") or ""))
    if model:
        lines.append(_self._toml_string("model", model))

    effort = _self.current_codex_reasoning_effort() or _self.normalize_codex_reasoning_effort(
        str(config.get("model_reasoning_effort") or "")
    )
    if effort:
        lines.append(_self._toml_string("model_reasoning_effort", effort))

    lines.append(_self._toml_string("approval_policy", "never"))
    lines.append(_self._toml_string("sandbox_mode", "read-only"))

    if active_provider:
        provider_name, provider = active_provider
        lines.append("")
        lines.append(f"[model_providers.{_self._toml_key(provider_name)}]")
        for key in ("name", "base_url", "wire_api", "experimental_bearer_token", "env_key"):
            value = provider.get(key)
            if isinstance(value, str) and value:
                lines.append(_self._toml_string(key, value))
        for key in ("requires_openai_auth", "supports_websockets"):
            value = provider.get(key)
            if isinstance(value, bool):
                lines.append(f"{key} = {'true' if value else 'false'}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _active_codex_provider(config: dict) -> tuple[str, dict] | None:
    """Return the active custom provider, adapting loopback URLs for Docker."""
    provider_name = config.get("model_provider")
    if not isinstance(provider_name, str) or not provider_name:
        return None

    providers = config.get("model_providers")
    if not isinstance(providers, dict):
        return None
    source = providers.get(provider_name)
    if not isinstance(source, dict):
        return None

    provider = dict(source)
    base_url = str(provider.get("base_url") or "").strip()
    parsed = urlsplit(base_url)
    docker_host = _self.os.environ.get("CODEX_DOCKER_HOST", "").strip()
    if docker_host and parsed.hostname in {"localhost", "127.0.0.1", "::1"}:
        port = f":{parsed.port}" if parsed.port else ""
        provider["base_url"] = urlunsplit(parsed._replace(netloc=f"{docker_host}{port}"))
    return provider_name, provider


def _read_codex_config() -> dict:
    path = _self._codex_home() / "config.toml"
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            return _self.tomllib.load(f)
    except _self.tomllib.TOMLDecodeError:
        return _self._read_codex_config_lenient(path)
    except OSError:
        return {}


def _read_codex_config_lenient(path: Path) -> dict:
    config: dict[str, str] = {}
    pattern = _self.re.compile(r'^\s*([A-Za-z0-9_-]+)\s*=\s*"([^"]*)"\s*$')
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = pattern.match(line)
            if match:
                config[match.group(1)] = match.group(2)
    except OSError:
        pass
    return config


def _toml_string(key: str, value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'{key} = "{escaped}"'


def _toml_key(value: str) -> str:
    if _self.re.fullmatch(r"[A-Za-z0-9_-]+", value):
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _clean_process_text(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    return _ANSI_RE.sub("", text).strip()


def _read_output_file(path: Path) -> str:
    if path.exists():
        return _ANSI_RE.sub("", path.read_text(encoding="utf-8", errors="replace")).strip()
    return ""
