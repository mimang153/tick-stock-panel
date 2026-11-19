"""AI provider 契约测试 (codex CLI 失败路径/探测/清扫分册)。

从 test_ai_provider.py 拆分, 测试语义与断言完全保留。
"""

from __future__ import annotations

import os
import time

import pytest

from app.services import ai_provider

# ── 401/超时失败的登录指引 (auth.json 缺失 = 钥匙串登录态无法复用) ─────


def _codex_fail_run(monkeypatch, tmp_path, *, auth_json: bool, fail: Exception | tuple):
    """_run_codex_cli 的失败路径桩: 跳过外部读取, 只验证提示文案。"""
    home = tmp_path / "codex-home-src"
    home.mkdir()
    if auth_json:
        (home / "auth.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(ai_provider, "_codex_base_command", lambda: ["codex"])
    monkeypatch.setattr(ai_provider, "_prepare_codex_home", lambda p: None)
    monkeypatch.setattr(ai_provider, "_codex_process_env", lambda p: {})
    monkeypatch.setattr(ai_provider, "_read_output_file", lambda p: "")
    monkeypatch.setattr(ai_provider, "_remove_tree_best_effort", lambda p: None)
    monkeypatch.setattr(ai_provider, "_sweep_stale_codex_run_dirs", lambda **kw: None)
    monkeypatch.setattr(ai_provider, "current_ai_model", lambda: "")
    monkeypatch.setattr(ai_provider, "_codex_home", lambda: home)

    if isinstance(fail, tuple):
        monkeypatch.setattr(ai_provider, "_run_codex_process", lambda *a, **kw: fail)
    else:

        def raise_timeout(*a, **kw):
            raise fail

        monkeypatch.setattr(ai_provider, "_run_codex_process", raise_timeout)

    return ai_provider._run_codex_cli(
        [{"role": "user", "content": "hi"}],
        max_tokens=None,
        timeout=1.0,
    )


@pytest.mark.asyncio
async def test_run_codex_cli_401_without_auth_json_appends_hint(monkeypatch, tmp_path):
    """401 且 ~/.codex/auth.json 缺失 → 错误附可执行登录指引。"""
    coro = _codex_fail_run(
        monkeypatch,
        tmp_path,
        auth_json=False,
        fail=(1, b"", b"ERROR: unexpected status 401 Unauthorized: Missing bearer"),
    )
    with pytest.raises(RuntimeError, match="codex login"):
        await coro


@pytest.mark.asyncio
async def test_run_codex_cli_401_with_auth_json_no_hint(monkeypatch, tmp_path):
    """auth.json 存在 → 保持原始错误, 不加误导性登录指引。"""
    coro = _codex_fail_run(
        monkeypatch,
        tmp_path,
        auth_json=True,
        fail=(1, b"", b"ERROR: unexpected status 401 Unauthorized: expired token"),
    )
    with pytest.raises(RuntimeError) as exc_info:
        await coro
    assert "codex login" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_run_codex_cli_timeout_without_auth_json_appends_hint(monkeypatch, tmp_path):
    """无凭证时 codex 反复重试表现为挂起超时 — 同样附登录指引。"""
    coro = _codex_fail_run(
        monkeypatch,
        tmp_path,
        auth_json=False,
        fail=RuntimeError("Codex CLI 调用超时, 请稍后重试或检查本机 Codex 登录状态"),
    )
    with pytest.raises(RuntimeError, match="codex login"):
        await coro


# ── codex 真实探测 (旧实现只验 --version, 登录态缺失测不出来) ──────────


@pytest.mark.asyncio
async def test_probe_codex_cli_not_installed(monkeypatch):
    monkeypatch.setattr(ai_provider, "codex_cli_available", lambda: False)
    with pytest.raises(RuntimeError, match="未找到"):
        await ai_provider.probe_ai_config(
            provider="codex_cli",
            api_key="",
            base_url="",
            model="",
        )


@pytest.mark.asyncio
async def test_probe_codex_cli_runs_real_exec(monkeypatch):
    captured: dict = {}

    async def fake_run_cli(messages, *, max_tokens, timeout, model_override):
        captured["messages"] = messages
        captured["timeout"] = timeout
        captured["model"] = model_override
        return "OK"

    monkeypatch.setattr(ai_provider, "codex_cli_available", lambda: True)
    monkeypatch.setattr(ai_provider, "_run_codex_cli", fake_run_cli)

    text = await ai_provider.probe_ai_config(
        provider="codex_cli",
        api_key="",
        base_url="",
        model="draft-model",
    )
    assert text == "OK"
    assert captured["timeout"] >= 60.0  # 真实 exec 需要 CLI 启动时间
    assert captured["model"] == "draft-model"


@pytest.mark.asyncio
async def test_probe_codex_cli_surfaces_auth_guidance(monkeypatch):
    """登录态缺失的探测失败信息透传 (含指引), 不再假成功。"""

    async def fail_run_cli(messages, *, max_tokens, timeout, model_override):
        raise RuntimeError("Codex CLI 调用失败: 401 Unauthorized\n请执行 codex login")

    monkeypatch.setattr(ai_provider, "codex_cli_available", lambda: True)
    monkeypatch.setattr(ai_provider, "_run_codex_cli", fail_run_cli)

    with pytest.raises(RuntimeError, match="codex login"):
        await ai_provider.probe_ai_config(
            provider="codex_cli",
            api_key="",
            base_url="",
            model="",
        )


# ── 陈旧运行目录清扫 (进程退出中断 finally 清理时的兜底) ────────────────


def test_sweep_stale_codex_run_dirs_removes_only_stale(tmp_path, monkeypatch):
    stale = tmp_path / "tickflow-codex-run-stale"
    fresh = tmp_path / "tickflow-codex-run-fresh"
    unrelated = tmp_path / "unrelated-dir"
    for d in (stale, fresh, unrelated):
        d.mkdir()
        (d / "codex-home").mkdir()
    old_ts = time.time() - 7200
    os.utime(stale, (old_ts, old_ts))

    monkeypatch.setattr(ai_provider.tempfile, "gettempdir", lambda: str(tmp_path))
    ai_provider._sweep_stale_codex_run_dirs(force=True)

    assert not stale.exists()
    assert fresh.exists()
    assert unrelated.exists()


def test_sweep_stale_codex_run_dirs_runs_once_per_process(tmp_path, monkeypatch):
    """非 force 调用受进程级开关保护 — 第二次不再扫描。"""
    first = tmp_path / "tickflow-codex-run-first"
    first.mkdir()
    old_ts = time.time() - 7200
    os.utime(first, (old_ts, old_ts))

    monkeypatch.setattr(ai_provider.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(ai_provider, "_stale_sweep_done", True)
    ai_provider._sweep_stale_codex_run_dirs()
    assert first.exists()  # 开关已置位, 未清扫

    ai_provider._sweep_stale_codex_run_dirs(force=True)
    assert not first.exists()
