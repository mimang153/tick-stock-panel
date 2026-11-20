from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.config import Settings


def _settings(**overrides: Any) -> Settings:
    """_env_file 是 pydantic-settings 的私有构造参数, 经 **kwargs 传递以避开 mypy 签名限制。"""
    return Settings(**overrides)


def test_settings_reads_server_and_auth_values_from_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    for name in ("HOST", "PORT", "LOG_LEVEL", "AUTH_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "HOST=127.0.0.1\nPORT=4318\nLOG_LEVEL=DEBUG\nAUTH_PASSWORD=config-secret\n",
        encoding="utf-8",
    )

    configured = _settings(_env_file=env_path)

    assert configured.host == "127.0.0.1"
    assert configured.port == 4318
    assert configured.log_level == "DEBUG"
    assert configured.auth_password == "config-secret"


def test_redis_mining_backend_requires_shared_artifact_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("MINING_ARTIFACT_ROOT", raising=False)
    env_path = tmp_path / ".env"
    env_path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="MINING_ARTIFACT_ROOT"):
        _settings(_env_file=env_path, mining_store_backend="redis")


def test_multi_worker_forces_redis_mining_backend(tmp_path: Path) -> None:
    """D-05：多 worker 下禁用 memory 文件版 mining（flock 无法跨 worker 互斥）。"""
    env_path = tmp_path / ".env"
    env_path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="不允许多 worker"):
        _settings(_env_file=env_path, mining_store_backend="memory", api_workers=2)
    # 单 worker 的 memory 离线兼容仍可用。
    configured = _settings(_env_file=env_path, mining_store_backend="memory", api_workers=1)
    assert configured.mining_store_backend == "memory"
    assert configured.api_workers == 1

    configured = _settings(
        _env_file=env_path,
        mining_store_backend="redis",
        mining_artifact_root=tmp_path / "shared-artifacts",
    )
    assert configured.mining_artifact_root is not None
