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
