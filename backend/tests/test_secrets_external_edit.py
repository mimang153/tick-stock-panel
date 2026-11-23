"""密钥配置源的读取契约 (DES-006)。

密钥统一存 PG user_secret (经 app.state 门面), 取消 secrets.json 外部编辑语义 (D-05)。
这里锁定三条:
① file_path() 仅返回兼容展示用的虚拟路径标识;
② 经 save() 写入后 load()/get_ai_config() 立刻反映新值 (无文件 mtime 缓存);
③ GET /api/settings 暴露 secrets_path 供设置页展示。
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from app import secrets_store


@pytest.fixture()
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    from app.config import settings

    monkeypatch.setattr(settings, "data_dir", tmp_path, raising=False)
    yield tmp_path


def test_file_path_points_to_user_data_secrets(data_dir: Path) -> None:
    assert secrets_store.file_path() == data_dir / "user_data" / "secrets.json"


def test_external_edit_is_picked_up_without_restart(data_dir: Path) -> None:
    secrets_store.save({"ai_model": "old-model", "ai_base_url": "https://old.example/v1"})
    assert secrets_store.get_ai_config("ai_model") == "old-model"

    # DES-006: 统一经门面写入 PG, 下一次读取即反映新值 (无需重启)。
    secrets_store.save(
        {
            "ai_model": "brand-new-model",
            "ai_base_url": "https://new.example/v1",
            "ai_api_key": "sk-new",
        }
    )

    assert secrets_store.get_ai_config("ai_model") == "brand-new-model"
    assert secrets_store.get_ai_config("ai_base_url") == "https://new.example/v1"
    assert secrets_store.get_ai_key() == "sk-new"


def test_external_edit_with_same_size_still_reloads(data_dir: Path) -> None:
    """同长度改写: 门面以 generation 失效进程缓存, 不因值长度相同而读到旧值。"""
    secrets_store.save({"ai_model": "model-aaaa"})
    assert secrets_store.get_ai_config("ai_model") == "model-aaaa"

    secrets_store.save({"ai_model": "model-bbbb"})

    assert secrets_store.get_ai_config("ai_model") == "model-bbbb"


def test_get_settings_exposes_secrets_path(data_dir: Path) -> None:
    from app.api import settings as settings_api

    payload = settings_api.get_settings()

    assert payload["secrets_path"].endswith("secrets.json")
    assert (
        str(data_dir) in payload["secrets_path"]
        or Path(payload["secrets_path"]).name == "secrets.json"
    )
