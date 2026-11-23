"""运行时页面插件 API — 上传/清单回填/启停/删除/bundle 分发 + 源码导入。

分发端点位于 /api 前缀下, 自动受集中鉴权中间件保护: 浏览器同源动态
import 会携带登录 cookie, 与页面会话同一访问边界。
"""

from __future__ import annotations

import hashlib
import re
import shutil
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from app.services import ext_source_import
from app.services.ext_pages import PagePackMeta
from app.state import delete_ext_page, list_ext_pages, resolve_blob_uri, save_ext_page

router = APIRouter(prefix="/api/ext-pages", tags=["ext-pages"])

_VALID_ID = re.compile(r"^[a-zA-Z0-9_]{1,64}$")


class _StatePageStore:
    """页面插件以 PG 元数据 + user_blob 存储，保持 PagePackStore 的调用面。"""

    @staticmethod
    def _rows() -> list[dict]:
        """读取合法 ID 的页面插件元数据行。"""
        return [row for row in list_ext_pages() if _VALID_ID.match(str(row.get("id", "")))]

    def _validate_id(self, pack_id: str) -> None:
        if not _VALID_ID.match(pack_id or ""):
            raise ValueError("页面插件 ID 仅允许字母/数字/下划线, 长度 1-64")

    def get(self, pack_id: str) -> PagePackMeta | None:
        """读取单个页面插件元数据；不存在返回 None。"""
        self._validate_id(pack_id)
        row = next((item for item in self._rows() if item["id"] == pack_id), None)
        if row is None:
            return None
        config = row.get("config") or {}
        return PagePackMeta(
            id=pack_id,
            enabled=bool(config.get("enabled", False)),
            label=str(config.get("label") or ""),
            path=str(config.get("path") or ""),
            size=int(config.get("size") or 0),
            sha256=str(config.get("sha256") or ""),
            uploaded_at=float(config.get("uploaded_at") or 0),
            error=str(config.get("error") or ""),
            managed_by=str(config.get("managed_by") or ""),
            version=str(config.get("version") or ""),
        )

    def _put_meta(self, meta: PagePackMeta) -> None:
        """元数据更新必须回传 bundle 字节，避免 save_ext_page 清空 blob_hash。"""
        save_ext_page(meta.id, asdict(meta), self.bundle(meta.id))

    def save_upload(self, pack_id: str, bundle_src: Path) -> PagePackMeta:
        """收编上传的 bundle 到 user_blob，同 ID 覆盖更新。"""
        self._validate_id(pack_id)
        bundle = bundle_src.read_bytes()
        config = {
            "enabled": False,
            "label": "",
            "path": "",
            "size": len(bundle),
            "sha256": hashlib.sha256(bundle).hexdigest(),
            "uploaded_at": time.time(),
            "error": "",
            "managed_by": "",
            "version": "",
        }
        save_ext_page(pack_id, config, bundle)
        meta = self.get(pack_id)
        if meta is None:
            raise RuntimeError("页面插件保存后读取失败")
        return meta

    def list(self) -> list[PagePackMeta]:
        """列出全部页面插件元数据。"""
        return [meta for row in self._rows() if (meta := self.get(row["id"])) is not None]

    def bundle(self, pack_id: str) -> bytes | None:
        """从 user_blob 解析 bundle 字节。"""
        self._validate_id(pack_id)
        row = next((item for item in self._rows() if item["id"] == pack_id), None)
        if row is None or not row.get("bundle_hash"):
            return None
        return resolve_blob_uri(f"pgblob://{row['bundle_hash']}")

    def update_manifest(self, pack_id: str, *, label: str, path: str, error: str) -> PagePackMeta:
        """前端试载结果回填；error 非空则停用。"""
        meta = self.get(pack_id)
        if meta is None:
            raise KeyError(pack_id)
        meta.label = label
        meta.path = path
        meta.error = error
        meta.enabled = not error
        self._put_meta(meta)
        return meta

    def set_enabled(self, pack_id: str, enabled: bool) -> PagePackMeta:
        """启停页面插件。"""
        meta = self.get(pack_id)
        if meta is None:
            raise KeyError(pack_id)
        meta.enabled = enabled
        self._put_meta(meta)
        return meta

    def delete(self, pack_id: str) -> bool:
        """删除页面插件元数据；blob 由内容寻址表自然去重保留。"""
        self._validate_id(pack_id)
        if self.get(pack_id) is None:
            return False
        delete_ext_page(pack_id)
        return True


# 单文件 ES 模块产物通常 < 1MB, 5MB 上限留足余量; 分块写防 OOM(同 ext_data)
_MAX_BUNDLE_BYTES = 5 * 1024 * 1024
# 源码文件本身很小; 限制上传体积只是防误传大文件的保险丝
_MAX_SOURCE_BYTES = 1024 * 1024
_UPLOAD_CHUNK_BYTES = 1024 * 1024


async def _write_bundle_capped(file: UploadFile, dest: Path, max_bytes: int) -> None:
    """分块写入, 累计超限立即 413 并停止读取(防内存放大)。"""
    total = 0
    with dest.open("wb") as f:
        while True:
            chunk = await file.read(_UPLOAD_CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(413, f"页面插件过大(上限 {max_bytes // (1024 * 1024)}MB)")
            f.write(chunk)


def _store(request: Request) -> _StatePageStore:
    """返回 PG/blob 页面插件存储；request 仅用于保持既有调用签名。"""
    return _StatePageStore()


class ManifestBody(BaseModel):
    label: str = ""
    path: str = ""
    error: str = ""


class EnabledBody(BaseModel):
    enabled: bool


@router.post("/upload")
async def upload_page_pack(
    request: Request,
    id: Annotated[str, Form()],
    file: Annotated[UploadFile, File()],
) -> PagePackMeta:
    """上传页面插件产物(.js/.mjs), 覆盖式: 同 ID 重新上传即更新。"""
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".js", ".mjs"):
        raise HTTPException(400, "仅支持 .js / .mjs 页面插件产物")
    store = _store(request)
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tmp_path = tmp_dir / "bundle.js"
        await _write_bundle_capped(file, tmp_path, _MAX_BUNDLE_BYTES)
        if tmp_path.stat().st_size == 0:
            raise HTTPException(400, "页面插件为空文件")
        try:
            return store.save_upload(id, tmp_path)
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


@router.get("")
def list_page_packs(request: Request) -> list[PagePackMeta]:
    from app.extensions.packages import ExtensionPackageStore

    return _store(request).list() + ExtensionPackageStore(
        request.app.state.repo.store.data_dir
    ).runtime_pages(
        getattr(request.app.state, "managed_extension_versions", {}),
    )


# ── 源码导入: .tsx 编译为标准页面插件 (仅源码/桌面部署可发起) ──


def _save_compiled(store: _StatePageStore, pack_id: str, bundle: bytes) -> PagePackMeta:
    """编译产物落库 (save_upload 吃 Path, 中转临时文件)。"""
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        tmp_path = tmp_dir / "bundle.js"
        tmp_path.write_bytes(bundle)
        return store.save_upload(pack_id, tmp_path)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _derive_pack_id(raw: str, fallback: str) -> str:
    # 存储层 id 只允许字母数字下划线; 连字符等替换为下划线保留可读性
    pack_id = re.sub(r"[^a-zA-Z0-9_]", "_", raw or fallback)[:64]
    return pack_id or "imported_page"


@router.get("/import-source/namespaces")
def list_source_namespaces() -> list[str]:
    """本项目 frontend/src/custom/ 下可导入的源码扩展清单。"""
    return ext_source_import.list_namespaces()


@router.post("/import-source")
async def import_source_pack(
    request: Request,
    file: Annotated[UploadFile, File()],
    id: Annotated[str, Form()] = "",
) -> PagePackMeta:
    """上传单个 .tsx 源码扩展, esbuild 编译+契约适配后入库为页面插件。"""
    suffix = Path(file.filename or "").suffix.lower()
    if suffix != ".tsx":
        raise HTTPException(400, "源码导入仅支持 .tsx 文件")
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        src = tmp_dir / "source.tsx"
        await _write_bundle_capped(file, src, _MAX_SOURCE_BYTES)
        if src.stat().st_size == 0:
            raise HTTPException(400, "源码文件为空")
        try:
            bundle, _summary = ext_source_import.compile_source(src, file.filename or "")
        except ext_source_import.SourceImportUnavailableError as e:
            raise HTTPException(409, str(e)) from e
        except ext_source_import.SourceCompileError as e:
            raise HTTPException(400, str(e)) from e
        pack_id = _derive_pack_id(id, Path(file.filename or "").stem)
        return _save_compiled(_store(request), pack_id, bundle)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


class ImportFromProjectBody(BaseModel):
    namespace: str
    id: str = ""


@router.post("/import-source/from-project")
def import_source_from_project(request: Request, body: ImportFromProjectBody) -> PagePackMeta:
    """把本项目 frontend/src/custom/<ns>/extension.tsx 编译导入为页面插件。"""
    try:
        src = ext_source_import.namespace_source_path(body.namespace)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    try:
        bundle, _summary = ext_source_import.compile_source(src, body.namespace)
    except ext_source_import.SourceImportUnavailableError as e:
        raise HTTPException(409, str(e)) from e
    except ext_source_import.SourceCompileError as e:
        raise HTTPException(400, str(e)) from e
    pack_id = _derive_pack_id(body.id, body.namespace)
    return _save_compiled(_store(request), pack_id, bundle)


@router.get("/{pack_id}/bundle.js")
def get_page_pack_bundle(pack_id: str, request: Request) -> Response:
    """分发产物。no-store + ETag(sha256): 热更新产物后浏览器立即取新, 未变时可用缓存协商。"""
    store = _store(request)
    if pack_id.startswith("package:"):
        from app.extensions.packages import ExtensionPackageStore

        try:
            bundle = ExtensionPackageStore(request.app.state.repo.store.data_dir).runtime_bundle(
                pack_id,
                getattr(request.app.state, "managed_extension_versions", {}),
            )
        except (ValueError, OSError, KeyError, TypeError):
            bundle = None
        if bundle is None:
            raise HTTPException(404, "配套后端尚未就绪，页面未启用")
        return FileResponse(
            bundle, media_type="text/javascript", headers={"Cache-Control": "no-store"}
        )
    try:
        meta = store.get(pack_id)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    bundle_bytes = store.bundle(pack_id) if meta else None
    if meta is None or bundle_bytes is None:
        raise HTTPException(404, f"页面插件 '{pack_id}' 不存在")
    return Response(
        content=bundle_bytes,
        media_type="text/javascript",
        headers={
            "Cache-Control": "no-store",
            "ETag": f'"{meta.sha256}"',
        },
    )


@router.put("/{pack_id}/manifest")
def put_page_pack_manifest(pack_id: str, body: ManifestBody, request: Request) -> PagePackMeta:
    """前端试载结果回填: error 非空 → 停用并记错误, 否则启用并登记摘要。"""
    store = _store(request)
    try:
        return store.update_manifest(pack_id, label=body.label, path=body.path, error=body.error)
    except KeyError:
        raise HTTPException(404, f"页面插件 '{pack_id}' 不存在") from None
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.put("/{pack_id}/enabled")
def put_page_pack_enabled(pack_id: str, body: EnabledBody, request: Request) -> PagePackMeta:
    store = _store(request)
    try:
        return store.set_enabled(pack_id, body.enabled)
    except KeyError:
        raise HTTPException(404, f"页面插件 '{pack_id}' 不存在") from None
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.delete("/{pack_id}")
def delete_page_pack(pack_id: str, request: Request) -> dict:
    store = _store(request)
    try:
        if not store.delete(pack_id):
            raise HTTPException(404, f"页面插件 '{pack_id}' 不存在")
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"ok": True}
