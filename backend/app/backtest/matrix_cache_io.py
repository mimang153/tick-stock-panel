from __future__ import annotations

import app.backtest.matrix as _root

from .matrix_core import (
    _MATRIX_DISK_CACHE_LEASES,
    _MATRIX_DISK_CACHE_LOCK,
    _MATRIX_DISK_CACHE_PENDING_DELETE,
    Any,
    Mapping,
    MappingProxyType,
    MarketDataMatrix,
    Path,
    date,
    hashlib,
    io,
    json,
    logger,
    np,
    os,
    pl,
    shutil,
    suppress,
    zipfile,
)


def _matrix_binary_layout(
    shape: tuple[int, int],
    matrix_fields: list[str],
    vector_fields: list[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], int]:
    time_count = shape[0]
    arrays = [
        ("timestamps", np.dtype(np.int64), (time_count,)),
        ("session_ids", np.dtype(np.int32), (time_count,)),
        ("open", np.dtype(np.float32), shape),
        ("high", np.dtype(np.float32), shape),
        ("low", np.dtype(np.float32), shape),
        ("close", np.dtype(np.float32), shape),
        ("volume", np.dtype(np.float32), shape),
        ("tradable", np.dtype(np.uint8), shape),
        ("limit_up_locked", np.dtype(np.uint8), shape),
        ("limit_down_locked", np.dtype(np.uint8), shape),
    ]
    offset = 0

    def add_spec(dtype: np.dtype, value_shape: tuple[int, ...]) -> dict[str, Any]:
        nonlocal offset
        offset += (-offset) % 64
        spec = {
            "offset": offset,
            "dtype": dtype.str,
            "shape": list(value_shape),
        }
        offset += int(np.prod(value_shape, dtype=np.int64)) * dtype.itemsize
        return spec

    array_specs = {name: add_spec(dtype, value_shape) for name, dtype, value_shape in arrays}  # type: ignore[arg-type]
    field_specs = {name: add_spec(np.dtype(np.float32), shape) for name in matrix_fields}
    field_specs.update(
        {name: add_spec(np.dtype(np.float32), (shape[1],)) for name in vector_fields}
    )
    return array_specs, field_specs, offset


def _open_matrix_memmap(
    path: Path,
    spec: Mapping[str, Any],
    mapped: list[np.memmap],
) -> np.memmap:
    values = np.memmap(
        path,
        dtype=np.dtype(str(spec["dtype"])),
        mode="r+",
        offset=int(spec["offset"]),
        shape=tuple(int(value) for value in spec["shape"]),
        order="C",
    )
    mapped.append(values)
    return values


def _mask_unseen_staging_core(
    arrays: Mapping[str, np.ndarray],
    fields: Mapping[str, np.ndarray],
    parquet_fields: list[str],
    seen: np.ndarray,
) -> None:
    rows_per_chunk = max(1, (32 * 1024 * 1024) // max(1, seen.shape[1]))
    targets = [
        arrays["open"],
        arrays["high"],
        arrays["low"],
        arrays["close"],
        *(fields[name] for name in parquet_fields),
    ]
    for start in range(0, seen.shape[0], rows_per_chunk):
        stop = min(seen.shape[0], start + rows_per_chunk)
        missing = ~seen[start:stop]
        for target in targets:
            target[start:stop][missing] = np.nan


def _mask_unseen_staging_fields(
    fields: Mapping[str, np.ndarray],
    seen: np.ndarray,
) -> None:
    rows_per_chunk = max(1, (32 * 1024 * 1024) // max(1, seen.shape[1]))
    for start in range(0, seen.shape[0], rows_per_chunk):
        stop = min(seen.shape[0], start + rows_per_chunk)
        missing = ~seen[start:stop]
        for target in fields.values():
            if target.ndim == 2:
                target[start:stop][missing] = np.nan


def _close_matrix_memmaps(mapped: list[np.memmap]) -> None:
    for values in reversed(mapped):
        with suppress(OSError, ValueError):
            values.flush()
        mmap_obj = getattr(values, "_mmap", None)
        if mmap_obj is not None:
            with suppress(OSError, ValueError):
                mmap_obj.close()


_MATRIX_CACHE_MANIFEST_SUFFIX = ":manifest"


def _matrix_cache_store():
    """矩阵缓存持久层（CK）；未初始化返回 None，保持离线/单测的纯本地行为。"""
    from app.storage.runtime import get_storage

    runtime = get_storage()
    return None if runtime is None else runtime.store


def _publish_matrix_cache(cache_path: Path, max_bytes: int) -> None:
    """把构建好的缓存目录打包上传 CK；超预算时只留本地。"""
    store = _matrix_cache_store()
    if store is None or not cache_path.is_dir():
        return
    manifest_path = cache_path / "manifest.json"
    if not manifest_path.exists():
        return
    files = [f for f in cache_path.rglob("*") if f.is_file()]
    total = sum(f.stat().st_size for f in files)
    if total > max_bytes:
        logger.info(
            "矩阵缓存 %s 超出预算(%d > %d), 仅保留本地",
            cache_path.name,
            total,
            max_bytes,
        )
        return
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in files:
            zf.write(file, file.relative_to(cache_path).as_posix())
    try:
        store.put_matrix_cache(cache_path.name, buf.getvalue())
        store.put_matrix_cache(
            f"{cache_path.name}{_MATRIX_CACHE_MANIFEST_SUFFIX}",
            json.dumps(
                {"manifest": manifest_path.read_text(encoding="utf-8"), "body_size": total},
                ensure_ascii=False,
            ).encode("utf-8"),
        )
        logger.info("矩阵缓存已上传 CK: %s (%d 字节)", cache_path.name, total)
    except Exception as exc:  # noqa: BLE001 - 持久化失败降级为纯本地缓存
        logger.warning("矩阵缓存上传 CK 失败 %s: %s", cache_path.name, exc)


def _restore_matrix_cache(cache_path: Path) -> bool:
    """按需从 CK 下载并解压缓存目录（memmap 需要真实文件才能读）。"""
    store = _matrix_cache_store()
    if store is None:
        return False
    try:
        payload = store.get_matrix_cache(cache_path.name)
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("矩阵缓存读取 CK 失败 %s: %s", cache_path.name, exc)
        return False
    if payload is None:
        return False
    cache_path.mkdir(parents=True, exist_ok=True)
    root = cache_path.resolve()
    try:
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            for member in zf.infolist():
                target = (cache_path / member.filename).resolve()
                if not str(target).startswith(str(root)):
                    raise ValueError(f"缓存包含非法路径 {member.filename}")
            zf.extractall(cache_path)
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("矩阵缓存解压失败 %s: %s", cache_path.name, exc)
        shutil.rmtree(cache_path, ignore_errors=True)
        return False
    return True


def _sync_matrix_cache_index(cache_dir: Path) -> None:
    """把 CK 里的 manifest 落到本地工作区，供既有命中筛选逻辑直接使用。

    只写 manifest.json（几 KB），矩阵本体留到真正命中时再下载。
    """
    store = _matrix_cache_store()
    if store is None:
        return
    try:
        manifests = store.list_matrix_cache(f"v{_root._DIRECT_MATRIX_LOADER_VERSION}-")
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning("矩阵缓存索引读取失败: %s", exc)
        return
    for key, (text, payload_size) in manifests.items():
        manifest_path = cache_dir / key / "manifest.json"
        if manifest_path.exists():
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("矩阵缓存 manifest 损坏, 跳过: %s", key)
            continue
        # 本地只有 manifest 时, 命中筛选要靠这个字段判断体积
        data["_ck_payload_size"] = payload_size
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _partition_fingerprints(
    root: Path,
    start: date,
    end: date,
    *,
    include_predecessor: bool = False,
) -> dict[str, str]:
    selected: list[tuple[date, Path]] = []
    predecessor: tuple[date, Path] | None = None
    for partition in root.glob("date=*"):
        try:
            partition_date = date.fromisoformat(partition.name.removeprefix("date="))
        except ValueError:
            continue
        if partition_date < start:
            if predecessor is None or partition_date > predecessor[0]:
                predecessor = (partition_date, partition)
            continue
        if partition_date <= end:
            selected.append((partition_date, partition))
    if include_predecessor and predecessor is not None:
        selected.append(predecessor)
    result: dict[str, str] = {}
    for partition_date, partition in sorted(selected):
        digest = hashlib.blake2b(digest_size=20)
        files = sorted(partition.rglob("*.parquet"))
        if not files:
            continue
        for path in files:
            stat = path.stat()
            digest.update(str(path.relative_to(root)).encode("utf-8"))
            digest.update(int(stat.st_size).to_bytes(8, "little", signed=False))
            digest.update(int(stat.st_mtime_ns).to_bytes(8, "little", signed=False))
        result[partition_date.isoformat()] = digest.hexdigest()
    return result


def _partition_date_bounds(root: Path) -> tuple[date | None, date | None]:
    earliest: date | None = None
    latest: date | None = None
    for partition in root.glob("date=*"):
        try:
            value = date.fromisoformat(partition.name.removeprefix("date="))
        except ValueError:
            continue
        if earliest is None or value < earliest:
            earliest = value
        if latest is None or value > latest:
            latest = value
    return earliest, latest


def _latest_partition_date(root: Path) -> date | None:
    return _partition_date_bounds(root)[1]


def _instrument_fingerprint(instruments: pl.DataFrame | None) -> bytes:
    if instruments is None or instruments.is_empty() or "symbol" not in instruments.columns:
        return b"no-instruments"
    columns = [
        name
        for name in ("symbol", "name", "total_shares", "float_shares", "limit_up", "limit_down")
        if name in instruments.columns
    ]
    payload = instruments.select(columns).sort("symbol").to_dicts()
    return hashlib.blake2b(
        json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
        digest_size=20,
    ).digest()


class _MatrixDiskCacheLease:
    def __init__(self, path: Path) -> None:
        self.path = str(path)
        with _MATRIX_DISK_CACHE_LOCK:
            _MATRIX_DISK_CACHE_LEASES[self.path] = _MATRIX_DISK_CACHE_LEASES.get(self.path, 0) + 1

    def __del__(self) -> None:
        path = self.path
        should_delete = False
        with _MATRIX_DISK_CACHE_LOCK:
            remaining = _MATRIX_DISK_CACHE_LEASES.get(path, 0) - 1
            if remaining > 0:
                _MATRIX_DISK_CACHE_LEASES[path] = remaining
            else:
                _MATRIX_DISK_CACHE_LEASES.pop(path, None)
                should_delete = path in _MATRIX_DISK_CACHE_PENDING_DELETE
        if should_delete:
            _try_delete_matrix_cache_path(Path(path))


def _try_delete_matrix_cache_path(path: Path) -> bool:
    try:
        shutil.rmtree(path)
    except OSError as exc:
        logger.debug("matrix disk cache prune skipped %s: %s", path, exc)
        with _MATRIX_DISK_CACHE_LOCK:
            _MATRIX_DISK_CACHE_PENDING_DELETE.add(str(path))
        return False
    with _MATRIX_DISK_CACHE_LOCK:
        _MATRIX_DISK_CACHE_PENDING_DELETE.discard(str(path))
    return True


def _load_market_data_matrix_cache(
    path: Path,
    *,
    cache_status: str,
) -> MarketDataMatrix:
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if int(manifest["version"]) != _root._DIRECT_MATRIX_LOADER_VERSION:
        raise ValueError("matrix disk cache version mismatch")
    storage_path = path / str(manifest["storage"])
    if not storage_path.exists():
        # 本地只有索引（CK 命中）：下载本体后重新读 manifest
        if not _restore_matrix_cache(path):
            raise FileNotFoundError(f"matrix cache payload unavailable: {path.name}")
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        storage_path = path / str(manifest["storage"])

    def load_array(spec: Mapping[str, Any]) -> np.ndarray:
        values = np.memmap(
            storage_path,
            dtype=np.dtype(str(spec["dtype"])),
            mode="r",
            offset=int(spec["offset"]),
            shape=tuple(int(value) for value in spec["shape"]),
            order="C",
        )
        values.flags.writeable = False
        return values

    arrays = {name: load_array(spec) for name, spec in manifest["arrays"].items()}
    stored_fields = {name: load_array(spec) for name, spec in manifest["fields"].items()}
    shape = arrays["open"].shape
    if any(
        values.shape != shape
        for name, values in arrays.items()
        if name not in {"timestamps", "session_ids"}
    ):
        raise ValueError("matrix disk cache contains inconsistent array shapes")
    if any(values.shape not in {shape, (shape[1],)} for values in stored_fields.values()):
        raise ValueError("matrix disk cache contains inconsistent field shapes")
    vector_field_names = frozenset(
        name for name, values in stored_fields.items() if values.shape == (shape[1],)
    )
    fields = {
        name: (values if values.shape == shape else np.broadcast_to(values.reshape(1, -1), shape))
        for name, values in stored_fields.items()
    }
    _root._make_read_only(*fields.values())
    os.utime(path, None)
    return MarketDataMatrix(
        timestamps=arrays["timestamps"],
        timestamp_labels=tuple(str(value) for value in manifest["timestamp_labels"]),
        session_ids=arrays["session_ids"],
        symbols=tuple(str(value) for value in manifest["symbols"]),
        names=tuple(str(value) for value in manifest["names"]),
        open=arrays["open"],
        high=arrays["high"],
        low=arrays["low"],
        close=arrays["close"],
        volume=arrays["volume"],
        tradable=arrays["tradable"],
        limit_up_locked=arrays["limit_up_locked"],
        limit_down_locked=arrays["limit_down_locked"],
        fields=MappingProxyType(fields),
        cache_status=cache_status,
        cache_path=str(path),
        cache_lease=_MatrixDiskCacheLease(path),
        vector_fields=vector_field_names,
        cache_timing_ms=MappingProxyType(
            {str(name): float(value) for name, value in manifest.get("build_timing_ms", {}).items()}
        ),
    )


def _slice_and_project_market_data_matrix(
    market: MarketDataMatrix,
    start: date,
    end: date,
    requested_fields: frozenset[str],
) -> MarketDataMatrix:
    labels = market.timestamp_labels
    start_label = start.isoformat()
    end_label = end.isoformat()
    start_id = 0
    while start_id < len(labels) and labels[start_id] < start_label:
        start_id += 1
    stop_id = start_id
    while stop_id < len(labels) and labels[stop_id] <= end_label:
        stop_id += 1
    if start_id >= stop_id:
        raise ValueError("matrix parquet range contains no market data")
    sliced = _root.slice_market_data_matrix(market, start_id, stop_id)
    missing = requested_fields - set(sliced.fields)
    if missing:
        raise ValueError(f"matrix disk cache missing requested fields: {sorted(missing)}")
    projected_values: dict[str, np.ndarray] = {}
    for name in sorted(requested_fields):
        values = sliced.fields[name]
        if name in sliced.vector_fields:
            values = np.where(np.isfinite(sliced.close), values, np.nan).astype(
                np.float32,
                copy=False,
            )
            values.flags.writeable = False
        projected_values[name] = values
    projected = MappingProxyType(projected_values)
    return MarketDataMatrix(
        timestamps=sliced.timestamps,
        timestamp_labels=sliced.timestamp_labels,
        session_ids=sliced.session_ids,
        symbols=sliced.symbols,
        names=sliced.names,
        open=sliced.open,
        high=sliced.high,
        low=sliced.low,
        close=sliced.close,
        volume=sliced.volume,
        tradable=sliced.tradable,
        limit_up_locked=sliced.limit_up_locked,
        limit_down_locked=sliced.limit_down_locked,
        fields=projected,
        cache_status=sliced.cache_status,
        cache_path=sliced.cache_path,
        cache_lease=sliced.cache_lease,
        vector_fields=frozenset(),
        cache_timing_ms=sliced.cache_timing_ms,
    )
