"""扩展数据文件/探测/schema/序列：上传探测、schema 发现与迷你图序列。"""

from __future__ import annotations

import ipaddress
import shutil
import socket
import tempfile
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import polars as pl
from fastapi import File, HTTPException, Query, Request, UploadFile

from app.api._root_ref import bind as _bind
from app.api.ext_data import DetectUrlReq
from app.services.ext_data import (
    ExtConfig,
    detect_symbol_candidates,
    ensure_utf8_csv,
    infer_fields_from_df,
    run_ext_data_io,
)
from app.services.ext_data_schema import PullAuthReq as PullAuthReq

from ._ext_read import (
    _clean_col_names,
    _ext_data_dir,
    _granularity_effective,
    _parquet_glob,
    _partition_date,
    _safe_json_value,
)

_root = _bind("app.api.ext_data")
logger = _root.logger


# ---------------------------------------------------------------------------
# Schema 发现
# ---------------------------------------------------------------------------


def _daily_series(
    config: ExtConfig,
    data_dir: Path,
    field: str,
    date: str | None,
    symbols: str | None,
    days: int,
) -> dict:
    """日级粒度序列: 近 N 个分区, 每标的每日一值 [YYYY-MM-DD, value]。

    优先按字段映射后的业务日期或表自带 date 列分日, 缺少日期列时才使用
    分区日期。同日多行 (误配/补写) 取末个非空值 — 日末定型语义。
    窗口按「实际存在的分区」截取近 N 个, 中途缺日不补点。
    """
    base = _ext_data_dir(config, data_dir)
    partitions: list[str] = []
    if base.exists():
        partitions = sorted(
            d.name[5:]
            for d in base.iterdir()
            if d.is_dir() and d.name.startswith("date=") and (d / "part.parquet").exists()
        )
    if date:
        target = _partition_date(date)
        partitions = [p for p in partitions if p <= target]
    empty = {  # type: ignore[var-annotated]
        "config_id": config.id,
        "field": field,
        "granularity": "daily",
        "date": partitions[-1] if partitions else None,
        "items": {},
    }
    if not partitions:
        return empty
    window = partitions[-days:]
    df, _ = _root._read_ext_dataframe(config, data_dir, start_date=window[0], end_date=window[-1])
    # 日期字段可被映射为「日期」等名称; 拉取分区日期不一定等于业务日期。
    mapped_date = config.pull.field_map.get("date") if config.pull else None
    day_col = next((c for c in (mapped_date, "date", "data_date") if c and c in df.columns), None)
    if df.is_empty() or field not in df.columns or day_col is None or "symbol" not in df.columns:
        return empty

    if symbols:
        wanted = [s.strip() for s in symbols.split(",") if s.strip()]
        df = df.filter(pl.col("symbol").cast(pl.Utf8).is_in(wanted))

    points = (
        df.select(
            pl.col("symbol").cast(pl.Utf8).alias("symbol"),
            pl.col(day_col).cast(pl.Utf8).str.slice(0, 10).alias("d"),
            pl.col(field).cast(pl.Float64).alias("v"),
        )
        .filter(pl.col("v").is_not_null() & pl.col("d").is_not_null() & (pl.col("d") <= window[-1]))
        .sort(["symbol", "d"], maintain_order=True)
        .group_by(["symbol", "d"], maintain_order=True)
        .agg(pl.col("v").last())
    )
    items: dict[str, list[list[str | float]]] = {}
    for row in points.iter_rows(named=True):
        items.setdefault(row["symbol"], []).append([row["d"], float(row["v"])])
    return {
        "config_id": config.id,
        "field": field,
        "granularity": "daily",
        "date": f"{window[0]}..{window[-1]}",
        "days": len(window),
        "items": items,
    }


def _intraday_series_items(
    df: pl.DataFrame,
    field: str,
    time_field: str,
    symbols: str | None,
) -> dict[str, list[list[str | float]]]:
    """构建日内迷你图点列；时间列兼容 CK 还原后的 ts。"""
    time_col = "ts" if "ts" in df.columns else time_field
    if field not in df.columns or time_col not in df.columns:
        return {}

    filtered = df
    if symbols:
        wanted = [s.strip() for s in symbols.split(",") if s.strip()]
        filtered = df.filter(pl.col("symbol").cast(pl.Utf8).is_in(wanted))

    ts = pl.col(time_col).cast(pl.Utf8)
    tod = pl.when(ts.str.len_chars() >= 11).then(ts.str.slice(11, 8)).otherwise(ts)
    points = (
        filtered.select(
            pl.col("symbol").cast(pl.Utf8).alias("symbol"),
            tod.alias("t"),
            pl.col(field).cast(pl.Float64).alias("v"),
        )
        .filter(pl.col("v").is_not_null() & pl.col("t").is_not_null())
        .sort(["symbol", "t"])
    )
    items: dict[str, list[list[str | float]]] = {}
    for row in points.select("symbol", "t", "v").iter_rows(named=True):
        items.setdefault(row["symbol"], []).append([row["t"], float(row["v"])])
    return items


@_root.router.get("/{config_id}/series")
def ext_series(
    request: Request,
    config_id: str,
    field: str = Query(..., min_length=1, description="数值字段名"),
    date: str | None = Query(
        None, description="YYYY-MM-DD, 缺省取最新分区; 日级粒度时为近N日窗口右端"
    ),
    symbols: str | None = Query(None, description="逗号分隔标的过滤, 缺省全表"),
    days: int = Query(10, ge=1, le=60, description="日级粒度的近 N 日窗口"),
    granularity: Literal["auto", "intraday", "daily"] = "auto",
):
    """扩展列迷你图序列 — 列级显示粒度优先, auto 沿用表的默认粒度。

    - intraday (日内多盘, 如集合竞价): 每标的一天的 [HH:MM:SS, value] 点列;
    - daily (日级别, 如主力净流入每日值): 每标的近 N 日、每日一值
      [YYYY-MM-DD, value] — 按业务日期分日, 缺少日期列时使用分区日期;
      无需 time_field, 同日多行取末个非空值 (日末定型语义)。
    列级覆盖仅影响本次读取, 不修改配置或原始多盘记录。
    value 为 null 的点剔除 (折线/柱状不画空点)。symbols 用于按可见页过滤,
    控制响应体积; 无分区时 items 为空 (fail-closed, 不回退其他窗口)。
    """
    config = _root._store(request).get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")
    if config.mode != "timeseries":
        raise HTTPException(400, "仅时序扩展表支持序列查询")
    data_dir = _root._data_dir(request)
    effective = granularity if granularity != "auto" else _granularity_effective(config, data_dir)
    if effective == "daily":
        return _daily_series(config, data_dir, field, date, symbols, days)
    time_field = config.pull.time_field if config.pull else None
    if not time_field:
        raise HTTPException(400, "仅配置了时间列 (time_field) 的时序扩展表支持序列查询")

    if date:
        _partition_date(date)  # 先做非法日期校验 (400), 与原路径构造行为一致
        df, day = _root._read_ext_dataframe(config, data_dir, snapshot_date=date)
    else:
        df, day = _root._read_ext_dataframe(config, data_dir)
    if df.is_empty():
        return {
            "config_id": config_id,
            "field": field,
            "granularity": "intraday",
            "date": day,
            "items": {},
        }

    items = _intraday_series_items(df, field, time_field, symbols)
    return {
        "config_id": config_id,
        "field": field,
        "granularity": "intraday",
        "date": day,
        "items": items,
    }


@_root.router.post("/detect-fields")
async def detect_fields(
    request: Request,
    file: UploadFile = File(...),  # noqa: B008 - B008: FastAPI Depends 默认值为框架惯例
):
    """上传 CSV/Excel 文件，自动检测列名和类型。

    返回 symbol_candidates（数据匹配 000001.SZ 格式的列）和
    code_candidates（数据匹配 6位纯数字的列）。
    """
    return await run_ext_data_io(_detect_fields, file)


def _detect_fields(file: UploadFile):
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".csv", ".xlsx", ".xls"):
        raise HTTPException(400, "仅支持 CSV / Excel 文件")

    tmp_dir = Path(tempfile.mkdtemp())
    tmp_path = tmp_dir / f"upload{suffix}"
    try:
        _root._copy_upload_capped(file, tmp_path, _root._MAX_UPLOAD_BYTES)

        # 直接读取，不要求 symbol 列
        if suffix == ".csv":
            df = pl.read_csv(ensure_utf8_csv(tmp_path), infer_schema_length=10000)
        elif suffix in (".xlsx", ".xls"):
            df = pl.read_excel(tmp_path)
        else:
            raise HTTPException(400, f"不支持的文件格式: {suffix}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # 清洗列名：去掉括号内的时间戳等信息
    df = _clean_col_names(df)
    symbol_candidates, code_candidates = detect_symbol_candidates(df)

    return {
        "fields": infer_fields_from_df(df),
        "rows": len(df),
        "symbol_candidates": symbol_candidates,
        "code_candidates": code_candidates,
    }


def _find_row_arrays(data, prefix: str = "", limit: int = 8) -> list[str]:
    """自动寻找 JSON 中可能的数据数组路径。"""
    found: list[str] = []

    def walk(value, path: str) -> None:
        if len(found) >= limit:
            return
        if isinstance(value, list):
            if value and isinstance(value[0], dict):
                found.append(path)
            elif value and isinstance(value[0], list):
                for i, item in enumerate(value[:3]):
                    walk(item, f"{path}.{i}" if path else str(i))
        elif isinstance(value, dict):
            for key, child in value.items():
                next_path = f"{path}.{key}" if path else key
                walk(child, next_path)

    walk(data, prefix)
    return found


def _assert_public_http_url(url: str) -> None:
    """拒绝探测内网/环回/元数据地址 (SSRF 防线)。

    仅校验 scheme 与可解析到的 IP; DNS 解析失败时放行给 httpx 报错, 避免
    对测试桩/临时域名产生假拒绝。注意: DNS rebinding (解析后改指向内网)
    不在静态校验可防范围, 该端点已受全局认证保护, 风险限于已登录用户。
    """
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise HTTPException(400, "仅支持 http/https 协议")
    if not parts.hostname:
        raise HTTPException(400, "URL 缺少主机名")
    if parts.username or parts.password:
        raise HTTPException(400, "URL 不允许携带用户名/密码")
    try:
        infos = socket.getaddrinfo(parts.hostname, parts.port or None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return  # 解析失败交给 httpx 报错
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            raise HTTPException(400, f"不允许探测内网/保留地址: {parts.hostname}")


@_root.router.post("/detect-url")
async def detect_url(body: DetectUrlReq):  # noqa: C901 - 校验/请求/解析为单一探测流程, 已含 SSRF 防线
    """请求外部 URL，自动检测 JSON 行数据的字段和标的代码列。"""
    _assert_public_http_url(body.url)
    import httpx

    from app.services.ext_pull import _apply_field_map, _extract_rows

    method = body.method.upper()
    if method not in ("GET", "POST"):
        raise HTTPException(400, "仅支持 GET / POST")

    def _check_redirect(response) -> None:
        # 重定向目标二次校验, 防止公网 URL 302 到内网绕过静态检查
        location = response.headers.get("location")
        if response.is_redirect and location:
            target = str(response.url.join(location))
            _assert_public_http_url(target)

    try:
        async with httpx.AsyncClient(
            timeout=body.timeout_seconds,
            follow_redirects=True,
            event_hooks={"response": [_check_redirect]},
        ) as client:
            headers = body.headers or {}
            kwargs: dict = {"headers": headers}
            if method == "POST" and body.body:
                kwargs["content"] = body.body
                if "content-type" not in {k.lower() for k in headers}:
                    kwargs["headers"]["Content-Type"] = "application/json"
            resp = await client.request(method, body.url, **kwargs)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        raise HTTPException(400, f"URL 请求失败: {e}") from e

    path_candidates = _find_row_arrays(data)
    response_path = body.response_path
    if not response_path:
        if not path_candidates:
            raise HTTPException(400, "未在响应中找到对象数组，请填写响应数据路径")
        response_path = path_candidates[0]

    try:
        rows = _extract_rows(data, response_path)
        rows = _apply_field_map(rows, body.field_map or {})
    except Exception as e:
        raise HTTPException(400, f"响应解析失败: {e}") from e

    if not rows:
        raise HTTPException(400, "提取到的行数为 0")
    if not all(isinstance(row, dict) for row in rows[:200]):
        raise HTTPException(400, "响应数据数组中的元素必须是对象")

    sample_rows = rows[: min(len(rows), 500)]
    try:
        df = pl.DataFrame(sample_rows)
    except Exception as e:
        raise HTTPException(400, f"样例数据解析失败: {e}") from e

    df = _clean_col_names(df)
    symbol_candidates, code_candidates = detect_symbol_candidates(df)
    preview = [{k: _safe_json_value(v) for k, v in row.items()} for row in df.head(10).to_dicts()]

    return {
        "status": "ok",
        "total_rows": len(rows),
        "response_path": response_path,
        "response_path_candidates": path_candidates,
        "fields": infer_fields_from_df(df),
        "symbol_candidates": symbol_candidates,
        "code_candidates": code_candidates,
        "preview": preview,
    }


def _describe_parquet_columns(glob: str) -> list[tuple[str, str]]:
    """读取 Parquet 的 (列名, 类型)，供前端列选择器展示。

    用 polars 逐文件取 schema 并集（兼容时序分区中途加列的 schema drift），
    不再依赖 DuckDB 的 read_parquet/union_by_name。返回的类型名保持 DuckDB
    风格（VARCHAR/DOUBLE/BIGINT…）— 前端按 type 判数值列，依赖这些字符串。
    """
    from glob import iglob

    import polars as pl

    # polars dtype → DuckDB 类型名，保持既有 API 契约
    type_names = {
        pl.String: "VARCHAR",
        pl.Utf8: "VARCHAR",
        pl.Float64: "DOUBLE",
        pl.Float32: "FLOAT",
        pl.Int64: "BIGINT",
        pl.Int32: "INTEGER",
        pl.Int16: "SMALLINT",
        pl.Int8: "TINYINT",
        pl.UInt64: "UBIGINT",
        pl.UInt32: "UINTEGER",
        pl.UInt16: "USMALLINT",
        pl.UInt8: "UTINYINT",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Datetime: "TIMESTAMP",
        pl.Duration: "INTERVAL",
    }

    def _type_name(dtype) -> str:
        base = dtype.base_type() if hasattr(dtype, "base_type") else dtype
        return type_names.get(base, str(dtype).upper())

    columns: dict[str, str] = {}
    paths = sorted(iglob(glob, recursive=True))
    if not paths:
        raise FileNotFoundError(glob)
    for path in paths:
        schema = pl.read_parquet_schema(path)
        for name, dtype in schema.items():
            columns.setdefault(name, _type_name(dtype))
    return list(columns.items())


@_root.router.get("/schema/{config_id}")
def discover_schema(request: Request, config_id: str):
    """发现扩展数据的实际 Parquet schema（基于已有数据）。"""
    config = _root._store(request).get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    data_dir = _root._data_dir(request)
    glob = _parquet_glob(config, data_dir)

    try:
        return {
            "columns": [
                {"name": name, "type": dtype} for name, dtype in _describe_parquet_columns(glob)
            ]
        }
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        # 无数据时返回配置中定义的字段 — 键名与真实 schema 对齐 (type 而非 dtype)
        return {
            "columns": [{"name": f.name, "type": f.dtype, "label": f.label} for f in config.fields]
        }


@_root.router.get("/schema-all")
def discover_all_schemas(request: Request):
    """发现所有扩展表的 schema（用于前端动态列选择）。"""
    configs = _root._store(request).load_all()
    result = []
    for config in configs:
        data_dir = _root._data_dir(request)
        glob = _parquet_glob(config, data_dir)

        try:
            field_labels = {f.name: f.label for f in config.fields}
            columns = [
                {"name": name, "type": dtype, "label": field_labels.get(name, name)}
                for name, dtype in _describe_parquet_columns(glob)
            ]
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            columns = [{"name": f.name, "type": f.dtype, "label": f.label} for f in config.fields]

        result.append(
            {
                "id": config.id,
                "label": config.label,
                "mode": config.mode,
                # 日内多盘表的时间列名 (pull.time_field): 前端据此展示「选盘时刻/迷你图」
                # 列配置; snapshot 表与未配置时间列的表为 null
                "time_field": (config.pull.time_field if config.pull else None),
                # 生效粒度: intraday=日内多盘 (当日逐盘) | daily=日级别 (近N日) |
                # null=snapshot 表。列表列自定义器据此选择迷你图形态与序列口径
                "granularity": _granularity_effective(config, data_dir),
                "columns": columns,
            }
        )
    return {"items": result}
