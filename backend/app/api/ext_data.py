"""扩展数据 API — CRUD + 文件上传 + JSON 写入 + 定时拉取 + schema 发现。"""

from __future__ import annotations

import importlib
import logging
import shutil
import tempfile
import time
from datetime import date
from pathlib import Path

import polars as pl
from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.market_time import cn_today
from app.parquet import scan_parquet_compat
from app.services import ext_config_transfer
from app.services.ext_data import (
    ExtConfig,
    ExtConfigStore,
    ExtField,
    ensure_utf8_csv,
    ext_api_key_field,
    fix_symbol_format,
    rows_to_parquet,
    run_ext_data_io,
    write_ext_parquet,
)
from app.services.ext_data import (
    get_ext_api_key as get_ext_api_key,
)
from app.services.ext_data_schema import CreateExtReq, FieldDef
from app.services.ext_data_schema import PullAuthReq as PullAuthReq
from app.services.ext_data_schema import PullConfigReq as PullConfigReq
from app.services.ext_pull import pull_scheduler as pull_scheduler

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/ext-data", tags=["ext-data"])


# ---------------------------------------------------------------------------
# Pydantic 模型
# ---------------------------------------------------------------------------


class UpdateExtReq(BaseModel):
    label: str | None = None
    fields: list[FieldDef] | None = None
    description: str | None = None
    symbol_map: dict | None = None
    code_map: dict | None = None
    market_level: bool | None = None


class IngestReq(BaseModel):
    """JSON 批量写入请求。"""

    date: str | None = None  # YYYY-MM-DD，不传默认今天
    rows: list[dict] = Field(..., min_length=1)


class ApiKeyReq(BaseModel):
    """设置拉取接口 API Key; 空串 = 清除。"""

    key: str = Field(..., max_length=4096)


class DetectUrlReq(BaseModel):
    """URL 探测请求，不依赖已存在的扩展配置。"""

    url: str = Field(..., min_length=1)
    method: str = "GET"
    headers: dict[str, str] | None = None
    body: str | None = None
    response_path: str = ""
    field_map: dict[str, str] | None = None
    # 探测超时; 与 PullConfig.timeout_seconds 同口径 (默认 30 与历史行为一致),
    # 大响应接口 (如全量集合竞价 /day ~77s) 探测时需要更高超时
    timeout_seconds: int = Field(30, ge=5, le=300)


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


def _store(request: Request) -> ExtConfigStore:
    return ExtConfigStore(request.app.state.repo.store.data_dir)


def _data_dir(request: Request) -> Path:
    return request.app.state.repo.store.data_dir


_CONFIG_FILE = File(...)
_CONFIG_ID = Form(None)


async def _read_config_package(file: UploadFile) -> bytes:
    data = await file.read(ext_config_transfer.MAX_UPLOAD + 1)
    if not data or len(data) > ext_config_transfer.MAX_UPLOAD:
        raise HTTPException(400, "请选择不超过 4 MiB 的扩展配置 ZIP")
    return data


@router.get("/{config_id}/config/export")
async def export_config(request: Request, config_id: str):
    try:
        data, filename = await run_ext_data_io(
            ext_config_transfer.export_config, _data_dir(request), config_id
        )
    except FileNotFoundError as exc:
        raise HTTPException(404, "扩展数据配置不存在") from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return Response(
        data,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@router.post("/config/inspect")
async def inspect_config_package(request: Request, file: UploadFile = _CONFIG_FILE):
    data = await _read_config_package(file)
    try:
        return await run_ext_data_io(ext_config_transfer.inspect_config, _data_dir(request), data)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/config/import")
async def import_config_package(
    request: Request,
    file: UploadFile = _CONFIG_FILE,
    config_id: str | None = _CONFIG_ID,
):
    data = await _read_config_package(file)
    try:
        config = await run_ext_data_io(
            ext_config_transfer.import_config,
            _data_dir(request),
            data,
            config_id=config_id,
        )
    except FileExistsError as exc:
        raise HTTPException(
            409, "该标识已存在，请修改导入标识；现有配置、数据和 Key 均保留"
        ) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    await run_ext_data_io(_refresh_views, request)
    # Imported tables are new and disabled; scheduling begins only when the
    # recipient explicitly saves an enabled pull configuration.
    return config


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def _read_ext_dataframe(
    config: ExtConfig,
    data_dir: Path,
    snapshot_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    columns: list[str] | None = None,
    required_columns: list[str] | None = None,
) -> tuple[pl.DataFrame, str | None]:
    """读取扩展表数据。

    - snapshot: 当前快照 (不支持日期范围)。
    - timeseries + snapshot_date: 单日分区 (历史行为)。
    - timeseries + start_date/end_date (可只给一端): 范围内全部分区 concat,
      active_date 返回 "首..末"。date 与范围同时给出时 date 优先。
    """
    cfg_dir = data_dir / "ext_data" / config.id

    if config.mode == "snapshot":
        if start_date or end_date:
            raise HTTPException(400, "快照模式无历史分区, 不支持日期范围查询")
        from app.storage.runtime import get_storage

        runtime = get_storage()
        # market_level 扩展（无 symbol 列）无法以 (ext_id, symbol, ...) 主键入库,
        # 快照仍以本地分区为准; 存储未初始化（离线脚本/单测）同样回退本地。
        if runtime is not None and not config.market_level:
            ck_result = _read_ext_snapshot_ck(
                config,
                runtime.store,
                data_dir,
                columns,
                required_columns,
            )
            if ck_result is not None:
                return ck_result
        path = cfg_dir / "part.parquet"
        if not path.exists():
            return pl.DataFrame(), None
        return (
            _collect_ext_frame(scan_parquet_compat(path), columns, required_columns),
            _latest_sync_date(config, data_dir),
        )

    # timeseries 模式: CK 为唯一读源
    from app.storage.runtime import get_storage

    runtime = get_storage()

    # 防穿越校验先于任何存储查询 (与原本地路径一致: 表为空时非法日期同样 400)
    snapshot_day, lo, hi = _resolve_ext_range(snapshot_date, start_date, end_date)

    if config.market_level:
        # 市场级表 (无 symbol 列) 在 CK 主键 (ext_id, symbol, date, time_value)
        # 下无法无损表达, 写入侧不镜像 → 读侧同样以本地 Parquet 分区为准。
        return _read_market_level_timeseries(
            config,
            data_dir,
            snapshot_day=snapshot_day,
            lo=lo,
            hi=hi,
            columns=columns,
            required_columns=required_columns,
        )

    if runtime is None:
        logger.warning("ext_data: 存储层未初始化, 扩展表 %s 返回空", config.id)
        return pl.DataFrame(), None

    return _read_ext_timeseries_ck(
        config,
        runtime.store,
        snapshot_day,
        lo,
        hi,
        columns,
        required_columns,
    )


def _with_instrument_name(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    if df.is_empty() or "symbol" not in df.columns:
        return df
    if "name" in df.columns and df["name"].null_count() == 0:
        return df
    try:
        from app.services.instrument_sync import load_instruments

        inst = load_instruments()
        if "symbol" in inst.columns and "name" in inst.columns:
            inst = inst.select(["symbol", "name"]).unique(subset=["symbol"], keep="last")
            if "name" in df.columns:
                lookup_name = "__instrument_name"
                while lookup_name in df.columns:
                    lookup_name += "_"
                return (
                    df.join(inst.rename({"name": lookup_name}), on="symbol", how="left")
                    .with_columns(pl.coalesce("name", lookup_name).alias("name"))
                    .drop(lookup_name)
                )
            return df.join(inst, on="symbol", how="left")
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return df
    return df


@router.get("")
def list_configs(request: Request):
    """列出所有扩展数据配置。"""
    configs = _store(request).load_all()
    data_dir = _data_dir(request)
    items = []
    for c in configs:
        d = c.to_dict()
        d["latest_sync_date"] = _latest_sync_date(c, data_dir)
        d["date_range"] = _date_range(c, data_dir)
        d["plate"] = _plate_stats(c, data_dir)
        d["granularity_effective"] = _granularity_effective(c, data_dir)
        items.append(d)
    return {"items": items}


@router.post("")
def create_config(request: Request, body: CreateExtReq):
    """创建扩展数据配置。"""
    store = _store(request)
    if store.get(body.id):
        raise HTTPException(400, f"配置 '{body.id}' 已存在")
    config = ExtConfig(
        id=body.id,
        label=body.label,
        mode=body.mode,
        fields=[ExtField(f.name, f.dtype, f.label) for f in body.fields],
        description=body.description,
        symbol_map=body.symbol_map,
        code_map=body.code_map,
        market_level=body.market_level,
    )
    try:
        store.create(config)
    except FileExistsError as exc:
        raise HTTPException(400, "该标识已存在，请使用其他标识") from exc
    _refresh_views(request)
    return config.to_dict()


@router.put("/{config_id}")
def update_config(request: Request, config_id: str, body: UpdateExtReq):
    """更新扩展数据配置。"""
    store = _store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")
    if body.label is not None:
        config.label = body.label
    if body.fields is not None:
        config.fields = [ExtField(f.name, f.dtype, f.label) for f in body.fields]
    if body.description is not None:
        config.description = body.description
    if body.symbol_map is not None:
        config.symbol_map = body.symbol_map
    if body.code_map is not None:
        config.code_map = body.code_map
    if body.market_level is not None:
        config.market_level = body.market_level
    store.upsert(config)
    _refresh_views(request)
    return config.to_dict()


@router.delete("/{config_id}")
def delete_config(request: Request, config_id: str):
    """删除扩展数据配置。"""
    store = _store(request)
    if not store.delete(config_id):
        raise HTTPException(404, f"配置 '{config_id}' 不存在")
    # 同步清掉 secrets.json 里残留的拉取 API Key, 避免同名重建配置时误用旧 Key
    from app import secrets_store

    secrets_store.clear(ext_api_key_field(config_id))
    _refresh_views(request)
    return {"status": "deleted"}


def _apply_row_filters(df: pl.DataFrame, filters: list[str] | None) -> pl.DataFrame:
    """等值过滤: 每个条目 `field:v1|v2` (值间 OR); 多条目间 AND; 按字符串比较。

    数值列的等值匹配走字符串口径 (1 → "1", 1.0 → "1.0"), 覆盖二开最常见的
    "维度取值筛选"场景; 区间/模糊匹配暂不做, 留给真实需求出现。
    """
    if not filters:
        return df
    for entry in filters:
        field, sep, raw_values = entry.partition(":")
        field = field.strip()
        values = [v for v in (raw_values.split("|") if sep else []) if v != ""]
        if not field or not values:
            raise HTTPException(400, f"过滤条件格式错误 (应为 字段:值1|值2): {entry}")
        if field not in df.columns:
            raise HTTPException(400, f"过滤字段不存在: {field} (可用: {df.columns})")
        df = df.filter(pl.col(field).cast(pl.Utf8).is_in(values))
    return df


def _apply_row_sort(df: pl.DataFrame, sort: str | None) -> pl.DataFrame:
    """排序: `field` 升序 / `field:desc` 降序; 未知字段 400。"""
    if not sort:
        return df
    field, _, direction = sort.partition(":")
    field = field.strip()
    descending = direction.strip().lower() == "desc"
    if field not in df.columns:
        raise HTTPException(400, f"排序字段不存在: {field} (可用: {df.columns})")
    return df.sort(field, descending=descending, nulls_last=True)


# 模块级常量避免 B008 (参数默认值内的函数调用); 语义与行内 Query(...) 完全一致
_ROW_FILTER_QUERY = Query(None, description="过滤 条件`字段:值1|值2`, 可重复多条 (AND)")


@router.get("/{config_id}/rows")
def list_rows(
    request: Request,
    config_id: str,
    snapshot_date: str | None = Query(None, alias="date"),
    columns: str | None = Query(None, description="逗号分隔的字段列表"),
    limit: int = Query(1000, ge=1, le=20000),
    offset: int = Query(0, ge=0, le=1_000_000, description="分页偏移 (配合 limit/total)"),
    filter: list[str] | None = _ROW_FILTER_QUERY,
    sort: str | None = Query(None, description="排序字段, 降序加 :desc"),
    start_date: str | None = Query(None, description="时序起始日期 YYYY-MM-DD (含)"),
    end_date: str | None = Query(None, description="时序结束日期 YYYY-MM-DD (含)"),
):
    """读取扩展数据明细。

    - snapshot: 返回当前快照。
    - timeseries: 默认返回最新日期分区; date=YYYY-MM-DD 指定单日;
      start_date/end_date 取日期范围 (可只给一端), 全部分区合并返回。
    - filter/sort/offset: 服务端过滤/排序/分页, 二开方无需一次拉全量自行筛。
    """
    config = _store(request).get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    data_dir = _data_dir(request)
    requested = [c.strip() for c in (columns or "").split(",") if c.strip()]
    tf = config.pull.time_field if config.pull else None
    required = [entry.partition(":")[0].strip() for entry in (filter or [])]
    if sort:
        required.append(sort.partition(":")[0].strip())
    if tf:
        required.append(tf)
    df, active_date = _read_ext_dataframe(
        config,
        data_dir,
        snapshot_date,
        start_date,
        end_date,
        columns=["symbol", "code", "name", *requested] if requested else None,
        required_columns=required,
    )
    df = _with_instrument_name(df, data_dir)
    # 日内序列表: 按时间列升序, 保证分页/截断取到的是最早的盘
    if tf and tf in df.columns:
        df = df.sort(tf)
    df = _apply_row_filters(df, filter)
    df = _apply_row_sort(df, sort)
    total = len(df)
    df = df.slice(offset, limit)
    if requested:
        keep = [c for c in ["symbol", "code", "name", *requested] if c in df.columns]
        if keep:
            df = df.select(list(dict.fromkeys(keep)))

    rows = []
    for row in df.to_dicts():
        rows.append({k: _safe_json_value(v) for k, v in row.items()})  # noqa: PERF401 - PERF401: 循环内含多行处理/异常, 改写推导式损可读性

    return {
        "id": config.id,
        "label": config.label,
        "mode": config.mode,
        "date": active_date,
        "total": total,
        "offset": offset,
        "limit": limit,
        "fields": [f.to_dict() for f in config.fields],
        "rows": rows,
    }


@router.get("/{config_id}/values")
def field_values(
    request: Request,
    config_id: str,
    field: str = Query(..., min_length=1, description="要枚举去重的字段名"),
    snapshot_date: str | None = Query(None, alias="date"),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    """字段取值枚举 (去重 + 计数, 按出现次数降序)。

    `/rows?filter=` 的配套端点: 二开方先取某字段的候选值做筛选下拉,
    再带 filter 查明细。日期语义与 /rows 相同 (date 单日 / 范围 / 默认最新)。
    """
    config = _store(request).get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    data_dir = _data_dir(request)
    df, active_date = _read_ext_dataframe(config, data_dir, snapshot_date, start_date, end_date)
    if field not in df.columns:
        raise HTTPException(400, f"字段不存在: {field} (可用: {df.columns})")
    counted = df.group_by(field).len().sort(["len", field], descending=[True, False]).head(limit)
    values = [
        {"value": _safe_json_value(row[field]), "count": row["len"]} for row in counted.to_dicts()
    ]
    return {
        "id": config.id,
        "field": field,
        "date": active_date,
        "total": df.height,
        "distinct": df[field].n_unique(),
        "values": values,
    }


@router.get("/{config_id}/dimension-members")
def dimension_members(
    request: Request,
    config_id: str,
    field: str = Query(..., min_length=1),
    value: str = Query(..., min_length=1),
    snapshot_date: str | None = Query(None, alias="date"),
    limit: int = Query(1000, ge=1, le=10000),
):
    """按扩展字段的完整标签值返回成分股，不绑定具体概念/行业数据源。"""
    config = _store(request).get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    data_dir = _data_dir(request)
    df, active_date = _read_ext_dataframe(config, data_dir, snapshot_date)
    df = _with_instrument_name(df, data_dir)
    matched = _filter_dimension_member_rows(df, field, value)
    total = len(matched)

    columns = ["symbol", "code", "name", "股票代码", "股票简称", field]
    for mapping in (config.symbol_map, config.code_map):
        if isinstance(mapping, dict) and mapping.get("type") == "mapped" and mapping.get("col"):
            columns.append(str(mapping["col"]))  # noqa: PERF401 - PERF401: 循环内含多行处理/异常, 改写推导式损可读性
    selected = [column for column in dict.fromkeys(columns) if column in matched.columns]
    if selected:
        matched = matched.select(selected)
    if total > limit:
        matched = matched.head(limit)

    symbol_columns = ["symbol", "code", "股票代码", "代码"]
    name_columns = ["name", "股票简称", "名称"]
    for mapping in (config.symbol_map, config.code_map):
        if isinstance(mapping, dict) and mapping.get("type") == "mapped" and mapping.get("col"):
            symbol_columns.append(str(mapping["col"]))  # noqa: PERF401 - PERF401: 循环内含多行处理/异常, 改写推导式损可读性

    rows = []
    for raw in matched.to_dicts():
        row = {key: _safe_json_value(item) for key, item in raw.items()}
        if not row.get("symbol"):
            row["symbol"] = next(
                (str(row[column]) for column in symbol_columns if row.get(column)), ""
            )
        if not row.get("name"):
            row["name"] = next((str(row[column]) for column in name_columns if row.get(column)), "")
        rows.append(row)
    return {
        "id": config.id,
        "label": config.label,
        "date": active_date,
        "field": field,
        "value": value.strip(),
        "total": total,
        "limit": limit,
        "rows": rows,
    }


@router.get("/{config_id}/dimension-intraday")
def dimension_intraday(
    request: Request,
    config_id: str,
    field: str = Query(..., min_length=1),
    value: str = Query(..., min_length=1),
    snapshot_date: str | None = Query(None, alias="date"),
):
    """板块分时走势 (等权): 成分股 × 当日分钟K聚合; 60s 缓存, 点击触发不预计算。"""
    config = _store(request).get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    cache_key = (config_id, field, value.strip(), snapshot_date)
    now = time.monotonic()
    hit = _DIMENSION_INTRADAY_CACHE.get(cache_key)
    if hit is not None and now - hit[0] < _DIMENSION_INTRADAY_CACHE_TTL_S:
        return hit[1]

    payload = _dimension_intraday_compute(config, _data_dir(request), field, value, snapshot_date)
    _DIMENSION_INTRADAY_CACHE[cache_key] = (now, payload)
    return payload


# ---------------------------------------------------------------------------
# 文件上传
# ---------------------------------------------------------------------------

# 扩展数据 CSV/Excel 上传上限(与自选截图 OCR 的 12MB 上限属同类保护, 见 watchlist.py)。
# 通过分块写入临时文件, 超限即拒绝, 避免 `await file.read()` 把整个文件读入内存。
_MAX_UPLOAD_BYTES = 50 * 1024 * 1024
_UPLOAD_CHUNK_BYTES = 1024 * 1024


async def _write_upload_capped(file: UploadFile, dest: Path, max_bytes: int) -> None:
    await run_ext_data_io(_copy_upload_capped, file, dest, max_bytes)


def _copy_upload_capped(file: UploadFile, dest: Path, max_bytes: int) -> None:
    """分块把上传文件写入 dest, 累计超过 max_bytes 立即拒绝(413)。

    避免一次性 `await file.read()` 把整个文件读入内存(大文件可能触发高内存占用、
    进程 OOM 或服务不可用); 超限时停止继续读取与落盘。
    """
    total = 0
    with dest.open("wb") as f:
        while True:
            chunk = file.file.read(_UPLOAD_CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(413, f"文件过大(上限 {max_bytes // (1024 * 1024)}MB)")
            f.write(chunk)


@router.post("/{config_id}/upload")
async def upload_data(
    request: Request,
    config_id: str,
    file: UploadFile = File(...),  # noqa: B008 - B008: FastAPI Depends 默认值为框架惯例
    snapshot_date: str | None = None,
):
    """上传 CSV/Excel 文件写入扩展数据。"""
    return await run_ext_data_io(_upload_data, request, config_id, file, snapshot_date)


def _upload_data(request: Request, config_id: str, file: UploadFile, snapshot_date: str | None):
    store = _store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    # 校验文件后缀
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".csv", ".xlsx", ".xls"):
        raise HTTPException(400, "仅支持 CSV / Excel 文件")

    # 写到临时文件再解析
    tmp_dir = Path(tempfile.mkdtemp())
    tmp_path = tmp_dir / f"upload{suffix}"
    try:
        _copy_upload_capped(file, tmp_path, _MAX_UPLOAD_BYTES)

        # 直接读取文件，不做列重命名
        if suffix == ".csv":
            df = pl.read_csv(ensure_utf8_csv(tmp_path), infer_schema_length=10000)
        elif suffix in (".xlsx", ".xls"):
            df = pl.read_excel(tmp_path)
        else:
            raise HTTPException(400, f"不支持的文件格式: {suffix}")

        # 清洗列名：去掉括号内的时间戳等信息
        df = _clean_col_names(df)

        # 按映射关系自动生成 symbol 和 code 列
        df = _apply_mapping(df, config, _data_dir(request))
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # 确保配置的字段列存在于上传数据中（symbol/code 由映射自动生成，不校验）
    auto_fields = {"symbol", "code"}
    config_cols = {f.name for f in config.fields} - auto_fields
    missing = config_cols - set(df.columns)
    if missing:
        raise HTTPException(400, f"上传数据缺少字段: {', '.join(sorted(missing))}")

    # 只保留配置中定义的列（包括自动生成的 symbol/code），忽略文件中多余的字段
    all_config_cols = {f.name for f in config.fields}
    keep = [c for c in df.columns if c in all_config_cols]
    df = df.select(keep)

    # 解析快照日期: 非法值 400 (与 /rows 同一校验); 缺省按北京日期落盘,
    # 服务器时区不能决定分区归属 (UTC 容器北京 08:00 前会写进前一天)
    snap = date.fromisoformat(_partition_date(snapshot_date)) if snapshot_date else cn_today()

    rows = write_ext_parquet(df, config, _data_dir(request), snapshot_date=snap)

    # 刷新 DuckDB 视图
    _refresh_views(request)

    return {"status": "ok", "rows": rows, "date": snap.isoformat()}


# ---------------------------------------------------------------------------
# JSON 接口写入
# ---------------------------------------------------------------------------


@router.post("/{config_id}/ingest")
def ingest_data(request: Request, config_id: str, body: IngestReq):
    """通过 JSON 接口批量写入扩展数据。"""
    store = _store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    # 校验必填字段
    configured = {f.name for f in config.fields}
    required = configured - {"symbol"}
    for i, row in enumerate(body.rows):
        # 市场级表 (无标的列) 不强制 symbol; 标的表仍要求逐行携带
        if not config.market_level and "symbol" not in row:
            raise HTTPException(400, f"第 {i + 1} 行缺少 symbol 字段")
        missing = required - set(row.keys())
        if missing:
            raise HTTPException(400, f"第 {i + 1} 行缺少字段: {', '.join(sorted(missing))}")

    # 同 /upload: 非法日期 400, 缺省按北京日期落盘
    snap = date.fromisoformat(_partition_date(body.date)) if body.date else cn_today()

    rows_written = rows_to_parquet(body.rows, config, _data_dir(request), snapshot_date=snap)

    _refresh_views(request)

    return {"status": "ok", "rows": rows_written, "date": snap.isoformat()}


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Symbol 格式修复
# ---------------------------------------------------------------------------


@router.post("/{config_id}/fix-symbol")
def fix_symbol(request: Request, config_id: str):
    """扫描已有 Parquet 数据，将 symbol 列标准化为 代码.交易所 格式。"""
    store = _store(request)
    config = store.get(config_id)
    if not config:
        raise HTTPException(404, f"配置 '{config_id}' 不存在")

    fixed = fix_symbol_format(config, _data_dir(request))
    _refresh_views(request)
    return {"status": "ok", "fixed_files": fixed}


# ---------------------------------------------------------------------------
# 视图刷新
# ---------------------------------------------------------------------------


def _refresh_views(request: Request) -> None:
    """扩展数据变更后清理缓存（原 DuckDB 视图注册已随 CK 化移除）。

    扩展列已接入 enriched 帧 (compute_signals/compute_enriched_today 注入)，
    repo 内存 enriched 缓存 (_enriched_cache/_etf_/_index_) 持有含旧扩展列的
    帧，必须清理，否则写入后监控/列表仍用旧值。扩展数据读路径已全部走 CK
    ext_timeseries / _read_ext_dataframe，无需再注册本地 parquet 视图。
    """
    repo = request.app.state.repo
    if hasattr(repo, "clear_cache"):
        repo.clear_cache()


# ---------------------------------------------------------------------------
# 子模块 re-export（保持原公开 API 与 monkeypatch 可观察性）
# ---------------------------------------------------------------------------
# 用赋值式 re-export：子模块的路由注册在 importlib.import_module 时完成；
# 赋值目标进入本模块 globals，函数体与测试 monkeypatch 仍按原模块名解析。

__ext_read_mod = importlib.import_module("._ext_read", __package__)
_DIMENSION_SEPARATOR_CLASS = __ext_read_mod._DIMENSION_SEPARATOR_CLASS
_apply_mapping = __ext_read_mod._apply_mapping
_clean_col_names = __ext_read_mod._clean_col_names
_collect_ext_frame = __ext_read_mod._collect_ext_frame
_date_range = __ext_read_mod._date_range
_ext_data_dir = __ext_read_mod._ext_data_dir
_filter_dimension_member_rows = __ext_read_mod._filter_dimension_member_rows
_granularity_effective = __ext_read_mod._granularity_effective
_latest_sync_date = __ext_read_mod._latest_sync_date
_latest_sync_from_partitions = __ext_read_mod._latest_sync_from_partitions
_market_level_partition_days = __ext_read_mod._market_level_partition_days
_normalize_ext_dates = __ext_read_mod._normalize_ext_dates
_parquet_glob = __ext_read_mod._parquet_glob
_partition_date = __ext_read_mod._partition_date
_plate_stats = __ext_read_mod._plate_stats
_read_ext_snapshot_ck = __ext_read_mod._read_ext_snapshot_ck
_read_ext_timeseries_ck = __ext_read_mod._read_ext_timeseries_ck
_read_market_level_timeseries = __ext_read_mod._read_market_level_timeseries
_resolve_ext_range = __ext_read_mod._resolve_ext_range
_safe_json_value = __ext_read_mod._safe_json_value
_select_ext_columns = __ext_read_mod._select_ext_columns
_select_ext_partitions = __ext_read_mod._select_ext_partitions
del __ext_read_mod

__ext_dimension_mod = importlib.import_module("._ext_dimension", __package__)
_DIMENSION_INTRADAY_CACHE = __ext_dimension_mod._DIMENSION_INTRADAY_CACHE
_DIMENSION_INTRADAY_CACHE_TTL_S = __ext_dimension_mod._DIMENSION_INTRADAY_CACHE_TTL_S
_DIMENSION_INTRADAY_FFILL_CAP = __ext_dimension_mod._DIMENSION_INTRADAY_FFILL_CAP
_DIMENSION_INTRADAY_PCT_BOUND = __ext_dimension_mod._DIMENSION_INTRADAY_PCT_BOUND
_DIMENSION_INTRADAY_PCT_EPS = __ext_dimension_mod._DIMENSION_INTRADAY_PCT_EPS
_bare_symbol_expr = __ext_dimension_mod._bare_symbol_expr
_dimension_intraday_bars = __ext_dimension_mod._dimension_intraday_bars
_dimension_intraday_combine = __ext_dimension_mod._dimension_intraday_combine
_dimension_intraday_compute = __ext_dimension_mod._dimension_intraday_compute
_dimension_intraday_points = __ext_dimension_mod._dimension_intraday_points
_dimension_intraday_refs = __ext_dimension_mod._dimension_intraday_refs
_dimension_intraday_target = __ext_dimension_mod._dimension_intraday_target
_dimension_member_bares = __ext_dimension_mod._dimension_member_bares
_prev_daily_close = __ext_dimension_mod._prev_daily_close
_r4 = __ext_dimension_mod._r4
del __ext_dimension_mod

__ext_pull_mod = importlib.import_module("._ext_pull", __package__)
backfill_history_ep = __ext_pull_mod.backfill_history_ep
configure_pull = __ext_pull_mod.configure_pull
get_pull_api_key = __ext_pull_mod.get_pull_api_key
run_pull = __ext_pull_mod.run_pull
set_pull_api_key = __ext_pull_mod.set_pull_api_key
test_pull = __ext_pull_mod.test_pull
del __ext_pull_mod

__ext_files_mod = importlib.import_module("._ext_files", __package__)
_assert_public_http_url = __ext_files_mod._assert_public_http_url
_daily_series = __ext_files_mod._daily_series
_describe_parquet_columns = __ext_files_mod._describe_parquet_columns
_detect_fields = __ext_files_mod._detect_fields
_find_row_arrays = __ext_files_mod._find_row_arrays
_intraday_series_items = __ext_files_mod._intraday_series_items
detect_fields = __ext_files_mod.detect_fields
detect_url = __ext_files_mod.detect_url
discover_all_schemas = __ext_files_mod.discover_all_schemas
discover_schema = __ext_files_mod.discover_schema
ext_series = __ext_files_mod.ext_series
del __ext_files_mod
