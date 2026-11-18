"""从 services/ext_data.py 抽取的 ext_data_io 子模块（保持原公开 API）。"""

from __future__ import annotations

import codecs
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from app.services.fs_utils import atomic_write_parquet

if TYPE_CHECKING:
    from app.services.ext_data import ExtConfig


def __getattr__(name):
    if name == "ExtConfig":
        import importlib

        return getattr(importlib.import_module("app.services.ext_data"), name)
    raise AttributeError(name)


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.services.ext_data"), name)


_rootmod = _RootProxy()


def _decodes_as(file_path: Path, encoding: str) -> bool:
    """整个文件能否按 encoding 完整解码，逐块判断，不把文件读进内存。"""
    decoder = codecs.getincrementaldecoder(encoding)()
    try:
        with file_path.open("rb") as src:
            while chunk := src.read(_rootmod._TRANSCODE_CHUNK_BYTES):
                decoder.decode(chunk)
            decoder.decode(b"", True)  # 结尾处的半个字符也算解码失败
    except UnicodeDecodeError:
        return False
    return True


def _transcode_to_utf8(file_path: Path, out_path: Path, encoding: str) -> bool:
    """按 encoding 逐块转成 UTF-8 写入 out_path；解码失败则删除半成品返回 False。

    增量解码器负责跨块边界的多字节字符：GBK 一个汉字两字节，正好落在块边界
    上时前半截会被留到下一块，不会被误判成解码失败。
    """
    decoder = codecs.getincrementaldecoder(encoding)()
    try:
        with (
            file_path.open("rb") as src,
            out_path.open("w", encoding="utf-8", newline="") as dst,
        ):
            while chunk := src.read(_rootmod._TRANSCODE_CHUNK_BYTES):
                dst.write(decoder.decode(chunk))
            dst.write(decoder.decode(b"", True))
    except UnicodeDecodeError:
        out_path.unlink(missing_ok=True)
        return False
    return True


def ensure_utf8_csv(file_path: Path) -> Path:
    """确保 CSV 文件以 UTF-8 编码可读，非 UTF-8（如 GBK/GB18030）则转换。

    国内行情软件（同花顺/东财/通达信）和 Windows 中文 Excel 导出的 CSV 多为
    GBK 系编码，Polars 的 read_csv 默认按 UTF-8 解析会抛 "invalid utf-8 sequence"。
    这里在交给 Polars 前做一次编码规范化。

    返回值：若已是 UTF-8 则返回原路径；否则在同目录写一个 *.utf8 文件并返回它
    （调用方用临时目录，随目录一起清理）。
    """
    # BOM 处理：UTF-8-SIG 等带 BOM 文件直接交给 Polars（它认识 BOM）
    if _decodes_as(file_path, "utf-8"):
        return file_path  # 已是合法 UTF-8
    # 依次尝试常见中文编码，第一个能完整解码的即为命中
    for enc in ("gb18030", "gbk", "gb2312", "big5"):
        out_path = file_path.with_suffix(file_path.suffix + ".utf8")
        if not _transcode_to_utf8(file_path, out_path, enc):
            continue
        _rootmod.logger.info("CSV 编码转换 %s → %s (%s)", file_path.name, out_path.name, enc)
        return out_path
    # 都无法解码：返回原路径，让 Polars 抛出更精确的原始错误
    return file_path


def parse_upload_file(
    file_path: Path, symbol_col: str = "symbol", data_dir: Path | None = None
) -> pl.DataFrame:
    """解析上传的 CSV / Excel 文件为 Polars DataFrame。"""
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        df = pl.read_csv(ensure_utf8_csv(file_path), infer_schema_length=10000)
    elif suffix in (".xlsx", ".xls"):
        df = pl.read_excel(file_path)
    else:
        raise ValueError(f"不支持的文件格式: {suffix}")

    if symbol_col not in df.columns:
        # 尝试模糊匹配
        candidates = [c for c in df.columns if c.lower() in ("symbol", "code", "代码", "标的")]
        if candidates:
            df = df.rename({candidates[0]: symbol_col})
        else:
            raise ValueError(f"未找到标的代码列 (symbol)，可选列: {df.columns}")

    # 确保 symbol 列为字符串并标准化
    lookup = _rootmod.build_code_lookup(data_dir) if data_dir else None
    df = df.with_columns(_rootmod.normalize_symbol(df[symbol_col].cast(pl.Utf8), lookup))
    return df


def _config_dir(config_id: str, data_dir: Path) -> Path:
    """返回扩展配置的根目录 data/ext_data/{config_id}/。"""
    return data_dir / "ext_data" / config_id


def _validate_schema_compatible(old_schema, new_schema) -> None:
    """合并旧分区前校验重名字段类型兼容；不兼容时拒绝写入。"""
    for name in old_schema.keys() & new_schema.keys():
        old_type, new_type = old_schema[name], new_schema[name]
        if (
            old_type != new_type
            and old_type != pl.Null
            and new_type != pl.Null
            and not (old_type.is_numeric() and new_type.is_numeric())
        ):
            raise ValueError(
                f"扩展字段 '{name}' 类型不兼容 ({old_type} / {new_type}), 原数据保持不变"
            )


def write_ext_parquet(
    df: pl.DataFrame,
    config: ExtConfig,
    data_dir: Path,
    snapshot_date: date | None = None,
    *,
    keep_strategy_cache: bool = False,
) -> int:
    """将 DataFrame 写入扩展数据 Parquet。

    目录结构:
      - snapshot:  data/ext_data/{id}/part.parquet（与 config.json 同级，覆盖写）
      - timeseries: data/ext_data/{id}/timeseries/date=xxx/part.parquet（按日分区）

    Returns:
        写入行数。
    """
    if df.is_empty():
        return 0

    # 缺省分区日期按北京日期, 与定时拉取 (ext_pull.fetch_and_ingest) 同口径
    snap = snapshot_date or _rootmod.cn_today()
    cfg_dir = _config_dir(config.id, data_dir)

    # 标准化 symbol 列: 用维表查找 → 准确匹配交易所
    if "symbol" in df.columns:
        lookup = _rootmod.build_code_lookup(data_dir)
        df = df.with_columns(_rootmod.normalize_symbol(df["symbol"], lookup))

    if config.mode == "snapshot":
        # 快照: 与 config.json 同级，直接覆盖
        cfg_dir.mkdir(parents=True, exist_ok=True)
        out_path = cfg_dir / "part.parquet"
    else:
        # 时序: timeseries/ 下按日期分区
        out_dir = cfg_dir / "timeseries" / f"date={snap}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part.parquet"

        # time_field 配置为保留名: 注入拉取时刻, 盘中每次拉取各存一盘
        # (同批一次注入 → 批内仍按 symbol 收敛; 不同批次不同盘)。
        tf = config.pull.time_field if config.pull else None
        if tf == _rootmod.PLATE_TS_FIELD and _rootmod.PLATE_TS_FIELD not in df.columns:
            df = df.with_columns(pl.lit(_rootmod._pull_ts_value()).alias(_rootmod.PLATE_TS_FIELD))

    tf = config.pull.time_field if config.pull and config.mode != "snapshot" else None
    if tf and (tf not in df.columns or df[tf].null_count()):
        raise ValueError(f"扩展数据时间字段 '{tf}' 缺失或为空, 原数据保持不变")

    # Normalize configured types before merging: an initially empty float column
    # and a later integer/float observation still belong to the same field.
    df = _rootmod.cast_df_to_schema(df, config.fields)
    key = ["symbol" if "symbol" in df.columns else df.columns[0]]
    if tf and tf in df.columns and tf not in key:
        key.append(tf)

    if out_path.exists():
        existing = _rootmod.cast_df_to_schema(pl.read_parquet(out_path), config.fields)
        _validate_schema_compatible(existing.schema, df.schema)
        # Union optional columns by name; incompatible schemas must fail before
        # atomic replacement, never fall back to discarding the existing rows.
        df = pl.concat([existing, df], how="diagonal_relaxed")

    df = df.unique(subset=key, keep="last", maintain_order=True)
    if config.mode != "snapshot":
        # 日内序列表: 分区内按 [symbol, 时间列] 升序落盘。读侧 (screener._load_ext_value_maps、
        # ext_factors 因子帧、板块资金流) 按 symbol unique(keep="last") 收敛到一行, 只认行序;
        # 上游及不同批次不保证时间序, 不排序时可能把更早的盘次当作末盘
        tf = config.pull.time_field if config.pull else None
        if tf and tf in df.columns:
            df = df.sort(["symbol", tf] if "symbol" in df.columns else [tf], maintain_order=True)
    # 中断时保留旧分区, 避免读侧看到半截文件。
    atomic_write_parquet(df, out_path)
    _mirror_ext_to_ck(config, df, snap)
    _rootmod.logger.info("扩展表写入: %s → %s (%d 行)", config.id, out_path, len(df))
    # 扩展列已接入 enriched 帧/因子注册表: 写入后必须失效相关缓存
    _invalidate_ext_derived(data_dir, keep_strategy_cache=keep_strategy_cache)
    return len(df)


def _ext_time_field(config: ExtConfig, df: pl.DataFrame) -> str | None:
    """扩展表的日内时间列（写入 CK 的 time_value 主键分量）。"""
    tf = config.pull.time_field if config.pull else None
    if tf and tf in df.columns:
        return tf
    for name in ("time", "trade_time", "datetime"):
        if name in df.columns:
            return name
    return None


def _ext_business_date_field(config: ExtConfig, df: pl.DataFrame) -> str | None:
    """日级表的业务日期列（映射日期优先，其次 date / data_date）。

    CK ``ext_timeseries`` 的主键是 (ext_id, symbol, date, time_value)，其中 date
    是分区日期；日级表若在同一分区内带多个业务日期（源按日期回补），仅靠分区
    日期无法区分，各行会在主键上互相覆盖。业务日期列因此作为 time_value 分量。
    """
    mapped = config.pull.field_map.get("date") if config.pull else None
    for candidate in (mapped, "date", "data_date"):
        if candidate and candidate in df.columns:
            return candidate
    return None


def _mirror_ext_to_ck(config: ExtConfig, df: pl.DataFrame, day: date, store=None) -> None:
    """扩展数据落盘后镜像到 CK（ext_timeseries）。

    市场级表（``market_level``，无 symbol 列）在主键 (ext_id, symbol, date,
    time_value) 下无法无损表达，仍以 Parquet 为准；其余有 symbol 的扩展双写，
    为后续读切换铺路。``store`` 显式传入供回填脚本复用；不传则取全局存储，
    未初始化（离线脚本/单测）时跳过。
    """
    if config.market_level or "symbol" not in df.columns or df.is_empty():
        return
    if store is None:
        try:
            from app.storage.runtime import get_storage

            runtime = get_storage()
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return
        if runtime is None:
            return
        store = runtime.store
    tf = _ext_time_field(config, df)
    if tf is None:
        # 日级表没有日内时刻列：业务日期进 time_value 分量，保留同分区内
        # 同一标的的多个业务日期（否则在 CK 主键上互相覆盖）。
        tf = _ext_business_date_field(config, df)
    frame = df.with_columns(
        pl.lit(config.id).alias("ext_id"),
        pl.lit(day).cast(pl.Date).alias("date"),
        (pl.col(tf).cast(pl.Utf8) if tf else pl.lit("")).fill_null("").alias("time_value"),
    )
    try:
        store.write_ext_timeseries(frame)
    except Exception as e:  # noqa: BLE001 - 双写失败不阻断主链路（Parquet 仍为准）
        _rootmod.logger.warning("扩展 %s 镜像 CK 失败: %s", config.id, e)


def _invalidate_ext_derived(data_dir: Path, *, keep_strategy_cache: bool = False) -> None:
    """扩展数据/配置变更 → 扩展帧缓存 + 因子同步状态 + 策略结果缓存。

    惰性导入避免与 ext_factors (反向惰性引用本模块) 构成模块级环。
    repo 内存 enriched 缓存由 API 层 repo.clear_cache() 补充清理。
    keep_strategy_cache 语义见 ext_factors.invalidate_ext_caches。
    """
    try:
        from app.factors.ext_factors import invalidate_ext_caches

        invalidate_ext_caches(data_dir, keep_strategy_cache=keep_strategy_cache)
    except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        _rootmod.logger.warning("扩展数据缓存失效失败: %s", e)


def delete_ext_parquet(config_id: str, data_dir: Path) -> None:
    """删除扩展数据源关联的所有 Parquet 数据（保留 config.json）。

    - snapshot: 删除 ext_data/{id}/part.parquet
    - timeseries: 删除 ext_data/{id}/timeseries/ 目录
    """
    cfg_dir = _config_dir(config_id, data_dir)
    # 删除快照文件
    snap = cfg_dir / "part.parquet"
    if snap.exists():
        snap.unlink()
    # 删除时序目录
    ts_dir = cfg_dir / "timeseries"
    if ts_dir.exists():
        import shutil

        shutil.rmtree(ts_dir, ignore_errors=True)
    _invalidate_ext_derived(data_dir)


def fix_symbol_format(config: ExtConfig, data_dir: Path) -> int:
    """扫描该扩展配置的所有 Parquet 文件，将 symbol 列标准化为 代码.交易所 格式。

    - snapshot: 扫描 ext_data/{id}/part.parquet
    - timeseries: 扫描 ext_data/{id}/timeseries/date=xxx/part.parquet

    Returns:
        修复的文件数。
    """
    cfg_dir = _config_dir(config.id, data_dir)
    if not cfg_dir.exists():
        return 0

    # 收集需要扫描的 parquet 文件列表
    parquet_files: list[Path] = []
    if config.mode == "snapshot":
        p = cfg_dir / "part.parquet"
        if p.exists():
            parquet_files.append(p)
    else:
        ts_dir = cfg_dir / "timeseries"
        if ts_dir.exists():
            for part_dir in sorted(ts_dir.iterdir()):
                if not part_dir.is_dir() or not part_dir.name.startswith("date="):
                    continue
                p = part_dir / "part.parquet"
                if p.exists():
                    parquet_files.append(p)

    fixed = 0
    lookup = _rootmod.build_code_lookup(data_dir)
    for parquet_path in parquet_files:
        try:
            df = pl.read_parquet(parquet_path)
            if "symbol" not in df.columns:
                continue
            old = df["symbol"].to_list()
            df = df.with_columns(_rootmod.normalize_symbol(df["symbol"], lookup))
            new = df["symbol"].to_list()
            if old != new:
                atomic_write_parquet(df, parquet_path)
                fixed += 1
                _rootmod.logger.info(
                    "代码格式修复: %s/%s (%d 行)", config.id, parquet_path.parent.name, len(df)
                )
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            _rootmod.logger.warning("代码格式修复跳过 %s: %s", parquet_path, e)

    return fixed


def rows_to_parquet(
    rows: list[dict],
    config: ExtConfig,
    data_dir: Path,
    snapshot_date: date | None = None,
    *,
    keep_strategy_cache: bool = False,
) -> int:
    """将 JSON 行列表转为 DataFrame 写入 Parquet，复用 write_ext_parquet 的存储逻辑。

    Returns:
        写入行数。
    """
    # Sparse fields can first appear in later snapshots, beyond Polars' default
    # 100-row inference window. Infer across this response so those keys survive.
    df = pl.DataFrame(rows, infer_schema_length=None)
    df = _rootmod.apply_config_mapping(df, config, data_dir)
    if "symbol" in df.columns:
        df = df.with_columns(pl.col("symbol").cast(pl.Utf8))
    return write_ext_parquet(
        df,
        config,
        data_dir,
        snapshot_date=snapshot_date,
        keep_strategy_cache=keep_strategy_cache,
    )
