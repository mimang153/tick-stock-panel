"""从 services/ext_data.py 抽取的 ext_data_format 子模块（保持原公开 API）。"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from app.db_safe import is_valid_ext_ident

if TYPE_CHECKING:
    from app.services.ext_data import ExtConfig, ExtField


def __getattr__(name):
    if name in {"ExtConfig", "ExtField"}:
        import importlib

        return getattr(importlib.import_module("app.services.ext_data"), name)
    raise AttributeError(name)


class _RootProxy:
    """按需解析根模块，保持 monkeypatch 对原模块符号的可见性。"""

    def __getattr__(self, name):
        import importlib

        return getattr(importlib.import_module("app.services.ext_data"), name)


_rootmod = _RootProxy()


def _pull_ts_value() -> str:
    """注入用拉取时刻 (北京时间 ISO 串); 测试 monkeypatch 本函数固定时刻。"""
    return _rootmod.cn_now().isoformat()


def norm_ext_snapshot_at(raw: str) -> str:
    """选盘时刻归一为 "HH:MM:SS" ("09:20" → "09:20:00"); 非法值抛 ValueError。"""
    m = _rootmod.EXT_SNAPSHOT_AT_RE.match(raw or "")
    if not m:
        raise ValueError(f"非法选盘时刻 {raw!r} (应为 HH:MM 或 HH:MM:SS)")
    return f"{raw}:00" if len(raw) == 5 else raw


def parse_ext_columns(ext_columns: str) -> list[tuple[str, str, str | None]]:
    """解析扩展列并按输出列名保序去重，非法项沿用跳过语义。

    输出列名不含选盘时刻，同一输出指定不同时刻或不同字段时拒绝整个请求，
    避免 JOIN 重名、静默覆盖或把一个时刻的值显示成另一个时刻。
    """
    columns: dict[str, tuple[str, str, str | None]] = {}
    for part in ext_columns.split(","):
        part = part.strip()
        if "." not in part:
            continue
        config_id, field_name = part.split(".", 1)
        at: str | None = None
        if "@" in field_name:
            field_name, at_raw = field_name.rsplit("@", 1)
            try:
                at = norm_ext_snapshot_at(at_raw)
            except ValueError:
                continue
        config_id = config_id.strip()
        field_name = field_name.strip()
        if (
            not config_id
            or not field_name
            or not is_valid_ext_ident(config_id)
            or "\x00" in field_name
        ):
            continue
        spec = (config_id, field_name, at)
        output = f"{config_id}__{field_name}"
        previous = columns.get(output)
        if previous is not None and previous != spec:
            if previous[:2] == spec[:2]:
                raise ValueError(f"扩展字段 {config_id}.{field_name} 不能同时指定不同选盘时刻")
            raise ValueError(f"扩展字段输出列名冲突：{output}")
        columns[output] = spec
    return list(columns.values())


def collapse_ext_rows_at(df: pl.DataFrame, at: str | None, time_field: str | None) -> pl.DataFrame:
    """日内多盘行收敛为每标的一行 (列表/富化取值用)。

    - at=None 或表未配置 time_field / 列缺失 → 末行收敛 (历史行为, 末盘定型值)。
    - at="HH:MM[:SS]" → 时间列时刻部分 <= at 的行中取最后一行, 即「该时刻或
      之前最近一盘」(与 tickflow-hub /snapshot?at= 同语义); 该时刻之前无任何
      行的标的不出现 (fail-closed, 不拿后盘伪造前盘值)。

    时间列兼容 ISO-T / 空格两种日期前缀 (时刻都在 [11:19]); 纯时刻列原样比较。
    """
    key_cols = ["symbol"] if "symbol" in df.columns else df.columns[:1]
    if at is None or not time_field or time_field not in df.columns:
        return df.unique(subset=key_cols, keep="last")
    want = norm_ext_snapshot_at(at)
    ts = pl.col(time_field).cast(pl.Utf8)
    tod = pl.when(ts.str.len_chars() >= 11).then(ts.str.slice(11, 8)).otherwise(ts)
    return (
        df.with_columns(tod.alias("_ext_tod"))
        .filter(pl.col("_ext_tod") <= want)
        .drop("_ext_tod")
        .sort(["symbol", time_field] if "symbol" in df.columns else [time_field])
        .unique(subset=key_cols, keep="last")
    )


def infer_fields_from_df(df: pl.DataFrame) -> list[dict]:
    """从 DataFrame 推断扩展字段定义。"""
    fields = []
    for col_name in df.columns:
        pl_type = df[col_name].dtype
        dtype = _rootmod._POLARS_TYPE_MAP.get(str(pl_type.base_type()), "string")
        fields.append({"name": col_name, "dtype": dtype, "label": col_name})
    return fields


def detect_symbol_candidates(df: pl.DataFrame) -> tuple[list[str], list[str]]:
    """识别 symbol/code 候选列。"""
    symbol_candidates: list[str] = []
    code_candidates: list[str] = []

    for col in df.columns:
        try:
            col_data = df[col].cast(pl.Utf8).drop_nulls()
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            continue
        if len(col_data) == 0:
            continue
        sample = col_data.head(200).to_list()
        sym_hits = sum(1 for v in sample if _rootmod._SYMBOL_PAT.match(str(v).strip()))
        code_hits = sum(1 for v in sample if _rootmod._CODE_PAT.match(str(v).strip()))
        total = len(sample)
        if total > 0:
            if sym_hits / total > 0.5:
                symbol_candidates.append(col)
            elif code_hits / total > 0.5:
                code_candidates.append(col)

    return symbol_candidates, code_candidates


def build_code_lookup(data_dir: Path) -> dict[str, str]:
    """从 instruments 维表构建 code → symbol 映射（CK 唯一读源）。

    CK 无数据或未初始化（离线脚本/单测）时回退本地维表文件；读取失败一律返回
    `{}`（写入链路的辅助查找，缺映射只影响 symbol 归一精度）。
    """
    try:
        from app.services.instrument_sync import load_instruments

        df = load_instruments().select(["code", "symbol"])
        if not df.is_empty():
            return dict(zip(df["code"].to_list(), df["symbol"].to_list(), strict=True))
    except Exception:  # noqa: BLE001 - 辅助查找失败回退本地维表
        pass
    path = data_dir / "instruments" / "instruments.parquet"
    if not path.exists():
        return {}
    try:
        df = pl.read_parquet(path, columns=["code", "symbol"])
        return dict(zip(df["code"].to_list(), df["symbol"].to_list(), strict=False))
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return {}


def normalize_symbol(series: pl.Series, lookup: dict[str, str] | None = None) -> pl.Series:
    """将 symbol 列标准化为 代码.交易所 格式。

    优先使用 instruments 维表查找 code → symbol，确保 100% 准确。
    查不到时按规则兜底：6开头 → .SH，其余 → .SZ。
    """
    _lookup = lookup or {}

    def _fix_one(val: str) -> str:
        if not val:
            return val
        val = val.strip()
        # 已经是标准格式（含 .），直接返回
        if "." in val:
            return val
        # 纯6位数字代码 → 优先查维表
        if len(val) == 6 and val.isdigit():
            mapped = _lookup.get(val)
            if mapped:
                return mapped
            # 兜底规则
            if val.startswith(("6",)):
                return f"{val}.SH"
            else:
                return f"{val}.SZ"
        return val

    return series.map_elements(_fix_one, return_dtype=pl.Utf8)


def apply_config_mapping(df: pl.DataFrame, config: ExtConfig, data_dir: Path) -> pl.DataFrame:
    """根据 config 的 symbol_map / code_map 自动生成 symbol 和 code 列。"""
    sm = config.symbol_map or {}
    cm = config.code_map or {}

    if sm.get("type") == "mapped" and sm["col"] in df.columns:
        df = df.with_columns(df[sm["col"]].cast(pl.Utf8).alias("symbol"))

    if cm.get("type") == "mapped" and cm["col"] in df.columns:
        df = df.with_columns(df[cm["col"]].cast(pl.Utf8).alias("code"))

    if "symbol" not in df.columns and sm.get("type") == "computed":  # noqa: SIM102 - SIM102: 嵌套if表达独立前置条件, 合并损可读性
        if sm.get("from") == "code" and "code" in df.columns:
            lookup = build_code_lookup(data_dir)
            df = df.with_columns(normalize_symbol(df["code"].cast(pl.Utf8), lookup).alias("symbol"))

    if "code" not in df.columns and cm.get("type") == "computed":  # noqa: SIM102 - SIM102: 嵌套if表达独立前置条件, 合并损可读性
        if cm.get("from") == "symbol" and "symbol" in df.columns:
            df = df.with_columns(
                df["symbol"].cast(pl.Utf8).str.split(".").list.first().alias("code")
            )

    if "symbol" in df.columns and "code" not in df.columns:
        df = df.with_columns(df["symbol"].cast(pl.Utf8).str.split(".").list.first().alias("code"))
    elif "code" in df.columns and "symbol" not in df.columns:
        lookup = build_code_lookup(data_dir)
        df = df.with_columns(normalize_symbol(df["code"].cast(pl.Utf8), lookup).alias("symbol"))

    if "symbol" in df.columns:
        lookup = build_code_lookup(data_dir)
        df = df.with_columns(normalize_symbol(df["symbol"].cast(pl.Utf8), lookup))

    return df


def cast_df_to_schema(df: pl.DataFrame, fields: list[ExtField]) -> pl.DataFrame:
    """按配置的字段类型转换 DataFrame 列类型。

    List → string 的处理: 上游接口常返回数组字段 (如 concepts: ["AI", "芯片"]),
    若声明为 string, 直接 cast 会抛 `cannot cast List type`。
    这里把列表元素先转字符串再以分号拼接, 与 _flatten_concept_rows 保持一致。
    """
    schema = df.schema
    for f in fields:
        target = _rootmod._POLARS_DTYPE_MAP.get(f.dtype, pl.Utf8)
        if f.name not in df.columns:
            # Optional observations may not exist yet (for example the first
            # auction snapshot). Keep their declared type without inventing a value.
            df = df.with_columns(pl.lit(None, dtype=target).alias(f.name))
            schema[f.name] = target
            continue
        src = schema[f.name]
        if isinstance(src, pl.List) and target == pl.Utf8:
            df = df.with_columns(pl.col(f.name).cast(pl.List(pl.Utf8)).list.join(";").cast(target))
        else:
            df = df.with_columns(pl.col(f.name).cast(target))
    return df
