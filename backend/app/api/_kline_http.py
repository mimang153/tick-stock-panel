"""K 线 HTTP 响应辅助：批量上限、JSON 净化、gzip 与拼音搜索。"""

from __future__ import annotations

import gzip
import json
import math
from functools import lru_cache

from fastapi import HTTPException, Request, Response

from app.api._root_ref import bind as _bind
from app.services import kline_sync

_root = _bind("app.api.kline")
logger = _root.logger


# 批量端点 (daily-batch / minute-batch) 单次 symbols 上限: 千只级自选整表直发
# 会在服务端构造百万行级的 Python dict 峰值 (内存暴涨主因之一)。上限值是可配置
# 偏好 (见 services/preferences.py), 前端分块大小读同一配置 —— 不再各处硬编码,
# 避免「后端调小、前端仍按旧值分块」导致的全站 400。
# 兜底留给旧客户端/异常调用: 超限明确报错, 文案回传当前值便于客户端自愈。
def _batch_symbols_limit(key: str) -> int:
    from app.services import preferences

    return (
        preferences.get_daily_batch_symbols()
        if key == "daily"
        else preferences.get_minute_batch_symbols()
    )


def _reject_over_limit(symbols: list, kind: str) -> None:
    limit = _batch_symbols_limit(kind)
    if len(symbols) <= limit:
        return
    label = "日K" if kind == "daily" else "分时"
    raise HTTPException(
        status_code=400,
        detail=(
            f"{label}批量单次上限 {limit} 只 (当前 {len(symbols)} 只), 请分批请求; "
            f"可在 系统设置 → 批量请求 调整上限"
        ),
    )


def _json_safe(obj):
    """把 nan/inf 换成 None, 保证 JSON 合法。

    gzip 路径原先 allow_nan=True, 会写出前端 JSON.parse 不能吃的 NaN/Infinity;
    未压缩路径走 Starlette allow_nan=False, 遇到非有限浮点整段 500。
    """
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_json_safe(v) for v in obj]
    return obj


def _compress_on(pref_key: str) -> bool:
    """读取传输压缩偏好; 未知键/读取异常一律按关闭。"""
    from app.services import preferences as _prefs

    _getters = {
        "minute_batch_compress": _prefs.get_minute_batch_compress,
        "daily_batch_compress": _prefs.get_daily_batch_compress,
    }
    getter = _getters.get(pref_key)
    if getter is None:
        return False
    try:
        return bool(getter())
    except Exception:  # noqa: BLE001 - 偏好读取失败按不压缩返回原样
        return False


def _client_accepts_gzip(request: Request) -> bool:
    headers = getattr(request, "headers", None) or {}
    return "gzip" in (headers.get("accept-encoding") or "")


def _gzip_payload(request: Request, payload: dict, *, pref_key: str) -> dict | Response:
    """大 JSON 响应的传输压缩: 偏好开启 + 客户端接受 gzip + 响应超阈值才压。

    分时/日K各自使用独立偏好键 (沿用已有 *_batch_compress 存储键保证兼容)。
    level 6 实测 13MB ≈ 290ms CPU 压掉 87%; level 9 要 2.5s 不可用。
    datetime → isoformat, 与 FastAPI jsonable_encoder 输出一致
    (前端 since 增量按字符串字典序比较, 格式必须与非压缩路径相同)。
    """
    if _compress_on(pref_key) and _client_accepts_gzip(request):
        # NaN/inf 只在停牌/坏源时出现, 常态直接序列化, 仅在 allow_nan=False
        # 抛 ValueError 时才回退 _json_safe 整树深拷贝 —— 深拷贝是批量响应
        # 峰值最大的一份瞬态分配 (与原 payload 同量级), 常态跳过后峰值约减半。
        def _dump(obj):
            return json.dumps(
                obj,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
                default=lambda o: o.isoformat() if hasattr(o, "isoformat") else str(o),
            ).encode()

        try:
            raw = _dump(payload)
        except ValueError:
            import app.api.kline as _root

            payload = _root._json_safe(payload)
            raw = _dump(payload)
        if len(raw) > 1024:
            return Response(
                content=gzip.compress(raw, 6),
                media_type="application/json",
                headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"},
            )
        # 未达压缩阈值: dumps 成功则 payload 必无非有限浮点, 回退分支则已净化
        return payload
    return _json_safe(payload)


def _gzip_raw(request: Request, raw: bytes, *, pref_key: str) -> Response:
    """已序列化 JSON bytes 的传输压缩: 规则与 _gzip_payload 一致 (偏好/阈值/level)。"""
    if _compress_on(pref_key) and _client_accepts_gzip(request) and len(raw) > 1024:
        return Response(
            content=gzip.compress(raw, 6),
            media_type="application/json",
            headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"},
        )
    return Response(content=raw, media_type="application/json")


def _batch_frames_json(parts, dt_col: str, dt_fmt: str, extra=None) -> bytes:
    """批量 K 响应直出 JSON: 每只 symbol 的帧由 Polars Rust 侧序列化为行式片段。

    与 to_dicts()+json.dumps 输出字节级等价 (tests/test_kline_batch_frames_json 锁定),
    但完全不构建 Python dict —— 对象图与序列化耗时一起消失 (实测 96k 根:
    1222ms / 峰值 69MB → 26ms / 峰值 18MB)。列集合与顺序沿用各帧自身 (含 symbol 列),
    与 to_dicts 的键序完全一致。
    datetime/date 列由 strftime 预格式化: write_json 原生输出空格分隔, 而前端
    since 增量按 ISO (T 分隔) 字符串字典序比较, 格式必须与既有响应一致。
    NaN/inf → null 为 write_json 原生行为, 与 _json_safe 净化语义一致。
    """
    import polars as pl

    frags = [
        json.dumps(sym) + ":" + sub.with_columns(pl.col(dt_col).dt.strftime(dt_fmt)).write_json()
        for sym, sub in parts.items()
    ]
    raw = '{"data":{' + ",".join(frags) + "}"
    if extra:
        # 去掉 dumps 的最外层花括号, 键值并入根对象 (与旧 dict 路径的 payload 布局一致)
        raw += "," + json.dumps(extra, ensure_ascii=False, separators=(",", ":"))[1:-1]
    return (raw + "}").encode()


def _minute_allowed(capset) -> bool:
    """仅当前分钟数据源确实可用时开放分钟 K 接口。"""
    from app.tickflow.capabilities import Cap

    provider = kline_sync._selected_minute_provider_name()
    if provider == "tickflow":
        return capset.has(Cap.KLINE_MINUTE_BATCH)
    _, fallback, error = kline_sync._resolve_minute_provider(provider)
    if error is not None:
        logger.warning("minute provider resolution failed while checking access: %s", error)
    return error is None and not fallback


@lru_cache(maxsize=8192)
def _name_pinyin_keys(name: str) -> tuple[str, ...]:
    """返回中文名称所有可能的拼音首字母串 (多音字展开为笛卡尔积)。

    '平安银行' -> ('PAYH',); '重庆百货' -> ('CQBH', 'CQMH', 'ZQBH', 'ZQMH')。
    非汉字字符原样保留: '万科A' -> ('WKA',)。
    股票名总量有限且不变, lru_cache 命中后单次查询 ≈ dict 查找, 全市场遍历 < 1ms。
    """
    from pypinyin import Style, pinyin

    if not name:
        return ()
    keys = [""]
    for group in pinyin(name, style=Style.FIRST_LETTER, heteronym=True):
        keys = [k + g.upper() for k in keys for g in group]
    return tuple(keys)


def _init_pinyin_dict() -> None:
    """加载 A 股高频多音字地名/词组词典, 使常见误读也能命中。

    pypinyin 默认词典对部分地名取常见读音 (如「重」→ chóng), 补充后「重庆」
    同时接受 zhòng/qìng (zq) 与 chóng/qīng (cq) 两种首字母, 与同花顺行为一致。
    幂等: 多次调用安全。
    """
    try:
        from pypinyin import load_phrases_dict

        # value 用二维 list: 每个字给一个或多个读音
        load_phrases_dict(
            {
                "重庆": [["zhòng", "chóng"], ["qīng"]],
                "长安": [["cháng", "zhǎng"], ["ān"]],
                "长春": [["cháng", "zhǎng"], ["chūn"]],
                "长沙": [["cháng", "zhǎng"], ["shā"]],
                "长城": [["cháng", "zhǎng"], ["chéng"]],
                "长江": [["cháng", "zhǎng"], ["jiāng"]],
            }
        )
    except Exception as exc:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        logger.warning(
            "pypinyin phrases dict load failed (polyphone coverage may degrade): %s", exc
        )


_init_pinyin_dict()


def _match_pinyin(name: str, keyword: str) -> bool:
    """keyword 是否匹配 name 任一拼音首字母串的前缀 (支持多音字)。"""
    return any(k.startswith(keyword) for k in _name_pinyin_keys(name))


def _instrument_frames(repo, types: list[str]):
    """按资产类型归一化 instruments 列，避免 concat 时 dtype 冲突。"""
    import polars as pl

    parts: list[pl.DataFrame] = []
    for asset_type in types:
        frame = repo.get_instruments_asset(asset_type)
        if frame.is_empty() or "symbol" not in frame.columns:
            continue
        parts.append(
            frame.with_columns(
                [
                    pl.col("symbol").cast(pl.Utf8).alias("symbol"),
                    (pl.col("name").cast(pl.Utf8) if "name" in frame.columns else pl.lit("")).alias(
                        "name"
                    ),
                    (pl.col("code").cast(pl.Utf8) if "code" in frame.columns else pl.lit("")).alias(
                        "code"
                    ),
                    pl.lit(asset_type).alias("asset_type"),
                ]
            ).select(["symbol", "name", "code", "asset_type"])
        )
    return parts


def _pinyin_prefix_hits(df, keyword: str, seen: set[str], remaining: int) -> list[dict]:
    import polars as pl

    rows: list[dict] = []
    if not keyword.isalpha() or not keyword.isascii() or remaining <= 0:
        return rows
    for row in df.filter(~pl.col("symbol").is_in(seen)).iter_rows(named=True):
        if _match_pinyin(row["name"], keyword):
            rows.append(row)
            if len(rows) >= remaining:
                break
    return rows
