"""扩展数据服务 — 配置管理 + 文件解析 + Parquet 存储。"""

from __future__ import annotations

import asyncio as asyncio
import codecs as codecs
import copy as copy
import json as json
import logging as logging
import re as re
import threading as threading
from contextlib import suppress as suppress
from datetime import date as date
from datetime import datetime as datetime
from pathlib import Path as Path
from typing import Literal as Literal

import anyio as anyio
import polars as pl

from app.db_safe import is_valid_ext_ident as is_valid_ext_ident
from app.market_time import cn_now as cn_now
from app.market_time import cn_today as cn_today
from app.services.fs_utils import (
    atomic_write_parquet as atomic_write_parquet,
)
from app.services.fs_utils import (
    atomic_write_text as atomic_write_text,
)

# --- re-export：保持 import 路径与 monkeypatch 可观察性 ---
from .ext_data_format import (
    _pull_ts_value as _pull_ts_value,
)
from .ext_data_format import (
    apply_config_mapping as apply_config_mapping,
)
from .ext_data_format import (
    build_code_lookup as build_code_lookup,
)
from .ext_data_format import (
    cast_df_to_schema as cast_df_to_schema,
)
from .ext_data_format import (
    collapse_ext_rows_at as collapse_ext_rows_at,
)
from .ext_data_format import (
    detect_symbol_candidates as detect_symbol_candidates,
)
from .ext_data_format import (
    infer_fields_from_df as infer_fields_from_df,
)
from .ext_data_format import (
    norm_ext_snapshot_at as norm_ext_snapshot_at,
)
from .ext_data_format import (
    normalize_symbol as normalize_symbol,
)
from .ext_data_format import (
    parse_ext_columns as parse_ext_columns,
)
from .ext_data_io import (
    _config_dir as _config_dir,
)
from .ext_data_io import (
    _decodes_as as _decodes_as,
)
from .ext_data_io import (
    _ext_business_date_field as _ext_business_date_field,
)
from .ext_data_io import (
    _ext_time_field as _ext_time_field,
)
from .ext_data_io import (
    _invalidate_ext_derived as _invalidate_ext_derived,
)
from .ext_data_io import (
    _mirror_ext_to_ck as _mirror_ext_to_ck,
)
from .ext_data_io import (
    _transcode_to_utf8 as _transcode_to_utf8,
)
from .ext_data_io import (
    _validate_schema_compatible as _validate_schema_compatible,
)
from .ext_data_io import (
    delete_ext_parquet as delete_ext_parquet,
)
from .ext_data_io import (
    ensure_utf8_csv as ensure_utf8_csv,
)
from .ext_data_io import (
    fix_symbol_format as fix_symbol_format,
)
from .ext_data_io import (
    parse_upload_file as parse_upload_file,
)
from .ext_data_io import (
    rows_to_parquet as rows_to_parquet,
)
from .ext_data_io import (
    write_ext_parquet as write_ext_parquet,
)

logger = logging.getLogger(__name__)

# Preserve the serial read/merge/write order previously provided by the event
# loop, without blocking it or multiplying peak memory for simultaneous imports.
_EXT_IO_LIMITER = anyio.CapacityLimiter(1)
_EXT_IO_TASKS: set[asyncio.Task] = set()


def _ext_io_done(task: asyncio.Task) -> None:
    _EXT_IO_TASKS.discard(task)
    if not task.cancelled():
        task.exception()  # A disconnected request may no longer await its result.


async def run_ext_data_io(func, *args, **kwargs):
    cancelled = threading.Event()

    def run():
        if not cancelled.is_set():
            return func(*args, **kwargs)

    # Raw asyncio cancellation otherwise releases AnyIO's limiter while its
    # worker is still writing. Keep the limiter owned until the worker finishes;
    # queued cancelled requests skip their work, running imports clean up in situ.
    task = asyncio.create_task(anyio.to_thread.run_sync(run, limiter=_EXT_IO_LIMITER))
    _EXT_IO_TASKS.add(task)
    task.add_done_callback(_ext_io_done)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        cancelled.set()
        raise


# ---------------------------------------------------------------------------
# 配置模型
# ---------------------------------------------------------------------------


class ExtField:
    """扩展字段定义。"""

    __slots__ = ("dtype", "label", "name")

    def __init__(self, name: str, dtype: str = "string", label: str = "") -> None:
        self.name = name
        self.dtype = dtype  # string | int | float | bool
        self.label = label or name

    def to_dict(self) -> dict:
        return {"name": self.name, "dtype": self.dtype, "label": self.label}

    @classmethod
    def from_dict(cls, d: dict) -> ExtField:
        return cls(d["name"], d.get("dtype", "string"), d.get("label", ""))


# 日期参数值的序列化格式 (date_format):
#   iso=YYYY-MM-DD (默认) / compact=YYYYMMDD / ts_s=unix秒 / ts_ms=unix毫秒
# 时间戳约定为该交易日北京时间 00:00:00
PULL_DATE_FORMATS = ("iso", "compact", "ts_s", "ts_ms")

# 每日固定拉取时刻 (schedule_time) 的合法格式: HH:MM (00:00~23:59)
SCHEDULE_TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

# 多盘扩展列选盘时刻 (ext_columns 的 "@HH:MM[:SS]" 后缀) 的合法格式
EXT_SNAPSHOT_AT_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")

# time_field 保留名: 源数据没有时间列时 (人气榜/资金流等每日快照表), 配置它
# 让写入路径注入拉取时刻, 快照表同样获得日内多盘能力 (去重键 [symbol, 拉取时刻])。
# 注入值是北京时间 ISO 串, 与 collapse_ext_rows_at 的时刻切片口径兼容。
PLATE_TS_FIELD = "__pull_ts__"


class PullConfig:
    """定时拉取配置。"""

    __slots__ = (
        "auth",
        "body",
        "date_format",
        "date_param",
        "enabled",
        "fanout",
        "field_map",
        "granularity",
        "headers",
        "last_message",
        "last_rows",
        "last_run",
        "last_status",
        "max_pages",
        "method",
        "next_run",
        "page_param",
        "page_size",
        "page_size_param",
        "page_start",
        "response_path",
        "schedule_day",
        "schedule_minutes",
        "schedule_period",
        "schedule_seconds",
        "schedule_time",
        "schedule_trading_only",
        "since_param",
        "time_field",
        "time_window_end",
        "time_window_start",
        "timeout_seconds",
        "url",
    )

    # ---- __init__ 分段 helper (字段归一化语义等价) ----

    def _init_core(
        self,
        url,
        method,
        headers,
        body,
        response_path,
        field_map,
        schedule_minutes,
        schedule_seconds,
        enabled,
        last_run,
        last_status,
        last_message,
        last_rows,
        next_run,
        fanout,
    ):
        self.fanout = fanout or None  # 两级拉取配方 (见 fanout.py), None=单请求
        self.url = url
        self.method = method  # GET | POST
        self.headers = headers or {}
        self.body = body  # JSON string (POST body template)
        self.response_path = response_path  # dot-path to rows array, e.g. "data.list"
        self.field_map = field_map or {}  # external_name → config_field_name
        self.schedule_minutes = schedule_minutes
        # 间隔秒数: 配置后优先于 schedule_minutes 生效 (秒级拉取, 如集合竞价
        # 30s 快照)。config.json 手改写入非法值时归一 None (fail-closed, 回退
        # 分钟制); 调度侧另有下限钳制兜底。
        self.schedule_seconds = (
            int(schedule_seconds)
            if isinstance(schedule_seconds, int | float) and 10 <= schedule_seconds <= 2_592_000
            else None
        )
        self.enabled = enabled
        self.last_run = last_run
        self.last_status = last_status  # "success" | "error"
        self.last_message = last_message
        self.last_rows = last_rows
        self.next_run = next_run  # 下次预计运行 (ISO, 调度器写入)

    def _init_datetime(
        self, time_window_start, time_window_end, date_param, date_format, time_field, since_param
    ):
        self.time_window_start = time_window_start  # 每日拉取窗口起始 "HH:MM", None=不限
        self.time_window_end = time_window_end  # 每日拉取窗口结束 "HH:MM", None=不限
        # 接口按日期查询的参数名 (如 "date"): 非 None 时请求
        # 带 ?{date_param}={按 date_format 序列化的日期}, 支持历史回补; None = 接口只有当日快照
        self.date_param = date_param
        # 日期参数值的格式; config.json 被手改写入非法值时归一为 iso (fail-closed)
        self.date_format = date_format if date_format in PULL_DATE_FORMATS else "iso"
        # 日内序列表: 响应行里的时间列名 (字段映射后的列名, 如 "ts")。配置后同一
        # symbol 允许多行 (按 [symbol, time_field] 去重), 用于集合竞价等日内多盘
        # 数据; None = 每日快照表 (按 symbol 去重, 默认)
        self.time_field = (time_field or "").strip() or None
        # 增量续传参数名 (如 "since"): 配置后每次拉取自动带上本地目标日分区
        # time_field 列的最大值, 源只返回该值之后的数据 —— 盘中多盘表免全量重拉
        # (缺口感知: 本地缺多少源补多少; 无本地数据时不带参数=全量, 见 ext_pull)。
        # 依赖 time_field (游标值取自该列), 未配置 time_field 时归一 None (失效);
        # PLATE_TS_FIELD 是本地注入的拉取时刻, 源无法按它过滤, 同样失效。
        self.since_param = (
            ((since_param or "").strip() or None)
            if self.time_field and self.time_field != PLATE_TS_FIELD
            else None
        )

    def _init_auth_timeout(self, auth, timeout_seconds):
        # 拉取接口鉴权方式 {"type": "none|bearer|header|query", "header": ..., "param": ...},
        # 与自定义行情源 AuthConfig 同口径; Key 本体存 secrets_store, 不落 config.json
        self.auth = auth
        # 单次拉取请求超时 (秒)。默认 30 与历史硬编码一致; 大响应接口 (如全量
        # 集合竞价 /day ~77s) 可调高。config.json 手改写入非法值时归一 30 (fail-closed)
        self.timeout_seconds = (
            int(timeout_seconds)
            if isinstance(timeout_seconds, int | float) and 5 <= timeout_seconds <= 300
            else 30
        )

    def _init_paging(self, page_param, page_size_param, page_size, page_start, max_pages):
        # ── 分页协议 (仅 GET; 页码经 query 参数传递) ──
        # page_param: 页码参数名 (如 "page"), 配置后按页循环拉取直到空页/短页/上限;
        #             None = 单次请求 (默认, 历史行为)。POST body 分页不支持 (body 是固定模板)
        self.page_param = (page_param or "").strip() or None
        # page_size_param: 每页条数参数名 (如 "pageSize"/"per_page"); 配合 page_size 一起发送
        self.page_size_param = (page_size_param or "").strip() or None
        # page_size: 每页条数值 (>0 且 page_size_param 已配置才发送); 也用于短页判停
        self.page_size = (
            int(page_size) if isinstance(page_size, int | float) and page_size > 0 else 0
        )
        # page_start: 起始页码 (有的接口从 0 计数)
        self.page_start = (
            int(page_start) if isinstance(page_start, int | float) and page_start >= 0 else 1
        )
        # max_pages: 安全上限, 防止接口永远返回数据把拉取循环拖死; 手改非法值归一 20
        self.max_pages = (
            int(max_pages) if isinstance(max_pages, int | float) and 1 <= max_pages <= 200 else 20
        )

    def _init_schedule(
        self, schedule_time, schedule_period, schedule_day, schedule_trading_only, granularity
    ):
        # 每日固定拉取时刻 "HH:MM" (北京时间): 间隔制锁相在启用时刻, 表达不了
        # 「每天 09:30 拉一次」; 配置后调度器按墙钟对齐到该时刻每天拉一次,
        # schedule_minutes 被忽略。config.json 手改写入非法值时归一 None
        # (fail-closed, 回退间隔制)。
        self.schedule_time = (
            schedule_time
            if isinstance(schedule_time, str) and SCHEDULE_TIME_RE.match(schedule_time)
            else None
        )
        # 固定时刻的重复周期 (仅 schedule_time 配置时生效): daily=每天 |
        # weekly=每周 (schedule_day 1=周一..7=周日) | monthly=每月 (schedule_day
        # 1..31, 短月无该日则该月跳过)。手改非法值归一 daily (fail-closed)。
        self.schedule_period = (
            schedule_period if schedule_period in ("daily", "weekly", "monthly") else "daily"
        )
        # weekly: ISO 周几 (1=周一..7=周日); monthly: 几号 (1..31)。
        # 与 period 的合法交由调度侧解释 (weekly 配 8..31 视为 1=周一)。越界归一 1
        self.schedule_day = (
            int(schedule_day)
            if isinstance(schedule_day, int | float) and 1 <= schedule_day <= 31
            else 1
        )
        # 仅交易日: 跳过周六日。法定节假日无法离线判定, 仍会按期尝试 —
        # 拉取幂等且失败仅记 last_status, 不影响后续调度
        self.schedule_trading_only = bool(schedule_trading_only)
        # 序列粒度声明: auto=按数据形态自动判断 (api 层 _granularity_effective) |
        # intraday=日内多盘 (当日逐盘序列) | daily=日级别 (近 N 日每日一值)。
        # 决定 series 端点口径与列表迷你图形态; 手改非法值归一 auto (fail-closed)。
        self.granularity = granularity if granularity in ("auto", "intraday", "daily") else "auto"

    def __init__(
        self,
        url: str = "",
        method: str = "GET",
        headers: dict[str, str] | None = None,
        body: str | None = None,
        response_path: str = "",
        field_map: dict[str, str] | None = None,
        schedule_minutes: int = 1440,
        schedule_seconds: int | None = None,
        enabled: bool = False,
        last_run: str | None = None,
        last_status: str | None = None,
        last_message: str | None = None,
        last_rows: int | None = None,
        next_run: str | None = None,
        time_window_start: str | None = None,
        time_window_end: str | None = None,
        date_param: str | None = None,
        date_format: str = "iso",
        time_field: str | None = None,
        auth: dict | None = None,
        timeout_seconds: int = 30,
        page_param: str | None = None,
        page_size_param: str | None = None,
        page_size: int = 0,
        page_start: int = 1,
        max_pages: int = 20,
        fanout: dict | None = None,
        schedule_time: str | None = None,
        schedule_period: str | None = None,
        schedule_day: int | None = None,
        schedule_trading_only: bool = False,
        since_param: str | None = None,
        granularity: str = "auto",
    ) -> None:
        self._init_core(
            url,
            method,
            headers,
            body,
            response_path,
            field_map,
            schedule_minutes,
            schedule_seconds,
            enabled,
            last_run,
            last_status,
            last_message,
            last_rows,
            next_run,
            fanout,
        )
        self._init_datetime(
            time_window_start, time_window_end, date_param, date_format, time_field, since_param
        )
        self._init_auth_timeout(auth, timeout_seconds)
        self._init_paging(page_param, page_size_param, page_size, page_start, max_pages)
        self._init_schedule(
            schedule_time, schedule_period, schedule_day, schedule_trading_only, granularity
        )

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "method": self.method,
            "headers": self.headers,
            "body": self.body,
            "response_path": self.response_path,
            "field_map": self.field_map,
            "schedule_minutes": self.schedule_minutes,
            "schedule_seconds": self.schedule_seconds,
            "enabled": self.enabled,
            "last_run": self.last_run,
            "last_status": self.last_status,
            "last_message": self.last_message,
            "last_rows": self.last_rows,
            "next_run": self.next_run,
            "time_window_start": self.time_window_start,
            "time_window_end": self.time_window_end,
            "date_param": self.date_param,
            "date_format": self.date_format,
            "time_field": self.time_field,
            "since_param": self.since_param,
            "granularity": self.granularity,
            "auth": self.auth,
            "timeout_seconds": self.timeout_seconds,
            "page_param": self.page_param,
            "page_size_param": self.page_size_param,
            "page_size": self.page_size,
            "page_start": self.page_start,
            "max_pages": self.max_pages,
            "fanout": self.fanout,
            "schedule_time": self.schedule_time,
            "schedule_period": self.schedule_period,
            "schedule_day": self.schedule_day,
            "schedule_trading_only": self.schedule_trading_only,
        }

    @classmethod
    def from_dict(cls, d: dict) -> PullConfig:
        if not d:
            return cls()
        return cls(
            url=d.get("url", ""),
            method=d.get("method", "GET"),
            headers=d.get("headers"),
            body=d.get("body"),
            response_path=d.get("response_path", ""),
            field_map=d.get("field_map"),
            schedule_minutes=d.get("schedule_minutes", 1440),
            schedule_seconds=d.get("schedule_seconds"),
            enabled=d.get("enabled", False),
            last_run=d.get("last_run"),
            last_status=d.get("last_status"),
            last_message=d.get("last_message"),
            last_rows=d.get("last_rows"),
            next_run=d.get("next_run"),
            time_window_start=d.get("time_window_start"),
            time_window_end=d.get("time_window_end"),
            date_param=d.get("date_param"),
            date_format=d.get("date_format", "iso"),
            time_field=d.get("time_field"),
            since_param=d.get("since_param"),
            granularity=d.get("granularity", "auto"),
            auth=d.get("auth"),
            timeout_seconds=d.get("timeout_seconds", 30),
            page_param=d.get("page_param"),
            page_size_param=d.get("page_size_param"),
            page_size=d.get("page_size", 0),
            page_start=d.get("page_start", 1),
            max_pages=d.get("max_pages", 20),
            fanout=d.get("fanout"),
            schedule_time=d.get("schedule_time"),
            schedule_period=d.get("schedule_period"),
            schedule_day=d.get("schedule_day"),
            schedule_trading_only=d.get("schedule_trading_only", False),
        )


def ext_api_key_field(config_id: str) -> str:
    """扩展数据拉取 API Key 在 secrets.json 中的字段名。"""
    return f"ext_{config_id}_api_key"


def get_ext_api_key(config_id: str) -> str:
    """取扩展数据拉取接口的 API Key: secrets.json 优先, 环境变量 EXT_{ID}_API_KEY 兜底。"""
    from app import secrets_store

    return secrets_store.get_env_backed_secret(
        ext_api_key_field(config_id), f"EXT_{config_id.upper()}_API_KEY"
    )


class ExtConfig:
    """一个扩展数据源的完整配置。"""

    __slots__ = (
        "code_map",
        "created_at",
        "description",
        "fields",
        "id",
        "label",
        "market_level",
        "mode",
        "pull",
        "symbol_map",
        "updated_at",
    )

    def __init__(
        self,
        id: str,
        label: str,
        mode: Literal["snapshot", "timeseries"],
        fields: list[ExtField],
        description: str = "",
        symbol_map: dict | None = None,
        code_map: dict | None = None,
        created_at: str | None = None,
        updated_at: str | None = None,
        pull: PullConfig | None = None,
        market_level: bool = False,
    ) -> None:
        self.id = id
        self.label = label
        self.mode = mode
        self.fields = fields
        self.description = description
        # 映射关系: {"type": "mapped", "col": "原始列名"} 或 {"type": "computed", "from": "symbol|code", "method": "strip_exchange|append_exchange"}
        self.symbol_map = symbol_map or {}
        self.code_map = code_map or {}
        # 市场级表: 行 = 全市场每日一条 (无 symbol 列), 如市场环境/情绪指数/择时
        # 状态序列。上传/写入/拉取跳过标的关联强制; 供 /rows API 与未来的
        # "市场级过滤数据集" 契约消费 (策略 JOIN 面向 symbol, 不适用此类表)。
        self.market_level = bool(market_level)
        self.created_at = created_at or datetime.now().isoformat()
        self.updated_at = updated_at or datetime.now().isoformat()
        self.pull = pull

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "label": self.label,
            "mode": self.mode,
            "fields": [f.to_dict() for f in self.fields],
            "description": self.description,
            "symbol_map": self.symbol_map,
            "code_map": self.code_map,
            "market_level": self.market_level,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        if self.pull:
            d["pull"] = self.pull.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> ExtConfig:
        return cls(
            id=d["id"],
            label=d["label"],
            mode=d["mode"],
            fields=[ExtField.from_dict(f) for f in d.get("fields", [])],
            description=d.get("description", ""),
            symbol_map=d.get("symbol_map"),
            code_map=d.get("code_map"),
            created_at=d.get("created_at"),
            updated_at=d.get("updated_at"),
            pull=PullConfig.from_dict(d["pull"]) if d.get("pull") else None,
            market_level=bool(d.get("market_level", False)),
        )


# ---------------------------------------------------------------------------
# 配置持久化
# ---------------------------------------------------------------------------

# load_all 进程内缓存: kline/screener/watchlist 等热路径每请求调用, 每次都
# iterdir + 逐 config.json read_text+parse 纯重复; 以配置目录的
# (目录名, mtime_ns, size, inode) 签名失效 (新增/编辑/删除配置都会改变签名)。
# 带 inode 是因为文件时间戳取自粗粒度时钟: 同一 tick 内「改配置」(大小不变)
# 的 mtime 不变, 而配置写入走「临时文件 + 替换」, inode 必变 → 缓存必然失效。
_load_all_cache: dict[str, tuple[tuple, list[ExtConfig]]] = {}


def _ext_config_dir_signature(base: Path) -> tuple | None:
    """配置目录下所有 config.json 的 (目录名, mtime_ns, size, inode) 签名; 出错返回 None (禁用缓存)。"""
    try:
        sig = []
        for d in sorted(base.iterdir()):
            cp = d / "config.json"
            if d.is_dir() and cp.exists():
                st = cp.stat()
                sig.append((d.name, st.st_mtime_ns, st.st_size, st.st_ino))
        return tuple(sig)
    except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
        return None


class ExtConfigStore:
    """扩展数据配置文件读写 — 每个表独立目录 data/ext/{config_id}/config.json。"""

    # 与创建端点 CreateExtReq.id 的 pattern 一致; load_all 之外的 config_id
    # 来自 URL path 参数, 必须先过白名单再拼路径, 防止 ../ 穿越删除。
    _VALID_ID = re.compile(r"^[a-zA-Z0-9_]+$")

    def __init__(self, data_dir: Path) -> None:
        self._base = data_dir / "ext_data"

    def _config_path(self, config_id: str) -> Path:
        if not self._VALID_ID.match(config_id):
            raise ValueError(f"非法 config_id: {config_id!r}")
        return self._base / config_id / "config.json"

    def load_all(self) -> list[ExtConfig]:
        # 兼容旧版: 如果目录为空且旧配置文件存在则迁移
        sig = _ext_config_dir_signature(self._base)
        if sig is not None:
            cached = _load_all_cache.get(str(self._base))
            if cached is not None and cached[0] == sig:
                return copy.deepcopy(cached[1])
        if not self._base.exists() or not any(self._base.iterdir()):
            old = self._base.parent / "ext_configs.json"
            if not old.exists():
                old = self._base.parent / "ext_configs.json.bak"
            if old.exists():
                self._migrate_legacy(old)
        if not self._base.exists():
            return []
        configs = []
        for d in sorted(self._base.iterdir()):
            cp = d / "config.json"
            if d.is_dir() and cp.exists():
                try:
                    raw = json.loads(cp.read_text(encoding="utf-8"))
                    configs.append(ExtConfig.from_dict(raw))
                except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
                    logger.warning("扩展表配置解析失败 %s: %s", cp, e)
        if sig is not None and configs:
            # 缓存存私有副本, 命中时返回深拷贝, 调用方改配置对象不会污染缓存。
            _load_all_cache[str(self._base)] = (sig, copy.deepcopy(configs))
        return configs

    def get(self, config_id: str) -> ExtConfig | None:
        try:
            cp = self._config_path(config_id)
        except ValueError:
            return None
        if not cp.exists():
            return None
        try:
            raw = json.loads(cp.read_text(encoding="utf-8"))
            return ExtConfig.from_dict(raw)
        except Exception:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            return None

    def create(self, config: ExtConfig) -> None:
        """Reserve a new table directory before publication; never replace data."""
        directory = self._config_path(config.id).parent
        directory.parent.mkdir(parents=True, exist_ok=True)
        directory.mkdir(exist_ok=False)
        try:
            self.upsert(config)
        except BaseException:
            # Clean up our unpublished temporary file so a failed import can be
            # retried, while retaining any committed configuration or data.
            path = self._config_path(config.id)
            if not path.exists():
                with suppress(OSError):
                    path.with_suffix(path.suffix + ".tmp").unlink(missing_ok=True)
                with suppress(OSError):
                    directory.rmdir()
            raise

    def upsert(self, config: ExtConfig, *, keep_strategy_cache: bool = False) -> None:
        config.updated_at = datetime.now().isoformat()
        cp = self._config_path(config.id)
        cp.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            cp,
            json.dumps(config.to_dict(), ensure_ascii=False, indent=2),
        )
        # 字段集/模式变化会改变扩展列集合: 失效扩展帧缓存与策略结果缓存。
        # 定时拉取循环的 last_run/next_run 例行回写传 keep_strategy_cache=True,
        # 否则每轮拉取后策略页缓存被状态回写清空 (数据写入链路已另行放行)。
        _invalidate_ext_derived(self._base.parent, keep_strategy_cache=keep_strategy_cache)

    def delete(self, config_id: str) -> bool:
        import shutil

        try:
            cp = self._config_path(config_id)
        except ValueError:
            return False
        if not cp.exists():
            return False
        shutil.rmtree(cp.parent, ignore_errors=True)
        _invalidate_ext_derived(self._base.parent)
        return True

    def _migrate_legacy(self, old_path: Path) -> None:
        """一次性迁移旧版 ext_configs.json 到独立目录结构。"""
        try:
            raw = json.loads(old_path.read_text(encoding="utf-8"))
            configs = [ExtConfig.from_dict(d) for d in raw]
            for c in configs:
                cp = self._config_path(c.id)
                cp.parent.mkdir(parents=True, exist_ok=True)
                atomic_write_text(
                    cp,
                    json.dumps(c.to_dict(), ensure_ascii=False, indent=2),
                )
            # 迁移完成后重命名旧文件作为备份
            backup = old_path.with_suffix(".json.bak")
            old_path.rename(backup)
            logger.info("ext_configs.json 已迁移至 ext/ (备份: %s)", backup.name)
        except Exception as e:  # noqa: BLE001 - BLE001: 外部IO/解析失败需降级, 非静默吞异常
            logger.warning("ext_configs 迁移失败: %s", e)


# ---------------------------------------------------------------------------
# CSV / Excel 解析 → Parquet 写入
# ---------------------------------------------------------------------------

_POLARS_DTYPE_MAP = {
    "string": pl.Utf8,
    "int": pl.Int64,
    "float": pl.Float64,
    "bool": pl.Boolean,
}

_POLARS_TYPE_MAP = {
    "Int64": "int",
    "Int32": "int",
    "Int16": "int",
    "Int8": "int",
    "UInt64": "int",
    "UInt32": "int",
    "UInt16": "int",
    "UInt8": "int",
    "Float64": "float",
    "Float32": "float",
    "Boolean": "bool",
    "Utf8": "string",
    "String": "string",
    "Date": "string",
    "Datetime": "string",
    "Duration": "string",
    "Categorical": "string",
}

_CODE_PAT = re.compile(r"^\d{6}$")
_SYMBOL_PAT = re.compile(r"^\d{6}\.[A-Z]{2}$")


# 编码识别与转换的分块大小，与 ext_data 上传写入用的块大小一致。
_TRANSCODE_CHUNK_BYTES = 1024 * 1024
