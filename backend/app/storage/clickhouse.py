"""ClickHouse 连接封装。

设计约束（../../../docs/v1.0.1/design/DES-001-clickhouse-storage-design.md）：

- 业务代码只依赖 storage 门面，不直接持有连接、不直接拼 SQL（§2）
- 时区契约：库里统一 ``DateTime64(*, 'Asia/Shanghai')``，进程内 Polars 一律是
  北京墙钟 naive datetime；转换只发生在本模块的 Arrow 边界，业务层不做二次换算（§5）
- 启动期不可达必须 fail-closed，不允许进入"半可用"状态（§4.12）

Arrow 边界为什么不能省：Arrow 的 timestamp 是 UTC 基准，naive 值直接写进带时区的
列会被当作 UTC 而整体偏移 8 小时；反过来读回的带时区列也必须按北京墙钟还原成 naive，
否则上层每个消费方都要各自处理时区。两处转换在这里收口。
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

from app.config import settings

logger = logging.getLogger(__name__)

BEIJING_TZ = "Asia/Shanghai"

# 旧版 CK(< 25) 的 Arrow 输出把 Date 编成 UInt16(自 1970 的天数)、DateTime 编成 UInt32(秒)，
# 读端拿不到原始列类型也没有开关可关。用一次轻量探测确认服务端行为，再按"本项目 schema
# 不使用 UInt16 列"的约定把 uint16 还原成 date32（守卫见 tests/test_ch_schema.py）。
# 25+ 起服务端直接输出 date32，探测为 False，不做任何额外处理。
_LEGACY_DATE_PROBE_SQL = "SELECT toDate('1970-01-02') AS d"


def _restore_legacy_date_columns(table: pa.Table) -> pa.Table:
    """把旧版 CK 编成 UInt16 的 Date 列还原为 date32（位等价，按天解释）。"""
    for idx, field in enumerate(table.schema):
        if not pa.types.is_uint16(field.type):
            continue
        column = pc.cast(pc.cast(table.column(idx), pa.int32()), pa.date32())
        table = table.set_column(
            idx,
            pa.field(field.name, pa.date32(), field.nullable, field.metadata),
            column,
        )
    return table


# 启动期连接重试节奏：最多 60s，每 2s 一次（§4.12）
CONNECT_RETRY_TOTAL_S = 60.0
CONNECT_RETRY_INTERVAL_S = 2.0


class ClickHouseUnavailableError(RuntimeError):
    """启动期连不上 ClickHouse。调用方应中止启动，而不是降级继续。"""


def parse_endpoint(url: str) -> tuple[str, int, bool]:
    """HTTP 端点 → (host, port, secure)。

    只接受 http/https：CK 原生 TCP 端口需要另一套客户端，不在本期范围内。
    """
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ValueError(f"CLICKHOUSE_URL 需形如 http://host:8123，当前为 {url!r}")
    secure = parts.scheme == "https"
    return parts.hostname, parts.port or (8443 if secure else 8123), secure


def to_arrow(df: pl.DataFrame) -> pa.Table:
    """Polars → Arrow，把 datetime 统一标注为北京时区。"""
    exprs = []
    for name, dtype in df.schema.items():
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone == BEIJING_TZ:
            continue
        column = pl.col(name)
        # naive 值按既有口径解释为北京墙钟(补时区)；已带时区的按瞬时值换算到北京。
        exprs.append(
            column.dt.replace_time_zone(BEIJING_TZ)
            if dtype.time_zone is None
            else column.dt.convert_time_zone(BEIJING_TZ)
        )
    return (df.with_columns(exprs) if exprs else df).to_arrow()


def from_arrow(table: pa.Table) -> pl.DataFrame:
    """Arrow → Polars，带时区的 datetime 还原成北京墙钟 naive。"""
    df = pl.from_arrow(table)
    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"Arrow 结果应为表格，实际为 {type(table)!r}")
    exprs = [
        pl.col(name).dt.convert_time_zone(BEIJING_TZ).dt.replace_time_zone(None)
        for name, dtype in df.schema.items()
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None
    ]
    return df.with_columns(exprs) if exprs else df


class ClickHouseClient:
    """单节点 CK 的轻量封装：懒连接 + 线程安全 + 类型边界收口。

    ``table_prefix`` 只服务于测试隔离（§4.7）：业务调用方永远用逻辑表名。
    ``table`` 收口了前缀拼接，验收文档 §3.2 要求"业务代码不得自行拼表名"。
    """

    def __init__(
        self,
        *,
        url: str,
        database: str,
        user: str,
        password: str = "",
        query_timeout_s: int = 60,
        table_prefix: str = "",
    ) -> None:
        self.url = url
        self.database = database
        self.user = user
        self.query_timeout_s = query_timeout_s
        self.table_prefix = table_prefix
        self._password = password
        self._client: Any = None
        self._lock = threading.Lock()
        # None=尚未探测；True=服务端 Arrow 输出把 Date 编成 UInt16（旧版 CK）
        self._legacy_arrow_date: bool | None = None

    @classmethod
    def from_settings(
        cls,
        *,
        database: str | None = None,
        table_prefix: str = "",
    ) -> ClickHouseClient:
        """按应用配置构造（调用方只需说明是否换库/加前缀）。"""
        return cls(
            url=settings.clickhouse_url,
            database=database or settings.clickhouse_database,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            query_timeout_s=settings.ch_query_timeout_s,
            table_prefix=table_prefix,
        )

    # ── 生命周期 ────────────────────────────────────────────────
    def connect(self, *, retry_total_s: float = CONNECT_RETRY_TOTAL_S) -> None:
        """建立连接并按需重试；超时抛 ClickHouseUnavailableError。"""
        deadline = time.monotonic() + max(retry_total_s, 0.0)
        attempt = 0
        while True:
            attempt += 1
            try:
                client = self._build_client()
                client.command("SELECT 1")
            except Exception as exc:  # 逐次重试，最终统一抛出
                if time.monotonic() >= deadline:
                    raise ClickHouseUnavailableError(
                        f"连接 ClickHouse 失败（{self.url}, database={self.database}, "
                        f"user={self.user}）：{exc}"
                    ) from exc
                logger.warning(
                    "ClickHouse 连接失败（第 %d 次），%.0fs 后重试：%s",
                    attempt,
                    CONNECT_RETRY_INTERVAL_S,
                    exc,
                )
                time.sleep(CONNECT_RETRY_INTERVAL_S)
                continue
            self._client = client
            logger.info("ClickHouse 已连接 %s/%s（user=%s）", self.url, self.database, self.user)
            return

    def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            client.close()

    def ping(self) -> bool:
        """健康检查；失败返回 False 而不是抛异常（/api/health 需要）。"""
        try:
            return bool(self._ensure_client().ping())
        except Exception as exc:  # noqa: BLE001 - 健康检查只用布尔结果
            logger.warning("ClickHouse ping 失败：%s", exc)
            return False

    # ── 表名 ────────────────────────────────────────────────────
    def table(self, name: str) -> str:
        """逻辑表名 → 物理表名（测试前缀在这里统一拼接）。"""
        return f"{self.table_prefix}{name}"

    # ── 读写 ────────────────────────────────────────────────────
    def execute(self, sql: str, parameters: Any = None, *, settings: dict | None = None) -> None:
        """执行 DDL / 写入类语句。"""
        self._ensure_client().command(sql, parameters=parameters, settings=settings)

    def query(
        self, sql: str, parameters: Any = None, *, settings: dict | None = None
    ) -> pl.DataFrame:
        """执行查询，返回带北京墙钟 naive datetime 的 Polars DataFrame。"""
        query_settings: dict[str, Any] = {"max_execution_time": self.query_timeout_s}
        if settings:
            query_settings.update(settings)
        table = self._ensure_client().query_arrow(
            sql, parameters=parameters, settings=query_settings, use_strings=True
        )
        if self._uses_legacy_arrow_date():
            table = _restore_legacy_date_columns(table)
        return from_arrow(table)

    def insert(self, table: str, df: pl.DataFrame) -> None:
        """批量写入。空表直接跳过，避免为 0 行发一次请求。"""
        if df.is_empty():
            return
        self._ensure_client().insert_arrow(table, to_arrow(df))

    def insert_rows(
        self,
        table: str,
        rows: Sequence[Sequence],
        *,
        column_names: Sequence[str] | None = None,
    ) -> None:
        """按行写入。

        ``Map(String, ...)`` 在 Arrow 里没有对应类型（读出来是 list<struct>），
        只能用原生协议写 Python dict，所以这条路绕开 Arrow。
        """
        if not rows:
            return
        self._ensure_client().insert(table, rows, column_names=column_names)

    # ── 内部 ────────────────────────────────────────────────────
    def _uses_legacy_arrow_date(self) -> bool:
        """探测一次服务端 Arrow 的 Date 编码（True=旧版 CK 的 UInt16）。

        探测失败时按"不需要还原"处理：真有问题会在首条查询的 dtype 上暴露，
        不影响连接与启动（fail-closed 只针对连不上）。
        """
        if self._legacy_arrow_date is not None:
            return self._legacy_arrow_date
        try:
            table = self._ensure_client().query_arrow(_LEGACY_DATE_PROBE_SQL)
            legacy = table.num_columns == 1 and pa.types.is_uint16(table.schema.field(0).type)
        except Exception as exc:  # noqa: BLE001 - 探测失败不阻断查询
            logger.warning("ClickHouse Arrow Date 编码探测失败，按新版处理：%s", exc)
            legacy = False
        if legacy:
            logger.info(
                "ClickHouse Arrow 输出把 Date 编成 UInt16（旧版本服务端），读端按 date32 还原"
            )
        self._legacy_arrow_date = legacy
        return legacy

    def _ensure_client(self) -> Any:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self.connect()
        return self._client

    def _build_client(self) -> Any:
        import clickhouse_connect

        host, port, secure = parse_endpoint(self.url)
        return clickhouse_connect.get_client(
            host=host,
            port=port,
            interface="https" if secure else "http",
            secure=secure,
            username=self.user,
            password=self._password,
            database=self.database,
            connect_timeout=10,
            send_receive_timeout=self.query_timeout_s,
            # 不绑定 session：clickhouse_connect 同一 client 在 session 模式下并发查询
            # 会抛 "concurrent queries within the same session"（FastAPI 多线程共享
            # 本 client，页面并发拉数会稳定 500，见 S8）。关闭后每次查询独立、可并发；
            # session_timezone 作为 settings 随每次请求下发，时区口径不变（§5）。
            autogenerate_session_id=False,
            # 会话时区固定为北京：宿主/容器 TZ 不参与业务口径（§5）。
            settings={"session_timezone": BEIJING_TZ},
        )
