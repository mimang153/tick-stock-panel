"""用户状态 PostgreSQL 门面（DES-006）。

用有界连接队列提供线程安全的连接复用；业务层只通过本模块执行参数化 SQL。
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg

from app.config import settings

logger = logging.getLogger(__name__)
_LOCK = threading.Lock()
_POOL: queue.Queue[psycopg.Connection[Any]] | None = None
_MAX_CONNECTIONS = 8


def init_state_pg() -> None:
    """初始化连接池并建表；重复调用安全。"""
    global _POOL
    with _LOCK:
        if _POOL is not None:
            return
        pool: queue.Queue[psycopg.Connection[Any]] = queue.Queue(maxsize=_MAX_CONNECTIONS)
        for _ in range(2):
            pool.put(psycopg.connect(settings.user_state_pg_url, autocommit=True))
        _POOL = pool
    from app.state.schema import ensure_user_state_schema

    ensure_user_state_schema()
    logger.info("user state PG ready: schema=%s", settings.user_state_pg_schema)


def close_state_pg() -> None:
    """关闭并清空连接池；重复调用安全。"""
    global _POOL
    with _LOCK:
        pool, _POOL = _POOL, None
    if pool is None:
        return
    while not pool.empty():
        try:
            pool.get_nowait().close()
        except queue.Empty:
            break


def is_ready() -> bool:
    """返回用户状态 PG 是否已初始化；供模块导入期只读门面安全降级。"""
    return _POOL is not None


def _pool() -> queue.Queue[psycopg.Connection[Any]]:
    """返回已初始化连接池；未初始化时抛 RuntimeError。"""
    if _POOL is None:
        raise RuntimeError("user state PG is not initialized")
    return _POOL


@contextmanager
def connection() -> Iterator[psycopg.Connection[Any]]:
    """借出一条连接并在用完后归还；异常不污染连接。"""
    pool = _pool()
    conn = pool.get()
    try:
        yield conn
    finally:
        pool.put(conn)


@contextmanager
def transaction() -> Iterator[psycopg.Connection[Any]]:
    """在单条连接上开启显式事务，成功提交、异常回滚。"""
    with connection() as conn:
        conn.execute("BEGIN")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")


def execute(sql: str, params: tuple[object, ...] = ()) -> int:
    """执行参数化写语句并返回受影响行数。"""
    with connection() as conn:
        result = conn.execute(sql, params)
        return int(result.rowcount)


def fetch_one(sql: str, params: tuple[object, ...] = ()) -> dict[str, Any] | None:
    """执行参数化查询并返回首行 dict；无结果返回 None。"""
    with connection() as conn:
        result = conn.execute(sql, params)
        row = result.fetchone()
        if row is None:
            return None
        columns = [column.name for column in result.description or []]
        return dict(zip(columns, row, strict=False))


def fetch_all(sql: str, params: tuple[object, ...] = ()) -> list[dict[str, Any]]:
    """执行参数化查询并返回全部 dict 行。"""
    with connection() as conn:
        result = conn.execute(sql, params)
        columns = [column.name for column in result.description or []]
        return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]
