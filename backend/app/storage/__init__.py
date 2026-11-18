"""存储层门面 — 业务代码访问 ClickHouse 的唯一入口。

设计见 ../../../docs/v1.0.1/design/DES-001-clickhouse-storage-design.md §2：业务层只依赖本包，不直接持有
CK 连接、不直接拼 SQL；表名一律经 ``ClickHouseClient.table()`` 生成。
"""

from __future__ import annotations

from app.storage.cache import CacheBackend, MemoryCacheBackend
from app.storage.clickhouse import (
    BEIJING_TZ,
    ClickHouseClient,
    ClickHouseUnavailableError,
)
from app.storage.protocol import (
    ADJ_FACTOR_TABLES,
    DAILY_TABLES,
    FINANCIAL_TABLES,
    INSTRUMENT_TABLES,
    MINUTE_TABLES,
    DedupeStrategy,
    StorageProtocol,
)
from app.storage.runtime import (
    StorageRuntime,
    get_storage,
    init_storage,
    require_storage,
    shutdown_storage,
    storage_health,
)
from app.storage.schema import (
    SCHEMA_VERSION,
    TABLE_NAMES,
    TABLE_READ_COLUMNS,
    build_statements,
    ensure_schema,
)
from app.storage.store import ClickHouseStore
from app.storage.sync_state import SyncState, SyncStateStore
from app.storage.writer import BufferedWriter

__all__ = [
    "ADJ_FACTOR_TABLES",
    "BEIJING_TZ",
    "DAILY_TABLES",
    "FINANCIAL_TABLES",
    "INSTRUMENT_TABLES",
    "MINUTE_TABLES",
    "SCHEMA_VERSION",
    "TABLE_NAMES",
    "TABLE_READ_COLUMNS",
    "BufferedWriter",
    "CacheBackend",
    "ClickHouseClient",
    "ClickHouseStore",
    "ClickHouseUnavailableError",
    "DedupeStrategy",
    "MemoryCacheBackend",
    "StorageProtocol",
    "StorageRuntime",
    "SyncState",
    "SyncStateStore",
    "build_statements",
    "ensure_schema",
    "get_storage",
    "init_storage",
    "require_storage",
    "shutdown_storage",
    "storage_health",
]
