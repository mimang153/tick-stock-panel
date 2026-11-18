"""pytest 全局配置。

测试策略（B）：所有用例都连真实 ClickHouse，连不上直接失败，不静默 skip。
为避免污染运行库，测试统一落在 ``tsp_test``；可用 ``CLICKHOUSE_TEST_DATABASE`` 覆盖。

必须在任何 ``app.*`` 导入之前设置环境变量：``app.config.settings`` 是模块级单例，
一旦被导入就固定了库名。
"""

from __future__ import annotations

import os

os.environ.setdefault(
    "CLICKHOUSE_DATABASE",
    os.environ.get("CLICKHOUSE_TEST_DATABASE", "tsp_test"),
)

import uuid
from collections.abc import Iterator

import pytest

from app.config import settings

# 标的维表相关的三张表；测试只需要这一组时用它，避免每次建 26 张表
_INSTRUMENT_TABLES = ("instruments", "instruments_index", "instruments_etf")


@pytest.fixture()
def ck_store():
    """带随机前缀的 CK 门面（只建标的维表三张表），用例结束即 drop。"""
    from app.config import settings
    from app.storage.clickhouse import ClickHouseClient
    from app.storage.schema import ensure_schema
    from app.storage.store import ClickHouseStore

    client = ClickHouseClient(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        table_prefix=f"t_{uuid.uuid4().hex[:8]}_",
    )
    client.connect(retry_total_s=5)
    ensure_schema(client, tables=_INSTRUMENT_TABLES)
    try:
        yield ClickHouseStore(client)
    finally:
        for name in (*_INSTRUMENT_TABLES, "schema_version"):
            client.execute(f"DROP TABLE IF EXISTS {client.table(name)}")
        client.close()


@pytest.fixture()
def ck_runtime() -> Iterator[object]:
    """把全局存储 runtime 指向带随机前缀的 CK 表，用例结束即回收。

    供依赖 ``get_storage()``/``require_storage()`` 的服务层读路径使用（验收文档
    §14.1 D 类迁移：fixture 换成 CK，断言逻辑不变）。入口先回收可能被其它
    用例（真实 app lifespan）泄漏的全局 runtime，避免读到无关前缀的数据；
    结束时恢复进入前的 runtime 状态（存在泄漏则原样恢复），不改变后续用例
    所见的全局状态。
    """
    from app.storage import runtime as storage_runtime
    from app.storage import schema as ck_schema
    from app.storage.clickhouse import ClickHouseClient
    from app.storage.schema import ensure_schema

    prev_runtime = storage_runtime.get_storage()
    prev_client = prev_runtime.store._client if prev_runtime is not None else None
    storage_runtime.shutdown_storage()
    client = ClickHouseClient(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        query_timeout_s=30,
        table_prefix=f"t_{uuid.uuid4().hex[:8]}_",
    )
    client.connect(retry_total_s=5)
    ensure_schema(client)
    storage_runtime.init_storage(client=client)
    try:
        yield storage_runtime.get_storage().store  # type: ignore[union-attr]
    finally:
        storage_runtime.shutdown_storage()
        if prev_client is not None:
            # 恢复泄漏状态, 保持与本文件引入前一致的套件行为
            try:
                storage_runtime.init_storage(client=prev_client)
            except Exception as exc:  # noqa: BLE001 - 恢复失败不盖住测试结论
                print(f"恢复全局 runtime 失败: {exc}")
        for name in ck_schema.TABLE_NAMES:
            try:
                client.execute(f"DROP TABLE IF EXISTS {client.table(name)}")
            except Exception as exc:  # noqa: BLE001 - 清理失败不盖住测试结论
                print(f"清理表 {name} 失败: {exc}")
        client.close()
