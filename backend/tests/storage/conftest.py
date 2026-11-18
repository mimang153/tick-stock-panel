"""真实 ClickHouse 的存储层 fixture（设计文档 §4.7 / 验收文档 §3.2）。

宿主 CK 与其它项目共用（存在 stock / zvt_* 等库），所以这里的硬约束是：

- 只在 ``tsp_test`` 内建表/删表，表名一律带随机前缀
- 只 drop 本次前缀的表，绝不 ``DROP DATABASE``、不做 ``SYSTEM`` 全局操作
- 连不上 CK 时直接失败（策略 B，不允许静默跳过）

连接参数（默认对齐设计文档 §4.8）：

    CLICKHOUSE_TEST_URL      http://localhost:8123
    CLICKHOUSE_TEST_DATABASE tsp_test
    CLICKHOUSE_TEST_USER     tsp
    CLICKHOUSE_TEST_PASSWORD (取自 .env)
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Iterator

import pytest

from app.config import settings
from app.storage import schema
from app.storage.clickhouse import ClickHouseClient, ClickHouseUnavailableError
from app.storage.store import ClickHouseStore

logger = logging.getLogger(__name__)

# 默认值取自应用配置（.env），保证"测试连的库/账号"和"应用连的库/账号"是同一套；
# 库名由 tests/conftest.py 强制指向 tsp_test，这里只是兜底。
TEST_URL = os.environ.get("CLICKHOUSE_TEST_URL", settings.clickhouse_url)
TEST_DATABASE = os.environ.get("CLICKHOUSE_TEST_DATABASE", settings.clickhouse_database)
TEST_USER = os.environ.get("CLICKHOUSE_TEST_USER", settings.clickhouse_user)
TEST_PASSWORD = os.environ.get("CLICKHOUSE_TEST_PASSWORD", settings.clickhouse_password)


def _drop_tables(client: ClickHouseClient) -> None:
    """清理本次前缀建的表；失败只记录，不掩盖测试结论。"""
    for name in schema.TABLE_NAMES:
        try:
            client.execute(f"DROP TABLE IF EXISTS {client.table(name)}")
        except Exception as exc:  # noqa: BLE001 - 清理失败不应盖住测试失败
            logger.warning("清理表 %s 失败：%s", client.table(name), exc)


@pytest.fixture()
def ch() -> Iterator[ClickHouseClient]:
    """带前缀的原始客户端：建表、清理、断言 SQL 都走它。"""
    client = ClickHouseClient(
        url=TEST_URL,
        database=TEST_DATABASE,
        user=TEST_USER,
        password=TEST_PASSWORD,
        query_timeout_s=30,
        table_prefix=f"t_{uuid.uuid4().hex[:8]}_",
    )
    try:
        client.connect(retry_total_s=0)
    except ClickHouseUnavailableError as exc:
        # 策略 B：不允许静默跳过，连不上就是失败
        pytest.fail(f"ClickHouse 不可用（{TEST_URL}/{TEST_DATABASE}）：{exc}")
    try:
        schema.ensure_schema(client)
        yield client
    finally:
        _drop_tables(client)
        client.close()


@pytest.fixture()
def store(ch: ClickHouseClient) -> ClickHouseStore:
    """仓储门面：业务与契约测试只用它，不碰 SQL。"""
    return ClickHouseStore(ch)
