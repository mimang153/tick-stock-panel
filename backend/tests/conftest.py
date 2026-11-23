"""pytest 全局配置。

测试策略（B）：所有用例都连真实 ClickHouse，连不上直接失败，不静默 skip。
为避免污染运行库，测试统一落在 ``tsp_test``；可用 ``CLICKHOUSE_TEST_DATABASE`` 覆盖。

必须在任何 ``app.*`` 导入之前设置环境变量：``app.config.settings`` 是模块级单例，
一旦被导入就固定了库名。
"""

from __future__ import annotations

import os

# 不能用 setdefault: dev 容器把 CLICKHOUSE_DATABASE=tsp 注入进程环境,
# 会导致带 t_ 前缀的测试表写进正式库。pytest 进程必须强制使用测试库。
os.environ["CLICKHOUSE_DATABASE"] = os.environ.get("CLICKHOUSE_TEST_DATABASE", "tsp_test")
# 生产 S4 起四后端默认 redis；测试必须显式锁 memory 保持确定性隔离：
# 共享 Redis 的固定 key（如 benchmark:momentum:v1）会跨用例污染，且 cache_health
# 断言不应依赖机器上是否启动了 Redis。redis 路径由 test_*_redis.py 单独覆盖。
os.environ["CACHE_BACKEND"] = "memory"
os.environ["EVENT_BUS_BACKEND"] = "memory"
os.environ["COORDINATION_BACKEND"] = "memory"
os.environ["MINING_STORE_BACKEND"] = "memory"
# mining memory 文件版仅允许单 worker（flock 是进程内锁）；测试进程固定 1。
os.environ["API_WORKERS"] = "1"
# DES-006 用户状态：测试用独立 schema/Redis 库，避免污染 dev 数据；
# 由 _user_state 夹具初始化并在用例间重置（见下）。
os.environ["USER_STATE_PG_SCHEMA"] = "tsp_user_state_test"
os.environ["USER_STATE_REDIS_URL"] = "redis://:123456@localhost:6379/15"
os.environ["USER_STATE_REDIS_PREFIX"] = "tsp:user:test:"
os.environ["USER_STATE_ENCRYPTION_KEY"] = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="

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
    """把全局存储 runtime 指向带随机前缀的 CK 表，    用例结束即回收，不恢复进入前的 runtime（避免泄漏链污染后续 CK 优先读路径）。

    供依赖 ``get_storage()``/``require_storage()`` 的服务层读路径使用（验收文档
    §14.1 D 类迁移：fixture 换成 CK，断言逻辑不变）。入口先回收可能被其它
    用例（真实 app lifespan）泄漏的全局 runtime，避免读到无关前缀的数据。
    """
    from app.storage import runtime as storage_runtime
    from app.storage import schema as ck_schema
    from app.storage.clickhouse import ClickHouseClient
    from app.storage.schema import ensure_schema

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
        # 先 drop 表再 shutdown，且不再恢复 prev_runtime（它可能是更早用例泄漏的
        # 脏状态），确保全局 runtime 不残留指向已删除表的客户端污染后续测试。
        for name in ck_schema.TABLE_NAMES:
            try:
                client.execute(f"DROP TABLE IF EXISTS {client.table(name)}")
            except Exception as exc:  # noqa: BLE001 - 清理失败不盖住测试结论
                print(f"清理表 {name} 失败: {exc}")
        storage_runtime.shutdown_storage()
        client.close()


# ── 模拟盘 PostgreSQL 事务层（DES-005）测试隔离 ──────────────────
@pytest.fixture(scope="module")
def paper_pg():
    """PG 连通性探测 + 幂等建表；无 PG 时整模块 skip（非 autouse，避免误伤纯函数测试）。"""
    import psycopg

    from app.storage import paper_pg as pg

    try:
        with psycopg.connect(settings.paper_pg_url, connect_timeout=2):
            pass
    except Exception as exc:  # noqa: BLE001 - 连通性探测需捕获全部异常以 skip
        # 只 skip 依赖该 fixture 的用例：config_backup 等非 paper 文件也复用它，
        # 不能用 allow_module_level 把整个模块（含不依赖 PG 的用例）一起跳过。
        pytest.skip(f"PostgreSQL 不可用: {exc}")
    pg.init_schema()
    return pg


@pytest.fixture()
def paper_account_id(paper_pg) -> Iterator[str]:
    """单个唯一 account_id，用例结束级联清理该账户全部数据。"""
    aid = f"t_{uuid.uuid4().hex[:12]}"
    yield aid
    paper_pg.delete_account(aid)


@pytest.fixture()
def paper_account_ids(paper_pg):
    """多账户唯一 id 工厂：make(n) 返回 n 个 id，用例结束全部清理。"""
    created: list[str] = []

    def make(n: int = 1) -> list[str]:
        ids = [f"t_{uuid.uuid4().hex[:12]}" for _ in range(n)]
        created.extend(ids)
        return ids

    yield make
    for aid in created:
        paper_pg.delete_account(aid)


# ── 用户状态门面（DES-006）测试隔离 ────────────────────────────
_USER_STATE_TABLES = (
    "schema_migrations",
    "user_auth",
    "api_token",
    "user_secret",
    "user_preference",
    "capability_cache",
    "watchlist_item",
    "watchlist_group",
    "watchlist_membership",
    "daily_integrity",
    "integrity_revision",
    "integrity_report",
    "repair_backoff",
    "data_source_config",
    "user_blob",
    "ext_page",
    "sound_pack",
    "integrity_backup",
    "state_migration_audit",
)


@pytest.fixture(autouse=True)
def _user_state() -> Iterator[None]:
    """每个用例重置 PG 用户状态表与 Redis 测试命名空间，保证用例间隔离。

    PG/Redis 不可用时（纯离线环境）不阻断：门面读取按未初始化降级，依赖
    真实状态的用例由各自 fixture 负责 skip。
    """
    from app.state.pg import execute, is_ready

    try:
        from app.state import init_state_pg

        init_state_pg()
    except Exception:  # noqa: BLE001 - PG 不可用时降级，不污染纯函数测试
        yield
        return
    if is_ready():
        for table in _USER_STATE_TABLES:
            execute(f"TRUNCATE TABLE {settings.user_state_pg_schema}.{table}")
    try:
        from app.state.cache import _client

        client = _client()
        keys = list(client.scan_iter(match=f"{settings.user_state_redis_prefix}*"))
        if keys:
            client.delete(*keys)
    except Exception:  # noqa: BLE001 - Redis 不可用时不影响 PG 状态测试
        pass
    import app.state.secrets as _secrets_state

    _secrets_state.secret_cache = None
    _secrets_state.secret_cache_generation = None
    yield
