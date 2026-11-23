"""用户状态 PG schema 定义（DES-006）。

依 tsp-rule/pgsql.mdc：id TEXT 主键、四审计字段、表/字段注释齐全。
"""

from __future__ import annotations

from app.config import settings
from app.state.pg import execute

# 表/字段注释字典与 _DDL 一一对应；由 _apply_comments 生成 COMMENT ON，避免 DDL 内重复书写。
_TABLE_COMMENTS: dict[str, str] = {
    "schema_migrations": "用户状态库版本登记表；记录已应用的 DDL 迁移版本",
    "user_auth": "单实例访问密码哈希；每行代表一份密码配置",
    "api_token": "开放接口 Token 元数据与哈希；不存明文 Token",
    "user_secret": "用户配置的加密密钥；只通过 API/UI 写入",
    "user_preference": "用户偏好与数据源路由配置；单条 JSON 文档",
    "capability_cache": "能力探测 last-known-good 快照；Redis 仅作短 TTL 缓存",
    "watchlist_item": "自选标的；symbol 为业务唯一键",
    "watchlist_group": "自选分组；group_key 为业务唯一键",
    "watchlist_membership": "自选标的与分组的多对多关系",
    "daily_integrity": "日K完整性闸门当前状态；请求热路径同步读取",
    "integrity_revision": "按资产族记录的完整性版本号",
    "integrity_report": "最近一次完整性报告；report_id 为业务凭证",
    "repair_backoff": "自动修补冷却账本；按坏日起点记录最近尝试",
    "data_source_config": "自定义数据源配置文本",
    "user_blob": "小体积二进制对象内容寻址表",
    "ext_page": "扩展页面配置；bundle 内容存 user_blob",
    "sound_pack": "告警音资源元数据；音频内容存 user_blob",
    "integrity_backup": "完整性修补备份元数据；文件内容存 user_blob",
    "state_migration_audit": "文件到 PG/Redis/CK 的一次性迁移审计",
}

_FIELD_COMMENTS: dict[str, str] = {
    "id": "主键，UUID4 hex",
    "created_by": "创建人/操作来源",
    "created_at": "创建时间，UTC；由数据库默认值生成",
    "updated_by": "更新人/操作来源",
    "updated_at": "更新时间，UTC；每次 UPSERT 更新",
    "singleton_key": "单例键，当前固定 default；业务唯一键",
    "password_hash": "PBKDF2-SHA256 密码哈希，不存明文",
    "password_salt": "密码盐，hex 编码",
    "hash_algo": "哈希算法标识",
    "iterations": "PBKDF2 迭代次数",
    "token_id": "Token 业务 ID，对外列表使用；业务唯一键",
    "name": "名称",
    "token_hash": "Token SHA-256 哈希；业务唯一键",
    "scopes": "授权范围数组，JSONB",
    "revoked": "是否已撤销",
    "last_used_at": "最近使用时间，Redis 节流后落库",
    "secret_key": "密钥键名；业务唯一键",
    "value_encrypted": "AES-GCM 加密后的密文，含 nonce/tag",
    "payload": "JSON 文档或二进制内容",
    "version": "配置版本号，每次写入递增",
    "schema_version": "缓存结构版本，当前 6",
    "probe_log": "最近一次探测日志数组",
    "missing_caps": "缺失能力列表",
    "invalid_key": "上次探测是否判定 Key 无效",
    "symbol": "标的代码；业务唯一键",
    "note": "用户备注",
    "position": "展示顺序",
    "added_at": "加入自选时间",
    "group_key": "分组业务 ID；业务唯一键",
    "color": "分组颜色编号",
    "group_id": "分组业务 ID，逻辑关联 watchlist_group.group_key",
    "asset_type": "资产类型：stock/etf/index",
    "day": "交易日",
    "kind": "问题类型：missing/recompute 等",
    "reason": "问题描述",
    "needs_raw": "是否需要重拉原始日K",
    "needs_enriched": "是否需要重算 enriched",
    "revision": "版本号字符串，用于缓存失效",
    "report_id": "自检报告业务 ID，修补接口使用；业务唯一键",
    "checked_at": "自检完成时间",
    "status": "状态：clean/issues/pending/running/succeeded/failed",
    "report": "报告完整 JSON",
    "start_date": "坏日起点日期；业务唯一键",
    "last_attempt_at": "最近一次自动修补尝试时间",
    "config_name": "数据源配置名；业务唯一键",
    "content": "配置正文",
    "content_hash": "内容 SHA-256；业务唯一键，pgblob:// URI 使用此值",
    "size_bytes": "对象字节数",
    "mime_type": "对象 MIME 类型",
    "page_id": "扩展页面业务 ID；业务唯一键",
    "config": "扩展页面配置 JSON",
    "bundle_hash": "bundle 内容哈希",
    "pack_name": "音频包名称；业务唯一键",
    "blob_hash": "音频内容哈希",
    "ext": "文件扩展名",
    "backup_id": "备份业务 ID；业务唯一键",
    "manifest": "备份清单 JSON",
    "blob_hashes": "备份对象内容哈希数组",
    "migration_id": "迁移项业务 ID；业务唯一键",
    "source_path": "旧文件路径",
    "target_ref": "目标表或 Redis 键",
    "checksum": "源内容校验和",
    "detail": "补充说明或失败原因",
    "applied_at": "迁移应用时间",
}

# 每张表均含 id 主键与四审计字段；{s} 运行时替换为配置的 schema，全部用 IF NOT EXISTS 保证幂等。
_DDL: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS {s}.schema_migrations (
        id TEXT PRIMARY KEY, version INTEGER NOT NULL UNIQUE, name TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.user_auth (
        id TEXT PRIMARY KEY, singleton_key TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL, password_salt TEXT NOT NULL,
        hash_algo TEXT NOT NULL, iterations INTEGER NOT NULL,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.api_token (
        id TEXT PRIMARY KEY, token_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
        token_hash TEXT NOT NULL UNIQUE, scopes JSONB NOT NULL,
        revoked BOOLEAN NOT NULL DEFAULT FALSE, last_used_at TIMESTAMPTZ,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.user_secret (
        id TEXT PRIMARY KEY, secret_key TEXT NOT NULL UNIQUE,
        value_encrypted BYTEA NOT NULL, created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.user_preference (
        id TEXT PRIMARY KEY, singleton_key TEXT NOT NULL UNIQUE,
        payload JSONB NOT NULL, version BIGINT NOT NULL,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.capability_cache (
        id TEXT PRIMARY KEY, singleton_key TEXT NOT NULL UNIQUE,
        schema_version INTEGER NOT NULL, payload JSONB NOT NULL,
        probe_log TEXT[] NOT NULL, missing_caps TEXT[] NOT NULL,
        invalid_key BOOLEAN NOT NULL DEFAULT FALSE,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.watchlist_item (
        id TEXT PRIMARY KEY, symbol TEXT NOT NULL UNIQUE, note TEXT NOT NULL DEFAULT '',
        position INTEGER NOT NULL DEFAULT 0, added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.watchlist_group (
        id TEXT PRIMARY KEY, group_key TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
        color INTEGER NOT NULL DEFAULT 0, position INTEGER NOT NULL DEFAULT 0,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.watchlist_membership (
        id TEXT PRIMARY KEY, symbol TEXT NOT NULL, group_id TEXT NOT NULL,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_watchlist_membership_symbol_group UNIQUE (symbol, group_id))""",
    """CREATE TABLE IF NOT EXISTS {s}.daily_integrity (
        id TEXT PRIMARY KEY, asset_type TEXT NOT NULL, day DATE NOT NULL,
        symbol TEXT NOT NULL DEFAULT '', kind TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
        needs_raw BOOLEAN NOT NULL DEFAULT FALSE, needs_enriched BOOLEAN NOT NULL DEFAULT FALSE,
        revision TEXT NOT NULL, created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_daily_integrity_key UNIQUE (asset_type, day, symbol, kind))""",
    """CREATE TABLE IF NOT EXISTS {s}.integrity_revision (
        id TEXT PRIMARY KEY, asset_type TEXT NOT NULL UNIQUE, revision TEXT NOT NULL,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.integrity_report (
        id TEXT PRIMARY KEY, report_id TEXT NOT NULL UNIQUE,
        checked_at TIMESTAMPTZ NOT NULL, status TEXT NOT NULL, report JSONB NOT NULL,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.repair_backoff (
        id TEXT PRIMARY KEY, start_date DATE NOT NULL UNIQUE,
        last_attempt_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.data_source_config (
        id TEXT PRIMARY KEY, config_name TEXT NOT NULL UNIQUE, kind TEXT NOT NULL,
        content TEXT NOT NULL, created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.user_blob (
        id TEXT PRIMARY KEY, content_hash TEXT NOT NULL UNIQUE, payload BYTEA NOT NULL,
        size_bytes BIGINT NOT NULL, mime_type TEXT NOT NULL,
        created_by TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_by TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.ext_page (
        id TEXT PRIMARY KEY, page_id TEXT NOT NULL UNIQUE, config JSONB NOT NULL,
        bundle_hash TEXT, created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.sound_pack (
        id TEXT PRIMARY KEY, pack_name TEXT NOT NULL UNIQUE, blob_hash TEXT NOT NULL,
        size_bytes BIGINT NOT NULL, ext TEXT NOT NULL, created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.integrity_backup (
        id TEXT PRIMARY KEY, backup_id TEXT NOT NULL UNIQUE, manifest JSONB NOT NULL,
        blob_hashes TEXT[] NOT NULL, created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS {s}.state_migration_audit (
        id TEXT PRIMARY KEY, migration_id TEXT NOT NULL UNIQUE, source_path TEXT NOT NULL,
        target_ref TEXT NOT NULL, status TEXT NOT NULL, checksum TEXT NOT NULL DEFAULT '',
        detail TEXT NOT NULL DEFAULT '', created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_by TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
)


def ensure_user_state_schema() -> None:
    """创建 schema、全部表和注释；重复调用幂等。"""
    schema = settings.user_state_pg_schema
    execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for statement in _DDL:
        execute(statement.format(s=schema))
    _apply_comments(schema)


def _apply_comments(schema: str) -> None:
    """按表名/字段名字典补齐注释，跳过未列出的业务字段。"""
    for table, comment in _TABLE_COMMENTS.items():
        execute(f"COMMENT ON TABLE {schema}.{table} IS '{comment}'")
    for table in _TABLE_COMMENTS:
        for field, comment in _FIELD_COMMENTS.items():
            if field not in _table_fields(table):
                continue
            execute(f"COMMENT ON COLUMN {schema}.{table}.{field} IS '{comment}'")


def _table_fields(table: str) -> set[str]:
    """返回表字段集合，供注释生成跳过不存在的列。"""
    ddl = next(statement for statement in _DDL if f".{table} (" in statement)
    body = ddl.split("(", 1)[1].rsplit(")", 1)[0]
    return {part.strip().split()[0] for part in body.split(",") if "CONSTRAINT" not in part}
