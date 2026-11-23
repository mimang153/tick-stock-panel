"""用户状态门面（DES-006）。

对外只暴露稳定函数，业务层不得直接持有 PG/Redis 客户端或拼接 SQL。
"""

# 子模块按职责分组导出：持久态(pg/schema)、短态(cache)、各类业务状态与审计。
from app.state import (
    audit,
    auth,
    blob,
    cache,
    capability,
    integrity,
    jobs,
    objects,
    pg,
    preferences,
    schema,
    shared,
    tokens,
)

# 审计门面：配置变更、完整性历史、迁移留痕统一写 CK。
from app.state.audit import (
    record_migration_audit,
    write_config_audit,
    write_integrity_report_history,
)

# 认证：密码哈希存 PG，会话短态存 Redis（见 auth 门面）。
from app.state.auth import get_auth, invalidate_sessions, save_auth

# 二进制对象：内容寻址，pgblob:// URI 统一解析。
from app.state.blob import delete_blob, get_blob, put_blob, resolve_blob_uri

# Redis 短态门面：JSON/会话/generation/发布订阅，业务层不直接持客户端。
from app.state.cache import (
    bump_generation,
    check_persistence,
    delete_key,
    delete_prefix,
    delete_session,
    generation,
    get_json,
    get_session,
    ping,
    publish,
    set_json,
    set_session,
    start_listener,
    subscribe,
)

# 能力探测：PG last-known-good + Redis 5 分钟短缓存。
from app.state.capability import get_capability_cache, save_capability_cache

# 完整性闸门：当前态在 PG，报告历史与修补冷却经审计门面落 CK。
from app.state.integrity import (
    get_daily_integrity_snapshot,
    get_integrity_revision,
    get_latest_integrity_report,
    put_daily_integrity_snapshot,
    record_repair_attempt,
    repair_backoff_remaining_s,
    save_integrity_report,
)

# 任务状态：live/progress 存 Redis，完成后写 CK 历史。
from app.state.jobs import (
    clear_jobs,
    get_job_progress,
    get_job_state,
    list_recent_jobs,
    put_job_progress,
    put_job_state,
    write_job_history,
)

# 配置对象：数据源/ext_page/音包/备份等 PG 元数据与 user_blob。
from app.state.objects import (
    delete_data_source_config,
    delete_ext_page,
    delete_sound_pack,
    get_data_source_config,
    list_data_source_configs,
    list_ext_pages,
    list_sound_packs,
    read_integrity_backup,
    read_sound_pack,
    save_data_source_config,
    save_ext_page,
    save_integrity_backup,
    save_sound_pack,
)
from app.state.pg import close_state_pg, init_state_pg
from app.state.preferences import (
    delete_preference_keys,
    get_preferences,
    preferences_version,
    replace_preferences,
    save_preferences,
)
from app.state.schema import ensure_user_state_schema
from app.state.secrets import get_secret, get_secrets, save_secrets
from app.state.shared import (
    shared_data_root,
    shared_plugin_root,
    shared_strategy_dirs,
    shared_strategy_path,
    shared_strategy_root,
)
from app.state.tokens import (
    create_api_token,
    get_api_token_by_hash,
    list_api_tokens,
    revoke_api_token,
    touch_api_token,
)
from app.state.watchlist import (
    get_watchlist_version,
    list_watchlist_groups,
    list_watchlist_items,
    save_watchlist_groups,
    save_watchlist_items,
)

__all__ = [
    "audit",
    "auth",
    "blob",
    "bump_generation",
    "cache",
    "capability",
    "check_persistence",
    "clear_jobs",
    "close_state_pg",
    "create_api_token",
    "delete_blob",
    "delete_data_source_config",
    "delete_ext_page",
    "delete_key",
    "delete_preference_keys",
    "delete_prefix",
    "delete_session",
    "delete_sound_pack",
    "ensure_user_state_schema",
    "generation",
    "get_api_token_by_hash",
    "get_auth",
    "get_blob",
    "get_capability_cache",
    "get_daily_integrity_snapshot",
    "get_data_source_config",
    "get_integrity_revision",
    "get_job_progress",
    "get_job_state",
    "get_json",
    "get_latest_integrity_report",
    "get_preferences",
    "get_secret",
    "get_secrets",
    "get_session",
    "get_watchlist_version",
    "init_state_pg",
    "integrity",
    "invalidate_sessions",
    "jobs",
    "list_api_tokens",
    "list_data_source_configs",
    "list_ext_pages",
    "list_recent_jobs",
    "list_sound_packs",
    "list_watchlist_groups",
    "list_watchlist_items",
    "objects",
    "pg",
    "ping",
    "preferences",
    "preferences_version",
    "publish",
    "put_blob",
    "put_daily_integrity_snapshot",
    "put_job_progress",
    "put_job_state",
    "read_integrity_backup",
    "read_sound_pack",
    "record_migration_audit",
    "record_repair_attempt",
    "repair_backoff_remaining_s",
    "replace_preferences",
    "resolve_blob_uri",
    "revoke_api_token",
    "save_auth",
    "save_capability_cache",
    "save_data_source_config",
    "save_ext_page",
    "save_integrity_backup",
    "save_integrity_report",
    "save_preferences",
    "save_secrets",
    "save_sound_pack",
    "save_watchlist_groups",
    "save_watchlist_items",
    "schema",
    "set_json",
    "set_session",
    "shared",
    "shared_data_root",
    "shared_plugin_root",
    "shared_strategy_dirs",
    "shared_strategy_path",
    "shared_strategy_root",
    "start_listener",
    "subscribe",
    "tokens",
    "touch_api_token",
    "write_config_audit",
    "write_integrity_report_history",
    "write_job_history",
]
