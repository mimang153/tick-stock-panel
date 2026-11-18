"""KlineRepository mixin 共享属性声明。

属性在 KlineRepository.__init__ 中初始化；此处声明供 mypy 识别，
__getattr__ 让各 mixin 能引用兄弟 mixin 提供的方法。

本模块只做类型层面的声明，不含运行时逻辑：

- 每个 ``_xxx: Any`` 声明一个由具体 mixin 或根类提供的属性/方法；
- ``__getattr__`` 在属性缺失时返回惰性占位，避免类定义期NameError；
- 阶段 4 把 KlineRepository 拆为多个 mixin 后，本模块作为它们的公共基类，
  保证跨 mixin 的符号引用对类型检查器可见。
"""

from __future__ import annotations

from typing import Any


class _RepoMixinBase:
    _assert_daily_usable: Any
    _atomic_write_parquet: Any
    _build_live_agg: Any
    _build_live_agg_from_parquet: Any
    _ck_store: Any
    _compute_enriched_range: Any
    _compute_index_enriched_range: Any
    _enriched_cache: Any
    _enriched_cache_date: Any
    _enriched_glob: Any
    _enriched_history_cache: Any
    _enriched_history_generation: Any
    _enriched_history_snapshot: Any
    _enriched_history_start: Any
    _enriched_warming: Any
    _etf_enriched_cache: Any
    _etf_enriched_cache_date: Any
    _etf_enriched_glob: Any
    _etf_inst_glob: Any
    _etf_instruments_cache: Any
    _etf_live_agg_cache: Any
    _etf_live_agg_cache_date: Any
    _etf_minute_glob: Any
    _etf_symbol_set_cache: Any
    _filter_cached: Any
    _filter_cached_batch: Any
    _historical_shares_cache: Any
    _historical_shares_mtime_ns: Any
    _index_enriched_cache: Any
    _index_enriched_cache_date: Any
    _index_enriched_glob: Any
    _index_inst_glob: Any
    _index_instruments_cache: Any
    _index_symbol_set_cache: Any
    _inst_glob: Any
    _instruments_cache: Any
    _latest_enriched_date_duckdb: Any
    _live_agg_baseline_date: Any
    _live_agg_cache: Any
    _live_agg_cache_date: Any
    _live_agg_check_date: Any
    _live_agg_ema_rsi: Any
    _live_agg_extra_states: Any
    _live_agg_rolling: Any
    _live_agg_source: Any
    _maybe_adjust_minute: Any
    _migrate_legacy_data_dir: Any
    _minute_glob: Any
    _minute_glob_for: Any
    _minute_partition_path: Any
    _mirror_daily_repair_to_ck: Any
    _name_map_cache: Any
    _notify_refresh_done: Any
    _on_refresh_done: Any
    _on_warmup_done: Any
    _optimistic_upsert_partition: Any
    _partition_fingerprint: Any
    _prepared_daily: Any
    _prepared_daily_lock: Any
    _refresh_collect_history: Any
    _refresh_compute_history: Any
    _refresh_enriched: Any
    _refresh_enriched_impl: Any
    _refresh_etf_enriched: Any
    _refresh_etf_instruments: Any
    _refresh_index_enriched: Any
    _refresh_index_instruments: Any
    _refresh_instruments: Any
    _refresh_publish_history: Any
    _refresh_read_latest: Any
    _refresh_resolve_latest: Any
    _restore_missing_latest_rows: Any
    _scan_daily_batch: Any
    _scan_daily_symbol: Any
    _scan_etf_daily_symbol: Any
    _scan_index_daily_symbol: Any
    _start_enriched_warmup: Any
    _validate_daily_repair_payload: Any
    _validate_repair_path: Any
    _warmup_enriched: Any
    _warmup_lock: Any
    _warmup_thread: Any
    _with_instrument_metadata: Any
    _write_daily_partition: Any
    _write_lock: Any
    _write_partition_locked: Any
    append_daily: Any
    append_enriched: Any
    append_etf_daily: Any
    append_etf_enriched: Any
    append_index_daily: Any
    append_index_enriched: Any
    clear_cache: Any
    daily_repair_nontrading_symbols: Any
    data_dir: Any
    flush_live_daily_asset: Any
    flush_live_enriched_asset: Any
    get_daily: Any
    get_enriched_generation_snapshot: Any
    get_enriched_latest: Any
    get_etf_daily: Any
    get_etf_instruments: Any
    get_etf_symbol_set: Any
    get_historical_shares: Any
    get_index_daily: Any
    get_index_instruments: Any
    get_index_symbol_set: Any
    get_instruments: Any
    get_instruments_asset: Any
    get_matrix_data_generation: Any
    storage: Any
    store: Any

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)
