"""mining run 状态机的 Redis 权威实现（DES-004 方案 C）。

run manifest / summary / events 存 Redis，状态转换用 Lua 在 Redis 端原子完成
"读当前状态 → 校验合法转换 → 更新时间戳 → 写回"，替代本地文件的
``threading.RLock`` + manifest.json 原子替换。多实例共享同一 Redis 时天然串行化，
flock 不再是 mining 状态机的参与者。

接口与 ``MiningRunStore`` 完全兼容（依赖注入替换），文件版保留作离线兼容。
CK 审计由 ``mining_audit`` 在转换后落表，本模块只负责 Redis 权威态。

数据布局（键前缀 ``{coordination_redis_prefix}``）：
- Hash ``runs:<ns>``: field=run_id → manifest JSON（含 artifacts）
- Hash ``summary:<ns>``: field=run_id → summary JSON
- List ``events:<ns>:<rid>``: 事件 JSON，LTRIM 保留最近 MAX_EVENTS 条
- String ``eventseq:<ns>:<rid>``: 事件 id 自增（INCR，单调递增）
- Set ``runindex:<ns>``: 所有 run_id（供 list/find/recover 扫描）
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from app.services.mining_jobs import (
    _ALLOWED_TRANSITIONS,
    _NAMESPACE_PATTERN,
    _RUN_ID_PATTERN,
    ACTIVE_RUN_STATUSES,
    ARTIFACT_NAMES,
    MAX_EVENT_PAYLOAD_BYTES,
    MAX_EVENTS,
    RUN_STATUSES,
    TERMINAL_RUN_STATUSES,
    InvalidMiningStatusTransitionError,
    MiningRunValidationError,
    _now_iso,
    canonicalize_request,
    compute_run_signature,
)

# KEYS[1]=runs hash; ARGV[1]=run_id; ARGV[2]=target status;
# ARGV[3]=allowed 前驱集合(JSON array); ARGV[4]=now iso;
# ARGV[5]=error or ''; ARGV[6]=terminal 状态集合(JSON array)
_TRANSITION_LUA = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return redis.error_reply('run not found: ' .. ARGV[1]) end
local manifest = cjson.decode(raw)
local previous = manifest['status']
if previous == ARGV[2] then return raw end
local allowed = cjson.decode(ARGV[3])
local ok = false
for _, s in ipairs(allowed) do if s == ARGV[2] then ok = true end end
if not ok then return redis.error_reply('invalid transition: ' .. previous .. ' -> ' .. ARGV[2]) end
manifest['status'] = ARGV[2]
manifest['updated_at'] = ARGV[4]
if ARGV[2] == 'running' and (not manifest['started_at'] or manifest['started_at'] == cjson.null) then
  manifest['started_at'] = ARGV[4]
end
if ARGV[2] == 'cancelling' then manifest['cancellation_requested_at'] = ARGV[4] end
local terminal = cjson.decode(ARGV[6])
for _, s in ipairs(terminal) do if s == ARGV[2] then manifest['finished_at'] = ARGV[4] end end
if ARGV[5] ~= '' then manifest['error'] = ARGV[5] end
local encoded = cjson.encode(manifest)
redis.call('HSET', KEYS[1], ARGV[1], encoded)
return encoded
"""


def _redis_client():
    import redis

    from app.config import settings

    # 与 strategy 缓存共用同一个应用 Redis 实例（两者都是应用层状态，非 pipeline 协调态）。
    client = redis.Redis.from_url(
        settings.cache_redis_url,
        decode_responses=False,
        socket_timeout=settings.coordination_redis_socket_timeout_s,
    )
    try:
        client.ping()
    except Exception as exc:
        raise RuntimeError(f"Redis mining store 不可用: {exc}") from exc
    return client


class RedisMiningRunStore:
    """与 MiningRunStore 接口兼容的 Redis 实现；状态机用 Lua 原子化。"""

    def __init__(self, data_dir: Path | str | None = None, *, namespace: str = "mining") -> None:
        if not _NAMESPACE_PATTERN.fullmatch(namespace):
            raise MiningRunValidationError(f"unsupported run store namespace: {namespace!r}")
        self.namespace = namespace
        self._client = _redis_client()
        # 独立命名空间，避免被 `redis-cache.sh flush`（清 tickflow:cache:*）误删 run 状态。
        prefix = "tickflow:mining"
        self._runs_key = f"{prefix}:runs:{namespace}"
        self._summary_key = f"{prefix}:summary:{namespace}"
        self._index_key = f"{prefix}:runindex:{namespace}"

    def _events_key(self, run_id: str) -> str:
        return f"{self._runs_key}:events:{run_id}"

    def _seq_key(self, run_id: str) -> str:
        return f"{self._runs_key}:eventseq:{run_id}"

    def _validate_run_id(self, run_id: str) -> str:
        if not isinstance(run_id, str) or not _RUN_ID_PATTERN.fullmatch(run_id):
            raise MiningRunValidationError(f"invalid run id: {run_id!r}")
        return run_id

    def _manifest(self, run_id: str) -> dict[str, Any] | None:
        raw = self._client.hget(self._runs_key, run_id)
        return json.loads(raw) if raw is not None else None

    def create(
        self,
        request: Mapping[str, Any],
        data_fingerprint: Any,
        *,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        safe_id = self._validate_run_id(uuid.uuid4().hex if run_id is None else run_id)
        canonical = canonicalize_request(request)
        now = _now_iso()
        manifest = {
            "schema_version": 1,
            "run_id": safe_id,
            "status": "queued",
            "request": canonical,
            "data_fingerprint": data_fingerprint,
            "run_signature": compute_run_signature(canonical, data_fingerprint),
            "artifacts": {},
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "finished_at": None,
            "cancellation_requested_at": None,
            "error": None,
        }
        pipe = self._client.pipeline()
        pipe.hsetnx(self._runs_key, safe_id, json.dumps(manifest, default=str))
        pipe.hsetnx(self._summary_key, safe_id, "{}")
        pipe.sadd(self._index_key, safe_id)
        created = pipe.execute()[0]
        if not created:
            raise MiningRunValidationError(f"run already exists: {safe_id}")
        return manifest

    def get(self, run_id: str) -> dict[str, Any] | None:
        return self._manifest(self._validate_run_id(run_id))

    def transition_status(
        self,
        run_id: str,
        status: str,
        *,
        error: str | None = None,
    ) -> dict[str, Any]:
        if status not in RUN_STATUSES:
            raise MiningRunValidationError(f"unsupported mining run status: {status!r}")
        safe_id = self._validate_run_id(run_id)
        current = self._manifest(safe_id)
        if current is None:
            raise KeyError(safe_id)
        if current["status"] == status:
            return current
        if status not in _ALLOWED_TRANSITIONS[current["status"]]:
            raise InvalidMiningStatusTransitionError(
                f"cannot transition from {current['status']} to {status}"
            )
        try:
            raw = self._client.eval(
                _TRANSITION_LUA,
                1,
                self._runs_key,
                safe_id,
                status,
                json.dumps(sorted(_ALLOWED_TRANSITIONS[current["status"]])),
                _now_iso(),
                "" if error is None else str(error),
                json.dumps(sorted(TERMINAL_RUN_STATUSES)),
            )
        except Exception as exc:
            if "invalid transition" in str(exc):
                raise InvalidMiningStatusTransitionError(str(exc)) from exc
            raise
        manifest = cast(dict[str, Any], json.loads(raw))
        from app.services import mining_audit

        if mining_audit.enabled():
            mining_audit.record_transition(
                self.namespace, safe_id, current["status"], status, manifest, error
            )
        return manifest

    def write_summary(self, run_id: str, summary: Mapping[str, Any]) -> dict[str, Any]:
        safe_id = self._validate_run_id(run_id)
        if self._manifest(safe_id) is None:
            raise KeyError(safe_id)
        from app.services.mining_jobs import _canonicalize_json_value

        clean = cast(dict[str, Any], _canonicalize_json_value(summary))
        self._client.hset(self._summary_key, safe_id, json.dumps(clean, default=str))
        return clean

    def read_summary(self, run_id: str) -> dict[str, Any]:
        safe_id = self._validate_run_id(run_id)
        if self._manifest(safe_id) is None:
            raise KeyError(safe_id)
        raw = self._client.hget(self._summary_key, safe_id)
        return cast(dict[str, Any], json.loads(raw)) if raw is not None else {}

    def _run_dir(self, run_id: str) -> Path:
        from app.config import settings

        return Path(settings.data_dir) / "research" / self.namespace / "runs" / run_id

    def _safe_artifact_path(self, run_id: str, path: Path) -> Path:
        candidate = (self._run_dir(run_id) / path).resolve()
        root = self._run_dir(run_id).resolve()
        if candidate != root and root not in candidate.parents:
            raise MiningRunValidationError("artifact path escapes run directory")
        return candidate

    def artifact_path(self, run_id: str, name: str) -> Path:
        safe_id = self._validate_run_id(run_id)
        if name not in ARTIFACT_NAMES:
            raise MiningRunValidationError(f"unsupported artifact: {name!r}")
        return self._safe_artifact_path(safe_id, Path(f"{name}.parquet"))

    def register_artifact(
        self,
        run_id: str,
        name: str,
        path: Path | str | None = None,
    ) -> dict[str, Any]:
        safe_id = self._validate_run_id(run_id)
        if name not in ARTIFACT_NAMES:
            raise MiningRunValidationError(f"unsupported artifact: {name!r}")
        artifact_path = self._safe_artifact_path(
            safe_id, Path(path) if path is not None else Path(f"{name}.parquet")
        )
        if artifact_path.suffix.lower() != ".parquet":
            raise MiningRunValidationError("mining artifacts must use the .parquet suffix")
        relative_path = artifact_path.relative_to(self._run_dir(safe_id)).as_posix()
        manifest = self._manifest(safe_id)
        if manifest is None:
            raise KeyError(safe_id)
        artifacts = dict(manifest.get("artifacts") or {})
        artifacts[name] = relative_path
        manifest["artifacts"] = artifacts
        manifest["updated_at"] = _now_iso()
        self._client.hset(self._runs_key, safe_id, json.dumps(manifest, default=str))
        return manifest

    def append_event(
        self,
        run_id: str,
        event_type: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        from app.services.mining_jobs import _canonicalize_json_value

        safe_id = self._validate_run_id(run_id)
        if self._manifest(safe_id) is None:
            raise KeyError(safe_id)
        if not isinstance(event_type, str):
            raise MiningRunValidationError("event_type must be a string")
        clean_type = event_type.strip()
        if not clean_type or len(clean_type) > 64:
            raise MiningRunValidationError("event_type must contain 1 to 64 characters")
        raw_payload: Any = {} if payload is None else payload
        if not isinstance(raw_payload, Mapping):
            raise MiningRunValidationError("event payload must be a mapping")
        clean_payload = cast(dict[str, Any], _canonicalize_json_value(raw_payload))
        encoded = json.dumps(
            clean_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) > MAX_EVENT_PAYLOAD_BYTES:
            raise MiningRunValidationError("event payload too large")
        event_id = self._client.incr(self._seq_key(safe_id))
        event = {
            "id": event_id,
            "timestamp": _now_iso(),
            "type": clean_type,
            "payload": clean_payload,
        }
        pipe = self._client.pipeline()
        pipe.rpush(self._events_key(safe_id), json.dumps(event, default=str))
        pipe.ltrim(self._events_key(safe_id), -MAX_EVENTS, -1)
        pipe.execute()
        return event

    def read_events(self, run_id: str, after_id: int = 0) -> list[dict[str, Any]]:
        safe_id = self._validate_run_id(run_id)
        if not isinstance(after_id, int) or after_id < 0:
            raise MiningRunValidationError("after_id must be a non-negative int")
        if self._manifest(safe_id) is None:
            raise KeyError(safe_id)
        raw_events = self._client.lrange(self._events_key(safe_id), 0, -1)
        events = [json.loads(raw) for raw in raw_events]
        return [event for event in events if event.get("id", 0) > after_id][-MAX_EVENTS:]

    def list_runs(
        self,
        *,
        limit: int = 50,
        statuses: Any = None,
    ) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
            raise MiningRunValidationError("limit must be between 1 and 200")
        allowed = None if statuses is None else set(statuses)
        if allowed is not None and not allowed <= RUN_STATUSES:
            raise MiningRunValidationError("statuses contains an unsupported mining run status")
        ids = self._client.smembers(self._index_key)
        runs = [m for rid in ids if (m := self._manifest(rid.decode())) is not None]
        if allowed is not None:
            runs = [m for m in runs if m.get("status") in allowed]
        from app.services.mining_jobs import _manifest_sort_key

        runs.sort(key=_manifest_sort_key, reverse=True)
        return runs[:limit]

    def find_by_signature(
        self,
        run_signature: str,
        *,
        statuses: Any = None,
    ) -> dict[str, Any] | None:
        if not isinstance(run_signature, str) or not run_signature:
            raise MiningRunValidationError("run_signature must not be empty")
        allowed = None if statuses is None else set(statuses)
        if allowed is not None and not allowed <= RUN_STATUSES:
            raise MiningRunValidationError("statuses contains an unsupported mining run status")
        from app.services.mining_jobs import _manifest_sort_key

        ids = self._client.smembers(self._index_key)
        matches = [
            m
            for rid in ids
            if (m := self._manifest(rid.decode())) is not None
            and m.get("run_signature") == run_signature
            and (allowed is None or m.get("status") in allowed)
        ]
        return max(matches, key=_manifest_sort_key, default=None)

    def recover_interrupted(self) -> list[str]:
        recovered: list[str] = []
        for manifest in self.list_runs(limit=10000):
            if manifest.get("status") in ACTIVE_RUN_STATUSES:
                self.transition_status(manifest["run_id"], "interrupted")
                recovered.append(manifest["run_id"])
        return recovered
