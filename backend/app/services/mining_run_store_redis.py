"""mining run 状态机的 Redis 权威实现（DES-004 方案 C）。

run manifest / summary / events 存 Redis，状态转换用 Lua 在 Redis 端原子完成
"读当前状态 → 校验合法转换 → 更新时间戳 → 写回"，替代本地文件的
``threading.RLock`` + manifest.json 原子替换。多实例共享同一 Redis 时天然串行化，
flock 不再是 mining 状态机的参与者。

接口与 ``MiningRunStore`` 完全兼容（依赖注入替换），文件版保留作离线兼容。
审计事件与状态转换在同一 Lua 中写入 Redis outbox，由 ``mining_audit`` 幂等落 CK。

数据布局（键前缀 ``{coordination_redis_prefix}``）：
- Hash ``runs:<ns>``: field=run_id → manifest JSON（含 artifacts）
- Hash ``summary:<ns>``: field=run_id → summary JSON
- List ``events:<ns>:<rid>``: 事件 JSON，LTRIM 保留最近 MAX_EVENTS 条
- String ``eventseq:<ns>:<rid>``: 事件 id 自增（INCR，单调递增）
- Set ``runindex:<ns>``: 所有 run_id（供 list/find/recover 扫描）
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from app.services.mining_jobs import (
    _ALLOWED_TRANSITIONS,
    _LEGACY_READ_RUN_ID_PATTERN,
    _NAMESPACE_PATTERN,
    _RUN_ID_PATTERN,
    ACTIVE_RUN_STATUSES,
    ARTIFACT_NAMES,
    MAX_EVENT_PAYLOAD_BYTES,
    MAX_EVENTS,
    RUN_STATUSES,
    SUCCESS_RUN_STATUSES,
    TERMINAL_RUN_STATUSES,
    InvalidMiningStatusTransitionError,
    MiningRunStore,
    MiningRunValidationError,
    _artifact_content_metadata,
    _manifest_sort_key,
    _now_iso,
    canonicalize_request,
    compute_run_signature,
)

logger = logging.getLogger(__name__)

# KEYS[1]=runs hash; KEYS[2]=audit outbox list; KEYS[3]=outbox namespace index;
# ARGV[1]=run_id; ARGV[2]=target status;
# ARGV[3]=allowed 前驱集合(JSON array); ARGV[4]=now iso;
# ARGV[5]=error or ''; ARGV[6]=terminal 状态集合(JSON array);
# ARGV[7]=event_id; ARGV[8]=namespace; ARGV[9]=previous status
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
local event = cjson.encode({
  event_id = ARGV[7],
  namespace = ARGV[8],
  run_id = ARGV[1],
  from_status = ARGV[9],
  to_status = ARGV[2],
  manifest = cjson.decode(encoded),
  error = (ARGV[5] == '' and cjson.null or ARGV[5]),
  created_at = ARGV[4]
})
redis.call('HSET', KEYS[1], ARGV[1], encoded)
redis.call('RPUSH', KEYS[2], event)
redis.call('SADD', KEYS[3], ARGV[8])
return encoded
"""

# KEYS[1]=signature hash; KEYS[2]=runs hash;
# ARGV[1]=signature; ARGV[2]=proposed run_id; ARGV[3]=reusable statuses (JSON array)
_CLAIM_SIGNATURE_LUA = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
local reusable = cjson.decode(ARGV[3])
if existing then
  local raw = redis.call('HGET', KEYS[2], existing)
  if not raw then return existing end
  local manifest = cjson.decode(raw)
  for _, s in ipairs(reusable) do
    if manifest['status'] == s then return existing end
  end
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return ARGV[2]
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return ARGV[2]
"""

# KEYS[1]=runs hash; ARGV[1]=run_id; ARGV[2]=artifact name;
# ARGV[3]=artifact URI; ARGV[4]=content metadata JSON; ARGV[5]=updated_at
_REGISTER_ARTIFACT_LUA = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return redis.error_reply('run not found: ' .. ARGV[1]) end
local manifest = cjson.decode(raw)
local artifacts = manifest['artifacts'] or {}
artifacts[ARGV[2]] = ARGV[3]
manifest['artifacts'] = artifacts
local content = manifest['artifact_content'] or {}
content[ARGV[2]] = cjson.decode(ARGV[4])
manifest['artifact_content'] = content
manifest['updated_at'] = ARGV[5]
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
        from app.config import settings

        self.namespace = namespace
        self.data_dir = Path(data_dir) if data_dir is not None else Path(settings.data_dir)
        self._client = _redis_client()
        # 独立命名空间，避免被 `redis-cache.sh flush`（清 tickflow:cache:*）误删 run 状态。
        prefix = "tickflow:mining"
        self._runs_key = f"{prefix}:runs:{namespace}"
        self._summary_key = f"{prefix}:summary:{namespace}"
        self._index_key = f"{prefix}:runindex:{namespace}"
        self._signature_key = f"{prefix}:signature:{namespace}"
        from app.services import mining_audit

        self._audit_outbox_key = mining_audit.outbox_key(namespace)
        self._audit_index_key = mining_audit._OUTBOX_INDEX

    def _events_key(self, run_id: str) -> str:
        return f"{self._runs_key}:events:{run_id}"

    def _seq_key(self, run_id: str) -> str:
        return f"{self._runs_key}:eventseq:{run_id}"

    def _validate_run_id(self, run_id: str) -> str:
        if not isinstance(run_id, str) or not _RUN_ID_PATTERN.fullmatch(run_id):
            raise MiningRunValidationError(f"invalid run id: {run_id!r}")
        return run_id

    def _legacy_validate_run_id(self, run_id: str) -> str:
        """Read-only validation tolerating legacy directory names on the file fallback."""
        if not isinstance(run_id, str) or not _LEGACY_READ_RUN_ID_PATTERN.fullmatch(run_id):
            raise MiningRunValidationError(f"invalid run id: {run_id!r}")
        return run_id

    def _file_store(self) -> MiningRunStore:
        """Return the file backend for runs written before Redis was authoritative."""
        return MiningRunStore(self.data_dir, namespace=self.namespace)

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
        pipe.hsetnx(self._signature_key, manifest["run_signature"], safe_id)
        pipe.sadd(self._index_key, safe_id)
        created = pipe.execute()[0]
        if not created:
            raise MiningRunValidationError(f"run already exists: {safe_id}")
        return manifest

    def get(self, run_id: str) -> dict[str, Any] | None:
        safe_id = self._legacy_validate_run_id(run_id)
        manifest = self._manifest(safe_id)
        if manifest is not None:
            return manifest
        return self._file_store().get(safe_id)

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
        event_id = uuid.uuid4().hex
        try:
            raw = self._client.eval(
                _TRANSITION_LUA,
                3,
                self._runs_key,
                self._audit_outbox_key,
                self._audit_index_key,
                safe_id,
                status,
                json.dumps(sorted(_ALLOWED_TRANSITIONS[current["status"]])),
                _now_iso(),
                "" if error is None else str(error),
                json.dumps(sorted(TERMINAL_RUN_STATUSES)),
                event_id,
                self.namespace,
                current["status"],
            )
        except Exception as exc:
            if "invalid transition" in str(exc):
                raise InvalidMiningStatusTransitionError(str(exc)) from exc
            raise
        manifest = cast(dict[str, Any], json.loads(raw))
        from app.services import mining_audit

        try:
            mining_audit.flush(self.namespace)
        except Exception as exc:  # noqa: BLE001 - CK 审计失败不能阻断已提交的 Redis 状态
            logger.warning("mining 审计 outbox 延迟写入: %s", exc)
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
        safe_id = self._legacy_validate_run_id(run_id)
        if self._manifest(safe_id) is None:
            return self._file_store().read_summary(safe_id)
        raw = self._client.hget(self._summary_key, safe_id)
        return cast(dict[str, Any], json.loads(raw)) if raw is not None else {}

    def _run_dir(self, run_id: str) -> Path:
        return self.data_dir / "research" / self.namespace / "runs" / run_id

    def _artifact_root(self, run_id: str) -> Path:
        from app.config import settings

        root = (
            self._run_dir(run_id)
            if settings.mining_artifact_root is None
            else settings.mining_artifact_root / self.namespace / self._validate_run_id(run_id)
        )
        resolved = root.resolve()
        if settings.mining_artifact_root is not None and not resolved.is_relative_to(
            settings.mining_artifact_root.resolve()
        ):
            raise MiningRunValidationError("artifact root escapes configured mining root")
        return resolved

    def _safe_artifact_path(self, run_id: str, path: Path) -> Path:
        root = self._artifact_root(run_id)
        candidate = (root / path).resolve()
        if candidate != root and root not in candidate.parents:
            raise MiningRunValidationError("artifact path escapes run directory")
        return candidate

    def artifact_path(self, run_id: str, name: str) -> Path:
        safe_id = self._validate_run_id(run_id)
        if name not in ARTIFACT_NAMES:
            raise MiningRunValidationError(f"unsupported artifact: {name!r}")
        self._artifact_root(safe_id).mkdir(parents=True, exist_ok=True)
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
        relative_path = artifact_path.relative_to(self._artifact_root(safe_id)).as_posix()
        from app.services.mining_artifact import record_artifact
        from app.storage.artifact_store import store_artifact_uri

        content = {
            **_artifact_content_metadata(artifact_path),
            "uri": store_artifact_uri(artifact_path, self.namespace, safe_id, name),
        }
        raw = self._client.eval(
            _REGISTER_ARTIFACT_LUA,
            1,
            self._runs_key,
            safe_id,
            name,
            relative_path,
            json.dumps(content, default=str),
            _now_iso(),
        )
        manifest = cast(dict[str, Any], json.loads(raw))
        record_artifact(self.namespace, safe_id, name, content)
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
        safe_id = self._legacy_validate_run_id(run_id)
        if not isinstance(after_id, int) or after_id < 0:
            raise MiningRunValidationError("after_id must be a non-negative int")
        if self._manifest(safe_id) is None:
            return self._file_store().read_events(safe_id, after_id=after_id)
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
        # Redis 迁移前的本地 run 在只读路径合并进来，同名 run 以 Redis 为准。
        seen = {m["run_id"] for m in runs}
        for manifest in self._file_store().list_runs(limit=limit, statuses=statuses):
            if manifest["run_id"] not in seen:
                runs.append(manifest)
                seen.add(manifest["run_id"])
        if allowed is not None:
            runs = [m for m in runs if m.get("status") in allowed]
        runs.sort(key=_manifest_sort_key, reverse=True)
        return runs[:limit]

    def claim_signature(self, run_signature: str, run_id: str) -> str:
        """Atomically claim a reusable signature or return its existing run id."""
        if not isinstance(run_signature, str) or not run_signature:
            raise MiningRunValidationError("run_signature must not be empty")
        safe_id = self._validate_run_id(run_id)
        reusable = sorted(ACTIVE_RUN_STATUSES | SUCCESS_RUN_STATUSES)
        claimed = self._client.eval(
            _CLAIM_SIGNATURE_LUA,
            2,
            self._signature_key,
            self._runs_key,
            run_signature,
            safe_id,
            json.dumps(reusable),
        )
        return claimed.decode() if isinstance(claimed, bytes) else str(claimed)

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
        ids = self._client.smembers(self._index_key)
        matches = {
            m["run_id"]: m
            for rid in ids
            if (m := self._manifest(rid.decode())) is not None
            and m.get("run_signature") == run_signature
            and (allowed is None or m.get("status") in allowed)
        }
        file_match = self._file_store().find_by_signature(run_signature, statuses=statuses)
        if file_match is not None and file_match["run_id"] not in matches:
            matches[file_match["run_id"]] = file_match
        return max(matches.values(), key=_manifest_sort_key, default=None)

    def set_run_owner(self, run_id: str, pid: int, host: str) -> None:
        """记录执行该 run 的 worker pid/host，供启动恢复判定存活（DIST-12）。"""
        safe_id = self._validate_run_id(run_id)
        raw = self._client.hget(self._runs_key, safe_id)
        if raw is None:
            return
        manifest = json.loads(raw)
        manifest["owner_pid"] = pid
        manifest["owner_host"] = host
        self._client.hset(self._runs_key, safe_id, json.dumps(manifest, default=str))

    @staticmethod
    def _owner_alive(manifest: dict[str, Any]) -> bool:
        """owner 在同主机且进程存活时视为 live，recover 不得误标 interrupted。"""
        import os
        import socket

        pid = manifest.get("owner_pid")
        host = manifest.get("owner_host")
        if not isinstance(pid, int) or not host:
            return False  # 老 run 无 owner 信息：按中断处理（保守，与既有语义一致）
        if host != socket.gethostname():
            return True  # 跨主机无法探测，保守认为对端仍在执行
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False

    def recover_interrupted(self) -> list[str]:
        recovered: list[str] = []
        # 只读回退的历史文件 run 不参与 Redis 状态机恢复，避免在只读副本上写状态。
        for rid in self._client.smembers(self._index_key):
            run_id = rid.decode() if isinstance(rid, bytes) else str(rid)
            manifest = self._manifest(run_id)
            if manifest is None or manifest.get("status") not in ACTIVE_RUN_STATUSES:
                continue
            # DIST-12：owner 仍存活的 run 不得被其他 worker 的启动恢复误标中断。
            if manifest.get("status") == "running" and self._owner_alive(manifest):
                continue
            self.transition_status(run_id, "interrupted")
            recovered.append(run_id)
        self.clear_dangling_signatures()
        return recovered

    def clear_dangling_signatures(self) -> int:
        """删除指向不存在 run 的悬挂签名映射（DIST-07）。

        claim 后进程崩溃会留下 signature→run_id 但 run 从未创建；不清理会让
        后续 claim 永远返回不存在的 run_id。启动时随 recover_interrupted 调用。
        """
        raw = self._client.hgetall(self._signature_key)
        removed = 0
        for signature, run_id in (raw or {}).items():
            target = run_id.decode() if isinstance(run_id, bytes) else str(run_id)
            if self._manifest(target) is None:
                self._client.hdel(self._signature_key, signature)
                removed += 1
        return removed
