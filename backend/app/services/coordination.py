"""多实例协调状态：跨进程取消标志与全局重任务计数。

默认全部走本地空实现，既有 ``threading.Event`` / 进程内计数器语义不变；
配置 ``COORDINATION_BACKEND=redis`` 后，取消标志与容量计数对多实例可见。
取消标志只负责跨进程可见性，本地 Event 仍保留用于即时唤醒，两者互补。
"""

from __future__ import annotations

import json
import time
import uuid
from contextlib import contextmanager
from typing import Any, Protocol

from app.config import settings

_REGISTER_TTL_S = 3600  # job 注册租约，覆盖最长任务 1800s
_CANCEL_TTL_S = 1800
_COUNTER_TTL_S = 3600  # 进程崩溃后计数兜底回收
_MAX_CAS_RETRIES = 16  # WATCH 冲突重试上限，避免异常 client 导致死循环
_RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class CoordinationUnavailableError(RuntimeError):
    """Redis 协调层不可用时直接抛出，禁止静默降级为本地语义。"""

    def __init__(self, operation: str, cause: BaseException) -> None:
        super().__init__(
            f"Redis 协调层不可用（COORDINATION_BACKEND=redis, "
            f"COORDINATION_REDIS_URL={settings.coordination_redis_url}）："
            f"{operation} 失败：{cause}。请确认 Redis 可达后再操作。"
        )
        self.cause = cause


class CancelFlagBackend(Protocol):
    """取消标志契约：注册 job、置取消、检查、清理。"""

    def register(self, job_id: str) -> None: ...

    def set(self, job_id: str) -> bool: ...

    def is_set(self, job_id: str) -> bool: ...

    def clear(self, job_id: str) -> None: ...


class LimiterCounterBackend(Protocol):
    """全局容量计数契约：try_acquire 成功时立即计数，release 归还。"""

    def try_acquire(self, weight: int, limit: int) -> bool: ...

    def release(self, weight: int) -> None: ...

    def used(self) -> int: ...

    def restore(self, used: int) -> None: ...


class MemoryCancelFlags:
    """本地空实现：进程内 threading.Event 已是权威。"""

    def register(self, job_id: str) -> None:
        return None

    def set(self, job_id: str) -> bool:
        return False

    def is_set(self, job_id: str) -> bool:
        return False

    def clear(self, job_id: str) -> None:
        return None


class RedisCancelFlags:
    """Redis 取消标志：按 job_id 的 SETEX + EXISTS，本地 Event 仍并行维护。"""

    def __init__(self, client: Any, prefix: str) -> None:
        self._client = client
        self._register_prefix = f"{prefix}job:"
        self._cancel_prefix = f"{prefix}cancel:"

    def register(self, job_id: str) -> None:
        try:
            self._client.setex(self._register_prefix + job_id, _REGISTER_TTL_S, b"1")
        except Exception as exc:
            raise CoordinationUnavailableError(f"注册 job {job_id}", exc) from exc

    def set(self, job_id: str) -> bool:
        try:
            # 跨实例取消时本地无 flag，仍需注册标记供其他实例感知。
            self._client.setex(self._register_prefix + job_id, _REGISTER_TTL_S, b"1")
            self._client.setex(self._cancel_prefix + job_id, _CANCEL_TTL_S, b"1")
            return True
        except Exception as exc:
            raise CoordinationUnavailableError(f"设置 job {job_id} 取消标志", exc) from exc

    def is_set(self, job_id: str) -> bool:
        try:
            return bool(self._client.exists(self._cancel_prefix + job_id))
        except Exception as exc:
            raise CoordinationUnavailableError(f"读取 job {job_id} 取消标志", exc) from exc

    def clear(self, job_id: str) -> None:
        try:
            self._client.delete(self._cancel_prefix + job_id, self._register_prefix + job_id)
        except Exception as exc:
            raise CoordinationUnavailableError(f"清理 job {job_id} 取消标志", exc) from exc


class MemoryLimiterCounter:
    """本地空实现：容量完全由进程内 HeavyJobLimiter 管理。"""

    def try_acquire(self, weight: int, limit: int) -> bool:
        return True

    def release(self, weight: int) -> None:
        return None

    def used(self) -> int:
        return 0

    def restore(self, used: int) -> None:
        return None


class RedisLimiterCounter:
    """Redis 全局计数：WATCH + MULTI 原子比较后 INCRBY，TTL 防崩溃泄漏。"""

    def __init__(self, client: Any, key: str, *, ttl: int = _COUNTER_TTL_S) -> None:
        self._client = client
        self._key = key
        self._ttl = ttl

    def try_acquire(self, weight: int, limit: int) -> bool:
        import redis

        for _attempt in range(_MAX_CAS_RETRIES):
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(self._key)
                    current = int(pipe.get(self._key) or 0)
                    if current + weight > limit:
                        pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.incrby(self._key, weight)
                    pipe.expire(self._key, self._ttl)
                    pipe.execute()
                    return True
            except redis.WatchError:
                continue
            except Exception as exc:
                raise CoordinationUnavailableError("获取全局容量", exc) from exc
        raise CoordinationUnavailableError(
            "获取全局容量", RuntimeError(f"WATCH 冲突超过 {_MAX_CAS_RETRIES} 次")
        )

    def release(self, weight: int) -> None:
        import redis

        for _attempt in range(_MAX_CAS_RETRIES):
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(self._key)
                    current = int(pipe.get(self._key) or 0)
                    pipe.multi()
                    if current <= weight:
                        pipe.delete(self._key)
                    else:
                        pipe.decrby(self._key, weight)
                        pipe.expire(self._key, self._ttl)
                    pipe.execute()
                    return
            except redis.WatchError:
                continue
            except Exception as exc:
                raise CoordinationUnavailableError("归还全局容量", exc) from exc
        raise CoordinationUnavailableError(
            "归还全局容量", RuntimeError(f"WATCH 冲突超过 {_MAX_CAS_RETRIES} 次")
        )

    def used(self) -> int:
        try:
            return int(self._client.get(self._key) or 0)
        except Exception as exc:
            raise CoordinationUnavailableError("读取全局容量", exc) from exc

    def restore(self, used: int) -> None:
        """重建时直接写入容量值（覆盖式，仅在启动恢复阶段调用）。"""
        try:
            if used <= 0:
                self._client.delete(self._key)
            else:
                self._client.set(self._key, used, ex=self._ttl)
        except Exception as exc:
            raise CoordinationUnavailableError("重建全局容量", exc) from exc


class JobStateBackend(Protocol):
    """任务状态注册表契约：本地快照之外提供跨实例 get/list。"""

    def put(self, job_id: str, job: dict[str, Any]) -> None: ...

    def get(self, job_id: str) -> dict[str, Any] | None: ...

    def recent(self, limit: int) -> list[dict[str, Any]]: ...

    def clear(self) -> None: ...


class MemoryJobState:
    """本地空实现：磁盘快照与内存摘要已是权威。"""

    def put(self, job_id: str, job: dict[str, Any]) -> None:
        return None

    def get(self, job_id: str) -> dict[str, Any] | None:
        return None

    def recent(self, limit: int) -> list[dict[str, Any]]:
        return []

    def clear(self) -> None:
        return None


class RedisJobState:
    """Redis Hash + ZSET：跨实例共享 job 终态与最近列表。"""

    def __init__(self, client: Any, prefix: str) -> None:
        self._client = client
        self._hash = f"{prefix}jobs"
        self._index = f"{prefix}jobs:index"

    def put(self, job_id: str, job: dict[str, Any]) -> None:
        try:
            pipe = self._client.pipeline()
            pipe.hset(self._hash, job_id, json.dumps(job, ensure_ascii=False))
            pipe.zadd(self._index, {job_id: time.time()})
            pipe.execute()
        except Exception as exc:
            raise CoordinationUnavailableError(f"写入任务 {job_id} 状态", exc) from exc

    def get(self, job_id: str) -> dict[str, Any] | None:
        try:
            raw = self._client.hget(self._hash, job_id)
            return json.loads(raw) if raw else None
        except Exception as exc:
            raise CoordinationUnavailableError(f"读取任务 {job_id} 状态", exc) from exc

    def recent(self, limit: int) -> list[dict[str, Any]]:
        try:
            ids = self._client.zrevrange(self._index, 0, max(1, limit) - 1)
            if not ids:
                return []
            return [json.loads(raw) for raw in self._client.hmget(self._hash, *ids) if raw]
        except Exception as exc:
            raise CoordinationUnavailableError("列举任务状态", exc) from exc

    def clear(self) -> None:
        try:
            self._client.delete(self._hash, self._index)
        except Exception as exc:
            raise CoordinationUnavailableError("清理任务状态", exc) from exc


class Coordination:
    """按配置装配取消标志、全局计数器、任务状态与启动单飞标记。"""

    def __init__(self) -> None:
        self.cancel_flags: CancelFlagBackend = MemoryCancelFlags()
        self.limiter_counter: LimiterCounterBackend = MemoryLimiterCounter()
        self.job_state: JobStateBackend = MemoryJobState()
        self._once_client: Any | None = None
        self._once_prefix = ""
        self._memory_once: set[str] = set()
        self._memory_locks: dict[str, bytes] = {}
        self._fence_seq = 0  # memory 后端的 fencing 序号（离线兼容）
        if settings.coordination_backend != "redis":
            return
        import redis

        client = redis.Redis.from_url(
            settings.coordination_redis_url,
            decode_responses=False,
            socket_timeout=settings.coordination_redis_socket_timeout_s,
        )
        try:
            client.ping()
        except Exception as exc:
            raise CoordinationUnavailableError("启动连接检查", exc) from exc
        prefix = settings.coordination_redis_prefix
        self.cancel_flags = RedisCancelFlags(client, prefix)
        self.limiter_counter = RedisLimiterCounter(client, f"{prefix}limiter")
        self.job_state = RedisJobState(client, prefix)
        self._once_client = client
        self._once_prefix = f"{prefix}once:"

    def once(self, key: str, ttl_s: int = _REGISTER_TTL_S) -> bool:
        """跨 worker 一次性执行标记：True 表示当前进程获得执行权。

        Redis 模式用 SET NX PX（TTL 兜底崩溃后重试）；memory 模式单 worker，
        进程内去重即可。startup 钩子用 ``startup:<name>`` 作为 key。
        """
        if self._once_client is None:
            if key in self._memory_once:
                return False
            self._memory_once.add(key)
            return True
        try:
            return bool(
                self._once_client.set(self._once_prefix + key, b"1", nx=True, px=ttl_s * 1000)
            )
        except Exception as exc:
            raise CoordinationUnavailableError(f"once 标记 {key}", exc) from exc

    def acquire_lock(self, key: str, ttl_s: int = 600) -> bytes | None:
        """获取可释放的跨实例租约；返回 token 表示当前调用者持有。

        token 形如 ``<全局单调序号>:<uuid>``：序号由 Redis INCR 生成，供
        被保护资源做 fencing（迟到的旧 owner 序号更小可被拒，DIST-13）；
        uuid 保证释放时比对唯一。memory 后端序号为进程内计数（离线兼容）。
        """
        if self._once_client is None:
            if key in self._memory_locks:
                return None
            self._fence_seq += 1
            token = f"{self._fence_seq}:{uuid.uuid4().hex}".encode()
            self._memory_locks[key] = token
            return token
        try:
            seq = self._once_client.incr(f"{self._once_prefix}fence_seq")
            token = f"{seq}:{uuid.uuid4().hex}".encode()
        except Exception as exc:
            raise CoordinationUnavailableError(f"生成 fencing 序号 {key}", exc) from exc
        lock_key = f"{self._once_prefix}lock:{key}"
        try:
            acquired = self._once_client.set(lock_key, token, nx=True, px=ttl_s * 1000)
        except Exception as exc:
            raise CoordinationUnavailableError(f"获取租约 {key}", exc) from exc
        return token if acquired else None

    @staticmethod
    def fence_sequence(token: bytes | None) -> int | None:
        """从租约 token 提取 fencing 序号；被保护资源据此拒绝迟到的旧 owner。"""
        if token is None:
            return None
        prefix = token.split(b":", 1)[0]
        try:
            return int(prefix)
        except ValueError:
            return None

    def release_lock(self, key: str, token: bytes | None) -> None:
        """释放自己持有的租约，避免误删其他实例的锁。"""
        if token is None:
            return
        if self._once_client is None:
            self._memory_locks.pop(key, None)
            return
        lock_key = f"{self._once_prefix}lock:{key}"
        try:
            self._once_client.eval(_RELEASE_LOCK_LUA, 1, lock_key, token)
        except Exception as exc:
            raise CoordinationUnavailableError(f"释放租约 {key}", exc) from exc

    @contextmanager
    def lock(self, key: str, ttl_s: int = 600):
        """上下文形式获取并自动释放租约；yield 是否获得执行权。"""
        token = self.acquire_lock(key, ttl_s)
        try:
            yield token is not None
        finally:
            self.release_lock(key, token)


coordination = Coordination()
