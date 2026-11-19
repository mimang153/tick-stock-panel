"""Keep a provider alive until its last call / streaming iterator completes."""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from typing import Any

from app.data_providers.base import ProviderRequestError
from app.data_providers.minute_stream import with_minute_stream

logger = logging.getLogger(__name__)


class _Stream(Iterator):
    def __init__(self, iterator, release):
        """包装迭代器并持有释放回调。"""
        self.iterator, self.release = iterator, release

    def __next__(self):
        """迭代出错时先释放再抛出，避免在途引用泄漏。"""
        try:
            return next(self.iterator)
        except BaseException:
            self.close()
            raise

    def close(self):
        """幂等释放底层迭代器；重复调用只生效一次。"""
        if self.release is not None:
            release, self.release = self.release, None
            try:
                close = getattr(self.iterator, "close", None)
                if close:
                    close()
            finally:
                release()

    def __del__(self):
        self.close()


class ManagedProvider:
    """Stable handle; swaps do not close in-flight calls or streaming iterators."""

    # loader 加载外部插件包时写入的指纹（鸭子类型属性，声明供类型检查）。
    _external_signature: Any

    builtin = True

    def __init__(self, provider, *, builtin=True):
        self.builtin = builtin
        self._lock = threading.RLock()
        self._current = [with_minute_stream(provider), 0, False]

    def _release(self, entry):
        """递减在途调用计数，归零后关闭待淘汰的 provider。"""
        with self._lock:
            entry[1] -= 1
            close = entry[2] and entry[1] == 0
        if close:
            self._close(entry[0])

    @staticmethod
    def _close(provider):
        try:
            provider.close()
        except Exception:  # noqa: BLE001 — cleanup cannot invalidate the replacement
            logger.warning("provider close failed", exc_info=False)

    def replace(self, provider):
        """原子替换 provider，等在途调用结束后再关闭旧实例。"""
        with self._lock:
            old = self._current
            self._current = [with_minute_stream(provider), 0, False]
            old[2] = True
            close = old[1] == 0
        if close:
            self._close(old[0])

    def close(self):
        """标记停止并关闭当前 provider（幂等）。"""
        with self._lock:
            old = self._current
            if old[2]:
                return
            old[2] = True
            close = old[1] == 0
        if close:
            self._close(old[0])

    def __getattr__(self, name):
        """代理方法调用，跟踪在途数并安全关闭流式结果。"""
        with self._lock:
            value = getattr(self._current[0], name)
        if not callable(value):
            return value

        def invoke(*args, **kwargs):
            with self._lock:
                entry = self._current
                if entry[2]:
                    raise ProviderRequestError("数据源已停止，请重新选择数据源")
                method = getattr(entry[0], name)
                entry[1] += 1
            try:
                result = method(*args, **kwargs)
            except BaseException:
                self._release(entry)
                raise
            if isinstance(result, Iterator):
                return _Stream(result, lambda: self._release(entry))
            self._release(entry)
            return result

        return invoke
