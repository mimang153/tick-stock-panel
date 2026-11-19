"""TdxQuantProvider 基础 mixin: 生命周期、鉴权熔断与 realtime/指数快照。"""

from __future__ import annotations

import contextlib
import math
import sys
import time

from app.plugins.tdxquant.client import TdxQuantClient, TdxQuantError

from ._common import (
    _INDEX_CODE_MAP,
    _SNAPSHOT_BATCH,
    _map_snapshot_row,
    _TdxQuantConfig,
    logger,
)

_fp = sys.modules[__package__]


class TdxQuantProviderBase:
    """生命周期 + 鉴权熔断 + realtime; 无跨 mixin super() 调用。"""

    name = "tdxquant"
    builtin = True

    # 鉴权熔断 (issue #515): Key 失效时 SDK 表现为逐只「返回空数据」, 无异常可分。
    # 连续 N 只全空 → 判定凭据级故障, 熔断期内整轮快速失败 (不再逐只调原生库,
    # DLL 打进 stdout 的错误刷屏随之停止)。恢复 = load_all 重建 provider (设置页
    # 重新保存 Key) 或冷却期满自动放行重试。
    _AUTH_BLANK_STREAK = 10
    _AUTH_COOLDOWN_S = 15 * 60.0

    def __init__(self) -> None:
        self.config = _TdxQuantConfig()
        self._client: TdxQuantClient | None = None
        self._auth_failed_at: float | None = None
        self._auth_failure_reason = ""

    def _auth_cooldown_active(self) -> bool:
        if self._auth_failed_at is None:
            return False
        if time.monotonic() - self._auth_failed_at < self._AUTH_COOLDOWN_S:
            return True
        self._auth_failed_at = None  # 冷却期满, 放行重试
        return False

    def _trip_auth_breaker(self, streak: int, first_error: str) -> None:
        self._auth_failed_at = time.monotonic()
        self._auth_failure_reason = first_error
        logger.error(
            "TdxQuant 连续 %d 只标的分钟请求失败, 疑似凭据、权限或额度异常, "
            "已熔断 %d 分钟: 分钟增量/修复轮暂停拉取。请到 设置→数据源 重新保存 "
            "TdxQuant Key (保存即自动恢复)。首个错误: %s",
            streak,
            int(self._AUTH_COOLDOWN_S // 60),
            first_error or "未知",
        )

    @staticmethod
    def _is_blank_error(e: TdxQuantError) -> bool:
        return "返回空数据" in str(e)

    def close(self) -> None:  # loader.load_all 重建注册表时会对每个 provider 调 close
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None

    def _get_client(self) -> TdxQuantClient:
        if self._client is None:
            self._client = TdxQuantClient(api_key=_fp.get_api_key())
        return self._client

    # ---- realtime ----
    def get_realtime(self) -> list[dict]:
        """全市场实时快照 → 内部 realtime records。失败软返回空列表(不阻断轮询)。"""
        client = self._get_client()
        try:
            universe = client.stock_universe()
        except TdxQuantError as e:
            logger.warning("TdxQuant 股票池枚举失败: %s", e)
            return []

        fallback_ms = int(time.time() * 1000)
        records: list[dict] = []
        dropped = failed_batches = 0
        for i in range(0, len(universe), _SNAPSHOT_BATCH):
            codes = universe[i : i + _SNAPSHOT_BATCH]
            try:
                rows = client.snapshot_batch(codes)
            except TdxQuantError as e:
                failed_batches += 1
                logger.warning(
                    "TdxQuant 快照批次失败(%d 只, 第 %d 批): %s",
                    len(codes),
                    i // _SNAPSHOT_BATCH + 1,
                    e,
                )
                continue
            for code, row in rows.items():
                rec = _map_snapshot_row(code, row, fallback_ms)
                if rec is not None:
                    records.append(rec)
                else:
                    dropped += 1
        if failed_batches:
            logger.warning(
                "TdxQuant 快照 %d/%d 批失败, 本轮仅返回 %d 条",
                failed_batches,
                math.ceil(len(universe) / _SNAPSHOT_BATCH),
                len(records),
            )
        if not records and universe:
            logger.warning(
                "TdxQuant 实时行情本轮 0 条(请求 %d 只), 疑似连接或协议异常", len(universe)
            )
        elif dropped:
            logger.info(
                "TdxQuant 实时行情拉取完成: %d 条(丢弃 %d 行无价快照)", len(records), dropped
            )
        return records

    def get_realtime_symbols(
        self, symbols: list[str], *, asset_type: str = "stock"
    ) -> list[dict] | None:
        """Targeted stock/ETF demand, using the same native snapshot and unit mapper."""
        records: list[dict] = []
        fallback_ms = int(time.time() * 1000)
        for i in range(0, len(symbols), _SNAPSHOT_BATCH):
            try:
                rows = self._get_client().snapshot_batch(symbols[i : i + _SNAPSHOT_BATCH])
            except TdxQuantError as exc:
                logger.warning("TdxQuant 订阅行情补拉失败: %s", exc)
                return records or None
            for code, row in rows.items():
                rec = _map_snapshot_row(code, row, fallback_ms)
                if rec is not None:
                    records.append(rec)
        return records

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """指数实时快照(可选插件协议, quote_service 鸭子类型调用)。

        TDX 快照直接支持指数代码; 未映射的代码跳过(上层用本地日K兜底)。
        失败返回 None 保留上轮有效缓存, 成功但无数据返回 []。
        """
        wanted: list[str] = []
        for s in symbols:
            if not s:
                continue
            code = _INDEX_CODE_MAP.get(s, s)
            if code not in wanted:
                wanted.append(code)
        if not wanted:
            return []
        try:
            rows = self._get_client().snapshot_batch(wanted)
        except TdxQuantError as e:
            logger.warning("TdxQuant 指数行情拉取失败: %s", e)
            return None
        fallback_ms = int(time.time() * 1000)
        records = []
        for code, row in rows.items():
            rec = _map_snapshot_row(code, row, fallback_ms)
            if rec is not None:
                records.append(rec)
        logger.info("TdxQuant 指数行情拉取完成: %d 条(请求 %d 只)", len(records), len(wanted))
        return records
