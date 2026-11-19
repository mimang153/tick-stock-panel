"""FuyaoProvider 基础能力 mixin: 生命周期、dump 缓存与 realtime 数据集。"""

from __future__ import annotations

import contextlib
import sys
import time
from datetime import date
from pathlib import Path

import polars as pl

from app.plugins.fuyao.client import FuyaoClient, FuyaoError

from ._common import (
    _DAILY10_DUMP_KIND,
    _DAILY_DUMP_KIND,
    _dump_covers,
    _dump_date_range,
    _FuyaoConfig,
    _map_snapshot_row,
    _release_of,
    logger,
)

_fp = sys.modules[__package__]


class FuyaoProviderBase:
    """生命周期 + dump 缓存 + realtime; 无 __init__ 之外的跨 mixin super() 调用。"""

    name = "fuyao"
    builtin = True

    def __init__(self) -> None:
        self.config = _FuyaoConfig()
        self._client: FuyaoClient | None = None
        self._dump_memo: dict[str, pl.DataFrame] = {}
        self._dump_path_memo: dict[str, Path] = {}

    def close(self) -> None:  # loader.load_all 重建注册表时会对每个 provider 调 close
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.close()
            self._client = None
        self._dump_memo.clear()
        self._dump_path_memo.clear()

    def _get_client(self) -> FuyaoClient:
        if self._client is None:
            self._client = _fp.fuyao_client.FuyaoClient(api_key=_fp.get_api_key())
        return self._client

    # ---- dump 缓存 ----
    def _ensure_dump_path(self, dump_kind: str, cache_prefix: str) -> Path:
        """确保最新 release 的 dump 已落盘, 返回缓存路径(大文件不整读进内存)。

        release 号取自预签名 URL 的 releases/<date>/ 路径; 新 release 落盘后清理旧版缓存。
        """
        memo = self._dump_path_memo.get(dump_kind)
        if memo is not None and memo.exists():
            return memo
        client = self._get_client()
        info = client.dump_download_url(dump_kind)
        release = _release_of(str(info.get("presigned_url") or ""))
        dest = _fp._cache_dir() / f"{cache_prefix}__{release}.parquet"
        if not dest.exists():
            client.download_dump(dump_kind, dest)
            for old in dest.parent.glob(f"{cache_prefix}__*.parquet"):
                if old.name != dest.name:
                    old.unlink(missing_ok=True)
            logger.info("扶摇 dump %s(release %s)已下载: %s", dump_kind, release, dest.name)
        self._dump_path_memo[dump_kind] = dest
        return dest

    def _ensure_dump(self, dump_kind: str, cache_prefix: str) -> pl.DataFrame:
        """小体量 dump(快照 10d / 因子)整读 + 进程内 memo, 避免重复打接口/读盘。"""
        memo = self._dump_memo.get(dump_kind)
        if memo is not None:
            return memo
        df = pl.read_parquet(self._ensure_dump_path(dump_kind, cache_prefix))
        self._dump_memo[dump_kind] = df
        return df

    def _daily10_for_window(self, start_d: date, end_d: date) -> pl.DataFrame:
        """近端缓存覆盖不足时检查新 release, 避免常驻进程永久降级逐票拉取。"""
        kind = _DAILY10_DUMP_KIND
        cached = kind in self._dump_memo or kind in self._dump_path_memo
        dump = self._ensure_dump(kind, "daily_k_10d")
        if cached and not _dump_covers(dump, start_d, end_d):
            # 帧和路径必须一起失效, 否则仍会读回旧 release 的磁盘文件。
            self._dump_memo.pop(kind, None)
            self._dump_path_memo.pop(kind, None)
            logger.info("扶摇近端日K缓存未覆盖 [%s ~ %s], 检查最新 dump", start_d, end_d)
            dump = self._ensure_dump(kind, "daily_k_10d")
        return dump

    def _ensure_daily_big_dump(self, start_d: date | None) -> Path | None:
        """10 年全量日K dump(约 172MB)。要求覆盖窗口起点时校验; 末端缺口由 10d dump 补。

        start_d=None 表示不校验覆盖(配价场景): 只要拿到 dump 本身即可,
        个别覆盖不到的标的由 get_adj_factors 逐只回退 — 避免"全市场最早的
        一个远古事件早于 dump 起点"就把整轮配价打回逐标的接口。

        已有缓存覆盖起点就直接复用, 不追新 release(避免深窗口高频触发时日日重下
        172MB — 旧 release 的中段历史不会变, 尾部新鲜度交给 1MB 的 10d dump)。
        """
        cached = sorted(_fp._cache_dir().glob("daily_k__*.parquet"), reverse=True)
        if start_d is None:
            if cached:
                return cached[0]
            try:
                return self._ensure_dump_path(_DAILY_DUMP_KIND, "daily_k")
            except FuyaoError as e:
                logger.warning("扶摇 10 年 dump 不可用, 回退单标的接口: %s", e)
                return None
        for f in cached:
            try:
                dmin, _ = _dump_date_range(f)
            except Exception:  # noqa: BLE001 - dump缓存损坏不致命, 换下一个候选或重拉
                continue
            if dmin is not None and start_d >= dmin:
                return f
        try:
            path = self._ensure_dump_path(_DAILY_DUMP_KIND, "daily_k")
        except FuyaoError as e:
            logger.warning("扶摇 10 年 dump 不可用, 回退单标的接口: %s", e)
            return None
        dmin, _ = _dump_date_range(path)
        if dmin is None or start_d < dmin:
            return None  # 窗口比 10 年更早 → 单标的兜底
        return path

    # ---- realtime ----
    def get_realtime(self) -> list[dict]:
        """全市场实时快照 → 内部 realtime records。失败软返回空列表(不阻断轮询)。"""
        try:
            rows, server_ts = self._get_client().snapshot_all()
        except FuyaoError as e:
            logger.warning("扶摇实时行情拉取失败: %s", e)
            return []

        # 优先用服务端时间戳(行情归属); 缺失时退回本地时间
        fetched_ms = server_ts or int(time.time() * 1000)

        records = []
        dropped = 0
        for row in rows:
            rec = _map_snapshot_row(row, fetched_ms)
            if rec is not None:
                records.append(rec)
            else:
                dropped += 1
        if dropped and not records:
            # 整页都识别不出 thscode → 大概率接口 schema 变了, 明确告警而非静默空数据
            logger.warning("扶摇快照 %d 行全部缺少 thscode 字段, 疑似接口结构变化", dropped)
            return []
        logger.info("扶摇实时行情拉取完成: %d 条(丢弃 %d 行)", len(records), dropped)
        return records

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """指数实时快照 → 内部 realtime record (可选插件协议, quote_service 鸭子类型调用)。

        A 股快照不含指数, 指数在扶摇是独立端点; 覆盖沪深交易所指数 + 同花顺板块,
        无北交所 (未知代码会整批 1002 连坐, .BJ 直接跳过)。失败返回 None,
        让上层与"成功但无数据"的空列表区分, 保留上轮有效指数缓存。
        """
        wanted = [s for s in symbols if s and not s.upper().endswith(".BJ")]
        if not wanted:
            return []
        try:
            rows, server_ts = self._get_client().index_snapshot(wanted)
        except FuyaoError as e:
            logger.warning("扶摇指数行情拉取失败: %s", e)
            return None

        fetched_ms = server_ts or int(time.time() * 1000)
        records = []
        for row in rows:
            rec = _map_snapshot_row(row, fetched_ms, volume_to_hand=False)
            if rec is not None:
                records.append(rec)
        logger.info("扶摇指数行情拉取完成: %d 条(请求 %d 只)", len(records), len(wanted))
        return records
