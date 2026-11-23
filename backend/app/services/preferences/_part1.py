"""用户偏好设置拆分 part 1（原 app.services.preferences）。"""

from __future__ import annotations

import re

import app.services.preferences as _self

from ._state import (
    _ALLOWED_DATA_PROVIDERS,
    _BACKTEST_RSS_LIMIT_GIB_DEFAULT,
    _BACKTEST_RSS_LIMIT_GIB_MAX,
    _INTRADAY_REFRESH_INTERVAL_MAX,
    _INTRADAY_REFRESH_INTERVAL_MIN,
    _MAINLINE_MAX_MEMBERS_MAX,
    _MAINLINE_MAX_MEMBERS_MIN,
    _MAINLINE_MIN_MEMBERS_MAX,
    _MAINLINE_MIN_MEMBERS_MIN,
    _MINUTE_REFRESH_INTERVAL_MAX,
    _MINUTE_REFRESH_INTERVAL_MIN,
    _MONITOR_EXT_FIELDS_DEFAULT,
    _PIPELINE_PULL_KEYS,
    _REGIME_BATCH_DAYS_MAX,
    _REGIME_BATCH_DAYS_MIN,
    _REGIME_WARMUP_DAYS_MAX,
    _REGIME_WARMUP_DAYS_MIN,
    _SAVE_LOCK,
    DATA_SOURCE_JOB_TIMEOUT_MIN_S,
    logger,
)


def get_realtime_quotes_enabled() -> bool:
    return _self.load().get("realtime_quotes_enabled", False)


def get_watchlist_groups_in_nav() -> bool:
    """自选分组是否显示在侧边栏（可展开二级子菜单）。默认 False。"""
    return _self.load().get("watchlist_groups_in_nav", False)


def get_realtime_quote_interval() -> float:
    return _self.load().get("realtime_quote_interval", 6.0)


def set_realtime_quote_interval(interval: float) -> float:
    """保存行情轮询间隔（不在此做 min/max 校验，由调用方按档位限制）。

    走 _self.save() 而不是自己 load + write_text: 锁外的 read-modify-write 会用旧快照
    整体覆盖文件, 把并发写入的另一个偏好丢掉 (见 save 的 docstring)。
    """
    _self.save({"realtime_quote_interval": interval})
    return interval


def get_minute_sync_enabled() -> bool:
    return _self.load().get("minute_sync_enabled", False)


def get_minute_intraday_refresh() -> bool:
    """自选列表分时图是否跟随实时行情刷新。

    默认值随权限: 有实时行情权限 (Pro+) 的用户默认开启, 否则关闭。
    用户主动设置过的 (key 存在) 以用户选择为准, 即使是 False 也尊重。
    """
    data = _self.load()
    if "minute_intraday_refresh" in data:
        return bool(data["minute_intraday_refresh"])
    # 未设置过: 有权限默认开, 无权限默认关。
    try:
        from app.services.quote_service import QuoteService

        return QuoteService.is_realtime_allowed()
    except Exception:  # noqa: BLE001 - 配置探测失败按不可用处理, 保持静默降级
        return False


# 分时图实时刷新间隔允许范围 (秒)。下限 3s, 上限 60s。


def get_minute_intraday_refresh_interval() -> int:
    """分时图实时刷新轮询间隔 (秒)。默认 6s, 范围 [3, 60]。"""
    return max(
        _INTRADAY_REFRESH_INTERVAL_MIN,
        min(
            _INTRADAY_REFRESH_INTERVAL_MAX,
            int(_self.load().get("minute_intraday_refresh_interval", 6)),
        ),
    )


# 监控中心个股通知 ext 字段默认配置 (与 ext_presets 内置预设对齐)


def _normalize_ext_field(raw) -> dict | None:
    """规范化单个 ext 字段配置, 兼容旧字符串格式 ("id.field") 和新对象格式。

    新格式: {"field": "id.field", "maxTags": N, "hiddenIndices": [...]}
    maxTags=0 或缺省=不限制; hiddenIndices 指定要隐藏的位置 (0-based)。
    """
    if raw is None:
        return None
    # 旧格式: 纯字符串 "configId.fieldName"
    if isinstance(raw, str):
        return {"field": raw}
    if isinstance(raw, dict):
        field = raw.get("field")
        if not field:
            return None
        return {
            "field": field,
            "maxTags": int(raw["maxTags"]) if raw.get("maxTags") else 0,
            "hiddenIndices": [int(i) for i in raw["hiddenIndices"]]
            if raw.get("hiddenIndices")
            else [],
        }
    return None


def get_monitor_ext_fields() -> dict:
    """监控中心个股通知要展示的 ext 字段 (concept/industry)。

    返回 {"concept": {"field", "maxTags", "hiddenIndices"} | None, ...}。
    后端只需读 .field 构建 ext_columns; maxTags/hiddenIndices 供前端渲染裁剪。
    兼容旧字符串格式 ("id.field") 自动升级。
    """
    data = _self.load()
    raw = data.get("monitor_ext_fields")
    if raw is None:
        return {
            "concept": {"field": _MONITOR_EXT_FIELDS_DEFAULT["concept"]},
            "industry": {"field": _MONITOR_EXT_FIELDS_DEFAULT["industry"]},
        }
    return {
        "concept": _self._normalize_ext_field(raw.get("concept")),
        "industry": _self._normalize_ext_field(raw.get("industry")),
    }


def get_minute_sync_days() -> int:
    return max(1, min(30, _self.load().get("minute_sync_days", 5)))


def get_minute_sync_segment_days() -> int:
    """分钟 K 拉取的单段大小(交易日)。默认 20,范围 [5, 30]。

    每段拉完后立即落盘(流式),避免全量攒内存导致 OOM。
    段越小内存峰值越低但总耗时越长(限速 sleep 随段数线性增加);
    物理上限 ~41 交易日(TickFlow 单次 10000 根 / 一天 241 根 ≈ 41 天),max=30 留出余量。
    """
    return max(5, min(30, _self.load().get("minute_sync_segment_days", 20)))


# ===== 盘中分钟增量刷新 (Expert 专有) =====

# 稳态轮为 intraday.universe 单请求增量, 无脉冲并发, 间隔可低至 3s;
# 全天修复轮 (intraday.batch 28 块爆发) 的 rpm 安全与间隔无关, 由轮次
# 调度 max(间隔, 单轮完成) 天然防重叠。


def get_minute_refresh_enabled() -> bool:
    """盘中分钟K增量落盘开关。默认关闭; 能力门控 (Expert) 在服务层判断。"""
    return bool(_self.load().get("minute_refresh_enabled", False))


def get_minute_refresh_interval() -> int:
    """盘中分钟增量刷新间隔(秒)。默认 6,范围 [3, 120]。"""
    return max(
        _MINUTE_REFRESH_INTERVAL_MIN,
        min(_MINUTE_REFRESH_INTERVAL_MAX, int(_self.load().get("minute_refresh_interval", 6))),
    )


# ===== 数据源选择 (默认 TickFlow；第一阶段仅日K切换入口) =====


def get_data_source_job_timeout_s() -> int:
    """返回普通数据后台任务的卡死判定时间(秒)。"""
    from app.services.pipeline_jobs import DEFAULT_JOB_TIMEOUT_S

    raw = _self.load().get("data_source_job_timeout_s", DEFAULT_JOB_TIMEOUT_S)
    try:
        timeout_s = int(raw)
    except (TypeError, ValueError):
        timeout_s = DEFAULT_JOB_TIMEOUT_S
    return max(DATA_SOURCE_JOB_TIMEOUT_MIN_S, timeout_s)


def get_data_source_long_job_timeout_s() -> int:
    """返回分钟 K 全市场等长任务的卡死判定时间(秒)。"""
    from app.services.pipeline_jobs import LONG_JOB_TIMEOUT_S

    raw = _self.load().get(
        "data_source_long_job_timeout_s",
        LONG_JOB_TIMEOUT_S,
    )
    try:
        timeout_s = int(raw)
    except (TypeError, ValueError):
        timeout_s = LONG_JOB_TIMEOUT_S
    return max(DATA_SOURCE_JOB_TIMEOUT_MIN_S, timeout_s)


def get_minute_batch_compress() -> bool:
    """分时详情与批量响应是否启用 gzip 传输压缩。默认开启 (公网部署传输是大头);
    本机/内网可关闭省服务端 CPU。每次请求即时读取, 开关保存后立即生效。
    """
    raw = _self.load().get("minute_batch_compress", True)
    return bool(raw)


def get_daily_batch_compress() -> bool:
    """日K详情与批量响应是否启用 gzip 传输压缩 (与分时各自独立配置)。默认开启。"""
    raw = _self.load().get("daily_batch_compress", True)
    return bool(raw)


# ===== 批量端点单请求标的数上限 (daily-batch / minute-batch) =====
# 这两个上限是「单请求成本」的闸门, 不是业务契约: 成本 ≈ 标的数 × 每只根数
# (日K 约 12 根, 分时约 240 根, 差 20 倍), 所以两档独立配置而非共用一个数。
# 上限同时也是前端的单批分块大小 —— 前端读同一偏好分块, 不会发出必然被拒的
# 请求 (此前 400/300/300 三处硬编码, 任一处不一致即全站 400)。
def get_daily_batch_symbols() -> int:
    """daily-batch 单请求标的数上限。默认 2000, 范围 [50, 5000]。"""
    raw = _self.load().get("daily_batch_symbols", 400)
    try:
        size = int(raw)
    except (TypeError, ValueError):
        size = 400
    return max(50, min(5000, size))


def set_daily_batch_symbols(size: int) -> int:
    size = max(50, min(5000, int(size)))
    _self.save({"daily_batch_symbols": size})
    return size


def get_minute_batch_symbols() -> int:
    """minute-batch 单请求标的数上限。默认 400, 范围 [50, 2000]。

    分时每只回 ~240 根 (日K ~12 根), 同样的标的数内存/传输量高一个量级,
    因此上限与推荐区间都比日K保守。
    """
    raw = _self.load().get("minute_batch_symbols", 400)
    try:
        size = int(raw)
    except (TypeError, ValueError):
        size = 400
    return max(50, min(2000, size))


def set_minute_batch_symbols(size: int) -> int:
    size = max(50, min(2000, int(size)))
    _self.save({"minute_batch_symbols": size})
    return size


# ===== 回测/优化 worker RSS 上限 (GiB) =====
# 环境变量 TICK_WORKER_RSS_LIMIT_BYTES 仍是更高优先级的部署级覆盖 (见
# app/backtest/worker.py 的解析顺序); 这里只承接设置页的界面化配置。


def get_backtest_worker_rss_limit_gib() -> int:
    """回测/优化独立 worker 的 RSS 上限 (GiB)。默认 6, 范围 [0, 64], 0 = 不限制。

    单位是 GiB 而非字节: 界面与报错文案均以 GiB 呈现, 换算只发生在
    worker.py 的字节级解析处, 避免两套单位并存。
    """
    raw = _self.load().get("backtest_worker_rss_limit_gib", _BACKTEST_RSS_LIMIT_GIB_DEFAULT)
    try:
        gib = int(raw)
    except (TypeError, ValueError):
        gib = _BACKTEST_RSS_LIMIT_GIB_DEFAULT
    return max(0, min(_BACKTEST_RSS_LIMIT_GIB_MAX, gib))


def set_backtest_worker_rss_limit_gib(gib: int) -> int:
    gib = max(0, min(_BACKTEST_RSS_LIMIT_GIB_MAX, int(gib)))
    _self.save({"backtest_worker_rss_limit_gib": gib})
    return gib


def _allowed_data_providers() -> set[str]:
    try:
        from app.data_providers import custom as custom_sources

        return _ALLOWED_DATA_PROVIDERS | custom_sources.names()
    except Exception:  # noqa: BLE001
        return set(_ALLOWED_DATA_PROVIDERS)


def _validated_provider(raw: str, pref_key: str) -> str:
    """数据源偏好白名单校验。非法/已卸载的源回退 tickflow 并告警。

    静默回退曾让「配了 fuyao 却全程走免费档」无从排查 (fuyao 事件的
    定位难点之一): 回退必须留痕。
    """
    if raw in _self._allowed_data_providers():
        return raw
    logger.warning(
        "数据源偏好 %s=%r 不在可用列表 %s, 回退 tickflow",
        pref_key,
        raw,
        sorted(_self._allowed_data_providers()),
    )
    return "tickflow"


def get_daily_data_provider() -> str:
    provider = str(_self.load().get("daily_data_provider", "tickflow") or "tickflow").lower()
    return _self._validated_provider(provider, "daily_data_provider")


def get_adj_factor_provider() -> str:
    # 「跟随日K」(same_as_daily) 特殊值已下线: 存量配置里的旧值按非法值回退 tickflow
    provider = str(_self.load().get("adj_factor_provider", "tickflow") or "tickflow").lower()
    return _self._validated_provider(provider, "adj_factor_provider")


def get_minute_data_provider() -> str:
    provider = str(_self.load().get("minute_data_provider", "tickflow") or "tickflow").lower()
    return _self._validated_provider(provider, "minute_data_provider")


def get_full_minute_data_provider() -> str:
    provider = str(_self.load().get("full_minute_data_provider", "tickflow") or "tickflow").lower()
    return _self._validated_provider(provider, "full_minute_data_provider")


def get_depth5_data_provider() -> str:
    provider = str(_self.load().get("depth5_data_provider", "tickflow") or "tickflow").lower()
    return _self._validated_provider(provider, "depth5_data_provider")


def get_realtime_data_provider() -> str:
    provider = str(_self.load().get("realtime_data_provider", "tickflow") or "tickflow").lower()
    return _self._validated_provider(provider, "realtime_data_provider")


def get_financial_provider() -> str:
    provider = str(_self.load().get("financial_data_provider", "tickflow") or "tickflow").lower()
    return _self._validated_provider(provider, "financial_data_provider")


# ===== 盘后管道拉取内容开关 (A股 / ETF / 指数 独立控制) =====


def get_pipeline_pull_a_share() -> bool:
    """是否拉取 A 股日K。默认 True。"""
    return _self.load().get("pipeline_pull_a_share", True)


def get_pipeline_pull_etf() -> bool:
    """是否拉取 ETF 日K。默认 False(标的多,首次较慢)。"""
    return _self.load().get("pipeline_pull_etf", False)


def get_pipeline_pull_index() -> bool:
    """是否拉取指数日K。默认 True。"""
    return _self.load().get("pipeline_pull_index", True)


def get_pipeline_regime_enabled() -> bool:
    """盘后管道是否自动计算市场环境(regime)。默认 False。

    regime 是本地聚合计算(非拉取), 首次/regime 表为空时需全量回填多日,
    内存与耗时较高, 故默认关闭; 用户可在数据页「市场环境」卡片设置里开启,
    或直接在该页面点「重算」手动触发(不受此开关影响)。
    """
    return _self.load().get("pipeline_regime_enabled", False)


# regime 全量回填分批参数范围:
# - batch_days: 每批目标交易日数。越小内存越省、批次越多越慢; ma20 需 20 交易日,
#   故下限 25(留 warmup 余量), 上限 500(约 2 年)。
# - warmup_days: 每批前缀预热天数(日历日), 必须 > ma20 的 20 交易日(≈28 日历日),
#   下限 35 留余量, 上限 90。


def get_regime_batch_days() -> int:
    """regime 全量回填每批目标交易日数。默认 60(约一季度)。

    超过此天数的范围会被切成多批, 每批独立算指标后拼接, 控制内存峰值。
    """
    v = _self.load().get("regime_batch_days", 60)
    try:
        return max(_REGIME_BATCH_DAYS_MIN, min(_REGIME_BATCH_DAYS_MAX, int(v)))
    except (TypeError, ValueError):
        return 60


def get_regime_warmup_days() -> int:
    """regime 分批每批的 warmup 前缀日历天数。默认 40。

    用于预热 ma20 等滚动窗口指标, 使每批边界计算正确。必须 > 20 交易日。
    """
    v = _self.load().get("regime_warmup_days", 40)
    try:
        return max(_REGIME_WARMUP_DAYS_MIN, min(_REGIME_WARMUP_DAYS_MAX, int(v)))
    except (TypeError, ValueError):
        return 40


# ── 市场主线(概念/行业涨停梯队)过滤 ──
# 宽基/风格标签(融资融券 ~7700 成分、深股通/沪股通 ~3300-3700、国企改革 ~2900)
# 会按"家数"霸占主线榜首, 但它们不是可操作的题材主线。默认按成分股数上限过滤。
# 标定(2026-08 THS 概念): 成员 >600 的 55 个概念几乎全是此类风格标签,
# 真实题材(华为概念 2006/人工智能 2166/固态电池等)均在 600 以下或可自行调整。


def get_mainline_max_members() -> int:
    """主线维度成员数上限, 超过视为宽基/风格标签被过滤。默认 600。"""
    v = _self.load().get("mainline_max_members", 600)
    try:
        return max(_MAINLINE_MAX_MEMBERS_MIN, min(_MAINLINE_MAX_MEMBERS_MAX, int(v)))
    except (TypeError, ValueError):
        return 600


def get_mainline_min_members() -> int:
    """主线维度成员数下限, 过滤微型标签。默认 4。"""
    v = _self.load().get("mainline_min_members", 4)
    try:
        return max(_MAINLINE_MIN_MEMBERS_MIN, min(_MAINLINE_MIN_MEMBERS_MAX, int(v)))
    except (TypeError, ValueError):
        return 4


def get_mainline_blacklist() -> list[str]:
    """用户自定义屏蔽的维度成员名(不论成员数大小)。默认空。

    保存时接受 list 或逗号/顿号/分号/空白分隔的字符串。
    """
    v = _self.load().get("mainline_blacklist", [])
    if isinstance(v, str):
        v = [part for part in re.split(r"[,，、;；\s]+", v) if part]
    if not isinstance(v, list):
        return []
    return [str(x).strip() for x in v if str(x).strip()]


def get_sentiment_exclude_st() -> bool:
    """市场环境/主线统计是否剔除风险警示(ST)股。默认 True。

    口径: 主板 ST 在 2026-07 前享 5% 涨跌幅(封板成本减半), 且 ST 是跨行业的
    状态桶而非投资题材, 混入会系统性抬高涨停宽度/高度(弱市尤甚)。剔除后
    涨跌家数等宽度占比几乎不受影响。修改后需重算 regime 与主线生效。
    """
    return bool(_self.load().get("sentiment_exclude_st", True))


def set_sentiment_exclude_st(v: bool) -> bool:
    _self.save({"sentiment_exclude_st": bool(v)})
    return _self.get_sentiment_exclude_st()


def get_mainline_filter_config() -> dict:
    """主线过滤配置汇总(供 API 返回与计算读取)。"""
    return {
        "min_members": _self.get_mainline_min_members(),
        "max_members": _self.get_mainline_max_members(),
        "blacklist": _self.get_mainline_blacklist(),
        "exclude_st": _self.get_sentiment_exclude_st(),
    }


def get_emotion_score_config() -> dict | None:
    """情绪评分口径配置 (维度权重/子项权重与区间/档位阈值)。

    返回原始 dict 或 None (未自定义 = 用出厂口径)。**不在此处做结构校验与补全** ——
    合并与归一化统一由 emotion_score.merge_config 负责, 避免两处各写一套默认值而漂移。
    """
    v = _self.load().get("emotion_score_config")
    return v if isinstance(v, dict) else None


def set_emotion_score_config(cfg: dict | None) -> dict | None:
    """保存情绪评分口径。传 None 或 {} = 恢复出厂口径 (清除该键)。

    只做形状校验, 值的合法性 (负数权重/区间倒置/阈值乱序) 由 emotion_score 层
    在每次计算时归一化 —— 存进去什么由用户负责, 算出来一定安全。
    """
    from app.services.emotion_score import merge_config

    if not cfg:
        from app.state import delete_preference_keys

        with _SAVE_LOCK:
            delete_preference_keys(["emotion_score_config"])
            _self._invalidate_cache()
        return None

    # 落盘前过一遍 merge_config: 校验形状并剔除未知字段, 避免脏配置长期驻留
    normalized = merge_config(cfg)
    _self.save({"emotion_score_config": normalized})
    return normalized


def set_mainline_filter_config(cfg: dict) -> dict:
    """保存主线过滤配置(白名单字段, 部分更新)。修改后需重算主线生效。"""
    updates: dict = {}
    if "min_members" in cfg and cfg["min_members"] is not None:
        updates["mainline_min_members"] = cfg["min_members"]
    if "max_members" in cfg and cfg["max_members"] is not None:
        updates["mainline_max_members"] = cfg["max_members"]
    if "exclude_st" in cfg and cfg["exclude_st"] is not None:
        updates["sentiment_exclude_st"] = bool(cfg["exclude_st"])
    if "blacklist" in cfg and cfg["blacklist"] is not None:
        raw = cfg["blacklist"]
        if isinstance(raw, str):
            raw = [part for part in re.split(r"[,，、;；\s]+", raw) if part]
        updates["mainline_blacklist"] = [str(x).strip() for x in (raw or []) if str(x).strip()]
    if updates:
        _self.save(updates)
    return _self.get_mainline_filter_config()


def get_pipeline_pull_types() -> dict:
    """返回三个拉取开关的当前值。"""
    return {
        "pipeline_pull_a_share": _self.get_pipeline_pull_a_share(),
        "pipeline_pull_etf": _self.get_pipeline_pull_etf(),
        "pipeline_pull_index": _self.get_pipeline_pull_index(),
    }


def set_pipeline_pull_types(cfg: dict) -> dict:
    """批量保存拉取开关。只接受白名单内的布尔字段。"""
    updates = {k: bool(v) for k, v in cfg.items() if k in _PIPELINE_PULL_KEYS and v is not None}
    _self.save(updates)
    return _self.get_pipeline_pull_types()


def get_pipeline_index_symbols() -> str:
    """指数自定义拉取代码(逗号/换行/空格分隔)。空串表示全量。"""
    return str(_self.load().get("pipeline_index_symbols", "") or "").strip()


def set_pipeline_index_symbols(symbols: str) -> str:
    """保存指数自定义代码,返回规范化后的字符串。"""
    _self.save({"pipeline_index_symbols": symbols})
    return _self.get_pipeline_index_symbols()


def get_pipeline_schedule() -> dict:
    """返回盘后管道调度时间 {"hour": 15, "minute": 35}。

    默认 15:35 而非 15:30 整: 盘后固定价交易 15:30 才彻底结束, 且供应商
    聚合含盘后量的官方日K需要时间 —— 整点即拉可能写入不含盘后成交的
    日线, 也与 quote 定版重试窗口终点 (15:30) 精确重合。留 5 分钟缓冲。
    """
    d = _self.load().get("pipeline_schedule", {"hour": 15, "minute": 35})
    return {"hour": d.get("hour", 15), "minute": d.get("minute", 35)}


def set_pipeline_schedule(hour: int, minute: int) -> dict:
    h = max(0, min(23, hour))
    m = max(0, min(59, minute))
    # 盘后管道不早于 15:35: 15:30 盘后固定价才终止 (量/额此前仍会变),
    # 且供应商官方日线定稿需要缓冲 —— 更早启动可能固化不含盘后量的当日分区
    if h * 60 + m < 15 * 60 + 35:
        h, m = 15, 35
    _self.save({"pipeline_schedule": {"hour": h, "minute": m}})
    return {"hour": h, "minute": m}


def get_instruments_schedule() -> dict:
    """返回盘前标的维表调度时间 {"hour": 9, "minute": 10}。"""
    d = _self.load().get("instruments_schedule", {"hour": 9, "minute": 10})
    return {"hour": d.get("hour", 9), "minute": d.get("minute", 10)}


def set_instruments_schedule(hour: int, minute: int) -> dict:
    h = max(0, min(23, hour))
    m = max(0, min(59, minute))
    # 盘前不晚于 09:15
    if h * 60 + m > 9 * 60 + 15:
        h, m = 9, 15
    _self.save({"instruments_schedule": {"hour": h, "minute": m}})
    return {"hour": h, "minute": m}


def get_enriched_batch_size() -> int:
    """返回 enriched 全量计算每批 symbol 数量。"""
    return max(1, min(10000, _self.load().get("enriched_batch_size", 1000)))


def set_enriched_batch_size(size: int) -> int:
    """保存 enriched 全量计算批次大小。"""
    size = max(10, min(6000, size))
    _self.save({"enriched_batch_size": size})
    return size


def get_index_daily_batch_size() -> int:
    """返回指数日 K 同步每批 symbol 数量。"""
    return max(1, min(10000, _self.load().get("index_daily_batch_size", 100)))


def set_index_daily_batch_size(size: int) -> int:
    """保存指数日 K 同步批次大小。"""
    size = max(1, min(10000, size))
    _self.save({"index_daily_batch_size": size})
    return size


# ── 五档盘口 sealed(真假涨停) 配置 ──────────────────────


def get_limit_ladder_monitor_enabled() -> bool:
    """连板梯队 5 档监控开关。关闭时 depth 不轮询(连板梯队降级显示)。"""
    return _self.load().get("limit_ladder_monitor_enabled", False)


def get_depth_polling_interval() -> float:
    """depth 盘中轮询间隔(秒)。默认 10(Pro/Expert 都适用)。"""
    return float(_self.load().get("depth_polling_interval", 10.0))


def set_depth_polling_interval(interval: float) -> float:
    """保存 depth 轮询间隔。套餐范围 clamp 由 depth_service 按档位做。"""
    interval = max(1.0, min(600.0, float(interval)))
    _self.save({"depth_polling_interval": interval})
    return interval


def get_depth_finalize_time() -> dict:
    """盘后 sealed 定版时间 {"hour": 15, "minute": 2}。范围 15:01~18:00。"""
    d = _self.load().get("depth_finalize_time", {"hour": 15, "minute": 2})
    return {"hour": d.get("hour", 15), "minute": d.get("minute", 2)}


def set_depth_finalize_time(hour: int, minute: int) -> dict:
    """保存盘后 sealed 定版时间,强制范围 15:01~18:00。"""
    h = max(0, min(23, hour))
    m = max(0, min(59, minute))
    # 下限 15:01, 上限 18:00
    if h * 60 + m < 15 * 60 + 1:
        h, m = 15, 1
    if h * 60 + m > 18 * 60:
        h, m = 18, 0
    _self.save({"depth_finalize_time": {"hour": h, "minute": m}})
    return {"hour": h, "minute": m}


# 监控与复盘共用的外部推送渠道白名单。
# 多选: 不推送 = 空数组, 而非 'none'


def get_review_schedule() -> dict:
    """定时复盘调度 {"enabled": False, "hour": 15, "minute": 40}。默认关闭。

    默认 15:40: 盘后管道默认 15:35 启动, 留 5 分钟缓冲, 复盘使用管道
    产出的最终口径数据 (含盘后量校正的日K/enriched)。强制下限 15:00 —
    偏好收盘后即时复盘 (走实时快照缓存, 不等管道) 的用户可自行调早。
    """
    d = _self.load().get("review_schedule", {"enabled": False, "hour": 15, "minute": 40})
    return {
        "enabled": bool(d.get("enabled", False)),
        "hour": d.get("hour", 15),
        "minute": d.get("minute", 40),
    }


def set_review_schedule(enabled: bool, hour: int, minute: int) -> dict:
    """保存定时复盘调度。强制时间下限 15:00(A股收盘)。

    enabled=False 时时间仍保存(下次开启可沿用), 但调度器不会注册 job。
    """
    h = max(0, min(23, hour))
    m = max(0, min(59, minute))
    # 下限 15:00: A股 15:00 收盘, 收盘后才有当日完整数据复盘
    if h * 60 + m < 15 * 60:
        h, m = 15, 0
    _self.save({"review_schedule": {"enabled": bool(enabled), "hour": h, "minute": m}})
    return {"enabled": bool(enabled), "hour": h, "minute": m}


# 复盘系统提示词长度上限: 默认模板约 2.4k 字符, 留足自定义余量同时防止误存超长文本


def get_market_recap_system_prompt() -> str:
    """用户自定义的复盘系统提示词。空串 = 未自定义 (market_recap 回退内置默认模板)。"""
    v = _self.load().get("market_recap_system_prompt")
    return v.strip() if isinstance(v, str) else ""
