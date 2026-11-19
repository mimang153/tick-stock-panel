"""settings API 共享定义 _common（由 settings.py 机械拆分）。"""

from __future__ import annotations

import logging
import sys as _sys
import time
from pathlib import Path
from typing import Literal

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from app import secrets_store
from app.data_providers.custom.config import MAX_TIMEOUT

_api_pkg = _sys.modules["app.api.settings"]

logger = logging.getLogger("app.api.settings")


DEFAULT_PAID_ENDPOINT = "https://api.tickflow.org"


def _sync_financial_scheduler_caps(app_state, capset) -> None:
    """把重新探测出的能力同步给财务调度器。

    app.state.capabilities 在此已更新, 但 FinancialScheduler 在启动时捕获的是旧引用,
    需显式刷新, 否则用户升级到 Expert 后点「全部同步」仍会因调度器读旧 capset 而被拒。
    """
    fs = getattr(app_state, "financial_scheduler", None)
    if fs is None:
        return
    try:
        fs.update_capabilities(capset)
    except Exception as e:  # noqa: BLE001
        logging.getLogger("app.api.settings").warning(
            "update financial_scheduler capabilities failed: %s", e
        )


class TickflowKeyIn(BaseModel):
    api_key: str


class SwitchEndpointIn(BaseModel):
    url: str


def _probe_and_sync_capabilities(app_state):
    """重新探测能力并同步到 app.state 与财务调度器。"""
    capset = _api_pkg.detect_capabilities(force=True)
    app_state.capabilities = capset
    _sync_financial_scheduler_caps(app_state, capset)
    return capset


class AiSettingsIn(BaseModel):
    provider: str = "openai_compat"
    base_url: str = ""
    api_key: str | None = None
    model: str = ""
    protocol: str = ""  # chat_completions | responses; 空=保持原值
    reasoning_effort: str = Field(default="high", max_length=64)
    codex_command: str = ""
    codex_reasoning_effort: str = ""
    user_agent: str = ""
    max_output_tokens: int | None = None  # 输出上限, 钳制所有任务的 max_tokens
    context_window: int | None = None  # 输入上下文窗口上限 (约 token)
    round_checkpoint: int | None = None  # 助手工具轮次检查点; 0=不检查


class AiTestIn(BaseModel):
    """草稿连通性测试入参 — 只读表单值, 与保存完全解耦。"""

    provider: str = "openai_compat"
    base_url: str = ""
    api_key: str | None = None  # 留空回退已保存 Key (改模型/协议不必重输)
    model: str = ""
    protocol: str = ""


def _apply_codex_settings(req, updates: dict, settings) -> None:
    """应用 codex_cli 专属配置（含命令校验）。"""
    from app.services.ai_provider import (
        normalize_codex_command,
        normalize_codex_model,
        normalize_codex_reasoning_effort,
    )

    updates["ai_codex_model"] = normalize_codex_model(req.model)
    try:
        codex_command = normalize_codex_command(req.codex_command)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    codex_reasoning_effort = normalize_codex_reasoning_effort(req.codex_reasoning_effort)
    updates["ai_codex_command"] = codex_command
    updates["ai_codex_reasoning_effort"] = codex_reasoning_effort
    settings.ai_codex_command = codex_command
    settings.ai_codex_reasoning_effort = codex_reasoning_effort


def _apply_http_settings(req, updates: dict, settings) -> None:
    """应用 HTTP 类 provider 的 URL/协议/密钥/模型配置。"""
    from app.services.ai_provider import (
        CHAT_COMPLETIONS_PROTOCOL,
        OPENAI_PROVIDER,
        RESPONSES_PROTOCOL,
    )

    if req.base_url:
        updates["ai_base_url"] = req.base_url
        settings.ai_base_url = req.base_url
    if req.protocol:
        if req.protocol not in (CHAT_COMPLETIONS_PROTOCOL, RESPONSES_PROTOCOL):
            raise HTTPException(status_code=400, detail=f"不支持的协议: {req.protocol}")
        updates["ai_protocol"] = req.protocol
        settings.ai_protocol = req.protocol
    if req.api_key is not None:
        if req.api_key:
            updates["ai_api_key"] = req.api_key
            settings.ai_api_key = req.api_key
        else:
            secrets_store.clear("ai_api_key")
            settings.ai_api_key = ""
    if req.model:
        updates["ai_model"] = req.model
        settings.ai_model = req.model
    if req.provider == OPENAI_PROVIDER:
        updates["ai_reasoning_effort"] = req.reasoning_effort.strip()


def _apply_ai_settings_updates(req: AiSettingsIn, settings) -> dict:
    """根据请求构建并应用 AI 配置更新，返回待持久化的 updates。"""
    updates: dict = {}
    if req.provider:
        updates["ai_provider"] = req.provider
        settings.ai_provider = req.provider
    if req.provider == "codex_cli":
        _apply_codex_settings(req, updates, settings)
    else:
        _apply_http_settings(req, updates, settings)
    updates["ai_user_agent"] = req.user_agent
    settings.ai_user_agent = req.user_agent
    if req.max_output_tokens is not None:
        if req.max_output_tokens <= 0:
            raise HTTPException(status_code=400, detail="输出上限必须为正整数")
        updates["ai_max_output_tokens"] = req.max_output_tokens
        settings.ai_max_output_tokens = req.max_output_tokens
    if req.context_window is not None:
        if req.context_window <= 0:
            raise HTTPException(status_code=400, detail="上下文窗口必须为正整数")
        updates["ai_context_window"] = req.context_window
        settings.ai_context_window = req.context_window
    if req.round_checkpoint is not None:
        if req.round_checkpoint != 0 and req.round_checkpoint < 5:
            raise HTTPException(status_code=400, detail="轮次检查点须为 0(不检查)或不小于 5 的整数")
        updates["ai_round_checkpoint"] = req.round_checkpoint
        settings.ai_round_checkpoint = req.round_checkpoint
    return updates


def _ai_settings_status() -> dict:
    """读取并返回当前 AI 配置状态。"""
    from app.services.ai_provider import (
        ai_configured,
        current_ai_context_window,
        current_ai_max_output_tokens,
        current_ai_model,
        current_ai_protocol,
        current_ai_provider,
        current_ai_round_checkpoint,
        current_codex_command,
        current_codex_model,
        current_codex_reasoning_effort,
        current_openai_model,
        current_openai_reasoning_effort,
    )

    provider = current_ai_provider()
    return {
        "ok": True,
        "ai_provider": provider,
        "ai_protocol": current_ai_protocol(),
        "ai_model": current_ai_model(),
        "ai_openai_model": current_openai_model(),
        "ai_reasoning_effort": current_openai_reasoning_effort(),
        "ai_codex_model": current_codex_model(),
        "ai_codex_command": current_codex_command(),
        "ai_codex_reasoning_effort": current_codex_reasoning_effort(),
        "ai_configured": ai_configured(provider),
        "ai_max_output_tokens": current_ai_max_output_tokens(),
        "ai_context_window": current_ai_context_window(),
        "ai_round_checkpoint": current_ai_round_checkpoint(),
    }


def _realtime_allowed() -> bool:
    """当前档位是否允许实时行情(none/free 不允许)。"""
    from app.services.quote_service import QuoteService

    return QuoteService.is_realtime_allowed()


def _minute_history_days() -> int | None:
    """当前分钟源的 1 分钟历史深度(交易日); None = 深历史(tickflow 基准)。

    provider 可选类属性 minute_history_days 声明 (如浅源 = 5,
    免费分时接口只保留最近 5 个交易日); 未声明或走 tickflow 时视为深历史。
    前端分时档位/默认值据此收窄。
    """
    from app.services import kline_sync, preferences

    provider_name = preferences.get_minute_data_provider()
    provider, fallback, _err = kline_sync._resolve_minute_provider(provider_name)
    if fallback or provider is None:
        return None
    return getattr(provider, "minute_history_days", None)


class MinuteSyncPrefs(BaseModel):
    minute_sync_enabled: bool
    minute_sync_days: int = 5
    # 单段大小(交易日),None 表示不修改现有值。范围 [5, 30],默认 20。
    minute_sync_segment_days: int | None = None


class DataProvidersIn(BaseModel):
    daily_data_provider: str | None = None
    adj_factor_provider: str | None = None
    minute_data_provider: str | None = None
    full_minute_data_provider: str | None = None
    depth5_data_provider: str | None = None
    realtime_data_provider: str | None = None
    financial_data_provider: str | None = None


class PluginKeyIn(BaseModel):
    plugin: str
    api_key: str


class DataSourceJobTimeoutPrefs(BaseModel):
    data_source_job_timeout_s: int = Field(ge=60)
    data_source_long_job_timeout_s: int = Field(ge=60)


class MinuteBatchCompressPrefs(BaseModel):
    minute_batch_compress: bool


class DailyBatchCompressPrefs(BaseModel):
    daily_batch_compress: bool


class DatasetFieldMapItem(BaseModel):
    source: str
    target: str


class DatasetConfigIn(BaseModel):
    url: str
    method: str = "GET"
    batch: int | None = None
    rpm: int | None = None
    response_path: str = ""
    field_map: dict[str, str] = {}
    transforms: dict[str, str] = {}
    symbols_param: str = "symbols"
    start_param: str = "start_time"
    end_param: str = "end_time"
    asset_type_param: str | None = None
    freq_param: str | None = None
    timeout: float | None = Field(
        default=None,
        gt=0,
        le=MAX_TIMEOUT,
        allow_inf_nan=False,
    )


class AuthConfigIn(BaseModel):
    type: str = "none"
    token_env: str | None = None
    header: str = "Authorization"
    param: str = "token"


class CustomSourceIn(BaseModel):
    name: str
    display_name: str = ""
    auth: AuthConfigIn = AuthConfigIn()
    datasets: dict[str, DatasetConfigIn] = {}


class CustomSourceTestIn(BaseModel):
    provider: str
    dataset: str
    symbols: list[str] | None = None
    config: CustomSourceIn | None = None


class MiningSchedulePrefs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    mining_schedule_enabled: bool
    mining_schedule_weekday: int = Field(ge=0, le=4)
    mining_budget_profile: Literal["balanced", "strict"]


class NavOrderIn(BaseModel):
    nav_order: list[str]


class NavHiddenIn(BaseModel):
    nav_hidden: list[str]


class NavLayoutIn(BaseModel):
    nav_layout: dict | None
    nav_hidden: list[str] | None = None


class DashboardLayoutIn(BaseModel):
    """看板自定义布局; layout=null 恢复默认。"""

    layout: dict | None = None


class UiPrefsIn(BaseModel):
    """界面偏好增量更新; 值为 null = 删除该项 (回前端默认)。"""

    model_config = ConfigDict(extra="allow")


class RealtimeQuotesPrefs(BaseModel):
    realtime_quotes_enabled: bool


class RealtimeQuoteScopePrefs(BaseModel):
    realtime_pull_stock: bool | None = None
    realtime_pull_etf: bool | None = None


def _sync_depth_polling(depth_svc, realtime_on: bool) -> None:
    """实时行情开关联动 depth 盘中轮询: 开→恢复(仍受监控开关/能力门控), 关→立即停。

    实时行情关闭时 enriched 停留在上一交易日, depth 轮询只会反复拉陈旧名单。
    """
    if not depth_svc:
        return
    if realtime_on:
        depth_svc.start_polling()
    else:
        depth_svc.stop_polling()


def _force_disable_realtime_quotes(preferences, qs, depth_svc) -> dict:
    preferences.save({"realtime_quotes_enabled": False})
    if qs:
        qs.disable()
    _sync_depth_polling(depth_svc, False)
    return {"realtime_quotes_enabled": False, "realtime_allowed": False}


def _check_realtime_enable_gates(req: RealtimeQuotesPrefs, request: Request, qs) -> None:
    # 首用门禁: 本地完全无数据 (日K/enriched 均空, 与看板首用弹窗同口径) 时
    # 禁止开启 — 实时行情的展示 (enriched 底座 + 快照覆盖)、监控、连板梯队
    # 全部依赖本地数据, 空数据下轮询只是空转耗配额, 且无 instruments 维表。
    repo = getattr(request.app.state, "repo", None)
    if (
        repo is not None
        and repo.latest_daily_date() is None
        and repo.latest_enriched_date() is None
    ):
        raise HTTPException(
            status_code=409,
            detail="本地暂无行情数据，请先在「数据」页同步后再开启实时行情",
        )
    if qs and qs.is_paused():
        # 管道/数据修正运行期间禁止开启实时行情 — 防止写盘竞态
        raise HTTPException(
            status_code=409, detail="数据同步运行中，实时行情已临时暂停，请稍后再开启"
        )

    # 历史完整性门禁: 检测到最近交易日的盘中快照/缺口时禁止开启 —
    # 实时 flush 写出"今天"分区后, 盘后管道的"只刷今天"分支会让停机日的
    # 半日快照永久留存。同时自动创建修复任务, 修完即可正常开启。
    from app.services import data_integrity

    if repo is None:
        return
    try:
        issues = data_integrity.scan_recent_integrity(repo.store.data_dir)
        issues = [i for i in issues if i.table in data_integrity.active_daily_tables()]
    except Exception:  # noqa: BLE001
        issues = []
    earliest = data_integrity.earliest_issue_day(issues)
    if earliest is None:  # issues 非空时理论上必有最早坏日; 收窄类型供下游修复接口使用
        return
    if not issues or not data_integrity.within_auto_repair_window(earliest):
        return
    symbols = data_integrity._stock_snapshot_symbols(issues)
    job_id, is_new = data_integrity.launch_integrity_repair(
        request.app.state,
        earliest,
        "realtime_gate",
        symbols=symbols,
    )
    if job_id is not None:
        detail = (
            f"检测到{data_integrity.describe_issues(issues)}，"
            + (
                "已自动创建修复任务，完成后即可开启实时行情"
                if is_new
                else "修复任务正在进行中，请稍后再开启"
            )
            + f"（任务 {job_id}）"
        )
        raise HTTPException(status_code=409, detail=detail)
    backoff = data_integrity.repair_backoff_remaining_s(
        repo.store.data_dir,
        earliest,
    )
    if backoff > 0:
        raise HTTPException(
            status_code=409,
            detail=(
                f"检测到{data_integrity.describe_issues(issues)}；"
                f"上次自动修复未成功（多为数据源拉取失败），"
                f"{int(backoff // 60) + 1} 分钟后将自动重试，"
                "也可到 数据→修正数据 手动执行"
            ),
        )


def _apply_realtime_prefs(req: RealtimeQuotesPrefs, preferences, qs, depth_svc) -> None:
    preferences.save({"realtime_quotes_enabled": req.realtime_quotes_enabled})
    if qs:
        if req.realtime_quotes_enabled:
            qs.enable()
        else:
            qs.disable()
    _sync_depth_polling(depth_svc, req.realtime_quotes_enabled)


class AssistantAutoApproveIn(BaseModel):
    """AI 助手动作工具免确认开关。"""

    enabled: bool


class AssistantMaskIn(BaseModel):
    """AI 助手毛玻璃遮罩开关。"""

    enabled: bool


class WatchlistGroupsInNavPrefs(BaseModel):
    watchlist_groups_in_nav: bool


class RealtimeMonitorConfigIn(BaseModel):
    sse_refresh_pages: dict[str, bool] | None = None
    strategy_monitor_enabled: bool | None = None
    strategy_monitor_ids: list[str] | None = None
    screener_auto_run: bool | None = None
    minute_intraday_refresh: bool | None = None
    minute_intraday_refresh_interval: int | None = None
    # 盘中分钟增量落盘 (Expert 专有) — 交易时段常驻服务, 归实时监控配置
    minute_refresh_enabled: bool | None = None
    minute_refresh_interval: int | None = None
    monitor_ext_fields: dict | None = None


class PipelinePullTypesIn(BaseModel):
    """盘后管道拉取内容开关(A股 / ETF / 指数 独立控制)。"""

    pipeline_pull_a_share: bool | None = None
    pipeline_pull_etf: bool | None = None
    pipeline_pull_index: bool | None = None


class PipelineRegimeEnabledIn(BaseModel):
    """盘后管道是否自动计算市场环境(regime)。"""

    pipeline_regime_enabled: bool


class RegimeBatchParamsIn(BaseModel):
    """regime 全量回填分批参数(控制内存峰值)。"""

    batch_days: int | None = None
    warmup_days: int | None = None


class PipelineIndexSymbolsIn(BaseModel):
    """指数自定义拉取代码(逗号/换行/空格分隔,空串表示全量)。"""

    symbols: str = ""


class MainlineFilterIn(BaseModel):
    """市场主线过滤配置(宽基/风格标签按成员数过滤 + 名称黑名单 + ST 剔除开关)。"""

    min_members: int | None = None
    max_members: int | None = None
    blacklist: list[str] | str | None = None
    exclude_st: bool | None = None


class EmotionScoreConfigIn(BaseModel):
    """情绪评分口径 (维度权重 / 子项权重与归一化区间 / 档位阈值)。

    config 为整棵树或 null — 传 null 表示恢复出厂口径。不做逐字段白名单:
    结构由 emotion_score.merge_config 归一化 (未知 key 丢弃、缺项补默认、
    阈值乱序自动重排), 用户改坏权重也不会让看板算不出分。
    """

    config: dict | None = None


class QuoteIntervalIn(BaseModel):
    interval: float


class SystemNotifyPrefsIn(BaseModel):
    enabled: bool


class FeishuWebhookPrefsIn(BaseModel):
    url: str
    secret: str = ""


class WecomWebhookPrefsIn(BaseModel):
    url: str


class CustomWebhookPrefsIn(BaseModel):
    url: str
    # None preserves the stored secret; an explicit empty string clears it.
    secret: str | None = None


class EmailSmtpPrefsIn(BaseModel):
    host: str
    port: int = Field(default=465, ge=1, le=65535)
    security: Literal["ssl", "starttls", "none"] = "ssl"
    username: str = ""
    # None preserves the stored password; an explicit empty string clears it.
    password: str | None = None
    from_address: str = ""
    to_addresses: list[str] = Field(default_factory=list)


class WebhookTestIn(BaseModel):
    channel: Literal["feishu", "wecom", "custom", "email"]


class WecomBotPrefsIn(BaseModel):
    bot_id: str
    secret: str
    enabled: bool = True


class WecomBotToggleIn(BaseModel):
    enabled: bool


class WebhookEnabledDefaultIn(BaseModel):
    enabled: bool


class WebhookDefaultChannelsIn(BaseModel):
    channels: list[str]  # 多选: feishu / wecom / custom / email; 空数组=不推送


class TestEndpointIn(BaseModel):
    url: str
    # 测试轮数;不传时取 endpoints.json 的 testRounds(默认 5)
    rounds: int | None = None


ENDPOINTS_URL = "https://tickflow.org/endpoints.json"


ENDPOINTS_TTL = 300.0  # 秒


_FALLBACK_ENDPOINTS: list[dict] = [
    {
        "id": "default",
        "url": "https://api.tickflow.org",
        "label": "默认端点",
        "region": "auto",
        "description": "默认端点",
        "premium": False,
    },
    {
        "id": "hk",
        "url": "https://hk-api.tickflow.org",
        "label": "香港端点",
        "region": "ap-east-1",
        "description": "备用端点，部分地区访问更稳定",
        "premium": False,
    },
    {
        "id": "sg",
        "url": "https://sg-api.tickflow.org",
        "label": "新加坡端点",
        "region": "ap-southeast-1",
        "description": "备用端点，亚太地区访问更稳定",
        "premium": False,
    },
    {
        "id": "us",
        "url": "https://us-api.tickflow.org",
        "label": "美国端点",
        "region": "us-east-1",
        "description": "备用端点，欧美地区访问更稳定",
        "premium": False,
    },
    {
        "id": "cn",
        "url": "https://139.196.55.234:50443",
        "label": "中国大陆端点（Beta）",
        "region": "cn-east-1",
        "description": "备用端点，中国大陆地区访问更稳定，目前处于测试阶段，谨慎使用",
        "premium": False,
    },
    {
        "id": "cn-premium",
        "url": "https://106.15.238.72:50443",
        "label": "中国大陆专线端点",
        "region": "cn-east-1",
        "description": "专线加速端点，需要专线加速权限（该权限包含在 Expert 及以上套餐中，也可通过自定义组合单独开通）",
        "premium": True,
    },
]


_endpoints_cache: dict = {"ts": 0.0, "data": None}


async def _http_ping(url: str, timeout: float = 10.0) -> float | None:
    """单次异步 GET 请求并返回延迟(ms),失败返回 None。

    对齐官方 latency_test.py:用 /health 轻量端点测真实网络延迟,
    不携带 API Key(/health 公开)。异步实现,保证多端点并行测速不阻塞。
    """
    import httpx

    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url)
            dt = (time.perf_counter() - t0) * 1000
            # 只把 <400 视为成功;4xx/5xx 也算"不可达"
            if resp.status_code < 400:
                return round(dt, 2)
            return None
    except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPError, OSError):
        return None


class PipelineScheduleIn(BaseModel):
    hour: int
    minute: int


class EnrichedBatchSizeIn(BaseModel):
    size: int


class IndexDailyBatchSizeIn(BaseModel):
    size: int


class BatchSymbolsIn(BaseModel):
    size: int


class BacktestWorkerRssLimitIn(BaseModel):
    gib: int = Field(ge=0, le=64, description="回测/优化 worker RSS 上限 (GiB), 0 = 不限制")


class LimitLadderMonitorIn(BaseModel):
    enabled: bool


class DepthPollingIntervalIn(BaseModel):
    interval: float


class DepthFinalizeTimeIn(BaseModel):
    hour: int
    minute: int


class ReviewScheduleIn(BaseModel):
    enabled: bool
    hour: int
    minute: int


class ReviewPushIn(BaseModel):
    channels: list[str]  # 多选: feishu / wecom / custom / email; 空数组=不推送
    mode: str | None = None  # 可选: auto=归档即推 / manual=仅显式 push; 不传则不变


class MarketRecapPromptIn(BaseModel):
    prompt: str  # 自定义系统提示词全文; 空串 = 恢复内置默认模板


class ApiTokenCreateIn(BaseModel):
    name: str
    scopes: list[str]


_SOUND_PACK_DIR = "sound_packs"


_SOUND_PACK_MAX_BYTES = 4 * 1024 * 1024


_SOUND_PACK_EXTS = {".mp3", ".wav", ".ogg", ".m4a", ".aac", ".flac", ".opus", ".webm"}


_SOUND_PACK_MEDIA_TYPES = {
    ".mp3": "audio/mpeg",
    ".wav": "audio/wav",
    ".ogg": "audio/ogg",
    ".m4a": "audio/mp4",
    ".aac": "audio/aac",
    ".flac": "audio/flac",
    ".opus": "audio/ogg",
    ".webm": "audio/webm",
}


def _sound_pack_dir(request: Request) -> Path:
    return request.app.state.repo.store.data_dir / _SOUND_PACK_DIR


def _safe_pack_path(pack_dir: Path, name: str) -> Path:
    """名字 → 目录内路径; 拒绝穿越/非法名。"""
    if not name or "/" in name or "\\" in name or ".." in name or name != name.strip():
        raise HTTPException(status_code=400, detail="语音包名称非法")
    path = (pack_dir / name).resolve()
    if path.parent != pack_dir.resolve():
        raise HTTPException(status_code=400, detail="语音包名称非法")
    return path
