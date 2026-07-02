from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from services.operator.models import (
    BlockType,
    ReasonChannel,
    ReasonClassification,
    StageCategory,
)

ReasonRule = tuple[StageCategory, BlockType, ReasonChannel, str]


def classify_reason(reason_code: object) -> ReasonClassification:
    reason = str(reason_code or "UNKNOWN").strip().upper() or "UNKNOWN"
    stage, block_type, channel, hint = _classify(reason)
    return ReasonClassification(
        reason_code=reason,
        stage=stage,
        block_type=block_type,
        channel=channel,
        operator_hint=hint,
    )


def classify_reasons(reason_codes: Iterable[object]) -> list[ReasonClassification]:
    return [classify_reason(reason) for reason in reason_codes]


def primary_classification(reason_codes: Iterable[object]) -> ReasonClassification:
    classifications = classify_reasons(reason_codes)
    if not classifications:
        return classify_reason("UNKNOWN")
    return sorted(classifications, key=lambda item: _PRIORITY[item.block_type])[0]


def group_reason_codes_by_channel(reason_codes: Iterable[object]) -> dict[str, list[str]]:
    groups = {channel.value: [] for channel in ReasonChannel}
    for classification in classify_reasons(reason_codes):
        groups[classification.channel.value].append(classification.reason_code)
    return groups


def aggregate_reason_summary(reason_codes: Iterable[object]) -> dict[str, Any]:
    reason_counter: Counter[str] = Counter()
    stage_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    channel_counter: Counter[str] = Counter()
    channel_reason_counters = {channel.value: Counter() for channel in ReasonChannel}
    for classification in classify_reasons(reason_codes):
        channel = classification.channel.value
        reason_counter[classification.reason_code] += 1
        stage_counter[classification.stage.value] += 1
        type_counter[classification.block_type.value] += 1
        channel_counter[channel] += 1
        channel_reason_counters[channel][classification.reason_code] += 1
    return {
        "reason_counts": dict(reason_counter),
        "stage_counts": dict(stage_counter),
        "block_type_counts": dict(type_counter),
        "channel_counts": dict(channel_counter),
        "channel_reason_counts": {
            channel: dict(counter) for channel, counter in channel_reason_counters.items()
        },
    }


def summarize_classifications(items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    stage_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    channel_counter: Counter[str] = Counter()
    hard_block_count = 0
    data_wait_count = 0
    soft_wait_count = 0
    for item in items:
        stage = str(item.get("primary_block_stage") or StageCategory.UNKNOWN.value)
        block_type = str(item.get("primary_block_type") or BlockType.NOT_APPLICABLE.value)
        channel = str(item.get("primary_reason_channel") or ReasonChannel.INFO.value)
        stage_counter[stage] += 1
        type_counter[block_type] += 1
        channel_counter[channel] += 1
        if block_type in {
            BlockType.HARD_BLOCK.value,
            BlockType.SAFETY_BLOCK.value,
            BlockType.CONFIG_DISABLED.value,
        }:
            hard_block_count += 1
        if block_type == BlockType.DATA_WAIT.value:
            data_wait_count += 1
        if block_type in {BlockType.SOFT_WAIT.value, BlockType.THRESHOLD_MISS.value}:
            soft_wait_count += 1
    return {
        "stage_counts": dict(stage_counter),
        "block_type_counts": dict(type_counter),
        "channel_counts": dict(channel_counter),
        "hard_block_count": hard_block_count,
        "data_wait_count": data_wait_count,
        "soft_wait_count": soft_wait_count,
    }


_PRIORITY = {
    BlockType.SAFETY_BLOCK: 0,
    BlockType.CONFIG_DISABLED: 1,
    BlockType.HARD_BLOCK: 2,
    BlockType.DATA_WAIT: 3,
    BlockType.SOFT_WAIT: 4,
    BlockType.THRESHOLD_MISS: 5,
    BlockType.AI_NO_TRADE: 6,
    BlockType.NOT_APPLICABLE: 7,
}

_AI_HINT = "AI 관망 사유를 확인하되 주문 가능 상태로 승격하지 않습니다."
_AI_UNAVAILABLE_HINT = "AI 실행 실패는 시스템 안전 차단과 분리해서 확인합니다."
_CANDIDATE_HINT = "Candidate FSM 상태와 context readiness를 확인합니다."
_CONFIG_HINT = "설정 flag가 의도대로 켜져 있는지 확인합니다."
_DATA_WAIT_HINT = "가격/tick/bar 데이터의 최신성과 누락 여부를 확인합니다."
_DUPLICATE_HINT = "동일 종목 주문/포지션/스케일인 제한을 확인합니다."
_ENTRY_HINT = "EntryTiming 상태가 재시도/관망인지 확인합니다."
_GATEWAY_HINT = "Gateway heartbeat, orderable, command queue 상태를 확인합니다."
_INFO_HINT = "정보성 reason이므로 차단 원인 집계에서는 분리해서 봅니다."
_LIMIT_HINT = "일일 주문/금액/수량 한도와 주문 크기를 확인합니다."
_ORDER_PLAN_HINT = "OrderPlanDraft 상태와 만료 시각을 확인합니다."
_RECONCILE_HINT = "reconcile snapshot의 mismatch와 blocking_new_buy 값을 확인합니다."
_RISK_HINT = "Risk observation의 block/caution reason을 확인합니다."
_SAFETY_HINT = "LIVE_SIM safety gate와 kill switch 상태를 확인합니다."
_STRATEGY_HINT = "Strategy observation의 setup/status를 확인합니다."
_THEME_HINT = "ThemeLeadership snapshot과 watchset 선별 결과를 확인합니다."
_UNKNOWN_HINT = "분류되지 않은 reason이므로 원본 payload를 확인합니다."


def _rules(
    reason_codes: set[str],
    stage: StageCategory,
    block_type: BlockType,
    channel: ReasonChannel,
    hint: str,
) -> dict[str, ReasonRule]:
    return {
        reason: (
            stage,
            block_type,
            channel,
            hint,
        )
        for reason in reason_codes
    }


EXPLICIT_REASON_MAP: dict[str, ReasonRule] = {
    **_rules(
        {"AI_NO_TRADE", "AI_SELECTED_EMPTY", "AI_NO_SELECTION"},
        StageCategory.AI_ADVISORY,
        BlockType.AI_NO_TRADE,
        ReasonChannel.WAITING,
        _AI_HINT,
    ),
    **_rules(
        {"AI_UNAVAILABLE", "TIMEOUT", "INVALID_SCHEMA", "PROVIDER_ERROR"},
        StageCategory.AI_ADVISORY,
        BlockType.NOT_APPLICABLE,
        ReasonChannel.INFO,
        _AI_UNAVAILABLE_HINT,
    ),
    **_rules(
        {
            "CONDITION_ENTERED",
            "CONDITION_LEADER_OBSERVED",
            "CONDITION_PULLBACK_OBSERVED",
            "CONDITION_BREAKOUT_OBSERVED",
            "CONDITION_FUSION_PRIORITY_READY",
            "CONDITION_SENSOR_EVIDENCE",
            "CONTEXT_READY",
            "CANDIDATE_CREATED",
            "SOURCE_DETECTED",
            "DUPLICATE_SOURCE_MERGED",
            "MARKET_READINESS_READY",
            "MARKET_REGIME_ALIGNED",
            "MARKET_TICK_FRESH",
            "PRICE_ABOVE_VWAP",
            "PRICE_BELOW_VWAP",
            "PULLBACK_OBSERVED",
            "TRADE_VALUE_FLOW_OBSERVED",
            "VWAP_RECLAIM_OBSERVED",
            "BREAKOUT_RETEST_OBSERVED",
            "FOLLOWER_EXPANSION_OBSERVED",
            "MOMENTUM_FORMING",
            "SETUP_MATCHED",
            "THEME_CO_LEADER_MEMBER",
            "THEME_FOLLOWER_MEMBER",
            "THEME_LEADING_MEMBER",
            "THEME_STATE_LEADING",
            "THEME_STATE_SPREADING",
            "LEADER_BREAKOUT_FUSION_PRIORITY",
            "LEADER_PULLBACK_FUSION_PRIORITY",
            "ENTRY_TIMING_ALLOWED",
            "BREAKOUT_NEAR_HIGH_ALLOWED",
            "SIZE_REDUCED",
            "TIGHT_STOP_REQUIRED",
            "SHORT_TTL_REQUIRED",
            "MOMENTUM_CONTINUATION",
            "MOMENTUM_CONTINUATION_SIZE_REDUCED",
            "MOMENTUM_CONTINUATION_SHORT_TTL",
            "MOMENTUM_CONTINUATION_TIGHT_STOP_REQUIRED",
            "MOMENTUM_CONTINUATION_MIN_QUANTITY_FLOOR",
            "PLAN_READY",
        },
        StageCategory.UNKNOWN,
        BlockType.NOT_APPLICABLE,
        ReasonChannel.INFO,
        _INFO_HINT,
    ),
    **_rules(
        {
            "CANDIDATE_WITHOUT_ORDER_PLAN",
            "WATCHSET_WITHOUT_ORDER_PLAN",
            "CONDITION_DISCOVERY_OBSERVED",
            "DISCOVERY_OBSERVATION_ONLY",
            "DISCOVERY_PROMOTION_PENDING",
            "MARKET_SENSOR_NOT_BUY_SIGNAL",
            "OBSERVATION_BLOCKED",
            "SOURCE_EXITED",
            "THEME_ROTATED_OUT",
        },
        StageCategory.CANDIDATE,
        BlockType.SOFT_WAIT,
        ReasonChannel.WAITING,
        _CANDIDATE_HINT,
    ),
    **_rules(
        {
            "CANDIDATE_NOT_CONTEXT_READY",
            "CANDIDATE_CONTEXT_MISSING",
            "CANDIDATE_DATA_WAIT",
            "CANDIDATE_STALE",
            "MARKET_READINESS_MISSING",
            "MARKET_READINESS_STALE",
            "MARKET_READINESS_INVALID",
            "SOURCE_MISSING",
            "SOURCE_STALE",
            "THEME_CONTEXT_MISSING",
            "THEME_MISSING",
            "TICK_MISSING",
            "TICK_STALE",
            "BAR_MISSING",
            "BAR_1M_MISSING",
            "BAR_3M_MISSING",
            "BAR_5M_MISSING",
            "VWAP_MISSING",
            "LATEST_TICK_MISSING",
            "LATEST_TICK_STALE",
            "STRATEGY_OBSERVATION_MISSING",
            "STRATEGY_OBSERVATION_STALE",
            "RISK_OBSERVATION_MISSING",
            "DRY_RUN_EVIDENCE_MISSING",
        },
        StageCategory.DATA_WAIT,
        BlockType.DATA_WAIT,
        ReasonChannel.WAITING,
        _DATA_WAIT_HINT,
    ),
    **_rules(
        {
            "ORDER_PLAN_NOT_FOUND",
            "ORDER_PLAN_MISSING",
            "ORDER_PLAN_EMPTY",
            "ORDER_PLAN_NOT_READY",
            "ORDER_PLAN_EXPIRED",
            "ORDER_PLAN_NOT_LATEST",
            "ORDER_PLAN_DRY_RUN_EVIDENCE_MISSING",
            "ORDER_PLAN_CANDIDATE_NOT_CONTEXT_READY",
        },
        StageCategory.ORDER_PLAN,
        BlockType.SOFT_WAIT,
        ReasonChannel.WAITING,
        _ORDER_PLAN_HINT,
    ),
    **_rules(
        {
            "ORDER_PLAN_LATEST_TICK_MISSING",
            "ORDER_PLAN_LATEST_TICK_STALE",
        },
        StageCategory.DATA_WAIT,
        BlockType.DATA_WAIT,
        ReasonChannel.WAITING,
        _DATA_WAIT_HINT,
    ),
    **_rules(
        {
            "ORDER_PLAN_ENTRY_TIMING_NOT_ALLOWED",
            "NO_SETUP",
            "WAIT_RETRY",
            "BLOCKED_CHASE",
            "BLOCKED_OVERHEAT",
            "BLOCKED_STALE",
            "PULLBACK_TOO_SHALLOW",
            "PULLBACK_TOO_DEEP",
            "MOMENTUM_WEAK",
            "TRADE_VALUE_FLOW_WEAK",
            "SETUP_NOT_MATCHED",
        },
        StageCategory.ENTRY_TIMING,
        BlockType.THRESHOLD_MISS,
        ReasonChannel.WAITING,
        _ENTRY_HINT,
    ),
    **_rules(
        {
            "ORDER_PLAN_STRATEGY_NOT_MATCHED",
            "STRATEGY_NOT_MATCHED",
            "STRATEGY_FORMING_ONLY",
            "THEME_NOT_LEADING_OR_SPREADING",
            "THEME_ROLE_NOT_ALLOWED",
        },
        StageCategory.STRATEGY,
        BlockType.THRESHOLD_MISS,
        ReasonChannel.WAITING,
        _STRATEGY_HINT,
    ),
    **_rules(
        {
            "THEME_DATA_WAIT",
            "THEME_FRESH_COVERAGE_LOW",
            "LOW_FRESH_COVERAGE",
            "INSUFFICIENT_VALID_MEMBERS",
            "THEME_LEADER_MISSING",
            "THEME_MEMBERSHIP_EMPTY",
            "THEME_SNAPSHOT_NOT_BUILT",
        },
        StageCategory.THEME,
        BlockType.DATA_WAIT,
        ReasonChannel.WAITING,
        _THEME_HINT,
    ),
    **_rules(
        {
            "RISK_BLOCKED_BY_CONDITION",
            "CONDITION_RISK_BLOCKED",
            "ORDER_PLAN_RISK_NOT_PASS",
            "RISK_NOT_OBSERVE_PASS",
            "RISK_GATE_NOT_ORDER_APPROVAL",
            "PRIMARY_INDEX_RISK_OFF",
            "SECONDARY_INDEX_RISK_OFF",
            "MARKET_INDEX_INTRADAY_SHOCK",
            "MARKET_REGIME_RISK_OFF",
            "PRICE_NEAR_HIGH",
            "CHANGE_RATE_OVERHEAT",
            "VWAP_EXTENSION_HIGH",
            "SPREAD_TOO_WIDE",
            "CUMULATIVE_TRADE_VALUE_LOW",
            "EXECUTION_STRENGTH_WEAK",
            "VI_DATA_UNAVAILABLE",
            "MARKET_CONTEXT_UNAVAILABLE",
            "PORTFOLIO_CONTEXT_UNAVAILABLE",
            "OBSERVE_PASS_NOT_ORDER_APPROVAL",
        },
        StageCategory.RISK,
        BlockType.HARD_BLOCK,
        ReasonChannel.BLOCKING,
        _RISK_HINT,
    ),
    **_rules(
        {
            "LIVE_SIM_RECONCILE_MISMATCH_BLOCK",
            "RECONCILE_REQUIRED",
        },
        StageCategory.RECONCILE,
        BlockType.SAFETY_BLOCK,
        ReasonChannel.BLOCKING,
        _RECONCILE_HINT,
    ),
    **_rules(
        {
            "DUPLICATE_LIVE_SIM_ORDER",
            "DUPLICATE_ACTIVE_CANDIDATE",
            "DUPLICATE_DRY_RUN_POSITION",
            "LIVE_SIM_OPEN_POSITION_EXISTS",
            "LIVE_SIM_ACTIVE_EXIT_EXISTS",
            "LIVE_SIM_ACTIVE_CANCEL_EXISTS",
            "LIVE_SIM_POSITION_LIMIT_EXCEEDED",
            "OPEN_POSITION_EXISTS",
            "POSITION_EXISTS",
            "ACTIVE_POSITION_LIMIT_EXCEEDED",
            "ORDER_PLAN_DUPLICATE_INTENT",
        },
        StageCategory.DUPLICATE_POSITION,
        BlockType.HARD_BLOCK,
        ReasonChannel.BLOCKING,
        _DUPLICATE_HINT,
    ),
    **_rules(
        {
            "MAX_ORDER_NOTIONAL_EXCEEDED",
            "DAILY_ORDER_LIMIT_EXCEEDED",
            "DAILY_NOTIONAL_LIMIT_EXCEEDED",
            "ACTIVE_ORDER_LIMIT_EXCEEDED",
            "CODE_CONCENTRATION_LIMIT_EXCEEDED",
            "TOTAL_EXPOSURE_LIMIT_EXCEEDED",
            "INVALID_QUANTITY",
            "INVALID_NOTIONAL",
            "ORDER_PLAN_INVALID_PRICE",
            "ORDER_PLAN_INVALID_QUANTITY",
            "ORDER_PLAN_PRICE_DRIFT_EXCEEDED",
        },
        StageCategory.LIMIT,
        BlockType.HARD_BLOCK,
        ReasonChannel.BLOCKING,
        _LIMIT_HINT,
    ),
    **_rules(
        {
            "LIVE_SIM_DISABLED",
            "LIVE_SIM_KILL_SWITCH_ACTIVE",
            "LIVE_REAL_NOT_ALLOWED",
            "ACCOUNT_NOT_SIMULATION",
            "BROKER_ENV_NOT_SIMULATION",
            "SERVER_MODE_NOT_SIMULATION",
            "ACCOUNT_KILL_SWITCH_ACTIVE",
            "ORDER_PLAN_SAFETY_GATE_FAILED",
            "LIVE_SIM_LIFECYCLE_ERROR_BLOCK",
            "LIVE_SIM_REAL_ORDER_BLOCKED",
        },
        StageCategory.LIVE_SIM_SAFETY,
        BlockType.SAFETY_BLOCK,
        ReasonChannel.BLOCKING,
        _SAFETY_HINT,
    ),
    **_rules(
        {
            "GATEWAY_HEARTBEAT_STALE",
            "GATEWAY_NOT_ORDERABLE",
            "GATEWAY_COMMAND_DISABLED",
            "GATEWAY_COMMAND_QUEUE_UNHEALTHY",
            "GATEWAY_REALTIME_STALLED",
            "COMMAND_QUEUE_REJECTED",
            "BROKER_ACK_TIMEOUT",
            "BROKER_REJECTED",
        },
        StageCategory.GATEWAY,
        BlockType.SAFETY_BLOCK,
        ReasonChannel.BLOCKING,
        _GATEWAY_HINT,
    ),
    **_rules(
        {
            "ORDER_ROUTING_DISABLED",
            "ORDER_PLAN_ROUTING_DISABLED",
            "PILOT_PIPELINE_DISABLED",
            "PILOT_AUTO_QUEUE_DISABLED",
            "LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED",
            "LIVE_SIM_GATEWAY_COMMAND_ENABLED",
            "AUTO_QUEUE_DISABLED",
            "PROFILE_DISABLED",
            "AI_ORDER_TOOLS_ENABLED",
            "DASHBOARD_ORDER_CONTROLS_UNAVAILABLE",
        },
        StageCategory.CONFIG,
        BlockType.CONFIG_DISABLED,
        ReasonChannel.BLOCKING,
        _CONFIG_HINT,
    ),
    **_rules(
        {
            "ORDER_PLAN_NOT_BUY",
            "ORDER_PLAN_MARKET_ORDER_NOT_ALLOWED",
            "MARKET_ORDER_NOT_ALLOWED",
            "SELL_NOT_ALLOWED",
            "INVALID_INTENT_STATUS",
            "IDEMPOTENCY_KEY_REQUIRED",
        },
        StageCategory.ORDER_PLAN,
        BlockType.HARD_BLOCK,
        ReasonChannel.BLOCKING,
        _ORDER_PLAN_HINT,
    ),
}


def _classify(reason: str) -> ReasonRule:
    explicit = EXPLICIT_REASON_MAP.get(reason)
    if explicit is not None:
        return explicit
    if "RECONCILE" in reason or "MISMATCH" in reason:
        return (
            StageCategory.RECONCILE,
            BlockType.SAFETY_BLOCK,
            ReasonChannel.BLOCKING,
            _RECONCILE_HINT,
        )
    if any(token in reason for token in ("DUPLICATE", "OPEN_POSITION", "POSITION_EXISTS")):
        return (
            StageCategory.DUPLICATE_POSITION,
            BlockType.HARD_BLOCK,
            ReasonChannel.BLOCKING,
            _DUPLICATE_HINT,
        )
    if "KILL_SWITCH" in reason or "SAFETY_GATE" in reason:
        return (
            StageCategory.LIVE_SIM_SAFETY,
            BlockType.SAFETY_BLOCK,
            ReasonChannel.BLOCKING,
            _SAFETY_HINT,
        )
    if any(token in reason for token in ("HEARTBEAT", "GATEWAY", "ORDERABLE")):
        return (
            StageCategory.GATEWAY,
            BlockType.SAFETY_BLOCK,
            ReasonChannel.BLOCKING,
            _GATEWAY_HINT,
        )
    if any(token in reason for token in ("DISABLED", "ROUTING", "AUTO_QUEUE", "PROFILE")):
        return (
            StageCategory.CONFIG,
            BlockType.CONFIG_DISABLED,
            ReasonChannel.BLOCKING,
            _CONFIG_HINT,
        )
    if any(token in reason for token in ("LIMIT", "NOTIONAL", "QUANTITY", "DAILY")):
        return (
            StageCategory.LIMIT,
            BlockType.HARD_BLOCK,
            ReasonChannel.BLOCKING,
            _LIMIT_HINT,
        )
    if "ORDER_PLAN" in reason or "PLAN_" in reason:
        if any(token in reason for token in ("NOT_READY", "EXPIRED", "NOT_LATEST", "MISSING")):
            return (
                StageCategory.ORDER_PLAN,
                BlockType.SOFT_WAIT,
                ReasonChannel.WAITING,
                _ORDER_PLAN_HINT,
            )
        if "TICK" in reason or "DATA" in reason or "STALE" in reason:
            return (
                StageCategory.DATA_WAIT,
                BlockType.DATA_WAIT,
                ReasonChannel.WAITING,
                _DATA_WAIT_HINT,
            )
        return (
            StageCategory.ORDER_PLAN,
            BlockType.HARD_BLOCK,
            ReasonChannel.BLOCKING,
            _ORDER_PLAN_HINT,
        )
    if "ENTRY" in reason or "WAIT_RETRY" in reason or "NO_SETUP" in reason:
        return (
            StageCategory.ENTRY_TIMING,
            BlockType.SOFT_WAIT,
            ReasonChannel.WAITING,
            _ENTRY_HINT,
        )
    if "THEME" in reason or "WATCHSET" in reason:
        block_type = BlockType.DATA_WAIT if "DATA_WAIT" in reason else BlockType.SOFT_WAIT
        return (
            StageCategory.THEME,
            block_type,
            ReasonChannel.WAITING,
            _THEME_HINT,
        )
    if "CANDIDATE" in reason:
        block_type = (
            BlockType.DATA_WAIT
            if "CONTEXT" in reason or "DATA" in reason
            else BlockType.SOFT_WAIT
        )
        return (
            StageCategory.CANDIDATE,
            block_type,
            ReasonChannel.WAITING,
            _CANDIDATE_HINT,
        )
    if "STRATEGY" in reason:
        return (
            StageCategory.STRATEGY,
            BlockType.THRESHOLD_MISS,
            ReasonChannel.WAITING,
            _STRATEGY_HINT,
        )
    if "RISK" in reason:
        return (
            StageCategory.RISK,
            BlockType.HARD_BLOCK,
            ReasonChannel.BLOCKING,
            _RISK_HINT,
        )
    if "DATA_WAIT" in reason or "STALE" in reason or "MISSING" in reason:
        return (
            StageCategory.DATA_WAIT,
            BlockType.DATA_WAIT,
            ReasonChannel.WAITING,
            _DATA_WAIT_HINT,
        )
    if "LIFECYCLE" in reason:
        return (
            StageCategory.EXECUTION_LIFECYCLE,
            BlockType.SAFETY_BLOCK,
            ReasonChannel.BLOCKING,
            "LIVE_SIM lifecycle error 이벤트를 확인합니다.",
        )
    return (
        StageCategory.UNKNOWN,
        BlockType.NOT_APPLICABLE,
        ReasonChannel.INFO,
        _UNKNOWN_HINT,
    )
