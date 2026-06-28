from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from services.operator.models import BlockType, ReasonClassification, StageCategory


def classify_reason(reason_code: object) -> ReasonClassification:
    reason = str(reason_code or "UNKNOWN").strip().upper() or "UNKNOWN"
    stage, block_type, hint = _classify(reason)
    return ReasonClassification(
        reason_code=reason,
        stage=stage,
        block_type=block_type,
        operator_hint=hint,
    )


def classify_reasons(reason_codes: Iterable[object]) -> list[ReasonClassification]:
    return [classify_reason(reason) for reason in reason_codes]


def primary_classification(reason_codes: Iterable[object]) -> ReasonClassification:
    classifications = classify_reasons(reason_codes)
    if not classifications:
        return classify_reason("UNKNOWN")
    return sorted(classifications, key=lambda item: _PRIORITY[item.block_type])[0]


def aggregate_reason_summary(reason_codes: Iterable[object]) -> dict[str, Any]:
    reason_counter: Counter[str] = Counter()
    stage_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    for classification in classify_reasons(reason_codes):
        reason_counter[classification.reason_code] += 1
        stage_counter[classification.stage.value] += 1
        type_counter[classification.block_type.value] += 1
    return {
        "reason_counts": dict(reason_counter),
        "stage_counts": dict(stage_counter),
        "block_type_counts": dict(type_counter),
    }


def summarize_classifications(items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    stage_counter: Counter[str] = Counter()
    type_counter: Counter[str] = Counter()
    hard_block_count = 0
    data_wait_count = 0
    soft_wait_count = 0
    for item in items:
        stage = str(item.get("primary_block_stage") or StageCategory.UNKNOWN.value)
        block_type = str(item.get("primary_block_type") or BlockType.NOT_APPLICABLE.value)
        stage_counter[stage] += 1
        type_counter[block_type] += 1
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


def _classify(reason: str) -> tuple[StageCategory, BlockType, str]:
    if reason in {"AI_NO_TRADE", "AI_SELECTED_EMPTY", "AI_NO_SELECTION"}:
        return (
            StageCategory.AI_ADVISORY,
            BlockType.AI_NO_TRADE,
            "AI 관망 사유를 확인하되 주문 가능 상태로 승격하지 않습니다.",
        )
    if reason in {"AI_UNAVAILABLE", "TIMEOUT", "INVALID_SCHEMA", "PROVIDER_ERROR"}:
        return (
            StageCategory.AI_ADVISORY,
            BlockType.NOT_APPLICABLE,
            "AI 실행 실패는 시스템 안전 차단과 분리해서 확인합니다.",
        )
    if "RECONCILE" in reason or "MISMATCH" in reason:
        return (
            StageCategory.RECONCILE,
            BlockType.SAFETY_BLOCK,
            "reconcile snapshot의 mismatch와 blocking_new_buy 값을 확인합니다.",
        )
    if any(token in reason for token in ("DUPLICATE", "OPEN_POSITION", "POSITION_EXISTS")):
        return (
            StageCategory.DUPLICATE_POSITION,
            BlockType.HARD_BLOCK,
            "동일 종목 주문/포지션/스케일인 제한을 확인합니다.",
        )
    if "KILL_SWITCH" in reason or "SAFETY_GATE" in reason:
        return (
            StageCategory.LIVE_SIM_SAFETY,
            BlockType.SAFETY_BLOCK,
            "LIVE_SIM safety gate와 kill switch 상태를 확인합니다.",
        )
    if any(token in reason for token in ("HEARTBEAT", "GATEWAY", "ORDERABLE", "QUEUE_HEALTHY")):
        return (
            StageCategory.GATEWAY,
            BlockType.SAFETY_BLOCK,
            "Gateway heartbeat, orderable, command queue 상태를 확인합니다.",
        )
    if any(token in reason for token in ("DISABLED", "ROUTING", "AUTO_QUEUE", "PROFILE")):
        return (
            StageCategory.CONFIG,
            BlockType.CONFIG_DISABLED,
            "설정 flag가 의도대로 켜져 있는지 확인합니다.",
        )
    if any(token in reason for token in ("LIMIT", "NOTIONAL", "QUANTITY", "DAILY")):
        return (
            StageCategory.LIMIT,
            BlockType.HARD_BLOCK,
            "일일 주문/금액/수량 한도와 주문 크기를 확인합니다.",
        )
    if "ORDER_PLAN" in reason or "PLAN_" in reason:
        if any(token in reason for token in ("NOT_READY", "EXPIRED", "NOT_LATEST")):
            return (
                StageCategory.ORDER_PLAN,
                BlockType.SOFT_WAIT,
                "OrderPlanDraft 상태와 만료 시각을 확인합니다.",
            )
        if "TICK" in reason or "DATA" in reason or "STALE" in reason:
            return (
                StageCategory.DATA_WAIT,
                BlockType.DATA_WAIT,
                "가격/tick/bar 데이터의 최신성과 누락 여부를 확인합니다.",
            )
        return (
            StageCategory.ORDER_PLAN,
            BlockType.HARD_BLOCK,
            "OrderPlanDraft의 reason code를 확인합니다.",
        )
    if "ENTRY" in reason or "WAIT_RETRY" in reason or "NO_SETUP" in reason:
        return (
            StageCategory.ENTRY_TIMING,
            BlockType.SOFT_WAIT,
            "EntryTiming 상태가 재시도/관망인지 확인합니다.",
        )
    if "THEME" in reason or "WATCHSET" in reason:
        block_type = BlockType.DATA_WAIT if "DATA_WAIT" in reason else BlockType.SOFT_WAIT
        return (
            StageCategory.THEME,
            block_type,
            "ThemeLeadership snapshot과 watchset 선별 결과를 확인합니다.",
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
            "Candidate FSM 상태와 context readiness를 확인합니다.",
        )
    if "STRATEGY" in reason:
        return (
            StageCategory.STRATEGY,
            BlockType.THRESHOLD_MISS,
            "Strategy observation의 setup/status를 확인합니다.",
        )
    if "RISK" in reason:
        return (
            StageCategory.RISK,
            BlockType.HARD_BLOCK,
            "Risk observation의 block/caution reason을 확인합니다.",
        )
    if "DATA_WAIT" in reason or "STALE" in reason or "MISSING" in reason:
        return (
            StageCategory.DATA_WAIT,
            BlockType.DATA_WAIT,
            "누락 또는 지연된 입력 데이터를 확인합니다.",
        )
    if "LIFECYCLE" in reason:
        return (
            StageCategory.EXECUTION_LIFECYCLE,
            BlockType.SAFETY_BLOCK,
            "LIVE_SIM lifecycle error 이벤트를 확인합니다.",
        )
    return (
        StageCategory.UNKNOWN,
        BlockType.NOT_APPLICABLE,
        "분류되지 않은 reason이므로 원본 payload를 확인합니다.",
    )
