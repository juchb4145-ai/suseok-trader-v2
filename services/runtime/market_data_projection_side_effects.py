from __future__ import annotations

import logging
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.utils import BrokerValidationError, datetime_to_wire, utc_now

from services.candidate_quote_refresh import candidate_quote_refresh_codes_from_payload
from services.condition_fusion import rebuild_condition_fusion_for_code
from services.config import Settings, load_settings
from services.runtime.incremental_evaluation import (
    DIRTY_REASON_CANDIDATE_QUOTE_REFRESH,
    enqueue_incremental_evaluation_for_code,
    enqueue_incremental_evaluation_for_event,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class MarketDataProjectionSideEffectResult:
    status: str
    event_id: str
    event_type: str
    source: str
    side_effect_type: str
    code_count: int = 0
    enqueued_count: int = 0
    ignored_count: int = 0
    processed_count: int = 0
    applied_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    code: str | None = None
    codes: Sequence[str] = field(default_factory=tuple)
    statuses: Sequence[str] = field(default_factory=tuple)
    candidate_ids: Sequence[str] = field(default_factory=tuple)
    errors: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    reason_codes: Sequence[str] = field(default_factory=tuple)
    evidence: Mapping[str, Any] = field(default_factory=dict)
    no_order_side_effects: bool = True
    created_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "side_effect_type": self.side_effect_type,
            "code_count": self.code_count,
            "enqueued_count": self.enqueued_count,
            "ignored_count": self.ignored_count,
            "processed_count": self.processed_count,
            "applied_count": self.applied_count,
            "skipped_count": self.skipped_count,
            "error_count": self.error_count,
            "code": self.code,
            "codes": list(self.codes),
            "statuses": list(self.statuses),
            "candidate_ids": list(self.candidate_ids),
            "errors": [dict(error) for error in self.errors],
            "reason_codes": list(self.reason_codes),
            "evidence": dict(self.evidence),
            "no_order_side_effects": self.no_order_side_effects,
            "no_trading_side_effects": True,
            "real_order_allowed": False,
            "created_at": self.created_at,
        }


def enqueue_incremental_for_price_tick_projection(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
    source: str,
) -> MarketDataProjectionSideEffectResult:
    resolved_settings = settings or load_settings()
    try:
        result = enqueue_incremental_evaluation_for_event(
            connection,
            event,
            settings=resolved_settings,
        )
    except Exception as exc:
        logger.exception("price_tick incremental side effect enqueue failed")
        return MarketDataProjectionSideEffectResult(
            status="ERROR",
            event_id=event.event_id,
            event_type=event.event_type,
            source=source,
            side_effect_type="price_tick_incremental_evaluation",
            error_count=1,
            errors=({"event_id": event.event_id, "error_message": str(exc)},),
            reason_codes=("PRICE_TICK_INCREMENTAL_ENQUEUE_ERROR",),
        )
    return MarketDataProjectionSideEffectResult(
        status=result.status,
        event_id=event.event_id,
        event_type=event.event_type,
        source=source,
        side_effect_type="price_tick_incremental_evaluation",
        code_count=1 if result.code else 0,
        enqueued_count=result.enqueued_count,
        ignored_count=0 if result.enqueued_count else 1,
        codes=(result.code,) if result.code else (),
        statuses=(result.status,),
        candidate_ids=tuple(result.candidate_ids),
        reason_codes=(f"PRICE_TICK_INCREMENTAL_{result.status}",),
        evidence={"enqueue_result": result.to_dict()},
    )


def enqueue_incremental_for_candidate_quote_refresh_tr_response(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
    source: str,
) -> MarketDataProjectionSideEffectResult:
    resolved_settings = settings or load_settings()
    codes = tuple(candidate_quote_refresh_codes_from_payload(event.payload))
    evidence = {
        "parent_event_id": event.event_id,
        "parent_command_id": event.command_id,
        "parent_tr_code": event.payload.get("tr_code"),
        "parent_request_name": event.payload.get("request_name"),
        "source": source,
        "no_order_side_effects": True,
    }
    if not codes:
        return MarketDataProjectionSideEffectResult(
            status="SKIPPED",
            event_id=event.event_id,
            event_type=event.event_type,
            source=source,
            side_effect_type="candidate_quote_refresh_incremental_evaluation",
            code_count=0,
            reason_codes=("CANDIDATE_QUOTE_REFRESH_CODES_EMPTY",),
            evidence=evidence,
        )

    statuses: list[str] = []
    candidate_ids: list[str] = []
    errors: list[dict[str, Any]] = []
    enqueued_count = 0
    ignored_count = 0
    for code in codes:
        try:
            result = enqueue_incremental_evaluation_for_code(
                connection,
                code,
                reason=DIRTY_REASON_CANDIDATE_QUOTE_REFRESH,
                source_event_id=event.event_id,
                event_id=event.event_id,
                priority=90,
                settings=resolved_settings,
            )
        except Exception as exc:
            logger.exception("candidate quote refresh incremental side effect failed")
            statuses.append("ERROR")
            errors.append({"code": code, "error_message": str(exc)})
            continue
        statuses.append(result.status)
        enqueued_count += int(result.enqueued_count)
        if result.enqueued_count:
            candidate_ids.extend(str(candidate_id) for candidate_id in result.candidate_ids)
        else:
            ignored_count += 1

    error_count = len(errors)
    reason_codes = _candidate_quote_refresh_reason_codes(
        statuses,
        error_count=error_count,
    )
    status = _candidate_quote_refresh_status(
        statuses,
        enqueued_count=enqueued_count,
        error_count=error_count,
    )
    return MarketDataProjectionSideEffectResult(
        status=status,
        event_id=event.event_id,
        event_type=event.event_type,
        source=source,
        side_effect_type="candidate_quote_refresh_incremental_evaluation",
        code_count=len(codes),
        enqueued_count=enqueued_count,
        ignored_count=ignored_count,
        error_count=error_count,
        codes=codes,
        statuses=tuple(statuses),
        candidate_ids=tuple(dict.fromkeys(candidate_ids)),
        errors=tuple(errors),
        reason_codes=tuple(reason_codes),
        evidence=evidence,
    )


def refresh_condition_fusion_for_condition_event_projection(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
    source: str,
) -> MarketDataProjectionSideEffectResult:
    resolved_settings = settings or load_settings()
    base_evidence: dict[str, Any] = {
        "parent_event_id": event.event_id,
        "parent_command_id": event.command_id,
        "parent_idempotency_key": event.idempotency_key,
        "source": source,
        "candidate_ingest_executed": False,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    if not resolved_settings.condition_fusion_event_incremental_enabled:
        return MarketDataProjectionSideEffectResult(
            status="SKIPPED",
            event_id=event.event_id,
            event_type=event.event_type,
            source=source,
            side_effect_type="condition_fusion_refresh",
            skipped_count=1,
            reason_codes=("CONDITION_FUSION_INCREMENTAL_DISABLED",),
            evidence=base_evidence,
        )
    try:
        condition = BrokerConditionEvent.from_dict(event.payload)
    except (BrokerValidationError, ValueError) as exc:
        logger.exception("condition_event payload invalid for condition fusion refresh")
        return MarketDataProjectionSideEffectResult(
            status="ERROR",
            event_id=event.event_id,
            event_type=event.event_type,
            source=source,
            side_effect_type="condition_fusion_refresh",
            error_count=1,
            errors=({"event_id": event.event_id, "error_message": str(exc)},),
            reason_codes=("CONDITION_EVENT_PAYLOAD_INVALID",),
            evidence={**base_evidence, "error_message": str(exc)},
        )
    try:
        result = rebuild_condition_fusion_for_code(
            connection,
            condition.code,
            settings=resolved_settings,
        )
    except Exception as exc:
        logger.exception("condition_fusion refresh side effect failed")
        return MarketDataProjectionSideEffectResult(
            status="ERROR",
            event_id=event.event_id,
            event_type=event.event_type,
            source=source,
            side_effect_type="condition_fusion_refresh",
            code=condition.code,
            code_count=1,
            error_count=1,
            errors=({"code": condition.code, "error_message": str(exc)},),
            reason_codes=("CONDITION_FUSION_REFRESH_ERROR",),
            evidence={**base_evidence, "condition_code": condition.code},
        )

    status = "APPLIED" if result.fused_code_count else "IGNORED_NO_PROFILE"
    processed_count = int(result.processed_event_count)
    applied_count = int(result.fused_code_count)
    skipped_count = 0 if applied_count else 1
    return MarketDataProjectionSideEffectResult(
        status=status,
        event_id=event.event_id,
        event_type=event.event_type,
        source=source,
        side_effect_type="condition_fusion_refresh",
        code=condition.code,
        code_count=1,
        processed_count=processed_count,
        applied_count=applied_count,
        skipped_count=skipped_count,
        statuses=(status,),
        reason_codes=(
            "CONDITION_FUSION_REFRESH_APPLIED"
            if applied_count
            else "CONDITION_FUSION_REFRESH_NO_PROFILE",
        ),
        evidence={
            **base_evidence,
            "condition_code": condition.code,
            "condition_name": condition.condition_name,
            "condition_action": condition.action.value,
            "processed_event_count": processed_count,
            "fused_code_count": applied_count,
            "rebuild_result": result.to_dict(),
        },
    )


def legacy_gateway_candidate_quote_refresh_status(
    result: MarketDataProjectionSideEffectResult,
) -> str | None:
    if result.status == "SKIPPED" and "CANDIDATE_QUOTE_REFRESH_CODES_EMPTY" in set(
        result.reason_codes
    ):
        return None
    statuses = [str(status) for status in result.statuses]
    if any(status == "ENQUEUED" for status in statuses):
        return "ENQUEUED"
    if result.error_count > 0:
        return "ERROR"
    if not statuses:
        return None if result.status == "SKIPPED" else result.status
    return ",".join(sorted(set(statuses)))


def _candidate_quote_refresh_status(
    statuses: Sequence[str],
    *,
    enqueued_count: int,
    error_count: int,
) -> str:
    if error_count > 0:
        return "COMPLETED_WITH_ERRORS"
    if enqueued_count > 0 or any(status == "ENQUEUED" for status in statuses):
        return "ENQUEUED"
    if not statuses:
        return "SKIPPED"
    return ",".join(sorted(set(statuses)))


def _candidate_quote_refresh_reason_codes(
    statuses: Sequence[str],
    *,
    error_count: int,
) -> list[str]:
    reasons: list[str] = []
    if any(status == "ENQUEUED" for status in statuses):
        reasons.append("CANDIDATE_QUOTE_REFRESH_INCREMENTAL_ENQUEUED")
    if error_count > 0:
        reasons.append("CANDIDATE_QUOTE_REFRESH_ENQUEUE_ERROR")
    for status in sorted(set(statuses)):
        if status != "ENQUEUED":
            reasons.append(f"CANDIDATE_QUOTE_REFRESH_{status}")
    return reasons or ["CANDIDATE_QUOTE_REFRESH_NOOP"]
