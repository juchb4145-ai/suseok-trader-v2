from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now

from services.config import Settings, load_settings
from services.entry_timing.models import (
    EntryTimingEvaluation,
    EntryTimingInput,
    EntryTimingState,
    OrderPlanDraft,
    OrderPlanStatus,
)
from services.entry_timing.tick_size import add_ticks

READY_TIMING_STATES = {
    EntryTimingState.GOOD_PULLBACK,
    EntryTimingState.PULLBACK_RECLAIM,
    EntryTimingState.VWAP_RECLAIM,
    EntryTimingState.MOMENTUM_CONTINUATION,
}
MOMENTUM_CONTINUATION_SIZE_MULTIPLIER = 0.5
MOMENTUM_CONTINUATION_TTL_MULTIPLIER = 0.5


@dataclass(frozen=True, kw_only=True)
class LimitPriceResult:
    limit_price: int | None
    source: str
    reason_codes: tuple[str, ...]


class OrderPlanDraftBuilder:
    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()

    def build(
        self,
        item: EntryTimingInput,
        evaluation: EntryTimingEvaluation,
    ) -> OrderPlanDraft | None:
        status, status_reasons = self.resolve_status(item, evaluation)
        if status not in {
            OrderPlanStatus.PLAN_READY,
            OrderPlanStatus.WAIT_RETRY,
            OrderPlanStatus.DATA_WAIT,
        }:
            return None

        limit_price = calculate_limit_price(item, settings=self.settings)
        if limit_price.limit_price is None:
            return None

        idempotency_key = make_order_plan_idempotency_key(item, evaluation)
        order_plan_id = order_plan_id_from_key(idempotency_key)
        route_controls = _route_risk_controls(evaluation, self.settings)
        max_notional = float(self.settings.entry_timing_max_notional)
        base_notional = float(self.settings.entry_timing_default_notional)
        configured_notional = min(
            base_notional * float(route_controls["size_multiplier"]),
            max_notional,
        )
        quantity = int(configured_notional // limit_price.limit_price)
        if (
            status is OrderPlanStatus.PLAN_READY
            and quantity < 1
            and limit_price.limit_price <= max_notional
        ):
            quantity = 1
            route_controls["min_quantity_floor_applied"] = True
            route_controls["reason_codes"] = [
                *[str(reason) for reason in route_controls["reason_codes"]],
                "MOMENTUM_CONTINUATION_MIN_QUANTITY_FLOOR",
            ]
        if status is not OrderPlanStatus.PLAN_READY:
            quantity = 0
        suggested_notional = float(quantity * limit_price.limit_price)
        created_at = utc_now()
        ttl_seconds = int(route_controls["ttl_seconds"])
        expires_at = created_at + timedelta(seconds=ttl_seconds)
        priority_score, priority_reasons, condition_priority = _order_plan_priority(item)
        reasons = _dedupe(
            [
                *evaluation.reason_codes,
                *status_reasons,
                *limit_price.reason_codes,
                *priority_reasons,
                *[str(reason) for reason in route_controls["reason_codes"]],
                "BUY_LIMIT_ONLY",
                "PLAN_READY_NOT_ORDER_APPROVAL"
                if status is OrderPlanStatus.PLAN_READY
                else "NEAR_MISS_RETAINED",
            ]
        )
        return OrderPlanDraft(
            order_plan_id=order_plan_id,
            trade_date=item.trade_date,
            candidate_instance_id=item.candidate_instance_id,
            code=item.code,
            name=item.name,
            side="BUY",
            status=status,
            setup_type=evaluation.setup_type,
            entry_timing_state=evaluation.entry_timing_state,
            price_location_state=evaluation.price_location_state,
            theme_id=item.theme_id,
            theme_name=item.theme_name,
            theme_state=item.theme_state,
            theme_rank=item.theme_rank,
            stock_role=item.stock_role,
            priority_score=priority_score,
            current_price=float(item.current_price or 0),
            limit_price=float(limit_price.limit_price),
            limit_price_source=limit_price.source,
            limit_price_offset_ticks=self.settings.entry_timing_price_offset_ticks,
            suggested_quantity=quantity,
            suggested_notional=suggested_notional,
            max_notional=max_notional,
            risk_budget_source="ENTRY_TIMING_PILOT",
            expires_at=expires_at,
            idempotency_key=idempotency_key,
            reason_codes=reasons,
            evidence_json={
                "evaluation": evaluation.to_dict(),
                "configured_notional": configured_notional,
                "order_type": "LIMIT",
                "market_order_allowed": False,
                "safety_gate_required": True,
                "observe_only": True,
                "not_order_signal": True,
                "condition_fusion_priority": condition_priority,
                "entry_route": route_controls,
            },
            created_at=created_at,
        )

    def resolve_status(
        self,
        item: EntryTimingInput,
        evaluation: EntryTimingEvaluation,
    ) -> tuple[OrderPlanStatus, list[str]]:
        pipeline_coherency = item.raw_context.get("pipeline_coherency")
        if isinstance(pipeline_coherency, Mapping) and (
            str(pipeline_coherency.get("status") or "").upper() != "PASS"
        ):
            reasons = [
                str(reason).strip().upper()
                for reason in pipeline_coherency.get("reason_codes", [])
                if str(reason).strip()
            ]
            return OrderPlanStatus.DATA_WAIT, _dedupe(
                [*reasons, "PIPELINE_COHERENCY_GUARD_BLOCKED"]
            )
        if item.risk_observation_status == "OBSERVE_BLOCK":
            return OrderPlanStatus.BLOCKED_RISK, ["RISK_OBSERVE_BLOCK"]
        if item.risk_observation_status in {"DATA_WAIT", "INVALID_CONTEXT"}:
            return OrderPlanStatus.DATA_WAIT, ["RISK_DATA_WAIT"]
        if item.risk_observation_status == "STALE_CONTEXT":
            return OrderPlanStatus.BLOCKED_RISK, ["RISK_STALE_CONTEXT"]

        if evaluation.entry_timing_state not in READY_TIMING_STATES:
            return evaluation.status, []

        soft_reasons: list[str] = []
        if item.spread_ticks is None:
            soft_reasons.append("SPREAD_MISSING")
        elif item.spread_ticks > self.settings.entry_timing_max_spread_ticks:
            soft_reasons.append("SPREAD_TOO_WIDE")
        if item.turnover_krw is None:
            soft_reasons.append("TURNOVER_WARMUP")
        elif item.turnover_krw < self.settings.entry_timing_min_turnover_krw:
            soft_reasons.append("TURNOVER_BELOW_MIN")
        if item.execution_strength is None:
            soft_reasons.append("EXECUTION_STRENGTH_WARMUP")
        elif item.execution_strength < self.settings.entry_timing_min_execution_strength:
            soft_reasons.append("EXECUTION_STRENGTH_BELOW_MIN")
        if item.strategy_observation_status is None:
            soft_reasons.append("STRATEGY_OBSERVATION_MISSING")
        elif (
            self.settings.entry_timing_require_strategy_matched
            and item.strategy_observation_status != "MATCHED_OBSERVATION"
        ):
            soft_reasons.append("STRATEGY_NOT_MATCHED")
        if item.risk_observation_status is None:
            soft_reasons.append("RISK_OBSERVATION_MISSING")
        elif (
            self.settings.entry_timing_require_risk_observe_pass
            and item.risk_observation_status != "OBSERVE_PASS"
        ):
            soft_reasons.append("RISK_NOT_OBSERVE_PASS")
        if soft_reasons:
            return OrderPlanStatus.WAIT_RETRY, soft_reasons
        return OrderPlanStatus.PLAN_READY, ["PLAN_READY_DRAFT_ONLY"]


def calculate_limit_price(
    item: EntryTimingInput,
    *,
    settings: Settings | None = None,
) -> LimitPriceResult:
    resolved_settings = settings or load_settings()
    if resolved_settings.entry_timing_allow_market_order:
        return LimitPriceResult(
            limit_price=None,
            source="MARKET_DISABLED",
            reason_codes=("MARKET_ORDER_DISABLED",),
        )
    if item.current_price is None or item.current_price <= 0:
        return LimitPriceResult(
            limit_price=None,
            source="DATA_WAIT",
            reason_codes=("PRICE_MISSING",),
        )
    reasons: list[str] = []
    base_price = item.current_price
    source = "CURRENT_PRICE"
    if (
        item.best_ask is not None
        and item.best_ask > 0
        and item.spread_ticks is not None
        and item.spread_ticks <= resolved_settings.entry_timing_max_spread_ticks
    ):
        base_price = item.best_ask
        source = "BEST_ASK"
        reasons.append("BEST_ASK_REFERENCE")
    else:
        reasons.append("CURRENT_PRICE_REFERENCE")
    try:
        limit_price = add_ticks(base_price, resolved_settings.entry_timing_price_offset_ticks)
    except ValueError:
        return LimitPriceResult(
            limit_price=None,
            source="DATA_WAIT",
            reason_codes=("PRICE_MISSING",),
        )
    if limit_price <= 0:
        return LimitPriceResult(
            limit_price=None,
            source="DATA_WAIT",
            reason_codes=("LIMIT_PRICE_INVALID",),
        )
    return LimitPriceResult(
        limit_price=limit_price,
        source=source,
        reason_codes=tuple(reasons),
    )


def _order_plan_priority(
    item: EntryTimingInput,
) -> tuple[float | None, list[str], dict[str, object]]:
    base_score = item.theme_priority_score
    fusion_score = item.condition_fusion_priority_score
    condition_priority = {
        "condition_fusion_priority_score": fusion_score,
        "active_condition_roles": list(item.active_condition_roles),
        "condition_risk_blocked": item.condition_risk_blocked,
        "condition_fusion_reason_codes": list(item.condition_fusion_reason_codes),
        "condition_names": list(item.condition_names),
        "condition_latest_hit_at": item.condition_latest_hit_at,
        "not_order_signal": True,
        "not_order_approval": True,
        "used": False,
        "contribution": 0.0,
    }
    if base_score is None:
        return None, [], condition_priority
    reasons: list[str] = []
    score = float(base_score)
    blocked = item.condition_risk_blocked or "DISCOVERY_OBSERVATION_ONLY" in {
        str(reason).upper() for reason in item.condition_fusion_reason_codes
    }
    if fusion_score is not None and fusion_score > 0 and not blocked:
        contribution = min(float(fusion_score) * 0.02, 25.0)
        score += contribution
        condition_priority["used"] = True
        condition_priority["contribution"] = contribution
        reasons.extend(
            [
                "CONDITION_FUSION_PRIORITY_USED",
                "CONDITION_FUSION_NOT_ORDER_APPROVAL",
            ]
        )
    return score, reasons, condition_priority


def _route_risk_controls(
    evaluation: EntryTimingEvaluation,
    settings: Settings,
) -> dict[str, object]:
    if evaluation.entry_timing_state is EntryTimingState.MOMENTUM_CONTINUATION:
        ttl_seconds = max(
            1,
            int(settings.entry_timing_plan_ttl_seconds * MOMENTUM_CONTINUATION_TTL_MULTIPLIER),
        )
        return {
            "route": "MOMENTUM_CONTINUATION",
            "size_multiplier": MOMENTUM_CONTINUATION_SIZE_MULTIPLIER,
            "ttl_seconds": ttl_seconds,
            "tight_stop_required": True,
            "min_quantity_floor_applied": False,
            "reason_codes": [
                "MOMENTUM_CONTINUATION_SIZE_REDUCED",
                "MOMENTUM_CONTINUATION_SHORT_TTL",
                "MOMENTUM_CONTINUATION_TIGHT_STOP_REQUIRED",
            ],
        }
    return {
        "route": evaluation.entry_timing_state.value,
        "size_multiplier": 1.0,
        "ttl_seconds": settings.entry_timing_plan_ttl_seconds,
        "tight_stop_required": False,
        "min_quantity_floor_applied": False,
        "reason_codes": [],
    }


def make_order_plan_idempotency_key(
    item: EntryTimingInput,
    evaluation: EntryTimingEvaluation,
) -> str:
    theme = item.theme_id or item.theme_name or "NO_THEME"
    payload = "|".join(
        [
            item.trade_date,
            item.code,
            theme,
            evaluation.setup_type.value,
            evaluation.entry_timing_state.value,
            "BUY",
            item.candidate_instance_id,
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return (
        f"ORDERPLAN-{item.trade_date}-{item.code}-"
        f"{evaluation.setup_type.value}-{evaluation.entry_timing_state.value}-{digest}"
    )


def order_plan_id_from_key(idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:20]
    return f"OPD-{digest}"


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value).upper() for value in values if str(value).strip())]


def wire_ts(value: object) -> str:
    return datetime_to_wire(parse_timestamp(value, "timestamp"))
