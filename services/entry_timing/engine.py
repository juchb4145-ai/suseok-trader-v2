from __future__ import annotations

from collections.abc import Sequence

from domain.broker.utils import new_message_id, utc_now

from services.config import Settings, load_settings
from services.entry_timing.models import (
    EntryTimingEvaluation,
    EntryTimingInput,
    EntryTimingState,
    OrderPlanStatus,
    PriceLocationResult,
    PriceLocationState,
    SetupType,
)
from services.entry_timing.price_location import PriceLocationClassifier


class EntryTimingEngine:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        classifier: PriceLocationClassifier | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self.classifier = classifier or PriceLocationClassifier(settings=self.settings)

    def evaluate(self, item: EntryTimingInput) -> EntryTimingEvaluation:
        price_location = self.classifier.classify(item)
        state, setup_type, status, reasons = self._classify_timing(item, price_location)
        evidence = {
            "input": item.to_dict(),
            "price_location": price_location.to_dict(),
            "thresholds": _threshold_evidence(self.settings),
            "allowed_timing": state
            in {
                EntryTimingState.GOOD_PULLBACK,
                EntryTimingState.PULLBACK_RECLAIM,
                EntryTimingState.VWAP_RECLAIM,
            },
            "observe_only": True,
            "not_order_signal": True,
        }
        return EntryTimingEvaluation(
            entry_timing_evaluation_id=new_message_id("entry_timing_eval"),
            trade_date=item.trade_date,
            candidate_instance_id=item.candidate_instance_id,
            code=item.code,
            name=item.name,
            evaluated_at=utc_now(),
            setup_type=setup_type,
            entry_timing_state=state,
            price_location_state=price_location.state,
            status=status,
            reason_codes=_dedupe([*price_location.reason_codes, *reasons, "OBSERVE_ONLY"]),
            evidence_json=evidence,
        )

    def _classify_timing(
        self,
        item: EntryTimingInput,
        price_location: PriceLocationResult,
    ) -> tuple[EntryTimingState, SetupType, OrderPlanStatus, list[str]]:
        hard_state = _hard_context_state(item, self.settings)
        if hard_state is not None:
            return hard_state

        metrics = price_location.metrics
        pullback = _float_or_none(metrics.get("pullback_from_high_pct"))
        price_vs_vwap = _float_or_none(metrics.get("price_vs_vwap_pct"))
        momentum_ready = _any_number(item.momentum_1m, item.momentum_3m, item.momentum_5m)
        momentum_positive = _positive(item.momentum_1m) or _positive(item.momentum_3m)
        momentum_negative = _negative(item.momentum_1m) and _negative(item.momentum_3m)
        role = _normalize_role(item.stock_role)
        theme_state = _normalize(item.theme_state)

        if item.vwap is None:
            return (
                EntryTimingState.DATA_WAIT,
                SetupType.NO_SETUP,
                OrderPlanStatus.DATA_WAIT,
                ["VWAP_MISSING", "DATA_WAIT_NEAR_MISS"],
            )
        if not momentum_ready:
            return (
                EntryTimingState.DATA_WAIT,
                SetupType.NO_SETUP,
                OrderPlanStatus.DATA_WAIT,
                ["MOMENTUM_WARMUP", "DATA_WAIT_NEAR_MISS"],
            )
        if price_location.state is PriceLocationState.EXTENDED_FROM_VWAP:
            return (
                EntryTimingState.VWAP_OVEREXTENDED,
                SetupType.NO_SETUP,
                OrderPlanStatus.BLOCKED_OVERHEAT,
                ["VWAP_OVEREXTENDED"],
            )
        if price_location.state is PriceLocationState.NEAR_DAY_HIGH or (
            pullback is not None and pullback < self.settings.entry_timing_pullback_min_pct
        ):
            return (
                EntryTimingState.CHASE_HIGH,
                SetupType.NO_SETUP,
                OrderPlanStatus.BLOCKED_CHASE,
                ["CHASE_HIGH", "PULLBACK_TOO_SHALLOW"],
            )
        if (
            pullback is not None
            and pullback > self.settings.entry_timing_pullback_max_pct
            and momentum_negative
            and price_vs_vwap is not None
            and price_vs_vwap < 0
        ):
            return (
                EntryTimingState.FAILED_BREAKOUT,
                SetupType.BREAKOUT_RETEST,
                OrderPlanStatus.NO_PLAN,
                ["FAILED_BREAKOUT", "MOMENTUM_WEAKENING", "BELOW_VWAP"],
            )

        if _is_vwap_reclaim(price_vs_vwap, momentum_positive, self.settings):
            if role in {"LEADER", "CO_LEADER"} or _follower_allowed(theme_state, self.settings):
                return (
                    EntryTimingState.VWAP_RECLAIM,
                    SetupType.VWAP_RECLAIM,
                    OrderPlanStatus.PLAN_READY,
                    ["VWAP_RECLAIM", "ENTRY_TIMING_ALLOWED"],
                )

        if _is_pullback_reclaim(item, price_vs_vwap, pullback, momentum_positive, self.settings):
            if role in {"LEADER", "CO_LEADER"}:
                return (
                    EntryTimingState.PULLBACK_RECLAIM,
                    SetupType.THEME_LEADER_PULLBACK,
                    OrderPlanStatus.PLAN_READY,
                    ["PULLBACK_RECLAIM", "ENTRY_TIMING_ALLOWED"],
                )
            if _follower_allowed(theme_state, self.settings):
                return (
                    EntryTimingState.PULLBACK_RECLAIM,
                    SetupType.THEME_FOLLOWER_EXPANSION,
                    OrderPlanStatus.PLAN_READY,
                    ["PULLBACK_RECLAIM", "FOLLOWER_LIMITED_ALLOWED"],
                )

        if _is_good_pullback(item, pullback, self.settings):
            if role in {"LEADER", "CO_LEADER"}:
                return (
                    EntryTimingState.GOOD_PULLBACK,
                    SetupType.THEME_LEADER_PULLBACK,
                    OrderPlanStatus.PLAN_READY,
                    ["GOOD_PULLBACK", "ENTRY_TIMING_ALLOWED"],
                )

        return (
            EntryTimingState.NO_SETUP,
            SetupType.NO_SETUP,
            OrderPlanStatus.NO_PLAN,
            ["NO_ENTRY_TIMING_SETUP"],
        )


def _hard_context_state(
    item: EntryTimingInput,
    settings: Settings,
) -> tuple[EntryTimingState, SetupType, OrderPlanStatus, list[str]] | None:
    reasons: list[str] = []
    candidate_state = _normalize(item.candidate_state)
    theme_state = _normalize(item.theme_state)
    role = _normalize_role(item.stock_role)
    tick_age = item.tick_age_sec
    condition_reasons = {_normalize(reason) for reason in item.condition_fusion_reason_codes}
    theme_reasons = {_normalize(reason) for reason in item.theme_reason_codes}
    sensor_reasons = condition_reasons | theme_reasons
    if item.condition_risk_blocked or "CONDITION_RISK_BLOCKED" in sensor_reasons:
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            OrderPlanStatus.BLOCKED_RISK,
            ["CONDITION_RISK_BLOCKED", "CONDITION_SENSOR_NOT_BUY_SIGNAL"],
        )
    if "DISCOVERY_OBSERVATION_ONLY" in sensor_reasons:
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            OrderPlanStatus.NO_PLAN,
            ["DISCOVERY_OBSERVATION_ONLY", "CONDITION_SENSOR_NOT_BUY_SIGNAL"],
        )
    if item.stale or (tick_age is not None and tick_age > settings.entry_timing_stale_max_seconds):
        return (
            EntryTimingState.STALE,
            SetupType.NO_SETUP,
            OrderPlanStatus.BLOCKED_STALE,
            ["TICK_STALE"],
        )
    if candidate_state in {"CLOSED", "STALE"}:
        return (
            EntryTimingState.STALE,
            SetupType.NO_SETUP,
            OrderPlanStatus.BLOCKED_STALE,
            [f"CANDIDATE_{candidate_state}"],
        )
    if candidate_state == "BLOCKED_OBSERVATION":
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            OrderPlanStatus.NO_PLAN,
            ["CANDIDATE_BLOCKED_OBSERVATION"],
        )
    if item.vi_active:
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            OrderPlanStatus.BLOCKED_OVERHEAT,
            ["VI_ACTIVE"],
        )
    if item.upper_limit_near:
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            OrderPlanStatus.BLOCKED_OVERHEAT,
            ["UPPER_LIMIT_NEAR"],
        )
    if item.current_price is None or item.current_price <= 0:
        reasons.append("PRICE_MISSING")
    if item.day_high is None or item.day_low is None or item.day_high <= item.day_low:
        reasons.append("DAY_RANGE_MISSING")
    if reasons:
        return (
            EntryTimingState.DATA_WAIT,
            SetupType.NO_SETUP,
            OrderPlanStatus.DATA_WAIT,
            [*reasons, "DATA_WAIT_NEAR_MISS"],
        )
    if not theme_state:
        return (
            EntryTimingState.DATA_WAIT,
            SetupType.NO_SETUP,
            OrderPlanStatus.DATA_WAIT,
            ["THEME_CONTEXT_MISSING", "DATA_WAIT_NEAR_MISS"],
        )
    if not role:
        return (
            EntryTimingState.DATA_WAIT,
            SetupType.NO_SETUP,
            OrderPlanStatus.DATA_WAIT,
            ["STOCK_ROLE_MISSING", "DATA_WAIT_NEAR_MISS"],
        )
    if theme_state == "DATA_WAIT":
        return (
            EntryTimingState.DATA_WAIT,
            SetupType.NO_SETUP,
            OrderPlanStatus.DATA_WAIT,
            ["THEME_DATA_WAIT", "DATA_WAIT_NEAR_MISS"],
        )
    if theme_state not in {"LEADING", "SPREADING", "LEADER_ONLY"}:
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            OrderPlanStatus.NO_PLAN,
            [f"THEME_{theme_state}_NOT_ENTRY_ELIGIBLE"],
        )
    if role in {"OVERHEATED", "LATE_LAGGARD", "WEAK_MEMBER", "STALE", "UNKNOWN"}:
        status = (
            OrderPlanStatus.BLOCKED_OVERHEAT
            if role == "OVERHEATED"
            else OrderPlanStatus.BLOCKED_STALE
            if role == "STALE"
            else OrderPlanStatus.NO_PLAN
        )
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            status,
            [f"ROLE_{role}_BLOCKED"],
        )
    if role == "FOLLOWER" and theme_state == "LEADER_ONLY":
        return (
            EntryTimingState.BLOCKED_CONTEXT,
            SetupType.NO_SETUP,
            OrderPlanStatus.NO_PLAN,
            ["LEADER_ONLY_FOLLOWER_BLOCKED"],
        )
    return None


def _is_good_pullback(
    item: EntryTimingInput,
    pullback: float | None,
    settings: Settings,
) -> bool:
    theme_state = _normalize(item.theme_state)
    if theme_state not in {"LEADING", "SPREADING", "LEADER_ONLY"}:
        return False
    return pullback is not None and (
        settings.entry_timing_pullback_min_pct <= pullback <= settings.entry_timing_pullback_max_pct
    )


def _is_pullback_reclaim(
    item: EntryTimingInput,
    price_vs_vwap: float | None,
    pullback: float | None,
    momentum_positive: bool,
    settings: Settings,
) -> bool:
    if not momentum_positive or pullback is None:
        return False
    if pullback < settings.entry_timing_pullback_min_pct:
        return False
    if pullback > settings.entry_timing_pullback_max_pct:
        return False
    if price_vs_vwap is None:
        return False
    return price_vs_vwap >= -settings.entry_timing_vwap_reclaim_tolerance_pct


def _is_vwap_reclaim(
    price_vs_vwap: float | None,
    momentum_positive: bool,
    settings: Settings,
) -> bool:
    if price_vs_vwap is None or not momentum_positive:
        return False
    return -settings.entry_timing_vwap_reclaim_tolerance_pct <= price_vs_vwap <= (
        settings.entry_timing_vwap_reclaim_tolerance_pct
    )


def _follower_allowed(theme_state: str, settings: Settings) -> bool:
    if theme_state == "SPREADING":
        return settings.entry_timing_allow_follower_in_spreading
    if theme_state == "LEADING":
        return True
    if theme_state == "LEADER_ONLY":
        return settings.entry_timing_allow_follower_in_leader_only
    return False


def _normalize(value: str | None) -> str:
    return "" if value is None else value.strip().upper()


def _normalize_role(value: str | None) -> str:
    role = _normalize(value)
    mapping = {
        "LEADER_CANDIDATE": "LEADER",
        "CO_LEADER_CANDIDATE": "CO_LEADER",
        "FOLLOWER_CANDIDATE": "FOLLOWER",
        "LAGGARD": "LATE_LAGGARD",
    }
    return mapping.get(role, role)


def _positive(value: float | None) -> bool:
    return value is not None and value > 0


def _negative(value: float | None) -> bool:
    return value is not None and value < 0


def _any_number(*values: float | None) -> bool:
    return any(value is not None for value in values)


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _threshold_evidence(settings: Settings) -> dict[str, float | int | bool]:
    return {
        "pullback_min_pct": settings.entry_timing_pullback_min_pct,
        "pullback_max_pct": settings.entry_timing_pullback_max_pct,
        "vwap_reclaim_tolerance_pct": settings.entry_timing_vwap_reclaim_tolerance_pct,
        "vwap_overextended_pct": settings.entry_timing_vwap_overextended_pct,
        "chase_near_high_pct": settings.entry_timing_chase_near_high_pct,
        "max_spread_ticks": settings.entry_timing_max_spread_ticks,
        "min_turnover_krw": settings.entry_timing_min_turnover_krw,
        "min_execution_strength": settings.entry_timing_min_execution_strength,
        "stale_max_seconds": settings.entry_timing_stale_max_seconds,
        "allow_market_order": settings.entry_timing_allow_market_order,
    }


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value).upper() for value in values if str(value).strip())]
