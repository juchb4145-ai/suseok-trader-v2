from __future__ import annotations

from collections import Counter, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.condition_profiles import (
    ConditionProfile,
    ConditionRole,
    ConditionSessionProfile,
    PriceSubscribePolicy,
)
from domain.broker.utils import datetime_to_wire, utc_now

_STRONG_ROLES = {
    ConditionRole.LEADER,
    ConditionRole.PULLBACK,
    ConditionRole.BREAKOUT,
    ConditionRole.MANUAL,
}


@dataclass(frozen=True, kw_only=True)
class ConditionAdmissionDecision:
    profile_id: str
    role: str
    action: str
    source: str
    subscribed: bool
    register_immediate: bool = False
    register_batch: bool = False
    reason_codes: tuple[str, ...] = field(default_factory=tuple)
    priority: int = 0
    adaptive_budget: Mapping[str, Any] = field(default_factory=dict)
    decided_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "role": self.role,
            "action": self.action,
            "source": self.source,
            "subscribed": bool(self.subscribed),
            "register_immediate": bool(self.register_immediate),
            "register_batch": bool(self.register_batch),
            "reason_codes": list(self.reason_codes),
            "priority": int(self.priority),
            "adaptive_budget": dict(self.adaptive_budget),
            "decided_at": datetime_to_wire(self.decided_at),
        }


@dataclass
class _ConditionProfileMetric:
    profile_id: str
    condition_name: str
    role: str
    hit_count: int = 0
    enter_count: int = 0
    exit_count: int = 0
    subscribed_count: int = 0
    skipped_count: int = 0
    skip_reason_summary: Counter[str] = field(default_factory=Counter)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "condition_name": self.condition_name,
            "role": self.role,
            "hit_count": self.hit_count,
            "enter_count": self.enter_count,
            "exit_count": self.exit_count,
            "subscribed_count": self.subscribed_count,
            "skipped_count": self.skipped_count,
            "skip_reason_summary": dict(self.skip_reason_summary),
        }


class GatewayConditionAdmissionController:
    def __init__(self) -> None:
        self._metrics: dict[str, _ConditionProfileMetric] = {}
        self._realtime_enter_at: dict[str, deque[datetime]] = {}
        self._initial_enter_counts: Counter[str] = Counter()
        self._latest_budget: dict[str, Any] = {}

    def decide(
        self,
        *,
        profile: ConditionProfile,
        action: str,
        source: str,
        registered_realtime_count: int,
        runtime_metrics: Mapping[str, Any],
        session_profile: ConditionSessionProfile,
        batch_allowed: bool = False,
        planned_batch_count: int = 0,
        now: datetime | None = None,
    ) -> ConditionAdmissionDecision:
        observed_at = now or utc_now()
        normalized_action = str(action or "").upper()
        normalized_source = str(source or "").strip().lower()
        effective_registered_count = registered_realtime_count + max(
            int(planned_batch_count),
            0,
        )
        budget = self._adaptive_budget(
            profile=profile,
            registered_realtime_count=effective_registered_count,
            runtime_metrics=runtime_metrics,
            session_profile=session_profile,
        )
        reasons: list[str] = ["CONDITION_SENSOR_EVIDENCE", "MARKET_SENSOR_NOT_BUY_SIGNAL"]
        register_immediate = False
        register_batch = False

        if normalized_action != "ENTER":
            reasons.append("EXIT_EVENT_NO_IMMEDIATE_REMOVE")
        elif profile.role is ConditionRole.RISK_BLOCK:
            reasons.append("RISK_BLOCK_NO_PRICE_SUBSCRIBE")
        elif profile.price_subscribe_policy is PriceSubscribePolicy.NONE:
            reasons.append("PRICE_SUBSCRIBE_POLICY_NONE")
        elif (
            normalized_source == "tr_condition"
            and self._initial_enter_counts[profile.profile_id] >= profile.max_initial
        ):
            reasons.append("PROFILE_MAX_INITIAL_REACHED")
        elif effective_registered_count >= int(budget["cap"]):
            reasons.append("ADAPTIVE_REALTIME_BUDGET_EXHAUSTED")
        elif normalized_source == "real_condition" and not self._within_realtime_rate(
            profile,
            observed_at,
        ):
            reasons.append("PROFILE_REALTIME_RATE_LIMIT")
        elif (
            session_profile
            in {
                ConditionSessionProfile.MIDDAY,
                ConditionSessionProfile.AFTERNOON,
                ConditionSessionProfile.CLOSING,
            }
            and profile.role is ConditionRole.DISCOVERY
        ):
            reasons.append("SESSION_DISCOVERY_LIMITED")
            if effective_registered_count < int(budget["cap"]):
                register_batch = normalized_source == "tr_condition" and batch_allowed
        elif profile.price_subscribe_policy is PriceSubscribePolicy.IMMEDIATE:
            register_immediate = True
            reasons.append("PRICE_SUBSCRIBE_IMMEDIATE")
        elif profile.price_subscribe_policy is PriceSubscribePolicy.REAL_ONLY:
            if normalized_source == "real_condition":
                register_immediate = True
                reasons.append("PRICE_SUBSCRIBE_REAL_ONLY")
            else:
                reasons.append("PRICE_SUBSCRIBE_REAL_ONLY_SKIPPED_INITIAL")
        elif profile.price_subscribe_policy is PriceSubscribePolicy.BATCH:
            if normalized_source == "tr_condition" and batch_allowed:
                register_batch = True
                reasons.append("PRICE_SUBSCRIBE_BATCH_INITIAL")
            elif normalized_source == "real_condition":
                register_immediate = True
                reasons.append("PRICE_SUBSCRIBE_BATCH_REALTIME_IMMEDIATE")
            else:
                reasons.append("PRICE_SUBSCRIBE_BATCH_DEFERRED")

        subscribed = bool(register_immediate or register_batch)
        if subscribed:
            self._record_realtime_enter(profile.profile_id, observed_at)
            if normalized_source == "tr_condition":
                self._initial_enter_counts[profile.profile_id] += 1
        decision = ConditionAdmissionDecision(
            profile_id=profile.profile_id,
            role=profile.role.value,
            action=normalized_action,
            source=normalized_source,
            subscribed=subscribed,
            register_immediate=register_immediate,
            register_batch=register_batch,
            reason_codes=tuple(_dedupe(reasons)),
            priority=profile.priority,
            adaptive_budget=budget,
            decided_at=observed_at,
        )
        self._record_metric(profile, decision)
        self._latest_budget = dict(budget)
        return decision

    def metrics(self) -> list[dict[str, Any]]:
        return [
            metric.to_dict()
            for metric in sorted(
                self._metrics.values(),
                key=lambda item: (-item.hit_count, item.profile_id),
            )
        ]

    def latest_budget(self) -> dict[str, Any]:
        return dict(self._latest_budget)

    def _record_metric(
        self,
        profile: ConditionProfile,
        decision: ConditionAdmissionDecision,
    ) -> None:
        metric = self._metrics.setdefault(
            profile.profile_id,
            _ConditionProfileMetric(
                profile_id=profile.profile_id,
                condition_name=profile.condition_name,
                role=profile.role.value,
            ),
        )
        metric.hit_count += 1
        if decision.action == "ENTER":
            metric.enter_count += 1
        elif decision.action == "EXIT":
            metric.exit_count += 1
        if decision.subscribed:
            metric.subscribed_count += 1
        else:
            metric.skipped_count += 1
            for reason in decision.reason_codes:
                metric.skip_reason_summary[str(reason).upper()] += 1

    def _within_realtime_rate(self, profile: ConditionProfile, now: datetime) -> bool:
        if profile.max_realtime_per_min <= 0:
            return False
        entries = self._realtime_enter_at.setdefault(profile.profile_id, deque())
        while entries and (now - entries[0]).total_seconds() >= 60:
            entries.popleft()
        return len(entries) < profile.max_realtime_per_min

    def _record_realtime_enter(self, profile_id: str, now: datetime) -> None:
        self._realtime_enter_at.setdefault(profile_id, deque()).append(now)

    def _adaptive_budget(
        self,
        *,
        profile: ConditionProfile,
        registered_realtime_count: int,
        runtime_metrics: Mapping[str, Any],
        session_profile: ConditionSessionProfile,
    ) -> dict[str, Any]:
        quality, quality_reasons = _quality(runtime_metrics)
        if profile.role in _STRONG_ROLES:
            cap = 100
        elif profile.role is ConditionRole.RISK_BLOCK:
            cap = 0
        else:
            cap = _discovery_session_cap(session_profile)
        if quality == "DEGRADED":
            cap = min(cap, 30 if profile.role in _STRONG_ROLES else 5)
        elif quality == "CALLBACK_TIMEOUT":
            cap = min(cap, 10 if profile.role in _STRONG_ROLES else 0)
        elif quality == "WARMUP":
            cap = min(cap, 20 if profile.role in _STRONG_ROLES else 8)
        return {
            "quality_status": quality,
            "reason_codes": quality_reasons,
            "cap": max(int(cap), 0),
            "registered_realtime_code_count": int(registered_realtime_count),
            "session_profile": session_profile.value,
            "role": profile.role.value,
        }


def _quality(runtime_metrics: Mapping[str, Any]) -> tuple[str, list[str]]:
    health = str(runtime_metrics.get("realtime_subscription_health") or "").upper()
    parsed = int(runtime_metrics.get("parsed_price_tick_count") or 0)
    errors = int(runtime_metrics.get("realtime_parse_error_count") or 0)
    callbacks = int(runtime_metrics.get("realtime_callback_count") or 0)
    registered = int(runtime_metrics.get("registered_realtime_code_count") or 0)
    reasons: list[str] = []
    if health in {
        "CALLBACK_TIMEOUT",
        "ACTIVE_X_CALLBACK_SUSPECTED",
        "CORE_IO_BLOCKING_SUSPECTED",
    }:
        reasons.append(health)
        return "CALLBACK_TIMEOUT", reasons
    if health == "PARSE_ERROR" or (errors >= 3 and errors > max(parsed, 1) / 2):
        reasons.append("REALTIME_PARSE_ERRORS_HIGH")
        return "DEGRADED", reasons
    if registered > 0 and callbacks == 0 and parsed == 0:
        reasons.append("WAITING_FOR_FIRST_REALTIME_CALLBACK")
        return "WARMUP", reasons
    return "GOOD", ["REALTIME_QUALITY_OK"]


def _discovery_session_cap(session_profile: ConditionSessionProfile) -> int:
    if session_profile is ConditionSessionProfile.PREOPEN_NXT:
        return 0
    if session_profile is ConditionSessionProfile.OPENING_0900_0915:
        return 80
    if session_profile is ConditionSessionProfile.MORNING_TREND:
        return 40
    if session_profile is ConditionSessionProfile.MIDDAY:
        return 12
    if session_profile is ConditionSessionProfile.AFTERNOON:
        return 10
    return 5


def _dedupe(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value).strip().upper()
        if text and text not in result:
            result.append(text)
    return result
