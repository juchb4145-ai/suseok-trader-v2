from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from domain.strategy.models import SetupObservation, StrategyCandidateContext
from domain.strategy.reasons import StrategyReasonCode
from domain.strategy.setup import StrategySetupType
from domain.strategy.status import StrategyObservationStatus


def evaluate_theme_leader_pullback(
    context: StrategyCandidateContext,
    settings: Any,
) -> SetupObservation:
    reasons: list[str] = []
    evidence: dict[str, Any] = {
        "observe_only": True,
        "theme_state": context.theme_state,
        "theme_role": context.theme_role,
        "price": context.price,
        "day_high": context.day_high,
    }

    theme_gate = _theme_gate(context, settings, allow_follower=True)
    if theme_gate is not None:
        return _setup(
            StrategySetupType.THEME_LEADER_PULLBACK,
            theme_gate[0],
            reasons=theme_gate[1],
            evidence=evidence,
        )

    if context.price is None:
        reasons.append(StrategyReasonCode.TICK_MISSING.value)
    if context.day_high is None:
        reasons.append(StrategyReasonCode.MARKET_READINESS_MISSING.value)
        evidence["day_high_missing"] = True
    if reasons:
        return _setup(
            StrategySetupType.THEME_LEADER_PULLBACK,
            StrategyObservationStatus.DATA_WAIT,
            reasons=reasons,
            evidence=evidence,
        )

    pullback_pct = _safe_pct(context.day_high - context.price, context.day_high)
    evidence["pullback_pct"] = pullback_pct
    min_pct = _setting(settings, "strategy_pullback_min_pct", 0.3)
    max_pct = _setting(settings, "strategy_pullback_max_pct", 5.0)
    flow_observed = _flow_observed(context, settings)
    evidence["flow_observed"] = flow_observed

    if pullback_pct < min_pct:
        return _setup(
            StrategySetupType.THEME_LEADER_PULLBACK,
            StrategyObservationStatus.WATCH,
            score=0.2,
            confidence=0.5,
            reasons=[
                StrategyReasonCode.PULLBACK_TOO_SHALLOW.value,
                _flow_reason(flow_observed),
                StrategyReasonCode.SETUP_NOT_MATCHED.value,
            ],
            evidence=evidence,
        )
    if pullback_pct > max_pct:
        return _setup(
            StrategySetupType.THEME_LEADER_PULLBACK,
            StrategyObservationStatus.NO_SETUP,
            score=0.1,
            confidence=0.55,
            reasons=[
                StrategyReasonCode.PULLBACK_TOO_DEEP.value,
                _flow_reason(flow_observed),
                StrategyReasonCode.SETUP_NOT_MATCHED.value,
            ],
            evidence=evidence,
        )

    reasons.extend(
        [
            StrategyReasonCode.PULLBACK_OBSERVED.value,
            _flow_reason(flow_observed),
        ]
    )
    if flow_observed:
        status = StrategyObservationStatus.MATCHED_OBSERVATION
        score = 0.86
        confidence = 0.82
        reasons.append(StrategyReasonCode.SETUP_MATCHED.value)
    else:
        status = StrategyObservationStatus.FORMING
        score = 0.58
        confidence = 0.64
        reasons.append(StrategyReasonCode.MOMENTUM_FORMING.value)
    return _setup(
        StrategySetupType.THEME_LEADER_PULLBACK,
        status,
        score=score,
        confidence=confidence,
        reasons=reasons,
        evidence=evidence,
    )


def evaluate_vwap_reclaim(
    context: StrategyCandidateContext,
    settings: Any,
) -> SetupObservation:
    evidence: dict[str, Any] = {
        "observe_only": True,
        "price": context.price,
        "vwap": context.vwap,
    }
    if context.price is None:
        return _setup(
            StrategySetupType.VWAP_RECLAIM,
            StrategyObservationStatus.DATA_WAIT,
            reasons=[StrategyReasonCode.TICK_MISSING.value],
            evidence=evidence,
        )
    if context.vwap is None:
        status = (
            StrategyObservationStatus.DATA_WAIT
            if bool(_setting(settings, "strategy_engine_require_vwap", False))
            else StrategyObservationStatus.WATCH
        )
        return _setup(
            StrategySetupType.VWAP_RECLAIM,
            status,
            score=0.15 if status is StrategyObservationStatus.WATCH else 0.0,
            confidence=0.3 if status is StrategyObservationStatus.WATCH else 0.0,
            reasons=[StrategyReasonCode.VWAP_MISSING.value],
            evidence=evidence,
        )

    tolerance_pct = _setting(settings, "strategy_vwap_reclaim_tolerance_pct", 1.0)
    distance_pct = _safe_pct(abs(context.price - context.vwap), context.vwap)
    near_vwap = distance_pct <= tolerance_pct
    above_vwap = context.price >= context.vwap
    flow_observed = _flow_observed(context, settings)
    evidence.update(
        {
            "distance_pct": distance_pct,
            "near_vwap": near_vwap,
            "above_vwap": above_vwap,
            "flow_observed": flow_observed,
        }
    )
    reasons = [
        (
            StrategyReasonCode.PRICE_ABOVE_VWAP.value
            if above_vwap
            else StrategyReasonCode.PRICE_BELOW_VWAP.value
        ),
        _flow_reason(flow_observed),
    ]

    if above_vwap and near_vwap:
        reasons.append(StrategyReasonCode.VWAP_RECLAIM_OBSERVED.value)
        if flow_observed:
            reasons.append(StrategyReasonCode.SETUP_MATCHED.value)
            return _setup(
                StrategySetupType.VWAP_RECLAIM,
                StrategyObservationStatus.MATCHED_OBSERVATION,
                score=0.84,
                confidence=0.8,
                reasons=reasons,
                evidence=evidence,
            )
        reasons.append(StrategyReasonCode.MOMENTUM_FORMING.value)
        return _setup(
            StrategySetupType.VWAP_RECLAIM,
            StrategyObservationStatus.FORMING,
            score=0.57,
            confidence=0.62,
            reasons=reasons,
            evidence=evidence,
        )
    if near_vwap:
        reasons.append(StrategyReasonCode.MOMENTUM_FORMING.value)
        return _setup(
            StrategySetupType.VWAP_RECLAIM,
            StrategyObservationStatus.FORMING,
            score=0.45,
            confidence=0.58,
            reasons=reasons,
            evidence=evidence,
        )

    return _setup(
        StrategySetupType.VWAP_RECLAIM,
        StrategyObservationStatus.WATCH if above_vwap else StrategyObservationStatus.NO_SETUP,
        score=0.25 if above_vwap else 0.1,
        confidence=0.48,
        reasons=[*reasons, StrategyReasonCode.SETUP_NOT_MATCHED.value],
        evidence=evidence,
    )


def evaluate_breakout_retest(
    context: StrategyCandidateContext,
    settings: Any,
) -> SetupObservation:
    evidence: dict[str, Any] = {
        "observe_only": True,
        "price": context.price,
        "day_high": context.day_high,
    }
    reasons: list[str] = []
    if context.price is None:
        reasons.append(StrategyReasonCode.TICK_MISSING.value)
    if context.day_high is None:
        reasons.append(StrategyReasonCode.MARKET_READINESS_MISSING.value)
        evidence["day_high_missing"] = True
    if reasons:
        return _setup(
            StrategySetupType.BREAKOUT_RETEST,
            StrategyObservationStatus.DATA_WAIT,
            reasons=reasons,
            evidence=evidence,
        )

    near_high_pct = _safe_pct(context.day_high - context.price, context.day_high)
    near_threshold = _setting(settings, "strategy_breakout_retest_near_high_pct", 2.0)
    flow_observed = _flow_observed(context, settings)
    evidence.update(
        {
            "near_high_pct": near_high_pct,
            "near_threshold_pct": near_threshold,
            "flow_observed": flow_observed,
        }
    )
    if near_high_pct <= near_threshold:
        reasons.extend(
            [
                StrategyReasonCode.BREAKOUT_RETEST_OBSERVED.value,
                _flow_reason(flow_observed),
            ]
        )
        if flow_observed:
            reasons.append(StrategyReasonCode.SETUP_MATCHED.value)
            return _setup(
                StrategySetupType.BREAKOUT_RETEST,
                StrategyObservationStatus.MATCHED_OBSERVATION,
                score=0.8,
                confidence=0.77,
                reasons=reasons,
                evidence=evidence,
            )
        reasons.append(StrategyReasonCode.MOMENTUM_FORMING.value)
        return _setup(
            StrategySetupType.BREAKOUT_RETEST,
            StrategyObservationStatus.FORMING,
            score=0.54,
            confidence=0.62,
            reasons=reasons,
            evidence=evidence,
        )

    return _setup(
        StrategySetupType.BREAKOUT_RETEST,
        StrategyObservationStatus.WATCH
        if near_high_pct <= near_threshold * 2
        else StrategyObservationStatus.NO_SETUP,
        score=0.22,
        confidence=0.5,
        reasons=[
            StrategyReasonCode.MOMENTUM_WEAK.value,
            _flow_reason(flow_observed),
            StrategyReasonCode.SETUP_NOT_MATCHED.value,
        ],
        evidence=evidence,
    )


def evaluate_theme_follower_expansion(
    context: StrategyCandidateContext,
    settings: Any,
) -> SetupObservation:
    evidence: dict[str, Any] = {
        "observe_only": True,
        "theme_state": context.theme_state,
        "theme_role": context.theme_role,
        "theme_rising_ratio": _theme_rising_ratio(context),
    }
    theme_gate = _theme_gate(context, settings, allow_follower=True)
    if theme_gate is not None:
        return _setup(
            StrategySetupType.THEME_FOLLOWER_EXPANSION,
            theme_gate[0],
            reasons=theme_gate[1],
            evidence=evidence,
        )
    if str(context.theme_role or "").upper() != "FOLLOWER_CANDIDATE":
        return _setup(
            StrategySetupType.THEME_FOLLOWER_EXPANSION,
            StrategyObservationStatus.NO_SETUP,
            score=0.05,
            confidence=0.6,
            reasons=[
                StrategyReasonCode.THEME_ROLE_NOT_ALLOWED.value,
                StrategyReasonCode.SETUP_NOT_MATCHED.value,
            ],
            evidence=evidence,
        )

    rising_ratio = _theme_rising_ratio(context)
    if rising_ratio is None:
        return _setup(
            StrategySetupType.THEME_FOLLOWER_EXPANSION,
            StrategyObservationStatus.DATA_WAIT,
            reasons=[StrategyReasonCode.THEME_CONTEXT_MISSING.value],
            evidence=evidence,
        )

    threshold = _setting(settings, "strategy_follower_expansion_min_theme_rising_ratio", 0.35)
    flow_observed = _flow_observed(context, settings)
    evidence["rising_ratio_threshold"] = threshold
    evidence["flow_observed"] = flow_observed

    if rising_ratio >= threshold:
        reasons = [
            StrategyReasonCode.FOLLOWER_EXPANSION_OBSERVED.value,
            _flow_reason(flow_observed),
        ]
        if flow_observed:
            reasons.append(StrategyReasonCode.SETUP_MATCHED.value)
            return _setup(
                StrategySetupType.THEME_FOLLOWER_EXPANSION,
                StrategyObservationStatus.MATCHED_OBSERVATION,
                score=0.78,
                confidence=0.76,
                reasons=reasons,
                evidence=evidence,
            )
        reasons.append(StrategyReasonCode.MOMENTUM_FORMING.value)
        return _setup(
            StrategySetupType.THEME_FOLLOWER_EXPANSION,
            StrategyObservationStatus.FORMING,
            score=0.52,
            confidence=0.62,
            reasons=reasons,
            evidence=evidence,
        )

    return _setup(
        StrategySetupType.THEME_FOLLOWER_EXPANSION,
        StrategyObservationStatus.NO_SETUP,
        score=0.12,
        confidence=0.5,
        reasons=[
            StrategyReasonCode.MOMENTUM_WEAK.value,
            _flow_reason(flow_observed),
            StrategyReasonCode.SETUP_NOT_MATCHED.value,
        ],
        evidence=evidence,
    )


def _theme_gate(
    context: StrategyCandidateContext,
    settings: Any,
    *,
    allow_follower: bool,
) -> tuple[StrategyObservationStatus, list[str]] | None:
    if not context.theme_id or not context.theme_state or not context.theme_role:
        return StrategyObservationStatus.DATA_WAIT, [
            StrategyReasonCode.THEME_CONTEXT_MISSING.value
        ]
    allowed_states = _upper_set(_setting(settings, "strategy_engine_allowed_theme_states", ()))
    if allowed_states and str(context.theme_state).upper() not in allowed_states:
        return StrategyObservationStatus.NO_SETUP, [
            StrategyReasonCode.THEME_NOT_LEADING_OR_SPREADING.value,
            StrategyReasonCode.SETUP_NOT_MATCHED.value,
        ]
    allowed_roles = _upper_set(_setting(settings, "strategy_engine_allowed_theme_roles", ()))
    if not allow_follower:
        allowed_roles.discard("FOLLOWER_CANDIDATE")
    if allowed_roles and str(context.theme_role).upper() not in allowed_roles:
        return StrategyObservationStatus.NO_SETUP, [
            StrategyReasonCode.THEME_ROLE_NOT_ALLOWED.value,
            StrategyReasonCode.SETUP_NOT_MATCHED.value,
        ]
    return None


def _flow_observed(context: StrategyCandidateContext, settings: Any) -> bool:
    min_delta_1m = _setting(settings, "strategy_min_trade_value_delta_1m", 0.0)
    min_delta_3m = _setting(settings, "strategy_min_trade_value_delta_3m", 0.0)
    delta_1m = context.trade_value_delta_1m
    delta_3m = context.trade_value_delta_3m
    return (delta_1m is not None and delta_1m >= min_delta_1m) or (
        delta_3m is not None and delta_3m >= min_delta_3m
    )


def _flow_reason(flow_observed: bool) -> str:
    if flow_observed:
        return StrategyReasonCode.TRADE_VALUE_FLOW_OBSERVED.value
    return StrategyReasonCode.TRADE_VALUE_FLOW_WEAK.value


def _theme_rising_ratio(context: StrategyCandidateContext) -> float | None:
    resolved = context.raw_context.get("theme_context_resolved")
    if isinstance(resolved, dict) and resolved.get("rising_ratio") is not None:
        return float(resolved["rising_ratio"])
    theme_snapshot = context.raw_context.get("theme_latest_snapshot")
    if isinstance(theme_snapshot, dict) and theme_snapshot.get("rising_ratio") is not None:
        return float(theme_snapshot["rising_ratio"])
    theme_context = context.raw_context.get("candidate_context", {}).get("theme_context", {})
    if isinstance(theme_context, dict) and theme_context.get("rising_ratio") is not None:
        return float(theme_context["rising_ratio"])
    return None


def _setup(
    setup_type: StrategySetupType,
    status: StrategyObservationStatus,
    *,
    score: float = 0.0,
    confidence: float = 0.0,
    reasons: Sequence[str],
    evidence: dict[str, Any],
) -> SetupObservation:
    return SetupObservation(
        setup_type=setup_type,
        status=status,
        score=score,
        confidence=confidence,
        reason_codes=[*dict.fromkeys([*reasons, StrategyReasonCode.OBSERVE_ONLY.value])],
        evidence_json=evidence,
    )


def _setting(settings: Any, name: str, default: Any) -> Any:
    return getattr(settings, name, default)


def _safe_pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return max(numerator / denominator * 100.0, 0.0)


def _upper_set(values: Any) -> set[str]:
    if values is None:
        return set()
    if isinstance(values, str):
        values = [part.strip() for part in values.split(",")]
    return {str(value).strip().upper() for value in values if str(value).strip()}
