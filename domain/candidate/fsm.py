from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.candidate.reasons import CandidateReasonCode
from domain.candidate.state import CandidateState


@dataclass(frozen=True, kw_only=True)
class CandidateStateDecision:
    next_state: CandidateState
    reason_codes: tuple[str, ...] = field(default_factory=tuple)
    close_candidate: bool = False
    stale_candidate: bool = False


def determine_next_state(
    current_snapshot: Mapping[str, Any],
    context: Mapping[str, Any],
) -> CandidateStateDecision:
    current_state = CandidateState(str(current_snapshot["state"]))
    if current_state is CandidateState.CLOSED:
        return CandidateStateDecision(next_state=CandidateState.CLOSED)

    close_reasons = should_close_candidate(current_snapshot, context)
    if close_reasons:
        return CandidateStateDecision(
            next_state=CandidateState.CLOSED,
            reason_codes=tuple(close_reasons),
            close_candidate=True,
        )

    stale_reasons = should_mark_stale(current_snapshot, context)
    if stale_reasons:
        return CandidateStateDecision(
            next_state=CandidateState.STALE,
            reason_codes=tuple(stale_reasons),
            stale_candidate=True,
        )

    if _bool(context.get("observation_blocked")):
        return CandidateStateDecision(
            next_state=CandidateState.BLOCKED_OBSERVATION,
            reason_codes=(CandidateReasonCode.OBSERVATION_BLOCKED.value,),
        )

    if current_state is CandidateState.DETECTED:
        return CandidateStateDecision(
            next_state=CandidateState.HYDRATING,
            reason_codes=(CandidateReasonCode.SOURCE_DETECTED.value,),
        )

    data_wait_reasons = _data_wait_reasons(context)
    context_ready = _context_ready(context)
    hydration_ready = _hydration_ready(context)

    if current_state is CandidateState.HYDRATING:
        if data_wait_reasons:
            return CandidateStateDecision(
                next_state=CandidateState.DATA_WAIT,
                reason_codes=tuple(
                    merge_reason_codes(context.get("reason_codes", ()), data_wait_reasons)
                ),
            )
        if hydration_ready:
            return CandidateStateDecision(
                next_state=CandidateState.WATCHING,
                reason_codes=tuple(
                    merge_reason_codes(
                        context.get("reason_codes", ()),
                        (CandidateReasonCode.MARKET_READINESS_READY.value,),
                    )
                ),
            )

    if current_state in {CandidateState.DATA_WAIT, CandidateState.STALE} and hydration_ready:
        return CandidateStateDecision(
            next_state=CandidateState.WATCHING,
            reason_codes=tuple(
                merge_reason_codes(
                    context.get("reason_codes", ()),
                    (CandidateReasonCode.MARKET_READINESS_READY.value,),
                )
            ),
        )

    if context_ready:
        return CandidateStateDecision(
            next_state=CandidateState.CONTEXT_READY,
            reason_codes=tuple(
                merge_reason_codes(
                    context.get("reason_codes", ()),
                    (
                        CandidateReasonCode.CONTEXT_READY.value,
                        CandidateReasonCode.MARKET_READINESS_READY.value,
                    ),
                )
            ),
        )

    if data_wait_reasons:
        return CandidateStateDecision(
            next_state=CandidateState.DATA_WAIT,
            reason_codes=tuple(
                merge_reason_codes(context.get("reason_codes", ()), data_wait_reasons)
            ),
        )

    if hydration_ready:
        return CandidateStateDecision(
            next_state=CandidateState.WATCHING,
            reason_codes=tuple(
                merge_reason_codes(
                    context.get("reason_codes", ()),
                    (CandidateReasonCode.MARKET_READINESS_READY.value,),
                )
            ),
        )

    return CandidateStateDecision(
        next_state=current_state,
        reason_codes=tuple(merge_reason_codes(context.get("reason_codes", ()), ())),
    )


def should_close_candidate(
    current_snapshot: Mapping[str, Any],
    context: Mapping[str, Any],
) -> tuple[str, ...]:
    reasons: list[str] = []
    if _bool(context.get("explicit_close")):
        reasons.append(CandidateReasonCode.SOURCE_EXITED.value)
    if int(context.get("active_source_count") or 0) <= 0:
        reasons.append(CandidateReasonCode.SOURCE_EXITED.value)
    if _bool(context.get("ttl_expired")):
        reasons.append(CandidateReasonCode.EPISODE_TTL_EXPIRED.value)
    if str(context.get("theme_state") or "").upper() == "ROTATED_OUT":
        reasons.append(CandidateReasonCode.THEME_ROTATED_OUT.value)
    if _bool(context.get("only_condition_exit_sources")):
        reasons.append(CandidateReasonCode.CONDITION_EXITED.value)
    return tuple(merge_reason_codes(reasons))


def should_mark_stale(
    current_snapshot: Mapping[str, Any],
    context: Mapping[str, Any],
) -> tuple[str, ...]:
    reasons: list[str] = []
    if _bool(context.get("source_stale")):
        reasons.append(CandidateReasonCode.SOURCE_STALE.value)
    if _bool(context.get("tick_stale")):
        reasons.append(CandidateReasonCode.TICK_STALE.value)
    return tuple(merge_reason_codes(reasons))


def merge_reason_codes(*groups: Sequence[str] | object) -> list[str]:
    merged: list[str] = []
    for group in groups:
        if group is None:
            continue
        if isinstance(group, str):
            items = [group]
        else:
            try:
                items = list(group)  # type: ignore[arg-type]
            except TypeError:
                items = [str(group)]
        for item in items:
            value = str(item).strip().upper()
            if value and value not in merged:
                merged.append(value)
    return merged


def _hydration_ready(context: Mapping[str, Any]) -> bool:
    status = str(context.get("market_readiness_status") or "").upper()
    return (
        _bool(context.get("has_latest_tick"))
        and status not in {"", "MISSING", "INVALID"}
        and int(context.get("active_source_count") or 0) > 0
    )


def _context_ready(context: Mapping[str, Any]) -> bool:
    status = str(context.get("market_readiness_status") or "").upper()
    if status in {"", "MISSING", "STALE", "INVALID"}:
        return False
    if not _bool(context.get("has_latest_tick")):
        return False
    if _bool(context.get("require_1m_bar"), default=True) and not _bool(
        context.get("bar_1m_ready")
    ):
        return False
    if _bool(context.get("require_vwap")) and not _bool(context.get("vwap_ready")):
        return False
    if _bool(context.get("theme_source")) and not _bool(context.get("theme_context_present")):
        return False
    if _bool(context.get("condition_source")) and not _bool(
        context.get("condition_signal_present")
    ):
        return False
    return int(context.get("active_source_count") or 0) > 0


def _data_wait_reasons(context: Mapping[str, Any]) -> tuple[str, ...]:
    reasons: list[str] = []
    status = str(context.get("market_readiness_status") or "").upper()
    if not _bool(context.get("has_latest_tick")) or status == "MISSING":
        reasons.append(CandidateReasonCode.MARKET_READINESS_MISSING.value)
    if _bool(context.get("require_1m_bar"), default=True) and not _bool(
        context.get("bar_1m_ready")
    ):
        reasons.append(CandidateReasonCode.BAR_MISSING.value)
    if _bool(context.get("require_vwap")) and not _bool(context.get("vwap_ready")):
        reasons.append(CandidateReasonCode.VWAP_MISSING.value)
    if _bool(context.get("theme_source")) and not _bool(context.get("theme_context_present")):
        reasons.append(CandidateReasonCode.THEME_MISSING.value)
    if int(context.get("active_source_count") or 0) <= 0:
        reasons.append(CandidateReasonCode.SOURCE_MISSING.value)
    return tuple(merge_reason_codes(reasons))


def _bool(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    return bool(value)
