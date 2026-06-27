from __future__ import annotations

from collections.abc import Sequence

from domain.risk.models import RiskCheckObservation
from domain.risk.reasons import RiskReasonCode
from domain.risk.status import RiskCheckStatus, RiskObservationStatus, RiskSeverity

_SEVERITY_RANK = {
    RiskSeverity.INFO: 0,
    RiskSeverity.LOW: 1,
    RiskSeverity.MEDIUM: 2,
    RiskSeverity.HIGH: 3,
    RiskSeverity.CRITICAL: 4,
}


def max_severity(checks: Sequence[RiskCheckObservation]) -> RiskSeverity:
    if not checks:
        return RiskSeverity.INFO
    return max((check.severity for check in checks), key=lambda severity: _SEVERITY_RANK[severity])


def calculate_overall_status(checks: Sequence[RiskCheckObservation]) -> RiskObservationStatus:
    if not checks:
        return RiskObservationStatus.NOT_EVALUATED

    reason_codes = {reason for check in checks for reason in check.reason_codes}
    if RiskReasonCode.CANDIDATE_NOT_CONTEXT_READY.value in reason_codes and any(
        check.severity is RiskSeverity.CRITICAL for check in checks
    ):
        return RiskObservationStatus.INVALID_CONTEXT
    if _has_reason(
        reason_codes,
        {
            RiskReasonCode.TICK_STALE.value,
            RiskReasonCode.STRATEGY_OBSERVATION_STALE.value,
            RiskReasonCode.CANDIDATE_STALE.value,
        },
    ):
        return RiskObservationStatus.STALE_CONTEXT
    if _has_reason(
        reason_codes,
        {
            RiskReasonCode.LATEST_TICK_MISSING.value,
            RiskReasonCode.MARKET_READINESS_MISSING.value,
            RiskReasonCode.STRATEGY_OBSERVATION_MISSING.value,
        },
    ):
        return RiskObservationStatus.DATA_WAIT
    if any(check.status is RiskCheckStatus.DATA_WAIT for check in checks):
        return RiskObservationStatus.DATA_WAIT
    if any(check.status is RiskCheckStatus.BLOCK_OBSERVED for check in checks):
        return RiskObservationStatus.OBSERVE_BLOCK
    if any(check.status is RiskCheckStatus.CAUTION_OBSERVED for check in checks):
        return RiskObservationStatus.OBSERVE_CAUTION
    if all(
        check.status in {RiskCheckStatus.PASS_OBSERVED, RiskCheckStatus.NOT_EVALUATED}
        for check in checks
    ):
        return RiskObservationStatus.OBSERVE_PASS
    return RiskObservationStatus.NOT_EVALUATED


def _has_reason(reason_codes: set[str], targets: set[str]) -> bool:
    return bool(reason_codes.intersection(targets))
