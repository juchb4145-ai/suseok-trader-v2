from __future__ import annotations

from domain.exit.reasons import DryRunExitReasonCode
from domain.exit.status import DryRunExitSignalType

SIGNAL_REASON_BY_TYPE = {
    DryRunExitSignalType.STOP_LOSS: DryRunExitReasonCode.STOP_LOSS_TRIGGERED,
    DryRunExitSignalType.TAKE_PROFIT: DryRunExitReasonCode.TAKE_PROFIT_TRIGGERED,
    DryRunExitSignalType.TRAILING_STOP: DryRunExitReasonCode.TRAILING_STOP_TRIGGERED,
    DryRunExitSignalType.MAX_HOLD: DryRunExitReasonCode.MAX_HOLD_TRIGGERED,
    DryRunExitSignalType.DATA_STALE_EXIT_CAUTION: DryRunExitReasonCode.DATA_STALE_EXIT_CAUTION,
    DryRunExitSignalType.THEME_WEAKENING: DryRunExitReasonCode.THEME_WEAKENING_OBSERVED,
    DryRunExitSignalType.RISK_DETERIORATION: DryRunExitReasonCode.RISK_DETERIORATION_OBSERVED,
    DryRunExitSignalType.STRATEGY_INVALIDATED: DryRunExitReasonCode.STRATEGY_INVALIDATED_OBSERVED,
    DryRunExitSignalType.MANUAL_REVIEW: DryRunExitReasonCode.MANUAL_REVIEW_REQUIRED,
}

CAUTION_SIGNAL_TYPES = {
    DryRunExitSignalType.DATA_STALE_EXIT_CAUTION,
    DryRunExitSignalType.MANUAL_REVIEW,
}

THEME_WEAK_STATES = {"DATA_WAIT", "WATCH", "FADING", "ROTATED_OUT"}
RISK_DETERIORATED_STATUSES = {"OBSERVE_BLOCK", "OBSERVE_CAUTION"}
STRATEGY_INVALIDATED_STATUSES = {
    "DATA_WAIT",
    "NO_SETUP",
    "STALE_CONTEXT",
    "INVALID_CONTEXT",
}
