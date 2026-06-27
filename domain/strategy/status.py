from __future__ import annotations

from enum import StrEnum


class StrategyObservationStatus(StrEnum):
    NOT_EVALUATED = "NOT_EVALUATED"
    DATA_WAIT = "DATA_WAIT"
    NO_SETUP = "NO_SETUP"
    WATCH = "WATCH"
    FORMING = "FORMING"
    MATCHED_OBSERVATION = "MATCHED_OBSERVATION"
    INVALID_CONTEXT = "INVALID_CONTEXT"
    STALE_CONTEXT = "STALE_CONTEXT"
