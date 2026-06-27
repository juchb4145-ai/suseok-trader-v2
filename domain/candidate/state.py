from __future__ import annotations

from enum import StrEnum


class CandidateState(StrEnum):
    DETECTED = "DETECTED"
    HYDRATING = "HYDRATING"
    WATCHING = "WATCHING"
    CONTEXT_READY = "CONTEXT_READY"
    DATA_WAIT = "DATA_WAIT"
    BLOCKED_OBSERVATION = "BLOCKED_OBSERVATION"
    STALE = "STALE"
    COOLDOWN = "COOLDOWN"
    CLOSED = "CLOSED"


ACTIVE_CANDIDATE_STATES: frozenset[CandidateState] = frozenset(
    state for state in CandidateState if state is not CandidateState.CLOSED
)
