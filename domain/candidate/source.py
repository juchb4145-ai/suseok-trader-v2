from __future__ import annotations

from enum import StrEnum


class CandidateSourceType(StrEnum):
    CONDITION_ENTER = "CONDITION_ENTER"
    CONDITION_EXIT = "CONDITION_EXIT"
    THEME_LEADER = "THEME_LEADER"
    THEME_CO_LEADER = "THEME_CO_LEADER"
    THEME_FOLLOWER = "THEME_FOLLOWER"
    THEME_SPREADING_MEMBER = "THEME_SPREADING_MEMBER"
    MARKET_OBSERVED = "MARKET_OBSERVED"
    MANUAL_WATCH = "MANUAL_WATCH"
    MOCK = "MOCK"


class CandidateEventType(StrEnum):
    SOURCE_DETECTED = "SOURCE_DETECTED"
    SOURCE_UPDATED = "SOURCE_UPDATED"
    SOURCE_EXITED = "SOURCE_EXITED"
    CANDIDATE_CREATED = "CANDIDATE_CREATED"
    STATE_CHANGED = "STATE_CHANGED"
    CONTEXT_REFRESHED = "CONTEXT_REFRESHED"
    CANDIDATE_CLOSED = "CANDIDATE_CLOSED"
    CANDIDATE_STALE = "CANDIDATE_STALE"


THEME_SOURCE_TYPES: frozenset[CandidateSourceType] = frozenset(
    {
        CandidateSourceType.THEME_LEADER,
        CandidateSourceType.THEME_CO_LEADER,
        CandidateSourceType.THEME_FOLLOWER,
        CandidateSourceType.THEME_SPREADING_MEMBER,
    }
)

CONDITION_SOURCE_TYPES: frozenset[CandidateSourceType] = frozenset(
    {
        CandidateSourceType.CONDITION_ENTER,
        CandidateSourceType.CONDITION_EXIT,
    }
)
