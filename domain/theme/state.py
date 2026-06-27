from __future__ import annotations

from enum import StrEnum


class ThemeState(StrEnum):
    DATA_WAIT = "DATA_WAIT"
    WATCH = "WATCH"
    SPREADING = "SPREADING"
    LEADING = "LEADING"
    FADING = "FADING"
    ROTATED_OUT = "ROTATED_OUT"


class ThemeMemberRole(StrEnum):
    UNKNOWN = "UNKNOWN"
    LEADER_CANDIDATE = "LEADER_CANDIDATE"
    CO_LEADER_CANDIDATE = "CO_LEADER_CANDIDATE"
    FOLLOWER_CANDIDATE = "FOLLOWER_CANDIDATE"
    LAGGARD = "LAGGARD"
    STALE = "STALE"
