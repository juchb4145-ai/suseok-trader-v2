from __future__ import annotations

from enum import StrEnum


class ThemeSnapshotQuality(StrEnum):
    MISSING_MEMBERSHIP = "MISSING_MEMBERSHIP"
    DATA_WAIT = "DATA_WAIT"
    PARTIAL = "PARTIAL"
    FRESH = "FRESH"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"
