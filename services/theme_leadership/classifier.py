from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from services.theme_leadership.models import ThemeState


@dataclass(frozen=True, kw_only=True)
class ThemeStateInput:
    valid_member_count: int
    fresh_coverage_ratio: float
    rising_count: int
    rising_ratio: float
    weighted_return_pct: float
    total_turnover_krw: float
    leader_score: float
    score: float
    reason_codes: Sequence[str]
    min_valid_members: int
    min_fresh_coverage_ratio: float


class ThemeStateClassifier:
    def classify(self, value: ThemeStateInput) -> tuple[ThemeState, list[str]]:
        reasons = list(value.reason_codes)
        if value.valid_member_count < value.min_valid_members:
            reasons.append("INSUFFICIENT_VALID_MEMBERS")
            return ThemeState.DATA_WAIT, _dedupe(reasons)
        if value.fresh_coverage_ratio < value.min_fresh_coverage_ratio:
            reasons.append("LOW_FRESH_COVERAGE")
            return ThemeState.DATA_WAIT, _dedupe(reasons)

        if value.rising_count <= 0 or value.weighted_return_pct <= 0:
            reasons.append("WEAK_THEME_RETURN")
            return ThemeState.WEAK, _dedupe(reasons)

        leader_strong = value.leader_score >= 18.0
        if leader_strong and value.rising_ratio < 0.35:
            reasons.append("LEADER_ONLY_BREADTH_WEAK")
            return ThemeState.LEADER_ONLY, _dedupe(reasons)

        if (
            value.rising_ratio >= 0.5
            and value.weighted_return_pct >= 1.0
            and value.fresh_coverage_ratio >= 0.7
            and value.score >= 45.0
        ):
            reasons.append("THEME_LEADING")
            return ThemeState.LEADING, _dedupe(reasons)

        if value.rising_ratio >= 0.35 and value.total_turnover_krw > 0:
            reasons.append("THEME_SPREADING")
            return ThemeState.SPREADING, _dedupe(reasons)

        if leader_strong:
            reasons.append("LEADER_ONLY_BREADTH_WEAK")
            return ThemeState.LEADER_ONLY, _dedupe(reasons)

        reasons.append("THEME_WATCH")
        return ThemeState.WATCH, _dedupe(reasons)


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).upper()
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result
