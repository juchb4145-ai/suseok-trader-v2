from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from domain.broker.utils import utc_now

from services.theme_leadership.models import (
    StockRole,
    ThemeLeadershipSnapshot,
    ThemeMemberLeadership,
    ThemeState,
    WatchsetItem,
    WatchsetResult,
)


class WatchsetSelector:
    def __init__(self, *, settings: Any | None = None) -> None:
        self.settings = settings

    def select(self, snapshots: Sequence[ThemeLeadershipSnapshot]) -> WatchsetResult:
        top_theme_count = self._setting("theme_leadership_top_theme_count", 5)
        max_per_theme = self._setting("theme_leadership_max_stocks_per_theme", 3)
        max_total = self._setting("theme_leadership_max_total_watchset", 20)
        ttl_sec = self._setting("candidate_source_stale_sec", 300)
        now = utc_now()
        expires_at = now + timedelta(seconds=ttl_sec)

        items: list[WatchsetItem] = []
        excluded: list[dict[str, Any]] = []
        near_miss: list[dict[str, Any]] = []
        reason_summary: Counter[str] = Counter()

        for theme in sorted(snapshots, key=lambda item: item.rank)[:top_theme_count]:
            selected_for_theme = 0
            eligible_roles = _eligible_roles(theme.state)
            if not eligible_roles:
                near_miss.extend(_theme_near_misses(theme, "THEME_STATE_NOT_WATCHSET_ELIGIBLE"))
                reason_summary["THEME_STATE_NOT_WATCHSET_ELIGIBLE"] += len(theme.members)
                continue

            for member in sorted(theme.members, key=lambda item: (-item.member_score, item.code)):
                block_reasons = _watchset_block_reasons(theme, member)
                if block_reasons:
                    excluded.append(_excluded(theme, member, block_reasons))
                    for reason in block_reasons:
                        reason_summary[reason] += 1
                    continue
                if member.role not in eligible_roles:
                    near_miss.append(_near_miss(theme, member, "ROLE_NOT_ELIGIBLE_FOR_THEME_STATE"))
                    reason_summary["ROLE_NOT_ELIGIBLE_FOR_THEME_STATE"] += 1
                    continue
                if selected_for_theme >= max_per_theme:
                    near_miss.append(_near_miss(theme, member, "MAX_STOCKS_PER_THEME_REACHED"))
                    reason_summary["MAX_STOCKS_PER_THEME_REACHED"] += 1
                    continue
                if len(items) >= max_total:
                    near_miss.append(_near_miss(theme, member, "MAX_TOTAL_WATCHSET_REACHED"))
                    reason_summary["MAX_TOTAL_WATCHSET_REACHED"] += 1
                    continue

                reason_codes = _dedupe(
                    [
                        *theme.reason_codes,
                        *member.reason_codes,
                        "WATCHSET_SELECTED",
                        f"THEME_{theme.state.value}",
                        f"ROLE_{member.role.value}",
                    ]
                )
                item = WatchsetItem(
                    code=member.code,
                    name=member.name,
                    theme_id=theme.theme_id,
                    theme_name=theme.theme_name,
                    theme_state=theme.state,
                    theme_rank=theme.rank,
                    stock_role=member.role,
                    priority_score=round(theme.score * 0.65 + member.member_score * 0.35, 6),
                    reason_codes=reason_codes,
                    source_type=_source_type(theme.state, member.role),
                    source_detail={
                        "theme_score": theme.score,
                        "member_score": member.member_score,
                        "observe_only": True,
                        "not_order_signal": True,
                    },
                    expires_at=expires_at,
                    observed_at=theme.created_at,
                )
                items.append(item)
                selected_for_theme += 1
                reason_summary["WATCHSET_SELECTED"] += 1

        items.sort(key=lambda item: (-item.priority_score, item.theme_rank, item.code))
        return WatchsetResult(
            items=items[:max_total],
            excluded=excluded,
            near_miss=near_miss,
            reason_summary=dict(reason_summary),
        )

    def _setting(self, name: str, default: Any) -> Any:
        return getattr(self.settings, name, default)


def _eligible_roles(state: ThemeState) -> set[StockRole]:
    if state in {ThemeState.LEADING, ThemeState.SPREADING}:
        return {StockRole.LEADER, StockRole.CO_LEADER, StockRole.FOLLOWER}
    if state is ThemeState.LEADER_ONLY:
        return {StockRole.LEADER, StockRole.CO_LEADER}
    return set()


def _watchset_block_reasons(
    theme: ThemeLeadershipSnapshot,
    member: ThemeMemberLeadership,
) -> list[str]:
    reasons: list[str] = []
    if member.role in {
        StockRole.STALE,
        StockRole.OVERHEATED,
        StockRole.LATE_LAGGARD,
        StockRole.WEAK_MEMBER,
        StockRole.UNKNOWN,
    }:
        reasons.append(f"ROLE_{member.role.value}_EXCLUDED")
    if member.source_flags.get("condition_include") and theme.state is ThemeState.DATA_WAIT:
        reasons.append("CONDITION_ONLY_WITHOUT_REALTIME_EXCLUDED")
    if any(
        reason in member.reason_codes
        for reason in ("VI_ACTIVE", "UPPER_LIMIT_NEAR", "ABNORMAL_SPREAD")
    ):
        reasons.extend(
            reason
            for reason in ("VI_ACTIVE", "UPPER_LIMIT_NEAR", "ABNORMAL_SPREAD")
            if reason in member.reason_codes
        )
    if theme.state is ThemeState.LEADER_ONLY and member.role is StockRole.FOLLOWER:
        reasons.append("LEADER_ONLY_FOLLOWER_EXCLUDED")
    return _dedupe(reasons)


def _source_type(state: ThemeState, role: StockRole) -> str:
    if role is StockRole.LEADER:
        return "THEME_LEADER"
    if role is StockRole.CO_LEADER:
        return "THEME_CO_LEADER"
    if role is StockRole.FOLLOWER and state is ThemeState.SPREADING:
        return "THEME_SPREADING_MEMBER"
    if role is StockRole.FOLLOWER:
        return "THEME_FOLLOWER"
    return "THEME_SPREADING_MEMBER"


def _excluded(
    theme: ThemeLeadershipSnapshot,
    member: ThemeMemberLeadership,
    reasons: Sequence[str],
) -> dict[str, Any]:
    return {
        "code": member.code,
        "name": member.name,
        "theme_id": theme.theme_id,
        "theme_name": theme.theme_name,
        "theme_state": theme.state.value,
        "theme_rank": theme.rank,
        "stock_role": member.role.value,
        "member_score": member.member_score,
        "reason_codes": list(reasons),
    }


def _near_miss(
    theme: ThemeLeadershipSnapshot,
    member: ThemeMemberLeadership,
    reason: str,
) -> dict[str, Any]:
    return {
        "code": member.code,
        "name": member.name,
        "theme_id": theme.theme_id,
        "theme_name": theme.theme_name,
        "theme_state": theme.state.value,
        "theme_rank": theme.rank,
        "stock_role": member.role.value,
        "member_score": member.member_score,
        "reason_codes": [reason],
    }


def _theme_near_misses(theme: ThemeLeadershipSnapshot, reason: str) -> list[dict[str, Any]]:
    return [_near_miss(theme, member, reason) for member in theme.members]


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).upper()
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result
