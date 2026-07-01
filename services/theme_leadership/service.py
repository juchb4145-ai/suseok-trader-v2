from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from math import ceil
from typing import Any

from domain.broker.utils import datetime_to_wire, normalize_payload, parse_timestamp, utc_now
from domain.candidate.models import CandidateSourceEvent
from domain.candidate.source import CandidateSourceType

from services.candidate_service import (
    CandidateSourceApplyResult,
    create_or_merge_candidate_from_source,
)
from services.config import Settings, candidate_timezone, load_settings
from services.theme_leadership.models import (
    ThemeLeadershipSnapshot,
    ThemeState,
    WatchsetItem,
    WatchsetResult,
    to_legacy_member_role,
    to_legacy_theme_state,
)
from services.theme_leadership.ranker import ThemeLeadershipRanker
from services.theme_leadership.snapshot import RealtimeSnapshotBuilder
from services.theme_leadership.universe import ThemeUniverseBuilder
from services.theme_leadership.watchset import WatchsetSelector


@dataclass(frozen=True, kw_only=True)
class ThemeLeadershipRebuildResult:
    status: str
    snapshots: Sequence[ThemeLeadershipSnapshot] = field(default_factory=tuple)
    watchset: WatchsetResult = field(default_factory=WatchsetResult)
    candidate_source_events: Sequence[CandidateSourceEvent] = field(default_factory=tuple)
    candidate_apply_result: CandidateSourceApplyResult = field(
        default_factory=CandidateSourceApplyResult
    )
    observe_only: bool = True
    no_trading_side_effects: bool = True
    diagnostic_top_theme_count: int = 0
    eligible_theme_count: int = 0
    watchset_selection_theme_count: int = 0
    watchset_selection_source: str | None = None
    warning: str | None = None

    def to_dict(self, *, include_members: bool = True) -> dict[str, Any]:
        return {
            "status": self.status,
            "observe_only": self.observe_only,
            "no_trading_side_effects": self.no_trading_side_effects,
            "diagnostic_top_theme_count": self.diagnostic_top_theme_count,
            "eligible_theme_count": self.eligible_theme_count,
            "watchset_selection_theme_count": self.watchset_selection_theme_count,
            "watchset_selection_source": self.watchset_selection_source,
            "warning": self.warning,
            "top_themes": [
                snapshot.to_dict(include_members=include_members) for snapshot in self.snapshots
            ],
            "watchset": self.watchset.to_dict(),
            "candidate_source_events": [event.to_dict() for event in self.candidate_source_events],
            "candidate_apply_result": {
                "source_event_count": self.candidate_apply_result.source_event_count,
                "candidate_created_count": self.candidate_apply_result.candidate_created_count,
                "candidate_updated_count": self.candidate_apply_result.candidate_updated_count,
                "duplicate_source_count": self.candidate_apply_result.duplicate_source_count,
                "transition_count": self.candidate_apply_result.transition_count,
                "closed_count": self.candidate_apply_result.closed_count,
                "error_count": self.candidate_apply_result.error_count,
            },
        }


class ThemeLeadershipService:
    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()
        self.universe_builder = ThemeUniverseBuilder(settings=self.settings)
        self.snapshot_builder = RealtimeSnapshotBuilder(settings=self.settings)
        self.ranker = ThemeLeadershipRanker(settings=self.settings)
        self.watchset_selector = WatchsetSelector(settings=self.settings)

    def rebuild(
        self,
        connection: sqlite3.Connection,
        *,
        trade_date: str | None = None,
        write_candidate_sources: bool | None = None,
    ) -> ThemeLeadershipRebuildResult:
        if not self.settings.theme_leadership_enabled:
            return ThemeLeadershipRebuildResult(status="DISABLED")

        universe = self.universe_builder.build(connection)
        stock_snapshots = self.snapshot_builder.build_for_universe(connection, universe)
        snapshots = self.ranker.rank(universe, stock_snapshots, created_at=utc_now())
        top_count = self.settings.theme_leadership_top_theme_count
        top_snapshots = snapshots[:top_count]
        (
            watchset_source,
            eligible_theme_count,
            watchset_selection_source,
            warning,
        ) = _watchset_selection_source(
            snapshots,
            top_snapshots=top_snapshots,
            top_count=top_count,
            settings=self.settings,
        )
        watchset = self.watchset_selector.select(
            watchset_source,
            theme_limit=len(watchset_source) or top_count,
        )
        if warning:
            watchset = _with_watchset_warning(watchset, warning)
        candidate_events = build_candidate_source_events(
            watchset.items,
            trade_date=_resolve_trade_date(trade_date, self.settings),
        )
        should_write = (
            self.settings.theme_leadership_write_candidate_sources
            if write_candidate_sources is None
            else write_candidate_sources
        )
        apply_result = CandidateSourceApplyResult()
        if should_write:
            apply_result = _write_candidate_source_events(
                connection,
                candidate_events,
                settings=self.settings,
            )
            connection.commit()
        return ThemeLeadershipRebuildResult(
            status="OK",
            snapshots=top_snapshots,
            watchset=watchset,
            candidate_source_events=candidate_events,
            candidate_apply_result=apply_result,
            diagnostic_top_theme_count=len(top_snapshots),
            eligible_theme_count=eligible_theme_count,
            watchset_selection_theme_count=len(watchset_source),
            watchset_selection_source=watchset_selection_source,
            warning=warning,
        )


ELIGIBLE_WATCHSET_THEME_STATES = {
    ThemeState.LEADING,
    ThemeState.SPREADING,
    ThemeState.LEADER_ONLY,
}


def _watchset_selection_source(
    snapshots: Sequence[ThemeLeadershipSnapshot],
    *,
    top_snapshots: Sequence[ThemeLeadershipSnapshot],
    top_count: int,
    settings: Settings,
) -> tuple[list[ThemeLeadershipSnapshot], int, str, str | None]:
    eligible_snapshots = [
        snapshot for snapshot in snapshots if snapshot.state in ELIGIBLE_WATCHSET_THEME_STATES
    ]
    if not eligible_snapshots:
        return (
            list(top_snapshots),
            0,
            "diagnostic_top",
            "THEME_LEADERSHIP_NO_ELIGIBLE_THEME",
        )

    max_per_theme = max(int(settings.theme_leadership_max_stocks_per_theme), 1)
    max_total = max(int(settings.theme_leadership_max_total_watchset), 0)
    min_theme_needed = 0 if max_total <= 0 else ceil(max_total / max_per_theme)
    selection_theme_limit = max(int(top_count), min_theme_needed + 2)
    warning = (
        "DATA_WAIT_TOP_THEMES_SKIPPED_FOR_WATCHSET"
        if any(snapshot.state is ThemeState.DATA_WAIT for snapshot in top_snapshots)
        else None
    )
    return (
        eligible_snapshots[:selection_theme_limit],
        len(eligible_snapshots),
        "eligible_ranked",
        warning,
    )


def _with_watchset_warning(watchset: WatchsetResult, warning: str) -> WatchsetResult:
    reason_summary = dict(watchset.reason_summary)
    reason_summary[warning] = int(reason_summary.get(warning, 0)) + 1
    return WatchsetResult(
        items=watchset.items,
        excluded=watchset.excluded,
        near_miss=watchset.near_miss,
        reason_summary=reason_summary,
    )


def rebuild_theme_leadership(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    write_candidate_sources: bool | None = None,
    settings: Settings | None = None,
) -> ThemeLeadershipRebuildResult:
    service = ThemeLeadershipService(settings=settings)
    return service.rebuild(
        connection,
        trade_date=trade_date,
        write_candidate_sources=write_candidate_sources,
    )


def build_candidate_source_events(
    watchset_items: Sequence[WatchsetItem],
    *,
    trade_date: str,
) -> list[CandidateSourceEvent]:
    events: list[CandidateSourceEvent] = []
    for item in watchset_items:
        source_type = CandidateSourceType(item.source_type)
        observed_at = parse_timestamp(item.observed_at, "observed_at")
        payload = normalize_payload(
            {
                "observe_only": True,
                "not_order_signal": True,
                "theme_id": item.theme_id,
                "theme_name": item.theme_name,
                "state": to_legacy_theme_state(item.theme_state).value,
                "member_role": to_legacy_member_role(item.stock_role).value,
                "theme_state": item.theme_state.value,
                "theme_rank": item.theme_rank,
                "stock_role": item.stock_role.value,
                "priority_score": item.priority_score,
                "reason_codes": list(item.reason_codes),
                "source_detail": item.source_detail,
                "expires_at": datetime_to_wire(parse_timestamp(item.expires_at, "expires_at")),
            }
        )
        events.append(
            CandidateSourceEvent(
                source_event_id=_source_event_id(
                    "rt_tls",
                    trade_date,
                    item.theme_id,
                    item.code,
                    item.stock_role.value,
                    datetime_to_wire(observed_at),
                ),
                trade_date=trade_date,
                code=item.code,
                name=item.name,
                source_type=source_type,
                source_id=item.theme_id,
                action="OBSERVE",
                theme_id=item.theme_id,
                theme_name=item.theme_name,
                event_ts=observed_at,
                observed_at=observed_at,
                payload=payload,
                reason_codes=_candidate_reason_codes(item),
            )
        )
    return events


def _write_candidate_source_events(
    connection: sqlite3.Connection,
    events: Sequence[CandidateSourceEvent],
    *,
    settings: Settings,
) -> CandidateSourceApplyResult:
    total = _MutableCandidateApplyResult()
    for event in events:
        result = create_or_merge_candidate_from_source(connection, event, settings=settings)
        total.add(result)
    return total.to_result()


def _candidate_reason_codes(item: WatchsetItem) -> list[str]:
    reasons: list[str] = ["SOURCE_DETECTED"]
    if item.theme_state.value == "LEADING":
        reasons.append("THEME_STATE_LEADING")
    elif item.theme_state.value == "SPREADING":
        reasons.append("THEME_STATE_SPREADING")
    if item.stock_role.value == "LEADER":
        reasons.append("THEME_LEADING_MEMBER")
    elif item.stock_role.value == "CO_LEADER":
        reasons.append("THEME_CO_LEADER_MEMBER")
    elif item.stock_role.value == "FOLLOWER":
        reasons.append("THEME_FOLLOWER_MEMBER")
    reasons.extend(item.reason_codes)
    return _dedupe(reasons)


def _source_event_id(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
    return f"CSE-RTTLS-{digest}"


def _resolve_trade_date(trade_date: str | None, settings: Settings) -> str:
    if trade_date is not None:
        return str(trade_date).strip()
    return (
        datetime.now(candidate_timezone(settings.candidate_trade_date_timezone)).date().isoformat()
    )


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).upper()
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


@dataclass
class _MutableCandidateApplyResult:
    source_event_count: int = 0
    candidate_created_count: int = 0
    candidate_updated_count: int = 0
    duplicate_source_count: int = 0
    transition_count: int = 0
    closed_count: int = 0
    error_count: int = 0

    def add(self, result: CandidateSourceApplyResult) -> None:
        self.source_event_count += result.source_event_count
        self.candidate_created_count += result.candidate_created_count
        self.candidate_updated_count += result.candidate_updated_count
        self.duplicate_source_count += result.duplicate_source_count
        self.transition_count += result.transition_count
        self.closed_count += result.closed_count
        self.error_count += result.error_count

    def to_result(self) -> CandidateSourceApplyResult:
        return CandidateSourceApplyResult(
            source_event_count=self.source_event_count,
            candidate_created_count=self.candidate_created_count,
            candidate_updated_count=self.candidate_updated_count,
            duplicate_source_count=self.duplicate_source_count,
            transition_count=self.transition_count,
            closed_count=self.closed_count,
            error_count=self.error_count,
        )
