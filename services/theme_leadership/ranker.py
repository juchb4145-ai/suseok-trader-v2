from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from domain.broker.utils import utc_now

from services.theme_leadership.classifier import ThemeStateClassifier, ThemeStateInput
from services.theme_leadership.models import (
    RealtimeStockSnapshot,
    StockRole,
    ThemeLeadershipSnapshot,
    ThemeMemberLeadership,
    ThemeUniverseMember,
)
from services.theme_leadership.stock_role import (
    StockRoleClassifier,
    member_reason_codes,
    member_score,
)


class ThemeLeadershipRanker:
    def __init__(self, *, settings: Any | None = None) -> None:
        self.settings = settings
        self.state_classifier = ThemeStateClassifier()
        self.role_classifier = StockRoleClassifier()

    def rank(
        self,
        universe: Sequence[ThemeUniverseMember],
        stock_snapshots: Mapping[str, RealtimeStockSnapshot],
        *,
        created_at: datetime | None = None,
    ) -> list[ThemeLeadershipSnapshot]:
        created = created_at or utc_now()
        grouped: dict[str, list[ThemeUniverseMember]] = defaultdict(list)
        for member in universe:
            if member.active:
                grouped[member.theme_id].append(member)

        raw_theme_metrics = [
            self._build_raw_theme(member_group, stock_snapshots)
            for member_group in grouped.values()
        ]
        total_turnover = sum(item["total_turnover_krw"] for item in raw_theme_metrics)
        snapshots: list[ThemeLeadershipSnapshot] = []
        for item in raw_theme_metrics:
            turnover_share = (
                item["total_turnover_krw"] / total_turnover if total_turnover > 0 else 0.0
            )
            score, components = self._score_theme(item, turnover_share)
            state, reasons = self.state_classifier.classify(
                ThemeStateInput(
                    valid_member_count=item["valid_member_count"],
                    fresh_coverage_ratio=item["fresh_coverage_ratio"],
                    rising_count=item["rising_count"],
                    rising_ratio=item["rising_ratio"],
                    weighted_return_pct=item["weighted_return_pct"],
                    total_turnover_krw=item["total_turnover_krw"],
                    leader_score=item["leader_score"],
                    score=score,
                    reason_codes=item["reason_codes"],
                    min_valid_members=self._setting("theme_leadership_min_valid_members", 2),
                    min_fresh_coverage_ratio=self._setting(
                        "theme_leadership_min_fresh_coverage_ratio",
                        0.4,
                    ),
                )
            )
            members = self.role_classifier.classify(item["member_inputs"], theme_state=state)
            leader = _first_role(members, StockRole.LEADER)
            snapshots.append(
                ThemeLeadershipSnapshot(
                    theme_id=item["theme_id"],
                    theme_name=item["theme_name"],
                    state=state,
                    score=score,
                    rank=0,
                    valid_member_count=item["valid_member_count"],
                    fresh_member_count=item["fresh_member_count"],
                    fresh_coverage_ratio=item["fresh_coverage_ratio"],
                    rising_count=item["rising_count"],
                    rising_ratio=item["rising_ratio"],
                    leader_count=sum(1 for member in members if member.role is StockRole.LEADER),
                    co_leader_count=sum(
                        1 for member in members if member.role is StockRole.CO_LEADER
                    ),
                    follower_count=sum(
                        1 for member in members if member.role is StockRole.FOLLOWER
                    ),
                    total_turnover_krw=item["total_turnover_krw"],
                    turnover_share=turnover_share,
                    weighted_return_pct=item["weighted_return_pct"],
                    avg_change_rate_pct=item["avg_change_rate_pct"],
                    max_change_rate_pct=item["max_change_rate_pct"],
                    leader_concentration=item["leader_concentration"],
                    leader_code=leader.code if leader else item["leader_code"],
                    leader_name=leader.name if leader else item["leader_name"],
                    members=members,
                    reason_codes=reasons,
                    created_at=created,
                    score_components=components,
                )
            )

        snapshots.sort(
            key=lambda snapshot: (
                -snapshot.score,
                -snapshot.fresh_member_count,
                -snapshot.valid_member_count,
                -snapshot.total_turnover_krw,
                snapshot.theme_name,
                snapshot.theme_id,
            )
        )
        return [_with_rank(snapshot, rank=index + 1) for index, snapshot in enumerate(snapshots)]

    def _build_raw_theme(
        self,
        members: Sequence[ThemeUniverseMember],
        stock_snapshots: Mapping[str, RealtimeStockSnapshot],
    ) -> dict[str, Any]:
        first = members[0]
        active_count = len(members)
        member_inputs = []
        observed_snapshots: list[RealtimeStockSnapshot] = []
        fresh_snapshots: list[RealtimeStockSnapshot] = []
        reason_codes: list[str] = []
        weighted_turnover_return = 0.0
        total_weight = 0.0
        total_turnover = 0.0
        rising_count = 0
        max_change = 0.0
        change_sum = 0.0
        change_count = 0

        for member in members:
            snapshot = stock_snapshots.get(member.code)
            score = member_score(snapshot, weight=member.weight)
            reasons = member_reason_codes(snapshot)
            if snapshot is None or snapshot.current_price is None:
                reason_codes.append("MEMBER_SNAPSHOT_MISSING")
            else:
                observed_snapshots.append(snapshot)
                turnover = snapshot.turnover_krw or 0.0
                change = snapshot.change_rate_pct or 0.0
                total_turnover += turnover
                weighted_turnover_return += change * max(turnover, member.weight)
                total_weight += max(turnover, member.weight)
                max_change = max(max_change, change)
                change_sum += change
                change_count += 1
                if change > 0:
                    rising_count += 1
                if not snapshot.stale and snapshot.data_quality == "FRESH":
                    fresh_snapshots.append(snapshot)
                else:
                    reason_codes.append("MEMBER_NOT_FRESH")
                if snapshot.source_flags.get("condition_include"):
                    reason_codes.append("CONDITION_DISCOVERY_PRESENT")
            member_inputs.append((snapshot, score, reasons))

        leader_snapshot, leader_score = _best_member(member_inputs)
        leader_turnover = leader_snapshot.turnover_krw if leader_snapshot else 0.0
        weighted_return = weighted_turnover_return / total_weight if total_weight > 0 else 0.0
        return {
            "theme_id": first.theme_id,
            "theme_name": first.theme_name,
            "active_member_count": active_count,
            "valid_member_count": len(observed_snapshots),
            "fresh_member_count": len(fresh_snapshots),
            "fresh_coverage_ratio": _ratio(len(fresh_snapshots), active_count),
            "rising_count": rising_count,
            "rising_ratio": _ratio(rising_count, max(len(observed_snapshots), 1)),
            "total_turnover_krw": total_turnover,
            "weighted_return_pct": weighted_return,
            "avg_change_rate_pct": change_sum / change_count if change_count else 0.0,
            "max_change_rate_pct": max_change,
            "leader_score": leader_score,
            "leader_concentration": _ratio(leader_turnover or 0.0, total_turnover),
            "leader_code": leader_snapshot.code if leader_snapshot else None,
            "leader_name": leader_snapshot.name if leader_snapshot else None,
            "member_inputs": member_inputs,
            "reason_codes": _dedupe(reason_codes),
        }

    def _score_theme(
        self, item: Mapping[str, Any], turnover_share: float
    ) -> tuple[float, dict[str, float]]:
        turnover_score = min(item["total_turnover_krw"] / 1_000_000_000.0, 1.0) * 14.0
        turnover_share_score = min(turnover_share, 0.5) * 20.0
        breadth_score = item["rising_ratio"] * 20.0
        weighted_return_score = max(min(item["weighted_return_pct"] / 5.0, 1.0), -1.0) * 20.0
        leader_strength_score = min(max(item["leader_score"], 0.0) / 40.0, 1.0) * 16.0
        momentum_score = max(min(item["avg_change_rate_pct"] / 3.0, 1.0), -1.0) * 8.0
        freshness_score = item["fresh_coverage_ratio"] * 10.0
        condition_boost = (
            3.0
            if self._setting("theme_leadership_condition_boost_enabled", True)
            and "CONDITION_DISCOVERY_PRESENT" in item["reason_codes"]
            else 0.0
        )
        concentration_penalty = max(item["leader_concentration"] - 0.7, 0.0) * 10.0
        data_quality_penalty = (1.0 - item["fresh_coverage_ratio"]) * 12.0
        score = (
            turnover_score
            + turnover_share_score
            + breadth_score
            + weighted_return_score
            + leader_strength_score
            + momentum_score
            + freshness_score
            + condition_boost
            - concentration_penalty
            - data_quality_penalty
        )
        score = max(score, 0.0)
        components = {
            "turnover_score": turnover_score,
            "turnover_share_score": turnover_share_score,
            "breadth_score": breadth_score,
            "weighted_return_score": weighted_return_score,
            "leader_strength_score": leader_strength_score,
            "momentum_score": momentum_score,
            "freshness_score": freshness_score,
            "condition_boost": condition_boost,
            "concentration_penalty": concentration_penalty,
            "data_quality_penalty": data_quality_penalty,
        }
        return round(score, 6), {key: round(value, 6) for key, value in components.items()}

    def _setting(self, name: str, default: Any) -> Any:
        return getattr(self.settings, name, default)


def _best_member(
    member_inputs: Sequence[tuple[RealtimeStockSnapshot | None, float, list[str]]],
) -> tuple[RealtimeStockSnapshot | None, float]:
    candidates = [
        (snapshot, score)
        for snapshot, score, _ in member_inputs
        if snapshot is not None and snapshot.current_price is not None
    ]
    if not candidates:
        return None, 0.0
    return max(candidates, key=lambda item: (item[1], item[0].code))


def _first_role(
    members: Sequence[ThemeMemberLeadership],
    role: StockRole,
) -> ThemeMemberLeadership | None:
    for member in members:
        if member.role is role:
            return member
    return None


def _with_rank(snapshot: ThemeLeadershipSnapshot, *, rank: int) -> ThemeLeadershipSnapshot:
    return ThemeLeadershipSnapshot(
        theme_id=snapshot.theme_id,
        theme_name=snapshot.theme_name,
        state=snapshot.state,
        score=snapshot.score,
        rank=rank,
        valid_member_count=snapshot.valid_member_count,
        fresh_member_count=snapshot.fresh_member_count,
        fresh_coverage_ratio=snapshot.fresh_coverage_ratio,
        rising_count=snapshot.rising_count,
        rising_ratio=snapshot.rising_ratio,
        leader_count=snapshot.leader_count,
        co_leader_count=snapshot.co_leader_count,
        follower_count=snapshot.follower_count,
        total_turnover_krw=snapshot.total_turnover_krw,
        turnover_share=snapshot.turnover_share,
        weighted_return_pct=snapshot.weighted_return_pct,
        leader_code=snapshot.leader_code,
        leader_name=snapshot.leader_name,
        members=snapshot.members,
        reason_codes=snapshot.reason_codes,
        created_at=snapshot.created_at,
        avg_change_rate_pct=snapshot.avg_change_rate_pct,
        max_change_rate_pct=snapshot.max_change_rate_pct,
        leader_concentration=snapshot.leader_concentration,
        score_components=snapshot.score_components,
    )


def _ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).upper()
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result
