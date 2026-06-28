from __future__ import annotations

from collections.abc import Sequence

from services.theme_leadership.models import (
    RealtimeStockSnapshot,
    StockRole,
    ThemeMemberLeadership,
    ThemeState,
)


class StockRoleClassifier:
    def classify(
        self,
        members: Sequence[tuple[RealtimeStockSnapshot | None, float, list[str]]],
        *,
        theme_state: ThemeState,
    ) -> list[ThemeMemberLeadership]:
        prelim: list[tuple[RealtimeStockSnapshot | None, float, list[str], StockRole]] = []
        for snapshot, score, reasons in members:
            role = self._base_role(snapshot)
            prelim.append((snapshot, score, reasons, role))

        eligible = [
            item
            for item in prelim
            if item[0] is not None
            and item[3] is StockRole.UNKNOWN
            and (item[0].change_rate_pct or 0.0) > 0
            and not item[0].stale
        ]
        eligible.sort(key=lambda item: (-item[1], item[0].code if item[0] else ""))
        leader_score = eligible[0][1] if eligible else 0.0
        leader_code = eligible[0][0].code if eligible and eligible[0][0] is not None else None

        resolved: list[ThemeMemberLeadership] = []
        for snapshot, score, reasons, base_role in prelim:
            if snapshot is None:
                resolved.append(_unknown_member(score, reasons))
                continue

            role = base_role
            role_reasons = list(reasons)
            if role is StockRole.UNKNOWN:
                change = snapshot.change_rate_pct or 0.0
                turnover = snapshot.turnover_krw or 0.0
                if snapshot.code == leader_code and leader_score > 0:
                    role = StockRole.LEADER
                    role_reasons.append("ROLE_LEADER")
                elif leader_score > 0 and score >= leader_score * 0.75 and change > 0:
                    role = StockRole.CO_LEADER
                    role_reasons.append("ROLE_CO_LEADER")
                elif change > 0 and turnover > 0:
                    if theme_state in {
                        ThemeState.LEADING,
                        ThemeState.SPREADING,
                        ThemeState.LEADER_ONLY,
                    }:
                        role = StockRole.FOLLOWER
                        role_reasons.append("ROLE_FOLLOWER")
                    else:
                        role = StockRole.LATE_LAGGARD
                        role_reasons.append("ROLE_LATE_LAGGARD")
                elif theme_state in {ThemeState.LEADING, ThemeState.SPREADING} and change > -0.5:
                    role = StockRole.LATE_LAGGARD
                    role_reasons.append("ROLE_LATE_LAGGARD")
                else:
                    role = StockRole.WEAK_MEMBER
                    role_reasons.append("ROLE_WEAK_MEMBER")

            resolved.append(
                ThemeMemberLeadership(
                    code=snapshot.code,
                    name=snapshot.name,
                    role=role,
                    member_score=score,
                    change_rate_pct=snapshot.change_rate_pct,
                    turnover_krw=snapshot.turnover_krw,
                    execution_strength=snapshot.execution_strength,
                    momentum_1m=snapshot.momentum_1m,
                    momentum_3m=snapshot.momentum_3m,
                    momentum_5m=snapshot.momentum_5m,
                    vwap=snapshot.vwap,
                    pullback_from_high_pct=snapshot.pullback_from_high_pct,
                    stale=snapshot.stale,
                    reason_codes=_dedupe(role_reasons),
                    source_flags=snapshot.source_flags,
                )
            )
        resolved.sort(key=lambda member: (-member.member_score, member.code))
        return resolved

    def _base_role(self, snapshot: RealtimeStockSnapshot | None) -> StockRole:
        if snapshot is None or snapshot.current_price is None:
            return StockRole.UNKNOWN
        if snapshot.stale or snapshot.data_quality in {"MISSING", "INVALID", "STALE"}:
            return StockRole.STALE
        if snapshot.vi_active or snapshot.upper_limit_near:
            return StockRole.OVERHEATED
        if (snapshot.spread_ticks or 0) >= 10:
            return StockRole.STALE
        if (
            snapshot.pullback_from_high_pct is not None
            and snapshot.pullback_from_high_pct <= 0.1
            and (snapshot.change_rate_pct or 0.0) >= 20.0
        ):
            return StockRole.OVERHEATED
        return StockRole.UNKNOWN


def member_score(snapshot: RealtimeStockSnapshot | None, *, weight: float = 1.0) -> float:
    if snapshot is None or snapshot.current_price is None:
        return -1.0
    change = snapshot.change_rate_pct or 0.0
    turnover = snapshot.turnover_krw or 0.0
    execution = snapshot.execution_strength or 0.0
    momentum = max(
        snapshot.momentum_1m or 0.0,
        snapshot.momentum_3m or 0.0,
        snapshot.momentum_5m or 0.0,
    )
    score = (
        max(change, -5.0) * 4.0
        + min(turnover / 100_000_000.0, 5.0) * 8.0
        + max(execution - 100.0, 0.0) / 5.0
        + max(momentum, 0.0) * 3.0
        + (2.0 if snapshot.source_flags.get("condition_include") else 0.0)
    ) * max(weight, 0.0)
    if snapshot.stale:
        score *= 0.2
    if snapshot.vi_active or snapshot.upper_limit_near:
        score *= 0.4
    return round(score, 6)


def member_reason_codes(snapshot: RealtimeStockSnapshot | None) -> list[str]:
    if snapshot is None:
        return ["SNAPSHOT_MISSING"]
    reasons = list(snapshot.reason_codes)
    if (snapshot.change_rate_pct or 0.0) > 0:
        reasons.append("MEMBER_RISING")
    if (snapshot.turnover_krw or 0.0) > 0:
        reasons.append("TURNOVER_OBSERVED")
    if snapshot.source_flags.get("condition_include"):
        reasons.append("CONDITION_DISCOVERY_BOOST")
    return _dedupe(reasons)


def _unknown_member(score: float, reasons: Sequence[str]) -> ThemeMemberLeadership:
    return ThemeMemberLeadership(
        code="000000",
        name="UNKNOWN",
        role=StockRole.UNKNOWN,
        member_score=score,
        change_rate_pct=None,
        turnover_krw=None,
        execution_strength=None,
        momentum_1m=None,
        momentum_3m=None,
        momentum_5m=None,
        vwap=None,
        pullback_from_high_pct=None,
        stale=True,
        reason_codes=[*reasons, "SNAPSHOT_MISSING"],
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
