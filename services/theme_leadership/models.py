from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_bool,
    parse_float,
    parse_int,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)
from domain.theme.state import ThemeMemberRole as LegacyThemeMemberRole
from domain.theme.state import ThemeState as LegacyThemeState


class ThemeState(StrEnum):
    DATA_WAIT = "DATA_WAIT"
    WATCH = "WATCH"
    LEADER_ONLY = "LEADER_ONLY"
    SPREADING = "SPREADING"
    LEADING = "LEADING"
    FADING = "FADING"
    WEAK = "WEAK"


class StockRole(StrEnum):
    LEADER = "LEADER"
    CO_LEADER = "CO_LEADER"
    FOLLOWER = "FOLLOWER"
    LATE_LAGGARD = "LATE_LAGGARD"
    WEAK_MEMBER = "WEAK_MEMBER"
    OVERHEATED = "OVERHEATED"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"


def to_legacy_theme_state(state: ThemeState | str) -> LegacyThemeState:
    resolved = ThemeState(str(state).upper())
    if resolved is ThemeState.DATA_WAIT:
        return LegacyThemeState.DATA_WAIT
    if resolved is ThemeState.SPREADING:
        return LegacyThemeState.SPREADING
    if resolved is ThemeState.LEADING:
        return LegacyThemeState.LEADING
    if resolved is ThemeState.FADING:
        return LegacyThemeState.FADING
    return LegacyThemeState.WATCH


def to_legacy_member_role(role: StockRole | str) -> LegacyThemeMemberRole:
    resolved = StockRole(str(role).upper())
    if resolved is StockRole.LEADER:
        return LegacyThemeMemberRole.LEADER_CANDIDATE
    if resolved is StockRole.CO_LEADER:
        return LegacyThemeMemberRole.CO_LEADER_CANDIDATE
    if resolved is StockRole.FOLLOWER:
        return LegacyThemeMemberRole.FOLLOWER_CANDIDATE
    if resolved is StockRole.STALE:
        return LegacyThemeMemberRole.STALE
    if resolved is StockRole.UNKNOWN:
        return LegacyThemeMemberRole.UNKNOWN
    return LegacyThemeMemberRole.LAGGARD


@dataclass(frozen=True, kw_only=True)
class ThemeUniverseMember:
    theme_id: str
    theme_name: str
    code: str
    name: str
    weight: float = 1.0
    source_type: str = "UNKNOWN"
    source_name: str | None = None
    active: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "theme_id", require_non_empty_str(self.theme_id, "theme_id"))
        object.__setattr__(self, "theme_name", require_non_empty_str(self.theme_name, "theme_name"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "weight", parse_float(self.weight, "weight", min_value=0.0))
        object.__setattr__(
            self, "source_type", require_non_empty_str(self.source_type, "source_type").upper()
        )
        if self.source_name is not None:
            object.__setattr__(
                self, "source_name", require_non_empty_str(self.source_name, "source_name")
            )
        object.__setattr__(self, "active", parse_bool(self.active, "active"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "code": self.code,
            "name": self.name,
            "weight": self.weight,
            "source_type": self.source_type,
            "source_name": self.source_name,
            "active": self.active,
            "metadata": normalize_payload(self.metadata),
        }


@dataclass(frozen=True, kw_only=True)
class RealtimeStockSnapshot:
    code: str
    name: str
    market: str | None
    current_price: int | None
    change_rate_pct: float | None
    turnover_krw: float | None
    cum_volume: int | None
    execution_strength: float | None
    best_bid: int | None
    best_ask: int | None
    spread_ticks: int | None
    day_high: int | None
    day_low: int | None
    open_price: int | None
    prev_close: int | None
    momentum_1m: float | None
    momentum_3m: float | None
    momentum_5m: float | None
    vwap: float | None
    pullback_from_high_pct: float | None
    stale: bool
    vi_active: bool
    upper_limit_near: bool
    data_quality: str
    source_flags: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        if self.market is not None:
            object.__setattr__(self, "market", require_non_empty_str(self.market, "market").upper())
        if self.current_price is not None:
            object.__setattr__(
                self, "current_price", parse_int(self.current_price, "current_price", min_value=1)
            )
        if self.cum_volume is not None:
            object.__setattr__(
                self, "cum_volume", parse_int(self.cum_volume, "cum_volume", min_value=0)
            )
        for field_name in (
            "change_rate_pct",
            "turnover_krw",
            "execution_strength",
            "momentum_1m",
            "momentum_3m",
            "momentum_5m",
            "vwap",
            "pullback_from_high_pct",
        ):
            value = getattr(self, field_name)
            if value is not None:
                min_value = (
                    0.0
                    if field_name
                    in {"turnover_krw", "execution_strength", "vwap", "pullback_from_high_pct"}
                    else None
                )
                object.__setattr__(
                    self, field_name, parse_float(value, field_name, min_value=min_value)
                )
        for field_name in (
            "best_bid",
            "best_ask",
            "spread_ticks",
            "day_high",
            "day_low",
            "open_price",
            "prev_close",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_int(value, field_name, min_value=0))
        object.__setattr__(self, "stale", parse_bool(self.stale, "stale"))
        object.__setattr__(self, "vi_active", parse_bool(self.vi_active, "vi_active"))
        object.__setattr__(
            self, "upper_limit_near", parse_bool(self.upper_limit_near, "upper_limit_near")
        )
        object.__setattr__(
            self, "data_quality", require_non_empty_str(self.data_quality, "data_quality").upper()
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(
                require_non_empty_str(reason, "reason_code").upper() for reason in self.reason_codes
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "market": self.market,
            "current_price": self.current_price,
            "change_rate_pct": self.change_rate_pct,
            "turnover_krw": self.turnover_krw,
            "cum_volume": self.cum_volume,
            "execution_strength": self.execution_strength,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread_ticks": self.spread_ticks,
            "day_high": self.day_high,
            "day_low": self.day_low,
            "open_price": self.open_price,
            "prev_close": self.prev_close,
            "momentum_1m": self.momentum_1m,
            "momentum_3m": self.momentum_3m,
            "momentum_5m": self.momentum_5m,
            "vwap": self.vwap,
            "pullback_from_high_pct": self.pullback_from_high_pct,
            "stale": self.stale,
            "vi_active": self.vi_active,
            "upper_limit_near": self.upper_limit_near,
            "data_quality": self.data_quality,
            "source_flags": normalize_payload(self.source_flags),
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, kw_only=True)
class ThemeMemberLeadership:
    code: str
    name: str
    role: StockRole
    member_score: float
    change_rate_pct: float | None
    turnover_krw: float | None
    execution_strength: float | None
    momentum_1m: float | None
    momentum_3m: float | None
    momentum_5m: float | None
    vwap: float | None
    pullback_from_high_pct: float | None
    stale: bool
    reason_codes: Sequence[str] = field(default_factory=tuple)
    source_flags: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "role", StockRole(str(self.role).upper()))
        object.__setattr__(self, "member_score", parse_float(self.member_score, "member_score"))
        object.__setattr__(self, "stale", parse_bool(self.stale, "stale"))
        object.__setattr__(
            self,
            "reason_codes",
            tuple(
                require_non_empty_str(reason, "reason_code").upper() for reason in self.reason_codes
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "role": self.role.value,
            "member_score": self.member_score,
            "change_rate_pct": self.change_rate_pct,
            "turnover_krw": self.turnover_krw,
            "execution_strength": self.execution_strength,
            "momentum_1m": self.momentum_1m,
            "momentum_3m": self.momentum_3m,
            "momentum_5m": self.momentum_5m,
            "vwap": self.vwap,
            "pullback_from_high_pct": self.pullback_from_high_pct,
            "stale": self.stale,
            "reason_codes": list(self.reason_codes),
            "source_flags": normalize_payload(self.source_flags),
        }


@dataclass(frozen=True, kw_only=True)
class ThemeLeadershipSnapshot:
    theme_id: str
    theme_name: str
    state: ThemeState
    score: float
    rank: int
    observable_member_count: int
    valid_member_count: int
    fresh_member_count: int
    fresh_coverage_ratio: float
    rising_count: int
    rising_ratio: float
    leader_count: int
    co_leader_count: int
    follower_count: int
    total_turnover_krw: float
    turnover_share: float
    weighted_return_pct: float
    leader_code: str | None
    leader_name: str | None
    members: Sequence[ThemeMemberLeadership] = field(default_factory=tuple)
    reason_codes: Sequence[str] = field(default_factory=tuple)
    created_at: datetime | str = field(default_factory=datetime.utcnow)
    avg_change_rate_pct: float = 0.0
    max_change_rate_pct: float = 0.0
    leader_concentration: float = 0.0
    full_member_count: int = 0
    full_observed_count: int = 0
    full_fresh_member_count: int = 0
    full_fresh_coverage_ratio: float = 0.0
    score_components: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "theme_id", require_non_empty_str(self.theme_id, "theme_id"))
        object.__setattr__(self, "theme_name", require_non_empty_str(self.theme_name, "theme_name"))
        object.__setattr__(self, "state", ThemeState(str(self.state).upper()))
        for field_name in (
            "rank",
            "observable_member_count",
            "valid_member_count",
            "fresh_member_count",
            "rising_count",
            "leader_count",
            "co_leader_count",
            "follower_count",
            "full_member_count",
            "full_observed_count",
            "full_fresh_member_count",
        ):
            object.__setattr__(
                self, field_name, parse_int(getattr(self, field_name), field_name, min_value=0)
            )
        for field_name in (
            "score",
            "fresh_coverage_ratio",
            "rising_ratio",
            "total_turnover_krw",
            "turnover_share",
            "weighted_return_pct",
            "avg_change_rate_pct",
            "max_change_rate_pct",
            "leader_concentration",
            "full_fresh_coverage_ratio",
        ):
            min_value = (
                0.0
                if field_name
                not in {"weighted_return_pct", "avg_change_rate_pct", "max_change_rate_pct"}
                else None
            )
            object.__setattr__(
                self,
                field_name,
                parse_float(getattr(self, field_name), field_name, min_value=min_value),
            )
        if self.leader_code is not None:
            object.__setattr__(self, "leader_code", validate_stock_code(self.leader_code))
        if self.leader_name is not None:
            object.__setattr__(
                self, "leader_name", require_non_empty_str(self.leader_name, "leader_name")
            )
        object.__setattr__(self, "created_at", parse_timestamp(self.created_at, "created_at"))
        object.__setattr__(self, "members", tuple(self.members))
        object.__setattr__(
            self,
            "reason_codes",
            tuple(
                require_non_empty_str(reason, "reason_code").upper() for reason in self.reason_codes
            ),
        )

    def to_dict(self, *, include_members: bool = True) -> dict[str, Any]:
        data = {
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "state": self.state.value,
            "legacy_state": to_legacy_theme_state(self.state).value,
            "score": self.score,
            "rank": self.rank,
            "observable_member_count": self.observable_member_count,
            "valid_member_count": self.valid_member_count,
            "fresh_member_count": self.fresh_member_count,
            "fresh_coverage_ratio": self.fresh_coverage_ratio,
            "rising_count": self.rising_count,
            "rising_ratio": self.rising_ratio,
            "leader_count": self.leader_count,
            "co_leader_count": self.co_leader_count,
            "follower_count": self.follower_count,
            "total_turnover_krw": self.total_turnover_krw,
            "turnover_share": self.turnover_share,
            "weighted_return_pct": self.weighted_return_pct,
            "avg_change_rate_pct": self.avg_change_rate_pct,
            "max_change_rate_pct": self.max_change_rate_pct,
            "leader_concentration": self.leader_concentration,
            "full_member_count": self.full_member_count,
            "full_observed_count": self.full_observed_count,
            "full_fresh_member_count": self.full_fresh_member_count,
            "full_fresh_coverage_ratio": self.full_fresh_coverage_ratio,
            "leader_code": self.leader_code,
            "leader_name": self.leader_name,
            "reason_codes": list(self.reason_codes),
            "created_at": datetime_to_wire(parse_timestamp(self.created_at, "created_at")),
            "score_components": dict(self.score_components),
        }
        if include_members:
            data["members"] = [member.to_dict() for member in self.members]
        return data


@dataclass(frozen=True, kw_only=True)
class WatchsetItem:
    code: str
    name: str
    theme_id: str
    theme_name: str
    theme_state: ThemeState
    theme_rank: int
    stock_role: StockRole
    priority_score: float
    reason_codes: Sequence[str]
    source_type: str
    source_detail: Mapping[str, Any]
    expires_at: datetime | str
    observed_at: datetime | str

    def __post_init__(self) -> None:
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "theme_id", require_non_empty_str(self.theme_id, "theme_id"))
        object.__setattr__(self, "theme_name", require_non_empty_str(self.theme_name, "theme_name"))
        object.__setattr__(self, "theme_state", ThemeState(str(self.theme_state).upper()))
        object.__setattr__(
            self, "theme_rank", parse_int(self.theme_rank, "theme_rank", min_value=0)
        )
        object.__setattr__(self, "stock_role", StockRole(str(self.stock_role).upper()))
        object.__setattr__(
            self, "priority_score", parse_float(self.priority_score, "priority_score")
        )
        object.__setattr__(
            self, "source_type", require_non_empty_str(self.source_type, "source_type").upper()
        )
        object.__setattr__(self, "expires_at", parse_timestamp(self.expires_at, "expires_at"))
        object.__setattr__(self, "observed_at", parse_timestamp(self.observed_at, "observed_at"))
        object.__setattr__(
            self,
            "reason_codes",
            tuple(
                require_non_empty_str(reason, "reason_code").upper() for reason in self.reason_codes
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "theme_state": self.theme_state.value,
            "legacy_theme_state": to_legacy_theme_state(self.theme_state).value,
            "theme_rank": self.theme_rank,
            "stock_role": self.stock_role.value,
            "legacy_member_role": to_legacy_member_role(self.stock_role).value,
            "priority_score": self.priority_score,
            "reason_codes": list(self.reason_codes),
            "source_type": self.source_type,
            "source_detail": normalize_payload(self.source_detail),
            "expires_at": datetime_to_wire(parse_timestamp(self.expires_at, "expires_at")),
            "observed_at": datetime_to_wire(parse_timestamp(self.observed_at, "observed_at")),
        }


@dataclass(frozen=True, kw_only=True)
class WatchsetResult:
    items: Sequence[WatchsetItem] = field(default_factory=tuple)
    excluded: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    near_miss: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    reason_summary: Mapping[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": [item.to_dict() for item in self.items],
            "excluded": [normalize_payload(item) for item in self.excluded],
            "near_miss": [normalize_payload(item) for item in self.near_miss],
            "reason_summary": dict(self.reason_summary),
        }
