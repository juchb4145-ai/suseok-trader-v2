from __future__ import annotations

from collections.abc import Mapping
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
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)
from domain.theme.quality import ThemeSnapshotQuality
from domain.theme.state import ThemeMemberRole, ThemeState


class ThemeSourceType(StrEnum):
    MANUAL = "MANUAL"
    IMPORTED = "IMPORTED"
    NAVER_REFERENCE = "NAVER_REFERENCE"
    CONDITION_DERIVED = "CONDITION_DERIVED"
    MOCK = "MOCK"


@dataclass(frozen=True, kw_only=True)
class ThemeMembership:
    """Read-only theme membership observation input, not a trading decision."""

    theme_id: str
    theme_name: str
    code: str
    name: str
    source_type: ThemeSourceType
    source_name: str | None = None
    active: bool = True
    weight: float = 1.0
    created_at: datetime | str | None = None
    updated_at: datetime | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "theme_id", require_non_empty_str(self.theme_id, "theme_id"))
        object.__setattr__(self, "theme_name", require_non_empty_str(self.theme_name, "theme_name"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(
            self,
            "source_type",
            parse_str_enum(self.source_type, ThemeSourceType, "source_type"),
        )
        if self.source_name is not None:
            object.__setattr__(
                self,
                "source_name",
                require_non_empty_str(self.source_name, "source_name"),
            )
        object.__setattr__(self, "active", parse_bool(self.active, "active"))
        object.__setattr__(self, "weight", parse_float(self.weight, "weight", min_value=0.0))
        if self.created_at is not None:
            object.__setattr__(
                self,
                "created_at",
                parse_timestamp(self.created_at, "created_at"),
            )
        if self.updated_at is not None:
            object.__setattr__(
                self,
                "updated_at",
                parse_timestamp(self.updated_at, "updated_at"),
            )

    def to_dict(self) -> dict[str, Any]:
        data = {
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "code": self.code,
            "name": self.name,
            "source_type": self.source_type.value,
            "source_name": self.source_name,
            "active": self.active,
            "weight": self.weight,
            "metadata": normalize_payload(self.metadata),
        }
        if self.created_at is not None:
            data["created_at"] = datetime_to_wire(parse_timestamp(self.created_at, "created_at"))
        if self.updated_at is not None:
            data["updated_at"] = datetime_to_wire(parse_timestamp(self.updated_at, "updated_at"))
        return data


@dataclass(frozen=True, kw_only=True)
class ThemeMemberSnapshot:
    """Per-member observation summary used to rebuild a theme snapshot."""

    theme_id: str
    theme_name: str
    code: str
    name: str
    price: int | None
    change_rate: float | None
    cumulative_trade_value: float | None
    volume_delta_1m: int = 0
    trade_value_delta_1m: float = 0.0
    trade_value_delta_3m: float = 0.0
    trade_value_delta_5m: float = 0.0
    execution_strength: float | None = None
    vwap: float | None = None
    above_vwap: bool = False
    readiness_status: str = "MISSING"
    member_role: ThemeMemberRole = ThemeMemberRole.UNKNOWN
    observation_source: str = "UNKNOWN"
    tick_age_sec: float | None = None
    event_ts: datetime | str | None = None
    calculated_at: datetime | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "theme_id", require_non_empty_str(self.theme_id, "theme_id"))
        object.__setattr__(self, "theme_name", require_non_empty_str(self.theme_name, "theme_name"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        if self.price is not None:
            object.__setattr__(self, "price", parse_int(self.price, "price", min_value=1))
        if self.change_rate is not None:
            object.__setattr__(self, "change_rate", parse_float(self.change_rate, "change_rate"))
        if self.cumulative_trade_value is not None:
            object.__setattr__(
                self,
                "cumulative_trade_value",
                parse_float(
                    self.cumulative_trade_value,
                    "cumulative_trade_value",
                    min_value=0.0,
                ),
            )
        object.__setattr__(
            self,
            "volume_delta_1m",
            parse_int(self.volume_delta_1m, "volume_delta_1m", min_value=0),
        )
        object.__setattr__(
            self,
            "trade_value_delta_1m",
            parse_float(self.trade_value_delta_1m, "trade_value_delta_1m", min_value=0.0),
        )
        object.__setattr__(
            self,
            "trade_value_delta_3m",
            parse_float(self.trade_value_delta_3m, "trade_value_delta_3m", min_value=0.0),
        )
        object.__setattr__(
            self,
            "trade_value_delta_5m",
            parse_float(self.trade_value_delta_5m, "trade_value_delta_5m", min_value=0.0),
        )
        if self.execution_strength is not None:
            object.__setattr__(
                self,
                "execution_strength",
                parse_float(self.execution_strength, "execution_strength", min_value=0.0),
            )
        if self.vwap is not None:
            object.__setattr__(self, "vwap", parse_float(self.vwap, "vwap", min_value=0.0))
        object.__setattr__(self, "above_vwap", parse_bool(self.above_vwap, "above_vwap"))
        object.__setattr__(
            self,
            "readiness_status",
            require_non_empty_str(self.readiness_status, "readiness_status"),
        )
        object.__setattr__(
            self,
            "member_role",
            parse_str_enum(self.member_role, ThemeMemberRole, "member_role"),
        )
        object.__setattr__(
            self,
            "observation_source",
            require_non_empty_str(self.observation_source, "observation_source").upper(),
        )
        if self.tick_age_sec is not None:
            object.__setattr__(
                self,
                "tick_age_sec",
                parse_float(self.tick_age_sec, "tick_age_sec", min_value=0.0),
            )
        if self.event_ts is not None:
            object.__setattr__(self, "event_ts", parse_timestamp(self.event_ts, "event_ts"))
        if self.calculated_at is not None:
            object.__setattr__(
                self,
                "calculated_at",
                parse_timestamp(self.calculated_at, "calculated_at"),
            )

    def to_dict(self) -> dict[str, Any]:
        data = {
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "code": self.code,
            "name": self.name,
            "price": self.price,
            "change_rate": self.change_rate,
            "cumulative_trade_value": self.cumulative_trade_value,
            "volume_delta_1m": self.volume_delta_1m,
            "trade_value_delta_1m": self.trade_value_delta_1m,
            "trade_value_delta_3m": self.trade_value_delta_3m,
            "trade_value_delta_5m": self.trade_value_delta_5m,
            "execution_strength": self.execution_strength,
            "vwap": self.vwap,
            "above_vwap": self.above_vwap,
            "readiness_status": self.readiness_status,
            "member_role": self.member_role.value,
            "observation_source": self.observation_source,
            "tick_age_sec": self.tick_age_sec,
            "metadata": normalize_payload(self.metadata),
        }
        if self.event_ts is not None:
            data["event_ts"] = datetime_to_wire(parse_timestamp(self.event_ts, "event_ts"))
        else:
            data["event_ts"] = None
        if self.calculated_at is not None:
            data["calculated_at"] = datetime_to_wire(
                parse_timestamp(self.calculated_at, "calculated_at")
            )
        return data


@dataclass(frozen=True, kw_only=True)
class ThemeSnapshot:
    """Theme-level market observation summary; it never creates order intent."""

    snapshot_id: str
    theme_id: str
    theme_name: str
    calculated_at: datetime | str
    member_count: int
    active_member_count: int
    observed_member_count: int
    fresh_member_count: int
    fresh_coverage_ratio: float
    scan_coverage_ratio: float = 0.0
    realtime_coverage_ratio: float = 0.0
    rising_member_count: int
    rising_ratio: float
    avg_change_rate: float
    max_change_rate: float
    total_trade_value: float
    trade_value_delta_1m: float
    trade_value_delta_3m: float
    trade_value_delta_5m: float
    flow_trade_value_delta: float = 0.0
    flow_rank_inflow_count: int = 0
    flow_score: float = 0.0
    leading_code: str | None
    leading_name: str | None
    co_leader_codes: list[str] = field(default_factory=list)
    follower_codes: list[str] = field(default_factory=list)
    state: ThemeState = ThemeState.DATA_WAIT
    quality_status: ThemeSnapshotQuality = ThemeSnapshotQuality.DATA_WAIT
    reason_codes: list[str] = field(default_factory=list)
    members: list[ThemeMemberSnapshot] = field(default_factory=list)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "snapshot_id",
            require_non_empty_str(self.snapshot_id, "snapshot_id"),
        )
        object.__setattr__(self, "theme_id", require_non_empty_str(self.theme_id, "theme_id"))
        object.__setattr__(self, "theme_name", require_non_empty_str(self.theme_name, "theme_name"))
        object.__setattr__(
            self,
            "calculated_at",
            parse_timestamp(self.calculated_at, "calculated_at"),
        )
        for field_name in (
            "member_count",
            "active_member_count",
            "observed_member_count",
            "fresh_member_count",
            "rising_member_count",
            "flow_rank_inflow_count",
        ):
            object.__setattr__(
                self,
                field_name,
                parse_int(getattr(self, field_name), field_name, min_value=0),
            )
        for field_name in (
            "fresh_coverage_ratio",
            "scan_coverage_ratio",
            "realtime_coverage_ratio",
            "rising_ratio",
            "avg_change_rate",
            "max_change_rate",
            "total_trade_value",
            "trade_value_delta_1m",
            "trade_value_delta_3m",
            "trade_value_delta_5m",
            "flow_trade_value_delta",
            "flow_score",
        ):
            min_value = 0.0 if field_name not in {"avg_change_rate", "max_change_rate"} else None
            object.__setattr__(
                self,
                field_name,
                parse_float(getattr(self, field_name), field_name, min_value=min_value),
            )
        if self.leading_code is not None:
            object.__setattr__(self, "leading_code", validate_stock_code(self.leading_code))
        if self.leading_name is not None:
            object.__setattr__(
                self,
                "leading_name",
                require_non_empty_str(self.leading_name, "leading_name"),
            )
        object.__setattr__(
            self,
            "co_leader_codes",
            [validate_stock_code(code) for code in self.co_leader_codes],
        )
        object.__setattr__(
            self,
            "follower_codes",
            [validate_stock_code(code) for code in self.follower_codes],
        )
        object.__setattr__(self, "state", parse_str_enum(self.state, ThemeState, "state"))
        object.__setattr__(
            self,
            "quality_status",
            parse_str_enum(self.quality_status, ThemeSnapshotQuality, "quality_status"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            [require_non_empty_str(reason, "reason_code") for reason in self.reason_codes],
        )

    def to_dict(self, *, include_members: bool = True) -> dict[str, Any]:
        data = {
            "snapshot_id": self.snapshot_id,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "calculated_at": datetime_to_wire(parse_timestamp(self.calculated_at, "calculated_at")),
            "member_count": self.member_count,
            "active_member_count": self.active_member_count,
            "observed_member_count": self.observed_member_count,
            "fresh_member_count": self.fresh_member_count,
            "fresh_coverage_ratio": self.fresh_coverage_ratio,
            "scan_coverage_ratio": self.scan_coverage_ratio,
            "realtime_coverage_ratio": self.realtime_coverage_ratio,
            "rising_member_count": self.rising_member_count,
            "rising_ratio": self.rising_ratio,
            "avg_change_rate": self.avg_change_rate,
            "max_change_rate": self.max_change_rate,
            "total_trade_value": self.total_trade_value,
            "trade_value_delta_1m": self.trade_value_delta_1m,
            "trade_value_delta_3m": self.trade_value_delta_3m,
            "trade_value_delta_5m": self.trade_value_delta_5m,
            "flow_trade_value_delta": self.flow_trade_value_delta,
            "flow_rank_inflow_count": self.flow_rank_inflow_count,
            "flow_score": self.flow_score,
            "leading_code": self.leading_code,
            "leading_name": self.leading_name,
            "co_leader_codes": list(self.co_leader_codes),
            "follower_codes": list(self.follower_codes),
            "state": self.state.value,
            "quality_status": self.quality_status.value,
            "reason_codes": list(self.reason_codes),
            "metadata": normalize_payload(self.metadata),
        }
        if include_members:
            data["members"] = [member.to_dict() for member in self.members]
        return data


def row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
