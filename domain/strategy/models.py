from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_bool,
    parse_float,
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)
from domain.strategy.reasons import StrategyReasonCode
from domain.strategy.setup import StrategySetupType
from domain.strategy.status import StrategyObservationStatus


@dataclass(frozen=True, kw_only=True)
class StrategyCandidateContext:
    candidate_instance_id: str
    trade_date: str
    code: str
    name: str
    candidate_state: str
    theme_id: str | None = None
    theme_name: str | None = None
    theme_state: str | None = None
    theme_role: str | None = None
    market_readiness_status: str | None = None
    market_regime_status: str | None = None
    market_regime_quality_status: str | None = None
    primary_index_code: str | None = None
    secondary_index_code: str | None = None
    primary_index_return_5m: float | None = None
    primary_index_drawdown_15m: float | None = None
    secondary_index_return_5m: float | None = None
    secondary_index_drawdown_15m: float | None = None
    tick_age_sec: float | None = None
    price: float | None = None
    change_rate: float | None = None
    cumulative_trade_value: float | None = None
    trade_value_delta_1m: float | None = None
    trade_value_delta_3m: float | None = None
    trade_value_delta_5m: float | None = None
    day_high: float | None = None
    day_low: float | None = None
    vwap: float | None = None
    above_vwap: bool = False
    bar_1m_ready: bool = False
    bar_3m_ready: bool = False
    bar_5m_ready: bool = False
    source_count: int = 0
    active_source_count: int = 0
    reason_codes: Sequence[StrategyReasonCode | str] = field(default_factory=tuple)
    raw_context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(
            self,
            "candidate_state",
            require_non_empty_str(self.candidate_state, "candidate_state").upper(),
        )
        for field_name in (
            "theme_id",
            "theme_name",
            "theme_state",
            "theme_role",
            "market_readiness_status",
            "market_regime_status",
            "market_regime_quality_status",
            "primary_index_code",
            "secondary_index_code",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, require_non_empty_str(value, field_name))
        for field_name in (
            "tick_age_sec",
            "price",
            "change_rate",
            "cumulative_trade_value",
            "trade_value_delta_1m",
            "trade_value_delta_3m",
            "trade_value_delta_5m",
            "day_high",
            "day_low",
            "vwap",
        ):
            value = getattr(self, field_name)
            if value is not None:
                min_value = 0.0 if field_name not in {"change_rate"} else None
                object.__setattr__(
                    self,
                    field_name,
                    parse_float(value, field_name, min_value=min_value),
                )
        for field_name in (
            "primary_index_return_5m",
            "primary_index_drawdown_15m",
            "secondary_index_return_5m",
            "secondary_index_drawdown_15m",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, parse_float(value, field_name))
        for field_name in ("above_vwap", "bar_1m_ready", "bar_3m_ready", "bar_5m_ready"):
            object.__setattr__(self, field_name, parse_bool(getattr(self, field_name), field_name))
        for field_name in ("source_count", "active_source_count"):
            value = int(getattr(self, field_name))
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")
            object.__setattr__(self, field_name, value)
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_instance_id": self.candidate_instance_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "candidate_state": self.candidate_state,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "theme_state": self.theme_state,
            "theme_role": self.theme_role,
            "market_readiness_status": self.market_readiness_status,
            "market_regime_status": self.market_regime_status,
            "market_regime_quality_status": self.market_regime_quality_status,
            "primary_index_code": self.primary_index_code,
            "secondary_index_code": self.secondary_index_code,
            "primary_index_return_5m": self.primary_index_return_5m,
            "primary_index_drawdown_15m": self.primary_index_drawdown_15m,
            "secondary_index_return_5m": self.secondary_index_return_5m,
            "secondary_index_drawdown_15m": self.secondary_index_drawdown_15m,
            "tick_age_sec": self.tick_age_sec,
            "price": self.price,
            "change_rate": self.change_rate,
            "cumulative_trade_value": self.cumulative_trade_value,
            "trade_value_delta_1m": self.trade_value_delta_1m,
            "trade_value_delta_3m": self.trade_value_delta_3m,
            "trade_value_delta_5m": self.trade_value_delta_5m,
            "day_high": self.day_high,
            "day_low": self.day_low,
            "vwap": self.vwap,
            "above_vwap": self.above_vwap,
            "bar_1m_ready": self.bar_1m_ready,
            "bar_3m_ready": self.bar_3m_ready,
            "bar_5m_ready": self.bar_5m_ready,
            "source_count": self.source_count,
            "active_source_count": self.active_source_count,
            "reason_codes": list(self.reason_codes),
            "raw_context": normalize_payload(self.raw_context),
        }


@dataclass(frozen=True, kw_only=True)
class SetupObservation:
    setup_type: StrategySetupType
    status: StrategyObservationStatus
    score: float = 0.0
    confidence: float = 0.0
    reason_codes: Sequence[StrategyReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "setup_type",
            parse_str_enum(self.setup_type, StrategySetupType, "setup_type"),
        )
        object.__setattr__(
            self,
            "status",
            parse_str_enum(self.status, StrategyObservationStatus, "status"),
        )
        object.__setattr__(self, "score", _bounded_observation_float(self.score, "score"))
        object.__setattr__(
            self,
            "confidence",
            _bounded_observation_float(self.confidence, "confidence"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "setup_type": self.setup_type.value,
            "status": self.status.value,
            "score": self.score,
            "confidence": self.confidence,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(self.evidence_json),
        }


@dataclass(frozen=True, kw_only=True)
class StrategyObservation:
    strategy_observation_id: str
    candidate_instance_id: str
    trade_date: str
    code: str
    name: str
    evaluated_at: datetime | str
    overall_status: StrategyObservationStatus
    primary_setup_type: StrategySetupType | str | None
    primary_setup_status: StrategyObservationStatus | str | None
    setup_observations: Sequence[SetupObservation] = field(default_factory=tuple)
    reason_codes: Sequence[StrategyReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    config_version: str = "observe_v1"
    observe_only: bool = True
    score: float = 0.0
    confidence: float = 0.0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "strategy_observation_id",
            require_non_empty_str(self.strategy_observation_id, "strategy_observation_id"),
        )
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(
            self,
            "evaluated_at",
            parse_timestamp(self.evaluated_at, "evaluated_at"),
        )
        object.__setattr__(
            self,
            "overall_status",
            parse_str_enum(self.overall_status, StrategyObservationStatus, "overall_status"),
        )
        if self.primary_setup_type is not None:
            object.__setattr__(
                self,
                "primary_setup_type",
                parse_str_enum(
                    self.primary_setup_type,
                    StrategySetupType,
                    "primary_setup_type",
                ),
            )
        if self.primary_setup_status is not None:
            object.__setattr__(
                self,
                "primary_setup_status",
                parse_str_enum(
                    self.primary_setup_status,
                    StrategyObservationStatus,
                    "primary_setup_status",
                ),
            )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )
        object.__setattr__(
            self,
            "config_version",
            require_non_empty_str(self.config_version, "config_version"),
        )
        object.__setattr__(self, "observe_only", parse_bool(self.observe_only, "observe_only"))
        object.__setattr__(self, "score", _bounded_observation_float(self.score, "score"))
        object.__setattr__(
            self,
            "confidence",
            _bounded_observation_float(self.confidence, "confidence"),
        )

    def to_dict(self, *, include_setups: bool = True) -> dict[str, Any]:
        data = {
            "strategy_observation_id": self.strategy_observation_id,
            "candidate_instance_id": self.candidate_instance_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "evaluated_at": datetime_to_wire(parse_timestamp(self.evaluated_at, "evaluated_at")),
            "overall_status": self.overall_status.value,
            "primary_setup_type": (
                self.primary_setup_type.value if self.primary_setup_type is not None else None
            ),
            "primary_setup_status": (
                self.primary_setup_status.value
                if self.primary_setup_status is not None
                else None
            ),
            "score": self.score,
            "confidence": self.confidence,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(self.evidence_json),
            "config_version": self.config_version,
            "observe_only": self.observe_only,
        }
        if include_setups:
            data["setup_observations"] = [
                setup.to_dict() for setup in self.setup_observations
            ]
        return data


def _bounded_observation_float(value: float, field_name: str) -> float:
    parsed = parse_float(value, field_name, min_value=0.0)
    return min(parsed, 1.0)


def _reason_value(reason: StrategyReasonCode | str) -> str:
    if isinstance(reason, StrategyReasonCode):
        return reason.value
    return require_non_empty_str(reason, "reason_code").upper()
