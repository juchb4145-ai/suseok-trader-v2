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
    parse_int,
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    validate_stock_code,
)
from domain.risk.category import RiskCategory
from domain.risk.reasons import RiskReasonCode
from domain.risk.status import RiskCheckStatus, RiskObservationStatus, RiskSeverity


@dataclass(frozen=True, kw_only=True)
class RiskInputContext:
    candidate_instance_id: str
    trade_date: str
    code: str
    name: str
    candidate_state: str | None = None
    strategy_observation_id: str | None = None
    strategy_status: str | None = None
    strategy_evaluated_at: datetime | str | None = None
    primary_setup_type: str | None = None
    score: float = 0.0
    confidence: float = 0.0
    theme_id: str | None = None
    theme_name: str | None = None
    theme_state: str | None = None
    theme_role: str | None = None
    theme_fresh_coverage_ratio: float | None = None
    theme_rising_ratio: float | None = None
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
    day_high: float | None = None
    spread_ticks: int | None = None
    cumulative_trade_value: float | None = None
    trade_value_delta_1m: float | None = None
    trade_value_delta_3m: float | None = None
    execution_strength: float | None = None
    vwap: float | None = None
    above_vwap: bool = False
    source_count: int = 0
    active_source_count: int = 0
    bar_1m_ready: bool = False
    reason_codes: Sequence[RiskReasonCode | str] = field(default_factory=tuple)
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
        for field_name in (
            "candidate_state",
            "strategy_observation_id",
            "strategy_status",
            "primary_setup_type",
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
        if self.strategy_evaluated_at is not None:
            object.__setattr__(
                self,
                "strategy_evaluated_at",
                parse_timestamp(self.strategy_evaluated_at, "strategy_evaluated_at"),
            )
        for field_name in (
            "score",
            "confidence",
            "theme_fresh_coverage_ratio",
            "theme_rising_ratio",
            "tick_age_sec",
            "price",
            "day_high",
            "cumulative_trade_value",
            "trade_value_delta_1m",
            "trade_value_delta_3m",
            "execution_strength",
            "vwap",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(
                    self,
                    field_name,
                    parse_float(value, field_name, min_value=0.0),
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
        if self.change_rate is not None:
            object.__setattr__(self, "change_rate", parse_float(self.change_rate, "change_rate"))
        if self.spread_ticks is not None:
            object.__setattr__(
                self,
                "spread_ticks",
                parse_int(self.spread_ticks, "spread_ticks", min_value=0),
            )
        object.__setattr__(self, "score", min(float(self.score), 1.0))
        object.__setattr__(self, "confidence", min(float(self.confidence), 1.0))
        object.__setattr__(self, "above_vwap", parse_bool(self.above_vwap, "above_vwap"))
        for field_name in ("source_count", "active_source_count"):
            value = int(getattr(self, field_name))
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")
            object.__setattr__(self, field_name, value)
        object.__setattr__(
            self,
            "bar_1m_ready",
            parse_bool(self.bar_1m_ready, "bar_1m_ready"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_instance_id": self.candidate_instance_id,
            "strategy_observation_id": self.strategy_observation_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "candidate_state": self.candidate_state,
            "strategy_status": self.strategy_status,
            "strategy_evaluated_at": (
                datetime_to_wire(self.strategy_evaluated_at)
                if self.strategy_evaluated_at is not None
                else None
            ),
            "primary_setup_type": self.primary_setup_type,
            "score": self.score,
            "confidence": self.confidence,
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "theme_state": self.theme_state,
            "theme_role": self.theme_role,
            "theme_fresh_coverage_ratio": self.theme_fresh_coverage_ratio,
            "theme_rising_ratio": self.theme_rising_ratio,
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
            "day_high": self.day_high,
            "spread_ticks": self.spread_ticks,
            "cumulative_trade_value": self.cumulative_trade_value,
            "trade_value_delta_1m": self.trade_value_delta_1m,
            "trade_value_delta_3m": self.trade_value_delta_3m,
            "execution_strength": self.execution_strength,
            "vwap": self.vwap,
            "above_vwap": self.above_vwap,
            "source_count": self.source_count,
            "active_source_count": self.active_source_count,
            "bar_1m_ready": self.bar_1m_ready,
            "reason_codes": list(self.reason_codes),
            "raw_context": normalize_payload(self.raw_context),
        }


@dataclass(frozen=True, kw_only=True)
class RiskCheckObservation:
    category: RiskCategory | str
    status: RiskCheckStatus | str
    severity: RiskSeverity | str
    reason_codes: Sequence[RiskReasonCode | str] = field(default_factory=tuple)
    message: str = ""
    evidence_json: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "category",
            parse_str_enum(self.category, RiskCategory, "category"),
        )
        object.__setattr__(self, "status", parse_str_enum(self.status, RiskCheckStatus, "status"))
        object.__setattr__(
            self,
            "severity",
            parse_str_enum(self.severity, RiskSeverity, "severity"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(_reason_value(reason) for reason in self.reason_codes),
        )
        message = self.message.strip() if self.message else self.category.value
        object.__setattr__(self, "message", require_non_empty_str(message, "message"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category.value,
            "status": self.status.value,
            "severity": self.severity.value,
            "reason_codes": list(self.reason_codes),
            "message": self.message,
            "evidence_json": normalize_payload(self.evidence_json),
        }


@dataclass(frozen=True, kw_only=True)
class RiskObservation:
    risk_observation_id: str
    candidate_instance_id: str
    strategy_observation_id: str | None
    trade_date: str
    code: str
    name: str
    evaluated_at: datetime | str
    overall_status: RiskObservationStatus | str
    max_severity: RiskSeverity | str
    blocked_count: int = 0
    caution_count: int = 0
    pass_count: int = 0
    check_observations: Sequence[RiskCheckObservation] = field(default_factory=tuple)
    reason_codes: Sequence[RiskReasonCode | str] = field(default_factory=tuple)
    evidence_json: Mapping[str, Any] = field(default_factory=dict)
    config_version: str = "observe_v1"
    observe_only: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "risk_observation_id",
            require_non_empty_str(self.risk_observation_id, "risk_observation_id"),
        )
        object.__setattr__(
            self,
            "candidate_instance_id",
            require_non_empty_str(self.candidate_instance_id, "candidate_instance_id"),
        )
        if self.strategy_observation_id is not None:
            object.__setattr__(
                self,
                "strategy_observation_id",
                require_non_empty_str(self.strategy_observation_id, "strategy_observation_id"),
            )
        object.__setattr__(self, "trade_date", require_non_empty_str(self.trade_date, "trade_date"))
        object.__setattr__(self, "code", validate_stock_code(self.code))
        object.__setattr__(self, "name", require_non_empty_str(self.name, "name"))
        object.__setattr__(self, "evaluated_at", parse_timestamp(self.evaluated_at, "evaluated_at"))
        object.__setattr__(
            self,
            "overall_status",
            parse_str_enum(self.overall_status, RiskObservationStatus, "overall_status"),
        )
        object.__setattr__(
            self,
            "max_severity",
            parse_str_enum(self.max_severity, RiskSeverity, "max_severity"),
        )
        for field_name in ("blocked_count", "caution_count", "pass_count"):
            value = int(getattr(self, field_name))
            if value < 0:
                raise ValueError(f"{field_name} must be >= 0")
            object.__setattr__(self, field_name, value)
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

    def to_dict(self, *, include_checks: bool = True) -> dict[str, Any]:
        data = {
            "risk_observation_id": self.risk_observation_id,
            "candidate_instance_id": self.candidate_instance_id,
            "strategy_observation_id": self.strategy_observation_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "name": self.name,
            "evaluated_at": datetime_to_wire(self.evaluated_at),
            "overall_status": self.overall_status.value,
            "max_severity": self.max_severity.value,
            "blocked_count": self.blocked_count,
            "caution_count": self.caution_count,
            "pass_count": self.pass_count,
            "reason_codes": list(self.reason_codes),
            "evidence_json": normalize_payload(self.evidence_json),
            "config_version": self.config_version,
            "observe_only": self.observe_only,
        }
        if include_checks:
            data["check_observations"] = [
                check.to_dict() for check in self.check_observations
            ]
        return data


def _reason_value(reason: RiskReasonCode | str) -> str:
    if isinstance(reason, RiskReasonCode):
        return reason.value
    return require_non_empty_str(reason, "reason_code").upper()
