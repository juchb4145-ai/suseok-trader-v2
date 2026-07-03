from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    parse_str_enum,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.candidate.state import CandidateState
from domain.market.quality import tick_age_seconds
from domain.risk.category import RiskCategory
from domain.risk.evaluator import calculate_overall_status, max_severity
from domain.risk.models import RiskCheckObservation, RiskInputContext, RiskObservation
from domain.risk.reasons import RiskReasonCode
from domain.risk.status import RiskCheckStatus, RiskObservationStatus, RiskSeverity
from domain.strategy.status import StrategyObservationStatus
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.live_sim.daily_loss_guard import build_live_sim_daily_loss_evidence
from services.runtime.evaluation_run_guard import (
    EVALUATION_PIPELINE_LOCK,
    immediate_transaction,
    runtime_execution_lock,
)

ACTIVE_DRY_RUN_INTENT_STATUSES = ("CREATED",)
ACTIVE_DRY_RUN_ORDER_STATUSES = (
    "CREATED",
    "SIMULATED_SUBMITTED",
    "SIMULATED_PARTIALLY_FILLED",
)
ACTIVE_DRY_RUN_POSITION_STATUSES = ("OPEN",)
ACTIVE_LIVE_SIM_ORDER_STATUSES = (
    "INTENT_CREATED",
    "COMMAND_QUEUED",
    "COMMAND_DISPATCHED",
    "BROKER_ACKED",
    "PARTIALLY_FILLED",
    "CANCEL_REQUESTED",
    "CANCEL_COMMAND_QUEUED",
    "EXIT_REQUESTED",
    "EXIT_COMMAND_QUEUED",
)
ACTIVE_LIVE_SIM_POSITION_STATUSES = ("OPEN", "CLOSING", "RECONCILE_MISMATCH")
LIVE_SIM_POSITION_ORDER_STATUSES = ("PARTIALLY_FILLED", "FILLED")


@dataclass(frozen=True, kw_only=True)
class RiskEvaluationRunResult:
    run_id: str
    trade_date: str | None
    strategy_observation_count: int = 0
    evaluated_count: int = 0
    observe_pass_count: int = 0
    caution_count: int = 0
    block_count: int = 0
    data_wait_count: int = 0
    error_count: int = 0
    config_version: str = "observe_v1"
    status: str = "COMPLETED"
    observe_only: bool = True
    order_routing_enabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "strategy_observation_count": self.strategy_observation_count,
            "evaluated_count": self.evaluated_count,
            "observe_pass_count": self.observe_pass_count,
            "caution_count": self.caution_count,
            "block_count": self.block_count,
            "data_wait_count": self.data_wait_count,
            "error_count": self.error_count,
            "config_version": self.config_version,
            "status": self.status,
            "observe_only": True,
            "order_routing_enabled": False,
        }


def load_risk_input_context(
    connection: sqlite3.Connection,
    candidate_instance_id: str | None = None,
    strategy_observation_id: str | None = None,
    settings: Settings | None = None,
) -> RiskInputContext:
    resolved_settings = settings or load_settings()
    strategy = _strategy_row(
        connection,
        candidate_instance_id=candidate_instance_id,
        strategy_observation_id=strategy_observation_id,
    )
    normalized_candidate_id = _candidate_id_from_inputs(candidate_instance_id, strategy)
    candidate = _candidate_row(connection, normalized_candidate_id)
    if candidate is None and strategy is None:
        raise ValueError(f"candidate not found: {normalized_candidate_id}")

    code = _first_text(
        candidate["code"] if candidate is not None else None,
        strategy["code"] if strategy is not None else None,
    )
    if code is None:
        raise ValueError(f"code not found for candidate: {normalized_candidate_id}")
    normalized_code = validate_stock_code(code)
    trade_date = _first_text(
        candidate["trade_date"] if candidate is not None else None,
        strategy["trade_date"] if strategy is not None else None,
    )
    name = _first_text(
        candidate["name"] if candidate is not None else None,
        strategy["name"] if strategy is not None else None,
    )
    if trade_date is None or name is None:
        raise ValueError(f"risk context incomplete for candidate: {normalized_candidate_id}")

    context_row = _candidate_context_row(connection, normalized_candidate_id)
    candidate_context = _candidate_context_row_to_dict(context_row)
    tick = _latest_tick_row(connection, normalized_code)
    bars = {
        interval: _latest_bar_row(connection, normalized_code, interval)
        for interval in (60, 180, 300)
    }
    theme = _theme_context_row(
        connection,
        candidate["theme_id"] if candidate is not None else None,
        normalized_code,
    )
    setups = _strategy_setup_rows(
        connection,
        strategy["strategy_observation_id"] if strategy else None,
    )

    readiness = candidate_context.get("readiness", {}) if candidate_context else {}
    market_context = candidate_context.get("market_context", {}) if candidate_context else {}
    source_context = candidate_context.get("source_context", {}) if candidate_context else {}
    market_regime = _dict_or_empty(
        market_context.get("market_regime") if market_context else None
    )
    latest_tick_from_context = market_context.get("latest_tick") if market_context else None
    tick_source = (
        _row_to_dict(tick) if tick is not None else _dict_or_empty(latest_tick_from_context)
    )
    latest_1m = _row_to_dict(bars[60]) if bars[60] is not None else {}
    latest_3m = _row_to_dict(bars[180]) if bars[180] is not None else {}
    theme_member = theme.get("member", {})
    latest_snapshot = theme.get("latest_snapshot", {})

    price = _first_number(tick_source.get("price"), theme_member.get("price"))
    vwap = _first_number(latest_1m.get("vwap"), theme_member.get("vwap"))
    tick_age = _first_number(
        candidate["tick_age_sec"] if candidate is not None else None,
        readiness.get("tick_age_sec"),
        tick_age_seconds(tick_source["event_ts"]) if tick_source.get("event_ts") else None,
    )
    strategy_reasons = _strategy_reason_codes(strategy, setups)
    reason_codes = _merge_reasons(
        [
            *strategy_reasons,
            *(_json_load_array(candidate["reason_codes_json"]) if candidate is not None else []),
        ]
    )
    raw_context = {
        "candidate": _candidate_row_to_dict(candidate) if candidate is not None else {},
        "candidate_missing": candidate is None,
        "candidate_context": candidate_context,
        "market_regime": market_regime,
        "strategy_observation": _strategy_row_to_dict(strategy) if strategy is not None else {},
        "strategy_setup_observations": setups,
        "latest_tick": tick_source,
        "latest_bars": {
            "60": latest_1m,
            "180": latest_3m,
            "300": _row_to_dict(bars[300]) if bars[300] is not None else {},
        },
        "theme_latest_snapshot": latest_snapshot,
        "theme_snapshot_member": theme_member,
        "settings": {
            "config_version": resolved_settings.risk_gate_config_version,
            "observe_only": True,
        },
    }
    raw_context["context_hash"] = _context_hash(raw_context)

    return RiskInputContext(
        candidate_instance_id=normalized_candidate_id,
        strategy_observation_id=(
            strategy["strategy_observation_id"] if strategy is not None else None
        ),
        trade_date=trade_date,
        code=normalized_code,
        name=name,
        candidate_state=candidate["state"] if candidate is not None else None,
        strategy_status=strategy["overall_status"] if strategy is not None else None,
        strategy_evaluated_at=strategy["evaluated_at"] if strategy is not None else None,
        primary_setup_type=strategy["primary_setup_type"] if strategy is not None else None,
        score=_first_number(strategy["score"] if strategy is not None else None) or 0.0,
        confidence=_first_number(strategy["confidence"] if strategy is not None else None) or 0.0,
        theme_id=_first_text(
            latest_snapshot.get("theme_id"),
            candidate["theme_id"] if candidate else None,
        ),
        theme_name=_first_text(
            latest_snapshot.get("theme_name"),
            candidate["theme_name"] if candidate else None,
        ),
        theme_state=_first_text(
            latest_snapshot.get("state"),
            candidate["theme_state"] if candidate else None,
        ),
        theme_role=_first_text(
            theme_member.get("member_role"),
            candidate["theme_role"] if candidate else None,
        ),
        theme_fresh_coverage_ratio=_first_number(
            latest_snapshot.get("fresh_coverage_ratio"),
            _dict_or_empty(candidate_context.get("theme_context")).get("fresh_coverage_ratio"),
        ),
        theme_rising_ratio=_first_number(
            latest_snapshot.get("rising_ratio"),
            _dict_or_empty(candidate_context.get("theme_context")).get("rising_ratio"),
        ),
        market_readiness_status=_first_text(
            candidate["market_readiness_status"] if candidate is not None else None,
            readiness.get("quality_status"),
            tick_source.get("quality_status"),
        ),
        market_regime_status=_first_text(market_regime.get("regime_status")),
        market_regime_quality_status=_first_text(market_regime.get("quality_status")),
        primary_index_code=_first_text(market_regime.get("primary_index_code")),
        secondary_index_code=_first_text(market_regime.get("secondary_index_code")),
        primary_index_return_5m=_first_number(market_regime.get("primary_return_5m")),
        primary_index_drawdown_15m=_first_number(market_regime.get("primary_drawdown_15m")),
        secondary_index_return_5m=_first_number(market_regime.get("secondary_return_5m")),
        secondary_index_drawdown_15m=_first_number(
            market_regime.get("secondary_drawdown_15m")
        ),
        tick_age_sec=tick_age,
        price=price,
        change_rate=_first_number(tick_source.get("change_rate"), theme_member.get("change_rate")),
        day_high=_positive_or_none(_first_number(tick_source.get("day_high"))),
        spread_ticks=_first_int(tick_source.get("spread_ticks")),
        cumulative_trade_value=_first_number(
            tick_source.get("cumulative_trade_value"),
            theme_member.get("cumulative_trade_value"),
        ),
        trade_value_delta_1m=_first_number(
            latest_1m.get("trade_value_delta"),
            theme_member.get("trade_value_delta_1m"),
        ),
        trade_value_delta_3m=_first_number(
            latest_3m.get("trade_value_delta"),
            theme_member.get("trade_value_delta_3m"),
        ),
        execution_strength=_first_number(
            tick_source.get("execution_strength"),
            theme_member.get("execution_strength"),
        ),
        vwap=vwap,
        above_vwap=bool(price is not None and vwap is not None and price >= vwap),
        source_count=int(
            _first_number(
                candidate["source_count"] if candidate is not None else None,
                source_context.get("source_count"),
                0,
            )
            or 0
        ),
        active_source_count=int(
            _first_number(
                candidate["active_source_count"] if candidate is not None else None,
                source_context.get("active_source_count"),
                0,
            )
            or 0
        ),
        bar_1m_ready=_bool_first(
            candidate["bar_1m_ready"] if candidate is not None else None,
            readiness.get("has_1m_bar"),
            bars[60] is not None,
        ),
        reason_codes=reason_codes,
        raw_context=raw_context,
    )


def evaluate_risk_for_strategy_observation(
    connection: sqlite3.Connection,
    strategy_observation_id: str,
    settings: Settings | None = None,
) -> RiskObservation:
    resolved_settings = settings or load_settings()
    context = load_risk_input_context(
        connection,
        strategy_observation_id=strategy_observation_id,
        settings=resolved_settings,
    )
    return _evaluate_context(connection, context, resolved_settings)


def evaluate_risk_for_candidate(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings | None = None,
) -> RiskObservation:
    resolved_settings = settings or load_settings()
    context = load_risk_input_context(
        connection,
        candidate_instance_id=candidate_instance_id,
        settings=resolved_settings,
    )
    return _evaluate_context(connection, context, resolved_settings)


def check_data_quality(context: RiskInputContext, settings: Settings) -> RiskCheckObservation:
    reasons: list[str] = []
    severity = RiskSeverity.INFO
    status = RiskCheckStatus.PASS_OBSERVED
    evidence = {
        "latest_tick_present": bool(context.raw_context.get("latest_tick")),
        "price": context.price,
        "tick_age_sec": context.tick_age_sec,
        "stale_tick_sec": settings.risk_gate_stale_tick_sec,
        "market_readiness_status": context.market_readiness_status,
        "bar_1m_ready": context.bar_1m_ready,
        "vwap": context.vwap,
    }
    if not context.raw_context.get("latest_tick") or context.price is None:
        return _check(
            RiskCategory.DATA_QUALITY,
            RiskCheckStatus.BLOCK_OBSERVED,
            RiskSeverity.HIGH,
            [RiskReasonCode.LATEST_TICK_MISSING],
            "Latest tick is missing for risk observation.",
            evidence,
        )
    if (
        context.tick_age_sec is not None
        and context.tick_age_sec > settings.risk_gate_stale_tick_sec
    ):
        reasons.append(RiskReasonCode.TICK_STALE.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.HIGH

    readiness = str(context.market_readiness_status or "").upper()
    if readiness in {"", "MISSING"}:
        reasons.append(RiskReasonCode.MARKET_READINESS_MISSING.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.HIGH)
    elif readiness == "INVALID":
        reasons.append(RiskReasonCode.MARKET_READINESS_INVALID.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.HIGH)
    elif readiness in {"STALE", "DEGRADED"} and status is RiskCheckStatus.PASS_OBSERVED:
        reasons.append(RiskReasonCode.MARKET_READINESS_STALE.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.MEDIUM)

    if not context.bar_1m_ready and status is not RiskCheckStatus.BLOCK_OBSERVED:
        reasons.append(RiskReasonCode.BAR_1M_MISSING.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.MEDIUM)
    if context.vwap is None and status is not RiskCheckStatus.BLOCK_OBSERVED:
        reasons.append(RiskReasonCode.VWAP_MISSING.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.LOW)

    if not reasons:
        reasons.append(RiskReasonCode.OBSERVE_ONLY.value)
    return _check(
        RiskCategory.DATA_QUALITY,
        status,
        severity,
        reasons,
        "Data quality risk observed.",
        evidence,
    )


def check_candidate_context(context: RiskInputContext, settings: Settings) -> RiskCheckObservation:
    del settings
    evidence = {
        "candidate_state": context.candidate_state,
        "source_count": context.source_count,
        "active_source_count": context.active_source_count,
        "candidate_missing": context.raw_context.get("candidate_missing", False),
    }
    if context.raw_context.get("candidate_missing"):
        return _check(
            RiskCategory.CANDIDATE_CONTEXT,
            RiskCheckStatus.BLOCK_OBSERVED,
            RiskSeverity.CRITICAL,
            [RiskReasonCode.CANDIDATE_NOT_CONTEXT_READY],
            "Candidate context is missing.",
            evidence,
        )

    state = str(context.candidate_state or "").upper()
    reasons: list[str] = []
    status = RiskCheckStatus.PASS_OBSERVED
    severity = RiskSeverity.INFO
    if state == CandidateState.CLOSED.value:
        reasons.append(RiskReasonCode.CANDIDATE_CLOSED.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.HIGH
    elif state == CandidateState.STALE.value:
        reasons.append(RiskReasonCode.CANDIDATE_STALE.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.HIGH
    elif state == CandidateState.DATA_WAIT.value:
        reasons.append(RiskReasonCode.CANDIDATE_NOT_CONTEXT_READY.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.HIGH
    elif state != CandidateState.CONTEXT_READY.value:
        reasons.append(RiskReasonCode.CANDIDATE_NOT_CONTEXT_READY.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = RiskSeverity.MEDIUM
    if "CONDITION_RISK_BLOCKED" in set(context.reason_codes):
        reasons.append(RiskReasonCode.CONDITION_RISK_BLOCKED.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.HIGH)
    if context.active_source_count <= 0:
        reasons.append(RiskReasonCode.ACTIVE_SOURCE_MISSING.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.HIGH)
    if not reasons:
        reasons.append(RiskReasonCode.OBSERVE_ONLY.value)
    return _check(
        RiskCategory.CANDIDATE_CONTEXT,
        status,
        severity,
        reasons,
        "Candidate context risk observed.",
        evidence,
    )


def check_strategy_context(context: RiskInputContext, settings: Settings) -> RiskCheckObservation:
    evidence = {
        "strategy_observation_id": context.strategy_observation_id,
        "strategy_status": context.strategy_status,
        "strategy_evaluated_at": (
            datetime_to_wire(context.strategy_evaluated_at)
            if context.strategy_evaluated_at is not None
            else None
        ),
        "strategy_stale_sec": settings.risk_gate_strategy_stale_sec,
        "require_strategy_matched": settings.risk_gate_require_strategy_matched,
    }
    base_reasons = [
        RiskReasonCode.RISK_GATE_NOT_ORDER_APPROVAL.value,
        RiskReasonCode.OBSERVE_PASS_NOT_ORDER_APPROVAL.value,
    ]
    if context.strategy_observation_id is None or context.strategy_status is None:
        return _check(
            RiskCategory.STRATEGY_CONTEXT,
            RiskCheckStatus.DATA_WAIT,
            RiskSeverity.HIGH,
            [RiskReasonCode.STRATEGY_OBSERVATION_MISSING, *base_reasons],
            "Strategy observation is missing.",
            evidence,
        )

    if (
        context.strategy_evaluated_at is not None
        and _age_seconds(context.strategy_evaluated_at) > settings.risk_gate_strategy_stale_sec
    ):
        return _check(
            RiskCategory.STRATEGY_CONTEXT,
            RiskCheckStatus.BLOCK_OBSERVED,
            RiskSeverity.HIGH,
            [RiskReasonCode.STRATEGY_OBSERVATION_STALE, *base_reasons],
            "Strategy observation is stale.",
            evidence,
        )

    status = str(context.strategy_status).upper()
    if status == StrategyObservationStatus.MATCHED_OBSERVATION.value:
        return _check(
            RiskCategory.STRATEGY_CONTEXT,
            RiskCheckStatus.PASS_OBSERVED,
            RiskSeverity.INFO,
            [RiskReasonCode.OBSERVE_ONLY, *base_reasons],
            "Matched strategy observation was read as observe-only input.",
            evidence,
        )
    if status == StrategyObservationStatus.FORMING.value:
        return _check(
            RiskCategory.STRATEGY_CONTEXT,
            RiskCheckStatus.CAUTION_OBSERVED,
            RiskSeverity.MEDIUM,
            [RiskReasonCode.STRATEGY_FORMING_ONLY, *base_reasons],
            "Strategy observation is still forming.",
            evidence,
        )
    if status in {
        StrategyObservationStatus.DATA_WAIT.value,
        StrategyObservationStatus.STALE_CONTEXT.value,
        StrategyObservationStatus.INVALID_CONTEXT.value,
    }:
        return _check(
            RiskCategory.STRATEGY_CONTEXT,
            RiskCheckStatus.BLOCK_OBSERVED,
            RiskSeverity.HIGH,
            [RiskReasonCode.STRATEGY_NOT_MATCHED, *base_reasons],
            "Strategy context is not usable for a clean risk observation.",
            evidence,
        )

    check_status = (
        RiskCheckStatus.BLOCK_OBSERVED
        if settings.risk_gate_require_strategy_matched
        else RiskCheckStatus.CAUTION_OBSERVED
    )
    severity = (
        RiskSeverity.MEDIUM
        if check_status is RiskCheckStatus.BLOCK_OBSERVED
        else RiskSeverity.LOW
    )
    return _check(
        RiskCategory.STRATEGY_CONTEXT,
        check_status,
        severity,
        [RiskReasonCode.STRATEGY_NOT_MATCHED, *base_reasons],
        "Strategy observation is not matched.",
        evidence,
    )


def check_theme_context(context: RiskInputContext, settings: Settings) -> RiskCheckObservation:
    evidence = {
        "theme_id": context.theme_id,
        "theme_state": context.theme_state,
        "theme_role": context.theme_role,
        "fresh_coverage_ratio": context.theme_fresh_coverage_ratio,
        "fresh_coverage_threshold": settings.risk_gate_min_theme_fresh_coverage_ratio,
        "rising_ratio": context.theme_rising_ratio,
        "rising_ratio_threshold": settings.risk_gate_min_theme_rising_ratio,
        "leading_code": context.raw_context.get("theme_latest_snapshot", {}).get("leading_code"),
    }
    if not context.theme_id or not context.theme_state:
        return _check(
            RiskCategory.THEME_CONTEXT,
            RiskCheckStatus.CAUTION_OBSERVED,
            RiskSeverity.MEDIUM,
            [RiskReasonCode.THEME_CONTEXT_MISSING],
            "Theme context is missing.",
            evidence,
        )

    reasons: list[str] = []
    status = RiskCheckStatus.PASS_OBSERVED
    severity = RiskSeverity.INFO
    theme_state = context.theme_state.upper()
    if theme_state == "DATA_WAIT":
        reasons.append(RiskReasonCode.THEME_DATA_WAIT.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.MEDIUM
    elif theme_state in {"FADING", "ROTATED_OUT"}:
        reasons.append(RiskReasonCode.THEME_STATE_WEAK.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.MEDIUM
    elif theme_state == "WATCH":
        reasons.append(RiskReasonCode.THEME_STATE_WEAK.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = RiskSeverity.LOW

    if (
        context.theme_fresh_coverage_ratio is not None
        and context.theme_fresh_coverage_ratio < settings.risk_gate_min_theme_fresh_coverage_ratio
    ):
        reasons.append(RiskReasonCode.THEME_FRESH_COVERAGE_LOW.value)
        status = _caution_if_pass(status)
        severity = _higher_severity(severity, RiskSeverity.MEDIUM)
    if (
        context.theme_rising_ratio is not None
        and context.theme_rising_ratio < settings.risk_gate_min_theme_rising_ratio
    ):
        reasons.append(RiskReasonCode.THEME_RISING_RATIO_WEAK.value)
        status = _caution_if_pass(status)
        severity = _higher_severity(severity, RiskSeverity.LOW)
    if not evidence["leading_code"]:
        reasons.append(RiskReasonCode.THEME_LEADER_MISSING.value)
        status = _caution_if_pass(status)
        severity = _higher_severity(severity, RiskSeverity.LOW)
    if not reasons:
        reasons.append(RiskReasonCode.OBSERVE_ONLY.value)
    return _check(
        RiskCategory.THEME_CONTEXT,
        status,
        severity,
        reasons,
        "Theme context risk observed.",
        evidence,
    )


def check_chase_overheat(context: RiskInputContext, settings: Settings) -> RiskCheckObservation:
    reasons: list[str] = [RiskReasonCode.VI_DATA_UNAVAILABLE.value]
    status = RiskCheckStatus.NOT_EVALUATED
    severity = RiskSeverity.INFO
    evidence = {
        "price": context.price,
        "change_rate": context.change_rate,
        "max_change_rate": settings.risk_gate_max_change_rate,
        "day_high": context.day_high,
        "near_day_high_pct": settings.risk_gate_near_day_high_pct,
        "vwap": context.vwap,
        "max_vwap_extension_pct": settings.risk_gate_max_vwap_extension_pct,
        "vi_data_available": False,
    }
    if context.change_rate is not None and context.change_rate > settings.risk_gate_max_change_rate:
        reasons.append(RiskReasonCode.CHANGE_RATE_OVERHEAT.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.HIGH
    if context.price is not None and context.day_high is not None and context.day_high > 0:
        near_high_pct = max((context.day_high - context.price) / context.day_high * 100.0, 0.0)
        evidence["distance_from_day_high_pct"] = near_high_pct
        if near_high_pct <= settings.risk_gate_near_day_high_pct:
            reasons.append(RiskReasonCode.PRICE_NEAR_HIGH.value)
            status = _caution_if_not_block(status)
            severity = _higher_severity(severity, RiskSeverity.LOW)
    if context.price is not None and context.vwap is not None and context.vwap > 0:
        extension_pct = (context.price - context.vwap) / context.vwap * 100.0
        evidence["vwap_extension_pct"] = extension_pct
        if extension_pct > settings.risk_gate_max_vwap_extension_pct:
            reasons.append(RiskReasonCode.VWAP_EXTENSION_HIGH.value)
            status = RiskCheckStatus.BLOCK_OBSERVED
            severity = _higher_severity(severity, RiskSeverity.MEDIUM)
    if "PULLBACK_TOO_SHALLOW" in set(context.reason_codes):
        reasons.append(RiskReasonCode.PULLBACK_TOO_SHALLOW.value)
        status = _caution_if_not_block(status)
        severity = _higher_severity(severity, RiskSeverity.LOW)
    return _check(
        RiskCategory.CHASE_OVERHEAT,
        status,
        severity,
        _merge_reasons(reasons),
        "Chase and overheat risk observed.",
        evidence,
    )


def check_liquidity_spread(context: RiskInputContext, settings: Settings) -> RiskCheckObservation:
    reasons: list[str] = []
    status = RiskCheckStatus.PASS_OBSERVED
    severity = RiskSeverity.INFO
    evidence = {
        "spread_ticks": context.spread_ticks,
        "max_spread_ticks": settings.risk_gate_max_spread_ticks,
        "trade_value_delta_1m": context.trade_value_delta_1m,
        "min_trade_value_delta_1m": settings.risk_gate_min_trade_value_delta_1m,
        "cumulative_trade_value": context.cumulative_trade_value,
        "min_cumulative_trade_value": settings.risk_gate_min_cumulative_trade_value,
        "execution_strength": context.execution_strength,
        "min_execution_strength": settings.risk_gate_min_execution_strength,
    }
    if (
        context.spread_ticks is not None
        and context.spread_ticks > settings.risk_gate_max_spread_ticks
    ):
        reasons.append(RiskReasonCode.SPREAD_TOO_WIDE.value)
        status = RiskCheckStatus.BLOCK_OBSERVED
        severity = RiskSeverity.MEDIUM
    if (
        settings.risk_gate_min_trade_value_delta_1m > 0
        and (
            context.trade_value_delta_1m is None
            or context.trade_value_delta_1m < settings.risk_gate_min_trade_value_delta_1m
        )
    ):
        reasons.append(RiskReasonCode.TRADE_VALUE_FLOW_WEAK.value)
        status = _caution_if_not_block(status)
        severity = _higher_severity(severity, RiskSeverity.LOW)
    if (
        settings.risk_gate_min_cumulative_trade_value > 0
        and (
            context.cumulative_trade_value is None
            or context.cumulative_trade_value < settings.risk_gate_min_cumulative_trade_value
        )
    ):
        reasons.append(RiskReasonCode.CUMULATIVE_TRADE_VALUE_LOW.value)
        status = _caution_if_not_block(status)
        severity = _higher_severity(severity, RiskSeverity.MEDIUM)
    if (
        settings.risk_gate_min_execution_strength > 0
        and (
            context.execution_strength is None
            or context.execution_strength < settings.risk_gate_min_execution_strength
        )
    ):
        reasons.append(RiskReasonCode.EXECUTION_STRENGTH_WEAK.value)
        status = _caution_if_not_block(status)
        severity = _higher_severity(severity, RiskSeverity.LOW)
    if not reasons:
        reasons.append(RiskReasonCode.OBSERVE_ONLY.value)
    return _check(
        RiskCategory.LIQUIDITY_SPREAD,
        status,
        severity,
        reasons,
        "Liquidity and spread risk observed.",
        evidence,
    )


def check_duplicate_cooldown(
    connection: sqlite3.Connection,
    context: RiskInputContext,
    settings: Settings,
) -> RiskCheckObservation:
    active_count = _active_candidate_count(connection, context.trade_date, context.code)
    recent_observation = _recent_observation_row(connection, context, settings)
    reasons: list[str] = []
    status = RiskCheckStatus.PASS_OBSERVED
    severity = RiskSeverity.INFO
    evidence = {
        "active_candidate_count": active_count,
        "duplicate_active_candidate_limit": settings.risk_gate_duplicate_active_candidate_limit,
        "cooldown_sec": settings.risk_gate_observation_cooldown_sec,
        "recent_observation": (
            _row_to_dict(recent_observation) if recent_observation is not None else None
        ),
    }
    if active_count > settings.risk_gate_duplicate_active_candidate_limit:
        reasons.append(RiskReasonCode.DUPLICATE_ACTIVE_CANDIDATE.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = RiskSeverity.MEDIUM
    if recent_observation is not None:
        reasons.append(RiskReasonCode.RECENT_OBSERVATION_COOLDOWN.value)
        status = _caution_if_pass(status)
        severity = _higher_severity(severity, RiskSeverity.LOW)
    if not reasons:
        reasons.append(RiskReasonCode.OBSERVE_ONLY.value)
    return _check(
        RiskCategory.DUPLICATE_COOLDOWN,
        status,
        severity,
        reasons,
        "Duplicate and cooldown risk observed.",
        evidence,
    )


def check_market_regime(
    context: RiskInputContext,
    settings: Settings,
) -> RiskCheckObservation:
    market_regime = _dict_or_empty(context.raw_context.get("market_regime"))
    evidence = {
        "market_regime": market_regime,
        "regime_status": context.market_regime_status,
        "quality_status": context.market_regime_quality_status,
        "primary_index_code": context.primary_index_code,
        "secondary_index_code": context.secondary_index_code,
        "primary_index_return_5m": context.primary_index_return_5m,
        "primary_index_drawdown_15m": context.primary_index_drawdown_15m,
        "secondary_index_return_5m": context.secondary_index_return_5m,
        "secondary_index_drawdown_15m": context.secondary_index_drawdown_15m,
        "risk_off_return_5m": settings.market_regime_risk_off_return_5m,
        "risk_off_drawdown_15m": settings.market_regime_risk_off_drawdown_15m,
    }
    if not market_regime:
        if context.market_regime_status is None and context.market_regime_quality_status is None:
            return _check(
                RiskCategory.MARKET_CONTEXT,
                RiskCheckStatus.NOT_EVALUATED,
                RiskSeverity.INFO,
                [RiskReasonCode.MARKET_REGIME_MISSING],
                "Market regime context is absent in a legacy observe-only context.",
                evidence,
            )
        return _check(
            RiskCategory.MARKET_CONTEXT,
            RiskCheckStatus.CAUTION_OBSERVED,
            RiskSeverity.MEDIUM,
            [RiskReasonCode.MARKET_REGIME_MISSING],
            "Market regime context is missing; observe-only caution recorded.",
            evidence,
        )

    source_reasons = {
        str(reason).upper() for reason in market_regime.get("reason_codes", [])
    }
    regime_status = str(context.market_regime_status or "").upper()
    quality_status = str(context.market_regime_quality_status or "").upper()
    reasons: list[str] = []
    status = RiskCheckStatus.PASS_OBSERVED
    severity = RiskSeverity.INFO

    if "MARKET_INDEX_STALE" in source_reasons or quality_status in {"STALE", "DEGRADED"}:
        reasons.append(RiskReasonCode.MARKET_INDEX_STALE.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.MEDIUM)
    if regime_status in {"", "DATA_WAIT"} or quality_status in {"", "MISSING"}:
        reasons.append(RiskReasonCode.MARKET_REGIME_MISSING.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.MEDIUM)
    elif regime_status == "RISK_OFF":
        if "SECONDARY_INDEX_RISK_OFF" in source_reasons:
            reasons.append(RiskReasonCode.SECONDARY_INDEX_RISK_OFF.value)
        else:
            reasons.append(RiskReasonCode.PRIMARY_INDEX_RISK_OFF.value)
        if _intraday_shock_observed(context, settings):
            reasons.append(RiskReasonCode.MARKET_INDEX_INTRADAY_SHOCK.value)
        if quality_status == "FRESH":
            status = RiskCheckStatus.BLOCK_OBSERVED
            severity = RiskSeverity.HIGH
        else:
            status = RiskCheckStatus.CAUTION_OBSERVED
            severity = _higher_severity(severity, RiskSeverity.MEDIUM)
    elif regime_status == "WEAK":
        reasons.append(RiskReasonCode.MARKET_REGIME_WEAK.value)
        status = RiskCheckStatus.CAUTION_OBSERVED
        severity = _higher_severity(severity, RiskSeverity.MEDIUM)

    if not reasons:
        reasons.append(RiskReasonCode.OBSERVE_ONLY.value)
    return _check(
        RiskCategory.MARKET_CONTEXT,
        status,
        severity,
        reasons,
        "Market regime risk observed.",
        evidence,
    )


def check_portfolio_placeholder(
    context: RiskInputContext,
    settings: Settings,
) -> RiskCheckObservation:
    del context, settings
    return _check(
        RiskCategory.PORTFOLIO_PLACEHOLDER,
        RiskCheckStatus.NOT_EVALUATED,
        RiskSeverity.INFO,
        [RiskReasonCode.PORTFOLIO_CONTEXT_UNAVAILABLE],
        "Portfolio context is not available before the later OMS phase.",
        {"portfolio_service_available": False},
    )


def check_account_limits(
    connection: sqlite3.Connection,
    context: RiskInputContext,
    settings: Settings,
) -> RiskCheckObservation:
    reasons: list[str] = []
    evidence = {
        "portfolio_service_available": True,
        "trade_date": context.trade_date,
        "code": context.code,
        "price": context.price,
        "dry_run": _dry_run_account_limit_evidence(connection, context, settings),
        "live_sim": _live_sim_account_limit_evidence(connection, context, settings),
        "observe_only": True,
        "not_order_approval": True,
    }

    dry_run = evidence["dry_run"]
    if dry_run["estimated_order_notional"] > dry_run["max_position_notional"]:
        reasons.append(RiskReasonCode.MAX_ORDER_NOTIONAL_EXCEEDED.value)
    if dry_run["daily_intent_count"] >= dry_run["max_daily_intents"]:
        reasons.append(RiskReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value)
    if dry_run["active_position_count"] >= dry_run["max_active_positions"]:
        reasons.append(RiskReasonCode.ACTIVE_POSITION_LIMIT_EXCEEDED.value)
    if dry_run["code_open_position_count"] > 0:
        reasons.append(RiskReasonCode.CODE_CONCENTRATION_LIMIT_EXCEEDED.value)
    if dry_run["projected_total_exposure"] > dry_run["max_total_exposure"]:
        reasons.append(RiskReasonCode.TOTAL_EXPOSURE_LIMIT_EXCEEDED.value)

    live_sim = evidence["live_sim"]
    if live_sim["kill_switch_applicable"] and live_sim["kill_switch_active"]:
        reasons.append(RiskReasonCode.ACCOUNT_KILL_SWITCH_ACTIVE.value)
    if live_sim["estimated_order_notional"] > live_sim["max_order_notional"]:
        reasons.append(RiskReasonCode.MAX_ORDER_NOTIONAL_EXCEEDED.value)
    if live_sim["daily_order_count"] >= live_sim["max_daily_order_count"]:
        reasons.append(RiskReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value)
    if live_sim["projected_daily_notional"] > live_sim["max_daily_notional"]:
        reasons.append(RiskReasonCode.DAILY_NOTIONAL_LIMIT_EXCEEDED.value)
    if live_sim["daily_loss_limit_exceeded"]:
        reasons.append(RiskReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value)
    if live_sim["active_order_count"] >= live_sim["max_active_orders"]:
        reasons.append(RiskReasonCode.ACTIVE_ORDER_LIMIT_EXCEEDED.value)
    if live_sim["active_position_count"] >= live_sim["max_active_positions"]:
        reasons.append(RiskReasonCode.ACTIVE_POSITION_LIMIT_EXCEEDED.value)
    if live_sim["code_open_position_count"] > 0 and not live_sim["scale_in_allowed"]:
        reasons.append(RiskReasonCode.CODE_CONCENTRATION_LIMIT_EXCEEDED.value)
    if live_sim["projected_total_exposure"] > live_sim["max_total_exposure"]:
        reasons.append(RiskReasonCode.TOTAL_EXPOSURE_LIMIT_EXCEEDED.value)

    reasons = _merge_reasons(reasons)
    if not reasons:
        return _check(
            RiskCategory.ACCOUNT_LIMITS,
            RiskCheckStatus.PASS_OBSERVED,
            RiskSeverity.INFO,
            [RiskReasonCode.OBSERVE_ONLY],
            "Account-level limits observed.",
            evidence,
        )

    severity = (
        RiskSeverity.CRITICAL
        if (
            RiskReasonCode.ACCOUNT_KILL_SWITCH_ACTIVE.value in reasons
            or RiskReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value in reasons
        )
        else RiskSeverity.HIGH
    )
    return _check(
        RiskCategory.ACCOUNT_LIMITS,
        RiskCheckStatus.BLOCK_OBSERVED,
        severity,
        reasons,
        "Account-level limit breach observed.",
        evidence,
    )


def _dry_run_account_limit_evidence(
    connection: sqlite3.Connection,
    context: RiskInputContext,
    settings: Settings,
) -> dict[str, Any]:
    estimated_notional = _estimated_notional(
        context.price,
        target_notional=settings.dry_run_default_position_notional,
        max_notional=settings.dry_run_max_position_notional,
        min_quantity=settings.dry_run_min_quantity,
    )
    active_position_count = _count_rows_where(
        connection,
        "dry_run_positions",
        "status IN ({statuses})",
        statuses=ACTIVE_DRY_RUN_POSITION_STATUSES,
    )
    code_open_position_count = _count_rows_where(
        connection,
        "dry_run_positions",
        "trade_date = ? AND code = ? AND status IN ({statuses})",
        context.trade_date,
        context.code,
        statuses=ACTIVE_DRY_RUN_POSITION_STATUSES,
    )
    exposure = _sum_rows_where(
        connection,
        "dry_run_positions",
        "invested_notional",
        "status IN ({statuses})",
        statuses=ACTIVE_DRY_RUN_POSITION_STATUSES,
    )
    realized_pnl = _sum_rows_where(
        connection,
        "dry_run_positions",
        "realized_pnl",
        "status IN ({statuses})",
        statuses=ACTIVE_DRY_RUN_POSITION_STATUSES,
    )
    unrealized_pnl = _sum_rows_where(
        connection,
        "dry_run_positions",
        "unrealized_pnl",
        "status IN ({statuses})",
        statuses=ACTIVE_DRY_RUN_POSITION_STATUSES,
    )
    max_total_exposure = (
        settings.dry_run_max_active_positions * settings.dry_run_max_position_notional
    )
    return {
        "enabled": settings.dry_run_oms_enabled,
        "intent_creation_enabled": settings.dry_run_intent_creation_enabled,
        "estimated_order_notional": estimated_notional,
        "max_position_notional": settings.dry_run_max_position_notional,
        "daily_intent_count": _count_rows_where(
            connection,
            "dry_run_intents",
            "trade_date = ?",
            context.trade_date,
        ),
        "max_daily_intents": settings.dry_run_max_daily_intents,
        "active_intent_count": _count_rows_where(
            connection,
            "dry_run_intents",
            "status IN ({statuses})",
            statuses=ACTIVE_DRY_RUN_INTENT_STATUSES,
        ),
        "active_order_count": _count_rows_where(
            connection,
            "dry_run_orders",
            "status IN ({statuses})",
            statuses=ACTIVE_DRY_RUN_ORDER_STATUSES,
        ),
        "active_position_count": active_position_count,
        "max_active_positions": settings.dry_run_max_active_positions,
        "code_open_position_count": code_open_position_count,
        "current_total_exposure": exposure,
        "projected_total_exposure": exposure + estimated_notional,
        "max_total_exposure": max_total_exposure,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
    }


def _live_sim_account_limit_evidence(
    connection: sqlite3.Connection,
    context: RiskInputContext,
    settings: Settings,
) -> dict[str, Any]:
    dry_run_notional = _latest_dry_run_notional(connection, context.candidate_instance_id)
    target_notional = settings.live_sim_max_order_notional
    if dry_run_notional is not None:
        target_notional = min(target_notional, dry_run_notional)
    estimated_notional = _estimated_notional(
        context.price,
        target_notional=target_notional,
        max_notional=settings.live_sim_max_order_notional,
        min_quantity=1,
    )
    active_position_count = _live_sim_active_position_count(connection)
    code_open_position_count = _live_sim_open_position_count_for_code(connection, context.code)
    active_order_count = _count_rows_where(
        connection,
        "live_sim_orders",
        "status IN ({statuses})",
        statuses=ACTIVE_LIVE_SIM_ORDER_STATUSES,
    )
    daily_notional = _sum_rows_where(
        connection,
        "live_sim_orders",
        "notional",
        "trade_date = ?",
        context.trade_date,
    )
    exposure = _live_sim_total_exposure(connection)
    daily_loss = build_live_sim_daily_loss_evidence(
        connection,
        trade_date=context.trade_date,
        settings=settings,
    )
    live_sim_rows_present = bool(
        active_order_count
        or active_position_count
        or _count_rows_where(connection, "live_sim_orders", "trade_date = ?", context.trade_date)
    )
    kill_switch_applicable = bool(
        settings.live_sim_enabled
        or settings.live_sim_allowed
        or settings.trading_mode.value == "LIVE_SIM"
        or live_sim_rows_present
    )
    max_total_exposure = (
        settings.live_sim_max_active_positions * settings.live_sim_max_order_notional
    )
    return {
        "enabled": settings.live_sim_enabled,
        "allowed": settings.live_sim_allowed,
        "trading_mode": settings.trading_mode.value,
        "kill_switch_active": settings.live_sim_kill_switch,
        "kill_switch_applicable": kill_switch_applicable,
        "scale_in_allowed": settings.live_sim_position_allow_scale_in,
        "dry_run_notional": dry_run_notional,
        "estimated_order_notional": estimated_notional,
        "max_order_notional": settings.live_sim_max_order_notional,
        "daily_order_count": _count_rows_where(
            connection,
            "live_sim_orders",
            "trade_date = ?",
            context.trade_date,
        ),
        "max_daily_order_count": settings.live_sim_max_daily_order_count,
        "daily_notional": daily_notional,
        "projected_daily_notional": daily_notional + estimated_notional,
        "max_daily_notional": settings.live_sim_max_daily_notional,
        "active_order_count": active_order_count,
        "max_active_orders": settings.live_sim_max_active_orders,
        "active_position_count": active_position_count,
        "max_active_positions": settings.live_sim_max_active_positions,
        "code_open_position_count": code_open_position_count,
        "current_total_exposure": exposure,
        "projected_total_exposure": exposure + estimated_notional,
        "max_total_exposure": max_total_exposure,
        "realized_pnl": daily_loss["realized_pnl"],
        "unrealized_pnl": daily_loss["unrealized_pnl"],
        "daily_pnl": daily_loss["daily_pnl"],
        "daily_loss": daily_loss["daily_loss"],
        "max_daily_loss": daily_loss["max_daily_loss"],
        "max_daily_loss_pct": daily_loss["max_daily_loss_pct"],
        "pct_loss_limit": daily_loss["pct_loss_limit"],
        "effective_daily_loss_limit": daily_loss["effective_loss_limit"],
        "daily_loss_limit_enabled": daily_loss["daily_loss_limit_enabled"],
        "daily_loss_limit_exceeded": daily_loss["daily_loss_limit_exceeded"],
    }


def _estimated_notional(
    price: float | None,
    *,
    target_notional: float,
    max_notional: float,
    min_quantity: int,
) -> float:
    if price is None or price <= 0:
        return 0.0
    bounded_target = min(float(target_notional), float(max_notional))
    quantity = int(bounded_target // float(price))
    if quantity < min_quantity:
        return float(price * quantity)
    notional = float(price * quantity)
    if notional > max_notional:
        quantity = int(float(max_notional) // float(price))
        notional = float(price * quantity)
    return notional


def _latest_dry_run_notional(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> float | None:
    row = connection.execute(
        """
        SELECT notional
        FROM dry_run_intents
        WHERE candidate_instance_id = ?
        ORDER BY created_at DESC, dry_run_intent_id DESC
        LIMIT 1
        """,
        (candidate_instance_id,),
    ).fetchone()
    if row is None or row["notional"] is None:
        return None
    return float(row["notional"])


def _live_sim_active_position_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(DISTINCT code) AS count
        FROM (
            SELECT code
            FROM live_sim_positions
            WHERE status IN ({_placeholders(ACTIVE_LIVE_SIM_POSITION_STATUSES)})
                AND quantity > 0
            UNION
            SELECT code
            FROM live_sim_orders
            WHERE side = 'BUY'
                AND status IN ({_placeholders(LIVE_SIM_POSITION_ORDER_STATUSES)})
        )
        """,
        (*ACTIVE_LIVE_SIM_POSITION_STATUSES, *LIVE_SIM_POSITION_ORDER_STATUSES),
    ).fetchone()
    return int(row["count"] or 0)


def _live_sim_open_position_count_for_code(
    connection: sqlite3.Connection,
    code: str,
) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM (
            SELECT position_id AS id
            FROM live_sim_positions
            WHERE code = ?
                AND status IN ({_placeholders(ACTIVE_LIVE_SIM_POSITION_STATUSES)})
                AND quantity > 0
            UNION
            SELECT live_sim_order_id AS id
            FROM live_sim_orders
            WHERE code = ?
                AND side = 'BUY'
                AND status IN ({_placeholders(LIVE_SIM_POSITION_ORDER_STATUSES)})
        )
        """,
        (
            context_code := validate_stock_code(code),
            *ACTIVE_LIVE_SIM_POSITION_STATUSES,
            context_code,
            *LIVE_SIM_POSITION_ORDER_STATUSES,
        ),
    ).fetchone()
    return int(row["count"] or 0)


def _live_sim_total_exposure(connection: sqlite3.Connection) -> float:
    position_total = _sum_rows_where(
        connection,
        "live_sim_positions",
        "total_entry_notional",
        "status IN ({statuses}) AND quantity > 0",
        statuses=ACTIVE_LIVE_SIM_POSITION_STATUSES,
    )
    filled_order_total = _sum_rows_where(
        connection,
        "live_sim_orders",
        "notional",
        "side = 'BUY' AND status IN ({statuses})",
        statuses=LIVE_SIM_POSITION_ORDER_STATUSES,
    )
    return position_total + filled_order_total


def _count_rows_where(
    connection: sqlite3.Connection,
    table_name: str,
    where_sql: str,
    *params: Any,
    statuses: Sequence[str] = (),
) -> int:
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM {table_name}
        WHERE {where_sql.format(statuses=_placeholders(statuses))}
        """,
        (*params, *statuses),
    ).fetchone()
    return int(row["count"] or 0)


def _sum_rows_where(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    where_sql: str,
    *params: Any,
    statuses: Sequence[str] = (),
) -> float:
    row = connection.execute(
        f"""
        SELECT COALESCE(SUM({column_name}), 0) AS total
        FROM {table_name}
        WHERE {where_sql.format(statuses=_placeholders(statuses))}
        """,
        (*params, *statuses),
    ).fetchone()
    return float(row["total"] or 0.0)


def _placeholders(values: Sequence[str]) -> str:
    return ",".join("?" for _ in values)


def _intraday_shock_observed(context: RiskInputContext, settings: Settings) -> bool:
    return (
        context.primary_index_return_5m is not None
        and context.primary_index_return_5m <= settings.market_regime_risk_off_return_5m
    ) or (
        context.primary_index_drawdown_15m is not None
        and context.primary_index_drawdown_15m <= settings.market_regime_risk_off_drawdown_15m
    )


def save_risk_observation(
    connection: sqlite3.Connection,
    observation: RiskObservation,
) -> None:
    data = observation.to_dict(include_checks=False)
    connection.execute(
        """
        INSERT INTO risk_observations (
            risk_observation_id,
            candidate_instance_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            max_severity,
            blocked_count,
            caution_count,
            pass_count,
            reason_codes_json,
            evidence_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["risk_observation_id"],
            data["candidate_instance_id"],
            data["strategy_observation_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["evaluated_at"],
            data["overall_status"],
            data["max_severity"],
            data["blocked_count"],
            data["caution_count"],
            data["pass_count"],
            _json_dumps(data["reason_codes"]),
            canonical_json(data["evidence_json"]),
            data["config_version"],
            1 if data["observe_only"] else 0,
        ),
    )
    for check in observation.check_observations:
        check_data = check.to_dict()
        connection.execute(
            """
            INSERT INTO risk_check_observations (
                risk_observation_id,
                candidate_instance_id,
                category,
                status,
                severity,
                reason_codes_json,
                message,
                evidence_json,
                evaluated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation.risk_observation_id,
                observation.candidate_instance_id,
                check_data["category"],
                check_data["status"],
                check_data["severity"],
                _json_dumps(check_data["reason_codes"]),
                check_data["message"],
                canonical_json(check_data["evidence_json"]),
                data["evaluated_at"],
            ),
        )
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id,
            risk_observation_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            max_severity,
            blocked_count,
            caution_count,
            pass_count,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_instance_id) DO UPDATE SET
            risk_observation_id = excluded.risk_observation_id,
            strategy_observation_id = excluded.strategy_observation_id,
            trade_date = excluded.trade_date,
            code = excluded.code,
            name = excluded.name,
            evaluated_at = excluded.evaluated_at,
            overall_status = excluded.overall_status,
            max_severity = excluded.max_severity,
            blocked_count = excluded.blocked_count,
            caution_count = excluded.caution_count,
            pass_count = excluded.pass_count,
            reason_codes_json = excluded.reason_codes_json,
            config_version = excluded.config_version,
            observe_only = excluded.observe_only
        """,
        (
            data["candidate_instance_id"],
            data["risk_observation_id"],
            data["strategy_observation_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["evaluated_at"],
            data["overall_status"],
            data["max_severity"],
            data["blocked_count"],
            data["caution_count"],
            data["pass_count"],
            _json_dumps(data["reason_codes"]),
            data["config_version"],
            1 if data["observe_only"] else 0,
        ),
    )


def evaluate_risk_observations(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    strategy_status: StrategyObservationStatus | str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
    candidate_instance_id: str | None = None,
    strategy_observation_id: str | None = None,
    manage_run_lock: bool = True,
) -> RiskEvaluationRunResult:
    with runtime_execution_lock(
        connection,
        EVALUATION_PIPELINE_LOCK,
        details={"run_type": "risk_evaluation", "trade_date": trade_date},
        manage_lock=manage_run_lock,
    ):
        with immediate_transaction(connection):
            return _evaluate_risk_observations(
                connection,
                trade_date=trade_date,
                strategy_status=strategy_status,
                limit=limit,
                settings=settings,
                candidate_instance_id=candidate_instance_id,
                strategy_observation_id=strategy_observation_id,
            )


def _evaluate_risk_observations(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    strategy_status: StrategyObservationStatus | str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
    candidate_instance_id: str | None = None,
    strategy_observation_id: str | None = None,
) -> RiskEvaluationRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("risk_run")
    started_at = datetime_to_wire(utc_now())
    bounded_limit = _bounded_limit(limit or resolved_settings.risk_gate_max_strategy_observations)
    _insert_run(
        connection,
        run_id=run_id,
        trade_date=trade_date,
        started_at=started_at,
        config_version=resolved_settings.risk_gate_config_version,
        status="RUNNING",
    )
    if not resolved_settings.risk_gate_enabled:
        _complete_run(
            connection,
            run_id=run_id,
            strategy_observation_count=0,
            evaluated_count=0,
            observe_pass_count=0,
            caution_count=0,
            block_count=0,
            data_wait_count=0,
            error_count=0,
            status="DISABLED",
        )
        connection.commit()
        return RiskEvaluationRunResult(
            run_id=run_id,
            trade_date=trade_date,
            status="DISABLED",
            config_version=resolved_settings.risk_gate_config_version,
        )

    targets = _risk_evaluation_targets(
        connection,
        trade_date=trade_date,
        strategy_status=strategy_status,
        limit=bounded_limit,
        candidate_instance_id=candidate_instance_id,
        strategy_observation_id=strategy_observation_id,
    )
    evaluated_count = observe_pass_count = caution_count = block_count = data_wait_count = 0
    error_count = 0
    for target in targets:
        try:
            if target.get("strategy_observation_id"):
                observation = evaluate_risk_for_strategy_observation(
                    connection,
                    target["strategy_observation_id"],
                    settings=resolved_settings,
                )
            else:
                observation = evaluate_risk_for_candidate(
                    connection,
                    target["candidate_instance_id"],
                    settings=resolved_settings,
                )
            save_risk_observation(connection, observation)
            evaluated_count += 1
            if observation.overall_status is RiskObservationStatus.OBSERVE_PASS:
                observe_pass_count += 1
            elif observation.overall_status is RiskObservationStatus.OBSERVE_CAUTION:
                caution_count += 1
            elif observation.overall_status is RiskObservationStatus.OBSERVE_BLOCK:
                block_count += 1
            elif observation.overall_status is RiskObservationStatus.DATA_WAIT:
                data_wait_count += 1
        except Exception as exc:
            error_count += 1
            _record_evaluation_error(
                connection,
                run_id=run_id,
                candidate_instance_id=target.get("candidate_instance_id"),
                strategy_observation_id=target.get("strategy_observation_id"),
                code=target.get("code"),
                error_message=str(exc),
                payload=target,
            )
    status = "COMPLETED_WITH_ERRORS" if error_count else "COMPLETED"
    _complete_run(
        connection,
        run_id=run_id,
        strategy_observation_count=len(targets),
        evaluated_count=evaluated_count,
        observe_pass_count=observe_pass_count,
        caution_count=caution_count,
        block_count=block_count,
        data_wait_count=data_wait_count,
        error_count=error_count,
        status=status,
    )
    connection.commit()
    return RiskEvaluationRunResult(
        run_id=run_id,
        trade_date=trade_date,
        strategy_observation_count=len(targets),
        evaluated_count=evaluated_count,
        observe_pass_count=observe_pass_count,
        caution_count=caution_count,
        block_count=block_count,
        data_wait_count=data_wait_count,
        error_count=error_count,
        config_version=resolved_settings.risk_gate_config_version,
        status=status,
    )


def get_risk_status(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    return {
        "enabled": resolved_settings.risk_gate_enabled,
        "observe_only": True,
        "configured_observe_only": resolved_settings.risk_gate_observe_only,
        "config_version": resolved_settings.risk_gate_config_version,
        "latest_observation_count": _count_rows(connection, "risk_observations_latest"),
        "observe_pass_count": _count_rows(
            connection,
            "risk_observations_latest",
            where="overall_status = 'OBSERVE_PASS'",
        ),
        "caution_count": _count_rows(
            connection,
            "risk_observations_latest",
            where="overall_status = 'OBSERVE_CAUTION'",
        ),
        "block_count": _count_rows(
            connection,
            "risk_observations_latest",
            where="overall_status = 'OBSERVE_BLOCK'",
        ),
        "data_wait_count": _count_rows(
            connection,
            "risk_observations_latest",
            where="overall_status = 'DATA_WAIT'",
        ),
        "error_count": _count_rows(connection, "risk_evaluation_errors"),
        "stale_tick_sec": resolved_settings.risk_gate_stale_tick_sec,
        "strategy_stale_sec": resolved_settings.risk_gate_strategy_stale_sec,
        "order_routing_enabled": False,
    }


def get_latest_risk_observation(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    include_checks: bool = False,
) -> dict[str, Any] | None:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    row = connection.execute(
        """
        SELECT
            l.*,
            o.evidence_json
        FROM risk_observations_latest AS l
        LEFT JOIN risk_observations AS o
            ON o.risk_observation_id = l.risk_observation_id
        WHERE l.candidate_instance_id = ?
        """,
        (normalized_id,),
    ).fetchone()
    if row is None:
        return None
    observation = _latest_observation_row_to_dict(row)
    if include_checks:
        observation["check_observations"] = list_risk_check_observations(
            connection,
            observation["risk_observation_id"],
        )
    return observation


def get_risk_observation(
    connection: sqlite3.Connection,
    risk_observation_id: str,
    *,
    include_checks: bool = True,
) -> dict[str, Any] | None:
    normalized_id = require_non_empty_str(risk_observation_id, "risk_observation_id")
    row = connection.execute(
        """
        SELECT *
        FROM risk_observations
        WHERE risk_observation_id = ?
        """,
        (normalized_id,),
    ).fetchone()
    if row is None:
        return None
    observation = _observation_row_to_dict(row)
    if include_checks:
        observation["check_observations"] = list_risk_check_observations(
            connection,
            observation["risk_observation_id"],
        )
    return observation


def list_latest_risk_observations(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: RiskObservationStatus | str | None = None,
    code: str | None = None,
    max_severity: RiskSeverity | str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("l.trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if status is not None:
        normalized_status = parse_str_enum(status, RiskObservationStatus, "status")
        clauses.append("l.overall_status = ?")
        params.append(normalized_status.value)
    if code is not None:
        clauses.append("l.code = ?")
        params.append(validate_stock_code(code))
    if max_severity is not None:
        normalized_severity = parse_str_enum(max_severity, RiskSeverity, "max_severity")
        clauses.append("l.max_severity = ?")
        params.append(normalized_severity.value)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(_bounded_limit(limit))
    rows = connection.execute(
        f"""
        SELECT
            l.*,
            o.evidence_json
        FROM risk_observations_latest AS l
        LEFT JOIN risk_observations AS o
            ON o.risk_observation_id = l.risk_observation_id
        {where_sql}
        ORDER BY l.evaluated_at DESC, l.code ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_latest_observation_row_to_dict(row) for row in rows]


def list_risk_observations_for_candidate(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    rows = connection.execute(
        """
        SELECT *
        FROM risk_observations
        WHERE candidate_instance_id = ?
        ORDER BY evaluated_at DESC, risk_observation_id DESC
        LIMIT ?
        """,
        (normalized_id, _bounded_limit(limit)),
    ).fetchall()
    return [_observation_row_to_dict(row) for row in rows]


def list_risk_check_observations(
    connection: sqlite3.Connection,
    risk_observation_id: str,
) -> list[dict[str, Any]]:
    normalized_id = require_non_empty_str(risk_observation_id, "risk_observation_id")
    rows = connection.execute(
        """
        SELECT *
        FROM risk_check_observations
        WHERE risk_observation_id = ?
        ORDER BY
            CASE status
                WHEN 'BLOCK_OBSERVED' THEN 0
                WHEN 'DATA_WAIT' THEN 1
                WHEN 'CAUTION_OBSERVED' THEN 2
                WHEN 'PASS_OBSERVED' THEN 3
                ELSE 4
            END,
            category ASC
        """,
        (normalized_id,),
    ).fetchall()
    return [_check_row_to_dict(row) for row in rows]


def list_risk_runs(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM risk_evaluation_runs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def list_risk_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM risk_evaluation_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    errors = []
    for row in rows:
        item = _row_to_dict(row)
        item["payload"] = json.loads(item.pop("payload_json"))
        errors.append(item)
    return errors


def _evaluate_context(
    connection: sqlite3.Connection,
    context: RiskInputContext,
    settings: Settings,
) -> RiskObservation:
    checks = [
        check_data_quality(context, settings),
        check_market_regime(context, settings),
        check_theme_context(context, settings),
        check_candidate_context(context, settings),
        check_strategy_context(context, settings),
        check_chase_overheat(context, settings),
        check_liquidity_spread(context, settings),
        check_duplicate_cooldown(connection, context, settings),
        check_account_limits(connection, context, settings),
    ]
    blocked_count = sum(1 for check in checks if check.status is RiskCheckStatus.BLOCK_OBSERVED)
    caution_count = sum(1 for check in checks if check.status is RiskCheckStatus.CAUTION_OBSERVED)
    pass_count = sum(1 for check in checks if check.status is RiskCheckStatus.PASS_OBSERVED)
    reasons = _merge_reasons(
        [
            *(reason for check in checks for reason in check.reason_codes),
            RiskReasonCode.OBSERVE_ONLY.value,
            RiskReasonCode.RISK_GATE_NOT_ORDER_APPROVAL.value,
        ]
    )
    return RiskObservation(
        risk_observation_id=new_message_id("risk_observation"),
        candidate_instance_id=context.candidate_instance_id,
        strategy_observation_id=context.strategy_observation_id,
        trade_date=context.trade_date,
        code=context.code,
        name=context.name,
        evaluated_at=datetime_to_wire(utc_now()),
        overall_status=calculate_overall_status(checks),
        max_severity=max_severity(checks),
        blocked_count=blocked_count,
        caution_count=caution_count,
        pass_count=pass_count,
        check_observations=checks,
        reason_codes=reasons,
        evidence_json={
            "observe_only": True,
            "order_routing_enabled": False,
            "context_hash": context.raw_context.get("context_hash"),
            "strategy_status": context.strategy_status,
            "candidate_state": context.candidate_state,
            "market_regime": context.raw_context.get("market_regime", {}),
            "account_limits": _check_evidence(checks, RiskCategory.ACCOUNT_LIMITS),
            "config_version": settings.risk_gate_config_version,
        },
        config_version=settings.risk_gate_config_version,
        observe_only=True,
    )


def _risk_evaluation_targets(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    strategy_status: StrategyObservationStatus | str | None,
    limit: int,
    candidate_instance_id: str | None,
    strategy_observation_id: str | None,
) -> list[dict[str, Any]]:
    if strategy_observation_id is not None:
        normalized_id = require_non_empty_str(strategy_observation_id, "strategy_observation_id")
        row = connection.execute(
            """
            SELECT strategy_observation_id, candidate_instance_id, trade_date, code, overall_status
            FROM strategy_observations
            WHERE strategy_observation_id = ?
            """,
            (normalized_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"strategy observation not found: {normalized_id}")
        return [_row_to_dict(row)]
    if candidate_instance_id is not None:
        normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
        row = connection.execute(
            """
            SELECT strategy_observation_id, candidate_instance_id, trade_date, code, overall_status
            FROM strategy_observations_latest
            WHERE candidate_instance_id = ?
            """,
            (normalized_id,),
        ).fetchone()
        if row is not None:
            return [_row_to_dict(row)]
        candidate = _candidate_row(connection, normalized_id)
        if candidate is None:
            raise ValueError(f"candidate not found: {normalized_id}")
        return [
            {
                "strategy_observation_id": None,
                "candidate_instance_id": normalized_id,
                "trade_date": candidate["trade_date"],
                "code": candidate["code"],
                "overall_status": None,
            }
        ]

    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if strategy_status is not None:
        normalized_status = parse_str_enum(
            strategy_status,
            StrategyObservationStatus,
            "strategy_status",
        )
        clauses.append("overall_status = ?")
        params.append(normalized_status.value)
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    params.append(limit)
    rows = connection.execute(
        f"""
        SELECT strategy_observation_id, candidate_instance_id, trade_date, code, overall_status
        FROM strategy_observations_latest
        {where_sql}
        ORDER BY
            CASE overall_status
                WHEN 'MATCHED_OBSERVATION' THEN 0
                WHEN 'FORMING' THEN 1
                WHEN 'WATCH' THEN 2
                WHEN 'DATA_WAIT' THEN 3
                ELSE 4
            END,
            evaluated_at DESC,
            candidate_instance_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _strategy_row(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str | None,
    strategy_observation_id: str | None,
) -> sqlite3.Row | None:
    if strategy_observation_id is not None:
        normalized_id = require_non_empty_str(strategy_observation_id, "strategy_observation_id")
        return connection.execute(
            """
            SELECT *
            FROM strategy_observations
            WHERE strategy_observation_id = ?
            """,
            (normalized_id,),
        ).fetchone()
    if candidate_instance_id is not None:
        normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
        return connection.execute(
            """
            SELECT
                l.*,
                o.evidence_json
            FROM strategy_observations_latest AS l
            LEFT JOIN strategy_observations AS o
                ON o.strategy_observation_id = l.strategy_observation_id
            WHERE l.candidate_instance_id = ?
            """,
            (normalized_id,),
        ).fetchone()
    return None


def _candidate_id_from_inputs(
    candidate_instance_id: str | None,
    strategy: sqlite3.Row | None,
) -> str:
    if candidate_instance_id is not None:
        return require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    if strategy is not None:
        return require_non_empty_str(strategy["candidate_instance_id"], "candidate_instance_id")
    raise ValueError("candidate_instance_id or strategy_observation_id is required")


def _candidate_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM candidates
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _candidate_context_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM candidate_context_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()


def _latest_tick_row(connection: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        WHERE code = ?
        """,
        (validate_stock_code(code),),
    ).fetchone()


def _latest_bar_row(
    connection: sqlite3.Connection,
    code: str,
    interval_sec: int,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM market_minute_bars
        WHERE code = ? AND interval_sec = ?
        ORDER BY bucket_start DESC
        LIMIT 1
        """,
        (validate_stock_code(code), interval_sec),
    ).fetchone()


def _theme_context_row(
    connection: sqlite3.Connection,
    theme_id: str | None,
    code: str,
) -> dict[str, Any]:
    if theme_id is None:
        return {"latest_snapshot": {}, "member": {}}
    row = connection.execute(
        """
        SELECT
            l.snapshot_id,
            l.theme_id,
            l.theme_name,
            l.calculated_at,
            l.state,
            l.quality_status,
            l.leading_code,
            l.leading_name,
            l.fresh_coverage_ratio,
            l.rising_ratio,
            l.total_trade_value,
            l.trade_value_delta_1m AS theme_trade_value_delta_1m,
            l.trade_value_delta_3m AS theme_trade_value_delta_3m,
            l.trade_value_delta_5m AS theme_trade_value_delta_5m,
            m.code,
            m.name,
            m.price,
            m.change_rate,
            m.cumulative_trade_value,
            m.trade_value_delta_1m,
            m.trade_value_delta_3m,
            m.trade_value_delta_5m,
            m.execution_strength,
            m.vwap,
            m.above_vwap,
            m.readiness_status,
            m.member_role,
            m.tick_age_sec,
            m.event_ts,
            m.metadata_json
        FROM theme_latest_snapshots AS l
        LEFT JOIN theme_snapshot_members AS m
            ON m.snapshot_id = l.snapshot_id AND m.code = ?
        WHERE l.theme_id = ?
        """,
        (validate_stock_code(code), theme_id),
    ).fetchone()
    if row is None:
        return {"latest_snapshot": {}, "member": {}}
    data = _row_to_dict(row)
    latest_snapshot_keys = {
        "snapshot_id",
        "theme_id",
        "theme_name",
        "calculated_at",
        "state",
        "quality_status",
        "leading_code",
        "leading_name",
        "fresh_coverage_ratio",
        "rising_ratio",
        "total_trade_value",
        "theme_trade_value_delta_1m",
        "theme_trade_value_delta_3m",
        "theme_trade_value_delta_5m",
    }
    latest_snapshot = {key: data[key] for key in latest_snapshot_keys}
    member = {key: value for key, value in data.items() if key not in latest_snapshot_keys}
    if member.get("metadata_json") is not None:
        member["metadata"] = _json_load_object(member.pop("metadata_json"))
    else:
        member.pop("metadata_json", None)
    if "above_vwap" in member and member["above_vwap"] is not None:
        member["above_vwap"] = bool(member["above_vwap"])
    return {"latest_snapshot": latest_snapshot, "member": member}


def _strategy_setup_rows(
    connection: sqlite3.Connection,
    strategy_observation_id: str | None,
) -> list[dict[str, Any]]:
    if strategy_observation_id is None:
        return []
    rows = connection.execute(
        """
        SELECT *
        FROM strategy_setup_observations
        WHERE strategy_observation_id = ?
        """,
        (strategy_observation_id,),
    ).fetchall()
    return [_setup_row_to_dict(row) for row in rows]


def _strategy_reason_codes(
    strategy: sqlite3.Row | None,
    setups: Sequence[Mapping[str, Any]],
) -> list[str]:
    if strategy is None:
        return []
    reasons = _json_load_array(strategy["reason_codes_json"])
    for setup in setups:
        setup_reasons = setup.get("reason_codes", [])
        if isinstance(setup_reasons, Sequence) and not isinstance(setup_reasons, str):
            reasons.extend(str(reason) for reason in setup_reasons)
    return _merge_reasons(reasons)


def _candidate_context_row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {
        "candidate_instance_id": row["candidate_instance_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "theme_context": _json_load_object(row["theme_context_json"]),
        "market_context": _json_load_object(row["market_context_json"]),
        "source_context": _json_load_object(row["source_context_json"]),
        "readiness": _json_load_object(row["readiness_json"]),
        "refreshed_at": row["refreshed_at"],
    }


def _candidate_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["vwap_ready"] = bool(data["vwap_ready"])
    data["bar_1m_ready"] = bool(data["bar_1m_ready"])
    data["bar_3m_ready"] = bool(data["bar_3m_ready"])
    data["bar_5m_ready"] = bool(data["bar_5m_ready"])
    data["reason_codes"] = _json_load_array(data.pop("reason_codes_json"))
    data["metadata"] = _json_load_object(data.pop("metadata_json"))
    return data


def _strategy_row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    data = _row_to_dict(row)
    data["observe_only"] = bool(data["observe_only"])
    data["reason_codes"] = _json_load_array(data.pop("reason_codes_json"))
    evidence_json = data.pop("evidence_json", None)
    data["evidence_json"] = _json_load_object(evidence_json)
    return data


def _latest_observation_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["observe_only"] = bool(data["observe_only"])
    data["reason_codes"] = _json_load_array(data.pop("reason_codes_json"))
    evidence_json = data.pop("evidence_json", None)
    data["evidence_json"] = _json_load_object(evidence_json) if evidence_json else {}
    return data


def _observation_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["observe_only"] = bool(data["observe_only"])
    data["reason_codes"] = _json_load_array(data.pop("reason_codes_json"))
    data["evidence_json"] = _json_load_object(data.pop("evidence_json"))
    return data


def _check_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["reason_codes"] = _json_load_array(data.pop("reason_codes_json"))
    data["evidence_json"] = _json_load_object(data.pop("evidence_json"))
    return data


def _setup_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["reason_codes"] = _json_load_array(data.pop("reason_codes_json"))
    data["evidence_json"] = _json_load_object(data.pop("evidence_json"))
    return data


def _check(
    category: RiskCategory,
    status: RiskCheckStatus,
    severity: RiskSeverity,
    reasons: Sequence[RiskReasonCode | str],
    message: str,
    evidence: Mapping[str, Any],
) -> RiskCheckObservation:
    return RiskCheckObservation(
        category=category,
        status=status,
        severity=severity,
        reason_codes=_merge_reasons([_reason_value(reason) for reason in reasons]),
        message=message,
        evidence_json={**dict(evidence), "observe_only": True},
    )


def _check_evidence(
    checks: Sequence[RiskCheckObservation],
    category: RiskCategory,
) -> dict[str, Any]:
    for check in checks:
        if check.category is category:
            return dict(check.evidence_json)
    return {}


def _insert_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    trade_date: str | None,
    started_at: str,
    config_version: str,
    status: str,
) -> None:
    connection.execute(
        """
        INSERT INTO risk_evaluation_runs (
            run_id,
            trade_date,
            started_at,
            config_version,
            status
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, trade_date, started_at, config_version, status),
    )


def _complete_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    strategy_observation_count: int,
    evaluated_count: int,
    observe_pass_count: int,
    caution_count: int,
    block_count: int,
    data_wait_count: int,
    error_count: int,
    status: str,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE risk_evaluation_runs
        SET completed_at = ?,
            strategy_observation_count = ?,
            evaluated_count = ?,
            observe_pass_count = ?,
            caution_count = ?,
            block_count = ?,
            data_wait_count = ?,
            error_count = ?,
            status = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        (
            datetime_to_wire(utc_now()),
            strategy_observation_count,
            evaluated_count,
            observe_pass_count,
            caution_count,
            block_count,
            data_wait_count,
            error_count,
            status,
            error_message,
            run_id,
        ),
    )


def _record_evaluation_error(
    connection: sqlite3.Connection,
    *,
    run_id: str | None,
    candidate_instance_id: str | None,
    strategy_observation_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO risk_evaluation_errors (
            run_id,
            candidate_instance_id,
            strategy_observation_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            candidate_instance_id,
            strategy_observation_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            canonical_json(payload),
        ),
    )


def _active_candidate_count(connection: sqlite3.Connection, trade_date: str, code: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM candidates
        WHERE trade_date = ?
            AND code = ?
            AND state != 'CLOSED'
        """,
        (trade_date, validate_stock_code(code)),
    ).fetchone()
    return int(row["count"])


def _recent_observation_row(
    connection: sqlite3.Connection,
    context: RiskInputContext,
    settings: Settings,
) -> sqlite3.Row | None:
    if settings.risk_gate_observation_cooldown_sec <= 0:
        return None
    cutoff = datetime_to_wire(
        utc_now() - timedelta(seconds=settings.risk_gate_observation_cooldown_sec)
    )
    return connection.execute(
        """
        SELECT risk_observation_id, evaluated_at, overall_status
        FROM risk_observations_latest
        WHERE candidate_instance_id = ?
            AND evaluated_at >= ?
        """,
        (context.candidate_instance_id, cutoff),
    ).fetchone()


def _json_load_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    loaded = json.loads(value)
    return loaded if isinstance(loaded, dict) else {}


def _json_load_array(value: str | None) -> list[str]:
    if not value:
        return []
    loaded = json.loads(value)
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _dict_or_empty(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first_text(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_number(*values: object) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_int(*values: object) -> int | None:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _positive_or_none(value: float | None) -> float | None:
    if value is None or value <= 0:
        return None
    return value


def _bool_first(*values: object) -> bool:
    for value in values:
        if value is None:
            continue
        return bool(value)
    return False


def _age_seconds(value: object) -> float:
    parsed = parse_timestamp(value, "timestamp")
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _higher_severity(current: RiskSeverity, candidate: RiskSeverity) -> RiskSeverity:
    order = {
        RiskSeverity.INFO: 0,
        RiskSeverity.LOW: 1,
        RiskSeverity.MEDIUM: 2,
        RiskSeverity.HIGH: 3,
        RiskSeverity.CRITICAL: 4,
    }
    return candidate if order[candidate] > order[current] else current


def _caution_if_not_block(status: RiskCheckStatus) -> RiskCheckStatus:
    if status is RiskCheckStatus.BLOCK_OBSERVED:
        return status
    return RiskCheckStatus.CAUTION_OBSERVED


def _caution_if_pass(status: RiskCheckStatus) -> RiskCheckStatus:
    if status is RiskCheckStatus.PASS_OBSERVED:
        return RiskCheckStatus.CAUTION_OBSERVED
    return status


def _merge_reasons(reasons: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]


def _reason_value(reason: RiskReasonCode | str) -> str:
    if isinstance(reason, RiskReasonCode):
        return reason.value
    return str(reason)


def _context_hash(payload: Mapping[str, Any]) -> str:
    payload_json = canonical_json(payload)
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def _json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _count_rows(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    where: str | None = None,
) -> int:
    where_sql = "" if where is None else f"WHERE {where}"
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name} {where_sql}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
