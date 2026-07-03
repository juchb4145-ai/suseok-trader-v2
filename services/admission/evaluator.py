from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.candidate.state import CandidateState
from domain.market.quality import tick_age_seconds
from domain.risk.status import RiskObservationStatus
from domain.strategy.status import StrategyObservationStatus


class AdmissionReason(StrEnum):
    CANDIDATE_NOT_FOUND = "CANDIDATE_NOT_FOUND"
    CANDIDATE_NOT_CONTEXT_READY = "CANDIDATE_NOT_CONTEXT_READY"
    CANDIDATE_CONTEXT_MISSING = "CANDIDATE_CONTEXT_MISSING"
    STRATEGY_OBSERVATION_MISSING = "STRATEGY_OBSERVATION_MISSING"
    STRATEGY_NOT_MATCHED = "STRATEGY_NOT_MATCHED"
    STRATEGY_OBSERVE_ONLY_MISMATCH = "STRATEGY_OBSERVE_ONLY_MISMATCH"
    RISK_OBSERVATION_MISSING = "RISK_OBSERVATION_MISSING"
    RISK_NOT_OBSERVE_PASS = "RISK_NOT_OBSERVE_PASS"
    RISK_OBSERVE_ONLY_MISMATCH = "RISK_OBSERVE_ONLY_MISMATCH"
    LATEST_TICK_MISSING = "LATEST_TICK_MISSING"
    LATEST_TICK_STALE = "LATEST_TICK_STALE"
    DRY_RUN_EVIDENCE_MISSING = "DRY_RUN_EVIDENCE_MISSING"


@dataclass(frozen=True, kw_only=True)
class AdmissionPolicy:
    name: str
    require_candidate_context_ready: bool = True
    require_candidate_context_row: bool = False
    require_strategy_matched: bool = True
    require_risk_observe_pass: bool = True
    require_fresh_tick: bool = True
    stale_tick_sec: float = 0.0
    require_dry_run_evidence: bool = False
    require_strategy_observe_only: bool | None = None
    require_risk_observe_only: bool | None = None


@dataclass(frozen=True, kw_only=True)
class AdmissionTrace:
    policy_name: str
    candidate_instance_id: str
    eligible: bool
    reason_codes: Sequence[str] = field(default_factory=tuple)
    candidate_evidence: Mapping[str, Any] = field(default_factory=dict)
    candidate_context_evidence: Mapping[str, Any] = field(default_factory=dict)
    strategy_evidence: Mapping[str, Any] = field(default_factory=dict)
    risk_evidence: Mapping[str, Any] = field(default_factory=dict)
    latest_tick_evidence: Mapping[str, Any] = field(default_factory=dict)
    dry_run_evidence: Mapping[str, Any] = field(default_factory=dict)
    trade_date: str | None = None
    code: str | None = None
    name: str = "UNKNOWN"
    strategy_observation_id: str | None = None
    risk_observation_id: str | None = None
    computed_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))

    def to_evidence(self) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "admission_trace": {
                "policy": self.policy_name,
                "eligible": self.eligible,
                "reason_codes": list(self.reason_codes),
                "candidate_instance_id": self.candidate_instance_id,
                "strategy_observation_id": self.strategy_observation_id,
                "risk_observation_id": self.risk_observation_id,
                "trade_date": self.trade_date,
                "code": self.code,
                "name": self.name,
                "computed_at": self.computed_at,
            }
        }
        if self.candidate_evidence:
            evidence["candidate"] = dict(self.candidate_evidence)
        if self.candidate_context_evidence:
            evidence["candidate_context"] = dict(self.candidate_context_evidence)
        if self.strategy_evidence:
            evidence["strategy"] = dict(self.strategy_evidence)
        if self.risk_evidence:
            evidence["risk"] = dict(self.risk_evidence)
        if self.latest_tick_evidence:
            evidence["latest_tick"] = dict(self.latest_tick_evidence)
        if self.dry_run_evidence:
            evidence["dry_run"] = dict(self.dry_run_evidence)
        return evidence


def evaluate_trade_admission(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    policy: AdmissionPolicy,
    *,
    fallback_trade_date: str | None = None,
    fallback_code: str | None = None,
    fallback_name: str | None = None,
    dry_run_evidence: Mapping[str, Any] | None = None,
) -> AdmissionTrace:
    normalized_id = require_non_empty_str(candidate_instance_id, "candidate_instance_id")
    candidate = _candidate_row(connection, normalized_id)
    candidate_context = _candidate_context_row(connection, normalized_id)
    strategy = _strategy_latest_row(connection, normalized_id)
    risk = _risk_latest_row(connection, normalized_id)
    dry_run = dict(dry_run_evidence or {})

    reason_codes: list[str] = []
    trade_date = fallback_trade_date
    code = validate_stock_code(fallback_code) if fallback_code else None
    name = fallback_name or "UNKNOWN"
    candidate_evidence: dict[str, Any] = {}
    candidate_context_evidence = _candidate_context_evidence(candidate_context)

    if candidate is None:
        reason_codes.append(AdmissionReason.CANDIDATE_NOT_FOUND.value)
    else:
        trade_date = str(candidate["trade_date"])
        code = validate_stock_code(candidate["code"])
        name = str(candidate["name"])
        candidate_evidence = _candidate_evidence(candidate)
        if (
            policy.require_candidate_context_ready
            and str(candidate["state"]).upper() != CandidateState.CONTEXT_READY.value
        ):
            reason_codes.append(AdmissionReason.CANDIDATE_NOT_CONTEXT_READY.value)

    if policy.require_candidate_context_row and candidate_context is None:
        reason_codes.append(AdmissionReason.CANDIDATE_CONTEXT_MISSING.value)

    strategy_id = None
    strategy_evidence: dict[str, Any] = {}
    if strategy is None:
        reason_codes.append(AdmissionReason.STRATEGY_OBSERVATION_MISSING.value)
    else:
        strategy_id = str(strategy["strategy_observation_id"])
        strategy_evidence = _strategy_evidence(strategy)
        if (
            policy.require_strategy_matched
            and str(strategy["overall_status"]).upper()
            != StrategyObservationStatus.MATCHED_OBSERVATION.value
        ):
            reason_codes.append(AdmissionReason.STRATEGY_NOT_MATCHED.value)
        if (
            policy.require_strategy_observe_only is not None
            and bool(strategy["observe_only"]) != policy.require_strategy_observe_only
        ):
            reason_codes.append(AdmissionReason.STRATEGY_OBSERVE_ONLY_MISMATCH.value)

    risk_id = None
    risk_evidence: dict[str, Any] = {}
    if risk is None:
        reason_codes.append(AdmissionReason.RISK_OBSERVATION_MISSING.value)
    else:
        risk_id = str(risk["risk_observation_id"])
        risk_evidence = _risk_evidence(risk)
        if (
            policy.require_risk_observe_pass
            and str(risk["overall_status"]).upper() != RiskObservationStatus.OBSERVE_PASS.value
        ):
            reason_codes.append(AdmissionReason.RISK_NOT_OBSERVE_PASS.value)
        if (
            policy.require_risk_observe_only is not None
            and bool(risk["observe_only"]) != policy.require_risk_observe_only
        ):
            reason_codes.append(AdmissionReason.RISK_OBSERVE_ONLY_MISMATCH.value)

    latest_tick_evidence: dict[str, Any] = {}
    if code is None:
        reason_codes.append(AdmissionReason.LATEST_TICK_MISSING.value)
    else:
        tick = _latest_tick_row(connection, code)
        if tick is None:
            reason_codes.append(AdmissionReason.LATEST_TICK_MISSING.value)
        else:
            tick_age = tick_age_seconds(tick["event_ts"])
            latest_tick_evidence = _tick_evidence(tick, tick_age)
            if policy.require_fresh_tick and tick_age > policy.stale_tick_sec:
                reason_codes.append(AdmissionReason.LATEST_TICK_STALE.value)

    if dry_run:
        dry_run = dict(dry_run)
    elif policy.require_dry_run_evidence:
        reason_codes.append(AdmissionReason.DRY_RUN_EVIDENCE_MISSING.value)

    merged_reasons = _merge_reasons(reason_codes)
    return AdmissionTrace(
        policy_name=require_non_empty_str(policy.name, "policy.name"),
        candidate_instance_id=normalized_id,
        eligible=not merged_reasons,
        reason_codes=merged_reasons,
        candidate_evidence=candidate_evidence,
        candidate_context_evidence=candidate_context_evidence,
        strategy_evidence=strategy_evidence,
        risk_evidence=risk_evidence,
        latest_tick_evidence=latest_tick_evidence,
        dry_run_evidence=dry_run,
        trade_date=trade_date,
        code=code,
        name=name,
        strategy_observation_id=strategy_id,
        risk_observation_id=risk_id,
    )


def _candidate_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (candidate_instance_id,),
    ).fetchone()


def _candidate_context_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM candidate_context_latest WHERE candidate_instance_id = ?",
        (candidate_instance_id,),
    ).fetchone()


def _strategy_latest_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM strategy_observations_latest WHERE candidate_instance_id = ?",
        (candidate_instance_id,),
    ).fetchone()


def _risk_latest_row(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM risk_observations_latest WHERE candidate_instance_id = ?",
        (candidate_instance_id,),
    ).fetchone()


def _latest_tick_row(connection: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM market_ticks_latest WHERE code = ? AND exchange = 'KRX'",
        (validate_stock_code(code),),
    ).fetchone()


def _candidate_evidence(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "candidate_instance_id": row["candidate_instance_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "state": row["state"],
    }


def _candidate_context_evidence(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {
        "refreshed_at": row["refreshed_at"],
        "readiness": _json_object(row["readiness_json"]),
    }


def _strategy_evidence(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "strategy_observation_id": row["strategy_observation_id"],
        "overall_status": row["overall_status"],
        "evaluated_at": row["evaluated_at"],
        "observe_only": bool(row["observe_only"]),
    }


def _risk_evidence(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "risk_observation_id": row["risk_observation_id"],
        "strategy_observation_id": row["strategy_observation_id"],
        "overall_status": row["overall_status"],
        "reason_codes": _json_array(row["reason_codes_json"]),
        "evaluated_at": row["evaluated_at"],
        "observe_only": bool(row["observe_only"]),
    }


def _tick_evidence(row: sqlite3.Row, tick_age: float) -> dict[str, Any]:
    evidence = {
        "code": row["code"],
        "name": row["name"],
        "price": row["price"],
        "event_ts": row["event_ts"],
        "quality_status": row["quality_status"],
        "tick_age_sec": tick_age,
    }
    for field_name in ("best_bid", "best_ask", "spread_ticks"):
        if field_name in row.keys():
            evidence[field_name] = row[field_name]
    return evidence


def _json_array(value: object) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return []


def _json_object(value: object) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _merge_reasons(reasons: list[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]
