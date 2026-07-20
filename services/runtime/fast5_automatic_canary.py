from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

from domain.broker.account_snapshot import BrokerSnapshotStatus, canonical_snapshot_status
from domain.broker.utils import (
    datetime_to_wire,
    market_today,
    new_message_id,
    normalize_value,
    parse_timestamp,
    utc_now,
)
from storage.gateway_order_broker_boundary import get_order_broker_boundary_status

from services.config import Settings, load_settings
from services.live_sim.execution_lifecycle_status import (
    build_live_sim_execution_lifecycle_status,
)
from services.live_sim.live_sim_service import get_latest_live_sim_reconcile
from services.pipeline_coherency import build_pipeline_coherency_status
from services.runtime.live_sim_operating_orchestrator import (
    LiveSimOperatingRunResult,
    run_live_sim_operating_cycle_once,
    save_live_sim_operating_run,
)
from services.runtime.preflight import (
    LiveSimPreflightResult,
    OperatingMode,
    PreflightStatus,
    run_live_sim_preflight,
)

FAST5_POLICY_VERSION = "fast5-guarded-automatic-canary.v2"
FAST5_ROLLBACK_POLICY_VERSIONS = {
    "fast5-guarded-automatic-canary.v1",
    FAST5_POLICY_VERSION,
}
FAST5_BOOTSTRAP_APPROVAL_CONTRACT = "fast5-bootstrap-automatic-canary-approval.v1"


class Fast5CanaryMode(StrEnum):
    READY = "READY"
    PROTECT_ONLY = "PROTECT_ONLY"


class Fast5CanaryPhase(StrEnum):
    BLOCKED = "BLOCKED"
    BOOTSTRAP = "BOOTSTRAP"
    QUALIFIED = "QUALIFIED"


@dataclass(frozen=True, kw_only=True)
class Fast5AutomaticCanaryGate:
    status: Fast5CanaryMode
    trade_date: str
    queue_commands_requested: bool
    phase: Fast5CanaryPhase = Fast5CanaryPhase.BLOCKED
    reason_codes: Sequence[str] = field(default_factory=tuple)
    checks: Mapping[str, Any] = field(default_factory=dict)
    effective_limits: Mapping[str, Any] = field(default_factory=dict)
    rollback_latch: Mapping[str, Any] = field(default_factory=dict)
    bootstrap_approval: Mapping[str, Any] = field(default_factory=dict)
    evaluated_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))

    @property
    def ready(self) -> bool:
        return self.status is Fast5CanaryMode.READY and not self.reason_codes

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_version": FAST5_POLICY_VERSION,
            "status": self.status.value,
            "phase": self.phase.value,
            "ready": self.ready,
            "trade_date": self.trade_date,
            "queue_commands_requested": self.queue_commands_requested,
            "reason_codes": list(self.reason_codes),
            "checks": normalize_value(dict(self.checks)),
            "effective_limits": normalize_value(dict(self.effective_limits)),
            "rollback_latch": normalize_value(dict(self.rollback_latch)),
            "bootstrap_approval": normalize_value(dict(self.bootstrap_approval)),
            "evaluated_at": self.evaluated_at,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "ai_routing_effect": 0,
            "automatic_local_repair": False,
            "no_order_side_effects": True,
        }


@dataclass(frozen=True, kw_only=True)
class Fast5AutomaticCanaryRunResult:
    run_id: str
    status: str
    mode: OperatingMode
    queue_commands_requested: bool
    queue_commands_effective: bool
    gate: Fast5AutomaticCanaryGate
    operating_run: Mapping[str, Any] | None = None
    rollback_latched: bool = False
    side_effects_unknown: bool = False
    created_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))

    def to_dict(self) -> dict[str, Any]:
        buy_command_count = int(
            (self.operating_run or {}).get("buy_command_count") or 0
        )
        cancel_command_count = int(
            (self.operating_run or {}).get("cancel_command_count") or 0
        )
        exit_command_count = int(
            (self.operating_run or {}).get("exit_command_count") or 0
        )
        command_count = buy_command_count + cancel_command_count + exit_command_count
        return {
            "run_id": self.run_id,
            "policy_version": FAST5_POLICY_VERSION,
            "status": self.status,
            "mode": self.mode.value,
            "queue_commands_requested": self.queue_commands_requested,
            "queue_commands_effective": self.queue_commands_effective,
            "buy_command_count": buy_command_count,
            "cancel_command_count": cancel_command_count,
            "exit_command_count": exit_command_count,
            "gate": self.gate.to_dict(),
            "operating_run": normalize_value(self.operating_run),
            "rollback_latched": self.rollback_latched,
            "side_effects_unknown": self.side_effects_unknown,
            "created_at": self.created_at,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "ai_routing_effect": 0,
            "automatic_local_repair": False,
            "no_order_side_effects": command_count == 0
            and not self.side_effects_unknown,
        }


def build_fast5_bootstrap_approval(
    settings: Settings,
    *,
    trade_date: str,
) -> dict[str, Any]:
    """Build the public, deterministic approval contract for one SIM bootstrap BUY."""

    strict = _strict_fast5_settings(settings, bootstrap=True)
    payload = {
        "contract": FAST5_BOOTSTRAP_APPROVAL_CONTRACT,
        "policy_version": FAST5_POLICY_VERSION,
        "approval_id": settings.live_sim_fast5_bootstrap_approval_id,
        "trade_date": str(trade_date),
        "one_shot": True,
        "mode": OperatingMode.PILOT_BUY_ONLY.value,
        "trading_mode": "LIVE_SIM",
        "live_real_allowed": False,
        "exchange": "KRX",
        "allowed_side": "BUY",
        "order_type": "LIMIT",
        "max_buy_commands": 1,
        "max_daily_buy_count": 1,
        "max_order_notional": strict.live_sim_max_order_notional,
        "max_active_orders": 1,
        "max_active_positions": 1,
        "scale_in_allowed": False,
        "ai_routing_effect": 0,
        "automatic_reprice": False,
    }
    canonical_json = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    expected_sha256 = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    configured_sha256 = settings.live_sim_fast5_bootstrap_approval_sha256
    return {
        "contract": FAST5_BOOTSTRAP_APPROVAL_CONTRACT,
        "payload": payload,
        "canonical_json": canonical_json,
        "expected_sha256": expected_sha256,
        "configured_sha256": configured_sha256 or None,
        "matches": bool(configured_sha256 == expected_sha256),
    }


def evaluate_fast5_automatic_canary_gate(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    trade_date: str | None = None,
    queue_commands: bool = False,
) -> Fast5AutomaticCanaryGate:
    gate, _ = _evaluate_gate(
        connection,
        settings=settings or load_settings(),
        trade_date=trade_date,
        queue_commands=queue_commands,
    )
    return gate


def run_fast5_automatic_canary_once(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    trade_date: str | None = None,
    queue_commands: bool = False,
    limit: int | None = None,
) -> Fast5AutomaticCanaryRunResult:
    resolved_settings = settings or load_settings()
    gate, preflight = _evaluate_gate(
        connection,
        settings=resolved_settings,
        trade_date=trade_date,
        queue_commands=queue_commands,
    )
    run_id = new_message_id("fast5_automatic_canary")
    if not queue_commands:
        return Fast5AutomaticCanaryRunResult(
            run_id=run_id,
            status="PREVIEW",
            mode=(
                OperatingMode.PILOT_BUY_ONLY
                if gate.ready
                else OperatingMode.PROTECT_ONLY
            ),
            queue_commands_requested=False,
            queue_commands_effective=False,
            gate=gate,
        )

    if not gate.ready:
        existing_latch = str(gate.rollback_latch.get("latched_run_id") or "")
        if existing_latch:
            return Fast5AutomaticCanaryRunResult(
                run_id=run_id,
                status="PROTECT_ONLY",
                mode=OperatingMode.PROTECT_ONLY,
                queue_commands_requested=True,
                queue_commands_effective=False,
                gate=gate,
                rollback_latched=True,
            )
        if (
            gate.phase is Fast5CanaryPhase.BOOTSTRAP
            and "FAST5_BOOTSTRAP_ALREADY_CONSUMED" in gate.reason_codes
            and resolved_settings.live_sim_fast5_automatic_canary_enabled
            and resolved_settings.live_sim_fast5_auto_queue_enabled
            and resolved_settings.live_sim_pilot_auto_queue_command
            and resolved_settings.live_sim_operating_write_runs
        ):
            return _run_consumed_bootstrap_protection(
                connection,
                settings=resolved_settings,
                gate=gate,
                run_id=run_id,
                limit=limit,
            )
        return Fast5AutomaticCanaryRunResult(
            run_id=run_id,
            status="PROTECT_ONLY",
            mode=OperatingMode.PROTECT_ONLY,
            queue_commands_requested=True,
            queue_commands_effective=False,
            gate=gate,
            rollback_latched=False,
        )

    bootstrap = gate.phase is Fast5CanaryPhase.BOOTSTRAP
    bootstrap_reservation_run_id: str | None = None
    if bootstrap:
        reserved, bootstrap_reservation_run_id = _reserve_bootstrap_once(
            connection,
            settings=resolved_settings,
            gate=gate,
            preflight=preflight,
        )
        if not reserved:
            refreshed_gate, _ = _evaluate_gate(
                connection,
                settings=resolved_settings,
                trade_date=gate.trade_date,
                queue_commands=True,
            )
            return Fast5AutomaticCanaryRunResult(
                run_id=run_id,
                status="PROTECT_ONLY",
                mode=OperatingMode.PROTECT_ONLY,
                queue_commands_requested=True,
                queue_commands_effective=False,
                gate=refreshed_gate,
                operating_run={
                    "bootstrap_reservation_run_id": bootstrap_reservation_run_id
                },
            )

    strict_settings = _strict_fast5_settings(resolved_settings, bootstrap=bootstrap)
    try:
        operating = run_live_sim_operating_cycle_once(
            connection,
            settings=replace(strict_settings, live_sim_operating_write_runs=False),
            mode=OperatingMode.PILOT_BUY_ONLY,
            queue_commands=True,
            trade_date=gate.trade_date,
            limit=limit,
            include_ai=False,
            include_no_buy=True,
        )
    except Exception as exc:
        failed = LiveSimOperatingRunResult(
            run_id=run_id,
            trade_date=gate.trade_date,
            mode=OperatingMode.PROTECT_ONLY,
            queue_commands=True,
            preflight=preflight,
            status="PROTECT_ONLY",
            errors=(
                {
                    "stage": "fast5_operating_cycle",
                    "error_type": type(exc).__name__,
                },
            ),
            reason_summary={
                "policy": FAST5_POLICY_VERSION,
                "fast5_automatic_canary": gate.to_dict(),
                "fast5_bootstrap": (
                    {
                        "contract": FAST5_BOOTSTRAP_APPROVAL_CONTRACT,
                        "approval_id": resolved_settings.live_sim_fast5_bootstrap_approval_id,
                        "approval_sha256": (
                            resolved_settings.live_sim_fast5_bootstrap_approval_sha256
                        ),
                        "trade_date": gate.trade_date,
                        "reservation_run_id": bootstrap_reservation_run_id,
                        "one_shot_consumed": True,
                    }
                    if bootstrap
                    else None
                ),
                "rollback_latched": True,
            },
        )
        _save_fast5_operating_run(connection, failed, resolved_settings)
        return Fast5AutomaticCanaryRunResult(
            run_id=run_id,
            status="PROTECT_ONLY",
            mode=OperatingMode.PROTECT_ONLY,
            queue_commands_requested=True,
            queue_commands_effective=True,
            gate=gate,
            operating_run={
                **failed.to_dict(),
                "no_order_side_effects": False,
                "side_effects_unknown": True,
            },
            rollback_latched=True,
            side_effects_unknown=True,
        )
    invariant_errors: list[dict[str, Any]] = []
    if operating.buy_command_count > 1:
        invariant_errors.append(
            {
                "stage": "fast5_command_budget",
                "error": "FAST-5 created more than one BUY command in a cycle.",
            }
        )
    if operating.cancel_command_count or operating.exit_command_count:
        invariant_errors.append(
            {
                "stage": "fast5_buy_only_policy",
                "error": "FAST-5 BUY-only cycle created lifecycle commands.",
            }
        )
    rollback_latched = bool(
        invariant_errors
        or operating.errors
        or operating.preflight.status is not PreflightStatus.PASS
        or operating.status not in {"COMPLETED", "READY"}
    )
    successful_status = operating.status
    if bootstrap and not rollback_latched:
        successful_status = (
            "BOOTSTRAP_QUEUED"
            if operating.buy_command_count == 1
            else "BOOTSTRAP_NO_COMMAND"
        )
    enriched = replace(
        operating,
        run_id=run_id,
        status="PROTECT_ONLY" if rollback_latched else successful_status,
        errors=tuple([*operating.errors, *invariant_errors]),
        reason_summary={
            **dict(operating.reason_summary),
            "fast5_automatic_canary": gate.to_dict(),
            "policy": FAST5_POLICY_VERSION,
            "rollback_latched": rollback_latched,
            "fast5_bootstrap": (
                {
                    "contract": FAST5_BOOTSTRAP_APPROVAL_CONTRACT,
                    "approval_id": resolved_settings.live_sim_fast5_bootstrap_approval_id,
                    "approval_sha256": resolved_settings.live_sim_fast5_bootstrap_approval_sha256,
                    "trade_date": gate.trade_date,
                    "reservation_run_id": bootstrap_reservation_run_id,
                    "one_shot_consumed": True,
                }
                if bootstrap
                else None
            ),
        },
    )
    _save_fast5_operating_run(connection, enriched, resolved_settings)
    return Fast5AutomaticCanaryRunResult(
        run_id=run_id,
        status=enriched.status,
        mode=(
            OperatingMode.PROTECT_ONLY
            if rollback_latched
            else OperatingMode.PILOT_BUY_ONLY
        ),
        queue_commands_requested=True,
        queue_commands_effective=True,
        gate=gate,
        operating_run=enriched.to_dict(),
        rollback_latched=rollback_latched,
    )


def _fast5_phase(settings: Settings) -> Fast5CanaryPhase:
    status = settings.live_sim_fast5_bootstrap_status
    if status == "PENDING":
        return Fast5CanaryPhase.BOOTSTRAP
    if status == "PASS":
        return Fast5CanaryPhase.QUALIFIED
    return Fast5CanaryPhase.BLOCKED


def _evaluate_gate(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    trade_date: str | None,
    queue_commands: bool,
) -> tuple[Fast5AutomaticCanaryGate, LiveSimPreflightResult]:
    resolved_trade_date = str(trade_date or market_today())
    phase = _fast5_phase(settings)
    bootstrap_approval = build_fast5_bootstrap_approval(
        settings,
        trade_date=resolved_trade_date,
    )
    checks: dict[str, Any] = {}
    reasons: list[str] = []

    def add(
        name: str,
        passed: bool,
        reason_code: str,
        details: Mapping[str, Any],
        *,
        required: bool = True,
    ) -> None:
        checks[name] = {
            "status": "PASS" if passed else ("BLOCK" if required else "WARN"),
            "reason_code": None if passed else reason_code,
            "required": required,
            **normalize_value(dict(details)),
        }
        if not passed and required:
            reasons.append(reason_code)

    add(
        "automation_enabled",
        settings.live_sim_fast5_automatic_canary_enabled,
        "FAST5_AUTOMATIC_CANARY_DISABLED",
        {"enabled": settings.live_sim_fast5_automatic_canary_enabled},
    )
    add(
        "automatic_queue_enabled",
        not queue_commands
        or (
            settings.live_sim_fast5_auto_queue_enabled
            and settings.live_sim_pilot_auto_queue_command
        ),
        "FAST5_AUTO_QUEUE_DISABLED",
        {
            "queue_commands_requested": queue_commands,
            "auto_queue_enabled": settings.live_sim_fast5_auto_queue_enabled,
            "pilot_auto_queue_enabled": settings.live_sim_pilot_auto_queue_command,
        },
    )
    _add_external_evidence_checks(
        add,
        settings,
        phase=phase,
        trade_date=resolved_trade_date,
        bootstrap_approval=bootstrap_approval,
    )
    bootstrap_usage = _bootstrap_usage(
        connection,
        str(settings.live_sim_fast5_bootstrap_approval_sha256 or ""),
    )
    if phase is Fast5CanaryPhase.BOOTSTRAP:
        add(
            "bootstrap_one_shot",
            not bool(bootstrap_usage.get("consumed")),
            "FAST5_BOOTSTRAP_ALREADY_CONSUMED",
            bootstrap_usage,
        )
        add(
            "audit_persistence",
            settings.live_sim_operating_write_runs,
            "FAST5_BOOTSTRAP_AUDIT_PERSISTENCE_DISABLED",
            {"live_sim_operating_write_runs": settings.live_sim_operating_write_runs},
        )
    else:
        checks["bootstrap_one_shot"] = {
            "status": "NOT_APPLICABLE",
            "required": False,
            **normalize_value(bootstrap_usage),
        }

    ai_routing_disabled = bool(
        not settings.ai_sidecar_order_tools_enabled
        and not settings.ai_candidate_scorer_allow_order_actions
        and not settings.ai_candidate_scorer_attach_to_order_plan
        and not settings.ai_candidate_scorer_attach_to_live_sim_run
    )
    add(
        "ai_routing_isolation",
        ai_routing_disabled,
        "FAST5_AI_ROUTING_NOT_ISOLATED",
        {
            "ai_routing_effect": 0 if ai_routing_disabled else "UNSAFE",
            "sidecar_order_tools_enabled": settings.ai_sidecar_order_tools_enabled,
            "candidate_allow_order_actions": (
                settings.ai_candidate_scorer_allow_order_actions
            ),
            "candidate_attach_to_order_plan": (
                settings.ai_candidate_scorer_attach_to_order_plan
            ),
            "candidate_attach_to_live_sim_run": (
                settings.ai_candidate_scorer_attach_to_live_sim_run
            ),
        },
    )
    static_policy_ok = bool(
        settings.live_sim_order_exchange == "KRX"
        and not settings.live_sim_allow_market_order
        and not settings.live_sim_order_plan_allow_market_order
        and settings.live_sim_default_order_type == "LIMIT"
        and settings.live_sim_order_plan_allowed_side == "BUY"
        and not settings.live_sim_position_allow_scale_in
    )
    add(
        "static_order_policy",
        static_policy_ok,
        "FAST5_STATIC_ORDER_POLICY_UNSAFE",
        {
            "exchange": settings.live_sim_order_exchange,
            "order_type": settings.live_sim_default_order_type,
            "allowed_side": settings.live_sim_order_plan_allowed_side,
            "market_order_allowed": settings.live_sim_allow_market_order,
            "order_plan_market_allowed": settings.live_sim_order_plan_allow_market_order,
            "scale_in_allowed": settings.live_sim_position_allow_scale_in,
        },
    )

    preflight = run_live_sim_preflight(
        connection,
        settings=_strict_fast5_settings(
            settings,
            bootstrap=phase is Fast5CanaryPhase.BOOTSTRAP,
        ),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=True,
        trade_date=resolved_trade_date,
        include_ai=False,
        include_no_buy=False,
    )
    add(
        "preflight",
        preflight.status is PreflightStatus.PASS,
        "FAST5_PREFLIGHT_NON_PASS",
        {
            "preflight_status": preflight.status.value,
            "blocking_reasons": list(preflight.blocking_reasons),
            "warnings": list(preflight.warnings),
        },
    )
    _add_pipeline_check(add, connection, settings, resolved_trade_date)
    _add_broker_boundary_check(add, connection)
    _add_lifecycle_check(add, connection)
    _add_broker_reconcile_check(add, connection, settings, resolved_trade_date)

    latch = _latest_unacknowledged_rollback(connection, settings)
    add(
        "rollback_latch",
        not bool(latch.get("latched_run_id")),
        "FAST5_ROLLBACK_LATCHED",
        latch,
    )
    reason_codes = tuple(dict.fromkeys(reasons))
    gate = Fast5AutomaticCanaryGate(
        status=(
            Fast5CanaryMode.READY
            if not reason_codes
            else Fast5CanaryMode.PROTECT_ONLY
        ),
        phase=phase,
        trade_date=resolved_trade_date,
        queue_commands_requested=queue_commands,
        reason_codes=reason_codes,
        checks=checks,
        effective_limits=_effective_limits(
            settings,
            bootstrap=phase is Fast5CanaryPhase.BOOTSTRAP,
        ),
        rollback_latch=latch,
        bootstrap_approval=bootstrap_approval,
    )
    return gate, preflight


def _add_external_evidence_checks(
    add: Any,
    settings: Settings,
    *,
    phase: Fast5CanaryPhase,
    trade_date: str,
    bootstrap_approval: Mapping[str, Any],
) -> None:
    bootstrap_passed = False
    bootstrap_reason = "FAST5_BOOTSTRAP_NOT_AUTHORIZED"
    if phase is Fast5CanaryPhase.BOOTSTRAP:
        bootstrap_passed = bool(
            settings.live_sim_fast5_bootstrap_trade_date == trade_date
            and bootstrap_approval.get("matches") is True
        )
        bootstrap_reason = "FAST5_BOOTSTRAP_APPROVAL_MISMATCH"
    elif phase is Fast5CanaryPhase.QUALIFIED:
        bootstrap_passed = _is_sha256(
            settings.live_sim_fast5_bootstrap_evidence_sha256
        )
        bootstrap_reason = "FAST5_BOOTSTRAP_EVIDENCE_MISSING"
    add(
        "bootstrap",
        bootstrap_passed,
        bootstrap_reason,
        {
            "qualification_status": settings.live_sim_fast5_bootstrap_status,
            "configured_trade_date": (
                settings.live_sim_fast5_bootstrap_trade_date or None
            ),
            "expected_trade_date": trade_date,
            "approval_id": settings.live_sim_fast5_bootstrap_approval_id or None,
            "approval_sha256": (
                settings.live_sim_fast5_bootstrap_approval_sha256 or None
            ),
            "expected_approval_sha256": bootstrap_approval.get("expected_sha256"),
            "approval_matches": bootstrap_approval.get("matches"),
            "evidence_sha256": (
                settings.live_sim_fast5_bootstrap_evidence_sha256 or None
            ),
        },
    )
    contracts = (
        (
            "alpha",
            settings.live_sim_fast5_alpha_status,
            "ALPHA_QUALIFIED",
            settings.live_sim_fast5_alpha_evidence_sha256,
            "FAST5_ALPHA_NOT_QUALIFIED",
        ),
        (
            "parallel_shadow",
            settings.live_sim_fast5_shadow_status,
            "PASS",
            settings.live_sim_fast5_shadow_evidence_sha256,
            "FAST5_SHADOW_NOT_QUALIFIED",
        ),
    )
    for name, status, passing_status, evidence_sha256, reason_code in contracts:
        passed = status == passing_status and _is_sha256(evidence_sha256)
        add(
            name,
            passed,
            reason_code,
            {
                "qualification_status": status,
                "required_status": passing_status,
                "evidence_sha256": evidence_sha256 or None,
                "evidence_bound": _is_sha256(evidence_sha256),
            },
            required=phase is Fast5CanaryPhase.QUALIFIED,
        )


def _add_pipeline_check(
    add: Any,
    connection: sqlite3.Connection,
    settings: Settings,
    trade_date: str,
) -> None:
    try:
        inventory_count = _pipeline_inventory_count(connection, trade_date)
        status = build_pipeline_coherency_status(
            connection,
            trade_date=trade_date,
            max_age_sec=settings.live_sim_fast5_pipeline_max_age_sec,
            limit=500,
        )
        passed = bool(
            status.get("status") == "PASS"
            and int(status.get("candidate_count") or 0) > 0
            and inventory_count <= 500
            and int(status.get("candidate_count") or 0) == inventory_count
            and int(status.get("mismatch_count") or 0) == 0
            and int(status.get("missing_lineage_count") or 0) == 0
            and int(status.get("stale_count") or 0) == 0
        )
        details = {
            "qualification_status": status.get("status"),
            "qualification_reason_codes": status.get("reason_codes"),
            "candidate_count": status.get("candidate_count"),
            "full_inventory_count": inventory_count,
            "full_inventory_covered": (
                inventory_count <= 500
                and int(status.get("candidate_count") or 0) == inventory_count
            ),
            "coherent_count": status.get("coherent_count"),
            "mismatch_count": status.get("mismatch_count"),
            "missing_lineage_count": status.get("missing_lineage_count"),
            "stale_count": status.get("stale_count"),
            "generated_at": status.get("generated_at"),
        }
    except Exception as exc:
        passed = False
        details = {"classifier_error_type": type(exc).__name__}
    add("pipeline_coherency", passed, "FAST5_PIPELINE_NON_PASS", details)


def _pipeline_inventory_count(connection: sqlite3.Connection, trade_date: str) -> int:
    row = connection.execute(
        """
        WITH pipeline_candidates AS (
            SELECT candidate_instance_id FROM strategy_observations_latest
            WHERE trade_date = ?
            UNION
            SELECT candidate_instance_id FROM risk_observations_latest
            WHERE trade_date = ?
            UNION
            SELECT candidate_instance_id FROM entry_timing_evaluations
            WHERE trade_date = ?
            UNION
            SELECT candidate_instance_id FROM order_plan_drafts_latest
            WHERE trade_date = ?
        )
        SELECT COUNT(*) AS count FROM pipeline_candidates
        """,
        (trade_date, trade_date, trade_date, trade_date),
    ).fetchone()
    return int(row["count"] if row else 0)


def _add_broker_boundary_check(add: Any, connection: sqlite3.Connection) -> None:
    try:
        status = get_order_broker_boundary_status(connection)
        passed = bool(
            int(status.get("effective_unconfirmed_count") or 0) == 0
            and status.get("effective_block_new_order_routing") is False
            and status.get("resolution_maintenance_fence_active") is False
            and not status.get("reason_codes")
        )
        details = {
            "effective_status": status.get("effective_status"),
            "raw_unconfirmed_count": status.get("raw_unconfirmed_count"),
            "effective_unconfirmed_count": status.get("effective_unconfirmed_count"),
            "effective_block_new_order_routing": status.get(
                "effective_block_new_order_routing"
            ),
            "resolution_maintenance_fence_active": status.get(
                "resolution_maintenance_fence_active"
            ),
            "reason_codes": status.get("reason_codes"),
        }
    except Exception as exc:
        passed = False
        details = {"classifier_error_type": type(exc).__name__}
    add("broker_boundary", passed, "FAST5_BROKER_BOUNDARY_BLOCKED", details)


def _add_lifecycle_check(add: Any, connection: sqlite3.Connection) -> None:
    try:
        status = build_live_sim_execution_lifecycle_status(connection)
        passed = bool(
            status.get("qualification_status") == "PASS"
            and int(status.get("effective_blocker_count") or 0) == 0
            and status.get("classifier_fail_closed") is not True
        )
        details = {
            "qualification_status": status.get("qualification_status"),
            "effective_blocker_count": status.get("effective_blocker_count"),
            "classifier_fail_closed": status.get("classifier_fail_closed"),
            "reason_codes": status.get("reason_codes"),
        }
    except Exception as exc:
        passed = False
        details = {"classifier_error_type": type(exc).__name__}
    add("execution_lifecycle", passed, "FAST5_LIFECYCLE_BLOCKED", details)


def _add_broker_reconcile_check(
    add: Any,
    connection: sqlite3.Connection,
    settings: Settings,
    trade_date: str,
) -> None:
    latest = get_latest_live_sim_reconcile(connection)
    snapshot_json = _mapping((latest or {}).get("snapshot_json"))
    broker_snapshot = _mapping(snapshot_json.get("broker_snapshot"))
    snapshot_at = str(broker_snapshot.get("snapshot_at") or "")
    fresh, age_sec = _fresh_snapshot(
        snapshot_at,
        int(
            broker_snapshot.get("stale_after_sec")
            or settings.live_sim_broker_snapshot_stale_sec
        ),
    )
    passed = bool(
        latest
        and settings.live_sim_reconcile_request_broker_snapshot_enabled
        and str(latest.get("status") or "").upper() == "OK"
        and int(latest.get("mismatch_count") or 0) == 0
        and latest.get("blocking_new_buy") is False
        and canonical_snapshot_status(broker_snapshot.get("snapshot_status"))
        is BrokerSnapshotStatus.COMPLETE
        and broker_snapshot.get("complete") is True
        and fresh
        and str(broker_snapshot.get("trade_date") or "") == trade_date
    )
    add(
        "broker_reconcile",
        passed,
        "FAST5_BROKER_RECONCILE_NON_PASS",
        {
            "reconcile_id": (latest or {}).get("reconcile_id"),
            "broker_snapshot_requests_enabled": (
                settings.live_sim_reconcile_request_broker_snapshot_enabled
            ),
            "reconcile_status": (latest or {}).get("status"),
            "mismatch_count": (latest or {}).get("mismatch_count"),
            "blocking_new_buy": (latest or {}).get("blocking_new_buy"),
            "snapshot_id": broker_snapshot.get("snapshot_id"),
            "snapshot_status": broker_snapshot.get("snapshot_status"),
            "snapshot_complete": broker_snapshot.get("complete"),
            "snapshot_age_sec": age_sec,
            "snapshot_fresh": fresh,
            "snapshot_trade_date": broker_snapshot.get("trade_date"),
            "expected_trade_date": trade_date,
        },
    )


def _bootstrap_usage(
    connection: sqlite3.Connection,
    approval_sha256: str,
) -> dict[str, Any]:
    if not _is_sha256(approval_sha256):
        return {
            "consumed": False,
            "approval_sha256": approval_sha256 or None,
            "reservation_run_id": None,
            "reserved_at": None,
        }
    rows = connection.execute(
        """
        SELECT run_id, reason_summary_json, created_at
        FROM live_sim_operating_runs
        WHERE status = 'BOOTSTRAP_RESERVED'
        ORDER BY created_at DESC, run_id DESC
        """
    ).fetchall()
    for row in rows:
        try:
            summary = json.loads(str(row["reason_summary_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        bootstrap = _mapping(_mapping(summary).get("fast5_bootstrap"))
        if bootstrap.get("approval_sha256") != approval_sha256:
            continue
        return {
            "consumed": True,
            "approval_sha256": approval_sha256,
            "reservation_run_id": str(row["run_id"]),
            "reserved_at": str(row["created_at"]),
        }
    return {
        "consumed": False,
        "approval_sha256": approval_sha256,
        "reservation_run_id": None,
        "reserved_at": None,
    }


def _reserve_bootstrap_once(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    gate: Fast5AutomaticCanaryGate,
    preflight: LiveSimPreflightResult,
) -> tuple[bool, str | None]:
    if connection.in_transaction:
        raise RuntimeError("FAST-5 bootstrap reservation requires a clean transaction")
    approval_sha256 = settings.live_sim_fast5_bootstrap_approval_sha256
    try:
        connection.execute("BEGIN IMMEDIATE")
        usage = _bootstrap_usage(connection, approval_sha256)
        if usage.get("consumed"):
            connection.rollback()
            return False, str(usage.get("reservation_run_id") or "") or None
        reservation_run_id = new_message_id("fast5_bootstrap_reservation")
        reservation = LiveSimOperatingRunResult(
            run_id=reservation_run_id,
            trade_date=gate.trade_date,
            mode=OperatingMode.PILOT_BUY_ONLY,
            queue_commands=False,
            preflight=preflight,
            status="BOOTSTRAP_RESERVED",
            reason_summary={
                "policy": FAST5_POLICY_VERSION,
                "rollback_latched": False,
                "fast5_bootstrap": {
                    "contract": FAST5_BOOTSTRAP_APPROVAL_CONTRACT,
                    "approval_id": settings.live_sim_fast5_bootstrap_approval_id,
                    "approval_sha256": approval_sha256,
                    "trade_date": gate.trade_date,
                    "one_shot_reserved": True,
                },
            },
        )
        save_live_sim_operating_run(connection, reservation)
        connection.commit()
        return True, reservation_run_id
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise


def _run_consumed_bootstrap_protection(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    gate: Fast5AutomaticCanaryGate,
    run_id: str,
    limit: int | None,
) -> Fast5AutomaticCanaryRunResult:
    strict = _strict_fast5_settings(settings, bootstrap=True)
    try:
        operating = run_live_sim_operating_cycle_once(
            connection,
            settings=replace(strict, live_sim_operating_write_runs=False),
            mode=OperatingMode.PROTECT_ONLY,
            queue_commands=True,
            trade_date=gate.trade_date,
            limit=limit,
            include_ai=False,
            include_no_buy=False,
        )
    except Exception as exc:
        failed = LiveSimOperatingRunResult(
            run_id=run_id,
            trade_date=gate.trade_date,
            mode=OperatingMode.PROTECT_ONLY,
            queue_commands=True,
            preflight=LiveSimPreflightResult(
                status=PreflightStatus.BLOCK,
                mode=OperatingMode.PROTECT_ONLY,
                queue_commands=True,
                blocking_reasons=("FAST5_PROTECT_ONLY_CYCLE_ERROR",),
            ),
            status="PROTECT_ONLY",
            errors=(
                {
                    "stage": "fast5_bootstrap_protect_only_cycle",
                    "error_type": type(exc).__name__,
                },
            ),
            reason_summary={
                "policy": FAST5_POLICY_VERSION,
                "fast5_automatic_canary": gate.to_dict(),
                "fast5_bootstrap": {
                    "contract": FAST5_BOOTSTRAP_APPROVAL_CONTRACT,
                    "approval_id": settings.live_sim_fast5_bootstrap_approval_id,
                    "approval_sha256": settings.live_sim_fast5_bootstrap_approval_sha256,
                    "trade_date": gate.trade_date,
                    "one_shot_consumed": True,
                    "protect_only_lifecycle": True,
                },
                "rollback_latched": True,
            },
        )
        _save_fast5_operating_run(connection, failed, settings)
        return Fast5AutomaticCanaryRunResult(
            run_id=run_id,
            status="PROTECT_ONLY",
            mode=OperatingMode.PROTECT_ONLY,
            queue_commands_requested=True,
            queue_commands_effective=True,
            gate=gate,
            operating_run={
                **failed.to_dict(),
                "no_order_side_effects": False,
                "side_effects_unknown": True,
            },
            rollback_latched=True,
            side_effects_unknown=True,
        )
    invariant_errors: list[dict[str, Any]] = []
    if operating.buy_command_count:
        invariant_errors.append(
            {
                "stage": "fast5_bootstrap_protect_only",
                "error": "Protect-only lifecycle created a BUY command.",
            }
        )
    rollback_latched = bool(operating.errors or invariant_errors)
    enriched = replace(
        operating,
        run_id=run_id,
        status="PROTECT_ONLY",
        errors=tuple([*operating.errors, *invariant_errors]),
        reason_summary={
            **dict(operating.reason_summary),
            "policy": FAST5_POLICY_VERSION,
            "fast5_automatic_canary": gate.to_dict(),
            "fast5_bootstrap": {
                "contract": FAST5_BOOTSTRAP_APPROVAL_CONTRACT,
                "approval_id": settings.live_sim_fast5_bootstrap_approval_id,
                "approval_sha256": settings.live_sim_fast5_bootstrap_approval_sha256,
                "trade_date": gate.trade_date,
                "one_shot_consumed": True,
                "protect_only_lifecycle": True,
            },
            "rollback_latched": rollback_latched,
        },
    )
    _save_fast5_operating_run(connection, enriched, settings)
    return Fast5AutomaticCanaryRunResult(
        run_id=run_id,
        status="PROTECT_ONLY",
        mode=OperatingMode.PROTECT_ONLY,
        queue_commands_requested=True,
        queue_commands_effective=(
            operating.preflight.status is PreflightStatus.PASS
        ),
        gate=gate,
        operating_run=enriched.to_dict(),
        rollback_latched=rollback_latched,
    )


def _latest_unacknowledged_rollback(
    connection: sqlite3.Connection,
    settings: Settings,
) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT run_id, reason_summary_json
        FROM live_sim_operating_runs
        WHERE status = 'PROTECT_ONLY'
        ORDER BY created_at DESC, run_id DESC
        """
    ).fetchall()
    for row in rows:
        try:
            summary = _mapping(json.loads(str(row["reason_summary_json"] or "{}")))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if summary.get("policy") not in FAST5_ROLLBACK_POLICY_VERSIONS:
            continue
        if summary.get("rollback_latched") is not True:
            continue
        run_id = str(row["run_id"] or "")
        acknowledged = settings.live_sim_fast5_rollback_ack_run_id == run_id
        return {
            "latched_run_id": None if acknowledged else run_id,
            "latest_rollback_run_id": run_id,
            "acknowledged": acknowledged,
            "acknowledgement_required": not acknowledged,
        }
    return {
        "latched_run_id": None,
        "latest_rollback_run_id": None,
        "acknowledged": False,
        "acknowledgement_required": False,
    }


def _strict_fast5_settings(
    settings: Settings,
    *,
    bootstrap: bool = False,
) -> Settings:
    max_order_notional = min(
        settings.live_sim_max_order_notional,
        settings.live_sim_order_plan_max_notional,
        settings.live_sim_fast5_max_order_notional,
    )
    max_daily_count = min(
        settings.live_sim_max_daily_order_count,
        1 if bootstrap else settings.live_sim_fast5_max_daily_buy_count,
    )
    return replace(
        settings,
        live_sim_max_order_notional=max_order_notional,
        live_sim_order_plan_max_notional=max_order_notional,
        live_sim_max_daily_order_count=max_daily_count,
        live_sim_max_daily_notional=min(
            settings.live_sim_max_daily_notional,
            max_order_notional * max_daily_count,
        ),
        live_sim_max_active_orders=1,
        live_sim_max_active_positions=1,
        live_sim_operating_max_buy_commands_per_cycle=1,
        live_sim_order_plan_max_commands_per_run=1,
        live_sim_allow_market_order=False,
        live_sim_order_plan_allow_market_order=False,
        live_sim_default_order_type="LIMIT",
        live_sim_order_plan_allowed_side="BUY",
        live_sim_position_allow_scale_in=False,
        live_sim_reprice_enabled=False,
        live_sim_reconcile_enabled=False,
    )


def _effective_limits(
    settings: Settings,
    *,
    bootstrap: bool = False,
) -> dict[str, Any]:
    strict = _strict_fast5_settings(settings, bootstrap=bootstrap)
    return {
        "exchange": "KRX",
        "allowed_side": "BUY",
        "order_type": "LIMIT",
        "max_buy_commands_per_cycle": 1,
        "max_daily_buy_count": strict.live_sim_max_daily_order_count,
        "max_order_notional": strict.live_sim_max_order_notional,
        "max_daily_notional": strict.live_sim_max_daily_notional,
        "max_active_orders": 1,
        "max_active_positions": 1,
        "scale_in_allowed": False,
        "ai_routing_effect": 0,
    }


def _save_fast5_operating_run(
    connection: sqlite3.Connection,
    result: LiveSimOperatingRunResult,
    settings: Settings,
) -> None:
    if not settings.live_sim_operating_write_runs:
        return
    save_live_sim_operating_run(connection, result)
    connection.commit()


def _fresh_snapshot(snapshot_at: str, stale_after_sec: int) -> tuple[bool, float | None]:
    if not snapshot_at:
        return False, None
    try:
        parsed = parse_timestamp(snapshot_at, "snapshot_at")
    except (TypeError, ValueError):
        return False, None
    age_sec = (utc_now() - parsed).total_seconds()
    return -5 <= age_sec <= stale_after_sec, round(max(age_sec, 0.0), 3)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _is_sha256(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)
