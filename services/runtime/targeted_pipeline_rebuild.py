from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

from domain.broker.utils import new_message_id, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import (
    DEFAULT_ENV_FILE_PATH,
    ENV_FILE_PATH_ENV,
    Settings,
    TradingMode,
    TradingProfile,
    load_settings,
)
from services.entry_timing.service import evaluate_entry_timing
from services.pipeline_coherency import resolve_candidate_source_lineage
from services.risk_gate import (
    evaluate_risk_for_strategy_observation,
    save_risk_observation,
)
from services.runtime.evaluation_run_guard import (
    EVALUATION_PIPELINE_LOCK,
    RuntimeExecutionLease,
    assert_runtime_execution_fence,
    immediate_transaction,
    runtime_execution_lock,
)
from services.strategy_engine import (
    evaluate_candidate_strategy,
    save_strategy_observation,
)

MAX_TARGETED_PIPELINE_REBUILD_BATCH_SIZE = 5
TARGETED_PIPELINE_REBUILD_LOCK_TTL_SEC = 600
TARGETED_PIPELINE_REBUILD_MAX_WALL_SEC = 300.0
TARGETED_PIPELINE_REBUILD_PROGRESS_OPCODES = 1_000
TARGETED_PIPELINE_RECONCILIATION_MAX_SEC = 30.0
TARGETED_PIPELINE_CURRENT_ACTIVE_STATES = frozenset({"CONTEXT_READY"})

_FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off"})
_REQUIRED_EXPLICIT_ENV = frozenset(
    {
        "TRADING_PROFILE",
        "TRADING_MODE",
        "TRADING_ALLOW_LIVE_SIM",
        "TRADING_ALLOW_LIVE_REAL",
        "LIVE_SIM_KILL_SWITCH",
        "TRADING_DB_PATH",
        "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS",
        "INCREMENTAL_EVALUATION_WORKER_ENABLED",
        "ENTRY_TIMING_WRITE_ORDER_PLAN_DRAFTS",
        "STRATEGY_ENGINE_ENABLED",
        "STRATEGY_ENGINE_OBSERVE_ONLY",
        "RISK_GATE_ENABLED",
        "RISK_GATE_OBSERVE_ONLY",
        "ENTRY_TIMING_ENABLED",
    }
)
_COMMAND_PRODUCER_SETTINGS = (
    "realtime_subscription_queue_commands",
    "dry_run_oms_enabled",
    "dry_run_intent_creation_enabled",
    "dry_run_order_routing_enabled",
    "dry_run_gateway_command_enabled",
    "dry_run_simulated_fill_enabled",
    "dry_run_exit_engine_enabled",
    "dry_run_exit_intent_creation_enabled",
    "dry_run_exit_order_creation_enabled",
    "dry_run_exit_order_routing_enabled",
    "dry_run_exit_gateway_command_enabled",
    "dry_run_exit_simulated_fill_enabled",
    "live_sim_enabled",
    "live_sim_order_routing_enabled",
    "live_sim_gateway_command_enabled",
    "live_sim_reprice_enabled",
    "live_sim_pilot_pipeline_enabled",
    "live_sim_pilot_auto_queue_command",
    "live_sim_order_plan_routing_enabled",
    "live_sim_cancel_enabled",
    "live_sim_cancel_unfilled_enabled",
    "live_sim_exit_engine_enabled",
    "live_sim_exit_order_creation_enabled",
    "live_sim_exit_gateway_command_enabled",
    "live_sim_exit_eod_flatten_enabled",
    "live_sim_reconcile_request_broker_snapshot_enabled",
    "live_sim_operating_cycle_enabled",
    "live_sim_operating_loop_enabled",
    "live_sim_operating_loop_queue_commands",
    "live_sim_lifecycle_consumer_enabled",
    "live_sim_lifecycle_worker_enabled",
    "live_sim_lifecycle_cutover_dry_run_enabled",
    "live_sim_lifecycle_cutover_enabled",
    "live_sim_lifecycle_inline_fallback_enabled",
    "projection_outbox_worker_enabled",
    "projection_outbox_apply_projection_enabled",
    "projection_outbox_market_data_apply_enabled",
    "projection_outbox_market_reference_apply_enabled",
    "projection_outbox_market_index_apply_enabled",
    "projection_outbox_market_regime_apply_enabled",
    "projection_outbox_market_scan_apply_enabled",
)
_COMMAND_PRODUCER_ENV_NAMES = frozenset(
    field_name.upper() for field_name in _COMMAND_PRODUCER_SETTINGS
)
_REQUIRED_EXPLICIT_ENV = _REQUIRED_EXPLICIT_ENV | _COMMAND_PRODUCER_ENV_NAMES
_SAFETY_SETTING_NAMES = (
    "trading_profile",
    "trading_mode",
    "trading_allow_live_sim",
    "trading_allow_live_real",
    "trading_db_path",
    "live_sim_kill_switch",
    "incremental_evaluation_worker_enabled",
    *_COMMAND_PRODUCER_SETTINGS,
)
_ORDER_ARTIFACT_TABLES = (
    "order_plan_drafts",
    "order_plan_drafts_latest",
    "dry_run_intents",
    "dry_run_orders",
    "dry_run_executions",
    "dry_run_exit_intents",
    "dry_run_exit_orders",
    "dry_run_exit_executions",
    "live_sim_intents",
    "live_sim_orders",
    "live_sim_executions",
    "live_sim_exit_intents",
    "live_sim_cancel_intents",
    "gateway_commands",
    "gateway_command_events",
    "gateway_command_dedupe_keys",
    "gateway_events",
    "gateway_order_broker_boundaries",
    "gateway_order_broker_boundary_resolutions",
)
_ORDER_COMMAND_TYPES = ("send_order", "cancel_order", "modify" + "_order")
_PIPELINE_WRITE_TABLES = frozenset(
    {
        "strategy_observations",
        "strategy_observations_latest",
        "strategy_setup_observations",
        "risk_observations",
        "risk_observations_latest",
        "risk_check_observations",
        "entry_timing_evaluations",
        "entry_timing_evaluation_errors",
        "runtime_execution_locks",
        "runtime_execution_lock_fences",
        "sqlite_sequence",
    }
)
_SQLITE_WRITE_ACTIONS = frozenset(
    {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
)
_SQLITE_ALWAYS_DENIED_ACTIONS = frozenset(
    action
    for action_name in (
        "SQLITE_TRANSACTION",
        "SQLITE_SAVEPOINT",
        "SQLITE_ATTACH",
        "SQLITE_DETACH",
        "SQLITE_PRAGMA",
        "SQLITE_ALTER_TABLE",
        "SQLITE_ANALYZE",
        "SQLITE_CREATE_INDEX",
        "SQLITE_CREATE_TABLE",
        "SQLITE_CREATE_TEMP_INDEX",
        "SQLITE_CREATE_TEMP_TABLE",
        "SQLITE_CREATE_TEMP_TRIGGER",
        "SQLITE_CREATE_TEMP_VIEW",
        "SQLITE_CREATE_TRIGGER",
        "SQLITE_CREATE_VIEW",
        "SQLITE_CREATE_VTABLE",
        "SQLITE_DROP_INDEX",
        "SQLITE_DROP_TABLE",
        "SQLITE_DROP_TEMP_INDEX",
        "SQLITE_DROP_TEMP_TABLE",
        "SQLITE_DROP_TEMP_TRIGGER",
        "SQLITE_DROP_TEMP_VIEW",
        "SQLITE_DROP_TRIGGER",
        "SQLITE_DROP_VIEW",
        "SQLITE_DROP_VTABLE",
        "SQLITE_REINDEX",
    )
    if isinstance((action := getattr(sqlite3, action_name, None)), int)
)

_PIPELINE_STAGE_TABLES = (
    "strategy_observations",
    "strategy_observations_latest",
    "strategy_setup_observations",
    "risk_observations",
    "risk_observations_latest",
    "risk_check_observations",
    "entry_timing_evaluations",
    "entry_timing_evaluation_errors",
)
_PIPELINE_INSERT_DELTA_KEYS = {
    "strategy_observations": "strategy_observation_count",
    "strategy_setup_observations": "strategy_setup_observation_count",
    "risk_observations": "risk_observation_count",
    "risk_check_observations": "risk_check_observation_count",
    "entry_timing_evaluations": "entry_timing_evaluation_count",
    "entry_timing_evaluation_errors": "entry_timing_error_count",
}


@dataclass(frozen=True)
class _AuditedEnvContract:
    path: Path
    sha256: str
    device: int
    inode: int
    size: int
    mtime_ns: int
    link_count: int


@dataclass(frozen=True)
class _TransactionReconciliation:
    outcome: str
    evidence: dict[str, Any]
    artifact_snapshot_after: dict[str, Any] | None = None
    pipeline_stage_delta: dict[str, Any] | None = None


class TargetedPipelineRebuildError(RuntimeError):
    def __init__(
        self,
        *reason_codes: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        normalized = tuple(dict.fromkeys(str(code) for code in reason_codes if code))
        if not normalized:
            normalized = ("TARGETED_PIPELINE_REBUILD_REJECTED",)
        super().__init__(", ".join(normalized))
        self.reason_codes = normalized
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "TARGETED_PIPELINE_REBUILD_REJECTED",
            "reason_codes": list(self.reason_codes),
            "details": dict(self.details),
            "no_order_plans_created": True,
            "no_order_commands_created": True,
            "live_sim_allowed": False,
            "live_real_allowed": False,
        }


def preview_targeted_pipeline_rebuild(
    connection: sqlite3.Connection,
    candidate_instance_ids: Sequence[str],
    *,
    trade_date: str,
    settings: Settings | None = None,
    environ: Mapping[str, str] | None = None,
    not_order_intent: bool = True,
) -> dict[str, Any]:
    _require_not_order_intent(not_order_intent)
    candidate_ids = _normalize_candidate_ids(candidate_instance_ids)
    target_trade_date = _normalize_trade_date(trade_date)
    resolved_settings, safety, _env_contract = _validate_runtime_safety(
        connection,
        settings=settings,
        environ=environ,
    )
    source_run_id = new_message_id("fast0r3_source_run")
    candidates, blocked = _classify_candidates(
        connection,
        candidate_ids,
        trade_date=target_trade_date,
        settings=resolved_settings,
        source_run_id=source_run_id,
    )
    _require_all_candidates_eligible(candidate_ids, candidates, blocked)
    artifact_snapshot = _order_artifact_snapshot(connection)
    return {
        "status": "ELIGIBLE",
        "eligible": True,
        "trade_date": target_trade_date,
        "candidate_instance_ids": candidate_ids,
        "candidate_count": len(candidate_ids),
        "source_run_id": source_run_id,
        "candidates": [_public_candidate(item) for item in candidates],
        "runtime_safety": safety,
        "artifact_snapshot": artifact_snapshot,
        "read_only": True,
        "observe_only": True,
        "not_order_intent": True,
        "no_order_side_effects": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }


def run_targeted_pipeline_rebuild(
    connection: sqlite3.Connection,
    candidate_instance_ids: Sequence[str],
    *,
    trade_date: str,
    settings: Settings | None = None,
    environ: Mapping[str, str] | None = None,
    not_order_intent: bool = True,
) -> dict[str, Any]:
    started_monotonic = time.monotonic()
    _require_not_order_intent(not_order_intent)
    _normalize_candidate_ids(candidate_instance_ids)
    _normalize_trade_date(trade_date)
    if connection.in_transaction:
        raise TargetedPipelineRebuildError(
            "PREEXISTING_TRANSACTION_FORBIDDEN"
        )
    deadline_monotonic = started_monotonic + TARGETED_PIPELINE_REBUILD_MAX_WALL_SEC
    connection.set_progress_handler(
        lambda: int(time.monotonic() > deadline_monotonic),
        TARGETED_PIPELINE_REBUILD_PROGRESS_OPCODES,
    )
    try:
        return _run_targeted_pipeline_rebuild(
            connection,
            candidate_instance_ids,
            trade_date=trade_date,
            settings=settings,
            environ=environ,
            not_order_intent=not_order_intent,
            started_monotonic=started_monotonic,
            deadline_monotonic=deadline_monotonic,
        )
    except sqlite3.OperationalError as exc:
        if time.monotonic() > deadline_monotonic and "interrupt" in str(exc).lower():
            raise TargetedPipelineRebuildError(
                "TARGETED_REBUILD_WALL_TIME_EXCEEDED",
                details={"stage": "SQL_PROGRESS_HANDLER"},
            ) from exc
        raise
    finally:
        connection.set_progress_handler(None, 0)


def _run_targeted_pipeline_rebuild(
    connection: sqlite3.Connection,
    candidate_instance_ids: Sequence[str],
    *,
    trade_date: str,
    settings: Settings | None,
    environ: Mapping[str, str] | None,
    not_order_intent: bool,
    started_monotonic: float,
    deadline_monotonic: float,
) -> dict[str, Any]:
    _require_not_order_intent(not_order_intent)
    candidate_ids = _normalize_candidate_ids(candidate_instance_ids)
    target_trade_date = _normalize_trade_date(trade_date)
    resolved_settings, safety, env_contract = _validate_runtime_safety(
        connection,
        settings=settings,
        environ=environ,
    )
    source_run_id = new_message_id("fast0r3_source_run")

    # Reject empty/ineligible work before acquiring a lease or changing its fence.
    candidates, blocked = _classify_candidates(
        connection,
        candidate_ids,
        trade_date=target_trade_date,
        settings=resolved_settings,
        source_run_id=source_run_id,
    )
    _require_all_candidates_eligible(candidate_ids, candidates, blocked)
    _order_artifact_snapshot(connection)
    _assert_deadline(deadline_monotonic, stage="PREVALIDATION_COMPLETED")

    owner_id = new_message_id("fast0r3_targeted_rebuild")
    lock_context = runtime_execution_lock(
        connection,
        EVALUATION_PIPELINE_LOCK,
        owner_id=owner_id,
        ttl_sec=TARGETED_PIPELINE_REBUILD_LOCK_TTL_SEC,
        heartbeat_interval_sec=0.0,
        details={
            "run_type": "fast0r3_targeted_pipeline_rebuild",
            "trade_date": target_trade_date,
            "candidate_count": len(candidate_ids),
            "source_run_id": source_run_id,
        },
    )
    lease = lock_context.__enter__()
    transaction_ready_for_commit = False
    transaction_finalization_error: Exception | None = None
    transaction_reconciliation: _TransactionReconciliation | None = None
    try:
        if lease is None:
            raise TargetedPipelineRebuildError("EVALUATION_PIPELINE_LEASE_MISSING")
        try:
            with immediate_transaction(connection, lease=lease):
                assert_runtime_execution_fence(connection, lease=lease, renew=True)
                transaction_candidates, transaction_blocked = _classify_candidates(
                    connection,
                    candidate_ids,
                    trade_date=target_trade_date,
                    settings=resolved_settings,
                    source_run_id=source_run_id,
                )
                _require_all_candidates_eligible(
                    candidate_ids,
                    transaction_candidates,
                    transaction_blocked,
                )
                _assert_same_source_snapshots(candidates, transaction_candidates)
                _assert_deadline(deadline_monotonic, stage="TRANSACTION_VALIDATED")

                before = _order_artifact_snapshot(connection)
                stage_before = _pipeline_stage_snapshot(
                    connection,
                    candidate_ids,
                    deadline_monotonic=deadline_monotonic,
                )
                results: list[dict[str, Any]] = []
                connection.set_authorizer(_pipeline_write_authorizer)
                try:
                    for candidate in transaction_candidates:
                        results.append(
                            _run_candidate_pipeline(
                                connection,
                                candidate,
                                trade_date=target_trade_date,
                                settings=resolved_settings,
                                source_run_id=source_run_id,
                                lease=lease,
                                started_monotonic=started_monotonic,
                            )
                        )

                    assert_runtime_execution_fence(connection, lease=lease, renew=True)
                    _assert_audited_env_unchanged(env_contract)
                    _assert_persisted_row_fingerprints(connection, results)
                    after = _order_artifact_snapshot(connection)
                    stage_after = _pipeline_stage_snapshot(
                        connection,
                        candidate_ids,
                        deadline_monotonic=deadline_monotonic,
                    )
                    stage_delta = _assert_pipeline_stage_delta(
                        stage_before,
                        stage_after,
                        results,
                        candidate_ids,
                    )
                    changed_tables = [
                        table_name
                        for table_name in _ORDER_ARTIFACT_TABLES
                        if before["tables"][table_name]
                        != after["tables"][table_name]
                    ]
                    if before["command_type_counts"] != after["command_type_counts"]:
                        changed_tables.append("gateway_commands:send_cancel_modify")
                    if changed_tables:
                        raise TargetedPipelineRebuildError(
                            "ORDER_ARTIFACT_INVARIANT_CHANGED",
                            details={"changed_tables": changed_tables},
                        )
                    _assert_deadline(deadline_monotonic, stage="PRE_COMMIT")
                    transaction_ready_for_commit = True
                finally:
                    connection.set_authorizer(None)
        except Exception as exc:
            if not transaction_ready_for_commit:
                if (
                    isinstance(exc, sqlite3.OperationalError)
                    and time.monotonic() > deadline_monotonic
                    and "interrupt" in str(exc).lower()
                ):
                    raise TargetedPipelineRebuildError(
                        "TARGETED_REBUILD_WALL_TIME_EXCEEDED",
                        details={"stage": "SQL_PROGRESS_HANDLER"},
                    ) from exc
                raise
            transaction_finalization_error = exc
            transaction_reconciliation = _reconcile_transaction_finalization(
                connection,
                candidate_ids=candidate_ids,
                trade_date=target_trade_date,
                settings=resolved_settings,
                source_run_id=source_run_id,
                expected_candidates=transaction_candidates,
                expected_items=results,
                artifact_snapshot_before=before,
                pipeline_stage_before=stage_before,
                env_contract=env_contract,
                lease=lease,
            )
            if transaction_reconciliation.outcome == "NOT_COMMITTED_CONFIRMED":
                raise
    except Exception:
        exception_info = sys.exc_info()
        lock_context.__exit__(*exception_info)
        raise

    if transaction_reconciliation is None:
        result_status = "COMPLETED"
        data_committed: bool | None = True
        transaction_outcome: dict[str, Any] = {
            "outcome": "COMMITTED",
            "commit_signal_received": True,
            "reconciled_after_error": False,
        }
        artifact_snapshot_basis = "PRECOMMIT_AND_COMMIT_SIGNAL"
    else:
        if transaction_reconciliation.artifact_snapshot_after is not None:
            after = transaction_reconciliation.artifact_snapshot_after
        if transaction_reconciliation.pipeline_stage_delta is not None:
            stage_delta = transaction_reconciliation.pipeline_stage_delta
        if transaction_reconciliation.outcome == "COMMITTED_RECONCILED":
            result_status = "COMMITTED_TRANSACTION_SIGNAL_FAILED"
            data_committed = True
            artifact_snapshot_basis = "POST_ERROR_RECONCILIATION"
        else:
            result_status = "OUTCOME_UNKNOWN"
            data_committed = None
            artifact_snapshot_basis = "PRECOMMIT_ONLY"
        transaction_outcome = {
            **transaction_reconciliation.evidence,
            "outcome": transaction_reconciliation.outcome,
            "commit_signal_received": False,
            "reconciled_after_error": True,
            "error_type": type(transaction_finalization_error).__name__,
        }

    result: dict[str, Any] = {
        "status": result_status,
        "data_committed": data_committed,
        "trade_date": target_trade_date,
        "candidate_instance_ids": candidate_ids,
        "candidate_count": len(candidate_ids),
        "source_run_id": source_run_id,
        "owner_id": owner_id,
        "fencing_token": lease.fencing_token,
        "items": results,
        "runtime_safety": safety,
        "artifact_snapshot_before": before,
        "artifact_snapshot_after": after,
        "artifact_snapshot_basis": artifact_snapshot_basis,
        "pipeline_stage_delta": stage_delta,
        "order_artifacts_unchanged": before == after,
        "command_type_counts_unchanged": (
            before["command_type_counts"] == after["command_type_counts"]
        ),
        "single_evaluation_pipeline_lease": True,
        "single_source_run": True,
        "lock_ttl_sec": TARGETED_PIPELINE_REBUILD_LOCK_TTL_SEC,
        "max_wall_sec": TARGETED_PIPELINE_REBUILD_MAX_WALL_SEC,
        "observe_only": True,
        "not_order_intent": True,
        "no_order_plans_created": True,
        "no_order_commands_created": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
        "transaction_outcome": transaction_outcome,
        "failure_codes": (
            [] if result_status == "COMPLETED" else [result_status]
        ),
    }
    if result_status != "COMPLETED":
        result["operator_action_required"] = True

    if (
        result_status == "OUTCOME_UNKNOWN"
        and transaction_reconciliation is not None
        and transaction_reconciliation.evidence.get(
            "connection_in_transaction_after_reconciliation"
        )
    ):
        assert transaction_finalization_error is not None
        cleanup_error: Exception | None = None
        try:
            lock_context.__exit__(
                type(transaction_finalization_error),
                transaction_finalization_error,
                transaction_finalization_error.__traceback__,
            )
        except Exception as exc:
            cleanup_error = exc
        result["lock_cleanup"] = {
            "status": "FAILED" if cleanup_error is not None else "UNKNOWN",
            "reason_code": "OUTCOME_UNKNOWN_TRANSACTION_STILL_OPEN",
            "lock_name": lease.lock_name,
            "owner_id": lease.owner_id,
            "fencing_token": lease.fencing_token,
            "operator_action_required": True,
        }
        if cleanup_error is not None:
            result["lock_cleanup"]["error_type"] = type(cleanup_error).__name__
            result["failure_codes"] = [
                *result["failure_codes"],
                "LOCK_CLEANUP_FAILED_WITH_OUTCOME_UNKNOWN",
            ]
        return result
    try:
        lock_context.__exit__(None, None, None)
    except Exception as exc:
        cleanup_reason = (
            "COMMITTED_CLEANUP_FAILED"
            if data_committed is True
            else "LOCK_CLEANUP_FAILED_WITH_OUTCOME_UNKNOWN"
        )
        if result_status == "COMPLETED":
            result["status"] = "COMMITTED_CLEANUP_FAILED"
        result["failure_codes"] = [
            *result["failure_codes"],
            cleanup_reason,
        ]
        result["lock_cleanup"] = {
            "status": "FAILED",
            "reason_code": cleanup_reason,
            "error_type": type(exc).__name__,
            "lock_name": lease.lock_name,
            "owner_id": lease.owner_id,
            "fencing_token": lease.fencing_token,
            "operator_action_required": True,
        }
        result["operator_action_required"] = True
        return result
    result["lock_cleanup"] = {
        "status": "COMPLETED",
        "lock_name": lease.lock_name,
        "fencing_token": lease.fencing_token,
    }
    result["operator_action_required"] = result_status != "COMPLETED"
    return result


def _run_candidate_pipeline(
    connection: sqlite3.Connection,
    candidate: Mapping[str, Any],
    *,
    trade_date: str,
    settings: Settings,
    source_run_id: str,
    lease: RuntimeExecutionLease,
    started_monotonic: float,
) -> dict[str, Any]:
    candidate_id = str(candidate["candidate_instance_id"])
    expected_lineage = dict(candidate["lineage"])

    _assert_stage_fence(
        connection,
        lease,
        "SOURCE_VALIDATE",
        started_monotonic=started_monotonic,
    )
    current = _require_current_candidate(
        connection,
        candidate_id,
        trade_date=trade_date,
        settings=settings,
        source_run_id=source_run_id,
    )
    _assert_same_source_snapshots([candidate], [current])

    _assert_stage_fence(
        connection,
        lease,
        "CANDIDATE_VALIDATE",
        started_monotonic=started_monotonic,
    )
    strategy = evaluate_candidate_strategy(
        connection,
        candidate_id,
        settings=settings,
    )
    if strategy.candidate_instance_id != candidate_id or strategy.trade_date != trade_date:
        raise TargetedPipelineRebuildError("STRATEGY_TARGET_MISMATCH")
    if not strategy.observe_only:
        raise TargetedPipelineRebuildError("STRATEGY_NOT_OBSERVE_ONLY")
    save_strategy_observation(
        connection,
        strategy,
        source_run_id=source_run_id,
        source_lineage=expected_lineage,
    )
    _assert_stage_fence(
        connection,
        lease,
        "STRATEGY_SAVED",
        started_monotonic=started_monotonic,
    )

    risk = evaluate_risk_for_strategy_observation(
        connection,
        strategy.strategy_observation_id,
        settings=settings,
    )
    if risk.candidate_instance_id != candidate_id or risk.trade_date != trade_date:
        raise TargetedPipelineRebuildError("RISK_TARGET_MISMATCH")
    if risk.strategy_observation_id != strategy.strategy_observation_id:
        raise TargetedPipelineRebuildError("RISK_STRATEGY_FK_MISMATCH")
    if not risk.observe_only:
        raise TargetedPipelineRebuildError("RISK_NOT_OBSERVE_ONLY")
    save_risk_observation(
        connection,
        risk,
        source_run_id=source_run_id,
        source_lineage=expected_lineage,
    )
    _assert_stage_fence(
        connection,
        lease,
        "RISK_SAVED",
        started_monotonic=started_monotonic,
    )

    entry_result = evaluate_entry_timing(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        limit=1,
        write_order_plan_drafts=False,
        persist_evaluations=True,
        settings=settings,
        manage_run_lock=False,
        source_run_id=source_run_id,
        commit=False,
    )
    if (
        entry_result.error_count
        or entry_result.candidate_count != 1
        or entry_result.evaluated_count != 1
        or len(entry_result.evaluations) != 1
    ):
        raise TargetedPipelineRebuildError(
            "ENTRY_TIMING_EVALUATION_INCOMPLETE",
            details={
                "candidate_count": entry_result.candidate_count,
                "evaluated_count": entry_result.evaluated_count,
                "error_count": entry_result.error_count,
            },
        )
    evaluation = entry_result.evaluations[0]
    if evaluation.order_plan_id is not None:
        raise TargetedPipelineRebuildError("ENTRY_TIMING_ORDER_PLAN_REFERENCE_PRESENT")
    if not evaluation.observe_only or not evaluation.not_order_intent:
        raise TargetedPipelineRebuildError("ENTRY_TIMING_NOT_ORDER_INTENT_REQUIRED")
    _assert_stage_fence(
        connection,
        lease,
        "ENTRY_TIMING_SAVED",
        started_monotonic=started_monotonic,
    )

    strategy_row = _required_row(
        connection,
        "strategy_observations",
        "strategy_observation_id",
        strategy.strategy_observation_id,
    )
    risk_row = _required_row(
        connection,
        "risk_observations",
        "risk_observation_id",
        risk.risk_observation_id,
    )
    entry_row = _required_row(
        connection,
        "entry_timing_evaluations",
        "entry_timing_evaluation_id",
        evaluation.entry_timing_evaluation_id,
    )
    strategy_latest = _required_row(
        connection,
        "strategy_observations_latest",
        "candidate_instance_id",
        candidate_id,
    )
    risk_latest = _required_row(
        connection,
        "risk_observations_latest",
        "candidate_instance_id",
        candidate_id,
    )
    if strategy_latest["strategy_observation_id"] != strategy.strategy_observation_id:
        raise TargetedPipelineRebuildError("STRATEGY_LATEST_ID_MISMATCH")
    if risk_latest["risk_observation_id"] != risk.risk_observation_id:
        raise TargetedPipelineRebuildError("RISK_LATEST_ID_MISMATCH")
    _verify_persisted_pipeline(
        candidate_id=candidate_id,
        trade_date=trade_date,
        source_run_id=source_run_id,
        expected_watermark_hash=str(expected_lineage["source_watermark_hash"]),
        strategy_row=strategy_row,
        risk_row=risk_row,
        entry_row=entry_row,
        strategy_latest=strategy_latest,
        risk_latest=risk_latest,
    )
    _verify_exact_persisted_rows(
        connection,
        strategy=strategy,
        risk=risk,
        evaluation=evaluation,
        expected_lineage=expected_lineage,
        strategy_row=strategy_row,
        risk_row=risk_row,
        entry_row=entry_row,
        strategy_latest=strategy_latest,
        risk_latest=risk_latest,
    )
    canonical_persisted_rows = _capture_persisted_row_fingerprints(
        connection,
        candidate_id=candidate_id,
        strategy_observation_id=strategy.strategy_observation_id,
        risk_observation_id=risk.risk_observation_id,
        entry_timing_evaluation_id=evaluation.entry_timing_evaluation_id,
    )

    final_source = _require_current_candidate(
        connection,
        candidate_id,
        trade_date=trade_date,
        settings=settings,
        source_run_id=source_run_id,
    )
    _assert_same_source_snapshots([candidate], [final_source])
    return {
        "candidate_instance_id": candidate_id,
        "source_run_id": source_run_id,
        "source_watermark_hash": expected_lineage["source_watermark_hash"],
        "strategy_observation_id": strategy.strategy_observation_id,
        "risk_observation_id": risk.risk_observation_id,
        "entry_timing_evaluation_id": evaluation.entry_timing_evaluation_id,
        "strategy_observation_count": 1,
        "strategy_setup_observation_count": len(strategy.setup_observations),
        "risk_observation_count": 1,
        "risk_check_observation_count": len(risk.check_observations),
        "entry_timing_evaluation_count": 1,
        "entry_timing_error_count": 0,
        "canonical_persisted_rows": canonical_persisted_rows,
        "entry_timing_status": evaluation.status.value,
        "order_plan_id": None,
        "observe_only": True,
        "not_order_intent": True,
        "stage_order": [
            "source",
            "candidate",
            "strategy",
            "risk",
            "entry_timing",
        ],
    }


def _verify_persisted_pipeline(
    *,
    candidate_id: str,
    trade_date: str,
    source_run_id: str,
    expected_watermark_hash: str,
    strategy_row: Mapping[str, Any],
    risk_row: Mapping[str, Any],
    entry_row: Mapping[str, Any],
    strategy_latest: Mapping[str, Any],
    risk_latest: Mapping[str, Any],
) -> None:
    rows = (strategy_row, risk_row, entry_row, strategy_latest, risk_latest)
    for row in rows:
        if str(row["candidate_instance_id"]) != candidate_id:
            raise TargetedPipelineRebuildError("PERSISTED_CANDIDATE_MISMATCH")
        if str(row["trade_date"]) != trade_date:
            raise TargetedPipelineRebuildError("PERSISTED_TRADE_DATE_MISMATCH")
        if str(row["source_run_id"] or "") != source_run_id:
            raise TargetedPipelineRebuildError("MIXED_SOURCE_RUN")
        if str(row["source_watermark_hash"] or "") != expected_watermark_hash:
            raise TargetedPipelineRebuildError("SOURCE_WATERMARK_HASH_MISMATCH")
        if not _stored_watermark_hash_valid(row):
            raise TargetedPipelineRebuildError("SOURCE_WATERMARK_HASH_INVALID")
        if int(row["observe_only"] or 0) != 1:
            raise TargetedPipelineRebuildError("PERSISTED_ROW_NOT_OBSERVE_ONLY")

    if risk_row["strategy_observation_id"] != strategy_row["strategy_observation_id"]:
        raise TargetedPipelineRebuildError("RISK_STRATEGY_FK_MISMATCH")
    if risk_latest["strategy_observation_id"] != strategy_latest[
        "strategy_observation_id"
    ]:
        raise TargetedPipelineRebuildError("RISK_LATEST_STRATEGY_FK_MISMATCH")
    if entry_row["strategy_observation_id"] != strategy_row["strategy_observation_id"]:
        raise TargetedPipelineRebuildError("ENTRY_STRATEGY_FK_MISMATCH")
    if entry_row["risk_observation_id"] != risk_row["risk_observation_id"]:
        raise TargetedPipelineRebuildError("ENTRY_RISK_FK_MISMATCH")
    if entry_row["order_plan_id"] is not None:
        raise TargetedPipelineRebuildError("ENTRY_ORDER_PLAN_FK_PRESENT")
    if int(entry_row["not_order_intent"] or 0) != 1:
        raise TargetedPipelineRebuildError("ENTRY_NOT_ORDER_INTENT_REQUIRED")

    strategy_at = parse_timestamp(strategy_row["evaluated_at"], "strategy.evaluated_at")
    risk_at = parse_timestamp(risk_row["evaluated_at"], "risk.evaluated_at")
    entry_at = parse_timestamp(entry_row["evaluated_at"], "entry.evaluated_at")
    if not strategy_at <= risk_at <= entry_at:
        raise TargetedPipelineRebuildError("PIPELINE_STAGE_ORDER_VIOLATION")


def _verify_exact_persisted_rows(
    connection: sqlite3.Connection,
    *,
    strategy: Any,
    risk: Any,
    evaluation: Any,
    expected_lineage: Mapping[str, Any],
    strategy_row: Mapping[str, Any],
    risk_row: Mapping[str, Any],
    entry_row: Mapping[str, Any],
    strategy_latest: Mapping[str, Any],
    risk_latest: Mapping[str, Any],
) -> None:
    strategy_data = strategy.to_dict(include_setups=False)
    strategy_expected = {
        "strategy_observation_id": strategy_data["strategy_observation_id"],
        "candidate_instance_id": strategy_data["candidate_instance_id"],
        "trade_date": strategy_data["trade_date"],
        "code": strategy_data["code"],
        "name": strategy_data["name"],
        "evaluated_at": strategy_data["evaluated_at"],
        "overall_status": strategy_data["overall_status"],
        "primary_setup_type": strategy_data["primary_setup_type"],
        "primary_setup_status": strategy_data["primary_setup_status"],
        "score": strategy_data["score"],
        "confidence": strategy_data["confidence"],
        "reason_codes_json": _compact_json(strategy_data["reason_codes"]),
        "evidence_json": canonical_json(strategy_data["evidence_json"]),
        "config_version": strategy_data["config_version"],
        "observe_only": 1,
        **_expected_lineage_columns(expected_lineage, generated_by="strategy_engine"),
    }
    _assert_exact_db_row(
        strategy_row,
        strategy_expected,
        table_name="strategy_observations",
    )
    strategy_latest_expected = dict(strategy_expected)
    strategy_latest_expected.pop("evidence_json")
    _assert_exact_db_row(
        strategy_latest,
        strategy_latest_expected,
        table_name="strategy_observations_latest",
    )

    setup_rows = connection.execute(
        "SELECT * FROM strategy_setup_observations "
        "WHERE strategy_observation_id = ? ORDER BY id",
        (strategy.strategy_observation_id,),
    ).fetchall()
    if len(setup_rows) != len(strategy.setup_observations):
        raise TargetedPipelineRebuildError("STRATEGY_SETUP_ROW_COUNT_MISMATCH")
    for row, setup in zip(setup_rows, strategy.setup_observations, strict=True):
        setup_data = setup.to_dict()
        _assert_exact_db_row(
            dict(row),
            {
                "id": row["id"],
                "strategy_observation_id": strategy.strategy_observation_id,
                "candidate_instance_id": strategy.candidate_instance_id,
                "setup_type": setup_data["setup_type"],
                "status": setup_data["status"],
                "score": setup_data["score"],
                "confidence": setup_data["confidence"],
                "reason_codes_json": _compact_json(setup_data["reason_codes"]),
                "evidence_json": canonical_json(setup_data["evidence_json"]),
                "evaluated_at": strategy_data["evaluated_at"],
            },
            table_name="strategy_setup_observations",
        )

    risk_data = risk.to_dict(include_checks=False)
    risk_expected = {
        "risk_observation_id": risk_data["risk_observation_id"],
        "candidate_instance_id": risk_data["candidate_instance_id"],
        "strategy_observation_id": risk_data["strategy_observation_id"],
        "trade_date": risk_data["trade_date"],
        "code": risk_data["code"],
        "name": risk_data["name"],
        "evaluated_at": risk_data["evaluated_at"],
        "overall_status": risk_data["overall_status"],
        "max_severity": risk_data["max_severity"],
        "blocked_count": risk_data["blocked_count"],
        "caution_count": risk_data["caution_count"],
        "pass_count": risk_data["pass_count"],
        "reason_codes_json": _compact_json(risk_data["reason_codes"]),
        "evidence_json": canonical_json(risk_data["evidence_json"]),
        "config_version": risk_data["config_version"],
        "observe_only": 1,
        **_expected_lineage_columns(expected_lineage, generated_by="risk_gate"),
    }
    _assert_exact_db_row(risk_row, risk_expected, table_name="risk_observations")
    risk_latest_expected = dict(risk_expected)
    risk_latest_expected.pop("evidence_json")
    _assert_exact_db_row(
        risk_latest,
        risk_latest_expected,
        table_name="risk_observations_latest",
    )

    check_rows = connection.execute(
        "SELECT * FROM risk_check_observations "
        "WHERE risk_observation_id = ? ORDER BY id",
        (risk.risk_observation_id,),
    ).fetchall()
    if len(check_rows) != len(risk.check_observations):
        raise TargetedPipelineRebuildError("RISK_CHECK_ROW_COUNT_MISMATCH")
    for row, check in zip(check_rows, risk.check_observations, strict=True):
        check_data = check.to_dict()
        _assert_exact_db_row(
            dict(row),
            {
                "id": row["id"],
                "risk_observation_id": risk.risk_observation_id,
                "candidate_instance_id": risk.candidate_instance_id,
                "category": check_data["category"],
                "status": check_data["status"],
                "severity": check_data["severity"],
                "reason_codes_json": _compact_json(check_data["reason_codes"]),
                "message": check_data["message"],
                "evidence_json": canonical_json(check_data["evidence_json"]),
                "evaluated_at": risk_data["evaluated_at"],
            },
            table_name="risk_check_observations",
        )

    entry_data = evaluation.to_dict()
    entry_lineage = _expected_lineage_columns(
        expected_lineage,
        generated_by="entry_timing_engine",
    )
    try:
        entry_data_age_sec = float(entry_row["data_age_sec"])
    except (TypeError, ValueError) as exc:
        raise TargetedPipelineRebuildError(
            "ENTRY_TIMING_DATA_AGE_INVALID"
        ) from exc
    if entry_data_age_sec < 0:
        raise TargetedPipelineRebuildError("ENTRY_TIMING_DATA_AGE_INVALID")
    entry_lineage["data_age_sec"] = entry_row["data_age_sec"]
    entry_expected = {
        "entry_timing_evaluation_id": entry_data["entry_timing_evaluation_id"],
        "trade_date": entry_data["trade_date"],
        "candidate_instance_id": entry_data["candidate_instance_id"],
        "code": entry_data["code"],
        "name": entry_data["name"],
        "evaluated_at": entry_data["evaluated_at"],
        "setup_type": entry_data["setup_type"],
        "entry_timing_state": entry_data["entry_timing_state"],
        "price_location_state": entry_data["price_location_state"],
        "status": entry_data["status"],
        "order_plan_id": None,
        "reason_codes_json": _compact_json(entry_data["reason_codes"]),
        "evidence_json": canonical_json(entry_data["evidence_json"]),
        "observe_only": 1,
        "not_order_intent": 1,
        "strategy_observation_id": strategy.strategy_observation_id,
        "risk_observation_id": risk.risk_observation_id,
        **entry_lineage,
    }
    _assert_exact_db_row(
        entry_row,
        entry_expected,
        table_name="entry_timing_evaluations",
    )


def _expected_lineage_columns(
    lineage: Mapping[str, Any],
    *,
    generated_by: str,
) -> dict[str, Any]:
    watermark_json = lineage.get("source_watermark_json")
    if not isinstance(watermark_json, str):
        watermark = lineage.get("source_watermark")
        watermark_json = canonical_json(
            dict(watermark) if isinstance(watermark, Mapping) else {}
        )
    return {
        "source_run_id": lineage.get("source_run_id"),
        "source_watermark": watermark_json,
        "source_watermark_hash": lineage.get("source_watermark_hash"),
        "source_event_id": lineage.get("source_event_id"),
        "source_observed_at": lineage.get("source_observed_at"),
        "data_age_sec": lineage.get("data_age_sec"),
        "generated_by": generated_by,
    }


def _assert_exact_db_row(
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    table_name: str,
) -> None:
    actual_dict = dict(actual)
    expected_dict = dict(expected)
    if actual_dict != expected_dict:
        changed_columns = sorted(
            key
            for key in set(actual_dict) | set(expected_dict)
            if actual_dict.get(key) != expected_dict.get(key)
        )
        raise TargetedPipelineRebuildError(
            "TARGET_PIPELINE_CANONICAL_ROW_MISMATCH",
            details={"table": table_name, "changed_columns": changed_columns},
        )


def _compact_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _classify_candidates(
    connection: sqlite3.Connection,
    candidate_ids: Sequence[str],
    *,
    trade_date: str,
    settings: Settings,
    source_run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    eligible: list[dict[str, Any]] = []
    blocked: dict[str, list[str]] = {}
    for candidate_id in candidate_ids:
        try:
            eligible.append(
                _require_current_candidate(
                    connection,
                    candidate_id,
                    trade_date=trade_date,
                    settings=settings,
                    source_run_id=source_run_id,
                )
            )
        except TargetedPipelineRebuildError as exc:
            blocked[candidate_id] = list(exc.reason_codes)
    return eligible, blocked


def _require_all_candidates_eligible(
    candidate_ids: Sequence[str],
    eligible: Sequence[Mapping[str, Any]],
    blocked: Mapping[str, Sequence[str]],
) -> None:
    if not eligible:
        raise TargetedPipelineRebuildError(
            "NO_ELIGIBLE_CANDIDATES",
            details={"candidate_instance_ids": list(candidate_ids), "blocked": blocked},
        )
    if blocked or len(eligible) != len(candidate_ids):
        raise TargetedPipelineRebuildError(
            "INELIGIBLE_CANDIDATE_PRESENT",
            details={"blocked": blocked},
        )


def _require_current_candidate(
    connection: sqlite3.Connection,
    candidate_id: str,
    *,
    trade_date: str,
    settings: Settings,
    source_run_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    reasons: list[str] = []
    if row is None:
        raise TargetedPipelineRebuildError("CANDIDATE_NOT_FOUND")
    candidate = dict(row)
    if str(candidate["trade_date"]) != trade_date:
        reasons.append("CANDIDATE_TRADE_DATE_MISMATCH")
    candidate_code = str(candidate["code"] or "").strip()
    if not candidate_code:
        reasons.append("CANDIDATE_CODE_MISSING")
    if candidate.get("closed_at") is not None:
        reasons.append("CANDIDATE_CLOSED_AT_PRESENT")
    if str(candidate["state"]) not in TARGETED_PIPELINE_CURRENT_ACTIVE_STATES:
        reasons.append("CANDIDATE_NOT_CURRENT_ACTIVE")
    if str(candidate["state"]) not in TARGETED_PIPELINE_CURRENT_ACTIVE_STATES:
        reasons.append("CANDIDATE_NOT_STRATEGY_ELIGIBLE")
    if int(candidate["active_source_count"] or 0) < 1:
        reasons.append("CANDIDATE_ACTIVE_SOURCE_COUNT_ZERO")
    if int(candidate["source_count"] or 0) < int(candidate["active_source_count"] or 0):
        reasons.append("CANDIDATE_SOURCE_COUNT_INVALID")
    candidate_age = _strict_age_seconds(
        candidate.get("last_seen_at"),
        field_name="candidate.last_seen_at",
        reasons=reasons,
    )
    if (
        candidate_age is not None
        and candidate_age > float(settings.candidate_source_stale_sec)
    ):
        reasons.append("CANDIDATE_STALE")

    candidate_context = connection.execute(
        """
        SELECT candidate_instance_id, trade_date, code, name, refreshed_at
        FROM candidate_context_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    ).fetchone()
    if candidate_context is None:
        reasons.append("CANDIDATE_CONTEXT_MISSING")
    elif (
        str(candidate_context["candidate_instance_id"]) != candidate_id
        or str(candidate_context["trade_date"]) != trade_date
        or str(candidate_context["code"]) != candidate_code
        or str(candidate_context["name"]) != str(candidate["name"])
    ):
        reasons.append("CANDIDATE_CONTEXT_IDENTITY_MISMATCH")
    else:
        context_age = _strict_age_seconds(
            candidate_context["refreshed_at"],
            field_name="candidate_context.refreshed_at",
            reasons=reasons,
        )
        if (
            context_age is not None
            and context_age > float(settings.candidate_source_stale_sec)
        ):
            reasons.append("CANDIDATE_CONTEXT_STALE")

    candidate_reasons = _json_string_set(candidate.get("reason_codes_json"))
    if "INVALID_REASON_CODES_JSON" in candidate_reasons:
        reasons.append("CANDIDATE_REASON_CODES_INVALID")
    if "CONDITION_RISK_BLOCKED" in candidate_reasons:
        reasons.append("CANDIDATE_CONDITION_RISK_BLOCKED")
    if "DISCOVERY_OBSERVATION_ONLY" in candidate_reasons:
        reasons.append("CANDIDATE_DISCOVERY_OBSERVATION_ONLY")
    fusion = connection.execute(
        """
        SELECT risk_blocked
        FROM candidate_condition_fusion
        WHERE trade_date = ? AND code = ?
        """,
        (trade_date, candidate_code),
    ).fetchone()
    if fusion is not None and int(fusion["risk_blocked"] or 0) != 0:
        reasons.append("CANDIDATE_CONDITION_FUSION_RISK_BLOCKED")

    active_sources = connection.execute(
        """
        SELECT *
        FROM candidate_sources_latest
        WHERE candidate_instance_id = ? AND active = 1
        ORDER BY last_seen_at DESC, source_type, source_id
        """,
        (candidate_id,),
    ).fetchall()
    if not active_sources:
        reasons.append("ACTIVE_CANDIDATE_SOURCE_MISSING")
    else:
        if int(candidate["active_source_count"] or 0) != len(active_sources):
            reasons.append("CANDIDATE_ACTIVE_SOURCE_COUNT_MISMATCH")
        primary_identity = (
            str(candidate["primary_source_type"] or "").strip(),
            str(candidate["primary_source_id"] or "").strip(),
        )
        source_identities: set[tuple[str, str]] = set()
        for index, source in enumerate(active_sources):
            source_type = str(source["source_type"] or "").strip()
            source_id = str(source["source_id"] or "").strip()
            source_identities.add((source_type, source_id))
            if (
                str(source["candidate_instance_id"]) != candidate_id
                or str(source["trade_date"]) != trade_date
                or str(source["code"]) != candidate_code
                or str(source["name"]) != str(candidate["name"])
                or not source_type
                or not source_id
                or not str(source["last_event_id"] or "").strip()
            ):
                reasons.append("ACTIVE_CANDIDATE_SOURCE_IDENTITY_MISMATCH")
            first_seen_age = _strict_age_seconds(
                source["first_seen_at"],
                field_name=f"candidate_source_{index}.first_seen_at",
                reasons=reasons,
            )
            last_seen_age = _strict_age_seconds(
                source["last_seen_at"],
                field_name=f"candidate_source_{index}.last_seen_at",
                reasons=reasons,
            )
            if (
                last_seen_age is not None
                and last_seen_age > float(settings.candidate_source_stale_sec)
            ):
                reasons.append("ACTIVE_CANDIDATE_SOURCE_STALE")
            if (
                first_seen_age is not None
                and last_seen_age is not None
                and first_seen_age + 5.0 < last_seen_age
            ):
                reasons.append("ACTIVE_CANDIDATE_SOURCE_TIME_ORDER_INVALID")
            source_event = connection.execute(
                """
                SELECT candidate_instance_id, trade_date, code, name,
                       source_type, source_id, active
                FROM candidate_source_events
                WHERE source_event_id = ?
                """,
                (source["last_event_id"],),
            ).fetchone()
            if source_event is None or (
                str(source_event["candidate_instance_id"] or "") != candidate_id
                or str(source_event["trade_date"]) != trade_date
                or str(source_event["code"]) != candidate_code
                or str(source_event["name"]) != str(candidate["name"])
                or str(source_event["source_type"]) != source_type
                or str(source_event["source_id"]) != source_id
                or int(source_event["active"] or 0) != 1
            ):
                reasons.append("ACTIVE_CANDIDATE_SOURCE_IDENTITY_MISMATCH")
                reasons.append("ACTIVE_CANDIDATE_SOURCE_EVENT_IDENTITY_MISMATCH")
        if primary_identity not in source_identities:
            reasons.append("ACTIVE_CANDIDATE_SOURCE_IDENTITY_MISMATCH")
            reasons.append("CANDIDATE_PRIMARY_SOURCE_IDENTITY_MISMATCH")

    lineage = resolve_candidate_source_lineage(
        connection,
        candidate_id,
        source_run_id=source_run_id,
        generated_by="fast0r3_targeted_pipeline_rebuild",
        fallback_trade_date=trade_date,
    )
    if not lineage.get("candidate_present"):
        reasons.append("CURRENT_LINEAGE_CANDIDATE_MISSING")
    if lineage.get("trade_date") != trade_date:
        reasons.append("CURRENT_LINEAGE_TRADE_DATE_MISMATCH")
    if lineage.get("source_run_id") != source_run_id:
        reasons.append("CURRENT_LINEAGE_SOURCE_RUN_MISMATCH")
    watermark = lineage.get("source_watermark")
    if not isinstance(watermark, Mapping) or not watermark:
        reasons.append("CURRENT_SOURCE_WATERMARK_MISSING")
    else:
        calculated = hashlib.sha256(canonical_json(watermark).encode("utf-8")).hexdigest()
        if calculated != lineage.get("source_watermark_hash"):
            reasons.append("CURRENT_SOURCE_WATERMARK_HASH_INVALID")
        candidate_watermark = watermark.get("candidate")
        if not isinstance(candidate_watermark, Mapping):
            reasons.append("CURRENT_SOURCE_CANDIDATE_WATERMARK_MISSING")
        elif (
            candidate_watermark.get("candidate_instance_id") != candidate_id
            or candidate_watermark.get("state") != candidate["state"]
        ):
            reasons.append("CURRENT_SOURCE_CANDIDATE_WATERMARK_MISMATCH")
    if not str(lineage.get("source_event_id") or "").strip():
        reasons.append("CURRENT_SOURCE_EVENT_MISSING")
    source_age = _strict_age_seconds(
        lineage.get("source_observed_at"),
        field_name="lineage.source_observed_at",
        reasons=reasons,
    )
    if (
        source_age is not None
        and source_age > float(settings.entry_timing_stale_max_seconds)
    ):
        reasons.append("CURRENT_SOURCE_STALE")
    if reasons:
        raise TargetedPipelineRebuildError(*reasons)
    return {
        "candidate_instance_id": candidate_id,
        "trade_date": trade_date,
        "state": str(candidate["state"]),
        "lineage": lineage,
        "active_source_count": len(active_sources),
        "active_source_fingerprint": _rows_fingerprint(active_sources),
    }


def _strict_age_seconds(
    value: object,
    *,
    field_name: str,
    reasons: list[str],
) -> float | None:
    if value is None or not str(value).strip():
        reasons.append(f"{field_name.upper().replace('.', '_')}_MISSING")
        return None
    try:
        timestamp = parse_timestamp(value, field_name)
    except (TypeError, ValueError):
        reasons.append(f"{field_name.upper().replace('.', '_')}_INVALID")
        return None
    age = (utc_now() - timestamp).total_seconds()
    if age < -5.0:
        reasons.append(f"{field_name.upper().replace('.', '_')}_FUTURE")
    return max(age, 0.0)


def _validate_runtime_safety(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None,
    environ: Mapping[str, str] | None,
) -> tuple[Settings, dict[str, Any], _AuditedEnvContract]:
    source_environment = dict(os.environ if environ is None else environ)
    configured_env = source_environment.get(ENV_FILE_PATH_ENV, "").strip()
    reasons: list[str] = []
    env_path: Path | None = None
    env_values: dict[str, str] = {}
    env_contract: _AuditedEnvContract | None = None
    if not configured_env:
        reasons.append("EXPLICIT_TRADING_ENV_FILE_REQUIRED")
    else:
        env_path = Path(configured_env).expanduser()
        if not env_path.is_file():
            reasons.append("TRADING_ENV_FILE_NOT_FOUND")
        elif _same_path(env_path, DEFAULT_ENV_FILE_PATH):
            reasons.append("DEFAULT_DOTENV_FORBIDDEN")
        else:
            try:
                env_values, env_contract = _read_audited_env_file(env_path)
            except TargetedPipelineRebuildError as exc:
                reasons.extend(exc.reason_codes)
            except (OSError, ValueError):
                reasons.append("TRADING_ENV_FILE_INVALID")

    missing_explicit = sorted(_REQUIRED_EXPLICIT_ENV - set(env_values))
    reasons.extend(f"ENV_SETTING_NOT_EXPLICIT:{name}" for name in missing_explicit)
    theme_value = env_values.get("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS")
    if theme_value is not None and theme_value.strip().lower() not in _FALSE_VALUES:
        reasons.append("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_FALSE")
    for env_name in sorted(_COMMAND_PRODUCER_ENV_NAMES):
        value = env_values.get(env_name)
        if value is not None and value.strip().lower() not in _FALSE_VALUES:
            reasons.append(f"COMMAND_PRODUCER_ENV_NOT_FALSE:{env_name}")
    if env_values.get("INCREMENTAL_EVALUATION_WORKER_ENABLED", "").strip().lower() not in (
        _FALSE_VALUES
    ):
        reasons.append("INCREMENTAL_EVALUATION_WORKER_NOT_FALSE")
    if env_values.get("ENTRY_TIMING_WRITE_ORDER_PLAN_DRAFTS", "").strip().lower() not in (
        _FALSE_VALUES
    ):
        reasons.append("ENTRY_TIMING_WRITE_ORDER_PLAN_DRAFTS_NOT_FALSE")

    try:
        audited_settings = load_settings(environ=env_values)
    except (TypeError, ValueError):
        audited_settings = Settings()
        reasons.append("AUDITED_SETTINGS_INVALID")
    resolved_settings = settings or audited_settings
    reasons.extend(_settings_safety_reason_codes(audited_settings, prefix="AUDITED"))
    if settings is not None:
        reasons.extend(_settings_safety_reason_codes(settings, prefix="RUNTIME"))
        evaluation_setting_mismatches = [
            name
            for name in settings.__dataclass_fields__
            if name != "deprecated_flag_warnings"
            and getattr(settings, name) != getattr(audited_settings, name)
        ]
        if evaluation_setting_mismatches:
            reasons.append("RUNTIME_SETTINGS_DO_NOT_MATCH_AUDITED_ENV")
        for name in _SAFETY_SETTING_NAMES:
            runtime_value = getattr(settings, name)
            audited_value = getattr(audited_settings, name)
            if name == "trading_db_path":
                matches = _same_path(Path(runtime_value), Path(audited_value))
            else:
                matches = runtime_value == audited_value
            if not matches:
                reasons.append(f"RUNTIME_AUDITED_SETTING_MISMATCH:{name.upper()}")

    database_path = _connection_database_path(connection)
    if database_path is None:
        reasons.append("DATABASE_PATH_UNAVAILABLE")
    else:
        if not _same_path(Path(audited_settings.trading_db_path), database_path):
            reasons.append("DB_PATH_DOES_NOT_MATCH_AUDITED_ENV")
        if not _same_path(Path(resolved_settings.trading_db_path), database_path):
            reasons.append("DB_PATH_DOES_NOT_MATCH_RUNTIME_SETTINGS")

    if env_contract is None:
        reasons.append("AUDITED_ENV_CONTRACT_MISSING")
    if reasons:
        raise TargetedPipelineRebuildError(*reasons)
    assert env_contract is not None
    return resolved_settings, {
        "trading_env_file_sha256": env_contract.sha256,
        "trading_env_file_explicit": True,
        "trading_env_file_path_redacted": True,
        "settings_sha256": _settings_sha256(resolved_settings),
        "strategy_config_version": resolved_settings.strategy_config_version,
        "risk_config_version": resolved_settings.risk_gate_config_version,
        "entry_timing_config_version": resolved_settings.entry_timing_config_version,
        "trading_profile": resolved_settings.trading_profile.value,
        "trading_mode": resolved_settings.trading_mode.value,
        "live_sim_allowed": False,
        "live_real_allowed": False,
        "kill_switch_active": True,
        "incremental_worker_enabled": False,
        "theme_refresh_queue_market_scan_commands": False,
        "enabled_command_producers": [],
        "database_path_matches": True,
    }, env_contract


def _settings_safety_reason_codes(settings: Settings, *, prefix: str) -> list[str]:
    reasons: list[str] = []
    if settings.trading_profile is not TradingProfile.OBSERVE:
        reasons.append(f"{prefix}_TRADING_PROFILE_NOT_OBSERVE")
    if settings.trading_mode is not TradingMode.OBSERVE:
        reasons.append(f"{prefix}_TRADING_MODE_NOT_OBSERVE")
    if settings.trading_allow_live_sim:
        reasons.append(f"{prefix}_LIVE_SIM_ALLOWED")
    if settings.trading_allow_live_real:
        reasons.append(f"{prefix}_LIVE_REAL_ALLOWED")
    if not settings.live_sim_kill_switch:
        reasons.append(f"{prefix}_LIVE_SIM_KILL_SWITCH_OFF")
    if settings.incremental_evaluation_worker_enabled:
        reasons.append(f"{prefix}_INCREMENTAL_EVALUATION_WORKER_ENABLED")
    if not settings.strategy_engine_enabled:
        reasons.append(f"{prefix}_STRATEGY_ENGINE_DISABLED")
    if not settings.strategy_engine_observe_only:
        reasons.append(f"{prefix}_STRATEGY_ENGINE_NOT_OBSERVE_ONLY")
    if not settings.risk_gate_enabled:
        reasons.append(f"{prefix}_RISK_GATE_DISABLED")
    if not settings.risk_gate_observe_only:
        reasons.append(f"{prefix}_RISK_GATE_NOT_OBSERVE_ONLY")
    if not settings.entry_timing_enabled:
        reasons.append(f"{prefix}_ENTRY_TIMING_DISABLED")
    for field_name in _COMMAND_PRODUCER_SETTINGS:
        if bool(getattr(settings, field_name)):
            reasons.append(f"{prefix}_COMMAND_PRODUCER_ENABLED:{field_name.upper()}")
    return reasons


def _read_audited_env_file(
    path: Path,
) -> tuple[dict[str, str], _AuditedEnvContract]:
    if path.is_symlink():
        raise TargetedPipelineRebuildError("TRADING_ENV_FILE_SYMLINK_FORBIDDEN")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    descriptor = os.open(path, flags)
    try:
        opened_stat = os.fstat(descriptor)
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read()
    finally:
        os.close(descriptor)
    path_stat = os.stat(path, follow_symlinks=False)
    opened_identity = (
        int(opened_stat.st_dev),
        int(opened_stat.st_ino),
        int(opened_stat.st_size),
        int(opened_stat.st_mtime_ns),
    )
    path_identity = (
        int(path_stat.st_dev),
        int(path_stat.st_ino),
        int(path_stat.st_size),
        int(path_stat.st_mtime_ns),
    )
    if opened_identity != path_identity:
        raise TargetedPipelineRebuildError("TRADING_ENV_FILE_CHANGED_DURING_READ")
    link_count = int(opened_stat.st_nlink)
    if link_count != 1:
        raise TargetedPipelineRebuildError("TRADING_ENV_FILE_HARDLINK_FORBIDDEN")
    try:
        contents = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("audited env must be UTF-8") from exc
    values = _parse_env_contents(contents)
    resolved_path = path.resolve(strict=True)
    return values, _AuditedEnvContract(
        path=resolved_path,
        sha256=hashlib.sha256(raw).hexdigest(),
        device=int(opened_stat.st_dev),
        inode=int(opened_stat.st_ino),
        size=int(opened_stat.st_size),
        mtime_ns=int(opened_stat.st_mtime_ns),
        link_count=link_count,
    )


def _parse_env_contents(contents: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        contents.splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        separator = line.find("=")
        if separator < 1:
            raise ValueError(f"invalid env line: {line_number}")
        name = line[:separator].strip()
        value = line[separator + 1 :].strip()
        if not name or name in values:
            raise ValueError(f"duplicate/empty env setting: {line_number}")
        if len(value) >= 2 and (
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = value[1:-1]
        values[name] = value
    return values


def _assert_audited_env_unchanged(contract: _AuditedEnvContract) -> None:
    try:
        _values, current = _read_audited_env_file(contract.path)
    except (OSError, ValueError, TargetedPipelineRebuildError) as exc:
        raise TargetedPipelineRebuildError(
            "AUDITED_ENV_CHANGED_DURING_REBUILD"
        ) from exc
    if current != contract:
        raise TargetedPipelineRebuildError("AUDITED_ENV_CHANGED_DURING_REBUILD")


def _order_artifact_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    tables: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for table_name in _ORDER_ARTIFACT_TABLES:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        if exists is None:
            missing.append(table_name)
            continue
        tables[table_name] = _table_guard_snapshot(connection, table_name)
    if missing:
        raise TargetedPipelineRebuildError(
            "ORDER_ARTIFACT_SCHEMA_INCOMPLETE",
            details={"missing_tables": missing},
        )
    return {
        "tables": tables,
        "command_type_counts": _command_type_counts(connection),
        "guard_contract": (
            "STRICT_MAIN_WRITE_AUTHORIZER+BEGIN_IMMEDIATE+COUNT_HIGH_WATER"
        ),
    }


def _reconcile_transaction_finalization(
    connection: sqlite3.Connection,
    *,
    candidate_ids: Sequence[str],
    trade_date: str,
    settings: Settings,
    source_run_id: str,
    expected_candidates: Sequence[Mapping[str, Any]],
    expected_items: Sequence[Mapping[str, Any]],
    artifact_snapshot_before: Mapping[str, Any],
    pipeline_stage_before: Mapping[str, Any],
    env_contract: _AuditedEnvContract,
    lease: RuntimeExecutionLease,
) -> _TransactionReconciliation:
    checks: dict[str, bool | None] = {
        "rollback_attempted": False,
        "rollback_succeeded": None,
        "canonical_target_rows": False,
        "pipeline_stage_delta": False,
        "source_watermark": False,
        "order_artifacts": False,
        "fence_current": False,
        "audited_env_unchanged": False,
        "durable_state_matches_precommit": False,
        "independent_read_only_connection": False,
    }
    error_types: dict[str, str] = {}
    transaction_was_open = bool(connection.in_transaction)
    checks["transaction_open_after_error"] = transaction_was_open
    connection.set_progress_handler(None, 0)
    if transaction_was_open:
        checks["rollback_attempted"] = True
        try:
            connection.rollback()
            checks["rollback_succeeded"] = True
        except Exception as exc:
            checks["rollback_succeeded"] = False
            error_types["rollback"] = type(exc).__name__

    reconciliation_deadline = time.monotonic() + TARGETED_PIPELINE_RECONCILIATION_MAX_SEC
    artifact_snapshot_after: dict[str, Any] | None = None
    pipeline_stage_after: dict[str, Any] | None = None
    pipeline_stage_delta: dict[str, Any] | None = None

    try:
        _assert_audited_env_unchanged(env_contract)
        checks["audited_env_unchanged"] = True
    except Exception as exc:
        error_types["audited_env_unchanged"] = type(exc).__name__

    evidence_connection: sqlite3.Connection | None = None
    try:
        database_path = Path(str(lease.database_path or "")).expanduser()
        if not str(lease.database_path or "").strip() or not database_path.is_file():
            raise OSError("durable reconciliation database path unavailable")
        uri_path = quote(database_path.resolve(strict=True).as_posix(), safe="/:")
        evidence_connection = sqlite3.connect(
            f"file:{uri_path}?mode=ro",
            uri=True,
            timeout=5.0,
        )
        evidence_connection.row_factory = sqlite3.Row
        evidence_connection.execute("PRAGMA query_only=ON")
        query_only = evidence_connection.execute("PRAGMA query_only").fetchone()
        if query_only is None or int(query_only[0] or 0) != 1:
            raise sqlite3.OperationalError("independent connection is not query-only")
        evidence_connection.set_progress_handler(
            lambda: int(time.monotonic() > reconciliation_deadline),
            TARGETED_PIPELINE_REBUILD_PROGRESS_OPCODES,
        )
        checks["independent_read_only_connection"] = True
    except Exception as exc:
        error_types["independent_read_only_connection"] = type(exc).__name__

    if evidence_connection is not None:
        try:
            assert_runtime_execution_fence(
                evidence_connection,
                lease=lease,
                renew=False,
            )
            fence_row = evidence_connection.execute(
                """
                SELECT last_fencing_token
                FROM runtime_execution_lock_fences
                WHERE lock_name = ?
                """,
                (lease.lock_name,),
            ).fetchone()
            checks["fence_current"] = bool(
                fence_row is not None
                and int(fence_row["last_fencing_token"] or 0) == lease.fencing_token
            )
        except Exception as exc:
            error_types["fence_current"] = type(exc).__name__

        try:
            current_candidates, blocked = _classify_candidates(
                evidence_connection,
                candidate_ids,
                trade_date=trade_date,
                settings=settings,
                source_run_id=source_run_id,
            )
            _require_all_candidates_eligible(candidate_ids, current_candidates, blocked)
            _assert_same_source_snapshots(expected_candidates, current_candidates)
            checks["source_watermark"] = True
        except Exception as exc:
            error_types["source_watermark"] = type(exc).__name__

        try:
            artifact_snapshot_after = _order_artifact_snapshot(evidence_connection)
            checks["order_artifacts"] = (
                dict(artifact_snapshot_before) == artifact_snapshot_after
            )
        except Exception as exc:
            error_types["order_artifacts"] = type(exc).__name__

        try:
            pipeline_stage_after = _pipeline_stage_snapshot(
                evidence_connection,
                candidate_ids,
                deadline_monotonic=reconciliation_deadline,
            )
            checks["durable_state_matches_precommit"] = (
                dict(pipeline_stage_before) == pipeline_stage_after
            )
        except Exception as exc:
            error_types["pipeline_stage_snapshot"] = type(exc).__name__

        try:
            _assert_persisted_row_fingerprints(evidence_connection, expected_items)
            checks["canonical_target_rows"] = True
        except Exception as exc:
            error_types["canonical_target_rows"] = type(exc).__name__

        if pipeline_stage_after is not None:
            try:
                pipeline_stage_delta = _assert_pipeline_stage_delta(
                    pipeline_stage_before,
                    pipeline_stage_after,
                    expected_items,
                    candidate_ids,
                )
                checks["pipeline_stage_delta"] = True
            except Exception as exc:
                error_types["pipeline_stage_delta"] = type(exc).__name__

        try:
            evidence_connection.set_progress_handler(None, 0)
            evidence_connection.close()
        except Exception as exc:
            error_types["independent_connection_close"] = type(exc).__name__

    connection_still_open = bool(connection.in_transaction)
    checks["connection_in_transaction_after_reconciliation"] = connection_still_open
    committed = (
        not connection_still_open
        and checks["independent_read_only_connection"] is True
        and checks["canonical_target_rows"] is True
        and checks["pipeline_stage_delta"] is True
        and checks["source_watermark"] is True
        and checks["order_artifacts"] is True
        and checks["fence_current"] is True
        and checks["audited_env_unchanged"] is True
    )
    not_committed = (
        not connection_still_open
        and checks["independent_read_only_connection"] is True
        and checks["durable_state_matches_precommit"] is True
        and checks["source_watermark"] is True
        and checks["order_artifacts"] is True
        and checks["fence_current"] is True
        and checks["audited_env_unchanged"] is True
    )
    evidence: dict[str, Any] = {
        "checks": checks,
        "check_error_types": error_types,
        "reconciliation_max_sec": TARGETED_PIPELINE_RECONCILIATION_MAX_SEC,
        "operator_action_required": not not_committed,
    }
    if committed:
        return _TransactionReconciliation(
            outcome="COMMITTED_RECONCILED",
            evidence=evidence,
            artifact_snapshot_after=artifact_snapshot_after,
            pipeline_stage_delta=pipeline_stage_delta,
        )
    if not_committed:
        return _TransactionReconciliation(
            outcome="NOT_COMMITTED_CONFIRMED",
            evidence=evidence,
        )
    return _TransactionReconciliation(
        outcome="OUTCOME_UNKNOWN",
        evidence=evidence,
    )


def _pipeline_stage_snapshot(
    connection: sqlite3.Connection,
    candidate_ids: Sequence[str],
    *,
    deadline_monotonic: float,
) -> dict[str, Any]:
    placeholders = ",".join("?" for _ in candidate_ids)
    tables: dict[str, Any] = {}
    for table_name in _PIPELINE_STAGE_TABLES:
        target_rows = connection.execute(
            f'SELECT rowid AS "__rowid__", * FROM "{table_name}" '
            f"WHERE candidate_instance_id IN ({placeholders}) ORDER BY rowid",
            tuple(candidate_ids),
        ).fetchall()
        non_target_rows = connection.execute(
            f'SELECT rowid AS "__rowid__", * FROM "{table_name}" '
            f"WHERE candidate_instance_id IS NULL "
            f"OR candidate_instance_id NOT IN ({placeholders}) ORDER BY rowid",
            tuple(candidate_ids),
        )
        target_snapshot = _canonical_rows_snapshot(
            target_rows,
            deadline_monotonic=deadline_monotonic,
        )
        non_target_snapshot = _canonical_rows_snapshot(
            non_target_rows,
            deadline_monotonic=deadline_monotonic,
        )
        candidate_counts = {
            str(row["candidate_instance_id"]): int(row["row_count"] or 0)
            for row in connection.execute(
                f'SELECT candidate_instance_id, COUNT(*) AS row_count '
                f'FROM "{table_name}" '
                f"WHERE candidate_instance_id IN ({placeholders}) "
                "GROUP BY candidate_instance_id ORDER BY candidate_instance_id",
                tuple(candidate_ids),
            ).fetchall()
        }
        target_snapshot["candidate_counts"] = candidate_counts
        tables[table_name] = {
            "target": target_snapshot,
            "non_target": non_target_snapshot,
        }
    return {"tables": tables}


def _canonical_rows_snapshot(
    rows: Sequence[sqlite3.Row] | Any,
    *,
    deadline_monotonic: float,
) -> dict[str, Any]:
    digest = hashlib.sha256()
    row_hashes: list[str] = []
    count = 0
    for row in rows:
        if count % 256 == 0:
            _assert_deadline(deadline_monotonic, stage="CANONICAL_ROW_SNAPSHOT")
        row_hash = _mapping_sha256(dict(row))
        digest.update(row_hash.encode("ascii"))
        digest.update(b"\n")
        row_hashes.append(row_hash)
        count += 1
    return {
        "row_count": count,
        "content_sha256": digest.hexdigest(),
        "canonical_row_sha256": row_hashes,
    }


def _assert_pipeline_stage_delta(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
    candidate_ids: Sequence[str],
) -> dict[str, Any]:
    expected_totals = {
        key: sum(int(item.get(key, 0)) for item in items)
        for key in set(_PIPELINE_INSERT_DELTA_KEYS.values())
    }
    deltas: dict[str, Any] = {}
    for table_name in _PIPELINE_STAGE_TABLES:
        before_table = before["tables"][table_name]
        after_table = after["tables"][table_name]
        before_target = before_table["target"]
        after_target = after_table["target"]
        before_non_target = before_table["non_target"]
        after_non_target = after_table["non_target"]
        target_delta = int(after_target["row_count"]) - int(
            before_target["row_count"]
        )
        non_target_delta = int(after_non_target["row_count"]) - int(
            before_non_target["row_count"]
        )
        if non_target_delta != 0 or (
            before_non_target["content_sha256"]
            != after_non_target["content_sha256"]
        ):
            raise TargetedPipelineRebuildError(
                "NON_TARGET_PIPELINE_ROW_CHANGED",
                details={"table": table_name},
            )

        expected_key = _PIPELINE_INSERT_DELTA_KEYS.get(table_name)
        if expected_key is not None:
            expected_delta = expected_totals[expected_key]
            if target_delta != expected_delta:
                raise TargetedPipelineRebuildError(
                    "TARGET_PIPELINE_ROW_DELTA_MISMATCH",
                    details={
                        "table": table_name,
                        "expected_delta": expected_delta,
                        "actual_delta": target_delta,
                    },
                )
            before_hashes = set(before_target["canonical_row_sha256"])
            after_hashes = set(after_target["canonical_row_sha256"])
            if not before_hashes.issubset(after_hashes):
                raise TargetedPipelineRebuildError(
                    "EXISTING_TARGET_PIPELINE_ROW_CHANGED",
                    details={"table": table_name},
                )
        else:
            expected_delta = len(candidate_ids) - int(before_target["row_count"])
            if target_delta != expected_delta or int(after_target["row_count"]) != len(
                candidate_ids
            ):
                raise TargetedPipelineRebuildError(
                    "TARGET_LATEST_ROW_DELTA_MISMATCH",
                    details={
                        "table": table_name,
                        "expected_delta": expected_delta,
                        "actual_delta": target_delta,
                    },
                )
        deltas[table_name] = {
            "target_row_delta": target_delta,
            "non_target_row_delta": non_target_delta,
            "expected_target_row_delta": expected_delta,
            "non_target_canonical_unchanged": True,
        }
    return deltas


def _mapping_sha256(row: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        canonical_json(
            {key: _fingerprint_value(value) for key, value in row.items()}
        ).encode("utf-8")
    ).hexdigest()


def _capture_persisted_row_fingerprints(
    connection: sqlite3.Connection,
    *,
    candidate_id: str,
    strategy_observation_id: str,
    risk_observation_id: str,
    entry_timing_evaluation_id: str,
) -> list[dict[str, str]]:
    row_specs: list[tuple[str, str, str]] = [
        ("strategy_observations", "strategy_observation_id", strategy_observation_id),
        ("strategy_observations_latest", "candidate_instance_id", candidate_id),
        ("risk_observations", "risk_observation_id", risk_observation_id),
        ("risk_observations_latest", "candidate_instance_id", candidate_id),
        (
            "entry_timing_evaluations",
            "entry_timing_evaluation_id",
            entry_timing_evaluation_id,
        ),
    ]
    fingerprints: list[dict[str, str]] = []
    for table_name, id_column, row_id in row_specs:
        row = _required_row(connection, table_name, id_column, row_id)
        fingerprints.append(
            {
                "table": table_name,
                "id_column": id_column,
                "row_id": row_id,
                "canonical_row_sha256": _mapping_sha256(row),
            }
        )
    for table_name, parent_column, parent_id in (
        (
            "strategy_setup_observations",
            "strategy_observation_id",
            strategy_observation_id,
        ),
        ("risk_check_observations", "risk_observation_id", risk_observation_id),
    ):
        for row in connection.execute(
            f'SELECT * FROM "{table_name}" WHERE "{parent_column}" = ? ORDER BY id',
            (parent_id,),
        ).fetchall():
            fingerprints.append(
                {
                    "table": table_name,
                    "id_column": "id",
                    "row_id": str(row["id"]),
                    "canonical_row_sha256": _mapping_sha256(dict(row)),
                }
            )
    return fingerprints


def _assert_persisted_row_fingerprints(
    connection: sqlite3.Connection,
    items: Sequence[Mapping[str, Any]],
) -> None:
    for item in items:
        fingerprints = item.get("canonical_persisted_rows")
        if not isinstance(fingerprints, Sequence):
            raise TargetedPipelineRebuildError("CANONICAL_ROW_EVIDENCE_MISSING")
        for fingerprint in fingerprints:
            if not isinstance(fingerprint, Mapping):
                raise TargetedPipelineRebuildError("CANONICAL_ROW_EVIDENCE_INVALID")
            row = _required_row(
                connection,
                str(fingerprint["table"]),
                str(fingerprint["id_column"]),
                str(fingerprint["row_id"]),
            )
            if _mapping_sha256(row) != fingerprint["canonical_row_sha256"]:
                raise TargetedPipelineRebuildError(
                    "TARGET_PIPELINE_CANONICAL_ROW_CHANGED",
                    details={"table": fingerprint["table"]},
                )


def _table_guard_snapshot(
    connection: sqlite3.Connection,
    table_name: str,
) -> dict[str, Any]:
    row = connection.execute(
        f'SELECT COUNT(*) AS row_count, COALESCE(MAX(rowid), 0) AS high_water_rowid '
        f'FROM "{table_name}"'
    ).fetchone()
    schema_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return {
        "row_count": int(row["row_count"] or 0),
        "high_water_rowid": int(row["high_water_rowid"] or 0),
        "schema_sha256": hashlib.sha256(
            str(schema_row["sql"] or "").encode("utf-8")
        ).hexdigest(),
        "content_guarded_by_authorizer": True,
    }


def _rows_fingerprint(rows: Sequence[sqlite3.Row]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(
            canonical_json({key: _fingerprint_value(row[key]) for key in row.keys()}).encode(
                "utf-8"
            )
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _fingerprint_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    return value


def _command_type_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts = {command_type: 0 for command_type in _ORDER_COMMAND_TYPES}
    rows = connection.execute(
        """
        SELECT command_type, COUNT(*) AS count
        FROM gateway_commands
        WHERE command_type IN (?, ?, ?)
        GROUP BY command_type
        """,
        _ORDER_COMMAND_TYPES,
    ).fetchall()
    for row in rows:
        counts[str(row["command_type"])] = int(row["count"] or 0)
    return counts


def _stored_watermark_hash_valid(row: Mapping[str, Any]) -> bool:
    try:
        watermark = row["source_watermark"]
        parsed = (
            watermark
            if isinstance(watermark, Mapping)
            else json.loads(str(watermark or ""))
        )
    except (TypeError, ValueError):
        return False
    if not isinstance(parsed, Mapping) or not parsed:
        return False
    calculated = hashlib.sha256(canonical_json(parsed).encode("utf-8")).hexdigest()
    return calculated == str(row["source_watermark_hash"] or "")


def _required_row(
    connection: sqlite3.Connection,
    table_name: str,
    id_column: str,
    row_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        f'SELECT * FROM "{table_name}" WHERE "{id_column}" = ?',
        (row_id,),
    ).fetchone()
    if row is None:
        raise TargetedPipelineRebuildError(
            "PERSISTED_PIPELINE_ROW_MISSING",
            details={"table": table_name},
        )
    return dict(row)


def _assert_stage_fence(
    connection: sqlite3.Connection,
    lease: RuntimeExecutionLease,
    stage: str,
    *,
    started_monotonic: float,
) -> None:
    _assert_deadline(
        started_monotonic + TARGETED_PIPELINE_REBUILD_MAX_WALL_SEC,
        stage=stage,
    )
    try:
        assert_runtime_execution_fence(connection, lease=lease, renew=True)
    except Exception as exc:
        raise TargetedPipelineRebuildError(
            "EVALUATION_RUN_FENCE_LOST",
            details={"stage": stage},
        ) from exc


def _assert_deadline(deadline_monotonic: float, *, stage: str) -> None:
    if time.monotonic() > deadline_monotonic:
        raise TargetedPipelineRebuildError(
            "TARGETED_REBUILD_WALL_TIME_EXCEEDED",
            details={"stage": stage},
        )


def _assert_same_source_snapshots(
    expected: Sequence[Mapping[str, Any]],
    actual: Sequence[Mapping[str, Any]],
) -> None:
    expected_hashes = _source_snapshot_hashes(expected)
    actual_hashes = _source_snapshot_hashes(actual)
    if expected_hashes != actual_hashes:
        raise TargetedPipelineRebuildError("CURRENT_SOURCE_CHANGED_DURING_REBUILD")


def _source_snapshot_hashes(
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for item in candidates:
        lineage = item.get("lineage")
        if not isinstance(lineage, Mapping):
            raise TargetedPipelineRebuildError("CURRENT_SOURCE_LINEAGE_MISSING")
        hashes[str(item["candidate_instance_id"])] = hashlib.sha256(
            canonical_json(
                {
                    "source_watermark_hash": lineage.get("source_watermark_hash"),
                    "active_source_fingerprint": item.get(
                        "active_source_fingerprint"
                    ),
                    "active_source_count": item.get("active_source_count"),
                }
            ).encode("utf-8")
        ).hexdigest()
    return hashes


def _normalize_candidate_ids(candidate_ids: Sequence[str]) -> list[str]:
    if isinstance(candidate_ids, (str, bytes)):
        raise TargetedPipelineRebuildError("CANDIDATE_IDS_REQUIRED")
    normalized = [str(candidate_id).strip() for candidate_id in candidate_ids]
    if not normalized or any(not candidate_id for candidate_id in normalized):
        raise TargetedPipelineRebuildError("CANDIDATE_IDS_REQUIRED")
    if len(normalized) > MAX_TARGETED_PIPELINE_REBUILD_BATCH_SIZE:
        raise TargetedPipelineRebuildError("BATCH_SIZE_EXCEEDED")
    if len(set(normalized)) != len(normalized):
        raise TargetedPipelineRebuildError("DUPLICATE_CANDIDATE_ID")
    return normalized


def _normalize_trade_date(value: str) -> str:
    normalized = str(value).strip()
    try:
        parsed = date.fromisoformat(normalized)
    except ValueError as exc:
        raise TargetedPipelineRebuildError("TRADE_DATE_INVALID") from exc
    if parsed.isoformat() != normalized:
        raise TargetedPipelineRebuildError("TRADE_DATE_INVALID")
    return normalized


def _connection_database_path(connection: sqlite3.Connection) -> Path | None:
    for row in connection.execute("PRAGMA database_list"):
        if str(row[1]) == "main" and str(row[2]).strip():
            return Path(str(row[2])).expanduser()
    return None


def _same_path(left: Path, right: Path) -> bool:
    try:
        return left.expanduser().resolve() == right.expanduser().resolve()
    except OSError:
        return False


def _public_candidate(item: Mapping[str, Any]) -> dict[str, Any]:
    lineage = item["lineage"]
    assert isinstance(lineage, Mapping)
    return {
        "candidate_instance_id": item["candidate_instance_id"],
        "trade_date": item["trade_date"],
        "state": item["state"],
        "active_source_count": item["active_source_count"],
        "active_source_fingerprint": item["active_source_fingerprint"],
        "source_run_id": lineage["source_run_id"],
        "source_watermark_hash": lineage["source_watermark_hash"],
    }


def _require_not_order_intent(value: bool) -> None:
    if value is not True:
        raise TargetedPipelineRebuildError("NOT_ORDER_INTENT_REQUIRED")


def _json_string_set(value: object) -> set[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError):
        return {"INVALID_REASON_CODES_JSON"}
    if not isinstance(parsed, list):
        return {"INVALID_REASON_CODES_JSON"}
    return {str(item).strip().upper() for item in parsed if str(item).strip()}


def _settings_sha256(settings: Settings) -> str:
    payload = [
        (name, repr(getattr(settings, name)))
        for name in settings.__dataclass_fields__
        if name != "deprecated_flag_warnings"
    ]
    return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()


def _pipeline_write_authorizer(
    action: int,
    table_name: str | None,
    _column_name: str | None,
    database_name: str | None,
    _trigger_name: str | None,
) -> int:
    if action in _SQLITE_ALWAYS_DENIED_ACTIONS:
        return sqlite3.SQLITE_DENY
    if action in _SQLITE_WRITE_ACTIONS and (
        str(database_name or "") != "main"
        or str(table_name or "") not in _PIPELINE_WRITE_TABLES
    ):
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK
