from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.pipeline_coherency_disposition import (  # noqa: E402
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    preview_pipeline_coherency_disposition,
)
from storage.sqlite import APP_NAME, SCHEMA_VERSION  # noqa: E402
from tools import ops_fast0_blocker_resolution_plan as plan_tool  # noqa: E402
from tools import ops_fast0_strict_requalification as fast0_tool  # noqa: E402

_CONTRACT = "fast0-pipeline-blocker-reconciliation.v1"
_EXPECTED_PLAN_CONTRACT = "fast0-blocker-resolution-plan.v1"
_EXPECTED_SCHEMA_VERSION = "63"
_TARGET_ACTIONS = (
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
)
_SOURCE_STATES = (
    "CANDIDATE_ABSENT",
    "CURRENT_ACTIVE",
    "HISTORICAL_EVENT_ONLY",
    "NO_ACTIVE_SOURCE",
    "ORPHAN_ACTIVE_SOURCE_PRESENT",
    "PROJECTION_INCONSISTENT",
    "UNKNOWN",
)
_DOWNSTREAM_STATES = (
    "ACTIVE_DOWNSTREAM_BLOCKED",
    "MANUAL_BROKER_EVIDENCE_REQUIRED",
    "REPAIR_REQUIRED",
    "SAFE_DB_PROVEN",
    "UNKNOWN",
    "UNSAFE_COMMAND_RECONCILIATION_REQUIRED",
)
_OUTCOMES = (
    "ACTIVE_DOWNSTREAM_BLOCKED",
    "ACTIVE_SOURCE_BLOCKED",
    "CONTRACT_BLOCKED",
    "DB_PREVIEW_ELIGIBLE",
    "DB_RECONCILIATION_CANDIDATE",
    "INVALID_EVIDENCE",
    "MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED",
    "REPAIR_REQUIRED",
)
_REPAIR_DOWNSTREAM_KEYS = (
    "malformed_gateway_command_payload_count",
    "malformed_unbound_order_command_count",
    "unbound_order_command_count",
    "unbound_order_artifact_count",
)
_ACTIVE_DOWNSTREAM_KEYS = (
    "active_live_sim_intent_count",
    "active_live_sim_order_count",
    "active_dry_run_intent_count",
    "active_other_downstream_count",
    "unbound_active_order_artifact_count",
)
_DOWNSTREAM_COUNT_KEYS = (
    "live_sim_intent_count",
    "active_live_sim_intent_count",
    "live_sim_order_count",
    "active_live_sim_order_count",
    "live_sim_execution_count",
    "dry_run_intent_count",
    "active_dry_run_intent_count",
    "dry_run_order_count",
    "dry_run_execution_count",
    "live_sim_rejection_count",
    "dry_run_rejection_count",
    "active_other_downstream_count",
    "gateway_command_count",
    "unsafe_gateway_command_count",
    "malformed_gateway_command_payload_count",
    "malformed_unbound_order_command_count",
    "unbound_order_command_count",
    "unbound_active_order_artifact_count",
    "unbound_order_artifact_count",
    "unresolved_boundary_count",
    "unknown_boundary_count",
    "unexpired_plan_count",
)
_TOOL_ACTIONS = {
    "core_start_invoked": False,
    "gateway_start_invoked": False,
    "worker_start_invoked": False,
    "order_operation_invoked": False,
    "broker_operation_invoked": False,
}


class PipelineBlockerReconciliationError(RuntimeError):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build aggregate-only strict read-only evidence that separates FAST-0 "
            "pipeline blockers into DB-proven, manual-evidence, and repair paths."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--blocker-plan-report", required=True)
    parser.add_argument("--blocker-plan-report-sha256", required=True)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "pipeline_blocker_reconciliation"),
    )
    args = parser.parse_args()
    try:
        report = run_report(
            db_path=Path(args.db),
            blocker_plan_report=Path(args.blocker_plan_report),
            expected_blocker_plan_report_sha256=str(args.blocker_plan_report_sha256).lower(),
            out_dir=Path(args.out_dir),
        )
    except Exception as exc:
        print(
            f"pipeline blocker reconciliation: ERROR error_type={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    print(render_console_summary(report))
    verdict = _mapping(report.get("verdict"))
    return 0 if verdict.get("evidence_status") == "PASS" else 2


def run_report(
    *,
    db_path: Path,
    blocker_plan_report: Path,
    expected_blocker_plan_report_sha256: str,
    out_dir: Path,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    if str(SCHEMA_VERSION) != _EXPECTED_SCHEMA_VERSION:
        raise PipelineBlockerReconciliationError("CODE_TARGET_SCHEMA_MISMATCH")
    if not plan_tool._is_sha256(expected_blocker_plan_report_sha256):
        raise PipelineBlockerReconciliationError("PLAN_REPORT_EXPECTED_SHA256_INVALID")

    baseline_payload, actual_report_sha256 = fast0_tool._read_stable_json(
        blocker_plan_report,
        missing_reason="PLAN_REPORT_NOT_FOUND",
        invalid_reason="PLAN_REPORT_INVALID",
    )
    if actual_report_sha256 != expected_blocker_plan_report_sha256:
        raise PipelineBlockerReconciliationError("PLAN_REPORT_SHA256_MISMATCH")
    baseline = _validate_plan_baseline(
        baseline_payload,
        report_sha256=actual_report_sha256,
    )

    resolved_path = fast0_tool._validated_database_path(db_path)
    fast0_tool._assert_no_sidecars(resolved_path)
    writer_probe_before = fast0_tool._probe_no_other_open_handles(resolved_path)
    collected_at = observed_at or datetime.now(UTC)
    identity_pin = fast0_tool._PinnedDatabaseIdentity(resolved_path)
    with identity_pin:
        files_before = fast0_tool._file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
        baseline_main_matches = fast0_tool._fingerprints_exact(
            baseline["database_main"],
            files_before["main"],
        )
        connection = fast0_tool._open_strict_read_only(
            resolved_path,
            sqlite_source=identity_pin.sqlite_source,
        )
        connection.execute("BEGIN DEFERRED")
        try:
            quick_check_raw = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
            quick_check = ["ok"] if quick_check_raw == ["ok"] else ["not_ok"]
            query_only_row = connection.execute("PRAGMA query_only").fetchone()
            query_only = bool(query_only_row and int(query_only_row[0]) == 1)
            database_list = [
                {"seq": int(row[0]), "name": str(row[1]), "file_present": bool(row[2])}
                for row in connection.execute("PRAGMA database_list").fetchall()
                if bool(row[2])
            ]
            identity = fast0_tool._read_database_identity(connection)
            schema_raw = fast0_tool._validate_schema_manifest(connection)
            schema_manifest = {
                "ready": schema_raw.get("ready") is True,
                "expected_object_count": schema_raw.get("expected_object_count"),
                "present_object_count": schema_raw.get("present_object_count"),
                "valid_object_count": schema_raw.get("valid_object_count"),
                "invalid_object_count": len(schema_raw.get("invalid_objects") or []),
                "persistent_trigger_set_valid": (
                    schema_raw.get("persistent_trigger_set_valid") is True
                ),
                "critical_trigger_sets_valid": (
                    schema_raw.get("critical_trigger_sets_valid") is True
                ),
            }
            runtime_lock_count = int(
                connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0]
            )
            first, items, pages = plan_tool._read_pipeline_pages(
                connection,
                observed_at=collected_at,
            )
            reconciliation = _reconcile_pipeline_targets(
                connection,
                first=first,
                items=items,
                pages=pages,
                observed_at=collected_at,
            )
        finally:
            connection.rollback()
            connection.close()

        identity_pin.assert_path_identity()
        fast0_tool._validated_database_path(resolved_path)
        fast0_tool._assert_no_sidecars(resolved_path)
        files_after = fast0_tool._file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
    writer_probe_after = fast0_tool._probe_no_other_open_handles(resolved_path)
    whole_window_writer_absence_proven = bool(
        identity_pin.method == "WINDOWS_READ_HANDLE_DENY_WRITE_DELETE"
        and writer_probe_before.get("status") == "PASS"
        and writer_probe_before.get("no_other_open_handles") is True
        and writer_probe_after.get("status") == "PASS"
        and writer_probe_after.get("no_other_open_handles") is True
    )

    report: dict[str, Any] = {
        "contract": _CONTRACT,
        "generated_at": _wire(collected_at),
        "source_blocker_plan": {
            "contract": baseline["contract"],
            "report_sha256": actual_report_sha256,
            "evidence_status": baseline["evidence_status"],
            "plan_status": baseline["plan_status"],
            "database_main_matches": baseline_main_matches,
            "pipeline_expected": baseline["pipeline_expected"],
            "path_recorded": False,
        },
        "database": {
            "identity": identity,
            "identity_pin": identity_pin.public_status(),
            "connection": {
                "mode": "ro",
                "immutable": True,
                "query_only": query_only,
                "single_deferred_snapshot": True,
            },
            "database_list": database_list,
            "quick_check": quick_check,
            "files_before": files_before,
            "files_after": files_after,
            "sidecars_absent_before": True,
            "sidecars_absent_after": True,
            "writer_probe_before": writer_probe_before,
            "writer_probe_after": writer_probe_after,
            "whole_window_writer_absence_proven": whole_window_writer_absence_proven,
            "runtime_lock_count": runtime_lock_count,
        },
        "schema_manifest": schema_manifest,
        "pipeline_reconciliation": reconciliation,
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "database_write_performed": False,
        "apply_authorized": False,
        "eligibility_changed": False,
        "tool_actions": {
            "core_start_invoked": False,
            "gateway_start_invoked": False,
            "worker_start_invoked": False,
            "order_operation_invoked": False,
            "broker_operation_invoked": False,
        },
        "side_effect_scope": "THIS_TOOL_EXECUTION_ONLY",
        "external_process_state_verified": False,
        "external_broker_state_verified": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    return report


def _validate_plan_baseline(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
) -> dict[str, Any]:
    verdict = _mapping(payload.get("verdict"))
    database = _mapping(payload.get("database"))
    identity = _mapping(database.get("identity"))
    identity_pin = _mapping(database.get("identity_pin"))
    files_after = _mapping(database.get("files_after"))
    database_main = _mapping(files_after.get("main"))
    pipeline = _mapping(payload.get("pipeline"))
    action_counts = _mapping(pipeline.get("expected_action_counts"))
    required_action_counts = {action: action_counts.get(action) for action in _TARGET_ACTIONS}
    valid = bool(
        payload.get("contract") == _EXPECTED_PLAN_CONTRACT
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("plan_status") == "COMPLETE"
        and payload.get("read_only") is True
        and payload.get("database_write_performed") is False
        and payload.get("identifiers_recorded") is False
        and identity.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and identity.get("schema_version_value_valid") is True
        and identity_pin.get("status") == "PASS"
        and plan_tool._fingerprint_valid(database_main)
        and plan_tool._is_nonnegative_int(pipeline.get("full_count"))
        and plan_tool._is_sha256(pipeline.get("inventory_digest"))
        and plan_tool._is_nonnegative_int(pipeline.get("collected_count"))
        and pipeline.get("inventory_count_consistent") is True
        and all(
            plan_tool._is_nonnegative_int(required_action_counts[action])
            for action in _TARGET_ACTIONS
        )
    )
    if not valid:
        raise PipelineBlockerReconciliationError("PLAN_REPORT_CONTRACT_INVALID")
    return {
        "contract": payload.get("contract"),
        "report_sha256": report_sha256,
        "evidence_status": verdict.get("evidence_status"),
        "plan_status": verdict.get("plan_status"),
        "database_main": dict(database_main),
        "pipeline_expected": {
            "full_count": pipeline.get("full_count"),
            "collected_count": pipeline.get("collected_count"),
            "inventory_digest": pipeline.get("inventory_digest"),
            "expected_action_counts": required_action_counts,
        },
    }


def _reconcile_pipeline_targets(
    connection: sqlite3.Connection,
    *,
    first: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
    pages: Sequence[Mapping[str, Any]],
    observed_at: datetime,
) -> dict[str, Any]:
    action_counts: Counter[str] = Counter()
    source_states: Counter[str] = Counter()
    downstream_states: Counter[str] = Counter()
    outcomes: Counter[str] = Counter()
    blocker_reasons: Counter[str] = Counter()
    preview_eligible_actions: Counter[str] = Counter()
    target_entries: list[dict[str, Any]] = []
    expired_entries: list[dict[str, Any]] = []
    orphan_entries: list[dict[str, Any]] = []
    invalid_item_count = 0
    preview_eligible_count = 0
    manual_evidence_required_count = 0
    repair_required_count = 0

    for item in sorted(items, key=lambda value: str(value.get("candidate_instance_id") or "")):
        action = plan_tool._expected_pipeline_action(item)
        if action not in _TARGET_ACTIONS:
            continue
        candidate_id = item.get("candidate_instance_id")
        trade_date = item.get("trade_date")
        if (
            not isinstance(candidate_id, str)
            or not candidate_id
            or not isinstance(trade_date, str)
            or not trade_date
        ):
            invalid_item_count += 1
            outcomes["INVALID_EVIDENCE"] += 1
            continue
        preview = preview_pipeline_coherency_disposition(
            connection,
            trade_date=trade_date,
            candidate_instance_id=candidate_id,
            action=action,
            as_of=observed_at,
        )
        expected = {
            "candidate_instance_id": candidate_id,
            "trade_date": trade_date,
            "action": action,
            "pipeline_fingerprint": item.get("pipeline_fingerprint"),
            "subject_version": item.get("subject_version"),
        }
        for key in plan_tool._PIPELINE_CAS_KEYS[2:]:
            expected[key] = preview.get(key)
        cas_safe = plan_tool._pipeline_preview_safe(preview, expected=expected)
        source = _mapping(preview.get("source"))
        candidate = _mapping(preview.get("candidate"))
        downstream = _mapping(preview.get("downstream"))
        source_state = _source_state(source, candidate=candidate)
        downstream_state = _downstream_state(downstream)
        reasons, reasons_valid = _preview_reason_codes(preview)
        semantic_safe = _preview_semantics_safe(
            preview,
            action=action,
            reasons=reasons,
            reasons_valid=reasons_valid,
            source_state=source_state,
            downstream_state=downstream_state,
        )
        safe = cas_safe and semantic_safe
        outcome = _reconciliation_outcome(
            action=action,
            source_state=source_state,
            downstream_state=downstream_state,
            preview_eligible=preview.get("eligible") is True,
            preview_safe=safe,
        )
        for reason in reasons:
            blocker_reasons[reason] += 1
        if not cas_safe:
            blocker_reasons["PIPELINE_PREVIEW_SAFETY_CONTRACT_INVALID"] += 1
        if not semantic_safe:
            blocker_reasons["PIPELINE_PREVIEW_SEMANTIC_CONTRACT_INVALID"] += 1
        if not safe or outcome == "INVALID_EVIDENCE":
            invalid_item_count += 1
        if preview.get("eligible") is True and safe:
            preview_eligible_count += 1
            preview_eligible_actions[action] += 1
        if outcome == "MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED":
            manual_evidence_required_count += 1
        if outcome == "REPAIR_REQUIRED":
            repair_required_count += 1
        action_counts[action] += 1
        source_states[source_state] += 1
        downstream_states[downstream_state] += 1
        outcomes[outcome] += 1
        entry = {
            **expected,
            "source_state": source_state,
            "downstream_state": downstream_state,
            "outcome": outcome,
            "reason_codes": sorted(set(reasons)),
        }
        target_entries.append(entry)
        if action == ACTION_DISPOSE_EXPIRED_PLAN_READY:
            expired_entries.append(entry)
        else:
            orphan_entries.append(entry)

    target_entries = plan_tool._with_aliases(
        target_entries,
        prefix="C",
        width=2,
        ordering_key="candidate_instance_id",
    )
    expired_entries = plan_tool._with_aliases(
        expired_entries,
        prefix="E",
        width=2,
        ordering_key="candidate_instance_id",
    )
    orphan_entries = plan_tool._with_aliases(
        orphan_entries,
        prefix="O",
        width=2,
        ordering_key="candidate_instance_id",
    )
    return {
        "trade_date_present": bool(first.get("trade_date")),
        "full_count": int(first.get("full_count") or 0),
        "collected_count": len(items),
        "page_count": len(pages),
        "inventory_digest": first.get("inventory_digest"),
        "inventory_end_digest": first.get("inventory_end_digest"),
        "inventory_count_consistent": first.get("inventory_count_consistent") is True,
        "target_count": len(target_entries),
        "expected_action_counts": {
            action: int(action_counts[action]) for action in _TARGET_ACTIONS
        },
        "source_state_counts": {state: int(source_states[state]) for state in _SOURCE_STATES},
        "downstream_state_counts": {
            state: int(downstream_states[state]) for state in _DOWNSTREAM_STATES
        },
        "outcome_counts": {state: int(outcomes[state]) for state in _OUTCOMES},
        "preview_eligible_count": preview_eligible_count,
        "preview_eligible_action_counts": {
            action: int(preview_eligible_actions[action]) for action in _TARGET_ACTIONS
        },
        "manual_evidence_required_count": manual_evidence_required_count,
        "repair_required_count": repair_required_count,
        "blocker_reason_counts": dict(sorted(blocker_reasons.items())),
        "target_set_sha256": plan_tool._manifest_sha256(target_entries),
        "expired_plan_target_sha256": plan_tool._manifest_sha256(expired_entries),
        "orphan_target_sha256": plan_tool._manifest_sha256(orphan_entries),
        "alias_contract": {
            "target_format": "C{ordinal:02d}",
            "expired_format": "E{ordinal:02d}",
            "orphan_format": "O{ordinal:02d}",
            "mapping_recorded": False,
        },
        "invalid_item_count": invalid_item_count,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "read_only": True,
        "observe_only": True,
        "database_write_performed": False,
        "eligibility_changed": False,
        "apply_authorized": False,
    }


def _source_state(
    source: Mapping[str, Any],
    *,
    candidate: Mapping[str, Any],
) -> str:
    historical_active = _strict_nonnegative_int(
        source.get("historical_event_active_count", source.get("active_count"))
    )
    latest_active = _strict_nonnegative_int(source.get("latest_active_count"))
    candidate_present = candidate.get("present")
    consistent = source.get("source_projection_consistent")
    if historical_active is None or latest_active is None or type(candidate_present) is not bool:
        return "UNKNOWN"
    if candidate_present is False:
        if historical_active > 0 or latest_active > 0:
            return "ORPHAN_ACTIVE_SOURCE_PRESENT"
        return "CANDIDATE_ABSENT"
    candidate_active = _strict_nonnegative_int(candidate.get("active_source_count"))
    if candidate_active is None or type(consistent) is not bool:
        return "UNKNOWN"
    if not consistent or candidate_active != latest_active:
        return "PROJECTION_INCONSISTENT"
    if latest_active > 0 or candidate_active > 0:
        return "CURRENT_ACTIVE"
    if historical_active > 0:
        return "HISTORICAL_EVENT_ONLY"
    return "NO_ACTIVE_SOURCE"


def _downstream_state(downstream: Mapping[str, Any]) -> str:
    if any(_strict_nonnegative_int(downstream.get(key)) is None for key in _DOWNSTREAM_COUNT_KEYS):
        return "UNKNOWN"
    if any(_positive_count(downstream.get(key)) for key in _REPAIR_DOWNSTREAM_KEYS):
        return "REPAIR_REQUIRED"
    if any(_positive_count(downstream.get(key)) for key in _ACTIVE_DOWNSTREAM_KEYS):
        return "ACTIVE_DOWNSTREAM_BLOCKED"
    if _positive_count(downstream.get("unknown_boundary_count")) or _positive_count(
        downstream.get("unresolved_boundary_count")
    ):
        return "MANUAL_BROKER_EVIDENCE_REQUIRED"
    if _positive_count(downstream.get("unsafe_gateway_command_count")):
        return "UNSAFE_COMMAND_RECONCILIATION_REQUIRED"
    return "SAFE_DB_PROVEN"


def _preview_reason_codes(preview: Mapping[str, Any]) -> tuple[list[str], bool]:
    raw = preview.get("reason_codes")
    if type(raw) is not list:
        return [], False
    if any(type(value) is not str or not value.strip() for value in raw):
        return [], False
    reasons = list(raw)
    return reasons, len(reasons) == len(set(reasons))


def _preview_semantics_safe(
    preview: Mapping[str, Any],
    *,
    action: str,
    reasons: Sequence[str],
    reasons_valid: bool,
    source_state: str,
    downstream_state: str,
) -> bool:
    eligible = preview.get("eligible")
    if type(eligible) is not bool or not reasons_valid:
        return False
    if preview.get("action") != action or preview.get("expected_action") != action:
        return False
    if preview.get("status") != ("ELIGIBLE" if eligible else "BLOCKED"):
        return False
    if eligible is not (len(reasons) == 0):
        return False
    eligible_source_state = {
        ACTION_DISPOSE_EXPIRED_PLAN_READY: "NO_ACTIVE_SOURCE",
        ACTION_DISPOSE_ORPHAN: "CANDIDATE_ABSENT",
    }.get(action)
    if eligible and source_state != eligible_source_state:
        return False
    if eligible and downstream_state != "SAFE_DB_PROVEN":
        return False
    return source_state != "UNKNOWN" and downstream_state != "UNKNOWN"


def _reconciliation_outcome(
    *,
    action: str,
    source_state: str,
    downstream_state: str,
    preview_eligible: bool,
    preview_safe: bool,
) -> str:
    if not preview_safe or source_state == "UNKNOWN" or downstream_state == "UNKNOWN":
        return "INVALID_EVIDENCE"
    if source_state == "PROJECTION_INCONSISTENT" or downstream_state == "REPAIR_REQUIRED":
        return "REPAIR_REQUIRED"
    if source_state in {"CURRENT_ACTIVE", "ORPHAN_ACTIVE_SOURCE_PRESENT"}:
        return "ACTIVE_SOURCE_BLOCKED"
    if downstream_state == "ACTIVE_DOWNSTREAM_BLOCKED":
        return "ACTIVE_DOWNSTREAM_BLOCKED"
    if action == ACTION_DISPOSE_ORPHAN:
        return "MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED"
    if downstream_state in {
        "MANUAL_BROKER_EVIDENCE_REQUIRED",
        "UNSAFE_COMMAND_RECONCILIATION_REQUIRED",
    }:
        return "MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED"
    if preview_eligible:
        return "DB_PREVIEW_ELIGIBLE"
    if source_state == "HISTORICAL_EVENT_ONLY":
        return "DB_RECONCILIATION_CANDIDATE"
    return "CONTRACT_BLOCKED"


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    preparation: list[str] = []
    source = _mapping(report.get("source_blocker_plan"))
    expected = _mapping(source.get("pipeline_expected"))
    expected_actions = _mapping(expected.get("expected_action_counts"))
    database = _mapping(report.get("database"))
    identity = _mapping(database.get("identity"))
    identity_pin = _mapping(database.get("identity_pin"))
    connection = _mapping(database.get("connection"))
    schema = _mapping(report.get("schema_manifest"))
    reconciliation = _mapping(report.get("pipeline_reconciliation"))
    actual_actions = _mapping(reconciliation.get("expected_action_counts"))
    before = database.get("files_before")
    after = database.get("files_after")

    if report.get("contract") != _CONTRACT:
        failures.append("RECONCILIATION_REPORT_CONTRACT_INVALID")
    if (
        source.get("contract") != _EXPECTED_PLAN_CONTRACT
        or not plan_tool._is_sha256(source.get("report_sha256"))
        or source.get("evidence_status") != "PASS"
        or source.get("plan_status") != "COMPLETE"
        or source.get("path_recorded") is not False
    ):
        failures.append("SOURCE_BLOCKER_PLAN_CONTRACT_INVALID")
    if source.get("database_main_matches") is not True:
        failures.append("DATABASE_DOES_NOT_MATCH_APPROVED_PLAN")
    if (
        before != after
        or not plan_tool._fingerprint_valid(_mapping(_mapping(before).get("main")))
        or not plan_tool._fingerprint_valid(_mapping(_mapping(after).get("main")))
    ):
        failures.append("DATABASE_FILES_CHANGED_DURING_RECONCILIATION")
    if database.get("quick_check") != ["ok"]:
        failures.append("DATABASE_QUICK_CHECK_FAILED")
    if (
        identity.get("app_name") != APP_NAME
        or _strict_nonnegative_int(identity.get("app_name_row_count")) != 1
        or identity.get("app_name_value_valid") is not True
    ):
        failures.append("DATABASE_APP_IDENTITY_INVALID")
    if identity.get("schema_version") != _EXPECTED_SCHEMA_VERSION:
        failures.append("DATABASE_SCHEMA_VERSION_MISMATCH")
    if (
        _strict_nonnegative_int(identity.get("schema_version_row_count")) != 1
        or identity.get("schema_version_value_valid") is not True
    ):
        failures.append("DATABASE_SCHEMA_IDENTITY_INVALID")
    if (
        identity_pin.get("status") != "PASS"
        or identity_pin.get("method")
        not in {"WINDOWS_READ_HANDLE_DENY_WRITE_DELETE", "POSIX_PINNED_DESCRIPTOR_PATH"}
        or identity_pin.get("held_across_snapshot") is not True
        or identity_pin.get("path_identity_stable") is not True
        or identity_pin.get("raw_identity_recorded") is not False
    ):
        failures.append("DATABASE_IDENTITY_PIN_FAILED")
    if not _exact_mapping_contract(
        connection,
        {
            "mode": "ro",
            "immutable": True,
            "query_only": True,
            "single_deferred_snapshot": True,
        },
    ):
        failures.append("STRICT_READ_ONLY_CONNECTION_INVALID")
    database_list = database.get("database_list")
    if not (
        type(database_list) is list
        and len(database_list) == 1
        and isinstance(database_list[0], Mapping)
        and _exact_mapping_contract(
            database_list[0],
            {"seq": 0, "name": "main", "file_present": True},
        )
    ):
        failures.append("DATABASE_ATTACHMENT_CONTRACT_INVALID")
    if (
        database.get("sidecars_absent_before") is not True
        or database.get("sidecars_absent_after") is not True
    ):
        failures.append("DATABASE_SIDECAR_CONTRACT_INVALID")
    for key in ("writer_probe_before", "writer_probe_after"):
        probe = _mapping(database.get(key))
        if probe.get("status") != "PASS" or probe.get("no_other_open_handles") is not True:
            failures.append("OTHER_DATABASE_OPEN_HANDLE_PRESENT")
    if _strict_nonnegative_int(database.get("runtime_lock_count")) != 0:
        failures.append("RUNTIME_EXECUTION_LOCK_PRESENT")
    if (
        schema.get("ready") is not True
        or _strict_nonnegative_int(schema.get("invalid_object_count")) != 0
    ):
        failures.append("SCHEMA_MANIFEST_INVALID")
    expected_full_count = _strict_nonnegative_int(expected.get("full_count"))
    expected_collected_count = _strict_nonnegative_int(expected.get("collected_count"))
    actual_full_count = _strict_nonnegative_int(reconciliation.get("full_count"))
    actual_collected_count = _strict_nonnegative_int(reconciliation.get("collected_count"))
    if (
        reconciliation.get("inventory_count_consistent") is not True
        or expected_full_count is None
        or expected_collected_count is None
        or actual_full_count is None
        or actual_collected_count is None
        or not plan_tool._is_sha256(expected.get("inventory_digest"))
        or not plan_tool._is_sha256(reconciliation.get("inventory_digest"))
        or not plan_tool._is_sha256(reconciliation.get("inventory_end_digest"))
        or reconciliation.get("inventory_digest") != reconciliation.get("inventory_end_digest")
        or actual_full_count != expected_full_count
        or actual_collected_count != expected_collected_count
        or reconciliation.get("inventory_digest") != expected.get("inventory_digest")
    ):
        failures.append("PIPELINE_INVENTORY_BASELINE_MISMATCH")
    if any(
        actual_actions.get(action) != expected_actions.get(action) for action in _TARGET_ACTIONS
    ):
        failures.append("PIPELINE_TARGET_COUNT_BASELINE_MISMATCH")
    target_count = _strict_nonnegative_int(reconciliation.get("target_count"))
    source_counts = _mapping(reconciliation.get("source_state_counts"))
    downstream_counts = _mapping(reconciliation.get("downstream_state_counts"))
    outcome_counts = _mapping(reconciliation.get("outcome_counts"))
    preview_eligible_count = _strict_nonnegative_int(reconciliation.get("preview_eligible_count"))
    preview_eligible_actions = _mapping(reconciliation.get("preview_eligible_action_counts"))
    manual_evidence_required_count = _strict_nonnegative_int(
        reconciliation.get("manual_evidence_required_count")
    )
    repair_required_count = _strict_nonnegative_int(reconciliation.get("repair_required_count"))
    if (
        target_count is None
        or not _fixed_counts_conserve(expected_actions, _TARGET_ACTIONS, target_count)
        or not _fixed_counts_conserve(actual_actions, _TARGET_ACTIONS, target_count)
        or not _fixed_counts_conserve(source_counts, _SOURCE_STATES, target_count)
        or not _fixed_counts_conserve(downstream_counts, _DOWNSTREAM_STATES, target_count)
        or not _fixed_counts_conserve(outcome_counts, _OUTCOMES, target_count)
        or preview_eligible_count is None
        or preview_eligible_count > target_count
        or not _fixed_counts_conserve(
            preview_eligible_actions, _TARGET_ACTIONS, preview_eligible_count or 0
        )
        or any(
            preview_eligible_actions.get(action, 0) > actual_actions.get(action, 0)
            for action in _TARGET_ACTIONS
        )
        or outcome_counts.get("DB_PREVIEW_ELIGIBLE")
        != preview_eligible_actions.get(ACTION_DISPOSE_EXPIRED_PLAN_READY)
        or preview_eligible_actions.get(ACTION_DISPOSE_ORPHAN, 0)
        > outcome_counts.get("MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED", 0)
        or manual_evidence_required_count is None
        or manual_evidence_required_count
        != outcome_counts.get("MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED")
        or repair_required_count is None
        or repair_required_count != outcome_counts.get("REPAIR_REQUIRED")
    ):
        failures.append("PIPELINE_RECONCILIATION_COUNT_CONSERVATION_INVALID")
    if _strict_nonnegative_int(reconciliation.get("invalid_item_count")) != 0:
        failures.append("PIPELINE_RECONCILIATION_ITEM_INVALID")
    top_level_contract = {
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "database_write_performed": False,
        "apply_authorized": False,
        "eligibility_changed": False,
        "side_effect_scope": "THIS_TOOL_EXECUTION_ONLY",
        "external_process_state_verified": False,
        "external_broker_state_verified": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    reconciliation_contract = {
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "database_write_performed": False,
        "apply_authorized": False,
        "eligibility_changed": False,
    }
    if (
        any(not _exact_scalar(report.get(key), value) for key, value in top_level_contract.items())
        or not _exact_mapping_contract(_mapping(report.get("tool_actions")), _TOOL_ACTIONS)
        or any(
            not _exact_scalar(reconciliation.get(key), value)
            for key, value in reconciliation_contract.items()
        )
    ):
        failures.append("READ_ONLY_RECONCILIATION_CONTRACT_INVALID")

    if _positive_count(outcome_counts.get("INVALID_EVIDENCE")):
        failures.append("PIPELINE_RECONCILIATION_EVIDENCE_INVALID")
    if _positive_count(outcome_counts.get("MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED")):
        preparation.append("PIPELINE_MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED")
    if _positive_count(outcome_counts.get("REPAIR_REQUIRED")):
        preparation.append("PIPELINE_DATA_REPAIR_DESIGN_REQUIRED")
    if _positive_count(outcome_counts.get("ACTIVE_SOURCE_BLOCKED")):
        preparation.append("PIPELINE_ACTIVE_SOURCE_RECONCILIATION_REQUIRED")
    if _positive_count(outcome_counts.get("ACTIVE_DOWNSTREAM_BLOCKED")):
        preparation.append("PIPELINE_ACTIVE_DOWNSTREAM_RECONCILIATION_REQUIRED")
    if _positive_count(outcome_counts.get("DB_RECONCILIATION_CANDIDATE")):
        preparation.append("PIPELINE_ELIGIBILITY_CONTRACT_REVIEW_REQUIRED")
    if _positive_count(outcome_counts.get("CONTRACT_BLOCKED")):
        preparation.append("PIPELINE_DISPOSITION_CONTRACT_BLOCKED")
    if _positive_count(outcome_counts.get("DB_PREVIEW_ELIGIBLE")):
        preparation.append("PIPELINE_SEPARATE_APPLY_APPROVAL_REQUIRED")
    if database.get("whole_window_writer_absence_proven") is not True:
        preparation.append("PIPELINE_WHOLE_WINDOW_WRITER_QUIESCENCE_REQUIRED")
    preparation.append("PIPELINE_LEGACY_AUTHORITATIVE_EVIDENCE_RESOLVER_REQUIRED")

    evidence_status = "PASS" if not failures else "FAIL"
    reconciliation_status = "COMPLETE" if not failures else "BLOCKED"
    return {
        "status": evidence_status,
        "evidence_status": evidence_status,
        "reconciliation_status": reconciliation_status,
        "execution_readiness": ("PREPARATION_REQUIRED" if not failures else "BLOCKED"),
        "evidence_failures": sorted(set(failures)),
        "preparation_requirements": sorted(set(preparation)),
        "database_files_unchanged": before == after,
        "read_only": True,
        "eligibility_changed": False,
        "apply_authorized": False,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    safe_report = _mapping(fast0_tool._redact(report))
    plan_tool._assert_aggregate_only_report(safe_report)
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    commands_path = report_dir / "commands.txt"
    raw_path.write_text(
        json.dumps(safe_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(safe_report), encoding="utf-8")
    commands_path.write_text(
        "python -B -m tools.ops_pipeline_blocker_reconciliation "
        "--db <schema-62-db> --blocker-plan-report <approved-plan-raw.json> "
        "--blocker-plan-report-sha256 <approved-sha256> --out-dir <evidence-dir>\n"
        "apply: NOT AUTHORIZED; this tool does not change eligibility or database state\n",
        encoding="utf-8",
    )
    return {
        "raw_json": raw_path,
        "summary_md": summary_path,
        "commands_txt": commands_path,
    }


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    reconciliation = _mapping(report.get("pipeline_reconciliation"))
    outcomes = _mapping(reconciliation.get("outcome_counts"))
    return "\n".join(
        [
            "# FAST-0 Pipeline Blocker Reconciliation",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- evidence_status: `{verdict.get('evidence_status')}`",
            f"- reconciliation_status: `{verdict.get('reconciliation_status')}`",
            f"- execution_readiness: `{verdict.get('execution_readiness')}`",
            f"- target_count: `{reconciliation.get('target_count')}`",
            f"- DB-preview eligible: `{outcomes.get('DB_PREVIEW_ELIGIBLE')}`",
            (
                "- manual-authoritative-evidence required: "
                f"`{outcomes.get('MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED')}`"
            ),
            f"- repair required: `{outcomes.get('REPAIR_REQUIRED')}`",
            f"- target_set_sha256: `{reconciliation.get('target_set_sha256')}`",
            f"- failures: `{', '.join(verdict.get('evidence_failures') or []) or '-'}`",
            "",
            "This report is aggregate-only and does not change eligibility or database state.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    reconciliation = _mapping(report.get("pipeline_reconciliation"))
    return (
        "pipeline blocker reconciliation: "
        f"evidence={verdict.get('evidence_status')} "
        f"status={verdict.get('reconciliation_status')} "
        f"targets={reconciliation.get('target_count')} "
        "apply_authorized=false"
    )


def _strict_nonnegative_int(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _positive_count(value: Any) -> bool:
    return bool(type(value) is int and value > 0)


def _fixed_counts_conserve(
    counts: Mapping[str, Any],
    keys: Sequence[str],
    expected_total: int,
) -> bool:
    return bool(
        frozenset(counts) == frozenset(keys)
        and all(_strict_nonnegative_int(counts.get(key)) is not None for key in keys)
        and sum(int(counts[key]) for key in keys) == expected_total
    )


def _exact_scalar(value: Any, expected: Any) -> bool:
    return type(value) is type(expected) and value == expected


def _exact_mapping_contract(value: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return bool(
        frozenset(value) == frozenset(expected)
        and all(
            _exact_scalar(value.get(key), expected_value)
            for key, expected_value in expected.items()
        )
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
