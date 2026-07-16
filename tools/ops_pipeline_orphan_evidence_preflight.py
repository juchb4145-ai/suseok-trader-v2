from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.pipeline_coherency_disposition import (  # noqa: E402
    ACTION_DISPOSE_ORPHAN,
    audit_pipeline_orphan_disposition_rows,
    preview_pipeline_coherency_disposition,
)
from services.pipeline_orphan_manual_evidence import (  # noqa: E402
    ORPHAN_CLASSIFICATION,
    ORPHAN_EVIDENCE_CONTRACT,
    ORPHAN_EVIDENCE_SOURCE_TYPES,
    PRIVATE_TARGET_SET_CONTRACT,
    OrphanManualEvidenceError,
    read_stable_file_fingerprint,
    read_stable_json_document,
    require_utc_z_timestamp,
    validate_orphan_evidence_binding,
    validate_orphan_manual_evidence_document,
    validate_private_target_manifest,
)
from storage.sqlite import APP_NAME, SCHEMA_VERSION  # noqa: E402
from tools import ops_fast0_blocker_resolution_plan as plan_tool  # noqa: E402
from tools import ops_fast0_strict_requalification as fast0_tool  # noqa: E402
from tools import ops_pipeline_blocker_reconciliation as reconciliation_tool  # noqa: E402

_CONTRACT = "fast0-pipeline-orphan-evidence-preflight.v1"
_EXPECTED_PLAN_CONTRACT = "fast0-blocker-resolution-plan.v1"
_EXPECTED_RECONCILIATION_CONTRACT = "fast0-pipeline-blocker-reconciliation.v1"
_EXPECTED_SCHEMA_VERSION = "62"
_ALIAS_RE = re.compile(r"^M(?:00[1-9]|0[1-9][0-9]|[1-9][0-9]{2})$")
_KST = timezone(timedelta(hours=9), name="Asia/Seoul")
_TOOL_ACTIONS = {
    "core_start_invoked": False,
    "gateway_start_invoked": False,
    "worker_start_invoked": False,
    "order_operation_invoked": False,
    "broker_operation_invoked": False,
}


class PipelineOrphanEvidencePreflightError(RuntimeError):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Strict read-only preflight that binds one private orphan evidence file "
            "to approved FAST-0 plans, the private target manifest, and current DB CAS."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--blocker-plan-report", required=True)
    parser.add_argument("--blocker-plan-report-sha256", required=True)
    parser.add_argument("--reconciliation-report", required=True)
    parser.add_argument("--reconciliation-report-sha256", required=True)
    parser.add_argument("--private-target-manifest", required=True)
    parser.add_argument("--private-target-manifest-sha256", required=True)
    parser.add_argument("--evidence-file", required=True)
    parser.add_argument("--evidence-file-sha256", required=True)
    parser.add_argument("--authoritative-artifact", required=True)
    parser.add_argument("--authoritative-artifact-sha256", required=True)
    parser.add_argument("--account-scope-sha256", required=True)
    parser.add_argument("--alias", required=True)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "pipeline_orphan_evidence_preflight"),
    )
    args = parser.parse_args()
    try:
        report = run_report(
            db_path=Path(args.db),
            blocker_plan_report=Path(args.blocker_plan_report),
            expected_blocker_plan_report_sha256=str(args.blocker_plan_report_sha256).lower(),
            reconciliation_report=Path(args.reconciliation_report),
            expected_reconciliation_report_sha256=str(args.reconciliation_report_sha256).lower(),
            private_target_manifest=Path(args.private_target_manifest),
            expected_private_target_manifest_sha256=str(
                args.private_target_manifest_sha256
            ).lower(),
            evidence_file=Path(args.evidence_file),
            expected_evidence_file_sha256=str(args.evidence_file_sha256).lower(),
            authoritative_artifact=Path(args.authoritative_artifact),
            expected_authoritative_artifact_sha256=str(args.authoritative_artifact_sha256).lower(),
            expected_account_scope_sha256=str(args.account_scope_sha256).lower(),
            alias=str(args.alias),
            out_dir=Path(args.out_dir),
        )
    except Exception as exc:
        print(
            f"pipeline orphan evidence preflight: ERROR error_type={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    print(render_console_summary(report))
    return 0 if _mapping(report.get("verdict")).get("evidence_status") == "PASS" else 2


def run_report(
    *,
    db_path: Path,
    blocker_plan_report: Path,
    expected_blocker_plan_report_sha256: str,
    reconciliation_report: Path,
    expected_reconciliation_report_sha256: str,
    private_target_manifest: Path,
    expected_private_target_manifest_sha256: str,
    evidence_file: Path,
    expected_evidence_file_sha256: str,
    authoritative_artifact: Path,
    expected_authoritative_artifact_sha256: str,
    expected_account_scope_sha256: str,
    alias: str,
    out_dir: Path,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    if str(SCHEMA_VERSION) != _EXPECTED_SCHEMA_VERSION:
        raise PipelineOrphanEvidencePreflightError("CODE_TARGET_SCHEMA_MISMATCH")
    collected_at = observed_at or datetime.now(UTC)
    if collected_at.tzinfo is None or collected_at.utcoffset() is None:
        raise PipelineOrphanEvidencePreflightError("PREFLIGHT_OBSERVED_AT_INVALID")
    collected_at = collected_at.astimezone(UTC)
    for name, value in (
        ("PLAN_REPORT_SHA256", expected_blocker_plan_report_sha256),
        ("RECONCILIATION_REPORT_SHA256", expected_reconciliation_report_sha256),
        ("PRIVATE_TARGET_MANIFEST_SHA256", expected_private_target_manifest_sha256),
        ("EVIDENCE_FILE_SHA256", expected_evidence_file_sha256),
        ("AUTHORITATIVE_ARTIFACT_SHA256", expected_authoritative_artifact_sha256),
        ("ACCOUNT_SCOPE_SHA256", expected_account_scope_sha256),
    ):
        if not plan_tool._is_sha256(value):
            raise PipelineOrphanEvidencePreflightError(f"{name}_INVALID")

    plan_payload, plan_report_sha256 = fast0_tool._read_stable_json(
        blocker_plan_report,
        missing_reason="PLAN_REPORT_NOT_FOUND",
        invalid_reason="PLAN_REPORT_INVALID",
    )
    if plan_report_sha256 != expected_blocker_plan_report_sha256:
        raise PipelineOrphanEvidencePreflightError("PLAN_REPORT_SHA256_MISMATCH")
    plan = _validate_plan(plan_payload, report_sha256=plan_report_sha256)

    reconciliation_payload, reconciliation_report_sha256 = fast0_tool._read_stable_json(
        reconciliation_report,
        missing_reason="RECONCILIATION_REPORT_NOT_FOUND",
        invalid_reason="RECONCILIATION_REPORT_INVALID",
    )
    if reconciliation_report_sha256 != expected_reconciliation_report_sha256:
        raise PipelineOrphanEvidencePreflightError("RECONCILIATION_REPORT_SHA256_MISMATCH")
    reconciliation = _validate_reconciliation(
        reconciliation_payload,
        report_sha256=reconciliation_report_sha256,
        plan=plan,
    )
    if reconciliation["generated_at"] > collected_at:
        raise PipelineOrphanEvidencePreflightError("SOURCE_REPORT_TIME_ORDER_INVALID")
    try:
        manifest_document = read_stable_json_document(
            private_target_manifest,
            expected_sha256=expected_private_target_manifest_sha256,
        )
        manifest = validate_private_target_manifest(
            manifest_document,
            expected_target_set_sha256=plan["target_set_sha256"],
            expected_count=plan["target_count"],
        )
        target = _select_target(manifest, alias=alias)
        evidence_document = read_stable_json_document(
            evidence_file,
            expected_sha256=expected_evidence_file_sha256,
        )
        artifact_fingerprint = read_stable_file_fingerprint(
            authoritative_artifact,
            expected_sha256=expected_authoritative_artifact_sha256,
        )
        evidence_binding = validate_orphan_manual_evidence_document(
            evidence_document,
            expected_target_set_sha256=manifest["target_set_sha256"],
            expected_alias=alias,
            expected_target=target,
            expected_artifact_sha256=artifact_fingerprint.sha256,
            expected_artifact_size=artifact_fingerprint.size,
            expected_account_scope_sha256=expected_account_scope_sha256,
            not_after=collected_at,
        )
    except OrphanManualEvidenceError as exc:
        raise PipelineOrphanEvidencePreflightError(exc.code) from exc

    resolved_path = fast0_tool._validated_database_path(db_path)
    fast0_tool._assert_no_sidecars(resolved_path)
    writer_probe_before = fast0_tool._probe_no_other_open_handles(resolved_path)
    identity_pin = fast0_tool._PinnedDatabaseIdentity(resolved_path)
    with identity_pin:
        files_before = fast0_tool._file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
        plan_main_matches = fast0_tool._fingerprints_exact(
            plan["database_main"], files_before["main"]
        )
        reconciliation_main_matches = fast0_tool._fingerprints_exact(
            reconciliation["database_main"], files_before["main"]
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
            orphan_ledger_audit = audit_pipeline_orphan_disposition_rows(connection)
            preview = preview_pipeline_coherency_disposition(
                connection,
                trade_date=target["trade_date"],
                candidate_instance_id=target["candidate_instance_id"],
                action=ACTION_DISPOSE_ORPHAN,
                as_of=collected_at,
            )
            preview_binding = _validate_current_preview(
                preview,
                target=target,
                evidence_binding=evidence_binding,
                evidence_sha256=evidence_document.sha256,
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

    try:
        manifest_after = read_stable_json_document(
            private_target_manifest,
            expected_sha256=expected_private_target_manifest_sha256,
        )
        evidence_after = read_stable_json_document(
            evidence_file,
            expected_sha256=expected_evidence_file_sha256,
        )
        artifact_after = read_stable_file_fingerprint(
            authoritative_artifact,
            expected_sha256=expected_authoritative_artifact_sha256,
        )
    except OrphanManualEvidenceError as exc:
        raise PipelineOrphanEvidencePreflightError(exc.code) from exc
    private_files_unchanged = bool(
        _stable_file_identity(manifest_after) == _stable_file_identity(manifest_document)
        and _stable_file_identity(evidence_after) == _stable_file_identity(evidence_document)
        and _stable_file_identity(artifact_after) == _stable_file_identity(artifact_fingerprint)
    )
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
            "contract": plan["contract"],
            "report_sha256": plan_report_sha256,
            "evidence_status": plan["evidence_status"],
            "plan_status": plan["plan_status"],
            "target_count": plan["target_count"],
            "target_set_sha256": plan["target_set_sha256"],
            "path_recorded": False,
        },
        "source_reconciliation": {
            "contract": reconciliation["contract"],
            "report_sha256": reconciliation_report_sha256,
            "evidence_status": reconciliation["evidence_status"],
            "reconciliation_status": reconciliation["reconciliation_status"],
            "orphan_target_sha256": reconciliation["orphan_target_sha256"],
            "target_count": reconciliation["target_count"],
            "path_recorded": False,
        },
        "private_target_manifest": {
            "contract": PRIVATE_TARGET_SET_CONTRACT,
            "file_sha256": manifest_document.sha256,
            "file_size": manifest_document.size,
            "target_set_sha256": manifest["target_set_sha256"],
            "target_count": manifest["count"],
            "selected_alias": alias,
            "alias_contract": "M{ordinal:03d}",
            "raw_identifier_mapping_recorded": False,
            "content_embedded": False,
            "path_recorded": False,
        },
        "evidence": {
            "contract": ORPHAN_EVIDENCE_CONTRACT,
            "file_sha256": evidence_document.sha256,
            "file_size": evidence_document.size,
            "target_set_sha256": evidence_binding["target_set_sha256"],
            "alias": evidence_binding["alias"],
            "target_sha256": evidence_binding["target_sha256"],
            "determination_sha256": evidence_binding["determination_sha256"],
            "provenance_sha256": evidence_binding["provenance_sha256"],
            "provenance_source_type": evidence_binding["provenance_source_type"],
            "artifact_sha256": evidence_binding["artifact_sha256"],
            "artifact_size": evidence_binding["artifact_size"],
            "coverage_trade_date": evidence_binding["coverage_trade_date"],
            "coverage_status": evidence_binding["coverage_status"],
            "broker_scope_sha256": evidence_binding["account_scope_sha256"],
            "observed_at": evidence_binding["observed_at"],
            "reviewed_at": evidence_binding["reviewed_at"],
            "evidence_preview_sha256": evidence_binding["evidence_preview_sha256"],
            "content_embedded": False,
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
            "plan_main_matches": plan_main_matches,
            "reconciliation_main_matches": reconciliation_main_matches,
            "sidecars_absent_before": True,
            "sidecars_absent_after": True,
            "writer_probe_before": writer_probe_before,
            "writer_probe_after": writer_probe_after,
            "whole_window_writer_absence_proven": whole_window_writer_absence_proven,
            "runtime_lock_count": runtime_lock_count,
        },
        "schema_manifest": schema_manifest,
        "orphan_ledger_audit": orphan_ledger_audit,
        "preview_binding": preview_binding,
        "private_files_unchanged": private_files_unchanged,
        "external_broker_artifact_bound": True,
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "database_write_performed": False,
        "apply_authorized": False,
        "eligibility_changed": False,
        "tool_actions": dict(_TOOL_ACTIONS),
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


def _validate_plan(payload: Mapping[str, Any], *, report_sha256: str) -> dict[str, Any]:
    verdict = _mapping(payload.get("verdict"))
    recomputed_verdict = plan_tool.evaluate_report(payload)
    source = _mapping(payload.get("source_fast0_report"))
    database = _mapping(payload.get("database"))
    connection = _mapping(database.get("connection"))
    files_after = _mapping(database.get("files_after"))
    files_before = _mapping(database.get("files_before"))
    database_main = _mapping(files_after.get("main"))
    identity = _mapping(database.get("identity"))
    identity_pin = _mapping(database.get("identity_pin"))
    pipeline = _mapping(payload.get("pipeline"))
    action_counts = _mapping(pipeline.get("expected_action_counts"))
    target_count = pipeline.get("manual_evidence_disposition_count")
    normalized_target_count = target_count if type(target_count) is int else -1
    target_set_sha256 = pipeline.get("manual_evidence_campaign_sha256")
    try:
        generated_at = require_utc_z_timestamp("PLAN_GENERATED_AT", payload.get("generated_at"))
    except OrphanManualEvidenceError as exc:
        raise PipelineOrphanEvidencePreflightError("PLAN_REPORT_TIME_INVALID") from exc
    top_level_contract = {
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "database_write_performed": False,
        "apply_authorized": False,
        "side_effect_scope": "THIS_TOOL_EXECUTION_ONLY",
        "external_process_state_verified": False,
        "external_broker_state_verified": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    pipeline_contract = {
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "apply_authorized": False,
    }
    valid = bool(
        payload.get("contract") == _EXPECTED_PLAN_CONTRACT
        and verdict == recomputed_verdict
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("plan_status") == "COMPLETE"
        and _mapping_has_values(payload, top_level_contract)
        and _mapping_has_values(pipeline, pipeline_contract)
        and source.get("database_main_matches") is True
        and source.get("path_recorded") is False
        and identity.get("app_name") == APP_NAME
        and _fixed_int(identity.get("app_name_row_count"), 1)
        and identity.get("app_name_value_valid") is True
        and identity.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and _fixed_int(identity.get("schema_version_row_count"), 1)
        and identity.get("schema_version_value_valid") is True
        and identity_pin.get("status") == "PASS"
        and identity_pin.get("held_across_snapshot") is True
        and identity_pin.get("path_identity_stable") is True
        and connection.get("mode") == "ro"
        and connection.get("immutable") is True
        and connection.get("query_only") is True
        and connection.get("single_deferred_snapshot") is True
        and _database_list_is_main_only(database.get("database_list"))
        and database.get("quick_check") == ["ok"]
        and database.get("sidecars_absent_before") is True
        and database.get("sidecars_absent_after") is True
        and database.get("whole_window_writer_absence_proven") is True
        and plan_tool._is_nonnegative_int(database.get("runtime_lock_count"))
        and database.get("runtime_lock_count") == 0
        and _writer_probes_pass(database)
        and plan_tool._fingerprint_valid(database_main)
        and files_before == files_after
        and _mapping(files_before.get("main")) == database_main
        and normalized_target_count > 0
        and plan_tool._is_nonnegative_int(action_counts.get(ACTION_DISPOSE_ORPHAN))
        and action_counts.get(ACTION_DISPOSE_ORPHAN) == target_count
        and plan_tool._is_nonnegative_int(pipeline.get("manual_evidence_required_count"))
        and pipeline.get("manual_evidence_required_count") == target_count
        and plan_tool._is_nonnegative_int(pipeline.get("manual_evidence_preview_blocked_count"))
        and pipeline.get("manual_evidence_preview_blocked_count") == 0
        and plan_tool._is_nonnegative_int(pipeline.get("invalid_item_count"))
        and pipeline.get("invalid_item_count") == 0
        and plan_tool._is_sha256(target_set_sha256)
        and _tool_actions_safe(payload.get("tool_actions"))
    )
    if not valid:
        raise PipelineOrphanEvidencePreflightError("PLAN_REPORT_CONTRACT_INVALID")
    return {
        "contract": payload.get("contract"),
        "report_sha256": report_sha256,
        "evidence_status": verdict.get("evidence_status"),
        "plan_status": verdict.get("plan_status"),
        "database_main": dict(database_main),
        "generated_at": generated_at,
        "target_count": normalized_target_count,
        "target_set_sha256": str(target_set_sha256),
    }


def _validate_reconciliation(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    verdict = _mapping(payload.get("verdict"))
    recomputed_verdict = reconciliation_tool.evaluate_report(payload)
    source = _mapping(payload.get("source_blocker_plan"))
    database = _mapping(payload.get("database"))
    connection = _mapping(database.get("connection"))
    files_after = _mapping(database.get("files_after"))
    files_before = _mapping(database.get("files_before"))
    database_main = _mapping(files_after.get("main"))
    identity = _mapping(database.get("identity"))
    identity_pin = _mapping(database.get("identity_pin"))
    reconciliation = _mapping(payload.get("pipeline_reconciliation"))
    action_counts = _mapping(reconciliation.get("expected_action_counts"))
    eligible_counts = _mapping(reconciliation.get("preview_eligible_action_counts"))
    source_counts = _mapping(reconciliation.get("source_state_counts"))
    outcome_counts = _mapping(reconciliation.get("outcome_counts"))
    target_count = plan.get("target_count")
    normalized_target_count = target_count if type(target_count) is int else -1
    plan_generated_at = plan.get("generated_at")
    try:
        generated_at = require_utc_z_timestamp(
            "RECONCILIATION_GENERATED_AT", payload.get("generated_at")
        )
    except OrphanManualEvidenceError as exc:
        raise PipelineOrphanEvidencePreflightError("RECONCILIATION_REPORT_TIME_INVALID") from exc
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
    valid = bool(
        payload.get("contract") == _EXPECTED_RECONCILIATION_CONTRACT
        and verdict == recomputed_verdict
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("reconciliation_status") == "COMPLETE"
        and source.get("report_sha256") == plan.get("report_sha256")
        and isinstance(plan_generated_at, datetime)
        and generated_at >= plan_generated_at
        and source.get("database_main_matches") is True
        and source.get("path_recorded") is False
        and _mapping_has_values(payload, top_level_contract)
        and _mapping_has_values(reconciliation, reconciliation_contract)
        and identity.get("app_name") == APP_NAME
        and _fixed_int(identity.get("app_name_row_count"), 1)
        and identity.get("app_name_value_valid") is True
        and identity.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and _fixed_int(identity.get("schema_version_row_count"), 1)
        and identity.get("schema_version_value_valid") is True
        and identity_pin.get("status") == "PASS"
        and identity_pin.get("held_across_snapshot") is True
        and identity_pin.get("path_identity_stable") is True
        and connection.get("mode") == "ro"
        and connection.get("immutable") is True
        and connection.get("query_only") is True
        and connection.get("single_deferred_snapshot") is True
        and _database_list_is_main_only(database.get("database_list"))
        and database.get("quick_check") == ["ok"]
        and database.get("sidecars_absent_before") is True
        and database.get("sidecars_absent_after") is True
        and database.get("whole_window_writer_absence_proven") is True
        and plan_tool._is_nonnegative_int(database.get("runtime_lock_count"))
        and database.get("runtime_lock_count") == 0
        and _writer_probes_pass(database)
        and plan_tool._fingerprint_valid(database_main)
        and database_main == plan.get("database_main")
        and files_before == files_after
        and _mapping(files_before.get("main")) == database_main
        and plan_tool._is_nonnegative_int(action_counts.get(ACTION_DISPOSE_ORPHAN))
        and action_counts.get(ACTION_DISPOSE_ORPHAN) == target_count
        and plan_tool._is_nonnegative_int(eligible_counts.get(ACTION_DISPOSE_ORPHAN))
        and eligible_counts.get(ACTION_DISPOSE_ORPHAN) == target_count
        and plan_tool._is_nonnegative_int(reconciliation.get("manual_evidence_required_count"))
        and reconciliation.get("manual_evidence_required_count") == target_count
        and plan_tool._is_nonnegative_int(source_counts.get("CANDIDATE_ABSENT"))
        and source_counts.get("CANDIDATE_ABSENT") == target_count
        and plan_tool._is_nonnegative_int(
            outcome_counts.get("MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED")
        )
        and outcome_counts.get("MANUAL_AUTHORITATIVE_EVIDENCE_REQUIRED") == target_count
        and plan_tool._is_nonnegative_int(reconciliation.get("invalid_item_count"))
        and reconciliation.get("invalid_item_count") == 0
        and plan_tool._is_sha256(reconciliation.get("orphan_target_sha256"))
        and _tool_actions_safe(payload.get("tool_actions"))
    )
    if not valid:
        raise PipelineOrphanEvidencePreflightError("RECONCILIATION_REPORT_CONTRACT_INVALID")
    return {
        "contract": payload.get("contract"),
        "report_sha256": report_sha256,
        "evidence_status": verdict.get("evidence_status"),
        "reconciliation_status": verdict.get("reconciliation_status"),
        "database_main": dict(database_main),
        "generated_at": generated_at,
        "target_count": normalized_target_count,
        "orphan_target_sha256": reconciliation.get("orphan_target_sha256"),
    }


def validate_apply_preflight_report(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
    expected_private_manifest_sha256: str,
    expected_evidence_sha256: str,
    expected_artifact_sha256: str,
    expected_artifact_size: int,
    expected_target_set_sha256: str,
    expected_alias: str,
    expected_evidence_preview_sha256: str,
) -> dict[str, Any]:
    for name, value in (
        ("REPORT_SHA256", report_sha256),
        ("PRIVATE_MANIFEST_SHA256", expected_private_manifest_sha256),
        ("EVIDENCE_SHA256", expected_evidence_sha256),
        ("ARTIFACT_SHA256", expected_artifact_sha256),
        ("TARGET_SET_SHA256", expected_target_set_sha256),
        ("EVIDENCE_PREVIEW_SHA256", expected_evidence_preview_sha256),
    ):
        if not plan_tool._is_sha256(value):
            raise PipelineOrphanEvidencePreflightError(f"APPLY_{name}_INVALID")
    if type(expected_artifact_size) is not int or expected_artifact_size <= 0:
        raise PipelineOrphanEvidencePreflightError("APPLY_ARTIFACT_SIZE_INVALID")
    verdict = evaluate_report(payload)
    stored_verdict = _mapping(payload.get("verdict"))
    manifest = _mapping(payload.get("private_target_manifest"))
    evidence = _mapping(payload.get("evidence"))
    database = _mapping(payload.get("database"))
    files_after = _mapping(database.get("files_after"))
    database_main = _mapping(files_after.get("main"))
    source_plan = _mapping(payload.get("source_blocker_plan"))
    source_reconciliation = _mapping(payload.get("source_reconciliation"))
    valid = bool(
        payload.get("contract") == _CONTRACT
        and stored_verdict == verdict
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("preflight_status") == "COMPLETE"
        and verdict.get("apply_authorized") is False
        and payload.get("apply_authorized") is False
        and payload.get("database_write_performed") is False
        and payload.get("private_files_unchanged") is True
        and payload.get("external_broker_artifact_bound") is True
        and manifest.get("file_sha256") == expected_private_manifest_sha256
        and manifest.get("target_set_sha256") == expected_target_set_sha256
        and manifest.get("selected_alias") == expected_alias
        and evidence.get("file_sha256") == expected_evidence_sha256
        and evidence.get("artifact_sha256") == expected_artifact_sha256
        and evidence.get("artifact_size") == expected_artifact_size
        and plan_tool._is_sha256(evidence.get("broker_scope_sha256"))
        and evidence.get("target_set_sha256") == expected_target_set_sha256
        and evidence.get("alias") == expected_alias
        and evidence.get("evidence_preview_sha256") == expected_evidence_preview_sha256
        and _positive_int(manifest.get("target_count"))
        and plan_tool._fingerprint_valid(database_main)
        and plan_tool._is_sha256(source_plan.get("report_sha256"))
        and plan_tool._is_sha256(source_reconciliation.get("report_sha256"))
    )
    if not valid:
        raise PipelineOrphanEvidencePreflightError("APPROVED_ORPHAN_PREFLIGHT_CONTRACT_INVALID")
    try:
        generated_at = require_utc_z_timestamp(
            "PREFLIGHT_GENERATED_AT",
            payload.get("generated_at"),
        )
    except OrphanManualEvidenceError as exc:
        raise PipelineOrphanEvidencePreflightError(
            "APPROVED_ORPHAN_PREFLIGHT_TIME_INVALID"
        ) from exc
    return {
        "contract": _CONTRACT,
        "report_sha256": report_sha256,
        "generated_at": generated_at.astimezone(UTC),
        "generated_at_wire": str(payload["generated_at"]),
        "target_count": int(manifest["target_count"]),
        "database_main": dict(database_main),
        "broker_scope_sha256": evidence["broker_scope_sha256"],
        "private_manifest_sha256": manifest["file_sha256"],
        "source_plan_report_sha256": source_plan["report_sha256"],
        "source_reconciliation_report_sha256": source_reconciliation["report_sha256"],
    }


def _select_target(manifest: Mapping[str, Any], *, alias: str) -> dict[str, Any]:
    matches = [
        dict(item)
        for item in manifest.get("items") or []
        if isinstance(item, Mapping) and item.get("alias") == alias
    ]
    if len(matches) != 1:
        raise PipelineOrphanEvidencePreflightError("PRIVATE_TARGET_ALIAS_NOT_FOUND")
    target = matches[0]
    target.pop("alias", None)
    return target


def _validate_current_preview(
    preview: Mapping[str, Any],
    *,
    target: Mapping[str, Any],
    evidence_binding: Mapping[str, Any],
    evidence_sha256: str,
) -> dict[str, Any]:
    reasons, reasons_valid = reconciliation_tool._preview_reason_codes(preview)
    source_state = reconciliation_tool._source_state(
        _mapping(preview.get("source")),
        candidate=_mapping(preview.get("candidate")),
    )
    downstream_state = reconciliation_tool._downstream_state(_mapping(preview.get("downstream")))
    cas_exact = plan_tool._pipeline_preview_safe(preview, expected=target)
    semantic_safe = reconciliation_tool._preview_semantics_safe(
        preview,
        action=ACTION_DISPOSE_ORPHAN,
        reasons=reasons,
        reasons_valid=reasons_valid,
        source_state=source_state,
        downstream_state=downstream_state,
    )
    if not (
        preview.get("eligible") is True
        and preview.get("status") == "ELIGIBLE"
        and preview.get("classification") == ORPHAN_CLASSIFICATION
        and preview.get("expected_action") == ACTION_DISPOSE_ORPHAN
        and not reasons
        and cas_exact
        and semantic_safe
    ):
        raise PipelineOrphanEvidencePreflightError("CURRENT_ORPHAN_PREVIEW_INVALID")
    try:
        binding = validate_orphan_evidence_binding(
            evidence_binding,
            evidence_sha256=evidence_sha256,
            candidate_instance_id=str(preview.get("candidate_instance_id") or ""),
            trade_date=str(preview.get("trade_date") or ""),
            action=str(preview.get("action") or ""),
            pipeline_fingerprint=str(preview.get("pipeline_fingerprint") or ""),
            subject_version=str(preview.get("subject_version") or ""),
            source_fingerprint=str(preview.get("source_fingerprint") or ""),
            candidate_fingerprint=str(preview.get("candidate_fingerprint") or ""),
            downstream_fingerprint=str(preview.get("downstream_fingerprint") or ""),
            boundary_fingerprint=str(preview.get("boundary_fingerprint") or ""),
            expected_preview_sha256=str(evidence_binding.get("evidence_preview_sha256") or ""),
        )
    except OrphanManualEvidenceError as exc:
        raise PipelineOrphanEvidencePreflightError(exc.code) from exc
    return {
        "alias": binding["alias"],
        "status": "ELIGIBLE",
        "eligible": True,
        "classification": ORPHAN_CLASSIFICATION,
        "action": ACTION_DISPOSE_ORPHAN,
        "source_state": source_state,
        "downstream_state": downstream_state,
        "reason_count": 0,
        "cas_exact": cas_exact,
        "semantic_safe": semantic_safe,
        "target_sha256": binding["target_sha256"],
        "evidence_preview_sha256": binding["evidence_preview_sha256"],
        "identifiers_recorded": False,
    }


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    source_plan = _mapping(report.get("source_blocker_plan"))
    source_reconciliation = _mapping(report.get("source_reconciliation"))
    manifest = _mapping(report.get("private_target_manifest"))
    evidence = _mapping(report.get("evidence"))
    database = _mapping(report.get("database"))
    identity = _mapping(database.get("identity"))
    identity_pin = _mapping(database.get("identity_pin"))
    connection = _mapping(database.get("connection"))
    schema = _mapping(report.get("schema_manifest"))
    orphan_ledger = _mapping(report.get("orphan_ledger_audit"))
    preview = _mapping(report.get("preview_binding"))
    before = _mapping(database.get("files_before"))
    after = _mapping(database.get("files_after"))
    actions = _mapping(report.get("tool_actions"))

    if not (
        report.get("contract") == _CONTRACT
        and source_plan.get("contract") == _EXPECTED_PLAN_CONTRACT
        and source_plan.get("evidence_status") == "PASS"
        and source_plan.get("plan_status") == "COMPLETE"
        and plan_tool._is_sha256(source_plan.get("report_sha256"))
        and _positive_int(source_plan.get("target_count"))
        and plan_tool._is_sha256(source_plan.get("target_set_sha256"))
        and source_plan.get("path_recorded") is False
        and source_reconciliation.get("contract") == _EXPECTED_RECONCILIATION_CONTRACT
        and source_reconciliation.get("evidence_status") == "PASS"
        and source_reconciliation.get("reconciliation_status") == "COMPLETE"
        and plan_tool._is_sha256(source_reconciliation.get("report_sha256"))
        and plan_tool._is_sha256(source_reconciliation.get("orphan_target_sha256"))
        and _positive_int(source_reconciliation.get("target_count"))
        and source_reconciliation.get("path_recorded") is False
    ):
        failures.append("SOURCE_EVIDENCE_CONTRACT_INVALID")
    if not (
        manifest.get("contract") == PRIVATE_TARGET_SET_CONTRACT
        and plan_tool._is_sha256(manifest.get("file_sha256"))
        and _positive_int(manifest.get("file_size"))
        and _positive_int(manifest.get("target_count"))
        and isinstance(manifest.get("selected_alias"), str)
        and _ALIAS_RE.fullmatch(str(manifest.get("selected_alias"))) is not None
        and manifest.get("alias_contract") == "M{ordinal:03d}"
        and manifest.get("target_count") == source_plan.get("target_count")
        and manifest.get("target_count") == source_reconciliation.get("target_count")
        and manifest.get("target_set_sha256") == source_plan.get("target_set_sha256")
        and plan_tool._is_sha256(manifest.get("target_set_sha256"))
        and manifest.get("selected_alias") == evidence.get("alias")
        and manifest.get("content_embedded") is False
        and manifest.get("raw_identifier_mapping_recorded") is False
        and manifest.get("path_recorded") is False
    ):
        failures.append("PRIVATE_TARGET_MANIFEST_CONTRACT_INVALID")
    if not (
        evidence.get("contract") == ORPHAN_EVIDENCE_CONTRACT
        and plan_tool._is_sha256(evidence.get("file_sha256"))
        and _positive_int(evidence.get("file_size"))
        and plan_tool._is_sha256(evidence.get("target_sha256"))
        and plan_tool._is_sha256(evidence.get("determination_sha256"))
        and plan_tool._is_sha256(evidence.get("provenance_sha256"))
        and plan_tool._is_sha256(evidence.get("artifact_sha256"))
        and _positive_int(evidence.get("artifact_size"))
        and plan_tool._is_sha256(evidence.get("broker_scope_sha256"))
        and plan_tool._is_sha256(evidence.get("evidence_preview_sha256"))
        and evidence.get("provenance_source_type") in ORPHAN_EVIDENCE_SOURCE_TYPES
        and _evidence_time_and_coverage_valid(
            evidence,
            not_after=report.get("generated_at"),
        )
        and evidence.get("target_set_sha256") == manifest.get("target_set_sha256")
        and evidence.get("target_sha256") == preview.get("target_sha256")
        and evidence.get("evidence_preview_sha256") == preview.get("evidence_preview_sha256")
        and evidence.get("content_embedded") is False
        and evidence.get("path_recorded") is False
    ):
        failures.append("ORPHAN_EVIDENCE_BINDING_INVALID")
    if not (
        identity.get("app_name") == APP_NAME
        and _fixed_int(identity.get("app_name_row_count"), 1)
        and identity.get("app_name_value_valid") is True
        and identity.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and _fixed_int(identity.get("schema_version_row_count"), 1)
        and identity.get("schema_version_value_valid") is True
    ):
        failures.append("DATABASE_APP_IDENTITY_INVALID")
    if not (
        identity_pin.get("status") == "PASS"
        and identity_pin.get("held_across_snapshot") is True
        and identity_pin.get("path_identity_stable") is True
        and identity_pin.get("raw_identity_recorded") is False
    ):
        failures.append("DATABASE_IDENTITY_PIN_FAILED")
    if not (
        connection.get("mode") == "ro"
        and connection.get("immutable") is True
        and connection.get("query_only") is True
        and connection.get("single_deferred_snapshot") is True
    ):
        failures.append("STRICT_READ_ONLY_CONNECTION_INVALID")
    if not (
        _database_list_is_main_only(database.get("database_list"))
        and database.get("quick_check") == ["ok"]
        and database.get("sidecars_absent_before") is True
        and database.get("sidecars_absent_after") is True
        and plan_tool._is_nonnegative_int(database.get("runtime_lock_count"))
        and database.get("runtime_lock_count") == 0
        and database.get("plan_main_matches") is True
        and database.get("reconciliation_main_matches") is True
        and _writer_probes_pass(database)
        and plan_tool._fingerprint_valid(_mapping(before.get("main")))
        and plan_tool._fingerprint_valid(_mapping(after.get("main")))
        and before == after
    ):
        failures.append("DATABASE_SNAPSHOT_CONTRACT_INVALID")
    if database.get("whole_window_writer_absence_proven") is not True:
        failures.append("WHOLE_WINDOW_WRITER_ABSENCE_NOT_PROVEN")
    if not (
        schema.get("ready") is True
        and plan_tool._is_nonnegative_int(schema.get("expected_object_count"))
        and plan_tool._is_nonnegative_int(schema.get("present_object_count"))
        and plan_tool._is_nonnegative_int(schema.get("valid_object_count"))
        and plan_tool._is_nonnegative_int(schema.get("invalid_object_count"))
        and schema.get("expected_object_count") == schema.get("present_object_count")
        and schema.get("present_object_count") == schema.get("valid_object_count")
        and schema.get("invalid_object_count") == 0
        and schema.get("persistent_trigger_set_valid") is True
        and schema.get("critical_trigger_sets_valid") is True
    ):
        failures.append("SCHEMA_MANIFEST_INVALID")
    if not (
        _orphan_ledger_counts_valid(orphan_ledger)
        and orphan_ledger.get("invalid_row_count") == 0
        and orphan_ledger.get("legacy_generic_row_count") == 0
        and orphan_ledger.get("invalid_json_row_count") == 0
        and orphan_ledger.get("noncanonical_json_row_count") == 0
        and orphan_ledger.get("invalid_chain_subject_count") == 0
        and orphan_ledger.get("identifiers_recorded") is False
        and orphan_ledger.get("raw_rows_recorded") is False
        and orphan_ledger.get("read_only") is True
        and orphan_ledger.get("ready") is True
    ):
        failures.append("ORPHAN_LEDGER_GLOBAL_AUDIT_INVALID")
    if not (
        preview.get("status") == "ELIGIBLE"
        and preview.get("eligible") is True
        and preview.get("alias") == evidence.get("alias")
        and preview.get("classification") == ORPHAN_CLASSIFICATION
        and preview.get("action") == ACTION_DISPOSE_ORPHAN
        and preview.get("source_state") == "CANDIDATE_ABSENT"
        and preview.get("downstream_state") == "SAFE_DB_PROVEN"
        and _fixed_int(preview.get("reason_count"), 0)
        and preview.get("cas_exact") is True
        and preview.get("semantic_safe") is True
        and preview.get("identifiers_recorded") is False
    ):
        failures.append("CURRENT_ORPHAN_PREVIEW_INVALID")
    if not (
        report.get("private_files_unchanged") is True
        and report.get("external_broker_artifact_bound") is True
        and report.get("read_only") is True
        and report.get("observe_only") is True
        and report.get("identifiers_recorded") is False
        and report.get("raw_rows_recorded") is False
        and report.get("raw_payloads_recorded") is False
        and report.get("database_write_performed") is False
        and report.get("apply_authorized") is False
        and report.get("eligibility_changed") is False
        and report.get("side_effect_scope") == "THIS_TOOL_EXECUTION_ONLY"
        and report.get("external_process_state_verified") is False
        and report.get("external_broker_state_verified") is False
        and report.get("live_sim_allowed") is False
        and report.get("live_real_allowed") is False
        and _tool_actions_safe(actions)
    ):
        failures.append("READ_ONLY_PREFLIGHT_CONTRACT_INVALID")

    evidence_status = "PASS" if not failures else "FAIL"
    return {
        "status": evidence_status,
        "evidence_status": evidence_status,
        "preflight_status": "COMPLETE" if not failures else "BLOCKED",
        "execution_readiness": "PREPARATION_REQUIRED" if not failures else "BLOCKED",
        "evidence_failures": sorted(set(failures)),
        "preparation_requirements": (
            [
                "BYTE_IDENTICAL_BACKUP_AND_BACKUP_QUICK_CHECK_REQUIRED",
                "SEPARATE_APPEND_ONLY_APPLY_APPROVAL_REQUIRED",
                "ONE_ALIAS_APPLY_THEN_STRICT_VERIFY_REQUIRED",
            ]
            if not failures
            else []
        ),
        "apply_authorized": False,
        "database_files_unchanged": before == after,
        "read_only": True,
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
        "preflight: python -B -m tools.ops_pipeline_orphan_evidence_preflight "
        "--db <schema-62-db> --blocker-plan-report <approved-r6-raw.json> "
        "--blocker-plan-report-sha256 <approved-r6-sha256> "
        "--reconciliation-report <approved-r7-raw.json> "
        "--reconciliation-report-sha256 <approved-r7-sha256> "
        "--private-target-manifest <restricted-private-manifest.json> "
        "--private-target-manifest-sha256 <approved-private-file-sha256> "
        "--evidence-file <restricted-evidence.json> "
        "--evidence-file-sha256 <approved-evidence-sha256> "
        "--authoritative-artifact <restricted-broker-artifact> "
        "--authoritative-artifact-sha256 <approved-artifact-sha256> "
        "--account-scope-sha256 <approved-account-scope-sha256> "
        "--alias <Mnnn> --out-dir <evidence-dir>\n"
        "apply: NOT AUTHORIZED; requires a separate append-only operating approval\n",
        encoding="utf-8",
    )
    return {"raw_json": raw_path, "summary_md": summary_path, "commands_txt": commands_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    manifest = _mapping(report.get("private_target_manifest"))
    evidence = _mapping(report.get("evidence"))
    preview = _mapping(report.get("preview_binding"))
    return "\n".join(
        [
            "# FAST-0 Pipeline Orphan Evidence Preflight",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- evidence_status: `{verdict.get('evidence_status')}`",
            f"- preflight_status: `{verdict.get('preflight_status')}`",
            f"- execution_readiness: `{verdict.get('execution_readiness')}`",
            f"- target_count: `{manifest.get('target_count')}`",
            f"- selected_alias: `{manifest.get('selected_alias')}`",
            f"- preview: `{preview.get('status')}`",
            f"- evidence_preview_sha256: `{evidence.get('evidence_preview_sha256')}`",
            "- apply_authorized: `false`",
            "",
            (
                "No database write, process start, order, broker, LIVE_SIM, "
                "or LIVE_REAL action is performed."
            ),
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    preview = _mapping(report.get("preview_binding"))
    return (
        "pipeline orphan evidence preflight: "
        f"{verdict.get('evidence_status')} preflight={verdict.get('preflight_status')} "
        f"alias={preview.get('alias')} apply_authorized=false"
    )


def _mapping_has_values(value: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return all(
        value.get(key) == item and type(value.get(key)) is type(item)
        for key, item in expected.items()
    )


def _writer_probes_pass(database: Mapping[str, Any]) -> bool:
    return all(
        _mapping(database.get(key)).get("status") == "PASS"
        and _mapping(database.get(key)).get("no_other_open_handles") is True
        for key in ("writer_probe_before", "writer_probe_after")
    )


def _tool_actions_safe(value: Any) -> bool:
    actions = _mapping(value)
    return set(actions) == set(_TOOL_ACTIONS) and all(
        actions.get(key) is False for key in _TOOL_ACTIONS
    )


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _fixed_int(value: Any, expected: int) -> bool:
    return type(value) is int and value == expected


def _database_list_is_main_only(value: Any) -> bool:
    if type(value) is not list or len(value) != 1 or not isinstance(value[0], Mapping):
        return False
    item = dict(value[0])
    return bool(
        set(item) == {"seq", "name", "file_present"}
        and _fixed_int(item.get("seq"), 0)
        and item.get("name") == "main"
        and type(item.get("name")) is str
        and item.get("file_present") is True
    )


def _orphan_ledger_counts_valid(value: Mapping[str, Any]) -> bool:
    keys = (
        "row_count",
        "subject_count",
        "valid_binding_row_count",
        "invalid_row_count",
        "legacy_generic_row_count",
        "invalid_json_row_count",
        "noncanonical_json_row_count",
        "invalid_chain_subject_count",
    )
    if not all(plan_tool._is_nonnegative_int(value.get(key)) for key in keys):
        return False
    counts = {key: int(value[key]) for key in keys}
    return bool(
        counts["valid_binding_row_count"] + counts["invalid_row_count"] == counts["row_count"]
        and counts["subject_count"] <= counts["row_count"]
    )


def _evidence_time_and_coverage_valid(
    evidence: Mapping[str, Any],
    *,
    not_after: Any,
) -> bool:
    if evidence.get("coverage_status") != "FINAL_COMPLETE":
        return False
    coverage_raw = evidence.get("coverage_trade_date")
    if not isinstance(coverage_raw, str):
        return False
    try:
        coverage_date = date.fromisoformat(coverage_raw)
        if coverage_date.isoformat() != coverage_raw:
            return False
        observed_at = require_utc_z_timestamp("OBSERVED_AT", evidence.get("observed_at"))
        reviewed_at = require_utc_z_timestamp("REVIEWED_AT", evidence.get("reviewed_at"))
        generated_at = require_utc_z_timestamp("GENERATED_AT", not_after)
    except (OrphanManualEvidenceError, ValueError):
        return False
    return bool(
        reviewed_at >= observed_at
        and observed_at <= generated_at
        and reviewed_at <= generated_at
        and observed_at.astimezone(_KST).date() > coverage_date
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _stable_file_identity(value: Any) -> tuple[Any, ...]:
    return (
        getattr(value, "sha256", None),
        getattr(value, "size", None),
        getattr(value, "device", None),
        getattr(value, "inode", None),
        getattr(value, "mtime_ns", None),
    )


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
