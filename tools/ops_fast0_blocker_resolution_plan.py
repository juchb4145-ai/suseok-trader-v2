from __future__ import annotations

import argparse
import hashlib
import json
import re
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

from services.pipeline_coherency import (  # noqa: E402
    build_pipeline_coherency_rca_status,
)
from services.pipeline_coherency_disposition import (  # noqa: E402
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    ACTION_DISPOSE_STALE_OTHER_DATE,
    preview_pipeline_coherency_disposition,
    resolve_pipeline_coherency_dispositions,
)
from services.pipeline_legacy_evidence import (  # noqa: E402
    MANIFEST_CONTRACT as LEGACY_EVIDENCE_MANIFEST_CONTRACT,
)
from services.pipeline_legacy_evidence import (  # noqa: E402
    resolve_pipeline_legacy_evidence,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (  # noqa: E402
    ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
    BUCKET_ACTIVE_UNRESOLVED,
    BUCKET_HISTORICAL_DISPOSED,
    BUCKET_HISTORICAL_PENDING,
    BUCKET_INVALID_DISPOSITION,
    BUCKET_MANUAL_REVIEW,
    BUCKET_RECOVERY_PENDING,
    BUCKET_RECOVERY_VERIFIED,
    EFFECTIVE_BUCKETS,
    build_incremental_evaluation_dead_letter_effective_status,
    list_incremental_evaluation_dead_letter_effective_rows,
    preview_incremental_evaluation_dead_letter_disposition,
)
from storage.sqlite import SCHEMA_VERSION  # noqa: E402
from tools import ops_fast0_strict_requalification as fast0_tool  # noqa: E402

_CONTRACT = "fast0-blocker-resolution-plan.v1"
_EXPECTED_FAST0_CONTRACT = "fast0-strict-offline-requalification.v1"
_EXPECTED_SCHEMA_VERSION = "62"
_PIPELINE_MAX_AGE_SEC = 60.0
_PAGE_LIMIT = 500
_MAX_PAGES = 100
_SHA256_CHARS = frozenset("0123456789abcdef")
_WINDOWS_ABSOLUTE_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_FORBIDDEN_REPORT_KEYS = frozenset(
    {
        "account_id",
        "candidate_instance_id",
        "db_path",
        "dead_letter_id",
        "error_message",
        "evidence_path",
        "evidence_ref",
        "items",
        "last_error",
        "operator_id",
        "owner_id",
        "password",
        "payload",
        "raw_dead_letter",
        "secret",
        "token",
    }
)

_PIPELINE_CLASSIFICATIONS = (
    "HISTORICAL_CLOSED",
    "MISSING_CANDIDATE_MANUAL_REVIEW",
    "STALE_OTHER_DATE_MANUAL_REVIEW",
    "ACTIVE_CURRENT",
)
_PIPELINE_ACTIONS = (
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    ACTION_DISPOSE_STALE_OTHER_DATE,
)
_PIPELINE_CAS_KEYS = (
    "pipeline_fingerprint",
    "subject_version",
    "source_fingerprint",
    "candidate_fingerprint",
    "downstream_fingerprint",
    "boundary_fingerprint",
)
_INCREMENTAL_BUCKET_OUTPUT_KEYS = {
    BUCKET_ACTIVE_UNRESOLVED: "active_unresolved_dead_letter_count",
    BUCKET_HISTORICAL_PENDING: "historical_pending_disposition_count",
    BUCKET_HISTORICAL_DISPOSED: "historical_disposed_dead_letter_count",
    BUCKET_MANUAL_REVIEW: "manual_review_dead_letter_count",
    BUCKET_RECOVERY_PENDING: "recovery_pending_count",
    BUCKET_RECOVERY_VERIFIED: "recovery_verified_count",
    BUCKET_INVALID_DISPOSITION: "invalid_disposition_count",
}


class Fast0BlockerResolutionPlanError(RuntimeError):
    pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build an aggregate-only, strict read-only execution plan for the "
            "incremental dead-letter and pipeline FAST-0 blockers."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--fast0-report", required=True)
    parser.add_argument("--fast0-report-sha256", required=True)
    parser.add_argument("--legacy-evidence-manifest")
    parser.add_argument("--legacy-evidence-manifest-sha256")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "fast0_blocker_resolution_plan"),
    )
    args = parser.parse_args()
    try:
        report = run_report(
            db_path=Path(args.db),
            fast0_report=Path(args.fast0_report),
            expected_fast0_report_sha256=str(args.fast0_report_sha256).lower(),
            out_dir=Path(args.out_dir),
            legacy_evidence_manifest=(
                None
                if args.legacy_evidence_manifest is None
                else Path(args.legacy_evidence_manifest)
            ),
            expected_legacy_evidence_manifest_sha256=(
                None
                if args.legacy_evidence_manifest_sha256 is None
                else str(args.legacy_evidence_manifest_sha256).lower()
            ),
        )
    except Exception as exc:
        print(
            "FAST-0 blocker resolution plan: ERROR "
            f"error_type={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    print(render_console_summary(report))
    return 0 if _mapping(report.get("verdict")).get("evidence_status") == "PASS" else 2


def run_report(
    *,
    db_path: Path,
    fast0_report: Path,
    expected_fast0_report_sha256: str,
    out_dir: Path,
    observed_at: datetime | None = None,
    legacy_evidence_manifest: Path | None = None,
    expected_legacy_evidence_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    if str(SCHEMA_VERSION) != _EXPECTED_SCHEMA_VERSION:
        raise Fast0BlockerResolutionPlanError("CODE_TARGET_SCHEMA_MISMATCH")
    if not _is_sha256(expected_fast0_report_sha256):
        raise Fast0BlockerResolutionPlanError("FAST0_REPORT_EXPECTED_SHA256_INVALID")

    baseline_payload, actual_report_sha256 = fast0_tool._read_stable_json(
        fast0_report,
        missing_reason="FAST0_REPORT_NOT_FOUND",
        invalid_reason="FAST0_REPORT_INVALID",
    )
    if actual_report_sha256 != expected_fast0_report_sha256:
        raise Fast0BlockerResolutionPlanError("FAST0_REPORT_SHA256_MISMATCH")
    baseline = _validate_fast0_baseline(
        baseline_payload,
        report_sha256=actual_report_sha256,
    )
    if (legacy_evidence_manifest is None) is not (
        expected_legacy_evidence_manifest_sha256 is None
    ):
        raise Fast0BlockerResolutionPlanError("LEGACY_EVIDENCE_ARGUMENTS_INCOMPLETE")
    legacy_manifest_payload: Mapping[str, Any] | None = None
    legacy_manifest_sha256: str | None = None
    if legacy_evidence_manifest is not None:
        if not _is_sha256(expected_legacy_evidence_manifest_sha256):
            raise Fast0BlockerResolutionPlanError("LEGACY_EVIDENCE_SHA256_INVALID")
        legacy_manifest_payload, legacy_manifest_sha256 = fast0_tool._read_stable_json(
            legacy_evidence_manifest,
            missing_reason="LEGACY_EVIDENCE_MANIFEST_NOT_FOUND",
            invalid_reason="LEGACY_EVIDENCE_MANIFEST_INVALID",
        )
        if legacy_manifest_sha256 != expected_legacy_evidence_manifest_sha256:
            raise Fast0BlockerResolutionPlanError("LEGACY_EVIDENCE_SHA256_MISMATCH")

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
            quick_check_raw = [
                str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")
            ]
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
            incremental = _plan_incremental(connection)
            pipeline = _plan_pipeline(
                connection,
                observed_at=collected_at,
                legacy_evidence_manifest=legacy_manifest_payload,
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
    if legacy_evidence_manifest is not None:
        _, manifest_sha256_after = fast0_tool._read_stable_json(
            legacy_evidence_manifest,
            missing_reason="LEGACY_EVIDENCE_MANIFEST_NOT_FOUND",
            invalid_reason="LEGACY_EVIDENCE_MANIFEST_INVALID",
        )
        if manifest_sha256_after != legacy_manifest_sha256:
            raise Fast0BlockerResolutionPlanError("LEGACY_EVIDENCE_MANIFEST_CHANGED")
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
        "source_fast0_report": {
            "contract": baseline["contract"],
            "report_sha256": actual_report_sha256,
            "evidence_status": baseline["evidence_status"],
            "fast0_status": baseline["fast0_status"],
            "database_main_matches": baseline_main_matches,
            "incremental_expected": baseline["incremental_expected"],
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
            "runtime_lock_count": runtime_lock_count,
            "whole_window_writer_absence_proven": whole_window_writer_absence_proven,
        },
        "schema_manifest": schema_manifest,
        "incremental_dead_letter": incremental,
        "pipeline": pipeline,
        "legacy_evidence_input": {
            "configured": legacy_manifest_payload is not None,
            "contract": (
                LEGACY_EVIDENCE_MANIFEST_CONTRACT
                if legacy_manifest_payload is not None
                else None
            ),
            "file_sha256": legacy_manifest_sha256,
            "content_embedded": False,
            "path_recorded": False,
        },
        "execution_phases": _execution_phases(incremental, pipeline),
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
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
        "database_write_performed": False,
        "apply_authorized": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    return report


def _validate_fast0_baseline(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
) -> dict[str, Any]:
    verdict = _mapping(payload.get("verdict"))
    database = _mapping(payload.get("database"))
    database_identity = _mapping(database.get("identity"))
    database_pin = _mapping(database.get("identity_pin"))
    files_after = _mapping(database.get("files_after"))
    database_main = _mapping(files_after.get("main"))
    incremental = _mapping(payload.get("incremental_evaluation"))
    pipeline = _mapping(payload.get("pipeline_coherency"))
    independent_counts = _mapping(pipeline.get("independent_counts"))
    bucket_counts = _mapping(incremental.get("bucket_counts"))
    classifications = _mapping(pipeline.get("classification_counts"))
    required_bucket_keys = frozenset(
        {
            "active_unresolved_dead_letter_count",
            "historical_pending_disposition_count",
            "historical_disposed_dead_letter_count",
            "manual_review_dead_letter_count",
            "recovery_pending_count",
            "recovery_verified_count",
            "invalid_disposition_count",
        }
    )
    valid = bool(
        payload.get("contract") == _EXPECTED_FAST0_CONTRACT
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("fast0_status") == "BLOCKED"
        and payload.get("read_only") is True
        and payload.get("identifiers_recorded") is False
        and database_identity.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and database_identity.get("schema_version_value_valid") is True
        and database_pin.get("status") == "PASS"
        and _fingerprint_valid(database_main)
        and incremental.get("bucket_conservation_valid") is True
        and _is_nonnegative_int(incremental.get("raw_dead_letter_status_count"))
        and _is_nonnegative_int(incremental.get("effective_dead_letter_count"))
        and frozenset(bucket_counts) == required_bucket_keys
        and all(_is_nonnegative_int(value) for value in bucket_counts.values())
        and frozenset(classifications) == frozenset(_PIPELINE_CLASSIFICATIONS)
        and all(_is_nonnegative_int(value) for value in classifications.values())
        and _is_nonnegative_int(pipeline.get("full_count"))
        and _is_sha256(pipeline.get("inventory_digest"))
        and _is_nonnegative_int(pipeline.get("disposition_required_count"))
        and _is_nonnegative_int(independent_counts.get("active_current_non_pass_count"))
    )
    if not valid:
        raise Fast0BlockerResolutionPlanError("FAST0_REPORT_CONTRACT_INVALID")
    return {
        "contract": payload.get("contract"),
        "report_sha256": report_sha256,
        "evidence_status": verdict.get("evidence_status"),
        "fast0_status": verdict.get("fast0_status"),
        "database_main": dict(database_main),
        "incremental_expected": {
            "raw_dead_letter_count": incremental.get("raw_dead_letter_status_count"),
            "effective_dead_letter_count": incremental.get("effective_dead_letter_count"),
            "bucket_counts": dict(bucket_counts),
        },
        "pipeline_expected": {
            "full_count": pipeline.get("full_count"),
            "inventory_digest": pipeline.get("inventory_digest"),
            "classification_counts": dict(classifications),
            "disposition_required_count": pipeline.get("disposition_required_count"),
            "active_current_non_pass_count": independent_counts.get(
                "active_current_non_pass_count"
            ),
        },
    }


def _plan_incremental(connection: sqlite3.Connection) -> dict[str, Any]:
    status = build_incremental_evaluation_dead_letter_effective_status(connection)
    raw_count = int(status.get("raw_dead_letter_count") or 0)
    rows = list_incremental_evaluation_dead_letter_effective_rows(
        connection,
        limit=_PAGE_LIMIT,
    )
    bucket_counts = Counter(str(row.get("bucket") or "UNKNOWN") for row in rows)
    ordered = sorted(rows, key=lambda item: str(item.get("dead_letter_id") or ""))
    preview_eligible_count = 0
    blocked_reasons: Counter[str] = Counter()
    target_entries: list[dict[str, Any]] = []
    inventory_entries: list[dict[str, Any]] = []
    invalid_item_count = 0
    for row in ordered:
        dead_letter_id = row.get("dead_letter_id")
        fingerprint = row.get("dead_letter_fingerprint")
        candidate_version = row.get("candidate_version")
        bucket = str(row.get("bucket") or "UNKNOWN")
        if (
            not isinstance(dead_letter_id, str)
            or not dead_letter_id
            or not _is_sha256(fingerprint)
            or not _is_sha256(candidate_version)
            or bucket not in EFFECTIVE_BUCKETS
        ):
            invalid_item_count += 1
            continue
        inventory_entry = {
            "dead_letter_id": dead_letter_id,
            "dead_letter_fingerprint": fingerprint,
            "candidate_version": candidate_version,
            "bucket": bucket,
        }
        inventory_entries.append(inventory_entry)
        if bucket != BUCKET_HISTORICAL_PENDING:
            continue
        preview = preview_incremental_evaluation_dead_letter_disposition(
            connection,
            dead_letter_id,
            action=ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
            expected_dead_letter_fingerprint=str(fingerprint),
            expected_candidate_version=str(candidate_version),
        )
        reasons = [str(code) for code in preview.get("reason_codes") or []]
        safe = _incremental_preview_safe(preview, expected=inventory_entry)
        if preview.get("eligible") is True and not reasons and safe:
            preview_eligible_count += 1
            target_entries.append(
                {
                    "action": ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
                    **inventory_entry,
                }
            )
        else:
            if not safe:
                blocked_reasons["INCREMENTAL_PREVIEW_SAFETY_CONTRACT_INVALID"] += 1
            if reasons:
                for reason in reasons:
                    blocked_reasons[reason] += 1
            elif preview.get("eligible") is not True:
                blocked_reasons["INCREMENTAL_PREVIEW_NOT_ELIGIBLE"] += 1
    target_entries = _with_aliases(
        target_entries,
        prefix="U",
        width=2,
        ordering_key="dead_letter_id",
    )
    bucket_result = {
        output_key: int(bucket_counts[bucket])
        for bucket, output_key in sorted(_INCREMENTAL_BUCKET_OUTPUT_KEYS.items())
    }
    return {
        "raw_dead_letter_count": raw_count,
        "effective_dead_letter_count": int(status.get("effective_dead_letter_count") or 0),
        "bucket_counts": bucket_result,
        "inventory_count": len(rows),
        "inventory_complete": len(rows) == raw_count and raw_count <= _PAGE_LIMIT,
        "inventory_manifest_sha256": _manifest_sha256(inventory_entries),
        "disposition_target_count": int(bucket_counts[BUCKET_HISTORICAL_PENDING]),
        "preview_eligible_count": preview_eligible_count,
        "preview_blocked_count": (
            int(bucket_counts[BUCKET_HISTORICAL_PENDING]) - preview_eligible_count
        ),
        "preview_blocked_reason_counts": dict(sorted(blocked_reasons.items())),
        "campaign_manifest_sha256": _manifest_sha256(target_entries),
        "campaign_alias_contract": {
            "format": "U{ordinal:02d}",
            "ordering": "dead_letter_id ASC",
            "first_alias": "U01" if target_entries else None,
            "last_alias": f"U{len(target_entries):02d}" if target_entries else None,
            "raw_identifier_mapping_recorded": False,
        },
        "invalid_item_count": invalid_item_count,
        "schema_ready": status.get("schema_ready") is True,
        "raw_rows_recorded": False,
        "identifiers_recorded": False,
        "execution_mode": "ONE_ROW_PREVIEW_APPLY_VERIFY",
        "apply_authorized": False,
    }


def _plan_pipeline(
    connection: sqlite3.Connection,
    *,
    observed_at: datetime,
    legacy_evidence_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    first, items, pages = _read_pipeline_pages(
        connection,
        observed_at=observed_at,
        legacy_evidence_manifest=legacy_evidence_manifest,
    )
    classifications: Counter[str] = Counter()
    plan_statuses: Counter[str] = Counter()
    drift_statuses: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    blocked_reasons: Counter[str] = Counter()
    legacy_blocked_reasons: Counter[str] = Counter()
    auto_entries: list[dict[str, Any]] = []
    manual_entries: list[dict[str, Any]] = []
    unsupported_entries: list[dict[str, Any]] = []
    legacy_entries: list[dict[str, Any]] = []
    active_entries: list[dict[str, Any]] = []
    invalid_item_count = 0
    preview_eligible_count = 0
    disposition_required_count = 0
    manual_evidence_required_count = 0

    ordered = sorted(items, key=lambda item: str(item.get("candidate_instance_id") or ""))
    target_trade_date = first.get("trade_date")
    for item in ordered:
        candidate_id = item.get("candidate_instance_id")
        trade_date = item.get("trade_date")
        classification = str(item.get("classification") or "UNKNOWN")
        pipeline_fingerprint = item.get("pipeline_fingerprint")
        subject_version = item.get("subject_version")
        if (
            not isinstance(candidate_id, str)
            or not candidate_id
            or not isinstance(trade_date, str)
            or not trade_date
            or trade_date != target_trade_date
            or classification not in _PIPELINE_CLASSIFICATIONS
            or not _is_sha256(pipeline_fingerprint)
            or not _is_sha256(subject_version)
        ):
            invalid_item_count += 1
            continue
        classifications[classification] += 1
        latest_plan = _mapping(item.get("latest_plan"))
        plan_statuses[_plan_status_bucket(latest_plan)] += 1
        drift_statuses[_drift_bucket(item.get("current_source_drift"))] += 1
        base_entry = {
            "candidate_instance_id": candidate_id,
            "trade_date": trade_date,
            "classification": classification,
            "pipeline_fingerprint": pipeline_fingerprint,
            "subject_version": subject_version,
        }
        action = _expected_pipeline_action(item)
        if (
            classification == "HISTORICAL_CLOSED"
            and item.get("canonical_status") != "PASS"
            and action is None
        ):
            legacy_entries.append(
                {
                    **base_entry,
                    "legacy_warn_eligible": item.get("legacy_warn_candidate") is True,
                }
            )
            for reason in (
                item.get("legacy_warn_effective_reason_codes")
                or item.get("legacy_warn_reason_codes")
                or []
            ):
                legacy_blocked_reasons[str(reason)] += 1
        if item.get("active_recovery_required") is True:
            active_entries.append(base_entry)
        if item.get("disposition_required") is not True:
            continue
        disposition_required_count += 1
        if classification in {
            "MISSING_CANDIDATE_MANUAL_REVIEW",
            "STALE_OTHER_DATE_MANUAL_REVIEW",
        }:
            manual_evidence_required_count += 1
        if action is None:
            unsupported_entries.append(base_entry)
            blocked_reasons["PIPELINE_EXPECTED_ACTION_UNAVAILABLE"] += 1
            continue
        action_counts[action] += 1
        preview = preview_pipeline_coherency_disposition(
            connection,
            trade_date=trade_date,
            candidate_instance_id=candidate_id,
            action=action,
            as_of=observed_at,
        )
        preview_entry = {**base_entry, "action": action}
        for key in _PIPELINE_CAS_KEYS[2:]:
            preview_entry[key] = preview.get(key)
        reasons = [str(code) for code in preview.get("reason_codes") or []]
        safe = _pipeline_preview_safe(preview, expected=preview_entry)
        if preview.get("eligible") is True and not reasons and safe:
            preview_eligible_count += 1
            if classification == "HISTORICAL_CLOSED":
                auto_entries.append(preview_entry)
            else:
                manual_entries.append(preview_entry)
        else:
            if not safe:
                blocked_reasons["PIPELINE_PREVIEW_SAFETY_CONTRACT_INVALID"] += 1
            if reasons:
                for reason in reasons:
                    blocked_reasons[reason] += 1
            elif preview.get("eligible") is not True:
                blocked_reasons["PIPELINE_PREVIEW_NOT_ELIGIBLE"] += 1

    auto_entries = _with_aliases(
        auto_entries,
        prefix="P",
        width=3,
        ordering_key="candidate_instance_id",
    )
    manual_entries = _with_aliases(
        manual_entries,
        prefix="M",
        width=3,
        ordering_key="candidate_instance_id",
    )
    legacy_entries = _with_aliases(
        legacy_entries,
        prefix="L",
        width=3,
        ordering_key="candidate_instance_id",
    )
    active_entries = _with_aliases(
        active_entries,
        prefix="R",
        width=3,
        ordering_key="candidate_instance_id",
    )
    legacy_eligible_entries = [
        entry for entry in legacy_entries if entry.get("legacy_warn_eligible") is True
    ]
    legacy_blocked_entries = [
        entry for entry in legacy_entries if entry.get("legacy_warn_eligible") is not True
    ]
    full_count = int(first.get("full_count") or 0)
    collected_inventory_digest = _pipeline_inventory_digest(
        [str(item.get("candidate_instance_id")) for item in items]
    )
    return {
        "trade_date_present": bool(first.get("trade_date")),
        "canonical_status": first.get("canonical_status"),
        "qualification_status": first.get("qualification_status"),
        "qualification_reason_codes": list(first.get("qualification_reason_codes") or []),
        "full_count": full_count,
        "collected_count": len(items),
        "page_count": len(pages),
        "pages": pages,
        "inventory_digest": first.get("inventory_digest"),
        "collected_inventory_digest": collected_inventory_digest,
        "inventory_end_digest": first.get("inventory_end_digest"),
        "inventory_count_consistent": first.get("inventory_count_consistent") is True,
        "classification_counts": {
            key: int(classifications[key]) for key in _PIPELINE_CLASSIFICATIONS
        },
        "latest_plan_status_counts": {
            key: int(plan_statuses[key])
            for key in ("PLAN_READY", "NON_READY", "ABSENT", "UNKNOWN")
        },
        "current_source_drift_counts": {
            key: int(drift_statuses[key]) for key in ("DRIFT", "NO_DRIFT", "UNKNOWN")
        },
        "disposition_required_count": disposition_required_count,
        "expected_action_counts": {
            key: int(action_counts[key]) for key in _PIPELINE_ACTIONS
        },
        "preview_eligible_count": preview_eligible_count,
        "preview_blocked_count": disposition_required_count - preview_eligible_count,
        "preview_blocked_reason_counts": dict(sorted(blocked_reasons.items())),
        "automatic_historical_disposition_count": len(auto_entries),
        "automatic_historical_campaign_sha256": _manifest_sha256(auto_entries),
        "manual_evidence_disposition_count": len(manual_entries),
        "manual_evidence_campaign_sha256": _manifest_sha256(manual_entries),
        "manual_evidence_required_count": manual_evidence_required_count,
        "manual_evidence_preview_blocked_count": (
            manual_evidence_required_count - len(manual_entries)
        ),
        "legacy_evidence_target_count": len(legacy_entries),
        "legacy_evidence_eligible_count": len(legacy_eligible_entries),
        "legacy_evidence_eligible_sha256": _manifest_sha256(legacy_eligible_entries),
        "legacy_evidence_contract_blocked_count": len(legacy_blocked_entries),
        "legacy_evidence_contract_blocked_sha256": _manifest_sha256(
            legacy_blocked_entries
        ),
        "legacy_evidence_target_sha256": _manifest_sha256(legacy_entries),
        "legacy_evidence_contract_blocked_reason_counts": dict(
            sorted(legacy_blocked_reasons.items())
        ),
        "unsupported_action_count": len(unsupported_entries),
        "unsupported_action_target_sha256": _manifest_sha256(unsupported_entries),
        "active_current_target_count": len(active_entries),
        "targeted_rebuild_target_sha256": _manifest_sha256(active_entries),
        "targeted_rebuild_preview_status": (
            "NOT_REQUIRED" if not active_entries else "REQUIRES_AUDITED_OBSERVE_ENV_PREVIEW"
        ),
        "invalid_item_count": invalid_item_count,
        "schema_ready": first.get("schema_ready") is True,
        "raw_rows_recorded": False,
        "identifiers_recorded": False,
        "apply_authorized": False,
    }


def _read_pipeline_pages(
    connection: sqlite3.Connection,
    *,
    observed_at: datetime,
    legacy_evidence_manifest: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    first: dict[str, Any] | None = None
    target_trade_date: str | None = None
    items: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    offset = 0
    page_contract: dict[str, Any] | None = None
    for _ in range(_MAX_PAGES):
        page = build_pipeline_coherency_rca_status(
            connection,
            trade_date=target_trade_date,
            max_age_sec=_PIPELINE_MAX_AGE_SEC,
            limit=_PAGE_LIMIT,
            offset=offset,
            disposition_resolver=(
                lambda trade_date, subjects: resolve_pipeline_coherency_dispositions(
                    connection,
                    trade_date,
                    subjects,
                    as_of=observed_at,
                )
            ),
            legacy_evidence_resolver=(
                None
                if legacy_evidence_manifest is None
                else lambda trade_date, subjects: resolve_pipeline_legacy_evidence(
                    connection,
                    trade_date,
                    subjects,
                    manifest=legacy_evidence_manifest,
                )
            ),
            as_of=observed_at,
        )
        if first is None:
            first = dict(page)
            raw_trade_date = page.get("trade_date")
            target_trade_date = raw_trade_date if isinstance(raw_trade_date, str) else None
            page_contract = _pipeline_page_contract(page)
        elif _pipeline_page_contract(page) != page_contract:
            raise Fast0BlockerResolutionPlanError("PIPELINE_PAGE_CONTRACT_DRIFT")
        raw_items = page.get("items")
        if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
            raise Fast0BlockerResolutionPlanError("PIPELINE_ITEMS_INVALID")
        page_items = [dict(item) for item in raw_items if isinstance(item, Mapping)]
        if len(page_items) != len(raw_items):
            raise Fast0BlockerResolutionPlanError("PIPELINE_ITEM_TYPE_INVALID")
        if page.get("offset") != offset or page.get("returned_count") != len(page_items):
            raise Fast0BlockerResolutionPlanError("PIPELINE_PAGE_COUNT_INVALID")
        has_more = page.get("has_more")
        if type(has_more) is not bool:
            raise Fast0BlockerResolutionPlanError("PIPELINE_PAGINATION_CONTRACT_INVALID")
        expected_next = offset + len(page_items) if has_more else None
        if page.get("next_offset") != expected_next:
            raise Fast0BlockerResolutionPlanError("PIPELINE_PAGINATION_CONTRACT_INVALID")
        if has_more and not page_items:
            raise Fast0BlockerResolutionPlanError("PIPELINE_PAGINATION_STALLED")
        items.extend(page_items)
        pages.append(
            {
                "offset": page.get("offset"),
                "returned_count": page.get("returned_count"),
                "full_count": page.get("full_count"),
                "inventory_digest": page.get("inventory_digest"),
                "inventory_end_digest": page.get("inventory_end_digest"),
                "inventory_count_consistent": page.get("inventory_count_consistent"),
                "has_more": page.get("has_more"),
                "next_offset": page.get("next_offset"),
            }
        )
        if not has_more:
            break
        next_offset = page.get("next_offset")
        if type(next_offset) is not int or next_offset <= offset:
            raise Fast0BlockerResolutionPlanError("PIPELINE_PAGINATION_STALLED")
        offset = next_offset
    else:
        raise Fast0BlockerResolutionPlanError("PIPELINE_PAGE_LIMIT_EXCEEDED")
    if first is None:
        raise Fast0BlockerResolutionPlanError("PIPELINE_PAGE_MISSING")
    if len(items) != int(first.get("full_count") or 0):
        raise Fast0BlockerResolutionPlanError("PIPELINE_PAGINATION_INCOMPLETE")
    candidate_ids = [item.get("candidate_instance_id") for item in items]
    valid_candidate_ids = [
        value for value in candidate_ids if isinstance(value, str) and bool(value)
    ]
    if len(valid_candidate_ids) != len(candidate_ids):
        raise Fast0BlockerResolutionPlanError("PIPELINE_SUBJECT_ID_INVALID")
    if len(valid_candidate_ids) != len(set(valid_candidate_ids)):
        raise Fast0BlockerResolutionPlanError("PIPELINE_PAGINATION_DUPLICATE_SUBJECT")
    if _pipeline_inventory_digest(valid_candidate_ids) != first.get("inventory_digest"):
        raise Fast0BlockerResolutionPlanError("PIPELINE_PAGINATION_INVENTORY_MISMATCH")
    return first, items, pages


def _pipeline_page_contract(page: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_date": page.get("trade_date"),
        "full_count": page.get("full_count"),
        "inventory_digest": page.get("inventory_digest"),
        "inventory_end_digest": page.get("inventory_end_digest"),
        "inventory_count_consistent": page.get("inventory_count_consistent"),
    }


def _expected_pipeline_action(item: Mapping[str, Any]) -> str | None:
    classification = str(item.get("classification") or "")
    latest_plan = _mapping(item.get("latest_plan"))
    if (
        classification == "HISTORICAL_CLOSED"
        and str(latest_plan.get("status") or "").upper() == "PLAN_READY"
        and latest_plan.get("unexpired") is False
    ):
        return ACTION_DISPOSE_EXPIRED_PLAN_READY
    if classification == "MISSING_CANDIDATE_MANUAL_REVIEW":
        return ACTION_DISPOSE_ORPHAN
    if classification == "STALE_OTHER_DATE_MANUAL_REVIEW":
        return ACTION_DISPOSE_STALE_OTHER_DATE
    return None


def _incremental_preview_safe(
    preview: Mapping[str, Any],
    *,
    expected: Mapping[str, Any],
) -> bool:
    return bool(
        preview.get("read_only") is True
        and preview.get("observe_only") is True
        and preview.get("no_order_side_effects") is True
        and preview.get("auto_run_evaluation") is False
        and preview.get("live_sim_allowed") is False
        and preview.get("live_real_allowed") is False
        and preview.get("disposition_chain_valid") is True
        and preview.get("dead_letter_id") == expected.get("dead_letter_id")
        and preview.get("action") == ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE
        and preview.get("bucket") == expected.get("bucket")
        and preview.get("dead_letter_fingerprint")
        == expected.get("dead_letter_fingerprint")
        and preview.get("candidate_version") == expected.get("candidate_version")
        and _is_sha256(preview.get("dead_letter_fingerprint"))
        and _is_sha256(preview.get("candidate_version"))
    )


def _pipeline_preview_safe(
    preview: Mapping[str, Any],
    *,
    expected: Mapping[str, Any],
) -> bool:
    return bool(
        preview.get("read_only") is True
        and preview.get("observe_only") is True
        and preview.get("no_order_side_effects") is True
        and preview.get("auto_run_evaluation") is False
        and preview.get("live_sim_allowed") is False
        and preview.get("live_real_allowed") is False
        and preview.get("chain_valid") is True
        and preview.get("candidate_instance_id") == expected.get("candidate_instance_id")
        and preview.get("trade_date") == expected.get("trade_date")
        and preview.get("action") == expected.get("action")
        and all(
            _is_sha256(preview.get(key))
            and preview.get(key) == expected.get(key)
            for key in _PIPELINE_CAS_KEYS
        )
    )


def _execution_phases(
    incremental: Mapping[str, Any],
    pipeline: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "phase": "A",
            "name": "PRE_APPLY_SAFETY_AND_BACKUP",
            "status": "REQUIRED_BEFORE_ANY_WRITE",
            "requirements": [
                "ALL_WRITERS_STOPPED",
                "SIDECARS_ABSENT",
                "RUNTIME_LOCK_COUNT_ZERO",
                "PINNED_DATABASE_IDENTITY",
                "BYTE_IDENTICAL_BACKUP_AND_QUICK_CHECK",
                "CAMPAIGN_MANIFEST_SHA256_APPROVED",
            ],
        },
        {
            "phase": "B",
            "name": "INCREMENTAL_DEAD_LETTER_DISPOSITIONS",
            "status": "AWAITING_SEPARATE_APPLY_APPROVAL",
            "target_count": incremental.get("preview_eligible_count"),
            "campaign_manifest_sha256": incremental.get("campaign_manifest_sha256"),
            "execution": "ONE_ROW_PREVIEW_APPLY_STRICT_VERIFY_STOP_ON_ANY_DRIFT",
        },
        {
            "phase": "C",
            "name": "PIPELINE_HISTORICAL_DISPOSITIONS",
            "status": "AWAITING_SEPARATE_APPLY_APPROVAL_AND_EVIDENCE",
            "automatic_target_count": pipeline.get("automatic_historical_disposition_count"),
            "manual_evidence_target_count": pipeline.get("manual_evidence_disposition_count"),
            "execution": "ONE_SUBJECT_PREVIEW_APPLY_STRICT_VERIFY",
        },
        {
            "phase": "D",
            "name": "PIPELINE_LEGACY_EVIDENCE",
            "status": (
                "COMPLETE"
                if pipeline.get("legacy_evidence_target_count")
                == pipeline.get("legacy_evidence_eligible_count")
                and pipeline.get("legacy_evidence_contract_blocked_count") == 0
                else "CONTRACT_AND_AUTHORITATIVE_EVIDENCE_RESOLVER_REQUIRED"
            ),
            "target_count": pipeline.get("legacy_evidence_target_count"),
            "currently_eligible_count": pipeline.get("legacy_evidence_eligible_count"),
            "target_set_sha256": pipeline.get("legacy_evidence_target_sha256"),
        },
        {
            "phase": "E",
            "name": "PIPELINE_ACTIVE_CURRENT_REBUILD",
            "status": pipeline.get("targeted_rebuild_preview_status"),
            "target_count": pipeline.get("active_current_target_count"),
            "target_set_sha256": pipeline.get("targeted_rebuild_target_sha256"),
            "execution": "AUDITED_ENV_PREVIEW_THEN_SEPARATELY_APPROVED_EXACT_SHA_RUN",
        },
        {
            "phase": "F",
            "name": "FULL_FAST0_REQUALIFICATION",
            "status": "REQUIRED_AFTER_ALL_APPROVED_PHASES",
            "expected_incremental_raw_preserved": incremental.get("raw_dead_letter_count"),
            "expected_incremental_effective": 0,
            "expected_pipeline_qualification": "PASS",
        },
    ]


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    preparation: list[str] = []
    source = _mapping(report.get("source_fast0_report"))
    database = _mapping(report.get("database"))
    identity = _mapping(database.get("identity"))
    identity_pin = _mapping(database.get("identity_pin"))
    connection = _mapping(database.get("connection"))
    schema = _mapping(report.get("schema_manifest"))
    incremental = _mapping(report.get("incremental_dead_letter"))
    pipeline = _mapping(report.get("pipeline"))
    legacy_input = _mapping(report.get("legacy_evidence_input"))
    before = database.get("files_before")
    after = database.get("files_after")

    if source.get("database_main_matches") is not True:
        failures.append("DATABASE_DOES_NOT_MATCH_APPROVED_FAST0_SNAPSHOT")
    if database.get("quick_check") != ["ok"]:
        failures.append("DATABASE_QUICK_CHECK_FAILED")
    if before != after:
        failures.append("DATABASE_FILES_CHANGED_DURING_PLAN")
    if identity.get("schema_version") != _EXPECTED_SCHEMA_VERSION:
        failures.append("DATABASE_SCHEMA_VERSION_MISMATCH")
    if identity.get("schema_version_value_valid") is not True:
        failures.append("DATABASE_SCHEMA_IDENTITY_INVALID")
    if identity_pin.get("status") != "PASS":
        failures.append("DATABASE_IDENTITY_PIN_FAILED")
    if connection.get("query_only") is not True or connection.get("immutable") is not True:
        failures.append("STRICT_READ_ONLY_CONNECTION_INVALID")
    for key in ("writer_probe_before", "writer_probe_after"):
        writer_probe = _mapping(database.get(key))
        if (
            writer_probe.get("status") != "PASS"
            or writer_probe.get("no_other_open_handles") is not True
        ):
            failures.append("OTHER_DATABASE_OPEN_HANDLE_PRESENT")
    if schema.get("ready") is not True or schema.get("invalid_object_count") != 0:
        failures.append("SCHEMA_MANIFEST_INVALID")
    if database.get("runtime_lock_count") != 0:
        failures.append("RUNTIME_EXECUTION_LOCK_PRESENT")
    if report.get("read_only") is not True or report.get("database_write_performed") is not False:
        failures.append("READ_ONLY_PLAN_CONTRACT_INVALID")
    if report.get("identifiers_recorded") is not False:
        failures.append("IDENTIFIER_PRIVACY_CONTRACT_INVALID")
    if legacy_input.get("configured") is True:
        if not (
            legacy_input.get("contract") == LEGACY_EVIDENCE_MANIFEST_CONTRACT
            and _is_sha256(legacy_input.get("file_sha256"))
            and legacy_input.get("content_embedded") is False
            and legacy_input.get("path_recorded") is False
        ):
            failures.append("LEGACY_EVIDENCE_INPUT_CONTRACT_INVALID")
    elif legacy_input != {
        "configured": False,
        "contract": None,
        "file_sha256": None,
        "content_embedded": False,
        "path_recorded": False,
    }:
        failures.append("LEGACY_EVIDENCE_INPUT_CONTRACT_INVALID")

    incremental_expected = _mapping(source.get("incremental_expected"))
    expected_buckets = _mapping(incremental_expected.get("bucket_counts"))
    actual_buckets = _mapping(incremental.get("bucket_counts"))
    if (
        incremental.get("inventory_complete") is not True
        or incremental.get("invalid_item_count") != 0
        or incremental.get("raw_dead_letter_count")
        != incremental_expected.get("raw_dead_letter_count")
        or incremental.get("effective_dead_letter_count")
        != incremental_expected.get("effective_dead_letter_count")
        or actual_buckets != expected_buckets
        or incremental.get("schema_ready") is not True
    ):
        failures.append("INCREMENTAL_INVENTORY_BASELINE_MISMATCH")
    if incremental.get("preview_blocked_count"):
        preparation.append("INCREMENTAL_PREVIEW_BLOCKED_TARGETS_PRESENT")

    pipeline_expected = _mapping(source.get("pipeline_expected"))
    if (
        pipeline.get("inventory_count_consistent") is not True
        or pipeline.get("invalid_item_count") != 0
        or pipeline.get("full_count") != pipeline_expected.get("full_count")
        or pipeline.get("collected_count") != pipeline_expected.get("full_count")
        or pipeline.get("inventory_digest") != pipeline_expected.get("inventory_digest")
        or pipeline.get("collected_inventory_digest")
        != pipeline_expected.get("inventory_digest")
        or pipeline.get("classification_counts")
        != pipeline_expected.get("classification_counts")
        or int(pipeline.get("disposition_required_count") or 0)
        + int(pipeline.get("legacy_evidence_eligible_count") or 0)
        != pipeline_expected.get("disposition_required_count")
        or pipeline.get("active_current_target_count")
        != pipeline_expected.get("active_current_non_pass_count")
        or pipeline.get("schema_ready") is not True
    ):
        failures.append("PIPELINE_INVENTORY_BASELINE_MISMATCH")
    if pipeline.get("preview_blocked_count"):
        preparation.append("PIPELINE_PREVIEW_BLOCKED_OR_UNSUPPORTED_TARGETS_PRESENT")
    if pipeline.get("legacy_evidence_contract_blocked_count"):
        preparation.append("PIPELINE_AUTHORITATIVE_LEGACY_EVIDENCE_REQUIRED")
    if pipeline.get("legacy_evidence_contract_blocked_count"):
        preparation.append("PIPELINE_LEGACY_ELIGIBILITY_CONTRACT_BLOCKED")
    if pipeline.get("manual_evidence_required_count"):
        preparation.append("PIPELINE_MANUAL_EVIDENCE_REQUIRED")
    if pipeline.get("active_current_target_count"):
        preparation.append("PIPELINE_TARGETED_REBUILD_PREVIEW_REQUIRED")
    if database.get("whole_window_writer_absence_proven") is not True:
        preparation.append("PRE_APPLY_WHOLE_WINDOW_WRITER_QUIESCENCE_REQUIRED")
    preparation.append("PRE_APPLY_BYTE_IDENTICAL_BACKUP_REQUIRED")

    evidence_status = "PASS" if not failures else "FAIL"
    plan_status = "COMPLETE" if not failures else "BLOCKED"
    execution_readiness = (
        "PREPARATION_REQUIRED"
        if evidence_status == "PASS" and preparation
        else ("READY_FOR_SEPARATE_APPROVAL" if evidence_status == "PASS" else "BLOCKED")
    )
    return {
        "status": evidence_status,
        "evidence_status": evidence_status,
        "plan_status": plan_status,
        "execution_readiness": execution_readiness,
        "evidence_failures": sorted(set(failures)),
        "preparation_requirements": sorted(set(preparation)),
        "apply_authorized": False,
        "database_files_unchanged": before == after,
        "read_only": True,
        "tool_order_side_effects_invoked": False,
        "tool_trading_side_effects_invoked": False,
        "side_effect_scope": "THIS_TOOL_EXECUTION_ONLY",
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    safe_report = _mapping(fast0_tool._redact(report))
    _assert_aggregate_only_report(safe_report)
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
        "plan: python -B -m tools.ops_fast0_blocker_resolution_plan "
        "--db <schema-62-db> --fast0-report <approved-raw.json> "
        "--fast0-report-sha256 <approved-sha256> "
        "[--legacy-evidence-manifest <restricted-private-manifest.json> "
        "--legacy-evidence-manifest-sha256 <approved-private-sha256>] "
        "--out-dir <evidence-dir>\n"
        "apply: NOT AUTHORIZED; requires a separate campaign manifest and approval\n",
        encoding="utf-8",
    )
    return {"raw_json": raw_path, "summary_md": summary_path, "commands_txt": commands_path}


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    incremental = _mapping(report.get("incremental_dead_letter"))
    pipeline = _mapping(report.get("pipeline"))
    incremental_counts = (
        f"{incremental.get('disposition_target_count')}/"
        f"{incremental.get('preview_eligible_count')}"
    )
    pipeline_counts = (
        f"{pipeline.get('legacy_evidence_target_count')}/"
        f"{pipeline.get('manual_evidence_required_count')}/"
        f"{pipeline.get('active_current_target_count')}"
    )
    return "\n".join(
        [
            "# FAST-0 Blocker Resolution Plan",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- evidence_status: `{verdict.get('evidence_status')}`",
            f"- plan_status: `{verdict.get('plan_status')}`",
            f"- execution_readiness: `{verdict.get('execution_readiness')}`",
            f"- incremental target/eligible: `{incremental_counts}`",
            (
                "- pipeline DB-preview eligible/manual-evidence-required/"
                "apply-authorized: "
                f"`{pipeline.get('preview_eligible_count')}/"
                f"{pipeline.get('manual_evidence_required_count')}/0`"
            ),
            f"- pipeline legacy/manual/active: `{pipeline_counts}`",
            f"- preparation: `{', '.join(verdict.get('preparation_requirements') or []) or '-'}`",
            "",
            "This report is aggregate-only and does not authorize or perform a database write.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    incremental = _mapping(report.get("incremental_dead_letter"))
    pipeline = _mapping(report.get("pipeline"))
    return (
        "FAST-0 blocker resolution plan: "
        f"evidence={verdict.get('evidence_status')} "
        f"plan={verdict.get('plan_status')} "
        f"readiness={verdict.get('execution_readiness')} "
        f"incremental={incremental.get('preview_eligible_count')} "
        f"pipeline_db_preview={pipeline.get('preview_eligible_count')} "
        f"pipeline_manual_evidence={pipeline.get('manual_evidence_required_count')} "
        "apply_authorized=false"
    )


def _with_aliases(
    entries: Sequence[Mapping[str, Any]],
    *,
    prefix: str,
    width: int,
    ordering_key: str,
) -> list[dict[str, Any]]:
    ordered = sorted(entries, key=lambda item: str(item.get(ordering_key) or ""))
    return [
        {**dict(item), "alias": f"{prefix}{index:0{width}d}"}
        for index, item in enumerate(ordered, start=1)
    ]


def _manifest_sha256(entries: Sequence[Mapping[str, Any]]) -> str:
    payload = {
        "contract": "fast0-private-target-set.v1",
        "items": [dict(entry) for entry in entries],
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _pipeline_inventory_digest(candidate_instance_ids: Sequence[str]) -> str:
    payload = {
        "contract": "pipeline-coherency-inventory.v1",
        "candidate_instance_ids": list(candidate_instance_ids),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _assert_aggregate_only_report(value: Any, *, key: str = "") -> None:
    if isinstance(value, Mapping):
        for child, item in value.items():
            child_key = str(child).lower()
            if _report_key_forbidden(child_key):
                raise Fast0BlockerResolutionPlanError("REPORT_CONTAINS_FORBIDDEN_FIELD")
            _assert_aggregate_only_report(item, key=child_key)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_aggregate_only_report(item, key=key)
        return
    if isinstance(value, str) and _looks_like_absolute_path(value):
        raise Fast0BlockerResolutionPlanError("REPORT_CONTAINS_ABSOLUTE_PATH")


def _report_key_forbidden(key: str) -> bool:
    normalized = key[:-1] if key.endswith("s") else key
    if normalized in _FORBIDDEN_REPORT_KEYS:
        return True
    return any(
        normalized.endswith(f"_{suffix}")
        for suffix in (
            "account_id",
            "candidate_instance_id",
            "dead_letter_id",
            "operator_id",
            "owner_id",
        )
    )


def _looks_like_absolute_path(value: str) -> bool:
    return bool(
        _WINDOWS_ABSOLUTE_PATH.match(value) is not None
        or value.startswith("/")
        or value.startswith("\\\\")
    )


def _plan_status_bucket(latest_plan: Mapping[str, Any]) -> str:
    if latest_plan.get("present") is False:
        return "ABSENT"
    status = latest_plan.get("status")
    if not isinstance(status, str) or not status:
        return "UNKNOWN"
    return "PLAN_READY" if status.upper() == "PLAN_READY" else "NON_READY"


def _drift_bucket(value: Any) -> str:
    if value is True:
        return "DRIFT"
    if value is False:
        return "NO_DRIFT"
    return "UNKNOWN"


def _fingerprint_valid(value: Mapping[str, Any]) -> bool:
    return bool(
        value.get("exists") is True
        and _is_sha256(value.get("sha256"))
        and type(value.get("size")) is int
        and int(value["size"]) > 0
        and type(value.get("mtime_ns")) is int
    )


def _is_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in _SHA256_CHARS for character in value)
    )


def _is_nonnegative_int(value: Any) -> bool:
    return type(value) is int and value >= 0


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
