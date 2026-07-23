from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.incremental_dead_letter_campaign import (  # noqa: E402
    CAMPAIGN_ACTION,
    CAMPAIGN_BUCKET,
    CAMPAIGN_PREDECESSOR_KIND,
    CAMPAIGN_TARGET_COUNT,
    IncrementalDeadLetterCampaignError,
    campaign_chain_sha256,
    canonical_sha256,
    privacy_safe_target_projection,
    read_stable_json_document,
    require_alias,
    require_sha256,
    require_utc_z_timestamp,
    validate_campaign_apply_evidence_binding,
    validate_private_target_manifest,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (  # noqa: E402
    CAMPAIGN_EVIDENCE_TYPE,
    CAMPAIGN_REASON_CODE,
    DISPOSITION_TABLE,
    build_incremental_evaluation_dead_letter_effective_status,
    list_incremental_evaluation_dead_letter_effective_rows,
    preview_incremental_evaluation_dead_letter_disposition,
)
from storage.sqlite import APP_NAME, SCHEMA_VERSION  # noqa: E402
from tools import ops_fast0_blocker_resolution_plan as plan_tool  # noqa: E402
from tools import ops_fast0_strict_requalification as fast0_tool  # noqa: E402

PREFLIGHT_CONTRACT = "fast0-incremental-dead-letter-campaign-preflight.v1"
APPLY_REPORT_CONTRACT = "fast0-incremental-dead-letter-campaign-apply.v1"
_EXPECTED_PLAN_CONTRACT = "fast0-blocker-resolution-plan.v1"
_EXPECTED_SCHEMA_VERSION = "63"
_EXPECTED_TARGET_SET_SHA256 = "845b5b9bf82f1a2cebdda9ddf262627f3eda1e91fa53d1b027983284be43f31d"
_FORBIDDEN_PUBLIC_KEYS = frozenset(
    {
        "account_id",
        "candidate_instance_id",
        "db_path",
        "dead_letter_id",
        "error_message",
        "evidence_ref",
        "file_path",
        "operator_id",
        "path",
        "payload",
        "raw_dead_letter",
        "request_id",
        "source_event_id",
    }
)


class IncrementalDeadLetterCampaignPreflightError(RuntimeError):
    pass


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a strict read-only, aggregate/public FAST-0R9 preflight for "
            "exactly one U01-U38 incremental dead-letter campaign alias."
        )
    )
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--blocker-plan-report", required=True, type=Path)
    parser.add_argument("--blocker-plan-report-sha256", required=True)
    parser.add_argument("--private-target-manifest", required=True, type=Path)
    parser.add_argument("--private-target-manifest-sha256", required=True)
    parser.add_argument("--base-backup", required=True, type=Path)
    parser.add_argument("--base-backup-sha256", required=True)
    parser.add_argument("--alias", required=True)
    parser.add_argument("--predecessor-apply-report", type=Path)
    parser.add_argument("--predecessor-apply-report-sha256")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT_DIR / "reports" / "fast0_incremental_dead_letter_campaign_preflight",
    )
    args = parser.parse_args(argv)
    try:
        report = run_report(
            db_path=args.db,
            blocker_plan_report=args.blocker_plan_report,
            expected_blocker_plan_report_sha256=str(args.blocker_plan_report_sha256).lower(),
            private_target_manifest=args.private_target_manifest,
            expected_private_target_manifest_sha256=str(
                args.private_target_manifest_sha256
            ).lower(),
            base_backup=args.base_backup,
            expected_base_backup_sha256=str(args.base_backup_sha256).lower(),
            alias=str(args.alias),
            predecessor_apply_report=args.predecessor_apply_report,
            expected_predecessor_apply_report_sha256=(
                None
                if args.predecessor_apply_report_sha256 is None
                else str(args.predecessor_apply_report_sha256).lower()
            ),
            out_dir=args.out_dir,
        )
    except Exception as exc:
        print(
            f"incremental dead-letter campaign preflight: ERROR error_type={type(exc).__name__}",
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
    private_target_manifest: Path,
    expected_private_target_manifest_sha256: str,
    base_backup: Path,
    expected_base_backup_sha256: str,
    alias: str,
    predecessor_apply_report: Path | None,
    expected_predecessor_apply_report_sha256: str | None,
    out_dir: Path,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    if str(SCHEMA_VERSION) != _EXPECTED_SCHEMA_VERSION:
        raise IncrementalDeadLetterCampaignPreflightError("CODE_TARGET_SCHEMA_MISMATCH")
    normalized_alias = require_alias(alias)
    ordinal = int(normalized_alias[1:])
    collected_at = (observed_at or datetime.now(UTC)).astimezone(UTC)
    if observed_at is not None and (observed_at.tzinfo is None or observed_at.utcoffset() is None):
        raise IncrementalDeadLetterCampaignPreflightError("OBSERVED_AT_INVALID")
    generated_at = _wire(collected_at)

    plan_payload, plan_sha256 = fast0_tool._read_stable_json(
        blocker_plan_report,
        missing_reason="BLOCKER_PLAN_REPORT_NOT_FOUND",
        invalid_reason="BLOCKER_PLAN_REPORT_INVALID",
    )
    if plan_sha256 != require_sha256(
        "BLOCKER_PLAN_REPORT_SHA256", expected_blocker_plan_report_sha256
    ):
        raise IncrementalDeadLetterCampaignPreflightError("BLOCKER_PLAN_REPORT_SHA256_MISMATCH")
    plan = _validate_plan(plan_payload, report_sha256=plan_sha256)
    if plan["generated_at"] > collected_at:
        raise IncrementalDeadLetterCampaignPreflightError("SOURCE_REPORT_TIME_ORDER_INVALID")

    try:
        manifest_document = read_stable_json_document(
            private_target_manifest,
            expected_sha256=require_sha256(
                "PRIVATE_TARGET_MANIFEST_SHA256",
                expected_private_target_manifest_sha256,
            ),
        )
        manifest = validate_private_target_manifest(
            manifest_document,
            expected_semantic_sha256=plan["target_set_sha256"],
        )
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalDeadLetterCampaignPreflightError(exc.code) from exc
    target = dict(manifest["items"][ordinal - 1])
    target_projection = privacy_safe_target_projection(target)

    predecessor = _load_predecessor(
        ordinal=ordinal,
        report_path=predecessor_apply_report,
        expected_report_sha256=expected_predecessor_apply_report_sha256,
        source_plan_report_sha256=plan_sha256,
        private_manifest_sha256=manifest_document.sha256,
        target_set_sha256=manifest["target_set_sha256"],
    )
    backup = _validate_base_backup(
        base_backup,
        expected_sha256=expected_base_backup_sha256,
        plan_database_main=plan["database_main"],
    )

    resolved_path = fast0_tool._validated_database_path(db_path)
    fast0_tool._assert_no_sidecars(resolved_path)
    writer_probe_before = fast0_tool._probe_no_other_open_handles(resolved_path)
    identity_pin = fast0_tool._PinnedDatabaseIdentity(resolved_path)
    with identity_pin:
        files_before = fast0_tool._file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
        expected_main = (
            plan["database_main"]
            if ordinal == 1
            else _mapping(predecessor.get("database_main_after"))
        )
        predecessor_main_matches = fast0_tool._fingerprints_exact(
            expected_main, files_before["main"]
        )
        connection = fast0_tool._open_strict_read_only(
            resolved_path,
            sqlite_source=identity_pin.sqlite_source,
        )
        connection.execute("BEGIN DEFERRED")
        try:
            quick_check_raw = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
            quick_check = ["ok"] if quick_check_raw == ["ok"] else ["not_ok"]
            query_only = bool(int(connection.execute("PRAGMA query_only").fetchone()[0]))
            database_list = [
                {"seq": int(row[0]), "name": str(row[1]), "file_present": bool(row[2])}
                for row in connection.execute("PRAGMA database_list").fetchall()
                if bool(row[2])
            ]
            identity = fast0_tool._read_database_identity(connection)
            schema_raw = fast0_tool._validate_schema_manifest(connection)
            schema_manifest = _safe_schema_manifest(schema_raw)
            runtime_lock_count = int(
                connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0]
            )
            campaign = _campaign_snapshot(
                connection,
                manifest=manifest,
                expected_completed=ordinal - 1,
                source_plan_report_sha256=plan_sha256,
                collected_at=collected_at,
            )
            selected_preview = _validate_selected_preview(
                connection,
                target=target,
                expected_completed=ordinal - 1,
            )
            protected_database = _protected_database_digest(connection)
        finally:
            connection.rollback()
            connection.close()
        identity_pin.assert_path_identity()
        fast0_tool._assert_no_sidecars(resolved_path)
        files_after = fast0_tool._file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
    writer_probe_after = fast0_tool._probe_no_other_open_handles(resolved_path)
    manifest_after = read_stable_json_document(
        private_target_manifest,
        expected_sha256=manifest_document.sha256,
    )
    private_manifest_unchanged = _stable_identity(manifest_document) == _stable_identity(
        manifest_after
    )
    whole_window_writer_absence_proven = bool(
        writer_probe_before.get("status") == "PASS"
        and writer_probe_before.get("no_other_open_handles") is True
        and writer_probe_after.get("status") == "PASS"
        and writer_probe_after.get("no_other_open_handles") is True
        and identity_pin.public_status().get("status") == "PASS"
    )
    predecessor_chain_matches = (
        bool(
            campaign["completed_count"] == ordinal - 1
            and campaign["chain_sha256"] == predecessor.get("campaign_chain_sha256")
        )
        if ordinal > 1
        else campaign["chain_sha256"] is None
    )

    report: dict[str, Any] = {
        "contract": PREFLIGHT_CONTRACT,
        "generated_at": generated_at,
        "mode": "STRICT_READ_ONLY",
        "source_plan": {
            "contract": plan["contract"],
            "report_sha256": plan_sha256,
            "generated_at": _wire(plan["generated_at"]),
            "evidence_status": plan["evidence_status"],
            "plan_status": plan["plan_status"],
            "target_count": plan["target_count"],
            "target_set_sha256": plan["target_set_sha256"],
            "path_recorded": False,
        },
        "private_manifest": {
            "contract": manifest["contract"],
            "file_sha256": manifest_document.sha256,
            "file_size": manifest_document.size,
            "target_set_sha256": manifest["target_set_sha256"],
            "target_count": manifest["count"],
            "selected_alias": normalized_alias,
            "alias_contract": "U{ordinal:02d}",
            "content_embedded": False,
            "path_recorded": False,
            "raw_identifier_mapping_recorded": False,
        },
        "base_backup": backup,
        "predecessor": {
            "kind": predecessor.get("kind"),
            "report_sha256": predecessor.get("report_sha256"),
            "campaign_chain_sha256": predecessor.get("campaign_chain_sha256"),
            "alias": predecessor.get("alias"),
            "ordinal": predecessor.get("ordinal"),
            "database_main_after": predecessor.get("database_main_after"),
            "validated": predecessor.get("validated") is True,
            "content_embedded": False,
            "path_recorded": False,
        },
        "selected_target": {
            **target_projection,
            **selected_preview,
            "content_embedded": False,
        },
        "campaign": {
            **campaign,
            "predecessor_chain_matches": predecessor_chain_matches,
            "protected_database": protected_database,
            "raw_identifiers_recorded": False,
            "raw_rows_recorded": False,
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
            "expected_predecessor_main_matches": predecessor_main_matches,
            "sidecars_absent_before": True,
            "sidecars_absent_after": True,
            "writer_probe_before": writer_probe_before,
            "writer_probe_after": writer_probe_after,
            "whole_window_writer_absence_proven": whole_window_writer_absence_proven,
            "runtime_lock_count": runtime_lock_count,
        },
        "schema_manifest": schema_manifest,
        "private_manifest_unchanged": private_manifest_unchanged,
        "read_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "database_write_performed": False,
        "apply_authorized": False,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    report["verdict"] = evaluate_report(report)
    _assert_public_report(report)
    write_report(report, out_dir=out_dir)
    return report


def _validate_plan(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
) -> dict[str, Any]:
    verdict = _mapping(payload.get("verdict"))
    database = _mapping(payload.get("database"))
    files_before = _mapping(database.get("files_before"))
    files_after = _mapping(database.get("files_after"))
    database_main = _mapping(files_after.get("main"))
    identity = _mapping(database.get("identity"))
    connection = _mapping(database.get("connection"))
    incremental = _mapping(payload.get("incremental_dead_letter"))
    alias_contract = _mapping(incremental.get("campaign_alias_contract"))
    try:
        generated_at = require_utc_z_timestamp("PLAN_GENERATED_AT", payload.get("generated_at"))
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalDeadLetterCampaignPreflightError(
            "BLOCKER_PLAN_REPORT_TIME_INVALID"
        ) from exc
    valid = bool(
        payload.get("contract") == _EXPECTED_PLAN_CONTRACT
        and verdict == plan_tool.evaluate_report(payload)
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("plan_status") == "COMPLETE"
        and payload.get("read_only") is True
        and payload.get("observe_only") is True
        and payload.get("identifiers_recorded") is False
        and payload.get("raw_rows_recorded") is False
        and payload.get("database_write_performed") is False
        and payload.get("apply_authorized") is False
        and database.get("quick_check") == ["ok"]
        and database.get("sidecars_absent_before") is True
        and database.get("sidecars_absent_after") is True
        and database.get("runtime_lock_count") == 0
        and identity.get("app_name") == APP_NAME
        and identity.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and connection.get("mode") == "ro"
        and connection.get("immutable") is True
        and connection.get("query_only") is True
        and files_before == files_after
        and plan_tool._fingerprint_valid(database_main)
        and incremental.get("raw_dead_letter_count") == CAMPAIGN_TARGET_COUNT
        and incremental.get("effective_dead_letter_count") == CAMPAIGN_TARGET_COUNT
        and incremental.get("disposition_target_count") == CAMPAIGN_TARGET_COUNT
        and incremental.get("preview_eligible_count") == CAMPAIGN_TARGET_COUNT
        and incremental.get("preview_blocked_count") == 0
        and incremental.get("invalid_item_count") == 0
        and incremental.get("inventory_complete") is True
        and incremental.get("schema_ready") is True
        and incremental.get("campaign_manifest_sha256") == _EXPECTED_TARGET_SET_SHA256
        and alias_contract.get("format") == "U{ordinal:02d}"
        and alias_contract.get("ordering") == "dead_letter_id ASC"
        and alias_contract.get("first_alias") == "U01"
        and alias_contract.get("last_alias") == "U38"
        and alias_contract.get("raw_identifier_mapping_recorded") is False
        and incremental.get("identifiers_recorded") is False
        and incremental.get("raw_rows_recorded") is False
        and incremental.get("apply_authorized") is False
    )
    if not valid:
        raise IncrementalDeadLetterCampaignPreflightError("BLOCKER_PLAN_REPORT_CONTRACT_INVALID")
    return {
        "contract": _EXPECTED_PLAN_CONTRACT,
        "report_sha256": report_sha256,
        "generated_at": generated_at,
        "evidence_status": verdict.get("evidence_status"),
        "plan_status": verdict.get("plan_status"),
        "target_count": CAMPAIGN_TARGET_COUNT,
        "target_set_sha256": _EXPECTED_TARGET_SET_SHA256,
        "database_main": dict(database_main),
    }


def _load_predecessor(
    *,
    ordinal: int,
    report_path: Path | None,
    expected_report_sha256: str | None,
    source_plan_report_sha256: str,
    private_manifest_sha256: str,
    target_set_sha256: str,
) -> dict[str, Any]:
    if ordinal == 1:
        if report_path is not None or expected_report_sha256 is not None:
            raise IncrementalDeadLetterCampaignPreflightError("U01_PREDECESSOR_REPORT_FORBIDDEN")
        return {
            "kind": None,
            "report_sha256": None,
            "campaign_chain_sha256": None,
            "alias": None,
            "ordinal": None,
            "database_main_after": None,
            "validated": True,
        }
    if report_path is None or expected_report_sha256 is None:
        raise IncrementalDeadLetterCampaignPreflightError("PREDECESSOR_APPLY_REPORT_REQUIRED")
    try:
        document = read_stable_json_document(
            report_path,
            expected_sha256=require_sha256(
                "PREDECESSOR_APPLY_REPORT_SHA256", expected_report_sha256
            ),
        )
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalDeadLetterCampaignPreflightError(exc.code) from exc
    return validate_apply_report(
        document.payload,
        report_sha256=document.sha256,
        expected_alias=f"U{ordinal - 1:02d}",
        source_plan_report_sha256=source_plan_report_sha256,
        private_manifest_sha256=private_manifest_sha256,
        target_set_sha256=target_set_sha256,
    )


def validate_apply_report(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
    expected_alias: str,
    source_plan_report_sha256: str,
    private_manifest_sha256: str,
    target_set_sha256: str,
) -> dict[str, Any]:
    alias = require_alias(expected_alias)
    ordinal = int(alias[1:])
    campaign = _mapping(payload.get("campaign"))
    database = _mapping(payload.get("database"))
    files_before = _mapping(database.get("files_before"))
    files_after = _mapping(database.get("files_after"))
    main_before = _mapping(files_before.get("main"))
    main_after = _mapping(files_after.get("main"))
    result = _mapping(payload.get("result"))
    progress = _mapping(payload.get("progress"))
    invariants = _mapping(payload.get("invariants"))
    verdict = _mapping(payload.get("verdict"))
    result_status = result.get("status")
    replay = result_status == "REPLAYED"
    expected_completed_before = ordinal if replay else ordinal - 1
    expected_effective_before = CAMPAIGN_TARGET_COUNT - expected_completed_before
    if campaign.get("alias") != alias or campaign.get("ordinal") != ordinal:
        raise IncrementalDeadLetterCampaignPreflightError("PREDECESSOR_ALIAS_INVALID")
    predecessor_chain = campaign.get("predecessor_chain_sha256")
    if ordinal == 1:
        predecessor_chain = None
    try:
        recomputed_chain = campaign_chain_sha256(
            source_plan_report_sha256=str(campaign.get("source_plan_report_sha256")),
            target_set_sha256=str(campaign.get("target_set_sha256")),
            alias=alias,
            predecessor_chain_sha256=(
                None if predecessor_chain is None else str(predecessor_chain)
            ),
            approval_binding_sha256=str(campaign.get("approval_binding_sha256")),
            request_id_sha256=str(result.get("request_id_sha256")),
            disposition_id_sha256=str(result.get("disposition_id_sha256")),
        )
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalDeadLetterCampaignPreflightError(exc.code) from exc
    valid = bool(
        payload.get("contract") == APPLY_REPORT_CONTRACT
        and payload.get("mode") == "APPLY"
        and payload.get("append_only") is True
        and payload.get("observe_only") is True
        and payload.get("identifiers_recorded") is False
        and payload.get("raw_rows_recorded") is False
        and payload.get("no_evaluation_run") is True
        and payload.get("no_order_commands_created") is True
        and payload.get("no_broker_calls") is True
        and campaign.get("source_plan_report_sha256") == source_plan_report_sha256
        and campaign.get("private_manifest_sha256") == private_manifest_sha256
        and campaign.get("target_set_sha256") == target_set_sha256
        and campaign.get("action") == CAMPAIGN_ACTION
        and campaign.get("campaign_chain_sha256") == recomputed_chain
        and plan_tool._fingerprint_valid(main_before)
        and plan_tool._fingerprint_valid(main_after)
        and result.get("action") == CAMPAIGN_ACTION
        and result_status in {"APPLIED", "REPLAYED"}
        and plan_tool._is_sha256(result.get("request_id_sha256"))
        and plan_tool._is_sha256(result.get("disposition_id_sha256"))
        and progress.get("completed_count_after") == ordinal
        and progress.get("completed_count_before") == expected_completed_before
        and progress.get("raw_dead_letter_count_before") == CAMPAIGN_TARGET_COUNT
        and progress.get("raw_dead_letter_count_after") == CAMPAIGN_TARGET_COUNT
        and progress.get("effective_dead_letter_count_before") == expected_effective_before
        and progress.get("effective_dead_letter_count_after") == CAMPAIGN_TARGET_COUNT - ordinal
        and invariants.get("raw_inventory_unchanged") is True
        and invariants.get("protected_database_unchanged") is True
        and invariants.get("single_disposition_delta_valid") is True
        and invariants.get("target_effective_transition_valid") is True
        and invariants.get("write_authorizer_violations") == []
        and verdict.get("status") in {"VERIFIED", "REPLAY_VERIFIED"}
        and verdict.get("committed") is True
        and verdict.get("evidence_written") is True
        and verdict.get("operator_action_required") is False
        and verdict.get("failures") == []
    )
    if not valid:
        raise IncrementalDeadLetterCampaignPreflightError(
            "PREDECESSOR_APPLY_REPORT_CONTRACT_INVALID"
        )
    _assert_public_report(payload)
    return {
        "kind": CAMPAIGN_PREDECESSOR_KIND,
        "report_sha256": require_sha256("APPLY_REPORT_SHA256", report_sha256),
        "campaign_chain_sha256": recomputed_chain,
        "alias": alias,
        "ordinal": ordinal,
        "database_main_after": dict(main_after),
        "validated": True,
    }


def _validate_base_backup(
    path: Path,
    *,
    expected_sha256: str,
    plan_database_main: Mapping[str, Any],
) -> dict[str, Any]:
    normalized_sha256 = require_sha256("BASE_BACKUP_SHA256", expected_sha256)
    resolved = fast0_tool._validated_database_path(path)
    fast0_tool._assert_no_sidecars(resolved)
    identity_pin = fast0_tool._PinnedDatabaseIdentity(resolved)
    with identity_pin:
        fingerprint = identity_pin.fingerprint()
        connection = fast0_tool._open_strict_read_only(
            resolved,
            sqlite_source=identity_pin.sqlite_source,
        )
        try:
            quick_check_raw = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
            identity = fast0_tool._read_database_identity(connection)
        finally:
            connection.close()
    matches_plan_base = bool(
        fingerprint.get("sha256") == plan_database_main.get("sha256")
        and fingerprint.get("size") == plan_database_main.get("size")
    )
    valid = bool(
        fingerprint.get("sha256") == normalized_sha256
        and matches_plan_base
        and quick_check_raw == ["ok"]
        and identity.get("app_name") == APP_NAME
        and identity.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and identity_pin.public_status().get("status") == "PASS"
    )
    return {
        "file_sha256": fingerprint.get("sha256"),
        "file_size": fingerprint.get("size"),
        "matches_approved_sha256": fingerprint.get("sha256") == normalized_sha256,
        "matches_source_plan_base": matches_plan_base,
        "quick_check": ["ok"] if quick_check_raw == ["ok"] else ["not_ok"],
        "app_name_valid": identity.get("app_name") == APP_NAME,
        "schema_version": identity.get("schema_version"),
        "identity_pin": identity_pin.public_status(),
        "validated": valid,
        "content_embedded": False,
        "path_recorded": False,
    }


def _campaign_snapshot(
    connection: sqlite3.Connection,
    *,
    manifest: Mapping[str, Any],
    expected_completed: int,
    source_plan_report_sha256: str,
    collected_at: datetime,
) -> dict[str, Any]:
    targets = [dict(item) for item in manifest.get("items") or []]
    rows = list_incremental_evaluation_dead_letter_effective_rows(
        connection,
        limit=500,
    )
    row_by_id = {str(row.get("dead_letter_id") or ""): row for row in rows}
    target_ids = [str(item["dead_letter_id"]) for item in targets]
    raw_inventory_exact = bool(
        len(rows) == CAMPAIGN_TARGET_COUNT
        and len(row_by_id) == CAMPAIGN_TARGET_COUNT
        and set(row_by_id) == set(target_ids)
    )
    target_version_mismatch_count = 0
    target_bucket_mismatch_count = 0
    for index, target in enumerate(targets):
        effective_row = row_by_id.get(str(target["dead_letter_id"]))
        if effective_row is None:
            target_version_mismatch_count += 1
            target_bucket_mismatch_count += 1
            continue
        if (
            effective_row.get("dead_letter_fingerprint") != target["dead_letter_fingerprint"]
            or effective_row.get("candidate_version") != target["candidate_version"]
        ):
            target_version_mismatch_count += 1
        expected_bucket = "HISTORICAL_DISPOSED" if index < expected_completed else CAMPAIGN_BUCKET
        if effective_row.get("bucket") != expected_bucket:
            target_bucket_mismatch_count += 1

    disposition_rows = connection.execute(
        f"SELECT * FROM {DISPOSITION_TABLE} ORDER BY created_at, disposition_id"
    ).fetchall()
    foreign_row_count = sum(
        str(row["dead_letter_id"]) not in set(target_ids) for row in disposition_rows
    )
    unexpected_action_count = sum(str(row["action"]) != CAMPAIGN_ACTION for row in disposition_rows)
    duplicate_target_count = len(disposition_rows) - len(
        {str(row["dead_letter_id"]) for row in disposition_rows}
    )
    invalid_row_count = 0
    invalid_chain_count = 0
    chain_sha256: str | None = None
    rows_by_target: dict[str, list[sqlite3.Row]] = {}
    for disposition_row in disposition_rows:
        rows_by_target.setdefault(str(disposition_row["dead_letter_id"]), []).append(
            disposition_row
        )
    for index, target in enumerate(targets[:expected_completed], start=1):
        target_rows = rows_by_target.get(str(target["dead_letter_id"]), [])
        if len(target_rows) != 1:
            invalid_row_count += 1
            continue
        disposition_row = target_rows[0]
        try:
            evidence = _strict_json_object(str(disposition_row["evidence_json"] or "{}"))
            normalized = validate_campaign_apply_evidence_binding(
                evidence,
                expected_alias=str(target["alias"]),
                dead_letter_id=str(target["dead_letter_id"]),
                expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
                expected_candidate_version=str(target["candidate_version"]),
                evidence_sha256=str(disposition_row["evidence_sha256"]),
                not_after=collected_at,
            )
            row_valid = _campaign_row_contract_valid(
                disposition_row,
                target=target,
                evidence=normalized,
            )
        except (IncrementalDeadLetterCampaignError, TypeError, ValueError):
            invalid_row_count += 1
            continue
        if not row_valid:
            invalid_row_count += 1
            continue
        if normalized["source_plan_report_sha256"] != source_plan_report_sha256:
            invalid_row_count += 1
        if normalized["private_manifest_sha256"] != manifest["manifest_file_sha256"]:
            invalid_row_count += 1
        if normalized["target_set_sha256"] != manifest["target_set_sha256"]:
            invalid_row_count += 1
        if index == 1:
            if any(
                normalized[key] is not None
                for key in (
                    "predecessor_kind",
                    "predecessor_report_sha256",
                    "predecessor_chain_sha256",
                )
            ):
                invalid_chain_count += 1
        elif (
            normalized["predecessor_kind"] != CAMPAIGN_PREDECESSOR_KIND
            or normalized["predecessor_chain_sha256"] != chain_sha256
        ):
            invalid_chain_count += 1
        try:
            chain_sha256 = campaign_chain_sha256(
                source_plan_report_sha256=source_plan_report_sha256,
                target_set_sha256=str(manifest["target_set_sha256"]),
                alias=str(target["alias"]),
                predecessor_chain_sha256=(None if index == 1 else chain_sha256),
                approval_binding_sha256=str(normalized["approval_binding_sha256"]),
                request_id_sha256=_text_sha256(str(disposition_row["request_id"])),
                disposition_id_sha256=_text_sha256(str(disposition_row["disposition_id"])),
            )
        except IncrementalDeadLetterCampaignError:
            invalid_chain_count += 1
            chain_sha256 = None

    completed_ids = set(target_ids[:expected_completed])
    pending_ids = set(target_ids[expected_completed:])
    missing_completed_count = sum(
        len(rows_by_target.get(identifier, [])) != 1 for identifier in completed_ids
    )
    premature_future_count = sum(bool(rows_by_target.get(identifier)) for identifier in pending_ids)
    effective = build_incremental_evaluation_dead_letter_effective_status(connection)
    valid_completed_count = max(
        0,
        expected_completed - invalid_row_count - invalid_chain_count - missing_completed_count,
    )
    progress_valid = bool(
        raw_inventory_exact
        and target_version_mismatch_count == 0
        and target_bucket_mismatch_count == 0
        and len(disposition_rows) == expected_completed
        and foreign_row_count == 0
        and unexpected_action_count == 0
        and duplicate_target_count == 0
        and missing_completed_count == 0
        and premature_future_count == 0
        and invalid_row_count == 0
        and invalid_chain_count == 0
        and effective.get("raw_dead_letter_count") == CAMPAIGN_TARGET_COUNT
        and effective.get("effective_dead_letter_count")
        == CAMPAIGN_TARGET_COUNT - expected_completed
        and effective.get("historical_pending_disposition_count")
        == CAMPAIGN_TARGET_COUNT - expected_completed
        and effective.get("historical_disposed_dead_letter_count") == expected_completed
    )
    return {
        "target_count": CAMPAIGN_TARGET_COUNT,
        "completed_count": expected_completed,
        "valid_completed_count": valid_completed_count,
        "pending_count": CAMPAIGN_TARGET_COUNT - expected_completed,
        "raw_dead_letter_count": effective.get("raw_dead_letter_count"),
        "effective_dead_letter_count": effective.get("effective_dead_letter_count"),
        "historical_pending_disposition_count": effective.get(
            "historical_pending_disposition_count"
        ),
        "historical_disposed_dead_letter_count": effective.get(
            "historical_disposed_dead_letter_count"
        ),
        "disposition_ledger_row_count": len(disposition_rows),
        "foreign_row_count": foreign_row_count,
        "unexpected_action_count": unexpected_action_count,
        "duplicate_target_count": duplicate_target_count,
        "missing_completed_count": missing_completed_count,
        "premature_future_count": premature_future_count,
        "invalid_row_count": invalid_row_count,
        "invalid_chain_count": invalid_chain_count,
        "target_version_mismatch_count": target_version_mismatch_count,
        "target_bucket_mismatch_count": target_bucket_mismatch_count,
        "raw_inventory_exact": raw_inventory_exact,
        "chain_sha256": chain_sha256,
        "progress_valid": progress_valid,
    }


def _campaign_row_contract_valid(
    row: sqlite3.Row,
    *,
    target: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> bool:
    if not (
        str(row["action"]) == CAMPAIGN_ACTION
        and str(row["dead_letter_id"]) == target["dead_letter_id"]
        and str(row["expected_dead_letter_fingerprint"]) == target["dead_letter_fingerprint"]
        and str(row["expected_candidate_version"]) == target["candidate_version"]
        and str(row["reason_code"]) == CAMPAIGN_REASON_CODE
        and int(row["sequence_no"]) == 1
        and row["supersedes_disposition_id"] is None
        and row["recovery_session_id"] is None
        and row["batch_size"] is None
        and row["fencing_token"] is None
        and int(row["observe_only"]) == 1
        and int(row["live_sim_allowed"]) == 0
        and int(row["live_real_allowed"]) == 0
        and int(row["auto_run_evaluation"]) == 0
        and str(row["evidence_sha256"]) == evidence["evidence_sha256"]
    ):
        return False
    request_payload = {
        "action": str(row["action"]),
        "dead_letter_id": str(row["dead_letter_id"]),
        "request_id": str(row["request_id"]),
        "expected_dead_letter_fingerprint": str(row["expected_dead_letter_fingerprint"]),
        "expected_candidate_version": str(row["expected_candidate_version"]),
        "reason_code": str(row["reason_code"]),
        "operator_id": str(row["operator_id"]),
        "evidence_type": CAMPAIGN_EVIDENCE_TYPE,
        "evidence_ref": str(row["evidence_ref"]),
        "evidence_sha256": str(row["evidence_sha256"]),
        "supersedes_disposition_id": None,
        "recovery_session_id": None,
        "batch_size": None,
        "authorization_disposition_id": None,
        "campaign_evidence_json_sha256": canonical_sha256(evidence),
    }
    return bool(canonical_sha256(request_payload) == str(row["request_hash"]))


def _validate_selected_preview(
    connection: sqlite3.Connection,
    *,
    target: Mapping[str, Any],
    expected_completed: int,
) -> dict[str, Any]:
    preview = preview_incremental_evaluation_dead_letter_disposition(
        connection,
        str(target["dead_letter_id"]),
        action=CAMPAIGN_ACTION,
        expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
        expected_candidate_version=str(target["candidate_version"]),
    )
    reason_codes = [str(value) for value in preview.get("reason_codes") or []]
    cas_matches = bool(
        preview.get("dead_letter_fingerprint") == target["dead_letter_fingerprint"]
        and preview.get("candidate_version") == target["candidate_version"]
    )
    safe = bool(
        preview.get("action") == CAMPAIGN_ACTION
        and preview.get("bucket") == CAMPAIGN_BUCKET
        and preview.get("eligible") is True
        and not reason_codes
        and cas_matches
        and preview.get("read_only") is True
        and preview.get("observe_only") is True
        and preview.get("no_order_side_effects") is True
        and preview.get("auto_run_evaluation") is False
        and preview.get("live_sim_allowed") is False
        and preview.get("live_real_allowed") is False
    )
    return {
        "expected_completed_prefix_count": expected_completed,
        "eligible": preview.get("eligible") is True,
        "reason_code_count": len(reason_codes),
        "cas_matches": cas_matches,
        "preview_contract_valid": safe,
        "read_only": preview.get("read_only") is True,
        "observe_only": preview.get("observe_only") is True,
        "no_order_side_effects": preview.get("no_order_side_effects") is True,
        "auto_run_evaluation": preview.get("auto_run_evaluation") is True,
        "live_sim_allowed": preview.get("live_sim_allowed") is True,
        "live_real_allowed": preview.get("live_real_allowed") is True,
    }


def _protected_database_digest(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = [
        str(row[0])
        for row in connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
              AND name != ?
            ORDER BY name
            """,
            (DISPOSITION_TABLE,),
        ).fetchall()
    ]
    digest = hashlib.sha256()
    row_count = 0
    for table in tables:
        table_result = _table_digest(connection, table)
        encoded = json.dumps(
            {"table": table, **table_result},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        row_count += int(table_result["row_count"])
    return {
        "contract": "fast0-incremental-campaign-protected-database.v1",
        "table_count": len(tables),
        "row_count": row_count,
        "sha256": digest.hexdigest(),
        "table_names_recorded": False,
        "raw_rows_recorded": False,
    }


def _table_digest(connection: sqlite3.Connection, table: str) -> dict[str, Any]:
    quoted = '"' + table.replace('"', '""') + '"'
    columns = [str(row[1]) for row in connection.execute(f"PRAGMA table_info({quoted})").fetchall()]
    try:
        cursor = connection.execute(f"SELECT * FROM {quoted} ORDER BY _rowid_")
    except sqlite3.OperationalError:
        order = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
        cursor = connection.execute(
            f"SELECT * FROM {quoted}" + (f" ORDER BY {order}" if order else "")
        )
    digest = hashlib.sha256()
    row_count = 0
    for row in cursor:
        values = [_hash_value(row[column]) for column in columns]
        encoded = json.dumps(values, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        row_count += 1
    return {"row_count": row_count, "sha256": digest.hexdigest()}


def _hash_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, bytes):
        return {
            "type": "blob",
            "size": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    if isinstance(value, float):
        return {"type": "float", "value": value.hex()}
    if isinstance(value, int):
        return {"type": "integer", "value": value}
    return {"type": "text", "value": str(value)}


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    source = _mapping(report.get("source_plan"))
    manifest = _mapping(report.get("private_manifest"))
    backup = _mapping(report.get("base_backup"))
    predecessor = _mapping(report.get("predecessor"))
    target = _mapping(report.get("selected_target"))
    campaign = _mapping(report.get("campaign"))
    protected = _mapping(campaign.get("protected_database"))
    database = _mapping(report.get("database"))
    schema = _mapping(report.get("schema_manifest"))
    connection = _mapping(database.get("connection"))
    if not (
        source.get("evidence_status") == "PASS"
        and source.get("plan_status") == "COMPLETE"
        and source.get("target_count") == CAMPAIGN_TARGET_COUNT
        and source.get("target_set_sha256") == _EXPECTED_TARGET_SET_SHA256
    ):
        failures.append("SOURCE_PLAN_INVALID")
    if not (
        manifest.get("file_sha256")
        and manifest.get("target_set_sha256") == source.get("target_set_sha256")
        and manifest.get("target_count") == CAMPAIGN_TARGET_COUNT
        and manifest.get("content_embedded") is False
        and manifest.get("path_recorded") is False
        and manifest.get("raw_identifier_mapping_recorded") is False
        and report.get("private_manifest_unchanged") is True
    ):
        failures.append("PRIVATE_MANIFEST_INVALID")
    if not (
        backup.get("validated") is True
        and backup.get("matches_approved_sha256") is True
        and backup.get("matches_source_plan_base") is True
        and backup.get("quick_check") == ["ok"]
        and backup.get("schema_version") == _EXPECTED_SCHEMA_VERSION
        and backup.get("path_recorded") is False
    ):
        failures.append("BASE_BACKUP_INVALID")
    if predecessor.get("validated") is not True:
        failures.append("PREDECESSOR_REPORT_INVALID")
    if database.get("expected_predecessor_main_matches") is not True:
        failures.append("DATABASE_PREDECESSOR_FINGERPRINT_MISMATCH")
    if database.get("quick_check") != ["ok"]:
        failures.append("DATABASE_QUICK_CHECK_FAILED")
    if database.get("files_before") != database.get("files_after"):
        failures.append("DATABASE_CHANGED_DURING_PREFLIGHT")
    if not (
        database.get("sidecars_absent_before") is True
        and database.get("sidecars_absent_after") is True
        and database.get("whole_window_writer_absence_proven") is True
        and database.get("runtime_lock_count") == 0
        and connection.get("mode") == "ro"
        and connection.get("immutable") is True
        and connection.get("query_only") is True
        and connection.get("single_deferred_snapshot") is True
        and _database_list_main_only(database.get("database_list"))
    ):
        failures.append("DATABASE_QUIESCENCE_OR_CONNECTION_INVALID")
    if not (
        schema.get("ready") is True
        and schema.get("invalid_object_count") == 0
        and schema.get("persistent_trigger_set_valid") is True
        and schema.get("critical_trigger_sets_valid") is True
    ):
        failures.append("SCHEMA_MANIFEST_INVALID")
    if not (
        campaign.get("progress_valid") is True
        and campaign.get("predecessor_chain_matches") is True
        and campaign.get("raw_inventory_exact") is True
        and campaign.get("invalid_row_count") == 0
        and campaign.get("invalid_chain_count") == 0
        and campaign.get("foreign_row_count") == 0
        and campaign.get("unexpected_action_count") == 0
        and campaign.get("premature_future_count") == 0
        and campaign.get("target_version_mismatch_count") == 0
        and campaign.get("target_bucket_mismatch_count") == 0
        and plan_tool._is_sha256(protected.get("sha256"))
        and protected.get("raw_rows_recorded") is False
    ):
        failures.append("CAMPAIGN_PROGRESS_INVALID")
    if not (
        target.get("eligible") is True
        and target.get("reason_code_count") == 0
        and target.get("cas_matches") is True
        and target.get("preview_contract_valid") is True
        and target.get("read_only") is True
        and target.get("observe_only") is True
        and target.get("no_order_side_effects") is True
        and target.get("auto_run_evaluation") is False
        and target.get("live_sim_allowed") is False
        and target.get("live_real_allowed") is False
    ):
        failures.append("SELECTED_ALIAS_PREVIEW_INVALID")
    if not (
        report.get("read_only") is True
        and report.get("observe_only") is True
        and report.get("identifiers_recorded") is False
        and report.get("raw_rows_recorded") is False
        and report.get("database_write_performed") is False
        and report.get("apply_authorized") is False
        and report.get("no_evaluation_run") is True
        and report.get("no_order_commands_created") is True
        and report.get("no_broker_calls") is True
        and report.get("live_sim_allowed") is False
        and report.get("live_real_allowed") is False
    ):
        failures.append("PREFLIGHT_SAFETY_CONTRACT_INVALID")
    status = "PASS" if not failures else "FAIL"
    return {
        "status": status,
        "evidence_status": status,
        "plan_status": "COMPLETE" if not failures else "BLOCKED",
        "execution_readiness": ("PREPARATION_REQUIRED" if not failures else "BLOCKED"),
        "evidence_failures": sorted(set(failures)),
        "preparation_requirements": (
            ["SEPARATE_SINGLE_ALIAS_APPLY_APPROVAL_REQUIRED"] if not failures else []
        ),
        "apply_authorized": False,
        "database_files_unchanged": database.get("files_before") == database.get("files_after"),
        "tool_order_side_effects_invoked": False,
        "tool_trading_side_effects_invoked": False,
    }


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    _assert_public_report(report)
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    return {"raw": raw_path, "summary": summary_path}


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    manifest = _mapping(report.get("private_manifest"))
    campaign = _mapping(report.get("campaign"))
    return (
        "FAST-0R9 incremental dead-letter campaign preflight\n"
        f"- alias: {manifest.get('selected_alias')}\n"
        f"- progress: {campaign.get('completed_count')}/{campaign.get('target_count')}\n"
        f"- evidence: {verdict.get('evidence_status')}\n"
        f"- readiness: {verdict.get('execution_readiness')}\n"
        "- apply: NOT AUTHORIZED; separate exact single-alias approval required"
    )


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    manifest = _mapping(report.get("private_manifest"))
    campaign = _mapping(report.get("campaign"))
    return "\n".join(
        [
            "# FAST-0R9 incremental dead-letter campaign preflight",
            "",
            f"- Alias: `{manifest.get('selected_alias')}`",
            f"- Progress: `{campaign.get('completed_count')}/{campaign.get('target_count')}`",
            f"- Evidence: `{verdict.get('evidence_status')}`",
            f"- Plan: `{verdict.get('plan_status')}`",
            f"- Readiness: `{verdict.get('execution_readiness')}`",
            "- Apply authorized: `false`",
            "- Raw identifiers recorded: `false`",
            "",
        ]
    )


def _safe_schema_manifest(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ready": value.get("ready") is True,
        "expected_object_count": value.get("expected_object_count"),
        "present_object_count": value.get("present_object_count"),
        "valid_object_count": value.get("valid_object_count"),
        "invalid_object_count": len(value.get("invalid_objects") or []),
        "persistent_trigger_set_valid": value.get("persistent_trigger_set_valid") is True,
        "critical_trigger_sets_valid": value.get("critical_trigger_sets_valid") is True,
    }


def _strict_json_object(value: str) -> dict[str, Any]:
    def reject_duplicates(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    def reject_constant(raw: str) -> None:
        raise ValueError(f"nonfinite JSON number: {raw}")

    parsed = json.loads(
        value,
        object_pairs_hook=reject_duplicates,
        parse_constant=reject_constant,
    )
    if not isinstance(parsed, dict):
        raise ValueError("JSON object required")
    return parsed


def _database_list_main_only(value: object) -> bool:
    return bool(
        isinstance(value, list)
        and len(value) == 1
        and isinstance(value[0], Mapping)
        and value[0].get("seq") == 0
        and value[0].get("name") == "main"
        and value[0].get("file_present") is True
    )


def _assert_public_report(value: Any, *, key: str = "") -> None:
    if isinstance(value, Mapping):
        for child, item in value.items():
            child_key = str(child).lower()
            if child_key in _FORBIDDEN_PUBLIC_KEYS:
                raise IncrementalDeadLetterCampaignPreflightError("PUBLIC_REPORT_FORBIDDEN_FIELD")
            if child_key.endswith(("_path", "_ref", "_operator_id")) and child_key not in {
                "path_recorded"
            }:
                raise IncrementalDeadLetterCampaignPreflightError("PUBLIC_REPORT_FORBIDDEN_FIELD")
            _assert_public_report(item, key=child_key)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_public_report(item, key=key)
        return
    if isinstance(value, str) and (
        value.startswith("/")
        or (len(value) >= 3 and value[1] == ":" and value[2] in {"/", "\\"})
        or value.startswith("\\\\")
    ):
        raise IncrementalDeadLetterCampaignPreflightError("PUBLIC_REPORT_ABSOLUTE_PATH_FORBIDDEN")


def _stable_identity(value: Any) -> tuple[Any, ...]:
    return (
        value.sha256,
        value.size,
        value.device,
        value.inode,
        value.mtime_ns,
    )


def _text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _wire(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
