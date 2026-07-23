from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.config import (  # noqa: E402
    ENV_FILE_PATH_ENV,
    clear_settings_cache,
    load_settings,
)
from services.incremental_dead_letter_campaign import (  # noqa: E402
    CAMPAIGN_ACTION,
    IncrementalDeadLetterCampaignError,
    build_campaign_apply_evidence_binding,
    campaign_chain_sha256,
    canonical_json,
    privacy_safe_target_projection,
    read_stable_json_document,
    require_alias,
    require_sha256,
    require_utc_z_timestamp,
    validate_campaign_apply_evidence_binding,
    validate_private_target_manifest,
)
from services.pipeline_orphan_manual_evidence import (  # noqa: E402
    OrphanManualEvidenceError,
    read_stable_file_fingerprint,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (  # noqa: E402
    CAMPAIGN_REASON_CODE,
    DISPOSITION_TABLE,
    _issue_incremental_campaign_apply_approval_context,
    append_incremental_dead_letter_campaign_disposition_in_transaction,
)
from tools import ops_fast0_strict_requalification as fast0_tool  # noqa: E402
from tools import ops_incremental_dead_letter_campaign_preflight as preflight_tool  # noqa: E402
from tools import resolve_incremental_evaluation_dead_letter as legacy_tool  # noqa: E402

APPLY_CONTRACT = "fast0-incremental-dead-letter-campaign-apply.v1"
APPLY_ACKNOWLEDGEMENT = "APPLY-EXACT-FAST0R9-SINGLE-ALIAS"
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@-]{2,199}$")
_SQLITE_WRITE_ACTIONS = frozenset(
    {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
)


class IncrementalDeadLetterCampaignApplyError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(self.code)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Append or reconcile exactly one approved FAST-0R9 U01-U38 "
            "incremental dead-letter disposition."
        )
    )
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--blocker-plan-report", required=True, type=Path)
    parser.add_argument("--blocker-plan-report-sha256", required=True)
    parser.add_argument("--campaign-preflight-report", required=True, type=Path)
    parser.add_argument("--campaign-preflight-report-sha256", required=True)
    parser.add_argument("--private-target-manifest", required=True, type=Path)
    parser.add_argument("--private-target-manifest-sha256", required=True)
    parser.add_argument("--alias", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--operator-id", required=True)
    parser.add_argument("--evidence-ref", required=True)
    parser.add_argument("--evidence-file", required=True, type=Path)
    parser.add_argument("--evidence-file-sha256", required=True)
    parser.add_argument("--acknowledge", required=True)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT_DIR / "reports" / "fast0_incremental_dead_letter_campaign_apply",
    )
    args = parser.parse_args(argv)
    try:
        report = apply_alias(
            db_path=args.db,
            blocker_plan_report=args.blocker_plan_report,
            expected_blocker_plan_report_sha256=str(args.blocker_plan_report_sha256).lower(),
            preflight_report=args.campaign_preflight_report,
            expected_preflight_report_sha256=str(args.campaign_preflight_report_sha256).lower(),
            private_target_manifest=args.private_target_manifest,
            expected_private_target_manifest_sha256=str(
                args.private_target_manifest_sha256
            ).lower(),
            alias=str(args.alias),
            request_id=str(args.request_id),
            operator_id=str(args.operator_id),
            evidence_ref=str(args.evidence_ref),
            evidence_file=args.evidence_file,
            expected_evidence_file_sha256=str(args.evidence_file_sha256).lower(),
            acknowledgement=str(args.acknowledge),
            out_dir=args.out_dir,
        )
    except Exception as exc:
        payload = {
            "contract": APPLY_CONTRACT,
            "mode": "NO_WRITE_OR_STATE_REQUIRES_STRICT_READBACK",
            "status": "REJECTED",
            "reason_code": str(getattr(exc, "code", "CAMPAIGN_APPLY_REJECTED")),
            "error_type": type(exc).__name__,
            "identifiers_recorded": False,
            "raw_rows_recorded": False,
            "no_evaluation_run": True,
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_sim_allowed": False,
            "live_real_allowed": False,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return (
        0
        if report.get("verdict", {}).get("status")
        in {
            "VERIFIED",
            "REPLAY_VERIFIED",
        }
        else 2
    )


def apply_alias(
    *,
    db_path: Path,
    blocker_plan_report: Path,
    expected_blocker_plan_report_sha256: str,
    preflight_report: Path,
    expected_preflight_report_sha256: str,
    private_target_manifest: Path,
    expected_private_target_manifest_sha256: str,
    alias: str,
    request_id: str,
    operator_id: str,
    evidence_ref: str,
    evidence_file: Path,
    expected_evidence_file_sha256: str,
    acknowledgement: str,
    out_dir: Path,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    if acknowledgement != APPLY_ACKNOWLEDGEMENT:
        raise IncrementalDeadLetterCampaignApplyError(
            "EXACT_SINGLE_ALIAS_APPLY_ACKNOWLEDGEMENT_REQUIRED"
        )
    normalized_alias = require_alias(alias)
    ordinal = int(normalized_alias[1:])
    normalized_request_id = _safe_label("REQUEST_ID", request_id)
    normalized_operator_id = _safe_label("OPERATOR_ID", operator_id)
    normalized_evidence_ref = _safe_label("EVIDENCE_REF", evidence_ref)
    now = observed_at or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise IncrementalDeadLetterCampaignApplyError("OBSERVED_AT_INVALID")
    now = now.astimezone(UTC)

    inputs = _load_inputs(
        blocker_plan_report=blocker_plan_report,
        expected_blocker_plan_report_sha256=expected_blocker_plan_report_sha256,
        preflight_report=preflight_report,
        expected_preflight_report_sha256=expected_preflight_report_sha256,
        private_target_manifest=private_target_manifest,
        expected_private_target_manifest_sha256=expected_private_target_manifest_sha256,
        evidence_file=evidence_file,
        expected_evidence_file_sha256=expected_evidence_file_sha256,
        alias=normalized_alias,
        not_after=now,
    )
    target = inputs["target"]
    preflight = inputs["preflight"]
    resolved_path = legacy_tool._validated_database_path(db_path)
    _assert_no_sidecars(resolved_path)
    writer_probe = fast0_tool._probe_no_other_open_handles(resolved_path)
    if not (
        writer_probe.get("status") == "PASS" and writer_probe.get("no_other_open_handles") is True
    ):
        raise IncrementalDeadLetterCampaignApplyError("DATABASE_WRITER_QUIESCENCE_REQUIRED")
    settings, env_document = _load_safe_settings(resolved_path)
    files_before = fast0_tool._file_fingerprints(resolved_path)
    approved_main = _mapping(preflight["database_main"])
    if not fast0_tool._fingerprints_exact(approved_main, files_before["main"]):
        # An exact replay after an ambiguous commit is handled under the writer
        # lock below; any other drift remains blocked.
        preflight_main_matches = False
    else:
        preflight_main_matches = True

    binding = build_campaign_apply_evidence_binding(
        target,
        source_plan_report_sha256=str(inputs["plan"]["report_sha256"]),
        private_manifest_sha256=str(inputs["manifest"]["manifest_file_sha256"]),
        target_set_sha256=str(inputs["manifest"]["target_set_sha256"]),
        approved_preflight_report_sha256=str(inputs["preflight_report_sha256"]),
        approved_database_main_sha256=str(approved_main["sha256"]),
        approved_database_main_size=int(approved_main["size"]),
        predecessor_kind=preflight["predecessor_kind"],
        predecessor_report_sha256=preflight["predecessor_report_sha256"],
        predecessor_chain_sha256=preflight["predecessor_chain_sha256"],
        evidence_sha256=str(inputs["evidence_sha256"]),
        generated_at=str(preflight["generated_at"]),
        preflight_generated_at=str(preflight["generated_at"]),
    )

    return _apply_under_lock(
        db_path=resolved_path,
        files_before=files_before,
        preflight_main_matches=preflight_main_matches,
        inputs=inputs,
        target=target,
        binding=binding,
        alias=normalized_alias,
        ordinal=ordinal,
        request_id=normalized_request_id,
        operator_id=normalized_operator_id,
        evidence_ref=normalized_evidence_ref,
        settings=settings,
        env_document=env_document,
        out_dir=out_dir,
        observed_at=now,
    )


def _load_inputs(
    *,
    blocker_plan_report: Path,
    expected_blocker_plan_report_sha256: str,
    preflight_report: Path,
    expected_preflight_report_sha256: str,
    private_target_manifest: Path,
    expected_private_target_manifest_sha256: str,
    evidence_file: Path,
    expected_evidence_file_sha256: str,
    alias: str,
    not_after: datetime,
) -> dict[str, Any]:
    try:
        plan_document = read_stable_json_document(
            blocker_plan_report,
            expected_sha256=require_sha256(
                "BLOCKER_PLAN_REPORT_SHA256",
                expected_blocker_plan_report_sha256,
            ),
        )
        plan = preflight_tool._validate_plan(
            plan_document.payload,
            report_sha256=plan_document.sha256,
        )
        manifest_document = read_stable_json_document(
            private_target_manifest,
            expected_sha256=require_sha256(
                "PRIVATE_TARGET_MANIFEST_SHA256",
                expected_private_target_manifest_sha256,
            ),
        )
        manifest = validate_private_target_manifest(
            manifest_document,
            expected_semantic_sha256=str(plan["target_set_sha256"]),
        )
        preflight_document = read_stable_json_document(
            preflight_report,
            expected_sha256=require_sha256(
                "CAMPAIGN_PREFLIGHT_REPORT_SHA256",
                expected_preflight_report_sha256,
            ),
        )
        preflight = _validate_preflight_report(
            preflight_document.payload,
            report_sha256=preflight_document.sha256,
            plan=plan,
            manifest=manifest,
            alias=alias,
            not_after=not_after,
        )
        evidence = read_stable_file_fingerprint(
            evidence_file,
            expected_sha256=require_sha256("EVIDENCE_FILE_SHA256", expected_evidence_file_sha256),
        )
    except (IncrementalDeadLetterCampaignError, OrphanManualEvidenceError) as exc:
        raise IncrementalDeadLetterCampaignApplyError(
            str(getattr(exc, "code", "CAMPAIGN_INPUT_INVALID"))
        ) from exc
    target = dict(manifest["items"][int(alias[1:]) - 1])
    identities = {
        "plan": _stable_identity(plan_document),
        "manifest": _stable_identity(manifest_document),
        "preflight": _stable_identity(preflight_document),
        "evidence": _stable_identity(evidence),
    }
    return {
        "plan": plan,
        "manifest": manifest,
        "preflight": preflight,
        "target": target,
        "evidence_sha256": evidence.sha256,
        "evidence_size": evidence.size,
        "preflight_report_sha256": preflight_document.sha256,
        "identities": identities,
        "paths": {
            "plan": blocker_plan_report,
            "manifest": private_target_manifest,
            "preflight": preflight_report,
            "evidence": evidence_file,
        },
        "expected_hashes": {
            "plan": plan_document.sha256,
            "manifest": manifest_document.sha256,
            "preflight": preflight_document.sha256,
            "evidence": evidence.sha256,
        },
    }


def _validate_preflight_report(
    payload: Mapping[str, Any],
    *,
    report_sha256: str,
    plan: Mapping[str, Any],
    manifest: Mapping[str, Any],
    alias: str,
    not_after: datetime,
) -> dict[str, Any]:
    verdict = _mapping(payload.get("verdict"))
    source = _mapping(payload.get("source_plan"))
    private_manifest = _mapping(payload.get("private_manifest"))
    selected = _mapping(payload.get("selected_target"))
    campaign = _mapping(payload.get("campaign"))
    protected = _mapping(campaign.get("protected_database"))
    predecessor = _mapping(payload.get("predecessor"))
    database = _mapping(payload.get("database"))
    files_after = _mapping(database.get("files_after"))
    database_main = _mapping(files_after.get("main"))
    target = dict(manifest["items"][int(alias[1:]) - 1])
    expected_projection = privacy_safe_target_projection(target)
    try:
        generated_at = require_utc_z_timestamp(
            "PREFLIGHT_GENERATED_AT", payload.get("generated_at")
        )
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalDeadLetterCampaignApplyError(exc.code) from exc
    expected_predecessor = None if alias == "U01" else "CAMPAIGN_APPLY_REPORT"
    valid = bool(
        payload.get("contract") == preflight_tool.PREFLIGHT_CONTRACT
        and payload.get("mode") == "STRICT_READ_ONLY"
        and verdict == preflight_tool.evaluate_report(payload)
        and verdict.get("evidence_status") == "PASS"
        and verdict.get("plan_status") == "COMPLETE"
        and verdict.get("execution_readiness") == "PREPARATION_REQUIRED"
        and verdict.get("apply_authorized") is False
        and generated_at <= not_after.astimezone(UTC)
        and source.get("report_sha256") == plan["report_sha256"]
        and source.get("target_set_sha256") == manifest["target_set_sha256"]
        and private_manifest.get("file_sha256") == manifest["manifest_file_sha256"]
        and private_manifest.get("target_set_sha256") == manifest["target_set_sha256"]
        and private_manifest.get("selected_alias") == alias
        and all(selected.get(key) == value for key, value in expected_projection.items())
        and selected.get("eligible") is True
        and selected.get("preview_contract_valid") is True
        and campaign.get("completed_count") == int(alias[1:]) - 1
        and campaign.get("progress_valid") is True
        and campaign.get("predecessor_chain_matches") is True
        and preflight_tool.plan_tool._is_sha256(protected.get("sha256"))
        and predecessor.get("kind") == expected_predecessor
        and predecessor.get("validated") is True
        and database.get("expected_predecessor_main_matches") is True
        and database.get("files_before") == database.get("files_after")
        and database.get("quick_check") == ["ok"]
        and database.get("whole_window_writer_absence_proven") is True
        and plan_tool_fingerprint_valid(database_main)
        and payload.get("read_only") is True
        and payload.get("database_write_performed") is False
        and payload.get("apply_authorized") is False
        and payload.get("identifiers_recorded") is False
        and payload.get("raw_rows_recorded") is False
    )
    if not valid:
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_PREFLIGHT_REPORT_CONTRACT_INVALID")
    preflight_tool._assert_public_report(payload)
    return {
        "report_sha256": require_sha256("PREFLIGHT_REPORT_SHA256", report_sha256),
        "generated_at": str(payload["generated_at"]),
        "database_main": dict(database_main),
        "campaign": dict(campaign),
        "protected_database": dict(protected),
        "predecessor_kind": predecessor.get("kind"),
        "predecessor_report_sha256": predecessor.get("report_sha256"),
        "predecessor_chain_sha256": predecessor.get("campaign_chain_sha256"),
    }


def _load_safe_settings(db_path: Path) -> tuple[Any, Any]:
    configured = os.environ.get(ENV_FILE_PATH_ENV, "").strip()
    if not configured:
        raise IncrementalDeadLetterCampaignApplyError("EXPLICIT_TRADING_ENV_FILE_REQUIRED")
    env_path = Path(configured).expanduser()
    try:
        env_document = read_stable_file_fingerprint(env_path)
    except OrphanManualEvidenceError as exc:
        raise IncrementalDeadLetterCampaignApplyError(exc.code) from exc
    clear_settings_cache()
    settings = load_settings()
    legacy_tool._assert_apply_runtime_safe(settings, db_path)
    env_after = read_stable_file_fingerprint(
        env_path,
        expected_sha256=env_document.sha256,
    )
    if _stable_identity(env_document) != _stable_identity(env_after):
        raise IncrementalDeadLetterCampaignApplyError("TRADING_ENV_FILE_CHANGED_DURING_LOAD")
    return settings, env_document


def _apply_under_lock(
    *,
    db_path: Path,
    files_before: Mapping[str, Any],
    preflight_main_matches: bool,
    inputs: Mapping[str, Any],
    target: Mapping[str, Any],
    binding: Mapping[str, Any],
    alias: str,
    ordinal: int,
    request_id: str,
    operator_id: str,
    evidence_ref: str,
    settings: Any,
    env_document: Any,
    out_dir: Path,
    observed_at: datetime,
) -> dict[str, Any]:
    connection = legacy_tool._open_existing_read_write(db_path)
    applied: Mapping[str, Any] | None = None
    existing_replay = False
    commit_error: Exception | None = None
    checkpoint_error: Exception | None = None
    write_violations: list[str] = []
    campaign_before: Mapping[str, Any] = {}
    campaign_after_tx: Mapping[str, Any] = {}
    protected_before: Mapping[str, Any] = {}
    protected_after_tx: Mapping[str, Any] = {}
    ledger_count_before = 0
    ledger_count_after = 0
    try:
        connection.execute("BEGIN EXCLUSIVE")
        _assert_wal_quiescent_under_lock(db_path)
        if [str(row[1]) for row in connection.execute("PRAGMA database_list")] != ["main"]:
            raise IncrementalDeadLetterCampaignApplyError("ATTACHED_DATABASE_NOT_ALLOWED")
        schema_row = connection.execute(
            "SELECT value FROM app_metadata WHERE key='schema_version'"
        ).fetchall()
        if len(schema_row) != 1 or str(schema_row[0][0]) != "63":
            raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_SCHEMA_63_REQUIRED")
        runtime_lock_count = int(
            connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0]
        )
        if runtime_lock_count:
            raise IncrementalDeadLetterCampaignApplyError("RUNTIME_EXECUTION_LOCK_PRESENT")
        env_path = Path(os.environ[ENV_FILE_PATH_ENV]).expanduser()
        env_after = read_stable_file_fingerprint(
            env_path,
            expected_sha256=env_document.sha256,
        )
        if _stable_identity(env_after) != _stable_identity(env_document):
            raise IncrementalDeadLetterCampaignApplyError("TRADING_ENV_FILE_CHANGED_DURING_APPLY")
        repeated_inputs = _load_inputs(
            blocker_plan_report=_mapping(inputs["paths"])["plan"],
            expected_blocker_plan_report_sha256=str(_mapping(inputs["expected_hashes"])["plan"]),
            preflight_report=_mapping(inputs["paths"])["preflight"],
            expected_preflight_report_sha256=str(_mapping(inputs["expected_hashes"])["preflight"]),
            private_target_manifest=_mapping(inputs["paths"])["manifest"],
            expected_private_target_manifest_sha256=str(
                _mapping(inputs["expected_hashes"])["manifest"]
            ),
            evidence_file=_mapping(inputs["paths"])["evidence"],
            expected_evidence_file_sha256=str(_mapping(inputs["expected_hashes"])["evidence"]),
            alias=alias,
            not_after=observed_at,
        )
        if repeated_inputs["identities"] != inputs["identities"]:
            raise IncrementalDeadLetterCampaignApplyError(
                "CAMPAIGN_PRIVATE_INPUT_CHANGED_DURING_APPLY"
            )
        approved_main = _mapping(_mapping(inputs["preflight"])["database_main"])
        current_main = _main_database_fingerprint(db_path)
        existing = connection.execute(
            f"SELECT * FROM {DISPOSITION_TABLE} WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        existing_replay = existing is not None
        if not existing_replay and (
            not preflight_main_matches
            or not fast0_tool._fingerprints_exact(approved_main, current_main)
        ):
            raise IncrementalDeadLetterCampaignApplyError(
                "CAMPAIGN_PREFLIGHT_DATABASE_MAIN_FINGERPRINT_MISMATCH"
            )
        expected_completed_before = ordinal if existing_replay else ordinal - 1
        campaign_before = preflight_tool._campaign_snapshot(
            connection,
            manifest=inputs["manifest"],
            expected_completed=expected_completed_before,
            source_plan_report_sha256=str(_mapping(inputs["plan"])["report_sha256"]),
            collected_at=observed_at,
        )
        protected_before = preflight_tool._protected_database_digest(connection)
        if protected_before != _mapping(_mapping(inputs["preflight"])["protected_database"]):
            raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_PROTECTED_DATABASE_DRIFT")
        if campaign_before.get("progress_valid") is not True:
            raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_PROGRESS_INVALID_BEFORE_APPLY")
        normalized_binding = validate_campaign_apply_evidence_binding(
            binding,
            expected_alias=alias,
            dead_letter_id=str(target["dead_letter_id"]),
            expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
            expected_candidate_version=str(target["candidate_version"]),
            evidence_sha256=str(inputs["evidence_sha256"]),
            not_after=observed_at,
        )
        context = _issue_incremental_campaign_apply_approval_context(
            normalized_binding,
            verified_preflight_report_sha256=str(inputs["preflight_report_sha256"]),
            verified_database_main_sha256=str(approved_main["sha256"]),
            expected_alias=alias,
            dead_letter_id=str(target["dead_letter_id"]),
            expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
            expected_candidate_version=str(target["candidate_version"]),
            evidence_sha256=str(inputs["evidence_sha256"]),
        )
        ledger_count_before = int(
            connection.execute(f"SELECT COUNT(*) FROM {DISPOSITION_TABLE}").fetchone()[0]
        )
        if existing_replay:
            assert existing is not None
            _validate_existing_replay(
                existing,
                target=target,
                binding=normalized_binding,
                request_id=request_id,
                operator_id=operator_id,
                evidence_ref=evidence_ref,
            )
            applied = {
                "disposition_id": str(existing["disposition_id"]),
                "sequence_no": int(existing["sequence_no"]),
                "idempotent_replay": True,
            }
        else:
            _install_write_authorizer(connection, violations=write_violations)
            try:
                applied = append_incremental_dead_letter_campaign_disposition_in_transaction(
                    connection,
                    dead_letter_id=str(target["dead_letter_id"]),
                    request_id=request_id,
                    expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
                    expected_candidate_version=str(target["candidate_version"]),
                    operator_id=operator_id,
                    evidence_ref=evidence_ref,
                    evidence_sha256=str(inputs["evidence_sha256"]),
                    evidence_json=normalized_binding,
                    safety_snapshot=_runtime_safety_snapshot(settings, env_document),
                    campaign_approval_context=context,
                )
            finally:
                connection.set_authorizer(None)
            if write_violations:
                raise IncrementalDeadLetterCampaignApplyError("UNAUTHORIZED_DATABASE_WRITE_BLOCKED")
        campaign_after_tx = preflight_tool._campaign_snapshot(
            connection,
            manifest=inputs["manifest"],
            expected_completed=ordinal,
            source_plan_report_sha256=str(_mapping(inputs["plan"])["report_sha256"]),
            collected_at=observed_at,
        )
        protected_after_tx = preflight_tool._protected_database_digest(connection)
        ledger_count_after = int(
            connection.execute(f"SELECT COUNT(*) FROM {DISPOSITION_TABLE}").fetchone()[0]
        )
        expected_delta = 0 if existing_replay else 1
        if not (
            campaign_after_tx.get("progress_valid") is True
            and protected_after_tx == protected_before
            and ledger_count_after - ledger_count_before == expected_delta
        ):
            raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_TRANSACTION_INVARIANT_FAILED")
        if existing_replay:
            connection.rollback()
        else:
            try:
                _commit_connection(connection)
            except Exception as exc:  # fault-injection/reconciliation boundary
                commit_error = exc
                if connection.in_transaction:
                    connection.rollback()
            if commit_error is None:
                try:
                    _checkpoint_wal(connection)
                except Exception as exc:
                    checkpoint_error = exc
    except Exception:
        connection.set_authorizer(None)
        if connection.in_transaction:
            connection.rollback()
        connection.close()
        raise
    finally:
        if connection:
            try:
                connection.close()
            except sqlite3.Error:
                pass
    assert applied is not None

    commit_reconciliation = "NOT_REQUIRED"
    if commit_error is not None:
        commit_reconciliation = _reconcile_commit(
            db_path,
            request_id=request_id,
            disposition_id=str(applied["disposition_id"]),
            binding=binding,
        )
        if commit_reconciliation == "NOT_COMMITTED":
            raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_COMMIT_NOT_DURABLE")
        if commit_reconciliation != "COMMITTED":
            return _outcome_unknown_report(
                alias=alias,
                ordinal=ordinal,
                request_id=request_id,
                disposition_id=str(applied["disposition_id"]),
                binding=binding,
                files_before=files_before,
                out_dir=out_dir,
                error_type=type(commit_error).__name__,
            )
        try:
            _checkpoint_committed_database(db_path)
        except Exception as exc:
            checkpoint_error = exc

    return _postverify_and_report(
        db_path=db_path,
        files_before=files_before,
        inputs=inputs,
        binding=binding,
        alias=alias,
        ordinal=ordinal,
        request_id=request_id,
        disposition_id=str(applied["disposition_id"]),
        sequence_no=int(applied["sequence_no"]),
        replay=existing_replay,
        campaign_before=campaign_before,
        campaign_after_tx=campaign_after_tx,
        protected_before=protected_before,
        protected_after_tx=protected_after_tx,
        ledger_count_before=ledger_count_before,
        ledger_count_after=ledger_count_after,
        write_violations=write_violations,
        commit_reconciliation=commit_reconciliation,
        initial_post_failures=(
            [] if checkpoint_error is None else ["CAMPAIGN_WAL_CHECKPOINT_FAILED"]
        ),
        initial_error_type=(None if checkpoint_error is None else type(checkpoint_error).__name__),
        out_dir=out_dir,
        observed_at=observed_at,
    )


def _postverify_and_report(
    *,
    db_path: Path,
    files_before: Mapping[str, Any],
    inputs: Mapping[str, Any],
    binding: Mapping[str, Any],
    alias: str,
    ordinal: int,
    request_id: str,
    disposition_id: str,
    sequence_no: int,
    replay: bool,
    campaign_before: Mapping[str, Any],
    campaign_after_tx: Mapping[str, Any],
    protected_before: Mapping[str, Any],
    protected_after_tx: Mapping[str, Any],
    ledger_count_before: int,
    ledger_count_after: int,
    write_violations: Sequence[str],
    commit_reconciliation: str,
    initial_post_failures: Sequence[str],
    initial_error_type: str | None,
    out_dir: Path,
    observed_at: datetime,
) -> dict[str, Any]:
    post_failures: list[str] = list(initial_post_failures)
    post_error_type: str | None = initial_error_type
    files_after: Mapping[str, Any] = {"status": "UNAVAILABLE"}
    campaign_post: Mapping[str, Any] = {}
    protected_post: Mapping[str, Any] = {}
    try:
        _assert_no_sidecars(db_path)
        files_after = fast0_tool._file_fingerprints(db_path)
        connection = fast0_tool._open_strict_read_only(db_path)
        connection.execute("BEGIN DEFERRED")
        try:
            quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")]
            campaign_post = preflight_tool._campaign_snapshot(
                connection,
                manifest=inputs["manifest"],
                expected_completed=ordinal,
                source_plan_report_sha256=str(_mapping(inputs["plan"])["report_sha256"]),
                collected_at=observed_at,
            )
            protected_post = preflight_tool._protected_database_digest(connection)
            stored = connection.execute(
                f"SELECT * FROM {DISPOSITION_TABLE} WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if stored is None or str(stored["disposition_id"]) != disposition_id:
                post_failures.append("CAMPAIGN_DISPOSITION_POSTVERIFY_MISSING")
        finally:
            connection.rollback()
            connection.close()
        if quick_check != ["ok"]:
            post_failures.append("DATABASE_QUICK_CHECK_FAILED")
        if campaign_post != campaign_after_tx:
            post_failures.append("CAMPAIGN_POSTVERIFY_STATE_MISMATCH")
        if protected_post != protected_before or protected_post != protected_after_tx:
            post_failures.append("PROTECTED_DATABASE_POSTVERIFY_MISMATCH")
    except Exception as exc:
        post_failures.append("CAMPAIGN_POSTVERIFY_UNAVAILABLE")
        post_error_type = type(exc).__name__

    request_id_sha256 = _text_sha256(request_id)
    disposition_id_sha256 = _text_sha256(disposition_id)
    chain_sha256 = campaign_chain_sha256(
        source_plan_report_sha256=str(_mapping(inputs["plan"])["report_sha256"]),
        target_set_sha256=str(_mapping(inputs["manifest"])["target_set_sha256"]),
        alias=alias,
        predecessor_chain_sha256=(
            None if alias == "U01" else str(binding["predecessor_chain_sha256"])
        ),
        approval_binding_sha256=str(binding["approval_binding_sha256"]),
        request_id_sha256=request_id_sha256,
        disposition_id_sha256=disposition_id_sha256,
    )
    result_status = "REPLAYED" if replay else "APPLIED"
    verdict_status = "REPLAY_VERIFIED" if replay else "VERIFIED"
    if post_failures:
        verdict_status = "COMMITTED_POSTCHECK_FAILED"
    report: dict[str, Any] = {
        "contract": APPLY_CONTRACT,
        "generated_at": _wire(observed_at),
        "mode": "APPLY",
        "campaign": {
            "source_plan_report_sha256": _mapping(inputs["plan"])["report_sha256"],
            "private_manifest_sha256": _mapping(inputs["manifest"])["manifest_file_sha256"],
            "target_set_sha256": _mapping(inputs["manifest"])["target_set_sha256"],
            "alias": alias,
            "ordinal": ordinal,
            "action": CAMPAIGN_ACTION,
            "approved_preflight_report_sha256": inputs["preflight_report_sha256"],
            "approved_database_main_sha256": binding["approved_database_main_sha256"],
            "approved_database_main_size": binding["approved_database_main_size"],
            "approval_binding_sha256": binding["approval_binding_sha256"],
            "predecessor_kind": binding["predecessor_kind"],
            "predecessor_report_sha256": binding["predecessor_report_sha256"],
            "predecessor_chain_sha256": binding["predecessor_chain_sha256"],
            "campaign_chain_sha256": chain_sha256,
            "content_embedded": False,
            "raw_identifier_mapping_recorded": False,
        },
        "database": {
            "schema_version": "63",
            "files_before": files_before,
            "files_after": files_after,
            "sidecars_absent_after": not post_failures,
        },
        "result": {
            "action": CAMPAIGN_ACTION,
            "status": result_status,
            "sequence_no": sequence_no,
            "request_id_sha256": request_id_sha256,
            "disposition_id_sha256": disposition_id_sha256,
        },
        "evidence": {
            "file_sha256": inputs["evidence_sha256"],
            "file_size": inputs["evidence_size"],
            "content_embedded": False,
            "path_recorded": False,
        },
        "progress": {
            "completed_count_before": campaign_before.get("completed_count"),
            "completed_count_after": campaign_after_tx.get("completed_count"),
            "raw_dead_letter_count_before": campaign_before.get("raw_dead_letter_count"),
            "raw_dead_letter_count_after": campaign_after_tx.get("raw_dead_letter_count"),
            "effective_dead_letter_count_before": campaign_before.get(
                "effective_dead_letter_count"
            ),
            "effective_dead_letter_count_after": campaign_after_tx.get(
                "effective_dead_letter_count"
            ),
            "historical_pending_count_before": campaign_before.get(
                "historical_pending_disposition_count"
            ),
            "historical_pending_count_after": campaign_after_tx.get(
                "historical_pending_disposition_count"
            ),
            "historical_disposed_count_before": campaign_before.get(
                "historical_disposed_dead_letter_count"
            ),
            "historical_disposed_count_after": campaign_after_tx.get(
                "historical_disposed_dead_letter_count"
            ),
        },
        "invariants": {
            "raw_inventory_unchanged": protected_before == protected_after_tx,
            "protected_database_unchanged": protected_before == protected_after_tx,
            "protected_database_sha256": protected_before.get("sha256"),
            "single_disposition_delta_valid": (
                ledger_count_after - ledger_count_before == (0 if replay else 1)
            ),
            "target_effective_transition_valid": (
                campaign_after_tx.get("completed_count") == ordinal
            ),
            "write_authorizer_violations": list(write_violations),
            "commit_reconciliation": commit_reconciliation,
        },
        "append_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
        "verdict": {
            "status": verdict_status,
            "failures": post_failures,
            "committed": True,
            "evidence_written": True,
            "operator_action_required": bool(post_failures),
            "error_type": post_error_type,
        },
    }
    preflight_tool._assert_public_report(report)
    try:
        _write_report(report, out_dir=out_dir)
    except (OSError, TypeError, ValueError) as exc:
        report["verdict"] = {
            "status": "COMMITTED_EVIDENCE_WRITE_FAILED",
            "failures": [*post_failures, "EVIDENCE_WRITE_FAILED"],
            "committed": True,
            "evidence_written": False,
            "operator_action_required": True,
            "error_type": type(exc).__name__,
        }
    return report


def _outcome_unknown_report(
    *,
    alias: str,
    ordinal: int,
    request_id: str,
    disposition_id: str,
    binding: Mapping[str, Any],
    files_before: Mapping[str, Any],
    out_dir: Path,
    error_type: str,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "contract": APPLY_CONTRACT,
        "generated_at": _wire(datetime.now(UTC)),
        "mode": "APPLY",
        "campaign": {
            "alias": alias,
            "ordinal": ordinal,
            "action": CAMPAIGN_ACTION,
            "approval_binding_sha256": binding["approval_binding_sha256"],
        },
        "database": {"schema_version": "63", "files_before": files_before},
        "result": {
            "action": CAMPAIGN_ACTION,
            "status": "OUTCOME_UNKNOWN",
            "request_id_sha256": _text_sha256(request_id),
            "disposition_id_sha256": _text_sha256(disposition_id),
        },
        "append_only": True,
        "observe_only": True,
        "identifiers_recorded": False,
        "raw_rows_recorded": False,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_broker_calls": True,
        "live_sim_allowed": False,
        "live_real_allowed": False,
        "verdict": {
            "status": "OUTCOME_UNKNOWN",
            "failures": ["CAMPAIGN_COMMIT_OUTCOME_UNKNOWN"],
            "committed": None,
            "evidence_written": True,
            "operator_action_required": True,
            "error_type": error_type,
        },
    }
    preflight_tool._assert_public_report(report)
    try:
        _write_report(report, out_dir=out_dir)
    except (OSError, TypeError, ValueError):
        report["verdict"]["evidence_written"] = False
        report["verdict"]["failures"].append("EVIDENCE_WRITE_FAILED")
    return report


def _install_write_authorizer(
    connection: sqlite3.Connection,
    *,
    violations: list[str],
) -> None:
    schema_actions = {
        int(value)
        for name in (
            "SQLITE_CREATE_INDEX",
            "SQLITE_CREATE_TABLE",
            "SQLITE_CREATE_TRIGGER",
            "SQLITE_CREATE_VIEW",
            "SQLITE_DROP_INDEX",
            "SQLITE_DROP_TABLE",
            "SQLITE_DROP_TRIGGER",
            "SQLITE_DROP_VIEW",
            "SQLITE_ALTER_TABLE",
            "SQLITE_REINDEX",
            "SQLITE_ANALYZE",
            "SQLITE_ATTACH",
            "SQLITE_DETACH",
        )
        if (value := getattr(sqlite3, name, None)) is not None
    }

    def authorizer(
        action_code: int,
        arg1: str | None,
        _arg2: str | None,
        database: str | None,
        _trigger: str | None,
    ) -> int:
        table = str(arg1 or "")
        if action_code in _SQLITE_WRITE_ACTIONS:
            if (
                action_code == sqlite3.SQLITE_INSERT
                and table == DISPOSITION_TABLE
                and database == "main"
            ):
                return sqlite3.SQLITE_OK
            violations.append("UNAUTHORIZED_DATABASE_WRITE")
            return sqlite3.SQLITE_DENY
        if action_code in schema_actions:
            violations.append("UNAUTHORIZED_SCHEMA_WRITE")
            return sqlite3.SQLITE_DENY
        if action_code == sqlite3.SQLITE_TRANSACTION:
            violations.append("UNAUTHORIZED_TRANSACTION_CONTROL")
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorizer)


def _validate_existing_replay(
    row: sqlite3.Row,
    *,
    target: Mapping[str, Any],
    binding: Mapping[str, Any],
    request_id: str,
    operator_id: str,
    evidence_ref: str,
) -> None:
    try:
        evidence = json.loads(str(row["evidence_json"] or "{}"))
    except (TypeError, ValueError) as exc:
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_REPLAY_EVIDENCE_INVALID") from exc
    valid = bool(
        isinstance(evidence, dict)
        and canonical_json(evidence) == canonical_json(binding)
        and str(row["request_id"]) == request_id
        and str(row["dead_letter_id"]) == target["dead_letter_id"]
        and str(row["action"]) == CAMPAIGN_ACTION
        and str(row["reason_code"]) == CAMPAIGN_REASON_CODE
        and str(row["operator_id"]) == operator_id
        and str(row["evidence_ref"]) == evidence_ref
        and str(row["evidence_sha256"]) == binding["evidence_sha256"]
        and str(row["expected_dead_letter_fingerprint"]) == target["dead_letter_fingerprint"]
        and str(row["expected_candidate_version"]) == target["candidate_version"]
        and int(row["sequence_no"]) == 1
        and row["supersedes_disposition_id"] is None
    )
    if not valid:
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_REPLAY_REQUEST_CONFLICT")


def _reconcile_commit(
    path: Path,
    *,
    request_id: str,
    disposition_id: str,
    binding: Mapping[str, Any],
) -> str:
    connection: sqlite3.Connection | None = None
    try:
        connection = legacy_tool._open_live_read_only(path)
        row = connection.execute(
            f"SELECT disposition_id, evidence_json FROM {DISPOSITION_TABLE} WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            return "NOT_COMMITTED"
        evidence = json.loads(str(row["evidence_json"] or "{}"))
        if (
            str(row["disposition_id"]) == disposition_id
            and isinstance(evidence, dict)
            and canonical_json(evidence) == canonical_json(binding)
        ):
            return "COMMITTED"
        return "UNKNOWN"
    except (OSError, sqlite3.Error, TypeError, ValueError):
        return "UNKNOWN"
    finally:
        if connection is not None:
            connection.close()


def _checkpoint_committed_database(path: Path) -> None:
    connection = legacy_tool._open_existing_read_write(path)
    try:
        _checkpoint_wal(connection)
    finally:
        connection.close()


def _checkpoint_wal(connection: sqlite3.Connection) -> None:
    journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    if journal_mode != "wal":
        return
    result = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if result is None or int(result[0]) != 0 or int(result[1]) != int(result[2]):
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_WAL_CHECKPOINT_FAILED")


def _assert_no_sidecars(path: Path) -> None:
    if any(Path(f"{path}{suffix}").exists() for suffix in ("-wal", "-shm", "-journal")):
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_QUIESCENT_DATABASE_REQUIRED")


def _assert_wal_quiescent_under_lock(path: Path) -> None:
    journal = Path(f"{path}-journal")
    wal = Path(f"{path}-wal")
    shm = Path(f"{path}-shm")
    if journal.exists():
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_ROLLBACK_JOURNAL_PRESENT")
    try:
        wal_exists = wal.exists()
        wal_size = int(wal.stat().st_size) if wal_exists else 0
        shm_exists = shm.exists()
    except OSError as exc:
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_WAL_STATE_UNAVAILABLE") from exc
    if wal_size:
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_COMMITTED_WAL_FRAMES_PRESENT")
    if shm_exists and not wal_exists:
        raise IncrementalDeadLetterCampaignApplyError("CAMPAIGN_WAL_STATE_INCONSISTENT")


def _main_database_fingerprint(path: Path) -> dict[str, Any]:
    before = path.stat()
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        handle_before = os.fstat(stream.fileno())
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
        handle_after = os.fstat(stream.fileno())
    after = path.stat()
    identities = {
        (
            int(value.st_dev),
            int(value.st_ino),
            int(value.st_size),
            int(value.st_mtime_ns),
        )
        for value in (before, handle_before, handle_after, after)
    }
    if len(identities) != 1 or size != int(after.st_size):
        raise IncrementalDeadLetterCampaignApplyError("DATABASE_MAIN_CHANGED_DURING_FINGERPRINT")
    return {
        "exists": True,
        "size": size,
        "mtime_ns": int(after.st_mtime_ns),
        "sha256": digest.hexdigest(),
    }


def _runtime_safety_snapshot(settings: Any, env_document: Any) -> dict[str, Any]:
    summary = legacy_tool._runtime_safety_summary(settings)
    return {
        **summary,
        "explicit_env_file_sha256": env_document.sha256,
        "theme_refresh_queue_market_scan_commands": False,
        "not_order_intent": True,
        "order_commands_allowed": False,
    }


def _write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    preflight_tool._assert_public_report(report)
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    raw_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    verdict = _mapping(report.get("verdict"))
    campaign = _mapping(report.get("campaign"))
    summary_path.write_text(
        "\n".join(
            [
                "# FAST-0R9 incremental dead-letter campaign apply",
                "",
                f"- Alias: `{campaign.get('alias')}`",
                f"- Status: `{verdict.get('status')}`",
                f"- Committed: `{verdict.get('committed')}`",
                f"- Evidence written: `{verdict.get('evidence_written')}`",
                "- Raw identifiers recorded: `false`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {"raw": raw_path, "summary": summary_path}


def _commit_connection(connection: sqlite3.Connection) -> None:
    connection.commit()


def _safe_label(name: str, value: str) -> str:
    normalized = str(value).strip()
    if _SAFE_LABEL_RE.fullmatch(normalized) is None:
        raise IncrementalDeadLetterCampaignApplyError(f"{name}_INVALID")
    return normalized


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


def plan_tool_fingerprint_valid(value: Mapping[str, Any]) -> bool:
    return preflight_tool.plan_tool._fingerprint_valid(value)


if __name__ == "__main__":
    raise SystemExit(main())
