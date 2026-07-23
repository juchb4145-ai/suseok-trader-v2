from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.config import (  # noqa: E402
    DEFAULT_ENV_FILE_PATH,
    ENV_FILE_PATH_ENV,
    Settings,
    TradingMode,
    TradingProfile,
    clear_settings_cache,
    load_settings,
)
from services.pipeline_coherency_disposition import (  # noqa: E402
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    ACTION_DISPOSE_STALE_OTHER_DATE,
    ACTION_REVOKE,
    DISPOSITION_TABLE,
    PipelineCoherencyDispositionError,
    _issue_orphan_apply_approval_context,
    is_pipeline_coherency_disposition_schema_ready,
    preview_pipeline_coherency_disposition,
    record_pipeline_coherency_disposition,
    revoke_pipeline_coherency_disposition,
)
from services.pipeline_orphan_campaign import (  # noqa: E402
    orphan_campaign_chain_sha256,
)
from services.pipeline_orphan_manual_evidence import (  # noqa: E402
    ORPHAN_CLASSIFICATION,
    OrphanManualEvidenceError,
    build_orphan_apply_evidence_binding,
    read_stable_file_fingerprint,
    read_stable_json_document,
    validate_orphan_apply_evidence_binding,
    validate_orphan_evidence_binding,
    validate_orphan_manual_evidence_document,
    validate_private_target_manifest,
)
from tools import ops_pipeline_orphan_evidence_preflight as orphan_preflight_tool  # noqa: E402

ACTIONS = (
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    ACTION_DISPOSE_STALE_OTHER_DATE,
    ACTION_REVOKE,
)
APPLY_ACK = "APPLY_APPEND_ONLY_PIPELINE_DISPOSITION"
_FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off"})
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@/-]{2,199}$")
_SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"(?i)(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}"),
    re.compile(r"(?i)github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}=*"),
    re.compile(r"(?i)(?:token|password|secret|api[_-]?key|account)\s*[:=]\s*\S+"),
    re.compile(r"(?i)(?:acct|account|계좌)[_.:@/-]?\d{6,16}"),
    re.compile(r"(?<![A-Za-z0-9])\d{8,16}(?![A-Za-z0-9])"),
)
_HYPHENATED_ACCOUNT_PATTERN = re.compile(r"(?<!\d)\d{3,6}-\d{2,6}(?:-\d{1,6})?(?!\d)")
_ISO_DATE_PATTERN = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)")
_PRODUCER_SETTING_NAMES = (
    "realtime_subscription_queue_commands",
    "dry_run_oms_enabled",
    "dry_run_intent_creation_enabled",
    "dry_run_simulated_fill_enabled",
    "dry_run_order_routing_enabled",
    "dry_run_gateway_command_enabled",
    "dry_run_exit_engine_enabled",
    "dry_run_exit_intent_creation_enabled",
    "dry_run_exit_order_creation_enabled",
    "dry_run_exit_simulated_fill_enabled",
    "dry_run_exit_order_routing_enabled",
    "dry_run_exit_gateway_command_enabled",
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
_ORDER_ARTIFACT_TABLES = (
    "order_plan_drafts",
    "order_plan_drafts_latest",
    "entry_timing_evaluations",
    "dry_run_intents",
    "dry_run_orders",
    "dry_run_executions",
    "dry_run_intent_rejections",
    "dry_run_exit_intents",
    "dry_run_exit_orders",
    "dry_run_exit_executions",
    "live_sim_intents",
    "live_sim_orders",
    "live_sim_executions",
    "live_sim_rejections",
    "live_sim_exit_intents",
    "live_sim_cancel_intents",
    "gateway_commands",
    "gateway_order_broker_boundaries",
    "gateway_order_broker_boundary_resolutions",
)


class PipelineDispositionCliError(RuntimeError):
    def __init__(self, *reason_codes: str) -> None:
        self.reason_codes = list(dict.fromkeys(str(code) for code in reason_codes if str(code)))
        super().__init__(", ".join(self.reason_codes) or "PIPELINE_DISPOSITION_CLI_ERROR")


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    try:
        if args.apply:
            report = apply_disposition(
                db_path=Path(args.db),
                trade_date=args.trade_date,
                candidate_instance_id=args.candidate_instance_id,
                action=args.action,
                request_id=args.request_id,
                expected_pipeline_fingerprint=args.expected_pipeline_fingerprint,
                expected_subject_version=args.expected_subject_version,
                expected_source_fingerprint=args.expected_source_fingerprint,
                expected_candidate_fingerprint=args.expected_candidate_fingerprint,
                expected_downstream_fingerprint=args.expected_downstream_fingerprint,
                expected_boundary_fingerprint=args.expected_boundary_fingerprint,
                reason_code=args.reason_code,
                operator_id=args.operator_id,
                evidence_type=args.evidence_type,
                evidence_ref=args.evidence_ref,
                evidence_file=Path(args.evidence_file) if args.evidence_file else None,
                acknowledge=args.acknowledge,
                out_dir=Path(args.out_dir),
                expected_evidence_sha256=args.expected_evidence_sha256,
                expected_evidence_preview_sha256=args.expected_evidence_preview_sha256,
                evidence_target_set_sha256=args.evidence_target_set_sha256,
                evidence_alias=args.evidence_alias,
                orphan_preflight_report=(
                    Path(args.orphan_preflight_report) if args.orphan_preflight_report else None
                ),
                expected_orphan_preflight_report_sha256=(
                    args.expected_orphan_preflight_report_sha256
                ),
                private_target_manifest=(
                    Path(args.private_target_manifest) if args.private_target_manifest else None
                ),
                expected_private_target_manifest_sha256=(
                    args.expected_private_target_manifest_sha256
                ),
                authoritative_artifact=(
                    Path(args.authoritative_artifact) if args.authoritative_artifact else None
                ),
                expected_authoritative_artifact_sha256=(
                    args.expected_authoritative_artifact_sha256
                ),
            )
        else:
            report = preview_disposition(
                db_path=Path(args.db),
                trade_date=args.trade_date,
                candidate_instance_id=args.candidate_instance_id,
                action=args.action,
                out_dir=Path(args.out_dir),
            )
    except (
        OSError,
        sqlite3.Error,
        ValueError,
        PipelineCoherencyDispositionError,
        PipelineDispositionCliError,
    ) as exc:
        if isinstance(exc, PipelineCoherencyDispositionError):
            detail = exc.code
        elif isinstance(exc, PipelineDispositionCliError):
            detail = ",".join(exc.reason_codes)
        else:
            detail = type(exc).__name__
        print(f"pipeline disposition: ERROR {detail}", file=sys.stderr)
        return 2
    print(render_console_summary(report))
    return 0 if report["verdict"]["status"] in {"ELIGIBLE", "APPLIED", "IDEMPOTENT"} else 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Strict read-only preview, or explicitly acknowledged append-only apply, "
            "for one FAST-0 pipeline disposition subject."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--candidate-instance-id", required=True)
    parser.add_argument("--action", required=True, choices=ACTIONS)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--request-id")
    parser.add_argument("--expected-pipeline-fingerprint")
    parser.add_argument("--expected-subject-version")
    parser.add_argument("--expected-source-fingerprint")
    parser.add_argument("--expected-candidate-fingerprint")
    parser.add_argument("--expected-downstream-fingerprint")
    parser.add_argument("--expected-boundary-fingerprint")
    parser.add_argument("--reason-code")
    parser.add_argument("--operator-id")
    parser.add_argument("--evidence-type")
    parser.add_argument("--evidence-ref")
    parser.add_argument("--evidence-file")
    parser.add_argument("--expected-evidence-sha256")
    parser.add_argument("--expected-evidence-preview-sha256")
    parser.add_argument("--evidence-target-set-sha256")
    parser.add_argument("--evidence-alias")
    parser.add_argument("--orphan-preflight-report")
    parser.add_argument("--expected-orphan-preflight-report-sha256")
    parser.add_argument("--private-target-manifest")
    parser.add_argument("--expected-private-target-manifest-sha256")
    parser.add_argument("--authoritative-artifact")
    parser.add_argument("--expected-authoritative-artifact-sha256")
    parser.add_argument("--acknowledge")
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "pipeline_coherency_dispositions"),
    )
    return parser


def preview_disposition(
    *,
    db_path: Path,
    trade_date: str,
    candidate_instance_id: str,
    action: str,
    out_dir: Path,
) -> dict[str, Any]:
    resolved_path = _validated_database_path(db_path)
    before = _file_state(resolved_path)
    connection = _open_strict_read_only(resolved_path)
    try:
        schema_version = _schema_version(connection)
        preview = preview_pipeline_coherency_disposition(
            connection,
            trade_date=trade_date,
            candidate_instance_id=candidate_instance_id,
            action=action,
        )
    finally:
        connection.close()
    after = _file_state(resolved_path)
    failures = [] if before == after else ["DATABASE_DATA_FILE_CHANGED"]
    if not preview.get("eligible"):
        failures.extend(str(code) for code in preview.get("reason_codes") or [])
    orphan_private_report = action == ACTION_DISPOSE_ORPHAN
    public_preview = _privacy_safe_orphan_preview(preview) if orphan_private_report else preview
    report: dict[str, Any] = {
        "contract": "fast0-pipeline-disposition-preview.v1",
        "generated_at": _now(),
        "mode": "PREVIEW",
        "database": {
            **(
                {"filename_sha256": hashlib.sha256(resolved_path.name.encode("utf-8")).hexdigest()}
                if orphan_private_report
                else {"filename": resolved_path.name}
            ),
            "schema_version": schema_version,
            "files_before": before,
            "files_after": after,
        },
        "preview": public_preview,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "verdict": {
            "status": "ELIGIBLE" if not failures else "BLOCKED",
            "failures": list(dict.fromkeys(failures)),
            "database_files_unchanged": before == after,
        },
    }
    if orphan_private_report:
        _assert_orphan_apply_report_private(
            report,
            forbidden_values=(
                candidate_instance_id,
                str(preview.get("candidate_instance_id") or ""),
                str(preview.get("subject_key") or ""),
            ),
        )
    report["report_paths"] = _write_report(report, out_dir=out_dir)
    return report


def _privacy_safe_orphan_preview(preview: Mapping[str, Any]) -> dict[str, Any]:
    candidate_instance_id = str(preview.get("candidate_instance_id") or "")
    subject_key = str(preview.get("subject_key") or "")
    downstream_raw = preview.get("downstream")
    source_raw = preview.get("source")
    candidate_raw = preview.get("candidate")
    effective_raw = preview.get("effective_disposition")
    downstream = dict(downstream_raw) if isinstance(downstream_raw, Mapping) else {}
    source = dict(source_raw) if isinstance(source_raw, Mapping) else {}
    candidate = dict(candidate_raw) if isinstance(candidate_raw, Mapping) else {}
    effective = dict(effective_raw) if isinstance(effective_raw, Mapping) else {}
    safe_effective: dict[str, Any] = {
        key: effective.get(key)
        for key in ("status", "effective", "cas_valid", "chain_valid", "action", "sequence_no")
        if key in effective
    }
    if effective:
        safe_effective["binding_sha256"] = _json_sha256(effective)
    return {
        "status": preview.get("status"),
        "eligible": preview.get("eligible") is True,
        "reason_codes": [str(code) for code in preview.get("reason_codes") or []],
        "action": preview.get("action"),
        "expected_action": preview.get("expected_action"),
        "trade_date": preview.get("trade_date"),
        "candidate_instance_id_sha256": hashlib.sha256(
            candidate_instance_id.encode("utf-8")
        ).hexdigest(),
        "subject_key_sha256": hashlib.sha256(subject_key.encode("utf-8")).hexdigest(),
        "classification": preview.get("classification"),
        "latest_plan_status": preview.get("latest_plan_status"),
        "latest_plan_unexpired": preview.get("latest_plan_unexpired"),
        **{
            key: preview.get(key)
            for key in (
                "pipeline_fingerprint",
                "subject_version",
                "source_fingerprint",
                "candidate_fingerprint",
                "downstream_fingerprint",
                "boundary_fingerprint",
            )
        },
        "downstream": {
            key: value
            for key, value in downstream.items()
            if key.endswith("_count") and type(value) is int
        },
        "source": {
            key: source.get(key)
            for key in (
                "row_count",
                "active_count",
                "historical_event_active_count",
                "latest_row_count",
                "latest_active_count",
                "candidate_active_source_count",
                "source_projection_consistent",
                "source_state_classification",
            )
        },
        "candidate": {
            key: candidate.get(key)
            for key in ("present", "state", "trade_date", "closed_at", "active_source_count")
        },
        "effective_disposition": safe_effective,
        "chain_valid": preview.get("chain_valid") is True,
        "next_sequence_no": preview.get("next_sequence_no"),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "auto_run_evaluation": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }


def apply_disposition(
    *,
    db_path: Path,
    trade_date: str,
    candidate_instance_id: str,
    action: str,
    request_id: str | None,
    expected_pipeline_fingerprint: str | None,
    expected_subject_version: str | None,
    expected_source_fingerprint: str | None,
    expected_candidate_fingerprint: str | None,
    expected_downstream_fingerprint: str | None,
    expected_boundary_fingerprint: str | None,
    reason_code: str | None,
    operator_id: str | None,
    evidence_type: str | None,
    evidence_ref: str | None,
    evidence_file: Path | None,
    acknowledge: str | None,
    out_dir: Path,
    expected_evidence_sha256: str | None = None,
    expected_evidence_preview_sha256: str | None = None,
    evidence_target_set_sha256: str | None = None,
    evidence_alias: str | None = None,
    orphan_preflight_report: Path | None = None,
    expected_orphan_preflight_report_sha256: str | None = None,
    private_target_manifest: Path | None = None,
    expected_private_target_manifest_sha256: str | None = None,
    authoritative_artifact: Path | None = None,
    expected_authoritative_artifact_sha256: str | None = None,
) -> dict[str, Any]:
    if acknowledge != APPLY_ACK:
        raise PipelineDispositionCliError("EXACT_APPLY_ACKNOWLEDGEMENT_REQUIRED")
    required_text = {
        "request_id": request_id,
        "expected_pipeline_fingerprint": expected_pipeline_fingerprint,
        "expected_subject_version": expected_subject_version,
        "expected_source_fingerprint": expected_source_fingerprint,
        "expected_candidate_fingerprint": expected_candidate_fingerprint,
        "expected_downstream_fingerprint": expected_downstream_fingerprint,
        "expected_boundary_fingerprint": expected_boundary_fingerprint,
        "reason_code": reason_code,
        "operator_id": operator_id,
        "evidence_type": evidence_type,
        "evidence_ref": evidence_ref,
    }
    missing = [key for key, value in required_text.items() if not str(value or "").strip()]
    if evidence_file is None:
        missing.append("evidence_file")
    orphan_evidence_arguments = {
        "expected_evidence_sha256": expected_evidence_sha256,
        "expected_evidence_preview_sha256": expected_evidence_preview_sha256,
        "evidence_target_set_sha256": evidence_target_set_sha256,
        "evidence_alias": evidence_alias,
        "orphan_preflight_report": orphan_preflight_report,
        "expected_orphan_preflight_report_sha256": (expected_orphan_preflight_report_sha256),
        "private_target_manifest": private_target_manifest,
        "expected_private_target_manifest_sha256": (expected_private_target_manifest_sha256),
        "authoritative_artifact": authoritative_artifact,
        "expected_authoritative_artifact_sha256": (expected_authoritative_artifact_sha256),
    }
    if action == ACTION_DISPOSE_ORPHAN:
        missing.extend(
            key for key, value in orphan_evidence_arguments.items() if not str(value or "").strip()
        )
    elif any(str(value or "").strip() for value in orphan_evidence_arguments.values()):
        raise PipelineDispositionCliError("ORPHAN_EVIDENCE_ARGUMENTS_REQUIRE_ORPHAN_ACTION")
    if missing:
        raise PipelineDispositionCliError(
            *(f"APPLY_ARGUMENT_REQUIRED:{key.upper()}" for key in missing)
        )
    normalized_request_id = _safe_label("request_id", request_id)
    normalized_reason = _safe_label("reason_code", reason_code)
    normalized_operator = _safe_label("operator_id", operator_id)
    normalized_evidence_type = _safe_label("evidence_type", evidence_type)
    normalized_evidence_ref = _safe_label("evidence_ref", evidence_ref)
    expected_hashes = {
        key: _sha256_value(key, value)
        for key, value in required_text.items()
        if key.startswith("expected_")
    }
    assert evidence_file is not None
    evidence_json: dict[str, Any]
    orphan_input_identity: dict[str, tuple[Any, ...]] | None = None
    orphan_preflight_contract: dict[str, Any] | None = None
    if action == ACTION_DISPOSE_ORPHAN:
        evidence_sha256 = _sha256_value("expected_evidence_sha256", expected_evidence_sha256)
        expected_evidence_preview_sha256 = _sha256_value(
            "expected_evidence_preview_sha256", expected_evidence_preview_sha256
        )
        evidence_target_set_sha256 = _sha256_value(
            "evidence_target_set_sha256", evidence_target_set_sha256
        )
        normalized_preflight_sha256 = _sha256_value(
            "expected_orphan_preflight_report_sha256",
            expected_orphan_preflight_report_sha256,
        )
        normalized_manifest_sha256 = _sha256_value(
            "expected_private_target_manifest_sha256",
            expected_private_target_manifest_sha256,
        )
        normalized_artifact_sha256 = _sha256_value(
            "expected_authoritative_artifact_sha256",
            expected_authoritative_artifact_sha256,
        )
        assert orphan_preflight_report is not None
        assert private_target_manifest is not None
        assert authoritative_artifact is not None
        (
            evidence_json,
            evidence_size,
            orphan_input_identity,
            orphan_preflight_contract,
        ) = _validated_orphan_evidence(
            evidence_file=evidence_file,
            expected_evidence_sha256=evidence_sha256,
            expected_evidence_preview_sha256=expected_evidence_preview_sha256,
            evidence_target_set_sha256=evidence_target_set_sha256,
            evidence_alias=str(evidence_alias or ""),
            orphan_preflight_report=orphan_preflight_report,
            expected_orphan_preflight_report_sha256=normalized_preflight_sha256,
            private_target_manifest=private_target_manifest,
            expected_private_target_manifest_sha256=normalized_manifest_sha256,
            authoritative_artifact=authoritative_artifact,
            expected_authoritative_artifact_sha256=normalized_artifact_sha256,
            target=_orphan_target(
                trade_date=trade_date,
                candidate_instance_id=candidate_instance_id,
                action=action,
                expected_hashes=expected_hashes,
            ),
        )
    else:
        evidence_sha256, evidence_size = _sha256_file(evidence_file)
        evidence_json = {
            "contract": "fast0-pipeline-disposition-evidence.v1",
            "file_sha256": evidence_sha256,
            "file_size": evidence_size,
            "content_embedded": False,
        }
    resolved_path = _validated_database_path(db_path)
    settings, env_path = _load_and_assert_runtime_safe(resolved_path)
    safety_snapshot = _runtime_safety_snapshot(settings, env_path=env_path)
    files_before = _file_state(resolved_path)
    main_before = _main_database_fingerprint(resolved_path)
    connection = _open_existing_read_write(resolved_path)
    violations: list[str] = []
    applied: dict[str, Any] | None = None
    commit_error: Exception | None = None
    close_error: Exception | None = None
    rollback_error: Exception | None = None
    orphan_approval_context = None
    verified_orphan_database_main_sha256: str | None = None
    try:
        _assert_quiescent_sidecars(resolved_path)
        connection.execute("BEGIN EXCLUSIVE")
        if action == ACTION_DISPOSE_ORPHAN:
            _assert_orphan_apply_wal_quiescent(resolved_path)
        _validated_database_path(resolved_path)
        if [str(row[1]) for row in connection.execute("PRAGMA database_list")] != ["main"]:
            raise PipelineDispositionCliError("ATTACHED_DATABASE_NOT_ALLOWED")
        if _schema_version(connection) != "63":
            raise PipelineDispositionCliError("PIPELINE_DISPOSITION_SCHEMA_63_REQUIRED")
        if not is_pipeline_coherency_disposition_schema_ready(connection):
            raise PipelineDispositionCliError("PIPELINE_DISPOSITION_SCHEMA_CONTRACT_INVALID")
        if _runtime_lease_count(connection):
            raise PipelineDispositionCliError("RUNTIME_EXECUTION_LEASE_PRESENT")
        if _sha256_file(env_path)[0] != safety_snapshot["explicit_env_file_sha256"]:
            raise PipelineDispositionCliError("TRADING_ENV_FILE_CHANGED_DURING_APPLY")
        existing = connection.execute(
            f"SELECT request_id FROM {DISPOSITION_TABLE} WHERE request_id = ?",
            (normalized_request_id,),
        ).fetchone()
        if action == ACTION_DISPOSE_ORPHAN and existing is None:
            assert orphan_preflight_contract is not None
            current_main = _main_database_fingerprint(resolved_path)
            if (
                current_main != main_before
                or current_main != orphan_preflight_contract["database_main"]
            ):
                raise PipelineDispositionCliError(
                    "ORPHAN_PREFLIGHT_DATABASE_MAIN_FINGERPRINT_MISMATCH"
                )
            verified_orphan_database_main_sha256 = str(current_main["sha256"])
        artifact_before = _artifact_digest(connection)
        ledger_before = _ledger_state(connection)
        if existing is None or action == ACTION_DISPOSE_ORPHAN:
            preview = preview_pipeline_coherency_disposition(
                connection,
                trade_date=trade_date,
                candidate_instance_id=candidate_instance_id,
                action=action,
            )
            if existing is None and not preview.get("eligible"):
                raise PipelineDispositionCliError(
                    *(str(code) for code in preview.get("reason_codes") or ["NOT_ELIGIBLE"])
                )
            for key, expected in expected_hashes.items():
                preview_key = key.removeprefix("expected_")
                if preview.get(preview_key) != expected:
                    raise PipelineDispositionCliError(
                        f"PIPELINE_DISPOSITION_CAS_MISMATCH:{key.upper()}"
                    )
            if action == ACTION_DISPOSE_ORPHAN:
                (
                    evidence_json,
                    second_size,
                    second_input_identity,
                    second_preflight_contract,
                ) = _validated_orphan_evidence(
                    evidence_file=evidence_file,
                    expected_evidence_sha256=evidence_sha256,
                    expected_evidence_preview_sha256=(expected_evidence_preview_sha256 or ""),
                    evidence_target_set_sha256=evidence_target_set_sha256 or "",
                    evidence_alias=str(evidence_alias or ""),
                    orphan_preflight_report=orphan_preflight_report,
                    expected_orphan_preflight_report_sha256=(normalized_preflight_sha256),
                    private_target_manifest=private_target_manifest,
                    expected_private_target_manifest_sha256=(normalized_manifest_sha256),
                    authoritative_artifact=authoritative_artifact,
                    expected_authoritative_artifact_sha256=(normalized_artifact_sha256),
                    target={
                        "trade_date": preview.get("trade_date"),
                        "candidate_instance_id": preview.get("candidate_instance_id"),
                        "classification": preview.get("classification"),
                        "action": preview.get("action"),
                        **{
                            key: preview.get(key)
                            for key in (
                                "pipeline_fingerprint",
                                "subject_version",
                                "source_fingerprint",
                                "candidate_fingerprint",
                                "downstream_fingerprint",
                                "boundary_fingerprint",
                            )
                        },
                    },
                )
                if second_size != evidence_size:
                    raise PipelineDispositionCliError("ORPHAN_EVIDENCE_SIZE_CHANGED_DURING_APPLY")
                if second_input_identity != orphan_input_identity:
                    raise PipelineDispositionCliError(
                        "ORPHAN_PRIVATE_INPUT_IDENTITY_CHANGED_DURING_APPLY"
                    )
                if second_preflight_contract != orphan_preflight_contract:
                    raise PipelineDispositionCliError(
                        "ORPHAN_PREFLIGHT_CONTRACT_CHANGED_DURING_APPLY"
                    )
                if existing is None:
                    assert verified_orphan_database_main_sha256 is not None
                    orphan_approval_context = _issue_orphan_apply_approval_context(
                        evidence_json,
                        verified_preflight_report_sha256=normalized_preflight_sha256,
                        verified_database_main_sha256=verified_orphan_database_main_sha256,
                    )
        _install_write_authorizer(connection, violations=violations)
        kwargs: dict[str, Any] = {
            "trade_date": trade_date,
            "candidate_instance_id": candidate_instance_id,
            "action": action,
            "request_id": normalized_request_id,
            **expected_hashes,
            "reason_code": normalized_reason,
            "operator_id": normalized_operator,
            "evidence_type": normalized_evidence_type,
            "evidence_ref": normalized_evidence_ref,
            "evidence_sha256": evidence_sha256,
            "evidence_json": evidence_json,
            "safety_snapshot": safety_snapshot,
            "orphan_approval_context": orphan_approval_context,
        }
        if action == ACTION_REVOKE:
            applied = revoke_pipeline_coherency_disposition(connection, **kwargs)
        else:
            applied = record_pipeline_coherency_disposition(connection, **kwargs)
        connection.set_authorizer(None)
        if violations:
            raise PipelineDispositionCliError(*violations)
        artifact_after = _artifact_digest(connection)
        ledger_after = _ledger_state(connection)
        if artifact_before != artifact_after:
            raise PipelineDispositionCliError("ORDER_ARTIFACT_CHANGED_DURING_APPLY")
        expected_delta = 0 if existing is not None else 1
        if ledger_after["row_count"] - ledger_before["row_count"] != expected_delta:
            raise PipelineDispositionCliError("PIPELINE_LEDGER_ROW_DELTA_INVALID")
        stored = connection.execute(
            f"SELECT * FROM {DISPOSITION_TABLE} WHERE request_id = ?",
            (normalized_request_id,),
        ).fetchone()
        if stored is None or applied.get("disposition_id") != stored["disposition_id"]:
            raise PipelineDispositionCliError("PIPELINE_DISPOSITION_WRITE_NOT_PROVEN")
        try:
            _commit_connection(connection)
        except Exception as exc:  # reconciliation below distinguishes durable commit
            commit_error = exc
            if connection.in_transaction:
                try:
                    connection.rollback()
                except Exception as rollback_exc:  # pragma: no cover - fault injection
                    rollback_error = rollback_exc
    except Exception:
        connection.set_authorizer(None)
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        try:
            _close_connection(connection)
        except Exception as exc:  # pragma: no cover - driver/OS fault injection
            close_error = exc
    assert applied is not None
    reconciliation = "NOT_REQUIRED"
    postcommit_warnings: list[str] = []
    commit_outcome_unknown = False
    if commit_error is not None:
        reconciliation = _reconcile_committed_disposition(
            resolved_path,
            request_id=normalized_request_id,
            applied=applied,
        )
        if reconciliation == "NOT_COMMITTED":
            raise commit_error
        if reconciliation == "COMMITTED":
            postcommit_warnings.append("COMMIT_RAISED_AFTER_DURABLE_WRITE")
        else:
            commit_outcome_unknown = True
    elif close_error is not None:
        reconciliation = "COMMITTED_BY_SUCCESSFUL_COMMIT"
    if rollback_error is not None:
        postcommit_warnings.append("ROLLBACK_RAISED_DURING_COMMIT_RECONCILIATION")
    if close_error is not None:
        if commit_outcome_unknown:
            postcommit_warnings.append("CLOSE_RAISED_WITH_COMMIT_OUTCOME_UNKNOWN")
        else:
            postcommit_warnings.append("CLOSE_RAISED_AFTER_DURABLE_WRITE")
    postcommit_failures: list[str] = []
    postcommit_error_type: str | None = None
    checkpoint_complete = False
    main_after: dict[str, Any] = {"status": "UNAVAILABLE"}
    files_after: dict[str, Any]
    try:
        if action == ACTION_DISPOSE_ORPHAN and not commit_outcome_unknown:
            _checkpoint_committed_database(resolved_path)
            _assert_quiescent_sidecars(resolved_path)
            checkpoint_complete = True
            main_after = _main_database_fingerprint(resolved_path)
        elif action != ACTION_DISPOSE_ORPHAN:
            main_after = _main_database_fingerprint(resolved_path)
        files_after = _file_state(resolved_path)
    except (OSError, sqlite3.Error, ValueError, PipelineDispositionCliError) as exc:
        files_after = {"status": "UNAVAILABLE"}
        postcommit_failures.append(
            "ORPHAN_POSTCOMMIT_CHECKPOINT_OR_FINGERPRINT_FAILED"
            if action == ACTION_DISPOSE_ORPHAN
            else "POSTCOMMIT_DATABASE_FILE_STATE_UNAVAILABLE"
        )
        postcommit_error_type = type(exc).__name__
    status = "IDEMPOTENT" if existing is not None else "APPLIED"
    committed: bool | None = True
    failures = [*postcommit_warnings, *postcommit_failures]
    if commit_outcome_unknown:
        status = "OUTCOME_UNKNOWN"
        committed = None
        failures.insert(0, "PIPELINE_DISPOSITION_COMMIT_OUTCOME_UNKNOWN")
    elif postcommit_warnings:
        status = f"{status}_RECONCILED_WITH_WARNING"
    if postcommit_failures and not commit_outcome_unknown:
        status = "COMMITTED_POSTCHECK_FAILED"
    orphan_private_report = action == ACTION_DISPOSE_ORPHAN
    safe_campaign: Mapping[str, Any] | None = None
    if orphan_private_report:
        assert orphan_preflight_contract is not None
        orphan_evidence_binding = dict(evidence_json["evidence_binding"])
        request_id_sha256 = hashlib.sha256(normalized_request_id.encode("utf-8")).hexdigest()
        disposition_id_sha256 = hashlib.sha256(
            str(applied.get("disposition_id") or "").encode("utf-8")
        ).hexdigest()
        safe_result: Mapping[str, Any] = {
            "action": ACTION_DISPOSE_ORPHAN,
            "alias": str(evidence_alias),
            "sequence_no": applied.get("sequence_no"),
            "request_id_sha256": request_id_sha256,
            "disposition_id_sha256": disposition_id_sha256,
            "evidence_preview_sha256": orphan_evidence_binding["evidence_preview_sha256"],
        }
        safe_evidence: Mapping[str, Any] = {
            "type_sha256": hashlib.sha256(normalized_evidence_type.encode("utf-8")).hexdigest(),
            "ref_sha256": hashlib.sha256(normalized_evidence_ref.encode("utf-8")).hexdigest(),
            "sha256": evidence_sha256,
            "size": evidence_size,
            "artifact_sha256": orphan_evidence_binding["artifact_sha256"],
            "artifact_size": orphan_evidence_binding["artifact_size"],
            "target_set_sha256": orphan_evidence_binding["target_set_sha256"],
            "alias": orphan_evidence_binding["alias"],
            "approval_binding_sha256": evidence_json["approval_binding_sha256"],
            "preflight_report_sha256": orphan_preflight_contract["report_sha256"],
            "source_plan_report_sha256": orphan_preflight_contract["source_plan_report_sha256"],
            "source_reconciliation_report_sha256": orphan_preflight_contract[
                "source_reconciliation_report_sha256"
            ],
            "content_embedded": False,
        }
        campaign_link = dict(evidence_json["campaign"])
        safe_campaign = {
            "source_plan_report_sha256": orphan_preflight_contract["source_plan_report_sha256"],
            "source_reconciliation_report_sha256": orphan_preflight_contract[
                "source_reconciliation_report_sha256"
            ],
            "target_set_sha256": orphan_evidence_binding["target_set_sha256"],
            "link": campaign_link,
            "chain_sha256": orphan_campaign_chain_sha256(
                source_plan_report_sha256=orphan_preflight_contract["source_plan_report_sha256"],
                source_reconciliation_report_sha256=orphan_preflight_contract[
                    "source_reconciliation_report_sha256"
                ],
                target_set_sha256=orphan_evidence_binding["target_set_sha256"],
                campaign_link=campaign_link,
                approval_binding_sha256=str(evidence_json["approval_binding_sha256"]),
                request_id_sha256=request_id_sha256,
                disposition_id_sha256=disposition_id_sha256,
            ),
            "identifiers_recorded": False,
        }
    else:
        safe_result = applied
        safe_evidence = {
            "type": normalized_evidence_type,
            "ref": normalized_evidence_ref,
            "sha256": evidence_sha256,
            "size": evidence_size,
            "content_embedded": False,
        }
    retry_required = bool(commit_outcome_unknown or postcommit_failures)
    report: dict[str, Any] = {
        "contract": "fast0-pipeline-disposition-apply.v1",
        "generated_at": _now(),
        "mode": "APPLY",
        **({"campaign": dict(safe_campaign)} if safe_campaign is not None else {}),
        "database": {
            **(
                {"filename_sha256": hashlib.sha256(resolved_path.name.encode("utf-8")).hexdigest()}
                if orphan_private_report
                else {"filename": resolved_path.name}
            ),
            "schema_version": "63",
            "files_before": files_before,
            "files_after": files_after,
            **(
                {
                    "main_before": main_before,
                    "main_after": main_after,
                    "checkpoint_complete": checkpoint_complete,
                }
                if orphan_private_report
                else {}
            ),
        },
        "result": dict(safe_result),
        "evidence": dict(safe_evidence),
        "safety": safety_snapshot,
        "invariants": {
            "order_artifact_digest_before": artifact_before,
            "order_artifact_digest_after": artifact_after,
            "order_artifacts_unchanged": artifact_before == artifact_after,
            "ledger_row_count_before": ledger_before["row_count"],
            "ledger_row_count_after": ledger_after["row_count"],
            "write_authorizer_violations": violations,
            "commit_reconciliation": reconciliation,
            "commit_error_type": (None if commit_error is None else type(commit_error).__name__),
            "rollback_error_type": (
                None if rollback_error is None else type(rollback_error).__name__
            ),
            "close_error_type": (None if close_error is None else type(close_error).__name__),
            "postcommit_warnings": postcommit_warnings,
        },
        "read_only": False,
        "append_only": True,
        "observe_only": True,
        "not_order_intent": True,
        "no_order_side_effects": True,
        "verdict": {
            "status": status,
            "failures": failures,
            "committed": committed,
            "evidence_written": True,
            "operator_action_required": retry_required,
            "retry_with_same_request_id": (
                normalized_request_id if retry_required and not orphan_private_report else None
            ),
            "retry_request_id_sha256": (
                hashlib.sha256(normalized_request_id.encode("utf-8")).hexdigest()
                if retry_required and orphan_private_report
                else None
            ),
            "error_type": (
                type(commit_error).__name__
                if commit_outcome_unknown and commit_error is not None
                else postcommit_error_type
            ),
        },
    }
    if orphan_private_report:
        try:
            _assert_orphan_apply_report_private(
                report,
                forbidden_values=(
                    candidate_instance_id,
                    normalized_request_id,
                    normalized_operator,
                    normalized_evidence_ref,
                    str(applied.get("candidate_instance_id") or ""),
                    str(applied.get("subject_key") or ""),
                    str(applied.get("order_plan_id") or ""),
                    str(applied.get("disposition_id") or ""),
                ),
            )
        except PipelineDispositionCliError as exc:
            report = _orphan_postcommit_privacy_failure_report(
                generated_at=str(report["generated_at"]),
                alias=str(evidence_alias),
                request_id_sha256=hashlib.sha256(normalized_request_id.encode("utf-8")).hexdigest(),
                disposition_id_sha256=hashlib.sha256(
                    str(applied.get("disposition_id") or "").encode("utf-8")
                ).hexdigest(),
                approval_binding_sha256=str(evidence_json["approval_binding_sha256"]),
                evidence_sha256=evidence_sha256,
                committed=committed,
                failures=failures,
                ledger_row_count_before=int(ledger_before["row_count"]),
                ledger_row_count_after=int(ledger_after["row_count"]),
                error_type=type(exc).__name__,
            )
    try:
        report["report_paths"] = _write_report(report, out_dir=out_dir)
    except (OSError, TypeError, ValueError) as exc:
        prior_failures = list(report["verdict"].get("failures") or [])
        unknown_outcome = report["verdict"].get("committed") is None
        failed_verdict: dict[str, Any] = {
            "status": (
                "OUTCOME_UNKNOWN_EVIDENCE_WRITE_FAILED"
                if unknown_outcome
                else "COMMITTED_EVIDENCE_WRITE_FAILED"
            ),
            "failures": [*prior_failures, "EVIDENCE_WRITE_FAILED"],
            "committed": None if unknown_outcome else True,
            "evidence_written": False,
            "operator_action_required": True,
            "retry_with_same_request_id": (
                None if orphan_private_report else normalized_request_id
            ),
            "error_type": type(exc).__name__,
        }
        if orphan_private_report:
            failed_verdict["retry_request_id_sha256"] = hashlib.sha256(
                normalized_request_id.encode("utf-8")
            ).hexdigest()
        report["verdict"] = failed_verdict
        report["report_paths"] = {}
    return report


def _load_and_assert_runtime_safe(db_path: Path) -> tuple[Settings, Path]:
    configured_env = os.environ.get(ENV_FILE_PATH_ENV, "").strip()
    reasons: list[str] = []
    env_path: Path | None = None
    env_hash_before: str | None = None
    if not configured_env:
        reasons.append("EXPLICIT_TRADING_ENV_FILE_REQUIRED")
    else:
        env_path = Path(configured_env).expanduser()
        if not env_path.is_file():
            reasons.append("TRADING_ENV_FILE_NOT_FOUND")
        elif _same_path(env_path, DEFAULT_ENV_FILE_PATH):
            reasons.append("DEFAULT_DOTENV_FORBIDDEN_FOR_APPLY")
        elif int(env_path.resolve().stat().st_nlink) != 1:
            reasons.append("TRADING_ENV_FILE_HARDLINK_ALIAS_UNSAFE")
        else:
            env_path = env_path.resolve()
            env_hash_before = _sha256_file(env_path)[0]
    clear_settings_cache()
    settings = load_settings()
    if env_path is not None and env_hash_before is not None:
        if _sha256_file(env_path)[0] != env_hash_before:
            reasons.append("TRADING_ENV_FILE_CHANGED_DURING_LOAD")
    if settings.trading_profile is not TradingProfile.OBSERVE:
        reasons.append("TRADING_PROFILE_NOT_OBSERVE")
    if settings.trading_mode is not TradingMode.OBSERVE:
        reasons.append("TRADING_MODE_NOT_OBSERVE")
    if settings.trading_allow_live_sim:
        reasons.append("LIVE_SIM_ALLOWED")
    if settings.trading_allow_live_real:
        reasons.append("LIVE_REAL_ALLOWED")
    if not settings.live_sim_kill_switch:
        reasons.append("LIVE_SIM_KILL_SWITCH_OFF")
    if settings.incremental_evaluation_worker_enabled:
        reasons.append("INCREMENTAL_EVALUATION_WORKER_ENABLED")
    for name in _PRODUCER_SETTING_NAMES:
        if bool(getattr(settings, name, True)):
            reasons.append(f"COMMAND_PRODUCER_ENABLED:{name.upper()}")
    theme_value = (
        None
        if env_path is None or not env_path.is_file()
        else _explicit_env_value(env_path, "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS")
    )
    if theme_value is None:
        reasons.append("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_EXPLICIT")
    elif theme_value.strip().lower() not in _FALSE_VALUES:
        reasons.append("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_FALSE")
    if not _same_path(Path(settings.trading_db_path), db_path):
        reasons.append("DB_PATH_DOES_NOT_MATCH_SAFE_ENV")
    if reasons:
        raise PipelineDispositionCliError(*reasons)
    assert env_path is not None
    return settings, env_path


def _runtime_safety_snapshot(settings: Settings, *, env_path: Path) -> dict[str, Any]:
    return {
        "trading_profile": settings.trading_profile.value,
        "trading_mode": settings.trading_mode.value,
        "live_sim_allowed": bool(settings.trading_allow_live_sim),
        "live_real_allowed": bool(settings.trading_allow_live_real),
        "kill_switch_active": bool(settings.live_sim_kill_switch),
        "incremental_worker_enabled": bool(settings.incremental_evaluation_worker_enabled),
        "enabled_command_producers": [
            name for name in _PRODUCER_SETTING_NAMES if bool(getattr(settings, name, True))
        ],
        "explicit_env_file": True,
        "explicit_env_file_sha256": _sha256_file(env_path)[0],
        "theme_refresh_queue_market_scan_commands": False,
        "not_order_intent": True,
        "order_commands_allowed": False,
    }


def _install_write_authorizer(
    connection: sqlite3.Connection,
    *,
    violations: list[str],
) -> None:
    write_actions = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
    schema_actions = {
        int(value)
        for name in (
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
        arg2: str | None,
        database: str | None,
        _trigger: str | None,
    ) -> int:
        table = str(arg1 or "")
        if action_code in write_actions:
            if (
                action_code == sqlite3.SQLITE_INSERT
                and table == DISPOSITION_TABLE
                and database == "main"
            ):
                return sqlite3.SQLITE_OK
            violations.append(
                f"UNAUTHORIZED_DATABASE_WRITE:{database or 'UNKNOWN'}:{table or 'UNKNOWN'}"
            )
            return sqlite3.SQLITE_DENY
        if action_code in schema_actions:
            violations.append("UNAUTHORIZED_SCHEMA_WRITE")
            return sqlite3.SQLITE_DENY
        if action_code == sqlite3.SQLITE_PRAGMA:
            pragma_name = str(arg1 or "").lower()
            allowed_pragmas = {
                "query_only",
                "table_info",
                "index_list",
                "index_xinfo",
                "foreign_key_list",
            }
            if pragma_name not in allowed_pragmas and arg2 is not None:
                violations.append(f"UNAUTHORIZED_PRAGMA_WRITE:{arg1 or 'UNKNOWN'}")
                return sqlite3.SQLITE_DENY
        if action_code == getattr(sqlite3, "SQLITE_SAVEPOINT", -1):
            return sqlite3.SQLITE_OK
        if action_code == sqlite3.SQLITE_TRANSACTION:
            violations.append("UNAUTHORIZED_TRANSACTION_CONTROL")
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorizer)


def _artifact_digest(connection: sqlite3.Connection) -> dict[str, Any]:
    tables = {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    missing = sorted(set(_ORDER_ARTIFACT_TABLES) - tables)
    if missing:
        raise PipelineDispositionCliError(
            *(f"ORDER_ARTIFACT_TABLE_MISSING:{table.upper()}" for table in missing)
        )
    table_digests = {table: _table_digest(connection, table) for table in _ORDER_ARTIFACT_TABLES}
    return {
        "contract": "pipeline-disposition-order-artifacts.v1",
        "tables": table_digests,
        "sha256": _json_sha256(table_digests),
    }


def _table_digest(connection: sqlite3.Connection, table: str) -> dict[str, Any]:
    columns = [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]
    digest = hashlib.sha256()
    row_count = 0
    try:
        rows = connection.execute(f'SELECT * FROM "{table}" ORDER BY _rowid_')
    except sqlite3.OperationalError:
        rows = connection.execute(f'SELECT * FROM "{table}"')
    for row in rows:
        payload = [_hash_value(row[column]) for column in columns]
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        row_count += 1
    return {"row_count": row_count, "sha256": digest.hexdigest()}


def _hash_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, bytes):
        return {"type": "blob", "size": len(value), "sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, float):
        return {"type": "float", "value": value.hex()}
    if isinstance(value, int):
        return {"type": "integer", "value": value}
    return {"type": "text", "value": str(value)}


def _ledger_state(connection: sqlite3.Connection) -> dict[str, Any]:
    return _table_digest(connection, DISPOSITION_TABLE)


def _runtime_lease_count(connection: sqlite3.Connection) -> int:
    return int(connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[0])


def _schema_version(connection: sqlite3.Connection) -> str | None:
    row = connection.execute("SELECT value FROM app_metadata WHERE key='schema_version'").fetchone()
    return None if row is None else str(row[0])


def _validated_database_path(path: Path) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
        stat_result = resolved.stat()
    except OSError as exc:
        raise PipelineDispositionCliError("DATABASE_NOT_FOUND") from exc
    if not resolved.is_file():
        raise PipelineDispositionCliError("DATABASE_NOT_FOUND")
    if int(stat_result.st_nlink) != 1:
        raise PipelineDispositionCliError("DATABASE_HARDLINK_ALIAS_UNSAFE")
    return resolved


def _open_strict_read_only(path: Path) -> sqlite3.Connection:
    _assert_quiescent_sidecars(path)
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=ro&immutable=1",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _open_existing_read_write(path: Path) -> sqlite3.Connection:
    _assert_quiescent_sidecars(path)
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=rw",
        uri=True,
        timeout=30.0,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def _commit_connection(connection: sqlite3.Connection) -> None:
    connection.commit()


def _close_connection(connection: sqlite3.Connection) -> None:
    connection.close()


def _reconcile_committed_disposition(
    path: Path,
    *,
    request_id: str,
    applied: Mapping[str, Any],
) -> str:
    connection: sqlite3.Connection | None = None
    try:
        _validated_database_path(path)
        uri_path = quote(path.as_posix(), safe="/:")
        connection = sqlite3.connect(
            f"file:{uri_path}?mode=ro",
            uri=True,
            timeout=30.0,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        if not is_pipeline_coherency_disposition_schema_ready(connection):
            return "UNKNOWN"
        row = connection.execute(
            f"SELECT * FROM {DISPOSITION_TABLE} WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            return "NOT_COMMITTED"
        expected_fields = (
            "disposition_id",
            "request_id",
            "request_hash",
            "candidate_instance_id",
            "subject_key",
            "action",
            "expected_pipeline_fingerprint",
            "expected_subject_version",
            "expected_source_fingerprint",
            "expected_candidate_fingerprint",
            "expected_downstream_fingerprint",
            "expected_boundary_fingerprint",
            "evidence_sha256",
        )
        if any(str(row[key]) != str(applied.get(key)) for key in expected_fields):
            return "UNKNOWN"
        return "COMMITTED"
    except (OSError, sqlite3.Error, KeyError, TypeError, ValueError):
        return "UNKNOWN"
    finally:
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass


def _checkpoint_committed_database(path: Path) -> None:
    uri_path = quote(path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=rw",
        uri=True,
        timeout=30.0,
        isolation_level=None,
    )
    try:
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        if journal_mode != "wal":
            return
        result = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if result is None or int(result[0]) != 0 or int(result[1]) != int(result[2]):
            raise PipelineDispositionCliError("ORPHAN_POSTCOMMIT_WAL_CHECKPOINT_FAILED")
    finally:
        connection.close()


def _assert_quiescent_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            raise PipelineDispositionCliError("QUIESCENT_DATABASE_REQUIRED")


def _assert_orphan_apply_wal_quiescent(path: Path) -> None:
    """Reject committed WAL frames after the orphan apply owns the writer lock.

    Opening a pristine WAL database and acquiring ``BEGIN EXCLUSIVE`` may create
    an empty WAL/SHM pair.  A non-empty WAL at this point, however, proves that a
    writer committed after the approved no-sidecar preflight (or before this
    transaction acquired the writer lock).  The lock prevents a new writer from
    racing the checks that follow.
    """

    journal_path = Path(f"{path}-journal")
    wal_path = Path(f"{path}-wal")
    shm_path = Path(f"{path}-shm")
    if journal_path.exists():
        raise PipelineDispositionCliError("ORPHAN_APPLY_JOURNAL_NOT_QUIESCENT")
    try:
        wal_exists = wal_path.exists()
        wal_size = int(wal_path.stat().st_size) if wal_exists else 0
        shm_exists = shm_path.exists()
    except OSError as exc:
        raise PipelineDispositionCliError("ORPHAN_APPLY_WAL_STATE_UNAVAILABLE") from exc
    if wal_size != 0:
        raise PipelineDispositionCliError("ORPHAN_APPLY_COMMITTED_WAL_FRAMES_PRESENT")
    if shm_exists and not wal_exists:
        raise PipelineDispositionCliError("ORPHAN_APPLY_WAL_STATE_INCONSISTENT")


def _file_state(path: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for suffix in ("", "-wal", "-shm", "-journal"):
        candidate = Path(f"{path}{suffix}")
        key = "main" if not suffix else suffix.removeprefix("-")
        if not candidate.exists():
            result[key] = {"exists": False, "size": 0, "mtime_ns": None}
            continue
        stat_result = candidate.stat()
        result[key] = {
            "exists": True,
            "size": int(stat_result.st_size),
            "mtime_ns": int(stat_result.st_mtime_ns),
        }
    return result


def _sha256_file(path: Path) -> tuple[str, int]:
    try:
        resolved = path.expanduser().resolve(strict=True)
        before = resolved.stat()
    except OSError as exc:
        raise PipelineDispositionCliError("EVIDENCE_FILE_NOT_FOUND") from exc
    if not resolved.is_file() or before.st_size <= 0:
        raise PipelineDispositionCliError("EVIDENCE_FILE_EMPTY")
    digest = hashlib.sha256()
    size = 0
    with resolved.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    after = resolved.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise PipelineDispositionCliError("EVIDENCE_FILE_CHANGED_DURING_HASH")
    return digest.hexdigest(), size


def _main_database_fingerprint(path: Path) -> dict[str, Any]:
    try:
        before = path.stat()
        sha256, size = _sha256_file(path)
        after = path.stat()
    except OSError as exc:
        raise PipelineDispositionCliError("DATABASE_MAIN_FINGERPRINT_UNAVAILABLE") from exc
    before_identity = (int(before.st_size), int(before.st_mtime_ns))
    after_identity = (int(after.st_size), int(after.st_mtime_ns))
    if before_identity != after_identity or size != int(after.st_size):
        raise PipelineDispositionCliError("DATABASE_MAIN_CHANGED_DURING_FINGERPRINT")
    return {
        "exists": True,
        "size": size,
        "mtime_ns": int(after.st_mtime_ns),
        "sha256": sha256,
    }


def _validated_orphan_evidence(
    *,
    evidence_file: Path,
    expected_evidence_sha256: str,
    expected_evidence_preview_sha256: str,
    evidence_target_set_sha256: str,
    evidence_alias: str,
    orphan_preflight_report: Path | None,
    expected_orphan_preflight_report_sha256: str,
    private_target_manifest: Path | None,
    expected_private_target_manifest_sha256: str,
    authoritative_artifact: Path | None,
    expected_authoritative_artifact_sha256: str,
    target: Mapping[str, Any],
) -> tuple[
    dict[str, Any],
    int,
    dict[str, tuple[Any, ...]],
    dict[str, Any],
]:
    if (
        orphan_preflight_report is None
        or private_target_manifest is None
        or authoritative_artifact is None
    ):
        raise PipelineDispositionCliError("ORPHAN_APPROVED_PRIVATE_INPUTS_REQUIRED")
    try:
        preflight_document = read_stable_json_document(
            orphan_preflight_report,
            expected_sha256=expected_orphan_preflight_report_sha256,
        )
        manifest_document = read_stable_json_document(
            private_target_manifest,
            expected_sha256=expected_private_target_manifest_sha256,
        )
        artifact_fingerprint = read_stable_file_fingerprint(
            authoritative_artifact,
            expected_sha256=expected_authoritative_artifact_sha256,
        )
        document = read_stable_json_document(
            evidence_file,
            expected_sha256=expected_evidence_sha256,
        )
        preflight_contract = orphan_preflight_tool.validate_apply_preflight_report(
            preflight_document.payload,
            report_sha256=preflight_document.sha256,
            expected_private_manifest_sha256=manifest_document.sha256,
            expected_evidence_sha256=document.sha256,
            expected_artifact_sha256=artifact_fingerprint.sha256,
            expected_artifact_size=artifact_fingerprint.size,
            expected_target_set_sha256=evidence_target_set_sha256,
            expected_alias=evidence_alias,
            expected_evidence_preview_sha256=expected_evidence_preview_sha256,
        )
        manifest = validate_private_target_manifest(
            manifest_document,
            expected_target_set_sha256=evidence_target_set_sha256,
            expected_count=preflight_contract["target_count"],
        )
        preflight_contract = {
            **preflight_contract,
            "campaign_targets": [dict(item) for item in manifest["items"]],
        }
        manifest_matches = [
            dict(item) for item in manifest["items"] if item.get("alias") == evidence_alias
        ]
        if len(manifest_matches) != 1:
            raise OrphanManualEvidenceError("ORPHAN_MANIFEST_ALIAS_NOT_FOUND")
        manifest_target = manifest_matches[0]
        manifest_target.pop("alias", None)
        if manifest_target != dict(target):
            raise OrphanManualEvidenceError("ORPHAN_MANIFEST_CURRENT_TARGET_MISMATCH")
        binding = validate_orphan_manual_evidence_document(
            document,
            expected_target_set_sha256=evidence_target_set_sha256,
            expected_alias=evidence_alias,
            expected_target=target,
            expected_artifact_sha256=artifact_fingerprint.sha256,
            expected_artifact_size=artifact_fingerprint.size,
            expected_account_scope_sha256=preflight_contract["broker_scope_sha256"],
            not_after=preflight_contract["generated_at"],
        )
        binding = validate_orphan_evidence_binding(
            binding,
            evidence_sha256=expected_evidence_sha256,
            candidate_instance_id=str(target.get("candidate_instance_id") or ""),
            trade_date=str(target.get("trade_date") or ""),
            action=str(target.get("action") or ""),
            pipeline_fingerprint=str(target.get("pipeline_fingerprint") or ""),
            subject_version=str(target.get("subject_version") or ""),
            source_fingerprint=str(target.get("source_fingerprint") or ""),
            candidate_fingerprint=str(target.get("candidate_fingerprint") or ""),
            downstream_fingerprint=str(target.get("downstream_fingerprint") or ""),
            boundary_fingerprint=str(target.get("boundary_fingerprint") or ""),
            expected_preview_sha256=expected_evidence_preview_sha256,
        )
        database_main = preflight_contract["database_main"]
        apply_binding = build_orphan_apply_evidence_binding(
            binding,
            approved_preflight_report_sha256=preflight_contract["report_sha256"],
            approved_private_manifest_sha256=preflight_contract["private_manifest_sha256"],
            source_plan_report_sha256=preflight_contract["source_plan_report_sha256"],
            source_reconciliation_report_sha256=preflight_contract[
                "source_reconciliation_report_sha256"
            ],
            approved_database_main_sha256=database_main["sha256"],
            approved_database_main_size=database_main["size"],
            broker_scope_sha256=preflight_contract["broker_scope_sha256"],
            preflight_generated_at=preflight_contract["generated_at_wire"],
            root_handoff_report_sha256=preflight_contract["root_handoff_report_sha256"],
            root_handoff_chain_sha256=preflight_contract["root_handoff_chain_sha256"],
            predecessor_kind=preflight_contract["predecessor_kind"],
            predecessor_report_sha256=preflight_contract["predecessor_report_sha256"],
            predecessor_chain_sha256=preflight_contract["predecessor_chain_sha256"],
            predecessor_database_main_sha256=database_main["sha256"],
            predecessor_database_main_size=database_main["size"],
        )
        binding = validate_orphan_apply_evidence_binding(
            apply_binding,
            evidence_sha256=expected_evidence_sha256,
            candidate_instance_id=str(target.get("candidate_instance_id") or ""),
            trade_date=str(target.get("trade_date") or ""),
            action=str(target.get("action") or ""),
            pipeline_fingerprint=str(target.get("pipeline_fingerprint") or ""),
            subject_version=str(target.get("subject_version") or ""),
            source_fingerprint=str(target.get("source_fingerprint") or ""),
            candidate_fingerprint=str(target.get("candidate_fingerprint") or ""),
            downstream_fingerprint=str(target.get("downstream_fingerprint") or ""),
            boundary_fingerprint=str(target.get("boundary_fingerprint") or ""),
            not_after=preflight_contract["generated_at"],
        )
    except (
        OrphanManualEvidenceError,
        orphan_preflight_tool.PipelineOrphanEvidencePreflightError,
    ) as exc:
        code = getattr(exc, "code", str(exc))
        raise PipelineDispositionCliError(code) from exc
    input_identity = {
        "preflight_report": _stable_input_identity(preflight_document),
        "private_manifest": _stable_input_identity(manifest_document),
        "evidence": _stable_input_identity(document),
        "authoritative_artifact": _stable_input_identity(artifact_fingerprint),
    }
    return binding, document.size, input_identity, preflight_contract


def _stable_input_identity(value: Any) -> tuple[Any, ...]:
    return (
        getattr(value, "sha256", None),
        getattr(value, "size", None),
        getattr(value, "device", None),
        getattr(value, "inode", None),
        getattr(value, "mtime_ns", None),
    )


def _orphan_target(
    *,
    trade_date: str,
    candidate_instance_id: str,
    action: str,
    expected_hashes: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "trade_date": trade_date,
        "candidate_instance_id": candidate_instance_id,
        "classification": ORPHAN_CLASSIFICATION,
        "action": action,
        **{key.removeprefix("expected_"): value for key, value in expected_hashes.items()},
    }


def _safe_label(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if (
        not _SAFE_LABEL_RE.fullmatch(normalized)
        or ".." in normalized
        or "\\" in normalized
        or _contains_sensitive_value(normalized)
    ):
        raise PipelineDispositionCliError(f"UNSAFE_LABEL:{name.upper()}")
    return normalized


def _sha256_value(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise PipelineDispositionCliError(f"INVALID_SHA256:{name.upper()}")
    return normalized


def _explicit_env_value(path: Path, env_name: str) -> str | None:
    value: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, candidate = line.split("=", 1)
        if key.strip() == env_name:
            value = candidate.strip().strip("'\"")
    return value


def _same_path(left: Path, right: Path) -> bool:
    try:
        return os.path.normcase(str(left.expanduser().resolve())) == os.path.normcase(
            str(right.expanduser().resolve())
        )
    except OSError:
        return False


def _json_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, str]:
    report_dir = out_dir / datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    commands_path = report_dir / "commands.txt"
    raw_path.write_text(
        json.dumps(_redact(report), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(report), encoding="utf-8")
    commands_path.write_text(
        "preview: python -B -m tools.resolve_pipeline_coherency <redacted arguments>\n"
        "apply: requires explicit safe TRADING_ENV_FILE and exact CAS hashes\n",
        encoding="utf-8",
    )
    return {
        "raw_json": str(raw_path),
        "summary_md": str(summary_path),
        "commands_txt": str(commands_path),
    }


def _redact(value: Any, *, key: str = "") -> Any:
    if any(part in key.lower() for part in ("account", "token", "password", "secret")):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {str(child): _redact(item, key=str(child)) for child, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, str) and _contains_sensitive_value(value):
        return "[REDACTED]"
    return value


def _assert_orphan_apply_report_private(
    value: Any,
    *,
    forbidden_values: tuple[str, ...],
    key: str = "",
) -> None:
    forbidden_keys = {
        "candidate_instance_id",
        "subject_key",
        "order_plan_id",
        "operator_id",
        "evidence_ref",
        "request_id",
        "disposition_id",
    }
    if isinstance(value, Mapping):
        for child, item in value.items():
            child_key = str(child)
            if child_key in forbidden_keys:
                raise PipelineDispositionCliError("ORPHAN_REPORT_PRIVATE_FIELD_PRESENT")
            _assert_orphan_apply_report_private(
                item,
                forbidden_values=forbidden_values,
                key=child_key,
            )
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _assert_orphan_apply_report_private(
                item,
                forbidden_values=forbidden_values,
                key=key,
            )
        return
    if isinstance(value, str):
        public_value_keys = {
            "action",
            "alias",
            "commit_reconciliation",
            "contract",
            "error_type",
            "failures",
            "generated_at",
            "mode",
            "schema_version",
            "status",
            "trading_mode",
            "trading_profile",
            "type",
        }
        if key not in public_value_keys and any(
            candidate and candidate == value for candidate in forbidden_values
        ):
            raise PipelineDispositionCliError("ORPHAN_REPORT_PRIVATE_VALUE_PRESENT")
        if re.match(r"^(?:[A-Za-z]:[/\\]|/|\\\\)", value):
            raise PipelineDispositionCliError("ORPHAN_REPORT_ABSOLUTE_PATH_PRESENT")


def _orphan_postcommit_privacy_failure_report(
    *,
    generated_at: str,
    alias: str,
    request_id_sha256: str,
    disposition_id_sha256: str,
    approval_binding_sha256: str,
    evidence_sha256: str,
    committed: bool | None,
    failures: list[str],
    ledger_row_count_before: int,
    ledger_row_count_after: int,
    error_type: str,
) -> dict[str, Any]:
    status = (
        "OUTCOME_UNKNOWN_POSTCHECK_FAILED" if committed is None else "COMMITTED_POSTCHECK_FAILED"
    )
    return {
        "contract": "fast0-pipeline-disposition-apply.v1",
        "generated_at": generated_at,
        "mode": "APPLY",
        "database": {"schema_version": "63"},
        "result": {
            "action": ACTION_DISPOSE_ORPHAN,
            "alias": alias,
            "request_id_sha256": request_id_sha256,
            "disposition_id_sha256": disposition_id_sha256,
        },
        "evidence": {
            "sha256": evidence_sha256,
            "approval_binding_sha256": approval_binding_sha256,
            "content_embedded": False,
        },
        "safety": {
            "observe_only": True,
            "not_order_intent": True,
            "no_order_side_effects": True,
        },
        "invariants": {
            "ledger_row_count_before": ledger_row_count_before,
            "ledger_row_count_after": ledger_row_count_after,
        },
        "read_only": False,
        "append_only": True,
        "observe_only": True,
        "not_order_intent": True,
        "no_order_side_effects": True,
        "verdict": {
            "status": status,
            "failures": [*failures, "ORPHAN_REPORT_PRIVACY_ASSERTION_FAILED"],
            "committed": committed,
            "evidence_written": True,
            "operator_action_required": True,
            "retry_with_same_request_id": None,
            "retry_request_id_sha256": request_id_sha256,
            "error_type": error_type,
        },
    }


def _contains_sensitive_value(value: str) -> bool:
    if any(pattern.search(value) is not None for pattern in _SENSITIVE_VALUE_PATTERNS):
        return True
    without_iso_dates = _ISO_DATE_PATTERN.sub("", value)
    return _HYPHENATED_ACCOUNT_PATTERN.search(without_iso_dates) is not None


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    safe_report = dict(_redact(report))
    verdict = dict(safe_report.get("verdict") or {})
    subject = dict(safe_report.get("preview") or safe_report.get("result") or {})
    if subject.get("action") == ACTION_DISPOSE_ORPHAN:
        subject_line = (
            f"- alias: `{subject.get('alias')}`"
            if subject.get("alias")
            else (
                f"- candidate_instance_id_sha256: `{subject.get('candidate_instance_id_sha256')}`"
            )
        )
    else:
        subject_line = f"- candidate_instance_id: `{subject.get('candidate_instance_id')}`"
    return "\n".join(
        [
            "# FAST-0 Pipeline Disposition",
            "",
            f"- generated_at: `{safe_report.get('generated_at')}`",
            f"- mode: `{safe_report.get('mode')}`",
            f"- verdict: `{verdict.get('status')}`",
            subject_line,
            f"- action: `{subject.get('action')}`",
            f"- failures: `{', '.join(verdict.get('failures') or []) or '-'}`",
            "",
            "No order, cancel, modify, broker, Core, or Gateway action is performed.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = dict(report.get("verdict") or {})
    if "committed" not in verdict:
        committed = "false"
    elif verdict.get("committed") is None:
        committed = "unknown"
    else:
        committed = str(bool(verdict.get("committed"))).lower()
    return (
        f"pipeline disposition: {verdict.get('status')} mode={report.get('mode')} "
        f"committed={committed}"
    )


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
