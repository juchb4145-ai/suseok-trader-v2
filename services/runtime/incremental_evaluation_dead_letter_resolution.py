from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    parse_timestamp,
    utc_now,
)
from domain.candidate.state import CandidateState

from services.config import (
    DEFAULT_ENV_FILE_PATH,
    ENV_FILE_PATH_ENV,
    Settings,
    TradingMode,
    TradingProfile,
)
from services.incremental_dead_letter_campaign import (
    IncrementalDeadLetterCampaignError,
    validate_campaign_apply_evidence_binding,
)
from services.runtime.evaluation_run_guard import (
    EVALUATION_PIPELINE_LOCK,
    runtime_execution_lock,
)

ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE = "DISPOSE_OBSOLETE_CLOSED_CANDIDATE"
ACTION_RESET_CANARY = "RESET_CANARY"
ACTION_VERIFY_CANARY = "VERIFY_CANARY"
ACTION_RESET_BATCH = "RESET_BATCH"
ACTION_REVOKE = "REVOKE"

CAMPAIGN_REASON_CODE = "OPERATOR_DISPOSED_OBSOLETE_CLOSED_CANDIDATE"
CAMPAIGN_EVIDENCE_TYPE = "FAST0R9_INCREMENTAL_DEAD_LETTER_CAMPAIGN"

DISPOSITION_TABLE = "incremental_evaluation_dead_letter_dispositions"
RAW_DEAD_LETTER_TABLE = "incremental_evaluation_dead_letters"

BUCKET_ACTIVE_UNRESOLVED = "ACTIVE_UNRESOLVED"
BUCKET_HISTORICAL_PENDING = "HISTORICAL_PENDING_DISPOSITION"
BUCKET_HISTORICAL_DISPOSED = "HISTORICAL_DISPOSED"
BUCKET_MANUAL_REVIEW = "MANUAL_REVIEW"
BUCKET_RECOVERY_PENDING = "RECOVERY_PENDING"
BUCKET_RECOVERY_VERIFIED = "RECOVERY_VERIFIED"
BUCKET_INVALID_DISPOSITION = "INVALID_DISPOSITION"

EFFECTIVE_BUCKETS = frozenset(
    {
        BUCKET_ACTIVE_UNRESOLVED,
        BUCKET_HISTORICAL_PENDING,
        BUCKET_HISTORICAL_DISPOSED,
        BUCKET_MANUAL_REVIEW,
        BUCKET_RECOVERY_PENDING,
        BUCKET_RECOVERY_VERIFIED,
        BUCKET_INVALID_DISPOSITION,
    }
)
BLOCKING_BUCKETS = frozenset(
    {
        BUCKET_ACTIVE_UNRESOLVED,
        BUCKET_HISTORICAL_PENDING,
        BUCKET_MANUAL_REVIEW,
        BUCKET_RECOVERY_PENDING,
        BUCKET_INVALID_DISPOSITION,
    }
)

_ACTIONS = frozenset(
    {
        ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
        ACTION_RESET_CANARY,
        ACTION_VERIFY_CANARY,
        ACTION_RESET_BATCH,
        ACTION_REVOKE,
    }
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@-]{2,199}$")
_FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off"})
_PRODUCER_SETTING_NAMES = (
    "realtime_subscription_queue_commands",
    "dry_run_order_routing_enabled",
    "dry_run_gateway_command_enabled",
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
)


class IncrementalEvaluationDeadLetterResolutionError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = str(code)
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "INCREMENTAL_EVALUATION_DEAD_LETTER_ACTION_REJECTED",
            "code": self.code,
            "message": str(self),
            "details": self.details,
            "raw_dead_letter_changed": False,
            "no_evaluation_run": True,
            "no_order_commands_created": True,
            "no_broker_calls": True,
            "live_real_allowed": False,
        }


_CAMPAIGN_APPROVAL_CONTEXT_SEAL = object()


@dataclass(frozen=True)
class _IncrementalCampaignApplyApprovalContext:
    approval_binding_sha256: str
    approved_preflight_report_sha256: str
    approved_database_main_sha256: str
    alias: str
    dead_letter_id_sha256: str
    _seal: object


def _issue_incremental_campaign_apply_approval_context(
    evidence_json: Mapping[str, Any],
    *,
    verified_preflight_report_sha256: str,
    verified_database_main_sha256: str,
    expected_alias: str,
    dead_letter_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    evidence_sha256: str,
) -> _IncrementalCampaignApplyApprovalContext:
    """Mint a process-local capability after the CLI owns the writer lock."""

    try:
        normalized = validate_campaign_apply_evidence_binding(
            evidence_json,
            expected_alias=expected_alias,
            dead_letter_id=dead_letter_id,
            expected_dead_letter_fingerprint=expected_dead_letter_fingerprint,
            expected_candidate_version=expected_candidate_version,
            evidence_sha256=evidence_sha256,
            not_after=utc_now(),
        )
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CAMPAIGN_APPROVAL_BINDING_INVALID",
            "campaign apply binding is invalid",
            details={"reason_codes": [exc.code]},
        ) from exc
    preflight_sha256 = _require_sha256(
        "verified_preflight_report_sha256", verified_preflight_report_sha256
    )
    database_sha256 = _require_sha256(
        "verified_database_main_sha256", verified_database_main_sha256
    )
    if (
        normalized["approved_preflight_report_sha256"] != preflight_sha256
        or normalized["approved_database_main_sha256"] != database_sha256
    ):
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CAMPAIGN_APPROVAL_CONTEXT_MISMATCH",
            "verified preflight or database does not match campaign approval binding",
        )
    return _IncrementalCampaignApplyApprovalContext(
        approval_binding_sha256=str(normalized["approval_binding_sha256"]),
        approved_preflight_report_sha256=preflight_sha256,
        approved_database_main_sha256=database_sha256,
        alias=str(normalized["alias"]),
        dead_letter_id_sha256=str(normalized["dead_letter_id_sha256"]),
        _seal=_CAMPAIGN_APPROVAL_CONTEXT_SEAL,
    )


def append_incremental_dead_letter_campaign_disposition_in_transaction(
    connection: sqlite3.Connection,
    *,
    dead_letter_id: str,
    request_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    operator_id: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_json: Mapping[str, Any],
    safety_snapshot: Mapping[str, Any],
    campaign_approval_context: _IncrementalCampaignApplyApprovalContext | None,
) -> dict[str, Any]:
    """Append one R9 disposition without owning or committing the transaction."""

    if not connection.in_transaction:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CAMPAIGN_ACTIVE_TRANSACTION_REQUIRED",
            "campaign append requires a caller-owned active transaction",
        )
    _assert_disposition_schema_ready(connection)
    if _runtime_lock_count(connection):
        raise IncrementalEvaluationDeadLetterResolutionError(
            "RUNTIME_EXECUTION_LOCK_PRESENT",
            "campaign append requires every runtime execution lease to be free",
        )
    if not (
        isinstance(campaign_approval_context, _IncrementalCampaignApplyApprovalContext)
        and campaign_approval_context._seal is _CAMPAIGN_APPROVAL_CONTEXT_SEAL
    ):
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CAMPAIGN_APPROVAL_CONTEXT_REQUIRED",
            "new R9 campaign append requires the sealed CLI approval context",
        )
    try:
        normalized = validate_campaign_apply_evidence_binding(
            evidence_json,
            expected_alias=campaign_approval_context.alias,
            dead_letter_id=dead_letter_id,
            expected_dead_letter_fingerprint=expected_dead_letter_fingerprint,
            expected_candidate_version=expected_candidate_version,
            evidence_sha256=evidence_sha256,
            not_after=utc_now(),
        )
    except IncrementalDeadLetterCampaignError as exc:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CAMPAIGN_APPROVAL_BINDING_INVALID",
            "campaign append requires an exact R9 approval binding",
            details={"reason_codes": [exc.code]},
        ) from exc
    context_valid = bool(
        normalized["approval_binding_sha256"] == campaign_approval_context.approval_binding_sha256
        and normalized["approved_preflight_report_sha256"]
        == campaign_approval_context.approved_preflight_report_sha256
        and normalized["approved_database_main_sha256"]
        == campaign_approval_context.approved_database_main_sha256
        and normalized["alias"] == campaign_approval_context.alias
        and normalized["dead_letter_id_sha256"] == campaign_approval_context.dead_letter_id_sha256
    )
    if not context_valid:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CAMPAIGN_APPROVAL_CONTEXT_REQUIRED",
            "new R9 campaign append requires the sealed CLI approval context",
        )
    return _append_disposition_in_transaction(
        connection,
        action=ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
        dead_letter_id=dead_letter_id,
        request_id=request_id,
        expected_dead_letter_fingerprint=expected_dead_letter_fingerprint,
        expected_candidate_version=expected_candidate_version,
        reason_code=CAMPAIGN_REASON_CODE,
        operator_id=operator_id,
        evidence_type=CAMPAIGN_EVIDENCE_TYPE,
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
        evidence_json=normalized,
        requested_supersedes_id=None,
        recovery_session_id=None,
        batch_size=None,
        fencing_token=None,
        safety_snapshot=safety_snapshot,
        exact_evidence_json=True,
    )


def preview_incremental_evaluation_dead_letter_disposition(
    connection: sqlite3.Connection,
    dead_letter_id: str,
    *,
    action: str = ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
    expected_dead_letter_fingerprint: str | None = None,
    expected_candidate_version: str | None = None,
) -> dict[str, Any]:
    normalized_action = str(action).strip().upper()
    if normalized_action not in _ACTIONS:
        raise ValueError("unsupported dead-letter disposition action")
    normalized_id = str(dead_letter_id).strip()
    if not normalized_id:
        raise ValueError("dead_letter_id must not be empty")
    raw_row = connection.execute(
        f"SELECT * FROM {RAW_DEAD_LETTER_TABLE} WHERE dead_letter_id = ?",
        (normalized_id,),
    ).fetchone()
    if raw_row is None:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "DEAD_LETTER_NOT_FOUND",
            "incremental evaluation dead-letter row was not found",
        )

    effective = _build_effective_row(connection, raw_row)
    reason_codes: list[str] = []
    if expected_dead_letter_fingerprint is not None:
        expected_fingerprint = _require_sha256(
            "expected_dead_letter_fingerprint",
            expected_dead_letter_fingerprint,
        )
        if expected_fingerprint != effective["dead_letter_fingerprint"]:
            reason_codes.append("DEAD_LETTER_FINGERPRINT_MISMATCH")
    if expected_candidate_version is not None:
        expected_version = _require_sha256(
            "expected_candidate_version",
            expected_candidate_version,
        )
        if expected_version != effective["candidate_version"]:
            reason_codes.append("CANDIDATE_VERSION_MISMATCH")

    projection = effective["disposition_projection"]
    active = projection.get("active_disposition")
    bucket = str(effective["bucket"])
    if normalized_action == ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE:
        if bucket != BUCKET_HISTORICAL_PENDING:
            reason_codes.append("NOT_HISTORICAL_PENDING_DISPOSITION")
    elif normalized_action == ACTION_REVOKE:
        if active is None:
            reason_codes.append("NO_EFFECTIVE_DISPOSITION_TO_REVOKE")
    elif normalized_action in {ACTION_RESET_CANARY, ACTION_RESET_BATCH}:
        reason_codes.extend(_recovery_candidate_reason_codes(connection, raw_row))
        if bucket != BUCKET_ACTIVE_UNRESOLVED:
            reason_codes.append("NOT_ACTIVE_UNRESOLVED")
    elif normalized_action == ACTION_VERIFY_CANARY:
        if active is None or active.get("action") != ACTION_RESET_CANARY:
            reason_codes.append("NO_PENDING_RECOVERY_CANARY")
        if effective["active_queue"] is not None:
            reason_codes.append("CANARY_QUEUE_STILL_ACTIVE")

    result = {
        "status": "ELIGIBLE" if not reason_codes else "BLOCKED",
        "action": normalized_action,
        "eligible": not reason_codes,
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "dead_letter_id": normalized_id,
        "dead_letter_fingerprint": effective["dead_letter_fingerprint"],
        "source_dead_letter_fingerprint": effective["dead_letter_fingerprint"],
        "candidate_version": effective["candidate_version"],
        "source_candidate_version": effective["candidate_version"],
        "bucket": bucket,
        "raw_dead_letter": effective["raw_dead_letter"],
        "candidate": effective["candidate"],
        "active_queue": effective["active_queue"],
        "effective_disposition": active,
        "disposition_chain_valid": bool(projection.get("chain_valid")),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "auto_run_evaluation": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    return result


def record_incremental_evaluation_dead_letter_disposition(
    connection: sqlite3.Connection,
    dead_letter_id: str,
    *,
    request_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    reason_code: str,
    operator_id: str,
    evidence_ref: str,
    evidence_sha256: str,
    disposition_type: str = "OBSOLETE_CLOSED_CANDIDATE",
    evidence_type: str = "FAST0R2_INCREMENTAL_DEAD_LETTER_RCA",
    evidence_json: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if str(disposition_type).strip().upper() != "OBSOLETE_CLOSED_CANDIDATE":
        raise ValueError("unsupported disposition_type")
    return _apply_single_disposition(
        connection,
        action=ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
        dead_letter_id=dead_letter_id,
        request_id=request_id,
        expected_dead_letter_fingerprint=expected_dead_letter_fingerprint,
        expected_candidate_version=expected_candidate_version,
        reason_code=reason_code,
        operator_id=operator_id,
        evidence_type=evidence_type,
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
        evidence_json=evidence_json,
        requested_supersedes_id=None,
    )


def revoke_incremental_evaluation_dead_letter_disposition(
    connection: sqlite3.Connection,
    dead_letter_id: str,
    *,
    request_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    supersedes_disposition_id: str,
    reason_code: str,
    operator_id: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_type: str = "FAST0R2_INCREMENTAL_DEAD_LETTER_CORRECTION",
    evidence_json: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return _apply_single_disposition(
        connection,
        action=ACTION_REVOKE,
        dead_letter_id=dead_letter_id,
        request_id=request_id,
        expected_dead_letter_fingerprint=expected_dead_letter_fingerprint,
        expected_candidate_version=expected_candidate_version,
        reason_code=reason_code,
        operator_id=operator_id,
        evidence_type=evidence_type,
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
        evidence_json=evidence_json,
        requested_supersedes_id=supersedes_disposition_id,
    )


def build_incremental_evaluation_dead_letter_effective_status(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    rows = connection.execute(
        f"""
        SELECT *
        FROM {RAW_DEAD_LETTER_TABLE}
        WHERE status = 'DEAD_LETTER'
        ORDER BY dead_lettered_at DESC, dead_letter_id DESC
        """
    ).fetchall()
    effective_rows = [_build_effective_row(connection, row) for row in rows]
    bucket_counts = {
        bucket: sum(item["bucket"] == bucket for item in effective_rows)
        for bucket in EFFECTIVE_BUCKETS
    }
    raw_count = len(effective_rows)
    effective_count = sum(bucket_counts[bucket] for bucket in BLOCKING_BUCKETS)
    schema_ready = _disposition_schema_ready(connection)
    reason_codes: list[str] = []
    if not schema_ready:
        reason_codes.append("INCREMENTAL_DEAD_LETTER_DISPOSITION_SCHEMA_INVALID")
    if bucket_counts[BUCKET_ACTIVE_UNRESOLVED]:
        reason_codes.append("INCREMENTAL_ACTIVE_DEAD_LETTER_PRESENT")
    if bucket_counts[BUCKET_HISTORICAL_PENDING]:
        reason_codes.append("INCREMENTAL_HISTORICAL_DISPOSITION_PENDING")
    if bucket_counts[BUCKET_MANUAL_REVIEW]:
        reason_codes.append("INCREMENTAL_DEAD_LETTER_MANUAL_REVIEW_REQUIRED")
    if bucket_counts[BUCKET_RECOVERY_PENDING]:
        reason_codes.append("INCREMENTAL_RECOVERY_PENDING")
    if bucket_counts[BUCKET_INVALID_DISPOSITION]:
        reason_codes.append("INCREMENTAL_INVALID_DISPOSITION_PRESENT")
    if raw_count and not effective_count:
        reason_codes.append("INCREMENTAL_HISTORICAL_DEAD_LETTER_PRESERVED")
    effective_failed = bool(effective_count or not schema_ready)
    return {
        "raw_dead_letter_count": raw_count,
        "effective_dead_letter_count": effective_count,
        "active_unresolved_dead_letter_count": bucket_counts[BUCKET_ACTIVE_UNRESOLVED],
        "historical_pending_disposition_count": bucket_counts[BUCKET_HISTORICAL_PENDING],
        "historical_disposed_dead_letter_count": bucket_counts[BUCKET_HISTORICAL_DISPOSED],
        "manual_review_dead_letter_count": bucket_counts[BUCKET_MANUAL_REVIEW],
        "recovery_pending_count": bucket_counts[BUCKET_RECOVERY_PENDING],
        "recovery_verified_count": bucket_counts[BUCKET_RECOVERY_VERIFIED],
        "invalid_disposition_count": bucket_counts[BUCKET_INVALID_DISPOSITION],
        "raw_status": "FAIL" if raw_count else "PASS",
        "effective_status": "FAIL" if effective_failed else "PASS",
        "fast_0_status": "BLOCKED" if effective_failed else "CLEAR",
        "reason_codes": reason_codes,
        "schema_ready": schema_ready,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
    }


def list_incremental_evaluation_dead_letter_effective_rows(
    connection: sqlite3.Connection,
    *,
    bucket: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_bucket = None if bucket is None else str(bucket).strip().upper()
    if normalized_bucket is not None and normalized_bucket not in EFFECTIVE_BUCKETS:
        raise ValueError("unsupported effective dead-letter bucket")
    rows = connection.execute(
        f"""
        SELECT *
        FROM {RAW_DEAD_LETTER_TABLE}
        WHERE status = 'DEAD_LETTER'
        ORDER BY dead_lettered_at DESC, dead_letter_id DESC
        """
    ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        item = _build_effective_row(connection, row)
        if normalized_bucket is not None and item["bucket"] != normalized_bucket:
            continue
        item.pop("disposition_projection", None)
        results.append(item)
        if len(results) >= max(min(int(limit), 500), 1):
            break
    return results


def is_incremental_evaluation_candidate_blocked_by_dead_letter(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> bool:
    rows = connection.execute(
        f"""
        SELECT * FROM {RAW_DEAD_LETTER_TABLE}
        WHERE candidate_instance_id = ? AND status = 'DEAD_LETTER'
        ORDER BY dead_lettered_at DESC, dead_letter_id DESC
        """,
        (str(candidate_instance_id),),
    ).fetchall()
    return any(_build_effective_row(connection, row)["bucket"] in BLOCKING_BUCKETS for row in rows)


def can_append_incremental_evaluation_dead_letter_generation(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> bool:
    rows = connection.execute(
        f"""
        SELECT * FROM {RAW_DEAD_LETTER_TABLE}
        WHERE candidate_instance_id = ? AND status = 'DEAD_LETTER'
        ORDER BY dead_lettered_at DESC, dead_letter_id DESC
        """,
        (str(candidate_instance_id),),
    ).fetchall()
    effective_rows = [_build_effective_row(connection, row) for row in rows]
    blockers = [item for item in effective_rows if item["bucket"] in BLOCKING_BUCKETS]
    return not blockers or all(item["bucket"] == BUCKET_RECOVERY_PENDING for item in blockers)


def recover_incremental_evaluation_dead_letters(
    connection: sqlite3.Connection,
    dead_letter_ids: Sequence[str],
    *,
    mode: str,
    recovery_session_id: str,
    request_id: str,
    expected_versions: Sequence[Mapping[str, Any]] | Mapping[str, Any],
    settings: Settings,
    operator_id: str,
    reason_code: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_type: str = "FAST0R2_INCREMENTAL_RECOVERY_EVIDENCE",
    evidence_json: Mapping[str, Any] | None = None,
    verify_canary_disposition_id: str | None = None,
) -> dict[str, Any]:
    normalized_mode = str(mode).strip().upper()
    if normalized_mode not in {ACTION_RESET_CANARY, ACTION_RESET_BATCH}:
        raise ValueError("recovery mode must be RESET_CANARY or RESET_BATCH")
    normalized_ids = [str(value).strip() for value in dead_letter_ids]
    if not normalized_ids or any(not value for value in normalized_ids):
        raise ValueError("dead_letter_ids must not be empty")
    if len(normalized_ids) != len(set(normalized_ids)):
        raise ValueError("dead_letter_ids must be unique")
    if normalized_mode == ACTION_RESET_CANARY and len(normalized_ids) != 1:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CANARY_REQUIRES_EXACTLY_ONE",
            "recovery canary must contain exactly one dead-letter",
        )
    if normalized_mode == ACTION_RESET_BATCH and not 2 <= len(normalized_ids) <= 5:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "RECOVERY_BATCH_LIMIT_INVALID",
            "recovery batch must contain between two and five dead-letters",
        )
    versions = _normalize_expected_versions(normalized_ids, expected_versions)
    normalized_session = _require_safe_identifier("recovery_session_id", recovery_session_id)
    normalized_request = _require_safe_identifier("request_id", request_id)
    normalized_operator = _require_safe_identifier("operator_id", operator_id)
    normalized_reason = _require_safe_identifier("reason_code", reason_code)
    normalized_evidence_type = _require_safe_identifier("evidence_type", evidence_type)
    normalized_evidence_ref = _require_safe_identifier("evidence_ref", evidence_ref)
    normalized_evidence_sha = _require_sha256("evidence_sha256", evidence_sha256)
    authorization_disposition_id: str | None = None
    if normalized_mode == ACTION_RESET_BATCH:
        if not verify_canary_disposition_id:
            raise IncrementalEvaluationDeadLetterResolutionError(
                "VERIFIED_CANARY_DISPOSITION_REQUIRED",
                "batch recovery requires an explicit verified canary disposition",
            )
        authorization_disposition_id = _require_safe_identifier(
            "verify_canary_disposition_id",
            verify_canary_disposition_id,
        )
    _assert_recovery_runtime_safe(connection, settings)
    if connection.in_transaction:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "ACTIVE_TRANSACTION",
            "recovery requires a connection without an active transaction",
        )
    if _runtime_lock_count(connection):
        raise IncrementalEvaluationDeadLetterResolutionError(
            "RUNTIME_EXECUTION_LOCK_PRESENT",
            "recovery requires every runtime execution lease to be free",
        )

    with runtime_execution_lock(
        connection,
        EVALUATION_PIPELINE_LOCK,
        owner_id=f"fast0r2.{normalized_session}",
        details={"run_type": "incremental_dead_letter_recovery"},
    ) as lease:
        assert lease is not None
        started = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            started = True
            _assert_disposition_schema_ready(connection)
            _assert_only_current_runtime_lock(connection, lease.owner_id)
            item_request_ids = [
                (
                    normalized_request
                    if len(normalized_ids) == 1
                    else f"{normalized_request}.item-{index + 1}"
                )
                for index in range(len(normalized_ids))
            ]
            existing_request_count = int(
                connection.execute(
                    f"SELECT COUNT(*) AS count FROM {DISPOSITION_TABLE} "
                    f"WHERE request_id IN ({','.join('?' for _ in item_request_ids)})",
                    tuple(item_request_ids),
                ).fetchone()["count"]
                or 0
            )
            if existing_request_count not in {0, len(item_request_ids)}:
                raise IncrementalEvaluationDeadLetterResolutionError(
                    "PARTIAL_RECOVERY_REQUEST_REPLAY",
                    "recovery request is only partially present in the ledger",
                )
            if normalized_mode == ACTION_RESET_BATCH and existing_request_count == 0:
                _assert_verified_canary(
                    connection,
                    normalized_session,
                    authorization_disposition_id,
                )
            command_snapshot = _gateway_command_snapshot(connection)
            order_artifact_snapshot = _order_artifact_snapshot(connection)
            results: list[dict[str, Any]] = []
            for index, dead_letter_id in enumerate(normalized_ids):
                expected = versions[dead_letter_id]
                item_request_id = item_request_ids[index]
                safety_snapshot = {
                    "settings": _runtime_safety_snapshot(settings),
                    "gateway_commands": command_snapshot,
                    "order_artifacts": order_artifact_snapshot,
                    "verified_canary_disposition_id": (authorization_disposition_id),
                    "auto_run_evaluation": False,
                }
                if existing_request_count == 0:
                    preview = preview_incremental_evaluation_dead_letter_disposition(
                        connection,
                        dead_letter_id,
                        action=normalized_mode,
                        expected_dead_letter_fingerprint=expected["dead_letter_fingerprint"],
                        expected_candidate_version=expected["candidate_version"],
                    )
                    if not preview["eligible"]:
                        raise IncrementalEvaluationDeadLetterResolutionError(
                            "RECOVERY_CANDIDATE_NOT_ELIGIBLE",
                            "dead-letter is not eligible for guarded recovery",
                            details={
                                "dead_letter_id": dead_letter_id,
                                "reason_codes": preview["reason_codes"],
                            },
                        )
                result = _append_disposition_in_transaction(
                    connection,
                    action=normalized_mode,
                    dead_letter_id=dead_letter_id,
                    request_id=item_request_id,
                    expected_dead_letter_fingerprint=expected["dead_letter_fingerprint"],
                    expected_candidate_version=expected["candidate_version"],
                    reason_code=normalized_reason,
                    operator_id=normalized_operator,
                    evidence_type=normalized_evidence_type,
                    evidence_ref=normalized_evidence_ref,
                    evidence_sha256=normalized_evidence_sha,
                    evidence_json=evidence_json,
                    requested_supersedes_id=None,
                    recovery_session_id=normalized_session,
                    batch_size=len(normalized_ids),
                    fencing_token=lease.fencing_token,
                    safety_snapshot=safety_snapshot,
                    authorization_disposition_id=(authorization_disposition_id),
                )
                if not result["idempotent_replay"]:
                    raw = connection.execute(
                        f"SELECT * FROM {RAW_DEAD_LETTER_TABLE} WHERE dead_letter_id = ?",
                        (dead_letter_id,),
                    ).fetchone()
                    assert raw is not None
                    _insert_recovery_queue_row(connection, raw)
                results.append(result)
            if _gateway_command_snapshot(connection) != command_snapshot:
                raise IncrementalEvaluationDeadLetterResolutionError(
                    "GATEWAY_COMMAND_STATE_CHANGED",
                    "gateway command state changed during recovery",
                )
            if _order_artifact_snapshot(connection) != order_artifact_snapshot:
                raise IncrementalEvaluationDeadLetterResolutionError(
                    "ORDER_ARTIFACT_STATE_CHANGED",
                    "order artifact state changed during recovery",
                )
            lease.assert_current(connection, renew=False)
            connection.commit()
            started = False
        except Exception:
            if started:
                connection.rollback()
            raise
    return {
        "status": "REPLAYED" if all(r["idempotent_replay"] for r in results) else "APPLIED",
        "mode": normalized_mode,
        "recovery_session_id": normalized_session,
        "batch_size": len(normalized_ids),
        "items": results,
        "auto_run_evaluation": False,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_order_side_effects": True,
        "live_real_allowed": False,
    }


def verify_incremental_evaluation_recovery_canary(
    connection: sqlite3.Connection,
    *,
    dead_letter_id: str | None = None,
    recovery_session_id: str,
    request_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    settings: Settings,
    operator_id: str,
    reason_code: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_type: str = "FAST0R2_INCREMENTAL_RECOVERY_EVIDENCE",
    evidence_json: Mapping[str, Any] | None = None,
    canary_disposition_id: str | None = None,
) -> dict[str, Any]:
    normalized_session = _require_safe_identifier("recovery_session_id", recovery_session_id)
    normalized_request = _require_safe_identifier("request_id", request_id)
    normalized_fingerprint = _require_sha256(
        "expected_dead_letter_fingerprint",
        expected_dead_letter_fingerprint,
    )
    normalized_candidate_version = _require_sha256(
        "expected_candidate_version", expected_candidate_version
    )
    normalized_reason = _require_safe_identifier("reason_code", reason_code)
    normalized_operator = _require_safe_identifier("operator_id", operator_id)
    normalized_evidence_type = _require_safe_identifier("evidence_type", evidence_type)
    normalized_evidence_ref = _require_safe_identifier("evidence_ref", evidence_ref)
    normalized_evidence_sha = _require_sha256("evidence_sha256", evidence_sha256)
    _assert_recovery_runtime_safe(connection, settings)
    if connection.in_transaction:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "ACTIVE_TRANSACTION",
            "canary verification requires a connection without an active transaction",
        )
    if _runtime_lock_count(connection):
        raise IncrementalEvaluationDeadLetterResolutionError(
            "RUNTIME_EXECUTION_LOCK_PRESENT",
            "canary verification requires every runtime execution lease to be free",
        )
    with runtime_execution_lock(
        connection,
        EVALUATION_PIPELINE_LOCK,
        owner_id=f"fast0r2.verify.{normalized_session}",
        details={"run_type": "incremental_recovery_canary_verification"},
    ) as lease:
        assert lease is not None
        started = False
        try:
            connection.execute("BEGIN IMMEDIATE")
            started = True
            _assert_disposition_schema_ready(connection)
            _assert_only_current_runtime_lock(connection, lease.owner_id)
            existing = connection.execute(
                f"SELECT * FROM {DISPOSITION_TABLE} WHERE request_id = ?",
                (normalized_request,),
            ).fetchone()
            if existing is not None:
                existing_dead_letter_id = str(existing["dead_letter_id"])
                existing_supersedes_id = _optional_text(existing["supersedes_disposition_id"])
                if (
                    dead_letter_id is not None
                    and str(dead_letter_id).strip() != existing_dead_letter_id
                ) or (
                    canary_disposition_id is not None
                    and str(canary_disposition_id).strip() != existing_supersedes_id
                ):
                    raise IncrementalEvaluationDeadLetterResolutionError(
                        "REQUEST_CONFLICT",
                        "request_id was already used for a different canary",
                    )
                result = _append_disposition_in_transaction(
                    connection,
                    action=ACTION_VERIFY_CANARY,
                    dead_letter_id=existing_dead_letter_id,
                    request_id=normalized_request,
                    expected_dead_letter_fingerprint=normalized_fingerprint,
                    expected_candidate_version=normalized_candidate_version,
                    reason_code=normalized_reason,
                    operator_id=normalized_operator,
                    evidence_type=normalized_evidence_type,
                    evidence_ref=normalized_evidence_ref,
                    evidence_sha256=normalized_evidence_sha,
                    evidence_json=evidence_json,
                    requested_supersedes_id=existing_supersedes_id,
                    recovery_session_id=normalized_session,
                    batch_size=1,
                    fencing_token=lease.fencing_token,
                    safety_snapshot={
                        "settings": _runtime_safety_snapshot(settings),
                        "gateway_commands": _gateway_command_snapshot(connection),
                        "order_artifacts": _order_artifact_snapshot(connection),
                        "auto_run_evaluation": False,
                    },
                )
                lease.assert_current(connection, renew=False)
                connection.commit()
                started = False
                return {
                    **result,
                    "status": "REPLAYED",
                    "recovery_session_id": normalized_session,
                    "auto_run_evaluation": False,
                    "no_evaluation_run": True,
                    "no_order_commands_created": True,
                    "no_order_side_effects": True,
                }
            canary = _find_recovery_canary(
                connection,
                normalized_session,
                canary_disposition_id,
                dead_letter_id,
            )
            resolved_dead_letter_id = str(canary["dead_letter_id"])
            preview = preview_incremental_evaluation_dead_letter_disposition(
                connection,
                resolved_dead_letter_id,
                action=ACTION_VERIFY_CANARY,
                expected_dead_letter_fingerprint=normalized_fingerprint,
                expected_candidate_version=normalized_candidate_version,
            )
            verification_reasons = list(preview["reason_codes"])
            verification_reasons.extend(_canary_verification_reason_codes(connection, canary))
            if verification_reasons:
                raise IncrementalEvaluationDeadLetterResolutionError(
                    "CANARY_VERIFICATION_NOT_PROVEN",
                    "canary verification evidence is incomplete",
                    details={"reason_codes": list(dict.fromkeys(verification_reasons))},
                )
            result = _append_disposition_in_transaction(
                connection,
                action=ACTION_VERIFY_CANARY,
                dead_letter_id=resolved_dead_letter_id,
                request_id=normalized_request,
                expected_dead_letter_fingerprint=normalized_fingerprint,
                expected_candidate_version=normalized_candidate_version,
                reason_code=normalized_reason,
                operator_id=normalized_operator,
                evidence_type=normalized_evidence_type,
                evidence_ref=normalized_evidence_ref,
                evidence_sha256=normalized_evidence_sha,
                evidence_json=evidence_json,
                requested_supersedes_id=str(canary["disposition_id"]),
                recovery_session_id=normalized_session,
                batch_size=1,
                fencing_token=lease.fencing_token,
                safety_snapshot={
                    "settings": _runtime_safety_snapshot(settings),
                    "gateway_commands": _gateway_command_snapshot(connection),
                    "order_artifacts": _order_artifact_snapshot(connection),
                    "auto_run_evaluation": False,
                },
            )
            lease.assert_current(connection, renew=False)
            connection.commit()
            started = False
        except Exception:
            if started:
                connection.rollback()
            raise
    return {
        **result,
        "status": "REPLAYED" if result["idempotent_replay"] else "VERIFIED",
        "recovery_session_id": normalized_session,
        "auto_run_evaluation": False,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_order_side_effects": True,
    }


def _apply_single_disposition(
    connection: sqlite3.Connection,
    *,
    action: str,
    dead_letter_id: str,
    request_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    reason_code: str,
    operator_id: str,
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_json: Mapping[str, Any] | None,
    requested_supersedes_id: str | None,
) -> dict[str, Any]:
    normalized = {
        "dead_letter_id": str(dead_letter_id).strip(),
        "request_id": _require_safe_identifier("request_id", request_id),
        "expected_dead_letter_fingerprint": _require_sha256(
            "expected_dead_letter_fingerprint",
            expected_dead_letter_fingerprint,
        ),
        "expected_candidate_version": _require_sha256(
            "expected_candidate_version", expected_candidate_version
        ),
        "reason_code": _require_safe_identifier("reason_code", reason_code),
        "operator_id": _require_safe_identifier("operator_id", operator_id),
        "evidence_type": _require_safe_identifier("evidence_type", evidence_type),
        "evidence_ref": _require_safe_identifier("evidence_ref", evidence_ref),
        "evidence_sha256": _require_sha256("evidence_sha256", evidence_sha256),
    }
    if not normalized["dead_letter_id"]:
        raise ValueError("dead_letter_id must not be empty")
    if connection.in_transaction:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "ACTIVE_TRANSACTION",
            "disposition requires a connection without an active transaction",
        )
    started = False
    try:
        connection.execute("BEGIN IMMEDIATE")
        started = True
        _assert_disposition_schema_ready(connection)
        if _runtime_lock_count(connection):
            raise IncrementalEvaluationDeadLetterResolutionError(
                "RUNTIME_EXECUTION_LOCK_PRESENT",
                "disposition requires every runtime execution lease to be free",
            )
        result = _append_disposition_in_transaction(
            connection,
            action=action,
            dead_letter_id=normalized["dead_letter_id"],
            request_id=normalized["request_id"],
            expected_dead_letter_fingerprint=normalized["expected_dead_letter_fingerprint"],
            expected_candidate_version=normalized["expected_candidate_version"],
            reason_code=normalized["reason_code"],
            operator_id=normalized["operator_id"],
            evidence_type=normalized["evidence_type"],
            evidence_ref=normalized["evidence_ref"],
            evidence_sha256=normalized["evidence_sha256"],
            evidence_json=evidence_json,
            requested_supersedes_id=requested_supersedes_id,
            recovery_session_id=None,
            batch_size=None,
            fencing_token=None,
            safety_snapshot={
                "runtime_execution_lock_count": 0,
                "auto_run_evaluation": False,
            },
        )
        connection.commit()
        started = False
        return result
    except IncrementalEvaluationDeadLetterResolutionError:
        if started:
            connection.rollback()
        raise
    except sqlite3.OperationalError as exc:
        if started:
            connection.rollback()
        if "locked" in str(exc).lower():
            raise IncrementalEvaluationDeadLetterResolutionError(
                "LOCKED_RETRYABLE",
                "database is locked; retry the same request_id",
            ) from exc
        raise
    except Exception:
        if started:
            connection.rollback()
        raise


def _append_disposition_in_transaction(
    connection: sqlite3.Connection,
    *,
    action: str,
    dead_letter_id: str,
    request_id: str,
    expected_dead_letter_fingerprint: str,
    expected_candidate_version: str,
    reason_code: str,
    operator_id: str,
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_json: Mapping[str, Any] | None,
    requested_supersedes_id: str | None,
    recovery_session_id: str | None,
    batch_size: int | None,
    fencing_token: int | None,
    safety_snapshot: Mapping[str, Any],
    authorization_disposition_id: str | None = None,
    exact_evidence_json: bool = False,
) -> dict[str, Any]:
    raw_row = connection.execute(
        f"SELECT * FROM {RAW_DEAD_LETTER_TABLE} WHERE dead_letter_id = ?",
        (dead_letter_id,),
    ).fetchone()
    if raw_row is None:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "DEAD_LETTER_NOT_FOUND",
            "incremental evaluation dead-letter row was not found",
        )
    projection = _disposition_projection(connection, raw_row)
    normalized_exact_evidence = dict(evidence_json or {}) if exact_evidence_json else None
    evidence_binding_sha256 = (
        _sha256_json(normalized_exact_evidence) if normalized_exact_evidence is not None else None
    )
    existing = connection.execute(
        f"SELECT * FROM {DISPOSITION_TABLE} WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    if existing is not None:
        replay_payload = {
            "action": action,
            "dead_letter_id": dead_letter_id,
            "request_id": request_id,
            "expected_dead_letter_fingerprint": expected_dead_letter_fingerprint,
            "expected_candidate_version": expected_candidate_version,
            "reason_code": reason_code,
            "operator_id": operator_id,
            "evidence_type": evidence_type,
            "evidence_ref": evidence_ref,
            "evidence_sha256": evidence_sha256,
            "supersedes_disposition_id": _optional_text(existing["supersedes_disposition_id"]),
            "recovery_session_id": recovery_session_id,
            "batch_size": batch_size,
            "authorization_disposition_id": authorization_disposition_id,
        }
        if exact_evidence_json:
            replay_payload["campaign_evidence_json_sha256"] = evidence_binding_sha256
        if str(existing["request_hash"]) != _sha256_json(replay_payload):
            raise IncrementalEvaluationDeadLetterResolutionError(
                "REQUEST_CONFLICT",
                "request_id was already used for a different request",
            )
        if exact_evidence_json:
            try:
                stored_exact_evidence = json.loads(str(existing["evidence_json"] or "{}"))
            except (TypeError, ValueError) as exc:
                raise IncrementalEvaluationDeadLetterResolutionError(
                    "REQUEST_CONFLICT",
                    "request_id has noncanonical stored campaign evidence",
                ) from exc
            if not isinstance(stored_exact_evidence, dict) or _canonical_json(
                stored_exact_evidence
            ) != _canonical_json(normalized_exact_evidence):
                raise IncrementalEvaluationDeadLetterResolutionError(
                    "REQUEST_CONFLICT",
                    "request_id was already used with different campaign evidence",
                )
        return {
            **_public_disposition(existing),
            "idempotent_replay": True,
            "raw_dead_letter_changed": False,
            "no_evaluation_run": True,
            "no_order_commands_created": True,
            "no_order_side_effects": True,
        }
    latest = projection.get("latest_disposition")
    expected_supersedes = None if latest is None else str(latest["disposition_id"])
    supersedes_id = (
        expected_supersedes
        if requested_supersedes_id is None
        else str(requested_supersedes_id).strip()
    )
    request_payload = {
        "action": action,
        "dead_letter_id": dead_letter_id,
        "request_id": request_id,
        "expected_dead_letter_fingerprint": expected_dead_letter_fingerprint,
        "expected_candidate_version": expected_candidate_version,
        "reason_code": reason_code,
        "operator_id": operator_id,
        "evidence_type": evidence_type,
        "evidence_ref": evidence_ref,
        "evidence_sha256": evidence_sha256,
        "supersedes_disposition_id": supersedes_id,
        "recovery_session_id": recovery_session_id,
        "batch_size": batch_size,
        "authorization_disposition_id": authorization_disposition_id,
    }
    if exact_evidence_json:
        request_payload["campaign_evidence_json_sha256"] = evidence_binding_sha256
    request_hash = _sha256_json(request_payload)
    if not projection["chain_valid"]:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "DISPOSITION_CHAIN_INVALID",
            "dead-letter disposition chain is invalid",
        )
    current_fingerprint = _dead_letter_fingerprint(raw_row)
    candidate, current_candidate_version = _candidate_row_and_version(
        connection, str(raw_row["candidate_instance_id"])
    )
    if current_fingerprint != expected_dead_letter_fingerprint:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "DEAD_LETTER_FINGERPRINT_MISMATCH",
            "raw dead-letter evidence changed since preview",
            details={"current_fingerprint": current_fingerprint},
        )
    if current_candidate_version != expected_candidate_version:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "CANDIDATE_VERSION_MISMATCH",
            "candidate evidence changed since preview",
            details={"current_candidate_version": current_candidate_version},
        )
    active = projection.get("active_disposition")
    if action == ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE:
        preview = preview_incremental_evaluation_dead_letter_disposition(
            connection,
            dead_letter_id,
            action=action,
            expected_dead_letter_fingerprint=expected_dead_letter_fingerprint,
            expected_candidate_version=expected_candidate_version,
        )
        if not preview["eligible"]:
            raise IncrementalEvaluationDeadLetterResolutionError(
                "DISPOSITION_NOT_ELIGIBLE",
                "dead-letter is not an obsolete closed candidate",
                details={"reason_codes": preview["reason_codes"]},
            )
    elif action == ACTION_REVOKE:
        if active is None:
            raise IncrementalEvaluationDeadLetterResolutionError(
                "NO_EFFECTIVE_DISPOSITION_TO_REVOKE",
                "there is no effective disposition to revoke",
            )
        if supersedes_id != str(active["disposition_id"]):
            raise IncrementalEvaluationDeadLetterResolutionError(
                "SUPERSEDES_DISPOSITION_MISMATCH",
                "revoke must supersede the current effective disposition",
            )
    elif action in {ACTION_RESET_CANARY, ACTION_RESET_BATCH}:
        if active is not None:
            raise IncrementalEvaluationDeadLetterResolutionError(
                "EFFECTIVE_DISPOSITION_EXISTS",
                "recovery cannot replace an effective disposition",
            )
    elif action == ACTION_VERIFY_CANARY:
        if active is None or active.get("action") != ACTION_RESET_CANARY:
            raise IncrementalEvaluationDeadLetterResolutionError(
                "NO_PENDING_RECOVERY_CANARY",
                "there is no pending recovery canary to verify",
            )
        if supersedes_id != str(active["disposition_id"]):
            raise IncrementalEvaluationDeadLetterResolutionError(
                "SUPERSEDES_DISPOSITION_MISMATCH",
                "verification must supersede the pending recovery canary",
            )
    else:
        raise ValueError("unsupported disposition action")

    sequence_no = len(projection["rows"]) + 1
    disposition_id = new_message_id("incremental_disposition")
    created_at = datetime_to_wire(utc_now())
    if exact_evidence_json:
        stored_evidence = dict(normalized_exact_evidence or {})
    else:
        stored_evidence = dict(evidence_json or {})
        stored_evidence.update(
            {
                "evidence_type": evidence_type,
                "request_payload": request_payload,
                "candidate_state": None if candidate is None else str(candidate["state"]),
                "authorization_disposition_id": authorization_disposition_id,
            }
        )
    connection.execute(
        f"""
        INSERT INTO {DISPOSITION_TABLE} (
            disposition_id, request_id, request_hash, dead_letter_id,
            sequence_no, action, supersedes_disposition_id, reason_code,
            operator_id, expected_dead_letter_fingerprint,
            expected_candidate_version, evidence_ref, evidence_sha256,
            evidence_json, recovery_session_id, batch_size, fencing_token,
            safety_snapshot_json, created_at, observe_only,
            live_sim_allowed, live_real_allowed, auto_run_evaluation
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                1, 0, 0, 0)
        """,
        (
            disposition_id,
            request_id,
            request_hash,
            dead_letter_id,
            sequence_no,
            action,
            supersedes_id,
            reason_code,
            operator_id,
            expected_dead_letter_fingerprint,
            expected_candidate_version,
            evidence_ref,
            evidence_sha256,
            _canonical_json(stored_evidence),
            recovery_session_id,
            batch_size,
            fencing_token,
            _canonical_json(dict(safety_snapshot)),
            created_at,
        ),
    )
    inserted = connection.execute(
        f"SELECT * FROM {DISPOSITION_TABLE} WHERE disposition_id = ?",
        (disposition_id,),
    ).fetchone()
    assert inserted is not None
    return {
        **_public_disposition(inserted),
        "idempotent_replay": False,
        "raw_dead_letter_changed": False,
        "no_evaluation_run": True,
        "no_order_commands_created": True,
        "no_order_side_effects": True,
    }


def _build_effective_row(
    connection: sqlite3.Connection,
    raw_row: sqlite3.Row,
) -> dict[str, Any]:
    candidate, candidate_version = _candidate_row_and_version(
        connection, str(raw_row["candidate_instance_id"])
    )
    active_queue = connection.execute(
        "SELECT * FROM incremental_evaluation_queue WHERE candidate_instance_id = ?",
        (str(raw_row["candidate_instance_id"]),),
    ).fetchone()
    projection = _disposition_projection(connection, raw_row)
    terminal = _historical_terminal_evidence(raw_row, candidate, active_queue)
    active = projection.get("active_disposition")
    if not projection["chain_valid"]:
        bucket = BUCKET_INVALID_DISPOSITION
    elif active is not None:
        action = str(active["action"])
        if action == ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE:
            bucket = (
                BUCKET_HISTORICAL_DISPOSED
                if terminal and str(active["expected_candidate_version"]) == candidate_version
                else BUCKET_INVALID_DISPOSITION
            )
        elif action == ACTION_RESET_CANARY:
            bucket = BUCKET_RECOVERY_PENDING
        elif action in {ACTION_VERIFY_CANARY, ACTION_RESET_BATCH}:
            bucket = BUCKET_RECOVERY_VERIFIED
        else:
            bucket = BUCKET_INVALID_DISPOSITION
    elif candidate is not None and str(candidate["state"]) != CandidateState.CLOSED.value:
        bucket = BUCKET_ACTIVE_UNRESOLVED
    elif terminal:
        bucket = BUCKET_HISTORICAL_PENDING
    else:
        bucket = BUCKET_MANUAL_REVIEW
    return {
        "dead_letter_id": str(raw_row["dead_letter_id"]),
        "candidate_instance_id": str(raw_row["candidate_instance_id"]),
        "bucket": bucket,
        "effective_blocker": bucket in BLOCKING_BUCKETS,
        "dead_letter_fingerprint": _dead_letter_fingerprint(raw_row),
        "candidate_version": candidate_version,
        "raw_dead_letter": _public_raw_dead_letter(raw_row),
        "candidate": _public_candidate(candidate),
        "active_queue": _public_queue(active_queue),
        "effective_disposition": active,
        "disposition_chain_valid": bool(projection["chain_valid"]),
        "disposition_projection": projection,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
    }


def _disposition_projection(
    connection: sqlite3.Connection,
    raw_row: sqlite3.Row,
) -> dict[str, Any]:
    if not _disposition_schema_ready(connection):
        return {
            "chain_valid": False,
            "rows": [],
            "latest_disposition": None,
            "active_disposition": None,
            "reason_codes": ["DISPOSITION_SCHEMA_INVALID"],
        }
    rows = connection.execute(
        f"""
        SELECT * FROM {DISPOSITION_TABLE}
        WHERE dead_letter_id = ?
        ORDER BY sequence_no, created_at, disposition_id
        """,
        (str(raw_row["dead_letter_id"]),),
    ).fetchall()
    expected_sequence = 1
    previous_id: str | None = None
    active: dict[str, Any] | None = None
    chain_valid = True
    reason_codes: list[str] = []
    raw_fingerprint = _dead_letter_fingerprint(raw_row)
    for row in rows:
        action = str(row["action"])
        supersedes = _optional_text(row["supersedes_disposition_id"])
        if int(row["sequence_no"]) != expected_sequence:
            chain_valid = False
            reason_codes.append("DISPOSITION_SEQUENCE_GAP")
        if supersedes != previous_id:
            chain_valid = False
            reason_codes.append("DISPOSITION_SUPERSEDES_CHAIN_INVALID")
        if action not in _ACTIONS or not _disposition_row_contract_valid(row):
            chain_valid = False
            reason_codes.append("DISPOSITION_ROW_CONTRACT_INVALID")
        if str(row["expected_dead_letter_fingerprint"]) != raw_fingerprint:
            chain_valid = False
            reason_codes.append("DISPOSITION_RAW_FINGERPRINT_INVALID")
        public = _public_disposition(row)
        if action == ACTION_REVOKE:
            if active is None or supersedes != str(active["disposition_id"]):
                chain_valid = False
                reason_codes.append("DISPOSITION_REVOKE_CHAIN_INVALID")
            active = None
        elif action == ACTION_VERIFY_CANARY:
            if active is None or active.get("action") != ACTION_RESET_CANARY:
                chain_valid = False
                reason_codes.append("CANARY_VERIFY_CHAIN_INVALID")
            active = public
        else:
            if active is not None:
                chain_valid = False
                reason_codes.append("MULTIPLE_EFFECTIVE_DISPOSITIONS")
            active = public
        expected_sequence += 1
        previous_id = str(row["disposition_id"])
    latest = None if not rows else _public_disposition(rows[-1])
    return {
        "chain_valid": chain_valid,
        "rows": [_public_disposition(row) for row in rows],
        "latest_disposition": latest,
        "active_disposition": active,
        "reason_codes": list(dict.fromkeys(reason_codes)),
    }


def _disposition_row_contract_valid(row: sqlite3.Row) -> bool:
    if not all(
        _SHA256_RE.fullmatch(str(row[field]))
        for field in (
            "request_hash",
            "expected_dead_letter_fingerprint",
            "expected_candidate_version",
            "evidence_sha256",
        )
    ):
        return False
    if (
        not bool(row["observe_only"])
        or bool(row["live_sim_allowed"])
        or bool(row["live_real_allowed"])
        or bool(row["auto_run_evaluation"])
    ):
        return False
    try:
        evidence = json.loads(str(row["evidence_json"] or "{}"))
        safety = json.loads(str(row["safety_snapshot_json"] or "{}"))
    except (TypeError, ValueError):
        return False
    if not isinstance(evidence, dict) or not isinstance(safety, dict):
        return False
    payload = evidence.get("request_payload")
    if isinstance(payload, dict) and _sha256_json(payload) != str(row["request_hash"]):
        return False
    return True


def _historical_terminal_evidence(
    raw_row: sqlite3.Row,
    candidate: sqlite3.Row | None,
    active_queue: sqlite3.Row | None,
) -> bool:
    if candidate is None or active_queue is not None:
        return False
    if str(candidate["state"]) != CandidateState.CLOSED.value:
        return False
    closed_at = _optional_text(candidate["closed_at"])
    if closed_at is None:
        return False
    try:
        return parse_timestamp(closed_at, "closed_at") >= parse_timestamp(
            str(raw_row["last_queue_updated_at"]),
            "last_queue_updated_at",
        )
    except (TypeError, ValueError):
        return False


def _recovery_candidate_reason_codes(
    connection: sqlite3.Connection,
    raw_row: sqlite3.Row,
) -> list[str]:
    reason_codes: list[str] = []
    candidate = connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (str(raw_row["candidate_instance_id"]),),
    ).fetchone()
    if candidate is None:
        reason_codes.append("RECOVERY_CANDIDATE_MISSING")
    elif str(candidate["state"]) == CandidateState.CLOSED.value:
        reason_codes.append("RECOVERY_CANDIDATE_CLOSED")
    queue = connection.execute(
        "SELECT 1 FROM incremental_evaluation_queue WHERE candidate_instance_id = ? LIMIT 1",
        (str(raw_row["candidate_instance_id"]),),
    ).fetchone()
    if queue is not None:
        reason_codes.append("ACTIVE_QUEUE_EXISTS")
    source_event_id = _optional_text(raw_row["source_event_id"])
    if source_event_id is None:
        reason_codes.append("RECOVERY_SOURCE_LINEAGE_MISSING")
    else:
        source = connection.execute(
            """
            SELECT active FROM candidate_source_events
            WHERE source_event_id = ? AND candidate_instance_id = ?
            """,
            (source_event_id, str(raw_row["candidate_instance_id"])),
        ).fetchone()
        if source is None:
            reason_codes.append("RECOVERY_SOURCE_LINEAGE_MISSING")
        elif not bool(source["active"]):
            reason_codes.append("RECOVERY_SOURCE_NOT_ACTIVE")
    return reason_codes


def _insert_recovery_queue_row(
    connection: sqlite3.Connection,
    raw_row: sqlite3.Row,
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO incremental_evaluation_queue (
            candidate_instance_id, trade_date, code, reason, source_event_id,
            priority, enqueued_at, updated_at, attempts, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
        """,
        (
            raw_row["candidate_instance_id"],
            raw_row["trade_date"],
            raw_row["code"],
            raw_row["reason"],
            raw_row["source_event_id"],
            int(raw_row["priority"]),
            now,
            now,
        ),
    )


def _normalize_expected_versions(
    dead_letter_ids: Sequence[str],
    expected_versions: Sequence[Mapping[str, Any]] | Mapping[str, Any],
) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    if isinstance(expected_versions, Mapping):
        items: Sequence[Any] = [
            {
                "dead_letter_id": dead_letter_id,
                **dict(expected_versions.get(dead_letter_id, {})),
            }
            for dead_letter_id in dead_letter_ids
        ]
    else:
        items = list(expected_versions)
    for item in items:
        if not isinstance(item, Mapping):
            raise ValueError("expected_versions must contain mappings")
        dead_letter_id = str(item.get("dead_letter_id") or "").strip()
        fingerprint = item.get("dead_letter_fingerprint") or item.get(
            "expected_dead_letter_fingerprint"
        )
        candidate_version = item.get("candidate_version") or item.get("expected_candidate_version")
        if dead_letter_id:
            result[dead_letter_id] = {
                "dead_letter_fingerprint": _require_sha256(
                    "expected_dead_letter_fingerprint", fingerprint
                ),
                "candidate_version": _require_sha256(
                    "expected_candidate_version", candidate_version
                ),
            }
    if set(result) != set(dead_letter_ids):
        raise ValueError("expected_versions must match dead_letter_ids exactly")
    return result


def _assert_recovery_runtime_safe(
    connection: sqlite3.Connection,
    settings: Settings,
) -> None:
    reason_codes: list[str] = []
    configured_env = os.environ.get(ENV_FILE_PATH_ENV, "").strip()
    env_path = Path(configured_env).expanduser() if configured_env else None
    if env_path is None:
        reason_codes.append("EXPLICIT_TRADING_ENV_FILE_REQUIRED")
    elif not env_path.is_file():
        reason_codes.append("TRADING_ENV_FILE_NOT_FOUND")
    elif _same_path(env_path, DEFAULT_ENV_FILE_PATH):
        reason_codes.append("DEFAULT_DOTENV_FORBIDDEN_FOR_RECOVERY")
    if settings.trading_profile is not TradingProfile.OBSERVE:
        reason_codes.append("TRADING_PROFILE_NOT_OBSERVE")
    if settings.trading_mode is not TradingMode.OBSERVE:
        reason_codes.append("TRADING_MODE_NOT_OBSERVE")
    if settings.trading_allow_live_sim:
        reason_codes.append("LIVE_SIM_ALLOWED")
    if settings.trading_allow_live_real:
        reason_codes.append("LIVE_REAL_ALLOWED")
    if not settings.live_sim_kill_switch:
        reason_codes.append("LIVE_SIM_KILL_SWITCH_OFF")
    if settings.incremental_evaluation_worker_enabled:
        reason_codes.append("INCREMENTAL_EVALUATION_WORKER_ENABLED")
    for name in _PRODUCER_SETTING_NAMES:
        if bool(getattr(settings, name, True)):
            reason_codes.append(f"COMMAND_PRODUCER_ENABLED:{name.upper()}")
    if env_path is not None and env_path.is_file():
        theme_value = _explicit_env_value(env_path, "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS")
        if theme_value is None:
            reason_codes.append("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_EXPLICIT")
        elif theme_value.strip().lower() not in _FALSE_VALUES:
            reason_codes.append("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_FALSE")
    db_path = _connection_database_path(connection)
    if db_path is None or not _same_path(Path(settings.trading_db_path), db_path):
        reason_codes.append("DB_PATH_DOES_NOT_MATCH_SAFE_ENV")
    if reason_codes:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "RECOVERY_RUNTIME_UNSAFE",
            "guarded recovery runtime contract is not satisfied",
            details={"reason_codes": list(dict.fromkeys(reason_codes))},
        )


def _runtime_safety_snapshot(settings: Settings) -> dict[str, Any]:
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
    }


def _assert_verified_canary(
    connection: sqlite3.Connection,
    recovery_session_id: str,
    disposition_id: str | None,
) -> None:
    if not disposition_id:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "VERIFIED_CANARY_DISPOSITION_REQUIRED",
            "batch recovery requires an explicit verified canary disposition",
        )
    row = connection.execute(
        f"""
        SELECT * FROM {DISPOSITION_TABLE}
        WHERE disposition_id = ? AND recovery_session_id = ?
        """,
        (str(disposition_id), recovery_session_id),
    ).fetchone()
    if row is None or str(row["action"]) != ACTION_VERIFY_CANARY:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "VERIFIED_CANARY_NOT_FOUND",
            "verified canary was not found in the recovery session",
        )
    raw_row = connection.execute(
        f"SELECT * FROM {RAW_DEAD_LETTER_TABLE} WHERE dead_letter_id = ?",
        (str(row["dead_letter_id"]),),
    ).fetchone()
    if raw_row is None:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "VERIFIED_CANARY_NOT_EFFECTIVE",
            "verified canary raw dead-letter is missing",
        )
    projection = _disposition_projection(connection, raw_row)
    active = projection.get("active_disposition")
    if (
        not projection["chain_valid"]
        or active is None
        or str(active["disposition_id"]) != str(disposition_id)
        or str(active["action"]) != ACTION_VERIFY_CANARY
    ):
        raise IncrementalEvaluationDeadLetterResolutionError(
            "VERIFIED_CANARY_NOT_EFFECTIVE",
            "verified canary is revoked, superseded, or invalid",
        )


def _find_recovery_canary(
    connection: sqlite3.Connection,
    recovery_session_id: str,
    disposition_id: str | None,
    dead_letter_id: str | None,
) -> sqlite3.Row:
    clauses = ["recovery_session_id = ?", "action = ?"]
    params: list[Any] = [recovery_session_id, ACTION_RESET_CANARY]
    if disposition_id:
        clauses.append("disposition_id = ?")
        params.append(str(disposition_id))
    if dead_letter_id:
        clauses.append("dead_letter_id = ?")
        params.append(str(dead_letter_id))
    row = connection.execute(
        f"""
        SELECT * FROM {DISPOSITION_TABLE}
        WHERE {" AND ".join(clauses)}
        ORDER BY created_at DESC, disposition_id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if row is None:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "RECOVERY_CANARY_NOT_FOUND",
            "pending recovery canary was not found",
        )
    return row


def _canary_verification_reason_codes(
    connection: sqlite3.Connection,
    canary: sqlite3.Row,
) -> list[str]:
    reason_codes: list[str] = []
    dead_letter = connection.execute(
        f"SELECT * FROM {RAW_DEAD_LETTER_TABLE} WHERE dead_letter_id = ?",
        (str(canary["dead_letter_id"]),),
    ).fetchone()
    if dead_letter is None:
        return ["CANARY_RAW_DEAD_LETTER_MISSING"]
    candidate_id = str(dead_letter["candidate_instance_id"])
    queue = connection.execute(
        "SELECT 1 FROM incremental_evaluation_queue WHERE candidate_instance_id = ? LIMIT 1",
        (candidate_id,),
    ).fetchone()
    if queue is not None:
        reason_codes.append("CANARY_QUEUE_STILL_ACTIVE")
    newer_raw = connection.execute(
        f"""
        SELECT 1 FROM {RAW_DEAD_LETTER_TABLE}
        WHERE candidate_instance_id = ?
          AND (dead_lettered_at > ? OR
               (dead_lettered_at = ? AND dead_letter_id > ?))
        LIMIT 1
        """,
        (
            candidate_id,
            dead_letter["dead_lettered_at"],
            dead_letter["dead_lettered_at"],
            dead_letter["dead_letter_id"],
        ),
    ).fetchone()
    if newer_raw is not None:
        reason_codes.append("NEWER_DEAD_LETTER_GENERATION_PRESENT")
    created_at = str(canary["created_at"])
    for table_name, code in (
        ("strategy_observations_latest", "CANARY_STRATEGY_EVIDENCE_MISSING"),
        ("risk_observations_latest", "CANARY_RISK_EVIDENCE_MISSING"),
    ):
        row = connection.execute(
            f"SELECT evaluated_at FROM {table_name} WHERE candidate_instance_id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None or str(row["evaluated_at"]) < created_at:
            reason_codes.append(code)
    try:
        safety = json.loads(str(canary["safety_snapshot_json"] or "{}"))
    except (TypeError, ValueError):
        safety = {}
    expected_commands = safety.get("gateway_commands") if isinstance(safety, dict) else None
    if expected_commands != _gateway_command_snapshot(connection):
        reason_codes.append("GATEWAY_COMMAND_STATE_CHANGED_SINCE_CANARY")
    expected_order_artifacts = safety.get("order_artifacts") if isinstance(safety, dict) else None
    if expected_order_artifacts != _order_artifact_snapshot(connection):
        reason_codes.append("ORDER_ARTIFACT_STATE_CHANGED_SINCE_CANARY")
    return reason_codes


def _gateway_command_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT command_id, command_type, status, attempts, created_at,
               available_at, dispatched_at, completed_at, expires_at
        FROM gateway_commands
        ORDER BY command_id
        """
    ).fetchall()
    payload = [{key: row[key] for key in row.keys()} for row in rows]
    order_command_types = {
        "send_order",
        "cancel_order",
        "_".join(("modify", "order")),
    }
    order_count = sum(str(row["command_type"]).lower() in order_command_types for row in rows)
    return {
        "total_count": len(rows),
        "order_count": order_count,
        "sha256": _sha256_json(payload),
    }


def _order_artifact_snapshot(connection: sqlite3.Connection) -> dict[str, int]:
    placeholders = ",".join("?" for _ in _ORDER_ARTIFACT_TABLES)
    existing = {
        str(row["name"])
        for row in connection.execute(
            f"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ({placeholders})",
            _ORDER_ARTIFACT_TABLES,
        ).fetchall()
    }
    missing = sorted(set(_ORDER_ARTIFACT_TABLES) - existing)
    if missing:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "ORDER_ARTIFACT_SCHEMA_INCOMPLETE",
            "order artifact safety tables are missing",
            details={"missing_tables": missing},
        )
    return {
        table_name: int(
            connection.execute(f'SELECT COUNT(*) AS count FROM "{table_name}"').fetchone()["count"]
            or 0
        )
        for table_name in _ORDER_ARTIFACT_TABLES
    }


def _assert_disposition_schema_ready(connection: sqlite3.Connection) -> None:
    if not _disposition_schema_ready(connection):
        raise IncrementalEvaluationDeadLetterResolutionError(
            "DISPOSITION_SCHEMA_INVALID",
            "schema 61 append-only disposition contract is not ready",
        )


def _disposition_schema_ready(connection: sqlite3.Connection) -> bool:
    try:
        if not _table_exists(connection, DISPOSITION_TABLE):
            return False
        schema_row = connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()
        schema_value = (
            -1
            if schema_row is None
            else schema_row["value"]
            if isinstance(schema_row, sqlite3.Row)
            else schema_row[0]
        )
        schema_version = int(schema_value)
    except (sqlite3.Error, IndexError, KeyError, TypeError, ValueError):
        return False
    if schema_version < 61:
        return False
    required_columns = {
        "disposition_id",
        "request_id",
        "request_hash",
        "dead_letter_id",
        "sequence_no",
        "action",
        "supersedes_disposition_id",
        "reason_code",
        "operator_id",
        "expected_dead_letter_fingerprint",
        "expected_candidate_version",
        "evidence_ref",
        "evidence_sha256",
        "evidence_json",
        "recovery_session_id",
        "batch_size",
        "fencing_token",
        "safety_snapshot_json",
        "created_at",
        "observe_only",
        "live_sim_allowed",
        "live_real_allowed",
        "auto_run_evaluation",
    }
    try:
        columns = {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
            for row in connection.execute(
                "SELECT name FROM pragma_table_info(?)",
                (DISPOSITION_TABLE,),
            ).fetchall()
        }
    except (sqlite3.Error, IndexError, KeyError, TypeError):
        return False
    if not required_columns <= columns:
        return False
    required_indexes = {
        "idx_incremental_evaluation_dead_letter_candidate_time",
        "idx_incremental_dead_letter_disposition_effective",
        "idx_incremental_dead_letter_disposition_session",
    }
    try:
        indexes = {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
    except (sqlite3.Error, IndexError, KeyError, TypeError):
        return False
    if not required_indexes <= indexes:
        return False
    required_triggers = {
        "trg_incremental_evaluation_dead_letters_no_update",
        "trg_incremental_evaluation_dead_letters_no_delete",
        "trg_incremental_dead_letter_dispositions_no_update",
        "trg_incremental_dead_letter_dispositions_no_delete",
    }
    try:
        trigger_rows = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
        trigger_sql = {
            str(row["name"] if isinstance(row, sqlite3.Row) else row[0]): str(
                (row["sql"] if isinstance(row, sqlite3.Row) else row[1]) or ""
            ).upper()
            for row in trigger_rows
        }
    except (sqlite3.Error, IndexError, KeyError, TypeError):
        return False
    if not required_triggers <= set(trigger_sql):
        return False
    trigger_contracts = {
        "trg_incremental_evaluation_dead_letters_no_update": (
            "BEFORE UPDATE ON INCREMENTAL_EVALUATION_DEAD_LETTERS",
            "RAISE",
        ),
        "trg_incremental_evaluation_dead_letters_no_delete": (
            "BEFORE DELETE ON INCREMENTAL_EVALUATION_DEAD_LETTERS",
            "RAISE",
        ),
        "trg_incremental_dead_letter_dispositions_no_update": (
            "BEFORE UPDATE ON INCREMENTAL_EVALUATION_DEAD_LETTER_DISPOSITIONS",
            "RAISE",
        ),
        "trg_incremental_dead_letter_dispositions_no_delete": (
            "BEFORE DELETE ON INCREMENTAL_EVALUATION_DEAD_LETTER_DISPOSITIONS",
            "RAISE",
        ),
    }
    return all(
        all(fragment in trigger_sql[name] for fragment in fragments)
        for name, fragments in trigger_contracts.items()
    )


def _runtime_lock_count(connection: sqlite3.Connection) -> int:
    if not _table_exists(connection, "runtime_execution_locks"):
        return 1
    row = connection.execute("SELECT COUNT(*) AS count FROM runtime_execution_locks").fetchone()
    return int(row["count"] if isinstance(row, sqlite3.Row) else row[0])


def _assert_only_current_runtime_lock(
    connection: sqlite3.Connection,
    owner_id: str,
) -> None:
    rows = connection.execute("SELECT owner_id FROM runtime_execution_locks").fetchall()
    if len(rows) != 1 or str(rows[0]["owner_id"]) != owner_id:
        raise IncrementalEvaluationDeadLetterResolutionError(
            "RUNTIME_EXECUTION_LOCK_CONFLICT",
            "a concurrent runtime execution lease appeared during recovery",
        )


def _candidate_row_and_version(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> tuple[sqlite3.Row | None, str]:
    row = connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (candidate_instance_id,),
    ).fetchone()
    snapshot = (
        {"candidate_instance_id": candidate_instance_id, "missing": True}
        if row is None
        else {
            "candidate_instance_id": str(row["candidate_instance_id"]),
            "state": str(row["state"]),
            "state_updated_at": row["state_updated_at"],
            "closed_at": row["closed_at"],
            "last_seen_at": row["last_seen_at"],
            "active_source_count": int(row["active_source_count"] or 0),
            "primary_source_type": row["primary_source_type"],
            "primary_source_id": row["primary_source_id"],
        }
    )
    return row, _sha256_json(snapshot)


def _dead_letter_fingerprint(row: sqlite3.Row) -> str:
    return _sha256_json({key: row[key] for key in row.keys()})


def _public_raw_dead_letter(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "dead_letter_id": str(row["dead_letter_id"]),
        "candidate_instance_id": str(row["candidate_instance_id"]),
        "trade_date": str(row["trade_date"]),
        "code": str(row["code"]),
        "reason": str(row["reason"]),
        "source_event_id": _optional_text(row["source_event_id"]),
        "priority": int(row["priority"]),
        "attempts": int(row["attempts"]),
        "last_error_present": bool(str(row["last_error"] or "").strip()),
        "last_error_sha256": hashlib.sha256(
            str(row["last_error"] or "").encode("utf-8")
        ).hexdigest(),
        "status": str(row["status"]),
        "last_queue_updated_at": str(row["last_queue_updated_at"]),
        "dead_lettered_at": str(row["dead_lettered_at"]),
    }


def _public_candidate(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "candidate_instance_id": str(row["candidate_instance_id"]),
        "trade_date": str(row["trade_date"]),
        "code": str(row["code"]),
        "state": str(row["state"]),
        "state_updated_at": str(row["state_updated_at"]),
        "closed_at": _optional_text(row["closed_at"]),
        "last_seen_at": str(row["last_seen_at"]),
        "active_source_count": int(row["active_source_count"] or 0),
        "primary_source_type": str(row["primary_source_type"]),
        "primary_source_id": str(row["primary_source_id"]),
    }


def _public_queue(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "candidate_instance_id": str(row["candidate_instance_id"]),
        "reason": str(row["reason"]),
        "source_event_id": _optional_text(row["source_event_id"]),
        "attempts": int(row["attempts"]),
        "updated_at": str(row["updated_at"]),
    }


def _public_disposition(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "disposition_id": str(row["disposition_id"]),
        "dead_letter_id": str(row["dead_letter_id"]),
        "sequence_no": int(row["sequence_no"]),
        "action": str(row["action"]),
        "supersedes_disposition_id": _optional_text(row["supersedes_disposition_id"]),
        "reason_code": str(row["reason_code"]),
        "operator_id": str(row["operator_id"]),
        "evidence_ref": str(row["evidence_ref"]),
        "evidence_sha256": str(row["evidence_sha256"]),
        "expected_dead_letter_fingerprint": str(row["expected_dead_letter_fingerprint"]),
        "expected_candidate_version": str(row["expected_candidate_version"]),
        "recovery_session_id": _optional_text(row["recovery_session_id"]),
        "batch_size": None if row["batch_size"] is None else int(row["batch_size"]),
        "fencing_token": (None if row["fencing_token"] is None else int(row["fencing_token"])),
        "created_at": str(row["created_at"]),
        "observe_only": bool(row["observe_only"]),
        "live_sim_allowed": bool(row["live_sim_allowed"]),
        "live_real_allowed": bool(row["live_real_allowed"]),
        "auto_run_evaluation": bool(row["auto_run_evaluation"]),
    }


def _require_safe_identifier(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if not _SAFE_IDENTIFIER_RE.fullmatch(normalized):
        raise IncrementalEvaluationDeadLetterResolutionError(
            f"INVALID_{name.upper()}",
            f"{name} must be a safe opaque identifier",
        )
    return normalized


def _require_sha256(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if not _SHA256_RE.fullmatch(normalized):
        raise IncrementalEvaluationDeadLetterResolutionError(
            f"INVALID_{name.upper()}",
            f"{name} must be a lowercase SHA-256 digest",
        )
    return normalized


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _connection_database_path(connection: sqlite3.Connection) -> Path | None:
    row = connection.execute("SELECT file FROM pragma_database_list WHERE name = 'main'").fetchone()
    if row is None:
        return None
    value = row["file"] if isinstance(row, sqlite3.Row) else row[0]
    return Path(str(value)).expanduser() if str(value).strip() else None


def _explicit_env_value(path: Path, env_name: str) -> str | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    value: str | None = None
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, candidate = line.split("=", 1)
        if key.strip() == env_name:
            value = candidate.strip().strip("'\"")
    return value


def _same_path(first: Path, second: Path) -> bool:
    try:
        return first.expanduser().resolve() == second.expanduser().resolve()
    except OSError:
        return False


def _optional_text(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
