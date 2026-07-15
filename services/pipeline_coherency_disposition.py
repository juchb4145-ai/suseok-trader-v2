from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, TypedDict

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json
from storage.gateway_order_broker_boundary import get_effective_order_broker_boundary

from services.pipeline_coherency import build_pipeline_coherency_rca_status

ACTION_DISPOSE_EXPIRED_PLAN_READY = "DISPOSE_EXPIRED_PLAN_READY"
ACTION_DISPOSE_ORPHAN = "DISPOSE_ORPHAN_PIPELINE_OBSERVATION"
ACTION_DISPOSE_STALE_OTHER_DATE = "DISPOSE_STALE_OTHER_DATE"
ACTION_REVOKE = "REVOKE"

DISPOSITION_TABLE = "pipeline_coherency_dispositions"
_ACTIONS = {
    ACTION_DISPOSE_EXPIRED_PLAN_READY,
    ACTION_DISPOSE_ORPHAN,
    ACTION_DISPOSE_STALE_OTHER_DATE,
    ACTION_REVOKE,
}
_ACTIVE_INTENT_STATUSES = {"CREATED", "COMMAND_QUEUED"}
_TERMINAL_ORDER_STATUSES = {
    "BROKER_REJECTED",
    "FILLED",
    "CANCELLED",
    "FAILED",
    "EXPIRED",
    "ORDER_EXPIRED",
    "CANCEL_ACKED",
    "CANCEL_REJECTED",
    "EXIT_FILLED",
    "REJECTED",
    "CONVERTED_TO_DRY_RUN_ORDER",
    "CONVERTED_TO_EXIT_ORDER",
    "SIMULATED_FILLED",
    "SIMULATED_CANCELLED",
    "SIMULATED_REJECTED",
    "CLOSED",
}
_SAFE_GATEWAY_COMMAND_STATUSES = {
    "ACKED",
    "BROKER_ACCEPTED",
    "CANCELLED",
    "CHEJAN_CONFIRMED",
    "EXPIRED",
    "FAILED",
    "REJECTED",
}
_KNOWN_BOUNDARY_STATES = {
    "CLAIMED",
    "GATEWAY_STARTED",
    "PRE_ACK_RECORDED",
    "BROKER_ACCEPTED",
    "CHEJAN_CONFIRMED",
    "UNCONFIRMED",
    "RESOLVED_BROKER_NOT_REACHED",
}
_REQUIRED_COLUMNS = {
    "disposition_id",
    "request_id",
    "request_hash",
    "candidate_instance_id",
    "subject_key",
    "trade_date",
    "order_plan_id",
    "sequence_no",
    "action",
    "supersedes_disposition_id",
    "reason_code",
    "operator_id",
    "expected_pipeline_fingerprint",
    "expected_subject_version",
    "expected_source_fingerprint",
    "expected_candidate_fingerprint",
    "expected_downstream_fingerprint",
    "expected_boundary_fingerprint",
    "evidence_type",
    "evidence_ref",
    "evidence_sha256",
    "evidence_json",
    "safety_snapshot_json",
    "created_at",
    "observe_only",
    "live_sim_allowed",
    "live_real_allowed",
    "order_commands_allowed",
    "not_order_intent",
    "no_order_side_effects",
    "auto_run_evaluation",
}
_EXPECTED_SCHEMA_VERSION = 62
_EXPECTED_SCHEMA_SQL_SHA256 = {
    ("table", DISPOSITION_TABLE): (
        "ca63469c6854d5d505072106a36e0c2c3e8b53c8bb7023076eb80a32bd2da0d0"
    ),
    ("index", "idx_pipeline_coherency_disposition_effective"): (
        "c4f56dea6b2fb74d80fcf9678c4b04b8e5e0efb0f9d35b42e74b08a11686b1d1"
    ),
    ("index", "idx_pipeline_coherency_disposition_action_created"): (
        "f6eb61a80dee95bec6fec5c466a87f61338c40e096afd77f75ae382c5569d946"
    ),
    ("trigger", "trg_pipeline_coherency_dispositions_no_update"): (
        "cfdb07cae97e840e4973088e6b19232dec6e083bea65cfe7d592dff22373956e"
    ),
    ("trigger", "trg_pipeline_coherency_dispositions_no_delete"): (
        "7d900ca05a201d28d287b0dec108846586a0242a8e413d5cf61999d933ff08a4"
    ),
}
class _IndexContract(TypedDict):
    columns: tuple[str, ...]
    descending: tuple[bool, ...]


_EXPECTED_INDEX_CONTRACTS: dict[str, _IndexContract] = {
    "idx_pipeline_coherency_disposition_effective": {
        "columns": ("subject_key", "sequence_no"),
        "descending": (False, True),
    },
    "idx_pipeline_coherency_disposition_action_created": {
        "columns": ("action", "created_at"),
        "descending": (False, True),
    },
}
_EXPECTED_TRIGGER_NAMES = {
    "trg_pipeline_coherency_dispositions_no_update",
    "trg_pipeline_coherency_dispositions_no_delete",
}


class PipelineCoherencyDispositionError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "details": self.details}


def preview_pipeline_coherency_disposition(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    candidate_instance_id: str,
    action: str,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    normalized_action = _require_action(action)
    normalized_trade_date = _require_text("trade_date", trade_date)
    candidate_id = _require_text("candidate_instance_id", candidate_instance_id)
    _assert_schema_ready(connection)
    observed_at = as_of or utc_now()
    rca = build_pipeline_coherency_rca_status(
        connection,
        trade_date=normalized_trade_date,
        candidate_instance_id=candidate_id,
        limit=1,
        offset=0,
        disposition_resolver=(
            lambda resolved_trade_date, subjects: resolve_pipeline_coherency_dispositions(
                connection,
                resolved_trade_date,
                subjects,
                as_of=observed_at,
            )
        ),
        as_of=observed_at,
    )
    items = list(rca.get("items") or [])
    if len(items) != 1:
        raise PipelineCoherencyDispositionError(
            "PIPELINE_SUBJECT_NOT_FOUND",
            "the exact pipeline qualification subject was not found",
            details={"trade_date": normalized_trade_date, "candidate_instance_id": candidate_id},
        )
    item = dict(items[0])
    subject = _subject_summary(item)
    current = _current_cas_snapshot(
        connection,
        subject=subject,
        as_of=observed_at,
    )
    chain = _project_chain(connection, _subject_key(normalized_trade_date, candidate_id))
    expected_action = _required_action(subject)
    reasons: list[str] = []
    if not chain["valid"]:
        reasons.append("PIPELINE_DISPOSITION_CHAIN_INVALID")
    effective = chain.get("effective")
    if normalized_action == ACTION_REVOKE:
        if not isinstance(effective, Mapping):
            reasons.append("EFFECTIVE_PIPELINE_DISPOSITION_REQUIRED")
    else:
        if expected_action is None:
            reasons.append("PIPELINE_SUBJECT_NOT_DISPOSITION_ELIGIBLE")
        elif normalized_action != expected_action:
            reasons.append("PIPELINE_DISPOSITION_ACTION_MISMATCH")
        if isinstance(effective, Mapping):
            reasons.append("PIPELINE_SUBJECT_ALREADY_DISPOSED")
        reasons.extend(_eligibility_reasons(subject, current, action=normalized_action))
    reasons = _dedupe(reasons)
    return {
        "status": "ELIGIBLE" if not reasons else "BLOCKED",
        "eligible": not reasons,
        "reason_codes": reasons,
        "action": normalized_action,
        "expected_action": expected_action,
        "trade_date": normalized_trade_date,
        "candidate_instance_id": candidate_id,
        "subject_key": _subject_key(normalized_trade_date, candidate_id),
        "classification": subject.get("classification"),
        "latest_plan_status": subject.get("latest_plan_status"),
        "latest_plan_unexpired": subject.get("latest_plan_unexpired"),
        "pipeline_fingerprint": subject.get("pipeline_fingerprint"),
        "subject_version": subject.get("subject_version"),
        "source_fingerprint": current["source_fingerprint"],
        "candidate_fingerprint": current["candidate_fingerprint"],
        "downstream_fingerprint": current["downstream_fingerprint"],
        "boundary_fingerprint": current["boundary_fingerprint"],
        "downstream": current["downstream_public"],
        "source": current["source_public"],
        "candidate": current["candidate_public"],
        "effective_disposition": dict(effective or {}),
        "chain_valid": bool(chain["valid"]),
        "next_sequence_no": int(chain["next_sequence_no"]),
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "auto_run_evaluation": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }


def record_pipeline_coherency_disposition(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    candidate_instance_id: str,
    action: str,
    request_id: str,
    expected_pipeline_fingerprint: str,
    expected_subject_version: str,
    expected_source_fingerprint: str,
    expected_candidate_fingerprint: str,
    expected_downstream_fingerprint: str,
    expected_boundary_fingerprint: str,
    reason_code: str,
    operator_id: str,
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_json: Mapping[str, Any] | None = None,
    safety_snapshot: Mapping[str, Any] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    normalized_action = _require_action(action)
    if normalized_action == ACTION_REVOKE:
        raise ValueError("use revoke_pipeline_coherency_disposition for REVOKE")
    return _append_pipeline_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_instance_id,
        action=normalized_action,
        request_id=request_id,
        expected_pipeline_fingerprint=expected_pipeline_fingerprint,
        expected_subject_version=expected_subject_version,
        expected_source_fingerprint=expected_source_fingerprint,
        expected_candidate_fingerprint=expected_candidate_fingerprint,
        expected_downstream_fingerprint=expected_downstream_fingerprint,
        expected_boundary_fingerprint=expected_boundary_fingerprint,
        reason_code=reason_code,
        operator_id=operator_id,
        evidence_type=evidence_type,
        evidence_ref=evidence_ref,
        evidence_sha256=evidence_sha256,
        evidence_json=evidence_json,
        safety_snapshot=safety_snapshot,
        as_of=as_of,
    )


def revoke_pipeline_coherency_disposition(
    connection: sqlite3.Connection,
    **kwargs: Any,
) -> dict[str, Any]:
    requested_action = kwargs.pop("action", ACTION_REVOKE)
    if _require_action(requested_action) != ACTION_REVOKE:
        raise ValueError("revoke action must be REVOKE")
    return _append_pipeline_disposition(connection, action=ACTION_REVOKE, **kwargs)


def resolve_pipeline_coherency_dispositions(
    connection: sqlite3.Connection,
    trade_date: str,
    subjects: Sequence[Mapping[str, Any]],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if not _schema_ready(connection):
        return {
            "schema_ready": False,
            "status": "SCHEMA_NOT_READY",
            "reason_codes": ["PIPELINE_DISPOSITION_SCHEMA_NOT_READY"],
            "items": {},
        }
    items: dict[str, dict[str, Any]] = {}
    invalid_count = 0
    for raw_subject in subjects:
        subject = dict(raw_subject)
        candidate_id = str(subject.get("candidate_instance_id") or "")
        subject_key = _subject_key(trade_date, candidate_id)
        chain = _project_chain(connection, subject_key)
        effective = chain.get("effective")
        projected: dict[str, Any] = {
            "status": "PENDING",
            "effective": False,
            "cas_valid": False,
            "chain_valid": bool(chain["valid"]),
            "pipeline_fingerprint": subject.get("pipeline_fingerprint"),
            "subject_version": subject.get("subject_version"),
        }
        if not chain["valid"]:
            projected["status"] = "INVALID_CHAIN"
            invalid_count += 1
        elif isinstance(effective, Mapping):
            current = _current_cas_snapshot(
                connection,
                subject=subject,
                as_of=as_of or utc_now(),
            )
            expected_action = _required_action(subject)
            cas_valid = bool(
                effective.get("action") == expected_action
                and effective.get("expected_pipeline_fingerprint")
                == subject.get("pipeline_fingerprint")
                and effective.get("expected_subject_version") == subject.get("subject_version")
                and effective.get("expected_source_fingerprint") == current["source_fingerprint"]
                and effective.get("expected_candidate_fingerprint")
                == current["candidate_fingerprint"]
                and effective.get("expected_downstream_fingerprint")
                == current["downstream_fingerprint"]
                and effective.get("expected_boundary_fingerprint")
                == current["boundary_fingerprint"]
                and not _eligibility_reasons(
                    subject,
                    current,
                    action=str(effective.get("action") or ""),
                    allow_existing=True,
                )
            )
            projected.update(
                {
                    "status": "EFFECTIVE" if cas_valid else "INVALID_CAS",
                    "effective": cas_valid,
                    "cas_valid": cas_valid,
                    "disposition_id": effective.get("disposition_id"),
                    "action": effective.get("action"),
                    "sequence_no": effective.get("sequence_no"),
                }
            )
            if not cas_valid:
                invalid_count += 1
        items[candidate_id] = projected
    return {
        "schema_ready": True,
        "status": "READY",
        "reason_codes": [],
        "invalid_count": invalid_count,
        "items": items,
    }


def list_pipeline_coherency_dispositions(
    connection: sqlite3.Connection,
    *,
    subject_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    _assert_schema_ready(connection)
    params: list[Any] = []
    where = ""
    if subject_key is not None:
        where = "WHERE subject_key = ?"
        params.append(_require_text("subject_key", subject_key))
    params.append(min(max(int(limit), 1), 500))
    rows = connection.execute(
        f"""
        SELECT *
        FROM {DISPOSITION_TABLE}
        {where}
        ORDER BY created_at DESC, disposition_id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_public_disposition(row) for row in rows]


def is_pipeline_coherency_disposition_schema_ready(
    connection: sqlite3.Connection,
) -> bool:
    """Return whether the exact schema-62 append-only ledger contract is present."""
    return _schema_ready(connection)


def _append_pipeline_disposition(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    candidate_instance_id: str,
    action: str,
    request_id: str,
    expected_pipeline_fingerprint: str,
    expected_subject_version: str,
    expected_source_fingerprint: str,
    expected_candidate_fingerprint: str,
    expected_downstream_fingerprint: str,
    expected_boundary_fingerprint: str,
    reason_code: str,
    operator_id: str,
    evidence_type: str,
    evidence_ref: str,
    evidence_sha256: str,
    evidence_json: Mapping[str, Any] | None = None,
    safety_snapshot: Mapping[str, Any] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    normalized_trade_date = _require_text("trade_date", trade_date)
    candidate_id = _require_text("candidate_instance_id", candidate_instance_id)
    normalized_request_id = _require_text("request_id", request_id)
    normalized_action = _require_action(action)
    fingerprints = {
        "pipeline_fingerprint": _require_sha256(
            "expected_pipeline_fingerprint", expected_pipeline_fingerprint
        ),
        "subject_version": _require_sha256("expected_subject_version", expected_subject_version),
        "source_fingerprint": _require_sha256(
            "expected_source_fingerprint", expected_source_fingerprint
        ),
        "candidate_fingerprint": _require_sha256(
            "expected_candidate_fingerprint", expected_candidate_fingerprint
        ),
        "downstream_fingerprint": _require_sha256(
            "expected_downstream_fingerprint", expected_downstream_fingerprint
        ),
        "boundary_fingerprint": _require_sha256(
            "expected_boundary_fingerprint", expected_boundary_fingerprint
        ),
    }
    normalized_evidence_sha = _require_sha256("evidence_sha256", evidence_sha256)
    normalized_safety_snapshot = _require_safety_snapshot(safety_snapshot)
    request_payload = {
        "contract": "pipeline-coherency-disposition-request.v1",
        "trade_date": normalized_trade_date,
        "candidate_instance_id": candidate_id,
        "action": normalized_action,
        **fingerprints,
        "reason_code": _require_text("reason_code", reason_code),
        "operator_id": _require_text("operator_id", operator_id),
        "evidence_type": _require_text("evidence_type", evidence_type),
        "evidence_ref": _require_text("evidence_ref", evidence_ref),
        "evidence_sha256": normalized_evidence_sha,
        "evidence_json": dict(evidence_json or {}),
        "safety_snapshot": normalized_safety_snapshot,
    }
    request_hash = _sha256_json(request_payload)
    _assert_schema_ready(connection)
    existing = connection.execute(
        f"SELECT * FROM {DISPOSITION_TABLE} WHERE request_id = ?",
        (normalized_request_id,),
    ).fetchone()
    if existing is not None:
        if str(existing["request_hash"]) != request_hash:
            raise PipelineCoherencyDispositionError(
                "PIPELINE_DISPOSITION_REQUEST_CONFLICT",
                "request_id was already used with a different request hash",
            )
        _assert_idempotent_disposition_is_current(
            connection,
            existing=existing,
            trade_date=normalized_trade_date,
            candidate_instance_id=candidate_id,
            action=normalized_action,
            fingerprints=fingerprints,
            as_of=as_of,
        )
        return _public_disposition(existing)

    started = not connection.in_transaction
    if started:
        connection.execute("BEGIN IMMEDIATE")
    try:
        preview = preview_pipeline_coherency_disposition(
            connection,
            trade_date=normalized_trade_date,
            candidate_instance_id=candidate_id,
            action=normalized_action,
            as_of=as_of,
        )
        if not preview["eligible"]:
            raise PipelineCoherencyDispositionError(
                "PIPELINE_DISPOSITION_NOT_ELIGIBLE",
                "pipeline disposition preview is not eligible",
                details={"reason_codes": preview["reason_codes"]},
            )
        for key, expected in fingerprints.items():
            if preview.get(key) != expected:
                raise PipelineCoherencyDispositionError(
                    "PIPELINE_DISPOSITION_CAS_MISMATCH",
                    f"pipeline disposition compare-and-set mismatch: {key}",
                )
        subject_key = str(preview["subject_key"])
        chain = _project_chain(connection, subject_key)
        previous = chain.get("last")
        disposition_id = (
            "pipeline_disposition_"
            + hashlib.sha256(f"{normalized_request_id}:{request_hash}".encode()).hexdigest()[:32]
        )
        created_at = datetime_to_wire(utc_now())
        connection.execute(
            f"""
            INSERT INTO {DISPOSITION_TABLE} (
                disposition_id, request_id, request_hash,
                candidate_instance_id, subject_key, trade_date, order_plan_id,
                sequence_no, action, supersedes_disposition_id,
                reason_code, operator_id,
                expected_pipeline_fingerprint, expected_subject_version,
                expected_source_fingerprint, expected_candidate_fingerprint,
                expected_downstream_fingerprint, expected_boundary_fingerprint,
                evidence_type, evidence_ref, evidence_sha256, evidence_json,
                safety_snapshot_json, created_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?
            )
            """,
            (
                disposition_id,
                normalized_request_id,
                request_hash,
                candidate_id,
                subject_key,
                normalized_trade_date,
                preview.get("effective_disposition", {}).get("order_plan_id")
                or _latest_plan_id(connection, candidate_id),
                int(chain["next_sequence_no"]),
                normalized_action,
                None if previous is None else previous.get("disposition_id"),
                request_payload["reason_code"],
                request_payload["operator_id"],
                fingerprints["pipeline_fingerprint"],
                fingerprints["subject_version"],
                fingerprints["source_fingerprint"],
                fingerprints["candidate_fingerprint"],
                fingerprints["downstream_fingerprint"],
                fingerprints["boundary_fingerprint"],
                request_payload["evidence_type"],
                request_payload["evidence_ref"],
                normalized_evidence_sha,
                canonical_json(request_payload["evidence_json"]),
                canonical_json(normalized_safety_snapshot),
                created_at,
            ),
        )
        row = connection.execute(
            f"SELECT * FROM {DISPOSITION_TABLE} WHERE disposition_id = ?",
            (disposition_id,),
        ).fetchone()
        if started:
            connection.commit()
    except Exception:
        if started:
            connection.rollback()
        raise
    assert row is not None
    return _public_disposition(row)


def _assert_idempotent_disposition_is_current(
    connection: sqlite3.Connection,
    *,
    existing: sqlite3.Row,
    trade_date: str,
    candidate_instance_id: str,
    action: str,
    fingerprints: Mapping[str, str],
    as_of: datetime | None,
) -> None:
    chain = _project_chain(
        connection,
        _subject_key(trade_date, candidate_instance_id),
    )
    existing_id = str(existing["disposition_id"])
    reasons: list[str] = []
    if not chain["valid"]:
        reasons.append("PIPELINE_DISPOSITION_CHAIN_INVALID")
    last = chain.get("last")
    if not isinstance(last, Mapping) or str(last.get("disposition_id") or "") != existing_id:
        reasons.append("PIPELINE_DISPOSITION_REQUEST_NO_LONGER_LATEST")

    if action == ACTION_REVOKE:
        if chain.get("effective") is not None:
            reasons.append("PIPELINE_DISPOSITION_REVOKE_NO_LONGER_EFFECTIVE")
    else:
        try:
            preview = preview_pipeline_coherency_disposition(
                connection,
                trade_date=trade_date,
                candidate_instance_id=candidate_instance_id,
                action=action,
                as_of=as_of,
            )
        except PipelineCoherencyDispositionError as exc:
            raise PipelineCoherencyDispositionError(
                "PIPELINE_DISPOSITION_IDEMPOTENT_STALE",
                "the existing request no longer proves a current effective disposition",
                details={"reason_codes": [exc.code]},
            ) from exc
        effective = _mapping(preview.get("effective_disposition"))
        if str(effective.get("disposition_id") or "") != existing_id:
            reasons.append("PIPELINE_DISPOSITION_REQUEST_NOT_EFFECTIVE")
        if preview.get("expected_action") != action:
            reasons.append("PIPELINE_DISPOSITION_ACTION_NO_LONGER_REQUIRED")
        for key, expected in fingerprints.items():
            if preview.get(key) != expected:
                reasons.append(f"PIPELINE_DISPOSITION_CAS_MISMATCH:{key.upper()}")
        reasons.extend(
            str(code)
            for code in preview.get("reason_codes") or []
            if str(code) != "PIPELINE_SUBJECT_ALREADY_DISPOSED"
        )

    reasons = _dedupe(reasons)
    if reasons:
        raise PipelineCoherencyDispositionError(
            "PIPELINE_DISPOSITION_IDEMPOTENT_STALE",
            "the existing request no longer proves a current effective disposition",
            details={"reason_codes": reasons},
        )


def _current_cas_snapshot(
    connection: sqlite3.Connection,
    *,
    subject: Mapping[str, Any],
    as_of: datetime,
) -> dict[str, Any]:
    candidate_id = str(subject.get("candidate_instance_id") or "")
    candidate = connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    candidate_payload = (
        {"present": False, "candidate_instance_id": candidate_id}
        if candidate is None
        else _row_dict(candidate)
    )
    sources = [
        _row_dict(row)
        for row in connection.execute(
            """
            SELECT *
            FROM candidate_source_events
            WHERE candidate_instance_id = ?
            ORDER BY event_ts, source_event_id
            """,
            (candidate_id,),
        ).fetchall()
    ]
    latest_sources = [
        _row_dict(row)
        for row in connection.execute(
            """
            SELECT *
            FROM candidate_sources_latest
            WHERE candidate_instance_id = ?
            ORDER BY trade_date, code, source_type, source_id
            """,
            (candidate_id,),
        ).fetchall()
    ]
    downstream = _downstream_snapshot(connection, subject=subject)
    boundary = _boundary_snapshot(connection, downstream["command_ids"])
    unexpired_plan_ids = _unexpired_plan_ids(connection, candidate_id, as_of=as_of)
    return {
        "source_fingerprint": _sha256_json(
            {
                "contract": "pipeline-source-cas.v2",
                "event_rows": sources,
                "latest_rows": latest_sources,
            }
        ),
        "candidate_fingerprint": _sha256_json(
            {"contract": "pipeline-candidate-cas.v1", "row": candidate_payload}
        ),
        "downstream_fingerprint": _sha256_json(
            {"contract": "pipeline-downstream-cas.v2", **downstream["hash_payload"]}
        ),
        "boundary_fingerprint": _sha256_json(
            {"contract": "pipeline-boundary-cas.v2", "rows": boundary["hash_rows"]}
        ),
        "source_public": {
            "row_count": len(sources),
            "active_count": sum(1 for row in sources if int(row.get("active") or 0)),
            "latest_row_count": len(latest_sources),
            "latest_active_count": sum(
                1 for row in latest_sources if int(row.get("active") or 0)
            ),
        },
        "candidate_public": {
            "present": candidate is not None,
            "state": None if candidate is None else candidate["state"],
            "trade_date": None if candidate is None else candidate["trade_date"],
            "closed_at": None if candidate is None else candidate["closed_at"],
            "active_source_count": (
                None if candidate is None else candidate["active_source_count"]
            ),
        },
        "downstream_public": {
            **downstream["public"],
            "unresolved_boundary_count": boundary["unresolved_count"],
            "unknown_boundary_count": boundary["unknown_count"],
            "unexpired_plan_count": len(unexpired_plan_ids),
        },
    }


def _downstream_snapshot(
    connection: sqlite3.Connection,
    *,
    subject: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = str(subject.get("candidate_instance_id") or "")
    candidate = connection.execute(
        "SELECT trade_date, code FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    subject_trade_date = str(subject.get("trade_date") or "")
    subject_code = str(subject.get("code") or "")
    candidate_trade_date = str(
        "" if candidate is None else candidate["trade_date"] or ""
    )
    candidate_code = str("" if candidate is None else candidate["code"] or "")
    trade_code_pairs = sorted(
        {
            (trade_date, code)
            for trade_date, code in (
                (subject_trade_date, subject_code),
                (candidate_trade_date, candidate_code),
            )
            if trade_date and code
        }
    )
    unbound_artifacts = _unbound_order_artifacts(connection)
    intents = [
        _row_dict(row)
        for row in connection.execute(
            """
            SELECT *
            FROM live_sim_intents
            WHERE candidate_instance_id = ?
            ORDER BY created_at, live_sim_intent_id
            """,
            (candidate_id,),
        ).fetchall()
    ]
    intent_ids = [str(row["live_sim_intent_id"]) for row in intents]
    orders = _rows_for_ids(
        connection,
        table="live_sim_orders",
        id_column="live_sim_intent_id",
        ids=intent_ids,
        select_columns=("*",),
        order_by="created_at, live_sim_order_id",
    )
    for trade_date, code in trade_code_pairs:
        orders = _merge_rows(
            orders,
            _trade_code_rows(
                connection,
                table="live_sim_orders",
                trade_date=trade_date,
                code=code,
                order_by="created_at, live_sim_order_id",
            ),
            key="live_sim_order_id",
        )
    orders = _merge_rows(
        orders,
        unbound_artifacts["live_sim_orders"],
        key="live_sim_order_id",
    )
    order_ids = [str(row["live_sim_order_id"]) for row in orders]
    executions = _merge_rows(
        _rows_for_ids(
            connection,
            table="live_sim_executions",
            id_column="live_sim_order_id",
            ids=order_ids,
            select_columns=("*",),
            order_by="executed_at, live_sim_execution_id",
        ),
        _rows_for_ids(
            connection,
            table="live_sim_executions",
            id_column="live_sim_intent_id",
            ids=intent_ids,
            select_columns=("*",),
            order_by="executed_at, live_sim_execution_id",
        ),
        key="live_sim_execution_id",
    )
    executions = _merge_rows(
        executions,
        unbound_artifacts["live_sim_executions"],
        key="live_sim_execution_id",
    )
    dry_intents = [
        _row_dict(row)
        for row in connection.execute(
            """
            SELECT *
            FROM dry_run_intents
            WHERE candidate_instance_id = ?
            ORDER BY created_at, dry_run_intent_id
            """,
            (candidate_id,),
        ).fetchall()
    ]
    dry_intent_ids = [str(row["dry_run_intent_id"]) for row in dry_intents]
    dry_orders = _rows_for_ids(
        connection,
        table="dry_run_orders",
        id_column="dry_run_intent_id",
        ids=dry_intent_ids,
        select_columns=("*",),
        order_by="created_at, dry_run_order_id",
    )
    for trade_date, code in trade_code_pairs:
        dry_orders = _merge_rows(
            dry_orders,
            _trade_code_rows(
                connection,
                table="dry_run_orders",
                trade_date=trade_date,
                code=code,
                order_by="created_at, dry_run_order_id",
            ),
            key="dry_run_order_id",
        )
    dry_orders = _merge_rows(
        dry_orders,
        unbound_artifacts["dry_run_orders"],
        key="dry_run_order_id",
    )
    dry_order_ids = [str(row["dry_run_order_id"]) for row in dry_orders]
    dry_executions = _merge_rows(
        _rows_for_ids(
            connection,
            table="dry_run_executions",
            id_column="dry_run_order_id",
            ids=dry_order_ids,
            select_columns=("*",),
            order_by="executed_at, dry_run_execution_id",
        ),
        _rows_for_ids(
            connection,
            table="dry_run_executions",
            id_column="dry_run_intent_id",
            ids=dry_intent_ids,
            select_columns=("*",),
            order_by="executed_at, dry_run_execution_id",
        ),
        key="dry_run_execution_id",
    )
    for trade_date, code in trade_code_pairs:
        dry_executions = _merge_rows(
            dry_executions,
            _trade_code_rows(
                connection,
                table="dry_run_executions",
                trade_date=trade_date,
                code=code,
                order_by="executed_at, dry_run_execution_id",
            ),
            key="dry_run_execution_id",
        )
    dry_executions = _merge_rows(
        dry_executions,
        unbound_artifacts["dry_run_executions"],
        key="dry_run_execution_id",
    )
    dry_rejections = _candidate_rows(
        connection,
        table="dry_run_intent_rejections",
        candidate_id=candidate_id,
        order_by="created_at, rejection_id",
    )
    live_rejections = _candidate_rows(
        connection,
        table="live_sim_rejections",
        candidate_id=candidate_id,
        order_by="created_at, rejection_id",
    )

    dry_positions: list[dict[str, Any]] = []
    for trade_date, code in trade_code_pairs:
        dry_positions = _merge_rows(
            dry_positions,
            _trade_code_rows(
                connection,
                table="dry_run_positions",
                trade_date=trade_date,
                code=code,
                order_by="opened_at, dry_run_position_id",
            ),
            key="dry_run_position_id",
        )
    dry_positions = _merge_rows(
        dry_positions,
        unbound_artifacts["dry_run_positions"],
        key="dry_run_position_id",
    )
    dry_position_ids = [str(row["dry_run_position_id"]) for row in dry_positions]
    dry_exit_evaluations = _rows_for_ids(
        connection,
        table="dry_run_exit_evaluations",
        id_column="dry_run_position_id",
        ids=dry_position_ids,
        select_columns=("*",),
        order_by="evaluated_at, exit_evaluation_id",
    )
    dry_exit_evaluation_ids = [
        str(row["exit_evaluation_id"]) for row in dry_exit_evaluations
    ]
    dry_exit_signals = _rows_for_ids(
        connection,
        table="dry_run_exit_signals",
        id_column="exit_evaluation_id",
        ids=dry_exit_evaluation_ids,
        select_columns=("*",),
        order_by="observed_at, exit_signal_id",
    )
    dry_exit_intents = _rows_for_ids(
        connection,
        table="dry_run_exit_intents",
        id_column="dry_run_position_id",
        ids=dry_position_ids,
        select_columns=("*",),
        order_by="created_at, dry_run_exit_intent_id",
    )
    for trade_date, code in trade_code_pairs:
        dry_exit_intents = _merge_rows(
            dry_exit_intents,
            _trade_code_rows(
                connection,
                table="dry_run_exit_intents",
                trade_date=trade_date,
                code=code,
                order_by="created_at, dry_run_exit_intent_id",
            ),
            key="dry_run_exit_intent_id",
        )
    dry_exit_intents = _merge_rows(
        dry_exit_intents,
        unbound_artifacts["dry_run_exit_intents"],
        key="dry_run_exit_intent_id",
    )
    dry_exit_intent_ids = [
        str(row["dry_run_exit_intent_id"]) for row in dry_exit_intents
    ]
    dry_exit_orders = _rows_for_ids(
        connection,
        table="dry_run_exit_orders",
        id_column="dry_run_exit_intent_id",
        ids=dry_exit_intent_ids,
        select_columns=("*",),
        order_by="created_at, dry_run_exit_order_id",
    )
    for trade_date, code in trade_code_pairs:
        dry_exit_orders = _merge_rows(
            dry_exit_orders,
            _trade_code_rows(
                connection,
                table="dry_run_exit_orders",
                trade_date=trade_date,
                code=code,
                order_by="created_at, dry_run_exit_order_id",
            ),
            key="dry_run_exit_order_id",
        )
    dry_exit_orders = _merge_rows(
        dry_exit_orders,
        unbound_artifacts["dry_run_exit_orders"],
        key="dry_run_exit_order_id",
    )
    dry_exit_order_ids = [str(row["dry_run_exit_order_id"]) for row in dry_exit_orders]
    dry_exit_executions = _rows_for_ids(
        connection,
        table="dry_run_exit_executions",
        id_column="dry_run_exit_order_id",
        ids=dry_exit_order_ids,
        select_columns=("*",),
        order_by="executed_at, dry_run_exit_execution_id",
    )
    for trade_date, code in trade_code_pairs:
        dry_exit_executions = _merge_rows(
            dry_exit_executions,
            _trade_code_rows(
                connection,
                table="dry_run_exit_executions",
                trade_date=trade_date,
                code=code,
                order_by="executed_at, dry_run_exit_execution_id",
            ),
            key="dry_run_exit_execution_id",
        )
    dry_exit_executions = _merge_rows(
        dry_exit_executions,
        unbound_artifacts["dry_run_exit_executions"],
        key="dry_run_exit_execution_id",
    )

    live_positions = _merge_rows(
        _rows_for_ids(
            connection,
            table="live_sim_positions",
            id_column="source_live_sim_order_id",
            ids=order_ids,
            select_columns=("*",),
            order_by="created_at, position_id",
        ),
        _rows_for_ids(
            connection,
            table="live_sim_positions",
            id_column="source_live_sim_intent_id",
            ids=intent_ids,
            select_columns=("*",),
            order_by="created_at, position_id",
        ),
        key="position_id",
    )
    for trade_date, code in trade_code_pairs:
        live_positions = _merge_rows(
            live_positions,
            _trade_code_rows(
                connection,
                table="live_sim_positions",
                trade_date=trade_date,
                code=code,
                order_by="created_at, position_id",
            ),
            key="position_id",
        )
    live_positions = _merge_rows(
        live_positions,
        unbound_artifacts["live_sim_positions"],
        key="position_id",
    )
    position_ids = [str(row["position_id"]) for row in live_positions]
    live_position_events = _rows_for_ids(
        connection,
        table="live_sim_position_events",
        id_column="position_id",
        ids=position_ids,
        select_columns=("*",),
        order_by="created_at, event_id",
    )
    live_exit_signals = _rows_for_ids(
        connection,
        table="live_sim_exit_signals",
        id_column="position_id",
        ids=position_ids,
        select_columns=("*",),
        order_by="created_at, exit_signal_id",
    )
    live_exit_intents = _rows_for_ids(
        connection,
        table="live_sim_exit_intents",
        id_column="position_id",
        ids=position_ids,
        select_columns=("*",),
        order_by="created_at, exit_intent_id",
    )
    live_exit_intents = _merge_rows(
        live_exit_intents,
        unbound_artifacts["live_sim_exit_intents"],
        key="exit_intent_id",
    )
    live_exit_intent_ids = [str(row["exit_intent_id"]) for row in live_exit_intents]
    referenced_exit_order_ids = [
        str(row["live_sim_order_id"])
        for row in live_exit_intents
        if row.get("live_sim_order_id")
    ]
    exit_orders = _merge_rows(
        _rows_for_ids(
            connection,
            table="live_sim_orders",
            id_column="live_sim_intent_id",
            ids=live_exit_intent_ids,
            select_columns=("*",),
            order_by="created_at, live_sim_order_id",
        ),
        _rows_for_ids(
            connection,
            table="live_sim_orders",
            id_column="live_sim_order_id",
            ids=referenced_exit_order_ids,
            select_columns=("*",),
            order_by="created_at, live_sim_order_id",
        ),
        key="live_sim_order_id",
    )
    orders = _merge_rows(orders, exit_orders, key="live_sim_order_id")
    order_ids = [str(row["live_sim_order_id"]) for row in orders]
    exit_executions = _merge_rows(
        _rows_for_ids(
            connection,
            table="live_sim_executions",
            id_column="live_sim_order_id",
            ids=[str(row["live_sim_order_id"]) for row in exit_orders],
            select_columns=("*",),
            order_by="executed_at, live_sim_execution_id",
        ),
        _rows_for_ids(
            connection,
            table="live_sim_executions",
            id_column="live_sim_intent_id",
            ids=live_exit_intent_ids,
            select_columns=("*",),
            order_by="executed_at, live_sim_execution_id",
        ),
        key="live_sim_execution_id",
    )
    executions = _merge_rows(executions, exit_executions, key="live_sim_execution_id")
    all_live_intent_ids = sorted({*intent_ids, *live_exit_intent_ids})
    live_cancel_intents = _rows_for_ids(
        connection,
        table="live_sim_cancel_intents",
        id_column="live_sim_order_id",
        ids=order_ids,
        select_columns=("*",),
        order_by="created_at, cancel_intent_id",
    )
    live_cancel_intents = _merge_rows(
        live_cancel_intents,
        unbound_artifacts["live_sim_cancel_intents"],
        key="cancel_intent_id",
    )
    live_lifecycle_events = _merge_rows(
        _rows_for_ids(
            connection,
            table="live_sim_lifecycle_events",
            id_column="live_sim_order_id",
            ids=order_ids,
            select_columns=("*",),
            order_by="created_at, lifecycle_event_id",
        ),
        _rows_for_ids(
            connection,
            table="live_sim_lifecycle_events",
            id_column="position_id",
            ids=position_ids,
            select_columns=("*",),
            order_by="created_at, lifecycle_event_id",
        ),
        key="lifecycle_event_id",
    )
    live_errors = _merge_rows(
        _rows_for_ids(
            connection,
            table="live_sim_errors",
            id_column="live_sim_intent_id",
            ids=all_live_intent_ids,
            select_columns=("*",),
            order_by="created_at, id",
        ),
        _rows_for_ids(
            connection,
            table="live_sim_errors",
            id_column="live_sim_order_id",
            ids=order_ids,
            select_columns=("*",),
            order_by="created_at, id",
        ),
        key="id",
    )
    direct_command_ids = sorted(
        {
            str(value)
            for value in [
                *(row.get("gateway_command_id") for row in intents),
                *(row.get("gateway_command_id") for row in orders),
                *(row.get("gateway_command_id") for row in live_exit_intents),
                *(row.get("gateway_command_id") for row in live_cancel_intents),
            ]
            if value
        }
    )
    order_plan_ids = sorted(
        {
            str(row[0])
            for table in ("order_plan_drafts", "order_plan_drafts_latest")
            for row in connection.execute(
                f"SELECT order_plan_id FROM {table} WHERE candidate_instance_id = ?",
                (candidate_id,),
            ).fetchall()
            if row[0]
        }
    )
    (
        commands,
        malformed_gateway_payload_count,
        malformed_unbound_order_command_count,
        unbound_order_command_count,
    ) = _gateway_commands_for_subject(
        connection,
        direct_command_ids=direct_command_ids,
        candidate_instance_id=candidate_id,
        live_sim_intent_ids=all_live_intent_ids,
        order_plan_ids=order_plan_ids,
    )
    command_ids = [str(row["command_id"]) for row in commands]
    command_events = _rows_for_ids(
        connection,
        table="gateway_command_events",
        id_column="command_id",
        ids=command_ids,
        select_columns=("*",),
        order_by="created_at, id",
    )
    command_dedupe = _rows_for_ids(
        connection,
        table="gateway_command_dedupe_keys",
        id_column="command_id",
        ids=command_ids,
        select_columns=("*",),
        order_by="created_at, idempotency_key",
    )
    active_intents = sum(
        1 for row in intents if str(row.get("status") or "").upper() in _ACTIVE_INTENT_STATUSES
    )
    active_orders = sum(
        1 for row in orders if str(row.get("status") or "").upper() not in _TERMINAL_ORDER_STATUSES
    )
    unsafe_commands = sum(
        1
        for row in commands
        if str(row.get("status") or "").upper() not in _SAFE_GATEWAY_COMMAND_STATUSES
    )
    active_dry_intents = sum(
        1 for row in dry_intents if str(row.get("status") or "").upper() in {"CREATED", "APPROVED"}
    )
    active_other = sum(
        1
        for row in [
            *dry_orders,
            *dry_positions,
            *dry_exit_intents,
            *dry_exit_orders,
            *live_positions,
            *live_exit_intents,
            *live_cancel_intents,
        ]
        if str(row.get("status") or "").upper() not in _TERMINAL_ORDER_STATUSES
    )
    hash_payload = {
        "trade_code_pairs": [
            {"trade_date": trade_date, "code": code}
            for trade_date, code in trade_code_pairs
        ],
        "live_sim_intents": intents,
        "live_sim_orders": orders,
        "live_sim_executions": executions,
        "live_sim_rejections": live_rejections,
        "live_sim_positions": live_positions,
        "live_sim_position_events": live_position_events,
        "live_sim_exit_signals": live_exit_signals,
        "live_sim_exit_intents": live_exit_intents,
        "live_sim_cancel_intents": live_cancel_intents,
        "live_sim_lifecycle_events": live_lifecycle_events,
        "live_sim_errors": live_errors,
        "dry_run_intents": dry_intents,
        "dry_run_orders": dry_orders,
        "dry_run_executions": dry_executions,
        "dry_run_intent_rejections": dry_rejections,
        "dry_run_positions": dry_positions,
        "dry_run_exit_evaluations": dry_exit_evaluations,
        "dry_run_exit_signals": dry_exit_signals,
        "dry_run_exit_intents": dry_exit_intents,
        "dry_run_exit_orders": dry_exit_orders,
        "dry_run_exit_executions": dry_exit_executions,
        "gateway_commands": commands,
        "gateway_command_events": command_events,
        "gateway_command_dedupe_keys": command_dedupe,
    }
    return {
        "hash_payload": hash_payload,
        "command_ids": command_ids,
        "public": {
            "live_sim_intent_count": len(intents),
            "active_live_sim_intent_count": active_intents,
            "live_sim_order_count": len(orders),
            "active_live_sim_order_count": active_orders,
            "live_sim_execution_count": len(executions),
            "dry_run_intent_count": len(dry_intents),
            "active_dry_run_intent_count": active_dry_intents,
            "dry_run_order_count": len(dry_orders),
            "dry_run_execution_count": len(dry_executions),
            "live_sim_rejection_count": len(live_rejections),
            "dry_run_rejection_count": len(dry_rejections),
            "active_other_downstream_count": active_other,
            "gateway_command_count": len(commands),
            "unsafe_gateway_command_count": unsafe_commands,
            "malformed_gateway_command_payload_count": (
                malformed_gateway_payload_count
            ),
            "malformed_unbound_order_command_count": (
                malformed_unbound_order_command_count
            ),
            "unbound_order_command_count": unbound_order_command_count,
            "unbound_active_order_artifact_count": sum(
                1
                for rows in unbound_artifacts.values()
                for row in rows
                if "status" in row
                and str(row.get("status") or "").upper()
                not in _TERMINAL_ORDER_STATUSES
            ),
            "unbound_order_artifact_count": sum(
                len(rows) for rows in unbound_artifacts.values()
            ),
        },
    }


def _boundary_snapshot(
    connection: sqlite3.Connection,
    command_ids: Sequence[str],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    requested_ids = {str(value) for value in command_ids if str(value)}
    boundary_ids = {
        str(row[0])
        for table in (
            "gateway_order_broker_boundaries",
            "gateway_order_broker_boundary_resolutions",
        )
        for row in connection.execute(f"SELECT DISTINCT command_id FROM {table}").fetchall()
        if row[0]
    }
    candidate_ids = sorted(requested_ids | boundary_ids)
    included_ids: list[str] = []
    unresolved = 0
    unknown = 0
    for command_id in candidate_ids:
        try:
            boundary = get_effective_order_broker_boundary(connection, command_id)
        except Exception:
            if command_id not in requested_ids and command_id not in boundary_ids:
                continue
            included_ids.append(command_id)
            unknown += 1
            rows.append({"command_id": command_id, "status": "ERROR"})
            continue
        if not isinstance(boundary, Mapping):
            if command_id not in requested_ids:
                continue
            included_ids.append(command_id)
            rows.append({"command_id": command_id, "status": "ABSENT"})
            unknown += 1
            continue
        state = str(
            boundary.get("effective_state") or boundary.get("state") or "UNKNOWN"
        ).upper()
        if state not in _KNOWN_BOUNDARY_STATES:
            state = "UNKNOWN"
        if command_id not in requested_ids and state not in {"UNCONFIRMED", "UNKNOWN"}:
            continue
        included_ids.append(command_id)
        if state == "UNCONFIRMED":
            unresolved += 1
        if state == "UNKNOWN":
            unknown += 1
        rows.append(
            {
                "command_id": command_id,
                "state": state,
                "raw_state": boundary.get("raw_state") or boundary.get("state"),
                "resolution_id": _nested_value(boundary, "effective_resolution", "resolution_id"),
                "updated_at": boundary.get("updated_at"),
                "broker_order_no_present": bool(boundary.get("broker_order_no")),
                "pre_ack_recorded_at": boundary.get("pre_ack_recorded_at"),
                "broker_accepted_at": boundary.get("broker_accepted_at"),
                "chejan_confirmed_at": boundary.get("chejan_confirmed_at"),
            }
        )
    raw_rows = _rows_for_ids(
        connection,
        table="gateway_order_broker_boundaries",
        id_column="command_id",
        ids=included_ids,
        select_columns=("*",),
        order_by="command_id",
    )
    resolution_rows = _rows_for_ids(
        connection,
        table="gateway_order_broker_boundary_resolutions",
        id_column="command_id",
        ids=included_ids,
        select_columns=("*",),
        order_by="command_id, sequence_no, created_at, resolution_id",
    )
    return {
        "rows": rows,
        "hash_rows": {
            "raw_boundaries": raw_rows,
            "resolution_ledger": resolution_rows,
            "effective_projection": rows,
        },
        "unresolved_count": unresolved,
        "unknown_count": unknown,
    }


def _eligibility_reasons(
    subject: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    action: str,
    allow_existing: bool = False,
) -> list[str]:
    del allow_existing
    reasons: list[str] = []
    candidate = _mapping(current.get("candidate_public"))
    source = _mapping(current.get("source_public"))
    downstream = _mapping(current.get("downstream_public"))
    if str(subject.get("canonical_status") or "").upper() != "FAIL":
        reasons.append("CANONICAL_PIPELINE_SUBJECT_NOT_FAIL")
    if downstream.get("unresolved_boundary_count"):
        reasons.append("EFFECTIVE_BROKER_BOUNDARY_UNCONFIRMED")
    if downstream.get("unknown_boundary_count"):
        reasons.append("BROKER_BOUNDARY_STATUS_UNKNOWN")
    if downstream.get("active_live_sim_intent_count"):
        reasons.append("ACTIVE_LIVE_SIM_INTENT_PRESENT")
    if downstream.get("active_live_sim_order_count"):
        reasons.append("ACTIVE_LIVE_SIM_ORDER_PRESENT")
    if downstream.get("unsafe_gateway_command_count"):
        reasons.append("UNSAFE_GATEWAY_COMMAND_PRESENT")
    if downstream.get("malformed_gateway_command_payload_count"):
        reasons.append("MALFORMED_GATEWAY_COMMAND_PAYLOAD_PRESENT")
    if downstream.get("malformed_unbound_order_command_count"):
        reasons.append("MALFORMED_UNBOUND_ORDER_COMMAND_PRESENT")
    if downstream.get("unbound_order_command_count"):
        reasons.append("UNBOUND_ORDER_COMMAND_PRESENT")
    if downstream.get("active_dry_run_intent_count"):
        reasons.append("ACTIVE_DRY_RUN_INTENT_PRESENT")
    if downstream.get("active_other_downstream_count"):
        reasons.append("ACTIVE_OTHER_DOWNSTREAM_PRESENT")
    if downstream.get("unbound_active_order_artifact_count"):
        reasons.append("UNBOUND_ACTIVE_ORDER_ARTIFACT_PRESENT")
    if downstream.get("unbound_order_artifact_count"):
        reasons.append("UNBOUND_ORDER_ARTIFACT_PRESENT")
    if downstream.get("unexpired_plan_count"):
        reasons.append("UNEXPIRED_PLAN_READY_PRESENT")
    if action == ACTION_DISPOSE_EXPIRED_PLAN_READY:
        if subject.get("classification") != "HISTORICAL_CLOSED":
            reasons.append("EXPIRED_PLAN_CANDIDATE_NOT_HISTORICAL_CLOSED")
        if str(subject.get("latest_plan_status") or "").upper() != "PLAN_READY":
            reasons.append("LATEST_PLAN_NOT_PLAN_READY")
        if subject.get("latest_plan_unexpired") is not False:
            reasons.append("LATEST_PLAN_NOT_PROVEN_EXPIRED")
        if str(candidate.get("state") or "").upper() != "CLOSED" or not candidate.get("closed_at"):
            reasons.append("CANDIDATE_TERMINAL_EVIDENCE_MISSING")
        if int(candidate.get("active_source_count") or 0):
            reasons.append("CANDIDATE_ACTIVE_SOURCE_COUNT_PRESENT")
        if source.get("active_count") or source.get("latest_active_count"):
            reasons.append("HISTORICAL_ACTIVE_SOURCE_PRESENT")
    elif action == ACTION_DISPOSE_ORPHAN:
        if subject.get("classification") != "MISSING_CANDIDATE_MANUAL_REVIEW":
            reasons.append("PIPELINE_SUBJECT_NOT_ORPHAN")
        if candidate.get("present"):
            reasons.append("ORPHAN_CANDIDATE_PRESENT")
        if source.get("active_count") or source.get("latest_active_count"):
            reasons.append("ORPHAN_ACTIVE_SOURCE_PRESENT")
    elif action == ACTION_DISPOSE_STALE_OTHER_DATE:
        if subject.get("classification") != "STALE_OTHER_DATE_MANUAL_REVIEW":
            reasons.append("PIPELINE_SUBJECT_NOT_STALE_OTHER_DATE")
        if str(candidate.get("state") or "").upper() not in {"STALE", "CLOSED"}:
            reasons.append("STALE_SUBJECT_IS_CURRENT_ACTIVE")
        if int(candidate.get("active_source_count") or 0):
            reasons.append("CANDIDATE_ACTIVE_SOURCE_COUNT_PRESENT")
        if source.get("active_count") or source.get("latest_active_count"):
            reasons.append("STALE_SUBJECT_ACTIVE_SOURCE_PRESENT")
    return _dedupe(reasons)


def _required_action(subject: Mapping[str, Any]) -> str | None:
    classification = str(subject.get("classification") or "")
    plan_status = str(subject.get("latest_plan_status") or "").upper()
    if (
        classification == "HISTORICAL_CLOSED"
        and plan_status == "PLAN_READY"
        and subject.get("latest_plan_unexpired") is False
    ):
        return ACTION_DISPOSE_EXPIRED_PLAN_READY
    if classification == "MISSING_CANDIDATE_MANUAL_REVIEW":
        return ACTION_DISPOSE_ORPHAN
    if classification == "STALE_OTHER_DATE_MANUAL_REVIEW":
        return ACTION_DISPOSE_STALE_OTHER_DATE
    return None


def _subject_summary(item: Mapping[str, Any]) -> dict[str, Any]:
    latest_plan = _mapping(item.get("latest_plan"))
    candidate = _mapping(item.get("candidate"))
    return {
        "candidate_instance_id": item.get("candidate_instance_id"),
        "trade_date": item.get("trade_date") or candidate.get("trade_date"),
        "code": item.get("code") or candidate.get("code"),
        "canonical_status": item.get("canonical_status"),
        "canonical_reason_codes": list(item.get("canonical_reason_codes") or []),
        "classification": item.get("classification"),
        "legacy_warn_candidate": bool(item.get("legacy_warn_candidate")),
        "manual_review_required": bool(item.get("manual_review_required")),
        "active_recovery_required": bool(item.get("active_recovery_required")),
        "disposition_required": bool(item.get("disposition_required")),
        "latest_plan_status": latest_plan.get("status"),
        "latest_plan_unexpired": latest_plan.get("unexpired"),
        "pipeline_fingerprint": item.get("pipeline_fingerprint"),
        "subject_version": item.get("subject_version"),
    }


def _project_chain(connection: sqlite3.Connection, subject_key: str) -> dict[str, Any]:
    rows = connection.execute(
        f"""
        SELECT * FROM {DISPOSITION_TABLE}
        WHERE subject_key = ?
        ORDER BY sequence_no, disposition_id
        """,
        (subject_key,),
    ).fetchall()
    valid = True
    previous: dict[str, Any] | None = None
    projected: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        item = _public_disposition(row)
        if not _stored_disposition_safety_valid(row):
            valid = False
        if item["sequence_no"] != index:
            valid = False
        expected_supersedes = None if previous is None else previous["disposition_id"]
        if item.get("supersedes_disposition_id") != expected_supersedes:
            valid = False
        if item["action"] not in _ACTIONS:
            valid = False
        projected.append(item)
        previous = item
    last = projected[-1] if projected else None
    effective = None if last is None or last["action"] == ACTION_REVOKE else last
    return {
        "valid": valid,
        "rows": projected,
        "last": last,
        "effective": effective,
        "next_sequence_no": len(projected) + 1,
    }


def _public_disposition(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    data = _row_dict(row)
    return {
        "disposition_id": data.get("disposition_id"),
        "request_id": data.get("request_id"),
        "request_hash": data.get("request_hash"),
        "candidate_instance_id": data.get("candidate_instance_id"),
        "subject_key": data.get("subject_key"),
        "trade_date": data.get("trade_date"),
        "order_plan_id": data.get("order_plan_id"),
        "sequence_no": int(data.get("sequence_no") or 0),
        "action": data.get("action"),
        "supersedes_disposition_id": data.get("supersedes_disposition_id"),
        "reason_code": data.get("reason_code"),
        "operator_id": data.get("operator_id"),
        "expected_pipeline_fingerprint": data.get("expected_pipeline_fingerprint"),
        "expected_subject_version": data.get("expected_subject_version"),
        "expected_source_fingerprint": data.get("expected_source_fingerprint"),
        "expected_candidate_fingerprint": data.get("expected_candidate_fingerprint"),
        "expected_downstream_fingerprint": data.get("expected_downstream_fingerprint"),
        "expected_boundary_fingerprint": data.get("expected_boundary_fingerprint"),
        "evidence_type": data.get("evidence_type"),
        "evidence_ref": data.get("evidence_ref"),
        "evidence_sha256": data.get("evidence_sha256"),
        "created_at": data.get("created_at"),
        "observe_only": _stored_bool(data.get("observe_only")),
        "not_order_intent": _stored_bool(data.get("not_order_intent")),
        "no_order_side_effects": _stored_bool(data.get("no_order_side_effects")),
        "live_sim_allowed": _stored_bool(data.get("live_sim_allowed")),
        "live_real_allowed": _stored_bool(data.get("live_real_allowed")),
        "order_commands_allowed": _stored_bool(data.get("order_commands_allowed")),
        "auto_run_evaluation": _stored_bool(data.get("auto_run_evaluation")),
    }


def _schema_ready(connection: sqlite3.Connection) -> bool:
    try:
        metadata = connection.execute(
            "SELECT value FROM app_metadata WHERE key = 'schema_version'"
        ).fetchone()
        if metadata is None or int(metadata["value"]) != _EXPECTED_SCHEMA_VERSION:
            return False
        table_row = connection.execute(
            "SELECT type, tbl_name, sql FROM sqlite_master WHERE name = ?",
            (DISPOSITION_TABLE,),
        ).fetchone()
        if not _schema_object_valid(
            table_row,
            object_type="table",
            object_name=DISPOSITION_TABLE,
        ):
            return False
        table_info = connection.execute(
            f"PRAGMA table_info({DISPOSITION_TABLE})"
        ).fetchall()
        columns = {str(item["name"]) for item in table_info}
        if not _REQUIRED_COLUMNS <= columns:
            return False
        for index_name, expected in _EXPECTED_INDEX_CONTRACTS.items():
            if not _index_contract_valid(
                connection,
                index_name=index_name,
                expected_columns=expected["columns"],
                expected_descending=expected["descending"],
            ):
                return False
        for trigger_name in _EXPECTED_TRIGGER_NAMES:
            trigger_row = connection.execute(
                "SELECT type, tbl_name, sql FROM sqlite_master WHERE name = ?",
                (trigger_name,),
            ).fetchone()
            if not _schema_object_valid(
                trigger_row,
                object_type="trigger",
                object_name=trigger_name,
            ):
                return False
        return True
    except (sqlite3.DatabaseError, TypeError, ValueError, KeyError):
        return False


def _schema_object_valid(
    row: sqlite3.Row | None,
    *,
    object_type: str,
    object_name: str,
) -> bool:
    if row is None:
        return False
    if str(row["type"]) != object_type or str(row["tbl_name"]) != DISPOSITION_TABLE:
        return False
    expected = _EXPECTED_SCHEMA_SQL_SHA256.get((object_type, object_name))
    return expected is not None and _normalized_sql_sha256(row["sql"]) == expected


def _index_contract_valid(
    connection: sqlite3.Connection,
    *,
    index_name: str,
    expected_columns: Sequence[str],
    expected_descending: Sequence[bool],
) -> bool:
    schema_row = connection.execute(
        "SELECT type, tbl_name, sql FROM sqlite_master WHERE name = ?",
        (index_name,),
    ).fetchone()
    if not _schema_object_valid(
        schema_row,
        object_type="index",
        object_name=index_name,
    ):
        return False
    index_row = next(
        (
            row
            for row in connection.execute(
                f"PRAGMA index_list({DISPOSITION_TABLE})"
            ).fetchall()
            if str(row["name"]) == index_name
        ),
        None,
    )
    if index_row is None:
        return False
    key_rows = [
        row
        for row in connection.execute(f"PRAGMA index_xinfo({index_name})").fetchall()
        if int(row["key"] or 0) == 1
    ]
    return bool(
        int(index_row["unique"] or 0) == 0
        and int(index_row["partial"] or 0) == 0
        and str(index_row["origin"]) == "c"
        and tuple(str(row["name"]) for row in key_rows) == tuple(expected_columns)
        and tuple(bool(row["desc"]) for row in key_rows)
        == tuple(expected_descending)
    )


def _normalized_sql_sha256(value: object) -> str:
    normalized = "".join(str(value or "").upper().split())
    normalized = normalized.replace("IFNOTEXISTS", "").rstrip(";")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _assert_schema_ready(connection: sqlite3.Connection) -> None:
    if not _schema_ready(connection):
        raise PipelineCoherencyDispositionError(
            "PIPELINE_DISPOSITION_SCHEMA_NOT_READY",
            "schema 62 pipeline disposition contract is not ready",
        )


def _unexpired_plan_ids(
    connection: sqlite3.Connection,
    candidate_id: str,
    *,
    as_of: datetime,
) -> list[str]:
    rows = connection.execute(
        """
        SELECT order_plan_id, expires_at
        FROM order_plan_drafts
        WHERE candidate_instance_id = ? AND status = 'PLAN_READY'
        ORDER BY created_at, order_plan_id
        """,
        (candidate_id,),
    ).fetchall()
    result: list[str] = []
    for row in rows:
        try:
            expires_at = parse_timestamp(row["expires_at"], "expires_at")
        except (TypeError, ValueError):
            result.append(str(row["order_plan_id"]))
            continue
        if expires_at > as_of:
            result.append(str(row["order_plan_id"]))
    return result


def _latest_plan_id(connection: sqlite3.Connection, candidate_id: str) -> str | None:
    row = connection.execute(
        """
        SELECT order_plan_id FROM order_plan_drafts_latest
        WHERE candidate_instance_id = ?
        ORDER BY created_at DESC, order_plan_id DESC
        LIMIT 1
        """,
        (candidate_id,),
    ).fetchone()
    return None if row is None else str(row["order_plan_id"])


def _rows_for_ids(
    connection: sqlite3.Connection,
    *,
    table: str,
    id_column: str,
    ids: Sequence[str],
    select_columns: Sequence[str],
    order_by: str,
) -> list[dict[str, Any]]:
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"SELECT {', '.join(select_columns)} FROM {table} "
        f"WHERE {id_column} IN ({placeholders}) ORDER BY {order_by}",
        tuple(ids),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _gateway_commands_for_subject(
    connection: sqlite3.Connection,
    *,
    direct_command_ids: Sequence[str],
    candidate_instance_id: str,
    live_sim_intent_ids: Sequence[str],
    order_plan_ids: Sequence[str],
) -> tuple[list[dict[str, Any]], int, int, int]:
    direct_ids = {str(value) for value in direct_command_ids if str(value)}
    intent_ids = {str(value) for value in live_sim_intent_ids if str(value)}
    plan_ids = {str(value) for value in order_plan_ids if str(value)}
    raw_tokens = {
        candidate_instance_id,
        *intent_ids,
        *plan_ids,
    } - {""}
    globally_linked_command_ids = {
        str(row[0])
        for table in (
            "live_sim_intents",
            "live_sim_orders",
            "live_sim_exit_intents",
            "live_sim_cancel_intents",
        )
        for row in connection.execute(
            f"SELECT gateway_command_id FROM {table} WHERE gateway_command_id IS NOT NULL"
        ).fetchall()
        if row[0]
    }
    globally_known_candidate_ids = {
        str(row[0])
        for row in connection.execute("SELECT candidate_instance_id FROM candidates").fetchall()
        if row[0]
    }
    globally_known_intent_ids = {
        str(row[0])
        for table, column in (
            ("live_sim_intents", "live_sim_intent_id"),
            ("live_sim_exit_intents", "exit_intent_id"),
        )
        for row in connection.execute(f"SELECT {column} FROM {table}").fetchall()
        if row[0]
    }
    globally_known_plan_ids = {
        str(row[0])
        for table in ("order_plan_drafts", "order_plan_drafts_latest")
        for row in connection.execute(f"SELECT order_plan_id FROM {table}").fetchall()
        if row[0]
    }
    matched: list[dict[str, Any]] = []
    malformed_count = 0
    malformed_unbound_order_count = 0
    unbound_order_count = 0
    rows = connection.execute(
        "SELECT * FROM gateway_commands ORDER BY created_at, command_id"
    ).fetchall()
    for raw_row in rows:
        row = _row_dict(raw_row)
        command_id = str(row.get("command_id") or "")
        payload_json = str(row.get("payload_json") or "")
        references, malformed = _gateway_command_payload_references(payload_json)
        associated = bool(
            command_id in direct_ids
            or candidate_instance_id in references["candidate_instance_id"]
            or intent_ids.intersection(references["live_sim_intent_id"])
            or plan_ids.intersection(references["order_plan_id"])
        )
        if malformed and not associated:
            associated = any(token in payload_json for token in raw_tokens)
        globally_bound = bool(
            command_id in globally_linked_command_ids
            or globally_known_candidate_ids.intersection(
                references["candidate_instance_id"]
            )
            or globally_known_intent_ids.intersection(
                references["live_sim_intent_id"]
            )
            or globally_known_plan_ids.intersection(references["order_plan_id"])
        )
        if not associated and not globally_bound and _is_order_command_type(
            row.get("command_type")
        ):
            associated = True
            unbound_order_count += 1
            if malformed:
                malformed_unbound_order_count += 1
        if not associated:
            continue
        matched.append(row)
        if malformed:
            malformed_count += 1
    return (
        matched,
        malformed_count,
        malformed_unbound_order_count,
        unbound_order_count,
    )


def _unbound_order_artifacts(
    connection: sqlite3.Connection,
) -> dict[str, list[dict[str, Any]]]:
    def ids(table: str, column: str) -> set[str]:
        return {
            str(row[0])
            for row in connection.execute(f"SELECT {column} FROM {table}").fetchall()
            if row[0]
        }

    def rows(table: str) -> list[dict[str, Any]]:
        return [
            _row_dict(row)
            for row in connection.execute(f"SELECT * FROM {table}").fetchall()
        ]

    live_intent_ids = ids("live_sim_intents", "live_sim_intent_id")
    live_order_ids = ids("live_sim_orders", "live_sim_order_id")
    live_positions = rows("live_sim_positions")
    live_position_ids = ids("live_sim_positions", "position_id")
    live_exit_intents = rows("live_sim_exit_intents")
    live_exit_intent_ids = {
        str(row["exit_intent_id"]) for row in live_exit_intents if row.get("exit_intent_id")
    }
    live_exit_order_ids = {
        str(row["live_sim_order_id"])
        for row in live_exit_intents
        if row.get("live_sim_order_id")
    }

    dry_intent_ids = ids("dry_run_intents", "dry_run_intent_id")
    dry_order_ids = ids("dry_run_orders", "dry_run_order_id")
    dry_executions = rows("dry_run_executions")
    dry_execution_pairs = {
        (str(row.get("trade_date") or ""), str(row.get("code") or ""))
        for row in dry_executions
        if row.get("trade_date") and row.get("code")
    }
    dry_positions = rows("dry_run_positions")
    dry_position_ids = ids("dry_run_positions", "dry_run_position_id")
    dry_exit_intent_ids = ids("dry_run_exit_intents", "dry_run_exit_intent_id")
    dry_exit_order_ids = ids("dry_run_exit_orders", "dry_run_exit_order_id")

    return {
        "live_sim_positions": [
            row
            for row in live_positions
            if str(row.get("source_live_sim_order_id") or "") not in live_order_ids
            and str(row.get("source_live_sim_intent_id") or "")
            not in live_intent_ids
        ],
        "live_sim_orders": [
            row
            for row in rows("live_sim_orders")
            if str(row.get("live_sim_intent_id") or "")
            not in live_intent_ids | live_exit_intent_ids
            and str(row.get("live_sim_order_id") or "") not in live_exit_order_ids
        ],
        "live_sim_executions": [
            row
            for row in rows("live_sim_executions")
            if str(row.get("live_sim_order_id") or "") not in live_order_ids
            and str(row.get("live_sim_intent_id") or "")
            not in live_intent_ids | live_exit_intent_ids
        ],
        "dry_run_orders": [
            row
            for row in rows("dry_run_orders")
            if str(row.get("dry_run_intent_id") or "") not in dry_intent_ids
        ],
        "dry_run_executions": [
            row
            for row in dry_executions
            if str(row.get("dry_run_order_id") or "") not in dry_order_ids
            and str(row.get("dry_run_intent_id") or "") not in dry_intent_ids
        ],
        "dry_run_positions": [
            row
            for row in dry_positions
            if (
                str(row.get("trade_date") or ""),
                str(row.get("code") or ""),
            )
            not in dry_execution_pairs
        ],
        "live_sim_exit_intents": [
            row
            for row in live_exit_intents
            if str(row.get("position_id") or "") not in live_position_ids
        ],
        "live_sim_cancel_intents": [
            row
            for row in rows("live_sim_cancel_intents")
            if str(row.get("live_sim_order_id") or "") not in live_order_ids
        ],
        "dry_run_exit_intents": [
            row
            for row in rows("dry_run_exit_intents")
            if str(row.get("dry_run_position_id") or "") not in dry_position_ids
        ],
        "dry_run_exit_orders": [
            row
            for row in rows("dry_run_exit_orders")
            if str(row.get("dry_run_exit_intent_id") or "") not in dry_exit_intent_ids
            and str(row.get("dry_run_position_id") or "") not in dry_position_ids
        ],
        "dry_run_exit_executions": [
            row
            for row in rows("dry_run_exit_executions")
            if str(row.get("dry_run_exit_order_id") or "") not in dry_exit_order_ids
            and str(row.get("dry_run_exit_intent_id") or "")
            not in dry_exit_intent_ids
            and str(row.get("dry_run_position_id") or "") not in dry_position_ids
        ],
    }


def _gateway_command_payload_references(
    payload_json: str,
) -> tuple[dict[str, set[str]], bool]:
    references: dict[str, set[str]] = {
        "candidate_instance_id": set(),
        "live_sim_intent_id": set(),
        "order_plan_id": set(),
    }
    try:
        loaded = json.loads(payload_json)
    except (TypeError, ValueError):
        return references, True
    if not isinstance(loaded, Mapping):
        return references, True

    malformed = False
    containers: list[Mapping[str, Any]] = [loaded]
    if "metadata" in loaded and loaded.get("metadata") is not None:
        metadata = loaded.get("metadata")
        if isinstance(metadata, Mapping):
            containers.append(metadata)
        else:
            malformed = True
    for container in containers:
        for key in references:
            if key not in container or container.get(key) is None:
                continue
            value = container.get(key)
            if not isinstance(value, str) or not value.strip():
                malformed = True
                continue
            references[key].add(value.strip())
    return references, malformed


def _is_order_command_type(value: object) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"send_order", "cancel_order", "modify_order"} or "order" in normalized


def _merge_rows(
    first: Sequence[Mapping[str, Any]],
    second: Sequence[Mapping[str, Any]],
    *,
    key: str,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in (*first, *second):
        item = dict(row)
        row_key = str(item.get(key) or "")
        if not row_key:
            raise PipelineCoherencyDispositionError(
                "PIPELINE_DOWNSTREAM_SCHEMA_INVALID",
                f"{key} is missing from a downstream CAS row",
            )
        previous = merged.get(row_key)
        if previous is not None and previous != item:
            raise PipelineCoherencyDispositionError(
                "PIPELINE_DOWNSTREAM_DUPLICATE_CONFLICT",
                f"conflicting downstream rows share {key}",
            )
        merged[row_key] = item
    return [merged[row_key] for row_key in sorted(merged)]


def _candidate_rows(
    connection: sqlite3.Connection,
    *,
    table: str,
    candidate_id: str,
    order_by: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"SELECT * FROM {table} WHERE candidate_instance_id = ? ORDER BY {order_by}",
        (candidate_id,),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _trade_code_rows(
    connection: sqlite3.Connection,
    *,
    table: str,
    trade_date: str,
    code: str,
    order_by: str,
) -> list[dict[str, Any]]:
    if not trade_date or not code:
        return []
    rows = connection.execute(
        f"SELECT * FROM {table} WHERE trade_date = ? AND code = ? ORDER BY {order_by}",
        (trade_date, code),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _nested_value(value: Mapping[str, Any], key: str, nested: str) -> Any:
    child = value.get(key)
    return child.get(nested) if isinstance(child, Mapping) else None


def _subject_key(trade_date: str, candidate_id: str) -> str:
    return f"{trade_date}:{candidate_id}"


def _require_action(value: object) -> str:
    action = _require_text("action", value).upper()
    if action not in _ACTIONS:
        raise ValueError(f"unsupported pipeline disposition action: {action}")
    return action


def _require_text(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 500:
        raise ValueError(f"{name} must be non-empty and at most 500 characters")
    return normalized


def _require_sha256(name: str, value: object) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return normalized


def _require_safety_snapshot(value: Mapping[str, Any] | None) -> dict[str, Any]:
    snapshot = dict(value or {})
    required_values: dict[str, Any] = {
        "trading_profile": "OBSERVE",
        "trading_mode": "OBSERVE",
        "live_sim_allowed": False,
        "live_real_allowed": False,
        "kill_switch_active": True,
        "incremental_worker_enabled": False,
        "enabled_command_producers": [],
        "explicit_env_file": True,
        "theme_refresh_queue_market_scan_commands": False,
        "not_order_intent": True,
        "order_commands_allowed": False,
    }
    invalid = [key for key, expected in required_values.items() if snapshot.get(key) != expected]
    if invalid:
        raise PipelineCoherencyDispositionError(
            "PIPELINE_DISPOSITION_RUNTIME_UNSAFE",
            "pipeline disposition safety snapshot is incomplete or unsafe",
            details={"reason_codes": [f"UNSAFE_SNAPSHOT:{key.upper()}" for key in invalid]},
        )
    return snapshot


def _sha256_json(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _row_dict(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _stored_bool(value: object) -> bool:
    return type(value) is int and value == 1


def _stored_fixed_int(value: object, expected: int) -> bool:
    return type(value) is int and value == expected


def _stored_disposition_safety_valid(
    row: sqlite3.Row | Mapping[str, Any],
) -> bool:
    data = _row_dict(row)
    try:
        evidence_json = json.loads(str(data.get("evidence_json") or ""))
        safety_snapshot = json.loads(str(data.get("safety_snapshot_json") or ""))
        if not isinstance(evidence_json, Mapping) or not isinstance(
            safety_snapshot, Mapping
        ):
            return False
        normalized_safety = _require_safety_snapshot(safety_snapshot)
        request_payload = {
            "contract": "pipeline-coherency-disposition-request.v1",
            "trade_date": data.get("trade_date"),
            "candidate_instance_id": data.get("candidate_instance_id"),
            "action": data.get("action"),
            "pipeline_fingerprint": data.get("expected_pipeline_fingerprint"),
            "subject_version": data.get("expected_subject_version"),
            "source_fingerprint": data.get("expected_source_fingerprint"),
            "candidate_fingerprint": data.get("expected_candidate_fingerprint"),
            "downstream_fingerprint": data.get("expected_downstream_fingerprint"),
            "boundary_fingerprint": data.get("expected_boundary_fingerprint"),
            "reason_code": data.get("reason_code"),
            "operator_id": data.get("operator_id"),
            "evidence_type": data.get("evidence_type"),
            "evidence_ref": data.get("evidence_ref"),
            "evidence_sha256": data.get("evidence_sha256"),
            "evidence_json": dict(evidence_json),
            "safety_snapshot": normalized_safety,
        }
        request_hash = _sha256_json(request_payload)
        expected_disposition_id = (
            "pipeline_disposition_"
            + hashlib.sha256(
                f"{data.get('request_id')}:{request_hash}".encode()
            ).hexdigest()[:32]
        )
    except (TypeError, ValueError, PipelineCoherencyDispositionError):
        return False
    return bool(
        _stored_fixed_int(data.get("observe_only"), 1)
        and _stored_fixed_int(data.get("live_sim_allowed"), 0)
        and _stored_fixed_int(data.get("live_real_allowed"), 0)
        and _stored_fixed_int(data.get("order_commands_allowed"), 0)
        and _stored_fixed_int(data.get("not_order_intent"), 1)
        and _stored_fixed_int(data.get("no_order_side_effects"), 1)
        and _stored_fixed_int(data.get("auto_run_evaluation"), 0)
        and data.get("subject_key")
        == _subject_key(
            str(data.get("trade_date") or ""),
            str(data.get("candidate_instance_id") or ""),
        )
        and data.get("request_hash") == request_hash
        and data.get("disposition_id") == expected_disposition_id
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _dedupe(values: Sequence[object]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result
