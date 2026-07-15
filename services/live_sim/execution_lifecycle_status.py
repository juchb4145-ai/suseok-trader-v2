from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.utils import parse_timestamp, validate_stock_code

_PAGE_LIMIT = 500
_CLASSIFICATIONS = (
    "HISTORICAL_RUNTIME_STATUS_AUDIT",
    "ACTIVE_LIFECYCLE_BLOCKER",
    "MANUAL_REVIEW_BLOCKER",
)
_RUNTIME_STATUS_EVENT_TYPES = {"heartbeat", "orderability"}
_IDENTIFIER_KEYS = {
    "command_id",
    "gateway_command_id",
    "idempotency_key",
    "live_sim_order_id",
    "client_order_id",
    "order_id",
    "order_no",
    "broker_order_id",
    "broker_order_no",
    "original_order_no",
    "live_sim_execution_id",
    "execution_id",
    "broker_execution_id",
    "chejan_execution_id",
    "execution_key",
    "position_id",
    "live_sim_intent_id",
    "intent_id",
}


def build_live_sim_execution_lifecycle_status(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
    offset: int = 0,
    code: str | None = None,
) -> dict[str, Any]:
    """Build a strict read-only view over mirrored LIVE_SIM lifecycle errors.

    ``code`` only narrows the diagnostic item page. Global qualification, counts,
    and inventory digests always cover the complete inventory.
    """

    bounded_limit = min(max(int(limit), 1), _PAGE_LIMIT)
    bounded_offset = max(int(offset), 0)
    code_filter = None if code is None else validate_stock_code(code)

    nested = connection.in_transaction
    savepoint = "fast0_execution_lifecycle_read_snapshot"
    if nested:
        connection.execute(f"SAVEPOINT {savepoint}")
    else:
        connection.execute("BEGIN DEFERRED")
    try:
        result = _build_snapshot(
            connection,
            limit=bounded_limit,
            offset=bounded_offset,
            code_filter=code_filter,
        )
    except BaseException:
        _rollback_read_snapshot(connection, nested=nested, savepoint=savepoint)
        raise
    _rollback_read_snapshot(connection, nested=nested, savepoint=savepoint)
    return result


def _build_snapshot(
    connection: sqlite3.Connection,
    *,
    limit: int,
    offset: int,
    code_filter: str | None,
) -> dict[str, Any]:
    errors = _read_error_rows(connection)
    lifecycle = _read_lifecycle_error_rows(connection)
    reconcile_events = _read_reconcile_event_rows(connection)
    reconcile_snapshots = _read_reconcile_snapshot_rows(connection)
    inventory_digest = _inventory_digest(
        errors, lifecycle, reconcile_events, reconcile_snapshots
    )

    mirror_subjects = _classify_subjects(errors, lifecycle)
    scanned_inventory_digest = _inventory_digest(
        errors, lifecycle, reconcile_events, reconcile_snapshots
    )
    reconcile_summary = _classify_reconcile(reconcile_snapshots, reconcile_events)
    subjects = [*mirror_subjects, *reconcile_summary["subjects"]]
    subjects.sort(
        key=lambda item: (str(item["created_at"]), str(item["subject_id"])),
        reverse=True,
    )

    ending_errors = _read_error_rows(connection)
    ending_lifecycle = _read_lifecycle_error_rows(connection)
    ending_reconcile_events = _read_reconcile_event_rows(connection)
    ending_reconcile_snapshots = _read_reconcile_snapshot_rows(connection)
    ending_inventory_digest = _inventory_digest(
        ending_errors,
        ending_lifecycle,
        ending_reconcile_events,
        ending_reconcile_snapshots,
    )
    inventory_count_consistent = bool(
        len(errors) == len(ending_errors)
        and len(lifecycle) == len(ending_lifecycle)
        and len(reconcile_events) == len(ending_reconcile_events)
        and len(reconcile_snapshots) == len(ending_reconcile_snapshots)
        and inventory_digest == scanned_inventory_digest == ending_inventory_digest
    )

    counts = {name: 0 for name in _CLASSIFICATIONS}
    for subject in subjects:
        counts[str(subject["classification"])] += 1
    historical_count = counts["HISTORICAL_RUNTIME_STATUS_AUDIT"]
    active_count = counts["ACTIVE_LIFECYCLE_BLOCKER"]
    manual_count = counts["MANUAL_REVIEW_BLOCKER"]
    exact_pair_count = sum(
        1 for subject in mirror_subjects if subject["mirror_status"] == "EXACT_1_TO_1"
    )
    mirror_consistent = bool(
        len(errors) == len(lifecycle) == exact_pair_count == len(mirror_subjects)
    )

    canonical_reasons: list[str] = []
    if errors:
        canonical_reasons.append("LIVE_SIM_RAW_ERROR_ROWS_PRESENT")
    if lifecycle:
        canonical_reasons.append("LIVE_SIM_RAW_LIFECYCLE_ERRORS_PRESENT")
    if reconcile_events:
        canonical_reasons.append("LIVE_SIM_RAW_RECONCILE_MISMATCH_EVENTS_PRESENT")
    if reconcile_summary["active_blocker_count"]:
        canonical_reasons.append("LIVE_SIM_ACTIVE_RECONCILE_MISMATCH_PRESENT")
    if reconcile_summary["manual_review_count"]:
        canonical_reasons.append("LIVE_SIM_RECONCILE_EVIDENCE_MANUAL_REVIEW_REQUIRED")
    canonical_status = "BLOCKED" if canonical_reasons else "PASS"

    qualification_reasons: list[str] = []
    if active_count:
        qualification_reasons.append("LIVE_SIM_ACTIVE_LIFECYCLE_BLOCKERS_PRESENT")
    if manual_count:
        qualification_reasons.append("LIVE_SIM_LIFECYCLE_MANUAL_REVIEW_REQUIRED")
    if reconcile_summary["active_blocker_count"]:
        qualification_reasons.append("LIVE_SIM_ACTIVE_RECONCILE_BLOCKER_PRESENT")
    if reconcile_summary["manual_review_count"]:
        qualification_reasons.append("LIVE_SIM_RECONCILE_MANUAL_REVIEW_REQUIRED")
    if not mirror_consistent:
        qualification_reasons.append("LIVE_SIM_LIFECYCLE_MIRROR_INCONSISTENT")
    if not inventory_count_consistent:
        qualification_reasons.append("LIVE_SIM_LIFECYCLE_INVENTORY_CHANGED_DURING_READ")

    semantic_blockers = active_count + manual_count
    integrity_blockers = int(not mirror_consistent and manual_count == 0) + int(
        not inventory_count_consistent
    )
    effective_blocker_count = semantic_blockers + integrity_blockers
    qualification_status = "PASS" if effective_blocker_count == 0 else "BLOCKED"

    diagnostic = (
        subjects
        if code_filter is None
        else [item for item in subjects if item.get("code") == code_filter]
    )
    page_items = diagnostic[offset : offset + limit]
    returned_count = len(page_items)
    full_count = len(diagnostic)
    has_more = offset + returned_count < full_count
    next_offset = offset + returned_count if has_more else None
    pagination = {
        "limit": limit,
        "offset": offset,
        "returned_count": returned_count,
        "full_count": full_count,
        "has_more": has_more,
        "next_offset": next_offset,
    }
    return {
        "status": qualification_status,
        "qualification_status": qualification_status,
        "qualification_reason_codes": _dedupe(qualification_reasons),
        "canonical_status": canonical_status,
        "canonical_reason_codes": _dedupe(canonical_reasons),
        "canonical": {
            "status": canonical_status,
            "reason_codes": _dedupe(canonical_reasons),
            "raw_error_count": len(errors),
            "raw_lifecycle_error_count": len(lifecycle),
            "read_only": True,
            "no_order_side_effects": True,
        },
        "classification_counts": counts,
        "raw_error_count": len(errors),
        "raw_error_row_count": len(errors),
        "mirror_lifecycle_count": len(lifecycle),
        "raw_lifecycle_error_count": len(lifecycle),
        "logical_subject_count": len(subjects),
        "mirrored_pair_count": exact_pair_count,
        "active_lifecycle_blocker_count": active_count,
        "active_blocker_count": active_count,
        "historical_runtime_status_audit_count": historical_count,
        "historical_audit_count": historical_count,
        "manual_review_blocker_count": manual_count,
        "manual_review_count": manual_count,
        "active_reconcile_blocker_count": reconcile_summary["active_blocker_count"],
        "historical_reconcile_event_count": reconcile_summary["historical_event_count"],
        "reconcile_manual_review_count": reconcile_summary["manual_review_count"],
        "effective_blocker_count": effective_blocker_count,
        "mirror_consistent": mirror_consistent,
        "inventory_count_consistent": inventory_count_consistent,
        "inventory_digest": inventory_digest,
        "scanned_inventory_digest": scanned_inventory_digest,
        "ending_inventory_digest": ending_inventory_digest,
        "reconcile": reconcile_summary["public"],
        "code_filter": code_filter,
        "code_filter_diagnostic_only": True,
        **pagination,
        "pagination": pagination,
        "items": page_items,
        "read_only": True,
        "observe_only": True,
        "no_order_side_effects": True,
        "real_order_allowed": False,
    }


def _classify_subjects(
    errors: Sequence[Mapping[str, Any]],
    lifecycle: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    error_records = [_error_record(row) for row in errors]
    lifecycle_records = [_lifecycle_record(row) for row in lifecycle]
    groups: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: {"errors": [], "lifecycle": []}
    )
    for record in error_records:
        groups[str(record["anchor"])]["errors"].append(record)
    for record in lifecycle_records:
        groups[str(record["anchor"])]["lifecycle"].append(record)

    subjects = [
        _classify_group(anchor, group["errors"], group["lifecycle"])
        for anchor, group in groups.items()
    ]
    subjects.sort(key=lambda item: (str(item["created_at"]), str(item["subject_id"])), reverse=True)
    return subjects


def _classify_group(
    anchor: str,
    errors: Sequence[Mapping[str, Any]],
    lifecycle: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reasons: list[str] = []
    malformed = False
    for record in [*errors, *lifecycle]:
        record_reasons = list(record.get("parse_reason_codes") or [])
        reasons.extend(record_reasons)
        malformed = malformed or bool(record_reasons)

    error_count = len(errors)
    lifecycle_count = len(lifecycle)
    duplicate = error_count > 1 or lifecycle_count > 1
    if duplicate:
        reasons.append("LIVE_SIM_MIRROR_DUPLICATE_FINGERPRINT")
    if error_count == 0:
        reasons.append("LIVE_SIM_MIRROR_ORPHAN_LIFECYCLE")
    if lifecycle_count == 0:
        reasons.append("LIVE_SIM_MIRROR_ORPHAN_ERROR")

    exact = bool(
        error_count == lifecycle_count == 1
        and not malformed
        and errors[0].get("shape_valid") is True
        and errors[0].get("full_fingerprint") == lifecycle[0].get("full_fingerprint")
        and errors[0].get("intent_metadata_consistent") is True
        and errors[0].get("event_metadata_consistent") is True
        and lifecycle[0].get("shape_valid") is True
    )
    if (
        error_count == 1
        and errors[0].get("intent_metadata_consistent") is not True
    ):
        reasons.append("LIVE_SIM_MIRROR_INTENT_METADATA_MISMATCH")
    if error_count == 1 and errors[0].get("event_metadata_consistent") is not True:
        reasons.append("LIVE_SIM_MIRROR_EVENT_METADATA_MISMATCH")
    if error_count == lifecycle_count == 1 and not malformed and not exact:
        reasons.append("LIVE_SIM_MIRROR_FIELD_MISMATCH")
    if lifecycle and any(record.get("shape_valid") is not True for record in lifecycle):
        reasons.append("LIVE_SIM_MIRROR_LIFECYCLE_FIELDS_INVALID")
    if errors and any(record.get("shape_valid") is not True for record in errors):
        reasons.append("LIVE_SIM_MIRROR_ERROR_FIELDS_INVALID")

    error = errors[0] if error_count == 1 else None
    event = lifecycle[0] if lifecycle_count == 1 else None
    if not exact:
        classification = "MANUAL_REVIEW_BLOCKER"
        mirror_status = (
            "DUPLICATE_FINGERPRINT"
            if duplicate
            else "ORPHAN_ERROR"
            if lifecycle_count == 0
            else "ORPHAN_LIFECYCLE"
            if error_count == 0
            else "MALFORMED_JSON"
            if malformed
            else "FIELD_MISMATCH"
        )
    elif _is_historical_runtime_status(error, event):
        classification = "HISTORICAL_RUNTIME_STATUS_AUDIT"
        mirror_status = "EXACT_1_TO_1"
        reasons.append("LIVE_SIM_HISTORICAL_RUNTIME_STATUS_AUDIT")
    else:
        classification = "ACTIVE_LIFECYCLE_BLOCKER"
        mirror_status = "EXACT_1_TO_1"
        reasons.append("LIVE_SIM_ACTIVE_LIFECYCLE_ERROR")

    representative = error or event or {}
    source_fingerprints = sorted(
        str(record["surface_fingerprint"]) for record in [*errors, *lifecycle]
    )
    subject_fingerprint = _sha(
        {
            "anchor": anchor,
            "source_fingerprints": source_fingerprints,
            "classification": classification,
            "mirror_status": mirror_status,
        }
    )
    payload = representative.get("payload")
    raw_inner_event_type = (
        payload.get("event_type") if isinstance(payload, Mapping) else None
    )
    inner_event_type = (
        raw_inner_event_type
        if raw_inner_event_type in _RUNTIME_STATUS_EVENT_TYPES
        else None
    )
    metadata_consistent = bool(
        exact and error is not None and event is not None and _event_metadata_consistent(error)
    )
    identifier_free = bool(
        exact and error is not None and event is not None and _identifier_free(error, event)
    )
    created_values = [str(record.get("created_at") or "") for record in [*errors, *lifecycle]]
    return {
        "subject_id": subject_fingerprint,
        "subject_fingerprint": subject_fingerprint,
        "classification": classification,
        "reason_codes": _dedupe(reasons),
        "mirror_status": mirror_status,
        "error_surface_count": error_count,
        "lifecycle_surface_count": lifecycle_count,
        "created_at": max(created_values, default=""),
        "code": representative.get("code"),
        "inner_event_type": inner_event_type,
        "payload_sha256": representative.get("payload_sha256"),
        "event_metadata_consistent": metadata_consistent,
        "identifier_free": identifier_free,
    }


def _error_record(row: Mapping[str, Any]) -> dict[str, Any]:
    payload, parse_reason = _parse_json_mapping(
        row.get("payload_json"), "LIVE_SIM_MIRROR_ERROR_PAYLOAD_JSON_INVALID"
    )
    unknown_gateway_event = row.get("error_message") == "UNKNOWN_LIVE_SIM_GATEWAY_EVENT"
    intent_values = _identifier_values(payload, "live_sim_intent_id")
    stored_intent = row.get("live_sim_intent_id")
    intent_metadata_consistent = bool(
        not unknown_gateway_event
        or (not _present(stored_intent) and not intent_values)
        or (
            _present(stored_intent)
            and bool(intent_values)
            and all(str(value) == str(stored_intent) for value in intent_values)
        )
    )
    event_metadata_consistent = bool(
        not unknown_gateway_event
        or (
            _unknown_gateway_metadata_matches_row(payload, row)
            and _gateway_event_metadata_consistent(payload)
        )
    )
    shared = {
        "created_at": row.get("created_at"),
        "reason": row.get("error_message"),
        "live_sim_order_id": row.get("live_sim_order_id"),
        "run_id": row.get("run_id"),
        "code": row.get("code"),
        "live_sim_intent_id": row.get("live_sim_intent_id") if unknown_gateway_event else None,
        "payload": payload,
    }
    anchor = _anchor(row.get("created_at"), payload, "error", row.get("id"))
    shape_valid = _error_row_shape_valid(row)
    return {
        "anchor": anchor,
        "full_fingerprint": _sha(shared) if payload is not None else None,
        "surface_fingerprint": _sha(dict(row)),
        "parse_reason_codes": [parse_reason] if parse_reason else [],
        "shape_valid": shape_valid,
        "intent_metadata_consistent": intent_metadata_consistent,
        "event_metadata_consistent": event_metadata_consistent,
        "payload": payload,
        "payload_sha256": (
            _sha(payload) if payload is not None else _sha(str(row.get("payload_json")))
        ),
        "created_at": row.get("created_at"),
        "code": _public_stock_code(row.get("code")),
        "run_id": row.get("run_id"),
        "live_sim_intent_id": row.get("live_sim_intent_id"),
        "live_sim_order_id": row.get("live_sim_order_id"),
        "reason": row.get("error_message"),
    }


def _lifecycle_record(row: Mapping[str, Any]) -> dict[str, Any]:
    evidence, evidence_reason = _parse_json_mapping(
        row.get("evidence_json"), "LIVE_SIM_MIRROR_LIFECYCLE_EVIDENCE_JSON_INVALID"
    )
    payload: Mapping[str, Any] | None = None
    payload_reason: str | None = None
    if evidence is not None:
        raw_payload = evidence.get("payload")
        if isinstance(raw_payload, Mapping):
            payload = dict(raw_payload)
        else:
            payload_reason = "LIVE_SIM_MIRROR_EVIDENCE_PAYLOAD_INVALID"
    reasons = [reason for reason in (evidence_reason, payload_reason) if reason]
    shared = {
        "created_at": row.get("created_at"),
        "reason": row.get("reason"),
        "live_sim_order_id": row.get("live_sim_order_id"),
        "run_id": None if evidence is None else evidence.get("run_id"),
        "code": None if evidence is None else evidence.get("code"),
        "live_sim_intent_id": (
            _single_identifier_value(payload, "live_sim_intent_id")
            if row.get("reason") == "UNKNOWN_LIVE_SIM_GATEWAY_EVENT"
            else None
        ),
        "payload": payload,
    }
    shape_valid = bool(
        _lifecycle_row_scalars_valid(row)
        and evidence is not None
        and set(evidence) == {"run_id", "code", "payload"}
        and (
            evidence.get("run_id") is None
            or isinstance(evidence.get("run_id"), str)
        )
        and _optional_stock_code_valid(evidence.get("code"))
        and str(row.get("event_type") or "") == "LIFECYCLE_ERROR"
        and str(row.get("entity_type") or "") == "LIVE_SIM_ERROR"
        and row.get("entity_id") is None
        and row.get("position_id") is None
        and str(row.get("status") or "") == "ERROR"
        and _integer_equals(row.get("live_sim_only"), 1)
        and _integer_equals(row.get("live_real_allowed"), 0)
    )
    anchor = _anchor(
        row.get("created_at"), payload, "lifecycle", row.get("lifecycle_event_id")
    )
    return {
        "anchor": anchor,
        "full_fingerprint": _sha(shared) if payload is not None else None,
        "surface_fingerprint": _sha(dict(row)),
        "parse_reason_codes": reasons,
        "shape_valid": shape_valid,
        "intent_metadata_consistent": True,
        "event_metadata_consistent": True,
        "payload": payload,
        "payload_sha256": (
            _sha(payload) if payload is not None else _sha(str(row.get("evidence_json")))
        ),
        "created_at": row.get("created_at"),
        "code": (
            None if evidence is None else _public_stock_code(evidence.get("code"))
        ),
        "run_id": None if evidence is None else evidence.get("run_id"),
        "entity_id": row.get("entity_id"),
        "live_sim_order_id": row.get("live_sim_order_id"),
        "position_id": row.get("position_id"),
        "reason": row.get("reason"),
    }


def _error_row_shape_valid(row: Mapping[str, Any]) -> bool:
    row_id = row.get("id")
    created_at = row.get("created_at")
    return bool(
        isinstance(row_id, int)
        and not isinstance(row_id, bool)
        and row_id > 0
        and all(
            value is None or isinstance(value, str)
            for value in (
                row.get("run_id"),
                row.get("live_sim_intent_id"),
                row.get("live_sim_order_id"),
            )
        )
        and _optional_stock_code_valid(row.get("code"))
        and _nonempty_string(row.get("error_message"))
        and isinstance(row.get("payload_json"), str)
        and isinstance(created_at, str)
        and _parsed_timestamp(created_at) is not None
    )


def _lifecycle_row_scalars_valid(row: Mapping[str, Any]) -> bool:
    created_at = row.get("created_at")
    return bool(
        _nonempty_string(row.get("lifecycle_event_id"))
        and _nonempty_string(row.get("event_type"))
        and _nonempty_string(row.get("entity_type"))
        and all(
            value is None or isinstance(value, str)
            for value in (
                row.get("entity_id"),
                row.get("live_sim_order_id"),
                row.get("position_id"),
                row.get("status"),
                row.get("reason"),
            )
        )
        and isinstance(row.get("evidence_json"), str)
        and isinstance(created_at, str)
        and _parsed_timestamp(created_at) is not None
        and _exact_integer(row.get("live_sim_only"), 1)
        and _exact_integer(row.get("live_real_allowed"), 0)
    )


def _optional_stock_code_valid(value: Any) -> bool:
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    try:
        return validate_stock_code(value) == value
    except (TypeError, ValueError):
        return False


def _public_stock_code(value: Any) -> str | None:
    return value if _optional_stock_code_valid(value) else None


def _nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _exact_integer(value: Any, expected: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value == expected


def _is_historical_runtime_status(
    error: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
) -> bool:
    if error is None or lifecycle is None:
        return False
    payload = error.get("payload")
    event_type = payload.get("event_type") if isinstance(payload, Mapping) else None
    return bool(
        error.get("shape_valid") is True
        and lifecycle.get("shape_valid") is True
        and error.get("reason") == "UNKNOWN_LIVE_SIM_GATEWAY_EVENT"
        and lifecycle.get("reason") == "UNKNOWN_LIVE_SIM_GATEWAY_EVENT"
        and event_type in _RUNTIME_STATUS_EVENT_TYPES
        and _identifier_free(error, lifecycle)
        and _event_metadata_consistent(error)
    )


def _event_metadata_consistent(error: Mapping[str, Any]) -> bool:
    return _gateway_event_metadata_consistent(error.get("payload"))


def _gateway_event_metadata_consistent(event: Any) -> bool:
    if not isinstance(event, Mapping) or not _gateway_event_envelope_valid(event):
        return False
    payload = event.get("payload")
    if not (
        isinstance(payload, Mapping)
        and str(event.get("event_id") or "").strip()
        and str(event.get("source") or "").strip()
        and str(event.get("ts") or "").strip()
    ):
        return False
    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, Mapping):
        return False
    metadata_mapping = metadata if isinstance(metadata, Mapping) else {}
    live_sim_marked = bool(
        payload.get("live_sim_only") is True
        or metadata_mapping.get("live_sim_only") is True
        or str(payload.get("mode") or "").upper() == "LIVE_SIM"
        or str(payload.get("live_mode") or "").upper() == "LIVE_SIM"
    )
    safety_consistent = all(
        (
            _optional_exact_bool(payload, "live_sim_only", expected=True),
            _optional_exact_bool(metadata_mapping, "live_sim_only", expected=True),
            _optional_exact_bool(payload, "live_real_allowed", expected=False),
            _optional_exact_bool(
                metadata_mapping, "live_real_allowed", expected=False
            ),
        )
    )
    return live_sim_marked and safety_consistent


def _optional_exact_bool(
    value: Mapping[str, Any], key: str, *, expected: bool
) -> bool:
    return key not in value or value.get(key) is expected


def _identifier_free(
    error: Mapping[str, Any], lifecycle: Mapping[str, Any]
) -> bool:
    if any(
        _present(value)
        for value in (
            error.get("live_sim_intent_id"),
            error.get("live_sim_order_id"),
            lifecycle.get("entity_id"),
            lifecycle.get("live_sim_order_id"),
            lifecycle.get("position_id"),
        )
    ):
        return False
    return not _contains_identifier(error.get("payload"))


def _contains_identifier(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if _is_identifier_key(key) and _present(nested):
                return True
            if _contains_identifier(nested):
                return True
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_contains_identifier(item) for item in value)
    return False


def _identifier_values(value: Any, target_key: str) -> list[Any]:
    values: list[Any] = []
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key).strip().lower() == target_key and _present(nested):
                values.append(nested)
            values.extend(_identifier_values(nested, target_key))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            values.extend(_identifier_values(item, target_key))
    return values


def _single_identifier_value(value: Any, target_key: str) -> Any:
    values = _identifier_values(value, target_key)
    normalized = {str(item) for item in values}
    return next(iter(normalized)) if len(normalized) == 1 else None


def _unknown_gateway_metadata_matches_row(
    event: Mapping[str, Any] | None,
    row: Mapping[str, Any],
) -> bool:
    if (
        not isinstance(event, Mapping)
        or not _gateway_event_envelope_valid(event)
        or not isinstance(event.get("payload"), Mapping)
    ):
        return False
    payload = event["payload"]
    raw_metadata = payload.get("metadata")
    if raw_metadata is not None and not isinstance(raw_metadata, Mapping):
        return False
    stored_code = row.get("code")
    if not _optional_stock_code_valid(stored_code):
        return False
    if "code" in payload:
        payload_code = payload.get("code")
        if (
            not _optional_stock_code_valid(payload_code)
            or payload_code != stored_code
        ):
            return False
    elif stored_code is not None:
        return False
    for key, column in (
        ("live_sim_intent_id", "live_sim_intent_id"),
        ("live_sim_order_id", "live_sim_order_id"),
    ):
        values = _identifier_values(payload, key)
        stored = row.get(column)
        if _present(stored):
            if not values or any(str(value) != str(stored) for value in values):
                return False
        elif values:
            return False
    return True


def _gateway_event_envelope_valid(event: Mapping[str, Any]) -> bool:
    required = {"event_id", "event_type", "source", "ts", "payload"}
    allowed = {*required, "command_id", "idempotency_key"}
    return bool(
        set(event) <= allowed
        and required <= set(event)
        and _nonempty_string(event.get("event_id"))
        and _nonempty_string(event.get("event_type"))
        and _nonempty_string(event.get("source"))
        and _nonempty_string(event.get("ts"))
        and isinstance(event.get("payload"), Mapping)
        and _parsed_timestamp(event.get("ts")) is not None
    )


def _is_identifier_key(value: Any) -> bool:
    key = str(value).strip().lower()
    if key in _IDENTIFIER_KEYS:
        return True
    for subject in ("command", "order", "execution", "position", "intent"):
        for suffix in ("id", "key", "no", "number"):
            if key == f"{subject}_{suffix}" or key.endswith(
                f"_{subject}_{suffix}"
            ):
                return True
    return False


def _classify_reconcile(
    snapshots: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    evaluations = [_evaluate_reconcile_snapshot(row) for row in snapshots]
    by_id = {
        str(item["reconcile_id"]): item
        for item in evaluations
        if str(item.get("reconcile_id") or "")
    }
    latest = evaluations[0] if evaluations else None
    latest_kind = None if latest is None else str(latest["kind"])
    latest_time = None if latest is None else latest.get("created_at_parsed")
    subjects: list[dict[str, Any]] = []
    latest_active_subject_present = False
    events_by_snapshot: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for event in events:
        entity_id = event.get("entity_id")
        if isinstance(entity_id, str) and entity_id in by_id:
            events_by_snapshot[entity_id].append(event)

    for event in events:
        evidence, evidence_reason = _parse_json_mapping(
            event.get("evidence_json"), "LIVE_SIM_RECONCILE_EVIDENCE_JSON_INVALID"
        )
        event_time = _parsed_timestamp(event.get("created_at"))
        shape_valid = _reconcile_event_shape_valid(event)
        linked = by_id.get(str(event.get("entity_id") or ""))
        linked_event_count = (
            0
            if linked is None
            else len(events_by_snapshot.get(str(linked.get("reconcile_id")), []))
        )
        reasons: list[str] = []
        if evidence_reason is not None or not shape_valid or event_time is None:
            classification = "MANUAL_REVIEW_BLOCKER"
            reasons.append("LIVE_SIM_RECONCILE_EVIDENCE_MANUAL_REVIEW_REQUIRED")
        elif linked is None:
            classification = "MANUAL_REVIEW_BLOCKER"
            reasons.append("LIVE_SIM_RECONCILE_EVENT_SNAPSHOT_UNLINKED")
        elif linked.get("kind") != "ACTIVE":
            classification = "MANUAL_REVIEW_BLOCKER"
            reasons.append("LIVE_SIM_RECONCILE_EVENT_SNAPSHOT_INVALID")
        elif linked_event_count != 1:
            classification = "MANUAL_REVIEW_BLOCKER"
            reasons.append("LIVE_SIM_RECONCILE_SNAPSHOT_EVENT_NOT_1_TO_1")
        elif not _reconcile_event_matches_snapshot(evidence, linked):
            classification = "MANUAL_REVIEW_BLOCKER"
            reasons.append("LIVE_SIM_RECONCILE_EVENT_SNAPSHOT_EVIDENCE_MISMATCH")
        elif latest_kind == "INVALID" or latest_time is None:
            classification = "MANUAL_REVIEW_BLOCKER"
            reasons.append("LIVE_SIM_RECONCILE_LATEST_SNAPSHOT_INVALID")
        elif latest_kind == "ACTIVE":
            if latest is not None and linked.get("row_sha256") == latest.get(
                "row_sha256"
            ):
                classification = "ACTIVE_LIFECYCLE_BLOCKER"
                reasons.append("LIVE_SIM_ACTIVE_RECONCILE_MISMATCH")
                latest_active_subject_present = True
            else:
                classification = "HISTORICAL_RUNTIME_STATUS_AUDIT"
                reasons.append("LIVE_SIM_HISTORICAL_RECONCILE_MISMATCH_AUDIT")
        elif (
            event_time > latest_time
            or linked.get("created_at_parsed") is None
            or linked["created_at_parsed"] > latest_time
        ):
            classification = "ACTIVE_LIFECYCLE_BLOCKER"
            reasons.append("LIVE_SIM_RECONCILE_EVENT_NEWER_THAN_CLEAN_SNAPSHOT")
        else:
            classification = "HISTORICAL_RUNTIME_STATUS_AUDIT"
            reasons.append("LIVE_SIM_HISTORICAL_RECONCILE_MISMATCH_AUDIT")
        linked_events = (
            []
            if linked is None
            else events_by_snapshot.get(str(linked.get("reconcile_id")), [])
        )
        if linked_event_count > 1 and event is not linked_events[0]:
            continue
        fingerprint = _sha(
            {
                "kind": "reconcile_event",
                "event": _sha(dict(event)),
                "linked_snapshot": None if linked is None else linked["row_sha256"],
                "latest_snapshot": None if latest is None else latest["row_sha256"],
                "classification": classification,
            }
        )
        subjects.append(
            _reconcile_subject(
                fingerprint,
                classification=classification,
                reasons=reasons,
                created_at=event.get("created_at"),
                payload_sha256=(
                    _sha(evidence)
                    if evidence is not None
                    else _sha(str(event.get("evidence_json")))
                ),
            )
        )

    for evaluation in evaluations:
        reconcile_id = evaluation.get("reconcile_id")
        linked_event_count = (
            len(events_by_snapshot.get(reconcile_id, []))
            if isinstance(reconcile_id, str)
            else 0
        )
        if evaluation.get("kind") == "CLEAN" or linked_event_count:
            continue
        reasons = (
            ["LIVE_SIM_RECONCILE_MISMATCH_SNAPSHOT_EVENT_MISSING"]
            if evaluation.get("kind") == "ACTIVE"
            else [
                "LIVE_SIM_RECONCILE_SNAPSHOT_MANUAL_REVIEW_REQUIRED",
                *list(evaluation.get("reason_codes") or []),
            ]
        )
        fingerprint = _sha(
            {
                "kind": "reconcile_snapshot",
                "snapshot": evaluation["row_sha256"],
                "latest_snapshot": (
                    None if latest is None else latest["row_sha256"]
                ),
                "classification": "MANUAL_REVIEW_BLOCKER",
            }
        )
        subjects.append(
            _reconcile_subject(
                fingerprint,
                classification="MANUAL_REVIEW_BLOCKER",
                reasons=_dedupe(reasons),
                created_at=evaluation.get("created_at"),
                payload_sha256=str(evaluation.get("snapshot_sha256") or ""),
            )
        )
    if latest_kind == "ACTIVE" and latest is not None and not latest_active_subject_present:
        fingerprint = _sha(
            {
                "kind": "reconcile_current_active_snapshot",
                "snapshot": latest["row_sha256"],
                "classification": "ACTIVE_LIFECYCLE_BLOCKER",
            }
        )
        subjects.append(
            _reconcile_subject(
                fingerprint,
                classification="ACTIVE_LIFECYCLE_BLOCKER",
                reasons=["LIVE_SIM_ACTIVE_RECONCILE_MISMATCH"],
                created_at=latest.get("created_at"),
                payload_sha256=str(latest.get("snapshot_sha256") or ""),
            )
        )
    active_count = sum(
        item["classification"] == "ACTIVE_LIFECYCLE_BLOCKER" for item in subjects
    )
    manual_count = sum(
        item["classification"] == "MANUAL_REVIEW_BLOCKER" for item in subjects
    )
    historical_events = sum(
        item["classification"] == "HISTORICAL_RUNTIME_STATUS_AUDIT"
        for item in subjects
    )
    if latest is None:
        public_status = "LATEST_SNAPSHOT_MISSING" if events else "NO_RECONCILE_EVIDENCE"
    elif latest_kind == "CLEAN":
        public_status = "LATEST_SNAPSHOT_CLEAN"
    elif latest_kind == "ACTIVE":
        public_status = "ACTIVE_RECONCILE_MISMATCH"
    else:
        public_status = "LATEST_SNAPSHOT_INVALID"
    return {
        "active_blocker_count": active_count,
        "manual_review_count": manual_count,
        "historical_event_count": historical_events,
        "subjects": subjects,
        "public": {
            "status": public_status,
            "latest_snapshot_present": latest is not None,
            "latest_snapshot_clean": latest_kind == "CLEAN",
            "mismatch_count": None if latest is None else latest.get("mismatch_count"),
            "blocking_new_buy": (
                None if latest is None else latest.get("blocking_new_buy")
            ),
            "allow_exit": None if latest is None else latest.get("allow_exit"),
            "snapshot_sha256": (
                None if latest is None else latest.get("snapshot_sha256")
            ),
            "created_at": (
                None if latest is None else _public_timestamp(latest.get("created_at"))
            ),
            "raw_mismatch_event_count": len(events),
            "historical_mismatch_event_count": historical_events,
            "malformed_evidence_count": manual_count,
            "snapshot_inventory_count": len(snapshots),
        },
    }


def _evaluate_reconcile_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    snapshot, json_reason = _parse_json_mapping(
        row.get("snapshot_json"), "LIVE_SIM_RECONCILE_SNAPSHOT_JSON_INVALID"
    )
    if json_reason is not None:
        reasons.append(json_reason)
    mismatch_count = _strict_nonnegative_int(row.get("mismatch_count"))
    blocking_new_buy = _strict_bool(row.get("blocking_new_buy"))
    allow_exit = _strict_bool(row.get("allow_exit"))
    if mismatch_count is None or blocking_new_buy is None or allow_exit is None:
        reasons.append("LIVE_SIM_RECONCILE_SCALAR_FIELDS_INVALID")
    if not str(row.get("reconcile_id") or "").strip():
        reasons.append("LIVE_SIM_RECONCILE_ID_INVALID")
    if not _integer_equals(row.get("live_sim_only"), 1):
        reasons.append("LIVE_SIM_RECONCILE_LIVE_SIM_ONLY_INVALID")
    created_at_parsed = _parsed_timestamp(row.get("created_at"))
    if created_at_parsed is None:
        reasons.append("LIVE_SIM_RECONCILE_CREATED_AT_INVALID")
    if snapshot is not None and not _reconcile_snapshot_payload_valid(
        snapshot,
        blocking_new_buy=blocking_new_buy,
        allow_exit=allow_exit,
    ):
        reasons.append("LIVE_SIM_RECONCILE_SNAPSHOT_CONTRACT_INVALID")

    raw_status = str(row.get("status") or "").strip().upper()
    mismatches = None if snapshot is None else snapshot.get("mismatches")
    kind = "INVALID"
    if not reasons and raw_status in {"OK", "LOCAL_ONLY_WITHOUT_BROKER_SNAPSHOT"}:
        if mismatch_count == 0 and blocking_new_buy is False and mismatches == []:
            kind = "CLEAN"
        else:
            reasons.append("LIVE_SIM_RECONCILE_CLEAN_STATE_MISMATCH")
    elif not reasons and raw_status == "RECONCILE_MISMATCH":
        if mismatch_count is not None and mismatch_count > 0 and bool(mismatches):
            kind = "ACTIVE"
        else:
            reasons.append("LIVE_SIM_RECONCILE_ACTIVE_STATE_MISMATCH")
    elif not reasons:
        reasons.append("LIVE_SIM_RECONCILE_STATUS_UNKNOWN")

    return {
        "kind": kind,
        "reason_codes": _dedupe(reasons),
        "reconcile_id": row.get("reconcile_id"),
        "mismatch_count": mismatch_count,
        "blocking_new_buy": blocking_new_buy,
        "allow_exit": allow_exit,
        "created_at": row.get("created_at"),
        "created_at_parsed": created_at_parsed,
        "snapshot_sha256": (
            _sha(snapshot)
            if snapshot is not None
            else _sha(str(row.get("snapshot_json")))
        ),
        "snapshot": snapshot,
        "row_sha256": _sha(dict(row)),
    }


def _reconcile_snapshot_payload_valid(
    snapshot: Mapping[str, Any],
    *,
    blocking_new_buy: bool | None,
    allow_exit: bool | None,
) -> bool:
    broker_snapshot_available = snapshot.get("broker_snapshot_available")
    broker_snapshot_status = snapshot.get("broker_snapshot_status")
    broker_snapshot_present = "broker_snapshot" in snapshot
    broker_snapshot = snapshot.get("broker_snapshot")
    broker_snapshot_contract_valid = bool(
        (
            broker_snapshot_available is True
            and broker_snapshot_status == "BROKER_SNAPSHOT_APPLIED"
            and broker_snapshot_present
            and _normalized_broker_snapshot_valid(broker_snapshot)
        )
        or (
            broker_snapshot_available is False
            and broker_snapshot_status == "BROKER_SNAPSHOT_UNAVAILABLE"
            and (
                not broker_snapshot_present
                or (
                    isinstance(broker_snapshot, Mapping)
                    and len(broker_snapshot) == 0
                )
            )
        )
    )
    return bool(
        broker_snapshot_contract_valid
        and isinstance(snapshot.get("open_orders"), list)
        and isinstance(snapshot.get("positions"), list)
        and isinstance(snapshot.get("mismatches"), list)
        and isinstance(snapshot.get("blocking_new_buy"), bool)
        and snapshot.get("blocking_new_buy") is blocking_new_buy
        and isinstance(snapshot.get("allow_exit"), bool)
        and snapshot.get("allow_exit") is allow_exit
        and snapshot.get("live_sim_only") is True
        and snapshot.get("live_real_allowed") is False
        and snapshot.get("broker_order_path") == "LIVE_SIM_ONLY"
    )


def _normalized_broker_snapshot_valid(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "account_id",
        "trade_date",
        "snapshot_at",
        "open_orders",
        "positions",
        "source",
        "complete",
        "live_sim_only",
        "live_real_allowed",
        "broker_order_path",
    }:
        return False
    open_orders = value.get("open_orders")
    positions = value.get("positions")
    return bool(
        isinstance(value.get("account_id"), str)
        and isinstance(value.get("trade_date"), str)
        and isinstance(value.get("snapshot_at"), str)
        and isinstance(value.get("source"), str)
        and isinstance(value.get("complete"), bool)
        and value.get("live_sim_only") is True
        and value.get("live_real_allowed") is False
        and value.get("broker_order_path") == "LIVE_SIM_ONLY"
        and isinstance(open_orders, list)
        and all(_normalized_broker_open_order_valid(item) for item in open_orders)
        and isinstance(positions, list)
        and all(_normalized_broker_position_valid(item) for item in positions)
    )


def _normalized_broker_open_order_valid(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "broker_order_no",
        "code",
        "side",
        "quantity",
        "remaining_quantity",
        "price",
        "raw",
    }:
        return False
    broker_order_no = value.get("broker_order_no")
    code = value.get("code")
    side = value.get("side")
    price = value.get("price")
    raw = value.get("raw")
    return bool(
        (broker_order_no is None or _nonempty_string(broker_order_no))
        and _optional_stock_code_valid(code)
        and (broker_order_no is not None or code is not None)
        and isinstance(side, str)
        and side == side.upper()
        and _is_exact_int(value.get("quantity"))
        and _is_exact_int(value.get("remaining_quantity"))
        and (price is None or isinstance(price, float))
        and isinstance(raw, Mapping)
        and len(raw) > 0
    )


def _normalized_broker_position_valid(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "code",
        "name",
        "quantity",
        "available_quantity",
        "avg_entry_price",
        "raw",
    }:
        return False
    available_quantity = value.get("available_quantity")
    avg_entry_price = value.get("avg_entry_price")
    raw = value.get("raw")
    return bool(
        isinstance(value.get("code"), str)
        and _optional_stock_code_valid(value.get("code"))
        and isinstance(value.get("name"), str)
        and _is_exact_int(value.get("quantity"))
        and (
            available_quantity is None or _is_exact_int(available_quantity)
        )
        and (avg_entry_price is None or isinstance(avg_entry_price, float))
        and isinstance(raw, Mapping)
        and len(raw) > 0
    )


def _is_exact_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _reconcile_event_shape_valid(event: Mapping[str, Any]) -> bool:
    return bool(
        _lifecycle_row_scalars_valid(event)
        and event.get("event_type") == "RECONCILE_MISMATCH"
        and event.get("entity_type") == "RECONCILE"
        and _nonempty_string(event.get("entity_id"))
        and event.get("status") == "RECONCILE_MISMATCH"
        and event.get("reason") == "RECONCILE_MISMATCH"
        and event.get("live_sim_order_id") is None
        and event.get("position_id") is None
    )


def _reconcile_event_matches_snapshot(
    evidence: Mapping[str, Any] | None,
    snapshot_evaluation: Mapping[str, Any],
) -> bool:
    snapshot = snapshot_evaluation.get("snapshot")
    if not isinstance(evidence, Mapping) or not isinstance(snapshot, Mapping):
        return False
    if set(evidence) != {"mismatches", "blocking_new_buy"}:
        return False
    mismatches = evidence.get("mismatches")
    blocking_new_buy = evidence.get("blocking_new_buy")
    if not isinstance(mismatches, list) or not isinstance(blocking_new_buy, bool):
        return False
    return bool(
        _sha(mismatches) == _sha(snapshot.get("mismatches"))
        and blocking_new_buy is snapshot_evaluation.get("blocking_new_buy")
        and blocking_new_buy is snapshot.get("blocking_new_buy")
    )


def _strict_nonnegative_int(value: Any) -> int | None:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _strict_bool(value: Any) -> bool | None:
    if not _integer_equals(value, 0) and not _integer_equals(value, 1):
        return None
    return _integer_equals(value, 1)


def _parsed_timestamp(value: Any) -> Any:
    try:
        return parse_timestamp(value, "created_at")
    except (TypeError, ValueError):
        return None


def _reconcile_subject(
    fingerprint: str,
    *,
    classification: str,
    reasons: Sequence[str],
    created_at: Any,
    payload_sha256: str | None,
) -> dict[str, Any]:
    return {
        "subject_id": fingerprint,
        "subject_fingerprint": fingerprint,
        "classification": classification,
        "reason_codes": list(reasons),
        "mirror_status": "RECONCILE_EVIDENCE",
        "error_surface_count": 0,
        "lifecycle_surface_count": 0,
        "created_at": _public_timestamp(created_at),
        "code": None,
        "inner_event_type": None,
        "payload_sha256": payload_sha256,
        "event_metadata_consistent": False,
        "identifier_free": False,
    }


def _public_timestamp(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _read_error_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return _read_all(
        connection,
        """
        SELECT id, run_id, live_sim_intent_id, live_sim_order_id, code,
               error_message, payload_json, created_at
        FROM live_sim_errors
        ORDER BY id
        LIMIT ? OFFSET ?
        """,
    )


def _read_lifecycle_error_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return _read_all(
        connection,
        """
        SELECT lifecycle_event_id, event_type, entity_type, entity_id,
               live_sim_order_id, position_id, status, reason, evidence_json,
               created_at, live_sim_only, live_real_allowed
        FROM live_sim_lifecycle_events
        WHERE (
            entity_type = 'LIVE_SIM_ERROR'
            OR UPPER(TRIM(COALESCE(event_type, ''))) = 'LIFECYCLE_ERROR'
            OR UPPER(TRIM(COALESCE(status, ''))) = 'ERROR'
        )
        AND NOT (
            entity_type = 'RECONCILE'
            OR UPPER(TRIM(COALESCE(event_type, ''))) = 'RECONCILE_MISMATCH'
            OR UPPER(TRIM(COALESCE(reason, ''))) = 'RECONCILE_MISMATCH'
        )
        ORDER BY created_at, lifecycle_event_id
        LIMIT ? OFFSET ?
        """,
    )


def _read_reconcile_event_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return _read_all(
        connection,
        """
        SELECT lifecycle_event_id, event_type, entity_type, entity_id,
               live_sim_order_id, position_id, status, reason, evidence_json,
               created_at, live_sim_only, live_real_allowed
        FROM live_sim_lifecycle_events
        WHERE entity_type = 'RECONCILE'
           OR UPPER(TRIM(COALESCE(event_type, ''))) = 'RECONCILE_MISMATCH'
           OR UPPER(TRIM(COALESCE(reason, ''))) = 'RECONCILE_MISMATCH'
           OR UPPER(TRIM(COALESCE(status, ''))) = 'RECONCILE_MISMATCH'
        ORDER BY created_at, lifecycle_event_id
        LIMIT ? OFFSET ?
        """,
    )


def _read_reconcile_snapshot_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return _read_all(
        connection,
        """
        SELECT reconcile_id, status, mismatch_count, blocking_new_buy,
               allow_exit, snapshot_json, created_at, live_sim_only
        FROM live_sim_reconcile_snapshots
        ORDER BY created_at DESC, reconcile_id DESC
        LIMIT ? OFFSET ?
        """,
    )


def _read_all(connection: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        cursor = connection.execute(sql, (_PAGE_LIMIT, offset))
        page = _cursor_dicts(cursor)
        rows.extend(page)
        if len(page) < _PAGE_LIMIT:
            return rows
        offset += len(page)


def _cursor_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [str(description[0]) for description in cursor.description or ()]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _inventory_digest(
    errors: Sequence[Mapping[str, Any]],
    lifecycle: Sequence[Mapping[str, Any]],
    reconcile_events: Sequence[Mapping[str, Any]],
    reconcile_snapshots: Sequence[Mapping[str, Any]],
) -> str:
    return _sha(
        {
            "errors": [_sha(dict(row)) for row in errors],
            "lifecycle": [_sha(dict(row)) for row in lifecycle],
            "reconcile_events": [_sha(dict(row)) for row in reconcile_events],
            "reconcile_snapshots": [_sha(dict(row)) for row in reconcile_snapshots],
        }
    )


def _parse_json_mapping(value: Any, reason: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None, reason
    if not isinstance(parsed, Mapping):
        return None, reason
    return dict(parsed), None


def _anchor(created_at: Any, payload: Mapping[str, Any] | None, surface: str, key: Any) -> str:
    if payload is None:
        return _sha({"malformed_surface": surface, "key": key})
    return _sha({"created_at": created_at, "payload": payload})


def _sha(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value)))


def _present(value: Any) -> bool:
    return value not in (None, "", (), [], {})


def _integer_equals(value: Any, expected: int) -> bool:
    try:
        return int(str(value)) == expected
    except (TypeError, ValueError):
        return False


def _rollback_read_snapshot(
    connection: sqlite3.Connection, *, nested: bool, savepoint: str
) -> None:
    if nested:
        connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        connection.execute(f"RELEASE SAVEPOINT {savepoint}")
    else:
        connection.rollback()
