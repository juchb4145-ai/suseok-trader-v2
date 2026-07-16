from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from domain.broker.utils import parse_timestamp
from storage.gateway_command_store import canonical_json

MANIFEST_CONTRACT = "fast0-pipeline-legacy-evidence-manifest.v1"
ITEM_CONTRACT = "pipeline-legacy-evidence-item.v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ALIAS_RE = re.compile(r"^L[0-9]{3}$")
_MANIFEST_KEYS = frozenset(
    {"contract", "trade_date", "target_set_sha256", "schema59_cutover", "items"}
)
_CUTOVER_KEYS = frozenset(
    {
        "completed_at",
        "migration_report_sha256",
        "source_database_sha256",
        "migrated_database_sha256",
    }
)
_ITEM_KEYS = frozenset(
    {
        "alias",
        "candidate_instance_id",
        "pipeline_fingerprint",
        "subject_version",
        "source_fingerprint",
        "closure_fingerprint",
        "pipeline_stage_fingerprint",
        "provenance_sha256",
    }
)
_LINEAGE_COLUMNS = (
    "source_run_id",
    "source_watermark_hash",
    "source_event_id",
    "source_observed_at",
)
_STAGE_SPECS = (
    ("strategy_observations_latest", "evaluated_at", "strategy_observation_id"),
    ("risk_observations_latest", "evaluated_at", "risk_observation_id"),
    ("entry_timing_evaluations", "evaluated_at", "entry_timing_evaluation_id"),
    ("order_plan_drafts_latest", "created_at", "order_plan_id"),
)


class PipelineLegacyEvidenceError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def validate_legacy_evidence_manifest(
    payload: Mapping[str, Any],
    *,
    expected_trade_date: str,
    subjects: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if set(payload) != _MANIFEST_KEYS or payload.get("contract") != MANIFEST_CONTRACT:
        raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_CONTRACT_INVALID")
    if payload.get("trade_date") != expected_trade_date:
        raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_TRADE_DATE_MISMATCH")
    cutover = _validate_cutover(_mapping(payload.get("schema59_cutover")))
    raw_items = payload.get("items")
    if not isinstance(raw_items, list) or any(not isinstance(item, Mapping) for item in raw_items):
        raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_ITEMS_INVALID")
    items = [dict(item) for item in raw_items]
    if any(set(item) != _ITEM_KEYS for item in items):
        raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_ITEM_CONTRACT_INVALID")
    ordered_subjects = sorted(
        (dict(item) for item in _legacy_subjects(subjects)),
        key=lambda item: str(item.get("candidate_instance_id") or ""),
    )
    if len(items) != len(ordered_subjects):
        raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_TARGET_COUNT_MISMATCH")
    provenance_sha256 = _sha256_json(cutover)
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ordinal, (item, subject) in enumerate(zip(items, ordered_subjects, strict=True), start=1):
        alias = str(item.get("alias") or "")
        candidate_id = str(item.get("candidate_instance_id") or "")
        if _ALIAS_RE.fullmatch(alias) is None or alias != f"L{ordinal:03d}":
            raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_ALIAS_SEQUENCE_INVALID")
        if not candidate_id or candidate_id in seen:
            raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_SUBJECT_INVALID")
        seen.add(candidate_id)
        if candidate_id != str(subject.get("candidate_instance_id") or ""):
            raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_SUBJECT_MISMATCH")
        for key in _ITEM_KEYS - {"alias", "candidate_instance_id"}:
            if not _is_sha256(item.get(key)):
                raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_DIGEST_INVALID")
        if item.get("pipeline_fingerprint") != subject.get("pipeline_fingerprint"):
            raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_PIPELINE_CAS_MISMATCH")
        if item.get("subject_version") != subject.get("subject_version"):
            raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_SUBJECT_CAS_MISMATCH")
        if item.get("provenance_sha256") != provenance_sha256:
            raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_PROVENANCE_MISMATCH")
        normalized.append(item)
    target_set_sha256 = legacy_target_set_sha256(normalized)
    if payload.get("target_set_sha256") != target_set_sha256:
        raise PipelineLegacyEvidenceError("LEGACY_MANIFEST_TARGET_SET_MISMATCH")
    return {
        "contract": MANIFEST_CONTRACT,
        "trade_date": expected_trade_date,
        "target_set_sha256": target_set_sha256,
        "schema59_cutover": cutover,
        "items": normalized,
    }


def resolve_pipeline_legacy_evidence(
    connection: sqlite3.Connection,
    trade_date: str,
    subjects: Sequence[Mapping[str, Any]],
    *,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        validated = validate_legacy_evidence_manifest(
            manifest,
            expected_trade_date=trade_date,
            subjects=subjects,
        )
        cutoff = _parse_utc_timestamp(
            str(_mapping(validated["schema59_cutover"])["completed_at"]),
            "LEGACY_SCHEMA59_CUTOVER_INVALID",
        )
        resolved: dict[str, dict[str, Any]] = {}
        legacy_subjects = _legacy_subjects(subjects)
        for subject, approved in zip(
            sorted(
                (dict(item) for item in legacy_subjects),
                key=lambda item: str(item.get("candidate_instance_id") or ""),
            ),
            validated["items"],
            strict=True,
        ):
            candidate_id = str(subject["candidate_instance_id"])
            evidence = _current_subject_evidence(
                connection,
                candidate_instance_id=candidate_id,
                trade_date=trade_date,
                cutoff=cutoff,
            )
            for key in (
                "source_fingerprint",
                "closure_fingerprint",
                "pipeline_stage_fingerprint",
            ):
                if approved.get(key) != evidence.get(key):
                    raise PipelineLegacyEvidenceError("LEGACY_EVIDENCE_DATABASE_CAS_MISMATCH")
            if evidence.get("terminal_closed") is not True:
                raise PipelineLegacyEvidenceError("LEGACY_EVIDENCE_TERMINAL_CLOSURE_NOT_PROVEN")
            if evidence.get("active_source_zero") is not True:
                raise PipelineLegacyEvidenceError("LEGACY_EVIDENCE_ACTIVE_SOURCE_NOT_ZERO")
            if evidence.get("pre_schema59") is not True:
                raise PipelineLegacyEvidenceError("LEGACY_EVIDENCE_PRE_SCHEMA59_NOT_PROVEN")
            resolved[candidate_id] = {
                "contract": ITEM_CONTRACT,
                "status": "AUTHORITATIVE",
                "authoritative": True,
                "pre_schema59": True,
                "terminal_closed": True,
                "active_source_zero": True,
                "current_source_no_drift": True,
                "pipeline_fingerprint": subject["pipeline_fingerprint"],
                "subject_version": subject["subject_version"],
                "source_fingerprint": evidence["source_fingerprint"],
                "closure_fingerprint": evidence["closure_fingerprint"],
                "pipeline_stage_fingerprint": evidence["pipeline_stage_fingerprint"],
                "provenance_sha256": approved["provenance_sha256"],
            }
    except (KeyError, TypeError, PipelineLegacyEvidenceError):
        return {
            "status": "BLOCKED",
            "reason_codes": ["PIPELINE_LEGACY_AUTHORITATIVE_EVIDENCE_INVALID"],
            "items": {},
        }
    return {"status": "READY", "reason_codes": [], "items": resolved}


def legacy_target_set_sha256(items: Sequence[Mapping[str, Any]]) -> str:
    return _sha256_json(
        {
            "contract": "fast0-pipeline-legacy-evidence-target-set.v1",
            "items": [dict(item) for item in items],
        }
    )


def _legacy_subjects(
    subjects: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    for subject in subjects:
        reasons = [str(value) for value in subject.get("canonical_reason_codes") or []]
        latest_plan = _mapping(subject.get("latest_plan"))
        if (
            subject.get("classification") == "HISTORICAL_CLOSED"
            and subject.get("canonical_status") != "PASS"
            and any("LINEAGE_MISSING" in value for value in reasons)
            and str(latest_plan.get("status") or "").upper() != "PLAN_READY"
        ):
            result.append(subject)
    return result


def _current_subject_evidence(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str,
    trade_date: str,
    cutoff: datetime,
) -> dict[str, Any]:
    candidate_row = connection.execute(
        "SELECT * FROM candidates WHERE candidate_instance_id = ?",
        (candidate_instance_id,),
    ).fetchone()
    candidate = _row_dict(candidate_row)
    latest_sources = [
        _row_dict(row)
        for row in connection.execute(
            """
            SELECT * FROM candidate_sources_latest
            WHERE candidate_instance_id = ?
            ORDER BY trade_date, code, source_type, source_id
            """,
            (candidate_instance_id,),
        ).fetchall()
    ]
    source_events = [
        _row_dict(row)
        for row in connection.execute(
            """
            SELECT * FROM candidate_source_events
            WHERE candidate_instance_id = ?
            ORDER BY event_ts, source_event_id
            """,
            (candidate_instance_id,),
        ).fetchall()
    ]
    stages: dict[str, dict[str, Any]] = {}
    timestamps: list[datetime] = []
    lineage_missing = True
    for table, timestamp_column, tie_column in _STAGE_SPECS:
        row = connection.execute(
            f"""
            SELECT * FROM {table}
            WHERE candidate_instance_id = ? AND trade_date = ?
            ORDER BY {timestamp_column} DESC, {tie_column} DESC
            LIMIT 1
            """,
            (candidate_instance_id, trade_date),
        ).fetchone()
        value = _row_dict(row)
        stages[table] = value
        if not value:
            continue
        timestamps.append(
            _parse_utc_timestamp(
                str(value.get(timestamp_column) or ""),
                "LEGACY_STAGE_TIME_INVALID",
            )
        )
        lineage_missing = lineage_missing and all(not value.get(key) for key in _LINEAGE_COLUMNS)
    terminal_closed = bool(
        candidate
        and str(candidate.get("state") or "").upper() == "CLOSED"
        and bool(candidate.get("closed_at"))
    )
    source_key_fields = ("trade_date", "code", "source_type", "source_id")
    latest_event_by_source: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in source_events:
        key = tuple(str(row.get(field) or "") for field in source_key_fields)
        latest_event_by_source[key] = row
    latest_projection_by_source = {
        tuple(str(row.get(field) or "") for field in source_key_fields): row
        for row in latest_sources
    }
    source_projection_consistent = bool(
        set(latest_event_by_source) == set(latest_projection_by_source)
        and all(
            int(latest_event_by_source[key].get("active") or 0)
            == int(latest_projection_by_source[key].get("active") or 0)
            for key in latest_event_by_source
        )
        and (
            bool(latest_event_by_source)
            or int(candidate.get("source_count") or 0) == 0
        )
    )
    latest_active_count = sum(
        1 for row in latest_projection_by_source.values() if int(row.get("active") or 0) != 0
    )
    pre_schema59 = bool(timestamps and lineage_missing and max(timestamps) <= cutoff)
    return {
        "source_fingerprint": _sha256_json(
            {
                "contract": "pipeline-legacy-source-evidence.v1",
                "latest_rows": latest_sources,
                "event_rows": source_events,
            }
        ),
        "closure_fingerprint": _sha256_json(
            {"contract": "pipeline-legacy-closure-evidence.v1", "candidate": candidate}
        ),
        "pipeline_stage_fingerprint": _sha256_json(
            {"contract": "pipeline-legacy-stage-evidence.v1", "stages": stages}
        ),
        "terminal_closed": terminal_closed,
        "active_source_zero": source_projection_consistent and latest_active_count == 0,
        "pre_schema59": pre_schema59,
    }


def _validate_cutover(value: Mapping[str, Any]) -> dict[str, str]:
    if set(value) != _CUTOVER_KEYS:
        raise PipelineLegacyEvidenceError("LEGACY_SCHEMA59_CUTOVER_CONTRACT_INVALID")
    completed_at = str(value.get("completed_at") or "")
    _parse_utc_timestamp(completed_at, "LEGACY_SCHEMA59_CUTOVER_INVALID")
    result = {"completed_at": completed_at}
    for key in _CUTOVER_KEYS - {"completed_at"}:
        if not _is_sha256(value.get(key)):
            raise PipelineLegacyEvidenceError("LEGACY_SCHEMA59_CUTOVER_DIGEST_INVALID")
        result[key] = str(value[key])
    return result


def _parse_utc_timestamp(value: str, code: str) -> datetime:
    try:
        parsed = parse_timestamp(value, "timestamp")
    except (TypeError, ValueError) as exc:
        raise PipelineLegacyEvidenceError(code) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return {} if row is None else {str(key): row[key] for key in row.keys()}


def _sha256_json(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
