from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from services.pipeline_orphan_campaign import (
    build_orphan_campaign_link,
    validate_orphan_campaign_link,
)

ORPHAN_ACTION = "DISPOSE_ORPHAN_PIPELINE_OBSERVATION"
ORPHAN_CLASSIFICATION = "MISSING_CANDIDATE_MANUAL_REVIEW"
ORPHAN_EVIDENCE_CONTRACT = "fast0-pipeline-orphan-manual-evidence.v1"
ORPHAN_EVIDENCE_BINDING_CONTRACT = "fast0-pipeline-orphan-evidence-binding.v1"
ORPHAN_APPLY_EVIDENCE_BINDING_CONTRACT = "fast0-pipeline-orphan-apply-binding.v2"
ORPHAN_APPLY_APPROVAL_CONTRACT = "fast0-pipeline-orphan-apply-approval.v2"
PRIVATE_TARGET_SET_CONTRACT = "fast0-private-target-set.v1"

ORPHAN_EVIDENCE_SOURCE_TYPES = frozenset(
    {
        "BROKER_ACCOUNT_ACTIVITY_EXPORT",
        "BROKER_EXECUTION_HISTORY_EXPORT",
        "BROKER_ORDER_HISTORY_CAPTURE",
        "BROKER_ORDER_HISTORY_EXPORT",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ALIAS_RE = re.compile(r"^M(?:00[1-9]|0[1-9][0-9]|[1-9][0-9]{2})$")
_UTC_Z_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?Z$"
)
_SAFE_REF_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@/-]{2,199}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:@-]{2,199}$")
_HYPHENATED_ACCOUNT_RE = re.compile(r"(?<!\d)\d{3,6}-\d{2,6}(?:-\d{1,6})?(?!\d)")
_WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"^[A-Za-z]:[/\\]")
_KST = timezone(timedelta(hours=9), name="Asia/Seoul")
_SENSITIVE_PATTERNS = (
    re.compile(r"(?i)(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}"),
    re.compile(r"(?i)github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}=*"),
    re.compile(r"(?i)(?:token|password|secret|api[_-]?key|account)\s*[:=]\s*\S+"),
    re.compile(r"(?i)(?:acct|account|계좌)[_.:@/-]?\d{6,16}"),
    re.compile(r"(?<![A-Za-z0-9])\d{8,16}(?![A-Za-z0-9])"),
)

_TARGET_KEYS = frozenset(
    {
        "trade_date",
        "candidate_instance_id",
        "classification",
        "action",
        "pipeline_fingerprint",
        "subject_version",
        "source_fingerprint",
        "candidate_fingerprint",
        "downstream_fingerprint",
        "boundary_fingerprint",
    }
)
_CAS_KEYS = (
    "pipeline_fingerprint",
    "subject_version",
    "source_fingerprint",
    "candidate_fingerprint",
    "downstream_fingerprint",
    "boundary_fingerprint",
)
_DETERMINATION_KEYS = frozenset(
    {
        "status",
        "terminal_orphan_confirmed",
        "candidate_present",
        "current_source_present",
        "order_or_broker_activity_present",
    }
)
_PROVENANCE_KEYS = frozenset(
    {
        "source_type",
        "source_ref",
        "artifact_sha256",
        "artifact_size",
        "coverage_trade_date",
        "coverage_status",
        "account_scope_sha256",
        "observed_at",
        "reviewed_at",
        "reviewer_id",
    }
)
_BINDING_KEYS = frozenset(
    {
        "contract",
        "source_contract",
        "file_sha256",
        "file_size",
        "target_set_sha256",
        "alias",
        "candidate_instance_id_sha256",
        "classification",
        "action",
        "target_sha256",
        "determination_sha256",
        "provenance_sha256",
        "determination_status",
        "terminal_orphan_confirmed",
        "candidate_present",
        "current_source_present",
        "order_or_broker_activity_present",
        "provenance_source_type",
        "artifact_sha256",
        "artifact_size",
        "coverage_trade_date",
        "coverage_status",
        "account_scope_sha256",
        "observed_at",
        "reviewed_at",
        "content_embedded",
        "evidence_preview_sha256",
    }
)
_APPLY_BINDING_KEYS = frozenset(
    {
        "contract",
        "evidence_binding",
        "approval",
        "campaign",
        "approval_binding_sha256",
    }
)
_APPLY_APPROVAL_KEYS = frozenset(
    {
        "contract",
        "approved_preflight_report_sha256",
        "approved_private_manifest_sha256",
        "source_plan_report_sha256",
        "source_reconciliation_report_sha256",
        "approved_database_main_sha256",
        "approved_database_main_size",
        "broker_scope_sha256",
        "artifact_sha256",
        "target_set_sha256",
        "alias",
        "preflight_generated_at",
    }
)


class OrphanManualEvidenceError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(self.code)


@dataclass(frozen=True)
class StableJsonDocument:
    payload: Mapping[str, Any]
    sha256: str
    size: int
    device: int
    inode: int
    mtime_ns: int


@dataclass(frozen=True)
class StableFileFingerprint:
    sha256: str
    size: int
    device: int
    inode: int
    mtime_ns: int


def read_stable_file_fingerprint(
    path: Path,
    *,
    expected_sha256: str | None = None,
    max_bytes: int = 100 * 1024 * 1024,
) -> StableFileFingerprint:
    expected = (
        None if expected_sha256 is None else require_sha256("EXPECTED_FILE_SHA256", expected_sha256)
    )
    candidate = Path(path)
    try:
        if candidate.is_symlink():
            raise OrphanManualEvidenceError("ARTIFACT_SYMLINK_FORBIDDEN")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        descriptor = os.open(candidate, flags)
    except OrphanManualEvidenceError:
        raise
    except OSError as exc:
        raise OrphanManualEvidenceError("ARTIFACT_NOT_FOUND") from exc
    digest = hashlib.sha256()
    size = 0
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise OrphanManualEvidenceError("ARTIFACT_REGULAR_FILE_REQUIRED")
        if int(opened.st_nlink) != 1:
            raise OrphanManualEvidenceError("ARTIFACT_HARDLINK_FORBIDDEN")
        if opened.st_size <= 0:
            raise OrphanManualEvidenceError("ARTIFACT_EMPTY")
        if opened.st_size > max_bytes:
            raise OrphanManualEvidenceError("ARTIFACT_TOO_LARGE")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
    finally:
        os.close(descriptor)
    try:
        current = os.stat(candidate, follow_symlinks=False)
    except OSError as exc:
        raise OrphanManualEvidenceError("ARTIFACT_CHANGED_DURING_READ") from exc
    opened_identity = (
        int(opened.st_dev),
        int(opened.st_ino),
        int(opened.st_size),
        int(opened.st_mtime_ns),
        int(opened.st_nlink),
    )
    current_identity = (
        int(current.st_dev),
        int(current.st_ino),
        int(current.st_size),
        int(current.st_mtime_ns),
        int(current.st_nlink),
    )
    if opened_identity != current_identity or size != int(opened.st_size):
        raise OrphanManualEvidenceError("ARTIFACT_CHANGED_DURING_READ")
    actual_sha256 = digest.hexdigest()
    if expected is not None and actual_sha256 != expected:
        raise OrphanManualEvidenceError("ARTIFACT_SHA256_MISMATCH")
    return StableFileFingerprint(
        sha256=actual_sha256,
        size=size,
        device=int(opened.st_dev),
        inode=int(opened.st_ino),
        mtime_ns=int(opened.st_mtime_ns),
    )


def read_stable_json_document(
    path: Path,
    *,
    expected_sha256: str | None = None,
    max_bytes: int = 1_048_576,
) -> StableJsonDocument:
    expected = (
        None
        if expected_sha256 is None
        else require_sha256("EXPECTED_DOCUMENT_SHA256", expected_sha256)
    )
    candidate = Path(path)
    try:
        if candidate.is_symlink():
            raise OrphanManualEvidenceError("DOCUMENT_SYMLINK_FORBIDDEN")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        descriptor = os.open(candidate, flags)
    except OrphanManualEvidenceError:
        raise
    except OSError as exc:
        raise OrphanManualEvidenceError("DOCUMENT_NOT_FOUND") from exc

    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise OrphanManualEvidenceError("DOCUMENT_REGULAR_FILE_REQUIRED")
        if int(opened.st_nlink) != 1:
            raise OrphanManualEvidenceError("DOCUMENT_HARDLINK_FORBIDDEN")
        if opened.st_size <= 0:
            raise OrphanManualEvidenceError("DOCUMENT_EMPTY")
        if opened.st_size > max_bytes:
            raise OrphanManualEvidenceError("DOCUMENT_TOO_LARGE")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(max_bytes + 1)
    finally:
        os.close(descriptor)

    try:
        current = os.stat(candidate, follow_symlinks=False)
    except OSError as exc:
        raise OrphanManualEvidenceError("DOCUMENT_CHANGED_DURING_READ") from exc
    opened_identity = (
        int(opened.st_dev),
        int(opened.st_ino),
        int(opened.st_size),
        int(opened.st_mtime_ns),
        int(opened.st_nlink),
    )
    current_identity = (
        int(current.st_dev),
        int(current.st_ino),
        int(current.st_size),
        int(current.st_mtime_ns),
        int(current.st_nlink),
    )
    if opened_identity != current_identity or len(raw) != int(opened.st_size):
        raise OrphanManualEvidenceError("DOCUMENT_CHANGED_DURING_READ")

    document_sha256 = hashlib.sha256(raw).hexdigest()
    if expected is not None and document_sha256 != expected:
        raise OrphanManualEvidenceError("DOCUMENT_SHA256_MISMATCH")

    def reject_duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number: {value}")

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_constant,
        )
    except (UnicodeError, ValueError) as exc:
        raise OrphanManualEvidenceError("DOCUMENT_JSON_INVALID") from exc
    if not isinstance(payload, Mapping):
        raise OrphanManualEvidenceError("DOCUMENT_ROOT_OBJECT_REQUIRED")
    return StableJsonDocument(
        payload=payload,
        sha256=document_sha256,
        size=len(raw),
        device=int(opened.st_dev),
        inode=int(opened.st_ino),
        mtime_ns=int(opened.st_mtime_ns),
    )


def validate_private_target_manifest(
    document: StableJsonDocument,
    *,
    expected_target_set_sha256: str,
    expected_count: int,
) -> dict[str, Any]:
    expected_digest = require_sha256("EXPECTED_TARGET_SET_SHA256", expected_target_set_sha256)
    if type(expected_count) is not int or expected_count < 1 or expected_count > 999:
        raise OrphanManualEvidenceError("TARGET_MANIFEST_EXPECTED_COUNT_INVALID")
    _require_exact_keys(document.payload, {"contract", "items"}, "TARGET_MANIFEST")
    if document.payload.get("contract") != PRIVATE_TARGET_SET_CONTRACT:
        raise OrphanManualEvidenceError("TARGET_MANIFEST_CONTRACT_INVALID")
    items = document.payload.get("items")
    if not isinstance(items, list) or len(items) != expected_count:
        raise OrphanManualEvidenceError("TARGET_MANIFEST_COUNT_MISMATCH")

    normalized: list[dict[str, Any]] = []
    candidate_ids: set[str] = set()
    for index, raw_item in enumerate(items, start=1):
        if not isinstance(raw_item, Mapping):
            raise OrphanManualEvidenceError("TARGET_MANIFEST_ITEM_INVALID")
        _require_exact_keys(raw_item, _TARGET_KEYS | {"alias"}, "TARGET_MANIFEST_ITEM")
        alias = _require_alias(raw_item.get("alias"))
        if alias != f"M{index:03d}":
            raise OrphanManualEvidenceError("TARGET_MANIFEST_ALIAS_SEQUENCE_INVALID")
        target = normalize_orphan_target({key: raw_item.get(key) for key in _TARGET_KEYS})
        candidate_id = target["candidate_instance_id"]
        if candidate_id in candidate_ids:
            raise OrphanManualEvidenceError("TARGET_MANIFEST_DUPLICATE_CANDIDATE")
        candidate_ids.add(candidate_id)
        normalized.append({**target, "alias": alias})
    if [item["candidate_instance_id"] for item in normalized] != sorted(candidate_ids):
        raise OrphanManualEvidenceError("TARGET_MANIFEST_ORDER_INVALID")

    semantic_sha256 = private_target_set_sha256(normalized)
    if semantic_sha256 != expected_digest:
        raise OrphanManualEvidenceError("TARGET_MANIFEST_DIGEST_MISMATCH")
    return {
        "contract": PRIVATE_TARGET_SET_CONTRACT,
        "document_sha256": document.sha256,
        "document_size": document.size,
        "target_set_sha256": semantic_sha256,
        "count": len(normalized),
        "items": normalized,
    }


def validate_orphan_manual_evidence_document(
    document: StableJsonDocument,
    *,
    expected_target_set_sha256: str,
    expected_alias: str,
    expected_target: Mapping[str, Any],
    expected_artifact_sha256: str,
    expected_artifact_size: int,
    expected_account_scope_sha256: str,
    not_after: datetime,
) -> dict[str, Any]:
    target_set_sha256 = require_sha256("EXPECTED_TARGET_SET_SHA256", expected_target_set_sha256)
    alias = _require_alias(expected_alias)
    target = normalize_orphan_target(expected_target)
    payload = document.payload
    _require_exact_keys(
        payload,
        {"contract", "target_set_sha256", "alias", "target", "determination", "provenance"},
        "ORPHAN_EVIDENCE",
    )
    if payload.get("contract") != ORPHAN_EVIDENCE_CONTRACT:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_CONTRACT_INVALID")
    if payload.get("target_set_sha256") != target_set_sha256:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_TARGET_SET_MISMATCH")
    if payload.get("alias") != alias:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_ALIAS_MISMATCH")

    raw_target = payload.get("target")
    if not isinstance(raw_target, Mapping):
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_TARGET_INVALID")
    normalized_target = normalize_orphan_target(raw_target)
    if normalized_target != target:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_TARGET_MISMATCH")

    determination = payload.get("determination")
    if not isinstance(determination, Mapping):
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_DETERMINATION_INVALID")
    _require_exact_keys(determination, _DETERMINATION_KEYS, "ORPHAN_DETERMINATION")
    expected_determination = {
        "status": "AUTHORITATIVE",
        "terminal_orphan_confirmed": True,
        "candidate_present": False,
        "current_source_present": False,
        "order_or_broker_activity_present": False,
    }
    if any(
        type(determination.get(key)) is not type(value)
        for key, value in expected_determination.items()
    ):
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_DETERMINATION_TYPE_INVALID")
    if dict(determination) != expected_determination:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_DETERMINATION_NOT_AUTHORITATIVE")

    provenance = payload.get("provenance")
    if not isinstance(provenance, Mapping):
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_PROVENANCE_INVALID")
    _require_exact_keys(provenance, _PROVENANCE_KEYS, "ORPHAN_PROVENANCE")
    source_type = provenance.get("source_type")
    if source_type not in ORPHAN_EVIDENCE_SOURCE_TYPES:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_SOURCE_TYPE_INVALID")
    source_ref = _require_safe_label("SOURCE_REF", provenance.get("source_ref"), allow_slash=True)
    reviewer_id = _require_safe_label(
        "REVIEWER_ID", provenance.get("reviewer_id"), allow_slash=False
    )
    artifact_sha256 = require_sha256("ARTIFACT_SHA256", provenance.get("artifact_sha256"))
    actual_expected_artifact_sha256 = require_sha256(
        "EXPECTED_ARTIFACT_SHA256", expected_artifact_sha256
    )
    if artifact_sha256 != actual_expected_artifact_sha256:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_ARTIFACT_SHA256_MISMATCH")
    artifact_size = provenance.get("artifact_size")
    if type(artifact_size) is not int or artifact_size <= 0:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_ARTIFACT_SIZE_INVALID")
    if type(expected_artifact_size) is not int or expected_artifact_size <= 0:
        raise OrphanManualEvidenceError("EXPECTED_ARTIFACT_SIZE_INVALID")
    if artifact_size != expected_artifact_size:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_ARTIFACT_SIZE_MISMATCH")
    coverage_trade_date = _require_trade_date(provenance.get("coverage_trade_date"))
    if coverage_trade_date != target["trade_date"]:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_COVERAGE_TRADE_DATE_MISMATCH")
    if provenance.get("coverage_status") != "FINAL_COMPLETE":
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_COVERAGE_STATUS_INVALID")
    account_scope_sha256 = require_sha256(
        "ACCOUNT_SCOPE_SHA256", provenance.get("account_scope_sha256")
    )
    approved_account_scope_sha256 = require_sha256(
        "EXPECTED_ACCOUNT_SCOPE_SHA256", expected_account_scope_sha256
    )
    if account_scope_sha256 != approved_account_scope_sha256:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_ACCOUNT_SCOPE_SHA256_MISMATCH")
    observed_at = require_utc_z_timestamp("OBSERVED_AT", provenance.get("observed_at"))
    reviewed_at = require_utc_z_timestamp("REVIEWED_AT", provenance.get("reviewed_at"))
    if reviewed_at < observed_at:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_REVIEW_TIME_INVALID")
    if not_after.tzinfo is None or not_after.utcoffset() is None:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_NOT_AFTER_INVALID")
    normalized_not_after = not_after.astimezone(UTC)
    if observed_at > normalized_not_after or reviewed_at > normalized_not_after:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_FUTURE_TIMESTAMP")
    target_date = date.fromisoformat(target["trade_date"])
    if observed_at.astimezone(_KST).date() <= target_date:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_NOT_FINAL_FOR_TRADE_DATE")
    normalized_provenance = {
        "source_type": source_type,
        "source_ref": source_ref,
        "artifact_sha256": artifact_sha256,
        "artifact_size": artifact_size,
        "coverage_trade_date": coverage_trade_date,
        "coverage_status": "FINAL_COMPLETE",
        "account_scope_sha256": account_scope_sha256,
        "observed_at": str(provenance.get("observed_at")),
        "reviewed_at": str(provenance.get("reviewed_at")),
        "reviewer_id": reviewer_id,
    }
    return _build_binding(
        file_sha256=document.sha256,
        file_size=document.size,
        target_set_sha256=target_set_sha256,
        alias=alias,
        target=target,
        determination=expected_determination,
        provenance=normalized_provenance,
    )


def validate_orphan_evidence_binding(
    binding: Mapping[str, Any] | None,
    *,
    evidence_sha256: str,
    candidate_instance_id: str,
    trade_date: str,
    action: str,
    pipeline_fingerprint: str,
    subject_version: str,
    source_fingerprint: str,
    candidate_fingerprint: str,
    downstream_fingerprint: str,
    boundary_fingerprint: str,
    expected_preview_sha256: str | None = None,
) -> dict[str, Any]:
    if not isinstance(binding, Mapping):
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_REQUIRED")
    _require_exact_keys(binding, _BINDING_KEYS, "ORPHAN_BINDING")
    if binding.get("contract") != ORPHAN_EVIDENCE_BINDING_CONTRACT:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_CONTRACT_INVALID")
    if binding.get("source_contract") != ORPHAN_EVIDENCE_CONTRACT:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_SOURCE_CONTRACT_INVALID")
    normalized_evidence_sha = require_sha256("EVIDENCE_SHA256", evidence_sha256)
    if binding.get("file_sha256") != normalized_evidence_sha:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_FILE_SHA256_MISMATCH")
    if type(binding.get("file_size")) is not int or int(binding["file_size"]) <= 0:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_FILE_SIZE_INVALID")
    target_set_sha256 = require_sha256("TARGET_SET_SHA256", binding.get("target_set_sha256"))
    alias = _require_alias(binding.get("alias"))
    target = normalize_orphan_target(
        {
            "trade_date": trade_date,
            "candidate_instance_id": candidate_instance_id,
            "classification": ORPHAN_CLASSIFICATION,
            "action": action,
            "pipeline_fingerprint": pipeline_fingerprint,
            "subject_version": subject_version,
            "source_fingerprint": source_fingerprint,
            "candidate_fingerprint": candidate_fingerprint,
            "downstream_fingerprint": downstream_fingerprint,
            "boundary_fingerprint": boundary_fingerprint,
        }
    )
    expected_values = {
        "candidate_instance_id_sha256": hashlib.sha256(
            target["candidate_instance_id"].encode("utf-8")
        ).hexdigest(),
        "classification": ORPHAN_CLASSIFICATION,
        "action": ORPHAN_ACTION,
        "target_sha256": orphan_target_sha256(target),
        "determination_status": "AUTHORITATIVE",
        "terminal_orphan_confirmed": True,
        "candidate_present": False,
        "current_source_present": False,
        "order_or_broker_activity_present": False,
        "content_embedded": False,
    }
    for key, expected in expected_values.items():
        if type(binding.get(key)) is not type(expected) or binding.get(key) != expected:
            raise OrphanManualEvidenceError(f"ORPHAN_EVIDENCE_BINDING_MISMATCH:{key.upper()}")
    for key in ("determination_sha256", "provenance_sha256"):
        require_sha256(key.upper(), binding.get(key))
    if binding.get("provenance_source_type") not in ORPHAN_EVIDENCE_SOURCE_TYPES:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_SOURCE_TYPE_INVALID")
    require_sha256("ARTIFACT_SHA256", binding.get("artifact_sha256"))
    if type(binding.get("artifact_size")) is not int or int(binding["artifact_size"]) <= 0:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_ARTIFACT_SIZE_INVALID")
    if binding.get("coverage_trade_date") != target["trade_date"]:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_COVERAGE_DATE_INVALID")
    if binding.get("coverage_status") != "FINAL_COMPLETE":
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_COVERAGE_STATUS_INVALID")
    require_sha256("ACCOUNT_SCOPE_SHA256", binding.get("account_scope_sha256"))
    observed_at = require_utc_z_timestamp("OBSERVED_AT", binding.get("observed_at"))
    reviewed_at = require_utc_z_timestamp("REVIEWED_AT", binding.get("reviewed_at"))
    if reviewed_at < observed_at:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_REVIEW_TIME_INVALID")
    if observed_at.astimezone(_KST).date() <= date.fromisoformat(target["trade_date"]):
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_BINDING_NOT_FINAL")

    normalized = dict(binding)
    normalized["target_set_sha256"] = target_set_sha256
    normalized["alias"] = alias
    actual_preview_sha256 = require_sha256(
        "EVIDENCE_PREVIEW_SHA256", normalized.pop("evidence_preview_sha256", None)
    )
    calculated_preview_sha256 = canonical_sha256(normalized)
    if actual_preview_sha256 != calculated_preview_sha256:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_PREVIEW_DIGEST_INVALID")
    if expected_preview_sha256 is not None:
        expected = require_sha256("EXPECTED_EVIDENCE_PREVIEW_SHA256", expected_preview_sha256)
        if actual_preview_sha256 != expected:
            raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_PREVIEW_SHA256_MISMATCH")
    normalized["evidence_preview_sha256"] = actual_preview_sha256
    return normalized


def build_orphan_apply_evidence_binding(
    evidence_binding: Mapping[str, Any],
    *,
    approved_preflight_report_sha256: str,
    approved_private_manifest_sha256: str,
    source_plan_report_sha256: str,
    source_reconciliation_report_sha256: str,
    approved_database_main_sha256: str,
    approved_database_main_size: int,
    broker_scope_sha256: str,
    preflight_generated_at: str,
    root_handoff_report_sha256: str,
    root_handoff_chain_sha256: str,
    predecessor_kind: str,
    predecessor_report_sha256: str,
    predecessor_chain_sha256: str,
    predecessor_database_main_sha256: str,
    predecessor_database_main_size: int,
) -> dict[str, Any]:
    binding = dict(evidence_binding)
    approval = {
        "contract": ORPHAN_APPLY_APPROVAL_CONTRACT,
        "approved_preflight_report_sha256": require_sha256(
            "APPROVED_PREFLIGHT_REPORT_SHA256", approved_preflight_report_sha256
        ),
        "approved_private_manifest_sha256": require_sha256(
            "APPROVED_PRIVATE_MANIFEST_SHA256", approved_private_manifest_sha256
        ),
        "source_plan_report_sha256": require_sha256(
            "SOURCE_PLAN_REPORT_SHA256", source_plan_report_sha256
        ),
        "source_reconciliation_report_sha256": require_sha256(
            "SOURCE_RECONCILIATION_REPORT_SHA256", source_reconciliation_report_sha256
        ),
        "approved_database_main_sha256": require_sha256(
            "APPROVED_DATABASE_MAIN_SHA256", approved_database_main_sha256
        ),
        "approved_database_main_size": approved_database_main_size,
        "broker_scope_sha256": require_sha256("BROKER_SCOPE_SHA256", broker_scope_sha256),
        "artifact_sha256": require_sha256("ARTIFACT_SHA256", binding.get("artifact_sha256")),
        "target_set_sha256": require_sha256("TARGET_SET_SHA256", binding.get("target_set_sha256")),
        "alias": _require_alias(binding.get("alias")),
        "preflight_generated_at": preflight_generated_at,
    }
    if type(approved_database_main_size) is not int or approved_database_main_size <= 0:
        raise OrphanManualEvidenceError("APPROVED_DATABASE_MAIN_SIZE_INVALID")
    require_utc_z_timestamp("PREFLIGHT_GENERATED_AT", preflight_generated_at)
    wrapper: dict[str, Any] = {
        "contract": ORPHAN_APPLY_EVIDENCE_BINDING_CONTRACT,
        "evidence_binding": binding,
        "approval": approval,
        "campaign": build_orphan_campaign_link(
            alias=str(binding["alias"]),
            root_handoff_report_sha256=root_handoff_report_sha256,
            root_handoff_chain_sha256=root_handoff_chain_sha256,
            predecessor_kind=predecessor_kind,
            predecessor_report_sha256=predecessor_report_sha256,
            predecessor_chain_sha256=predecessor_chain_sha256,
            predecessor_database_main_sha256=predecessor_database_main_sha256,
            predecessor_database_main_size=predecessor_database_main_size,
        ),
    }
    wrapper["approval_binding_sha256"] = canonical_sha256(wrapper)
    return wrapper


def validate_orphan_apply_evidence_binding(
    wrapper: Mapping[str, Any] | None,
    *,
    evidence_sha256: str,
    candidate_instance_id: str,
    trade_date: str,
    action: str,
    pipeline_fingerprint: str,
    subject_version: str,
    source_fingerprint: str,
    candidate_fingerprint: str,
    downstream_fingerprint: str,
    boundary_fingerprint: str,
    not_after: datetime,
) -> dict[str, Any]:
    if not isinstance(wrapper, Mapping):
        raise OrphanManualEvidenceError("ORPHAN_APPLY_BINDING_REQUIRED")
    _require_exact_keys(wrapper, _APPLY_BINDING_KEYS, "ORPHAN_APPLY_BINDING")
    if wrapper.get("contract") != ORPHAN_APPLY_EVIDENCE_BINDING_CONTRACT:
        raise OrphanManualEvidenceError("ORPHAN_APPLY_BINDING_CONTRACT_INVALID")
    raw_binding = wrapper.get("evidence_binding")
    if not isinstance(raw_binding, Mapping):
        raise OrphanManualEvidenceError("ORPHAN_APPLY_EVIDENCE_BINDING_INVALID")
    binding = validate_orphan_evidence_binding(
        raw_binding,
        evidence_sha256=evidence_sha256,
        candidate_instance_id=candidate_instance_id,
        trade_date=trade_date,
        action=action,
        pipeline_fingerprint=pipeline_fingerprint,
        subject_version=subject_version,
        source_fingerprint=source_fingerprint,
        candidate_fingerprint=candidate_fingerprint,
        downstream_fingerprint=downstream_fingerprint,
        boundary_fingerprint=boundary_fingerprint,
    )
    raw_approval = wrapper.get("approval")
    if not isinstance(raw_approval, Mapping):
        raise OrphanManualEvidenceError("ORPHAN_APPLY_APPROVAL_INVALID")
    _require_exact_keys(raw_approval, _APPLY_APPROVAL_KEYS, "ORPHAN_APPLY_APPROVAL")
    if raw_approval.get("contract") != ORPHAN_APPLY_APPROVAL_CONTRACT:
        raise OrphanManualEvidenceError("ORPHAN_APPLY_APPROVAL_CONTRACT_INVALID")
    approval = dict(raw_approval)
    try:
        campaign = validate_orphan_campaign_link(
            wrapper.get("campaign") if isinstance(wrapper.get("campaign"), Mapping) else None
        )
    except ValueError as exc:
        raise OrphanManualEvidenceError(str(exc)) from exc
    for key in (
        "approved_preflight_report_sha256",
        "approved_private_manifest_sha256",
        "source_plan_report_sha256",
        "source_reconciliation_report_sha256",
        "approved_database_main_sha256",
        "broker_scope_sha256",
        "artifact_sha256",
        "target_set_sha256",
    ):
        approval[key] = require_sha256(key.upper(), approval.get(key))
    if (
        type(approval.get("approved_database_main_size")) is not int
        or int(approval["approved_database_main_size"]) <= 0
    ):
        raise OrphanManualEvidenceError("ORPHAN_APPLY_APPROVED_DATABASE_SIZE_INVALID")
    approval["alias"] = _require_alias(approval.get("alias"))
    if approval["broker_scope_sha256"] != binding["account_scope_sha256"]:
        raise OrphanManualEvidenceError("ORPHAN_APPLY_BROKER_SCOPE_MISMATCH")
    for key in ("artifact_sha256", "target_set_sha256", "alias"):
        if approval[key] != binding[key]:
            raise OrphanManualEvidenceError(f"ORPHAN_APPLY_APPROVAL_MISMATCH:{key.upper()}")
    if campaign["alias"] != binding["alias"]:
        raise OrphanManualEvidenceError("ORPHAN_APPLY_CAMPAIGN_ALIAS_MISMATCH")
    preflight_generated_at = require_utc_z_timestamp(
        "PREFLIGHT_GENERATED_AT", approval.get("preflight_generated_at")
    )
    if not_after.tzinfo is None or not_after.utcoffset() is None:
        raise OrphanManualEvidenceError("ORPHAN_APPLY_NOT_AFTER_INVALID")
    observed_at = require_utc_z_timestamp("OBSERVED_AT", binding.get("observed_at"))
    reviewed_at = require_utc_z_timestamp("REVIEWED_AT", binding.get("reviewed_at"))
    if not (
        observed_at <= preflight_generated_at
        and reviewed_at <= preflight_generated_at
        and preflight_generated_at <= not_after.astimezone(UTC)
    ):
        raise OrphanManualEvidenceError("ORPHAN_APPLY_APPROVAL_TIME_INVALID")
    normalized_without_digest = {
        "contract": ORPHAN_APPLY_EVIDENCE_BINDING_CONTRACT,
        "evidence_binding": binding,
        "approval": approval,
        "campaign": campaign,
    }
    actual_digest = require_sha256(
        "APPROVAL_BINDING_SHA256", wrapper.get("approval_binding_sha256")
    )
    if actual_digest != canonical_sha256(normalized_without_digest):
        raise OrphanManualEvidenceError("ORPHAN_APPLY_APPROVAL_BINDING_DIGEST_INVALID")
    return {**normalized_without_digest, "approval_binding_sha256": actual_digest}


def normalize_orphan_target(value: Mapping[str, Any]) -> dict[str, str]:
    _require_exact_keys(value, _TARGET_KEYS, "ORPHAN_TARGET")
    trade_date = _require_trade_date(value.get("trade_date"))
    candidate_instance_id = _require_private_identifier(value.get("candidate_instance_id"))
    if value.get("classification") != ORPHAN_CLASSIFICATION:
        raise OrphanManualEvidenceError("ORPHAN_TARGET_CLASSIFICATION_INVALID")
    if value.get("action") != ORPHAN_ACTION:
        raise OrphanManualEvidenceError("ORPHAN_TARGET_ACTION_INVALID")
    result = {
        "trade_date": trade_date,
        "candidate_instance_id": candidate_instance_id,
        "classification": ORPHAN_CLASSIFICATION,
        "action": ORPHAN_ACTION,
    }
    for key in _CAS_KEYS:
        result[key] = require_sha256(key.upper(), value.get(key))
    return result


def private_target_set_sha256(items: Sequence[Mapping[str, Any]]) -> str:
    return canonical_sha256(
        {
            "contract": PRIVATE_TARGET_SET_CONTRACT,
            "items": [dict(item) for item in items],
        }
    )


def orphan_target_sha256(target: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {
            "contract": "fast0-pipeline-orphan-target.v1",
            "target": normalize_orphan_target(target),
        }
    )


def canonical_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def require_sha256(name: str, value: object) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise OrphanManualEvidenceError(f"INVALID_SHA256:{name}")
    return value


def require_utc_z_timestamp(name: str, value: object) -> datetime:
    if not isinstance(value, str) or _UTC_Z_RE.fullmatch(value) is None:
        raise OrphanManualEvidenceError(f"INVALID_UTC_TIMESTAMP:{name}")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise OrphanManualEvidenceError(f"INVALID_UTC_TIMESTAMP:{name}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise OrphanManualEvidenceError(f"INVALID_UTC_TIMESTAMP:{name}")
    return parsed


def _build_binding(
    *,
    file_sha256: str,
    file_size: int,
    target_set_sha256: str,
    alias: str,
    target: Mapping[str, Any],
    determination: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    normalized_target = normalize_orphan_target(target)
    binding: dict[str, Any] = {
        "contract": ORPHAN_EVIDENCE_BINDING_CONTRACT,
        "source_contract": ORPHAN_EVIDENCE_CONTRACT,
        "file_sha256": require_sha256("FILE_SHA256", file_sha256),
        "file_size": file_size,
        "target_set_sha256": require_sha256("TARGET_SET_SHA256", target_set_sha256),
        "alias": _require_alias(alias),
        "candidate_instance_id_sha256": hashlib.sha256(
            normalized_target["candidate_instance_id"].encode("utf-8")
        ).hexdigest(),
        "classification": ORPHAN_CLASSIFICATION,
        "action": ORPHAN_ACTION,
        "target_sha256": orphan_target_sha256(normalized_target),
        "determination_sha256": canonical_sha256(dict(determination)),
        "provenance_sha256": canonical_sha256(dict(provenance)),
        "determination_status": determination["status"],
        "terminal_orphan_confirmed": determination["terminal_orphan_confirmed"],
        "candidate_present": determination["candidate_present"],
        "current_source_present": determination["current_source_present"],
        "order_or_broker_activity_present": determination["order_or_broker_activity_present"],
        "provenance_source_type": provenance["source_type"],
        "artifact_sha256": provenance["artifact_sha256"],
        "artifact_size": provenance["artifact_size"],
        "coverage_trade_date": provenance["coverage_trade_date"],
        "coverage_status": provenance["coverage_status"],
        "account_scope_sha256": provenance["account_scope_sha256"],
        "observed_at": provenance["observed_at"],
        "reviewed_at": provenance["reviewed_at"],
        "content_embedded": False,
    }
    binding["evidence_preview_sha256"] = canonical_sha256(binding)
    return binding


def _require_exact_keys(
    value: Mapping[str, Any], expected: set[str] | frozenset[str], name: str
) -> None:
    if set(value) != set(expected):
        raise OrphanManualEvidenceError(f"{name}_KEYS_INVALID")


def _require_alias(value: object) -> str:
    if not isinstance(value, str) or _ALIAS_RE.fullmatch(value) is None:
        raise OrphanManualEvidenceError("ORPHAN_EVIDENCE_ALIAS_INVALID")
    return value


def _require_trade_date(value: object) -> str:
    if not isinstance(value, str):
        raise OrphanManualEvidenceError("ORPHAN_TARGET_TRADE_DATE_INVALID")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise OrphanManualEvidenceError("ORPHAN_TARGET_TRADE_DATE_INVALID") from exc
    if parsed.isoformat() != value:
        raise OrphanManualEvidenceError("ORPHAN_TARGET_TRADE_DATE_INVALID")
    return value


def _require_private_identifier(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 512
        or value != value.strip()
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise OrphanManualEvidenceError("ORPHAN_TARGET_IDENTIFIER_INVALID")
    return value


def _require_safe_label(name: str, value: object, *, allow_slash: bool) -> str:
    pattern = _SAFE_REF_RE if allow_slash else _SAFE_ID_RE
    if (
        not isinstance(value, str)
        or pattern.fullmatch(value) is None
        or ".." in value
        or "\\" in value
        or value.startswith("/")
        or _WINDOWS_ABSOLUTE_PATH_RE.match(value) is not None
        or value.startswith("\\\\")
        or _HYPHENATED_ACCOUNT_RE.search(value) is not None
        or any(candidate.search(value) is not None for candidate in _SENSITIVE_PATTERNS)
    ):
        raise OrphanManualEvidenceError(f"UNSAFE_OPAQUE_LABEL:{name}")
    return value


def parse_strict_json_mapping(value: str, *, name: str) -> dict[str, Any]:
    def reject_duplicate_keys(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    def reject_constant(raw: str) -> None:
        raise ValueError(f"non-finite JSON number: {raw}")

    try:
        payload = json.loads(
            value,
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_constant,
        )
    except (TypeError, ValueError) as exc:
        raise OrphanManualEvidenceError(f"{name}_JSON_INVALID") from exc
    if not isinstance(payload, Mapping):
        raise OrphanManualEvidenceError(f"{name}_OBJECT_REQUIRED")
    return dict(payload)
