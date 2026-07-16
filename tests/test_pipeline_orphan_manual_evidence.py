from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from services.pipeline_orphan_manual_evidence import (
    ORPHAN_ACTION,
    ORPHAN_CLASSIFICATION,
    ORPHAN_EVIDENCE_BINDING_CONTRACT,
    ORPHAN_EVIDENCE_CONTRACT,
    PRIVATE_TARGET_SET_CONTRACT,
    OrphanManualEvidenceError,
    private_target_set_sha256,
    read_stable_json_document,
    validate_orphan_evidence_binding,
    validate_orphan_manual_evidence_document,
    validate_private_target_manifest,
)

_ARTIFACT_SHA256 = "1" * 64
_ARTIFACT_SIZE = 128
_ACCOUNT_SCOPE_SHA256 = "2" * 64
_NOT_AFTER = datetime(2026, 7, 16, 2, 0, tzinfo=UTC)


def test_valid_orphan_evidence_builds_private_safe_binding(tmp_path: Path) -> None:
    target = _target()
    target_set_sha256 = "f" * 64
    evidence_path = _write_json(
        tmp_path / "evidence.json",
        _evidence(target=target, target_set_sha256=target_set_sha256),
    )
    document = read_stable_json_document(
        evidence_path,
        expected_sha256=_sha256_file(evidence_path),
    )

    binding = validate_orphan_manual_evidence_document(
        document,
        expected_target_set_sha256=target_set_sha256,
        expected_alias="M001",
        expected_target=target,
        expected_artifact_sha256=_ARTIFACT_SHA256,
        expected_artifact_size=_ARTIFACT_SIZE,
        expected_account_scope_sha256=_ACCOUNT_SCOPE_SHA256,
        not_after=_NOT_AFTER,
    )
    validated = validate_orphan_evidence_binding(
        binding,
        evidence_sha256=document.sha256,
        candidate_instance_id=target["candidate_instance_id"],
        trade_date=target["trade_date"],
        action=target["action"],
        pipeline_fingerprint=target["pipeline_fingerprint"],
        subject_version=target["subject_version"],
        source_fingerprint=target["source_fingerprint"],
        candidate_fingerprint=target["candidate_fingerprint"],
        downstream_fingerprint=target["downstream_fingerprint"],
        boundary_fingerprint=target["boundary_fingerprint"],
        expected_preview_sha256=binding["evidence_preview_sha256"],
    )

    serialized = json.dumps(validated, sort_keys=True)
    assert validated["contract"] == ORPHAN_EVIDENCE_BINDING_CONTRACT
    assert validated["determination_status"] == "AUTHORITATIVE"
    assert validated["content_embedded"] is False
    assert target["candidate_instance_id"] not in serialized
    assert "broker/order-history/private-ref" not in serialized
    assert "reviewer.safe" not in serialized
    assert str(tmp_path) not in serialized


def test_private_target_manifest_requires_exact_alias_order_and_semantic_digest(
    tmp_path: Path,
) -> None:
    target = _target()
    items = [{**target, "alias": "M001"}]
    payload = {"contract": PRIVATE_TARGET_SET_CONTRACT, "items": items}
    path = _write_json(tmp_path / "manifest.json", payload)
    document = read_stable_json_document(path, expected_sha256=_sha256_file(path))

    manifest = validate_private_target_manifest(
        document,
        expected_target_set_sha256=private_target_set_sha256(items),
        expected_count=1,
    )

    assert manifest["count"] == 1
    assert manifest["items"] == items

    invalid = copy.deepcopy(payload)
    invalid["items"][0]["alias"] = "M002"
    invalid_path = _write_json(tmp_path / "manifest-invalid.json", invalid)
    with pytest.raises(
        OrphanManualEvidenceError,
        match="TARGET_MANIFEST_ALIAS_SEQUENCE_INVALID",
    ):
        validate_private_target_manifest(
            read_stable_json_document(invalid_path),
            expected_target_set_sha256=private_target_set_sha256(items),
            expected_count=1,
        )


@pytest.mark.parametrize(
    ("raw", "code"),
    (
        ("not-json", "DOCUMENT_JSON_INVALID"),
        ("[]", "DOCUMENT_ROOT_OBJECT_REQUIRED"),
        ('{"contract":"one","contract":"two"}', "DOCUMENT_JSON_INVALID"),
        ('{"redacted":true}', "ORPHAN_EVIDENCE_KEYS_INVALID"),
        ("{}", "ORPHAN_EVIDENCE_KEYS_INVALID"),
    ),
)
def test_orphan_evidence_rejects_non_authoritative_json_shapes(
    tmp_path: Path,
    raw: str,
    code: str,
) -> None:
    path = tmp_path / f"invalid-{hashlib.sha256(raw.encode()).hexdigest()[:8]}.json"
    path.write_text(raw, encoding="utf-8")
    if code.startswith("DOCUMENT_"):
        with pytest.raises(OrphanManualEvidenceError, match=code):
            read_stable_json_document(path)
        return
    document = read_stable_json_document(path)
    with pytest.raises(OrphanManualEvidenceError, match=code):
        validate_orphan_manual_evidence_document(
            document,
            expected_target_set_sha256="f" * 64,
            expected_alias="M001",
            expected_target=_target(),
            expected_artifact_sha256=_ARTIFACT_SHA256,
            expected_artifact_size=_ARTIFACT_SIZE,
            expected_account_scope_sha256=_ACCOUNT_SCOPE_SHA256,
            not_after=_NOT_AFTER,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("trade_date", "2026-07-16"),
        ("candidate_instance_id", "different-private-id"),
        ("classification", "HISTORICAL_CLOSED"),
        ("action", "DISPOSE_EXPIRED_PLAN_READY"),
        ("pipeline_fingerprint", "0" * 64),
        ("subject_version", "1" * 64),
        ("source_fingerprint", "2" * 64),
        ("candidate_fingerprint", "3" * 64),
        ("downstream_fingerprint", "4" * 64),
        ("boundary_fingerprint", "5" * 64),
    ),
)
def test_orphan_evidence_rejects_target_binding_drift(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    target = _target()
    evidence = _evidence(target=target, target_set_sha256="f" * 64)
    evidence["target"][field] = value
    path = _write_json(tmp_path / f"wrong-{field}.json", evidence)

    with pytest.raises(OrphanManualEvidenceError):
        validate_orphan_manual_evidence_document(
            read_stable_json_document(path),
            expected_target_set_sha256="f" * 64,
            expected_alias="M001",
            expected_target=target,
            expected_artifact_sha256=_ARTIFACT_SHA256,
            expected_artifact_size=_ARTIFACT_SIZE,
            expected_account_scope_sha256=_ACCOUNT_SCOPE_SHA256,
            not_after=_NOT_AFTER,
        )


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("determination", "status"), "SELF_ASSERTED"),
        (("determination", "terminal_orphan_confirmed"), 1),
        (("determination", "candidate_present"), True),
        (("provenance", "source_type"), "UNREVIEWED_SCREENSHOT"),
        (("provenance", "source_type"), "OPERATING_DATABASE_AUDIT_EXPORT"),
        (("provenance", "source_ref"), "account:12345678"),
        (("provenance", "source_ref"), "broker/account/1234-5678"),
        (("provenance", "source_ref"), r"C:\private\evidence.json"),
        (("provenance", "artifact_sha256"), "A" * 64),
        (("provenance", "artifact_sha256"), "2" * 64),
        (("provenance", "artifact_size"), True),
        (("provenance", "artifact_size"), 129),
        (("provenance", "coverage_trade_date"), "2026-07-14"),
        (("provenance", "coverage_status"), "PARTIAL"),
        (("provenance", "account_scope_sha256"), "A" * 64),
        (("provenance", "account_scope_sha256"), "3" * 64),
        (("provenance", "observed_at"), "2026-07-15T00:00:00+00:00"),
        (("provenance", "reviewed_at"), "2026-07-14T00:00:00Z"),
        (("provenance", "reviewed_at"), "2026-07-16T02:00:01Z"),
    ),
)
def test_orphan_evidence_rejects_invalid_determination_or_provenance(
    tmp_path: Path,
    path: tuple[str, str],
    value: object,
) -> None:
    target = _target()
    evidence = _evidence(target=target, target_set_sha256="f" * 64)
    evidence[path[0]][path[1]] = value
    evidence_path = _write_json(tmp_path / f"invalid-{path[1]}.json", evidence)

    with pytest.raises(OrphanManualEvidenceError):
        validate_orphan_manual_evidence_document(
            read_stable_json_document(evidence_path),
            expected_target_set_sha256="f" * 64,
            expected_alias="M001",
            expected_target=target,
            expected_artifact_sha256=_ARTIFACT_SHA256,
            expected_artifact_size=_ARTIFACT_SIZE,
            expected_account_scope_sha256=_ACCOUNT_SCOPE_SHA256,
            not_after=_NOT_AFTER,
        )


def test_orphan_evidence_rejects_m000_alias(tmp_path: Path) -> None:
    target = _target()
    evidence = _evidence(target=target, target_set_sha256="f" * 64)
    evidence["alias"] = "M000"
    path = _write_json(tmp_path / "invalid-m000.json", evidence)

    with pytest.raises(OrphanManualEvidenceError, match="ORPHAN_EVIDENCE_ALIAS_INVALID"):
        validate_orphan_manual_evidence_document(
            read_stable_json_document(path),
            expected_target_set_sha256="f" * 64,
            expected_alias="M000",
            expected_target=target,
            expected_artifact_sha256=_ARTIFACT_SHA256,
            expected_artifact_size=_ARTIFACT_SIZE,
            expected_account_scope_sha256=_ACCOUNT_SCOPE_SHA256,
            not_after=_NOT_AFTER,
        )


def test_orphan_evidence_requires_observation_after_local_trade_date(tmp_path: Path) -> None:
    target = _target()
    evidence = _evidence(target=target, target_set_sha256="f" * 64)
    evidence["provenance"]["observed_at"] = "2026-07-15T14:59:59Z"
    evidence["provenance"]["reviewed_at"] = "2026-07-15T15:01:00Z"
    path = _write_json(tmp_path / "not-final-for-trade-date.json", evidence)

    with pytest.raises(
        OrphanManualEvidenceError,
        match="ORPHAN_EVIDENCE_NOT_FINAL_FOR_TRADE_DATE",
    ):
        validate_orphan_manual_evidence_document(
            read_stable_json_document(path),
            expected_target_set_sha256="f" * 64,
            expected_alias="M001",
            expected_target=target,
            expected_artifact_sha256=_ARTIFACT_SHA256,
            expected_artifact_size=_ARTIFACT_SIZE,
            expected_account_scope_sha256=_ACCOUNT_SCOPE_SHA256,
            not_after=_NOT_AFTER,
        )


def _target() -> dict[str, str]:
    return {
        "trade_date": "2026-07-15",
        "candidate_instance_id": "orphan-private-id",
        "classification": ORPHAN_CLASSIFICATION,
        "action": ORPHAN_ACTION,
        "pipeline_fingerprint": "a" * 64,
        "subject_version": "b" * 64,
        "source_fingerprint": "c" * 64,
        "candidate_fingerprint": "d" * 64,
        "downstream_fingerprint": "e" * 64,
        "boundary_fingerprint": "0" * 64,
    }


def _evidence(*, target: dict[str, str], target_set_sha256: str) -> dict:
    return {
        "contract": ORPHAN_EVIDENCE_CONTRACT,
        "target_set_sha256": target_set_sha256,
        "alias": "M001",
        "target": dict(target),
        "determination": {
            "status": "AUTHORITATIVE",
            "terminal_orphan_confirmed": True,
            "candidate_present": False,
            "current_source_present": False,
            "order_or_broker_activity_present": False,
        },
        "provenance": {
            "source_type": "BROKER_ORDER_HISTORY_EXPORT",
            "source_ref": "broker/order-history/private-ref",
            "artifact_sha256": _ARTIFACT_SHA256,
            "artifact_size": _ARTIFACT_SIZE,
            "coverage_trade_date": target["trade_date"],
            "coverage_status": "FINAL_COMPLETE",
            "account_scope_sha256": _ACCOUNT_SCOPE_SHA256,
            "observed_at": "2026-07-15T15:00:00Z",
            "reviewed_at": "2026-07-15T15:01:00Z",
            "reviewer_id": "reviewer.safe",
        },
    }


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
