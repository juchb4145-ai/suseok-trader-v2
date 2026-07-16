from __future__ import annotations

import json
from pathlib import Path

import pytest
from domain.broker.utils import datetime_to_wire, utc_now
from services.incremental_dead_letter_campaign import (
    CAMPAIGN_ACTION,
    CAMPAIGN_BUCKET,
    build_campaign_apply_evidence_binding,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (
    IncrementalEvaluationDeadLetterResolutionError,
    _issue_incremental_campaign_apply_approval_context,
    append_incremental_dead_letter_campaign_disposition_in_transaction,
)
from storage.sqlite import initialize_database
from tests.test_incremental_evaluation_dead_letter_resolution import (
    _insert_closed_legacy_dead_letter,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
SHA_E = "e" * 64
SHA_F = "f" * 64


def _binding(versions: dict[str, str]) -> dict[str, object]:
    now = datetime_to_wire(utc_now())
    return build_campaign_apply_evidence_binding(
        {
            "action": CAMPAIGN_ACTION,
            "dead_letter_id": versions["dead_letter_id"],
            "dead_letter_fingerprint": versions["dead_letter_fingerprint"],
            "candidate_version": versions["candidate_version"],
            "bucket": CAMPAIGN_BUCKET,
            "alias": "U01",
        },
        source_plan_report_sha256=SHA_A,
        private_manifest_sha256=SHA_B,
        target_set_sha256=SHA_C,
        approved_preflight_report_sha256=SHA_D,
        approved_database_main_sha256=SHA_E,
        approved_database_main_size=1234,
        predecessor_kind=None,
        predecessor_report_sha256=None,
        predecessor_chain_sha256=None,
        evidence_sha256=SHA_F,
        generated_at=now,
        preflight_generated_at=now,
    )


def test_campaign_service_requires_caller_transaction_and_sealed_context(
    tmp_path: Path,
) -> None:
    connection = initialize_database(tmp_path / "campaign-service-guard.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    binding = _binding(versions)
    kwargs = {
        "dead_letter_id": versions["dead_letter_id"],
        "request_id": "fast0r9.request.u01",
        "expected_dead_letter_fingerprint": versions["dead_letter_fingerprint"],
        "expected_candidate_version": versions["candidate_version"],
        "operator_id": "fast0r9.operator",
        "evidence_ref": "fast0r9.u01.evidence",
        "evidence_sha256": SHA_F,
        "evidence_json": binding,
        "safety_snapshot": {"observe_only": True},
        "campaign_approval_context": None,
    }

    with pytest.raises(IncrementalEvaluationDeadLetterResolutionError) as no_tx:
        append_incremental_dead_letter_campaign_disposition_in_transaction(connection, **kwargs)
    assert no_tx.value.code == "CAMPAIGN_ACTIVE_TRANSACTION_REQUIRED"

    connection.execute("BEGIN EXCLUSIVE")
    with pytest.raises(IncrementalEvaluationDeadLetterResolutionError) as no_context:
        append_incremental_dead_letter_campaign_disposition_in_transaction(connection, **kwargs)
    assert no_context.value.code == "CAMPAIGN_APPROVAL_CONTEXT_REQUIRED"
    connection.rollback()
    connection.close()


def test_campaign_service_appends_exact_evidence_without_committing(
    tmp_path: Path,
) -> None:
    connection = initialize_database(tmp_path / "campaign-service-append.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    binding = _binding(versions)
    context = _issue_incremental_campaign_apply_approval_context(
        binding,
        verified_preflight_report_sha256=SHA_D,
        verified_database_main_sha256=SHA_E,
        expected_alias="U01",
        dead_letter_id=versions["dead_letter_id"],
        expected_dead_letter_fingerprint=versions["dead_letter_fingerprint"],
        expected_candidate_version=versions["candidate_version"],
        evidence_sha256=SHA_F,
    )
    connection.execute("BEGIN EXCLUSIVE")
    result = append_incremental_dead_letter_campaign_disposition_in_transaction(
        connection,
        dead_letter_id=versions["dead_letter_id"],
        request_id="fast0r9.request.u01",
        expected_dead_letter_fingerprint=versions["dead_letter_fingerprint"],
        expected_candidate_version=versions["candidate_version"],
        operator_id="fast0r9.operator",
        evidence_ref="fast0r9.u01.evidence",
        evidence_sha256=SHA_F,
        evidence_json=binding,
        safety_snapshot={"observe_only": True},
        campaign_approval_context=context,
    )
    stored = connection.execute(
        "SELECT evidence_json FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()

    assert connection.in_transaction is True
    assert result["idempotent_replay"] is False
    assert stored is not None
    assert json.loads(stored["evidence_json"]) == binding
    assert "request_payload" not in json.loads(stored["evidence_json"])

    connection.rollback()
    count = connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    connection.close()
    assert count == 0


def test_campaign_service_exact_replay_rejects_evidence_change(tmp_path: Path) -> None:
    connection = initialize_database(tmp_path / "campaign-service-replay.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    binding = _binding(versions)
    context = _issue_incremental_campaign_apply_approval_context(
        binding,
        verified_preflight_report_sha256=SHA_D,
        verified_database_main_sha256=SHA_E,
        expected_alias="U01",
        dead_letter_id=versions["dead_letter_id"],
        expected_dead_letter_fingerprint=versions["dead_letter_fingerprint"],
        expected_candidate_version=versions["candidate_version"],
        evidence_sha256=SHA_F,
    )
    kwargs = {
        "dead_letter_id": versions["dead_letter_id"],
        "request_id": "fast0r9.request.u01",
        "expected_dead_letter_fingerprint": versions["dead_letter_fingerprint"],
        "expected_candidate_version": versions["candidate_version"],
        "operator_id": "fast0r9.operator",
        "evidence_ref": "fast0r9.u01.evidence",
        "evidence_sha256": SHA_F,
        "evidence_json": binding,
        "safety_snapshot": {"observe_only": True},
        "campaign_approval_context": context,
    }
    connection.execute("BEGIN EXCLUSIVE")
    append_incremental_dead_letter_campaign_disposition_in_transaction(connection, **kwargs)
    connection.commit()

    tampered = dict(binding)
    tampered["generated_at"] = "2026-07-16T00:00:00Z"
    connection.execute("BEGIN EXCLUSIVE")
    with pytest.raises(IncrementalEvaluationDeadLetterResolutionError) as conflict:
        append_incremental_dead_letter_campaign_disposition_in_transaction(
            connection,
            **{**kwargs, "evidence_json": tampered},
        )
    assert conflict.value.code == "CAMPAIGN_APPROVAL_BINDING_INVALID"
    connection.rollback()
    connection.close()
