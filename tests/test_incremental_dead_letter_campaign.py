from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from services.incremental_dead_letter_campaign import (
    CAMPAIGN_ACTION,
    CAMPAIGN_BUCKET,
    IncrementalDeadLetterCampaignError,
    build_campaign_apply_evidence_binding,
    campaign_chain_sha256,
    privacy_safe_target_projection,
    read_stable_json_document,
    validate_campaign_apply_evidence_binding,
    validate_private_target_manifest,
)

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
SHA_D = "d" * 64
SHA_E = "e" * 64
SHA_F = "f" * 64


def _items() -> list[dict[str, object]]:
    return [
        {
            "action": CAMPAIGN_ACTION,
            "dead_letter_id": f"incremental-dead-letter-{ordinal:03d}",
            "dead_letter_fingerprint": f"{ordinal:064x}",
            "candidate_version": f"{ordinal + 100:064x}",
            "bucket": CAMPAIGN_BUCKET,
            "alias": f"U{ordinal:02d}",
        }
        for ordinal in range(1, 39)
    ]


def _write_manifest(path: Path, items: list[dict[str, object]] | None = None) -> Path:
    path.write_text(
        json.dumps(
            {"contract": "fast0-private-target-set.v1", "items": items or _items()},
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    return path


def test_private_manifest_exact_contract_and_privacy_projection(tmp_path: Path) -> None:
    path = _write_manifest(tmp_path / "manifest.json")
    document = read_stable_json_document(path)
    validated = validate_private_target_manifest(
        document,
        expected_semantic_sha256=document.sha256,
    )
    projection = privacy_safe_target_projection(validated["items"][0])

    assert validated["count"] == 38
    assert validated["target_set_sha256"] == document.sha256
    assert projection["alias"] == "U01"
    assert projection["ordinal"] == 1
    assert projection["dead_letter_id_sha256"]
    assert "dead_letter_id" not in projection
    assert "incremental-dead-letter-001" not in json.dumps(projection)


@pytest.mark.parametrize(
    ("mutator", "reason"),
    [
        (lambda items: items.pop(), "TARGET_MANIFEST_COUNT_MISMATCH"),
        (
            lambda items: items[1].update(alias="U01"),
            "TARGET_MANIFEST_ALIAS_SEQUENCE_INVALID",
        ),
        (
            lambda items: items[1].update(dead_letter_id=items[0]["dead_letter_id"]),
            "TARGET_MANIFEST_DUPLICATE_DEAD_LETTER_ID",
        ),
        (
            lambda items: items[0].update(action="RESET_BATCH"),
            "TARGET_MANIFEST_ACTION_INVALID",
        ),
    ],
)
def test_private_manifest_mutations_fail_closed(
    tmp_path: Path,
    mutator,
    reason: str,
) -> None:
    items = _items()
    mutator(items)
    document = read_stable_json_document(_write_manifest(tmp_path / "manifest.json", items))

    with pytest.raises(IncrementalDeadLetterCampaignError) as exc_info:
        validate_private_target_manifest(
            document,
            expected_semantic_sha256=document.sha256,
        )

    assert exc_info.value.code == reason


def test_stable_json_rejects_duplicate_keys_and_hardlinks(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"contract":"x","contract":"y"}', encoding="utf-8")
    with pytest.raises(IncrementalDeadLetterCampaignError) as duplicate_error:
        read_stable_json_document(duplicate)
    assert duplicate_error.value.code == "DOCUMENT_JSON_INVALID"

    source = _write_manifest(tmp_path / "source.json")
    alias = tmp_path / "hardlink.json"
    os.link(source, alias)
    with pytest.raises(IncrementalDeadLetterCampaignError) as hardlink_error:
        read_stable_json_document(alias)
    assert hardlink_error.value.code == "DOCUMENT_HARDLINK_FORBIDDEN"


def test_apply_binding_is_exact_private_and_chained() -> None:
    target = _items()[0]
    binding = build_campaign_apply_evidence_binding(
        target,
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
        generated_at="2026-07-16T01:00:01Z",
        preflight_generated_at="2026-07-16T01:00:00Z",
    )
    normalized = validate_campaign_apply_evidence_binding(
        binding,
        expected_alias="U01",
        dead_letter_id=str(target["dead_letter_id"]),
        expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
        expected_candidate_version=str(target["candidate_version"]),
        evidence_sha256=SHA_F,
        not_after=datetime(2026, 7, 16, 1, 0, 2, tzinfo=UTC),
    )
    encoded = json.dumps(normalized, sort_keys=True)
    chain = campaign_chain_sha256(
        source_plan_report_sha256=SHA_A,
        target_set_sha256=SHA_C,
        alias="U01",
        predecessor_chain_sha256=None,
        approval_binding_sha256=str(normalized["approval_binding_sha256"]),
        request_id_sha256=SHA_D,
        disposition_id_sha256=SHA_E,
    )

    assert normalized == binding
    assert chain and len(chain) == 64
    assert str(target["dead_letter_id"]) not in encoded
    assert "operator" not in encoded
    assert "evidence_ref" not in encoded


def test_apply_binding_rejects_tamper_and_alias_skip() -> None:
    target = _items()[1]
    binding = build_campaign_apply_evidence_binding(
        target,
        source_plan_report_sha256=SHA_A,
        private_manifest_sha256=SHA_B,
        target_set_sha256=SHA_C,
        approved_preflight_report_sha256=SHA_D,
        approved_database_main_sha256=SHA_E,
        approved_database_main_size=1234,
        predecessor_kind="CAMPAIGN_APPLY_REPORT",
        predecessor_report_sha256=SHA_F,
        predecessor_chain_sha256=SHA_A,
        evidence_sha256=SHA_B,
        generated_at="2026-07-16T01:00:01Z",
        preflight_generated_at="2026-07-16T01:00:00Z",
    )
    tampered = dict(binding)
    tampered["approved_database_main_sha256"] = SHA_F

    with pytest.raises(IncrementalDeadLetterCampaignError) as tamper_error:
        validate_campaign_apply_evidence_binding(
            tampered,
            expected_alias="U02",
            dead_letter_id=str(target["dead_letter_id"]),
            expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
            expected_candidate_version=str(target["candidate_version"]),
            evidence_sha256=SHA_B,
            not_after=datetime(2026, 7, 16, 1, 0, 2, tzinfo=UTC),
        )
    assert tamper_error.value.code == "APPLY_BINDING_DIGEST_INVALID"

    with pytest.raises(IncrementalDeadLetterCampaignError) as skip_error:
        validate_campaign_apply_evidence_binding(
            binding,
            expected_alias="U03",
            dead_letter_id=str(target["dead_letter_id"]),
            expected_dead_letter_fingerprint=str(target["dead_letter_fingerprint"]),
            expected_candidate_version=str(target["candidate_version"]),
            evidence_sha256=SHA_B,
            not_after=datetime(2026, 7, 16, 1, 0, 2, tzinfo=UTC),
        )
    assert skip_error.value.code == "APPLY_BINDING_TARGET_MISMATCH:ALIAS"
