from __future__ import annotations

import copy
from pathlib import Path

from services.pipeline_coherency import build_pipeline_coherency_rca_status
from services.pipeline_legacy_evidence import (
    MANIFEST_CONTRACT,
    _current_subject_evidence,
    _sha256_json,
    legacy_target_set_sha256,
    resolve_pipeline_legacy_evidence,
)
from storage.sqlite import initialize_database
from tests.test_pipeline_coherency_rca import (
    AS_OF,
    TRADE_DATE,
    _insert_candidate,
    _insert_pipeline_observations,
)


def test_authoritative_legacy_resolver_replaces_only_two_proven_blockers(
    tmp_path: Path,
) -> None:
    connection = initialize_database(tmp_path / "legacy-evidence.sqlite3")
    _insert_candidate(
        connection,
        "candidate-legacy",
        state="CLOSED",
        closed_at="2026-07-10T01:00:00Z",
    )
    _insert_pipeline_observations(connection, "candidate-legacy")
    connection.execute(
        "UPDATE candidates SET active_source_count = 1 WHERE candidate_instance_id = ?",
        ("candidate-legacy",),
    )
    _seed_inactive_source(connection)
    connection.commit()

    baseline = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-legacy",
        as_of=AS_OF,
    )
    item = baseline["items"][0]
    assert item["legacy_warn_reason_codes"] == [
        "ACTIVE_SOURCE_NOT_PROVEN_ZERO",
        "CURRENT_SOURCE_DRIFT_UNKNOWN",
    ]
    manifest = _manifest(connection, item)

    qualified = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-legacy",
        legacy_evidence_resolver=lambda trade_date, subjects: resolve_pipeline_legacy_evidence(
            connection,
            trade_date,
            subjects,
            manifest=manifest,
        ),
        as_of=AS_OF,
    )
    resolved = qualified["items"][0]

    assert qualified["legacy_evidence_ready"] is True
    assert resolved["legacy_warn_candidate"] is True
    assert resolved["legacy_warn_effective_reason_codes"] == []
    assert resolved["disposition_required"] is False
    assert resolved["legacy_warn_evidence"]["active_source_zero"] is True
    assert resolved["legacy_warn_evidence"]["current_source_no_drift"] is True

    def contaminated_resolver(trade_date, subjects):
        value = resolve_pipeline_legacy_evidence(
            connection,
            trade_date,
            subjects,
            manifest=manifest,
        )
        value["items"]["candidate-legacy"]["raw_payload"] = "must-not-escape"
        return value

    contaminated = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-legacy",
        legacy_evidence_resolver=contaminated_resolver,
        as_of=AS_OF,
    )["items"][0]
    assert contaminated["legacy_warn_candidate"] is False
    assert contaminated["legacy_warn_evidence"]["status"] == (
        "INVALID_AUTHORITATIVE_EVIDENCE"
    )
    assert "raw_payload" not in contaminated["legacy_warn_evidence"]
    connection.close()


def test_legacy_resolver_fails_closed_on_manifest_or_database_drift(tmp_path: Path) -> None:
    connection = initialize_database(tmp_path / "legacy-drift.sqlite3")
    _insert_candidate(
        connection,
        "candidate-legacy",
        state="CLOSED",
        closed_at="2026-07-10T01:00:00Z",
    )
    _insert_pipeline_observations(connection, "candidate-legacy")
    connection.execute(
        "UPDATE candidates SET active_source_count = 1 WHERE candidate_instance_id = ?",
        ("candidate-legacy",),
    )
    _seed_inactive_source(connection)
    connection.commit()
    item = build_pipeline_coherency_rca_status(
        connection,
        trade_date=TRADE_DATE,
        max_age_sec=10**12,
        candidate_instance_id="candidate-legacy",
        as_of=AS_OF,
    )["items"][0]
    manifest = _manifest(connection, item)

    tampered = copy.deepcopy(manifest)
    tampered["items"][0]["source_fingerprint"] = "f" * 64
    tampered["target_set_sha256"] = legacy_target_set_sha256(tampered["items"])
    result = resolve_pipeline_legacy_evidence(
        connection,
        TRADE_DATE,
        [item],
        manifest=tampered,
    )
    assert result == {
        "status": "BLOCKED",
        "reason_codes": ["PIPELINE_LEGACY_AUTHORITATIVE_EVIDENCE_INVALID"],
        "items": {},
    }

    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id, candidate_instance_id, trade_date, code, name,
            source_type, source_id, action, event_ts, observed_at, active, payload_json
        ) VALUES ('event-active', ?, ?, '005930', 'fixture', 'TEST', 'source-1',
                  'REOPENED', '2026-07-10T02:00:00Z', '2026-07-10T02:00:00Z', 1, '{}')
        """,
        ("candidate-legacy", TRADE_DATE),
    )
    connection.execute(
        """
        UPDATE candidate_sources_latest
        SET active = 1, last_seen_at = '2026-07-10T02:00:00Z', last_event_id = 'event-active'
        WHERE candidate_instance_id = ?
        """,
        ("candidate-legacy",),
    )
    connection.commit()
    active_manifest = _manifest(connection, item)
    active = resolve_pipeline_legacy_evidence(
        connection,
        TRADE_DATE,
        [item],
        manifest=active_manifest,
    )
    assert active["status"] == "BLOCKED"
    assert active["items"] == {}
    connection.close()


def _manifest(connection, item):
    cutoff = {
        "completed_at": "2026-07-11T00:00:00Z",
        "migration_report_sha256": "1" * 64,
        "source_database_sha256": "2" * 64,
        "migrated_database_sha256": "3" * 64,
    }
    evidence = _current_subject_evidence(
        connection,
        candidate_instance_id=str(item["candidate_instance_id"]),
        trade_date=TRADE_DATE,
        cutoff=AS_OF.replace(day=11),
    )
    approved = {
        "alias": "L001",
        "candidate_instance_id": item["candidate_instance_id"],
        "pipeline_fingerprint": item["pipeline_fingerprint"],
        "subject_version": item["subject_version"],
        "source_fingerprint": evidence["source_fingerprint"],
        "closure_fingerprint": evidence["closure_fingerprint"],
        "pipeline_stage_fingerprint": evidence["pipeline_stage_fingerprint"],
        "provenance_sha256": _sha256_json(cutoff),
    }
    return {
        "contract": MANIFEST_CONTRACT,
        "trade_date": TRADE_DATE,
        "target_set_sha256": legacy_target_set_sha256([approved]),
        "schema59_cutover": cutoff,
        "items": [approved],
    }


def _seed_inactive_source(connection) -> None:
    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id, candidate_instance_id, trade_date, code, name,
            source_type, source_id, action, event_ts, observed_at, active, payload_json
        ) VALUES ('event-inactive', 'candidate-legacy', ?, '005930', 'fixture',
                  'TEST', 'source-1', 'CLOSED', '2026-07-10T00:00:00Z',
                  '2026-07-10T00:00:00Z', 0, '{}')
        """,
        (TRADE_DATE,),
    )
    connection.execute(
        """
        INSERT INTO candidate_sources_latest (
            trade_date, code, source_type, source_id, candidate_instance_id,
            name, active, first_seen_at, last_seen_at, last_event_id, payload_json
        ) VALUES (?, '005930', 'TEST', 'source-1', 'candidate-legacy', 'fixture', 0,
                  '2026-07-10T00:00:00Z', '2026-07-10T00:00:00Z', 'event-inactive', '{}')
        """,
        (TRADE_DATE,),
    )
