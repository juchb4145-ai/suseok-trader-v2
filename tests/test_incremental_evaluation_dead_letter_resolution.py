from __future__ import annotations

import sqlite3

import pytest
from services.runtime.incremental_evaluation_dead_letter_resolution import (
    ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
    ACTION_RESET_CANARY,
    BUCKET_HISTORICAL_PENDING,
    BUCKET_INVALID_DISPOSITION,
    IncrementalEvaluationDeadLetterResolutionError,
    build_incremental_evaluation_dead_letter_effective_status,
    list_incremental_evaluation_dead_letter_effective_rows,
    preview_incremental_evaluation_dead_letter_disposition,
    record_incremental_evaluation_dead_letter_disposition,
    revoke_incremental_evaluation_dead_letter_disposition,
)
from storage.sqlite import initialize_database

EVIDENCE_SHA256 = "a" * 64


def _insert_closed_legacy_dead_letter(
    connection: sqlite3.Connection,
    *,
    candidate_id: str = "CAND-2026-07-03-005930-1",
    dead_letter_id: str = "incremental_dead_letter_legacy_001",
) -> dict[str, str]:
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id, trade_date, code, name, generation,
            state, previous_state, detected_at, last_seen_at,
            state_updated_at, closed_at, primary_source_type,
            primary_source_id, source_count, active_source_count
        )
        VALUES (?, '2026-07-03', '005930', 'Samsung', 1,
                'CLOSED', 'CONTEXT_READY', '2026-07-03T00:00:00Z',
                '2026-07-03T00:05:00Z', '2026-07-03T00:10:00Z',
                '2026-07-03T00:10:00Z', 'THEME_LEADER',
                'theme-legacy', 0, 0)
        """,
        (candidate_id,),
    )
    connection.execute(
        """
        INSERT INTO incremental_evaluation_dead_letters (
            dead_letter_id, candidate_instance_id, trade_date, code,
            reason, source_event_id, priority, original_enqueued_at,
            last_queue_updated_at, attempts, last_error, status,
            dead_lettered_at, reset_at, reset_by, reset_evidence_json
        )
        VALUES (?, ?, '2026-07-03', '005930', 'PRICE_TICK', NULL, 100,
                '2026-07-03T00:01:00Z', '2026-07-03T00:02:00Z', 3,
                'LEGACY_RETRY_EXHAUSTED', 'DEAD_LETTER',
                '2026-07-13T00:00:00Z', NULL, NULL, '{}')
        """,
        (dead_letter_id, candidate_id),
    )
    connection.commit()
    preview = preview_incremental_evaluation_dead_letter_disposition(
        connection,
        dead_letter_id,
        action=ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
    )
    assert preview["eligible"] is True
    assert preview["bucket"] == BUCKET_HISTORICAL_PENDING
    return {
        "candidate_id": candidate_id,
        "dead_letter_id": dead_letter_id,
        "dead_letter_fingerprint": preview["dead_letter_fingerprint"],
        "candidate_version": preview["candidate_version"],
    }


def _record_disposition(
    connection: sqlite3.Connection,
    versions: dict[str, str],
    *,
    request_id: str = "fast0r2.request.001",
    reason_code: str = "OBSOLETE_CLOSED_CANDIDATE",
) -> dict[str, object]:
    return record_incremental_evaluation_dead_letter_disposition(
        connection,
        versions["dead_letter_id"],
        request_id=request_id,
        expected_dead_letter_fingerprint=versions["dead_letter_fingerprint"],
        expected_candidate_version=versions["candidate_version"],
        reason_code=reason_code,
        operator_id="fast0r2.operator",
        evidence_ref="fast0r2.rca.incremental-legacy",
        evidence_sha256=EVIDENCE_SHA256,
    )


def _raw_snapshot(
    connection: sqlite3.Connection,
    dead_letter_id: str,
) -> dict[str, object]:
    row = connection.execute(
        "SELECT * FROM incremental_evaluation_dead_letters WHERE dead_letter_id = ?",
        (dead_letter_id,),
    ).fetchone()
    assert row is not None
    return dict(row)


def test_historical_disposition_clears_effective_blocker_without_changing_raw_row(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "historical-disposition.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    raw_before = _raw_snapshot(connection, versions["dead_letter_id"])

    pending = build_incremental_evaluation_dead_letter_effective_status(connection)
    applied = _record_disposition(connection, versions)
    cleared = build_incremental_evaluation_dead_letter_effective_status(connection)
    replayed = _record_disposition(connection, versions)
    revoked = revoke_incremental_evaluation_dead_letter_disposition(
        connection,
        versions["dead_letter_id"],
        request_id="fast0r2.request.revoke.001",
        expected_dead_letter_fingerprint=versions["dead_letter_fingerprint"],
        expected_candidate_version=versions["candidate_version"],
        supersedes_disposition_id=str(applied["disposition_id"]),
        reason_code="DISPOSITION_CORRECTION",
        operator_id="fast0r2.operator",
        evidence_ref="fast0r2.correction.incremental-legacy",
        evidence_sha256=EVIDENCE_SHA256,
    )
    after_revoke = build_incremental_evaluation_dead_letter_effective_status(connection)
    raw_after = _raw_snapshot(connection, versions["dead_letter_id"])
    ledger = connection.execute(
        """
        SELECT action, sequence_no, supersedes_disposition_id
        FROM incremental_evaluation_dead_letter_dispositions
        ORDER BY sequence_no
        """
    ).fetchall()
    connection.close()

    assert pending["raw_dead_letter_count"] == 1
    assert pending["historical_pending_disposition_count"] == 1
    assert pending["effective_dead_letter_count"] == 1
    assert pending["fast_0_status"] == "BLOCKED"
    assert applied["idempotent_replay"] is False
    assert applied["raw_dead_letter_changed"] is False
    assert cleared["raw_dead_letter_count"] == 1
    assert cleared["raw_status"] == "FAIL"
    assert cleared["historical_disposed_dead_letter_count"] == 1
    assert cleared["effective_dead_letter_count"] == 0
    assert cleared["effective_status"] == "PASS"
    assert cleared["fast_0_status"] == "CLEAR"
    assert replayed["idempotent_replay"] is True
    assert replayed["disposition_id"] == applied["disposition_id"]
    assert revoked["action"] == "REVOKE"
    assert revoked["sequence_no"] == 2
    assert after_revoke["raw_dead_letter_count"] == 1
    assert after_revoke["historical_pending_disposition_count"] == 1
    assert after_revoke["effective_dead_letter_count"] == 1
    assert after_revoke["fast_0_status"] == "BLOCKED"
    assert raw_after == raw_before
    assert [row["action"] for row in ledger] == [
        "DISPOSE_OBSOLETE_CLOSED_CANDIDATE",
        "REVOKE",
    ]
    assert ledger[1]["supersedes_disposition_id"] == applied["disposition_id"]


def test_disposition_rejects_cas_mismatch_and_conflicting_request_reuse(tmp_path) -> None:
    connection = initialize_database(tmp_path / "disposition-cas.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)

    with pytest.raises(IncrementalEvaluationDeadLetterResolutionError) as mismatch:
        record_incremental_evaluation_dead_letter_disposition(
            connection,
            versions["dead_letter_id"],
            request_id="fast0r2.request.cas-mismatch",
            expected_dead_letter_fingerprint="b" * 64,
            expected_candidate_version=versions["candidate_version"],
            reason_code="OBSOLETE_CLOSED_CANDIDATE",
            operator_id="fast0r2.operator",
            evidence_ref="fast0r2.rca.incremental-legacy",
            evidence_sha256=EVIDENCE_SHA256,
        )
    assert mismatch.value.code == "DEAD_LETTER_FINGERPRINT_MISMATCH"
    assert (
        connection.execute(
            "SELECT COUNT(*) AS count FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchone()["count"]
        == 0
    )

    _record_disposition(connection, versions, request_id="fast0r2.request.conflict")
    with pytest.raises(IncrementalEvaluationDeadLetterResolutionError) as conflict:
        _record_disposition(
            connection,
            versions,
            request_id="fast0r2.request.conflict",
            reason_code="DIFFERENT_REASON_CODE",
        )
    ledger_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()["count"]
    connection.close()

    assert conflict.value.code == "REQUEST_CONFLICT"
    assert ledger_count == 1


def test_raw_dead_letter_and_disposition_ledger_are_append_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "append-only.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    applied = _record_disposition(connection, versions)

    with pytest.raises(sqlite3.IntegrityError, match="dead letters are immutable"):
        connection.execute(
            """
            UPDATE incremental_evaluation_dead_letters
            SET last_error = 'rewritten'
            WHERE dead_letter_id = ?
            """,
            (versions["dead_letter_id"],),
        )
    connection.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="dead letters are immutable"):
        connection.execute(
            "DELETE FROM incremental_evaluation_dead_letters WHERE dead_letter_id = ?",
            (versions["dead_letter_id"],),
        )
    connection.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="dispositions are append-only"):
        connection.execute(
            """
            UPDATE incremental_evaluation_dead_letter_dispositions
            SET reason_code = 'rewritten'
            WHERE disposition_id = ?
            """,
            (applied["disposition_id"],),
        )
    connection.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="dispositions are append-only"):
        connection.execute(
            """
            DELETE FROM incremental_evaluation_dead_letter_dispositions
            WHERE disposition_id = ?
            """,
            (applied["disposition_id"],),
        )
    connection.rollback()

    assert _raw_snapshot(connection, versions["dead_letter_id"])["last_error"] == (
        "LEGACY_RETRY_EXHAUSTED"
    )
    assert (
        connection.execute(
            "SELECT reason_code FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchone()["reason_code"]
        == "OBSOLETE_CLOSED_CANDIDATE"
    )
    connection.close()


def test_invalid_disposition_chain_fails_closed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "invalid-chain.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    connection.execute(
        """
        INSERT INTO incremental_evaluation_dead_letter_dispositions (
            disposition_id, request_id, request_hash, dead_letter_id,
            sequence_no, action, supersedes_disposition_id, reason_code,
            operator_id, expected_dead_letter_fingerprint,
            expected_candidate_version, evidence_ref, evidence_sha256,
            evidence_json, safety_snapshot_json, created_at
        )
        VALUES (
            'incremental_disposition_invalid_chain',
            'fast0r2.request.invalid-chain', ?, ?, 2,
            'DISPOSE_OBSOLETE_CLOSED_CANDIDATE', NULL,
            'OBSOLETE_CLOSED_CANDIDATE', 'fast0r2.operator', ?, ?,
            'fast0r2.rca.incremental-legacy', ?, '{}', '{}',
            '2026-07-15T00:00:00Z'
        )
        """,
        (
            "c" * 64,
            versions["dead_letter_id"],
            versions["dead_letter_fingerprint"],
            versions["candidate_version"],
            EVIDENCE_SHA256,
        ),
    )
    connection.commit()

    status = build_incremental_evaluation_dead_letter_effective_status(connection)
    rows = list_incremental_evaluation_dead_letter_effective_rows(
        connection,
        bucket=BUCKET_INVALID_DISPOSITION,
    )
    connection.close()

    assert status["raw_dead_letter_count"] == 1
    assert status["invalid_disposition_count"] == 1
    assert status["effective_dead_letter_count"] == 1
    assert status["effective_status"] == "FAIL"
    assert status["fast_0_status"] == "BLOCKED"
    assert "INCREMENTAL_INVALID_DISPOSITION_PRESENT" in status["reason_codes"]
    assert len(rows) == 1
    assert rows[0]["bucket"] == BUCKET_INVALID_DISPOSITION
    assert rows[0]["disposition_chain_valid"] is False


def test_closed_legacy_dead_letter_is_excluded_from_recovery(tmp_path) -> None:
    connection = initialize_database(tmp_path / "closed-recovery-exclusion.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)

    preview = preview_incremental_evaluation_dead_letter_disposition(
        connection,
        versions["dead_letter_id"],
        action=ACTION_RESET_CANARY,
        expected_dead_letter_fingerprint=versions["dead_letter_fingerprint"],
        expected_candidate_version=versions["candidate_version"],
    )
    queue_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()["count"]
    disposition_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()["count"]
    connection.close()

    assert preview["eligible"] is False
    assert preview["status"] == "BLOCKED"
    assert preview["bucket"] == BUCKET_HISTORICAL_PENDING
    assert "RECOVERY_CANDIDATE_CLOSED" in preview["reason_codes"]
    assert "RECOVERY_SOURCE_LINEAGE_MISSING" in preview["reason_codes"]
    assert "NOT_ACTIVE_UNRESOLVED" in preview["reason_codes"]
    assert queue_count == 0
    assert disposition_count == 0


def test_disposition_rejects_when_any_runtime_lease_is_present(tmp_path) -> None:
    connection = initialize_database(tmp_path / "runtime-lease.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    connection.execute(
        """
        INSERT INTO runtime_execution_locks (
            lock_name, owner_id, acquired_at, expires_at, process_id,
            thread_id, heartbeat_at, fencing_token, detail_json
        )
        VALUES (
            'evaluation_pipeline', 'other-owner',
            '2026-07-15T00:00:00Z', '2099-07-15T00:00:00Z',
            123, 456, '2026-07-15T00:00:00Z', 7, '{}'
        )
        """
    )
    connection.commit()

    with pytest.raises(IncrementalEvaluationDeadLetterResolutionError) as rejected:
        _record_disposition(connection, versions)
    disposition_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()["count"]
    raw = _raw_snapshot(connection, versions["dead_letter_id"])
    connection.close()

    assert rejected.value.code == "RUNTIME_EXECUTION_LOCK_PRESENT"
    assert disposition_count == 0
    assert raw["status"] == "DEAD_LETTER"
    assert raw["last_error"] == "LEGACY_RETRY_EXHAUSTED"


def test_missing_schema61_trigger_fails_closed_without_projection_crash(tmp_path) -> None:
    connection = initialize_database(tmp_path / "schema-contract-invalid.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)
    connection.execute("DROP TRIGGER trg_incremental_dead_letter_dispositions_no_delete")
    connection.commit()

    status = build_incremental_evaluation_dead_letter_effective_status(connection)
    preview = preview_incremental_evaluation_dead_letter_disposition(
        connection,
        versions["dead_letter_id"],
        action=ACTION_DISPOSE_OBSOLETE_CLOSED_CANDIDATE,
    )
    connection.close()

    assert status["schema_ready"] is False
    assert status["effective_status"] == "FAIL"
    assert status["fast_0_status"] == "BLOCKED"
    assert status["invalid_disposition_count"] == 1
    assert "INCREMENTAL_DEAD_LETTER_DISPOSITION_SCHEMA_INVALID" in status["reason_codes"]
    assert preview["eligible"] is False
    assert preview["bucket"] == BUCKET_INVALID_DISPOSITION
    assert preview["disposition_chain_valid"] is False


def test_public_preview_hashes_last_error_instead_of_exposing_raw_value(tmp_path) -> None:
    connection = initialize_database(tmp_path / "public-last-error.sqlite3")
    versions = _insert_closed_legacy_dead_letter(connection)

    preview = preview_incremental_evaluation_dead_letter_disposition(
        connection,
        versions["dead_letter_id"],
    )
    connection.close()

    raw = preview["raw_dead_letter"]
    assert "last_error" not in raw
    assert raw["last_error_present"] is True
    assert raw["last_error_sha256"]
    assert "LEGACY_RETRY_EXHAUSTED" not in str(preview)
