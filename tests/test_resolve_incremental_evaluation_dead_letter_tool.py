from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import tools.resolve_incremental_evaluation_dead_letter as resolution_tool
from domain.broker.utils import datetime_to_wire, utc_now
from domain.candidate.state import CandidateState
from services.runtime.incremental_evaluation import _mark_queue_error
from storage.sqlite import initialize_database, open_connection
from tests.test_strategy_service import _insert_strategy_fixture
from tools.resolve_incremental_evaluation_dead_letter import (
    ACTION_RESET_BATCH,
    main,
)

DEAD_LETTER_ID = "incremental-dead-letter-alpha"
CANDIDATE_ID = "CAND-2026-07-03-005930-1"
SOURCE_EVENT_ID = "evt-legacy-incremental-alpha"


def test_preview_is_strict_read_only_and_does_not_create_sidecars_or_rows(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "preview.sqlite3"
    _seed_closed_dead_letter(db_path)
    before = _database_contract(db_path)

    exit_code = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "PREVIEW"
    assert payload["mode"] == "STRICT_READ_ONLY"
    assert payload["read_only"] is True
    assert payload["query_only"] is True
    assert payload["no_evaluation_run"] is True
    assert payload["no_order_commands_created"] is True
    assert payload["preview"]["eligible"] is True
    assert _database_contract(db_path) == before


def test_preview_rejects_nonempty_rollback_journal_before_immutable_open(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "preview-hot-journal.sqlite3"
    _seed_closed_dead_letter(db_path)
    journal_path = Path(f"{db_path}-journal")
    journal_path.write_bytes(b"nonempty-hot-journal-sentinel")
    before = _database_contract(db_path)

    exit_code = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert payload["reason_codes"] == ["STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE"]
    assert _database_contract(db_path) == before


def test_strict_preview_rejects_hardlink_database_alias(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "preview-real.sqlite3"
    alias_path = tmp_path / "preview-alias.sqlite3"
    _seed_closed_dead_letter(db_path)
    os.link(db_path, alias_path)

    exit_code = main(
        [
            "--db",
            str(alias_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert payload["reason_codes"] == ["DATABASE_HARDLINK_ALIAS_UNSAFE"]


def test_apply_rejects_hardlink_database_alias_before_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "apply-real.sqlite3"
    alias_path = tmp_path / "apply-alias.sqlite3"
    _seed_closed_dead_letter(db_path)
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)
    os.link(db_path, alias_path)
    safe_env = _write_safe_env(tmp_path, alias_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))

    exit_code = main(
        _disposition_args(
            alias_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert payload["reason_codes"] == ["DATABASE_HARDLINK_ALIAS_UNSAFE"]
    assert _counts(db_path)["dispositions"] == 0


def test_apply_rejects_nonempty_rollback_journal_before_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "apply-hot-journal.sqlite3"
    _seed_closed_dead_letter(db_path)
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    journal_path = Path(f"{db_path}-journal")
    journal_path.write_bytes(b"nonempty-hot-journal-sentinel")
    before = _database_contract(db_path)

    exit_code = main(
        _disposition_args(
            db_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert payload["reason_codes"] == ["DATABASE_ROLLBACK_JOURNAL_PRESENT"]
    assert _database_contract(db_path) == before


def test_disposition_apply_requires_all_acknowledgements_before_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "ack-reject.sqlite3"
    _seed_closed_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)
    args = _disposition_args(
        db_path,
        evidence,
        fingerprint=fingerprint,
        candidate_version=candidate_version,
    )
    args.remove("--acknowledge-raw-audit-preserved")

    exit_code = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "RAW_AUDIT_PRESERVATION_ACK_REQUIRED" in payload["reason_codes"]
    assert _counts(db_path)["dispositions"] == 0


def test_disposition_apply_is_append_only_idempotent_and_redacts_evidence_path(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "apply.sqlite3"
    raw_before = _seed_closed_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence_content = '{"classification":"obsolete-closed-candidate"}'
    evidence = tmp_path / "private-account-98765432-evidence.json"
    evidence.write_text(evidence_content, encoding="utf-8")
    fingerprint, candidate_version = _preview_versions(db_path, capsys)
    args = _disposition_args(
        db_path,
        evidence,
        fingerprint=fingerprint,
        candidate_version=candidate_version,
    )

    first_exit = main(args)
    first = json.loads(capsys.readouterr().out)
    second_exit = main(args)
    second = json.loads(capsys.readouterr().out)

    assert first_exit == 0
    assert second_exit == 0
    assert first["status"] == "APPLIED"
    assert second["status"] == "REPLAYED"
    assert first["raw_dead_letters_unchanged"] is True
    assert first["command_count_delta"] == 0
    assert first["order_command_count_delta"] == 0
    assert first["auto_side_effect_count_invariant_ok"] is True
    assert first["target_write_invariant_ok"] is True
    assert first["no_evaluation_run"] is True
    assert first["no_order_artifacts_created"] is True
    assert first["no_order_commands_created"] is True
    assert _raw_dead_letter(db_path) == raw_before
    assert _counts(db_path) == {
        "raw_dead_letters": 1,
        "dispositions": 1,
        "queued": 0,
        "commands": 0,
        "order_commands": 0,
    }
    rendered = json.dumps([first, second], ensure_ascii=False)
    assert str(evidence) not in rendered
    assert evidence.name not in rendered
    assert evidence_content not in rendered
    assert hashlib.sha256(evidence_content.encode("utf-8")).hexdigest() in rendered


def test_apply_blocks_unexpected_evaluation_write_before_it_reaches_database(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "blocked-evaluation-write.sqlite3"
    _seed_closed_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)

    def attempt_unexpected_evaluation_write(connection, **_kwargs):
        connection.execute(
            """
            INSERT INTO strategy_evaluation_runs (
                run_id, started_at, candidate_count, evaluated_count,
                data_wait_count, matched_observation_count, error_count,
                config_version, status
            )
            VALUES ('unexpected-run', '2026-07-15T00:00:00Z', 1, 1, 0, 0, 0,
                    'unexpected', 'COMPLETED')
            """
        )
        return {"idempotent_replay": False}

    monkeypatch.setattr(
        resolution_tool,
        "record_incremental_evaluation_dead_letter_disposition",
        attempt_unexpected_evaluation_write,
    )

    exit_code = main(
        _disposition_args(
            db_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "UNEXPECTED_DATABASE_WRITE_BLOCKED" in payload["reason_codes"]
    connection = open_connection(db_path)
    try:
        run_count = connection.execute("SELECT COUNT(*) FROM strategy_evaluation_runs").fetchone()[
            0
        ]
    finally:
        connection.close()
    assert run_count == 0
    assert _counts(db_path)["dispositions"] == 0


def test_apply_row_guard_blocks_extra_disposition_insert(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "blocked-extra-disposition.sqlite3"
    _seed_closed_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)
    original = resolution_tool.record_incremental_evaluation_dead_letter_disposition

    def attempt_extra_disposition(connection, **kwargs):
        result = original(connection, **kwargs)
        connection.execute(
            """
            INSERT INTO incremental_evaluation_dead_letter_dispositions (
                disposition_id, request_id, request_hash, dead_letter_id,
                sequence_no, action, supersedes_disposition_id, reason_code,
                operator_id, expected_dead_letter_fingerprint,
                expected_candidate_version, evidence_ref, evidence_sha256,
                evidence_json, safety_snapshot_json, created_at
            ) VALUES (
                'incremental_disposition_unapproved_extra',
                'request.r2.unapproved-extra', ?, ?, 2, 'REVOKE', ?,
                'UNAPPROVED_EXTRA', 'operator.alpha', ?, ?,
                'FAST0R2_UNAPPROVED_EXTRA', ?, '{}', '{}',
                '2026-07-15T00:00:00Z'
            )
            """,
            (
                "f" * 64,
                DEAD_LETTER_ID,
                result["disposition_id"],
                fingerprint,
                candidate_version,
                "e" * 64,
            ),
        )
        return result

    monkeypatch.setattr(
        resolution_tool,
        "record_incremental_evaluation_dead_letter_disposition",
        attempt_extra_disposition,
    )

    exit_code = main(
        _disposition_args(
            db_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "UNEXPECTED_DATABASE_WRITE_BLOCKED" in payload["reason_codes"]
    connection = open_connection(db_path)
    try:
        rows = connection.execute(
            "SELECT request_id, action FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchall()
    finally:
        connection.close()
    assert [(row["request_id"], row["action"]) for row in rows] == [
        ("request.r2.disposition-alpha", "DISPOSE_OBSOLETE_CLOSED_CANDIDATE")
    ]


def test_reset_canary_applies_exactly_one_queue_row_without_running_evaluation(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "reset-canary.sqlite3"
    raw_before = _seed_active_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "RESET_CANARY",
            "--recovery-session-id",
            "recovery.session.alpha",
        ]
    )
    preview = json.loads(capsys.readouterr().out)
    assert preview_exit == 0
    assert preview["preview"]["eligible"] is True

    apply_args = [
        "--db",
        str(db_path),
        "--dead-letter-id",
        DEAD_LETTER_ID,
        "--action",
        "RESET_CANARY",
        "--apply",
        "--request-id",
        "request.r2.canary-alpha",
        "--expected-fingerprint",
        preview["preview"]["dead_letter_fingerprint"],
        "--expected-candidate-version",
        preview["preview"]["candidate_version"],
        "--evidence-file",
        str(evidence),
        "--evidence-ref",
        "FAST0R2_CANARY_ALPHA",
        "--operator-id",
        "operator.alpha",
        "--recovery-session-id",
        "recovery.session.alpha",
        "--acknowledge-effective-status-change",
        "--acknowledge-no-auto-evaluation",
        "--confirm-root-cause-fixed",
    ]
    exit_code = main(apply_args)
    payload = json.loads(capsys.readouterr().out)
    replay_exit_code = main(apply_args)
    replay = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "RECOVERY_CANARY_APPLIED"
    assert payload["raw_dead_letters_unchanged"] is True
    assert payload["auto_side_effect_count_invariant_ok"] is True
    assert payload["target_write_invariant_ok"] is True
    assert payload["no_evaluation_run"] is True
    assert payload["no_order_artifacts_created"] is True
    assert payload["no_order_commands_created"] is True
    assert replay_exit_code == 0
    assert replay["status"] == "REPLAYED"
    assert _raw_dead_letter(db_path) == raw_before
    connection = open_connection(db_path)
    try:
        queue_rows = connection.execute(
            "SELECT candidate_instance_id, attempts FROM incremental_evaluation_queue"
        ).fetchall()
        disposition_actions = [
            row["action"]
            for row in connection.execute(
                "SELECT action FROM incremental_evaluation_dead_letter_dispositions"
            ).fetchall()
        ]
        runtime_lock_count = connection.execute(
            "SELECT COUNT(*) FROM runtime_execution_locks"
        ).fetchone()[0]
    finally:
        connection.close()
    assert [(row["candidate_instance_id"], row["attempts"]) for row in queue_rows] == [
        (CANDIDATE_ID, 0)
    ]
    assert disposition_actions == ["RESET_CANARY"]
    assert runtime_lock_count == 0


def test_reset_canary_stale_cas_reports_guard_write_and_preserves_targets(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "reset-canary-stale.sqlite3"
    raw_before = _seed_active_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    _, candidate_version = _preview_versions(db_path, capsys)

    exit_code = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "RESET_CANARY",
            "--apply",
            "--request-id",
            "request.r2.stale-canary",
            "--expected-fingerprint",
            "0" * 64,
            "--expected-candidate-version",
            candidate_version,
            "--evidence-file",
            str(evidence),
            "--evidence-ref",
            "FAST0R2_STALE_CANARY",
            "--operator-id",
            "operator.alpha",
            "--recovery-session-id",
            "recovery.session.stale",
            "--acknowledge-effective-status-change",
            "--acknowledge-no-auto-evaluation",
            "--confirm-root-cause-fixed",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert payload["mode"] == ("GUARD_WRITE_POSSIBLE_TARGET_STATE_REQUIRES_READBACK")
    assert "RECOVERY_CANDIDATE_NOT_ELIGIBLE" in payload["reason_codes"]
    assert _raw_dead_letter(db_path) == raw_before
    connection = open_connection(db_path)
    try:
        queue_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_queue"
        ).fetchone()[0]
        disposition_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchone()[0]
        lock_count = connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[
            0
        ]
        fence_count = connection.execute(
            "SELECT COUNT(*) FROM runtime_execution_lock_fences"
        ).fetchone()[0]
    finally:
        connection.close()
    assert (queue_count, disposition_count, lock_count) == (0, 0, 0)
    assert fence_count == 1


def test_failed_canary_appends_new_raw_generation_and_cannot_be_verified(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "failed-canary-generation.sqlite3"
    original_raw = _seed_active_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    session_id = "recovery.session.failed-canary"
    preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "RESET_CANARY",
            "--recovery-session-id",
            session_id,
        ]
    )
    preview_payload = json.loads(capsys.readouterr().out)
    assert preview_exit == 0
    reset_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action="RESET_CANARY",
            dead_letter_ids=[DEAD_LETTER_ID],
            previews=[preview_payload["preview"]],
            request_id="request.failed-canary.reset",
            evidence_ref="FAST0R2_FAILED_CANARY_RESET",
            recovery_session_id=session_id,
        )
    )
    reset_payload = json.loads(capsys.readouterr().out)
    assert reset_exit == 0
    assert reset_payload["status"] == "RECOVERY_CANARY_APPLIED"

    connection = open_connection(db_path)
    try:
        failure_status = _mark_queue_error(
            connection,
            CANDIDATE_ID,
            retry_limit=1,
            error_message="CANARY_EVALUATION_FAILED",
        )
        connection.commit()
        raw_rows = connection.execute(
            """
            SELECT * FROM incremental_evaluation_dead_letters
            WHERE candidate_instance_id = ?
            ORDER BY dead_lettered_at, dead_letter_id
            """,
            (CANDIDATE_ID,),
        ).fetchall()
        queue_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_queue"
        ).fetchone()[0]
        original_after = connection.execute(
            "SELECT * FROM incremental_evaluation_dead_letters WHERE dead_letter_id = ?",
            (DEAD_LETTER_ID,),
        ).fetchone()
        _mark_canary_evaluation_complete(connection)
    finally:
        connection.close()

    assert failure_status == "DEAD_LETTER"
    assert len(raw_rows) == 2
    assert queue_count == 0
    assert {row["status"] for row in raw_rows} == {"DEAD_LETTER"}
    assert {row["last_error"] for row in raw_rows} == {
        "LEGACY_RETRY_EXHAUSTED",
        "CANARY_EVALUATION_FAILED",
    }
    assert {key: original_after[key] for key in original_after.keys()} == original_raw

    verify_preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "VERIFY_CANARY",
            "--recovery-session-id",
            session_id,
        ]
    )
    verify_preview_payload = json.loads(capsys.readouterr().out)
    assert verify_preview_exit == 0
    verify_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action="VERIFY_CANARY",
            dead_letter_ids=[DEAD_LETTER_ID],
            previews=[verify_preview_payload["preview"]],
            request_id="request.failed-canary.verify",
            evidence_ref="FAST0R2_FAILED_CANARY_VERIFY",
            recovery_session_id=session_id,
        )
    )
    verify_payload = json.loads(capsys.readouterr().out)

    assert verify_exit == 2
    assert verify_payload["status"] == "REJECTED"
    assert verify_payload["reason_codes"] == ["CANARY_VERIFICATION_NOT_PROVEN"]
    assert (
        "NEWER_DEAD_LETTER_GENERATION_PRESENT"
        in verify_payload["operation_error"]["details"]["reason_codes"]
    )


def test_canary_verification_rejects_order_artifact_created_after_reset(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "canary-order-artifact.sqlite3"
    _seed_active_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    session_id = "recovery.session.order-artifact"
    preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "RESET_CANARY",
            "--recovery-session-id",
            session_id,
        ]
    )
    preview_payload = json.loads(capsys.readouterr().out)
    assert preview_exit == 0
    reset_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action="RESET_CANARY",
            dead_letter_ids=[DEAD_LETTER_ID],
            previews=[preview_payload["preview"]],
            request_id="request.order-artifact.reset",
            evidence_ref="FAST0R2_ORDER_ARTIFACT_RESET",
            recovery_session_id=session_id,
        )
    )
    capsys.readouterr()
    assert reset_exit == 0

    connection = open_connection(db_path)
    try:
        _mark_canary_evaluation_complete(connection)
        connection.execute(
            """
            INSERT INTO dry_run_intents (
                dry_run_intent_id, candidate_instance_id,
                strategy_observation_id, risk_observation_id,
                trade_date, code, name, side, order_type, intended_price,
                quantity, notional, status, reason_codes_json,
                evidence_json, source, observe_only, dry_run_only,
                live_order_allowed, gateway_command_allowed
            ) VALUES (
                'dry-run-intent-after-canary', ?, 'strategy-e2e', 'risk-e2e',
                '2026-07-03', '005930', '삼성전자', 'BUY', 'LIMIT', 1,
                1, 1, 'PLANNED', '[]', '{}', 'test', 1, 1, 0, 0
            )
            """,
            (CANDIDATE_ID,),
        )
        connection.commit()
    finally:
        connection.close()

    verify_preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "VERIFY_CANARY",
            "--recovery-session-id",
            session_id,
        ]
    )
    verify_preview_payload = json.loads(capsys.readouterr().out)
    assert verify_preview_exit == 0
    verify_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action="VERIFY_CANARY",
            dead_letter_ids=[DEAD_LETTER_ID],
            previews=[verify_preview_payload["preview"]],
            request_id="request.order-artifact.verify",
            evidence_ref="FAST0R2_ORDER_ARTIFACT_VERIFY",
            recovery_session_id=session_id,
        )
    )
    verify_payload = json.loads(capsys.readouterr().out)

    assert verify_exit == 2
    assert verify_payload["reason_codes"] == ["CANARY_VERIFICATION_NOT_PROVEN"]
    assert (
        "ORDER_ARTIFACT_STATE_CHANGED_SINCE_CANARY"
        in verify_payload["operation_error"]["details"]["reason_codes"]
    )
    assert _counts(db_path)["order_commands"] == 0


def test_apply_row_guard_blocks_extra_queue_insert(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "blocked-extra-queue.sqlite3"
    _seed_active_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "RESET_CANARY",
            "--recovery-session-id",
            "recovery.session.queue-guard",
        ]
    )
    preview = json.loads(capsys.readouterr().out)
    assert preview_exit == 0
    original = resolution_tool.recover_incremental_evaluation_dead_letters

    def attempt_extra_queue(connection, *args, **kwargs):
        result = original(connection, *args, **kwargs)
        connection.execute(
            """
            INSERT INTO incremental_evaluation_queue (
                candidate_instance_id, trade_date, code, reason,
                source_event_id, priority, enqueued_at, updated_at,
                attempts, last_error
            ) VALUES (
                'CAND-UNAPPROVED-EXTRA', '2026-07-03', '999999',
                'UNAPPROVED_EXTRA', NULL, 0,
                '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z', 0, NULL
            )
            """
        )
        return result

    monkeypatch.setattr(
        resolution_tool,
        "recover_incremental_evaluation_dead_letters",
        attempt_extra_queue,
    )
    exit_code = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "RESET_CANARY",
            "--apply",
            "--request-id",
            "request.r2.queue-guard",
            "--expected-fingerprint",
            preview["preview"]["dead_letter_fingerprint"],
            "--expected-candidate-version",
            preview["preview"]["candidate_version"],
            "--evidence-file",
            str(evidence),
            "--evidence-ref",
            "FAST0R2_QUEUE_GUARD",
            "--operator-id",
            "operator.alpha",
            "--recovery-session-id",
            "recovery.session.queue-guard",
            "--acknowledge-effective-status-change",
            "--acknowledge-no-auto-evaluation",
            "--confirm-root-cause-fixed",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "UNEXPECTED_DATABASE_WRITE_BLOCKED" in payload["reason_codes"]
    connection = open_connection(db_path)
    try:
        queue_ids = [
            row["candidate_instance_id"]
            for row in connection.execute(
                "SELECT candidate_instance_id FROM incremental_evaluation_queue"
            ).fetchall()
        ]
        disposition_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchone()[0]
    finally:
        connection.close()
    assert queue_ids == [CANDIDATE_ID]
    assert disposition_count == 1


def test_cli_canary_verify_then_bounded_batch_recovery_end_to_end(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "recovery-e2e.sqlite3"
    _seed_active_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    session_id = "recovery.session.e2e"

    canary_preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "RESET_CANARY",
            "--recovery-session-id",
            session_id,
        ]
    )
    canary_preview_payload = json.loads(capsys.readouterr().out)
    assert canary_preview_exit == 0
    canary_preview = canary_preview_payload["preview"]
    canary_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action="RESET_CANARY",
            dead_letter_ids=[DEAD_LETTER_ID],
            previews=[canary_preview],
            request_id="request.e2e.canary",
            evidence_ref="FAST0R2_E2E_CANARY",
            recovery_session_id=session_id,
        )
    )
    canary = json.loads(capsys.readouterr().out)

    connection = open_connection(db_path)
    try:
        _mark_canary_evaluation_complete(connection)
    finally:
        connection.close()

    verify_preview_exit = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--action",
            "VERIFY_CANARY",
            "--recovery-session-id",
            session_id,
        ]
    )
    verify_preview_payload = json.loads(capsys.readouterr().out)
    assert verify_preview_exit == 0
    verify_preview = verify_preview_payload["preview"]
    verify_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action="VERIFY_CANARY",
            dead_letter_ids=[DEAD_LETTER_ID],
            previews=[verify_preview],
            request_id="request.e2e.verify",
            evidence_ref="FAST0R2_E2E_VERIFY",
            recovery_session_id=session_id,
        )
    )
    verified = json.loads(capsys.readouterr().out)
    verified_disposition_id = str(verified["result"]["disposition_id"])

    batch_dead_letter_ids = ["incremental-dead-letter-2", "incremental-dead-letter-3"]
    connection = open_connection(db_path)
    try:
        _insert_additional_active_dead_letter(
            connection,
            candidate_id="CAND-2026-07-03-000660-1",
            dead_letter_id=batch_dead_letter_ids[0],
            source_event_id="source-event-e2e-2",
            code="000660",
        )
        _insert_additional_active_dead_letter(
            connection,
            candidate_id="CAND-2026-07-03-035420-1",
            dead_letter_id=batch_dead_letter_ids[1],
            source_event_id="source-event-e2e-3",
            code="035420",
        )
    finally:
        connection.close()

    batch_preview_args = [
        "--db",
        str(db_path),
        "--action",
        ACTION_RESET_BATCH,
        "--recovery-session-id",
        session_id,
        "--verify-canary-disposition-id",
        verified_disposition_id,
    ]
    for dead_letter_id in batch_dead_letter_ids:
        batch_preview_args.extend(["--dead-letter-id", dead_letter_id])
    batch_preview_exit = main(batch_preview_args)
    batch_preview_payload = json.loads(capsys.readouterr().out)
    assert batch_preview_exit == 0
    assert all(item["eligible"] for item in batch_preview_payload["previews"])

    batch_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action=ACTION_RESET_BATCH,
            dead_letter_ids=batch_dead_letter_ids,
            previews=batch_preview_payload["previews"],
            request_id="request.e2e.batch",
            evidence_ref="FAST0R2_E2E_BATCH",
            recovery_session_id=session_id,
            verified_canary_disposition_id=verified_disposition_id,
        )
    )
    batch = json.loads(capsys.readouterr().out)

    assert canary_exit == 0
    assert canary["status"] == "RECOVERY_CANARY_APPLIED"
    assert canary["target_write_invariant_ok"] is True
    assert verify_exit == 0
    assert verified["status"] == "RECOVERY_CANARY_VERIFIED"
    assert verified["target_write_invariant_ok"] is True
    assert batch_exit == 0
    assert batch["status"] == "RECOVERY_BATCH_APPLIED"
    assert batch["target_write_invariant_ok"] is True
    assert len(batch["result"]["items"]) == 2
    assert batch["no_evaluation_run"] is True
    assert batch["no_order_artifacts_created"] is True
    assert batch["no_order_commands_created"] is True

    connection = open_connection(db_path)
    try:
        actions = [
            row["action"]
            for row in connection.execute(
                """
                SELECT action
                FROM incremental_evaluation_dead_letter_dispositions
                ORDER BY created_at, disposition_id
                """
            ).fetchall()
        ]
        queue_ids = {
            row["candidate_instance_id"]
            for row in connection.execute(
                "SELECT candidate_instance_id FROM incremental_evaluation_queue"
            ).fetchall()
        }
        raw_statuses = [
            row["status"]
            for row in connection.execute(
                "SELECT status FROM incremental_evaluation_dead_letters"
            ).fetchall()
        ]
        lock_count = connection.execute("SELECT COUNT(*) FROM runtime_execution_locks").fetchone()[
            0
        ]
        fence_token = connection.execute(
            """
            SELECT last_fencing_token FROM runtime_execution_lock_fences
            WHERE lock_name = 'evaluation_pipeline'
            """
        ).fetchone()[0]
    finally:
        connection.close()
    assert actions.count("RESET_CANARY") == 1
    assert actions.count("VERIFY_CANARY") == 1
    assert actions.count("RESET_BATCH") == 2
    assert queue_ids == {
        "CAND-2026-07-03-000660-1",
        "CAND-2026-07-03-035420-1",
    }
    assert raw_statuses == ["DEAD_LETTER", "DEAD_LETTER", "DEAD_LETTER"]
    assert lock_count == 0
    assert fence_token == 3

    changed_authorization_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action=ACTION_RESET_BATCH,
            dead_letter_ids=batch_dead_letter_ids,
            previews=batch_preview_payload["previews"],
            request_id="request.e2e.batch",
            evidence_ref="FAST0R2_E2E_BATCH",
            recovery_session_id=session_id,
            verified_canary_disposition_id="incremental_disposition_fake",
        )
    )
    changed_authorization = json.loads(capsys.readouterr().out)
    assert changed_authorization_exit == 2
    assert changed_authorization["reason_codes"] == ["REQUEST_CONFLICT"]

    connection = open_connection(db_path)
    try:
        resolution_tool.revoke_incremental_evaluation_dead_letter_disposition(
            connection,
            DEAD_LETTER_ID,
            request_id="request.e2e.revoke-verified-canary",
            expected_dead_letter_fingerprint=str(verify_preview["dead_letter_fingerprint"]),
            expected_candidate_version=str(verify_preview["candidate_version"]),
            supersedes_disposition_id=verified_disposition_id,
            reason_code="CANARY_AUTHORIZATION_REVOKED",
            operator_id="operator.e2e",
            evidence_ref="FAST0R2_E2E_REVOKE_CANARY",
            evidence_sha256="b" * 64,
        )
    finally:
        connection.close()

    revoked_canary_batch_exit = main(
        _recovery_apply_args(
            db_path,
            evidence,
            action=ACTION_RESET_BATCH,
            dead_letter_ids=batch_dead_letter_ids,
            previews=batch_preview_payload["previews"],
            request_id="request.e2e.batch-after-revoke",
            evidence_ref="FAST0R2_E2E_BATCH_AFTER_REVOKE",
            recovery_session_id=session_id,
            verified_canary_disposition_id=verified_disposition_id,
        )
    )
    revoked_canary_batch = json.loads(capsys.readouterr().out)
    assert revoked_canary_batch_exit == 2
    assert revoked_canary_batch["reason_codes"] == ["VERIFIED_CANARY_NOT_EFFECTIVE"]

    connection = open_connection(db_path)
    try:
        final_disposition_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchone()[0]
        final_queue_count = connection.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_queue"
        ).fetchone()[0]
        final_lock_count = connection.execute(
            "SELECT COUNT(*) FROM runtime_execution_locks"
        ).fetchone()[0]
        final_fence_token = connection.execute(
            """
            SELECT last_fencing_token FROM runtime_execution_lock_fences
            WHERE lock_name = 'evaluation_pipeline'
            """
        ).fetchone()[0]
    finally:
        connection.close()
    assert final_disposition_count == 5
    assert final_queue_count == 2
    assert final_lock_count == 0
    assert final_fence_token == 5


def test_apply_rejects_missing_explicit_environment_without_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "unsafe-env.sqlite3"
    _seed_closed_dead_letter(db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(tmp_path / "missing.env"))
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)

    exit_code = main(
        _disposition_args(
            db_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "TRADING_ENV_FILE_NOT_FOUND" in payload["reason_codes"]
    assert _counts(db_path)["dispositions"] == 0


def test_apply_rejects_incremental_worker_or_theme_command_producer(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "unsafe-producers.sqlite3"
    _seed_closed_dead_letter(db_path)
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)

    worker_env = _write_safe_env(tmp_path, db_path, name="worker.env")
    worker_env.write_text(
        worker_env.read_text(encoding="utf-8").replace(
            "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
            "INCREMENTAL_EVALUATION_WORKER_ENABLED=true",
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADING_ENV_FILE", str(worker_env))
    worker_exit = main(
        _disposition_args(
            db_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    worker_payload = json.loads(capsys.readouterr().out)

    theme_env = _write_safe_env(tmp_path, db_path, name="theme.env")
    theme_env.write_text(
        theme_env.read_text(encoding="utf-8").replace(
            "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
            "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=true",
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADING_ENV_FILE", str(theme_env))
    theme_exit = main(
        _disposition_args(
            db_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    theme_payload = json.loads(capsys.readouterr().out)

    assert worker_exit == 2
    assert "INCREMENTAL_EVALUATION_WORKER_ENABLED" in worker_payload["reason_codes"]
    assert theme_exit == 2
    assert (
        "COMMAND_PRODUCER_ENABLED:THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS"
        in theme_payload["reason_codes"]
    )
    assert "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS_NOT_FALSE" in theme_payload["reason_codes"]
    assert _counts(db_path)["dispositions"] == 0


def test_apply_rejects_active_runtime_execution_lease(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "active-lease.sqlite3"
    _seed_closed_dead_letter(db_path)
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)
    connection = open_connection(db_path)
    connection.execute(
        """
        INSERT INTO runtime_execution_locks (
            lock_name, owner_id, acquired_at, expires_at, detail_json
        )
        VALUES (
            'evaluation_pipeline', 'another-owner',
            '2026-07-15T00:00:00Z', '2099-01-01T00:00:00Z', '{}'
        )
        """
    )
    connection.commit()
    connection.close()
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))

    exit_code = main(
        _disposition_args(
            db_path,
            evidence,
            fingerprint=fingerprint,
            candidate_version=candidate_version,
        )
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert "RUNTIME_EXECUTION_LEASE_PRESENT" in payload["reason_codes"]
    assert _counts(db_path)["dispositions"] == 0


def test_reset_batch_rejects_six_rows_before_opening_database(capsys) -> None:
    ids = [f"dead-letter-{index}" for index in range(6)]
    args = [
        "--db",
        "does-not-exist.sqlite3",
        "--action",
        ACTION_RESET_BATCH,
        "--apply",
        "--request-id",
        "request.r2.batch-alpha",
        "--evidence-file",
        "does-not-exist-evidence.json",
        "--evidence-ref",
        "FAST0R2_BATCH_ALPHA",
        "--operator-id",
        "operator.alpha",
        "--recovery-session-id",
        "recovery.session.alpha",
        "--verify-canary-disposition-id",
        "disposition-canary-alpha",
        "--acknowledge-effective-status-change",
        "--acknowledge-no-auto-evaluation",
        "--confirm-root-cause-fixed",
    ]
    for index, dead_letter_id in enumerate(ids):
        args.extend(
            [
                "--dead-letter-id",
                dead_letter_id,
                "--expected-fingerprint",
                f"{index + 1:x}" * 64,
                "--expected-candidate-version",
                f"{index + 7:x}" * 64,
            ]
        )

    exit_code = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "RECOVERY_BATCH_LIMIT_EXCEEDED" in payload["reason_codes"]
    assert "DATABASE_NOT_FOUND" not in payload["reason_codes"]


def test_apply_rejects_non_hash_candidate_version_before_opening_database(
    capsys,
) -> None:
    exit_code = main(
        [
            "--db",
            "does-not-exist.sqlite3",
            "--dead-letter-id",
            DEAD_LETTER_ID,
            "--apply",
            "--request-id",
            "request.r2.invalid-version",
            "--expected-fingerprint",
            "a" * 64,
            "--expected-candidate-version",
            "not-a-sha256-version",
            "--evidence-file",
            "does-not-exist-evidence.json",
            "--evidence-ref",
            "FAST0R2_INVALID_VERSION",
            "--operator-id",
            "operator.alpha",
            "--acknowledge-effective-status-change",
            "--acknowledge-no-auto-evaluation",
            "--acknowledge-raw-audit-preserved",
            "--confirm-terminal-evidence-reviewed",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "EXPECTED_CANDIDATE_VERSION_INVALID" in payload["reason_codes"]
    assert "DATABASE_NOT_FOUND" not in payload["reason_codes"]


def test_preview_rejects_misaligned_cas_values_before_opening_database(capsys) -> None:
    exit_code = main(
        [
            "--db",
            "does-not-exist.sqlite3",
            "--action",
            ACTION_RESET_BATCH,
            "--dead-letter-id",
            "dead-letter-one",
            "--dead-letter-id",
            "dead-letter-two",
            "--expected-fingerprint",
            "a" * 64,
            "--expected-candidate-version",
            "b" * 64,
            "--expected-candidate-version",
            "c" * 64,
            "--recovery-session-id",
            "recovery.session.alpha",
            "--verify-canary-disposition-id",
            "disposition-canary-alpha",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "EXPECTED_FINGERPRINT_COUNT_MISMATCH" in payload["reason_codes"]
    assert "DATABASE_NOT_FOUND" not in payload["reason_codes"]


def test_account_like_operator_label_is_rejected_before_write(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "unsafe-label.sqlite3"
    _seed_closed_dead_letter(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = _write_evidence(tmp_path)
    fingerprint, candidate_version = _preview_versions(db_path, capsys)
    args = _disposition_args(
        db_path,
        evidence,
        fingerprint=fingerprint,
        candidate_version=candidate_version,
    )
    operator_index = args.index("--operator-id") + 1
    args[operator_index] = "operator.9876-5432"

    exit_code = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert "OPERATOR_ID_INVALID" in payload["reason_codes"]
    assert _counts(db_path)["dispositions"] == 0


def test_sanitize_redacts_paths_secrets_and_labeled_account_values() -> None:
    rendered = json.dumps(
        resolution_tool._sanitize(
            {
                "message": (
                    r"failed C:\private\evidence.json "
                    "token=super-secret-token account=9876-5432 "
                    "access_token=access-token-value client_secret=client-secret-value "
                    "raw-account 1234-5678 plain-account 12345678 "
                    "refreshToken=refresh-token-value clientSecret=camel-secret-value "
                    "Authorization: Digest username=alice, response=secret-response"
                ),
                "raw_json": ('{"token":"nested-secret-token","account_id":"9876-5432"}'),
                "bearer": "Authorization: Bearer abcdefghijklmnop",
                "embedded": (
                    'failed payload={"token":"embedded-secret-token",'
                    '"account_id":"9876-5432","password":"hunter2",'
                    '"credential":"embedded-credential"}'
                ),
                "embedded_numeric": ('failed payload={"account_id":98765432,"token":123456789}'),
                "embedded_escaped_quote": (
                    'failed payload={"token":"abc\\"remaining-secret","account_id":"98765432"}'
                ),
                "api_key": "opaque-api-key-value",
                "authorization": "opaque-authorization-value",
                "password": "opaque-password-value",
                "credential": "opaque-credential-value",
            }
        ),
        ensure_ascii=False,
    )

    assert r"C:\private\evidence.json" not in rendered
    assert "super-secret-token" not in rendered
    assert "nested-secret-token" not in rendered
    assert "abcdefghijklmnop" not in rendered
    assert "embedded-secret-token" not in rendered
    assert "hunter2" not in rendered
    assert "embedded-credential" not in rendered
    assert "123456789" not in rendered
    assert "remaining-secret" not in rendered
    assert "access-token-value" not in rendered
    assert "client-secret-value" not in rendered
    assert "1234-5678" not in rendered
    assert "12345678" not in rendered
    assert "refresh-token-value" not in rendered
    assert "camel-secret-value" not in rendered
    assert "secret-response" not in rendered
    assert "opaque-api-key-value" not in rendered
    assert "opaque-authorization-value" not in rendered
    assert "opaque-password-value" not in rendered
    assert "opaque-credential-value" not in rendered
    assert "9876-5432" not in rendered
    assert "REDACTED_PATH" in rendered
    assert "REDACTED_SECRET" in rendered
    assert "REDACTED_ACCOUNT" in rendered


def test_apply_authorizer_allows_only_exact_main_database_writes() -> None:
    blocked: list[str] = []
    authorize = resolution_tool._build_apply_write_authorizer(
        action="RESET_CANARY",
        blocked_write_tables=blocked,
    )

    assert (
        authorize(
            sqlite3.SQLITE_INSERT,
            "incremental_evaluation_queue",
            None,
            "main",
            None,
        )
        == sqlite3.SQLITE_OK
    )
    assert (
        authorize(
            sqlite3.SQLITE_UPDATE,
            "incremental_evaluation_queue",
            None,
            "main",
            None,
        )
        == sqlite3.SQLITE_DENY
    )
    assert (
        authorize(
            sqlite3.SQLITE_UPDATE,
            "incremental_evaluation_dead_letter_dispositions",
            None,
            "main",
            None,
        )
        == sqlite3.SQLITE_DENY
    )
    assert (
        authorize(
            sqlite3.SQLITE_INSERT,
            "incremental_evaluation_queue",
            None,
            "temp",
            None,
        )
        == sqlite3.SQLITE_DENY
    )
    assert (
        authorize(sqlite3.SQLITE_ATTACH, "private.sqlite3", None, None, None) == sqlite3.SQLITE_DENY
    )
    assert (
        authorize(sqlite3.SQLITE_PRAGMA, "journal_mode", "WAL", None, None) == sqlite3.SQLITE_DENY
    )
    assert (
        authorize(
            sqlite3.SQLITE_PRAGMA,
            "table_info",
            "incremental_evaluation_dead_letter_dispositions",
            None,
            None,
        )
        == sqlite3.SQLITE_OK
    )
    assert "incremental_evaluation_queue" in blocked
    assert "temp.incremental_evaluation_queue" in blocked
    assert "ATTACH" in blocked
    assert "PRAGMA:journal_mode" in blocked


def _seed_active_dead_letter(db_path: Path) -> dict[str, object]:
    _seed_closed_dead_letter(db_path)
    connection = open_connection(db_path)
    now = datetime_to_wire(utc_now())
    try:
        connection.execute(
            """
            UPDATE candidates
            SET state = ?, closed_at = NULL, active_source_count = 1,
                state_updated_at = ?, last_seen_at = ?
            WHERE candidate_instance_id = ?
            """,
            (CandidateState.CONTEXT_READY.value, now, now, CANDIDATE_ID),
        )
        connection.execute(
            """
            INSERT INTO candidate_source_events (
                source_event_id, candidate_instance_id, trade_date, code, name,
                source_type, source_id, action, event_ts, observed_at, active,
                reason_codes_json, payload_json
            )
            VALUES (?, ?, '2026-07-03', '005930', '삼성전자',
                    'THEME_LEADER', 'theme-005930', 'UPSERT', ?, ?, 1, '[]', '{}')
            """,
            (SOURCE_EVENT_ID, CANDIDATE_ID, now, now),
        )
        connection.commit()
        row = connection.execute(
            "SELECT * FROM incremental_evaluation_dead_letters WHERE dead_letter_id = ?",
            (DEAD_LETTER_ID,),
        ).fetchone()
        return {key: row[key] for key in row.keys()}
    finally:
        connection.close()


def _seed_closed_dead_letter(db_path: Path) -> dict[str, object]:
    connection = initialize_database(db_path)
    now = datetime_to_wire(utc_now())
    _insert_strategy_fixture(
        connection,
        candidate_id=CANDIDATE_ID,
        trade_date="2026-07-03",
        code="005930",
        state=CandidateState.CLOSED.value,
    )
    connection.execute(
        """
        UPDATE candidates
        SET active_source_count = 0,
            state_updated_at = ?,
            closed_at = ?
        WHERE candidate_instance_id = ?
        """,
        (now, now, CANDIDATE_ID),
    )
    connection.execute(
        """
        INSERT INTO candidate_state_transitions (
            transition_id, candidate_instance_id, trade_date, code,
            from_state, to_state, event_type, source_event_id,
            reason_codes_json, transitioned_at, metadata_json
        )
        VALUES (
            'transition-closed-alpha', ?, '2026-07-03', '005930',
            'STALE', 'CLOSED', 'CANDIDATE_CLOSED', ?, '[]', ?, '{}'
        )
        """,
        (CANDIDATE_ID, SOURCE_EVENT_ID, now),
    )
    connection.execute(
        """
        INSERT INTO incremental_evaluation_dead_letters (
            dead_letter_id, candidate_instance_id, trade_date, code,
            reason, source_event_id, priority, original_enqueued_at,
            last_queue_updated_at, attempts, last_error, status,
            dead_lettered_at, reset_evidence_json
        )
        VALUES (
            ?, ?, '2026-07-03', '005930', 'PRICE_TICK', ?, 100,
            ?, ?, 3, 'LEGACY_RETRY_EXHAUSTED', 'DEAD_LETTER', ?, '{}'
        )
        """,
        (DEAD_LETTER_ID, CANDIDATE_ID, SOURCE_EVENT_ID, now, now, now),
    )
    connection.commit()
    row = connection.execute(
        "SELECT * FROM incremental_evaluation_dead_letters WHERE dead_letter_id = ?",
        (DEAD_LETTER_ID,),
    ).fetchone()
    result = {key: row[key] for key in row.keys()}
    connection.close()
    return result


def _insert_additional_active_dead_letter(
    connection: sqlite3.Connection,
    *,
    candidate_id: str,
    dead_letter_id: str,
    source_event_id: str,
    code: str,
) -> None:
    now = datetime_to_wire(utc_now())
    _insert_strategy_fixture(
        connection,
        candidate_id=candidate_id,
        trade_date="2026-07-03",
        code=code,
        state=CandidateState.CONTEXT_READY.value,
    )
    connection.execute(
        """
        INSERT INTO candidate_source_events (
            source_event_id, candidate_instance_id, trade_date, code, name,
            source_type, source_id, action, event_ts, observed_at, active,
            reason_codes_json, payload_json
        ) VALUES (?, ?, '2026-07-03', ?, ?, 'THEME_LEADER', ?, 'UPSERT',
                  ?, ?, 1, '[]', '{}')
        """,
        (source_event_id, candidate_id, code, f"name-{code}", f"theme-{code}", now, now),
    )
    connection.execute(
        """
        INSERT INTO incremental_evaluation_dead_letters (
            dead_letter_id, candidate_instance_id, trade_date, code,
            reason, source_event_id, priority, original_enqueued_at,
            last_queue_updated_at, attempts, last_error, status,
            dead_lettered_at, reset_evidence_json
        ) VALUES (?, ?, '2026-07-03', ?, 'PRICE_TICK', ?, 100,
                  ?, ?, 3, 'RETRY_EXHAUSTED', 'DEAD_LETTER', ?, '{}')
        """,
        (dead_letter_id, candidate_id, code, source_event_id, now, now, now),
    )
    connection.commit()


def _mark_canary_evaluation_complete(connection: sqlite3.Connection) -> None:
    evaluated_at = "2099-07-15T00:00:00Z"
    connection.execute(
        "DELETE FROM incremental_evaluation_queue WHERE candidate_instance_id = ?",
        (CANDIDATE_ID,),
    )
    connection.execute(
        """
        INSERT INTO strategy_observations_latest (
            candidate_instance_id, strategy_observation_id, trade_date,
            code, name, evaluated_at, overall_status, score, confidence,
            reason_codes_json, config_version, observe_only
        ) VALUES (?, 'strategy-e2e', '2026-07-03', '005930', '삼성전자', ?,
                  'OBSERVED', 0, 0, '[]', 'e2e', 1)
        """,
        (CANDIDATE_ID, evaluated_at),
    )
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id, risk_observation_id,
            strategy_observation_id, trade_date, code, name, evaluated_at,
            overall_status, max_severity, blocked_count, caution_count,
            pass_count, reason_codes_json, config_version, observe_only
        ) VALUES (?, 'risk-e2e', 'strategy-e2e', '2026-07-03', '005930',
                  '삼성전자', ?, 'PASS', 'NONE', 0, 0, 1, '[]', 'e2e', 1)
        """,
        (CANDIDATE_ID, evaluated_at),
    )
    connection.commit()


def _recovery_apply_args(
    db_path: Path,
    evidence: Path,
    *,
    action: str,
    dead_letter_ids: list[str],
    previews: list[dict[str, object]],
    request_id: str,
    evidence_ref: str,
    recovery_session_id: str,
    verified_canary_disposition_id: str | None = None,
) -> list[str]:
    args = [
        "--db",
        str(db_path),
        "--action",
        action,
        "--apply",
        "--request-id",
        request_id,
        "--evidence-file",
        str(evidence),
        "--evidence-ref",
        evidence_ref,
        "--operator-id",
        "operator.e2e",
        "--recovery-session-id",
        recovery_session_id,
        "--acknowledge-effective-status-change",
        "--acknowledge-no-auto-evaluation",
    ]
    for dead_letter_id in dead_letter_ids:
        args.extend(["--dead-letter-id", dead_letter_id])
    for preview in previews:
        args.extend(
            [
                "--expected-fingerprint",
                str(preview["dead_letter_fingerprint"]),
                "--expected-candidate-version",
                str(preview["candidate_version"]),
            ]
        )
    if verified_canary_disposition_id is not None:
        args.extend(["--verify-canary-disposition-id", verified_canary_disposition_id])
    args.append(
        "--confirm-canary-verified" if action == "VERIFY_CANARY" else "--confirm-root-cause-fixed"
    )
    return args


def _preview_versions(db_path: Path, capsys) -> tuple[str, str]:
    exit_code = main(
        [
            "--db",
            str(db_path),
            "--dead-letter-id",
            DEAD_LETTER_ID,
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    preview = payload["preview"]
    fingerprint = preview.get("dead_letter_fingerprint") or preview.get(
        "source_dead_letter_fingerprint"
    )
    candidate_version = preview.get("candidate_version") or preview.get("source_candidate_version")
    assert isinstance(fingerprint, str) and len(fingerprint) == 64
    assert isinstance(candidate_version, str) and len(candidate_version) == 64
    return fingerprint, candidate_version


def _disposition_args(
    db_path: Path,
    evidence: Path,
    *,
    fingerprint: str,
    candidate_version: str,
) -> list[str]:
    return [
        "--db",
        str(db_path),
        "--dead-letter-id",
        DEAD_LETTER_ID,
        "--apply",
        "--request-id",
        "request.r2.disposition-alpha",
        "--expected-fingerprint",
        fingerprint,
        "--expected-candidate-version",
        candidate_version,
        "--evidence-file",
        str(evidence),
        "--evidence-ref",
        "FAST0R2_RCA_ALPHA",
        "--operator-id",
        "operator.alpha",
        "--acknowledge-effective-status-change",
        "--acknowledge-no-auto-evaluation",
        "--acknowledge-raw-audit-preserved",
        "--confirm-terminal-evidence-reviewed",
    ]


def _write_evidence(tmp_path: Path) -> Path:
    path = tmp_path / "evidence.json"
    path.write_text('{"classification":"obsolete"}', encoding="utf-8")
    return path


def _write_safe_env(tmp_path: Path, db_path: Path, *, name: str = "safe.env") -> Path:
    env_path = tmp_path / name
    env_path.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "LIVE_SIM_ENABLED=false",
                "LIVE_SIM_ORDER_ROUTING_ENABLED=false",
                "LIVE_SIM_GATEWAY_COMMAND_ENABLED=false",
                "LIVE_SIM_REPRICE_ENABLED=false",
                "LIVE_SIM_PILOT_PIPELINE_ENABLED=false",
                "LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=false",
                "LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=false",
                "LIVE_SIM_CANCEL_ENABLED=false",
                "LIVE_SIM_CANCEL_UNFILLED_ENABLED=false",
                "LIVE_SIM_EXIT_ENGINE_ENABLED=false",
                "LIVE_SIM_EXIT_ORDER_CREATION_ENABLED=false",
                "LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED=false",
                "LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED=false",
                "LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=false",
                "LIVE_SIM_OPERATING_CYCLE_ENABLED=false",
                "LIVE_SIM_OPERATING_LOOP_ENABLED=false",
                "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS=false",
                "REALTIME_SUBSCRIPTION_QUEUE_COMMANDS=false",
                "DRY_RUN_OMS_ENABLED=false",
                "DRY_RUN_INTENT_CREATION_ENABLED=false",
                "DRY_RUN_ORDER_ROUTING_ENABLED=false",
                "DRY_RUN_GATEWAY_COMMAND_ENABLED=false",
                "DRY_RUN_EXIT_ENGINE_ENABLED=false",
                "DRY_RUN_EXIT_INTENT_CREATION_ENABLED=false",
                "DRY_RUN_EXIT_ORDER_CREATION_ENABLED=false",
                "DRY_RUN_EXIT_ORDER_ROUTING_ENABLED=false",
                "DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                "LIVE_SIM_KILL_SWITCH=true",
            ]
        ),
        encoding="utf-8",
    )
    return env_path


def _raw_dead_letter(db_path: Path) -> dict[str, object]:
    connection = open_connection(db_path)
    try:
        row = connection.execute(
            "SELECT * FROM incremental_evaluation_dead_letters WHERE dead_letter_id = ?",
            (DEAD_LETTER_ID,),
        ).fetchone()
        return {key: row[key] for key in row.keys()}
    finally:
        connection.close()


def _counts(db_path: Path) -> dict[str, int]:
    connection = open_connection(db_path)
    try:
        return {
            "raw_dead_letters": int(
                connection.execute(
                    "SELECT COUNT(*) FROM incremental_evaluation_dead_letters"
                ).fetchone()[0]
            ),
            "dispositions": int(
                connection.execute(
                    "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
                ).fetchone()[0]
            ),
            "queued": int(
                connection.execute("SELECT COUNT(*) FROM incremental_evaluation_queue").fetchone()[
                    0
                ]
            ),
            "commands": int(
                connection.execute("SELECT COUNT(*) FROM gateway_commands").fetchone()[0]
            ),
            "order_commands": int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM gateway_commands
                    WHERE lower(command_type) IN (
                        'send_order', 'cancel_order', 'modify_order'
                    )
                    """
                ).fetchone()[0]
            ),
        }
    finally:
        connection.close()


def _database_contract(db_path: Path) -> dict[str, object]:
    return {
        suffix: _file_contract(Path(f"{db_path}{suffix}"))
        for suffix in ("", "-wal", "-shm", "-journal")
    }


def _file_contract(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"exists": False}
    stat_result = path.stat()
    return {
        "exists": True,
        "size": int(stat_result.st_size),
        "mtime_ns": int(stat_result.st_mtime_ns),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
