from __future__ import annotations

import hashlib

import pytest
from tests.test_pipeline_coherency_disposition import _expired_closed_pipeline
from tools import resolve_pipeline_coherency as tool


def test_pipeline_disposition_cli_preview_is_strict_read_only(tmp_path) -> None:
    db_path = tmp_path / "preview.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    connection.close()

    report = tool.preview_disposition(
        db_path=db_path,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        out_dir=tmp_path / "preview-evidence",
    )

    assert report["verdict"]["status"] == "ELIGIBLE"
    assert report["verdict"]["database_files_unchanged"] is True
    assert report["preview"]["eligible"] is True
    assert report["read_only"] is True
    assert report["no_order_side_effects"] is True


def test_pipeline_disposition_cli_apply_is_one_row_and_idempotent(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "apply.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    direct_preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    entry_count_before = connection.execute(
        "SELECT COUNT(*) FROM entry_timing_evaluations"
    ).fetchone()[0]
    command_count_before = connection.execute("SELECT COUNT(*) FROM gateway_commands").fetchone()[0]
    connection.close()

    safe_env = tmp_path / "pipeline-safe.env"
    producer_lines = [f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES]
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *producer_lines,
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))

    kwargs = {
        "db_path": db_path,
        "trade_date": trade_date,
        "candidate_instance_id": candidate_id,
        "action": tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        "request_id": "fast0r3.pipeline.request-one",
        "expected_pipeline_fingerprint": direct_preview["pipeline_fingerprint"],
        "expected_subject_version": direct_preview["subject_version"],
        "expected_source_fingerprint": direct_preview["source_fingerprint"],
        "expected_candidate_fingerprint": direct_preview["candidate_fingerprint"],
        "expected_downstream_fingerprint": direct_preview["downstream_fingerprint"],
        "expected_boundary_fingerprint": direct_preview["boundary_fingerprint"],
        "reason_code": "FAST0R3_EVIDENCED_HISTORICAL_PIPELINE",
        "operator_id": "fast0.operator",
        "evidence_type": "FAST0R3_PIPELINE_RCA",
        "evidence_ref": "fast0r3/pipeline-rca-redacted",
        "evidence_file": evidence_file,
        "acknowledge": tool.APPLY_ACK,
    }
    first = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "apply-evidence-one",
    )
    second = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "apply-evidence-two",
    )

    write_report = tool._write_report

    def fail_evidence_write(*_args, **_kwargs):
        raise OSError("injected evidence volume failure")

    monkeypatch.setattr(tool, "_write_report", fail_evidence_write)
    evidence_failed = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "apply-evidence-failure",
    )

    check = tool._open_strict_read_only(db_path)
    try:
        ledger_count = check.execute(
            "SELECT COUNT(*) FROM pipeline_coherency_dispositions"
        ).fetchone()[0]
        entry_count_after = check.execute(
            "SELECT COUNT(*) FROM entry_timing_evaluations"
        ).fetchone()[0]
        command_count_after = check.execute("SELECT COUNT(*) FROM gateway_commands").fetchone()[0]
        safety_json = check.execute(
            "SELECT safety_snapshot_json FROM pipeline_coherency_dispositions"
        ).fetchone()[0]
    finally:
        check.close()

    assert first["verdict"]["status"] == "APPLIED"
    assert second["verdict"]["status"] == "IDEMPOTENT"
    assert evidence_failed["verdict"] == {
        "status": "COMMITTED_EVIDENCE_WRITE_FAILED",
        "failures": ["EVIDENCE_WRITE_FAILED"],
        "committed": True,
        "evidence_written": False,
        "operator_action_required": True,
        "retry_with_same_request_id": "fast0r3.pipeline.request-one",
        "error_type": "OSError",
    }
    assert ledger_count == 1
    assert entry_count_after == entry_count_before
    assert command_count_after == command_count_before
    assert hashlib.sha256(safe_env.read_bytes()).hexdigest() in safety_json

    monkeypatch.setattr(tool, "_write_report", write_report)
    write = tool._open_existing_read_write(db_path)
    plan = write.execute(
        "SELECT * FROM order_plan_drafts WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()
    write.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id, candidate_instance_id,
            strategy_observation_id, risk_observation_id, order_plan_id,
            trade_date, account_id, code, name, side, order_type, quantity,
            limit_price, notional, status, idempotency_key,
            live_sim_only, live_real_allowed, broker_order_sent,
            created_at, expires_at
        ) VALUES (
            'late-cli-intent', ?, ?, ?, ?, ?, 'SIM_REDACTED', ?, ?, 'BUY',
            'LIMIT', 1, 1, 1, 'CREATED', 'late-cli-intent-key', 1, 0, 0,
            '2026-07-15T00:00:00Z', '2026-07-15T00:10:00Z'
        )
        """,
        (
            candidate_id,
            plan["strategy_observation_id"],
            plan["risk_observation_id"],
            plan["order_plan_id"],
            trade_date,
            plan["code"],
            plan["name"],
        ),
    )
    write.close()

    with pytest.raises(tool.PipelineCoherencyDispositionError) as exc_info:
        tool.apply_disposition(
            **kwargs,
            out_dir=tmp_path / "apply-evidence-stale-idempotent",
        )
    assert exc_info.value.code == "PIPELINE_DISPOSITION_IDEMPOTENT_STALE"


@pytest.mark.parametrize(
    "value",
    (
        "ghp_" + "a" * 36,
        "github_pat_" + "b" * 30,
        "acct9876-5432",
        "account:98765432",
    ),
)
def test_pipeline_disposition_evidence_rejects_and_redacts_sensitive_values(value) -> None:
    with pytest.raises(tool.PipelineDispositionCliError, match="UNSAFE_LABEL"):
        tool._safe_label("evidence_ref", value)

    redacted = tool._redact(
        {
            "evidence_ref": value,
            "candidate_instance_id": value,
            "nested": [value],
        }
    )
    summary = tool.render_markdown_summary(
        {
            "generated_at": "2026-07-15T00:00:00Z",
            "mode": "PREVIEW",
            "preview": {"candidate_instance_id": value, "action": "REVOKE"},
            "verdict": {"status": "BLOCKED", "failures": []},
        }
    )

    assert value not in str(redacted)
    assert value not in summary
    assert "[REDACTED]" in str(redacted)
    assert "[REDACTED]" in summary


def test_pipeline_disposition_redaction_preserves_iso_dates() -> None:
    label = "fast0/2026-07-15/evidence"
    payload = {
        "trade_date": "2026-07-15",
        "subject_key": "2026-07-15:candidate-safe",
    }

    assert tool._safe_label("evidence_ref", label) == label
    assert tool._redact(payload) == payload


def test_pipeline_disposition_cli_apply_requires_explicit_safe_env(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "unsafe.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    evidence_file = tmp_path / "evidence.json"
    evidence_file.write_text("{}", encoding="utf-8")
    monkeypatch.delenv("TRADING_ENV_FILE", raising=False)

    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="EXPLICIT_TRADING_ENV_FILE_REQUIRED",
    ):
        tool.apply_disposition(
            db_path=db_path,
            trade_date=trade_date,
            candidate_instance_id=candidate_id,
            action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
            request_id="fast0r3.pipeline.unsafe",
            expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
            expected_subject_version=preview["subject_version"],
            expected_source_fingerprint=preview["source_fingerprint"],
            expected_candidate_fingerprint=preview["candidate_fingerprint"],
            expected_downstream_fingerprint=preview["downstream_fingerprint"],
            expected_boundary_fingerprint=preview["boundary_fingerprint"],
            reason_code="FAST0R3_EVIDENCE",
            operator_id="fast0.operator",
            evidence_type="FAST0R3_PIPELINE_RCA",
            evidence_ref="fast0r3/pipeline-rca-redacted",
            evidence_file=evidence_file,
            acknowledge=tool.APPLY_ACK,
            out_dir=tmp_path / "unsafe-evidence",
        )


def test_pipeline_disposition_cli_rechecks_runtime_lease_under_exclusive_lock(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "lease-race.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / "lease-race-safe.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "lease-race-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))

    def injected_lease_count(connection) -> int:
        assert connection.in_transaction is True
        return 1

    monkeypatch.setattr(tool, "_runtime_lease_count", injected_lease_count)
    with pytest.raises(
        tool.PipelineDispositionCliError,
        match="RUNTIME_EXECUTION_LEASE_PRESENT",
    ):
        tool.apply_disposition(
            db_path=db_path,
            trade_date=trade_date,
            candidate_instance_id=candidate_id,
            action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
            request_id="fast0r3.pipeline.lease-race",
            expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
            expected_subject_version=preview["subject_version"],
            expected_source_fingerprint=preview["source_fingerprint"],
            expected_candidate_fingerprint=preview["candidate_fingerprint"],
            expected_downstream_fingerprint=preview["downstream_fingerprint"],
            expected_boundary_fingerprint=preview["boundary_fingerprint"],
            reason_code="FAST0R3_EVIDENCE",
            operator_id="fast0.operator",
            evidence_type="FAST0R3_PIPELINE_RCA",
            evidence_ref="fast0r3/pipeline-rca-redacted",
            evidence_file=evidence_file,
            acknowledge=tool.APPLY_ACK,
            out_dir=tmp_path / "lease-race-report",
        )

    check = tool._open_strict_read_only(db_path)
    try:
        assert check.execute(
            "SELECT COUNT(*) FROM pipeline_coherency_dispositions"
        ).fetchone()[0] == 0
    finally:
        check.close()


def test_pipeline_disposition_cli_reconciles_durable_commit_exception(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "commit-reconcile.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / "commit-reconcile-safe.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "commit-reconcile-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    original_commit = tool._commit_connection

    def durable_commit_then_raise(connection) -> None:
        original_commit(connection)
        raise OSError("injected post-commit acknowledgement failure")

    monkeypatch.setattr(tool, "_commit_connection", durable_commit_then_raise)
    report = tool.apply_disposition(
        db_path=db_path,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        request_id="fast0r3.pipeline.commit-reconcile",
        expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
        expected_subject_version=preview["subject_version"],
        expected_source_fingerprint=preview["source_fingerprint"],
        expected_candidate_fingerprint=preview["candidate_fingerprint"],
        expected_downstream_fingerprint=preview["downstream_fingerprint"],
        expected_boundary_fingerprint=preview["boundary_fingerprint"],
        reason_code="FAST0R3_EVIDENCE",
        operator_id="fast0.operator",
        evidence_type="FAST0R3_PIPELINE_RCA",
        evidence_ref="fast0r3/pipeline-rca-redacted",
        evidence_file=evidence_file,
        acknowledge=tool.APPLY_ACK,
        out_dir=tmp_path / "commit-reconcile-report",
    )

    assert report["verdict"]["status"] == "APPLIED_RECONCILED_WITH_WARNING"
    assert report["verdict"]["committed"] is True
    assert report["invariants"]["commit_reconciliation"] == "COMMITTED"
    assert (
        tool._reconcile_committed_disposition(
            db_path,
            request_id="fast0r3.pipeline.commit-reconcile",
            applied=report["result"],
        )
        == "COMMITTED"
    )


def test_pipeline_disposition_cli_reports_postcommit_file_state_failure(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "postcommit-file-state.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / "postcommit-file-state-safe.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / "postcommit-file-state-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    original_file_state = tool._file_state
    calls = 0

    def fail_second_file_state(path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected postcommit file-state failure")
        return original_file_state(path)

    monkeypatch.setattr(tool, "_file_state", fail_second_file_state)
    report = tool.apply_disposition(
        db_path=db_path,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        request_id="fast0r3.pipeline.postcommit-file-state",
        expected_pipeline_fingerprint=preview["pipeline_fingerprint"],
        expected_subject_version=preview["subject_version"],
        expected_source_fingerprint=preview["source_fingerprint"],
        expected_candidate_fingerprint=preview["candidate_fingerprint"],
        expected_downstream_fingerprint=preview["downstream_fingerprint"],
        expected_boundary_fingerprint=preview["boundary_fingerprint"],
        reason_code="FAST0R3_EVIDENCE",
        operator_id="fast0.operator",
        evidence_type="FAST0R3_PIPELINE_RCA",
        evidence_ref="fast0r3/pipeline-rca-redacted",
        evidence_file=evidence_file,
        acknowledge=tool.APPLY_ACK,
        out_dir=tmp_path / "postcommit-file-state-report",
    )

    check = tool._open_strict_read_only(db_path)
    try:
        row_count = check.execute(
            "SELECT COUNT(*) FROM pipeline_coherency_dispositions WHERE request_id = ?",
            ("fast0r3.pipeline.postcommit-file-state",),
        ).fetchone()[0]
    finally:
        check.close()

    assert row_count == 1
    assert report["verdict"]["status"] == "COMMITTED_POSTCHECK_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is True
    assert report["verdict"]["retry_with_same_request_id"] == (
        "fast0r3.pipeline.postcommit-file-state"
    )


def test_pipeline_disposition_cli_preserves_unknown_commit_outcome_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    db_path, kwargs = _apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r3.pipeline.outcome-unknown",
    )
    original_commit = tool._commit_connection

    def durable_commit_then_raise(connection) -> None:
        original_commit(connection)
        raise OSError("injected ambiguous commit signal")

    monkeypatch.setattr(tool, "_commit_connection", durable_commit_then_raise)
    monkeypatch.setattr(
        tool,
        "_reconcile_committed_disposition",
        lambda *_args, **_kwargs: "UNKNOWN",
    )

    report = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "outcome-unknown-report",
    )

    assert report["verdict"]["status"] == "OUTCOME_UNKNOWN"
    assert report["verdict"]["committed"] is None
    assert report["verdict"]["operator_action_required"] is True
    assert report["verdict"]["retry_with_same_request_id"] == (
        "fast0r3.pipeline.outcome-unknown"
    )
    assert "PIPELINE_DISPOSITION_COMMIT_OUTCOME_UNKNOWN" in report["verdict"][
        "failures"
    ]
    assert report["result"]["request_id"] == "fast0r3.pipeline.outcome-unknown"
    assert report["result"]["expected_pipeline_fingerprint"] == kwargs[
        "expected_pipeline_fingerprint"
    ]
    assert report["invariants"]["commit_reconciliation"] == "UNKNOWN"
    assert report["report_paths"]["raw_json"]
    assert "committed=unknown" in tool.render_console_summary(report)

    check = tool._open_strict_read_only(db_path)
    try:
        assert check.execute(
            "SELECT COUNT(*) FROM pipeline_coherency_dispositions "
            "WHERE request_id = ?",
            ("fast0r3.pipeline.outcome-unknown",),
        ).fetchone()[0] == 1
    finally:
        check.close()


def test_pipeline_disposition_cli_treats_close_only_failure_as_committed(
    tmp_path,
    monkeypatch,
) -> None:
    _, kwargs = _apply_case(
        tmp_path,
        monkeypatch,
        request_id="fast0r3.pipeline.close-only",
    )
    original_close = tool._close_connection

    def close_then_raise(connection) -> None:
        original_close(connection)
        raise OSError("injected close acknowledgement failure")

    monkeypatch.setattr(tool, "_close_connection", close_then_raise)
    report = tool.apply_disposition(
        **kwargs,
        out_dir=tmp_path / "close-only-report",
    )

    assert report["verdict"]["status"] == "APPLIED_RECONCILED_WITH_WARNING"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is False
    assert report["invariants"]["commit_reconciliation"] == (
        "COMMITTED_BY_SUCCESSFUL_COMMIT"
    )
    assert "CLOSE_RAISED_AFTER_DURABLE_WRITE" in report["verdict"]["failures"]


def _apply_case(tmp_path, monkeypatch, *, request_id: str):
    db_path = tmp_path / f"{request_id.rsplit('.', 1)[-1]}.sqlite3"
    connection, candidate_id, trade_date, _ = _expired_closed_pipeline(db_path)
    preview = tool.preview_pipeline_coherency_disposition(
        connection,
        trade_date=trade_date,
        candidate_instance_id=candidate_id,
        action=tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
    )
    connection.close()
    safe_env = tmp_path / f"{request_id.rsplit('.', 1)[-1]}.env"
    safe_env.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_KILL_SWITCH=true",
                "INCREMENTAL_EVALUATION_WORKER_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                *(f"{name.upper()}=false" for name in tool._PRODUCER_SETTING_NAMES),
            ]
        ),
        encoding="utf-8",
    )
    evidence_file = tmp_path / f"{request_id.rsplit('.', 1)[-1]}-evidence.json"
    evidence_file.write_text('{"redacted":true}', encoding="utf-8")
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    return db_path, {
        "db_path": db_path,
        "trade_date": trade_date,
        "candidate_instance_id": candidate_id,
        "action": tool.ACTION_DISPOSE_EXPIRED_PLAN_READY,
        "request_id": request_id,
        "expected_pipeline_fingerprint": preview["pipeline_fingerprint"],
        "expected_subject_version": preview["subject_version"],
        "expected_source_fingerprint": preview["source_fingerprint"],
        "expected_candidate_fingerprint": preview["candidate_fingerprint"],
        "expected_downstream_fingerprint": preview["downstream_fingerprint"],
        "expected_boundary_fingerprint": preview["boundary_fingerprint"],
        "reason_code": "FAST0R3_EVIDENCE",
        "operator_id": "fast0.operator",
        "evidence_type": "FAST0R3_PIPELINE_RCA",
        "evidence_ref": "fast0r3/pipeline-rca-redacted",
        "evidence_file": evidence_file,
        "acknowledge": tool.APPLY_ACK,
    }
