from __future__ import annotations

import sqlite3
import sys
from contextlib import contextmanager

import pytest
from services.runtime import evaluation_run_guard
from services.runtime import targeted_pipeline_rebuild as targeted
from tests.test_targeted_pipeline_rebuild import _count, _prepared_runner
from tools import run_targeted_pipeline_rebuild as tool


def test_targeted_pipeline_rebuild_cli_preview_then_exact_cas_run(tmp_path) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(
        tmp_path,
        count=1,
    )
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()

    preview = tool.preview_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        out_dir=tmp_path / "preview-evidence",
        environ=environ,
    )
    result = tool.run_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        expected_preview_sha256=preview["preview_sha256"],
        acknowledge=tool.RUN_ACK,
        out_dir=tmp_path / "run-evidence",
        environ=environ,
    )

    check = tool._open_strict_read_only(db_path)
    try:
        strategy_count = _count(check, "strategy_observations")
        risk_count = _count(check, "risk_observations")
        entry_count = _count(check, "entry_timing_evaluations")
        plan_count = _count(check, "order_plan_drafts")
        command_count = _count(check, "gateway_commands")
    finally:
        check.close()

    assert preview["verdict"]["status"] == "ELIGIBLE"
    assert len(preview["preview_sha256"]) == 64
    assert result["verdict"]["status"] == "COMPLETED"
    assert result["result"]["no_order_plans_created"] is True
    assert result["result"]["no_order_commands_created"] is True
    assert strategy_count == risk_count == entry_count == 1
    assert plan_count == command_count == 0


def test_targeted_pipeline_rebuild_cli_preserves_committed_cleanup_failure_evidence(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(
        tmp_path,
        count=1,
    )
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()
    preview = tool.preview_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        out_dir=tmp_path / "preview-cleanup-evidence",
        environ=environ,
    )

    def fail_release(*_args, **_kwargs):
        raise RuntimeError("synthetic lock cleanup failure")

    monkeypatch.setattr(evaluation_run_guard, "_release_lock", fail_release)
    report = tool.run_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        expected_preview_sha256=preview["preview_sha256"],
        acknowledge=tool.RUN_ACK,
        out_dir=tmp_path / "run-cleanup-evidence",
        environ=environ,
    )

    assert report["verdict"] == {
        "status": "COMMITTED_CLEANUP_FAILED",
        "failures": ["COMMITTED_CLEANUP_FAILED"],
        "committed": True,
        "operator_action_required": True,
    }
    assert report["result"]["data_committed"] is True
    assert report["result"]["lock_cleanup"]["operator_action_required"] is True
    assert tool.Path(report["report_paths"]["raw_json"]).is_file()


def test_targeted_pipeline_rebuild_cli_preserves_reconciled_commit_signal_failure(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(tmp_path)
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()
    preview = tool.preview_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        out_dir=tmp_path / "preview-commit-signal",
        environ=environ,
    )

    @contextmanager
    def commit_then_raise(connection, *, lease=None):
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            connection.rollback()
            raise
        connection.commit()
        raise sqlite3.OperationalError("synthetic durable commit signal failure")

    monkeypatch.setattr(targeted, "immediate_transaction", commit_then_raise)
    report = tool.run_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        expected_preview_sha256=preview["preview_sha256"],
        acknowledge=tool.RUN_ACK,
        out_dir=tmp_path / "run-commit-signal",
        environ=environ,
    )

    assert report["verdict"]["status"] == "COMMITTED_TRANSACTION_SIGNAL_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is True
    assert report["result"]["transaction_outcome"]["outcome"] == (
        "COMMITTED_RECONCILED"
    )
    assert tool.Path(report["report_paths"]["raw_json"]).is_file()


def test_targeted_pipeline_rebuild_cli_rejects_stale_preview_without_writes(
    tmp_path,
) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(tmp_path)
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()

    with pytest.raises(
        tool.TargetedPipelineRebuildCliError,
        match="TARGETED_REBUILD_PREVIEW_CAS_MISMATCH",
    ):
        tool.run_rebuild(
            db_path=db_path,
            trade_date="2026-07-15",
            candidate_instance_ids=candidate_ids,
            expected_preview_sha256="0" * 64,
            acknowledge=tool.RUN_ACK,
            out_dir=tmp_path / "run-evidence",
            environ=environ,
        )

    check = tool._open_strict_read_only(db_path)
    try:
        assert _count(check, "strategy_observations") == 0
        assert _count(check, "risk_observations") == 0
        assert _count(check, "entry_timing_evaluations") == 0
        assert _count(check, "runtime_execution_locks") == 0
        assert _count(check, "runtime_execution_lock_fences") == 0
    finally:
        check.close()


def test_targeted_pipeline_rebuild_cli_preserves_committed_close_failure(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(tmp_path)
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()
    preview = tool.preview_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        out_dir=tmp_path / "preview-close-failure",
        environ=environ,
    )
    real_open = tool._open_existing_read_write
    opened: list[_CloseFailingConnection] = []

    def open_with_failing_close(path):
        wrapper = _CloseFailingConnection(real_open(path))
        opened.append(wrapper)
        return wrapper

    monkeypatch.setattr(tool, "_open_existing_read_write", open_with_failing_close)
    report = tool.run_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        expected_preview_sha256=preview["preview_sha256"],
        acknowledge=tool.RUN_ACK,
        out_dir=tmp_path / "run-close-failure",
        environ=environ,
    )
    opened[0].connection.close()

    assert report["verdict"]["status"] == "COMMITTED_POSTCHECK_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is True
    assert "CONNECTION_CLOSE_FAILED" in report["verdict"]["failures"]
    assert report["result"]["data_committed"] is True
    assert tool.Path(report["report_paths"]["raw_json"]).is_file()


def test_targeted_pipeline_rebuild_cli_preserves_committed_file_state_failure(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(tmp_path)
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()
    preview = tool.preview_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        out_dir=tmp_path / "preview-file-state-failure",
        environ=environ,
    )
    real_file_state = tool._file_state
    call_count = 0

    def fail_postcommit_file_state(path):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise OSError("synthetic postcommit file-state failure")
        return real_file_state(path)

    monkeypatch.setattr(tool, "_file_state", fail_postcommit_file_state)
    report = tool.run_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        expected_preview_sha256=preview["preview_sha256"],
        acknowledge=tool.RUN_ACK,
        out_dir=tmp_path / "run-file-state-failure",
        environ=environ,
    )

    assert report["verdict"]["status"] == "COMMITTED_POSTCHECK_FAILED"
    assert report["verdict"]["committed"] is True
    assert "DATABASE_FILE_STATE_POSTCHECK_FAILED" in report["verdict"]["failures"]
    assert report["result"]["data_committed"] is True


def test_targeted_pipeline_rebuild_cli_preserves_committed_report_failure(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(tmp_path)
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()
    preview = tool.preview_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        out_dir=tmp_path / "preview-report-failure",
        environ=environ,
    )

    def fail_report_timestamp():
        raise RuntimeError("synthetic report construction failure")

    monkeypatch.setattr(tool, "_now", fail_report_timestamp)
    report = tool.run_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        expected_preview_sha256=preview["preview_sha256"],
        acknowledge=tool.RUN_ACK,
        out_dir=tmp_path / "run-report-failure",
        environ=environ,
    )

    assert report["verdict"]["status"] == "COMMITTED_REPORT_FAILED"
    assert report["verdict"]["committed"] is True
    assert "REPORT_CONSTRUCTION_FAILED" in report["verdict"]["failures"]
    assert report["result"]["data_committed"] is True
    assert tool.Path(report["report_paths"]["raw_json"]).is_file()


def test_targeted_pipeline_rebuild_cli_preserves_evidence_write_failure(
    tmp_path,
    monkeypatch,
) -> None:
    connection, _settings, environ, candidate_ids = _prepared_runner(tmp_path)
    db_path = tmp_path / "targeted-pipeline.sqlite3"
    connection.close()
    preview = tool.preview_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        out_dir=tmp_path / "preview-write-failure",
        environ=environ,
    )

    def fail_write_report(*_args, **_kwargs):
        raise OSError("synthetic evidence write failure")

    monkeypatch.setattr(tool, "_write_report", fail_write_report)
    report = tool.run_rebuild(
        db_path=db_path,
        trade_date="2026-07-15",
        candidate_instance_ids=candidate_ids,
        expected_preview_sha256=preview["preview_sha256"],
        acknowledge=tool.RUN_ACK,
        out_dir=tmp_path / "run-write-failure",
        environ=environ,
    )

    assert report["verdict"]["status"] == "COMMITTED_EVIDENCE_WRITE_FAILED"
    assert report["verdict"]["committed"] is True
    assert "EVIDENCE_WRITE_FAILED" in report["verdict"]["failures"]
    assert report["result"]["data_committed"] is True
    assert report["report_paths"] == {}


@pytest.mark.parametrize(
    "status",
    [
        "COMMITTED_CLEANUP_FAILED",
        "COMMITTED_EVIDENCE_WRITE_FAILED",
        "COMMITTED_POSTCHECK_FAILED",
        "COMMITTED_REPORT_FAILED",
        "COMMITTED_TRANSACTION_SIGNAL_FAILED",
        "OUTCOME_UNKNOWN",
    ],
)
def test_targeted_pipeline_rebuild_main_returns_exit_two_for_fail_closed_status(
    monkeypatch,
    status,
) -> None:
    monkeypatch.setattr(
        tool,
        "run_rebuild",
        lambda **_kwargs: {"mode": "RUN", "verdict": {"status": status}},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_targeted_pipeline_rebuild",
            "--db",
            "redacted.sqlite3",
            "--trade-date",
            "2026-07-15",
            "--candidate-instance-id",
            "candidate-redacted",
            "--run",
            "--expected-preview-sha256",
            "0" * 64,
            "--acknowledge",
            tool.RUN_ACK,
        ],
    )

    assert tool.main() == 2


class _CloseFailingConnection:
    def __init__(self, connection) -> None:
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def close(self) -> None:
        raise OSError("synthetic close failure")
