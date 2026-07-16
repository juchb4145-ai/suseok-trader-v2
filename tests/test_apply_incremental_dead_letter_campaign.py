from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from tests.test_ops_incremental_dead_letter_campaign_preflight import (
    _fixture,
    _passing_probe,
)
from tests.test_resolve_incremental_evaluation_dead_letter_tool import _write_safe_env
from tools import apply_incremental_dead_letter_campaign as tool
from tools import ops_incremental_dead_letter_campaign_preflight as preflight_tool


def test_u01_apply_is_single_row_private_verified_and_replayable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, plan_path, manifest_path, backup_path, target_set_sha256 = _fixture(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(
        preflight_tool,
        "_EXPECTED_TARGET_SET_SHA256",
        target_set_sha256,
    )
    monkeypatch.setattr(
        preflight_tool.fast0_tool,
        "_probe_no_other_open_handles",
        _passing_probe,
    )
    preflight = preflight_tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        base_backup=backup_path,
        expected_base_backup_sha256=_sha256_file(backup_path),
        alias="U01",
        predecessor_apply_report=None,
        expected_predecessor_apply_report_sha256=None,
        out_dir=tmp_path / "preflight",
        observed_at=datetime(2026, 7, 16, 2, 0, tzinfo=UTC),
    )
    assert preflight["verdict"]["evidence_status"] == "PASS"
    preflight_path = next((tmp_path / "preflight").glob("*/raw.json"))
    evidence = tmp_path / "private-evidence.json"
    evidence.write_text('{"classification":"obsolete-closed"}', encoding="utf-8")
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)

    report = tool.apply_alias(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        preflight_report=preflight_path,
        expected_preflight_report_sha256=_sha256_file(preflight_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        alias="U01",
        request_id="fast0r9.request.u01",
        operator_id="fast0r9.operator",
        evidence_ref="fast0r9.u01.evidence",
        evidence_file=evidence,
        expected_evidence_file_sha256=_sha256_file(evidence),
        acknowledgement=tool.APPLY_ACKNOWLEDGEMENT,
        out_dir=tmp_path / "apply",
        observed_at=datetime(2026, 7, 16, 2, 1, tzinfo=UTC),
    )
    raw_path = next((tmp_path / "apply").glob("*/raw.json"))
    raw = raw_path.read_text(encoding="utf-8")
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    count = connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    row = connection.execute(
        "SELECT evidence_json FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()
    connection.close()

    assert report["verdict"]["status"] == "VERIFIED"
    assert report["verdict"]["committed"] is True
    assert report["progress"]["completed_count_before"] == 0
    assert report["progress"]["completed_count_after"] == 1
    assert report["invariants"]["single_disposition_delta_valid"] is True
    assert count == 1
    assert row is not None
    assert "request_payload" not in json.loads(row["evidence_json"])
    assert '"dead_letter_id"' not in raw
    assert '"operator_id"' not in raw
    assert "fast0r9.request.u01" not in raw
    assert str(tmp_path) not in raw

    validated = preflight_tool.validate_apply_report(
        json.loads(raw),
        report_sha256=_sha256_file(raw_path),
        expected_alias="U01",
        source_plan_report_sha256=_sha256_file(plan_path),
        private_manifest_sha256=_sha256_file(manifest_path),
        target_set_sha256=target_set_sha256,
    )
    assert validated["campaign_chain_sha256"] == report["campaign"]["campaign_chain_sha256"]

    replay = tool.apply_alias(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        preflight_report=preflight_path,
        expected_preflight_report_sha256=_sha256_file(preflight_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        alias="U01",
        request_id="fast0r9.request.u01",
        operator_id="fast0r9.operator",
        evidence_ref="fast0r9.u01.evidence",
        evidence_file=evidence,
        expected_evidence_file_sha256=_sha256_file(evidence),
        acknowledgement=tool.APPLY_ACKNOWLEDGEMENT,
        out_dir=tmp_path / "replay",
        observed_at=datetime(2026, 7, 16, 2, 2, tzinfo=UTC),
    )
    replay_path = next((tmp_path / "replay").glob("*/raw.json"))
    assert replay["verdict"]["status"] == "REPLAY_VERIFIED"
    assert replay["progress"]["completed_count_before"] == 1
    assert replay["progress"]["completed_count_after"] == 1
    replay_connection = sqlite3.connect(db_path)
    replay_count = replay_connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    replay_connection.close()
    assert replay_count == 1
    preflight_tool.validate_apply_report(
        json.loads(replay_path.read_text(encoding="utf-8")),
        report_sha256=_sha256_file(replay_path),
        expected_alias="U01",
        source_plan_report_sha256=_sha256_file(plan_path),
        private_manifest_sha256=_sha256_file(manifest_path),
        target_set_sha256=target_set_sha256,
    )


def test_apply_rejects_missing_ack_before_database_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = False

    def forbidden(*_args, **_kwargs):
        nonlocal opened
        opened = True
        raise AssertionError("database should not be opened")

    monkeypatch.setattr(tool.legacy_tool, "_validated_database_path", forbidden)
    with pytest.raises(
        tool.IncrementalDeadLetterCampaignApplyError,
        match="EXACT_SINGLE_ALIAS_APPLY_ACKNOWLEDGEMENT_REQUIRED",
    ):
        tool.apply_alias(
            db_path=tmp_path / "missing.sqlite3",
            blocker_plan_report=tmp_path / "plan.json",
            expected_blocker_plan_report_sha256="a" * 64,
            preflight_report=tmp_path / "preflight.json",
            expected_preflight_report_sha256="b" * 64,
            private_target_manifest=tmp_path / "manifest.json",
            expected_private_target_manifest_sha256="c" * 64,
            alias="U01",
            request_id="fast0r9.request.u01",
            operator_id="fast0r9.operator",
            evidence_ref="fast0r9.u01.evidence",
            evidence_file=tmp_path / "evidence.json",
            expected_evidence_file_sha256="d" * 64,
            acknowledgement="NO",
            out_dir=tmp_path / "apply",
        )
    assert opened is False


def test_u02_requires_and_consumes_exact_u01_predecessor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, plan_path, manifest_path, backup_path, target_set_sha256 = _fixture(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(preflight_tool, "_EXPECTED_TARGET_SET_SHA256", target_set_sha256)
    monkeypatch.setattr(
        preflight_tool.fast0_tool,
        "_probe_no_other_open_handles",
        _passing_probe,
    )
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = tmp_path / "private-evidence.json"
    evidence.write_text('{"classification":"obsolete-closed"}', encoding="utf-8")

    u01_preflight = preflight_tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        base_backup=backup_path,
        expected_base_backup_sha256=_sha256_file(backup_path),
        alias="U01",
        predecessor_apply_report=None,
        expected_predecessor_apply_report_sha256=None,
        out_dir=tmp_path / "u01-preflight",
        observed_at=datetime(2026, 7, 16, 2, 0, tzinfo=UTC),
    )
    assert u01_preflight["verdict"]["evidence_status"] == "PASS"
    u01_preflight_path = next((tmp_path / "u01-preflight").glob("*/raw.json"))
    u01_apply = tool.apply_alias(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        preflight_report=u01_preflight_path,
        expected_preflight_report_sha256=_sha256_file(u01_preflight_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        alias="U01",
        request_id="fast0r9.request.u01",
        operator_id="fast0r9.operator",
        evidence_ref="fast0r9.u01.evidence",
        evidence_file=evidence,
        expected_evidence_file_sha256=_sha256_file(evidence),
        acknowledgement=tool.APPLY_ACKNOWLEDGEMENT,
        out_dir=tmp_path / "u01-apply",
        observed_at=datetime(2026, 7, 16, 2, 1, tzinfo=UTC),
    )
    assert u01_apply["verdict"]["status"] == "VERIFIED"
    u01_apply_path = next((tmp_path / "u01-apply").glob("*/raw.json"))

    u02_preflight = preflight_tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        base_backup=backup_path,
        expected_base_backup_sha256=_sha256_file(backup_path),
        alias="U02",
        predecessor_apply_report=u01_apply_path,
        expected_predecessor_apply_report_sha256=_sha256_file(u01_apply_path),
        out_dir=tmp_path / "u02-preflight",
        observed_at=datetime(2026, 7, 16, 2, 2, tzinfo=UTC),
    )
    assert u02_preflight["verdict"]["evidence_status"] == "PASS"
    assert u02_preflight["campaign"]["completed_count"] == 1
    assert u02_preflight["campaign"]["predecessor_chain_matches"] is True
    u02_preflight_path = next((tmp_path / "u02-preflight").glob("*/raw.json"))
    u02_apply = tool.apply_alias(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        preflight_report=u02_preflight_path,
        expected_preflight_report_sha256=_sha256_file(u02_preflight_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        alias="U02",
        request_id="fast0r9.request.u02",
        operator_id="fast0r9.operator",
        evidence_ref="fast0r9.u02.evidence",
        evidence_file=evidence,
        expected_evidence_file_sha256=_sha256_file(evidence),
        acknowledgement=tool.APPLY_ACKNOWLEDGEMENT,
        out_dir=tmp_path / "u02-apply",
        observed_at=datetime(2026, 7, 16, 2, 3, tzinfo=UTC),
    )
    assert u02_apply["verdict"]["status"] == "VERIFIED"
    assert u02_apply["progress"]["completed_count_after"] == 2
    connection = sqlite3.connect(db_path)
    count = connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    connection.close()
    assert count == 2

    with pytest.raises(
        preflight_tool.IncrementalDeadLetterCampaignPreflightError,
        match="PREDECESSOR_ALIAS_INVALID",
    ):
        preflight_tool.run_report(
            db_path=db_path,
            blocker_plan_report=plan_path,
            expected_blocker_plan_report_sha256=_sha256_file(plan_path),
            private_target_manifest=manifest_path,
            expected_private_target_manifest_sha256=_sha256_file(manifest_path),
            base_backup=backup_path,
            expected_base_backup_sha256=_sha256_file(backup_path),
            alias="U03",
            predecessor_apply_report=u01_apply_path,
            expected_predecessor_apply_report_sha256=_sha256_file(u01_apply_path),
            out_dir=tmp_path / "u03-preflight",
            observed_at=datetime(2026, 7, 16, 2, 4, tzinfo=UTC),
        )


def test_commit_raise_after_durable_write_is_reconciled_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_u01(tmp_path, monkeypatch)
    real_commit = tool._commit_connection

    def commit_then_raise(connection: sqlite3.Connection) -> None:
        real_commit(connection)
        raise sqlite3.OperationalError("fault-after-durable-commit")

    monkeypatch.setattr(tool, "_commit_connection", commit_then_raise)
    report = tool.apply_alias(**prepared)

    assert report["verdict"]["status"] == "VERIFIED"
    assert report["verdict"]["committed"] is True
    assert report["invariants"]["commit_reconciliation"] == "COMMITTED"
    connection = sqlite3.connect(prepared["db_path"])
    count = connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    connection.close()
    assert count == 1


def test_commit_raise_before_durable_write_rolls_back_and_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_u01(tmp_path, monkeypatch)

    def raise_before_commit(_connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("fault-before-commit")

    monkeypatch.setattr(tool, "_commit_connection", raise_before_commit)
    with pytest.raises(
        tool.IncrementalDeadLetterCampaignApplyError,
        match="CAMPAIGN_COMMIT_NOT_DURABLE",
    ):
        tool.apply_alias(**prepared)

    connection = sqlite3.connect(prepared["db_path"])
    count = connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    connection.close()
    assert count == 0


def test_commit_outcome_unknown_writes_private_blocking_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_u01(tmp_path, monkeypatch)

    def raise_during_commit(_connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("fault-with-unknown-outcome")

    monkeypatch.setattr(tool, "_commit_connection", raise_during_commit)
    monkeypatch.setattr(tool, "_reconcile_commit", lambda *_args, **_kwargs: "UNKNOWN")
    report = tool.apply_alias(**prepared)
    raw_path = next((tmp_path / "apply").glob("*/raw.json"))
    raw = raw_path.read_text(encoding="utf-8")

    assert report["verdict"]["status"] == "OUTCOME_UNKNOWN"
    assert report["verdict"]["committed"] is None
    assert report["verdict"]["operator_action_required"] is True
    assert report["result"]["status"] == "OUTCOME_UNKNOWN"
    assert '"dead_letter_id"' not in raw
    assert '"operator_id"' not in raw
    assert "fast0r9.request.u01" not in raw
    assert str(tmp_path) not in raw


def test_evidence_write_failure_reports_committed_without_authorizing_next_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_u01(tmp_path, monkeypatch)

    def fail_report(*_args, **_kwargs):
        raise OSError("report-write-fault")

    monkeypatch.setattr(tool, "_write_report", fail_report)
    report = tool.apply_alias(**prepared)

    assert report["verdict"]["status"] == "COMMITTED_EVIDENCE_WRITE_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["evidence_written"] is False
    assert report["verdict"]["operator_action_required"] is True
    connection = sqlite3.connect(prepared["db_path"])
    count = connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    connection.close()
    assert count == 1


def test_checkpoint_failure_reports_committed_postcheck_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = _prepared_u01(tmp_path, monkeypatch)

    def fail_checkpoint(_connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("checkpoint-fault")

    monkeypatch.setattr(tool, "_checkpoint_wal", fail_checkpoint)
    report = tool.apply_alias(**prepared)

    assert report["verdict"]["status"] == "COMMITTED_POSTCHECK_FAILED"
    assert report["verdict"]["committed"] is True
    assert report["verdict"]["operator_action_required"] is True
    assert "CAMPAIGN_WAL_CHECKPOINT_FAILED" in report["verdict"]["failures"]
    connection = sqlite3.connect(prepared["db_path"])
    count = connection.execute(
        "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
    ).fetchone()[0]
    connection.close()
    assert count == 1


def _prepared_u01(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    db_path, plan_path, manifest_path, backup_path, target_set_sha256 = _fixture(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(preflight_tool, "_EXPECTED_TARGET_SET_SHA256", target_set_sha256)
    monkeypatch.setattr(
        preflight_tool.fast0_tool,
        "_probe_no_other_open_handles",
        _passing_probe,
    )
    monkeypatch.setattr(tool.fast0_tool, "_probe_no_other_open_handles", _passing_probe)
    preflight_tool.run_report(
        db_path=db_path,
        blocker_plan_report=plan_path,
        expected_blocker_plan_report_sha256=_sha256_file(plan_path),
        private_target_manifest=manifest_path,
        expected_private_target_manifest_sha256=_sha256_file(manifest_path),
        base_backup=backup_path,
        expected_base_backup_sha256=_sha256_file(backup_path),
        alias="U01",
        predecessor_apply_report=None,
        expected_predecessor_apply_report_sha256=None,
        out_dir=tmp_path / "preflight",
        observed_at=datetime(2026, 7, 16, 2, 0, tzinfo=UTC),
    )
    preflight_path = next((tmp_path / "preflight").glob("*/raw.json"))
    evidence = tmp_path / "private-evidence.json"
    evidence.write_text('{"classification":"obsolete-closed"}', encoding="utf-8")
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    return {
        "db_path": db_path,
        "blocker_plan_report": plan_path,
        "expected_blocker_plan_report_sha256": _sha256_file(plan_path),
        "preflight_report": preflight_path,
        "expected_preflight_report_sha256": _sha256_file(preflight_path),
        "private_target_manifest": manifest_path,
        "expected_private_target_manifest_sha256": _sha256_file(manifest_path),
        "alias": "U01",
        "request_id": "fast0r9.request.u01",
        "operator_id": "fast0r9.operator",
        "evidence_ref": "fast0r9.u01.evidence",
        "evidence_file": evidence,
        "expected_evidence_file_sha256": _sha256_file(evidence),
        "acknowledgement": tool.APPLY_ACKNOWLEDGEMENT,
        "out_dir": tmp_path / "apply",
        "observed_at": datetime(2026, 7, 16, 2, 1, tzinfo=UTC),
    }


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
