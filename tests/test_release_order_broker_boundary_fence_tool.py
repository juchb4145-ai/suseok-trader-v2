from __future__ import annotations

import json
import shutil
import sqlite3
from typing import cast

import pytest
import storage.gateway_order_broker_boundary as boundary_storage
from services.config import Settings
from storage.gateway_order_broker_boundary import (
    OrderBrokerBoundaryResolutionError,
    get_order_broker_boundary_status,
    preview_order_broker_boundary_resolution,
    record_order_broker_boundary_resolution,
)
from storage.sqlite import initialize_database
from tests.test_gateway_order_broker_boundary_resolution import (
    COMMAND_ID,
    EVIDENCE_REF,
    EVIDENCE_SHA256,
    EVIDENCE_TYPE,
    OPERATOR_ID,
    REASON_CODE,
    _insert_unconfirmed_boundary,
)
from tools import release_order_broker_boundary_fence as tool

TEST_TRADE_DATE = "2026-07-23"


@pytest.fixture(autouse=True)
def _freeze_fence_trade_date(monkeypatch) -> None:
    monkeypatch.setattr(tool, "market_today", lambda: TEST_TRADE_DATE)
    monkeypatch.setattr(boundary_storage, "market_today", lambda: TEST_TRADE_DATE)


def test_preflight_binds_exact_release_target_and_is_deterministic(tmp_path) -> None:
    db_path = tmp_path / "fence-preflight.sqlite3"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)

    first = tool.build_fence_event_preflight(
        connection,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
    )
    second = tool.build_fence_event_preflight(
        connection,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
    )
    connection.close()

    assert first["status"] == "APPROVAL_READY"
    assert first["apply_authorized"] is False
    assert first["approval"]["expected_sha256"] == second["approval"]["expected_sha256"]
    payload = first["approval"]["payload"]
    assert payload["command_alias"] == "U01"
    assert payload["action"] == "RELEASE"
    assert payload["target"]["resolution_id"]
    assert len(payload["target"]["resolution_request_hash"]) == 64
    assert len(payload["target"]["source_boundary_fingerprint"]) == 64
    assert payload["evidence_sha256"] == first["evidence"]["sha256"]


def test_exact_report_and_approval_apply_once_without_order_delta(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "fence-apply.sqlite3"
    report_path = tmp_path / "u01-preflight.json"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()

    preview = tool.run_fence_event_preflight(
        db_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )
    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)
    settings = cast(Settings, object())

    applied = tool.apply_fence_event_from_preflight(
        db_path,
        preflight_report=report_path,
        expected_preflight_report_sha256=preview["report_sha256"],
        expected_approval_sha256=preview["approval"]["expected_sha256"],
        settings=settings,
    )
    replay = tool.apply_fence_event_from_preflight(
        db_path,
        preflight_report=report_path,
        expected_preflight_report_sha256=preview["report_sha256"],
        expected_approval_sha256=preview["approval"]["expected_sha256"],
        settings=settings,
    )

    connection = initialize_database(db_path)
    status = get_order_broker_boundary_status(connection)
    event_count = connection.execute(
        "SELECT COUNT(*) FROM gateway_order_broker_boundary_fence_events"
    ).fetchone()[0]
    connection.close()
    assert applied["status"] == "APPLIED"
    assert applied["command_count_invariant_ok"] is True
    assert replay["status"] == "REPLAYED_EFFECTIVE"
    assert event_count == 1
    assert status["resolution_maintenance_fence_active"] is False
    assert status["effective_block_new_order_routing"] is False


def test_apply_rejects_wrong_approval_or_changed_report(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "fence-reject.sqlite3"
    report_path = tmp_path / "u01-preflight.json"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()
    preview = tool.run_fence_event_preflight(
        db_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )
    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)
    settings = cast(Settings, object())

    with pytest.raises(tool.FenceReleaseCliError) as wrong_approval:
        tool.apply_fence_event_from_preflight(
            db_path,
            preflight_report=report_path,
            expected_preflight_report_sha256=preview["report_sha256"],
            expected_approval_sha256="0" * 64,
            settings=settings,
        )
    assert "APPROVAL_SHA256_MISMATCH" in wrong_approval.value.reason_codes

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["approval"]["payload"]["command_alias"] = "U02"
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(tool.FenceReleaseCliError) as changed:
        tool.apply_fence_event_from_preflight(
            db_path,
            preflight_report=report_path,
            expected_preflight_report_sha256=preview["report_sha256"],
            expected_approval_sha256=preview["approval"]["expected_sha256"],
            settings=settings,
        )
    assert "PREFLIGHT_REPORT_SHA256_MISMATCH" in changed.value.reason_codes


def test_preflight_without_effective_resolution_is_blocked(tmp_path) -> None:
    connection = initialize_database(tmp_path / "fence-blocked.sqlite3")
    _insert_unconfirmed_boundary(connection)
    report = tool.build_fence_event_preflight(
        connection,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
    )
    connection.close()
    assert report["status"] == "BLOCKED"
    assert "ACTIVE_RESOLUTION_MISSING" in report["evidence"]["payload"]["reason_codes"]


def test_apply_rejects_preflight_replayed_against_cloned_database(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "source.sqlite3"
    clone_path = tmp_path / "clone.sqlite3"
    report_path = tmp_path / "source-preflight.json"
    connection = initialize_database(source_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()
    shutil.copy2(source_path, clone_path)
    preview = tool.run_fence_event_preflight(
        source_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )
    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as rejected:
        tool.apply_fence_event_from_preflight(
            clone_path,
            preflight_report=report_path,
            expected_preflight_report_sha256=preview["report_sha256"],
            expected_approval_sha256=preview["approval"]["expected_sha256"],
            settings=cast(Settings, object()),
        )

    assert rejected.value.code == "APPROVAL_BINDING_STALE"
    assert "DATABASE_IDENTITY_MISMATCH" in rejected.value.details["reason_codes"]


def test_apply_rejects_gateway_command_state_drift(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "command-drift.sqlite3"
    report_path = tmp_path / "command-drift-preflight.json"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()
    preview = tool.run_fence_event_preflight(
        db_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )
    drift = sqlite3.connect(db_path)
    drift.execute(
        "UPDATE gateway_commands SET status = 'DRIFTED' WHERE command_id = ?",
        (COMMAND_ID,),
    )
    drift.commit()
    drift.close()
    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as rejected:
        tool.apply_fence_event_from_preflight(
            db_path,
            preflight_report=report_path,
            expected_preflight_report_sha256=preview["report_sha256"],
            expected_approval_sha256=preview["approval"]["expected_sha256"],
            settings=cast(Settings, object()),
        )

    assert rejected.value.code == "APPROVAL_BINDING_STALE"
    assert "GATEWAY_COMMAND_STATE_FINGERPRINT_MISMATCH" in rejected.value.details["reason_codes"]


@pytest.mark.parametrize(
    ("metadata_key", "drift_value", "expected_reason"),
    (
        ("app_name", "wrong-app", "APP_NAME_MISMATCH"),
        ("schema_version", "62", "SCHEMA_VERSION_MISMATCH"),
    ),
)
def test_apply_rejects_app_or_schema_drift(
    tmp_path,
    monkeypatch,
    metadata_key: str,
    drift_value: str,
    expected_reason: str,
) -> None:
    db_path = tmp_path / f"{metadata_key}-drift.sqlite3"
    report_path = tmp_path / f"{metadata_key}-preflight.json"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()
    preview = tool.run_fence_event_preflight(
        db_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-request-u01",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )
    drift = sqlite3.connect(db_path)
    drift.execute(
        "UPDATE app_metadata SET value = ? WHERE key = ?",
        (drift_value, metadata_key),
    )
    drift.commit()
    drift.close()
    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)

    with pytest.raises(OrderBrokerBoundaryResolutionError) as rejected:
        tool.apply_fence_event_from_preflight(
            db_path,
            preflight_report=report_path,
            expected_preflight_report_sha256=preview["report_sha256"],
            expected_approval_sha256=preview["approval"]["expected_sha256"],
            settings=cast(Settings, object()),
        )

    assert rejected.value.code == "APPROVAL_BINDING_STALE"
    assert expected_reason in rejected.value.details["reason_codes"]


def test_cli_converts_safe_label_rejection_to_fail_closed_exit(tmp_path) -> None:
    db_path = tmp_path / "fence-cli-invalid.sqlite3"
    initialize_database(db_path).close()
    exit_code = tool.main(
        [
            "--db",
            str(db_path),
            "--command-id",
            COMMAND_ID,
            "--command-alias",
            "U01",
            "--approval-id",
            "account-12345678",
            "--request-id",
            "fence-release-request-u01",
            "--operator-id",
            "operator.bootstrap",
            "--trade-date",
            "2026-07-23",
            "--out",
            str(tmp_path / "blocked.json"),
        ]
    )
    assert exit_code == 2
    assert not (tmp_path / "blocked.json").exists()


@pytest.mark.parametrize(
    "failure_kind",
    ("oserror", "resolution", "value", "sqlite", "runtime"),
)
def test_cli_reports_expected_postcommit_readback_failure_as_applied_unknown(
    tmp_path,
    monkeypatch,
    capsys,
    failure_kind: str,
) -> None:
    db_path = tmp_path / f"postcommit-{failure_kind}.sqlite3"
    report_path = tmp_path / f"postcommit-{failure_kind}-preflight.json"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()
    preview = tool.run_fence_event_preflight(
        db_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id=f"fence-release-postcommit-{failure_kind}",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )
    failures = {
        "oserror": OSError("simulated post-commit readback failure"),
        "resolution": OrderBrokerBoundaryResolutionError(
            "POST_APPLY_PROJECTION_FAILED",
            "simulated post-commit readback failure",
        ),
        "value": ValueError("simulated post-commit readback failure"),
        "sqlite": sqlite3.OperationalError("simulated post-commit readback failure"),
        "runtime": RuntimeError("simulated post-commit readback failure"),
    }
    real_binding = tool.get_order_broker_boundary_fence_approval_binding
    binding_call_count = 0

    def fail_second_binding_read(connection):
        nonlocal binding_call_count
        binding_call_count += 1
        if binding_call_count == 2:
            raise failures[failure_kind]
        return real_binding(connection)

    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)
    monkeypatch.setattr(tool, "load_settings", lambda: cast(Settings, object()))
    monkeypatch.setattr(
        tool,
        "get_order_broker_boundary_fence_approval_binding",
        fail_second_binding_read,
    )

    exit_code = tool.main(_apply_cli_args(db_path, report_path, preview))
    payload = json.loads(capsys.readouterr().out)
    readback = sqlite3.connect(db_path)
    event_count = readback.execute(
        "SELECT COUNT(*) FROM gateway_order_broker_boundary_fence_events"
    ).fetchone()[0]
    readback.close()

    assert exit_code == 2
    assert payload["status"] == "APPLIED_WITH_VERIFICATION_FAILURE"
    assert payload["committed"] is True
    assert payload["apply_unknown"] is True
    assert payload["post_apply_verification_error"] == "POST_APPLY_READBACK_FAILED"
    assert event_count == 1


def test_cli_reports_committed_release_invalidated_at_readback_as_not_effective(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "postcommit-invalidated.sqlite3"
    report_path = tmp_path / "postcommit-invalidated-preflight.json"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()
    preview = tool.run_fence_event_preflight(
        db_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-postcommit-invalidated",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )

    def invalidated_release_status(connection, command_id):
        assert command_id == COMMAND_ID
        return {
            "maintenance_fence_released": False,
            "maintenance_fence_active": True,
            "fence_release_status": "RELEASE_INVALIDATED",
        }

    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(boundary_storage, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)
    monkeypatch.setattr(tool, "load_settings", lambda: cast(Settings, object()))
    monkeypatch.setattr(
        tool,
        "preview_order_broker_boundary_fence_release",
        invalidated_release_status,
    )

    exit_code = tool.main(_apply_cli_args(db_path, report_path, preview))
    payload = json.loads(capsys.readouterr().out)
    readback = sqlite3.connect(db_path)
    event_count = readback.execute(
        "SELECT COUNT(*) FROM gateway_order_broker_boundary_fence_events"
    ).fetchone()[0]
    readback.close()

    assert exit_code == 2
    assert payload["status"] == "APPLIED_NOT_EFFECTIVE"
    assert payload["committed"] is True
    assert payload["apply_unknown"] is False
    assert payload["event_status"]["fence_release_status"] == "RELEASE_INVALIDATED"
    assert event_count == 1


def test_cli_preserves_rejected_status_for_precommit_approval_failure(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "precommit-rejected.sqlite3"
    report_path = tmp_path / "precommit-rejected-preflight.json"
    connection = initialize_database(db_path)
    _insert_unconfirmed_boundary(connection)
    _resolve(connection)
    connection.close()
    preview = tool.run_fence_event_preflight(
        db_path,
        action="RELEASE",
        command_id=COMMAND_ID,
        command_alias="U01",
        approval_id="fence-release-jul23-u01",
        request_id="fence-release-precommit-rejected",
        operator_id="operator.bootstrap",
        trade_date="2026-07-23",
        out_path=report_path,
    )
    monkeypatch.setattr(tool, "market_today", lambda: "2026-07-23")
    monkeypatch.setattr(tool, "_assert_apply_runtime_safe", lambda *args: None)
    monkeypatch.setattr(tool, "load_settings", lambda: cast(Settings, object()))
    args = _apply_cli_args(db_path, report_path, preview)
    args[args.index("--approval-sha256") + 1] = "0" * 64

    exit_code = tool.main(args)
    payload = json.loads(capsys.readouterr().out)
    readback = sqlite3.connect(db_path)
    event_count = readback.execute(
        "SELECT COUNT(*) FROM gateway_order_broker_boundary_fence_events"
    ).fetchone()[0]
    readback.close()

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert event_count == 0


def _apply_cli_args(db_path, report_path, preview) -> list[str]:
    return [
        "--db",
        str(db_path),
        "--apply",
        "--preflight-report",
        str(report_path),
        "--preflight-report-sha256",
        str(preview["report_sha256"]),
        "--approval-sha256",
        str(preview["approval"]["expected_sha256"]),
        "--acknowledge-append-only-fence-change",
        "--acknowledge-raw-history-preserved",
        "--acknowledge-late-evidence-fail-closed",
    ]


def _resolve(connection) -> None:
    preview = preview_order_broker_boundary_resolution(connection, COMMAND_ID)
    record_order_broker_boundary_resolution(
        connection,
        command_id=COMMAND_ID,
        request_id="request-resolution-fence-tool",
        expected_fingerprint=str(preview["source_boundary_fingerprint"]),
        reason_code=REASON_CODE,
        evidence_type=EVIDENCE_TYPE,
        evidence_ref=EVIDENCE_REF,
        evidence_sha256=EVIDENCE_SHA256,
        operator_id=OPERATOR_ID,
    )
