from __future__ import annotations

import json
import sqlite3
from datetime import timedelta

import pytest
import services.runtime.market_open_observe_cycle as observe_cycle
from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event, make_price_tick_event
from services.config import Settings, candidate_timezone
from services.market_context_service import rebuild_market_context_snapshots
from services.market_index_service import process_market_index_event
from services.market_reference_service import process_market_symbols_event
from services.runtime.evaluation_run_guard import EVALUATION_PIPELINE_LOCK
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.sqlite import initialize_database, open_connection
from tests.test_market_index_service import index_tick_event
from tools.run_market_open_observe_cycle import (
    sanitize_observe_cycle_payload,
    write_observe_cycle_report,
)


def test_mock_events_project_and_observe_cycle_records_stage_updates(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "market_open_observe_cycle.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("MARKET_DATA_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_SOURCE_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("CANDIDATE_EPISODE_TTL_SEC", "999999999")
    monkeypatch.setenv("THEME_MIN_OBSERVABLE_MEMBERS", "1")
    monkeypatch.setenv("STRATEGY_ENGINE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("RISK_GATE_STRATEGY_STALE_SEC", "999999999")
    monkeypatch.setenv("ENTRY_TIMING_STALE_MAX_SECONDS", "999999999")
    trade_date = utc_now().astimezone(candidate_timezone("Asia/Seoul")).date().isoformat()

    with TestClient(app) as client:
        headers = {"X-Local-Token": "test-token"}
        tick = client.post(
            "/api/gateway/events",
            json=make_price_tick_event(
                code="005930",
                name="삼성전자",
                price=97_000,
                change_rate=2.0,
                volume=10_000,
                trade_value=970_000_000,
                execution_strength=130.0,
                day_high=100_000,
                day_low=94_000,
            ).to_dict(),
            headers=headers,
        )
        condition = client.post(
            "/api/gateway/events",
            json=make_condition_event(
                code="005930",
                name="삼성전자",
                condition_name="LeaderCondition",
                price=97_000,
                metadata=_condition_profile_metadata("LEADER", "LeaderCondition", 90),
            ).to_dict(),
            headers=headers,
        )
        latest_ticks = client.get("/api/market-data/ticks/latest")
        theme_import = client.post("/api/themes/import", json=_theme_payload(), headers=headers)
        _insert_fresh_market_inputs(db_path)
        result = client.post(
            f"/api/operator/observe-cycle/run-once?trade_date={trade_date}",
            headers=headers,
        )
        latest_run = client.get("/api/operator/observe-cycle/runs/latest")
        commands = client.get("/api/gateway/commands/status")

    payload = result.json()
    stages = payload["stage_summary"]

    assert tick.json()["projection_status"] == "APPLIED"
    assert condition.json()["projection_status"] == "APPLIED"
    assert latest_ticks.json()["ticks"][0]["code"] == "005930"
    assert theme_import.status_code == 200
    assert result.status_code == 200
    assert stages["Theme"]["status"] in {"PASS", "WARN"}
    assert stages["RealtimeSubscription"]["counts"]["queue_commands"] is False
    assert stages["RealtimeSubscription"]["counts"]["planned_register_count"] >= 0
    assert stages["Candidate"]["counts"]["active_candidate_count"] >= 1
    assert stages["ConditionFusion"]["counts"]["profile_count"] >= 1
    assert stages["ConditionFusion"]["counts"]["fused_code_count"] >= 1
    assert stages["ConditionFusion"]["counts"]["promoted_condition_source_count"] >= 1
    assert stages["Strategy"]["counts"]["evaluated_count"] >= 0
    assert stages["Risk"]["counts"]["evaluated_count"] >= 0
    assert stages["EntryTiming"]["counts"]["evaluated_count"] >= 1
    assert stages["Strategy"]["counts"]["matched_observation_count"] >= 1
    assert stages["Risk"]["counts"]["observe_pass_count"] >= 1
    assert stages["EntryTiming"]["counts"]["order_plan_draft_count"] >= 1
    assert stages["CommandSafety"]["status"] == "PASS"
    assert payload["send_order_delta"] == 0
    assert payload["queue_commands"] is False
    assert payload["elapsed_sec"] >= 0
    assert all(stage["elapsed_sec"] >= 0 for stage in stages.values())
    assert latest_run.json()["run"]["run_id"] == payload["run_id"]
    assert latest_run.json()["run"]["elapsed_sec"] == payload["elapsed_sec"]
    assert stages["Candidate"]["details"]["freshness_reference_at"] == payload["created_at"]
    assert commands.json()["counts"].get("QUEUED", 0) == 0

    connection = open_connection(db_path)
    try:
        send_order_count = connection.execute(
            "SELECT COUNT(*) AS count FROM gateway_commands WHERE command_type = 'send_order'"
        ).fetchone()["count"]
        matched_strategy_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM strategy_observations
            WHERE overall_status = 'MATCHED_OBSERVATION'
            """
        ).fetchone()["count"]
        risk_pass_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM risk_observations
            WHERE overall_status = 'OBSERVE_PASS'
            """
        ).fetchone()["count"]
        entry_timing_count = connection.execute(
            "SELECT COUNT(*) AS count FROM entry_timing_evaluations"
        ).fetchone()["count"]
        order_plan_count = connection.execute(
            "SELECT COUNT(*) AS count FROM order_plan_drafts"
        ).fetchone()["count"]
        unsafe_plan_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM order_plan_drafts
            WHERE observe_only != 1 OR not_order_intent != 1
            """
        ).fetchone()["count"]
    finally:
        connection.close()
    assert send_order_count == 0
    assert matched_strategy_count >= 1
    assert risk_pass_count >= 1
    assert entry_timing_count >= 1
    assert order_plan_count >= 1
    assert unsafe_plan_count == 0


def test_observe_cycle_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "observe_cycle_token.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "local-secret")

    with TestClient(app) as client:
        missing = client.post("/api/operator/observe-cycle/run-once")
        wrong = client.post(
            "/api/operator/observe-cycle/run-once",
            headers={"X-Core-Token": "wrong"},
        )
        accepted = client.post(
            "/api/operator/observe-cycle/run-once",
            headers={"X-Core-Token": "local-secret"},
        )
        auth_probe_missing = client.get("/api/gateway/auth/probe")
        auth_probe_ok = client.get(
            "/api/gateway/auth/probe",
            headers={"X-Core-Token": "local-secret"},
        )

    assert missing.status_code == 401
    assert wrong.status_code == 403
    assert accepted.status_code == 200
    assert auth_probe_missing.status_code == 401
    assert auth_probe_ok.status_code == 200
    assert auth_probe_ok.json()["read_only"] is True


def test_observe_cycle_returns_conflict_when_evaluation_lock_is_active(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "observe_cycle_locked.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    initialize_database(db_path).close()

    with TestClient(app) as client:
        connection = open_connection(db_path)
        now = utc_now()
        connection.execute(
            """
            INSERT INTO runtime_execution_locks (
                lock_name,
                owner_id,
                acquired_at,
                expires_at,
                detail_json
            )
            VALUES (?, 'test-owner', ?, ?, ?)
            """,
            (
                EVALUATION_PIPELINE_LOCK,
                datetime_to_wire(now),
                datetime_to_wire(now + timedelta(seconds=300)),
                canonical_json({"test": True}),
            ),
        )
        connection.commit()
        connection.close()
        response = client.post(
            "/api/operator/observe-cycle/run-once",
            headers={"X-Local-Token": "test-token"},
        )

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "EVALUATION_RUN_LOCKED"


def test_observe_cycle_uses_fixed_cycle_time_without_stage_replay(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "observe_cycle_transient_lock.sqlite3"
    connection = initialize_database(db_path)
    connection.execute(
        """
        CREATE TABLE observe_cycle_retry_probe (
            stage TEXT PRIMARY KEY,
            calculated_at TEXT
        )
        """
    )
    connection.commit()
    settings = Settings(
        trading_db_path=db_path,
        operator_sqlite_lock_retry_attempts=3,
        operator_sqlite_lock_retry_base_sleep_sec=0,
        operator_sqlite_lock_retry_max_sleep_sec=0,
        operator_sqlite_busy_timeout_ms=37,
    )
    _stub_non_retry_observe_cycle_stages(monkeypatch)
    theme_calculated_at = []
    theme_call_count = 0
    candidate_call_count = 0

    def calculate_theme_snapshots(
        actual_connection,
        *,
        calculated_at,
        settings,
        **kwargs,
    ):
        del settings, kwargs
        nonlocal theme_call_count
        theme_call_count += 1
        theme_calculated_at.append(calculated_at)
        actual_connection.execute(
            """
            INSERT INTO observe_cycle_retry_probe (stage, calculated_at)
            VALUES ('Theme', ?)
            """,
            (datetime_to_wire(calculated_at),),
        )
        actual_connection.commit()
        return _StubResult(
            {
                "processed_theme_count": 1,
                "snapshot_count": 1,
                "error_count": 0,
            }
        )

    def rebuild_candidates(actual_connection, *args, **kwargs):
        del args, kwargs
        nonlocal candidate_call_count
        candidate_call_count += 1
        actual_connection.execute(
            """
            INSERT INTO observe_cycle_retry_probe (stage, calculated_at)
            VALUES ('Candidate', NULL)
            """
        )
        actual_connection.commit()
        return _StubResult(_candidate_rebuild_payload())

    monkeypatch.setattr(
        observe_cycle,
        "calculate_all_theme_snapshots",
        calculate_theme_snapshots,
    )
    monkeypatch.setattr(
        observe_cycle,
        "rebuild_candidates_from_observations",
        rebuild_candidates,
    )

    try:
        result = observe_cycle.run_market_open_observe_cycle_once(
            connection,
            trade_date="2026-07-24",
            settings=settings,
            write_run=False,
        )
        probe_rows = connection.execute(
            """
            SELECT stage, calculated_at
            FROM observe_cycle_retry_probe
            ORDER BY stage
            """
        ).fetchall()
        busy_timeout_ms = int(connection.execute("PRAGMA busy_timeout").fetchone()[0])
    finally:
        connection.close()

    assert theme_call_count == 1
    assert candidate_call_count == 1
    assert datetime_to_wire(theme_calculated_at[0]) == result.created_at
    assert [(row["stage"], row["calculated_at"]) for row in probe_rows] == [
        ("Candidate", None),
        ("Theme", result.created_at),
    ]
    assert busy_timeout_ms == 37
    for stage_name in ("Theme", "Candidate"):
        stage = result.stages[stage_name]
        assert stage.status == "PASS"
        assert "sqlite_lock_retry_count" not in stage.counts
        assert "sqlite_lock_retry_count" not in stage.details
    assert result.send_order_delta == 0
    assert result.order_command_delta == {
        "cancel_order": 0,
        "modify_order": 0,
        "send_order": 0,
    }


def test_observe_cycle_reports_candidate_partial_progress_without_stage_replay(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "observe_cycle_exhausted_lock.sqlite3"
    connection = initialize_database(db_path)
    connection.execute(
        """
        CREATE TABLE observe_cycle_retry_probe (
            stage TEXT PRIMARY KEY
        )
        """
    )
    connection.commit()
    settings = Settings(
        trading_db_path=db_path,
        operator_sqlite_lock_retry_attempts=3,
        operator_sqlite_lock_retry_base_sleep_sec=0,
        operator_sqlite_lock_retry_max_sleep_sec=0,
        operator_sqlite_busy_timeout_ms=25,
    )
    _stub_non_retry_observe_cycle_stages(monkeypatch)
    candidate_call_count = 0

    def rebuild_candidates(actual_connection, *args, **kwargs):
        del args, kwargs
        nonlocal candidate_call_count
        candidate_call_count += 1
        actual_connection.execute(
            "INSERT INTO observe_cycle_retry_probe (stage) VALUES ('Candidate')"
        )
        actual_connection.commit()
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(
        observe_cycle,
        "rebuild_candidates_from_observations",
        rebuild_candidates,
    )

    try:
        result = observe_cycle.run_market_open_observe_cycle_once(
            connection,
            trade_date="2026-07-24",
            settings=settings,
            write_run=False,
        )
        probe_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM observe_cycle_retry_probe"
            ).fetchone()["count"]
        )
    finally:
        connection.close()

    candidate_stage = result.stages["Candidate"]
    assert candidate_call_count == 1
    assert probe_count == 1
    assert candidate_stage.status == "BLOCK"
    assert candidate_stage.reason_codes == (
        "CANDIDATE_REBUILD_NOT_RUN",
        "CANDIDATE_REBUILD_INCOMPLETE",
        "SQLITE_DATABASE_LOCKED",
    )
    assert candidate_stage.counts["stage_replay_count"] == 0
    assert candidate_stage.details["retryable"] is True
    assert (
        candidate_stage.details["automatic_retry_scope"]
        == "SHORT_WRITE_TRANSACTION_BEGIN"
    )
    assert candidate_stage.details["transaction_begin_retry_count_available"] is False
    assert candidate_stage.details["stage_replayed"] is False
    assert candidate_stage.details["partial_write_possible"] is True
    assert candidate_stage.details["completion_state"] == "PARTIAL_POSSIBLE"
    assert result.send_order_delta == 0
    assert all(delta == 0 for delta in result.order_command_delta.values())


def test_observe_cycle_does_not_replay_theme_after_durable_lock_failure(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "observe_cycle_theme_no_replay.sqlite3"
    connection = initialize_database(db_path)
    connection.execute(
        """
        CREATE TABLE observe_cycle_retry_probe (
            stage TEXT PRIMARY KEY
        )
        """
    )
    connection.commit()
    settings = Settings(
        trading_db_path=db_path,
        operator_sqlite_busy_timeout_ms=25,
    )
    _stub_non_retry_observe_cycle_stages(monkeypatch)
    theme_call_count = 0

    def calculate_theme_snapshots(actual_connection, *args, **kwargs):
        del args, kwargs
        nonlocal theme_call_count
        theme_call_count += 1
        actual_connection.execute(
            "INSERT INTO observe_cycle_retry_probe (stage) VALUES ('Theme')"
        )
        actual_connection.commit()
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(
        observe_cycle,
        "calculate_all_theme_snapshots",
        calculate_theme_snapshots,
    )

    try:
        result = observe_cycle.run_market_open_observe_cycle_once(
            connection,
            trade_date="2026-07-24",
            settings=settings,
            write_run=False,
        )
        probe_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM observe_cycle_retry_probe"
            ).fetchone()["count"]
        )
    finally:
        connection.close()

    theme_stage = result.stages["Theme"]
    assert theme_call_count == 1
    assert probe_count == 1
    assert theme_stage.status == "BLOCK"
    assert theme_stage.reason_codes == (
        "THEME_SNAPSHOT_NOT_BUILT",
        "THEME_STAGE_INCOMPLETE",
        "SQLITE_DATABASE_LOCKED",
    )
    assert theme_stage.counts["stage_replay_count"] == 0
    assert theme_stage.details["stage_replayed"] is False
    assert theme_stage.details["partial_write_possible"] is True
    assert theme_stage.details["completion_state"] == "PARTIAL_POSSIBLE"
    assert result.send_order_delta == 0
    assert all(delta == 0 for delta in result.order_command_delta.values())


@pytest.mark.parametrize(
    ("stage_name", "target_name", "legacy_reason", "incomplete_reason"),
    (
        (
            "Theme",
            "calculate_all_theme_snapshots",
            "THEME_SNAPSHOT_NOT_BUILT",
            "THEME_STAGE_INCOMPLETE",
        ),
        (
            "Candidate",
            "rebuild_candidates_from_observations",
            "CANDIDATE_REBUILD_NOT_RUN",
            "CANDIDATE_REBUILD_INCOMPLETE",
        ),
    ),
)
def test_observe_cycle_reports_non_lock_partial_progress_as_incomplete(
    tmp_path,
    monkeypatch,
    stage_name,
    target_name,
    legacy_reason,
    incomplete_reason,
) -> None:
    db_path = tmp_path / f"observe_cycle_{stage_name.lower()}_incomplete.sqlite3"
    connection = initialize_database(db_path)
    connection.execute(
        """
        CREATE TABLE observe_cycle_retry_probe (
            stage TEXT PRIMARY KEY
        )
        """
    )
    connection.commit()
    settings = Settings(trading_db_path=db_path)
    _stub_non_retry_observe_cycle_stages(monkeypatch)
    call_count = 0

    def commit_then_fail(actual_connection, *args, **kwargs):
        del args, kwargs
        nonlocal call_count
        call_count += 1
        actual_connection.execute(
            "INSERT INTO observe_cycle_retry_probe (stage) VALUES (?)",
            (stage_name,),
        )
        actual_connection.commit()
        raise RuntimeError("stage failed after durable progress")

    monkeypatch.setattr(observe_cycle, target_name, commit_then_fail)

    try:
        result = observe_cycle.run_market_open_observe_cycle_once(
            connection,
            trade_date="2026-07-24",
            settings=settings,
            write_run=False,
        )
        probe_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM observe_cycle_retry_probe"
            ).fetchone()["count"]
        )
    finally:
        connection.close()

    stage = result.stages[stage_name]
    assert call_count == 1
    assert probe_count == 1
    assert stage.status == "BLOCK"
    assert stage.reason_codes == (legacy_reason, incomplete_reason)
    assert "SQLITE_DATABASE_LOCKED" not in stage.reason_codes
    assert stage.details["stage_replayed"] is False
    assert stage.details["partial_write_possible"] is True
    assert stage.details["completion_state"] == "PARTIAL_POSSIBLE"
    assert result.send_order_delta == 0
    assert all(delta == 0 for delta in result.order_command_delta.values())


def test_observe_cycle_cli_report_writer_creates_json_and_markdown(tmp_path) -> None:
    payload = {
        "run_id": "cycle_1",
        "trade_date": "2026-06-29",
        "status": "COMPLETED",
        "stage_summary": {
            "EntryTiming": {
                "status": "PASS",
                "reason_codes": [],
                "summary": (
                    "failed account=1234-5678-90 "
                    "authorization=Bearer header-secret-value"
                ),
                "counts": {"evaluated_count": 1, "plan_ready_count": 1},
                "details": {
                    "preflight": {
                        "account_id": "sensitive-account-value",
                        "account_id_configured": True,
                        "operator_token": "sensitive-token-value",
                        "credentials": {
                            "bearer": "nested-credential-value",
                        },
                        "headers": {
                            "Authorization": "Bearer nested-header-value",
                            "authorization_backup": (
                                "Basic dXNlcjpwYXNzd29yZA=="
                            ),
                        },
                        "accountIds": ["2468135790"],
                        "accountNo": "1357-2468-90",
                        "APIKey": "acronym-api-key-value",
                        "APIToken": "acronym-api-token-value",
                        "operatorCredentials": {
                            "value": "prefixed-credential-value",
                        },
                        "httpAuthorization": "opaque-http-auth-value",
                        "계좌번호": "8642-1357-90",
                        "detail": {
                            "value": "account 4321-8765-00",
                        },
                        "source_watermark_hash": "abc12345678def",
                        "event_id": "evt_20260724060747816230",
                        "scan_id": "scan_20260724",
                        "idempotency_key": "cycle_1234567890_key",
                    }
                },
            },
            "CommandSafety": {
                "status": "PASS",
                "reason_codes": ["ORDER_COMMAND_ZERO_EXPECTED"],
                "summary": "No send_order GatewayCommand was created.",
                "counts": {"send_order_delta": 0},
            },
        },
        "command_counts_before": {},
        "command_counts_after": {},
        "send_order_count_before": 0,
        "send_order_count_after": 0,
        "send_order_delta": 0,
        "warnings": [],
        "errors": [
            {
                "error": (
                    "request failed for 9876-5432; "
                    "Bearer standalone-secret-value"
                    " credential=inline-credential-value"
                    " access_token=inline-access-token-value"
                    " operator_token=inline-operator-token-value"
                )
            }
        ],
        "created_at": "2026-06-29T09:01:02+09:00",
        "observe_only": True,
        "not_order_intent": True,
        "no_order_side_effects": True,
        "live_real_allowed": False,
        "real_order_allowed": False,
        "queue_commands": False,
        "order_controls_available": False,
    }

    paths = write_observe_cycle_report(payload, report_root=tmp_path / "observe")
    markdown = paths["run_md"].read_text(encoding="utf-8")
    saved_payload = paths["run_json"].read_text(encoding="utf-8")
    cli_payload = json.dumps(
        sanitize_observe_cycle_payload(payload),
        ensure_ascii=False,
    )

    assert paths["run_json"].exists()
    assert paths["run_md"].exists()
    assert "2026-06-29" in str(paths["run_json"])
    assert '"send_order_delta": 0' in saved_payload
    assert "sensitive-account-value" not in saved_payload
    assert "sensitive-token-value" not in saved_payload
    assert "nested-credential-value" not in saved_payload
    assert "nested-header-value" not in saved_payload
    assert "dXNlcjpwYXNzd29yZA==" not in saved_payload
    assert "acronym-api-key-value" not in saved_payload
    assert "acronym-api-token-value" not in saved_payload
    assert "prefixed-credential-value" not in saved_payload
    assert "opaque-http-auth-value" not in saved_payload
    assert "header-secret-value" not in saved_payload
    assert "standalone-secret-value" not in saved_payload
    assert "inline-credential-value" not in saved_payload
    assert "inline-access-token-value" not in saved_payload
    assert "inline-operator-token-value" not in saved_payload
    assert "1234-5678-90" not in saved_payload
    assert "9876-5432" not in saved_payload
    assert "2468135790" not in saved_payload
    assert "1357-2468-90" not in saved_payload
    assert "8642-1357-90" not in saved_payload
    assert "4321-8765-00" not in saved_payload
    assert '"account_id": "[REDACTED]"' in saved_payload
    assert '"operator_token": "[REDACTED]"' in saved_payload
    assert '"account_id_configured": true' in saved_payload
    assert '"source_watermark_hash": "abc12345678def"' in saved_payload
    assert '"event_id": "evt_20260724060747816230"' in saved_payload
    assert '"scan_id": "scan_20260724"' in saved_payload
    assert '"idempotency_key": "cycle_1234567890_key"' in saved_payload
    assert "nested-credential-value" not in markdown
    assert "header-secret-value" not in markdown
    assert "standalone-secret-value" not in markdown
    assert "nested-credential-value" not in cli_payload
    assert "header-secret-value" not in cli_payload
    assert "standalone-secret-value" not in cli_payload
    assert "CommandSafety" in markdown
    assert "send_order_delta: `0`" in markdown
    assert "queue_commands: `False`" in markdown


def _condition_profile_metadata(role: str, condition_name: str, priority: int) -> dict[str, object]:
    return {
        "sensor_evidence": True,
        "not_buy_signal": True,
        "condition_profile_id": f"profile-{condition_name}",
        "condition_role": role,
        "condition_profile": {
            "profile_id": f"profile-{condition_name}",
            "condition_name": condition_name,
            "role": role,
            "priority": priority,
            "ttl_sec": 999_999_999,
            "enabled": True,
            "price_subscribe_policy": "immediate",
        },
        "condition_admission": {
            "subscribed": True,
            "reason_codes": ["TEST"],
        },
    }


class _StubResult:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def to_dict(self, *args, **kwargs) -> dict[str, object]:
        del args, kwargs
        return dict(self.payload)


def _candidate_rebuild_payload() -> dict[str, object]:
    return {
        "source_event_count": 1,
        "candidate_created_count": 0,
        "candidate_updated_count": 1,
        "transition_count": 0,
        "context_refreshed_count": 1,
        "stale_count": 0,
        "closed_count": 0,
        "error_count": 0,
    }


def _stub_non_retry_observe_cycle_stages(monkeypatch) -> None:
    monkeypatch.setattr(
        observe_cycle,
        "run_realtime_subscription_once",
        lambda *args, **kwargs: _StubResult(
            {
                "status": "OK",
                "counts": {},
                "command_count": 0,
                "queue_commands": False,
            }
        ),
    )
    monkeypatch.setattr(
        observe_cycle,
        "get_theme_status",
        lambda *args, **kwargs: {
            "theme_count": 1,
            "active_theme_count": 1,
            "member_count": 1,
        },
    )
    monkeypatch.setattr(
        observe_cycle,
        "calculate_all_theme_snapshots",
        lambda *args, **kwargs: _StubResult(
            {
                "processed_theme_count": 1,
                "snapshot_count": 1,
                "error_count": 0,
            }
        ),
    )
    monkeypatch.setattr(
        observe_cycle,
        "rebuild_theme_leadership",
        lambda *args, **kwargs: _StubResult(
            {
                "status": "OK",
                "watchset": {"items": []},
                "candidate_apply_result": {},
            }
        ),
    )
    monkeypatch.setattr(
        observe_cycle,
        "rebuild_candidates_from_observations",
        lambda *args, **kwargs: _StubResult(_candidate_rebuild_payload()),
    )
    monkeypatch.setattr(
        observe_cycle,
        "get_candidate_status",
        lambda *args, **kwargs: {
            "candidate_count": 1,
            "active_candidate_count": 1,
        },
    )
    monkeypatch.setattr(observe_cycle, "list_condition_fusion", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        observe_cycle,
        "get_condition_profile_metrics",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        observe_cycle,
        "_condition_fusion_source_count",
        lambda *args, **kwargs: 0,
    )
    monkeypatch.setattr(
        observe_cycle,
        "evaluate_candidates",
        lambda *args, **kwargs: _StubResult(
            {
                "status": "OK",
                "candidate_count": 1,
                "evaluated_count": 0,
                "matched_observation_count": 0,
                "error_count": 0,
            }
        ),
    )
    monkeypatch.setattr(
        observe_cycle,
        "get_strategy_status",
        lambda *args, **kwargs: {"latest_observation_count": 0},
    )
    monkeypatch.setattr(
        observe_cycle,
        "evaluate_risk_observations",
        lambda *args, **kwargs: _StubResult(
            {
                "status": "OK",
                "strategy_observation_count": 0,
                "evaluated_count": 0,
                "observe_pass_count": 0,
                "block_count": 0,
                "error_count": 0,
            }
        ),
    )
    monkeypatch.setattr(
        observe_cycle,
        "get_risk_status",
        lambda *args, **kwargs: {"latest_observation_count": 0},
    )
    monkeypatch.setattr(
        observe_cycle,
        "evaluate_entry_timing",
        lambda *args, **kwargs: _StubResult(
            {
                "status": "OK",
                "candidate_count": 0,
                "evaluated_count": 0,
                "plan_ready_count": 0,
                "order_plan_drafts": [],
                "error_count": 0,
            }
        ),
    )
    monkeypatch.setattr(
        observe_cycle,
        "get_entry_timing_status",
        lambda *args, **kwargs: {"latest_plan_count": 0},
    )
    monkeypatch.setattr(
        observe_cycle,
        "get_live_sim_status",
        lambda *args, **kwargs: {
            "enabled": False,
            "kill_switch": True,
            "intent_count": 0,
            "order_count": 0,
            "open_order_count": 0,
            "open_position_count": 0,
        },
    )
    monkeypatch.setattr(
        observe_cycle,
        "run_live_sim_preflight",
        lambda *args, **kwargs: _StubResult({"status": "PASS"}),
    )


def _insert_fresh_market_inputs(db_path) -> None:
    connection = open_connection(db_path)
    try:
        now = utc_now()
        symbols = GatewayEvent(
            event_id="evt_market_symbols_observe_cycle",
            event_type="market_symbols",
            source="test-gateway",
            payload={
                "KOSPI": [{"code": "005930", "name": "삼성전자"}],
                "KOSDAQ": [{"code": "035420", "name": "NAVER"}],
            },
            ts=now,
        )
        append_gateway_event(connection, symbols)
        assert process_market_symbols_event(connection, symbols).status == "APPLIED"
        settings = Settings(market_index_stale_sec=999_999_999)
        for event in (
            index_tick_event(
                "evt_kospi_prev_observe_cycle",
                index_code="KOSPI",
                price=2800.0,
                ts=now - timedelta(minutes=5),
            ),
            index_tick_event(
                "evt_kospi_now_observe_cycle",
                index_code="KOSPI",
                price=2806.0,
                change_rate=0.21,
                change_value=6.0,
                ts=now,
            ),
            index_tick_event(
                "evt_kosdaq_prev_observe_cycle",
                index_code="KOSDAQ",
                price=1000.0,
                ts=now - timedelta(minutes=5),
            ),
            index_tick_event(
                "evt_kosdaq_now_observe_cycle",
                index_code="KOSDAQ",
                price=1002.0,
                change_rate=0.2,
                change_value=2.0,
                ts=now,
            ),
        ):
            append_gateway_event(connection, event)
            assert process_market_index_event(connection, event, settings=settings).status == (
                "APPLIED"
            )
        rebuild_market_context_snapshots(
            connection,
            settings=settings,
            generated_by="observe_cycle_fixture",
        )
        connection.commit()
    finally:
        connection.close()


def _theme_payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "observe_cycle_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [
                    {"code": "005930", "name": "삼성전자"},
                    {"code": "000660", "name": "SK하이닉스"},
                ],
            }
        ],
    }
