from __future__ import annotations

from datetime import timedelta

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
from tools.run_market_open_observe_cycle import write_observe_cycle_report


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
    assert latest_run.json()["run"]["run_id"] == payload["run_id"]
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


def test_observe_cycle_cli_report_writer_creates_json_and_markdown(tmp_path) -> None:
    payload = {
        "run_id": "cycle_1",
        "trade_date": "2026-06-29",
        "status": "COMPLETED",
        "stage_summary": {
            "EntryTiming": {
                "status": "PASS",
                "reason_codes": [],
                "summary": "evaluated=1, drafts=1, ready=1",
                "counts": {"evaluated_count": 1, "plan_ready_count": 1},
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
        "errors": [],
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

    assert paths["run_json"].exists()
    assert paths["run_md"].exists()
    assert "2026-06-29" in str(paths["run_json"])
    assert '"send_order_delta": 0' in saved_payload
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
