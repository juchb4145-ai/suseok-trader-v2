from __future__ import annotations

import json
from datetime import timedelta

from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import LiveSimIntentStatus
from fastapi.testclient import TestClient
from services.config import Settings, TradingMode, TradingProfile
from services.entry_timing.service import evaluate_entry_timing
from services.live_sim.order_plan_eligibility import evaluate_live_sim_order_plan_eligibility
from services.live_sim.order_plan_intent import create_live_sim_intent_from_order_plan
from services.risk_gate import evaluate_risk_for_candidate, save_risk_observation
from services.runtime.live_sim_pilot_pipeline import run_live_sim_pilot_pipeline_once
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation
from storage.sqlite import initialize_database
from tests.test_entry_timing import _raise_fixture_turnover
from tests.test_live_sim import _insert_live_sim_position, _mark_gateway_ready
from tests.test_strategy_service import _insert_strategy_fixture


def test_order_plan_eligibility_and_intent_create_without_command(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(tmp_path / "plan-intent.sqlite3")
    settings = _pilot_settings()

    eligibility = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=settings,
    )
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=settings,
    )
    command_count = _count(connection, "gateway_commands")
    connection.close()

    assert eligibility.eligible is True
    assert eligibility.live_real_allowed is False
    assert eligibility.evidence_json["admission_trace"]["policy"] == "live_sim_order_plan"
    assert eligibility.evidence_json["admission_trace"]["reason_codes"] == []
    assert "candidate_context" in eligibility.candidate_evidence
    assert intent.status is LiveSimIntentStatus.CREATED
    assert intent.order_type.value == "LIMIT"
    assert intent.side.value == "BUY"
    assert intent.evidence_json["order_plan_id"] == order_plan_id
    assert intent.evidence_json["not_order_intent_source"] is True
    assert intent.evidence_json["converted_to_live_sim_intent"] is True
    assert command_count == 0


def test_pilot_run_once_default_queue_false_creates_intent_only(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "run-no-queue.sqlite3")
    settings = _pilot_settings(live_sim_pilot_auto_queue_command=True)

    result = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=False,
    )
    command_count = _count(connection, "gateway_commands")
    connection.close()

    assert result.evaluated_count == 1
    assert result.eligible_count == 1
    assert result.intent_count == 1
    assert result.command_count == 0
    assert command_count == 0


def test_pilot_run_once_queues_command_and_duplicate_run_does_not_queue_again(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "run-queue.sqlite3")
    settings = _pilot_settings(live_sim_pilot_auto_queue_command=True)

    first = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=True,
    )
    second = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=True,
    )
    command_row = connection.execute(
        "SELECT command_type, source, payload_json FROM gateway_commands"
    ).fetchone()
    payload = json.loads(command_row["payload_json"])
    command_count = _count(connection, "gateway_commands")
    connection.close()

    assert first.command_count == 1
    assert second.command_count == 0
    assert second.rejection_count >= 1
    assert command_count == 1
    assert command_row["command_type"] == "send_order"
    assert command_row["source"] == "live_sim"
    assert payload["mode"] == "LIVE_SIM"
    assert payload["live_mode"] == "LIVE_SIM"
    assert payload["live_sim_only"] is True
    assert payload["live_real_allowed"] is False
    assert payload["metadata"]["live_sim_only"] is True
    assert payload["metadata"]["live_real_allowed"] is False


def test_pilot_disabled_records_rejection_without_command(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "pilot-disabled.sqlite3")
    settings = _pilot_settings(live_sim_pilot_pipeline_enabled=False)

    result = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=True,
    )
    rejection = connection.execute("SELECT reason_codes_json FROM live_sim_rejections").fetchone()
    connection.close()

    assert result.status == "BLOCKED"
    assert result.command_count == 0
    assert LiveSimReasonCode.PILOT_PIPELINE_DISABLED.value in json.loads(
        rejection["reason_codes_json"]
    )


def test_order_plan_rejects_plan_state_price_tick_strategy_risk_and_dry_run_modes(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(tmp_path / "plan-rejects.sqlite3")
    settings = _pilot_settings()

    _update_plan(connection, order_plan_id, status="WAIT_RETRY")
    not_ready = evaluate_live_sim_order_plan_eligibility(connection, order_plan_id, settings)
    _update_plan(connection, order_plan_id, status="PLAN_READY")
    _update_plan(connection, order_plan_id, expires_at="2020-01-01T00:00:00Z")
    expired = evaluate_live_sim_order_plan_eligibility(connection, order_plan_id, settings)
    _update_plan(
        connection,
        order_plan_id,
        expires_at=datetime_to_wire(utc_now() + timedelta(seconds=300)),
    )
    connection.execute(
        """
        UPDATE order_plan_drafts
        SET evidence_json = ?
        WHERE order_plan_id = ?
        """,
        (json.dumps({"order_type": "MARKET"}), order_plan_id),
    )
    connection.execute(
        """
        UPDATE order_plan_drafts_latest
        SET evidence_json = ?
        WHERE order_plan_id = ?
        """,
        (json.dumps({"order_type": "MARKET"}), order_plan_id),
    )
    market = evaluate_live_sim_order_plan_eligibility(connection, order_plan_id, settings)
    connection.execute(
        """
        UPDATE order_plan_drafts
        SET evidence_json = ?
        WHERE order_plan_id = ?
        """,
        (json.dumps({"order_type": "LIMIT"}), order_plan_id),
    )
    connection.execute(
        """
        UPDATE order_plan_drafts_latest
        SET evidence_json = ?
        WHERE order_plan_id = ?
        """,
        (json.dumps({"order_type": "LIMIT"}), order_plan_id),
    )
    limit_with_market_flag_settings = _pilot_settings()
    object.__setattr__(
        limit_with_market_flag_settings,
        "live_sim_order_plan_allow_market_order",
        True,
    )
    limit_with_market_flag = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=limit_with_market_flag_settings,
    )
    connection.execute(
        "UPDATE market_ticks_latest SET price = 98000 WHERE code = '005930'"
    )
    drift = evaluate_live_sim_order_plan_eligibility(connection, order_plan_id, settings)
    connection.execute(
        "UPDATE market_ticks_latest SET price = 97000, event_ts = '2020-01-01T00:00:00Z'"
        " WHERE code = '005930'"
    )
    stale = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(live_sim_order_plan_stale_sec=1),
    )
    connection.execute(
        """
        UPDATE strategy_observations_latest
        SET overall_status = 'NO_SETUP'
        WHERE candidate_instance_id = 'CAND-2026-06-27-005930-1'
        """
    )
    strategy = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(live_sim_order_plan_stale_sec=999_999_999),
    )
    connection.execute(
        """
        UPDATE strategy_observations_latest
        SET overall_status = 'MATCHED_OBSERVATION'
        WHERE candidate_instance_id = 'CAND-2026-06-27-005930-1'
        """
    )
    connection.execute(
        """
        UPDATE risk_observations_latest
        SET overall_status = 'OBSERVE_BLOCK'
        WHERE candidate_instance_id = 'CAND-2026-06-27-005930-1'
        """
    )
    risk = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(live_sim_order_plan_stale_sec=999_999_999),
    )
    dry_run = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(
            live_sim_order_plan_stale_sec=999_999_999,
            live_sim_order_plan_require_dry_run_evidence=True,
        ),
    )
    connection.close()

    assert LiveSimReasonCode.ORDER_PLAN_NOT_READY.value in not_ready.reason_codes
    assert LiveSimReasonCode.ORDER_PLAN_EXPIRED.value in expired.reason_codes
    assert LiveSimReasonCode.ORDER_PLAN_MARKET_ORDER_NOT_ALLOWED.value in market.reason_codes
    assert limit_with_market_flag.eligible is True
    assert (
        LiveSimReasonCode.ORDER_PLAN_MARKET_ORDER_NOT_ALLOWED.value
        not in limit_with_market_flag.reason_codes
    )
    assert LiveSimReasonCode.ORDER_PLAN_PRICE_DRIFT_EXCEEDED.value in drift.reason_codes
    assert LiveSimReasonCode.ORDER_PLAN_LATEST_TICK_STALE.value in stale.reason_codes
    assert LiveSimReasonCode.ORDER_PLAN_STRATEGY_NOT_MATCHED.value in strategy.reason_codes
    assert LiveSimReasonCode.ORDER_PLAN_RISK_NOT_PASS.value in risk.reason_codes
    assert LiveSimReasonCode.ORDER_PLAN_DRY_RUN_EVIDENCE_MISSING.value in dry_run.reason_codes


def test_order_plan_rejects_kill_switch_live_real_and_limits(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(tmp_path / "plan-limits.sqlite3")

    kill_switch = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(live_sim_kill_switch=True),
    )
    live_real = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(trading_allow_live_real=True),
    )
    _insert_live_sim_order(connection, status="COMMAND_QUEUED", order_id="active-order")
    active_order = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(),
    )
    connection.execute("DELETE FROM live_sim_orders")
    _insert_live_sim_order(connection, status="FILLED", order_id="filled-position")
    active_position = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(),
    )
    daily_limit = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(live_sim_max_daily_order_count=1),
    )
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="CLOSED",
        quantity=0,
        realized_pnl=-120_000,
    )
    daily_loss = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(live_sim_max_daily_loss=100_000),
    )
    connection.close()

    assert LiveSimReasonCode.LIVE_SIM_KILL_SWITCH_ACTIVE.value in kill_switch.reason_codes
    assert LiveSimReasonCode.LIVE_REAL_NOT_ALLOWED.value in live_real.reason_codes
    assert live_real.live_real_allowed is False
    assert LiveSimReasonCode.ACTIVE_ORDER_LIMIT_EXCEEDED.value in active_order.reason_codes
    assert LiveSimReasonCode.ACTIVE_POSITION_LIMIT_EXCEEDED.value in active_position.reason_codes
    assert LiveSimReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value in daily_limit.reason_codes
    assert LiveSimReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value in daily_loss.reason_codes


def test_live_sim_order_plan_api_routes(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "plan-api.sqlite3"
    connection, order_plan_id = _prepared_order_plan_connection(db_path)
    connection.close()
    _set_pilot_api_env(monkeypatch, db_path)

    with TestClient(app) as client:
        eligibility = client.get(
            "/api/live-sim/order-plan-eligibility",
            params={"order_plan_id": order_plan_id},
        )
        unauthorized = client.post(f"/api/live-sim/intents/from-order-plan/{order_plan_id}")
        created = client.post(
            f"/api/live-sim/intents/from-order-plan/{order_plan_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        runs = client.get("/api/live-sim/pilot/runs")

    assert eligibility.status_code == 200
    assert eligibility.json()["eligibility"]["eligible"] is True
    assert unauthorized.status_code == 401
    assert created.status_code == 200
    assert created.json()["intent"]["evidence_json"]["order_plan_id"] == order_plan_id
    assert runs.status_code == 200


def _prepared_order_plan_connection(path):
    connection = initialize_database(path)
    settings = _pilot_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _raise_fixture_turnover(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, risk)
    result = evaluate_entry_timing(
        connection,
        candidate_instance_id=candidate_id,
        settings=settings,
    )
    _mark_gateway_ready(connection)
    return connection, result.order_plan_drafts[0].order_plan_id


def _pilot_settings(**overrides) -> Settings:
    values = {
        "trading_profile": TradingProfile.LIVE_SIM_PILOT,
        "trading_mode": TradingMode.LIVE_SIM,
        "trading_allow_live_sim": True,
        "trading_allow_live_real": False,
        "live_sim_enabled": True,
        "live_sim_order_routing_enabled": True,
        "live_sim_gateway_command_enabled": True,
        "live_sim_account_id": "SIM-12345678",
        "live_sim_kill_switch": False,
        "live_sim_stale_tick_sec": 999_999_999,
        "live_sim_max_order_notional": 100_000,
        "live_sim_max_daily_notional": 300_000,
        "live_sim_pilot_pipeline_enabled": True,
        "live_sim_pilot_auto_queue_command": False,
        "live_sim_order_plan_routing_enabled": True,
        "live_sim_order_plan_stale_sec": 999_999_999,
        "market_data_tick_stale_sec": 999_999_999,
        "market_data_degraded_tick_stale_sec": 999_999_999,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
        "strategy_engine_stale_tick_sec": 999_999_999,
        "risk_gate_stale_tick_sec": 999_999_999,
        "risk_gate_strategy_stale_sec": 999_999_999,
        "entry_timing_stale_max_seconds": 999_999_999,
        "entry_timing_min_turnover_krw": 1_000,
        "entry_timing_min_execution_strength": 1,
    }
    values.update(overrides)
    return Settings(**values)


def _update_plan(connection, order_plan_id: str, **values) -> None:
    for table in ("order_plan_drafts", "order_plan_drafts_latest"):
        for key, value in values.items():
            connection.execute(
                f"UPDATE {table} SET {key} = ? WHERE order_plan_id = ?",
                (value, order_plan_id),
            )
    connection.commit()


def _insert_live_sim_order(connection, *, status: str, order_id: str) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id,
            live_sim_intent_id,
            trade_date,
            account_id,
            code,
            name,
            side,
            order_type,
            quantity,
            limit_price,
            notional,
            status,
            filled_quantity,
            remaining_quantity,
            idempotency_key,
            created_at
        )
        VALUES (?, ?, '2026-06-27', 'SIM-12345678', '005930', '삼성전자', 'BUY',
            'LIMIT', 1, 97000, 97000, ?, 0, 1, ?, ?)
        """,
        (order_id, f"intent-{order_id}", status, f"key-{order_id}", now),
    )
    connection.commit()


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _set_pilot_api_env(monkeypatch, db_path) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("TRADING_PROFILE", "LIVE_SIM_PILOT")
    monkeypatch.setenv("TRADING_MODE", "LIVE_SIM")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "true")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("LIVE_SIM_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_GATEWAY_COMMAND_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ACCOUNT_ID", "SIM-12345678")
    monkeypatch.setenv("LIVE_SIM_KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_SIM_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("LIVE_SIM_PILOT_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND", "false")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_STALE_SEC", "999999999")
