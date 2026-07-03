from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apps.core_api import app
from domain.broker.utils import datetime_to_wire, market_today, utc_now
from fastapi.testclient import TestClient
from services.ai_advisory.storage import save_scoring_run
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot
from services.live_sim.live_sim_service import reconcile_live_sim
from services.runtime.live_sim_operating_orchestrator import (
    OperatingMode,
    list_live_sim_operating_runs,
    run_live_sim_operating_cycle_once,
)
from services.runtime.preflight import (
    LiveSimPreflightResult,
    PreflightStatus,
    run_live_sim_preflight,
)
from storage.sqlite import initialize_database
from tests.test_live_sim_order_plan_pipeline import (
    _pilot_settings,
    _prepared_order_plan_connection,
    _set_pilot_api_env,
)


def test_operating_schema_config_and_mode_policy(tmp_path) -> None:
    connection = initialize_database(tmp_path / "operating-schema.sqlite3")
    tables = {
        row["name"]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    connection.close()
    settings = Settings()

    assert "live_sim_operating_runs" in tables
    assert settings.live_sim_operating_cycle_enabled is True
    assert settings.live_sim_operating_default_mode == "OBSERVE_CYCLE"
    assert OperatingMode.OBSERVE_CYCLE.observes_only is True
    assert OperatingMode.PILOT_BUY_ONLY.includes_buy is True
    assert OperatingMode.PILOT_FULL_LIFECYCLE.includes_lifecycle_commands is True
    assert OperatingMode.PROTECT_ONLY.includes_buy is False


def test_preflight_warn_block_cases_are_classified(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "preflight.sqlite3")
    settings = _operating_settings()
    reconcile_live_sim(connection, settings=settings)
    _insert_reconcile_block(connection)
    _save_ai_timeout(connection)

    reconcile_block = run_live_sim_preflight(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    kill_switch = run_live_sim_preflight(
        connection,
        settings=_operating_settings(live_sim_kill_switch=True),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    ai_warn = run_live_sim_preflight(
        connection,
        settings=_operating_settings(ai_candidate_scorer_enabled=True),
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=True,
        include_no_buy=False,
    )
    fee_warn = run_live_sim_preflight(
        connection,
        settings=settings,
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    broker_snapshot_required = run_live_sim_preflight(
        connection,
        settings=_operating_settings(live_sim_reconcile_request_broker_snapshot_enabled=True),
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    eod_warn = run_live_sim_preflight(
        connection,
        settings=_operating_settings(
            live_sim_exit_engine_enabled=True,
            live_sim_exit_eod_flatten_enabled=False,
        ),
        mode=OperatingMode.PROTECT_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    connection.close()

    assert reconcile_block.status is PreflightStatus.BLOCK
    assert any("reconcile" in reason for reason in reconcile_block.blocking_reasons)
    assert kill_switch.status is PreflightStatus.BLOCK
    assert _check_status(kill_switch, "live_sim_kill_switch") == "BLOCK"
    assert _check_status(ai_warn, "ai_advisory") == "WARN"
    assert ai_warn.status is not PreflightStatus.BLOCK
    assert _check_status(fee_warn, "fee_tax_config") == "WARN"
    assert _check_status(fee_warn, "theme_leadership") == "PASS"
    assert _check_status(fee_warn, "reconcile_latest_status") == "PASS"
    assert _check_status(fee_warn, "naver_import_recent") == "PASS"
    assert _check_status(broker_snapshot_required, "reconcile_latest_status") == "WARN"
    assert _check_status(eod_warn, "eod_flatten_config") == "WARN"
    assert eod_warn.status is not PreflightStatus.BLOCK


def test_preflight_ignores_unknown_gateway_status_lifecycle_errors(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(
        tmp_path / "preflight-unknown-status-event.sqlite3"
    )
    settings = _operating_settings()
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id,
            event_type,
            entity_type,
            status,
            reason,
            evidence_json,
            created_at,
            live_sim_only,
            live_real_allowed
        )
        VALUES (
            'benign-heartbeat-error',
            'LIFECYCLE_ERROR',
            'LIVE_SIM_ERROR',
            'ERROR',
            'UNKNOWN_LIVE_SIM_GATEWAY_EVENT',
            ?,
            ?,
            1,
            0
        )
        """,
        (
            json.dumps(
                {
                    "payload": {
                        "event_type": "heartbeat",
                        "payload": {"mode": "LIVE_SIM"},
                    }
                }
            ),
            now,
        ),
    )

    preflight = run_live_sim_preflight(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )

    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id,
            event_type,
            entity_type,
            entity_id,
            live_sim_order_id,
            status,
            reason,
            evidence_json,
            created_at,
            live_sim_only,
            live_real_allowed
        )
        VALUES (
            'real-order-lifecycle-error',
            'LIFECYCLE_ERROR',
            'LIVE_SIM_ORDER',
            'live-order-1',
            'live-order-1',
            'ERROR',
            'COMMAND_FAILED',
            '{}',
            ?,
            1,
            0
        )
        """,
        (now,),
    )
    blocked = run_live_sim_preflight(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    connection.close()

    assert _check_status(preflight, "lifecycle_error_count") == "PASS"
    assert _check_status(blocked, "lifecycle_error_count") == "BLOCK"
    assert any("lifecycle_error_count" in reason for reason in blocked.blocking_reasons)


def test_preflight_safety_preview_does_not_block_on_exhausted_buy_limit(
    tmp_path,
) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "preflight-buy-limit.sqlite3")
    settings = _operating_settings(live_sim_max_daily_order_count=1)
    _insert_today_buy_order(connection)

    preflight = run_live_sim_preflight(
        connection,
        settings=settings,
        mode=OperatingMode.PROTECT_ONLY,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    connection.close()

    assert _check_status(preflight, "live_sim_safety_gate_preview") == "PASS"
    assert preflight.safety_gate["purpose"] == "LIFECYCLE"
    assert preflight.safety_gate["daily_order_limit_exceeded"] is True
    assert "DAILY_ORDER_LIMIT_EXCEEDED" not in preflight.safety_gate["reason_codes"]


def test_preflight_external_llm_warn_only_for_external_ai_provider(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "preflight-ai-provider.sqlite3")

    mock_provider = run_live_sim_preflight(
        connection,
        settings=_operating_settings(
            ai_candidate_scorer_enabled=True,
            ai_candidate_scorer_provider="mock",
            ai_external_llm_enabled=False,
        ),
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=True,
        include_no_buy=False,
    )
    external_provider = run_live_sim_preflight(
        connection,
        settings=_operating_settings(
            ai_candidate_scorer_enabled=True,
            ai_candidate_scorer_provider="external_http",
            ai_external_llm_enabled=False,
        ),
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=True,
        include_no_buy=False,
    )
    connection.close()

    assert _check_status(mock_provider, "ai_advisory") == "PASS"
    assert _check_status(mock_provider, "external_llm") == "PASS"
    assert _check_status(external_provider, "external_llm") == "WARN"


def test_observe_and_queue_false_never_create_commands(tmp_path) -> None:
    observe_conn, _ = _prepared_order_plan_connection(tmp_path / "observe.sqlite3")
    observe = run_live_sim_operating_cycle_once(
        observe_conn,
        settings=_operating_settings(),
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    observe_commands = _count(observe_conn, "gateway_commands")
    observe_conn.close()

    queue_false_conn, _ = _prepared_order_plan_connection(tmp_path / "queue-false.sqlite3")
    queue_false = run_live_sim_operating_cycle_once(
        queue_false_conn,
        settings=_operating_settings(live_sim_pilot_auto_queue_command=True),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    queue_false_commands = _count(queue_false_conn, "gateway_commands")
    queue_false_conn.close()

    assert observe.buy_command_count == 0
    assert observe_commands == 0
    assert observe.stages["buy"]["status"] == "SKIPPED"
    assert queue_false.buy_evaluated_count == 1
    assert queue_false.buy_command_count == 0
    assert queue_false_commands == 0


def test_pilot_buy_only_can_queue_buy_but_never_cancel_exit(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "buy-only.sqlite3")
    settings = _operating_settings(
        live_sim_pilot_auto_queue_command=True,
        live_sim_operating_require_preflight_pass_for_queue=False,
        live_sim_cancel_enabled=True,
        live_sim_cancel_unfilled_enabled=True,
        live_sim_exit_engine_enabled=True,
        live_sim_exit_order_creation_enabled=True,
        live_sim_exit_gateway_command_enabled=True,
    )

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    command_rows = connection.execute(
        "SELECT command_type, payload_json FROM gateway_commands"
    ).fetchall()
    connection.close()

    assert result.buy_command_count == 1
    assert result.cancel_command_count == 0
    assert result.exit_command_count == 0
    assert len(command_rows) == 1
    assert command_rows[0]["command_type"] == "send_order"
    assert json.loads(command_rows[0]["payload_json"])["side"] == "BUY"


def test_protect_only_blocks_new_buy(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "protect.sqlite3")

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=_operating_settings(live_sim_pilot_auto_queue_command=True),
        mode=OperatingMode.PROTECT_ONLY,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    command_count = _count(connection, "gateway_commands")
    connection.close()

    assert result.buy_evaluated_count == 0
    assert result.buy_command_count == 0
    assert result.stages["buy"]["reason"] == "mode_disallows_buy"
    assert command_count == 0


def test_full_lifecycle_applies_operating_command_budget(tmp_path, monkeypatch) -> None:
    import services.runtime.live_sim_operating_orchestrator as orchestrator

    connection = initialize_database(tmp_path / "budget.sqlite3")
    settings = _operating_settings(
        live_sim_operating_require_preflight_pass_for_queue=False,
        live_sim_operating_max_buy_commands_per_cycle=1,
        live_sim_operating_max_cancel_commands_per_cycle=2,
        live_sim_operating_max_exit_commands_per_cycle=2,
        live_sim_order_plan_max_commands_per_run=5,
        live_sim_cancel_max_commands_per_run=5,
        live_sim_exit_max_commands_per_run=5,
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_preflight",
        lambda *args, **kwargs: LiveSimPreflightResult(
            status=PreflightStatus.PASS,
            mode=OperatingMode.PILOT_FULL_LIFECYCLE,
            queue_commands=True,
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_cancel_unfilled_once",
        lambda *args, settings, **kwargs: _FakeLifecycleResult(
            run_id="cancel",
            run_type="CANCEL",
            evaluated_count=5,
            command_count=settings.live_sim_cancel_max_commands_per_run,
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_exit_once",
        lambda *args, settings, **kwargs: _FakeLifecycleResult(
            run_id="exit",
            run_type="EXIT",
            evaluated_count=5,
            command_count=settings.live_sim_exit_max_commands_per_run,
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_pilot_pipeline_once",
        lambda *args, settings, **kwargs: _FakeBuyResult(
            run_id="buy",
            evaluated_count=5,
            command_count=settings.live_sim_order_plan_max_commands_per_run,
        ),
    )

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_FULL_LIFECYCLE,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    connection.close()

    assert result.buy_command_count == 1
    assert result.cancel_command_count == 2
    assert result.exit_command_count == 2


def test_reprice_consumes_operating_buy_budget(tmp_path, monkeypatch) -> None:
    import services.runtime.live_sim_operating_orchestrator as orchestrator

    connection = initialize_database(tmp_path / "reprice-budget.sqlite3")
    settings = _operating_settings(
        live_sim_reprice_enabled=True,
        live_sim_pilot_auto_queue_command=True,
        live_sim_operating_require_preflight_pass_for_queue=False,
        live_sim_operating_max_buy_commands_per_cycle=1,
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_preflight",
        lambda *args, **kwargs: LiveSimPreflightResult(
            status=PreflightStatus.PASS,
            mode=OperatingMode.PILOT_FULL_LIFECYCLE,
            queue_commands=True,
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_cancel_unfilled_once",
        lambda *args, **kwargs: _FakeLifecycleResult(
            run_id="cancel",
            run_type="CANCEL",
            evaluated_count=0,
            command_count=0,
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_exit_once",
        lambda *args, **kwargs: _FakeLifecycleResult(
            run_id="exit",
            run_type="EXIT",
            evaluated_count=0,
            command_count=0,
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_reprice_once",
        lambda *args, **kwargs: _FakeLifecycleResult(
            run_id="reprice",
            run_type="REPRICE",
            evaluated_count=1,
            command_count=1,
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_pilot_pipeline_once",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("buy pipeline should not run after reprice consumes budget")
        ),
    )

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_FULL_LIFECYCLE,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    connection.close()

    assert result.buy_command_count == 1
    assert result.reason_summary["command_counts"]["reprice_buy"] == 1
    assert result.stages["buy"]["reason"] == "reprice_consumed_buy_budget"


def test_preflight_block_forces_command_zero_and_run_is_saved(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "preflight-block.sqlite3")

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=_operating_settings(
            live_sim_kill_switch=True,
            live_sim_pilot_auto_queue_command=True,
        ),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    runs = list_live_sim_operating_runs(connection)
    command_count = _count(connection, "gateway_commands")
    connection.close()

    assert result.preflight.status is PreflightStatus.BLOCK
    assert result.buy_command_count == 0
    assert command_count == 0
    assert runs[0]["run_id"] == result.run_id
    assert runs[0]["live_real_allowed"] is False


def test_operator_api_and_dashboard_are_read_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "operator-api.sqlite3"
    connection, _ = _prepared_order_plan_connection(db_path)
    settings = _operating_settings()
    snapshot = build_dashboard_snapshot(connection, settings=settings)
    connection.close()
    _set_pilot_api_env(monkeypatch, db_path)
    monkeypatch.setenv("LIVE_SIM_OPERATING_REQUIRE_PREFLIGHT_PASS_FOR_QUEUE", "false")

    with TestClient(app) as client:
        runs = client.get("/api/live-sim/operator/runs")
        latest = client.get("/api/live-sim/operator/runs/latest")
        preflight = client.get("/api/live-sim/operator/preflight")
        status = client.get("/api/live-sim/operator/status")
        unauthorized = client.post("/api/live-sim/operator/run-once")
        posted = client.post(
            "/api/live-sim/operator/run-once",
            headers={"X-Local-Token": "secret-token"},
        )

    assert snapshot["live_sim"]["operating"]["read_only"] is True
    assert snapshot["live_sim"]["operating"]["run_buttons_available"] is False
    assert runs.status_code == 200
    assert runs.json()["no_order_side_effects"] is True
    assert latest.status_code == 200
    assert preflight.status_code == 200
    assert preflight.json()["no_order_side_effects"] is True
    assert status.status_code == 200
    assert status.json()["order_controls_available"] is False
    assert unauthorized.status_code == 401
    assert posted.status_code == 200
    assert posted.json()["buy_command_count"] == 0


def test_runtime_core_has_no_kiwoom_or_modify_order_imports() -> None:
    root = Path("services/runtime")
    forbidden = ("PyQt5", "QAxWidget", "Kiwoom", "modify_order")
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert not any(token in text for token in forbidden), path


@dataclass(frozen=True, kw_only=True)
class _FakeLifecycleResult:
    run_id: str
    run_type: str
    evaluated_count: int
    command_count: int
    signal_count: int = 0
    intent_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__


@dataclass(frozen=True, kw_only=True)
class _FakeBuyResult:
    run_id: str
    evaluated_count: int
    command_count: int
    status: str = "COMPLETED"

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__


def _operating_settings(**overrides) -> Settings:
    values = {
        "live_sim_operating_require_preflight_pass_for_queue": False,
        "live_sim_operating_include_ai": False,
        "live_sim_operating_include_no_buy": False,
    }
    values.update(overrides)
    return _pilot_settings(**values)


def _insert_reconcile_block(connection) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_reconcile_snapshots (
            reconcile_id,
            account_id,
            trade_date,
            mismatch_count,
            status,
            snapshot_json,
            created_at,
            blocking_new_buy,
            allow_exit
        )
        VALUES ('operating-reconcile-block', 'SIM-12345678', '2026-06-27', 1,
            'RECONCILE_MISMATCH', ?, ?, 1, 1)
        """,
        (json.dumps({"blocking_new_buy": True}), now),
    )
    connection.commit()


def _save_ai_timeout(connection) -> None:
    save_scoring_run(
        connection,
        run_id="ai-timeout",
        trade_date="2026-06-27",
        provider="mock",
        model="mock-model",
        status="TIMEOUT",
        candidate_count=1,
        selected_count=0,
        prompt_hash=None,
        raw_response_hash=None,
        summary=None,
        no_trade_reason=None,
        error_message="timeout",
    )
    connection.commit()


def _check_status(preflight: LiveSimPreflightResult, name: str) -> str:
    for check in preflight.checks:
        if check.name == name:
            return check.status.value
    raise AssertionError(f"missing preflight check: {name}")


def _insert_today_buy_order(connection) -> None:
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
        VALUES ('preflight-buy-limit', 'preflight-buy-intent', ?, 'SIM-12345678',
            '005930', '삼성전자', 'BUY', 'LIMIT', 1, 97000, 97000, 'FILLED',
            1, 0, 'preflight-buy-limit-key', ?)
        """,
        (market_today(), now),
    )
    connection.commit()


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
