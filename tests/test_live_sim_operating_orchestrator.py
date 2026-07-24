from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from apps.core_api import app
from domain.broker.commands import GatewayCommand
from domain.broker.utils import datetime_to_wire, market_today, utc_now
from fastapi.testclient import TestClient
from services.ai_advisory.storage import save_scoring_run
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot
from services.live_sim.live_sim_service import reconcile_live_sim
from services.runtime.evaluation_run_guard import EvaluationRunLockError
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
from services.runtime.theme_refresh_cycle import THEME_REFRESH_LOCK
from storage.gateway_command_store import enqueue_command
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


def test_preflight_passes_mirrored_historical_gateway_status_audit(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(
        tmp_path / "preflight-unknown-status-event.sqlite3"
    )
    settings = _operating_settings()
    now = datetime_to_wire(utc_now())
    runtime_status_event = {
        "event_id": "historical-heartbeat-event",
        "event_type": "heartbeat",
        "source": "kiwoom_gateway",
        "ts": now,
        "payload": {
            "mode": "LIVE_SIM",
            "live_sim_only": True,
            "live_real_allowed": False,
        },
    }
    connection.execute(
        """
        INSERT INTO live_sim_errors (
            error_message,
            payload_json,
            created_at
        )
        VALUES ('UNKNOWN_LIVE_SIM_GATEWAY_EVENT', ?, ?)
        """,
        (json.dumps(runtime_status_event), now),
    )
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
                    "run_id": None,
                    "code": None,
                    "payload": runtime_status_event,
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


def test_preflight_blocks_orphan_historical_gateway_status_lifecycle_row(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(
        tmp_path / "preflight-orphan-status-event.sqlite3"
    )
    settings = _operating_settings()
    now = datetime_to_wire(utc_now())
    runtime_status_event = {
        "event_id": "orphan-heartbeat-event",
        "event_type": "heartbeat",
        "source": "kiwoom_gateway",
        "ts": now,
        "payload": {
            "mode": "LIVE_SIM",
            "live_sim_only": True,
            "live_real_allowed": False,
        },
    }
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
            'orphan-heartbeat-error',
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
                    "run_id": None,
                    "code": None,
                    "payload": runtime_status_event,
                }
            ),
            now,
        ),
    )

    result = run_live_sim_preflight(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    check = _check(result, "lifecycle_error_count")
    connection.close()

    assert check.status is PreflightStatus.BLOCK
    assert check.details["effective_blocker_count"] >= 1
    assert check.details["manual_review_blocker_count"] >= 1
    assert "LIVE_SIM_LIFECYCLE_MIRROR_INCONSISTENT" in check.details[
        "qualification_reason_codes"
    ]


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


def test_preflight_blocks_nxt_exchange_without_operator_confirmation(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "preflight-nxt-order.sqlite3")

    blocked = run_live_sim_preflight(
        connection,
        settings=_operating_settings(live_sim_order_exchange="NXT"),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    confirmed = run_live_sim_preflight(
        connection,
        settings=_operating_settings(
            live_sim_order_exchange="SOR",
            live_sim_nxt_support_confirmed=True,
        ),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=True,
        include_ai=False,
        include_no_buy=False,
    )
    connection.close()

    blocked_check = _check(blocked, "nxt_order_support_verified")
    confirmed_check = _check(confirmed, "nxt_order_support_verified")
    assert blocked.status is PreflightStatus.BLOCK
    assert blocked_check.status is PreflightStatus.BLOCK
    assert blocked_check.details["order_exchange"] == "NXT"
    assert blocked_check.details["nxt_order_support_confirmed"] is False
    assert blocked_check.details["simulation_server_check_preserved"] is True
    assert blocked.safety_gate["nxt_order_support_verified"] is False
    assert "NXT_ORDER_SUPPORT_UNCONFIRMED" in blocked.safety_gate["reason_codes"]
    assert any("nxt_order_support_verified" in reason for reason in blocked.blocking_reasons)
    assert confirmed_check.status is PreflightStatus.PASS
    assert confirmed_check.details["order_exchange"] == "SOR"
    assert confirmed_check.details["nxt_order_support_confirmed"] is True
    assert confirmed.safety_gate["nxt_order_support_verified"] is True


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


def test_preflight_warns_on_pending_gateway_command_backlog(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "preflight-backlog.sqlite3")
    settings = _operating_settings(
        live_sim_preflight_pending_command_backlog_warn_threshold=2
    )
    for index in range(3):
        enqueue_command(connection, _queued_request_tr_command(f"cmd_backlog_{index}"))

    preflight = run_live_sim_preflight(
        connection,
        settings=settings,
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    check = _check(preflight, "gateway_pending_command_backlog")
    connection.close()

    assert check.status is PreflightStatus.WARN
    assert check.details["pending_command_count"] == 3
    assert check.details["pending_command_type_counts"] == {"request_tr": 3}
    assert check.details["threshold"] == 2
    assert preflight.gateway["pending_command_count"] == 3


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


def test_theme_refresh_lock_does_not_block_operating_entry_or_buy(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "theme-lock-operating.sqlite3")
    _insert_runtime_lock(
        connection,
        lock_name=THEME_REFRESH_LOCK,
        owner_id="theme-refresh-running",
    )

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=_operating_settings(live_sim_pilot_auto_queue_command=True),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    lock_count = connection.execute(
        "SELECT COUNT(*) AS count FROM runtime_execution_locks WHERE lock_name = ?",
        (THEME_REFRESH_LOCK,),
    ).fetchone()["count"]
    connection.close()

    assert result.stages["entry_timing"]["status"] == "COMPLETED"
    assert result.stages["buy"]["status"] == "COMPLETED"
    assert result.buy_evaluated_count == 1
    assert result.buy_command_count == 0
    assert lock_count == 1


def test_operating_sweeps_expired_command_before_reconcile_and_preflight(
    tmp_path,
) -> None:
    connection, _ = _prepared_order_plan_connection(
        tmp_path / "expired-sweep-operating.sqlite3"
    )
    _insert_expired_pre_dispatch_order(connection)
    _insert_reconcile_block(connection)

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=_operating_settings(),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    order_status = connection.execute(
        "SELECT status FROM live_sim_orders WHERE live_sim_order_id = 'expired-order'"
    ).fetchone()["status"]
    connection.close()

    assert result.stages["expired_command_sweep"]["expired_order_count"] == 1
    assert result.stages["reconcile"]["mismatch_count"] == 0
    assert result.stages["reconcile"]["snapshot_json"]["blocking_new_buy"] is False
    assert _check_status(result.preflight, "reconcile_latest_status") == "PASS"
    assert _check_status(result.preflight, "lifecycle_error_count") == "PASS"
    assert result.stages["buy"]["status"] == "COMPLETED"
    assert order_status == "ORDER_EXPIRED"


def test_operating_entry_timing_retries_evaluation_lock_then_succeeds(
    tmp_path,
    monkeypatch,
) -> None:
    import services.runtime.live_sim_operating_orchestrator as orchestrator

    connection = initialize_database(tmp_path / "entry-lock-retry.sqlite3")
    settings = _operating_settings(live_sim_reconcile_enabled=False)
    attempts = 0
    sleeps: list[float] = []

    def flaky_entry_timing(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise _evaluation_lock_error()
        return _FakeLifecycleResult(
            run_id="entry",
            run_type="ENTRY_TIMING",
            evaluated_count=1,
            command_count=0,
        )

    monkeypatch.setattr(orchestrator, "evaluate_entry_timing", flaky_entry_timing)
    monkeypatch.setattr(orchestrator.time, "sleep", lambda seconds: sleeps.append(seconds))
    _patch_noop_lifecycle_stages(monkeypatch, orchestrator)

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=settings,
        mode=OperatingMode.OBSERVE_CYCLE,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    connection.close()

    assert attempts == 3
    assert sleeps == [2.0, 2.0]
    assert result.stages["entry_timing"]["status"] == "COMPLETED"
    assert [error["stage"] for error in result.errors] == []


def test_bound_operating_cycle_skips_refresh_and_propagates_exact_binding(
    tmp_path,
    monkeypatch,
) -> None:
    import services.runtime.live_sim_operating_orchestrator as orchestrator

    connection = initialize_database(tmp_path / "bound-operating.sqlite3")
    binding = {
        "contract": "live-sim-order-plan-binding.v1",
        "order_plan_id": "OPD-bound-operating",
        "order_plan_snapshot_sha256": "c" * 64,
        "binding_sha256": "b" * 64,
    }
    captured: dict[str, Any] = {}
    settings = _operating_settings(
        live_sim_reconcile_enabled=False,
        live_sim_reprice_enabled=True,
    )

    monkeypatch.setattr(
        orchestrator,
        "evaluate_entry_timing",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("bound cycle must not rerun entry timing")
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_reprice_once",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("bound cycle must not reprice a different intent")
        ),
    )
    for stage_name in (
        "rebuild_theme_leadership",
        "run_candidate_quote_refresh_once",
        "enqueue_incremental_evaluation_for_fresh_candidates",
    ):
        monkeypatch.setattr(
            orchestrator,
            stage_name,
            lambda *args, _stage_name=stage_name, **kwargs: (
                (_ for _ in ()).throw(
                    AssertionError(
                        f"bound cycle must not run source-mutating stage: {_stage_name}"
                    )
                )
            ),
        )

    def bound_buy(*args, **kwargs):
        captured.update(kwargs)
        return _FakeBuyResult(
            run_id="bound-buy",
            evaluated_count=1,
            command_count=0,
        )

    monkeypatch.setattr(orchestrator, "run_live_sim_pilot_pipeline_once", bound_buy)
    _patch_noop_lifecycle_stages(monkeypatch, orchestrator)

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
        required_plan_binding=binding,
    )
    connection.close()

    assert result.stages["entry_timing"]["status"] == "SKIPPED"
    assert result.stages["entry_timing"]["reason"] == "required_plan_binding"
    assert result.stages["reprice"]["status"] == "SKIPPED"
    for stage_name in (
        "theme_leadership",
        "candidate_quote_refresh",
        "incremental_backfill",
    ):
        assert result.stages[stage_name]["status"] == "SKIPPED"
        assert result.stages[stage_name]["reason"] == "required_plan_binding"
        assert result.stages[stage_name]["source_mutation_skipped"] is True
    assert captured["required_plan_binding"] == binding
    assert result.reason_summary["required_plan_binding"] == binding


def test_operating_cycle_rejects_empty_required_binding_before_stages(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "empty-bound-operating.sqlite3")

    with pytest.raises(ValueError, match="required_plan_binding requires order_plan_id"):
        run_live_sim_operating_cycle_once(
            connection,
            settings=_operating_settings(live_sim_reconcile_enabled=False),
            mode=OperatingMode.PILOT_BUY_ONLY,
            queue_commands=False,
            required_plan_binding={},
        )

    run_count = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_operating_runs"
    ).fetchone()["count"]
    connection.close()

    assert run_count == 0


def test_bound_operating_cycle_propagates_pilot_terminal_error(
    tmp_path,
    monkeypatch,
) -> None:
    import services.runtime.live_sim_operating_orchestrator as orchestrator

    connection = initialize_database(tmp_path / "bound-operating-error.sqlite3")
    binding = {
        "contract": "live-sim-order-plan-binding.v1",
        "order_plan_id": "OPD-bound-error",
        "order_plan_snapshot_sha256": "c" * 64,
        "binding_sha256": "b" * 64,
    }
    monkeypatch.setattr(
        orchestrator,
        "run_live_sim_pilot_pipeline_once",
        lambda *args, **kwargs: _FakeBuyResult(
            run_id="bound-buy-error",
            evaluated_count=1,
            command_count=0,
            status="COMPLETED_WITH_ERRORS",
        ),
    )
    _patch_noop_lifecycle_stages(monkeypatch, orchestrator)

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=_operating_settings(live_sim_reconcile_enabled=False),
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
        required_plan_binding=binding,
    )
    connection.close()

    assert result.status == "COMPLETED_WITH_ERRORS"
    assert result.stages["buy"]["status"] == "COMPLETED_WITH_ERRORS"
    assert any(
        error["stage"] == "buy"
        and error["error"] == "Exact bound plan execution did not complete cleanly."
        for error in result.errors
    )


def test_operating_buy_records_lock_error_after_retries(
    tmp_path,
    monkeypatch,
) -> None:
    import services.runtime.live_sim_operating_orchestrator as orchestrator

    connection = initialize_database(tmp_path / "buy-lock-retry.sqlite3")
    settings = _operating_settings(live_sim_reconcile_enabled=False)
    buy_attempts = 0
    sleeps: list[float] = []

    def locked_buy(*args, **kwargs):
        nonlocal buy_attempts
        buy_attempts += 1
        raise _evaluation_lock_error(owner_id=f"incremental-{buy_attempts}")

    monkeypatch.setattr(
        orchestrator,
        "evaluate_entry_timing",
        lambda *args, **kwargs: _FakeLifecycleResult(
            run_id="entry",
            run_type="ENTRY_TIMING",
            evaluated_count=1,
            command_count=0,
        ),
    )
    monkeypatch.setattr(orchestrator, "run_live_sim_pilot_pipeline_once", locked_buy)
    monkeypatch.setattr(orchestrator.time, "sleep", lambda seconds: sleeps.append(seconds))
    _patch_noop_lifecycle_stages(monkeypatch, orchestrator)

    result = run_live_sim_operating_cycle_once(
        connection,
        settings=settings,
        mode=OperatingMode.PILOT_BUY_ONLY,
        queue_commands=False,
        include_ai=False,
        include_no_buy=False,
    )
    runs = list_live_sim_operating_runs(connection)
    connection.close()

    assert buy_attempts == 4
    assert sleeps == [2.0, 2.0, 2.0]
    assert result.status == "COMPLETED_WITH_ERRORS"
    assert result.stages["buy"]["status"] == "ERROR"
    assert result.errors[-1]["stage"] == "buy"
    assert result.errors[-1]["error"] == "EVALUATION_RUN_LOCKED"
    assert runs[0]["errors"][-1]["stage"] == "buy"
    assert runs[0]["errors"][-1]["error"] == "EVALUATION_RUN_LOCKED"


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


@dataclass(frozen=True, kw_only=True)
class _FakeThemeResult:
    status: str = "COMPLETED"

    def to_dict(self, *, include_members: bool = False) -> dict[str, Any]:
        return {"status": self.status, "include_members": include_members}


def _patch_noop_lifecycle_stages(monkeypatch, orchestrator) -> None:
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
        "rebuild_theme_leadership",
        lambda *args, **kwargs: _FakeThemeResult(),
    )


def _evaluation_lock_error(owner_id: str = "incremental-run") -> EvaluationRunLockError:
    return EvaluationRunLockError(
        lock_name="evaluation_pipeline",
        owner_id=owner_id,
        expires_at="2026-07-06T00:00:00Z",
    )


def _queued_request_tr_command(command_id: str) -> GatewayCommand:
    return GatewayCommand(
        command_id=command_id,
        command_type="request_tr",
        source="core",
        payload={
            "request_id": command_id,
            "tr_code": "OPT10001",
            "params": {"code": "005930"},
        },
    )


def _insert_runtime_lock(
    connection,
    *,
    lock_name: str,
    owner_id: str,
) -> None:
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
        VALUES (?, ?, ?, ?, '{}')
        """,
        (
            lock_name,
            owner_id,
            datetime_to_wire(now),
            datetime_to_wire(now + timedelta(seconds=300)),
        ),
    )
    connection.commit()


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
    mismatches = [{"reason": "operating_test_reconcile_mismatch"}]
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
        (
            json.dumps(
                {
                    "broker_snapshot": {},
                    "open_orders": [],
                    "positions": [],
                    "mismatches": mismatches,
                    "broker_snapshot_available": False,
                    "broker_snapshot_status": "BROKER_SNAPSHOT_UNAVAILABLE",
                    "blocking_new_buy": True,
                    "allow_exit": True,
                    "live_sim_only": True,
                    "live_real_allowed": False,
                    "broker_order_path": "LIVE_SIM_ONLY",
                },
                sort_keys=True,
            ),
            now,
        ),
    )
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id,
            event_type,
            entity_type,
            entity_id,
            status,
            reason,
            evidence_json,
            created_at,
            live_sim_only,
            live_real_allowed
        )
        VALUES (
            'operating-reconcile-block-event',
            'RECONCILE_MISMATCH',
            'RECONCILE',
            'operating-reconcile-block',
            'RECONCILE_MISMATCH',
            'RECONCILE_MISMATCH',
            ?,
            ?,
            1,
            0
        )
        """,
        (
            json.dumps(
                {
                    "mismatches": mismatches,
                    "blocking_new_buy": True,
                },
                sort_keys=True,
            ),
            now,
        ),
    )
    connection.commit()


def _insert_expired_pre_dispatch_order(connection) -> None:
    now = datetime_to_wire(utc_now())
    payload = json.dumps(
        {
            "mode": "LIVE_SIM",
            "live_mode": "LIVE_SIM",
            "code": "005930",
            "side": "BUY",
            "quantity": 1,
            "price": 97000,
            "metadata": {"live_sim_only": True, "live_real_allowed": False},
        },
        sort_keys=True,
    )
    connection.execute(
        """
        INSERT INTO gateway_commands (
            command_id,
            command_type,
            source,
            status,
            idempotency_key,
            payload_json,
            payload_hash,
            created_at,
            completed_at,
            expires_at
        )
        VALUES ('expired-command', 'send_order', 'live_sim', 'EXPIRED',
            'expired-key', ?, 'expired-hash', ?, ?, ?)
        """,
        (payload, now, now, now),
    )
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id,
            live_sim_intent_id,
            gateway_command_id,
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
            command_queued_at,
            created_at
        )
        VALUES ('expired-order', 'expired-intent', 'expired-command', ?,
            'SIM-12345678', '005930', '삼성전자', 'BUY', 'LIMIT', 1, 97000,
            97000, 'COMMAND_QUEUED', 0, 1, 'expired-key', ?, ?)
        """,
        (market_today(), now, now),
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
    return _check(preflight, name).status.value


def _check(preflight: LiveSimPreflightResult, name: str) -> Any:
    for check in preflight.checks:
        if check.name == name:
            return check
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
