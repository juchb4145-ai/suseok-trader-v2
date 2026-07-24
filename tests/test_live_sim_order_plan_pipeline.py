from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta

import domain.broker.utils as broker_utils
import pytest
from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from domain.live_sim.reasons import LiveSimReasonCode
from domain.live_sim.status import LiveSimIntentStatus, LiveSimOrderStatus
from fastapi.testclient import TestClient
from services.config import Settings, TradingMode, TradingProfile
from services.entry_timing.service import evaluate_entry_timing
from services.live_sim.live_sim_service import queue_live_sim_order_command
from services.live_sim.order_plan_binding import build_order_plan_binding
from services.live_sim.order_plan_eligibility import evaluate_live_sim_order_plan_eligibility
from services.live_sim.order_plan_intent import (
    create_live_sim_intent_from_order_plan,
    make_live_sim_order_plan_idempotency_key,
)
from services.risk_gate import evaluate_risk_for_candidate, save_risk_observation
from services.runtime.live_sim_pilot_pipeline import run_live_sim_pilot_pipeline_once
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation
from storage.sqlite import initialize_database, open_connection
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
    assert intent.order_plan_id == order_plan_id
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


def test_bound_pilot_uses_only_exact_plan_without_refresh_or_general_selection(
    tmp_path,
    monkeypatch,
) -> None:
    import services.runtime.live_sim_pilot_pipeline as pilot_pipeline

    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "run-bound-plan.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    assert plan is not None
    binding = build_order_plan_binding(dict(plan))
    settings = _pilot_settings(live_sim_pilot_auto_queue_command=True)
    monkeypatch.setattr(
        pilot_pipeline,
        "evaluate_entry_timing",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("bound pilot must not rerun entry timing")
        ),
    )
    monkeypatch.setattr(
        pilot_pipeline,
        "select_live_sim_order_plan_candidates",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("bound pilot must not run general plan selection")
        ),
    )

    result = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=True,
        required_plan_binding=binding,
    )
    command_count = _count(connection, "gateway_commands")
    connection.close()

    assert result.status == "COMPLETED"
    assert result.evaluated_count == 1
    assert result.command_count == 1
    assert command_count == 1
    assert [item["order_plan_id"] for item in result.selected_order_plans] == [
        order_plan_id
    ]
    assert result.preparation["entry_timing_evaluated"] is False
    assert result.preparation["selection_mode"] == "EXACT_BOUND_PLAN"
    assert result.preparation["required_plan_binding"] == binding


def test_bound_pilot_keeps_intent_and_terminal_enqueue_in_caller_transaction(
    tmp_path,
    monkeypatch,
) -> None:
    import services.runtime.live_sim_pilot_pipeline as pilot_pipeline

    db_path = tmp_path / "run-bound-transaction.sqlite3"
    connection, order_plan_id = _prepared_order_plan_connection(db_path)
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    assert plan is not None
    binding = build_order_plan_binding(dict(plan))
    settings = _pilot_settings(live_sim_pilot_auto_queue_command=True)
    queue_live_sim_order_command_original = pilot_pipeline.queue_live_sim_order_command
    observed: dict[str, bool] = {}

    def queue_while_competing_writer_is_blocked(*args, **kwargs):
        observed["caller_transaction_active"] = connection.in_transaction
        competing = open_connection(db_path)
        competing.execute("PRAGMA busy_timeout = 0")
        try:
            with pytest.raises(sqlite3.OperationalError, match="database is locked"):
                competing.execute(
                    """
                    UPDATE order_plan_drafts_latest
                    SET source_event_id = 'concurrent-source-mutation'
                    WHERE order_plan_id = ?
                    """,
                    (order_plan_id,),
                )
        finally:
            competing.close()
        return queue_live_sim_order_command_original(*args, **kwargs)

    monkeypatch.setattr(
        pilot_pipeline,
        "queue_live_sim_order_command",
        queue_while_competing_writer_is_blocked,
    )

    result = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=True,
        required_plan_binding=binding,
    )
    connection.close()

    assert observed["caller_transaction_active"] is True
    assert result.status == "COMPLETED"
    assert result.command_count == 1


def test_bound_pilot_duplicate_command_queued_intent_preserves_transaction(
    tmp_path,
    monkeypatch,
) -> None:
    import services.runtime.live_sim_pilot_pipeline as pilot_pipeline

    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "run-bound-duplicate-transaction.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    assert plan is not None
    binding = build_order_plan_binding(dict(plan))
    settings = _pilot_settings(live_sim_pilot_auto_queue_command=True)

    first = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=True,
        required_plan_binding=binding,
    )
    queue_live_sim_order_command_original = pilot_pipeline.queue_live_sim_order_command
    observed: dict[str, bool] = {}

    def observe_invalid_status_transaction(*args, **kwargs):
        try:
            return queue_live_sim_order_command_original(*args, **kwargs)
        except ValueError:
            observed["transaction_active_after_invalid_status"] = (
                connection.in_transaction
            )
            raise

    monkeypatch.setattr(
        pilot_pipeline,
        "queue_live_sim_order_command",
        observe_invalid_status_transaction,
    )
    second = run_live_sim_pilot_pipeline_once(
        connection,
        settings=settings,
        trade_date="2026-06-27",
        queue_commands=True,
        required_plan_binding=binding,
    )
    command_count = _count(connection, "gateway_commands")
    connection.close()

    assert first.command_count == 1
    assert second.command_count == 0
    assert second.error_count == 1
    assert second.status == "COMPLETED_WITH_ERRORS"
    assert observed["transaction_active_after_invalid_status"] is True
    assert command_count == 1


def test_bound_pilot_rejects_requested_trade_date_mismatch(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "run-bound-trade-date-mismatch.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    assert plan is not None
    binding = build_order_plan_binding(dict(plan))

    with pytest.raises(
        ValueError,
        match="required_plan_binding trade_date does not match requested trade_date",
    ):
        run_live_sim_pilot_pipeline_once(
            connection,
            settings=_pilot_settings(live_sim_pilot_auto_queue_command=True),
            trade_date="2099-12-31",
            queue_commands=True,
            required_plan_binding=binding,
        )

    command_count = _count(connection, "gateway_commands")
    pilot_run_count = _count(connection, "live_sim_runs")
    connection.close()

    assert command_count == 0
    assert pilot_run_count == 0


def test_terminal_queue_rejects_plan_content_changed_after_intent(
    tmp_path,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "terminal-plan-swap.sqlite3"
    )
    settings = _pilot_settings()
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=settings,
    )
    for table_name in ("order_plan_drafts", "order_plan_drafts_latest"):
        connection.execute(
            f"""
            UPDATE {table_name}
            SET limit_price = limit_price + 1000,
                suggested_notional = suggested_quantity * (limit_price + 1000)
            WHERE order_plan_id = ?
            """,
            (order_plan_id,),
        )
    connection.commit()

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value,
    ):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    persisted_intent = connection.execute(
        """
        SELECT status, reason_codes_json
        FROM live_sim_intents
        WHERE live_sim_intent_id = ?
        """,
        (intent.live_sim_intent_id,),
    ).fetchone()
    rejection = connection.execute(
        """
        SELECT reason_codes_json
        FROM live_sim_rejections
        ORDER BY created_at DESC, rejection_id DESC
        LIMIT 1
        """
    ).fetchone()
    command_count = _count(connection, "gateway_commands")
    order_count = _count(connection, "live_sim_orders")
    connection.close()

    assert persisted_intent["status"] == LiveSimIntentStatus.REJECTED.value
    assert LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value in json.loads(
        persisted_intent["reason_codes_json"]
    )
    assert LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value in json.loads(
        rejection["reason_codes_json"]
    )
    assert command_count == 0
    assert order_count == 0


def test_terminal_queue_rechecks_current_price_after_intent(
    tmp_path,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "terminal-price-drift.sqlite3"
    )
    settings = _pilot_settings()
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=settings,
    )
    connection.execute(
        """
        UPDATE market_ticks_latest
        SET price = price * 1.10,
            event_ts = ?
        WHERE code = '005930' AND exchange = 'KRX'
        """,
        (datetime_to_wire(utc_now()),),
    )
    connection.commit()

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value,
    ):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    rejection = connection.execute(
        """
        SELECT reason_codes_json
        FROM live_sim_rejections
        ORDER BY created_at DESC, rejection_id DESC
        LIMIT 1
        """
    ).fetchone()
    command_count = _count(connection, "gateway_commands")
    connection.close()

    reasons = json.loads(rejection["reason_codes_json"])
    assert LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value in reasons
    assert "ORDER_PLAN_BINDING_CURRENT_SOURCE_WATERMARK_MISMATCH" in reasons
    assert command_count == 0


def test_intent_creation_rejects_explicit_empty_plan_binding(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "empty-expected-binding.sqlite3"
    )

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value,
    ):
        create_live_sim_intent_from_order_plan(
            connection,
            order_plan_id,
            settings=_pilot_settings(),
            expected_binding={},
        )

    intent_count = _count(connection, "live_sim_intents")
    connection.close()

    assert intent_count == 0


def test_intent_creation_external_transaction_management_requires_transaction(
    tmp_path,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "intent-external-transaction-required.sqlite3"
    )
    assert connection.in_transaction is False

    with pytest.raises(
        RuntimeError,
        match="manage_transaction=False requires an active caller transaction",
    ):
        create_live_sim_intent_from_order_plan(
            connection,
            order_plan_id,
            settings=_pilot_settings(),
            manage_transaction=False,
        )

    intent_count = _count(connection, "live_sim_intents")
    connection.close()

    assert intent_count == 0


def test_terminal_queue_rejects_mutated_intent_execution_payload(
    tmp_path,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "terminal-intent-payload-tamper.sqlite3"
    )
    settings = _pilot_settings()
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=settings,
    )
    connection.execute(
        """
        UPDATE live_sim_intents
        SET quantity = 999999,
            notional = limit_price * 999999,
            idempotency_key = idempotency_key || ':tampered'
        WHERE live_sim_intent_id = ?
        """,
        (intent.live_sim_intent_id,),
    )
    connection.commit()

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value,
    ):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    persisted_intent = connection.execute(
        """
        SELECT status, reason_codes_json
        FROM live_sim_intents
        WHERE live_sim_intent_id = ?
        """,
        (intent.live_sim_intent_id,),
    ).fetchone()
    command_count = _count(connection, "gateway_commands")
    order_count = _count(connection, "live_sim_orders")
    connection.close()

    reasons = json.loads(persisted_intent["reason_codes_json"])
    assert persisted_intent["status"] == LiveSimIntentStatus.REJECTED.value
    assert LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value in reasons
    assert LiveSimReasonCode.ORDER_PLAN_INTENT_EXECUTION_MISMATCH.value in reasons
    assert command_count == 0
    assert order_count == 0


def test_terminal_queue_rejects_mutated_account_with_coherent_idempotency(
    tmp_path,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "terminal-intent-account-tamper.sqlite3"
    )
    settings = _pilot_settings()
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=settings,
    )
    mutated_account_id = "SIM-87654321"
    mutated_idempotency_key = make_live_sim_order_plan_idempotency_key(
        trade_date=intent.trade_date,
        account_id=mutated_account_id,
        order_plan_id=order_plan_id,
        code=intent.code,
        side=intent.side.value,
        limit_price=float(intent.limit_price or 0),
        quantity=int(intent.quantity),
    )
    connection.execute(
        """
        UPDATE live_sim_intents
        SET account_id = ?,
            idempotency_key = ?
        WHERE live_sim_intent_id = ?
        """,
        (
            mutated_account_id,
            mutated_idempotency_key,
            intent.live_sim_intent_id,
        ),
    )
    connection.commit()

    with pytest.raises(
        ValueError,
        match=LiveSimReasonCode.ORDER_PLAN_BINDING_INVALID.value,
    ):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    persisted_intent = connection.execute(
        """
        SELECT status, reason_codes_json
        FROM live_sim_intents
        WHERE live_sim_intent_id = ?
        """,
        (intent.live_sim_intent_id,),
    ).fetchone()
    command_count = _count(connection, "gateway_commands")
    order_count = _count(connection, "live_sim_orders")
    connection.close()

    reasons = json.loads(persisted_intent["reason_codes_json"])
    assert persisted_intent["status"] == LiveSimIntentStatus.REJECTED.value
    assert LiveSimReasonCode.ORDER_PLAN_INTENT_EXECUTION_MISMATCH.value in reasons
    assert command_count == 0
    assert order_count == 0


def test_terminal_queue_rereads_created_status_after_write_lock(
    tmp_path,
    monkeypatch,
) -> None:
    import services.live_sim.live_sim_service as live_sim_service

    db_path = tmp_path / "terminal-intent-status-race.sqlite3"
    connection, order_plan_id = _prepared_order_plan_connection(db_path)
    settings = _pilot_settings()
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=settings,
    )
    racing_connection = open_connection(db_path)
    acquire_real = live_sim_service._acquire_buy_queue_boundary_write_lock

    def transition_before_lock(target_connection):
        racing_connection.execute(
            """
            UPDATE live_sim_intents
            SET status = ?
            WHERE live_sim_intent_id = ?
            """,
            (
                LiveSimIntentStatus.REJECTED.value,
                intent.live_sim_intent_id,
            ),
        )
        racing_connection.commit()
        return acquire_real(target_connection)

    monkeypatch.setattr(
        live_sim_service,
        "_acquire_buy_queue_boundary_write_lock",
        transition_before_lock,
    )

    with pytest.raises(ValueError, match=LiveSimIntentStatus.REJECTED.value):
        queue_live_sim_order_command(
            connection,
            intent.live_sim_intent_id,
            settings=settings,
        )

    persisted_status = connection.execute(
        """
        SELECT status
        FROM live_sim_intents
        WHERE live_sim_intent_id = ?
        """,
        (intent.live_sim_intent_id,),
    ).fetchone()["status"]
    command_count = _count(connection, "gateway_commands")
    order_count = _count(connection, "live_sim_orders")
    racing_connection.close()
    connection.close()

    assert persisted_status == LiveSimIntentStatus.REJECTED.value
    assert command_count == 0
    assert order_count == 0


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


def test_order_plan_daily_limits_ignore_order_expired_before_dispatch(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "plan-expired-daily-budget.sqlite3"
    )
    _insert_live_sim_order(
        connection,
        status=LiveSimOrderStatus.ORDER_EXPIRED.value,
        order_id="expired-before-dispatch",
    )

    eligibility = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(
            live_sim_max_daily_order_count=1,
            live_sim_max_daily_notional=100_000,
        ),
    )
    connection.close()

    assert eligibility.eligible is True
    assert LiveSimReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value not in eligibility.reason_codes
    assert LiveSimReasonCode.DAILY_NOTIONAL_LIMIT_EXCEEDED.value not in eligibility.reason_codes


def test_order_plan_daily_limits_ignore_failed_order(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "plan-failed-daily-budget.sqlite3"
    )
    _insert_live_sim_order(
        connection,
        status=LiveSimOrderStatus.FAILED.value,
        order_id="failed-before-broker",
    )

    eligibility = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(
            live_sim_max_daily_order_count=1,
            live_sim_max_daily_notional=100_000,
        ),
    )
    connection.close()

    assert eligibility.eligible is True
    assert LiveSimReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value not in eligibility.reason_codes
    assert LiveSimReasonCode.DAILY_NOTIONAL_LIMIT_EXCEEDED.value not in eligibility.reason_codes


def test_order_plan_entry_window_blocks_buy_and_records_rejection(
    tmp_path,
    monkeypatch,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "plan-entry-window.sqlite3"
    )
    settings = _pilot_settings(
        live_sim_entry_window_start="09:05:00",
        live_sim_entry_window_end="14:30:00",
        live_sim_exit_eod_flatten_time="15:15:00",
    )
    monkeypatch.setattr(
        broker_utils,
        "utc_now",
        lambda: datetime(2026, 7, 1, 5, 30, 1, tzinfo=UTC),
    )

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
    rejection = connection.execute(
        """
        SELECT reason_codes_json
        FROM live_sim_rejections
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    connection.close()

    assert eligibility.eligible is False
    assert LiveSimReasonCode.ENTRY_WINDOW_CLOSED.value in eligibility.reason_codes
    assert eligibility.evidence_json["entry_window"]["current_time"] == "14:30:01"
    assert intent.status is LiveSimIntentStatus.REJECTED
    assert LiveSimReasonCode.ENTRY_WINDOW_CLOSED.value in json.loads(
        rejection["reason_codes_json"]
    )


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
        "live_sim_entry_window_start": "00:00:00",
        "live_sim_entry_window_end": "23:59:58",
        "live_sim_exit_eod_flatten_time": "23:59:59",
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
    monkeypatch.setenv("LIVE_SIM_ENTRY_WINDOW_START", "00:00:00")
    monkeypatch.setenv("LIVE_SIM_ENTRY_WINDOW_END", "23:59:58")
    monkeypatch.setenv("LIVE_SIM_EXIT_EOD_FLATTEN_TIME", "23:59:59")
    monkeypatch.setenv("LIVE_SIM_PILOT_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND", "false")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_STALE_SEC", "999999999")
