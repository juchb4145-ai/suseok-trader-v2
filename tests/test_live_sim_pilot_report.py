from __future__ import annotations

import json
import sqlite3

import pytest
from storage.sqlite import initialize_database
from tools.build_live_sim_pilot_report import (
    build_live_sim_pilot_kpi,
    open_readonly_connection,
    write_live_sim_pilot_report,
)

TRADE_DATE = "2026-06-27"
ACCOUNT_ID = "SIM-12345678"


def test_live_sim_pilot_kpi_report_builds_daily_sections(tmp_path) -> None:
    db_path = tmp_path / "live-sim-pilot-kpi.sqlite3"
    connection = initialize_database(db_path)
    _insert_kpi_fixture(connection)

    before_counts = _table_counts(connection)
    payload = build_live_sim_pilot_kpi(connection, TRADE_DATE)
    paths = write_live_sim_pilot_report(payload, report_root=tmp_path / "reports")
    after_counts = _table_counts(connection)
    connection.close()

    saved = json.loads(paths["kpi_json"].read_text(encoding="utf-8"))
    markdown = paths["kpi_md"].read_text(encoding="utf-8")

    assert before_counts == after_counts
    assert paths["kpi_json"].name == "kpi.json"
    assert paths["kpi_md"].name == "kpi.md"
    assert saved["read_only"] is True
    assert saved["gateway_command_allowed"] is False
    assert saved["live_real_allowed"] is False
    assert payload["funnel"]["counts"]["candidate"] == 2
    assert payload["funnel"]["counts"]["strategy_matched"] == 1
    assert payload["funnel"]["counts"]["risk_observe_pass"] == 1
    assert payload["funnel"]["counts"]["entry_plan_ready"] == 1
    assert payload["funnel"]["counts"]["live_sim_intent"] == 1
    assert payload["funnel"]["counts"]["live_sim_command"] == 1
    assert payload["funnel"]["counts"]["live_sim_filled_order"] == 1
    assert payload["funnel"]["stages"][1]["conversion_from_previous"] == 0.5
    assert payload["fills"]["buy_fill_rate"] == 1.0
    assert payload["fills"]["ttl_cancel_count"] == 1
    assert payload["fills"]["ttl_cancel_acked_count"] == 1
    assert payload["fills"]["reprice_attempt_count"] == 1
    assert payload["fills"]["reprice_success_count"] == 1
    assert payload["fills"]["slippage"]["sample_count"] == 2
    assert payload["strategy"]["closed_position_count"] == 2
    assert payload["strategy"]["exit_reason_distribution"]["STOP_LOSS"]["count"] == 1
    assert payload["strategy"]["exit_reason_distribution"]["TAKE_PROFIT"]["count"] == 1
    assert payload["strategy"]["stop_loss_mfe_pct_distribution"]["median"] == 1.0
    assert payload["strategy"]["winning_trade_mae_pct_distribution"]["median"] == -1.0
    assert payload["operating_latency"]["run_count"] == 2
    reconcile_latency = next(
        item
        for item in payload["operating_latency"]["stages"]
        if item["stage"] == "reconcile"
    )
    assert reconcile_latency["sample_count"] == 2
    assert reconcile_latency["p50_ms"] == 20.0
    assert reconcile_latency["p95_ms"] == 29.0
    assert "손절 트레이드의 MFE 분포" in markdown
    assert "승리 트레이드의 MAE 분포" in markdown
    assert "## Operating Latency" in markdown
    assert "| Reconcile | 2 | 20.0000 | 29.0000" in markdown


def test_live_sim_pilot_report_opens_database_readonly(tmp_path) -> None:
    db_path = tmp_path / "readonly.sqlite3"
    connection = initialize_database(db_path)
    _insert_kpi_fixture(connection)
    connection.close()

    readonly = open_readonly_connection(db_path)
    try:
        payload = build_live_sim_pilot_kpi(readonly, TRADE_DATE)
        with pytest.raises(sqlite3.OperationalError):
            readonly.execute(
                """
                INSERT INTO gateway_commands (
                    command_id,
                    command_type,
                    source,
                    status,
                    payload_json,
                    payload_hash
                )
                VALUES ('blocked', 'heartbeat_request', 'test', 'QUEUED', '{}', 'hash')
                """
            )
    finally:
        readonly.close()

    assert payload["read_only"] is True


def _insert_kpi_fixture(connection) -> None:
    now = "2026-06-27T09:00:00+09:00"
    later = "2026-06-27T09:20:00+09:00"
    _insert_candidate(connection, "CAND-1", "005930", "Samsung", "CONTEXT_READY", now)
    _insert_candidate(connection, "CAND-2", "000660", "SK Hynix", "CONTEXT_READY", now)
    _insert_strategy(connection, "CAND-1", "005930", "MATCHED_OBSERVATION", [], now)
    _insert_strategy(connection, "CAND-2", "000660", "NO_SETUP", ["NO_SETUP_MATCH"], now)
    _insert_risk(connection, "CAND-1", "005930", "OBSERVE_PASS", [], now)
    _insert_order_plan(connection, "PLAN-1", "CAND-1", "005930", "PLAN_READY", [], now)
    _insert_gateway_command(connection, "cmd-buy-1", now)
    _insert_gateway_command(connection, "cmd-reprice-1", now)
    _insert_live_sim_intent(
        connection,
        intent_id="intent-1",
        candidate_id="CAND-1",
        code="005930",
        status="COMMAND_QUEUED",
        command_id="cmd-buy-1",
        limit_price=100_000,
        evidence={"setup_type": "THEME_LEADER_PULLBACK", "order_plan_id": "PLAN-1"},
        created_at=now,
    )
    _insert_live_sim_intent(
        connection,
        intent_id="intent-reprice-1",
        candidate_id="CAND-1",
        code="005930",
        status="COMMAND_QUEUED",
        command_id="cmd-reprice-1",
        limit_price=99_000,
        evidence={
            "source": "live_sim_reprice",
            "setup_type": "THEME_LEADER_PULLBACK",
            "reprice": {"attempt": 1, "root_live_sim_order_id": "order-cancelled"},
        },
        created_at=now,
    )
    _insert_live_sim_order(
        connection,
        order_id="order-buy-1",
        intent_id="intent-1",
        command_id="cmd-buy-1",
        side="BUY",
        limit_price=100_000,
        status="FILLED",
        filled_quantity=1,
        avg_fill_price=100_500,
        created_at=now,
    )
    _insert_live_sim_order(
        connection,
        order_id="order-reprice-1",
        intent_id="intent-reprice-1",
        command_id="cmd-reprice-1",
        side="BUY",
        limit_price=99_000,
        status="FILLED",
        filled_quantity=1,
        avg_fill_price=99_000,
        created_at=now,
    )
    _insert_live_sim_order(
        connection,
        order_id="order-sell-win",
        intent_id="exit-win",
        command_id="cmd-sell-win",
        side="SELL",
        limit_price=108_000,
        status="EXIT_FILLED",
        filled_quantity=1,
        avg_fill_price=108_000,
        created_at=later,
    )
    _insert_live_sim_order(
        connection,
        order_id="order-sell-stop",
        intent_id="exit-stop",
        command_id="cmd-sell-stop",
        side="SELL",
        limit_price=96_000,
        status="EXIT_FILLED",
        filled_quantity=1,
        avg_fill_price=96_000,
        created_at=later,
    )
    _insert_execution(connection, "exec-buy-1", "order-buy-1", "intent-1", 100_500, now)
    _insert_execution(
        connection,
        "exec-reprice-1",
        "order-reprice-1",
        "intent-reprice-1",
        99_000,
        now,
    )
    _insert_position(
        connection,
        position_id="pos-win",
        source_intent_id="intent-1",
        realized_pnl=8_000,
        highest_price=108_000,
        lowest_price=99_000,
        opened_at=now,
        closed_at=later,
    )
    _insert_position(
        connection,
        position_id="pos-stop",
        source_intent_id="intent-1",
        realized_pnl=-4_000,
        highest_price=101_000,
        lowest_price=96_000,
        opened_at=now,
        closed_at=later,
    )
    _insert_exit(connection, "sig-win", "exit-win", "pos-win", "order-sell-win", "TAKE_PROFIT")
    _insert_exit(
        connection,
        "sig-stop",
        "exit-stop",
        "pos-stop",
        "order-sell-stop",
        "STOP_LOSS",
    )
    _insert_position_event(
        connection,
        "event-win",
        "pos-win",
        "order-sell-win",
        108_000,
        8_000,
        {"mfe": 0.08, "mae": -0.01, "exit_reason": "TAKE_PROFIT"},
    )
    _insert_position_event(
        connection,
        "event-stop",
        "pos-stop",
        "order-sell-stop",
        96_000,
        -4_000,
        {},
    )
    _insert_ttl_cancel(connection, "cancel-1", "order-buy-1", now)
    _insert_operating_run(
        connection,
        "operating-1",
        now,
        {"reconcile": {"duration_ms": 10.0}, "buy": {"duration_ms": 20.0}},
    )
    _insert_operating_run(
        connection,
        "operating-2",
        later,
        {
            "reconcile": {"duration_ms": 30.0},
            "preflight": {"duration_ms": 5.0},
            "buy": {"duration_ms": 40.0},
        },
    )
    connection.commit()


def _insert_candidate(connection, candidate_id, code, name, state, observed_at) -> None:
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            detected_at,
            last_seen_at,
            state_updated_at,
            primary_source_type,
            primary_source_id
        )
        VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, 'condition', ?)
        """,
        (candidate_id, TRADE_DATE, code, name, state, observed_at, observed_at, observed_at, code),
    )


def _insert_strategy(connection, candidate_id, code, status, reasons, observed_at) -> None:
    connection.execute(
        """
        INSERT INTO strategy_observations_latest (
            candidate_instance_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            primary_setup_type,
            primary_setup_status,
            score,
            confidence,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, ?, 'Name', ?, ?, 'THEME_LEADER_PULLBACK', ?, 0.8, 0.7, ?, 'test', 1)
        """,
        (
            candidate_id,
            f"strategy-{candidate_id}",
            TRADE_DATE,
            code,
            observed_at,
            status,
            status,
            json.dumps(reasons),
        ),
    )


def _insert_risk(connection, candidate_id, code, status, reasons, observed_at) -> None:
    connection.execute(
        """
        INSERT INTO risk_observations_latest (
            candidate_instance_id,
            risk_observation_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            max_severity,
            blocked_count,
            caution_count,
            pass_count,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES (?, ?, ?, ?, ?, 'Name', ?, ?, 'INFO', 0, 0, 1, ?, 'test', 1)
        """,
        (
            candidate_id,
            f"risk-{candidate_id}",
            f"strategy-{candidate_id}",
            TRADE_DATE,
            code,
            observed_at,
            status,
            json.dumps(reasons),
        ),
    )


def _insert_order_plan(
    connection,
    plan_id,
    candidate_id,
    code,
    status,
    reasons,
    created_at,
) -> None:
    connection.execute(
        """
        INSERT INTO order_plan_drafts_latest (
            idempotency_key,
            order_plan_id,
            trade_date,
            candidate_instance_id,
            code,
            name,
            side,
            status,
            setup_type,
            entry_timing_state,
            price_location_state,
            current_price,
            limit_price,
            limit_price_source,
            suggested_quantity,
            suggested_notional,
            max_notional,
            risk_budget_source,
            expires_at,
            reason_codes_json,
            evidence_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, 'Name', 'BUY', ?, 'THEME_LEADER_PULLBACK',
            'MOMENTUM_CONTINUATION', 'VWAP_RECLAIM', 100000, 100000, 'CURRENT_PRICE',
            1, 100000, 100000, 'TEST', '2026-06-27T09:30:00+09:00', ?, '{}', ?)
        """,
        (
            f"idem-{plan_id}",
            plan_id,
            TRADE_DATE,
            candidate_id,
            code,
            status,
            json.dumps(reasons),
            created_at,
        ),
    )


def _insert_gateway_command(connection, command_id, created_at) -> None:
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
            created_at
        )
        VALUES (?, 'send_order', 'live_sim', 'ACKED', ?, '{}', ?, ?)
        """,
        (command_id, f"idem-{command_id}", f"hash-{command_id}", created_at),
    )


def _insert_live_sim_intent(
    connection,
    *,
    intent_id,
    candidate_id,
    code,
    status,
    command_id,
    limit_price,
    evidence,
    created_at,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_intents (
            live_sim_intent_id,
            candidate_instance_id,
            strategy_observation_id,
            risk_observation_id,
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
            reason_codes_json,
            evidence_json,
            idempotency_key,
            gateway_command_id,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'Name', 'BUY', 'LIMIT', 1, ?, ?, ?, '[]', ?, ?, ?, ?)
        """,
        (
            intent_id,
            candidate_id,
            f"strategy-{candidate_id}",
            f"risk-{candidate_id}",
            TRADE_DATE,
            ACCOUNT_ID,
            code,
            limit_price,
            limit_price,
            status,
            json.dumps(evidence),
            f"idem-{intent_id}",
            command_id,
            created_at,
        ),
    )


def _insert_live_sim_order(
    connection,
    *,
    order_id,
    intent_id,
    command_id,
    side,
    limit_price,
    status,
    filled_quantity,
    avg_fill_price,
    created_at,
) -> None:
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
            broker_order_no,
            filled_quantity,
            remaining_quantity,
            avg_fill_price,
            idempotency_key,
            created_at,
            command_queued_at
        )
        VALUES (?, ?, ?, ?, ?, '005930', 'Name', ?, 'LIMIT', 1, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?)
        """,
        (
            order_id,
            intent_id,
            command_id,
            TRADE_DATE,
            ACCOUNT_ID,
            side,
            limit_price,
            limit_price,
            status,
            f"broker-{order_id}",
            filled_quantity,
            1 - filled_quantity,
            avg_fill_price,
            f"idem-{order_id}",
            created_at,
            created_at,
        ),
    )


def _insert_execution(connection, execution_id, order_id, intent_id, price, executed_at) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_executions (
            live_sim_execution_id,
            live_sim_order_id,
            live_sim_intent_id,
            broker_order_no,
            account_id,
            code,
            side,
            quantity,
            price,
            notional,
            executed_at
        )
        VALUES (?, ?, ?, ?, ?, '005930', 'BUY', 1, ?, ?, ?)
        """,
        (
            execution_id,
            order_id,
            intent_id,
            f"broker-{order_id}",
            ACCOUNT_ID,
            price,
            price,
            executed_at,
        ),
    )


def _insert_position(
    connection,
    *,
    position_id,
    source_intent_id,
    realized_pnl,
    highest_price,
    lowest_price,
    opened_at,
    closed_at,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id,
            account_id,
            trade_date,
            code,
            name,
            quantity,
            available_quantity,
            avg_entry_price,
            total_entry_notional,
            realized_pnl,
            unrealized_pnl,
            highest_price,
            lowest_price,
            opened_at,
            closed_at,
            last_price,
            last_price_at,
            status,
            source_live_sim_intent_id,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, '005930', 'Name', 0, 0, 100000, 0, ?, 0, ?, ?, ?, ?, ?, ?,
            'CLOSED', ?, ?, ?)
        """,
        (
            position_id,
            ACCOUNT_ID,
            TRADE_DATE,
            realized_pnl,
            highest_price,
            lowest_price,
            opened_at,
            closed_at,
            lowest_price,
            closed_at,
            source_intent_id,
            opened_at,
            closed_at,
        ),
    )


def _insert_exit(connection, signal_id, intent_id, position_id, order_id, reason) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_exit_signals (
            exit_signal_id,
            position_id,
            code,
            reason,
            quantity,
            status
        )
        VALUES (?, ?, '005930', ?, 1, 'CLOSED')
        """,
        (signal_id, position_id, reason),
    )
    connection.execute(
        """
        INSERT INTO live_sim_exit_intents (
            exit_intent_id,
            position_id,
            exit_signal_id,
            live_sim_order_id,
            code,
            quantity,
            limit_price,
            reason,
            status,
            idempotency_key
        )
        VALUES (?, ?, ?, ?, '005930', 1, 100000, ?, 'CLOSED', ?)
        """,
        (intent_id, position_id, signal_id, order_id, reason, f"idem-{intent_id}"),
    )


def _insert_position_event(
    connection,
    event_id,
    position_id,
    order_id,
    price,
    realized_pnl,
    evidence,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_position_events (
            event_id,
            position_id,
            event_type,
            live_sim_order_id,
            code,
            quantity_delta,
            price,
            realized_pnl,
            evidence_json
        )
        VALUES (?, ?, 'POSITION_CLOSED', ?, '005930', -1, ?, ?, ?)
        """,
        (event_id, position_id, order_id, price, realized_pnl, json.dumps(evidence)),
    )


def _insert_ttl_cancel(connection, cancel_id, order_id, created_at) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_cancel_intents (
            cancel_intent_id,
            live_sim_order_id,
            code,
            cancel_quantity,
            reason,
            status,
            idempotency_key,
            created_at
        )
        VALUES (?, ?, '005930', 1, 'TTL_EXPIRED', 'ACKED', ?, ?)
        """,
        (cancel_id, order_id, f"idem-{cancel_id}", created_at),
    )


def _insert_operating_run(connection, run_id, created_at, stage_latency) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_operating_runs (
            run_id,
            trade_date,
            mode,
            queue_commands,
            preflight_status,
            status,
            buy_evaluated_count,
            buy_command_count,
            cancel_candidate_count,
            cancel_command_count,
            exit_signal_count,
            exit_command_count,
            reconcile_status,
            no_buy_status,
            ai_run_status,
            reason_summary_json,
            warnings_json,
            errors_json,
            stage_latency_json,
            created_at
        )
        VALUES (?, ?, 'PILOT_FULL_LIFECYCLE', 1, 'PASS', 'COMPLETED',
            0, 0, 0, 0, 0, 0, 'OK', 'SKIPPED', 'SKIPPED',
            '{}', '[]', '[]', ?, ?)
        """,
        (run_id, TRADE_DATE, json.dumps(stage_latency), created_at),
    )


def _table_counts(connection) -> dict[str, int]:
    return {
        "gateway_commands": connection.execute(
            "SELECT COUNT(*) AS count FROM gateway_commands"
        ).fetchone()["count"],
        "live_sim_orders": connection.execute(
            "SELECT COUNT(*) AS count FROM live_sim_orders"
        ).fetchone()["count"],
        "live_sim_intents": connection.execute(
            "SELECT COUNT(*) AS count FROM live_sim_intents"
        ).fetchone()["count"],
    }
