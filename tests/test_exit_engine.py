from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from domain.broker.utils import datetime_to_wire, utc_now
from domain.exit.reasons import DryRunExitReasonCode
from domain.exit.status import (
    DryRunExitEvaluationStatus,
    DryRunExitIntentStatus,
    DryRunExitOrderStatus,
    DryRunExitSignalType,
)
from services.config import Settings
from services.exit_engine import (
    convert_exit_intent_to_dry_run_order,
    create_dry_run_exit_intent,
    evaluate_dry_run_exit_for_position,
    get_exit_status,
    list_exit_signals,
    simulate_fill_dry_run_exit_order,
    update_position_high_low_watermark,
)
from storage.sqlite import initialize_database
from tests.test_oms_dry_run import _prepared_connection

ROOT = Path(__file__).resolve().parents[1]


def test_exit_evaluation_handles_invalid_missing_and_hold_states(tmp_path) -> None:
    connection, _ = _prepared_connection(tmp_path / "exit-invalid.sqlite3")
    missing = evaluate_dry_run_exit_for_position(
        connection,
        "missing-position",
        settings=_exit_settings(),
    )
    closed_position_id = _insert_position(connection, status="CLOSED")
    closed = evaluate_dry_run_exit_for_position(
        connection,
        closed_position_id,
        settings=_exit_settings(),
    )
    hold_position_id = _insert_position(connection, avg_price=97_000)
    hold = evaluate_dry_run_exit_for_position(
        connection,
        hold_position_id,
        settings=_exit_settings(),
    )
    connection.execute("DELETE FROM market_ticks_latest WHERE code = '005930'")
    data_wait = evaluate_dry_run_exit_for_position(
        connection,
        hold_position_id,
        settings=_exit_settings(),
    )
    connection.close()

    assert missing.status is DryRunExitEvaluationStatus.INVALID_POSITION
    assert DryRunExitReasonCode.POSITION_NOT_FOUND.value in missing.reason_codes
    assert closed.status is DryRunExitEvaluationStatus.INVALID_POSITION
    assert DryRunExitReasonCode.POSITION_ALREADY_CLOSED.value in closed.reason_codes
    assert hold.status is DryRunExitEvaluationStatus.HOLD_OBSERVED
    assert DryRunExitReasonCode.NO_EXIT_SIGNAL.value in hold.reason_codes
    assert data_wait.status is DryRunExitEvaluationStatus.DATA_WAIT
    assert DryRunExitReasonCode.LATEST_TICK_MISSING.value in data_wait.reason_codes


def test_exit_evaluation_price_time_and_stale_rules_save_signals(tmp_path) -> None:
    stop_eval, stop_signals = _evaluate_rule_fixture(
        tmp_path / "exit-stop.sqlite3",
        avg_price=100_000,
    )
    take_eval, take_signals = _evaluate_rule_fixture(
        tmp_path / "exit-take.sqlite3",
        avg_price=90_000,
    )
    trailing_eval, trailing_signals = _evaluate_rule_fixture(
        tmp_path / "exit-trailing.sqlite3",
        avg_price=100_000,
        latest_price=100_000,
        position_last_price=105_000,
    )
    max_hold_eval, max_hold_signals = _evaluate_rule_fixture(
        tmp_path / "exit-max-hold.sqlite3",
        avg_price=97_000,
        opened_age_sec=3_600,
    )
    stale_eval, stale_signals = _evaluate_rule_fixture(
        tmp_path / "exit-stale.sqlite3",
        avg_price=97_000,
        stale_tick=True,
    )

    assert stop_eval.status is DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    assert _signal_types(stop_signals) == {DryRunExitSignalType.STOP_LOSS.value}
    assert take_eval.status is DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    assert DryRunExitSignalType.TAKE_PROFIT.value in _signal_types(take_signals)
    assert trailing_eval.status is DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    assert DryRunExitSignalType.TRAILING_STOP.value in _signal_types(trailing_signals)
    assert max_hold_eval.status is DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    assert DryRunExitSignalType.MAX_HOLD.value in _signal_types(max_hold_signals)
    assert stale_eval.status is DryRunExitEvaluationStatus.EXIT_CAUTION_OBSERVED
    assert _signal_types(stale_signals) == {
        DryRunExitSignalType.DATA_STALE_EXIT_CAUTION.value,
    }


def test_exit_evaluation_theme_risk_and_strategy_rules_are_read_only(tmp_path) -> None:
    theme_eval, theme_signals = _evaluate_rule_fixture(
        tmp_path / "exit-theme.sqlite3",
        avg_price=97_000,
        mutate=lambda connection: connection.execute(
            "UPDATE theme_latest_snapshots SET state = 'FADING' WHERE theme_id = 'theme-005930'"
        ),
    )
    risk_eval, risk_signals = _evaluate_rule_fixture(
        tmp_path / "exit-risk.sqlite3",
        avg_price=97_000,
        mutate=lambda connection: connection.execute(
            "UPDATE risk_observations_latest SET overall_status = 'OBSERVE_BLOCK'"
        ),
    )
    strategy_eval, strategy_signals = _evaluate_rule_fixture(
        tmp_path / "exit-strategy.sqlite3",
        avg_price=97_000,
        mutate=lambda connection: connection.execute(
            "UPDATE strategy_observations_latest SET overall_status = 'NO_SETUP'"
        ),
    )

    assert theme_eval.status is DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    assert DryRunExitSignalType.THEME_WEAKENING.value in _signal_types(theme_signals)
    assert risk_eval.status is DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    assert DryRunExitSignalType.RISK_DETERIORATION.value in _signal_types(risk_signals)
    assert strategy_eval.status is DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    assert DryRunExitSignalType.STRATEGY_INVALIDATED.value in _signal_types(strategy_signals)


def test_exit_intent_order_fill_close_position_and_never_create_gateway_command(tmp_path) -> None:
    connection, _ = _prepared_connection(tmp_path / "exit-lifecycle.sqlite3")
    position_id = _insert_position(connection, avg_price=100_000, quantity=10)
    before_candidate = connection.execute("SELECT state FROM candidates LIMIT 1").fetchone()[
        "state"
    ]
    before_strategy = connection.execute(
        "SELECT overall_status FROM strategy_observations_latest LIMIT 1"
    ).fetchone()["overall_status"]
    before_risk = connection.execute(
        "SELECT overall_status FROM risk_observations_latest LIMIT 1"
    ).fetchone()["overall_status"]

    rejected = create_dry_run_exit_intent(connection, position_id, settings=Settings())
    intent = create_dry_run_exit_intent(connection, position_id, settings=_exit_settings())
    with pytest.raises(ValueError, match=DryRunExitReasonCode.EXIT_ORDER_CREATION_DISABLED.value):
        convert_exit_intent_to_dry_run_order(
            connection, intent.dry_run_exit_intent_id, _exit_settings()
        )
    order = convert_exit_intent_to_dry_run_order(
        connection,
        intent.dry_run_exit_intent_id,
        _exit_settings(dry_run_exit_order_creation_enabled=True),
    )
    with pytest.raises(ValueError, match=DryRunExitReasonCode.SIMULATED_EXIT_FILL_DISABLED.value):
        simulate_fill_dry_run_exit_order(connection, order.dry_run_exit_order_id, _exit_settings())
    execution = simulate_fill_dry_run_exit_order(
        connection,
        order.dry_run_exit_order_id,
        _exit_settings(
            dry_run_exit_order_creation_enabled=True,
            dry_run_exit_simulated_fill_enabled=True,
        ),
    )

    position = connection.execute(
        "SELECT * FROM dry_run_positions WHERE dry_run_position_id = ?",
        (position_id,),
    ).fetchone()
    stored_order = connection.execute("SELECT status FROM dry_run_exit_orders").fetchone()["status"]
    ledger_events = {
        row["event_type"]
        for row in connection.execute("SELECT event_type FROM dry_run_ledger").fetchall()
    }
    gateway_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    after_candidate = connection.execute("SELECT state FROM candidates LIMIT 1").fetchone()["state"]
    after_strategy = connection.execute(
        "SELECT overall_status FROM strategy_observations_latest LIMIT 1"
    ).fetchone()["overall_status"]
    after_risk = connection.execute(
        "SELECT overall_status FROM risk_observations_latest LIMIT 1"
    ).fetchone()["overall_status"]
    connection.close()

    assert rejected.status is DryRunExitIntentStatus.REJECTED
    assert intent.status is DryRunExitIntentStatus.CREATED
    assert intent.side.value == "SELL"
    assert intent.close_only is True
    assert intent.dry_run_only is True
    assert intent.live_order_allowed is False
    assert intent.gateway_command_allowed is False
    assert intent.broker_order_sent is False
    assert order.status is DryRunExitOrderStatus.CREATED
    assert stored_order == DryRunExitOrderStatus.SIMULATED_FILLED.value
    assert execution.execution_type == "SIMULATED_EXIT"
    assert execution.realized_pnl == (97_000 - 100_000) * 10
    assert position["status"] == "CLOSED"
    assert position["quantity"] == 0
    assert position["realized_pnl"] == execution.realized_pnl
    assert {"EXIT_EVALUATION", "EXIT_INTENT_CREATED", "EXIT_ORDER_CREATED"}.issubset(ledger_events)
    assert {"SIMULATED_EXIT_FILL", "POSITION_CLOSED"}.issubset(ledger_events)
    assert gateway_count == 0
    assert before_candidate == after_candidate
    assert before_strategy == after_strategy
    assert before_risk == after_risk


def test_exit_watermark_updates_and_status_counts(tmp_path) -> None:
    connection, _ = _prepared_connection(tmp_path / "exit-watermark.sqlite3")
    position_id = _insert_position(connection, avg_price=100_000, last_price=100_000)

    first = update_position_high_low_watermark(connection, position_id, 101_000)
    second = update_position_high_low_watermark(connection, position_id, 99_000)
    evaluate_dry_run_exit_for_position(connection, position_id, settings=_exit_settings())
    status = get_exit_status(connection, _exit_settings())
    connection.close()

    assert first["high_watermark_price"] == 101_000
    assert second["high_watermark_price"] == 101_000
    assert second["low_watermark_price"] == 99_000
    assert status["evaluation_count"] == 1
    assert status["gateway_command_allowed"] is False
    assert status["broker_order_sent"] is False


def test_exit_cli_evaluate_does_not_create_intent_and_disabled_intent_rejects(tmp_path) -> None:
    db_path = tmp_path / "exit-cli.sqlite3"
    connection, _ = _prepared_connection(db_path)
    position_id = _insert_position(connection, avg_price=100_000)
    connection.close()
    env = os.environ.copy()
    env.update(
        {
            "TRADING_DB_PATH": str(db_path),
            "DRY_RUN_ALLOW_WITHOUT_SAFETY_DRAFT_FOR_TESTS": "true",
            "DRY_RUN_EXIT_STALE_TICK_SEC": "999999999",
        }
    )

    evaluated = subprocess.run(
        [
            sys.executable,
            "tools/evaluate_dry_run_exits.py",
            "--dry-run-position-id",
            position_id,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    rejected = subprocess.run(
        [
            sys.executable,
            "tools/create_dry_run_exit_intent.py",
            "--dry-run-position-id",
            position_id,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    payload = json.loads(evaluated.stdout)
    rejection = json.loads(rejected.stdout)
    connection = initialize_database(db_path)
    created_count = connection.execute(
        "SELECT COUNT(*) AS count FROM dry_run_exit_intents WHERE status = 'CREATED'"
    ).fetchone()["count"]
    connection.close()

    assert payload["status"] == DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED.value
    assert rejection["status"] == DryRunExitIntentStatus.REJECTED.value
    assert DryRunExitReasonCode.DRY_RUN_EXIT_DISABLED.value in rejection["reason_codes"]
    assert created_count == 0


def test_exit_engine_code_has_no_broker_or_gateway_order_surface() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            ROOT / "services" / "exit_engine.py",
            ROOT / "api" / "routes" / "dry_run_exit.py",
            ROOT / "domain" / "exit" / "models.py",
            ROOT / "tools" / "evaluate_dry_run_exits.py",
            ROOT / "tools" / "create_dry_run_exit_intent.py",
            ROOT / "tools" / "create_dry_run_exit_order.py",
            ROOT / "tools" / "simulate_dry_run_exit_fill.py",
        ]
    )

    assert "PyQt5" not in source
    assert "QAxWidget" not in source
    assert "BrokerOrderRequest" not in source
    assert "GatewayCommand(" not in source
    assert "send_order" not in source
    assert "cancel_order" not in source
    assert "modify_order" not in source
    assert "/api/orders/enqueue" not in source
    assert "LIVE_REAL" not in source


def _evaluate_rule_fixture(
    db_path,
    *,
    avg_price: float,
    latest_price: int = 97_000,
    position_last_price: float | None = None,
    opened_age_sec: int = 60,
    stale_tick: bool = False,
    mutate=None,
):
    connection, _ = _prepared_connection(db_path)
    _set_tick(connection, latest_price, stale=stale_tick)
    position_id = _insert_position(
        connection,
        avg_price=avg_price,
        last_price=position_last_price,
        opened_age_sec=opened_age_sec,
    )
    if mutate is not None:
        mutate(connection)
    settings = _exit_settings(dry_run_exit_stale_tick_sec=1) if stale_tick else _exit_settings()
    evaluation = evaluate_dry_run_exit_for_position(connection, position_id, settings=settings)
    signals = list_exit_signals(connection, dry_run_position_id=position_id, limit=20)
    connection.close()
    return evaluation, signals


def _insert_position(
    connection,
    *,
    avg_price: float = 100_000,
    quantity: int = 10,
    status: str = "OPEN",
    last_price: float | None = None,
    opened_age_sec: int = 60,
) -> str:
    now = utc_now()
    opened_at = datetime_to_wire(now - timedelta(seconds=opened_age_sec))
    updated_at = datetime_to_wire(now)
    position_id = (
        f"exit-position-{len(connection.execute('SELECT 1 FROM dry_run_positions').fetchall())}"
    )
    connection.execute(
        """
        INSERT INTO dry_run_positions (
            dry_run_position_id,
            trade_date,
            code,
            name,
            quantity,
            avg_price,
            invested_notional,
            realized_pnl,
            unrealized_pnl,
            last_price,
            status,
            opened_at,
            updated_at,
            closed_at,
            dry_run_only
        )
        VALUES (?, '2026-06-27', '005930', '삼성전자', ?, ?, ?, 0, 0, ?, ?, ?, ?, NULL, 1)
        """,
        (
            position_id,
            quantity,
            avg_price,
            avg_price * quantity,
            last_price,
            status,
            opened_at,
            updated_at,
        ),
    )
    connection.commit()
    return position_id


def _set_tick(connection, price: int, *, stale: bool = False) -> None:
    now = datetime_to_wire(utc_now())
    event_ts = "2020-01-01T00:00:00Z" if stale else now
    connection.execute(
        """
        UPDATE market_ticks_latest
        SET price = ?,
            event_ts = ?,
            received_at = ?,
            updated_at = ?
        WHERE code = '005930'
        """,
        (price, event_ts, now, now),
    )
    connection.commit()


def _exit_settings(**overrides) -> Settings:
    values = {
        "market_data_tick_stale_sec": 999_999_999,
        "market_data_degraded_tick_stale_sec": 999_999_999,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
        "strategy_engine_stale_tick_sec": 999_999_999,
        "risk_gate_stale_tick_sec": 999_999_999,
        "risk_gate_strategy_stale_sec": 999_999_999,
        "dry_run_allow_without_safety_draft_for_tests": True,
        "dry_run_exit_engine_enabled": True,
        "dry_run_exit_intent_creation_enabled": True,
        "dry_run_exit_stale_tick_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)


def _signal_types(signals: list[dict[str, object]]) -> set[str]:
    return {str(signal["signal_type"]) for signal in signals}
