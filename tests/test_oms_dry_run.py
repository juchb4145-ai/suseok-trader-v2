from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest
from domain.broker.utils import datetime_to_wire, utc_now
from domain.candidate.state import CandidateState
from domain.oms.reasons import DryRunRejectionReason
from domain.oms.status import DryRunIntentStatus, DryRunOrderStatus
from services.config import Settings, TradingMode, TradingProfile
from services.oms.dry_run_service import (
    convert_intent_to_dry_run_order,
    create_dry_run_intent,
    evaluate_dry_run_eligibility,
    simulate_fill_dry_run_order,
    update_dry_run_positions_mark_to_market,
)
from services.oms.safety_gate import check_pr10_safety_gate
from services.risk_gate import evaluate_risk_for_candidate, save_risk_observation
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture


def test_safety_gate_blocks_without_review_draft_and_allows_test_bypass(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dry-run-safety.sqlite3")

    blocked = check_pr10_safety_gate(connection, Settings())
    bypassed = check_pr10_safety_gate(
        connection,
        Settings(dry_run_allow_without_safety_draft_for_tests=True),
    )
    live_flag = check_pr10_safety_gate(
        connection,
        Settings(
            trading_allow_live_sim=True,
            dry_run_allow_without_safety_draft_for_tests=True,
        ),
    )
    connection.close()

    assert blocked.passed is False
    assert DryRunRejectionReason.SAFETY_GATE_FAILED.value in blocked.reason_codes
    assert bypassed.passed is True
    assert bypassed.order_routing_disabled is True
    assert bypassed.gateway_order_commands_disabled is True
    assert live_flag.passed is False
    assert DryRunRejectionReason.LIVE_FLAGS_ENABLED.value in live_flag.reason_codes


def test_eligibility_requires_strategy_risk_candidate_tick_and_saves_check(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "dry-run-eligibility.sqlite3")
    settings = _settings()

    eligible = evaluate_dry_run_eligibility(connection, candidate_id, settings=settings)
    connection.execute(
        """
        UPDATE strategy_observations_latest
        SET overall_status = 'NO_SETUP'
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    )
    strategy_blocked = evaluate_dry_run_eligibility(connection, candidate_id, settings=settings)
    connection.execute(
        """
        UPDATE strategy_observations_latest
        SET overall_status = 'MATCHED_OBSERVATION'
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    )
    connection.execute(
        """
        UPDATE risk_observations_latest
        SET overall_status = 'OBSERVE_BLOCK'
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    )
    risk_blocked = evaluate_dry_run_eligibility(connection, candidate_id, settings=settings)
    connection.execute(
        """
        UPDATE risk_observations_latest
        SET overall_status = 'OBSERVE_PASS'
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    )
    connection.execute(
        "UPDATE candidates SET state = 'WATCHING' WHERE candidate_instance_id = ?",
        (candidate_id,),
    )
    candidate_blocked = evaluate_dry_run_eligibility(connection, candidate_id, settings=settings)
    check_count = connection.execute(
        "SELECT COUNT(*) AS count FROM dry_run_eligibility_checks"
    ).fetchone()["count"]
    connection.close()

    assert eligible.eligible is True
    assert eligible.evidence_json["admission_trace"]["policy"] == "dry_run_shadow"
    assert eligible.evidence_json["admission_trace"]["reason_codes"] == []
    assert strategy_blocked.eligible is False
    assert DryRunRejectionReason.STRATEGY_NOT_MATCHED.value in strategy_blocked.reason_codes
    assert strategy_blocked.evidence_json["admission_trace"]["reason_codes"] == [
        DryRunRejectionReason.STRATEGY_NOT_MATCHED.value
    ]
    assert risk_blocked.eligible is False
    assert DryRunRejectionReason.RISK_NOT_OBSERVE_PASS.value in risk_blocked.reason_codes
    assert candidate_blocked.eligible is False
    assert DryRunRejectionReason.CANDIDATE_NOT_CONTEXT_READY.value in candidate_blocked.reason_codes
    assert check_count == 4


def test_live_sim_pilot_allows_shadow_dry_run_evidence(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "dry-run-live-sim-pilot.sqlite3")
    settings = _settings(
        trading_profile=TradingProfile.LIVE_SIM_PILOT,
        trading_mode=TradingMode.LIVE_SIM,
        trading_allow_live_sim=True,
        trading_allow_live_real=False,
    )

    eligible = evaluate_dry_run_eligibility(connection, candidate_id, settings=settings)
    intent = create_dry_run_intent(connection, candidate_id, settings=settings)
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert eligible.eligible is True
    assert intent.status is DryRunIntentStatus.CREATED
    assert DryRunRejectionReason.LIVE_FLAGS_ENABLED.value not in eligible.reason_codes
    assert eligible.evidence_json["live_sim_pilot_shadow_dry_run"] is True
    assert eligible.evidence_json["ignored_safety_reason_codes"] == [
        DryRunRejectionReason.LIVE_FLAGS_ENABLED.value
    ]
    assert command_count == 0


def test_eligibility_blocks_missing_stale_and_duplicate_market_state(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "dry-run-blocks.sqlite3")
    settings = _settings(dry_run_stale_tick_sec=1)
    stale_ts = datetime_to_wire(utc_now()).replace("Z", "+00:00")
    connection.execute(
        """
        UPDATE market_ticks_latest
        SET event_ts = '2020-01-01T00:00:00Z'
        WHERE code = '005930'
        """
    )
    stale = evaluate_dry_run_eligibility(connection, candidate_id, settings=settings)
    connection.execute("DELETE FROM market_ticks_latest WHERE code = '005930'")
    missing = evaluate_dry_run_eligibility(connection, candidate_id, settings=_settings())
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO market_ticks_latest (
            code,
            name,
            price,
            change_rate,
            cumulative_volume,
            cumulative_trade_value,
            execution_strength,
            best_bid,
            best_ask,
            spread_ticks,
            day_high,
            day_low,
            trade_time,
            event_ts,
            received_at,
            source,
            event_id,
            quality_status,
            updated_at
        )
        VALUES ('005930', '삼성전자', 97000, 2, 1000, 97000000, 120, 96900, 97000,
            1, 100000, 94000, ?, ?, ?, 'test', 'evt-restored', 'FRESH', ?)
        """,
        (stale_ts, now, now, now),
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
            status,
            opened_at,
            updated_at
        )
        VALUES ('position-dup', '2026-06-27', '005930', '삼성전자', 1, 97000, 97000,
            'OPEN', ?, ?)
        """,
        (now, now),
    )
    duplicate = evaluate_dry_run_eligibility(connection, candidate_id, settings=_settings())
    connection.close()

    assert stale.eligible is False
    assert DryRunRejectionReason.LATEST_TICK_STALE.value in stale.reason_codes
    assert missing.eligible is False
    assert DryRunRejectionReason.LATEST_TICK_MISSING.value in missing.reason_codes
    assert duplicate.eligible is False
    assert DryRunRejectionReason.DUPLICATE_DRY_RUN_POSITION.value in duplicate.reason_codes


def test_intent_creation_is_disabled_by_default_and_never_creates_gateway_command(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "dry-run-disabled.sqlite3")

    intent = create_dry_run_intent(connection, candidate_id, settings=Settings())
    rejection_count = connection.execute(
        "SELECT COUNT(*) AS count FROM dry_run_intent_rejections"
    ).fetchone()["count"]
    gateway_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert intent.status is DryRunIntentStatus.REJECTED
    assert DryRunRejectionReason.DRY_RUN_DISABLED.value in intent.reason_codes
    assert rejection_count == 1
    assert gateway_count == 0


def test_intent_order_fill_position_and_ledger_lifecycle_is_internal_only(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "dry-run-lifecycle.sqlite3")
    before_candidate = connection.execute(
        "SELECT state FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()["state"]

    intent = create_dry_run_intent(connection, candidate_id, settings=_settings())
    order = convert_intent_to_dry_run_order(connection, intent.dry_run_intent_id, _settings())
    with pytest.raises(ValueError, match=DryRunRejectionReason.SIMULATED_FILL_DISABLED.value):
        simulate_fill_dry_run_order(connection, order.dry_run_order_id, _settings())
    execution = simulate_fill_dry_run_order(
        connection,
        order.dry_run_order_id,
        _settings(dry_run_simulated_fill_enabled=True),
    )
    position = connection.execute("SELECT * FROM dry_run_positions").fetchone()
    ledger_count = connection.execute("SELECT COUNT(*) AS count FROM dry_run_ledger").fetchone()[
        "count"
    ]
    gateway_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    after_candidate = connection.execute(
        "SELECT state FROM candidates WHERE candidate_instance_id = ?",
        (candidate_id,),
    ).fetchone()["state"]
    stored_intent = connection.execute("SELECT status FROM dry_run_intents").fetchone()["status"]
    stored_order = connection.execute("SELECT status FROM dry_run_orders").fetchone()["status"]
    connection.close()

    assert intent.status is DryRunIntentStatus.CREATED
    assert intent.dry_run_only is True
    assert intent.live_order_allowed is False
    assert intent.gateway_command_allowed is False
    assert order.status is DryRunOrderStatus.CREATED
    assert execution.dry_run_only is True
    assert position["status"] == "OPEN"
    assert position["quantity"] == intent.quantity
    assert ledger_count >= 4
    assert gateway_count == 0
    assert before_candidate == after_candidate == CandidateState.CONTEXT_READY.value
    assert stored_intent == DryRunIntentStatus.CONVERTED_TO_DRY_RUN_ORDER.value
    assert stored_order == DryRunOrderStatus.SIMULATED_FILLED.value


def test_mark_to_market_updates_simulated_position_only(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "dry-run-mtm.sqlite3")
    intent = create_dry_run_intent(connection, candidate_id, settings=_settings())
    order = convert_intent_to_dry_run_order(connection, intent.dry_run_intent_id, _settings())
    simulate_fill_dry_run_order(
        connection,
        order.dry_run_order_id,
        _settings(dry_run_simulated_fill_enabled=True),
    )
    connection.execute("UPDATE market_ticks_latest SET price = 98000 WHERE code = '005930'")

    result = update_dry_run_positions_mark_to_market(connection, settings=_settings())
    position = connection.execute("SELECT * FROM dry_run_positions").fetchone()
    gateway_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert result["updated_count"] == 1
    assert position["last_price"] == 98_000
    assert position["unrealized_pnl"] == (98_000 - 97_000) * intent.quantity
    assert gateway_count == 0


def test_evaluate_dry_run_cli_prints_eligibility_without_creating_intent(tmp_path) -> None:
    db_path = tmp_path / "dry-run-cli.sqlite3"
    connection, candidate_id = _prepared_connection(db_path)
    connection.close()
    env = os.environ.copy()
    env.update(
        {
            "TRADING_DB_PATH": str(db_path),
            "DRY_RUN_OMS_ENABLED": "true",
            "DRY_RUN_INTENT_CREATION_ENABLED": "true",
            "DRY_RUN_ALLOW_WITHOUT_SAFETY_DRAFT_FOR_TESTS": "true",
            "DRY_RUN_STALE_TICK_SEC": "999999999",
        }
    )

    result = subprocess.run(
        [
            sys.executable,
            "tools/evaluate_dry_run_eligibility.py",
            "--candidate-instance-id",
            candidate_id,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    payload = json.loads(result.stdout)
    connection = initialize_database(db_path)
    intent_count = connection.execute("SELECT COUNT(*) AS count FROM dry_run_intents").fetchone()[
        "count"
    ]
    connection.close()

    assert payload["eligible"] is True
    assert intent_count == 0


def _prepared_connection(db_path):
    connection = initialize_database(db_path)
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, risk)
    connection.commit()
    return connection, candidate_id


def _settings(**overrides) -> Settings:
    values = {
        "market_data_tick_stale_sec": 999_999_999,
        "market_data_degraded_tick_stale_sec": 999_999_999,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
        "strategy_engine_stale_tick_sec": 999_999_999,
        "risk_gate_stale_tick_sec": 999_999_999,
        "risk_gate_strategy_stale_sec": 999_999_999,
        "dry_run_oms_enabled": True,
        "dry_run_intent_creation_enabled": True,
        "dry_run_allow_without_safety_draft_for_tests": True,
        "dry_run_stale_tick_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)
