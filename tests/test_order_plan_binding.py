from __future__ import annotations

from copy import deepcopy
from datetime import timedelta

from domain.broker.utils import datetime_to_wire, utc_now
from services.live_sim.order_plan_binding import (
    ORDER_PLAN_BINDING_CONTRACT,
    build_order_plan_binding,
    validate_order_plan_binding,
)
from tests.test_live_sim_order_plan_pipeline import _prepared_order_plan_connection


def test_order_plan_binding_validates_exact_coherent_plan(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "binding-pass.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()

    binding = build_order_plan_binding(plan)
    result = validate_order_plan_binding(
        connection,
        binding,
        max_age_sec=999_999_999,
        expected_trade_date="2026-06-27",
    )
    connection.close()

    assert binding["contract"] == ORDER_PLAN_BINDING_CONTRACT
    assert len(binding["order_plan_snapshot_sha256"]) == 64
    assert len(binding["binding_sha256"]) == 64
    assert result.passed is True
    assert result.reason_codes == ()
    assert result.order_plan["order_plan_id"] == order_plan_id
    assert result.lineage["status"] == "PASS"
    assert result.to_dict()["read_only"] is True


def test_order_plan_binding_rejects_binding_and_plan_content_drift(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "binding-drift.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    binding = build_order_plan_binding(plan)

    tampered_binding = deepcopy(binding)
    tampered_binding["source_run_id"] = "swapped-run"
    binding_drift = validate_order_plan_binding(
        connection,
        tampered_binding,
        max_age_sec=999_999_999,
    )

    connection.execute(
        "UPDATE order_plan_drafts SET limit_price = limit_price + 1000 "
        "WHERE order_plan_id = ?",
        (order_plan_id,),
    )
    connection.commit()
    plan_drift = validate_order_plan_binding(
        connection,
        binding,
        max_age_sec=999_999_999,
    )
    connection.close()

    assert binding_drift.passed is False
    assert "ORDER_PLAN_BINDING_HASH_MISMATCH" in binding_drift.reason_codes
    assert plan_drift.passed is False
    assert (
        "ORDER_PLAN_BINDING_ORDER_PLAN_SNAPSHOT_MISMATCH"
        in plan_drift.reason_codes
    )


def test_order_plan_binding_rejects_latest_and_pipeline_identity_swap(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "binding-latest-swap.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    binding = build_order_plan_binding(plan)

    connection.execute(
        """
        UPDATE order_plan_drafts_latest
        SET entry_timing_evaluation_id = 'entry-swapped'
        WHERE idempotency_key = ?
        """,
        (binding["idempotency_key"],),
    )
    connection.execute(
        """
        UPDATE strategy_observations_latest
        SET source_run_id = 'source-run-swapped'
        WHERE candidate_instance_id = ?
        """,
        (binding["candidate_instance_id"],),
    )
    connection.commit()

    result = validate_order_plan_binding(
        connection,
        binding,
        max_age_sec=999_999_999,
    )
    command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert result.passed is False
    assert "ORDER_PLAN_BINDING_LATEST_MISMATCH" in result.reason_codes
    assert "ORDER_PLAN_BINDING_STRATEGY_LATEST_MISMATCH" in result.reason_codes
    assert "ORDER_PLAN_LINEAGE_INVALID" in result.reason_codes
    assert command_count == 0


def test_order_plan_binding_rejects_expired_exact_binding(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "binding-expired.sqlite3"
    )
    expired_at = datetime_to_wire(utc_now() - timedelta(seconds=1))
    for table_name in ("order_plan_drafts", "order_plan_drafts_latest"):
        connection.execute(
            f"UPDATE {table_name} SET expires_at = ? WHERE order_plan_id = ?",
            (expired_at, order_plan_id),
        )
    connection.commit()
    expired_plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    binding = build_order_plan_binding(expired_plan)

    result = validate_order_plan_binding(
        connection,
        binding,
        max_age_sec=999_999_999,
    )
    connection.close()

    assert result.passed is False
    assert "ORDER_PLAN_EXPIRED" in result.reason_codes


def test_order_plan_binding_checks_embedded_intent_binding(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "binding-intent.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    binding = build_order_plan_binding(plan)
    expected_intent = {
        "order_plan_id": binding["order_plan_id"],
        "trade_date": binding["trade_date"],
        "candidate_instance_id": binding["candidate_instance_id"],
        "strategy_observation_id": binding["strategy_observation_id"],
        "risk_observation_id": binding["risk_observation_id"],
        "code": plan["code"],
        "side": plan["side"],
        "evidence_json": {"order_plan_binding": binding},
    }

    passed = validate_order_plan_binding(
        connection,
        binding,
        max_age_sec=999_999_999,
        expected_intent=expected_intent,
    )
    expected_intent["evidence_json"] = {
        "order_plan_binding": {**binding, "binding_sha256": "0" * 64}
    }
    failed = validate_order_plan_binding(
        connection,
        binding,
        max_age_sec=999_999_999,
        expected_intent=expected_intent,
    )
    connection.close()

    assert passed.passed is True
    assert failed.passed is False
    assert (
        "ORDER_PLAN_BINDING_INTENT_BINDING_MISMATCH"
        in failed.reason_codes
    )


def test_order_plan_binding_fails_closed_on_malformed_persisted_json(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "binding-malformed-json.sqlite3"
    )
    plan = connection.execute(
        "SELECT * FROM order_plan_drafts WHERE order_plan_id = ?",
        (order_plan_id,),
    ).fetchone()
    binding = build_order_plan_binding(plan)
    connection.execute(
        "UPDATE order_plan_drafts SET evidence_json = 'not-json' "
        "WHERE order_plan_id = ?",
        (order_plan_id,),
    )
    connection.commit()

    result = validate_order_plan_binding(
        connection,
        binding,
        max_age_sec=999_999_999,
    )
    connection.close()

    assert result.passed is False
    assert (
        "ORDER_PLAN_BINDING_ORDER_PLAN_SNAPSHOT_INVALID"
        in result.reason_codes
    )
