from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot
from services.market_data_service import (
    get_market_data_projection_watermark,
    process_gateway_event,
)
from services.runtime.evaluation_run_guard import (
    EVALUATION_PIPELINE_LOCK,
    runtime_execution_lock,
)
from services.runtime.incremental_evaluation import get_incremental_evaluation_status
from storage.event_store import append_gateway_event
from storage.gateway_command_store import (
    GatewayCommandStatus,
    enqueue_command,
    poll_commands,
)
from storage.sqlite import initialize_database, open_connection


def test_synthetic_tr_response_multiple_ticks_detects_event_id_collision(tmp_path) -> None:
    connection = initialize_database(tmp_path / "tr-collision.sqlite3")
    event_ts = utc_now()
    response = BrokerTrResponse(
        request_id="candidate_quote_refresh:2026-07-07:multi:1",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        success=True,
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "현재가": "+70500",
                "등락율": "+1.25",
                "거래량": "1234",
                "거래대금": "87000000",
                "고가": "+71000",
                "저가": "-69000",
            },
            {
                "종목코드": "A000660",
                "종목명": "SK하이닉스",
                "현재가": "+120000",
                "등락율": "+0.75",
                "거래량": "1000",
                "거래대금": "120000000",
                "고가": "+121000",
                "저가": "-119000",
            },
        ],
        ts=event_ts,
    )
    event = GatewayEvent(
        event_id="evt_candidate_quote_refresh_multi",
        event_type="tr_response",
        source="test-gateway",
        payload=response.to_dict(),
        ts=event_ts,
    )

    append_gateway_event(connection, event)
    result = process_gateway_event(connection, event, settings=Settings())

    sample_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tick_samples"
    ).fetchone()["count"]
    tr_snapshot_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots"
    ).fetchone()["count"]
    error = connection.execute(
        """
        SELECT error_message
        FROM market_projection_errors
        WHERE event_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (event.event_id,),
    ).fetchone()
    watermark = get_market_data_projection_watermark(connection)
    connection.close()

    assert result.status == "ERROR"
    assert "market_tick_samples.event_id" in (result.error_message or "")
    assert sample_count == 0
    assert tr_snapshot_count == 0
    assert error is not None
    assert "market_tick_samples.event_id" in error["error_message"]
    assert watermark.last_event_id == event.event_id


def test_runtime_execution_lock_can_be_reacquired_after_ttl_while_owner_still_running(
    tmp_path,
) -> None:
    db_path = tmp_path / "ttl-overlap.sqlite3"
    first = initialize_database(db_path)
    second = open_connection(db_path)

    try:
        with runtime_execution_lock(
            first,
            EVALUATION_PIPELINE_LOCK,
            owner_id="owner-one",
            ttl_sec=60,
        ):
            first.execute(
                """
                UPDATE runtime_execution_locks
                SET expires_at = ?
                WHERE lock_name = ? AND owner_id = ?
                """,
                (
                    datetime_to_wire(utc_now() - timedelta(seconds=1)),
                    EVALUATION_PIPELINE_LOCK,
                    "owner-one",
                ),
            )
            first.commit()

            with runtime_execution_lock(
                second,
                EVALUATION_PIPELINE_LOCK,
                owner_id="owner-two",
                ttl_sec=60,
            ):
                row = second.execute(
                    """
                    SELECT owner_id
                    FROM runtime_execution_locks
                    WHERE lock_name = ?
                    """,
                    (EVALUATION_PIPELINE_LOCK,),
                ).fetchone()

        assert row["owner_id"] == "owner-two"
    finally:
        first.close()
        second.close()


def test_dashboard_snapshot_mixed_latest_rows_are_detectable_by_guard_query(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "dashboard-mixed.sqlite3")
    _insert_mixed_dashboard_latest_rows(connection)

    snapshot = build_dashboard_snapshot(connection, Settings(), limit=5)
    mismatches = _dashboard_latest_mismatches(connection)
    connection.close()

    assert snapshot["pipeline_summary"]["strategy"]["latest_observation_count"] == 1
    assert snapshot["pipeline_summary"]["risk"]["latest_observation_count"] == 1
    assert snapshot["pipeline_summary"]["entry_timing"]["latest_plan_count"] == 1
    assert "coherency" not in snapshot["pipeline_summary"]
    assert mismatches == [
        {
            "candidate_instance_id": "candidate-mixed",
            "strategy_latest_id": "strategy-run-a-observation",
            "risk_points_to_strategy_id": "strategy-run-b-observation",
            "order_plan_evidence_strategy_id": "strategy-run-c-observation",
        }
    ]


def test_order_command_lifecycle_detects_dispatched_without_pre_ack(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dispatched-no-preack.sqlite3")
    command = _live_sim_order_command("cmd-dispatched-no-preack")

    enqueue_result = enqueue_command(connection, command)
    polled = poll_commands(connection, limit=1, wait_sec=0)
    stuck = _dispatched_order_commands_without_pre_ack(connection)
    connection.close()

    assert enqueue_result.accepted is True
    assert [item.command_id for item in polled] == [command.command_id]
    assert stuck == [
        {
            "command_id": command.command_id,
            "status": GatewayCommandStatus.DISPATCHED.value,
            "event_count": 0,
        }
    ]


def test_incremental_queue_backlog_and_stale_rows_are_detectable(tmp_path) -> None:
    connection = initialize_database(tmp_path / "incremental-stale.sqlite3")
    old = datetime_to_wire(utc_now() - timedelta(minutes=10))
    now = datetime_to_wire(utc_now())
    _insert_incremental_queue_row(
        connection,
        candidate_id="candidate-stale",
        code="005930",
        enqueued_at=old,
        updated_at=old,
        attempts=0,
    )
    _insert_incremental_queue_row(
        connection,
        candidate_id="candidate-retry-exhausted",
        code="000660",
        enqueued_at=now,
        updated_at=now,
        attempts=3,
    )
    settings = Settings(incremental_evaluation_retry_limit=3)

    status = get_incremental_evaluation_status(connection, settings=settings)
    diagnostics = _incremental_queue_diagnostics(
        connection,
        settings=settings,
        stale_sec=60,
        backlog_limit=1,
    )
    connection.close()

    assert status["queued_count"] == 2
    assert status["retry_exhausted_count"] == 1
    assert diagnostics["reason_codes"] == [
        "INCREMENTAL_QUEUE_BACKLOG",
        "INCREMENTAL_QUEUE_RETRY_EXHAUSTED",
        "INCREMENTAL_QUEUE_STALE",
    ]
    assert diagnostics["stale_queue_count"] == 1


def _insert_mixed_dashboard_latest_rows(connection) -> None:
    now = datetime_to_wire(utc_now())
    expires_at = datetime_to_wire(utc_now() + timedelta(minutes=5))
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
        VALUES (
            'candidate-mixed',
            'strategy-run-a-observation',
            '2026-07-07',
            '005930',
            '삼성전자',
            ?,
            'MATCHED_OBSERVATION',
            'THEME_LEADER_PULLBACK',
            'MATCHED',
            80,
            0.8,
            '[]',
            'test',
            1
        )
        """,
        (now,),
    )
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
        VALUES (
            'candidate-mixed',
            'risk-run-b-observation',
            'strategy-run-b-observation',
            '2026-07-07',
            '005930',
            '삼성전자',
            ?,
            'OBSERVE_PASS',
            'INFO',
            0,
            0,
            3,
            '[]',
            'test',
            1
        )
        """,
        (now,),
    )
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
            limit_price_offset_ticks,
            suggested_quantity,
            suggested_notional,
            max_notional,
            risk_budget_source,
            expires_at,
            reason_codes_json,
            evidence_json,
            observe_only,
            not_order_intent,
            created_at
        )
        VALUES (
            'idem-order-plan-mixed',
            'order-plan-run-c',
            '2026-07-07',
            'candidate-mixed',
            '005930',
            '삼성전자',
            'BUY',
            'PLAN_READY',
            'THEME_LEADER_PULLBACK',
            'GOOD_PULLBACK',
            'VWAP_RECLAIM',
            70000,
            70100,
            'LIMIT_PLUS_TICKS',
            1,
            1,
            70100,
            100000,
            'TEST',
            ?,
            '[]',
            ?,
            1,
            1,
            ?
        )
        """,
        (
            expires_at,
            json.dumps(
                {
                    "source_run_id": "entry-run-c",
                    "source_watermark": {"market_data": 300},
                    "strategy_observation_id": "strategy-run-c-observation",
                    "risk_observation_id": "risk-run-c-observation",
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            now,
        ),
    )
    connection.commit()


def _dashboard_latest_mismatches(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            s.candidate_instance_id,
            s.strategy_observation_id AS strategy_latest_id,
            r.strategy_observation_id AS risk_strategy_id,
            o.evidence_json AS order_plan_evidence_json
        FROM strategy_observations_latest AS s
        JOIN risk_observations_latest AS r
            ON r.candidate_instance_id = s.candidate_instance_id
        LEFT JOIN order_plan_drafts_latest AS o
            ON o.candidate_instance_id = s.candidate_instance_id
        """
    ).fetchall()
    mismatches: list[dict[str, Any]] = []
    for row in rows:
        evidence = _json_object(row["order_plan_evidence_json"])
        order_plan_strategy_id = str(evidence.get("strategy_observation_id") or "")
        if (
            row["risk_strategy_id"] != row["strategy_latest_id"]
            or order_plan_strategy_id
            and order_plan_strategy_id != row["strategy_latest_id"]
        ):
            mismatches.append(
                {
                    "candidate_instance_id": row["candidate_instance_id"],
                    "strategy_latest_id": row["strategy_latest_id"],
                    "risk_points_to_strategy_id": row["risk_strategy_id"],
                    "order_plan_evidence_strategy_id": order_plan_strategy_id,
                }
            )
    return mismatches


def _live_sim_order_command(command_id: str) -> GatewayCommand:
    idempotency_key = f"idem-{command_id}"
    return GatewayCommand(
        command_id=command_id,
        command_type="send_order",
        source="live_sim",
        idempotency_key=idempotency_key,
        payload={
            "account_id": "SIM-12345678",
            "account_mode": "SIMULATION",
            "broker_env": "SIMULATION",
            "server_mode": "SIMULATION",
            "code": "005930",
            "side": "BUY",
            "quantity": 1,
            "price": 70000,
            "mode": "LIVE_SIM",
            "live_mode": "LIVE_SIM",
            "live_sim_intent_id": f"intent-{command_id}",
            "idempotency_key": idempotency_key,
            "metadata": {
                "source": "live_sim",
                "live_sim_only": True,
                "live_real_allowed": False,
                "live_sim_intent_id": f"intent-{command_id}",
                "idempotency_key": idempotency_key,
            },
        },
    )


def _dispatched_order_commands_without_pre_ack(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            c.command_id,
            c.status,
            (
                SELECT COUNT(*)
                FROM gateway_events AS e
                WHERE e.command_id = c.command_id
            ) AS event_count
        FROM gateway_commands AS c
        WHERE c.status = ?
            AND c.command_type IN ('send_order', 'cancel_order')
            AND NOT EXISTS (
                SELECT 1
                FROM gateway_events AS e
                WHERE e.command_id = c.command_id
                    AND e.event_type = 'order_pre_ack'
            )
        ORDER BY c.command_id
        """,
        (GatewayCommandStatus.DISPATCHED.value,),
    ).fetchall()
    return [
        {
            "command_id": row["command_id"],
            "status": row["status"],
            "event_count": int(row["event_count"] or 0),
        }
        for row in rows
    ]


def _insert_incremental_queue_row(
    connection,
    *,
    candidate_id: str,
    code: str,
    enqueued_at: str,
    updated_at: str,
    attempts: int,
) -> None:
    connection.execute(
        """
        INSERT INTO incremental_evaluation_queue (
            candidate_instance_id,
            trade_date,
            code,
            reason,
            source_event_id,
            priority,
            enqueued_at,
            updated_at,
            attempts,
            last_error
        )
        VALUES (?, '2026-07-07', ?, 'PRICE_TICK', ?, 100, ?, ?, ?, NULL)
        """,
        (candidate_id, code, f"evt-{code}", enqueued_at, updated_at, attempts),
    )
    connection.commit()


def _incremental_queue_diagnostics(
    connection,
    *,
    settings: Settings,
    stale_sec: int,
    backlog_limit: int,
) -> dict[str, Any]:
    status = get_incremental_evaluation_status(connection, settings=settings)
    reason_codes: list[str] = []
    if int(status["queued_count"]) > backlog_limit:
        reason_codes.append("INCREMENTAL_QUEUE_BACKLOG")
    if int(status["retry_exhausted_count"]) > 0:
        reason_codes.append("INCREMENTAL_QUEUE_RETRY_EXHAUSTED")
    stale_count = _stale_incremental_queue_count(connection, stale_sec=stale_sec)
    if stale_count:
        reason_codes.append("INCREMENTAL_QUEUE_STALE")
    return {
        "reason_codes": reason_codes,
        "stale_queue_count": stale_count,
        "status": status,
    }


def _stale_incremental_queue_count(connection, *, stale_sec: int) -> int:
    cutoff = utc_now() - timedelta(seconds=stale_sec)
    rows = connection.execute(
        "SELECT enqueued_at FROM incremental_evaluation_queue"
    ).fetchall()
    count = 0
    for row in rows:
        try:
            enqueued_at = parse_timestamp(row["enqueued_at"], "enqueued_at")
        except ValueError:
            count += 1
            continue
        if enqueued_at < cutoff:
            count += 1
    return count


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
