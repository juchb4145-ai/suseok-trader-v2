from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import pytest
from domain.broker.commands import GatewayCommand
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.orders import BrokerExecutionEvent
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
    EvaluationRunLockError,
    runtime_execution_lock,
)
from services.runtime.incremental_evaluation import get_incremental_evaluation_status
from storage.event_store import append_gateway_event
from storage.gateway_command_store import (
    GatewayCommandStatus,
    enqueue_command,
    poll_commands,
)
from storage.projection_outbox import (
    enqueue_projection_jobs_for_gateway_event,
    get_projection_outbox_status,
    list_projection_outbox_jobs,
)
from storage.sqlite import initialize_database, open_connection


def test_synthetic_tr_response_multiple_ticks_uses_unique_child_event_ids(
    tmp_path,
) -> None:
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
        command_id="cmd_candidate_quote_refresh_multi",
        payload=response.to_dict(),
        ts=event_ts,
    )

    append_gateway_event(connection, event)
    result = process_gateway_event(connection, event, settings=Settings())

    sample_rows = connection.execute(
        """
        SELECT event_id, code, metadata_json
        FROM market_tick_samples
        ORDER BY code
        """
    ).fetchall()
    tr_snapshot_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots"
    ).fetchone()["count"]
    projection_error_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_projection_errors
        """
    ).fetchone()
    watermark = get_market_data_projection_watermark(connection)
    connection.close()

    assert result.status == "APPLIED"
    assert len(sample_rows) == 2
    assert tr_snapshot_count == 2
    assert projection_error_count["count"] == 0
    assert {row["event_id"] for row in sample_rows} == {
        "evt_candidate_quote_refresh_multi:synthetic_price_tick:0:005930:KRX",
        "evt_candidate_quote_refresh_multi:synthetic_price_tick:1:000660:KRX",
    }
    assert len({row["event_id"] for row in sample_rows}) == len(sample_rows)
    metadata_by_code = {
        row["code"]: json.loads(row["metadata_json"]) for row in sample_rows
    }
    assert metadata_by_code["005930"]["parent_event_id"] == event.event_id
    assert metadata_by_code["005930"]["parent_command_id"] == event.command_id
    assert metadata_by_code["005930"]["parent_tr_code"] == response.tr_code
    assert metadata_by_code["005930"]["parent_request_name"] == response.request_name
    assert metadata_by_code["005930"]["synthetic_event"] is True
    assert metadata_by_code["005930"]["row_index"] == 0
    assert metadata_by_code["000660"]["parent_event_id"] == event.event_id
    assert metadata_by_code["000660"]["parent_command_id"] == event.command_id
    assert metadata_by_code["000660"]["parent_tr_code"] == response.tr_code
    assert metadata_by_code["000660"]["parent_request_name"] == response.request_name
    assert metadata_by_code["000660"]["synthetic_event"] is True
    assert metadata_by_code["000660"]["row_index"] == 1
    assert watermark.last_event_id == event.event_id


def test_projection_outbox_enqueues_shadow_jobs_for_projection_events(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox.sqlite3")
    events = [
        (
            _price_tick_event("evt_outbox_price_tick"),
            {"market_data"},
        ),
        (
            _tr_response_event("evt_outbox_tr_response"),
            {"market_data"},
        ),
        (
            _condition_event("evt_outbox_condition"),
            {"market_data", "condition_fusion"},
        ),
        (
            _market_symbols_event("evt_outbox_market_symbols"),
            {"market_reference"},
        ),
        (
            _market_index_tick_event("evt_outbox_market_index"),
            {"market_index", "market_regime"},
        ),
    ]

    for event, expected_projections in events:
        append_result = append_gateway_event(connection, event)
        enqueue_result = enqueue_projection_jobs_for_gateway_event(connection, event)
        jobs = list_projection_outbox_jobs(connection, limit=100)
        event_jobs = [job for job in jobs if job["event_id"] == event.event_id]

        assert append_result.status == "ACCEPTED"
        assert append_result.duplicate is False
        assert enqueue_result.status == "ENQUEUED"
        assert enqueue_result.created_count == len(expected_projections)
        assert {job["projection_name"] for job in event_jobs} == expected_projections
        assert {job["status"] for job in event_jobs} == {"PENDING"}

    status = get_projection_outbox_status(connection)
    connection.close()

    assert status["enabled"] is True
    assert status["shadow_mode"] is True
    assert status["worker_enabled"] is False
    assert status["total_count"] == 7
    assert status["pending_count"] == 7
    assert status["processing_count"] == 0
    assert status["applied_count"] == 0
    assert status["skipped_count"] == 0
    assert status["error_count"] == 0
    assert status["dead_letter_count"] == 0
    assert status["oldest_pending_at"] is not None
    assert status["latest_error"] is None
    assert status["by_projection_name"]["market_data"]["pending_count"] == 3
    assert status["by_projection_name"]["condition_fusion"]["pending_count"] == 1
    assert status["by_projection_name"]["market_reference"]["pending_count"] == 1
    assert status["by_projection_name"]["market_index"]["pending_count"] == 1
    assert status["by_projection_name"]["market_regime"]["pending_count"] == 1


def test_projection_outbox_duplicate_event_id_does_not_create_duplicate_job(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-duplicate.sqlite3")
    event = _price_tick_event("evt_outbox_duplicate_price_tick")

    first_append = append_gateway_event(connection, event)
    first_enqueue = enqueue_projection_jobs_for_gateway_event(connection, event)
    duplicate_append = append_gateway_event(connection, event)
    duplicate_enqueue = enqueue_projection_jobs_for_gateway_event(connection, event)
    jobs = list_projection_outbox_jobs(connection, limit=100)
    connection.close()

    assert first_append.status == "ACCEPTED"
    assert first_enqueue.status == "ENQUEUED"
    assert first_enqueue.created_count == 1
    assert duplicate_append.duplicate is True
    assert duplicate_enqueue.status == "DUPLICATE"
    assert duplicate_enqueue.created_count == 0
    assert duplicate_enqueue.duplicate_count == 1
    assert [job["outbox_id"] for job in jobs] == [
        "market_data:evt_outbox_duplicate_price_tick"
    ]


def test_projection_outbox_enqueues_market_scan_for_scan_related_tr_response(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-scan.sqlite3")
    event = _market_scan_tr_response_event("evt_outbox_market_scan_tr")

    append_result = append_gateway_event(connection, event)
    enqueue_result = enqueue_projection_jobs_for_gateway_event(connection, event)
    jobs = list_projection_outbox_jobs(connection, limit=100)
    connection.close()

    assert append_result.status == "ACCEPTED"
    assert enqueue_result.status == "ENQUEUED"
    assert enqueue_result.created_count == 2
    assert {job["projection_name"] for job in jobs} == {"market_data", "market_scan"}


def test_projection_outbox_excludes_non_projection_gateway_events(tmp_path) -> None:
    connection = initialize_database(tmp_path / "projection-outbox-excluded.sqlite3")
    excluded_events = [
        GatewayEvent(
            event_id="evt_outbox_heartbeat",
            event_type="heartbeat",
            source="test-gateway",
            payload={"status": "ok"},
            ts=utc_now(),
        ),
        GatewayEvent(
            event_id="evt_outbox_gateway_log",
            event_type="gateway_log",
            source="test-gateway",
            payload={"level": "INFO", "message": "ok"},
            ts=utc_now(),
        ),
        GatewayEvent(
            event_id="evt_outbox_command_ack",
            event_type="command_ack",
            source="test-gateway",
            payload={"command_id": "cmd-noop", "status": "ACKED"},
            ts=utc_now(),
        ),
        GatewayEvent(
            event_id="evt_outbox_order_pre_ack",
            event_type="order_pre_ack",
            source="test-gateway",
            payload={"command_id": "cmd-noop", "status": "PRE_ACK"},
            ts=utc_now(),
        ),
        _execution_event("evt_outbox_execution"),
    ]

    for event in excluded_events:
        append_result = append_gateway_event(connection, event)
        enqueue_result = enqueue_projection_jobs_for_gateway_event(connection, event)

        assert append_result.status == "ACCEPTED"
        assert enqueue_result.status == "NOOP"
        assert enqueue_result.job_count == 0

    status = get_projection_outbox_status(connection)
    jobs = list_projection_outbox_jobs(connection, limit=100)
    connection.close()

    assert status["total_count"] == 0
    assert status["pending_count"] == 0
    assert jobs == []


def test_runtime_execution_lock_blocks_reacquire_after_ttl_while_owner_still_running(
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

            with pytest.raises(EvaluationRunLockError) as exc_info:
                with runtime_execution_lock(
                    second,
                    EVALUATION_PIPELINE_LOCK,
                    owner_id="owner-two",
                    ttl_sec=60,
                ):
                    raise AssertionError("live owner lock must not be replaced")

            row = second.execute(
                """
                SELECT owner_id
                FROM runtime_execution_locks
                WHERE lock_name = ?
                """,
                (EVALUATION_PIPELINE_LOCK,),
            ).fetchone()

        assert exc_info.value.reason == "OWNER_ALIVE_AFTER_TTL"
        assert row["owner_id"] == "owner-one"
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
    assert snapshot["pipeline_summary"]["coherency"]["status"] == "FAIL"
    assert snapshot["pipeline_coherency"]["status"] == "FAIL"
    assert snapshot["pipeline_coherency"]["mismatch_count"] == 1
    assert mismatches == [
        {
            "candidate_instance_id": "candidate-mixed",
            "strategy_latest_id": "strategy-run-a-observation",
            "risk_points_to_strategy_id": "strategy-run-b-observation",
            "order_plan_evidence_strategy_id": "strategy-run-c-observation",
        }
    ]


def test_order_command_lifecycle_detects_claimed_without_pre_ack(tmp_path) -> None:
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
            "status": GatewayCommandStatus.CLAIMED.value,
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


def _price_tick_event(event_id: str) -> GatewayEvent:
    now = utc_now()
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=70_000,
        change_rate=0.1,
        volume=1_000,
        trade_value=70_000_000,
        execution_strength=101.0,
        best_bid=69_900,
        best_ask=70_000,
        spread_ticks=1,
        day_high=70_500,
        day_low=69_500,
        trade_time=now,
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=now,
    )


def _tr_response_event(event_id: str) -> GatewayEvent:
    now = utc_now()
    response = BrokerTrResponse(
        request_id=f"candidate_quote_refresh:2026-07-07:{event_id}",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        success=True,
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "현재가": "+70000",
                "등락율": "+0.10",
                "거래량": "1000",
                "거래대금": "70000000",
                "고가": "+70500",
                "저가": "-69500",
            }
        ],
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="tr_response",
        source="test-gateway",
        payload=response.to_dict(),
        ts=now,
    )


def _market_scan_tr_response_event(event_id: str) -> GatewayEvent:
    now = utc_now()
    response = BrokerTrResponse(
        request_id="market_scan:TRADE_VALUE:KOSPI:outbox-guard",
        tr_code="OPT10032",
        request_name="market_scan_trade_value_kospi",
        success=True,
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "순위": "1",
                "현재가": "+70000",
                "등락률": "+2.5",
                "거래대금": "1200000000",
                "거래량": "100000",
            }
        ],
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="tr_response",
        source="test-gateway",
        payload=response.to_dict(),
        ts=now,
    )


def _condition_event(event_id: str) -> GatewayEvent:
    now = utc_now()
    condition = BrokerConditionEvent(
        condition_id="cond-outbox",
        condition_name="Outbox Guard",
        code="005930",
        name="삼성전자",
        action="ENTER",
        price=70_000,
        metadata={"test": "projection_outbox"},
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="condition_event",
        source="test-gateway",
        payload=condition.to_dict(),
        ts=now,
    )


def _market_symbols_event(event_id: str) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type="market_symbols",
        source="test-gateway",
        payload={
            "markets": {
                "KOSPI": [{"code": "005930", "name": "삼성전자"}],
                "KOSDAQ": [{"code": "035420", "name": "NAVER"}],
            }
        },
        ts=utc_now(),
    )


def _market_index_tick_event(event_id: str) -> GatewayEvent:
    now = utc_now()
    tick = BrokerMarketIndexTick(
        index_code="KOSPI",
        index_name="KOSPI",
        price=2_800.0,
        change_rate=0.1,
        change_value=2.8,
        trade_time=now,
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="market_index_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=now,
    )


def _execution_event(event_id: str) -> GatewayEvent:
    now = utc_now()
    execution = BrokerExecutionEvent(
        execution_id="exec-outbox-guard",
        broker_order_id="broker-order-outbox-guard",
        code="005930",
        side="BUY",
        quantity=1,
        price=70_000,
        executed_at=now,
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="execution_event",
        source="test-gateway",
        payload=execution.to_dict(),
        ts=now,
    )


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
        WHERE c.status IN (?, ?, ?)
            AND c.command_type IN ('send_order', 'cancel_order')
            AND NOT EXISTS (
                SELECT 1
                FROM gateway_events AS e
                WHERE e.command_id = c.command_id
                    AND e.event_type = 'order_pre_ack'
            )
        ORDER BY c.command_id
        """,
        (
            GatewayCommandStatus.DISPATCHED.value,
            GatewayCommandStatus.CLAIMED.value,
            GatewayCommandStatus.GATEWAY_STARTED.value,
        ),
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
