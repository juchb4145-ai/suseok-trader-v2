from __future__ import annotations

import json

from domain.broker.events import GatewayEvent
from gateway.event_factory import make_condition_event, make_tr_response_event
from services.config import Settings
from services.market_data_service import process_gateway_event
from services.runtime.gateway_projection_routing import (
    decide_market_data_projection_routing,
    get_latest_market_data_append_only_routing_status,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture


def test_worker_apply_tr_response_enqueues_deferred_candidate_quote_refresh(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "tr-worker-side-effect.sqlite3")
    settings = _tr_response_apply_settings()
    candidate_id = _insert_strategy_fixture(connection, code="005930", name="삼성전자")
    event = _candidate_quote_refresh_tr_response("evt_tr_worker_apply", ("005930",))
    append_gateway_event(connection, event)
    outbox_result = enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox_result.status,
    )

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )

    queue = connection.execute("SELECT * FROM incremental_evaluation_queue").fetchone()
    snapshot_count = _table_event_count(connection, "market_tr_snapshots", event.event_id)
    outbox = _outbox_row(connection, f"market_data:{event.event_id}")
    metadata = json.loads(outbox["metadata_json"])
    routing = connection.execute(
        """
        SELECT post_apply_deferred_side_effects_json
        FROM market_data_projection_routing_decisions
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    routing_side_effects = json.loads(routing["post_apply_deferred_side_effects_json"])
    status = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert "TR_RESPONSE_CUTOVER_DISABLED" in decision.blocked_reason_codes
    assert result.status == "COMPLETED"
    assert result.applied_by_worker_count == 1
    assert snapshot_count == 1
    assert queue["candidate_instance_id"] == candidate_id
    assert queue["reason"] == "CANDIDATE_QUOTE_REFRESH"
    assert outbox["status"] == "APPLIED"
    side_effects = metadata["last_worker_evidence"]["post_apply_side_effects"]
    assert side_effects["candidate_quote_refresh_enqueue_status"] == "ENQUEUED"
    assert side_effects["candidate_quote_refresh_code_count"] == 1
    assert side_effects["candidate_quote_refresh_enqueued_count"] == 1
    assert side_effects["candidate_quote_refresh_error_count"] == 0
    assert side_effects["deferred_from_gateway_path"] is True
    assert side_effects["source"] == "projection_outbox_worker_tr_response"
    assert side_effects["no_order_side_effects"] is True
    assert routing_side_effects["candidate_quote_refresh_enqueue_status"] == "ENQUEUED"
    assert status["tr_response_effective_skip_count"] == 0
    assert status["tr_response_deferred_side_effect_count"] == 1
    assert status["tr_response_deferred_side_effect_error_count"] == 0
    assert status["tr_response_worker_side_effect_ready"] is True


def test_worker_does_not_duplicate_tr_response_side_effect_after_inline_apply(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "tr-worker-inline-verify.sqlite3")
    settings = _tr_response_apply_settings()
    _insert_strategy_fixture(connection, code="005930", name="삼성전자")
    event = _candidate_quote_refresh_tr_response("evt_tr_worker_inline", ("005930",))
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    process_gateway_event(connection, event, settings=settings)

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )

    queue_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()["count"]
    outbox = _outbox_row(connection, f"market_data:{event.event_id}")
    metadata = json.loads(outbox["metadata_json"])
    connection.close()

    assert result.applied_by_verify_count == 1
    assert result.applied_by_worker_count == 0
    assert queue_count == 0
    assert metadata["last_worker_evidence"]["apply_result"] == "APPLIED_BY_VERIFY"
    assert "post_apply_side_effects" not in metadata["last_worker_evidence"]


def test_tr_response_effective_skip_is_forbidden_even_with_cutover_flags(tmp_path) -> None:
    connection = initialize_database(tmp_path / "tr-effective-skip-forbidden.sqlite3")
    settings = _tr_response_apply_settings(
        gateway_market_data_append_only_cutover_enabled=True,
        gateway_market_data_append_only_tr_response_cutover_enabled=True,
        gateway_market_data_append_only_cutover_event_types=("price_tick", "tr_response"),
    )
    event = _candidate_quote_refresh_tr_response("evt_tr_effective_forbidden", ("005930",))
    append_gateway_event(connection, event)
    outbox_result = enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox_result.status,
    )

    status = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert "TR_RESPONSE_SYNTHETIC_CHILD_GUARD_NOT_READY" in (
        decision.blocked_reason_codes
    )
    assert status["tr_response_effective_skip_count"] == 0
    assert status["invalid_effective_skip_count"] == 0


def test_condition_event_effective_skip_remains_forbidden(tmp_path) -> None:
    connection = initialize_database(tmp_path / "condition-effective-forbidden.sqlite3")
    settings = _tr_response_apply_settings(
        gateway_market_data_append_only_cutover_enabled=True,
        gateway_market_data_append_only_cutover_event_types=("price_tick", "condition_event"),
    )
    event = _condition_event("evt_condition_effective_forbidden")
    append_gateway_event(connection, event)
    outbox_result = enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox_result.status,
    )

    status = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    connection.close()

    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert "CONDITION_EVENT_CUTOVER_DISABLED_IN_PR9" in decision.blocked_reason_codes
    assert status["condition_event_effective_skip_count"] == 0
    assert status["invalid_effective_skip_count"] == 0


def _tr_response_apply_settings(**overrides) -> Settings:
    values = {
        "gateway_market_data_append_only_dry_run_enabled": True,
        "gateway_market_data_append_only_tr_response_dry_run_enabled": True,
        "gateway_market_data_append_only_cutover_enabled": False,
        "gateway_market_data_append_only_require_reconcile_pass": False,
        "gateway_market_data_append_only_require_latest_reconcile_pass": False,
        "projection_outbox_shadow_min_age_sec": 0,
        "projection_outbox_apply_min_age_sec": 0,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": True,
    }
    values.update(overrides)
    return Settings(**values)


def _candidate_quote_refresh_tr_response(
    event_id: str,
    codes: tuple[str, ...],
) -> GatewayEvent:
    rows = [
        {
            "종목코드": f"A{code}",
            "종목명": f"종목{code}",
            "현재가": "+70000",
            "등락율": "+0.10",
            "거래량": "1000",
            "거래대금": "70000000",
            "고가": "+70100",
            "저가": "+69900",
        }
        for code in codes
    ]
    event = make_tr_response_event(
        request_id=f"candidate_quote_refresh:2026-06-27:{event_id}",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        rows=rows,
        source="test-gateway",
    )
    return GatewayEvent(
        event_id=event_id,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        ts=event.ts,
    )


def _condition_event(event_id: str) -> GatewayEvent:
    event = make_condition_event(source="test-gateway")
    return GatewayEvent(
        event_id=event_id,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        ts=event.ts,
    )


def _outbox_row(connection, outbox_id: str):
    row = connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (outbox_id,),
    ).fetchone()
    assert row is not None
    return row


def _table_event_count(connection, table_name: str, event_id: str) -> int:
    return int(
        connection.execute(
            f"SELECT COUNT(*) AS count FROM {table_name} WHERE event_id = ?",
            (event_id,),
        ).fetchone()["count"]
    )
