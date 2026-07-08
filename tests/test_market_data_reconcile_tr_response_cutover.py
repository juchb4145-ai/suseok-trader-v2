from __future__ import annotations

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from services.runtime.gateway_projection_routing import decide_market_data_projection_routing
from services.runtime.market_data_projection_reconcile import (
    run_market_data_projection_reconcile,
)
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_gateway_market_data_tr_response_cutover import (
    _cutover_settings,
    _insert_reconcile_run,
    _tr_response_payload,
)


def test_reconcile_warns_for_effective_skipped_tr_response_pending_within_sla(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "tr-cutover-reconcile-pending.sqlite3")
    settings = _cutover_settings(projection_outbox_processing_ttl_sec=300)
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event = GatewayEvent.from_dict(
        _tr_response_payload("evt_tr_reconcile_pending", codes=("005930",))
    )
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox.status,
    )

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.status == "WARN"
    assert result.append_only_ready is False
    assert result.tr_response_effective_skip_count == 1
    assert result.tr_response_pending_within_sla_count == 1
    assert result.missing_projection_count == 0
    assert "MARKET_DATA_APPEND_ONLY_TR_RESPONSE_PENDING_WITHIN_SLA" in (
        result.reason_codes
    )


def test_reconcile_fails_when_effective_skipped_tr_response_terminal_missing_artifact(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "tr-cutover-reconcile-missing.sqlite3")
    settings = _cutover_settings()
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event = GatewayEvent.from_dict(
        _tr_response_payload("evt_tr_reconcile_missing", codes=("005930",))
    )
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox.status,
    )
    _mark_market_data_outbox(connection, event.event_id, "APPLIED")

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.status == "FAIL"
    assert result.missing_projection_count == 1
    assert (
        "MARKET_DATA_APPEND_ONLY_TR_RESPONSE_ARTIFACT_MISSING_AFTER_WORKER"
        in result.reason_codes
    )


def test_reconcile_fails_if_condition_event_effective_skip_is_seen(tmp_path) -> None:
    connection = initialize_database(tmp_path / "tr-cutover-reconcile-condition.sqlite3")
    settings = _cutover_settings()
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    from gateway.event_factory import make_condition_event

    event = GatewayEvent.from_dict(make_condition_event(source="test-gateway").to_dict())
    event = GatewayEvent(
        event_id="evt_tr_reconcile_condition_skip",
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        ts=event.ts,
    )
    append_gateway_event(connection, event)
    outbox = enqueue_projection_jobs_for_gateway_event(connection, event)
    decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=outbox.status,
    )
    connection.execute(
        """
        UPDATE market_data_projection_routing_decisions
        SET effective_skip_inline = 1
        WHERE event_id = ?
        """,
        (event.event_id,),
    )
    connection.commit()

    result = run_market_data_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "FAIL"
    assert result.condition_event_effective_skip_count == 1
    assert result.invalid_effective_skip_count == 1
    assert "CONDITION_EVENT_EFFECTIVE_SKIP_FORBIDDEN_IN_PR10" in result.reason_codes


def _mark_market_data_outbox(connection, event_id: str, status: str) -> None:
    now = datetime_to_wire(utc_now())
    metadata = {
        "last_worker_evidence": {
            "verification_reason": "TEST_TR_RESPONSE_CUTOVER_RECONCILE",
            "apply_mode": "MARKET_DATA_APPLY",
            "apply_result": "APPLIED_BY_WORKER",
        }
    }
    connection.execute(
        """
        UPDATE projection_outbox
        SET status = ?,
            updated_at = ?,
            processed_at = ?,
            metadata_json = ?,
            last_error = NULL
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (status, now, now, canonical_json(metadata), event_id),
    )
    connection.commit()
