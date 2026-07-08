from __future__ import annotations

import json

from domain.broker.events import GatewayEvent
from services.runtime.gateway_projection_routing import decide_market_data_projection_routing
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_gateway_market_data_tr_response_cutover import (
    _cutover_settings,
    _insert_reconcile_run,
    _outbox_row,
    _table_event_count,
    _tr_response_payload,
)
from tests.test_strategy_service import _insert_strategy_fixture


def test_worker_applies_effective_skipped_tr_response_and_records_cutover_metadata(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "tr-cutover-worker.sqlite3")
    settings = _cutover_settings()
    _insert_strategy_fixture(connection, code="005930", name="삼성전자")
    _insert_reconcile_run(connection, status="PASS", append_only_ready=True)
    event = GatewayEvent.from_dict(
        _tr_response_payload("evt_tr_cutover_worker", codes=("005930",))
    )
    append_gateway_event(connection, event)
    enqueue_result = enqueue_projection_jobs_for_gateway_event(connection, event)
    decision = decide_market_data_projection_routing(
        connection,
        event,
        settings=settings,
        outbox_status=enqueue_result.status,
    )

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )

    outbox = _outbox_row(connection, "market_data:evt_tr_cutover_worker")
    metadata = json.loads(outbox["metadata_json"])
    queue_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM incremental_evaluation_queue
            WHERE source_event_id = 'evt_tr_cutover_worker'
            """
        ).fetchone()["count"]
    )
    snapshot_count = _table_event_count(
        connection,
        "market_tr_snapshots",
        "evt_tr_cutover_worker",
    )
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.applied_by_worker_count == 1
    assert outbox["status"] == "APPLIED"
    assert snapshot_count == 1
    assert queue_count == 1
    evidence = metadata["last_worker_evidence"]
    assert evidence["append_only_cutover"]["gateway_inline_skipped"] is True
    assert evidence["append_only_cutover"]["cutover_event_type"] == "tr_response"
    assert evidence["post_apply_side_effects"]["no_order_side_effects"] is True
