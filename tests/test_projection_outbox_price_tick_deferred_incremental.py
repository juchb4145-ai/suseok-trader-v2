from __future__ import annotations

import json
from datetime import UTC, datetime

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now
from gateway.event_factory import make_price_tick_event
from services.config import Settings
from services.market_data_service import process_gateway_event
from services.runtime.gateway_projection_routing import decide_market_data_projection_routing
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture


def test_worker_apply_price_tick_enqueues_deferred_incremental_evaluation(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "price-cutover-worker.sqlite3")
    settings = _cutover_apply_settings()
    candidate_id = _insert_strategy_fixture(connection)
    _insert_reconcile_run_on_connection(connection, status="PASS", append_only_ready=True)
    event = _price_tick_event("evt_price_cutover_worker_apply")
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

    sample_count = _table_event_count(
        connection,
        "market_tick_samples",
        event.event_id,
    )
    latest = connection.execute(
        """
        SELECT event_id, price
        FROM market_ticks_latest
        WHERE code = '005930' AND exchange = 'KRX'
        """
    ).fetchone()
    queue = connection.execute("SELECT * FROM incremental_evaluation_queue").fetchone()
    outbox = _outbox_row(connection, f"market_data:{event.event_id}")
    outbox_metadata = json.loads(outbox["metadata_json"])
    routing = connection.execute(
        """
        SELECT post_apply_deferred_side_effects_json
        FROM market_data_projection_routing_decisions
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    routing_side_effects = json.loads(routing["post_apply_deferred_side_effects_json"])
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.status == "COMPLETED"
    assert result.applied_by_worker_count == 1
    assert result.mutated_projection_names == ("market_data",)
    assert sample_count == 1
    assert latest["event_id"] == event.event_id
    assert latest["price"] == 71_000
    assert queue["candidate_instance_id"] == candidate_id
    assert queue["reason"] == "PRICE_TICK"
    assert queue["source_event_id"] == event.event_id
    assert outbox["status"] == "APPLIED"
    side_effects = outbox_metadata["last_worker_evidence"]["post_apply_side_effects"]
    assert side_effects["incremental_evaluation_enqueue_status"] == "ENQUEUED"
    assert side_effects["deferred_from_gateway_path"] is True
    assert side_effects["no_order_side_effects"] is True
    assert routing_side_effects["incremental_evaluation_enqueue_status"] == "ENQUEUED"
    assert routing_side_effects["deferred_from_gateway_path"] is True


def test_worker_applies_effective_skip_price_tick_even_after_newer_inline_tick(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "price-cutover-worker-newer.sqlite3")
    settings = _cutover_apply_settings()
    _insert_strategy_fixture(connection)
    _insert_reconcile_run_on_connection(connection, status="PASS", append_only_ready=True)
    skipped_event = _price_tick_event("evt_price_cutover_worker_older")
    append_gateway_event(connection, skipped_event)
    outbox_result = enqueue_projection_jobs_for_gateway_event(connection, skipped_event)
    decision = decide_market_data_projection_routing(
        connection,
        skipped_event,
        settings=settings,
        outbox_status=outbox_result.status,
    )
    newer_event = _price_tick_event("evt_price_cutover_worker_newer")
    append_gateway_event(connection, newer_event)
    process_gateway_event(connection, newer_event, settings=settings)

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
    )

    skipped_sample_count = _table_event_count(
        connection,
        "market_tick_samples",
        skipped_event.event_id,
    )
    latest = connection.execute(
        """
        SELECT event_id
        FROM market_ticks_latest
        WHERE code = '005930' AND exchange = 'KRX'
        """
    ).fetchone()
    queue = connection.execute(
        """
        SELECT source_event_id
        FROM incremental_evaluation_queue
        WHERE source_event_id = ?
        """,
        (skipped_event.event_id,),
    ).fetchone()
    outbox = _outbox_row(connection, f"market_data:{skipped_event.event_id}")
    metadata = json.loads(outbox["metadata_json"])
    connection.close()

    assert decision.effective_skip_inline is True
    assert result.applied_by_worker_count == 1
    assert result.skipped_count == 0
    assert skipped_sample_count == 1
    assert latest["event_id"] == newer_event.event_id
    assert queue["source_event_id"] == skipped_event.event_id
    evidence = metadata["last_worker_evidence"]
    assert evidence["verification_before_apply"]["reason"] == (
        "MARKET_DATA_PRICE_TICK_OLDER_THAN_LATEST"
    )
    assert evidence["apply_result"] == "APPLIED_BY_WORKER"
    assert evidence["post_apply_side_effects"][
        "incremental_evaluation_enqueue_status"
    ] == "ENQUEUED"


def _cutover_apply_settings() -> Settings:
    return Settings(
        gateway_market_data_append_only_dry_run_enabled=True,
        gateway_market_data_append_only_cutover_enabled=True,
        gateway_market_data_append_only_operating_mode="PRICE_TICK_ONLY",
        gateway_market_data_append_only_global_kill_switch=False,
        gateway_market_data_append_only_global_max_skip_per_minute=10,
        gateway_market_data_append_only_price_tick_cutover_enabled=True,
        gateway_market_data_append_only_cutover_event_types=("price_tick",),
        gateway_market_data_append_only_price_tick_max_skip_per_minute=10,
        projection_outbox_shadow_min_age_sec=0,
        projection_outbox_apply_min_age_sec=0,
        projection_outbox_apply_projection_enabled=True,
        projection_outbox_market_data_apply_enabled=True,
    )


def _price_tick_event(event_id: str) -> GatewayEvent:
    event = make_price_tick_event(
        source="test-gateway",
        price=71_000,
        volume=1_100,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        ts=event.ts,
    )


def _insert_reconcile_run_on_connection(
    connection,
    *,
    status: str,
    append_only_ready: bool,
) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_reconcile_runs (
            run_id,
            status,
            checked_event_count,
            checked_price_tick_count,
            checked_condition_event_count,
            checked_tr_response_count,
            outbox_job_count,
            outbox_pending_count,
            outbox_processing_count,
            outbox_applied_count,
            outbox_skipped_count,
            outbox_error_count,
            outbox_dead_letter_count,
            missing_projection_count,
            inline_projection_error_count,
            outbox_error_issue_count,
            duplicate_or_conflict_count,
            synthetic_child_event_issue_count,
            watermark_risk_count,
            append_only_ready,
            reason_codes_json,
            summary_json,
            created_at
        )
        VALUES (?, ?, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, '[]', ?, ?)
        """,
        (
            f"run_{status.lower()}_{datetime.now(UTC).timestamp()}",
            status,
            int(append_only_ready),
            canonical_json(
                {
                    "status": status,
                    "append_only_ready": append_only_ready,
                    "test": "price_tick_deferred_incremental",
                }
            ),
            datetime_to_wire(utc_now()),
        ),
    )
    connection.commit()


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
