from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings
from services.market_data_service import process_gateway_event
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_retention import (
    EventRetentionSafetyError,
    get_event_retention_status,
    prune_event_store_events,
)
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def test_event_retention_prunes_only_safe_old_events(tmp_path) -> None:
    connection = initialize_database(tmp_path / "events.sqlite3")
    settings = Settings(
        event_store_retention_enabled=True,
        projection_outbox_shadow_min_age_sec=0,
    )
    processed_price = _price_tick("evt_retention_processed", volume=1000)
    unprocessed_price = _price_tick("evt_retention_unprocessed", volume=1010)
    quote = _event("evt_retention_quote", "quote_tick", {"code": "005930"})
    protected = _event(
        "evt_retention_order_pre_ack",
        "order_pre_ack",
        {"command_id": "cmd-protected"},
        command_id="cmd-protected",
    )
    recent = _event("evt_retention_recent_heartbeat", "heartbeat", {"status": "ok"})

    append_gateway_event(connection, processed_price)
    enqueue_projection_jobs_for_gateway_event(connection, processed_price)
    process_gateway_event(connection, processed_price, settings=settings)
    process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=False,
        projection_name="market_data",
    )
    append_gateway_event(connection, quote)
    append_gateway_event(connection, unprocessed_price)
    enqueue_projection_jobs_for_gateway_event(connection, unprocessed_price)
    append_gateway_event(connection, protected)
    append_gateway_event(connection, recent)
    _age_events(
        connection,
        processed_price.event_id,
        quote.event_id,
        unprocessed_price.event_id,
        protected.event_id,
    )

    status = get_event_retention_status(
        connection,
        settings=settings,
        exact_counts=True,
    )
    dry_run = prune_event_store_events(connection, settings=settings, dry_run=True)
    before_counts = _event_counts(connection)
    with pytest.raises(
        EventRetentionSafetyError,
        match="fail-closed safety gates",
    ):
        prune_event_store_events(connection, settings=settings, dry_run=False)

    process_gateway_event(connection, unprocessed_price, settings=settings)
    process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=False,
        projection_name="market_data",
    )
    ready_status = get_event_retention_status(
        connection,
        settings=settings,
        exact_counts=True,
    )
    executed = prune_event_store_events(connection, settings=settings, dry_run=False)
    after_counts = _event_counts(connection)
    remaining_ids = _remaining_gateway_event_ids(connection)
    retention_run_count = connection.execute(
        "SELECT COUNT(*) AS count FROM event_retention_runs"
    ).fetchone()["count"]
    connection.close()

    assert status["age_eligible_event_count"] == 3
    assert status["candidate_event_count"] == 2
    assert status["projection_blocked_event_count"] == 1
    assert status["apply_ready"] is False
    assert dry_run.candidate_event_count == 2
    assert dry_run.projection_blocked_event_count == 1
    assert dry_run.deleted_gateway_event_count == 0
    assert before_counts == {"gateway_events": 5, "raw_events": 5}
    assert ready_status["candidate_event_count"] == 3
    assert ready_status["projection_blocked_event_count"] == 0
    assert ready_status["apply_ready"] is True
    assert executed.candidate_event_count == 3
    assert executed.selected_event_count == 3
    assert executed.deleted_gateway_event_count == 3
    assert executed.deleted_raw_event_count == 3
    assert after_counts == {"gateway_events": 2, "raw_events": 2}
    assert remaining_ids == {
        protected.event_id,
        recent.event_id,
    }
    assert retention_run_count == 1


def _price_tick(event_id: str, *, volume: int) -> GatewayEvent:
    tick = BrokerPriceTick(
        code="005930",
        name="삼성전자",
        price=70000,
        change_rate=0.1,
        volume=volume,
        trade_value=70000 * volume,
        execution_strength=101.5,
        best_bid=69900,
        best_ask=70000,
        spread_ticks=1,
        day_high=71000,
        day_low=69000,
        trade_time=TS,
        ts=TS,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="price_tick",
        source="test-gateway",
        ts=TS,
        payload=tick.to_dict(),
    )


def _event(
    event_id: str,
    event_type: str,
    payload: dict[str, object],
    *,
    command_id: str | None = None,
) -> GatewayEvent:
    return GatewayEvent(
        event_id=event_id,
        event_type=event_type,
        source="test-gateway",
        ts=TS,
        payload=payload,
        command_id=command_id,
    )


def _age_events(connection, *event_ids: str) -> None:
    old_received_at = datetime_to_wire(utc_now() - timedelta(days=40))
    placeholders = ", ".join("?" for _ in event_ids)
    for table_name in ("gateway_events", "raw_events"):
        connection.execute(
            f"UPDATE {table_name} SET received_at = ? WHERE event_id IN ({placeholders})",
            (old_received_at, *event_ids),
        )
    connection.commit()


def _event_counts(connection) -> dict[str, int]:
    return {
        table_name: int(
            connection.execute(
                f"SELECT COUNT(*) AS count FROM {table_name}"
            ).fetchone()["count"]
        )
        for table_name in ("gateway_events", "raw_events")
    }


def _remaining_gateway_event_ids(connection) -> set[str]:
    rows = connection.execute("SELECT event_id FROM gateway_events").fetchall()
    return {str(row["event_id"]) for row in rows}
