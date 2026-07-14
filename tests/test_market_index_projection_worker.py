from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from domain.broker.events import GatewayEvent
from domain.broker.market_index import BrokerMarketIndexTick
from services.config import Settings
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 7, 10, 1, 10, tzinfo=UTC)


def test_market_index_worker_applies_only_index_projection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-worker.sqlite3")
    event = _event("evt_index_worker", "KOSPI", 2800.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = process_projection_outbox_batch(
        connection,
        settings=_settings(),
        limit=1,
        apply_projection=True,
        projection_name="market_index",
    )
    index_job = _outbox(connection, "market_index:evt_index_worker")
    regime_job = _outbox(connection, "market_regime:evt_index_worker")
    sample_count = _count(connection, "market_index_tick_samples")
    regime_count = _count(connection, "market_regime_snapshots")
    metadata = json.loads(index_job["metadata_json"])
    connection.close()

    assert result.applied_by_worker_count == 1
    assert result.mutated_projection_names == ("market_index",)
    assert result.market_index_apply_enabled is True
    assert index_job["status"] == "APPLIED"
    assert regime_job["status"] == "PENDING"
    assert sample_count == 1
    assert regime_count == 0
    evidence = metadata["last_worker_evidence"]
    assert evidence["apply_mode"] == "MARKET_INDEX_APPLY"
    assert evidence["apply_result"] == "APPLIED_BY_WORKER"
    assert evidence["market_regime_inline_path_unchanged_in_pr15"] is True


def test_market_index_worker_does_not_rewind_newer_projection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-worker-order.sqlite3")
    newer = _event(
        "evt_index_newer",
        "KOSPI",
        2810.0,
        ts=TS + timedelta(seconds=10),
    )
    older = _event("evt_index_older", "KOSPI", 2790.0, ts=TS)
    for event in (newer, older):
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)

    first = process_projection_outbox_batch(
        connection,
        settings=_settings(),
        limit=1,
        apply_projection=True,
        projection_name="market_index",
    )
    second = process_projection_outbox_batch(
        connection,
        settings=_settings(),
        limit=1,
        apply_projection=True,
        projection_name="market_index",
    )
    latest = connection.execute(
        "SELECT event_id, price FROM market_index_ticks_latest WHERE index_code = 'KOSPI'"
    ).fetchone()
    older_sample = connection.execute(
        "SELECT event_id FROM market_index_tick_samples WHERE event_id = ?",
        (older.event_id,),
    ).fetchone()
    older_job = _outbox(connection, "market_index:evt_index_older")
    connection.close()

    assert first.applied_by_worker_count == 1
    assert second.applied_by_worker_count == 1
    assert older_job["status"] == "APPLIED"
    assert older_sample is not None
    assert latest["event_id"] == "evt_index_newer"
    assert latest["price"] == 2810.0


def test_market_index_worker_fails_closed_on_implausible_value(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-worker-error.sqlite3")
    event = _event("evt_index_bad", "KOSPI", 800.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = process_projection_outbox_batch(
        connection,
        settings=_settings(projection_outbox_retry_limit=1),
        limit=1,
        apply_projection=True,
        projection_name="market_index",
    )
    job = _outbox(connection, "market_index:evt_index_bad")
    sample_count = _count(connection, "market_index_tick_samples")
    connection.close()

    assert result.dead_letter_count == 1
    assert result.projection_apply_error_count == 1
    assert job["status"] == "DEAD_LETTER"
    assert sample_count == 0
    assert "MARKET_INDEX_APPLY_FAILED" in job["last_error"]


def _settings(**overrides) -> Settings:
    values = {
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": False,
        "projection_outbox_market_reference_apply_enabled": False,
        "projection_outbox_market_index_apply_enabled": True,
        "projection_outbox_market_index_apply_min_age_sec": 0,
        "projection_outbox_shadow_min_age_sec": 0,
        "market_regime_enabled": False,
    }
    values.update(overrides)
    return Settings(**values)


def _event(
    event_id: str,
    index_code: str,
    price: float,
    *,
    ts: datetime = TS,
) -> GatewayEvent:
    tick = BrokerMarketIndexTick(
        index_code=index_code,
        index_name=index_code,
        price=price,
        change_rate=0.2,
        change_value=5.0,
        trade_time=ts,
        ts=ts,
        metadata={
            "parser_status": "VERIFIED",
            "source": "KIWOOM_REALTIME_MARKET_INDEX",
        },
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="market_index_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=ts,
    )


def _outbox(connection, outbox_id: str):
    return connection.execute(
        "SELECT * FROM projection_outbox WHERE outbox_id = ?",
        (outbox_id,),
    ).fetchone()


def _count(connection, table_name: str) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) AS count FROM {table_name}"
    ).fetchone()
    return int(row["count"])
