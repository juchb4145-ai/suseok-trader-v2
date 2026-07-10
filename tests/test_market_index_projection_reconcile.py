from __future__ import annotations

from datetime import UTC, datetime, timedelta

from domain.broker.events import GatewayEvent
from domain.broker.market_index import BrokerMarketIndexTick
from services.config import Settings
from services.market_index_service import process_market_index_event
from services.runtime.market_index_projection_reconcile import (
    get_latest_market_index_projection_reconcile,
    run_market_index_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 7, 10, 1, 20, tzinfo=UTC)


def test_market_index_reconcile_passes_verified_realtime_pair(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-reconcile.sqlite3")
    settings = _settings()
    for index, code in enumerate(("KOSPI", "KOSDAQ")):
        event = _event(
            f"evt_index_reconcile_{code.lower()}",
            code,
            2800.0 if code == "KOSPI" else 900.0,
            ts=TS + timedelta(seconds=index),
        )
        _append_inline_and_shadow(connection, event, settings)

    result = run_market_index_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=True,
    )
    latest = get_latest_market_index_projection_reconcile(connection)
    connection.close()

    assert result.status == "PASS"
    assert result.checked_event_count == 2
    assert result.sample_count == 2
    assert result.data_usability_ready is True
    assert result.parser_confidence_ready is True
    assert result.append_only_ready is True
    assert result.realtime_source_count == 2
    assert result.tr_bootstrap_source_count == 0
    assert result.unknown_source_count == 0
    assert latest["latest_run"]["run_id"] == result.run_id
    assert latest["latest_run"]["observed_index_codes"] == ["KOSDAQ", "KOSPI"]


def test_market_index_reconcile_separates_usable_data_from_parser_confidence(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-index-reconcile-parser.sqlite3")
    settings = _settings()
    for index, code in enumerate(("KOSPI", "KOSDAQ")):
        event = _event(
            f"evt_index_unverified_{code.lower()}",
            code,
            2800.0 if code == "KOSPI" else 900.0,
            parser_status="PILOT_UNVERIFIED_FID_MAP",
            ts=TS + timedelta(seconds=index),
        )
        _append_inline_and_shadow(connection, event, settings)

    result = run_market_index_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "PASS"
    assert result.data_usability_ready is True
    assert result.data_usable_count == 2
    assert result.parser_confidence_ready is False
    assert result.parser_unverified_count == 2
    assert result.append_only_ready is False
    assert "MARKET_INDEX_PARSER_UNVERIFIED" in result.reason_codes


def test_market_index_reconcile_rejects_tr_bootstrap_until_adapter_exists(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-reconcile-bootstrap.sqlite3")
    settings = _settings()
    for index, code in enumerate(("KOSPI", "KOSDAQ")):
        event = _event(
            f"evt_index_bootstrap_{code.lower()}",
            code,
            2800.0 if code == "KOSPI" else 900.0,
            source="KIWOOM_TR_BOOTSTRAP_MARKET_INDEX",
            ts=TS + timedelta(seconds=index),
        )
        _append_inline_and_shadow(connection, event, settings)

    result = run_market_index_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=False,
    )
    connection.close()

    assert result.status == "FAIL"
    assert result.tr_bootstrap_source_count == 2
    assert result.append_only_ready is False
    assert "MARKET_INDEX_TR_BOOTSTRAP_SOURCE_NOT_IMPLEMENTED" in result.reason_codes


def _append_inline_and_shadow(
    connection,
    event: GatewayEvent,
    settings: Settings,
) -> None:
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)
    projected = process_market_index_event(connection, event, settings=settings)
    assert projected.status == "APPLIED"
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        projection_name="market_index",
    )
    assert worker.applied_count == 1


def _settings() -> Settings:
    return Settings(
        projection_outbox_shadow_min_age_sec=0,
        gateway_market_index_append_only_require_parser_verified=True,
        market_index_stale_sec=999_999_999,
    )


def _event(
    event_id: str,
    index_code: str,
    price: float,
    *,
    parser_status: str = "VERIFIED",
    source: str = "KIWOOM_REALTIME_MARKET_INDEX",
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
        metadata={"parser_status": parser_status, "source": source},
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="market_index_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=ts,
    )
