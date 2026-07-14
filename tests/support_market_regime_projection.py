from __future__ import annotations

from datetime import datetime

from domain.broker.events import GatewayEvent
from gateway.event_factory import make_market_index_tick_event
from services.config import Settings, TradingMode, TradingProfile
from services.market_context_service import rebuild_market_context_snapshots
from services.market_index_service import process_market_index_event
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event


def market_regime_settings(**overrides) -> Settings:
    values = {
        "trading_profile": TradingProfile.OBSERVE,
        "trading_mode": TradingMode.OBSERVE,
        "trading_allow_live_sim": False,
        "trading_allow_live_real": False,
        "market_regime_enabled": True,
        "market_index_stale_sec": 3600,
        "market_context_snapshot_stale_sec": 3600,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_data_apply_enabled": False,
        "projection_outbox_market_reference_apply_enabled": False,
        "projection_outbox_market_index_apply_enabled": False,
        "projection_outbox_market_regime_apply_enabled": True,
        "projection_outbox_market_regime_apply_batch_size": 20,
        "projection_outbox_market_regime_apply_min_age_sec": 0,
        "projection_outbox_shadow_min_age_sec": 0,
    }
    values.update(overrides)
    return Settings(**values)


def seed_index_event(
    connection,
    index_code: str,
    event_id: str,
    *,
    settings: Settings,
    ts: datetime | None = None,
    parser_status: str = "VERIFIED",
    enqueue: bool = True,
    apply_index: bool = True,
) -> GatewayEvent:
    generated = make_market_index_tick_event(
        source="test-gateway",
        index_code=index_code,
        price=2800.0 if index_code == "KOSPI" else 900.0,
        metadata={
            "parser_status": parser_status,
            "projection_source": "REALTIME",
        },
    )
    event = GatewayEvent(
        event_id=event_id,
        event_type=generated.event_type,
        source=generated.source,
        payload=generated.payload,
        ts=ts or generated.ts,
    )
    assert append_gateway_event(connection, event).status == "ACCEPTED"
    if enqueue:
        enqueue_projection_jobs_for_gateway_event(connection, event)
    if apply_index:
        assert (
            process_market_index_event(
                connection,
                event,
                settings=settings,
            ).status
            == "APPLIED"
        )
    return event


def seed_ready_context(
    connection,
    *,
    settings: Settings,
    prefix: str = "evt_regime",
) -> tuple[GatewayEvent, GatewayEvent]:
    kospi = seed_index_event(
        connection,
        "KOSPI",
        f"{prefix}_kospi",
        settings=settings,
    )
    kosdaq = seed_index_event(
        connection,
        "KOSDAQ",
        f"{prefix}_kosdaq",
        settings=settings,
    )
    result = rebuild_market_context_snapshots(
        connection,
        settings=settings,
        source_event_id=kosdaq.event_id,
        source_projection="market_regime",
        generated_by="test",
    )
    assert result["created_count"] == 2
    return kospi, kosdaq


def count_rows(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
