from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.utils import datetime_to_wire
from fastapi.testclient import TestClient
from services.config import Settings
from services.market_index_service import process_market_index_event
from services.runtime.gateway_market_index_routing import (
    decide_market_index_append_only_routing,
    get_latest_market_index_append_only_routing_status,
)
from services.runtime.market_index_projection_reconcile import (
    run_market_index_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection

TS = datetime(2026, 7, 10, 1, 30, tzinfo=UTC)


def test_market_index_pr15_would_skip_but_never_effectively_skips(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-routing.sqlite3")
    settings = _settings()
    _seed_pass_reconcile(connection, settings)
    event = _event("evt_index_route", "KOSPI", 2801.0, ts=TS + timedelta(seconds=10))
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    status = get_latest_market_index_append_only_routing_status(
        connection,
        settings=settings,
    )
    connection.close()

    assert decision.data_usable is True
    assert decision.parser_verified is True
    assert decision.data_source == "REALTIME"
    assert decision.would_skip_inline is True
    assert decision.effective_skip_inline is False
    assert "MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_IN_PR15" in (
        decision.blocked_reason_codes
    )
    assert status["would_skip_inline_count"] == 1
    assert status["effective_skip_inline_count"] == 0
    assert status["failures"] == []


def test_market_index_pr15_cutover_flag_is_ignored(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-routing-cutover.sqlite3")
    settings = _settings(gateway_market_index_append_only_cutover_enabled=True)
    _seed_pass_reconcile(connection, settings)
    event = _event("evt_index_cutover_ignored", "KOSDAQ", 901.0)
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    decision = decide_market_index_append_only_routing(
        connection,
        event,
        settings=settings,
        outbox_status="ENQUEUED",
    )
    connection.close()

    assert decision.cutover_enabled is True
    assert decision.effective_skip_inline is False


def test_gateway_api_preserves_inline_index_and_regime_in_pr15(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-index-routing-api.sqlite3"
    settings = _settings()
    connection = initialize_database(db_path)
    _seed_pass_reconcile(connection, settings)
    before_commands = _count(connection, "gateway_commands")
    connection.close()

    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED", "true")
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED", "true")
    monkeypatch.setenv("PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED", "true")
    monkeypatch.setenv("PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED", "true")
    monkeypatch.setenv("GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_EVENT_AGE_SEC", "999999999")
    monkeypatch.setenv(
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_FUTURE_SKEW_SEC",
        "999999999",
    )
    monkeypatch.setenv(
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_GATEWAY_HEALTH_MAX_AGE_SEC",
        "999999999",
    )
    event = _event(
        "evt_index_gateway_pr15",
        "KOSPI",
        2802.0,
        ts=TS + timedelta(seconds=20),
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    sample = connection.execute(
        "SELECT event_id FROM market_index_tick_samples WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    decision = connection.execute(
        """
        SELECT would_skip_inline, effective_skip_inline
        FROM market_index_projection_routing_decisions
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    after_commands = _count(connection, "gateway_commands")
    connection.close()

    assert response.status_code == 200
    payload = response.json()
    assert payload["projection_statuses"]["market_index"] == "APPLIED"
    assert payload["projection_statuses"]["market_index_effective_skip_inline"] == "FALSE"
    assert payload["market_index_append_only_routing"]["effective_skip_inline"] is False
    assert sample is not None
    assert decision["would_skip_inline"] == 1
    assert decision["effective_skip_inline"] == 0
    assert after_commands == before_commands


def _seed_pass_reconcile(connection, settings: Settings) -> None:
    for index, code in enumerate(("KOSPI", "KOSDAQ")):
        event = _event(
            f"evt_index_seed_{code.lower()}",
            code,
            2800.0 if code == "KOSPI" else 900.0,
            ts=TS + timedelta(seconds=index),
        )
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)
        assert process_market_index_event(connection, event, settings=settings).status == "APPLIED"
        process_projection_outbox_batch(
            connection,
            settings=Settings(projection_outbox_shadow_min_age_sec=0),
            limit=1,
            projection_name="market_index",
        )
    process_projection_outbox_batch(
        connection,
        settings=Settings(projection_outbox_shadow_min_age_sec=0),
        limit=2,
        projection_name="market_regime",
    )
    append_gateway_event(
        connection,
        GatewayEvent(
            event_id="evt_index_gateway_health",
            event_type="heartbeat",
            source="test-gateway",
            ts=TS + timedelta(seconds=2),
            payload={
                "market_index_realtime_enabled": True,
                "market_index_adapter_health": "CALLBACK_ACTIVE",
                "parsed_market_index_tick_count": 2,
                "latest_market_index_tick_at": datetime_to_wire(
                    TS + timedelta(seconds=2)
                ),
            },
        ),
    )
    result = run_market_index_projection_reconcile(
        connection,
        settings=settings,
        limit=10,
        persist=True,
    )
    assert result.status == "PASS"
    assert result.append_only_ready is True


def _settings(**overrides) -> Settings:
    values = {
        "gateway_market_index_append_only_dry_run_enabled": True,
        "gateway_market_index_append_only_cutover_enabled": False,
        "gateway_market_index_append_only_require_reconcile_pass": True,
        "gateway_market_index_append_only_require_data_usable": True,
        "gateway_market_index_append_only_require_parser_verified": True,
        "gateway_market_index_append_only_max_event_age_sec": 999_999_999,
        "gateway_market_index_append_only_max_future_skew_sec": 999_999_999,
        "gateway_market_index_append_only_gateway_health_max_age_sec": 999_999_999,
        "projection_outbox_apply_projection_enabled": True,
        "projection_outbox_market_index_apply_enabled": True,
        "projection_outbox_market_index_apply_min_age_sec": 0,
        "projection_outbox_shadow_min_age_sec": 0,
        "market_index_stale_sec": 999_999_999,
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


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
