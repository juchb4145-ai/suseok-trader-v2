from __future__ import annotations

import sqlite3
from datetime import timedelta

import api.routes.operator as operator_routes
from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.utils import parse_timestamp, utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event, make_market_index_tick_event
from services import market_context_service
from services.candidate_service import (
    ingest_condition_sources,
    list_candidates,
    refresh_candidate_context,
)
from services.config import Settings, candidate_timezone
from services.market_context_service import (
    get_latest_market_context,
    get_market_context_for_code,
    get_market_context_status,
    rebuild_market_context_snapshots,
    should_rebuild_market_context_snapshots,
)
from services.market_data_service import process_gateway_event
from services.market_index_service import process_market_index_event
from services.market_reference_service import process_market_symbols_event
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database, open_connection


def test_market_context_builder_is_coherent_idempotent_and_read_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-context.sqlite3")
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(connection, "KOSPI", settings=settings)
    source_event = _seed_index(connection, "KOSDAQ", settings=settings)

    first = rebuild_market_context_snapshots(
        connection,
        settings=settings,
        source_event_id=source_event.event_id,
        source_projection="market_index",
        generated_by="test",
    )
    second = rebuild_market_context_snapshots(
        connection,
        settings=settings,
        source_event_id=source_event.event_id,
        source_projection="market_index",
        generated_by="test",
    )
    before_read_counts = _context_counts(connection)
    kospi = get_market_context_for_code(connection, "005930", settings=settings)
    kosdaq = get_market_context_for_code(connection, "035420", settings=settings)
    status = get_market_context_status(connection, settings=settings)
    after_read_counts = _context_counts(connection)
    connection.close()

    assert first["created_count"] == 2
    assert second["created_count"] == 0
    assert second["status"] == "APPLIED_BY_VERIFY"
    assert first["source_watermark_hash"] == second["source_watermark_hash"]
    assert {item["source_watermark_hash"] for item in first["snapshots"]} == {
        first["source_watermark_hash"]
    }
    assert kospi["market"] == "KOSPI"
    assert kosdaq["market"] == "KOSDAQ"
    assert kospi["parser_confidence_status"] == "VERIFIED"
    assert kospi["trading_data_usable"] is True
    assert kospi["trading_eligible"] is True
    assert status["status"] == "PASS"
    assert status["latest_watermark_coherent"] is True
    assert before_read_counts == after_read_counts == (2, 1)


def test_market_context_rebuild_cadence_allows_readiness_improvement(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "market-context-cadence.sqlite3")
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(
        connection,
        "KOSPI",
        settings=settings,
        parser_status="PILOT_UNVERIFIED",
    )
    rebuild_market_context_snapshots(connection, settings=settings)

    assert should_rebuild_market_context_snapshots(
        connection,
        settings=settings,
    ) is False

    _seed_index(
        connection,
        "KOSDAQ",
        settings=settings,
        parser_status="PILOT_UNVERIFIED",
    )
    assert should_rebuild_market_context_snapshots(
        connection,
        settings=settings,
    ) is True
    rebuild_market_context_snapshots(connection, settings=settings)
    _seed_index(connection, "KOSPI", settings=settings)
    assert should_rebuild_market_context_snapshots(
        connection,
        settings=settings,
    ) is False
    _seed_index(connection, "KOSDAQ", settings=settings)
    assert should_rebuild_market_context_snapshots(
        connection,
        settings=settings,
    ) is True
    rebuild_market_context_snapshots(connection, settings=settings)
    _seed_index(connection, "KOSPI", settings=settings)
    assert should_rebuild_market_context_snapshots(
        connection,
        settings=settings,
    ) is False

    latest = get_latest_market_context(connection, "KOSPI")
    assert latest is not None
    snapshot_at = parse_timestamp(latest["snapshot_at"], "snapshot_at")
    monkeypatch.setattr(
        market_context_service,
        "utc_now",
        lambda: snapshot_at + timedelta(seconds=6),
    )
    assert should_rebuild_market_context_snapshots(
        connection,
        settings=settings,
    ) is True
    connection.close()


def test_market_context_keeps_parser_confidence_separate_from_data_quality(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "market-context-parser.sqlite3")
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(connection, "KOSPI", settings=settings, parser_status="PILOT_UNVERIFIED")
    _seed_index(connection, "KOSDAQ", settings=settings, parser_status="PILOT_UNVERIFIED")

    rebuild_market_context_snapshots(connection, settings=settings)
    context = get_market_context_for_code(connection, "005930", settings=settings)
    status = get_market_context_status(connection, settings=settings)
    connection.close()

    assert context["parser_confidence_status"] == "UNVERIFIED"
    assert context["data_quality_status"] == "FRESH"
    assert context["trading_data_usable"] is True
    assert context["trading_eligible"] is False
    assert status["status"] == "WARN"
    assert status["parser_unverified_markets"] == ["KOSPI", "KOSDAQ"]
    assert status["data_unusable_markets"] == []


def test_market_context_status_detects_missing_source_regime_reference(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-context-regime-reference.sqlite3")
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(connection, "KOSPI", settings=settings)
    _seed_index(connection, "KOSDAQ", settings=settings)
    rebuild_market_context_snapshots(connection, settings=settings)
    connection.execute("DELETE FROM market_regime_snapshots")
    connection.commit()

    status = get_market_context_status(connection, settings=settings)
    connection.close()

    assert status["status"] == "WARN"
    assert status["latest_regime_coherent"] is True
    assert status["regime_reference_missing_count"] == 1


def test_market_context_missing_unknown_and_stale_are_fail_closed(
    tmp_path,
    monkeypatch,
) -> None:
    missing_connection = initialize_database(tmp_path / "market-context-missing.sqlite3")
    missing = get_market_context_for_code(missing_connection, "005930", settings=_settings())
    missing_connection.close()

    connection = initialize_database(tmp_path / "market-context-stale.sqlite3")
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(connection, "KOSPI", settings=settings)
    rebuild_market_context_snapshots(connection, settings=settings)
    incomplete = get_market_context_for_code(connection, "005930", settings=settings)
    _seed_index(connection, "KOSDAQ", settings=settings)
    rebuild_market_context_snapshots(connection, settings=settings)
    latest = get_latest_market_context(connection, "KOSPI")
    assert latest is not None
    snapshot_at = parse_timestamp(latest["snapshot_at"], "snapshot_at")
    monkeypatch.setattr(
        market_context_service,
        "utc_now",
        lambda: snapshot_at + timedelta(seconds=5),
    )
    stale = get_market_context_for_code(
        connection,
        "005930",
        settings=_settings(market_context_snapshot_stale_sec=1),
    )
    unknown = get_market_context_for_code(connection, "999999", settings=settings)
    connection.close()

    assert missing["market_regime"]["regime_status"] == "DATA_WAIT"
    assert missing["reason_codes"] == ["MARKET_MEMBERSHIP_UNKNOWN"]
    assert incomplete["trading_data_usable"] is False
    assert incomplete["trading_eligible"] is False
    assert stale["market_regime"]["regime_status"] == "DATA_WAIT"
    assert stale["trading_data_usable"] is False
    assert "MARKET_CONTEXT_SNAPSHOT_STALE" in stale["reason_codes"]
    assert unknown["trading_eligible"] is False
    assert unknown["reason_codes"] == ["MARKET_MEMBERSHIP_UNKNOWN"]


def test_candidates_reuse_one_common_market_context_without_regime_rebuild(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-context-candidates.sqlite3")
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(connection, "KOSPI", settings=settings)
    _seed_index(connection, "KOSDAQ", settings=settings)
    rebuild_market_context_snapshots(connection, settings=settings)
    for code, name in (("005930", "Samsung"), ("000660", "SK Hynix")):
        event = make_condition_event(
            condition_id=f"condition-{code}",
            code=code,
            name=name,
        )
        assert append_gateway_event(connection, event).status == "ACCEPTED"
        assert process_gateway_event(connection, event, settings=settings).status == "APPLIED"
    trade_date = (
        utc_now()
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )
    ingest_condition_sources(connection, trade_date, settings=settings)
    candidates = list_candidates(connection, trade_date=trade_date)
    before_counts = _context_counts(connection)

    for candidate in candidates:
        refresh_candidate_context(
            connection,
            candidate["candidate_instance_id"],
            settings=settings,
        )

    rows = connection.execute(
        """
        SELECT market_context_snapshot_id
        FROM candidate_context_latest
        ORDER BY candidate_instance_id
        """
    ).fetchall()
    latest_kospi = get_latest_market_context(connection, "KOSPI")
    context_status = get_market_context_status(connection, settings=settings)
    after_counts = _context_counts(connection)
    connection.close()

    assert len(candidates) == 2
    assert latest_kospi is not None
    assert {row["market_context_snapshot_id"] for row in rows} == {
        latest_kospi["snapshot_id"]
    }
    assert context_status["latest_regime_coherent"] is True
    assert context_status["regime_reference_missing_count"] == 0
    assert context_status["candidate_reference_count"] == 2
    assert context_status["candidate_unreferenced_count"] == 0
    assert context_status["candidate_missing_snapshot_count"] == 0
    assert before_counts == after_counts == (2, 1)


def test_market_context_api_and_dashboard_are_read_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "market-context-api.sqlite3"
    connection = initialize_database(db_path)
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(connection, "KOSPI", settings=settings)
    _seed_index(connection, "KOSDAQ", settings=settings)
    rebuild_market_context_snapshots(connection, settings=settings)
    before_commands = _count(connection, "gateway_commands")
    before_regime_snapshots = _count(connection, "market_regime_snapshots")
    connection.close()
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("MARKET_INDEX_STALE_SEC", "3600")
    monkeypatch.setenv("MARKET_CONTEXT_SNAPSHOT_STALE_SEC", "3600")

    with TestClient(app) as client:
        unauthorized_rebuild = client.post("/api/operator/market-context/rebuild")
        rebuild_response = client.post(
            "/api/operator/market-context/rebuild?live_safe=true",
            headers={"X-Local-Token": "test-token"},
        )
        unsafe_rebuild = client.post(
            "/api/operator/market-context/rebuild?live_safe=false",
            headers={"X-Local-Token": "test-token"},
        )
        operator_status = client.get("/api/operator/market-context/status")
        status_response = client.get("/api/market-regime/context/status")
        latest_response = client.get("/api/market-regime/context/latest/KOSPI")
        code_response = client.get("/api/market-regime/context/for-code/005930")
        regime_response = client.get("/api/market-regime/for-code/005930")
        invalid_response = client.get("/api/market-regime/context/latest/NXT")
        dashboard_response = client.get(
            "/api/dashboard/snapshot?fast=true&sections=market_context"
        )

    connection = open_connection(db_path)
    after_commands = _count(connection, "gateway_commands")
    after_regime_snapshots = _count(connection, "market_regime_snapshots")
    connection.close()

    assert unauthorized_rebuild.status_code == 401
    assert rebuild_response.status_code == 200
    assert rebuild_response.json()["status"] == "APPLIED_BY_VERIFY"
    assert rebuild_response.json()["observe_safe"] is True
    assert rebuild_response.json()["no_trading_side_effects"] is True
    assert unsafe_rebuild.status_code == 409
    assert operator_status.status_code == 200
    assert operator_status.json()["status"] == "PASS"
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "PASS"
    assert latest_response.status_code == 200
    assert latest_response.json()["market_context"]["market"] == "KOSPI"
    assert code_response.status_code == 200
    assert code_response.json()["market_context"]["snapshot_id"] is not None
    assert regime_response.status_code == 200
    assert regime_response.json()["market_context_snapshot_id"] is not None
    assert regime_response.json()["read_only"] is True
    assert invalid_response.status_code == 422
    assert dashboard_response.status_code == 200
    assert dashboard_response.json()["market_context"]["status"] == "PASS"
    assert after_commands == before_commands
    assert after_regime_snapshots == before_regime_snapshots


def test_market_context_operator_rebuild_retries_sqlite_lock(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "market-context-lock-retry.sqlite3"
    connection = initialize_database(db_path)
    settings = _settings()
    _seed_memberships(connection)
    _seed_index(connection, "KOSPI", settings=settings)
    _seed_index(connection, "KOSDAQ", settings=settings)
    connection.close()
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("MARKET_INDEX_STALE_SEC", "3600")
    monkeypatch.setenv("MARKET_CONTEXT_SNAPSHOT_STALE_SEC", "3600")
    original_rebuild = operator_routes.rebuild_market_context_snapshots
    call_count = 0

    def locked_once(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise sqlite3.OperationalError("database is locked")
        return original_rebuild(*args, **kwargs)

    monkeypatch.setattr(
        operator_routes,
        "rebuild_market_context_snapshots",
        locked_once,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/operator/market-context/rebuild?live_safe=true",
            headers={"X-Local-Token": "test-token"},
        )

    assert response.status_code == 200
    assert response.json()["status"] == "APPLIED"
    assert response.json()["locked_retry_count"] == 1
    assert call_count == 2


def _settings(**overrides) -> Settings:
    values = {
        "market_index_stale_sec": 3600,
        "market_context_snapshot_stale_sec": 3600,
        "market_data_tick_stale_sec": 3600,
        "market_data_degraded_tick_stale_sec": 3600,
        "candidate_source_stale_sec": 3600,
        "candidate_tick_stale_sec": 3600,
        "candidate_episode_ttl_sec": 3600,
    }
    values.update(overrides)
    return Settings(**values)


def _seed_memberships(connection) -> None:
    event = GatewayEvent(
        event_id="evt_market_context_symbols",
        event_type="market_symbols",
        source="test-gateway",
        payload={
            "KOSPI": [
                {"code": "005930", "name": "Samsung"},
                {"code": "000660", "name": "SK Hynix"},
            ],
            "KOSDAQ": [{"code": "035420", "name": "Naver"}],
        },
    )
    assert append_gateway_event(connection, event).status == "ACCEPTED"
    assert process_market_symbols_event(connection, event).status == "APPLIED"


def _seed_index(
    connection,
    index_code: str,
    *,
    settings: Settings,
    parser_status: str = "VERIFIED",
) -> GatewayEvent:
    event = make_market_index_tick_event(
        source="test-gateway",
        index_code=index_code,
        price=2800.0 if index_code == "KOSPI" else 900.0,
        metadata={
            "parser_status": parser_status,
            "projection_source": "REALTIME",
        },
    )
    assert append_gateway_event(connection, event).status == "ACCEPTED"
    assert process_market_index_event(connection, event, settings=settings).status == "APPLIED"
    return event


def _context_counts(connection) -> tuple[int, int]:
    return (
        _count(connection, "market_context_snapshots"),
        _count(connection, "market_regime_snapshots"),
    )


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
