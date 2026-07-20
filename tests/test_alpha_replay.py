from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from domain.broker.events import GatewayEvent
from services.config import Settings
from services.runtime.alpha_replay import (
    PointInTimeViolation,
    VirtualClock,
    run_point_in_time_alpha_replay,
)
from services.runtime.projection_replay import (
    SAFE_REPLAY_EVENT_TYPES,
    export_replay_bundle,
)
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database
from tools.ops_alpha_replay import copy_sqlite_snapshot, source_database_fingerprint


def test_alpha_replay_is_deterministic_and_blocks_first_page_qualification(tmp_path) -> None:
    source_db = _source_db(tmp_path / "source.sqlite3", scan_complete=False)
    bundle = export_replay_bundle(
        source_db_path=source_db,
        bundle_dir=tmp_path / "bundle",
        event_types=SAFE_REPLAY_EVENT_TYPES,
    )

    first = run_point_in_time_alpha_replay(
        bundle_dir=bundle.bundle_dir,
        isolated_db_path=tmp_path / "first.sqlite3",
        operational_db_path=source_db,
        settings=Settings(),
        commit_sha="abc123",
    )
    second = run_point_in_time_alpha_replay(
        bundle_dir=bundle.bundle_dir,
        isolated_db_path=tmp_path / "second.sqlite3",
        operational_db_path=source_db,
        settings=Settings(),
        commit_sha="abc123",
    )

    assert first.status == "WARN"
    assert first.result_sha256 == second.result_sha256
    assert first.deterministic_identity_sha256 == second.deterministic_identity_sha256
    assert first.point_in_time_violation_count == 0
    assert first.scan_coverage == "FIRST_PAGE_ONLY"
    assert first.alpha_qualified is False
    assert first.qualification_reasons == ("MARKET_SCAN_NOT_COMPLETE",)
    assert first.missing_sources == ()
    assert first.order_preserved is True
    assert first.no_trading_side_effects is True
    assert first.operational_db_write_count == 0
    assert first.input_source_coverage["theme_membership_lineage"]["count"] == 1
    assert first.input_source_coverage["config_lineage"]["count"] == 1
    assert first.virtual_clock_targets["order_plan_expiration"] == "VIRTUAL_CLOCK"


def test_complete_inputs_can_be_alpha_qualified(tmp_path) -> None:
    source_db = _source_db(tmp_path / "source.sqlite3", scan_complete=True)
    bundle = export_replay_bundle(
        source_db_path=source_db,
        bundle_dir=tmp_path / "bundle",
        event_types=SAFE_REPLAY_EVENT_TYPES,
    )

    result = run_point_in_time_alpha_replay(
        bundle_dir=bundle.bundle_dir,
        isolated_db_path=tmp_path / "alpha.sqlite3",
        operational_db_path=source_db,
        settings=Settings(),
        commit_sha="abc123",
    )

    assert result.status == "PASS"
    assert result.scan_coverage == "COMPLETE"
    assert result.missing_sources == ()
    assert result.point_in_time_violation_count == 0
    assert result.alpha_qualified is True
    assert result.qualification_reasons == ()


def test_future_timestamp_is_a_point_in_time_failure(tmp_path) -> None:
    source_db = tmp_path / "source.sqlite3"
    connection = initialize_database(source_db)
    try:
        event = GatewayEvent(
            event_id="future-tick",
            event_type="price_tick",
            source="fixture",
            ts=datetime.now(UTC) + timedelta(days=1),
            payload={
                "code": "005930",
                **_price_payload(datetime.now(UTC) + timedelta(days=1)),
            },
        )
        assert append_gateway_event(connection, event).accepted is True
        connection.commit()
    finally:
        connection.close()
    bundle = export_replay_bundle(
        source_db_path=source_db,
        bundle_dir=tmp_path / "bundle",
        event_types=SAFE_REPLAY_EVENT_TYPES,
    )

    result = run_point_in_time_alpha_replay(
        bundle_dir=bundle.bundle_dir,
        isolated_db_path=tmp_path / "alpha.sqlite3",
        operational_db_path=source_db,
        settings=Settings(),
        commit_sha="abc123",
    )

    assert result.status == "FAIL"
    assert result.point_in_time_violation_count == 1
    assert result.point_in_time_violations[0]["reason_code"] == (
        "EVENT_TIMESTAMP_AFTER_AVAILABILITY"
    )
    assert "POINT_IN_TIME_VIOLATION" in result.failures
    assert result.alpha_qualified is False


def test_virtual_clock_owns_freshness_expiry_session_and_hold_decisions() -> None:
    clock = VirtualClock()
    clock.advance_to("2026-07-20T00:10:00Z")

    assert clock.is_fresh("2026-07-20T00:09:55Z", stale_after_sec=10) is True
    assert clock.is_expired("2026-07-20T00:10:00Z") is True
    assert clock.cooldown_elapsed("2026-07-20T00:09:00Z", cooldown_sec=60) is True
    assert clock.inside_entry_window(start="09:05:00", end="14:30:00") is True
    assert clock.minimum_hold_elapsed("2026-07-20T00:09:30Z", minimum_hold_sec=30) is True
    assert clock.maximum_hold_elapsed("2026-07-20T00:00:00Z", maximum_hold_sec=600) is True
    assert clock.eod_due(eod_time="15:15:00") is False

    try:
        clock.age_seconds("2026-07-20T00:10:01Z")
    except PointInTimeViolation:
        pass
    else:
        raise AssertionError("future observations must fail closed")


def test_sidecar_free_export_does_not_create_sqlite_sidecars(tmp_path) -> None:
    source_db = _source_db(tmp_path / "source.sqlite3", scan_complete=True)
    wal_path = Path(f"{source_db}-wal")
    shm_path = Path(f"{source_db}-shm")
    assert wal_path.exists() is False
    assert shm_path.exists() is False

    export_replay_bundle(
        source_db_path=source_db,
        bundle_dir=tmp_path / "bundle",
        event_types=SAFE_REPLAY_EVENT_TYPES,
    )

    assert wal_path.exists() is False
    assert shm_path.exists() is False


def test_sqlite_snapshot_copy_does_not_open_or_change_source(tmp_path) -> None:
    source_db = _source_db(tmp_path / "source.sqlite3", scan_complete=True)
    before = source_database_fingerprint(source_db)

    snapshot = copy_sqlite_snapshot(
        source_db,
        target_path=tmp_path / "isolated" / "snapshot.sqlite3",
    )

    assert snapshot.read_bytes() == source_db.read_bytes()
    assert source_database_fingerprint(source_db) == before


def _source_db(path: Path, *, scan_complete: bool) -> Path:
    connection = initialize_database(path)
    base = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    events = [
        GatewayEvent(
            event_id="tick",
            event_type="price_tick",
            source="fixture",
            ts=base,
            payload={
                "code": "005930",
                **_price_payload(base),
                "metadata": {"exchange": "KRX"},
            },
        ),
        GatewayEvent(
            event_id="condition",
            event_type="condition_event",
            source="fixture",
            ts=base + timedelta(seconds=1),
            payload={
                "condition_id": "fixture-condition",
                "condition_name": "Fixture Condition",
                "code": "005930",
                "name": "Samsung Electronics",
                "action": "ENTER",
                "price": 70000,
                "metadata": {"theme_membership_lineage": "theme-fixture-v1"},
                "ts": base + timedelta(seconds=1),
            },
        ),
        GatewayEvent(
            event_id="symbols",
            event_type="market_symbols",
            source="fixture",
            ts=base + timedelta(seconds=2),
            payload={"markets": [{"market": "KOSPI", "symbols": ["005930"]}]},
        ),
        GatewayEvent(
            event_id="index",
            event_type="market_index_tick",
            source="fixture",
            ts=base + timedelta(seconds=3),
            payload={"index_code": "KOSPI", "price": 3000},
        ),
        GatewayEvent(
            event_id="candidate-tr",
            event_type="tr_response",
            source="fixture",
            ts=base + timedelta(seconds=4),
            payload={
                "request_id": "candidate_quote_refresh:005930:fixture",
                "request_name": "candidate_quote_refresh",
                "tr_code": "OPT10001",
                "success": True,
                "rows": [{"종목코드": "005930", "현재가": "70000"}],
                "metadata": {"source": "candidate_quote_refresh"},
            },
        ),
        GatewayEvent(
            event_id="index-tr",
            event_type="tr_response",
            source="fixture",
            ts=base + timedelta(seconds=5),
            payload={
                "request_id": "market_index_tr_bootstrap:KOSPI:fixture",
                "request_name": "market_index_tr_bootstrap_kospi",
                "tr_code": "OPT20001",
                "success": True,
                "rows": [],
                "metadata": {"source": "market_index_tr_bootstrap"},
            },
        ),
        GatewayEvent(
            event_id="scan-tr",
            event_type="tr_response",
            source="fixture",
            ts=base + timedelta(seconds=6),
            payload={
                "request_id": "market_scan:TRADE_VALUE:KOSPI:fixture",
                "request_name": "market_scan_trade_value_kospi",
                "tr_code": "OPT10032",
                "success": True,
                "continuation_key": "0" if scan_complete else "2",
                "rows": [{"종목코드": "005930", "순위": "1"}],
                "metadata": {
                    "source": "market_scan_service",
                    "pagination_complete": scan_complete,
                    "page_lineage": ["page-1"] if scan_complete else [],
                },
            },
        ),
    ]
    try:
        for event in events:
            assert append_gateway_event(connection, event).accepted is True
        connection.commit()
    finally:
        connection.close()
    return path


def _price_payload(ts: datetime) -> dict[str, object]:
    return {
        "name": "Samsung Electronics",
        "price": 70000,
        "change_rate": 0.1,
        "volume": 1000,
        "trade_value": 70_000_000,
        "execution_strength": 101.0,
        "best_bid": 69900,
        "best_ask": 70000,
        "spread_ticks": 1,
        "day_high": 70500,
        "day_low": 69500,
        "trade_time": ts,
        "ts": ts,
    }
