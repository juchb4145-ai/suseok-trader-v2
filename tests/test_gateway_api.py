from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

import pytest
from apps.core_api import app
from domain.broker.commands import GatewayCommand
from fastapi.testclient import TestClient
from gateway.event_factory import (
    make_market_index_tick_event,
    make_price_tick_event,
    make_tr_response_event,
)
from storage.gateway_command_store import GatewayCommandStatus, enqueue_command
from storage.sqlite import open_connection
from tests.test_gateway_command_store import make_live_sim_order_command

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC).isoformat()


def heartbeat_event(event_id: str = "evt_api_heartbeat") -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "heartbeat",
        "source": "test-gateway",
        "ts": TS,
        "payload": {"status": "ok"},
    }


def test_gateway_event_api_accepts_duplicates_and_lists_recent(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    with TestClient(app) as client:
        first = client.post(
            "/api/gateway/events",
            json=heartbeat_event(),
            headers={"X-Local-Token": "test-token"},
        )
        duplicate = client.post(
            "/api/gateway/events",
            json=heartbeat_event(),
            headers={"X-Local-Token": "test-token"},
        )
        recent = client.get("/api/gateway/events/recent")
        gateway_status = client.get("/api/gateway/status")

    assert first.status_code == 200
    assert first.json()["accepted"] is True
    assert first.json()["duplicate"] is False
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert recent.status_code == 200
    assert recent.json()["events"][0]["event_id"] == "evt_api_heartbeat"
    assert gateway_status.status_code == 200
    assert gateway_status.json()["last_heartbeat_at"] is not None
    assert gateway_status.json()["recent_event_count"] == 1
    assert gateway_status.json()["order_commands_allowed"] is False


def test_gateway_event_batch_api_processes_each_event_once(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api-batch.sqlite3"))
    events = [
        heartbeat_event("evt_api_batch_1"),
        heartbeat_event("evt_api_batch_2"),
    ]

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events/batch",
            json={"events": events},
            headers={"X-Local-Token": "test-token"},
        )
        duplicate = client.post(
            "/api/gateway/events/batch",
            json={"events": events},
            headers={"X-Local-Token": "test-token"},
        )
        recent = client.get("/api/gateway/events/recent?limit=10")

    assert response.status_code == 200
    assert response.json()["processed_count"] == 2
    assert response.json()["accepted_count"] == 2
    assert response.json()["failed_count"] == 0
    assert duplicate.json()["duplicate_count"] == 2
    assert len(recent.json()["events"]) == 2


def test_gateway_event_batch_retires_completed_market_index_shadow_jobs(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api-batch-index.sqlite3"))
    monkeypatch.setenv("MARKET_INDEX_ENABLED", "true")
    monkeypatch.setenv("MARKET_REGIME_ENABLED", "true")
    monkeypatch.setattr(
        "api.routes.gateway.should_rebuild_market_context_snapshots",
        lambda *args, **kwargs: False,
    )
    event = make_market_index_tick_event(
        source="test-gateway",
        metadata={"parser_status": "VERIFIED", "projection_source": "REALTIME"},
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events/batch",
            json={"events": [event.to_dict()]},
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(tmp_path / "api-batch-index.sqlite3")
    try:
        statuses = dict(
            connection.execute(
                """
                SELECT projection_name, status
                FROM projection_outbox
                WHERE event_id = ?
                """,
                (event.event_id,),
            ).fetchall()
        )
    finally:
        connection.close()

    assert response.status_code == 200
    assert response.json()["failed_count"] == 0
    assert statuses["market_index"] == "APPLIED"
    assert statuses["market_regime"] == "APPLIED"


def test_gateway_event_batch_keeps_market_context_at_latest_index_pair(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-batch-index-context.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("MARKET_INDEX_ENABLED", "true")
    monkeypatch.setenv("MARKET_REGIME_ENABLED", "true")
    metadata = {"parser_status": "VERIFIED", "projection_source": "REALTIME"}
    events = [
        make_market_index_tick_event(
            source="test-gateway",
            index_code=index_code,
            price=price,
            metadata=metadata,
        )
        for index_code, price in (
            ("KOSPI", 2800.0),
            ("KOSDAQ", 850.0),
            ("KOSPI", 2801.0),
            ("KOSDAQ", 851.0),
        )
    ]

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events/batch",
            json={"events": [event.to_dict() for event in events]},
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        rows = connection.execute(
            """
            SELECT snapshot.market, snapshot.source_watermark_json
            FROM market_context_latest AS latest
            JOIN market_context_snapshots AS snapshot
              ON snapshot.snapshot_id = latest.snapshot_id
            """
        ).fetchall()
    finally:
        connection.close()

    expected_event_ids = {
        "KOSPI": events[2].event_id,
        "KOSDAQ": events[3].event_id,
    }
    assert response.status_code == 200
    assert response.json()["failed_count"] == 0
    assert len(rows) == 2
    for row in rows:
        watermark = json.loads(row["source_watermark_json"])
        assert {
            index_code: item["event_id"] for index_code, item in watermark.items()
        } == expected_event_ids


def test_gateway_event_batch_api_isolates_permanent_rejection(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api-batch-reject.sqlite3"))

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events/batch",
            json={"events": [heartbeat_event("evt_api_batch_ok"), "invalid"]},
            headers={"X-Local-Token": "test-token"},
        )

    assert response.status_code == 200
    assert response.json()["processed_count"] == 2
    assert response.json()["accepted_count"] == 1
    assert response.json()["failed_count"] == 1
    assert response.json()["results"][1]["status"] == "REJECTED"


def test_gateway_duplicate_retry_recovers_outbox_after_raw_event_commit(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-duplicate-outbox-recovery.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    event = {
        "event_id": "evt_market_symbols_outbox_retry",
        "event_type": "market_symbols",
        "source": "test-gateway",
        "ts": TS,
        "payload": {
            "symbols": [
                {"code": "005930", "market": "KOSPI", "name": "Samsung"}
            ]
        },
    }
    from api.routes import gateway as gateway_route

    real_enqueue = gateway_route.enqueue_projection_jobs_for_gateway_event
    enqueue_call_count = 0

    def fail_first_outbox_enqueue(*args, **kwargs):
        nonlocal enqueue_call_count
        enqueue_call_count += 1
        if enqueue_call_count == 1:
            raise sqlite3.OperationalError("database is locked")
        return real_enqueue(*args, **kwargs)

    monkeypatch.setattr(
        gateway_route,
        "enqueue_projection_jobs_for_gateway_event",
        fail_first_outbox_enqueue,
    )

    with TestClient(app) as client:
        blocked = client.post(
            "/api/gateway/events",
            json=event,
            headers={"X-Local-Token": "test-token"},
        )
        recovered = client.post(
            "/api/gateway/events",
            json=event,
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        raw = connection.execute(
            """
            SELECT duplicate_count
            FROM raw_events
            WHERE event_id = ?
            """,
            (event["event_id"],),
        ).fetchone()
        outbox = connection.execute(
            """
            SELECT projection_name, status
            FROM projection_outbox
            WHERE event_id = ?
            """,
            (event["event_id"],),
        ).fetchall()
        projected_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM market_symbol_memberships
            WHERE event_id = ?
            """,
            (event["event_id"],),
        ).fetchone()[0]
    finally:
        connection.close()

    assert blocked.status_code == 503
    assert blocked.json()["detail"]["commit_outcome"] == (
        "UNKNOWN_RETRY_SAME_EVENT_ID"
    )
    assert recovered.status_code == 200
    assert recovered.json()["duplicate"] is True
    assert "projection_statuses" not in recovered.json()
    assert raw is not None
    assert raw["duplicate_count"] == 1
    assert [(row["projection_name"], row["status"]) for row in outbox] == [
        ("market_reference", "PENDING")
    ]
    assert projected_count == 0


def test_gateway_status_exposes_batch_and_data_plane_slo(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api-batch-slo.sqlite3"))
    event = heartbeat_event("evt_api_batch_slo")
    event["payload"].update(
        {
            "core_io_worker_batch_size": 100,
            "core_io_worker_batch_post_count": 12,
            "core_io_worker_latest_batch_size": 50,
            "core_io_worker_consecutive_post_error_count": 3,
            "core_io_worker_market_event_queue_size": 20,
            "core_io_worker_durable_event_queue_size": 4,
            "core_io_worker_oldest_event_age_sec": 3.0,
            "core_io_worker_oldest_market_event_age_sec": 1.5,
            "core_io_data_plane_health": "HEALTHY",
            "realtime_max_total": 80,
            "realtime_registration_budget_skip_count": 2,
        }
    )

    with TestClient(app) as client:
        client.post(
            "/api/gateway/events",
            json=event,
            headers={"X-Local-Token": "test-token"},
        )
        response = client.get("/api/gateway/status")

    payload = response.json()
    assert payload["core_io_data_plane_health"] == "HEALTHY"
    assert payload["core_io_worker_batch_size"] == 100
    assert payload["core_io_worker_consecutive_post_error_count"] == 3
    assert payload["core_io_worker_oldest_market_event_age_sec"] == 1.5
    assert payload["realtime_max_total"] == 80
    assert payload["realtime_registration_budget_skip_count"] == 2


def test_gateway_event_batch_price_tick_fast_path_preserves_projections(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-batch-price.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    events = [
        make_price_tick_event(code="005930", price=70000).to_dict(),
        make_price_tick_event(code="000660", price=120000).to_dict(),
    ]

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events/batch",
            json={"events": events},
            headers={"X-Local-Token": "test-token"},
        )
        duplicate = client.post(
            "/api/gateway/events/batch",
            json={"events": events},
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        counts = {
            "raw": connection.execute(
                "SELECT COUNT(*) AS count FROM raw_events"
            ).fetchone()["count"],
            "samples": connection.execute(
                "SELECT COUNT(*) AS count FROM market_tick_samples"
            ).fetchone()["count"],
            "outbox": connection.execute(
                "SELECT COUNT(*) AS count FROM projection_outbox"
            ).fetchone()["count"],
            "outbox_pending": connection.execute(
                "SELECT COUNT(*) AS count FROM projection_outbox WHERE status = 'PENDING'"
            ).fetchone()["count"],
            "outbox_applied": connection.execute(
                "SELECT COUNT(*) AS count FROM projection_outbox WHERE status = 'APPLIED'"
            ).fetchone()["count"],
            "routing": connection.execute(
                "SELECT COUNT(*) AS count "
                "FROM market_data_projection_routing_decisions"
            ).fetchone()["count"],
        }
    finally:
        connection.close()

    assert response.status_code == 200
    assert response.json()["accepted_count"] == 2
    assert duplicate.json()["duplicate_count"] == 2
    assert counts == {
        "raw": 2,
        "samples": 2,
        "outbox": 2,
        "outbox_pending": 0,
        "outbox_applied": 2,
        "routing": 2,
    }


def test_gateway_fast_batch_retries_transient_external_sqlite_writer(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-batch-transient-lock.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    events = [
        make_price_tick_event(code="005930", price=70000).to_dict(),
        make_price_tick_event(code="000660", price=120000).to_dict(),
    ]
    from api.routes import gateway as gateway_route

    real_open_connection = gateway_route.open_connection
    real_budget_sleep = gateway_route._sleep_within_gateway_write_budget
    writer_state: dict[str, sqlite3.Connection] = {}
    release_count = 0

    def open_then_lock(path, **kwargs):
        connection = real_open_connection(path, **kwargs)
        writer = sqlite3.connect(path, check_same_thread=False)
        writer.execute("PRAGMA busy_timeout=0")
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "UPDATE app_metadata SET updated_at = updated_at WHERE key = 'app_name'"
        )
        writer_state["writer"] = writer
        return connection

    def release_writer_on_retry(delay_sec, **kwargs):
        nonlocal release_count
        if release_count == 0:
            writer_state["writer"].commit()
        release_count += 1
        real_budget_sleep(0.0, **kwargs)

    monkeypatch.setattr(gateway_route, "open_connection", open_then_lock)
    monkeypatch.setattr(
        gateway_route,
        "_sleep_within_gateway_write_budget",
        release_writer_on_retry,
    )

    with TestClient(app) as client:
        try:
            response = client.post(
                "/api/gateway/events/batch",
                json={"events": events},
                headers={"X-Local-Token": "test-token"},
            )
        finally:
            writer = writer_state["writer"]
            if writer.in_transaction:
                writer.rollback()
            writer.close()
        monkeypatch.setattr(
            gateway_route,
            "open_connection",
            real_open_connection,
        )
        duplicate = client.post(
            "/api/gateway/events/batch",
            json={"events": events},
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        counts = {
            "raw": connection.execute(
                "SELECT COUNT(*) FROM raw_events"
            ).fetchone()[0],
            "gateway": connection.execute(
                "SELECT COUNT(*) FROM gateway_events"
            ).fetchone()[0],
            "samples": connection.execute(
                "SELECT COUNT(*) FROM market_tick_samples"
            ).fetchone()[0],
            "outbox": connection.execute(
                "SELECT COUNT(*) FROM projection_outbox"
            ).fetchone()[0],
            "order_commands": connection.execute(
                """
                SELECT COUNT(*)
                FROM gateway_commands
                WHERE command_type IN ('send_order', 'cancel_order', 'modify_order')
                """
            ).fetchone()[0],
        }
        quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
    finally:
        connection.close()

    assert response.status_code == 200
    assert response.json()["accepted_count"] == 2
    assert response.json()["failed_count"] == 0
    assert response.json()["sqlite_lock_retry_count"] >= 1
    assert release_count == 1
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate_count"] == 2
    assert counts == {
        "raw": 2,
        "gateway": 2,
        "samples": 2,
        "outbox": 2,
        "order_commands": 0,
    }
    assert quick_check == "ok"


def test_gateway_fast_batch_persistent_lock_is_retryable_and_atomic(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-batch-persistent-lock.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    events = [
        make_price_tick_event(code="005930", price=70000).to_dict(),
        make_price_tick_event(code="000660", price=120000).to_dict(),
    ]
    from api.routes import gateway as gateway_route

    real_open_connection = gateway_route.open_connection
    writer_state: dict[str, sqlite3.Connection] = {}

    def open_then_lock(path, **kwargs):
        connection = real_open_connection(path, **kwargs)
        writer = sqlite3.connect(path, check_same_thread=False)
        writer.execute("PRAGMA busy_timeout=0")
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "UPDATE app_metadata SET updated_at = updated_at WHERE key = 'app_name'"
        )
        writer_state["writer"] = writer
        return connection

    monkeypatch.setattr(gateway_route, "open_connection", open_then_lock)

    with TestClient(app) as client:
        try:
            blocked = client.post(
                "/api/gateway/events/batch",
                json={"events": events},
                headers={"X-Local-Token": "test-token"},
            )
            reader = sqlite3.connect(db_path)
            try:
                blocked_count = reader.execute(
                    "SELECT COUNT(*) FROM raw_events"
                ).fetchone()[0]
            finally:
                reader.close()
        finally:
            writer = writer_state["writer"]
            writer.rollback()
            writer.close()
        monkeypatch.setattr(
            gateway_route,
            "open_connection",
            real_open_connection,
        )
        recovered = client.post(
            "/api/gateway/events/batch",
            json={"events": events},
            headers={"X-Local-Token": "test-token"},
        )

    detail = blocked.json()["detail"]
    assert blocked.status_code == 503
    assert detail["status"] == "LOCKED_RETRYABLE"
    assert detail["retryable"] is True
    assert detail["reason_codes"] == ["SQLITE_DATABASE_LOCKED"]
    assert detail["phase"] == "BATCH_TRANSACTION"
    assert detail["attempts"] == 4
    assert detail["locked_retry_count"] == 3
    assert detail["batch_committed"] is False
    assert detail["no_order_side_effects"] is True
    assert blocked_count == 0
    assert recovered.status_code == 200
    assert recovered.json()["accepted_count"] == 2


def test_gateway_fast_batch_connection_lock_is_structured_retryable(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-batch-open-lock.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    event = make_price_tick_event(code="005930", price=70000).to_dict()
    from api.routes import gateway as gateway_route

    def locked_open_connection(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    with TestClient(app) as client:
        monkeypatch.setattr(
            gateway_route,
            "open_connection",
            locked_open_connection,
        )
        blocked = client.post(
            "/api/gateway/events/batch",
            json={"events": [event]},
            headers={"X-Local-Token": "test-token"},
        )

    detail = blocked.json()["detail"]
    assert blocked.status_code == 503
    assert detail["status"] == "LOCKED_RETRYABLE"
    assert detail["phase"] == "OPEN_CONNECTION"
    assert detail["attempts"] == 4
    assert detail["batch_committed"] is False
    assert "error_message" not in detail
    assert str(db_path) not in json.dumps(detail)


def test_gateway_fast_batch_retries_whole_transaction_after_mid_batch_lock(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-batch-mid-transaction-lock.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    events = [
        make_price_tick_event(code="005930", price=70000).to_dict(),
        make_price_tick_event(code="000660", price=120000).to_dict(),
    ]
    from api.routes import gateway as gateway_route

    real_process = gateway_route._process_gateway_event
    call_count = 0

    def fail_once_after_event_write(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        result = real_process(*args, **kwargs)
        if call_count == 1:
            raise sqlite3.OperationalError("database is locked")
        return result

    monkeypatch.setattr(
        gateway_route,
        "_process_gateway_event",
        fail_once_after_event_write,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events/batch",
            json={"events": events},
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        raw_rows = connection.execute(
            "SELECT event_id, duplicate_count FROM raw_events ORDER BY event_id"
        ).fetchall()
        sample_count = connection.execute(
            "SELECT COUNT(*) FROM market_tick_samples"
        ).fetchone()[0]
    finally:
        connection.close()

    assert response.status_code == 200
    assert response.json()["accepted_count"] == 2
    assert response.json()["sqlite_lock_retry_count"] == 1
    assert len(raw_rows) == 2
    assert all(row["duplicate_count"] == 0 for row in raw_rows)
    assert sample_count == 2


@pytest.mark.parametrize(
    ("path", "body", "expected_phase"),
    [
        (
            "/api/gateway/events",
            heartbeat_event("evt_single_locked"),
            "SINGLE_EVENT",
        ),
        (
            "/api/gateway/events/batch",
            {"events": [heartbeat_event("evt_batch_non_fast_locked")]},
            "BATCH_NON_FAST_EVENT",
        ),
    ],
    ids=["single-event", "batch-non-fast-event"],
)
def test_gateway_non_fast_event_lock_is_redacted_retryable(
    tmp_path,
    monkeypatch,
    path,
    body,
    expected_phase,
) -> None:
    db_path = tmp_path / f"{expected_phase.lower()}-lock.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    from api.routes import gateway as gateway_route
    from storage import event_store

    real_open_connection = gateway_route.open_connection
    writer_state: dict[str, sqlite3.Connection] = {}

    def open_then_lock(db_file, **kwargs):
        connection = real_open_connection(db_file, **kwargs)
        writer = sqlite3.connect(db_file, check_same_thread=False)
        writer.execute("PRAGMA busy_timeout=0")
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "UPDATE app_metadata SET updated_at = updated_at WHERE key = 'app_name'"
        )
        writer_state["writer"] = writer
        return connection

    monkeypatch.setattr(gateway_route, "open_connection", open_then_lock)
    monkeypatch.setattr(
        event_store,
        "_DATABASE_LOCK_RETRY_DELAYS_SEC",
        (0.0,),
    )

    with TestClient(app) as client:
        try:
            blocked = client.post(
                path,
                json=body,
                headers={"X-Local-Token": "test-token"},
            )
        finally:
            writer = writer_state["writer"]
            writer.rollback()
            writer.close()

    detail = blocked.json()["detail"]
    assert blocked.status_code == 503
    assert detail["status"] == "LOCKED_RETRYABLE"
    assert detail["phase"] == expected_phase
    assert detail["commit_outcome"] == "UNKNOWN_RETRY_SAME_EVENT_ID"
    if expected_phase == "BATCH_NON_FAST_EVENT":
        assert detail["batch_commit_state"] == "UNKNOWN"
        assert detail["committed_event_count"] == 0
        assert detail["committed_event_ids"] == []
        assert detail["retry_event_id"] == "evt_batch_non_fast_locked"
    assert "error_message" not in detail
    assert str(db_path) not in json.dumps(detail)
    reader = sqlite3.connect(db_path)
    try:
        assert reader.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 0
    finally:
        reader.close()


def test_gateway_mixed_batch_lock_reports_committed_prefix_and_retry_event(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "batch-partial-prefix-lock.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    from api.routes import gateway as gateway_route
    from storage import event_store

    real_open_connection = gateway_route.open_connection
    open_count = 0
    writer_state: dict[str, sqlite3.Connection] = {}

    def lock_second_event(db_file, **kwargs):
        nonlocal open_count
        open_count += 1
        connection = real_open_connection(db_file, **kwargs)
        if open_count == 3:
            writer = sqlite3.connect(db_file, check_same_thread=False)
            writer.execute("PRAGMA busy_timeout=0")
            writer.execute("BEGIN IMMEDIATE")
            writer.execute(
                "UPDATE app_metadata SET updated_at = updated_at WHERE key = 'app_name'"
            )
            writer_state["writer"] = writer
        return connection

    monkeypatch.setattr(gateway_route, "open_connection", lock_second_event)
    monkeypatch.setattr(event_store, "_DATABASE_LOCK_RETRY_DELAYS_SEC", (0.0,))
    first_id = "evt_batch_prefix_committed"
    retry_id = "evt_batch_prefix_retry"
    fast_event = make_price_tick_event(code="005930", price=70000).to_dict()
    fast_id = str(fast_event["event_id"])

    with TestClient(app) as client:
        try:
            blocked = client.post(
                "/api/gateway/events/batch",
                json={
                    "events": [
                        fast_event,
                        heartbeat_event(first_id),
                        heartbeat_event(retry_id),
                    ]
                },
                headers={"X-Local-Token": "test-token"},
            )
        finally:
            writer = writer_state["writer"]
            writer.rollback()
            writer.close()

    detail = blocked.json()["detail"]
    assert blocked.status_code == 503
    assert detail["batch_commit_state"] == "PARTIAL_PREFIX_COMMITTED"
    assert detail["commit_outcome"] == "UNKNOWN_RETRY_SAME_EVENT_ID"
    assert detail["committed_event_count"] == 2
    assert detail["committed_event_ids"] == [fast_id, first_id]
    assert detail["retry_event_id"] == retry_id
    reader = sqlite3.connect(db_path)
    try:
        raw_ids = [
            row[0]
            for row in reader.execute(
                "SELECT event_id FROM raw_events ORDER BY event_id"
            ).fetchall()
        ]
    finally:
        reader.close()
    assert set(raw_ids) == {fast_id, first_id}


def test_gateway_non_fast_batch_shares_one_request_write_deadline(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "batch-request-deadline.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    from api.routes import gateway as gateway_route

    class FakeClock:
        def __init__(self) -> None:
            self.now = 100.0

        def monotonic(self) -> float:
            return self.now

        def sleep(self, delay_sec: float) -> None:
            self.now += delay_sec

    clock = FakeClock()
    observed_deadlines: list[tuple[float, float]] = []

    def consume_entire_budget(event, **kwargs):
        observed_deadlines.append(
            (
                kwargs["request_started_at"],
                kwargs["write_deadline_at"],
            )
        )
        if kwargs["close_connection"]:
            kwargs["connection"].close()
        clock.now = kwargs["write_deadline_at"]
        return {
            "accepted": True,
            "event_id": event.event_id,
            "duplicate": False,
            "status": "ACCEPTED",
        }

    monkeypatch.setattr(gateway_route, "time", clock)
    monkeypatch.setattr(
        gateway_route,
        "_process_gateway_event",
        consume_entire_budget,
    )
    first_id = "evt_batch_deadline_first"
    retry_id = "evt_batch_deadline_retry"

    with TestClient(app) as client:
        blocked = client.post(
            "/api/gateway/events/batch",
            json={
                "events": [
                    heartbeat_event(first_id),
                    heartbeat_event(retry_id),
                ]
            },
            headers={"X-Local-Token": "test-token"},
        )

    detail = blocked.json()["detail"]
    assert blocked.status_code == 503
    assert observed_deadlines == [(100.0, 105.0)]
    assert detail["reason_codes"] == ["GATEWAY_WRITE_BUDGET_EXHAUSTED"]
    assert detail["phase"] == "BATCH_NON_FAST_LOOP"
    assert detail["elapsed_ms"] == 5000.0
    assert detail["batch_commit_state"] == "PARTIAL_PREFIX_COMMITTED"
    assert detail["committed_event_ids"] == [first_id]
    assert detail["retry_event_id"] == retry_id


@pytest.mark.parametrize(
    ("path", "body"),
    [
        (
            "/api/gateway/events",
            heartbeat_event("evt_python_lock_timeout_single"),
        ),
        (
            "/api/gateway/events/batch",
            {"events": [heartbeat_event("evt_python_lock_timeout_batch")]},
        ),
    ],
    ids=["single-event", "batch-non-fast-event"],
)
def test_gateway_python_write_lock_timeout_is_retryable_without_db_write(
    tmp_path,
    monkeypatch,
    path,
    body,
) -> None:
    db_path = tmp_path / "python-write-lock-timeout.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    from api.routes import gateway as gateway_route
    from storage.sqlite_locking import PROCESS_SQLITE_WRITER_COORDINATOR

    assert gateway_route._gateway_event_write_lock is PROCESS_SQLITE_WRITER_COORDINATOR

    with TestClient(app) as client:
        gateway_route._gateway_event_write_lock.acquire()
        try:
            blocked = client.post(
                path,
                json=body,
                headers={"X-Local-Token": "test-token"},
            )
        finally:
            gateway_route._gateway_event_write_lock.release()

    detail = blocked.json()["detail"]
    assert blocked.status_code == 503
    assert detail["status"] == "LOCKED_RETRYABLE"
    assert detail["phase"] == "PYTHON_WRITE_LOCK"
    assert detail["commit_outcome"] == "NOT_STARTED"
    reader = sqlite3.connect(db_path)
    try:
        assert reader.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 0
    finally:
        reader.close()


def test_gateway_commands_api_dispatches_queued_commands(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))

    with TestClient(app) as client:
        connection = open_connection(db_path)
        try:
            enqueue_command(
                connection,
                GatewayCommand(
                    command_id="cmd_api_poll",
                    command_type="request_tr",
                    source="core",
                    payload={"tr_code": "OPT10001", "params": {"code": "005930"}},
                ),
            )
        finally:
            connection.close()

        response = client.get(
            "/api/gateway/commands",
            headers={"X-Local-Token": "test-token"},
        )
        status_response = client.get("/api/gateway/commands/status")

    assert response.status_code == 200
    assert response.json()["commands"][0]["command_id"] == "cmd_api_poll"
    assert response.json()["commands"][0]["command_type"] == "request_tr"
    assert status_response.status_code == 200
    assert status_response.json()["counts"][GatewayCommandStatus.DISPATCHED.value] == 1


def test_gateway_pre_ack_api_confirms_durable_boundary_without_order_side_effect(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api-order-boundary.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    command = make_live_sim_order_command("cmd-api-pre-ack")
    event = {
        "event_id": "evt-api-pre-ack",
        "event_type": "order_pre_ack",
        "source": "test-gateway",
        "ts": TS,
        "command_id": command.command_id,
        "idempotency_key": command.idempotency_key,
        "payload": {
            "status": "PRE_ACK",
            "command_id": command.command_id,
            "command_type": command.command_type,
            "idempotency_key": command.idempotency_key,
            "account_id": "1234567890",
            "code": "005930",
            "side": "BUY",
        },
    }

    with TestClient(app) as client:
        connection = open_connection(db_path)
        try:
            assert enqueue_command(connection, command).accepted is True
        finally:
            connection.close()
        polled = client.get(
            "/api/gateway/commands",
            headers={"X-Local-Token": "test-token"},
        )
        accepted = client.post(
            "/api/gateway/events",
            json=event,
            headers={"X-Local-Token": "test-token"},
        )
        duplicate = client.post(
            "/api/gateway/events",
            json=event,
            headers={"X-Local-Token": "test-token"},
        )
        status_response = client.get(
            "/api/operator/gateway/order-broker-boundaries/status",
            headers={"X-Local-Token": "test-token"},
        )
        unauthorized_status = client.get(
            "/api/operator/gateway/order-broker-boundaries/status"
        )
        unauthorized_list = client.get(
            "/api/operator/gateway/order-broker-boundaries?limit=10"
        )
        unauthorized_preview = client.get(
            f"/api/operator/gateway/order-broker-boundaries/{command.command_id}"
        )
        list_response = client.get(
            "/api/operator/gateway/order-broker-boundaries?limit=10",
            headers={"X-Local-Token": "test-token"},
        )
        effective_list_response = client.get(
            "/api/operator/gateway/order-broker-boundaries"
            "?effective_state=PRE_ACK_RECORDED&limit=10",
            headers={"X-Local-Token": "test-token"},
        )
        preview_response = client.get(
            f"/api/operator/gateway/order-broker-boundaries/{command.command_id}",
            headers={"X-Local-Token": "test-token"},
        )

    assert polled.status_code == 200
    assert accepted.status_code == 200
    assert accepted.json()["accepted"] is True
    assert accepted.json()["broker_boundary_state"] == "PRE_ACK_RECORDED"
    assert accepted.json()["durable_pre_ack_recorded"] is True
    assert duplicate.json()["duplicate"] is True
    assert duplicate.json()["durable_pre_ack_recorded"] is True
    assert unauthorized_status.status_code == 401
    assert unauthorized_list.status_code == 401
    assert unauthorized_preview.status_code == 401
    assert status_response.json()["status"] == "PASS"
    assert status_response.json()["durable_pre_ack_count"] == 1
    assert list_response.json()["count"] == 1
    assert list_response.json()["items"][0]["state"] == "PRE_ACK_RECORDED"
    assert list_response.json()["no_order_side_effects"] is True
    assert effective_list_response.status_code == 200
    assert effective_list_response.json()["count"] == 1
    assert preview_response.status_code == 200
    assert preview_response.json()["raw_state"] == "PRE_ACK_RECORDED"
    public_payload = json.dumps(
        [list_response.json(), preview_response.json()],
        ensure_ascii=False,
    )
    assert "1234567890" not in public_payload
    assert str(command.idempotency_key) not in public_payload


def test_gateway_event_api_requires_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")

    with TestClient(app) as client:
        missing = client.post("/api/gateway/events", json=heartbeat_event("evt_missing_token"))
        wrong = client.post(
            "/api/gateway/events",
            json=heartbeat_event("evt_wrong_token"),
            headers={"X-Local-Token": "wrong"},
        )
        accepted = client.post(
            "/api/gateway/events",
            json=heartbeat_event("evt_with_token"),
            headers={"X-Core-Token": "secret-token"},
        )
        read_only_status = client.get("/api/gateway/status")

    assert missing.status_code == 401
    assert wrong.status_code == 403
    assert accepted.status_code == 200
    assert read_only_status.status_code == 200
    assert read_only_status.json()["token_required"] is True


def test_gateway_event_api_enqueues_projection_outbox_and_keeps_inline_projection(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "api_projection_outbox.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    event = make_price_tick_event(source="test-gateway")

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )
        outbox_status = client.get("/api/operator/projection-outbox/status")

    connection = open_connection(db_path)
    try:
        sample_count = connection.execute(
            "SELECT COUNT(*) AS count FROM market_tick_samples"
        ).fetchone()["count"]
        outbox_count = connection.execute(
            "SELECT COUNT(*) AS count FROM projection_outbox"
        ).fetchone()["count"]
    finally:
        connection.close()

    assert response.status_code == 200
    assert response.json()["projection_statuses"]["projection_outbox"] == "ENQUEUED"
    assert response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert outbox_status.status_code == 200
    assert outbox_status.json()["shadow_mode"] is True
    assert outbox_status.json()["pending_count"] == 1
    assert outbox_status.json()["by_projection_name"]["market_data"]["pending_count"] == 1
    assert sample_count == 1
    assert outbox_count == 1


def test_gateway_status_exposes_market_index_adapter_separate_from_projection_errors(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api_index_status.sqlite3"))

    heartbeat = heartbeat_event("evt_index_adapter_heartbeat")
    heartbeat["payload"] = {
        "status": "ok",
        "market_index_enabled": True,
        "market_index_registered_codes": ["KOSPI", "KOSDAQ"],
        "market_index_callback_count": 1,
        "parsed_market_index_tick_count": 0,
        "market_index_parse_error_count": 1,
        "latest_market_index_callback_at": TS,
        "latest_market_index_tick_at": "",
        "latest_market_index_parse_error": {
            "reason": "INDEX_PARSE_ERROR",
            "index_code": "KOSPI",
        },
        "market_index_adapter_health": "PARSE_ERROR",
    }
    invalid_index_event = {
        "event_id": "evt_invalid_index_projection",
        "event_type": "market_index_tick",
        "source": "test-gateway",
        "ts": TS,
        "payload": {"index_code": "KOSPI", "index_name": "KOSPI"},
    }

    with TestClient(app) as client:
        heartbeat_response = client.post(
            "/api/gateway/events",
            json=heartbeat,
            headers={"X-Local-Token": "test-token"},
        )
        projection_response = client.post(
            "/api/gateway/events",
            json=invalid_index_event,
            headers={"X-Local-Token": "test-token"},
        )
        gateway_status = client.get("/api/gateway/status")
        market_index_status = client.get("/api/market-indexes/status")

    assert heartbeat_response.status_code == 200
    assert projection_response.status_code == 200
    assert projection_response.json()["projection_statuses"]["market_index"] == "ERROR"
    assert gateway_status.json()["market_index_parse_error_count"] == 1
    assert gateway_status.json()["latest_market_index_callback_at"] == TS
    assert gateway_status.json()["latest_market_index_parse_error"]["reason"] == (
        "INDEX_PARSE_ERROR"
    )
    assert market_index_status.json()["projection_error_count"] == 1


def test_gateway_status_exposes_unregistered_realtime_callback_drops(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api_realtime_admission.sqlite3"))
    heartbeat = heartbeat_event("evt_realtime_admission_heartbeat")
    heartbeat["payload"] = {
        "status": "ok",
        "unregistered_realtime_callback_count": 17,
        "latest_unregistered_realtime_callback": {
            "code": "000660",
            "reason": "UNREGISTERED_REALTIME_CODE",
        },
    }

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json=heartbeat,
            headers={"X-Local-Token": "test-token"},
        )
        gateway_status = client.get("/api/gateway/status")

    assert response.status_code == 200
    assert gateway_status.json()["unregistered_realtime_callback_count"] == 17
    assert gateway_status.json()["latest_unregistered_realtime_callback"]["reason"] == (
        "UNREGISTERED_REALTIME_CODE"
    )


def test_gateway_event_api_projects_market_scan_tr_response(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "api_market_scan.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("MARKET_SCAN_ENABLED", "true")

    event = make_tr_response_event(
        request_id="market_scan:TRADE_VALUE:KOSPI:run-api",
        tr_code="OPT10032",
        request_name="market_scan_trade_value_kospi",
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "순위": "1",
                "현재가": "+70000",
                "등락률": "+2.5%",
                "거래대금": "1,200,000,000",
                "거래량": "100000",
            }
        ],
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )

    connection = open_connection(db_path)
    try:
        latest = connection.execute(
            """
            SELECT code, scan_type, market, trade_value, metadata_json
            FROM market_scan_latest
            WHERE code = '005930'
            """
        ).fetchone()
        order_count = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM gateway_commands
            WHERE command_type IN ('send_order', 'cancel_order', 'modify_order')
            """
        ).fetchone()["count"]
        outbox_projection_names = [
            row["projection_name"]
            for row in connection.execute(
                """
                SELECT projection_name
                FROM projection_outbox
                WHERE event_id = ?
                ORDER BY projection_name
                """,
                (event.event_id,),
            ).fetchall()
        ]
    finally:
        connection.close()

    assert response.status_code == 200
    assert response.json()["projection_statuses"]["projection_outbox"] == "ENQUEUED"
    assert response.json()["projection_statuses"]["market_scan"] == "APPLIED"
    assert latest is not None
    assert latest["scan_type"] == "TRADE_VALUE"
    assert latest["market"] == "KOSPI"
    assert latest["trade_value"] == 1_200_000_000
    assert '"parser_status":"PILOT_UNVERIFIED"' in latest["metadata_json"]
    assert order_count == 0
    assert outbox_projection_names == ["market_data", "market_scan"]
