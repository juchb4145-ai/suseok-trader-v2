from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from apps.core_api import app
from domain.broker.events import GatewayEvent
from fastapi.testclient import TestClient
from services.runtime import projection_replay
from services.runtime.projection_replay import (
    export_replay_bundle,
    get_projection_replay_status,
    import_replay_bundle,
    install_projection_replay_write_guard,
    run_projection_replay_parity,
    validate_replay_bundle,
)
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database, open_connection
from tools.ops_projection_replay import (
    build_projection_replay_report,
    write_projection_replay_report,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "projection_replay" / "market_data_events.json"
TRADE_DATE = "2026-06-26"


def test_replay_bundle_export_import_preserves_order_and_received_at(tmp_path) -> None:
    source_db = _fixture_source_db(tmp_path / "source.sqlite3")
    bundle_dir = tmp_path / "bundle"
    target_db = tmp_path / "isolated" / "replay.sqlite3"

    bundle = export_replay_bundle(
        source_db_path=source_db,
        bundle_dir=bundle_dir,
        trade_date=TRADE_DATE,
    )
    imported = import_replay_bundle(
        bundle_dir=bundle_dir,
        target_db_path=target_db,
        operational_db_path=tmp_path / "operational.sqlite3",
    )

    fixture = _fixture_rows()
    connection = open_connection(target_db)
    try:
        rows = connection.execute(
            "SELECT event_id, received_at FROM gateway_events ORDER BY rowid"
        ).fetchall()
    finally:
        connection.close()

    assert bundle.event_count == len(fixture)
    assert bundle.event_types == ("condition_event", "price_tick", "tr_response")
    assert bundle.venue_counts == {"KRX": 2, "NXT": 1, "UNKNOWN": 1}
    assert [row["event_id"] for row in rows] == [item["event"]["event_id"] for item in fixture]
    assert [row["received_at"] for row in rows] == [item["received_at"] for item in fixture]
    assert imported.imported_event_count == len(fixture)
    assert imported.order_preserved is True
    assert imported.imported_event_order_sha256 == bundle.event_order_sha256
    assert imported.no_trading_side_effects is True
    assert all(value == 0 for value in imported.side_effect_table_delta.values())


def test_replay_bundle_rejects_tamper_and_operational_target(tmp_path) -> None:
    source_db = _fixture_source_db(tmp_path / "source.sqlite3")
    bundle_dir = tmp_path / "bundle"
    export_replay_bundle(source_db_path=source_db, bundle_dir=bundle_dir)

    with pytest.raises(ValueError, match="operational DB"):
        import_replay_bundle(
            bundle_dir=bundle_dir,
            target_db_path=tmp_path / "operational.sqlite3",
            operational_db_path=tmp_path / "operational.sqlite3",
        )

    events_path = bundle_dir / "events.jsonl"
    records = events_path.read_text(encoding="utf-8").splitlines()
    record = json.loads(records[0])
    record["event"]["payload"]["price"] = 1
    records[0] = json.dumps(record, ensure_ascii=False, sort_keys=True)
    events_path.write_text("\n".join(records) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="payload hash mismatch"):
        validate_replay_bundle(bundle_dir)


def test_inline_worker_projection_replay_fixture_has_exact_parity(tmp_path) -> None:
    source_db = _fixture_source_db(tmp_path / "source.sqlite3")
    bundle_dir = tmp_path / "bundle"
    export_replay_bundle(
        source_db_path=source_db,
        bundle_dir=bundle_dir,
        trade_date=TRADE_DATE,
    )

    result = run_projection_replay_parity(
        bundle_dir=bundle_dir,
        work_root=tmp_path / "work",
        operational_db_path=tmp_path / "operational.sqlite3",
        batch_size=2,
    )

    assert result.status == "PASS"
    assert result.failures == ()
    assert result.warnings == ()
    assert result.projection_hash_match is True
    assert result.mismatched_tables == ()
    assert result.inline.reconcile_status == "PASS"
    assert result.worker.reconcile_status == "PASS"
    assert result.inline.market_data_outbox_counts["APPLIED"] == 4
    assert result.worker.market_data_outbox_counts["APPLIED"] == 4
    assert result.inline.market_data_outbox_counts["ERROR"] == 0
    assert result.worker.market_data_outbox_counts["DEAD_LETTER"] == 0
    assert result.inline.snapshot.projection_counts_by_venue == {"KRX": 2, "NXT": 1}
    assert result.worker.snapshot.projection_counts_by_venue == {"KRX": 2, "NXT": 1}
    assert result.no_trading_side_effects is True

    for db_path in (result.inline.db_path, result.worker.db_path):
        connection = open_connection(db_path)
        try:
            synthetic = connection.execute(
                """
                SELECT received_at
                FROM market_tick_samples
                WHERE event_id LIKE 'fixture-tr-response-1:synthetic_price_tick:%'
                """
            ).fetchone()
            parent = connection.execute(
                "SELECT received_at FROM gateway_events WHERE event_id = ?",
                ("fixture-tr-response-1",),
            ).fetchone()
            assert _count(connection, "gateway_commands") == 0
            assert _count(connection, "order_plan_drafts") == 0
            assert _count(connection, "incremental_evaluation_queue") == 0
            assert _count(connection, "candidate_condition_fusion") == 0
        finally:
            connection.close()
        assert synthetic["received_at"] == parent["received_at"]


def test_projection_replay_write_guard_denies_order_table_write(tmp_path) -> None:
    connection = initialize_database(tmp_path / "guard.sqlite3")
    blocked = install_projection_replay_write_guard(connection)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            connection.execute(
                """
                INSERT INTO gateway_commands (
                    command_id, command_type, source, status, payload_json, payload_hash
                )
                VALUES ('cmd-replay', 'send_order', 'test', 'QUEUED', '{}', 'hash')
                """
            )
    finally:
        connection.close()

    assert blocked
    assert blocked[0]["table_name"] == "gateway_commands"


def test_projection_replay_status_is_read_only_in_operator_and_dashboard(
    tmp_path,
    monkeypatch,
) -> None:
    source_db = _fixture_source_db(tmp_path / "source.sqlite3")
    bundle_dir = tmp_path / "bundle"
    export_replay_bundle(source_db_path=source_db, bundle_dir=bundle_dir)
    parity = run_projection_replay_parity(
        bundle_dir=bundle_dir,
        work_root=tmp_path / "work",
        operational_db_path=tmp_path / "operational.sqlite3",
    )
    report = build_projection_replay_report(parity.to_dict())
    report_root = tmp_path / "reports"
    paths = write_projection_replay_report(report, out_dir=report_root)
    monkeypatch.setattr(projection_replay, "DEFAULT_REPORT_ROOT", report_root)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "api.sqlite3"))

    direct = get_projection_replay_status()
    with TestClient(app) as client:
        operator = client.get("/api/operator/projection-replay/status")
        dashboard = client.get("/api/dashboard/snapshot?fast=true&sections=projection_replay")

    assert direct["status"] == "PASS"
    assert direct["latest_report_path"] == str(paths["raw_json"])
    assert direct["replay_execution_available"] is False
    assert direct["production_db_writes_allowed"] is False
    assert operator.status_code == 200
    assert operator.json()["status"] == "PASS"
    assert operator.json()["read_only"] is True
    assert dashboard.status_code == 200
    assert dashboard.json()["projection_replay"]["status"] == "PASS"
    assert dashboard.json()["projection_replay"]["no_order_side_effects"] is True


def _fixture_source_db(path: Path) -> Path:
    connection = initialize_database(path)
    try:
        for item in _fixture_rows():
            event = GatewayEvent.from_dict(item["event"])
            result = append_gateway_event(connection, event)
            assert result.status == "ACCEPTED"
            connection.execute(
                "UPDATE raw_events SET received_at = ? WHERE event_id = ?",
                (item["received_at"], event.event_id),
            )
            connection.execute(
                "UPDATE gateway_events SET received_at = ? WHERE event_id = ?",
                (item["received_at"], event.event_id),
            )
            connection.commit()
    finally:
        connection.close()
    return path


def _fixture_rows() -> list[dict]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    return payload


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
