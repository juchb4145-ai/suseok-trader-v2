from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from apps.core_api import app
from domain.broker.utils import datetime_to_wire, utc_now
from fastapi.testclient import TestClient
from services.config import Settings
from services.dashboard_service import build_dashboard_snapshot_sections
from services.theme_coherency import build_theme_coherency_status
from services.theme_leadership import (
    ThemeLeadershipRebuildResult,
    rebuild_theme_leadership,
)
from storage.sqlite import initialize_database
from tests.test_dashboard_service import _insert_theme_snapshot
from tests.test_theme_leadership_service import _leadership_snapshot


def test_flow_leadership_preserves_db_snapshot_provenance(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme-coherency-flow.sqlite3")
    calculated_at = datetime_to_wire(utc_now())
    _insert_theme_snapshot(
        connection,
        theme_id="theme-a",
        theme_name="Theme A",
        state="LEADING",
        calculated_at=calculated_at,
        total_trade_value=500_000_000,
    )
    settings = Settings(
        market_scan_enabled=True,
        theme_snapshot_stale_sec=999_999_999,
    )

    result = rebuild_theme_leadership(connection, settings=settings)
    status = build_theme_coherency_status(
        connection,
        settings=settings,
        leadership_result=result,
    )
    connection.close()

    snapshot = result.snapshots[0]
    payload = snapshot.to_dict(include_members=False)
    assert snapshot.source == "THEME_FLOW_SNAPSHOT"
    assert snapshot.snapshot_id == "snapshot-theme-a"
    assert datetime_to_wire(snapshot.calculated_at) == calculated_at
    assert {
        "source",
        "snapshot_id",
        "calculated_at",
        "data_age_sec",
        "watchset_selection_source",
    } <= payload.keys()
    assert status["status"] == "PASS"
    assert status["snapshot_mismatch_count"] == 0
    assert status["source_mismatch_count"] == 0
    assert status["db_top_items"][0]["snapshot_id"] == snapshot.snapshot_id
    assert status["leadership_items"][0]["snapshot_id"] == snapshot.snapshot_id


def test_coherency_fails_when_latest_pointer_moves_after_flow_rebuild(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme-coherency-drift.sqlite3")
    calculated_at = datetime_to_wire(utc_now())
    _insert_theme_snapshot(
        connection,
        theme_id="theme-a",
        theme_name="Theme A",
        state="LEADING",
        calculated_at=calculated_at,
        total_trade_value=500_000_000,
    )
    settings = Settings(
        market_scan_enabled=True,
        theme_snapshot_stale_sec=999_999_999,
    )
    result = rebuild_theme_leadership(connection, settings=settings)

    old_row = dict(
        connection.execute(
            "SELECT * FROM theme_snapshots WHERE snapshot_id = 'snapshot-theme-a'"
        ).fetchone()
    )
    new_calculated_at = datetime_to_wire(utc_now() + timedelta(seconds=1))
    old_row["snapshot_id"] = "snapshot-theme-a-v2"
    old_row["calculated_at"] = new_calculated_at
    columns = list(old_row)
    connection.execute(
        f"INSERT INTO theme_snapshots ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        tuple(old_row[column] for column in columns),
    )
    connection.execute(
        """
        UPDATE theme_latest_snapshots
        SET snapshot_id = ?, calculated_at = ?
        WHERE theme_id = ?
        """,
        ("snapshot-theme-a-v2", new_calculated_at, "theme-a"),
    )
    connection.commit()

    status = build_theme_coherency_status(
        connection,
        settings=settings,
        leadership_result=result,
    )
    connection.close()

    assert status["status"] == "FAIL"
    assert status["snapshot_mismatch_count"] == 2
    assert "THEME_FLOW_SNAPSHOT_POINTER_MISMATCH" in status["reason_codes"]
    assert "THEME_FLOW_SNAPSHOT_CALCULATED_AT_MISMATCH" in status["reason_codes"]


def test_realtime_leadership_source_difference_is_explicit_warn(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme-coherency-source.sqlite3")
    calculated_at = datetime_to_wire(utc_now())
    _insert_theme_snapshot(
        connection,
        theme_id="theme-a",
        theme_name="Theme A",
        state="LEADING",
        calculated_at=calculated_at,
    )
    realtime_snapshot = replace(
        _leadership_snapshot(rank=1, state="LEADING"),
        theme_id="theme-a",
        theme_name="Theme A",
        source="REALTIME_UNIVERSE_REBUILD",
        snapshot_id=None,
        calculated_at=calculated_at,
    )
    leadership = ThemeLeadershipRebuildResult(
        status="OK",
        snapshots=[realtime_snapshot],
        diagnostic_top_theme_count=1,
        eligible_theme_count=1,
        watchset_selection_theme_count=1,
        watchset_selection_source="eligible_ranked",
    )

    status = build_theme_coherency_status(
        connection,
        settings=Settings(theme_snapshot_stale_sec=999_999_999),
        leadership_result=leadership,
    )
    connection.close()

    assert status["status"] == "WARN"
    assert status["source_mismatch_count"] == 1
    assert status["snapshot_mismatch_count"] == 0
    assert "THEME_LEADERSHIP_SOURCE_DIFFERS_FROM_DB_TOP" in status["reason_codes"]
    assert status["leadership_items"][0]["watchset_selection_source"] == (
        "eligible_ranked"
    )


def test_theme_coherency_compares_matching_top_prefix_when_limits_differ(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "theme-coherency-top-prefix.sqlite3")
    calculated_at = datetime_to_wire(utc_now())
    for index in range(6):
        _insert_theme_snapshot(
            connection,
            theme_id=f"theme-{index}",
            theme_name=f"Theme {index}",
            state="LEADING",
            calculated_at=calculated_at,
            total_trade_value=600_000_000 - index * 10_000_000,
        )
    settings = Settings(
        market_scan_enabled=True,
        theme_leadership_top_theme_count=5,
        theme_snapshot_stale_sec=999_999_999,
    )

    status = build_theme_coherency_status(
        connection,
        settings=settings,
        limit=10,
    )
    connection.close()

    assert status["status"] == "PASS"
    assert status["db_top_count"] == 6
    assert status["leadership_top_count"] == 5
    assert status["top_comparison_count"] == 5
    assert status["overlap_count"] == 5
    assert status["top_set_mismatch_count"] == 0


def test_theme_coherency_operator_and_fast_dashboard_are_read_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "theme-coherency-api.sqlite3"
    connection = initialize_database(db_path)
    _insert_theme_snapshot(
        connection,
        theme_id="theme-a",
        theme_name="Theme A",
        state="LEADING",
        calculated_at=datetime_to_wire(utc_now()),
    )
    command_count_before = _command_count(connection)
    connection.close()
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("MARKET_SCAN_ENABLED", "true")
    monkeypatch.setenv("THEME_SNAPSHOT_STALE_SEC", "999999999")

    with TestClient(app) as client:
        direct = client.get("/api/operator/theme-coherency/status?limit=5")
        aggregate = client.get("/api/operator/status")
        dashboard = client.get(
            "/api/dashboard/snapshot",
            params={
                "fast": "true",
                "sections": "theme_coherency,pipeline_summary",
            },
        )

    connection = initialize_database(db_path)
    command_count_after = _command_count(connection)
    connection.close()

    assert direct.status_code == 200
    assert direct.json()["status"] == "PASS"
    assert aggregate.status_code == 200
    assert aggregate.json()["theme_coherency"]["status"] == "PASS"
    assert dashboard.status_code == 200
    assert dashboard.json()["theme_coherency"]["status"] == "PASS"
    assert dashboard.json()["pipeline_summary"]["theme_coherency"]["status"] == (
        "PASS"
    )
    assert command_count_after == command_count_before == 0


def test_fast_default_does_not_rebuild_theme_coherency(tmp_path, monkeypatch) -> None:
    connection = initialize_database(tmp_path / "theme-fast-default.sqlite3")

    def fail(*args, **kwargs):
        raise AssertionError("default fast dashboard must not rebuild theme leadership")

    monkeypatch.setattr("services.dashboard_service.build_theme_coherency_status", fail)
    snapshot = build_dashboard_snapshot_sections(
        connection,
        Settings(),
        sections={"pipeline_summary"},
    )
    connection.close()

    assert snapshot["pipeline_summary"]["theme_coherency"] is None


def _command_count(connection) -> int:
    row = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()
    return int(row["count"])
