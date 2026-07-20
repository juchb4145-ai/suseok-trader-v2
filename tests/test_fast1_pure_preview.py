from __future__ import annotations

import ast
import inspect
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from apps.core_api import app
from fastapi.testclient import TestClient
from services.live_sim import pure_preview
from services.live_sim.order_plan_eligibility import (
    evaluate_live_sim_order_plan_eligibility,
)
from services.live_sim.order_plan_intent import create_live_sim_intent_from_order_plan
from services.live_sim.pure_preview import (
    FAST0_TRANSITION_STATUS,
    FAST1_PREVIEW_CONTRACT,
    Fast1PreviewError,
    build_fast1_pure_preview,
    open_fast1_preview_connection,
)
from tests.support_fastapi_routes import iter_app_routes
from tests.test_live_sim_order_plan_pipeline import (
    _pilot_settings,
    _prepared_order_plan_connection,
    _set_pilot_api_env,
)

TRADE_DATE = "2026-06-27"


@pytest.fixture(autouse=True)
def _fixed_preview_clock(monkeypatch) -> None:
    monkeypatch.setattr(
        pure_preview,
        "utc_now",
        lambda: datetime(2026, 6, 27, 0, 1, 1, tzinfo=UTC),
    )


def test_fast1_preview_is_current_day_deterministic_and_strict_read_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "fast1-preview.sqlite3"
    connection, order_plan_id = _prepared_fast1_connection(db_path)
    expected = evaluate_live_sim_order_plan_eligibility(
        connection,
        order_plan_id,
        settings=_pilot_settings(),
    ).to_dict()
    _clone_plan_as_historical(connection, order_plan_id)
    connection.close()
    monkeypatch.setattr("services.live_sim.pure_preview.market_today", lambda: TRADE_DATE)

    read_only = open_fast1_preview_connection(db_path)
    first = build_fast1_pure_preview(
        read_only,
        settings=_pilot_settings(),
        trade_date=TRADE_DATE,
    )
    second = build_fast1_pure_preview(
        read_only,
        settings=_pilot_settings(),
        trade_date=TRADE_DATE,
    )
    query_only = int(read_only.execute("PRAGMA query_only").fetchone()[0])
    with pytest.raises(sqlite3.OperationalError):
        read_only.execute("DELETE FROM live_sim_intents")
    read_only.close()

    assert first["contract"] == FAST1_PREVIEW_CONTRACT
    assert first["qualification_transition"] == {
        "fast0_status": FAST0_TRANSITION_STATUS,
        "historical_blockers_resolved": False,
        "historical_data_mutated": False,
        "historical_qualification_required": False,
        "scope": "CURRENT_TRADE_DATE_ONLY",
        "current_gate_dependency": "CURRENT_MARKET_AND_CANARY",
        "operational_activation_authorized": False,
    }
    assert first["current_market"]["status"] == "READY"
    assert first["current_market"]["reason_codes"] == []
    assert first["current_market"]["stock_ticks"]["count"] == 1
    assert first["current_market"]["stock_ticks"]["fresh_count"] == 1
    assert first["current_market"]["market_indexes"]["count"] == 2
    assert first["current_market"]["market_indexes"]["fresh_count"] == 2
    assert first["current_market"]["market_contexts"]["count"] == 2
    assert first["current_market"]["market_contexts"]["fresh_count"] == 2
    assert first["current_market"]["pipeline"]["order_plan_drafts_latest"] == 1
    assert first["selection"]["canary_ready"] is True
    assert first["selection"]["candidate_count"] == 1
    assert first["selection"]["top_candidate"]["order_plan_id"] == order_plan_id
    assert first["selection"]["top_candidate"] == second["selection"]["top_candidate"]
    assert first["plans"][0]["eligible"] is expected["eligible"]
    assert first["plans"][0]["status"] == expected["status"]
    assert first["plans"][0]["reason_codes"] == expected["reason_codes"]
    expected_lineage = expected["evidence_json"]["pipeline_lineage_guard"]
    assert first["plans"][0]["pipeline_lineage"]["status"] == expected_lineage["status"]
    assert first["plans"][0]["pipeline_lineage"]["reason_codes"] == expected_lineage[
        "reason_codes"
    ]
    assert all(plan["order_plan_id"] != "historical-plan" for plan in first["plans"])
    assert first["side_effect_guard"]["total_absolute_delta"] == 0
    assert all(
        delta == 0
        for delta in first["side_effect_guard"]["forbidden_table_deltas"].values()
    )
    assert query_only == 1
    assert first["preview_only"] is True
    assert first["no_order_side_effects"] is True
    assert first["no_broker_calls"] is True
    assert first["live_real_allowed"] is False
    assert "SIM-12345678" not in json.dumps(first, ensure_ascii=False)


def test_fast1_preview_api_requires_token_preserves_all_forbidden_tables(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "fast1-preview-api.sqlite3"
    connection, order_plan_id = _prepared_fast1_connection(db_path)
    connection.close()
    _set_pilot_api_env(monkeypatch, db_path)
    monkeypatch.setattr("services.live_sim.pure_preview.market_today", lambda: TRADE_DATE)

    with TestClient(app) as client:
        before = _forbidden_counts(db_path)
        unauthorized = client.post("/api/live-sim/pilot/preview")
        response = client.post(
            "/api/live-sim/pilot/preview",
            params={"trade_date": TRADE_DATE},
            headers={"X-Local-Token": "secret-token"},
        )
        wrong_date = client.post(
            "/api/live-sim/pilot/preview",
            params={"trade_date": "2026-06-26"},
            headers={"X-Local-Token": "secret-token"},
        )
        after = _forbidden_counts(db_path)

    payload = response.json()
    assert unauthorized.status_code == 401
    assert response.status_code == 200
    assert payload["selection"]["top_candidate"]["order_plan_id"] == order_plan_id
    assert payload["selection"]["canary_ready"] is True
    assert payload["side_effect_guard"]["forbidden_table_counts_before"] == payload[
        "side_effect_guard"
    ]["forbidden_table_counts_after"]
    assert before == after
    assert wrong_date.status_code == 422
    assert wrong_date.json()["detail"]["reason_code"] == "FAST1_CURRENT_TRADE_DATE_ONLY"


def test_fast1_preview_does_not_select_duplicate_intent(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "fast1-preview-duplicate.sqlite3"
    connection, order_plan_id = _prepared_fast1_connection(db_path)
    create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=_pilot_settings(),
    )
    connection.close()
    monkeypatch.setattr("services.live_sim.pure_preview.market_today", lambda: TRADE_DATE)

    read_only = open_fast1_preview_connection(db_path)
    result = build_fast1_pure_preview(
        read_only,
        settings=_pilot_settings(),
        trade_date=TRADE_DATE,
    )
    read_only.close()

    assert result["selection"]["top_candidate"] is None
    assert result["selection"]["canary_ready"] is False
    assert "ORDER_PLAN_DUPLICATE_INTENT" in result["plans"][0]["reason_codes"]
    assert result["plans"][0]["duplicate_intent"] is True
    assert result["side_effect_guard"]["forbidden_table_counts_before"][
        "live_sim_intents"
    ] == 1
    assert result["side_effect_guard"]["total_absolute_delta"] == 0


def test_fast1_preview_selection_ignores_ai_advisory_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "fast1-preview-ai.sqlite3"
    connection, order_plan_id = _prepared_fast1_connection(db_path)
    connection.close()
    monkeypatch.setattr("services.live_sim.pure_preview.market_today", lambda: TRADE_DATE)

    read_only = open_fast1_preview_connection(db_path)
    before = build_fast1_pure_preview(
        read_only,
        settings=_pilot_settings(),
        trade_date=TRADE_DATE,
    )
    read_only.close()

    writable = sqlite3.connect(db_path)
    for table in ("order_plan_drafts", "order_plan_drafts_latest"):
        row = writable.execute(
            f"SELECT evidence_json FROM {table} WHERE order_plan_id = ?",
            (order_plan_id,),
        ).fetchone()
        evidence = json.loads(str(row[0]))
        evidence["ai_advisory"] = {"score": 999, "recommendation": "IGNORED"}
        writable.execute(
            f"UPDATE {table} SET evidence_json = ? WHERE order_plan_id = ?",
            (json.dumps(evidence), order_plan_id),
        )
    writable.commit()
    writable.close()

    read_only = open_fast1_preview_connection(db_path)
    after = build_fast1_pure_preview(
        read_only,
        settings=_pilot_settings(),
        trade_date=TRADE_DATE,
    )
    read_only.close()

    assert before["selection"]["top_candidate"] == after["selection"]["top_candidate"]
    assert before["selection"]["canary_ready"] is after["selection"]["canary_ready"]


def test_fast1_preview_rejects_non_query_only_connections(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "fast1-rw.sqlite3")

    with pytest.raises(Fast1PreviewError) as exc_info:
        build_fast1_pure_preview(connection, settings=_pilot_settings())
    connection.close()

    assert exc_info.value.reason_code == "PREVIEW_QUERY_ONLY_REQUIRED"


def test_fast1_preview_read_only_open_does_not_create_sqlite_sidecars(tmp_path) -> None:
    db_path = tmp_path / "fast1-no-sidecars.sqlite3"
    connection, _ = _prepared_fast1_connection(db_path)
    connection.close()
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")

    assert wal_path.exists() is False
    assert shm_path.exists() is False

    read_only = open_fast1_preview_connection(db_path)
    read_only.execute("SELECT COUNT(*) FROM market_ticks_latest").fetchone()
    read_only.close()

    assert wal_path.exists() is False
    assert shm_path.exists() is False


def test_fast1_preview_blocks_when_current_market_inputs_are_missing(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "fast1-preview-market-blocked.sqlite3"
    connection, order_plan_id = _prepared_fast1_connection(db_path)
    connection.execute(
        """
        UPDATE market_ticks_latest
        SET event_ts = '2026-06-26T00:01:00Z',
            received_at = '2026-06-26T00:01:00Z'
        """
    )
    connection.execute("DELETE FROM market_index_ticks_latest")
    connection.execute("DELETE FROM market_context_latest")
    connection.commit()
    connection.close()
    monkeypatch.setattr("services.live_sim.pure_preview.market_today", lambda: TRADE_DATE)

    read_only = open_fast1_preview_connection(db_path)
    result = build_fast1_pure_preview(
        read_only,
        settings=_pilot_settings(),
        trade_date=TRADE_DATE,
    )
    read_only.close()

    assert result["selection"]["top_candidate"]["order_plan_id"] == order_plan_id
    assert result["selection"]["canary_ready"] is False
    assert result["current_market"]["status"] == "BLOCKED"
    assert result["current_market"]["reason_codes"] == [
        "NO_CURRENT_MARKET_TICK",
        "NO_CURRENT_MARKET_INDEX",
        "NO_CURRENT_MARKET_CONTEXT",
    ]
    assert result["current_market"]["stock_ticks"]["latest_current_at"] is None
    assert result["current_market"]["stock_ticks"]["latest_available_at"] == (
        "2026-06-26T00:01:00Z"
    )
    assert result["selection"]["blocker_reason_codes"][:3] == result[
        "current_market"
    ]["reason_codes"]
    assert result["side_effect_guard"]["total_absolute_delta"] == 0


def test_fast1_preview_blocks_when_current_market_inputs_are_stale(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "fast1-preview-market-stale.sqlite3"
    connection, _ = _prepared_fast1_connection(db_path)
    connection.close()
    monkeypatch.setattr("services.live_sim.pure_preview.market_today", lambda: TRADE_DATE)
    monkeypatch.setattr(
        pure_preview,
        "utc_now",
        lambda: datetime(2026, 6, 27, 0, 2, tzinfo=UTC),
    )

    read_only = open_fast1_preview_connection(db_path)
    result = build_fast1_pure_preview(
        read_only,
        settings=_pilot_settings(
            market_data_tick_stale_sec=10,
            market_data_degraded_tick_stale_sec=30,
            market_index_stale_sec=30,
            market_context_snapshot_stale_sec=30,
        ),
        trade_date=TRADE_DATE,
    )
    read_only.close()

    assert result["selection"]["canary_ready"] is False
    assert result["current_market"]["status"] == "BLOCKED"
    assert result["current_market"]["reason_codes"] == [
        "CURRENT_MARKET_TICK_STALE",
        "CURRENT_MARKET_INDEX_STALE",
        "CURRENT_MARKET_CONTEXT_STALE",
    ]
    assert result["current_market"]["stock_ticks"]["count"] == 1
    assert result["current_market"]["stock_ticks"]["fresh_count"] == 0
    assert result["current_market"]["market_indexes"]["count"] == 2
    assert result["current_market"]["market_indexes"]["fresh_count"] == 0
    assert result["current_market"]["market_contexts"]["count"] == 2
    assert result["current_market"]["market_contexts"]["fresh_count"] == 0


def test_fast1_preview_route_is_explicit_and_has_no_generic_order_alias() -> None:
    paths = {route.path for route in iter_app_routes(app)}

    assert "/api/live-sim/pilot/preview" in paths
    assert "/api/orders/preview" not in paths


def test_fast1_preview_module_excludes_mutating_pipeline_calls() -> None:
    tree = ast.parse(inspect.getsource(pure_preview))
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    forbidden = {
        "evaluate_entry_timing",
        "create_live_sim_intent_from_order_plan",
        "record_live_sim_order_plan_rejection",
        "run_live_sim_pilot_pipeline_once",
        "queue_live_sim_order_command",
    }

    assert called_names.isdisjoint(forbidden)


def _forbidden_counts(db_path) -> dict[str, int]:
    connection = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    names = [
        str(row["name"])
        for row in rows
        if str(row["name"]).startswith("dry_run_")
        or str(row["name"])
        in {
            "entry_timing_evaluations",
            "order_plan_drafts",
            "order_plan_drafts_latest",
            "live_sim_intents",
            "live_sim_orders",
            "live_sim_runs",
            "live_sim_operating_runs",
            "live_sim_rejections",
            "gateway_commands",
            "gateway_command_events",
            "gateway_command_dedupe_keys",
        }
    ]
    counts = {
        name: int(
            connection.execute(
                f'SELECT COUNT(*) FROM "{name.replace(chr(34), chr(34) * 2)}"'
            ).fetchone()[0]
        )
        for name in names
    }
    connection.close()
    return counts


def _prepared_fast1_connection(path):
    connection, order_plan_id = _prepared_order_plan_connection(path)
    current_at = f"{TRADE_DATE}T00:01:00Z"
    connection.execute(
        """
        UPDATE market_ticks_latest
        SET event_ts = ?, received_at = ?, updated_at = ?
        """,
        (current_at, current_at, current_at),
    )
    for index_code, index_name in (("KOSPI", "KOSPI"), ("KOSDAQ", "KOSDAQ")):
        connection.execute(
            """
            INSERT INTO market_index_ticks_latest (
                index_code, index_name, price, change_rate, change_value,
                trade_time, event_ts, received_at, source, event_id,
                quality_status, metadata_json, updated_at
            )
            VALUES (?, ?, 1000, 0, 0, '090100', ?, ?, 'test', ?, 'FRESH', '{}', ?)
            """,
            (
                index_code,
                index_name,
                current_at,
                current_at,
                f"evt-{index_code}",
                current_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO market_context_latest (
                market, snapshot_id, trade_date, source_watermark_hash,
                snapshot_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                index_code,
                f"snapshot-{index_code}",
                TRADE_DATE,
                f"watermark-{index_code}",
                current_at,
                current_at,
            ),
        )
    connection.commit()
    return connection, order_plan_id


def _clone_plan_as_historical(connection, order_plan_id: str) -> None:
    for table in ("order_plan_drafts", "order_plan_drafts_latest"):
        row = connection.execute(
            f"SELECT * FROM {table} WHERE order_plan_id = ?",
            (order_plan_id,),
        ).fetchone()
        item = {key: row[key] for key in row.keys()}
        item["order_plan_id"] = "historical-plan"
        item["idempotency_key"] = "historical-plan-key"
        item["trade_date"] = "2026-06-26"
        item["priority_score"] = 1_000_000
        columns = list(item)
        placeholders = ", ".join("?" for _ in columns)
        connection.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            tuple(item[column] for column in columns),
        )
    connection.commit()
