from __future__ import annotations

from datetime import UTC, datetime

from apps.core_api import app
from domain.broker.events import GatewayEvent
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import utc_now
from fastapi.testclient import TestClient
from services.config import Settings, TradingMode
from services.market_index_service import (
    get_latest_market_index_tick,
    process_market_index_event,
)
from services.market_index_tr_bootstrap import (
    MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_VERSION,
    MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
    get_market_index_tr_bootstrap_status,
    inspect_market_index_tr_bootstrap_event,
    run_market_index_tr_bootstrap_once,
)
from services.runtime.market_index_projection_reconcile import (
    run_market_index_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch
from storage.event_store import append_gateway_event
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection


def test_bootstrap_plan_is_disabled_by_default_and_observe_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-bootstrap-plan.sqlite3")
    disabled = run_market_index_tr_bootstrap_once(connection, settings=Settings())
    enabled = run_market_index_tr_bootstrap_once(
        connection,
        settings=_settings(),
    )
    blocked = run_market_index_tr_bootstrap_once(
        connection,
        settings=_settings(
            trading_mode=TradingMode.LIVE_SIM,
            trading_allow_live_sim=True,
        ),
        queue_commands=True,
    )
    command_count = _command_count(connection)
    connection.close()

    assert disabled.status == "DISABLED"
    assert enabled.status == "PLAN_ONLY"
    assert enabled.planned_count == 2
    assert enabled.command_count == 0
    assert all(not item.queued for item in enabled.command_results)
    assert blocked.status == "BLOCKED_SAFETY"
    assert command_count == 0


def test_bootstrap_queue_creates_only_request_tr_commands(tmp_path, monkeypatch) -> None:
    connection = initialize_database(tmp_path / "market-index-bootstrap-queue.sqlite3")
    fixed_now = datetime(2026, 7, 10, 1, 0, tzinfo=UTC)
    monkeypatch.setattr(
        "services.market_index_tr_bootstrap.utc_now",
        lambda: fixed_now,
    )
    monkeypatch.setattr(
        "storage.gateway_command_store.utc_now",
        lambda: fixed_now,
    )
    result = run_market_index_tr_bootstrap_once(
        connection,
        settings=_settings(),
        queue_commands=True,
        trade_date="2026-07-10",
    )
    duplicate = run_market_index_tr_bootstrap_once(
        connection,
        settings=_settings(),
        queue_commands=True,
        trade_date="2026-07-10",
    )
    rows = connection.execute(
        "SELECT command_type, source, payload_json FROM gateway_commands ORDER BY created_at"
    ).fetchall()
    status = get_market_index_tr_bootstrap_status(
        connection,
        settings=_settings(),
    )
    connection.close()

    assert result.status == "QUEUED"
    assert result.command_count == 2
    assert duplicate.status == "DUPLICATE"
    assert duplicate.command_count == 0
    assert {row["command_type"] for row in rows} == {"request_tr"}
    assert {row["source"] for row in rows} == {MARKET_INDEX_TR_BOOTSTRAP_SOURCE}
    assert all("send_order" not in row["payload_json"] for row in rows)
    assert status["adapter_status"] == "IMPLEMENTED"
    assert status["command_count"] == 2
    assert status["status"] == "READY_KOA_PENDING"


def test_valid_bootstrap_parent_projects_with_durable_source_lineage(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-bootstrap-inline.sqlite3")
    settings = _settings(market_index_tr_bootstrap_parser_status="VERIFIED")
    event = _bootstrap_event("KOSPI", event_id="evt-bootstrap-kospi")
    append_gateway_event(connection, event)
    jobs = enqueue_projection_jobs_for_gateway_event(connection, event)

    inspection = inspect_market_index_tr_bootstrap_event(event, settings=settings)
    result = process_market_index_event(connection, event, settings=settings)
    duplicate = process_market_index_event(connection, event, settings=settings)
    latest = get_latest_market_index_tick(connection, "KOSPI")
    sample = connection.execute(
        "SELECT * FROM market_index_tick_samples WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    connection.close()

    assert inspection.recognized is True
    assert inspection.valid is True
    assert result.status == "APPLIED"
    assert duplicate.status == "DUPLICATE"
    assert latest["event_id"] == event.event_id
    assert latest["data_source"] == "TR_BOOTSTRAP"
    assert latest["metadata"]["parent_event_id"] == event.event_id
    assert sample["event_id"] == event.event_id
    assert {job["projection_name"] for job in jobs.jobs} == {
        "market_data",
        "market_index",
        "market_regime",
    }


def test_bootstrap_lineage_mismatch_fails_closed(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-bootstrap-invalid.sqlite3")
    settings = _settings()
    event = _bootstrap_event(
        "KOSPI",
        event_id="evt-bootstrap-invalid",
        metadata_overrides={"index_code": "KOSDAQ"},
    )
    append_gateway_event(connection, event)

    inspection = inspect_market_index_tr_bootstrap_event(event, settings=settings)
    result = process_market_index_event(connection, event, settings=settings)
    error = connection.execute(
        "SELECT reason_code FROM market_index_projection_errors WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()
    sample_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_index_tick_samples"
    ).fetchone()["count"]
    connection.close()

    assert inspection.recognized is True
    assert inspection.valid is False
    assert "MARKET_INDEX_TR_BOOTSTRAP_INDEX_LINEAGE_MISMATCH" in (
        inspection.reason_codes
    )
    assert result.status == "ERROR"
    assert error["reason_code"] == "MARKET_INDEX_TR_BOOTSTRAP_INVALID"
    assert sample_count == 0


def test_bootstrap_parent_can_be_applied_by_market_index_worker(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-bootstrap-worker.sqlite3")
    settings = _settings(
        market_index_tr_bootstrap_parser_status="VERIFIED",
        projection_outbox_apply_projection_enabled=True,
        projection_outbox_market_index_apply_enabled=True,
        projection_outbox_market_index_apply_min_age_sec=0,
        market_regime_enabled=False,
    )
    event = _bootstrap_event("KOSPI", event_id="evt-bootstrap-worker")
    append_gateway_event(connection, event)
    enqueue_projection_jobs_for_gateway_event(connection, event)

    result = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=1,
        apply_projection=True,
        live_safe=True,
        projection_name="market_index",
    )
    sample_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_index_tick_samples WHERE event_id = ?",
        (event.event_id,),
    ).fetchone()["count"]
    outbox = connection.execute(
        """
        SELECT status FROM projection_outbox
        WHERE projection_name = 'market_index' AND event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    connection.close()

    assert result.status == "COMPLETED"
    assert result.applied_by_worker_count == 1
    assert result.mutated_projection_names == ("market_index",)
    assert sample_count == 1
    assert outbox["status"] == "APPLIED"


def test_bootstrap_pair_reconcile_passes_without_not_implemented_failure(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-index-bootstrap-reconcile.sqlite3")
    settings = _settings(
        market_index_tr_bootstrap_parser_status="VERIFIED",
        market_regime_enabled=False,
    )
    for index_code in ("KOSPI", "KOSDAQ"):
        event = _bootstrap_event(
            index_code,
            event_id=f"evt-bootstrap-{index_code.lower()}",
        )
        append_gateway_event(connection, event)
        enqueue_projection_jobs_for_gateway_event(connection, event)
        assert process_market_index_event(connection, event, settings=settings).status == (
            "APPLIED"
        )
    worker = process_projection_outbox_batch(
        connection,
        settings=settings,
        limit=10,
        apply_projection=False,
        live_safe=True,
        projection_name="market_index",
    )
    reconcile = run_market_index_projection_reconcile(
        connection,
        settings=settings,
        persist=False,
    )
    connection.close()

    assert worker.applied_count == 2
    assert reconcile.status == "PASS"
    assert reconcile.tr_bootstrap_source_count == 2
    assert reconcile.realtime_source_count == 0
    assert reconcile.parser_verified_count == 2
    assert reconcile.data_usable_count == 2
    assert "MARKET_INDEX_TR_BOOTSTRAP_SOURCE_NOT_IMPLEMENTED" not in (
        reconcile.reason_codes
    )


def test_bootstrap_gateway_and_operator_api_remain_order_safe(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-index-bootstrap-api.sqlite3"
    initialize_database(db_path).close()
    safe_env = tmp_path / "missing-safe.env"
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "bootstrap-token")
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED", "true")
    monkeypatch.setenv("MARKET_INDEX_TR_BOOTSTRAP_PARSER_STATUS", "VERIFIED")
    monkeypatch.setenv("MARKET_REGIME_ENABLED", "false")
    event = _bootstrap_event("KOSPI", event_id="evt-bootstrap-api")

    with TestClient(app) as client:
        headers = {"X-Core-Token": "bootstrap-token", "X-Local-Token": "bootstrap-token"}
        posted = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers=headers,
        )
        status = client.get(
            "/api/operator/market-index/tr-bootstrap/status",
            headers=headers,
        )
        planned = client.post(
            "/api/operator/market-index/tr-bootstrap/run-once",
            headers=headers,
        )
        queued = client.post(
            "/api/operator/market-index/tr-bootstrap/run-once?queue_commands=true",
            headers=headers,
        )
        dashboard = client.get(
            "/api/dashboard/snapshot",
            params={
                "fast": "true",
                "sections": "market_index_tr_bootstrap,pipeline_summary",
            },
            headers=headers,
        )

    connection = open_connection(db_path)
    command_types = {
        row["command_type"]
        for row in connection.execute("SELECT command_type FROM gateway_commands")
    }
    order_command_count = connection.execute(
        """
        SELECT COUNT(*) AS count FROM gateway_commands
        WHERE command_type IN ('send_order', 'cancel_order')
        """
    ).fetchone()["count"]
    outbox_names = {
        row["projection_name"]
        for row in connection.execute(
            "SELECT projection_name FROM projection_outbox WHERE event_id = ?",
            (event.event_id,),
        )
    }
    connection.close()

    assert posted.status_code == 200
    assert posted.json()["projection_statuses"]["market_index"] == "APPLIED"
    assert posted.json()["projection_statuses"][
        "market_index_effective_skip_inline"
    ] == "FALSE"
    assert status.status_code == 200
    assert status.json()["adapter_status"] == "IMPLEMENTED"
    assert planned.json()["status"] == "PLAN_ONLY"
    assert planned.json()["command_count"] == 0
    assert queued.json()["status"] == "QUEUED"
    assert queued.json()["command_count"] == 2
    assert dashboard.json()["market_index_tr_bootstrap"]["status"] == "READY"
    assert dashboard.json()["pipeline_summary"]["market_index_tr_bootstrap"][
        "status"
    ] == "READY"
    assert command_types == {"request_tr"}
    assert order_command_count == 0
    assert outbox_names == {"market_data", "market_index", "market_regime"}


def _settings(**overrides) -> Settings:
    values = {
        "market_index_tr_bootstrap_enabled": True,
        "market_index_tr_bootstrap_parser_status": "PILOT_UNVERIFIED_KOA_STUDIO",
        "market_index_stale_sec": 999_999_999,
        "market_data_bar_intervals_sec": (60, 180, 300),
        "projection_outbox_market_index_apply_min_age_sec": 0,
        "projection_outbox_shadow_min_age_sec": 0,
    }
    values.update(overrides)
    return Settings(**values)


def _bootstrap_event(
    index_code: str,
    *,
    event_id: str,
    metadata_overrides: dict | None = None,
) -> GatewayEvent:
    now = utc_now()
    metadata = {
        "source": MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
        "projection_source": MARKET_INDEX_TR_BOOTSTRAP_SOURCE,
        "tr_bootstrap_contract_version": MARKET_INDEX_TR_BOOTSTRAP_CONTRACT_VERSION,
        "adapter_status": "IMPLEMENTED",
        "index_code": index_code,
        "parser_status": "VERIFIED",
        "market_type": "0" if index_code == "KOSPI" else "1",
        "industry_code": "001" if index_code == "KOSPI" else "101",
        **dict(metadata_overrides or {}),
    }
    response = BrokerTrResponse(
        request_id=f"market_index_tr_bootstrap:{index_code}:fixture-run",
        tr_code="OPT20001",
        request_name=f"market_index_tr_bootstrap_{index_code.lower()}",
        success=True,
        rows=[
            {
                "현재가": "2,850.25" if index_code == "KOSPI" else "850.75",
                "전일대비": "+12.50",
                "등락률": "+0.44",
                "체결시간": "093015",
            }
        ],
        metadata=metadata,
        ts=now,
    )
    return GatewayEvent(
        event_id=event_id,
        event_type="tr_response",
        source="kiwoom_gateway",
        ts=now,
        payload=response.to_dict(),
    )


def _command_count(connection) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS count FROM gateway_commands"
        ).fetchone()["count"]
    )
