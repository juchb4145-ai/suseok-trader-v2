from __future__ import annotations

from apps.core_api import app
from domain.broker.events import GatewayEvent
from fastapi.testclient import TestClient
from gateway.event_factory import make_tr_response_event
from services.config import Settings, clear_settings_cache
from services.runtime import market_data_projection_side_effects as side_effects
from services.runtime.market_data_projection_side_effects import (
    enqueue_incremental_for_candidate_quote_refresh_tr_response,
    legacy_gateway_candidate_quote_refresh_status,
)
from storage.sqlite import initialize_database, open_connection
from tests.test_strategy_service import _insert_strategy_fixture


def test_candidate_quote_refresh_side_effect_enqueues_each_code(tmp_path) -> None:
    connection = initialize_database(tmp_path / "tr-side-effects.sqlite3")
    first_candidate = _insert_strategy_fixture(connection, code="005930", name="삼성전자")
    second_candidate = _insert_strategy_fixture(
        connection,
        candidate_id="CAND-2026-06-27-000660-1",
        code="000660",
        name="SK하이닉스",
    )
    event = _candidate_quote_refresh_tr_response(
        "evt_tr_side_effect_multi",
        ("005930", "000660"),
    )

    result = enqueue_incremental_for_candidate_quote_refresh_tr_response(
        connection,
        event,
        settings=Settings(),
        source="test_side_effect",
    )

    rows = connection.execute(
        """
        SELECT candidate_instance_id, code, reason, source_event_id, priority
        FROM incremental_evaluation_queue
        ORDER BY code
        """
    ).fetchall()
    connection.close()

    assert result.status == "ENQUEUED"
    assert result.code_count == 2
    assert result.enqueued_count == 2
    assert result.error_count == 0
    assert result.candidate_ids == (first_candidate, second_candidate)
    assert [row["code"] for row in rows] == ["000660", "005930"]
    assert {row["reason"] for row in rows} == {"CANDIDATE_QUOTE_REFRESH"}
    assert {row["source_event_id"] for row in rows} == {event.event_id}
    assert {row["priority"] for row in rows} == {90}


def test_candidate_quote_refresh_side_effect_skips_empty_codes(tmp_path) -> None:
    connection = initialize_database(tmp_path / "tr-side-effects-empty.sqlite3")
    event = _non_candidate_tr_response("evt_tr_side_effect_empty")

    result = enqueue_incremental_for_candidate_quote_refresh_tr_response(
        connection,
        event,
        settings=Settings(),
        source="test_side_effect",
    )

    queued_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "SKIPPED"
    assert result.code_count == 0
    assert result.enqueued_count == 0
    assert result.reason_codes == ("CANDIDATE_QUOTE_REFRESH_CODES_EMPTY",)
    assert queued_count == 0


def test_candidate_quote_refresh_side_effect_reports_partial_errors(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "tr-side-effects-partial.sqlite3")
    _insert_strategy_fixture(connection, code="005930", name="삼성전자")
    event = _candidate_quote_refresh_tr_response(
        "evt_tr_side_effect_partial",
        ("005930", "000660"),
    )
    original = side_effects.enqueue_incremental_evaluation_for_code

    def flaky_enqueue(connection, code, **kwargs):  # noqa: ANN001, ANN002
        if code == "000660":
            raise RuntimeError("forced enqueue failure")
        return original(connection, code, **kwargs)

    monkeypatch.setattr(
        side_effects,
        "enqueue_incremental_evaluation_for_code",
        flaky_enqueue,
    )

    result = enqueue_incremental_for_candidate_quote_refresh_tr_response(
        connection,
        event,
        settings=Settings(),
        source="test_side_effect",
    )

    queued_count = connection.execute(
        "SELECT COUNT(*) AS count FROM incremental_evaluation_queue"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "COMPLETED_WITH_ERRORS"
    assert result.code_count == 2
    assert result.enqueued_count == 1
    assert result.error_count == 1
    assert queued_count == 1
    assert result.errors[0]["code"] == "000660"
    assert legacy_gateway_candidate_quote_refresh_status(result) == "ENQUEUED"


def test_gateway_inline_tr_response_behavior_is_unchanged(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "tr-side-effects-gateway.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "test-token")
    monkeypatch.setenv("INCREMENTAL_EVALUATION_WORKER_ENABLED", "false")
    clear_settings_cache()
    connection = initialize_database(db_path)
    candidate_id = _insert_strategy_fixture(connection, code="005930", name="삼성전자")
    connection.close()
    event = _candidate_quote_refresh_tr_response(
        "evt_tr_side_effect_gateway",
        ("005930",),
    )

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/gateway/events",
                json=event.to_dict(),
                headers={"X-Local-Token": "test-token"},
            )
    finally:
        clear_settings_cache()

    connection = open_connection(db_path)
    queue = connection.execute("SELECT * FROM incremental_evaluation_queue").fetchone()
    snapshot_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_tr_snapshots
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()["count"]
    connection.close()

    assert response.status_code == 200
    statuses = response.json()["projection_statuses"]
    assert statuses["market_data"] == "APPLIED"
    assert statuses["incremental_evaluation"] == "ENQUEUED"
    assert queue["candidate_instance_id"] == candidate_id
    assert queue["reason"] == "CANDIDATE_QUOTE_REFRESH"
    assert snapshot_count == 1


def _candidate_quote_refresh_tr_response(
    event_id: str,
    codes: tuple[str, ...],
) -> GatewayEvent:
    rows = [
        {
            "종목코드": f"A{code}",
            "종목명": f"종목{code}",
            "현재가": "+70000",
            "등락율": "+0.10",
            "거래량": "1000",
            "거래대금": "70000000",
            "고가": "+70100",
            "저가": "+69900",
        }
        for code in codes
    ]
    event = make_tr_response_event(
        request_id=f"candidate_quote_refresh:2026-06-27:{event_id}",
        tr_code="OPT10001",
        request_name="candidate_quote_refresh_opt10001",
        rows=rows,
        source="test-gateway",
    )
    return GatewayEvent(
        event_id=event_id,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        ts=event.ts,
    )


def _non_candidate_tr_response(event_id: str) -> GatewayEvent:
    event = make_tr_response_event(
        request_id=f"unrelated:{event_id}",
        tr_code="OPT10001",
        request_name="not_candidate_quote_refresh",
        rows=[
            {
                "종목코드": "A005930",
                "종목명": "삼성전자",
                "현재가": "+70000",
            }
        ],
        source="test-gateway",
    )
    return GatewayEvent(
        event_id=event_id,
        event_type=event.event_type,
        source=event.source,
        payload=event.payload,
        ts=event.ts,
    )
