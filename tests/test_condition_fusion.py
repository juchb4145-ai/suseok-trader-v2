from __future__ import annotations

from apps.core_api import app
from domain.broker.utils import utc_now
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event, make_price_tick_event
from services.candidate_service import (
    get_candidate,
    ingest_condition_sources,
    list_candidates,
    refresh_candidate_context,
)
from services.condition_fusion import (
    list_condition_fusion,
    rebuild_condition_fusion,
    rebuild_condition_fusion_for_code,
)
from services.config import Settings, candidate_timezone
from services.market_data_service import process_gateway_event
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database


def test_condition_fusion_scores_discovery_low_and_leader_pullback_high(tmp_path) -> None:
    connection = initialize_database(tmp_path / "condition-fusion.sqlite3")
    settings = _settings()
    trade_date = _trade_date(settings)
    _append_and_project(
        connection,
        _condition_event("005930", "Discovery", "DISCOVERY", priority=10),
        settings,
    )
    _append_and_project(
        connection,
        _condition_event("000660", "Leader", "LEADER", priority=500),
        settings,
    )
    _append_and_project(
        connection,
        _condition_event("000660", "Pullback", "PULLBACK", priority=450),
        settings,
    )

    result = rebuild_condition_fusion(connection, trade_date, settings=settings)
    rows = {row["code"]: row for row in list_condition_fusion(connection, settings=settings)}
    connection.close()

    assert result.processed_event_count == 3
    assert rows["005930"]["active_roles"] == ["DISCOVERY"]
    assert rows["005930"]["priority_score"] <= 25
    assert "DISCOVERY_OBSERVATION_ONLY" in rows["005930"]["reason_codes"]
    assert rows["000660"]["active_roles"] == ["LEADER", "PULLBACK"]
    assert rows["000660"]["priority_score"] > rows["005930"]["priority_score"]
    assert "LEADER_PULLBACK_FUSION_PRIORITY" in rows["000660"]["reason_codes"]


def test_condition_fusion_rebuild_for_code_updates_only_target_code(tmp_path) -> None:
    connection = initialize_database(tmp_path / "condition-fusion-code.sqlite3")
    settings = _settings()
    trade_date = _trade_date(settings)
    _append_and_project(
        connection,
        _condition_event("005930", "Leader", "LEADER", priority=90),
        settings,
    )
    _append_and_project(
        connection,
        _condition_event("000660", "Pullback", "PULLBACK", priority=95),
        settings,
    )

    result = rebuild_condition_fusion_for_code(
        connection,
        "005930",
        trade_date,
        settings=settings,
    )
    rows = {row["code"]: row for row in list_condition_fusion(connection, settings=settings)}
    connection.close()

    assert result.processed_event_count == 1
    assert result.fused_code_count == 1
    assert set(rows) == {"005930"}
    assert rows["005930"]["active_roles"] == ["LEADER"]


def test_gateway_condition_event_refreshes_condition_fusion_incrementally(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "condition-fusion-api.sqlite3"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    event = _condition_event("005930", "Leader", "LEADER", priority=90)

    with TestClient(app) as client:
        response = client.post(
            "/api/gateway/events",
            json=event.to_dict(),
            headers={"X-Local-Token": "test-token"},
        )

    connection = initialize_database(db_path)
    rows = {row["code"]: row for row in list_condition_fusion(connection, settings=_settings())}
    connection.close()

    assert response.status_code == 200
    assert response.json()["projection_statuses"]["market_data"] == "APPLIED"
    assert response.json()["projection_statuses"]["condition_fusion"] == "APPLIED"
    assert rows["005930"]["active_roles"] == ["LEADER"]


def test_condition_fusion_marks_risk_blocked_and_candidate_source_types(tmp_path) -> None:
    connection = initialize_database(tmp_path / "condition-fusion-candidate.sqlite3")
    settings = _settings()
    trade_date = _trade_date(settings)
    _append_and_project(
        connection,
        _condition_event("005930", "Leader", "LEADER", priority=500),
        settings,
    )
    _append_and_project(
        connection,
        _condition_event("005930", "RiskBlock", "RISK_BLOCK", priority=0),
        settings,
    )
    _append_and_project(connection, make_price_tick_event(code="005930"), settings)

    ingest_result = ingest_condition_sources(connection, trade_date, settings=settings)
    candidates = list_candidates(connection, trade_date=trade_date, active_only=True)
    candidate = candidates[0]
    refresh_candidate_context(connection, candidate["candidate_instance_id"], settings=settings)
    refreshed = list_candidates(connection, trade_date=trade_date, active_only=True)[0]
    fusion = list_condition_fusion(connection, settings=settings)[0]
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    latest_source = connection.execute(
        """
        SELECT source_type, payload_json
        FROM candidate_sources_latest
        WHERE code = '005930'
        """
    ).fetchone()
    connection.close()

    assert ingest_result.source_event_count == 1
    assert fusion["risk_blocked"] is True
    assert "RISK_BLOCKED_BY_CONDITION" in fusion["reason_codes"]
    assert latest_source["source_type"] == "CONDITION_RISK_BLOCK"
    assert "CONDITION_RISK_BLOCKED" in refreshed["reason_codes"]
    assert refreshed["state"] == "BLOCKED_OBSERVATION"
    assert command_count == 0


def test_discovery_only_condition_fusion_blocks_strategy_promotion(tmp_path) -> None:
    connection = initialize_database(tmp_path / "condition-discovery-only.sqlite3")
    settings = _settings()
    trade_date = _trade_date(settings)
    _append_and_project(
        connection,
        _condition_event("005930", "Discovery", "DISCOVERY", priority=10),
        settings,
    )
    _append_and_project(connection, make_price_tick_event(code="005930"), settings)

    ingest_condition_sources(connection, trade_date, settings=settings)
    candidate = list_candidates(connection, trade_date=trade_date, active_only=True)[0]
    refresh_candidate_context(connection, candidate["candidate_instance_id"], settings=settings)
    refreshed = list_candidates(connection, trade_date=trade_date, active_only=True)[0]
    connection.close()

    assert refreshed["primary_source_type"] == "CONDITION_DISCOVERY"
    assert "DISCOVERY_OBSERVATION_ONLY" in refreshed["reason_codes"]
    assert refreshed["state"] == "BLOCKED_OBSERVATION"


def test_candidate_context_includes_condition_fusion_fields(tmp_path) -> None:
    connection = initialize_database(tmp_path / "condition-fusion-context.sqlite3")
    settings = _settings()
    trade_date = _trade_date(settings)
    _append_and_project(
        connection,
        _condition_event("005930", "Leader", "LEADER", priority=90),
        settings,
    )
    _append_and_project(
        connection,
        _condition_event("005930", "Pullback", "PULLBACK", priority=95),
        settings,
    )
    _append_and_project(connection, make_price_tick_event(code="005930"), settings)

    ingest_condition_sources(connection, trade_date, settings=settings)
    candidate = list_candidates(connection, trade_date=trade_date, active_only=True)[0]
    refresh_candidate_context(connection, candidate["candidate_instance_id"], settings=settings)
    refreshed = get_candidate(
        connection,
        candidate["candidate_instance_id"],
        include_context=True,
    )
    connection.close()

    source_context = refreshed["context"]["source_context"]
    assert source_context["condition_fusion"]["present"] is True
    assert source_context["condition_fusion_priority_score"] > 0
    assert source_context["active_condition_roles"] == ["LEADER", "PULLBACK"]
    assert source_context["condition_risk_blocked"] is False
    assert "CONDITION_FUSION_PRIORITY_READY" in source_context["condition_fusion_reason_codes"]
    assert source_context["condition_names"] == ["Leader", "Pullback"]
    assert source_context["condition_latest_hit_at"]


def _condition_event(
    code: str,
    name: str,
    role: str,
    *,
    priority: int,
):
    return make_condition_event(
        condition_id=f"cond-{name}",
        condition_name=name,
        code=code,
        name=code,
        action="ENTER",
        metadata={
            "sensor_evidence": True,
            "not_buy_signal": True,
            "condition_profile_id": f"profile-{name}",
            "condition_role": role,
            "condition_profile": {
                "profile_id": f"profile-{name}",
                "condition_name": name,
                "condition_index": priority,
                "role": role,
                "priority": priority,
                "ttl_sec": 999_999_999,
                "enabled": True,
                "price_subscribe_policy": "immediate",
            },
            "condition_admission": {
                "subscribed": role != "RISK_BLOCK",
                "reason_codes": ["TEST"],
            },
        },
    )


def _settings() -> Settings:
    return Settings(
        market_data_tick_stale_sec=999_999_999,
        market_data_degraded_tick_stale_sec=999_999_999,
        candidate_source_stale_sec=999_999_999,
        candidate_tick_stale_sec=999_999_999,
        candidate_episode_ttl_sec=999_999_999,
    )


def _trade_date(settings: Settings) -> str:
    return (
        utc_now()
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _append_and_project(connection, event, settings: Settings) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_gateway_event(connection, event, settings=settings)
    assert result.status == "APPLIED"
