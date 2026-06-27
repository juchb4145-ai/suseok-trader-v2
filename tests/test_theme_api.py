from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from gateway.event_factory import make_condition_event, make_price_tick_event


def test_theme_api_import_rebuild_and_read_paths(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRADING_CORE_TOKEN", raising=False)
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "theme_api.sqlite3"))
    monkeypatch.setenv("MARKET_DATA_TICK_STALE_SEC", "999999999")
    monkeypatch.setenv("MARKET_DATA_DEGRADED_TICK_STALE_SEC", "999999999")

    with TestClient(app) as client:
        import_response = client.post("/api/themes/import", json=_payload())
        tick_1 = client.post(
            "/api/gateway/events",
            json=make_price_tick_event(
                code="005930",
                name="삼성전자",
                change_rate=1.0,
                trade_value=100_000_000,
            ).to_dict(),
        )
        tick_2 = client.post(
            "/api/gateway/events",
            json=make_price_tick_event(
                code="000660",
                name="SK하이닉스",
                price=120000,
                change_rate=0.8,
                volume=750,
                trade_value=90_000_000,
            ).to_dict(),
        )
        condition = client.post(
            "/api/gateway/events",
            json=make_condition_event(code="005930", name="삼성전자").to_dict(),
        )
        rebuild = client.post("/api/themes/snapshots/rebuild?theme_id=semiconductor")

        status = client.get("/api/themes/status")
        themes = client.get("/api/themes")
        detail = client.get("/api/themes/semiconductor?include_members=true")
        members = client.get("/api/themes/semiconductor/members")
        by_code = client.get("/api/themes/by-code/A005930")
        latest = client.get("/api/themes/snapshots/latest")
        latest_state = client.get("/api/themes/snapshots/latest?state=LEADING")
        latest_one = client.get(
            "/api/themes/semiconductor/snapshot/latest?include_members=true"
        )
        history = client.get("/api/themes/semiconductor/snapshots")
        errors = client.get("/api/themes/projection-errors")

    assert import_response.status_code == 200
    assert import_response.json()["theme_count"] == 1
    assert tick_1.json()["projection_status"] == "APPLIED"
    assert tick_2.json()["projection_status"] == "APPLIED"
    assert condition.json()["projection_status"] == "APPLIED"
    assert rebuild.status_code == 200
    assert rebuild.json()["snapshot_count"] == 1

    assert status.status_code == 200
    assert status.json()["theme_count"] == 1
    assert status.json()["active_theme_count"] == 1
    assert status.json()["member_count"] == 2
    assert status.json()["latest_snapshot_count"] == 1
    assert status.json()["min_fresh_coverage_ratio"] == 0.3

    assert themes.status_code == 200
    assert themes.json()["themes"][0]["theme_id"] == "semiconductor"
    assert detail.status_code == 200
    assert len(detail.json()["members"]) == 2
    assert members.status_code == 200
    assert members.json()["members"][0]["code"] in {"005930", "000660"}
    assert by_code.status_code == 200
    assert by_code.json()["themes"][0]["theme_id"] == "semiconductor"
    assert latest.status_code == 200
    assert latest.json()["snapshots"][0]["state"] == "LEADING"
    assert latest_state.status_code == 200
    assert latest_state.json()["snapshots"][0]["theme_id"] == "semiconductor"
    assert latest_one.status_code == 200
    assert len(latest_one.json()["snapshot"]["members"]) == 2
    assert history.status_code == 200
    assert history.json()["snapshots"][0]["theme_id"] == "semiconductor"
    assert errors.status_code == 200
    assert errors.json()["errors"] == []


def test_theme_api_post_endpoints_require_token_when_configured(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_CORE_TOKEN", "local-secret")
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "theme_token.sqlite3"))

    with TestClient(app) as client:
        missing_token = client.post("/api/themes/import", json=_payload())
        with_token = client.post(
            "/api/themes/import",
            json=_payload(),
            headers={"X-Core-Token": "local-secret"},
        )
        rebuild_missing_token = client.post("/api/themes/snapshots/rebuild")
        rebuild_with_token = client.post(
            "/api/themes/snapshots/rebuild",
            headers={"X-Local-Token": "local-secret"},
        )

    assert missing_token.status_code == 401
    assert with_token.status_code == 200
    assert rebuild_missing_token.status_code == 401
    assert rebuild_with_token.status_code == 200
    assert rebuild_with_token.json()["processed_theme_count"] == 1


def _payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "api_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [
                    {"code": "005930", "name": "삼성전자"},
                    {"code": "000660", "name": "SK하이닉스"},
                ],
            }
        ],
    }
