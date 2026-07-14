from __future__ import annotations

from api.routes import dashboard as dashboard_route
from apps.core_api import app
from fastapi.testclient import TestClient


def test_dashboard_snapshot_sections_api_returns_only_requested_fast_sections(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "dashboard-sections.sqlite3"))
    dashboard_route._SUMMARY_CACHE.clear()

    with TestClient(app) as client:
        response = client.get(
            "/api/dashboard/snapshot",
            params={
                "fast": "true",
                "sections": "gateway,projection_outbox",
                "limit": "5",
            },
            headers={"X-Local-Token": "test-token"},
        )

    assert response.status_code == 200
    snapshot = response.json()
    assert snapshot["fast_path"] is True
    assert snapshot["detail"] == "summary"
    assert snapshot["limit"] == 5
    assert set(snapshot["requested_sections"]) == {"gateway", "projection_outbox"}
    assert set(snapshot["included_sections"]) == {"gateway", "projection_outbox"}
    assert "gateway" in snapshot
    assert "projection_outbox" in snapshot
    assert "themes" not in snapshot
    assert "candidates" not in snapshot
    assert "ai_explanations" not in snapshot
    assert "no_buy_sentinel" not in snapshot
    assert "live_sim" not in snapshot


def test_dashboard_snapshot_sections_api_warns_for_unknown_section(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "dashboard-unknown.sqlite3"))
    dashboard_route._SUMMARY_CACHE.clear()

    with TestClient(app) as client:
        response = client.get(
            "/api/dashboard/snapshot",
            params={
                "fast": "true",
                "sections": "gateway,unknown_section",
            },
            headers={"X-Local-Token": "test-token"},
        )

    assert response.status_code == 200
    snapshot = response.json()
    assert snapshot["fast_path"] is True
    assert "gateway" in snapshot
    assert "UNKNOWN_DASHBOARD_SECTION:unknown_section" in snapshot["warnings"]
    assert {
        "section": "unknown_section",
        "reason": "UNKNOWN_DASHBOARD_SECTION",
    } in snapshot["skipped_sections"]


def test_dashboard_snapshot_fast_cache_key_includes_sections(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "dashboard-cache.sqlite3"))
    dashboard_route._SUMMARY_CACHE.clear()
    calls: list[tuple[str, ...]] = []

    def fake_build(connection, settings, *, detail, limit, sections, timeout_budget_ms):
        del connection, settings, timeout_budget_ms
        normalized = tuple(sorted(sections))
        calls.append(normalized)
        return {
            "generated_at": "2026-07-09T00:00:00Z",
            "detail": detail,
            "limit": limit,
            "fast_path": True,
            "requested_sections": list(normalized),
            "included_sections": list(normalized),
            "skipped_sections": [],
            "section_latency_ms": {},
            "total_latency_ms": 1,
            "timeout_budget_ms": None,
            "warnings": [],
            normalized[0]: {"section": normalized[0]},
        }

    monkeypatch.setattr(
        dashboard_route,
        "build_dashboard_snapshot_sections",
        fake_build,
    )

    with TestClient(app) as client:
        first = client.get(
            "/api/dashboard/snapshot",
            params={"fast": "true", "sections": "gateway"},
            headers={"X-Local-Token": "test-token"},
        )
        second = client.get(
            "/api/dashboard/snapshot",
            params={"fast": "true", "sections": "projection_outbox"},
            headers={"X-Local-Token": "test-token"},
        )
        cached = client.get(
            "/api/dashboard/snapshot",
            params={"fast": "true", "sections": "gateway"},
            headers={"X-Local-Token": "test-token"},
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert cached.status_code == 200
    assert calls == [("gateway",), ("projection_outbox",)]
