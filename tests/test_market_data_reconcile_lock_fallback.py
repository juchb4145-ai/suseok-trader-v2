from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from apps.core_api import app
from fastapi.testclient import TestClient
from storage.sqlite import initialize_database
from tests.test_operator_sqlite_lock_handling import _set_env


def test_market_data_reconcile_live_safe_defaults_to_read_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "reconcile-live-safe-default.sqlite3"
    initialize_database(db_path).close()
    _set_env(monkeypatch, db_path)
    calls: list[bool] = []

    def fake_reconcile(*args, **kwargs):
        calls.append(bool(kwargs["persist"]))
        return _FakeReconcileResult()

    monkeypatch.setattr(
        "api.routes.operator.run_market_data_projection_reconcile",
        fake_reconcile,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/operator/market-data-projection-reconcile/run-once?"
            "limit=10&live_safe=true",
            headers={"X-Local-Token": "secret-token"},
        )

    assert response.status_code == 200
    assert calls == [False]
    payload = response.json()
    assert payload["status"] == "PASS"
    assert payload["persist_requested"] is False
    assert payload["locked_fallback_used"] is False
    assert payload["read_only_projection"] is True


@dataclass(frozen=True)
class _FakeReconcileResult:
    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": "reconcile_live_safe_default",
            "status": "PASS",
            "checked_event_count": 0,
            "reason_codes": [],
            "issues": [],
            "no_trading_side_effects": True,
        }
