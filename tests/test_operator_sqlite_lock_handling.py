from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from apps.core_api import app
from fastapi.testclient import TestClient
from storage.sqlite import initialize_database


def test_projection_outbox_run_once_returns_locked_retryable(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "operator-locked-outbox.sqlite3"
    initialize_database(db_path).close()
    _set_env(monkeypatch, db_path)

    def raise_locked(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr("api.routes.operator.process_projection_outbox_batch", raise_locked)

    with TestClient(app) as client:
        response = client.post(
            "/api/operator/projection-outbox/run-once?limit=500&apply_projection=true",
            headers={"X-Local-Token": "secret-token"},
        )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["status"] == "LOCKED_RETRYABLE"
    assert detail["retryable"] is True
    assert detail["reason_codes"] == ["SQLITE_DATABASE_LOCKED"]
    assert detail["no_trading_side_effects"] is True
    assert detail["locked_retry_count"] == 2


def test_market_data_reconcile_run_once_falls_back_to_read_only_on_lock(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "operator-locked-reconcile.sqlite3"
    initialize_database(db_path).close()
    _set_env(monkeypatch, db_path)
    calls: list[bool] = []

    def fake_reconcile(*args, **kwargs):
        persist = bool(kwargs["persist"])
        calls.append(persist)
        if persist:
            raise sqlite3.OperationalError("database is locked")
        return _FakeReconcileResult()

    monkeypatch.setattr(
        "api.routes.operator.run_market_data_projection_reconcile",
        fake_reconcile,
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/operator/market-data-projection-reconcile/run-once?"
            "limit=10&persist=true&live_safe=true",
            headers={"X-Local-Token": "secret-token"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert calls == [True, True, True, False]
    assert payload["status"] == "WARN_READ_ONLY_RESULT"
    assert payload["persisted"] is False
    assert payload["persist_requested"] is True
    assert payload["locked_fallback_used"] is True
    assert payload["retryable"] is True
    assert "SQLITE_DATABASE_LOCKED" in payload["reason_codes"]
    assert payload["read_only_projection"] is True
    assert payload["no_trading_side_effects"] is True


@dataclass(frozen=True)
class _FakeReconcileResult:
    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": "reconcile_test",
            "status": "PASS",
            "checked_event_count": 0,
            "reason_codes": [],
            "issues": [],
            "no_trading_side_effects": True,
        }


def _set_env(monkeypatch, db_path) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("TRADING_PROFILE", "OBSERVE")
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("LIVE_SIM_ENABLED", "false")
    monkeypatch.setenv("LIVE_SIM_ORDER_ROUTING_ENABLED", "false")
    monkeypatch.setenv("LIVE_SIM_GATEWAY_COMMAND_ENABLED", "false")
    monkeypatch.setenv("OPERATOR_SQLITE_LOCK_RETRY_ATTEMPTS", "3")
    monkeypatch.setenv("OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC", "0")
    monkeypatch.setenv("OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC", "0")
