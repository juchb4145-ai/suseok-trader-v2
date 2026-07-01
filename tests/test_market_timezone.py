from __future__ import annotations

from datetime import UTC, datetime

import domain.broker.utils as broker_utils
from services.config import Settings
from services.live_sim.live_sim_service import _is_eod_flatten_time, _today_trade_date
from services.live_sim.safety_gate import _daily_live_sim_order_count
from storage.sqlite import initialize_database


def _freeze_utc(monkeypatch, value: datetime) -> None:
    monkeypatch.setattr(broker_utils, "utc_now", lambda: value)


def test_market_now_converts_utc_to_kst(monkeypatch) -> None:
    _freeze_utc(monkeypatch, datetime(2026, 6, 30, 23, 30, 0, tzinfo=UTC))
    assert broker_utils.market_today() == "2026-07-01"
    assert broker_utils.market_time_str() == "08:30:00"


def test_eod_flatten_fires_during_kst_session(monkeypatch) -> None:
    settings = Settings()
    # 06:15 UTC == 15:15 KST: the intended end-of-day flatten moment.
    _freeze_utc(monkeypatch, datetime(2026, 7, 1, 6, 15, 0, tzinfo=UTC))
    assert _is_eod_flatten_time(settings) is True
    # 06:14:59 UTC == 15:14:59 KST: one second before the cutoff.
    _freeze_utc(monkeypatch, datetime(2026, 7, 1, 6, 14, 59, tzinfo=UTC))
    assert _is_eod_flatten_time(settings) is False


def test_eod_flatten_does_not_fire_at_kst_midnight(monkeypatch) -> None:
    settings = Settings()
    # 15:15 UTC == 00:15 KST next day; the old UTC comparison fired here.
    _freeze_utc(monkeypatch, datetime(2026, 7, 1, 15, 15, 0, tzinfo=UTC))
    assert _is_eod_flatten_time(settings) is False


def test_today_trade_date_uses_kst_calendar(monkeypatch) -> None:
    _freeze_utc(monkeypatch, datetime(2026, 6, 30, 22, 0, 0, tzinfo=UTC))
    assert _today_trade_date() == "2026-07-01"
    _freeze_utc(monkeypatch, datetime(2026, 7, 1, 0, 30, 0, tzinfo=UTC))
    assert _today_trade_date() == "2026-07-01"


def test_daily_order_count_uses_kst_trade_date(tmp_path, monkeypatch) -> None:
    connection = initialize_database(tmp_path / "safety-gate-kst.sqlite3")
    try:
        # 23:00 UTC on 2026-06-30 is already 2026-07-01 in KST.
        _freeze_utc(monkeypatch, datetime(2026, 6, 30, 23, 0, 0, tzinfo=UTC))
        connection.execute(
            """
            INSERT INTO live_sim_orders (
                live_sim_order_id, live_sim_intent_id, trade_date, account_id, code,
                name, side, order_type, quantity, notional, status, idempotency_key,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "LSO-kst-1",
                "LSI-kst-1",
                broker_utils.market_today(),
                "ACC-1",
                "005930",
                "Samsung",
                "BUY",
                "LIMIT",
                1,
                70000.0,
                "QUEUED",
                "idem-kst-1",
                broker_utils.timestamp(),
            ),
        )
        connection.commit()
        assert _daily_live_sim_order_count(connection) == 1
        # Next KST day: the same row must no longer count as today's order.
        _freeze_utc(monkeypatch, datetime(2026, 7, 1, 23, 0, 0, tzinfo=UTC))
        assert _daily_live_sim_order_count(connection) == 0
    finally:
        connection.close()
