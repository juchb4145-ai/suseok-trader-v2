from __future__ import annotations

import json

from domain.broker.utils import datetime_to_wire, utc_now
from domain.candidate.state import CandidateState
from services.candidate_quote_refresh import run_candidate_quote_refresh_once
from services.config import Settings
from storage.sqlite import initialize_database


def test_candidate_quote_refresh_queues_request_tr_without_order_commands(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate-quote-refresh.sqlite3")
    now = datetime_to_wire(utc_now())
    _insert_candidate(
        connection,
        candidate_id="CAND-2026-06-30-005930-1",
        code="005930",
        name="삼성전자",
        trade_date="2026-06-30",
        state=CandidateState.CONTEXT_READY.value,
        now=now,
    )

    result = run_candidate_quote_refresh_once(
        connection,
        trade_date="2026-06-30",
        settings=Settings(entry_timing_stale_max_seconds=60),
        queue_commands=True,
    )
    rows = connection.execute(
        "SELECT command_type, source, payload_json FROM gateway_commands"
    ).fetchall()
    order_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_commands
        WHERE command_type IN ('send_order', 'cancel_order', 'modify_order')
        """
    ).fetchone()["count"]
    connection.close()

    assert result.status == "QUEUED"
    assert result.command_count == 1
    assert order_count == 0
    assert [row["command_type"] for row in rows] == ["request_tr"]
    payload = json.loads(rows[0]["payload_json"])
    assert payload["tr_code"] == "OPT10001"
    assert payload["params"]["종목코드"] == "005930"
    assert payload["metadata"]["source"] == "candidate_quote_refresh"
    assert payload["observe_only"] is True
    assert payload["no_order_side_effects"] is True


def test_candidate_quote_refresh_skips_fresh_candidate_tick(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate-quote-refresh-fresh.sqlite3")
    now = datetime_to_wire(utc_now())
    _insert_candidate(
        connection,
        candidate_id="CAND-2026-06-30-005930-1",
        code="005930",
        name="삼성전자",
        trade_date="2026-06-30",
        state=CandidateState.CONTEXT_READY.value,
        now=now,
    )
    _insert_latest_tick(connection, code="005930", name="삼성전자", now=now)

    result = run_candidate_quote_refresh_once(
        connection,
        trade_date="2026-06-30",
        settings=Settings(entry_timing_stale_max_seconds=60),
        queue_commands=True,
    )
    command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    connection.close()

    assert result.status == "NOOP"
    assert command_count == 0


def _insert_candidate(
    connection,
    *,
    candidate_id: str,
    code: str,
    name: str,
    trade_date: str,
    state: str,
    now: str,
) -> None:
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            previous_state,
            detected_at,
            last_seen_at,
            state_updated_at,
            closed_at,
            primary_source_type,
            primary_source_id,
            source_count,
            active_source_count,
            market_readiness_status,
            tick_age_sec,
            vwap_ready,
            bar_1m_ready,
            bar_3m_ready,
            bar_5m_ready,
            reason_codes_json,
            metadata_json
        )
        VALUES (?, ?, ?, ?, 1, ?, NULL, ?, ?, ?, NULL, 'THEME_LEADER', 'theme-1',
            1, 1, 'MISSING', NULL, 0, 0, 0, 0, '[]', '{}')
        """,
        (candidate_id, trade_date, code, name, state, now, now, now),
    )
    connection.commit()


def _insert_latest_tick(connection, *, code: str, name: str, now: str) -> None:
    connection.execute(
        """
        INSERT INTO market_ticks_latest (
            code,
            exchange,
            session,
            name,
            price,
            change_rate,
            cumulative_volume,
            cumulative_trade_value,
            execution_strength,
            best_bid,
            best_ask,
            spread_ticks,
            day_high,
            day_low,
            trade_time,
            event_ts,
            received_at,
            source,
            event_id,
            quality_status,
            updated_at
        )
        VALUES (?, 'KRX', 'REGULAR', ?, 70000, 1.0, 1000, 70000000, 100.0,
            69900, 70000, 1, 71000, 69000, ?, ?, ?, 'test', 'evt-tick', 'FRESH', ?)
        """,
        (code, name, now, now, now, now),
    )
    connection.commit()
