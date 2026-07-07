from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path("storage/suseok-trader-v2.sqlite3")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Monitor LIVE_SIM order lifecycle from local SQLite in read-only mode.",
    )
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--since", default="", help="UTC ISO timestamp lower bound.")
    parser.add_argument("--interval-sec", type=float, default=20.0)
    parser.add_argument("--duration-sec", type=float, default=0.0, help="0 means run forever.")
    parser.add_argument("--log", required=True, help="JSONL output path.")
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    log_path = Path(args.log).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    since = args.since.strip() or utc_now_wire()
    interval_sec = max(float(args.interval_sec), 1.0)
    duration_sec = max(float(args.duration_sec), 0.0)
    started = time.monotonic()

    with log_path.open("a", encoding="utf-8") as handle:
        write_event(
            handle,
            {
                "event": "monitor_started",
                "db": str(db_path),
                "since": since,
                "interval_sec": interval_sec,
                "duration_sec": duration_sec,
            },
        )
        seen: dict[str, set[str]] = {
            "plan": set(),
            "command": set(),
            "order": set(),
            "command_event": set(),
            "gateway_event": set(),
            "execution": set(),
            "position": set(),
            "exit_signal": set(),
            "exit_intent": set(),
            "run": set(),
        }
        while True:
            snapshot = read_snapshot(db_path, since)
            emit_changes(handle, snapshot, seen)
            handle.flush()
            if duration_sec and time.monotonic() - started >= duration_sec:
                write_event(handle, {"event": "monitor_finished", "since": since})
                return 0
            time.sleep(interval_sec)


def emit_changes(handle, snapshot: dict[str, Any], seen: dict[str, set[str]]) -> None:
    summary = {
        "event": "snapshot",
        "now": snapshot["now"],
        "latest_run": snapshot.get("latest_run"),
        "counts": snapshot["counts"],
    }
    write_event(handle, summary)
    emit_rows(handle, "plan", snapshot["plans"], "order_plan_id", seen)
    emit_rows(handle, "command", snapshot["commands"], "command_id", seen)
    emit_rows(handle, "order", snapshot["orders"], "live_sim_order_id", seen)
    emit_rows(handle, "command_event", snapshot["command_events"], "id", seen)
    emit_rows(handle, "gateway_event", snapshot["gateway_events"], "event_id", seen)
    emit_rows(handle, "execution", snapshot["executions"], "live_sim_execution_id", seen)
    emit_rows(handle, "position", snapshot["positions"], "position_id", seen)
    emit_rows(handle, "exit_signal", snapshot["exit_signals"], "exit_signal_id", seen)
    emit_rows(handle, "exit_intent", snapshot["exit_intents"], "exit_intent_id", seen)


def emit_rows(
    handle,
    event_name: str,
    rows: list[dict[str, Any]],
    key: str,
    seen: dict[str, set[str]],
) -> None:
    for row in rows:
        row_id = str(row.get(key) or "")
        if not row_id or row_id in seen[event_name]:
            continue
        seen[event_name].add(row_id)
        write_event(handle, {"event": event_name, "row": row})


def read_snapshot(db_path: Path, since: str) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        commands = rows(
            connection,
            """
            SELECT command_id, command_type, source, status, created_at, dispatched_at,
                   completed_at, expires_at, attempts, last_error
            FROM gateway_commands
            WHERE command_type IN ('send_order', 'cancel_order')
              AND created_at >= ?
            ORDER BY created_at DESC
            """,
            (since,),
        )
        command_ids = [row["command_id"] for row in commands]
        return {
            "now": scalar_now(connection),
            "latest_run": latest_run(connection),
            "counts": counts(connection, since),
            "plans": rows(
                connection,
                """
                SELECT order_plan_id, code, name, status, suggested_quantity,
                       suggested_notional, max_notional, created_at, expires_at,
                       reason_codes_json
                FROM order_plan_drafts
                WHERE created_at >= ?
                ORDER BY created_at DESC
                LIMIT 50
                """,
                (since,),
            ),
            "commands": commands,
            "orders": rows(
                connection,
                """
                SELECT live_sim_order_id, code, name, side, quantity, status,
                       gateway_command_id, broker_order_no, command_queued_at,
                       command_dispatched_at, broker_acked_at, filled_quantity,
                       remaining_quantity, created_at, last_event_at
                FROM live_sim_orders
                WHERE created_at >= ?
                ORDER BY created_at DESC
                """,
                (since,),
            ),
            "command_events": rows_for_ids(
                connection,
                """
                SELECT id, command_id, event_type, status, created_at, payload_json
                FROM gateway_command_events
                WHERE command_id IN ({placeholders})
                ORDER BY created_at DESC, id DESC
                """,
                command_ids,
            ),
            "gateway_events": rows_for_ids(
                connection,
                """
                SELECT event_id, event_type, command_id, event_ts, received_at,
                       status, error_message, payload_json
                FROM gateway_events
                WHERE command_id IN ({placeholders})
                ORDER BY received_at DESC, event_id DESC
                """,
                command_ids,
            ),
            "executions": rows(
                connection,
                """
                SELECT live_sim_execution_id, live_sim_order_id, broker_order_no,
                       code, side, quantity, price, notional, executed_at
                FROM live_sim_executions
                WHERE executed_at >= ?
                ORDER BY executed_at DESC
                """,
                (since,),
            ),
            "positions": rows(
                connection,
                """
                SELECT position_id, code, name, side, quantity, available_quantity,
                       avg_entry_price, status, opened_at, closed_at, updated_at,
                       source_live_sim_order_id
                FROM live_sim_positions
                WHERE opened_at >= ? OR closed_at >= ? OR updated_at >= ?
                ORDER BY updated_at DESC
                """,
                (since, since, since),
            ),
            "exit_signals": rows(
                connection,
                """
                SELECT exit_signal_id, position_id, code, reason, quantity, status,
                       created_at
                FROM live_sim_exit_signals
                WHERE created_at >= ?
                ORDER BY created_at DESC
                """,
                (since,),
            ),
            "exit_intents": rows(
                connection,
                """
                SELECT exit_intent_id, position_id, exit_signal_id, live_sim_order_id,
                       gateway_command_id, code, quantity, reason, status, created_at
                FROM live_sim_exit_intents
                WHERE created_at >= ?
                ORDER BY created_at DESC
                """,
                (since,),
            ),
        }
    finally:
        connection.close()


def latest_run(connection: sqlite3.Connection) -> dict[str, Any] | None:
    result = rows(
        connection,
        """
        SELECT run_id, created_at, status, preflight_status, buy_evaluated_count,
               buy_command_count, exit_signal_count, exit_command_count,
               reconcile_status, no_buy_status, errors_json
        FROM live_sim_operating_runs
        ORDER BY created_at DESC
        LIMIT 1
        """,
    )
    return result[0] if result else None


def counts(connection: sqlite3.Connection, since: str) -> dict[str, int]:
    return {
        "plan_ready": count(
            connection,
            """
            SELECT COUNT(*) FROM order_plan_drafts
            WHERE created_at >= ? AND status = 'PLAN_READY'
            """,
            (since,),
        ),
        "send_order": count(
            connection,
            """
            SELECT COUNT(*) FROM gateway_commands
            WHERE created_at >= ? AND command_type = 'send_order'
            """,
            (since,),
        ),
        "cancel_order": count(
            connection,
            """
            SELECT COUNT(*) FROM gateway_commands
            WHERE created_at >= ? AND command_type = 'cancel_order'
            """,
            (since,),
        ),
        "orders": count(
            connection,
            "SELECT COUNT(*) FROM live_sim_orders WHERE created_at >= ?",
            (since,),
        ),
        "executions": count(
            connection,
            "SELECT COUNT(*) FROM live_sim_executions WHERE executed_at >= ?",
            (since,),
        ),
        "positions": count(
            connection,
            """
            SELECT COUNT(*) FROM live_sim_positions
            WHERE opened_at >= ? OR closed_at >= ? OR updated_at >= ?
            """,
            (since, since, since),
        ),
        "exit_signals": count(
            connection,
            "SELECT COUNT(*) FROM live_sim_exit_signals WHERE created_at >= ?",
            (since,),
        ),
        "exit_intents": count(
            connection,
            "SELECT COUNT(*) FROM live_sim_exit_intents WHERE created_at >= ?",
            (since,),
        ),
    }


def rows(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(sql, params).fetchall()]


def rows_for_ids(
    connection: sqlite3.Connection,
    sql_template: str,
    ids: list[str],
) -> list[dict[str, Any]]:
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    return rows(connection, sql_template.format(placeholders=placeholders), tuple(ids))


def count(connection: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> int:
    row = connection.execute(sql, params).fetchone()
    return int(row[0] or 0)


def scalar_now(connection: sqlite3.Connection) -> str:
    row = connection.execute("SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')").fetchone()
    return str(row[0])


def write_event(handle, payload: dict[str, Any]) -> None:
    payload = {"ts": utc_now_wire(), **payload}
    handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def utc_now_wire() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


if __name__ == "__main__":
    raise SystemExit(main())
