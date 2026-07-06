# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domain.broker.utils import datetime_to_wire, new_message_id, require_non_empty_str, utc_now
from domain.live_sim.status import LiveSimIntentStatus, LiveSimOrderStatus
from services.config import Settings, TradingMode, load_settings
from services.live_sim.live_sim_service import reconcile_live_sim
from storage.sqlite import open_connection

OPERATOR_BROKER_ABSENT_REASON = "OPERATOR_CONFIRMED_BROKER_ABSENT"
OPERATOR_BROKER_CANCELLED_REASON = "OPERATOR_CONFIRMED_BROKER_CANCELLED"
DEFAULT_DB_LOCK_RETRY_SEC = 30.0
DB_LOCK_RETRY_SLEEP_SEC = 0.5
_ALLOWED_COMMAND_STATUSES = {"UNCONFIRMED", "EXPIRED"}
_ORDER_TERMINAL_STATUSES = {
    LiveSimOrderStatus.BROKER_REJECTED.value,
    LiveSimOrderStatus.FILLED.value,
    LiveSimOrderStatus.CANCELLED.value,
    LiveSimOrderStatus.FAILED.value,
    LiveSimOrderStatus.EXPIRED.value,
    LiveSimOrderStatus.ORDER_EXPIRED.value,
    LiveSimOrderStatus.CANCEL_ACKED.value,
    LiveSimOrderStatus.CANCEL_REJECTED.value,
    LiveSimOrderStatus.EXIT_FILLED.value,
}


class ResolveLiveSimOrderError(RuntimeError):
    pass


def summarize_live_sim_order(
    connection: sqlite3.Connection,
    order_id: str,
) -> dict[str, Any]:
    order = _get_row(
        connection,
        "SELECT * FROM live_sim_orders WHERE live_sim_order_id = ?",
        (require_non_empty_str(order_id, "order_id"),),
    )
    if order is None:
        raise ResolveLiveSimOrderError(f"LIVE_SIM 주문을 찾을 수 없습니다: {order_id}")
    command_id = _optional_str(order.get("gateway_command_id"))
    command = (
        _get_row(connection, "SELECT * FROM gateway_commands WHERE command_id = ?", (command_id,))
        if command_id
        else None
    )
    intent = _get_row(
        connection,
        "SELECT * FROM live_sim_intents WHERE live_sim_intent_id = ?",
        (str(order["live_sim_intent_id"]),),
    )
    event_summary = _event_summary(connection, command_id, str(order["live_sim_order_id"]))
    return {
        "order": _compact_order(order),
        "intent": None if intent is None else _compact_intent(intent),
        "gateway_command": None if command is None else _compact_command(command),
        "broker_order_no": order.get("broker_order_no"),
        "gateway_event_summary": event_summary,
        "operator_resolution_supported": _resolution_supported(order, command, event_summary),
        "live_sim_only": True,
        "live_real_allowed": False,
    }


def resolve_live_sim_order(
    connection: sqlite3.Connection,
    *,
    order_id: str,
    mode: str,
    note: str,
    settings: Settings,
    db_lock_retry_sec: float = DEFAULT_DB_LOCK_RETRY_SEC,
) -> dict[str, Any]:
    _assert_live_real_forbidden(settings)
    normalized_mode = require_non_empty_str(mode, "mode").strip().lower()
    if normalized_mode not in {"broker-absent", "broker-cancelled"}:
        raise ResolveLiveSimOrderError(f"지원하지 않는 resolve 모드입니다: {mode}")
    operator_note = require_non_empty_str(note, "note").strip()
    reason = (
        OPERATOR_BROKER_ABSENT_REASON
        if normalized_mode == "broker-absent"
        else OPERATOR_BROKER_CANCELLED_REASON
    )

    transaction_started = False
    try:
        _begin_immediate_with_retry(connection, retry_sec=db_lock_retry_sec)
        transaction_started = True
        summary = summarize_live_sim_order(connection, order_id)
        order = summary["order"]
        command = summary["gateway_command"]
        events = summary["gateway_event_summary"]
        _validate_resolution(summary, mode=normalized_mode)

        now = datetime_to_wire(utc_now())
        next_intent_status = (
            LiveSimIntentStatus.REJECTED.value
            if normalized_mode == "broker-absent"
            else LiveSimIntentStatus.CANCELLED.value
        )
        connection.execute(
            """
            UPDATE live_sim_orders
            SET status = ?,
                remaining_quantity = 0,
                broker_message = ?,
                last_event_at = ?
            WHERE live_sim_order_id = ?
            """,
            (
                LiveSimOrderStatus.FAILED.value,
                reason,
                now,
                order["live_sim_order_id"],
            ),
        )
        connection.execute(
            """
            UPDATE live_sim_intents
            SET status = ?,
                broker_order_sent = 0
            WHERE live_sim_intent_id = ?
                AND status IN (?, ?)
            """,
            (
                next_intent_status,
                order["live_sim_intent_id"],
                LiveSimIntentStatus.CREATED.value,
                LiveSimIntentStatus.COMMAND_QUEUED.value,
            ),
        )
        _record_operator_lifecycle_event(
            connection,
            order=order,
            command=command,
            reason=reason,
            note=operator_note,
            mode=normalized_mode,
            event_summary=events,
            created_at=now,
        )
        connection.commit()
        transaction_started = False
    except Exception:
        if transaction_started:
            connection.rollback()
        raise

    reconcile = _reconcile_with_retry(
        connection,
        settings=settings,
        retry_sec=db_lock_retry_sec,
    )
    refreshed = summarize_live_sim_order(connection, order_id)
    return {
        "resolved": True,
        "mode": normalized_mode,
        "reason": reason,
        "order": refreshed["order"],
        "intent": refreshed["intent"],
        "reconcile": {
            "reconcile_id": reconcile.reconcile_id,
            "status": reconcile.status,
            "mismatch_count": reconcile.mismatch_count,
            "blocking_new_buy": bool(reconcile.snapshot_json.get("blocking_new_buy")),
            "local_open_order_count": reconcile.local_open_order_count,
            "local_position_count": reconcile.local_position_count,
        },
        "live_sim_only": True,
        "live_real_allowed": False,
    }


def _validate_resolution(summary: Mapping[str, Any], *, mode: str) -> None:
    order = _mapping(summary.get("order"))
    command = summary.get("gateway_command")
    if not isinstance(command, Mapping):
        raise ResolveLiveSimOrderError("연결된 gateway command가 없습니다.")
    command_status = str(command.get("status") or "").upper()
    if command_status not in _ALLOWED_COMMAND_STATUSES:
        raise ResolveLiveSimOrderError(
            "gateway command 상태가 UNCONFIRMED/EXPIRED가 아닙니다: "
            f"{command_status or 'UNKNOWN'}"
        )
    command_type = str(command.get("command_type") or "").strip().lower()
    if command_type != "send_order":
        raise ResolveLiveSimOrderError(
            f"send_order command만 운영자 종결할 수 있습니다: {command_type}"
        )
    order_status = str(order.get("status") or "").upper()
    if order_status in _ORDER_TERMINAL_STATUSES:
        raise ResolveLiveSimOrderError(f"이미 종결된 주문입니다: {order_status}")
    if mode == "broker-absent" and _optional_str(order.get("broker_order_no")):
        raise ResolveLiveSimOrderError("broker-absent 모드는 broker_order_no가 없어야 합니다.")
    event_summary = _mapping(summary.get("gateway_event_summary"))
    blocking_counts = _mapping(event_summary.get("blocking_event_counts"))
    for key in (
        "command_started",
        "command_ack",
        "execution_event",
        "chejan_event",
        "live_sim_execution",
    ):
        if int(blocking_counts.get(key) or 0) > 0:
            raise ResolveLiveSimOrderError(
                f"브로커 도달 가능성이 있는 이벤트가 있어 종결을 거부합니다: {key}"
            )


def _begin_immediate_with_retry(
    connection: sqlite3.Connection,
    *,
    retry_sec: float,
) -> None:
    deadline = time.monotonic() + max(float(retry_sec), 0.0)
    while True:
        try:
            connection.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError as exc:
            if not _is_database_locked(exc) or time.monotonic() >= deadline:
                raise ResolveLiveSimOrderError(
                    "데이터베이스가 잠겨 있어 운영자 종결을 완료하지 못했습니다. "
                    "Core/Gateway의 짧은 쓰기 작업과 겹친 경우 잠시 후 같은 명령을 다시 실행하세요."
                ) from exc
            time.sleep(min(DB_LOCK_RETRY_SLEEP_SEC, max(deadline - time.monotonic(), 0.0)))


def _reconcile_with_retry(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    retry_sec: float,
) -> Any:
    deadline = time.monotonic() + max(float(retry_sec), 0.0)
    while True:
        try:
            return reconcile_live_sim(connection, settings=settings)
        except sqlite3.OperationalError as exc:
            if not _is_database_locked(exc) or time.monotonic() >= deadline:
                raise ResolveLiveSimOrderError(
                    "주문은 종결됐지만 reconcile 실행 중 데이터베이스 잠금이 해소되지 않았습니다. "
                    "잠시 후 tools\\reconcile_live_sim.py를 한 번 실행해 상태를 확인하세요."
                ) from exc
            time.sleep(min(DB_LOCK_RETRY_SLEEP_SEC, max(deadline - time.monotonic(), 0.0)))


def _is_database_locked(exc: sqlite3.OperationalError) -> bool:
    return "database is locked" in str(exc).lower()


def _resolution_supported(
    order: Mapping[str, Any],
    command: Mapping[str, Any] | None,
    event_summary: Mapping[str, Any],
) -> dict[str, Any]:
    reasons: list[str] = []
    if command is None:
        reasons.append("MISSING_COMMAND")
    else:
        command_status = str(command.get("status") or "").upper()
        if command_status not in _ALLOWED_COMMAND_STATUSES:
            reasons.append(f"COMMAND_STATUS_{command_status or 'UNKNOWN'}")
        if str(command.get("command_type") or "").strip().lower() != "send_order":
            reasons.append("COMMAND_TYPE_NOT_SEND_ORDER")
    if str(order.get("status") or "").upper() in _ORDER_TERMINAL_STATUSES:
        reasons.append("ORDER_ALREADY_TERMINAL")
    blocking_counts = _mapping(event_summary.get("blocking_event_counts"))
    if any(int(blocking_counts.get(key) or 0) > 0 for key in blocking_counts):
        reasons.append("BROKER_REACH_EVENT_EXISTS")
    return {"supported": not reasons, "reasons": reasons}


def _event_summary(
    connection: sqlite3.Connection,
    command_id: str | None,
    live_sim_order_id: str,
) -> dict[str, Any]:
    if not command_id:
        return {
            "command_id": None,
            "gateway_command_event_counts": {},
            "gateway_event_counts": {},
            "blocking_event_counts": {},
            "recent_gateway_events": [],
        }
    command_event_counts = _event_counts(
        connection,
        "gateway_command_events",
        "created_at",
        command_id,
    )
    gateway_event_counts = _event_counts(connection, "gateway_events", "received_at", command_id)
    recent_gateway_events = _recent_gateway_events(connection, command_id)
    blocking_counts = {
        "command_started": _event_exists_count(connection, command_id, "command_started"),
        "command_ack": _event_exists_count(connection, command_id, "command_ack"),
        "execution_event": _event_exists_count(connection, command_id, "execution_event"),
        "chejan_event": _chejan_event_count(connection, command_id),
        "live_sim_execution": _live_sim_execution_count(connection, live_sim_order_id),
    }
    return {
        "command_id": command_id,
        "gateway_command_event_counts": command_event_counts,
        "gateway_event_counts": gateway_event_counts,
        "blocking_event_counts": blocking_counts,
        "recent_gateway_events": recent_gateway_events,
    }


def _event_counts(
    connection: sqlite3.Connection,
    table_name: str,
    order_column: str,
    command_id: str,
) -> dict[str, int]:
    rows = connection.execute(
        f"""
        SELECT event_type, COUNT(*) AS count
        FROM {table_name}
        WHERE command_id = ?
        GROUP BY event_type
        ORDER BY {order_column} ASC
        """,
        (command_id,),
    ).fetchall()
    return {str(row["event_type"]): int(row["count"] or 0) for row in rows}


def _event_exists_count(
    connection: sqlite3.Connection,
    command_id: str,
    event_type: str,
) -> int:
    row = connection.execute(
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM gateway_command_events
                WHERE command_id = ? AND event_type = ?
            ) AS command_event_count,
            (
                SELECT COUNT(*)
                FROM gateway_events
                WHERE command_id = ? AND event_type = ?
            ) AS gateway_event_count
        """,
        (command_id, event_type, command_id, event_type),
    ).fetchone()
    return max(int(row["command_event_count"] or 0), int(row["gateway_event_count"] or 0))


def _chejan_event_count(connection: sqlite3.Connection, command_id: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_events
        WHERE command_id = ?
            AND event_type IN (
                'kiwoom_order_chejan',
                'kiwoom_balance_chejan',
                'kiwoom_special_chejan'
            )
        """,
        (command_id,),
    ).fetchone()
    return int(row["count"] or 0)


def _live_sim_execution_count(connection: sqlite3.Connection, live_sim_order_id: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM live_sim_executions
        WHERE live_sim_order_id = ?
        """,
        (live_sim_order_id,),
    ).fetchone()
    return int(row["count"] or 0)


def _recent_gateway_events(
    connection: sqlite3.Connection,
    command_id: str,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT event_id, event_type, source, command_id, event_ts, received_at, payload_json
        FROM gateway_events
        WHERE command_id = ?
        ORDER BY received_at DESC, event_id DESC
        LIMIT ?
        """,
        (command_id, max(int(limit), 1)),
    ).fetchall()
    return [_row_to_dict(row) | {"payload_json": _json_loads(row["payload_json"])} for row in rows]


def _record_operator_lifecycle_event(
    connection: sqlite3.Connection,
    *,
    order: Mapping[str, Any],
    command: Mapping[str, Any] | None,
    reason: str,
    note: str,
    mode: str,
    event_summary: Mapping[str, Any],
    created_at: str,
) -> None:
    evidence = {
        "operator_note": note,
        "resolve_mode": mode,
        "previous_order_status": order.get("status"),
        "next_order_status": LiveSimOrderStatus.FAILED.value,
        "next_intent_status": (
            LiveSimIntentStatus.REJECTED.value
            if mode == "broker-absent"
            else LiveSimIntentStatus.CANCELLED.value
        ),
        "gateway_command_id": order.get("gateway_command_id"),
        "command_status": None if command is None else command.get("status"),
        "command_dispatched_at": None if command is None else command.get("dispatched_at"),
        "broker_order_no": order.get("broker_order_no"),
        "event_summary": event_summary,
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
    }
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_events (
            lifecycle_event_id,
            event_type,
            entity_type,
            entity_id,
            live_sim_order_id,
            status,
            reason,
            evidence_json,
            created_at,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, 'OPERATOR_ORDER_RESOLVED', 'LIVE_SIM_ORDER', ?, ?, ?, ?, ?, ?, 1, 0)
        """,
        (
            new_message_id("live_sim_lifecycle"),
            order["live_sim_order_id"],
            order["live_sim_order_id"],
            LiveSimOrderStatus.FAILED.value,
            reason,
            _json_dumps(evidence),
            created_at,
        ),
    )


def _compact_order(order: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "live_sim_order_id",
        "live_sim_intent_id",
        "gateway_command_id",
        "trade_date",
        "account_id",
        "code",
        "name",
        "side",
        "order_type",
        "quantity",
        "limit_price",
        "notional",
        "status",
        "broker_order_no",
        "broker_result_code",
        "broker_message",
        "filled_quantity",
        "remaining_quantity",
        "idempotency_key",
        "command_queued_at",
        "command_dispatched_at",
        "broker_acked_at",
        "last_event_at",
        "created_at",
    )
    return {key: order.get(key) for key in keys}


def _compact_intent(intent: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "live_sim_intent_id",
        "candidate_instance_id",
        "strategy_observation_id",
        "risk_observation_id",
        "trade_date",
        "account_id",
        "code",
        "name",
        "side",
        "order_type",
        "quantity",
        "limit_price",
        "notional",
        "status",
        "idempotency_key",
        "gateway_command_id",
        "broker_order_sent",
        "expires_at",
        "created_at",
    )
    return {key: intent.get(key) for key in keys}


def _compact_command(command: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_loads(command.get("payload_json"))
    payload_summary: dict[str, Any] = {}
    if isinstance(payload, Mapping):
        for key in (
            "mode",
            "live_mode",
            "account_id",
            "code",
            "name",
            "side",
            "quantity",
            "limit_price",
            "price",
            "order_type",
            "order_exchange",
            "live_sim_intent_id",
            "live_sim_only",
            "live_real_allowed",
            "broker_order_path",
        ):
            if key in payload:
                payload_summary[key] = payload.get(key)
    keys = (
        "command_id",
        "command_type",
        "source",
        "status",
        "idempotency_key",
        "created_at",
        "available_at",
        "dispatched_at",
        "completed_at",
        "expires_at",
        "attempts",
        "last_error",
    )
    compact = {key: command.get(key) for key in keys}
    compact["payload_summary"] = payload_summary
    return compact


def _assert_live_real_forbidden(settings: Settings) -> None:
    if settings.trading_mode is TradingMode.LIVE_REAL or settings.trading_allow_live_real:
        raise ResolveLiveSimOrderError("LIVE_REAL 설정에서는 이 운영자 도구를 실행할 수 없습니다.")


def _get_row(
    connection: sqlite3.Connection,
    query: str,
    params: Sequence[Any],
) -> dict[str, Any] | None:
    row = connection.execute(query, tuple(params)).fetchone()
    return None if row is None else _row_to_dict(row)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_loads(value: object) -> Any:
    if not isinstance(value, str) or not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _optional_str(value: object) -> str:
    text = "" if value is None else str(value).strip()
    return text


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="UNCONFIRMED LIVE_SIM 주문을 운영자가 검증 후 종결합니다."
    )
    parser.add_argument("--order-id", required=True, help="live_sim_order_id")
    parser.add_argument(
        "--resolve",
        choices=("broker-absent", "broker-cancelled"),
        help="운영자 종결 모드. 생략하면 요약만 출력합니다.",
    )
    parser.add_argument("--note", help="운영자 확인 메모. --resolve 사용 시 필수입니다.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.resolve and not _optional_str(args.note):
        parser.error("--resolve 사용 시 --note가 필요합니다.")

    settings = load_settings()
    _assert_live_real_forbidden(settings)
    connection = open_connection(settings.trading_db_path)
    connection.execute("PRAGMA busy_timeout=30000")
    try:
        if args.resolve:
            result = resolve_live_sim_order(
                connection,
                order_id=args.order_id,
                mode=args.resolve,
                note=args.note or "",
                settings=settings,
            )
            print("운영자 종결 결과")
        else:
            result = summarize_live_sim_order(connection, args.order_id)
            print("LIVE_SIM 주문 요약")
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    except ResolveLiveSimOrderError as exc:
        print(f"거부: {exc}", file=sys.stderr)
        return 2
    except sqlite3.OperationalError as exc:
        if _is_database_locked(exc):
            print(
                "거부: 데이터베이스가 잠겨 있습니다. 잠시 후 같은 명령을 다시 실행하세요.",
                file=sys.stderr,
            )
            return 2
        raise
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
