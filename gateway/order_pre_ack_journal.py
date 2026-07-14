from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, normalize_payload, utc_now

from gateway.kiwoom_client import KiwoomOrderRequest, KiwoomOrderResult


@dataclass(frozen=True, kw_only=True)
class OrderPreAckRecord:
    command_id: str
    idempotency_key: str
    command_type: str
    status: str
    payload: Mapping[str, Any]
    recorded_at: str


class OrderPreAckJournal:
    """Append-only local journal for order commands that reached the broker boundary."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], Any] = utc_now,
    ) -> None:
        self.path = Path(path)
        self.clock = clock

    def record_pre_ack(
        self,
        command: GatewayCommand,
        request: KiwoomOrderRequest,
        *,
        broker_env: str,
        source: str = "kiwoom_gateway",
    ) -> GatewayEvent:
        event = make_order_pre_ack_event(
            command,
            request,
            broker_env=broker_env,
            source=source,
        )
        self._append(command, status="PRE_ACK", payload=event.payload)
        return event

    def mark_broker_result(
        self,
        command: GatewayCommand,
        result: KiwoomOrderResult,
    ) -> None:
        payload = {
            "command_id": command.command_id,
            "command_type": command.command_type,
            "idempotency_key": command.idempotency_key,
            "status": "BROKER_ACCEPTED" if result.ok else "BROKER_REJECTED",
            "broker_result_code": result.code,
            "broker_message": result.message,
            "broker_order_no": result.order_no,
        }
        self._append(command, status=str(payload["status"]), payload=payload)

    def mark_failed(self, command: GatewayCommand, error_message: str) -> None:
        payload = {
            "command_id": command.command_id,
            "command_type": command.command_type,
            "idempotency_key": command.idempotency_key,
            "status": "SEND_FAILED",
            "error_message": error_message,
        }
        self._append(command, status="SEND_FAILED", payload=payload)

    def mark_unconfirmed(self, command: GatewayCommand, error_message: str) -> None:
        payload = {
            "command_id": command.command_id,
            "command_type": command.command_type,
            "idempotency_key": command.idempotency_key,
            "status": "UNCONFIRMED",
            "broker_call_attempted": True,
            "broker_acceptance_unknown": True,
            "error_message": error_message,
        }
        self._append(command, status="UNCONFIRMED", payload=payload)

    def pending_records(self) -> list[OrderPreAckRecord]:
        latest: dict[str, OrderPreAckRecord] = {}
        for record in self._read_records():
            latest[record.command_id] = record
        return [
            record
            for record in latest.values()
            if record.status in {"PRE_ACK", "BROKER_ACCEPTED"}
        ]

    def recovery_events(self, *, source: str) -> list[GatewayEvent]:
        events: list[GatewayEvent] = []
        for record in self.pending_records():
            payload = dict(record.payload)
            payload["status"] = f"RECOVERED_{record.status}"
            payload["journal_path"] = str(self.path)
            payload["journal_recorded_at"] = record.recorded_at
            events.append(
                GatewayEvent(
                    event_type="order_pre_ack",
                    source=source,
                    command_id=record.command_id,
                    idempotency_key=record.idempotency_key or None,
                    payload=payload,
                )
            )
        return events

    def _append(
        self,
        command: GatewayCommand,
        *,
        status: str,
        payload: Mapping[str, Any],
    ) -> None:
        now = datetime_to_wire(self.clock())
        row = {
            "recorded_at": now,
            "command_id": command.command_id,
            "idempotency_key": command.idempotency_key or "",
            "command_type": command.command_type,
            "status": status,
            "payload": normalize_payload(payload),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    def _read_records(self) -> Iterable[OrderPreAckRecord]:
        if not self.path.exists():
            return []
        records: list[OrderPreAckRecord] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                records.append(
                    OrderPreAckRecord(
                        command_id=str(row.get("command_id") or ""),
                        idempotency_key=str(row.get("idempotency_key") or ""),
                        command_type=str(row.get("command_type") or ""),
                        status=str(row.get("status") or ""),
                        payload=dict(row.get("payload") or {}),
                        recorded_at=str(row.get("recorded_at") or ""),
                    )
                )
        return records


def make_order_pre_ack_event(
    command: GatewayCommand,
    request: KiwoomOrderRequest,
    *,
    broker_env: str,
    source: str = "kiwoom_gateway",
) -> GatewayEvent:
    payload = {
        "command_id": command.command_id,
        "command_type": command.command_type,
        "idempotency_key": command.idempotency_key,
        "status": "PRE_ACK",
        "broker_env": broker_env,
        "account_id": request.account,
        "code": request.code,
        "side": request.side,
        "quantity": request.quantity,
        "price": request.price,
        "order_type": request.order_type,
        "hoga": request.hoga,
        "original_order_no": request.original_order_no,
        "metadata": normalize_payload(request.metadata),
    }
    return GatewayEvent(
        event_type="order_pre_ack",
        source=source,
        command_id=command.command_id,
        idempotency_key=command.idempotency_key,
        payload=payload,
    )
