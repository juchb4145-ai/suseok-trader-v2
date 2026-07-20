from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from domain.broker.account_snapshot import (
    KIWOOM_LIVE_SIM_SNAPSHOT_TR_SPECS,
    BrokerSnapshotSection,
    BrokerSnapshotStatus,
    mask_account_id,
)
from domain.broker.utils import datetime_to_wire, normalize_payload, utc_now

from gateway.kiwoom_client import normalize_code

SnapshotCallback = Callable[[dict[str, Any]], None]


@dataclass
class _CollectionState:
    snapshot_id: str
    account_id: str
    trade_date: str
    stale_after_sec: int
    max_pages_per_section: int
    open_orders: list[dict[str, Any]] = field(default_factory=list)
    executions: list[dict[str, Any]] = field(default_factory=list)
    positions: list[dict[str, Any]] = field(default_factory=list)
    page_lineage: list[dict[str, Any]] = field(default_factory=list)
    completed_sections: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class KiwoomBrokerSnapshotCollector:
    """Assemble three read-only Kiwoom TR streams into one account snapshot."""

    def __init__(self, tr_runner: Any) -> None:
        self.tr_runner = tr_runner

    def start(
        self,
        *,
        snapshot_id: str,
        account_id: str,
        trade_date: str,
        stale_after_sec: int,
        max_pages_per_section: int,
        on_progress: SnapshotCallback,
        on_complete: SnapshotCallback,
    ) -> dict[str, Any]:
        state = _CollectionState(
            snapshot_id=str(snapshot_id),
            account_id=str(account_id),
            trade_date=str(trade_date),
            stale_after_sec=min(max(int(stale_after_sec), 1), 3600),
            max_pages_per_section=min(max(int(max_pages_per_section), 1), 50),
        )
        requested = self._payload(state, BrokerSnapshotStatus.REQUESTED)
        self._submit_page(
            state,
            spec_index=0,
            page_number=1,
            continuation_key=None,
            on_progress=on_progress,
            on_complete=on_complete,
        )
        return requested

    def _submit_page(
        self,
        state: _CollectionState,
        *,
        spec_index: int,
        page_number: int,
        continuation_key: str | None,
        on_progress: SnapshotCallback,
        on_complete: SnapshotCallback,
    ) -> None:
        if spec_index >= len(KIWOOM_LIVE_SIM_SNAPSHOT_TR_SPECS):
            on_complete(self._payload(state, BrokerSnapshotStatus.COMPLETE))
            return
        spec = KIWOOM_LIVE_SIM_SNAPSHOT_TR_SPECS[spec_index]
        request_id = (
            f"{state.snapshot_id}:{spec.section.value.lower()}:page-{page_number}"
        )

        def on_result(result: Any) -> None:
            errors = [str(item) for item in getattr(result, "errors", []) if str(item)]
            warnings = [
                str(item) for item in getattr(result, "warnings", []) if str(item)
            ]
            next_key = str(getattr(result, "continuation_key", "") or "").strip()
            rows = [
                dict(row)
                for row in getattr(result, "rows", [])
                if isinstance(row, Mapping)
            ]
            state.page_lineage.append(
                {
                    "snapshot_id": state.snapshot_id,
                    "section": spec.section.value,
                    "tr_code": spec.tr_code,
                    "request_id": request_id,
                    "page_number": page_number,
                    "continuation_in": str(continuation_key or ""),
                    "continuation_out": next_key,
                    "row_count": len(rows),
                    "warnings": warnings,
                    "errors": errors,
                    "received_at": datetime_to_wire(utc_now()),
                }
            )
            self._append_rows(state, spec.section, rows)
            if errors:
                state.errors.extend(errors)
                on_complete(self._payload(state, BrokerSnapshotStatus.FAILED))
                return
            on_progress(self._payload(state, BrokerSnapshotStatus.COLLECTING))
            if next_key == "2":
                if page_number >= state.max_pages_per_section:
                    state.errors.append(
                        f"SNAPSHOT_PAGE_LIMIT:{spec.section.value}:{page_number}"
                    )
                    on_complete(self._payload(state, BrokerSnapshotStatus.INCOMPLETE))
                    return
                self._submit_page(
                    state,
                    spec_index=spec_index,
                    page_number=page_number + 1,
                    continuation_key="2",
                    on_progress=on_progress,
                    on_complete=on_complete,
                )
                return
            state.completed_sections.append(spec.section.value)
            self._submit_page(
                state,
                spec_index=spec_index + 1,
                page_number=1,
                continuation_key=None,
                on_progress=on_progress,
                on_complete=on_complete,
            )

        try:
            submission = self.tr_runner.submit(
                request_id=request_id,
                tr_code=spec.tr_code,
                request_name=spec.request_name,
                params=spec.params(state.account_id),
                continuation_key=continuation_key,
                fields=list(spec.fields),
                screen_no=spec.screen_no,
                row_mode="multi",
                output_record_name=spec.output_record_name,
                on_complete=on_result,
            )
        except Exception as exc:
            state.errors.append(f"SNAPSHOT_TR_SUBMIT_FAILED:{spec.section.value}:{exc}")
            on_complete(self._payload(state, BrokerSnapshotStatus.FAILED))
            return
        if not submission.accepted:
            submission_errors = [
                str(item)
                for item in getattr(submission.result, "errors", [])
                if str(item)
            ]
            state.errors.extend(
                submission_errors
                or [f"SNAPSHOT_TR_REQUEST_REJECTED:{spec.section.value}"]
            )
            on_complete(self._payload(state, BrokerSnapshotStatus.FAILED))

    @staticmethod
    def _append_rows(
        state: _CollectionState,
        section: BrokerSnapshotSection,
        rows: list[dict[str, Any]],
    ) -> None:
        if section is BrokerSnapshotSection.OPEN_ORDERS:
            state.open_orders.extend(_normalize_open_order(row) for row in rows)
        elif section is BrokerSnapshotSection.EXECUTIONS:
            state.executions.extend(_normalize_execution(row) for row in rows)
        else:
            state.positions.extend(_normalize_position(row) for row in rows)

    @staticmethod
    def _payload(
        state: _CollectionState,
        status: BrokerSnapshotStatus,
    ) -> dict[str, Any]:
        complete = status is BrokerSnapshotStatus.COMPLETE
        now = datetime_to_wire(utc_now())
        return normalize_payload(
            {
                "snapshot_id": state.snapshot_id,
                "snapshot_status": status.value,
                "complete": complete,
                "fresh": complete,
                "account_id_masked": mask_account_id(state.account_id),
                "trade_date": state.trade_date,
                "snapshot_at": now,
                "stale_after_sec": state.stale_after_sec,
                "requested_sections": [
                    spec.section.value for spec in KIWOOM_LIVE_SIM_SNAPSHOT_TR_SPECS
                ],
                "completed_sections": list(state.completed_sections),
                "open_orders": list(state.open_orders),
                "executions": list(state.executions),
                "positions": list(state.positions),
                "page_lineage": list(state.page_lineage),
                "errors": list(state.errors),
                "source": "kiwoom_openapi_plus_read_only",
                "mode": "LIVE_SIM",
                "live_sim_only": True,
                "live_real_allowed": False,
                "broker_order_path": "LIVE_SIM_ONLY",
                "automatic_local_repair": False,
            }
        )


def _normalize_open_order(row: Mapping[str, Any]) -> dict[str, Any]:
    quantity = _int_value(_first(row, "주문수량", "quantity"))
    remaining = _int_value(_first(row, "미체결수량", "remaining_quantity"))
    filled = _int_value(_first(row, "체결량", "filled_quantity"))
    if filled == 0 and quantity >= remaining:
        filled = quantity - remaining
    return {
        "broker_order_no": _text(_first(row, "주문번호", "broker_order_no")),
        "original_order_no": _text(_first(row, "원주문번호", "original_order_no")),
        "code": _code(_first(row, "종목코드", "code")),
        "name": _text(_first(row, "종목명", "name")),
        "side": _side(_first(row, "주문구분", "매매구분", "side")),
        "order_status": _text(
            _first(row, "주문상태", "order_status", "status")
        ).upper(),
        "quantity": quantity,
        "filled_quantity": filled,
        "remaining_quantity": remaining,
        "price": _int_value(_first(row, "주문가격", "price")),
        "order_time": _text(_first(row, "시간", "주문시간", "order_time")),
    }


def _normalize_execution(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "broker_execution_id": _text(
            _first(row, "체결번호", "broker_execution_id", "execution_id")
        ),
        "broker_order_no": _text(_first(row, "주문번호", "broker_order_no")),
        "code": _code(_first(row, "종목코드", "code")),
        "name": _text(_first(row, "종목명", "name")),
        "side": _side(_first(row, "주문구분", "매매구분", "side")),
        "quantity": _int_value(_first(row, "체결량", "quantity")),
        "price": _int_value(_first(row, "체결가", "price")),
        "executed_at": _text(_first(row, "체결시간", "주문시간", "executed_at")),
    }


def _normalize_position(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "code": _code(_first(row, "종목번호", "종목코드", "code")),
        "name": _text(_first(row, "종목명", "name")),
        "quantity": _int_value(_first(row, "보유수량", "quantity")),
        "available_quantity": _int_value(
            _first(row, "매매가능수량", "available_quantity")
        ),
        "avg_entry_price": _number(_first(row, "매입가", "avg_entry_price")),
        "current_price": _number(_first(row, "현재가", "current_price")),
        "entry_notional": _number(_first(row, "매입금액", "entry_notional")),
        "market_value": _number(_first(row, "평가금액", "market_value")),
    }


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return ""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _code(value: Any) -> str:
    text = _text(value)
    if text.upper().startswith("A") and len(text) == 7:
        text = text[1:]
    try:
        return normalize_code(text)
    except (TypeError, ValueError):
        return text


def _number(value: Any) -> float:
    text = _text(value).replace(",", "").replace("%", "")
    if not text:
        return 0.0
    try:
        return abs(float(text))
    except ValueError:
        return 0.0


def _int_value(value: Any) -> int:
    return int(_number(value))


def _side(value: Any) -> str:
    normalized = _text(value).replace("+", "").replace("-", "").strip().upper()
    if "매수" in normalized or normalized == "BUY":
        return "BUY"
    if "매도" in normalized or normalized == "SELL":
        return "SELL"
    return normalized
