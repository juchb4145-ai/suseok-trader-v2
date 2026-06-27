from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, kw_only=True)
class TrResponseMeta:
    screen_no: str = ""
    rq_name: str = ""
    tr_code: str = ""
    record_name: str = ""
    prev_next: str = ""
    error_code: str = ""
    message: str = ""
    splm_msg: str = ""


@dataclass(frozen=True, kw_only=True)
class TrPage:
    meta: TrResponseMeta
    rows: list[dict[str, str]] = field(default_factory=list)
    single: dict[str, str] = field(default_factory=dict)


@dataclass
class TrRequestResult:
    request_id: str
    tr_code: str
    request_name: str
    pages: list[TrPage] = field(default_factory=list)
    request_count: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def rows(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for page in self.pages:
            rows.extend(page.rows)
        return rows

    @property
    def continuation_key(self) -> str | None:
        if not self.pages:
            return None
        prev_next = str(self.pages[-1].meta.prev_next or "").strip()
        return prev_next if prev_next else None

    @property
    def success(self) -> bool:
        return not self.errors


DEFAULT_TR_FIELDS: dict[str, list[str]] = {
    "OPT10001": [
        "종목코드",
        "종목명",
        "현재가",
        "등락율",
        "거래량",
        "거래대금",
        "시가",
        "고가",
        "저가",
        "상장주식",
    ],
}


class KiwoomTrRunner:
    def __init__(
        self,
        client: Any,
        *,
        timeout_sec: float = 20.0,
        request_delay_sec: float = 1.2,
        clock=time.monotonic,
        sleeper=time.sleep,
        process_events=None,
    ) -> None:
        self.client = client
        self.timeout_sec = max(float(timeout_sec), 0.1)
        self.request_delay_sec = max(float(request_delay_sec), 0.0)
        self.clock = clock
        self.sleeper = sleeper
        self.process_events = process_events or _process_qt_events
        self._pending_page: TrPage | None = None
        self._active_request: dict[str, Any] | None = None
        signal = getattr(client, "tr_data_received", None)
        if signal is not None and hasattr(signal, "connect"):
            signal.connect(self._handle_tr_data)

    def request(
        self,
        *,
        request_id: str,
        tr_code: str,
        request_name: str,
        params: dict[str, Any],
        continuation_key: str | None = None,
        fields: list[str] | None = None,
        screen_no: str = "8700",
    ) -> TrRequestResult:
        normalized_tr_code = str(tr_code or "").upper()
        normalized_request_name = str(request_name or normalized_tr_code or "KIWOOM_TR")
        result = TrRequestResult(
            request_id=str(request_id),
            tr_code=normalized_tr_code,
            request_name=normalized_request_name,
        )
        requested_fields = fields or DEFAULT_TR_FIELDS.get(normalized_tr_code, [])
        prev_next = 2 if str(continuation_key or "").strip() == "2" else 0

        for input_name, value in params.items():
            self.client.set_input_value(str(input_name), str(value))
        self._pending_page = None
        self._active_request = {
            "result": result,
            "fields": list(requested_fields),
            "tr_code": normalized_tr_code,
            "request_name": normalized_request_name,
        }
        request_code = int(
            self.client.comm_rq_data(
                normalized_request_name,
                normalized_tr_code,
                int(prev_next),
                str(screen_no),
            )
            or 0
        )
        result.request_count += 1
        if request_code < 0:
            result.errors.append(f"TR_REQUEST_FAILED:{normalized_tr_code}:{request_code}")
            self._active_request = None
            return result

        deadline = self.clock() + self.timeout_sec
        while self._pending_page is None and self.clock() < deadline:
            self.process_events()
            if self._pending_page is None:
                self.sleeper(0.01)
        if self._pending_page is None:
            result.errors.append(f"TR_TIMEOUT:{normalized_tr_code}:{normalized_request_name}")
            self._active_request = None
            return result
        result.pages.append(self._pending_page)
        self._pending_page = None
        self._active_request = None
        return result

    def _handle_tr_data(self, *args: Any) -> None:
        values = list(args) + [""] * 9
        meta = TrResponseMeta(
            screen_no=str(values[0] or ""),
            rq_name=str(values[1] or ""),
            tr_code=str(values[2] or ""),
            record_name=str(values[3] or ""),
            prev_next=str(values[4] or ""),
            error_code=str(values[6] or ""),
            message=str(values[7] or ""),
            splm_msg=str(values[8] or ""),
        )
        active = self._active_request
        if active is None:
            self._pending_page = TrPage(meta=meta, rows=[])
            return
        result = active["result"]
        fields = list(active["fields"])
        rows = self._extract_rows(result, meta, fields)
        self._pending_page = TrPage(meta=meta, rows=rows)

    def _extract_rows(
        self,
        result: TrRequestResult,
        meta: TrResponseMeta,
        fields: list[str],
    ) -> list[dict[str, str]]:
        if not fields:
            result.warnings.append(f"TR_FIELDS_EMPTY:{result.tr_code}:{result.request_name}")
            return []
        tr_code = meta.tr_code or result.tr_code
        record_candidates = _dedupe(
            [meta.rq_name, meta.record_name, result.request_name, result.tr_code]
        )
        record_name, repeat_count = self._repeat_count(tr_code, record_candidates)
        if repeat_count <= 0:
            row = self._extract_single_row(tr_code, record_candidates, fields)
            if row:
                result.warnings.append(f"TR_SINGLE_ROW_FALLBACK:{result.tr_code}")
                return [row]
            result.warnings.append(f"TR_PAGE_EMPTY:{result.tr_code}:{result.request_name}")
            return []
        rows: list[dict[str, str]] = []
        for index in range(repeat_count):
            row: dict[str, str] = {}
            for field_name in fields:
                row[field_name] = self._safe_get_comm_data(tr_code, record_name, index, field_name)
            rows.append(row)
        return rows

    def _repeat_count(self, tr_code: str, record_candidates: list[str]) -> tuple[str, int]:
        for record_name in record_candidates:
            try:
                count = int(self.client.get_repeat_count(tr_code, record_name) or 0)
            except Exception:
                continue
            if count > 0:
                return record_name, count
        return (record_candidates[0] if record_candidates else tr_code), 0

    def _extract_single_row(
        self,
        tr_code: str,
        record_candidates: list[str],
        fields: list[str],
    ) -> dict[str, str]:
        for record_name in record_candidates:
            row = {
                field_name: self._safe_get_comm_data(tr_code, record_name, 0, field_name)
                for field_name in fields
            }
            if any(str(value or "").strip() for value in row.values()):
                return row
        return {}

    def _safe_get_comm_data(
        self,
        tr_code: str,
        record_name: str,
        index: int,
        field_name: str,
    ) -> str:
        try:
            value = self.client.get_comm_data(tr_code, record_name, index, field_name)
            return str(value or "").strip()
        except Exception:
            return ""


def _process_qt_events() -> None:
    try:
        from PyQt5.QtWidgets import QApplication
    except ImportError:
        return
    app = QApplication.instance()
    if app is not None:
        app.processEvents()


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value or "")
        if text and text not in result:
            result.append(text)
    return result
