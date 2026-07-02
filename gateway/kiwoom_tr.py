from __future__ import annotations

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


@dataclass(frozen=True, kw_only=True)
class TrRequestSubmission:
    key: str
    request_code: int
    result: TrRequestResult
    completed: bool = False

    @property
    def accepted(self) -> bool:
        return self.request_code >= 0 and not self.result.errors


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
    ) -> None:
        self.client = client
        self.timeout_sec = max(float(timeout_sec), 0.1)
        self.request_delay_sec = max(float(request_delay_sec), 0.0)
        self._pending_page: TrPage | None = None
        self._pending_requests: dict[str, dict[str, Any]] = {}
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
        completed_results: list[TrRequestResult] = []
        submission = self.submit(
            request_id=request_id,
            tr_code=tr_code,
            request_name=request_name,
            params=params,
            continuation_key=continuation_key,
            fields=fields,
            screen_no=screen_no,
            on_complete=completed_results.append,
            schedule_timeout=False,
        )
        if completed_results:
            return completed_results[-1]
        if submission.completed:
            return submission.result
        self.cancel(submission.key)
        submission.result.errors.append(
            f"TR_PENDING_CALLBACK:{submission.result.tr_code}:{submission.result.request_name}"
        )
        return submission.result

    def submit(
        self,
        *,
        request_id: str,
        tr_code: str,
        request_name: str,
        params: dict[str, Any],
        continuation_key: str | None = None,
        fields: list[str] | None = None,
        screen_no: str = "8700",
        on_complete,
        schedule_timeout: bool = True,
    ) -> TrRequestSubmission:
        normalized_tr_code = str(tr_code or "").upper()
        normalized_request_name = str(request_name or normalized_tr_code or "KIWOOM_TR")
        normalized_screen_no = str(screen_no or "8700")
        result = TrRequestResult(
            request_id=str(request_id),
            tr_code=normalized_tr_code,
            request_name=normalized_request_name,
        )
        requested_fields = fields or DEFAULT_TR_FIELDS.get(normalized_tr_code, [])
        prev_next = 2 if str(continuation_key or "").strip() == "2" else 0
        key = _pending_key(normalized_screen_no, normalized_request_name, normalized_tr_code)

        for input_name, value in params.items():
            self.client.set_input_value(str(input_name), str(value))
        self._pending_page = None
        self._pending_requests[key] = {
            "result": result,
            "fields": list(requested_fields),
            "tr_code": normalized_tr_code,
            "request_name": normalized_request_name,
            "screen_no": normalized_screen_no,
            "on_complete": on_complete,
        }
        request_code = int(
            self.client.comm_rq_data(
                normalized_request_name,
                normalized_tr_code,
                int(prev_next),
                normalized_screen_no,
            )
            or 0
        )
        result.request_count += 1
        if request_code < 0:
            result.errors.append(f"TR_REQUEST_FAILED:{normalized_tr_code}:{request_code}")
            self._pending_requests.pop(key, None)
            return TrRequestSubmission(
                key=key,
                request_code=request_code,
                result=result,
                completed=True,
            )
        if key not in self._pending_requests:
            return TrRequestSubmission(
                key=key,
                request_code=request_code,
                result=result,
                completed=True,
            )
        if schedule_timeout:
            self._schedule_timeout(key)
        return TrRequestSubmission(key=key, request_code=request_code, result=result)

    def cancel(self, key: str) -> None:
        self._pending_requests.pop(str(key), None)

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
        key = self._find_pending_key(meta)
        if key is None:
            self._pending_page = TrPage(meta=meta, rows=[])
            return
        active = self._pending_requests.pop(key)
        result = active["result"]
        fields = list(active["fields"])
        rows = self._extract_rows(result, meta, fields)
        result.pages.append(TrPage(meta=meta, rows=rows))
        on_complete = active["on_complete"]
        if callable(on_complete):
            on_complete(result)

    def _find_pending_key(self, meta: TrResponseMeta) -> str | None:
        exact_key = _pending_key(meta.screen_no, meta.rq_name, meta.tr_code)
        if exact_key in self._pending_requests:
            return exact_key
        for key, pending in self._pending_requests.items():
            if str(pending.get("request_name") or "") == meta.rq_name:
                return key
        return None

    def _schedule_timeout(self, key: str) -> None:
        timeout_ms = max(int(self.timeout_sec * 1000), 1)
        try:
            from PyQt5.QtCore import QTimer
        except ImportError:
            return
        QTimer.singleShot(timeout_ms, lambda: self._timeout_request(key))

    def _timeout_request(self, key: str) -> None:
        pending = self._pending_requests.pop(str(key), None)
        if pending is None:
            return
        result = pending["result"]
        result.errors.append(f"TR_TIMEOUT:{result.tr_code}:{result.request_name}")
        on_complete = pending["on_complete"]
        if callable(on_complete):
            on_complete(result)

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


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value or "")
        if text and text not in result:
            result.append(text)
    return result


def _pending_key(screen_no: str, request_name: str, tr_code: str) -> str:
    return f"{str(screen_no or '')}|{str(request_name or '')}|{str(tr_code or '').upper()}"
