from __future__ import annotations

import os
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from domain.broker.market import BrokerPriceTick
from domain.broker.market_index import (
    DEFAULT_ALLOWED_INDEX_CODES,
    BrokerMarketIndexTick,
)
from domain.broker.utils import datetime_to_wire, utc_now, validate_stock_code

# Adapted from suseok_ai Kiwoom gateway assets; legacy strategy/runtime code intentionally excluded.

FID_CURRENT_PRICE = 10
FID_CHANGE_VALUE = 11
FID_CHANGE_RATE = 12
FID_ACC_VOLUME = 13
FID_ACC_TRADE_VALUE = 14
FID_OPEN_PRICE = 16
FID_HIGH_PRICE = 17
FID_LOW_PRICE = 18
FID_TRADE_TIME = 20
FID_BEST_ASK = 27
FID_BEST_BID = 28
FID_EXECUTION_STRENGTH = 228

REALTIME_STOCK_FIDS = [
    FID_CURRENT_PRICE,
    FID_CHANGE_RATE,
    FID_ACC_VOLUME,
    FID_ACC_TRADE_VALUE,
    FID_OPEN_PRICE,
    FID_HIGH_PRICE,
    FID_LOW_PRICE,
    FID_TRADE_TIME,
    FID_BEST_ASK,
    FID_BEST_BID,
    FID_EXECUTION_STRENGTH,
]

PRICE_TICK_REAL_TYPES = frozenset({"주식체결", "주식시세"})
QUOTE_REAL_TYPES = frozenset({"주식우선호가"})
MARKET_INDEX_REAL_TYPES = frozenset({"업종지수"})
MARKET_INDEX_KIWOOM_CODE_BY_INDEX = {
    "KOSPI": "001",
    "KOSDAQ": "101",
}
MARKET_INDEX_NAME_BY_CODE = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
}
MARKET_INDEX_REALTIME_FIDS = (
    FID_CURRENT_PRICE,
    FID_CHANGE_VALUE,
    FID_CHANGE_RATE,
    FID_TRADE_TIME,
)
MARKET_INDEX_PARSER_STATUS = "PILOT_UNVERIFIED_FID_MAP"
REALTIME_EXCHANGE_SUFFIXES = {
    "KRX": "",
    "NXT": "_NX",
    "ALL": "_AL",
}
REALTIME_SUFFIX_EXCHANGES = {
    "_NX": "NXT",
    "_AL": "ALL",
    "_KR": "KRX",
}

ERROR_MESSAGES = {
    0: "정상처리",
    -10: "실패",
    -100: "사용자정보교환실패",
    -101: "서버접속실패",
    -102: "버전처리실패",
    -103: "개인방화벽실패",
    -104: "메모리보호실패",
    -105: "함수입력값오류",
    -106: "통신연결종료",
    -200: "시세조회과부하",
    -201: "전문작성초기화실패",
    -202: "전문작성입력값오류",
    -203: "데이터없음",
    -204: "조회가능한종목수초과",
    -205: "데이터수신실패",
    -206: "조회가능한FID수초과",
    -207: "실시간해제오류",
    -300: "주문 입력값오류",
    -301: "계좌비밀번호없음",
    -302: "타인계좌사용오류",
    -303: "주문가격 20억원 초과",
    -304: "주문가격 50억원 초과",
    -305: "주문수량 총발행주수 1% 초과",
    -306: "주문수량 총발행주수 3% 초과",
    -307: "주문전송실패",
    -308: "주문전송과부하",
    -309: "주문수량 300계약 초과",
    -310: "주문수량 500계약 초과",
    -340: "계좌정보없음",
    -500: "종목코드없음",
}
LOGIN_EVENT_TIMEOUT_CODE = -1000
DEFAULT_LOGIN_EVENT_LOOP_TIMEOUT_MS = int(
    os.environ.get("KIWOOM_LOGIN_EVENT_LOOP_TIMEOUT_MS", "60000")
)
DEFAULT_CONDITION_EVENT_LOOP_TIMEOUT_MS = int(
    os.environ.get("KIWOOM_CONDITION_EVENT_LOOP_TIMEOUT_MS", "10000")
)
MAX_PENDING_THREAD_AUDIT_EVENTS = max(
    int(os.environ.get("KIWOOM_PENDING_THREAD_AUDIT_MAX_EVENTS", "200")),
    1,
)

ORDER_CHEJAN_FIDS = (
    9201,
    9203,
    9001,
    912,
    913,
    302,
    900,
    901,
    902,
    903,
    904,
    905,
    906,
    907,
    908,
    909,
    910,
    911,
    914,
    915,
    919,
    920,
)
BALANCE_CHEJAN_FIDS = (
    9201,
    9001,
    302,
    10,
    27,
    28,
    930,
    931,
    932,
    933,
    945,
    946,
    951,
    307,
    8019,
)


class ConditionLoadState(StrEnum):
    IDLE = "IDLE"
    LOADING = "LOADING"
    LOADED = "LOADED"
    FAILED = "FAILED"


@dataclass(frozen=True, kw_only=True)
class ConditionInfo:
    index: int
    name: str

    def to_dict(self) -> dict[str, Any]:
        return {"index": self.index, "name": self.name}


@dataclass(frozen=True, kw_only=True)
class KiwoomRealtimeCode:
    base_code: str
    kiwoom_code: str
    exchange: str


class Signal:
    def __init__(self) -> None:
        self._handlers: list[Callable[..., None]] = []

    def connect(self, handler: Callable[..., None]) -> None:
        self._handlers.append(handler)

    def emit(self, *args: Any, **kwargs: Any) -> None:
        for handler in list(self._handlers):
            handler(*args, **kwargs)


def active_x_thread_audit(method: str, *, phase: str = "CALL", **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "method": str(method),
        "phase": str(phase),
        "timestamp": datetime_to_wire(utc_now()),
        "python_thread_id": threading.get_ident(),
        "python_thread_name": threading.current_thread().name,
        "qt_thread_id": _current_qt_thread_id(),
    }
    payload.update(extra)
    return payload


def _current_qt_thread_id() -> str:
    try:
        from PyQt5.QtCore import QThread

        return str(QThread.currentThreadId())
    except Exception:
        return ""


@dataclass(frozen=True, kw_only=True)
class KiwoomOrderRequest:
    account: str
    code: str
    quantity: int
    price: int
    side: str
    tag: str
    order_type: int = 1
    hoga: str = "00"
    original_order_no: str = ""
    command_id: str = ""
    idempotency_key: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "code": normalize_code(self.code),
            "quantity": int(self.quantity),
            "price": int(self.price),
            "side": self.side,
            "tag": self.tag,
            "order_type": int(self.order_type),
            "hoga": self.hoga,
            "original_order_no": self.original_order_no,
            "command_id": self.command_id,
            "idempotency_key": self.idempotency_key,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, kw_only=True)
class KiwoomOrderResult:
    ok: bool
    code: int
    message: str
    request: KiwoomOrderRequest
    order_no: str = ""
    raw: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "code": self.code,
            "message": self.message,
            "request": self.request.to_dict(),
            "order_no": self.order_no,
            "command_id": self.request.command_id,
            "idempotency_key": self.request.idempotency_key,
            "raw": dict(self.raw),
        }


@dataclass(frozen=True, kw_only=True)
class KiwoomChejanParseResult:
    gubun: str
    gateway_event_type: str
    event_kind: str
    payload: Mapping[str, Any]
    execution_payload: Mapping[str, Any] | None = None

    def to_event_payload(self) -> dict[str, Any]:
        return dict(self.payload)


class KiwoomChejanParser:
    def parse(
        self,
        *,
        gubun: str,
        item_count: int,
        fid_list: str,
        raw_fids: Mapping[int | str, Any],
    ) -> KiwoomChejanParseResult:
        raw = {str(key): str(value or "").strip() for key, value in dict(raw_fids).items()}
        common = {
            "gubun": str(gubun or ""),
            "item_count": int(item_count or 0),
            "requested_fids": parse_fid_list(fid_list),
            "raw_fids": raw,
            "source": "KIWOOM_CHEJAN",
            "parser_version": "kiwoom_chejan_v2_adapter",
            "parsed_at": datetime_to_wire(utc_now()),
        }
        if str(gubun) == "0":
            return _parse_order_chejan(common)
        if str(gubun) == "1":
            return _parse_balance_chejan(common)
        payload = {
            **common,
            "event_kind": "special_signal",
            "parser_status": "UNSUPPORTED",
            "parser_warning_codes": ["SPECIAL_SIGNAL_IGNORED"],
        }
        return KiwoomChejanParseResult(
            gubun=str(gubun or ""),
            gateway_event_type="kiwoom_special_chejan",
            event_kind="special_signal",
            payload=payload,
        )


class KiwoomClient:
    login_waits_for_event_loop = True

    def __init__(self) -> None:
        try:
            from PyQt5.QAxContainer import QAxWidget
        except ImportError as exc:
            raise RuntimeError(
                "32-bit Python + PyQt5.QAxContainer + Kiwoom OpenAPI+ installation is required "
                "to run apps.kiwoom_gateway."
            ) from exc

        self.connected = Signal()
        self.price_received = Signal()
        self.price_tick_received = Signal()
        self.quote_received = Signal()
        self.market_index_tick_received = Signal()
        self.realtime_data_received = Signal()
        self.realtime_parse_error = Signal()
        self.realtime_registration_result = Signal()
        self.active_x_thread_audit = Signal()
        self.order_result = Signal()
        self.execution_received = Signal()
        self.chejan_event_received = Signal()
        self.message_received = Signal()
        self.condition_state_changed = Signal()
        self.condition_load_result = Signal()
        self.condition_loaded = Signal()
        self.condition_tr_received = Signal()
        self.condition_real_received = Signal()
        self.tr_data_received = Signal()
        self.condition_load_state = ConditionLoadState.IDLE
        self._conditions: list[ConditionInfo] = []
        self._realtime_screen_codes: dict[str, set[str]] = {}
        self._pending_thread_audit_events: list[dict[str, Any]] = []
        self._login_event_loop: Any | None = None
        self._login_callback_result: tuple[int, str] | None = None
        self._condition_event_loop: Any | None = None
        self._condition_callback_result: tuple[bool, str] | None = None
        self.chejan_parser = KiwoomChejanParser()
        self._market_index_realtime_codes: dict[str, str] = {}
        self._market_index_realtime_screen_codes: dict[str, set[str]] = {}

        self.ocx = self._create_ocx_widget(QAxWidget)

        self.ocx.OnEventConnect.connect(self._on_event_connect)
        self.ocx.OnReceiveRealData.connect(self._on_receive_real_data)
        self.ocx.OnReceiveChejanData.connect(self._on_receive_chejan_data)
        self.ocx.OnReceiveMsg.connect(self._on_receive_msg)
        self.ocx.OnReceiveConditionVer.connect(self._on_receive_condition_ver)
        self.ocx.OnReceiveTrCondition.connect(self._on_receive_tr_condition)
        self.ocx.OnReceiveRealCondition.connect(self._on_receive_real_condition)
        self.ocx.OnReceiveTrData.connect(self._on_receive_tr_data)
        self._record_thread_audit("QAxWidget.signal_connect", phase="RESULT", success=True)

    def login(self, timeout_ms: int | None = None) -> int:
        from PyQt5.QtCore import QTimer

        wait_timeout_ms = int(timeout_ms or DEFAULT_LOGIN_EVENT_LOOP_TIMEOUT_MS)
        loop, timer, timed_out = self._prepare_callback_wait(
            "OnEventConnect",
            wait_timeout_ms,
        )
        self._login_event_loop = loop
        self._login_callback_result = None
        result_holder: dict[str, int | None] = {"value": None}

        def call_comm_connect() -> None:
            self._record_thread_audit("CommConnect", phase="CALL")
            result_holder["value"] = int(self.ocx.dynamicCall("CommConnect()") or 0)
            self._record_thread_audit(
                "CommConnect",
                phase="RESULT",
                result_code=result_holder["value"],
            )
            if result_holder["value"] != 0 and self._login_event_loop is not None:
                self._login_event_loop.exit()

        QTimer.singleShot(0, call_comm_connect)
        self._exec_callback_wait(loop, timer)
        result = result_holder["value"]
        callback_result = self._login_callback_result
        timed_out_value = bool(timed_out["value"])
        self._login_event_loop = None
        self._login_callback_result = None
        if result is not None and result != 0:
            code = int(result)
            message = ERROR_MESSAGES.get(code, str(code))
        elif callback_result is None:
            code = LOGIN_EVENT_TIMEOUT_CODE
            message = f"OnEventConnect callback timeout after {wait_timeout_ms}ms"
        else:
            code, message = callback_result
        self._record_thread_audit(
            "OnEventConnect.wait",
            phase="RESULT",
            timed_out=timed_out_value,
            result_code=code,
        )
        self.connected.emit(code == 0, code, message)
        return int(result if result is not None else LOGIN_EVENT_TIMEOUT_CODE)

    def get_accounts(self) -> list[str]:
        raw = self.ocx.dynamicCall("GetLoginInfo(QString)", "ACCNO") or ""
        return [account for account in str(raw).split(";") if account]

    def get_user_id(self) -> str:
        return str(self.ocx.dynamicCall("GetLoginInfo(QString)", "USER_ID") or "")

    def get_server_gubun(self) -> str:
        return str(self.ocx.dynamicCall("GetLoginInfo(QString)", "GetServerGubun") or "").strip()

    def get_code_name(self, code: str) -> str:
        return str(self.ocx.dynamicCall("GetMasterCodeName(QString)", normalize_code(code)) or "")

    def get_master_last_price(self, code: str) -> str:
        try:
            return str(
                self.ocx.dynamicCall("GetMasterLastPrice(QString)", normalize_code(code)) or ""
            ).strip()
        except Exception:
            return ""

    def get_code_list_by_market(self, market_code: str) -> list[str]:
        raw = str(self.ocx.dynamicCall("GetCodeListByMarket(QString)", str(market_code)) or "")
        return [normalize_code(code) for code in raw.split(";") if str(code).strip()]

    def register_realtime(self, codes: Iterable[str], screen_no: str | None = None) -> None:
        code_list = [
            normalize_kiwoom_realtime_code(code)
            for code in codes
            if str(code or "").strip()
        ]
        screen_map = self._realtime_screen_code_map()
        for index in range(0, len(code_list), 100):
            chunk = code_list[index : index + 100]
            if not chunk:
                continue
            parsed_chunk = [parse_kiwoom_realtime_code(code) for code in chunk]
            chunk_screen_no = screen_no or f"{5000 + index // 100:04d}"
            screen_codes = screen_map.setdefault(chunk_screen_no, set())
            opt_type = "1" if screen_codes else "0"
            fid_string = realtime_stock_fid_string()
            self._record_thread_audit(
                "SetRealReg",
                phase="CALL",
                screen_no=chunk_screen_no,
                code_count=len(chunk),
                kiwoom_codes=list(chunk),
                fid_count=len(REALTIME_STOCK_FIDS),
                opt_type=opt_type,
            )
            result = self.ocx.dynamicCall(
                "SetRealReg(QString, QString, QString, QString)",
                chunk_screen_no,
                ";".join(chunk),
                fid_string,
                opt_type,
            )
            result_code = int(result or 0)
            self._record_thread_audit(
                "SetRealReg",
                phase="RESULT",
                screen_no=chunk_screen_no,
                kiwoom_codes=list(chunk),
                result_code=result_code,
                success=result_code >= 0,
            )
            self.realtime_registration_result.emit(
                {
                    "screen_no": chunk_screen_no,
                    "codes": [item.base_code for item in parsed_chunk],
                    "kiwoom_codes": [item.kiwoom_code for item in parsed_chunk],
                    "exchange_by_code": {
                        item.base_code: item.exchange for item in parsed_chunk
                    },
                    "fid_string": fid_string,
                    "fid_count": len(REALTIME_STOCK_FIDS),
                    "opt_type": opt_type,
                    "result_code": result_code,
                    "success": result_code >= 0,
                }
            )
            if result_code < 0:
                raise RuntimeError(
                    f"실시간 등록 실패: {ERROR_MESSAGES.get(result_code, str(result_code))}"
                )
            screen_codes.update(chunk)

    def register_market_index_realtime(
        self,
        codes: Iterable[str],
        *,
        screen_no: str = "5700",
    ) -> None:
        index_codes = _ordered_unique_market_index_codes(codes)
        if not index_codes:
            return
        kiwoom_codes = [kiwoom_market_index_code(code) for code in index_codes]
        screen_map = self._market_index_screen_code_map()
        screen_codes = screen_map.setdefault(str(screen_no), set())
        opt_type = "1" if screen_codes else "0"
        fid_string = market_index_realtime_fid_string()
        self._record_thread_audit(
            "SetRealReg",
            phase="CALL",
            registration_type="market_index",
            screen_no=str(screen_no),
            code_count=len(kiwoom_codes),
            index_codes=list(index_codes),
            kiwoom_codes=list(kiwoom_codes),
            fid_count=len(MARKET_INDEX_REALTIME_FIDS),
            parser_status=MARKET_INDEX_PARSER_STATUS,
            opt_type=opt_type,
        )
        result = self.ocx.dynamicCall(
            "SetRealReg(QString, QString, QString, QString)",
            str(screen_no),
            ";".join(kiwoom_codes),
            fid_string,
            opt_type,
        )
        result_code = int(result or 0)
        success = result_code >= 0
        self._record_thread_audit(
            "SetRealReg",
            phase="RESULT",
            registration_type="market_index",
            screen_no=str(screen_no),
            index_codes=list(index_codes),
            kiwoom_codes=list(kiwoom_codes),
            result_code=result_code,
            success=success,
            parser_status=MARKET_INDEX_PARSER_STATUS,
        )
        self.realtime_registration_result.emit(
            {
                "registration_type": "market_index",
                "screen_no": str(screen_no),
                "codes": list(index_codes),
                "index_codes": list(index_codes),
                "kiwoom_codes": list(kiwoom_codes),
                "index_code_by_kiwoom_code": {
                    kiwoom_codes[index]: index_code
                    for index, index_code in enumerate(index_codes)
                },
                "fid_string": fid_string,
                "fid_count": len(MARKET_INDEX_REALTIME_FIDS),
                "opt_type": opt_type,
                "result_code": result_code,
                "success": success,
                "parser_status": MARKET_INDEX_PARSER_STATUS,
                "parser_evidence": market_index_parser_evidence(),
            }
        )
        if result_code < 0:
            raise RuntimeError(
                f"지수 실시간 등록 실패: {ERROR_MESSAGES.get(result_code, str(result_code))}"
            )
        screen_codes.update(kiwoom_codes)
        for index, index_code in enumerate(index_codes):
            self._market_index_realtime_codes[kiwoom_codes[index]] = index_code

    def remove_market_index_realtime(
        self,
        codes: Iterable[str],
        *,
        screen_no: str | None = None,
    ) -> None:
        target_screen = screen_no or "ALL"
        screen_map = self._market_index_screen_code_map()
        for raw_code in codes:
            index_code = normalize_market_index_code(raw_code)
            kiwoom_code = kiwoom_market_index_code(index_code)
            self.ocx.dynamicCall("SetRealRemove(QString, QString)", target_screen, kiwoom_code)
            self._market_index_realtime_codes.pop(kiwoom_code, None)
            if target_screen == "ALL":
                for screen_codes in screen_map.values():
                    screen_codes.discard(kiwoom_code)
            else:
                screen_codes = screen_map.get(target_screen)
                if screen_codes is not None:
                    screen_codes.discard(kiwoom_code)
                    if not screen_codes:
                        screen_map.pop(target_screen, None)

    def remove_realtime(self, codes: Iterable[str], screen_no: str | None = None) -> None:
        target_screen = screen_no or "ALL"
        screen_map = self._realtime_screen_code_map()
        for raw_code in codes:
            code = normalize_code(raw_code)
            self.ocx.dynamicCall("SetRealRemove(QString, QString)", target_screen, code)
            if target_screen == "ALL":
                for screen_codes in screen_map.values():
                    screen_codes.discard(code)
            else:
                screen_codes = screen_map.get(target_screen)
                if screen_codes is not None:
                    screen_codes.discard(code)
                    if not screen_codes:
                        screen_map.pop(target_screen, None)

    def remove_all_realtime(self) -> None:
        self.ocx.dynamicCall("SetRealRemove(QString, QString)", "ALL", "ALL")
        self._realtime_screen_code_map().clear()
        self._market_index_screen_code_map().clear()
        self._market_index_realtime_codes.clear()

    def load_conditions(self, timeout_ms: int | None = None) -> int:
        from PyQt5.QtCore import QTimer

        self.condition_load_state = ConditionLoadState.LOADING
        self.condition_state_changed.emit(self.condition_load_state.value, "")
        wait_timeout_ms = int(timeout_ms or DEFAULT_CONDITION_EVENT_LOOP_TIMEOUT_MS)
        loop, timer, timed_out = self._prepare_callback_wait(
            "OnReceiveConditionVer",
            wait_timeout_ms,
        )
        self._condition_event_loop = loop
        self._condition_callback_result = None
        result_holder: dict[str, int | None] = {"value": None}

        def call_get_condition_load() -> None:
            self._record_thread_audit("GetConditionLoad", phase="CALL")
            result_holder["value"] = int(self.ocx.dynamicCall("GetConditionLoad()") or 0)
            self._record_thread_audit(
                "GetConditionLoad",
                phase="RESULT",
                result_code=result_holder["value"],
            )
            if result_holder["value"] <= 0 and self._condition_event_loop is not None:
                self._condition_event_loop.exit()

        QTimer.singleShot(0, call_get_condition_load)
        self._exec_callback_wait(loop, timer)
        result = int(result_holder["value"] or 0)
        if result <= 0:
            self._condition_event_loop = None
            self._condition_callback_result = None
            self.condition_load_state = ConditionLoadState.FAILED
            message = "GetConditionLoad failed"
            self.condition_state_changed.emit(self.condition_load_state.value, message)
            self.condition_load_result.emit(False, message)
            return result
        callback_result = self._condition_callback_result
        timed_out_value = bool(timed_out["value"])
        self._condition_event_loop = None
        self._condition_callback_result = None
        self._record_thread_audit(
            "OnReceiveConditionVer.wait",
            phase="RESULT",
            timed_out=timed_out_value,
            result_code=1 if callback_result and callback_result[0] else 0,
        )
        if callback_result is not None:
            self._emit_condition_load_result(*callback_result)
        return result

    def condition_name_list(self) -> list[ConditionInfo]:
        if self.condition_load_state != ConditionLoadState.LOADED:
            return []
        raw = str(self.ocx.dynamicCall("GetConditionNameList()") or "")
        self._conditions = parse_condition_name_list(raw)
        return list(self._conditions)

    def send_condition(
        self,
        screen_no: str,
        condition_name: str,
        condition_index: int,
        realtime: bool = True,
        search_type: int | None = None,
    ) -> int:
        n_search = int(search_type if search_type is not None else (1 if realtime else 0))
        self._record_thread_audit(
            "SendCondition",
            phase="CALL",
            screen_no=str(screen_no),
            condition_name=str(condition_name),
            condition_index=int(condition_index),
            search_type=n_search,
        )
        result = int(
            self.ocx.dynamicCall(
                "SendCondition(QString, QString, int, int)",
                str(screen_no),
                str(condition_name),
                int(condition_index),
                n_search,
            )
            or 0
        )
        self._record_thread_audit(
            "SendCondition",
            phase="RESULT",
            result_code=result,
            success=result >= 0,
        )
        return result

    def stop_condition(self, screen_no: str, condition_name: str, condition_index: int) -> None:
        self.ocx.dynamicCall(
            "SendConditionStop(QString, QString, int)",
            str(screen_no),
            str(condition_name),
            int(condition_index),
        )

    def set_input_value(self, input_name: str, value: str) -> None:
        self.ocx.dynamicCall("SetInputValue(QString, QString)", str(input_name), str(value))

    def comm_rq_data(self, rq_name: str, tr_code: str, prev_next: int, screen_no: str) -> int:
        return int(
            self.ocx.dynamicCall(
                "CommRqData(QString, QString, int, QString)",
                str(rq_name),
                str(tr_code),
                int(prev_next),
                str(screen_no),
            )
            or 0
        )

    def get_repeat_count(self, tr_code: str, rq_name: str) -> int:
        return int(
            self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", str(tr_code), str(rq_name))
            or 0
        )

    def get_comm_data(self, tr_code: str, rq_name: str, index: int, item_name: str) -> str:
        value = self.ocx.dynamicCall(
            "GetCommData(QString, QString, int, QString)",
            str(tr_code),
            str(rq_name),
            int(index),
            str(item_name),
        )
        return str(value or "").strip()

    def send_order(self, request: KiwoomOrderRequest) -> KiwoomOrderResult:
        result_code = int(
            self.ocx.dynamicCall(
                "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                [
                    request.tag,
                    "0101",
                    request.account,
                    int(request.order_type),
                    normalize_code(request.code),
                    int(request.quantity),
                    int(request.price),
                    request.hoga,
                    request.original_order_no,
                ],
            )
            or 0
        )
        result = KiwoomOrderResult(
            ok=result_code == 0,
            code=result_code,
            message=ERROR_MESSAGES.get(result_code, str(result_code)),
            request=request,
            raw={"result_code": result_code},
        )
        self.order_result.emit(result)
        return result

    def _on_event_connect(self, error_code: int) -> None:
        self._record_thread_audit(
            "OnEventConnect",
            phase="CALLBACK",
            error_code=int(error_code),
        )
        code = int(error_code)
        message = ERROR_MESSAGES.get(code, str(code))
        if self._login_event_loop is not None:
            self._login_callback_result = (code, message)
            self._login_event_loop.exit()
            return
        self.connected.emit(code == 0, code, message)

    def _on_receive_msg(self, screen_no: str, rq_name: str, tr_code: str, message: str) -> None:
        self._record_thread_audit(
            "OnReceiveMsg",
            phase="CALLBACK",
            screen_no=str(screen_no or ""),
            rq_name=str(rq_name or ""),
            tr_code=str(tr_code or ""),
        )
        self.message_received.emit(f"{screen_no} {rq_name} {tr_code}: {message}")

    def _on_receive_condition_ver(self, result: int, message: str) -> None:
        self._record_thread_audit(
            "OnReceiveConditionVer",
            phase="CALLBACK",
            result=int(result),
            message=str(message or ""),
        )
        success = int(result) == 1
        result_message = str(message or "")
        if self._condition_event_loop is not None:
            self._condition_callback_result = (success, result_message)
            self._condition_event_loop.exit()
            return
        self._emit_condition_load_result(success, result_message)

    def _on_receive_tr_condition(
        self,
        screen_no: str,
        code_list: str,
        condition_name: str,
        condition_index: int,
        next_flag: str,
    ) -> None:
        self._record_thread_audit(
            "OnReceiveTrCondition",
            phase="CALLBACK",
            screen_no=str(screen_no or ""),
            condition_name=str(condition_name or ""),
            condition_index=int(condition_index),
        )
        self.condition_tr_received.emit(
            str(screen_no or ""),
            str(code_list or ""),
            str(condition_name or ""),
            int(condition_index),
            str(next_flag or ""),
        )

    def _on_receive_real_condition(
        self,
        code: str,
        event_type: str,
        condition_name: str,
        condition_index: str,
    ) -> None:
        self._record_thread_audit(
            "OnReceiveRealCondition",
            phase="CALLBACK",
            code=normalize_code(code),
            event_type=str(event_type or ""),
            condition_name=str(condition_name or ""),
            condition_index=str(condition_index or ""),
        )
        try:
            index = int(condition_index)
        except (TypeError, ValueError):
            index = -1
        self.condition_real_received.emit(
            normalize_code(code),
            str(event_type or ""),
            str(condition_name or ""),
            index,
        )

    def _on_receive_tr_data(
        self,
        screen_no: str,
        rq_name: str,
        tr_code: str,
        record_name: str,
        prev_next: str,
        data_length: int,
        error_code: str,
        message: str,
        splm_msg: str,
    ) -> None:
        self._record_thread_audit(
            "OnReceiveTrData",
            phase="CALLBACK",
            screen_no=str(screen_no or ""),
            rq_name=str(rq_name or ""),
            tr_code=str(tr_code or ""),
            error_code=str(error_code or ""),
        )
        self.tr_data_received.emit(
            str(screen_no or ""),
            str(rq_name or ""),
            str(tr_code or ""),
            str(record_name or ""),
            str(prev_next or ""),
            int(data_length or 0),
            str(error_code or ""),
            str(message or ""),
            str(splm_msg or ""),
        )

    def _on_receive_real_data(self, code: str, real_type: str, real_data: str) -> None:
        raw_code = str(code or "")
        real_type_text = str(real_type or "").strip()
        market_index_code = self._market_index_code_for_callback(raw_code, real_type_text)
        is_market_index_callback = bool(market_index_code) or is_market_index_real_type(
            real_type_text
        )
        if is_market_index_callback:
            audit_code = market_index_code or str(raw_code or "").strip()
        else:
            audit_code = normalize_code(raw_code)
        self._record_thread_audit(
            "OnReceiveRealData",
            phase="CALLBACK",
            code=audit_code,
            kiwoom_code=raw_code,
            real_type=real_type_text,
            real_data_present=bool(str(real_data or "")),
            callback_asset_type="market_index" if is_market_index_callback else "stock",
        )
        if is_market_index_callback:
            if not market_index_code:
                self.realtime_data_received.emit(
                    str(raw_code or "").strip(),
                    real_type_text,
                    bool(str(real_data or "")),
                )
                self.realtime_parse_error.emit(
                    {
                        "reason": "INDEX_PARSE_ERROR",
                        "reason_codes": ["INDEX_PARSE_ERROR", "INDEX_CODE_UNMAPPED"],
                        "asset_type": "market_index",
                        "market_index": True,
                        "index_code": "",
                        "code": str(raw_code or "").strip(),
                        "kiwoom_code": raw_code,
                        "real_type": real_type_text,
                        "error": "INDEX_CODE_UNMAPPED",
                        "parser_status": "ERROR",
                        "parser_evidence": market_index_parser_evidence(),
                        "raw_fids_present": [],
                    }
                )
                return
            self.realtime_data_received.emit(
                market_index_code,
                real_type_text,
                bool(str(real_data or "")),
            )
            raw_values = {
                fid: self._market_index_real_raw(raw_code, fid)
                for fid in MARKET_INDEX_REALTIME_FIDS
            }
            try:
                payload = parse_market_index_tick_from_fids(
                    index_code=market_index_code,
                    index_name=market_index_name(market_index_code),
                    kiwoom_code=raw_code,
                    raw_fids=raw_values,
                    real_type=real_type_text,
                    real_data=str(real_data or ""),
                )
            except Exception as exc:
                self.realtime_parse_error.emit(
                    {
                        "reason": "INDEX_PARSE_ERROR",
                        "reason_codes": ["INDEX_PARSE_ERROR"],
                        "asset_type": "market_index",
                        "market_index": True,
                        "index_code": market_index_code,
                        "code": market_index_code,
                        "kiwoom_code": raw_code,
                        "real_type": real_type_text,
                        "error": str(exc),
                        "parser_status": "ERROR",
                        "parser_evidence": market_index_parser_evidence(),
                        "raw_fids_present": sorted(
                            fid
                            for fid, value in raw_values.items()
                            if str(value or "").strip()
                        ),
                    }
                )
                return
            self.market_index_tick_received.emit(payload)
            return
        parsed_code = parse_kiwoom_realtime_code(code)
        normalized_code = parsed_code.base_code
        self.realtime_data_received.emit(
            normalized_code,
            real_type_text,
            bool(str(real_data or "")),
        )
        if is_quote_real_type(real_type_text):
            raw_values = {
                fid: self._real_raw(code, fid)
                for fid in (FID_BEST_ASK, FID_BEST_BID, FID_TRADE_TIME)
            }
            payload = parse_quote_from_fids(
                code=parsed_code.kiwoom_code,
                name=self.get_code_name(normalized_code),
                raw_fids=raw_values,
                real_type=real_type_text,
                real_data=str(real_data or ""),
            )
            self.quote_received.emit(payload)
            return
        if not is_price_tick_real_type(real_type_text):
            return
        raw_values = {fid: self._real_raw(code, fid) for fid in REALTIME_STOCK_FIDS}
        try:
            payload = parse_price_tick_from_fids(
                code=parsed_code.kiwoom_code,
                name=self.get_code_name(normalized_code),
                raw_fids=raw_values,
                real_type=real_type_text,
                real_data=str(real_data or ""),
            )
        except Exception as exc:
            self.realtime_parse_error.emit(
                {
                    "code": normalized_code,
                    "real_type": real_type_text,
                    "error": str(exc),
                    "raw_fids_present": sorted(
                        fid for fid, value in raw_values.items() if str(value or "").strip()
                    ),
                }
            )
            return
        self.price_received.emit(
            normalized_code,
            payload["price"],
            payload["change_rate"],
            payload["volume"],
            payload["best_ask"],
            payload["best_bid"],
        )
        self.price_tick_received.emit(payload)

    def _on_receive_chejan_data(self, gubun: str, item_count: int, fid_list: str) -> None:
        self._record_thread_audit(
            "OnReceiveChejanData",
            phase="CALLBACK",
            gubun=str(gubun or ""),
            item_count=int(item_count or 0),
            fid_list=str(fid_list or ""),
        )
        raw_fids = read_chejan_raw(
            lambda fid: self._chejan(fid),
            gubun=str(gubun),
            fid_list=fid_list,
        )
        result = self.chejan_parser.parse(
            gubun=str(gubun or ""),
            item_count=int(item_count or 0),
            fid_list=str(fid_list or ""),
            raw_fids=raw_fids,
        )
        self.chejan_event_received.emit(result)
        if result.execution_payload is not None:
            self.execution_received.emit(dict(result.execution_payload))

    def _real_raw(self, code: str, fid: int) -> str:
        return str(
            self.ocx.dynamicCall("GetCommRealData(QString, int)", normalize_code(code), int(fid))
            or ""
        ).strip()

    def _market_index_real_raw(self, code: str, fid: int) -> str:
        return str(
            self.ocx.dynamicCall(
                "GetCommRealData(QString, int)",
                str(code or "").strip(),
                int(fid),
            )
            or ""
        ).strip()

    def _market_index_code_for_callback(self, raw_code: str, real_type: str) -> str:
        kiwoom_code = str(raw_code or "").strip()
        registered = getattr(self, "_market_index_realtime_codes", {})
        if isinstance(registered, dict) and kiwoom_code in registered:
            return str(registered[kiwoom_code])
        if is_market_index_real_type(real_type):
            return market_index_code_from_kiwoom_code(kiwoom_code)
        return ""

    def _chejan(self, fid: int) -> str:
        return str(self.ocx.dynamicCall("GetChejanData(int)", int(fid)) or "").strip()

    def _record_thread_audit(self, method: str, *, phase: str = "CALL", **extra: Any) -> None:
        payload = active_x_thread_audit(method, phase=phase, **extra)
        if not hasattr(self, "_pending_thread_audit_events"):
            self._pending_thread_audit_events = []
        if not hasattr(self, "active_x_thread_audit"):
            self.active_x_thread_audit = Signal()
        self._pending_thread_audit_events.append(payload)
        overflow = len(self._pending_thread_audit_events) - MAX_PENDING_THREAD_AUDIT_EVENTS
        if overflow > 0:
            del self._pending_thread_audit_events[:overflow]
        self.active_x_thread_audit.emit(payload)

    def drain_thread_audit_events(self) -> list[dict[str, Any]]:
        events = list(self._pending_thread_audit_events)
        self._pending_thread_audit_events.clear()
        return events

    def _emit_condition_load_result(self, success: bool, message: str = "") -> None:
        self.condition_load_state = (
            ConditionLoadState.LOADED if success else ConditionLoadState.FAILED
        )
        self.condition_state_changed.emit(self.condition_load_state.value, str(message or ""))
        self.condition_load_result.emit(bool(success), str(message or ""))
        if success:
            self.condition_loaded.emit(self.condition_name_list())

    def _prepare_callback_wait(
        self,
        method: str,
        timeout_ms: int,
    ) -> tuple[Any, Any, dict[str, bool]]:
        from PyQt5.QtCore import QEventLoop, QTimer

        loop = QEventLoop()
        timer = QTimer()
        timer.setSingleShot(True)
        timed_out = {"value": False}

        def on_timeout() -> None:
            timed_out["value"] = True
            self._record_thread_audit(method, phase="TIMEOUT", timeout_ms=int(timeout_ms))
            loop.exit()

        timer.timeout.connect(on_timeout)
        timer.setInterval(int(timeout_ms))
        self._record_thread_audit(method, phase="WAIT", timeout_ms=int(timeout_ms))
        return loop, timer, timed_out

    def _exec_callback_wait(self, loop: Any, timer: Any) -> None:
        timer.start()
        try:
            loop.exec_()
        finally:
            self._cleanup_callback_wait(timer)

    @staticmethod
    def _cleanup_callback_wait(timer: Any) -> None:
        try:
            if timer is not None and timer.isActive():
                timer.stop()
        except RuntimeError:
            pass

    def _realtime_screen_code_map(self) -> dict[str, set[str]]:
        screen_map = getattr(self, "_realtime_screen_codes", None)
        if not isinstance(screen_map, dict):
            screen_map = {}
            self._realtime_screen_codes = screen_map
        return screen_map

    def _market_index_screen_code_map(self) -> dict[str, set[str]]:
        screen_map = getattr(self, "_market_index_realtime_screen_codes", None)
        if not isinstance(screen_map, dict):
            screen_map = {}
            self._market_index_realtime_screen_codes = screen_map
        return screen_map

    def _create_ocx_widget(self, qax_widget_type: Any) -> Any:
        control_names = ("KHOPENAPI.KHOpenAPICtrl.1", "KHOpenAPI.KHOpenAPICtrl.1")
        last_widget = None
        widget_type = type("KiwoomAxWidget", (qax_widget_type,), {})
        for control_name in control_names:
            self._record_thread_audit(
                "QAxWidget.create",
                phase="CALL",
                control_name=control_name,
                creation_mode="subclass_setControl",
            )
            widget = widget_type()
            last_widget = widget
            self._record_thread_audit(
                "QAxWidget.setControl",
                phase="CALL",
                control_name=control_name,
            )
            set_result = bool(widget.setControl(control_name))
            is_null = bool(widget.isNull())
            self._record_thread_audit(
                "QAxWidget.setControl",
                phase="RESULT",
                control_name=control_name,
                result=set_result,
                is_null=is_null,
            )
            if set_result and not is_null:
                self._record_thread_audit(
                    "QAxWidget.create",
                    phase="RESULT",
                    control_name=control_name,
                    creation_mode="subclass_setControl",
                    is_null=False,
                )
                return widget
        if last_widget is not None:
            self._record_thread_audit(
                "QAxWidget.create_constructor_fallback",
                phase="CALL",
                control_name=control_names[0],
            )
            fallback_widget = qax_widget_type(control_names[0])
            if not bool(fallback_widget.isNull()):
                self._record_thread_audit(
                    "QAxWidget.create_constructor_fallback",
                    phase="RESULT",
                    control_name=control_names[0],
                    is_null=False,
                )
                return fallback_widget
        raise RuntimeError("Kiwoom OpenAPI+ ActiveX control is not registered.")


class MockKiwoomClient:
    def __init__(self) -> None:
        self.connected = Signal()
        self.price_received = Signal()
        self.price_tick_received = Signal()
        self.quote_received = Signal()
        self.market_index_tick_received = Signal()
        self.realtime_data_received = Signal()
        self.realtime_parse_error = Signal()
        self.realtime_registration_result = Signal()
        self.active_x_thread_audit = Signal()
        self.order_result = Signal()
        self.execution_received = Signal()
        self.chejan_event_received = Signal()
        self.message_received = Signal()
        self.condition_state_changed = Signal()
        self.condition_load_result = Signal()
        self.condition_loaded = Signal()
        self.condition_tr_received = Signal()
        self.condition_real_received = Signal()
        self.tr_data_received = Signal()
        self.condition_load_state = ConditionLoadState.IDLE
        self.registered_codes: set[str] = set()
        self.registered_market_index_codes: set[str] = set()
        self.registered_market_index_kiwoom_codes: set[str] = set()
        self.removed_codes: list[str] = []
        self.removed_market_index_codes: list[str] = []
        self.orders: list[KiwoomOrderRequest] = []
        self.send_condition_calls: list[dict[str, Any]] = []
        self.stop_condition_calls: list[dict[str, Any]] = []
        self._conditions: list[ConditionInfo] = []
        self._tr_inputs: dict[str, str] = {}
        self._tr_rows: list[dict[str, str]] = []
        self._names = {
            "005930": "삼성전자",
            "000660": "SK하이닉스",
            "035420": "NAVER",
            "KOSPI": "KOSPI",
            "KOSDAQ": "KOSDAQ",
        }
        self.server_gubun = "1"
        self._pending_thread_audit_events: list[dict[str, Any]] = []

    def login(self) -> int:
        self.connected.emit(True, 0, "MOCK 로그인 성공")
        return 0

    def get_accounts(self) -> list[str]:
        return ["1234567890"]

    def get_user_id(self) -> str:
        return "MOCK_USER"

    def get_server_gubun(self) -> str:
        return self.server_gubun

    def get_code_name(self, code: str) -> str:
        normalized = normalize_code(code)
        return self._names.get(normalized, f"MOCK-{normalized}")

    def get_master_last_price(self, code: str) -> str:
        return "70000" if normalize_code(code) else ""

    def get_code_list_by_market(self, market_code: str) -> list[str]:
        if str(market_code) == "10":
            return ["035420"]
        return ["005930", "000660"]

    def register_realtime(self, codes: Iterable[str], screen_no: str | None = None) -> None:
        kiwoom_codes = [
            normalize_kiwoom_realtime_code(code)
            for code in codes
            if str(code or "").strip()
        ]
        parsed_codes = [parse_kiwoom_realtime_code(code) for code in kiwoom_codes]
        self.registered_codes.update(kiwoom_codes)
        if kiwoom_codes:
            self.realtime_registration_result.emit(
                {
                    "screen_no": screen_no or "5000",
                    "codes": [item.base_code for item in parsed_codes],
                    "kiwoom_codes": kiwoom_codes,
                    "exchange_by_code": {
                        item.base_code: item.exchange for item in parsed_codes
                    },
                    "fid_string": realtime_stock_fid_string(),
                    "fid_count": len(REALTIME_STOCK_FIDS),
                    "opt_type": "0",
                    "result_code": 0,
                    "success": True,
                }
            )

    def register_market_index_realtime(
        self,
        codes: Iterable[str],
        *,
        screen_no: str = "5700",
    ) -> None:
        index_codes = _ordered_unique_market_index_codes(codes)
        kiwoom_codes = [kiwoom_market_index_code(code) for code in index_codes]
        self.registered_market_index_codes.update(index_codes)
        self.registered_market_index_kiwoom_codes.update(kiwoom_codes)
        if kiwoom_codes:
            self.realtime_registration_result.emit(
                {
                    "registration_type": "market_index",
                    "screen_no": str(screen_no),
                    "codes": list(index_codes),
                    "index_codes": list(index_codes),
                    "kiwoom_codes": kiwoom_codes,
                    "index_code_by_kiwoom_code": {
                        kiwoom_codes[index]: index_code
                        for index, index_code in enumerate(index_codes)
                    },
                    "fid_string": market_index_realtime_fid_string(),
                    "fid_count": len(MARKET_INDEX_REALTIME_FIDS),
                    "opt_type": "0",
                    "result_code": 0,
                    "success": True,
                    "parser_status": MARKET_INDEX_PARSER_STATUS,
                    "parser_evidence": market_index_parser_evidence(),
                }
            )

    def remove_market_index_realtime(
        self,
        codes: Iterable[str],
        *,
        screen_no: str | None = None,
    ) -> None:
        del screen_no
        for code in codes:
            normalized = normalize_market_index_code(code)
            kiwoom_code = kiwoom_market_index_code(normalized)
            self.registered_market_index_codes.discard(normalized)
            self.registered_market_index_kiwoom_codes.discard(kiwoom_code)
            self.removed_market_index_codes.append(normalized)

    def remove_realtime(self, codes: Iterable[str], screen_no: str | None = None) -> None:
        for code in codes:
            normalized = normalize_code(code)
            self.registered_codes.discard(normalized)
            self.removed_codes.append(normalized)

    def remove_all_realtime(self) -> None:
        self.registered_codes.clear()
        self.registered_market_index_codes.clear()
        self.registered_market_index_kiwoom_codes.clear()

    def drain_thread_audit_events(self) -> list[dict[str, Any]]:
        events = list(self._pending_thread_audit_events)
        self._pending_thread_audit_events.clear()
        return events

    def load_conditions(self) -> int:
        self.condition_load_state = ConditionLoadState.LOADING
        self.condition_state_changed.emit(self.condition_load_state.value, "")
        return 1

    def condition_name_list(self) -> list[ConditionInfo]:
        return list(self._conditions)

    def set_conditions(self, conditions: Iterable[tuple[int, str]]) -> None:
        self._conditions = [
            ConditionInfo(index=int(index), name=str(name)) for index, name in conditions
        ]

    def emit_condition_load_result(self, success: bool = True, message: str = "") -> None:
        self.condition_load_state = (
            ConditionLoadState.LOADED if success else ConditionLoadState.FAILED
        )
        self.condition_load_result.emit(bool(success), str(message or ""))
        if success:
            self.condition_loaded.emit(list(self._conditions))

    def send_condition(
        self,
        screen_no: str,
        condition_name: str,
        condition_index: int,
        realtime: bool = True,
        search_type: int | None = None,
    ) -> int:
        self.send_condition_calls.append(
            {
                "screen_no": str(screen_no),
                "condition_name": str(condition_name),
                "condition_index": int(condition_index),
                "realtime": bool(realtime),
                "search_type": search_type,
            }
        )
        return 1

    def stop_condition(self, screen_no: str, condition_name: str, condition_index: int) -> None:
        self.stop_condition_calls.append(
            {
                "screen_no": str(screen_no),
                "condition_name": str(condition_name),
                "condition_index": int(condition_index),
            }
        )
        return None

    def set_input_value(self, input_name: str, value: str) -> None:
        self._tr_inputs[str(input_name)] = str(value)

    def comm_rq_data(self, rq_name: str, tr_code: str, prev_next: int, screen_no: str) -> int:
        self.tr_data_received.emit(
            str(screen_no),
            str(rq_name),
            str(tr_code),
            str(rq_name),
            "",
            0,
            "",
            "",
            "",
        )
        return 0

    def get_repeat_count(self, tr_code: str, rq_name: str) -> int:
        return len(self._tr_rows)

    def get_comm_data(self, tr_code: str, rq_name: str, index: int, item_name: str) -> str:
        if index < 0 or index >= len(self._tr_rows):
            return ""
        return str(self._tr_rows[index].get(str(item_name), "") or "")

    def set_tr_rows(self, rows: list[dict[str, str]]) -> None:
        self._tr_rows = [dict(row) for row in rows]

    def send_order(self, request: KiwoomOrderRequest) -> KiwoomOrderResult:
        self.orders.append(request)
        result = KiwoomOrderResult(
            ok=True,
            code=0,
            message="MOCK 주문 정상처리",
            request=request,
            raw={"mock": True},
        )
        self.order_result.emit(result)
        return result

    def emit_market_index_tick(
        self,
        *,
        index_code: str = "KOSPI",
        price: float = 2800.0,
        change_rate: float = 0.0,
        change_value: float = 0.0,
        trade_time: datetime | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized = normalize_market_index_code(index_code)
        payload = BrokerMarketIndexTick(
            index_code=normalized,
            index_name=market_index_name(normalized),
            price=float(price),
            change_rate=float(change_rate),
            change_value=float(change_value),
            trade_time=trade_time or utc_now(),
            metadata={
                "source": "MOCK_KIWOOM_MARKET_INDEX",
                "parser_status": "MOCK",
                **dict(metadata or {}),
            },
        ).to_dict()
        self.realtime_data_received.emit(normalized, "MOCK_MARKET_INDEX", True)
        self.market_index_tick_received.emit(payload)
        return payload


def normalize_code(value: object) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("A") and len(text) == 7:
        text = text[1:]
    digits = "".join(ch for ch in text if ch.isdigit())
    if digits:
        return digits[-6:].zfill(6)
    return validate_stock_code(text)


def normalize_market_index_code(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in MARKET_INDEX_KIWOOM_CODE_BY_INDEX.values():
        return market_index_code_from_kiwoom_code(text)
    allowed = {code.upper() for code in DEFAULT_ALLOWED_INDEX_CODES}
    if text not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise ValueError(f"index_code must be one of: {allowed_text}")
    return text


def _ordered_unique_market_index_codes(codes: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if not str(code or "").strip():
            continue
        normalized = normalize_market_index_code(code)
        if normalized not in MARKET_INDEX_KIWOOM_CODE_BY_INDEX:
            raise ValueError(f"Kiwoom market index code mapping is not configured: {normalized}")
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def market_index_code_from_kiwoom_code(value: object) -> str:
    text = str(value or "").strip()
    reverse = {
        kiwoom_code: index_code
        for index_code, kiwoom_code in MARKET_INDEX_KIWOOM_CODE_BY_INDEX.items()
    }
    return reverse.get(text, "")


def kiwoom_market_index_code(index_code: object) -> str:
    normalized = normalize_market_index_code(index_code)
    kiwoom_code = MARKET_INDEX_KIWOOM_CODE_BY_INDEX.get(normalized)
    if not kiwoom_code:
        raise ValueError(f"Kiwoom market index code mapping is not configured: {normalized}")
    return kiwoom_code


def market_index_name(index_code: object) -> str:
    normalized = normalize_market_index_code(index_code)
    return MARKET_INDEX_NAME_BY_CODE.get(normalized, normalized)


def normalize_realtime_exchange(value: object) -> str:
    text = str(value or "KRX").strip().upper()
    aliases = {
        "": "KRX",
        "K": "KRX",
        "KRX": "KRX",
        "N": "NXT",
        "NX": "NXT",
        "NXT": "NXT",
        "A": "ALL",
        "AL": "ALL",
        "ALL": "ALL",
        "SOR": "ALL",
        "INTEGRATED": "ALL",
    }
    exchange = aliases.get(text)
    if exchange is None:
        raise ValueError(f"unsupported realtime exchange: {value}")
    return exchange


def parse_kiwoom_realtime_code(value: object) -> KiwoomRealtimeCode:
    text = str(value or "").strip().upper()
    exchange = "KRX"
    for suffix, suffix_exchange in REALTIME_SUFFIX_EXCHANGES.items():
        if text.endswith(suffix):
            text = text[: -len(suffix)]
            exchange = suffix_exchange
            break
    base_code = normalize_code(text)
    suffix = REALTIME_EXCHANGE_SUFFIXES[exchange]
    return KiwoomRealtimeCode(
        base_code=base_code,
        kiwoom_code=f"{base_code}{suffix}",
        exchange=exchange,
    )


def normalize_kiwoom_realtime_code(value: object) -> str:
    return parse_kiwoom_realtime_code(value).kiwoom_code


def realtime_code_for_exchange(code: object, exchange: object = "KRX") -> str:
    base_code = normalize_code(code)
    normalized_exchange = normalize_realtime_exchange(exchange)
    return f"{base_code}{REALTIME_EXCHANGE_SUFFIXES[normalized_exchange]}"


def parse_condition_name_list(raw: str) -> list[ConditionInfo]:
    conditions: list[ConditionInfo] = []
    for item in str(raw or "").split(";"):
        if not item or "^" not in item:
            continue
        index_text, name = item.split("^", 1)
        try:
            index = int(index_text)
        except ValueError:
            continue
        if name.strip():
            conditions.append(ConditionInfo(index=index, name=name.strip()))
    return conditions


def realtime_stock_fid_string() -> str:
    return ";".join(str(fid) for fid in REALTIME_STOCK_FIDS)


def market_index_realtime_fid_string() -> str:
    return ";".join(str(fid) for fid in MARKET_INDEX_REALTIME_FIDS)


def market_index_parser_evidence() -> dict[str, Any]:
    return {
        "mapping_status": "UNVERIFIED_PILOT",
        "requires_koa_studio_confirmation": True,
        "source": "adapter_pilot_candidate_fids",
        "index_code_map": dict(MARKET_INDEX_KIWOOM_CODE_BY_INDEX),
        "field_fids": {
            "price": FID_CURRENT_PRICE,
            "change_value": FID_CHANGE_VALUE,
            "change_rate": FID_CHANGE_RATE,
            "trade_time": FID_TRADE_TIME,
        },
        "real_types": sorted(MARKET_INDEX_REAL_TYPES),
    }


def is_price_tick_real_type(real_type: str) -> bool:
    return str(real_type or "").strip() in PRICE_TICK_REAL_TYPES


def is_quote_real_type(real_type: str) -> bool:
    return str(real_type or "").strip() in QUOTE_REAL_TYPES


def is_market_index_real_type(real_type: str) -> bool:
    return str(real_type or "").strip() in MARKET_INDEX_REAL_TYPES


def parse_quote_from_fids(
    *,
    code: str,
    name: str,
    raw_fids: Mapping[int | str, Any],
    real_type: str = "",
    real_data: str = "",
) -> dict[str, Any]:
    parsed_code = parse_kiwoom_realtime_code(code)
    raw_values = {int(fid): str(value or "").strip() for fid, value in dict(raw_fids).items()}
    reason_codes: list[str] = []
    best_ask, ask_ok = _parse_real_int(raw_values.get(FID_BEST_ASK))
    best_bid, bid_ok = _parse_real_int(raw_values.get(FID_BEST_BID))
    if not ask_ok or best_ask <= 0:
        reason_codes.append("BEST_ASK_MISSING")
        best_ask = 0
    if not bid_ok or best_bid <= 0:
        reason_codes.append("BEST_BID_MISSING")
        best_bid = 0
    if best_ask > 0 and best_bid > 0 and best_ask < best_bid:
        reason_codes.append("BEST_BID_ASK_ADJUSTED")
        best_ask = best_bid

    trade_time_raw = raw_values.get(FID_TRADE_TIME, "")
    spread_price = max(0, best_ask - best_bid) if best_ask > 0 and best_bid > 0 else 0
    spread_ticks = _spread_ticks(best_bid, best_ask)
    return {
        "code": parsed_code.base_code,
        "name": str(name or "").strip() or parsed_code.base_code,
        "best_ask": best_ask,
        "best_bid": best_bid,
        "spread_ticks": spread_ticks,
        "trade_time": datetime_to_wire(parse_kiwoom_trade_time(trade_time_raw)),
        "metadata": {
            "real_type": str(real_type or ""),
            "trade_time": str(trade_time_raw or "").strip(),
            "raw_fids_present": [
                fid for fid, value in sorted(raw_values.items()) if _has_real_value(value)
            ],
            "reason_codes": sorted(set(reason_codes)),
            "spread_price": spread_price,
            "quote_only": True,
            "exchange": parsed_code.exchange,
            "kiwoom_code": parsed_code.kiwoom_code,
        },
        "quote_only": True,
        "not_price_tick": True,
    }


def parse_market_index_tick_from_fids(
    *,
    index_code: str,
    index_name: str = "",
    kiwoom_code: str = "",
    raw_fids: Mapping[int | str, Any],
    real_type: str = "",
    real_data: str = "",
) -> dict[str, Any]:
    normalized_index_code = normalize_market_index_code(index_code)
    raw_values = {int(fid): str(value or "").strip() for fid, value in dict(raw_fids).items()}
    reason_codes: list[str] = []
    parse_fallback = False

    price, ok = _parse_real_float(raw_values.get(FID_CURRENT_PRICE), abs_value=True)
    parse_fallback = parse_fallback or not ok
    change_value, ok = _parse_real_float(raw_values.get(FID_CHANGE_VALUE), abs_value=False)
    parse_fallback = parse_fallback or not ok
    change_rate, ok = _parse_real_float(raw_values.get(FID_CHANGE_RATE), abs_value=False)
    parse_fallback = parse_fallback or not ok

    if price <= 0:
        reason_codes.append("INDEX_PRICE_MISSING")
    if FID_CHANGE_VALUE not in raw_values or not _has_real_value(raw_values.get(FID_CHANGE_VALUE)):
        reason_codes.append("INDEX_CHANGE_VALUE_MISSING")
    if FID_CHANGE_RATE not in raw_values or not _has_real_value(raw_values.get(FID_CHANGE_RATE)):
        reason_codes.append("INDEX_CHANGE_RATE_MISSING")
    if parse_fallback:
        reason_codes.append("INDEX_REAL_PARSE_FALLBACK")

    trade_time_raw = raw_values.get(FID_TRADE_TIME, "")
    if not _has_real_value(trade_time_raw):
        reason_codes.append("INDEX_TRADE_TIME_MISSING")

    metadata: dict[str, Any] = {
        "source": "KIWOOM_REALTIME_MARKET_INDEX",
        "real_type": str(real_type or ""),
        "kiwoom_code": str(kiwoom_code or "").strip(),
        "trade_time": str(trade_time_raw or "").strip(),
        "raw_fids_present": [
            fid for fid, value in sorted(raw_values.items()) if _has_real_value(value)
        ],
        "reason_codes": sorted(set(reason_codes)),
        "parser_status": MARKET_INDEX_PARSER_STATUS,
        "parser_evidence": market_index_parser_evidence(),
    }
    if real_data:
        metadata["real_data_present"] = True

    if price <= 0:
        raise ValueError("INDEX_PRICE_MISSING")

    tick = BrokerMarketIndexTick(
        index_code=normalized_index_code,
        index_name=str(index_name or "").strip() or market_index_name(normalized_index_code),
        price=price,
        change_rate=change_rate,
        change_value=change_value,
        trade_time=parse_kiwoom_trade_time(trade_time_raw),
        metadata=metadata,
    )
    return tick.to_dict()


def parse_price_tick_from_fids(
    *,
    code: str,
    name: str,
    raw_fids: Mapping[int | str, Any],
    real_type: str = "",
    real_data: str = "",
) -> dict[str, Any]:
    parsed_code = parse_kiwoom_realtime_code(code)
    raw_values = {int(fid): str(value or "").strip() for fid, value in dict(raw_fids).items()}
    reason_codes: list[str] = []
    parse_fallback = False

    price, ok = _parse_real_int(raw_values.get(FID_CURRENT_PRICE))
    parse_fallback = parse_fallback or not ok
    change_rate, ok = _parse_real_float(raw_values.get(FID_CHANGE_RATE), abs_value=False)
    parse_fallback = parse_fallback or not ok
    volume, ok = _parse_real_int(raw_values.get(FID_ACC_VOLUME))
    parse_fallback = parse_fallback or not ok
    trade_value_raw, ok = _parse_real_float(raw_values.get(FID_ACC_TRADE_VALUE), abs_value=True)
    parse_fallback = parse_fallback or not ok
    open_price, ok = _parse_real_int(raw_values.get(FID_OPEN_PRICE))
    parse_fallback = parse_fallback or not ok
    day_high, ok = _parse_real_int(raw_values.get(FID_HIGH_PRICE))
    parse_fallback = parse_fallback or not ok
    day_low, ok = _parse_real_int(raw_values.get(FID_LOW_PRICE))
    parse_fallback = parse_fallback or not ok
    best_ask, ok = _parse_real_int(raw_values.get(FID_BEST_ASK))
    parse_fallback = parse_fallback or not ok
    best_bid, ok = _parse_real_int(raw_values.get(FID_BEST_BID))
    parse_fallback = parse_fallback or not ok
    execution_strength, ok = _parse_real_float(
        raw_values.get(FID_EXECUTION_STRENGTH), abs_value=False
    )
    parse_fallback = parse_fallback or not ok

    if price <= 0:
        reason_codes.append("PRICE_MISSING")
        price = 1
    if trade_value_raw > 0:
        trade_value = int(trade_value_raw * 1_000_000)
        trade_value_unit = "million_krw"
    else:
        trade_value = 0
        trade_value_unit = ""
    if trade_value <= 0:
        reason_codes.append("TRADE_VALUE_MISSING")
        if price > 0 and volume > 0:
            trade_value = int(price * volume)
            reason_codes.append("TURNOVER_ESTIMATED")
    if execution_strength <= 0:
        reason_codes.append("EXECUTION_STRENGTH_MISSING")
        execution_strength = 0.0
    if day_high <= 0 or day_low <= 0:
        reason_codes.append("DAY_HIGH_LOW_MISSING")
        day_high = max(day_high, price)
        day_low = max(day_low, price)
    if day_high < day_low:
        reason_codes.append("DAY_HIGH_LOW_ADJUSTED")
        day_high, day_low = day_low, day_high
    if best_ask <= 0 or best_bid <= 0:
        reason_codes.append("BEST_BID_ASK_MISSING")
    if best_ask > 0 and best_bid > 0 and best_ask < best_bid:
        reason_codes.append("BEST_BID_ASK_ADJUSTED")
        best_ask = best_bid
    if parse_fallback:
        reason_codes.append("REAL_PARSE_FALLBACK")

    spread_price = max(0, best_ask - best_bid) if best_ask > 0 and best_bid > 0 else 0
    spread_ticks = _spread_ticks(best_bid, best_ask)
    if spread_price > 0 and spread_ticks == 0:
        reason_codes.append("SPREAD_APPROXIMATED")

    trade_time_raw = raw_values.get(FID_TRADE_TIME, "")
    metadata = {
        "real_type": str(real_type or ""),
        "trade_time": str(trade_time_raw or "").strip(),
        "raw_fids_present": [
            fid for fid, value in sorted(raw_values.items()) if _has_real_value(value)
        ],
        "reason_codes": sorted(set(reason_codes)),
        "spread_price": spread_price,
        "open_price": open_price,
        "exchange": parsed_code.exchange,
        "kiwoom_code": parsed_code.kiwoom_code,
    }
    if real_data:
        metadata["real_data_present"] = True
    if trade_value_unit:
        metadata["trade_value_unit"] = trade_value_unit

    tick = BrokerPriceTick(
        code=parsed_code.base_code,
        name=str(name or "").strip() or parsed_code.base_code,
        price=price,
        change_rate=change_rate,
        volume=volume,
        trade_value=trade_value,
        execution_strength=max(0.0, execution_strength),
        best_bid=best_bid,
        best_ask=best_ask,
        spread_ticks=spread_ticks,
        day_high=day_high,
        day_low=day_low,
        trade_time=parse_kiwoom_trade_time(trade_time_raw),
    )
    return {**tick.to_dict(), "metadata": metadata}


def condition_event_payload(
    *,
    code: str,
    event_type: str,
    condition_name: str,
    condition_index: int,
    name: str = "",
    price: int | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_index = int(condition_index)
    normalized_name = str(condition_name or "").strip() or f"Kiwoom Condition {normalized_index}"
    action = _condition_action(event_type)
    payload: dict[str, Any] = {
        "condition_id": f"kiwoom_condition_{normalized_index}",
        "condition_name": normalized_name,
        "code": normalize_code(code),
        "name": str(name or "").strip() or normalize_code(code),
        "action": action,
        "metadata": {
            "condition_index": normalized_index,
            "kiwoom_event_type": str(event_type or ""),
            "source": "KIWOOM_CONDITION",
            **dict(metadata or {}),
        },
    }
    if price is not None and int(price) > 0:
        payload["price"] = int(price)
    return payload


def read_chejan_raw(
    get_value: Callable[[int], str],
    *,
    gubun: str,
    fid_list: str,
) -> dict[str, str]:
    fids = set(parse_fid_list(fid_list))
    if str(gubun) == "0":
        fids.update(ORDER_CHEJAN_FIDS)
    elif str(gubun) == "1":
        fids.update(BALANCE_CHEJAN_FIDS)
    result: dict[str, str] = {}
    for fid in sorted(fids):
        try:
            value = str(get_value(int(fid)) or "").strip()
        except Exception as exc:
            value = ""
            result[f"{fid}:read_error"] = str(exc)
        result[str(fid)] = value
    return result


def parse_fid_list(fid_list: str) -> list[int]:
    result: list[int] = []
    for token in str(fid_list or "").replace(",", ";").split(";"):
        text = token.strip()
        if not text:
            continue
        try:
            result.append(int(text))
        except ValueError:
            continue
    return result


def parse_kiwoom_trade_time(value: object) -> datetime:
    text = str(value or "").strip()
    if text.isdigit() and 1 <= len(text) <= 6:
        padded = text.zfill(6)
        kst = timezone(timedelta(hours=9))
        now = datetime.now(kst)
        trade_time = now.replace(
            hour=int(padded[:2]),
            minute=int(padded[2:4]),
            second=int(padded[4:6]),
            microsecond=0,
        )
        return trade_time.astimezone(UTC)
    return utc_now()


def broker_env_from_server_gubun(value: object) -> str:
    text = str(value or "").strip().upper()
    if text in {"1", "SIM", "SIMULATION", "MOCK", "PAPER", "PAPER_TRADING", "LIVE_SIM", "TEST"}:
        return "SIMULATION"
    if text in {"0", "REAL", "LIVE", "PROD", "PRODUCTION", "LIVE_REAL"}:
        return "REAL"
    return "UNKNOWN"


def _parse_order_chejan(common: Mapping[str, Any]) -> KiwoomChejanParseResult:
    raw = dict(common["raw_fids"])
    account = _text(raw, 9201)
    order_no = _text(raw, 9203)
    code = normalize_code(_text(raw, 9001)) if _text(raw, 9001) else ""
    side = _normalize_order_side(side_code=_text(raw, 907), order_gubun=_text(raw, 905))
    order_status = _text(raw, 913)
    execution_price = _parse_int(_text(raw, 910))
    execution_quantity = _parse_int(_text(raw, 915)) or _parse_int(_text(raw, 911))
    order_quantity = _parse_int(_text(raw, 900))
    remaining_quantity = _parse_int(_text(raw, 902))
    event_time = _text(raw, 908)
    execution_id = _text(raw, 909)
    fill_like = execution_quantity > 0 and execution_price > 0
    if not fill_like and "체결" in order_status:
        fill_like = True
    event_kind = "order_fill" if fill_like else "order_status_snapshot"
    if "거부" in order_status or "거절" in order_status or _text(raw, 919).strip("0 "):
        event_kind = "order_rejected"
    elif "취소" in order_status:
        event_kind = "order_cancelled"
    timestamp = _kiwoom_event_timestamp(event_time)
    payload = {
        **dict(common),
        "event_kind": event_kind,
        "account_id": account,
        "account": account,
        "code": code,
        "name": _text(raw, 302),
        "order_no": order_no,
        "broker_order_no": order_no,
        "broker_order_id": order_no,
        "original_order_no": _text(raw, 904),
        "order_status": order_status,
        "order_gubun": _text(raw, 905),
        "side": side,
        "side_code": _text(raw, 907),
        "quantity": order_quantity or execution_quantity,
        "order_quantity": order_quantity,
        "order_price": _parse_int(_text(raw, 901)),
        "remaining_quantity": remaining_quantity,
        "execution_id": execution_id,
        "execution_price": execution_price,
        "execution_quantity": execution_quantity,
        "price": execution_price or _parse_int(_text(raw, 901)),
        "event_time": event_time,
        "timestamp": timestamp,
        "reject_reason": _text(raw, 919),
        "parser_status": "OK" if code and order_no else "DEGRADED",
        "parser_warning_codes": [] if code and order_no else ["REQUIRED_FIELD_MISSING"],
    }
    execution_payload = None
    if fill_like and code and order_no and execution_quantity > 0 and execution_price > 0:
        execution_payload = {
            "execution_id": execution_id or f"kiwoom-{order_no}-{event_time}",
            "broker_order_id": order_no,
            "broker_order_no": order_no,
            "account_id": account,
            "code": code,
            "side": side or "BUY",
            "quantity": execution_quantity,
            "price": execution_price,
            "remaining_quantity": remaining_quantity,
            "executed_at": timestamp,
            "metadata": {
                "source": "KIWOOM_CHEJAN",
                "event_kind": event_kind,
                "parser_version": "kiwoom_chejan_v2_adapter",
                "raw_fids": raw,
            },
        }
    return KiwoomChejanParseResult(
        gubun=str(common["gubun"]),
        gateway_event_type="kiwoom_order_chejan",
        event_kind=event_kind,
        payload=payload,
        execution_payload=execution_payload,
    )


def _parse_balance_chejan(common: Mapping[str, Any]) -> KiwoomChejanParseResult:
    raw = dict(common["raw_fids"])
    payload = {
        **dict(common),
        "event_kind": "position_delta",
        "account_id": _text(raw, 9201),
        "account": _text(raw, 9201),
        "code": normalize_code(_text(raw, 9001)) if _text(raw, 9001) else "",
        "name": _text(raw, 302),
        "current_price": _parse_int(_text(raw, 10)),
        "position_quantity": _parse_int(_text(raw, 930)),
        "available_quantity": _parse_int(_text(raw, 933)),
        "average_buy_price": _parse_int(_text(raw, 931)),
        "parser_status": "OK",
        "parser_warning_codes": [],
    }
    return KiwoomChejanParseResult(
        gubun=str(common["gubun"]),
        gateway_event_type="kiwoom_balance_chejan",
        event_kind="position_delta",
        payload=payload,
    )


def _condition_action(event_type: str) -> str:
    normalized = str(event_type or "").strip().upper()
    if normalized in {"I", "INCLUDE", "ENTER", "INSERT"}:
        return "ENTER"
    if normalized in {"D", "REMOVE", "EXIT", "DELETE"}:
        return "EXIT"
    return "ENTER"


def _parse_real_int(value: Any) -> tuple[int, bool]:
    text = str(value or "").strip().replace(",", "").replace("+", "")
    if not text:
        return 0, True
    try:
        return abs(int(float(text))), True
    except (TypeError, ValueError):
        return 0, False


def _parse_real_float(value: Any, *, abs_value: bool) -> tuple[float, bool]:
    text = str(value or "").strip().replace(",", "").replace("+", "").replace("%", "")
    if not text:
        return 0.0, True
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return 0.0, False
    return (abs(parsed) if abs_value else parsed), True


def _has_real_value(value: Any) -> bool:
    return str(value or "").strip() != ""


def _spread_ticks(best_bid: int, best_ask: int) -> int:
    if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
        return 0
    tick_size = _krx_stock_tick_size(best_bid or best_ask)
    if tick_size <= 0:
        return 0
    return max(0, int(round((best_ask - best_bid) / tick_size)))


def _krx_stock_tick_size(price: int) -> int:
    value = abs(int(price or 0))
    if value < 2_000:
        return 1
    if value < 5_000:
        return 5
    if value < 20_000:
        return 10
    if value < 50_000:
        return 50
    if value < 200_000:
        return 100
    if value < 500_000:
        return 500
    return 1_000


def _text(raw: Mapping[str, str], fid: int) -> str:
    return str(raw.get(str(fid), "") or "").strip()


def _parse_int(value: object) -> int:
    text = str(value or "").strip().replace(",", "").replace("+", "")
    if not text:
        return 0
    try:
        return abs(int(float(text)))
    except (TypeError, ValueError):
        return 0


def _normalize_order_side(*, side_code: str, order_gubun: str) -> str:
    text = f"{side_code} {order_gubun}".upper()
    if str(side_code).strip() == "2" or "매수" in text or "BUY" in text:
        return "BUY"
    if str(side_code).strip() == "1" or "매도" in text or "SELL" in text:
        return "SELL"
    return ""


def _kiwoom_event_timestamp(value: str) -> str:
    text = str(value or "").strip()
    if text.isdigit() and 1 <= len(text) <= 6:
        return datetime_to_wire(parse_kiwoom_trade_time(text))
    return datetime_to_wire(utc_now())


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}
