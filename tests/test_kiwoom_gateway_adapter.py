from __future__ import annotations

import importlib
import inspect
import json
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from apps.kiwoom_gateway import parse_args, request_kiwoom_login
from domain.broker.commands import GatewayCommand
from domain.broker.condition_profiles import (
    ConditionProfile,
    ConditionRole,
    ConditionSessionProfile,
    PriceSubscribePolicy,
)
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.market_index import BrokerMarketIndexTick
from domain.broker.tr import BrokerTrResponse
from gateway.core_io_worker import CoreIoWorker
from gateway.event_factory import make_command_started_event
from gateway.kiwoom_client import (
    FID_ACC_TRADE_VALUE,
    FID_ACC_VOLUME,
    FID_BEST_ASK,
    FID_BEST_BID,
    FID_CHANGE_RATE,
    FID_CHANGE_VALUE,
    FID_CURRENT_PRICE,
    FID_EXECUTION_STRENGTH,
    FID_HIGH_PRICE,
    FID_LOW_PRICE,
    FID_OPEN_PRICE,
    FID_TRADE_TIME,
    MAX_PENDING_THREAD_AUDIT_EVENTS,
    ConditionLoadState,
    KiwoomClient,
    KiwoomOrderRequest,
    KiwoomOrderResult,
    MarketIndexRealtimeParseError,
    MockKiwoomClient,
    Signal,
    broker_env_from_server_gubun,
    condition_event_payload,
    is_market_index_real_type,
    is_price_tick_real_type,
    is_quote_real_type,
    market_index_realtime_fid_string,
    normalize_market_index_code,
    normalize_order_exchange,
    parse_market_index_tick_from_fids,
    parse_price_tick_from_fids,
    parse_quote_from_fids,
    realtime_code_for_exchange,
)
from gateway.kiwoom_command_handlers import KiwoomGatewayCommandHandler
from gateway.kiwoom_runtime import (
    KiwoomGatewayRuntime,
    KiwoomGatewayRuntimeConfig,
    PendingOrderRegistry,
    _core_io_data_plane_health,
    wire_kiwoom_signals,
)
from gateway.order_pre_ack_journal import OrderPreAckJournal
from services.market_index_service import get_latest_market_index_tick, process_market_index_event
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database

TS = datetime(2026, 6, 26, 9, 1, 2, tzinfo=UTC)


def test_kiwoom_gateway_modules_import_without_loading_pyqt() -> None:
    pyqt_loaded_before = "PyQt5" in sys.modules

    importlib.import_module("apps.kiwoom_gateway")
    importlib.import_module("gateway.kiwoom_client")

    if not pyqt_loaded_before:
        assert "PyQt5" not in sys.modules


def test_kiwoom_gateway_token_default_accepts_trading_core_alias(monkeypatch) -> None:
    monkeypatch.setenv("TRADING_CORE_TOKEN", "trading-token")
    monkeypatch.delenv("GATEWAY_CORE_TOKEN", raising=False)

    assert parse_args([]).token == "trading-token"

    monkeypatch.setenv("GATEWAY_CORE_TOKEN", "gateway-token")

    assert parse_args([]).token == "gateway-token"
    assert parse_args(["--token", "explicit-token"]).token == "explicit-token"


def test_kiwoom_gateway_threaded_login_defaults_to_false() -> None:
    assert parse_args([]).threaded_login is False
    assert parse_args(["--threaded-login"]).threaded_login is True
    assert parse_args(["--no-threaded-login"]).threaded_login is False


def test_kiwoom_gateway_avoids_nested_qt_event_loops_and_process_events() -> None:
    for path in (Path("gateway/kiwoom_client.py"), Path("gateway/kiwoom_tr.py")):
        source = path.read_text(encoding="utf-8")
        assert "QEventLoop" not in source
        assert "processEvents" not in source


def test_kiwoom_gateway_realtime_exchange_option() -> None:
    assert parse_args([]).realtime_exchange == "krx"
    assert parse_args(["--realtime-exchange", "nxt"]).realtime_exchange == "nxt"
    assert parse_args(["--realtime-exchange", "all"]).realtime_exchange == "all"


def test_kiwoom_gateway_realtime_max_total_uses_core_planner_env(monkeypatch) -> None:
    monkeypatch.setenv("REALTIME_SUBSCRIPTION_MAX_TOTAL", "80")

    assert parse_args([]).realtime_max_total == 80
    assert parse_args(["--realtime-max-total", "60"]).realtime_max_total == 60


def test_kiwoom_gateway_batch_timeout_uses_env(monkeypatch) -> None:
    monkeypatch.setenv("GATEWAY_EVENT_TIMEOUT_SEC", "20")

    assert parse_args([]).event_timeout_sec == 20.0
    assert parse_args(["--event-timeout-sec", "12"]).event_timeout_sec == 12.0


def test_kiwoom_gateway_can_clear_realtime_on_login() -> None:
    assert parse_args([]).clear_realtime_on_login is False
    assert parse_args(["--clear-realtime-on-login"]).clear_realtime_on_login is True


def test_kiwoom_gateway_core_io_isolation_options() -> None:
    args = parse_args(
        [
            "--disable-core-io",
            "--disable-command-polling",
            "--disable-event-posting",
        ]
    )

    assert args.disable_core_io is True
    assert args.disable_command_polling is True
    assert args.disable_event_posting is True


def test_kiwoom_gateway_market_index_options_default_to_pilot_off(monkeypatch) -> None:
    monkeypatch.delenv("KIWOOM_MARKET_INDEX_ENABLED", raising=False)
    monkeypatch.delenv("KIWOOM_MARKET_INDEX_REALTIME_ENABLED", raising=False)
    monkeypatch.delenv("KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED", raising=False)

    args = parse_args([])

    assert args.market_index_enabled is False
    assert args.market_index_realtime_enabled is False
    assert args.market_index_tr_bootstrap_enabled is False
    assert args.market_index_codes == "KOSPI,KOSDAQ"
    assert args.market_index_screen_no == "5700"


def test_kiwoom_gateway_market_index_options_accept_env(monkeypatch) -> None:
    monkeypatch.setenv("KIWOOM_MARKET_INDEX_ENABLED", "true")
    monkeypatch.setenv("KIWOOM_MARKET_INDEX_REALTIME_ENABLED", "true")
    monkeypatch.setenv("KIWOOM_MARKET_INDEX_CODES", "KOSPI")

    args = parse_args([])

    assert args.market_index_enabled is True
    assert args.market_index_realtime_enabled is True
    assert args.market_index_codes == "KOSPI"


def test_kiwoom_gateway_dead_man_cancel_options_default_on() -> None:
    args = parse_args([])

    assert args.dead_man_cancel_enabled is True
    assert args.dead_man_cancel_core_stale_sec == 30.0
    assert args.dead_man_cancel_max_orders == 20


def test_kiwoom_gateway_app_drains_worker_commands_instead_of_sync_polling() -> None:
    from apps import kiwoom_gateway

    source = inspect.getsource(kiwoom_gateway.run_gateway)

    assert "command_timer.timeout.connect(runtime.drain_core_io_worker)" in source
    assert "command_timer.timeout.connect(runtime.poll_and_handle_commands)" not in source


def test_price_tick_parser_maps_required_fids_and_metadata() -> None:
    payload = parse_price_tick_from_fids(
        code="A005930",
        name="삼성전자",
        real_type="주식체결",
        raw_fids={
            FID_CURRENT_PRICE: "-70100",
            FID_CHANGE_RATE: "+1.25",
            FID_ACC_VOLUME: "1,234",
            FID_ACC_TRADE_VALUE: "123",
            FID_HIGH_PRICE: "71000",
            FID_LOW_PRICE: "69000",
            FID_TRADE_TIME: "091502",
            FID_BEST_ASK: "70200",
            FID_BEST_BID: "70100",
            FID_EXECUTION_STRENGTH: "105.5",
        },
    )

    tick = BrokerPriceTick.from_dict(payload)

    assert tick.code == "005930"
    assert tick.price == 70100
    assert tick.trade_value == 123_000_000
    assert tick.spread_ticks == 1
    assert payload["metadata"]["trade_value_unit"] == "million_krw"
    assert FID_CURRENT_PRICE in payload["metadata"]["raw_fids_present"]


def test_price_tick_parser_preserves_fallback_reason_codes() -> None:
    payload = parse_price_tick_from_fids(
        code="005930",
        name="삼성전자",
        raw_fids={
            FID_CURRENT_PRICE: "70000",
            FID_ACC_VOLUME: "10",
            FID_ACC_TRADE_VALUE: "",
            FID_HIGH_PRICE: "",
            FID_LOW_PRICE: "",
            FID_BEST_ASK: "",
            FID_BEST_BID: "",
            FID_EXECUTION_STRENGTH: "",
        },
    )

    BrokerPriceTick.from_dict(payload)

    reason_codes = set(payload["metadata"]["reason_codes"])
    assert "TRADE_VALUE_MISSING" in reason_codes
    assert "TURNOVER_ESTIMATED" in reason_codes
    assert "EXECUTION_STRENGTH_MISSING" in reason_codes
    assert "DAY_HIGH_LOW_MISSING" in reason_codes
    assert "BEST_BID_ASK_MISSING" in reason_codes


def test_quote_only_real_type_is_not_treated_as_price_tick() -> None:
    assert is_price_tick_real_type("주식체결") is True
    assert is_price_tick_real_type("주식시세") is True
    assert is_price_tick_real_type("주식우선호가") is False
    assert is_price_tick_real_type("") is False
    assert is_quote_real_type("주식우선호가") is True
    assert is_quote_real_type("주식체결") is False


def test_quote_parser_preserves_bid_ask_without_price_tick_fields() -> None:
    payload = parse_quote_from_fids(
        code="A005930",
        name="삼성전자",
        real_type="주식우선호가",
        raw_fids={
            FID_BEST_ASK: "70200",
            FID_BEST_BID: "70100",
        },
    )

    assert payload["code"] == "005930"
    assert payload["best_ask"] == 70200
    assert payload["best_bid"] == 70100
    assert payload["spread_ticks"] == 1
    assert payload["quote_only"] is True
    assert payload["not_price_tick"] is True
    assert payload["metadata"]["real_type"] == "주식우선호가"


def test_market_index_parser_maps_realtime_fids_with_pilot_evidence() -> None:
    payload = parse_market_index_tick_from_fids(
        index_code="KOSPI",
        index_name="KOSPI",
        kiwoom_code="001",
        real_type="업종지수",
        raw_fids={
            FID_CURRENT_PRICE: "2,812.34",
            FID_CHANGE_VALUE: "-12.30",
            FID_CHANGE_RATE: "-0.44",
            FID_TRADE_TIME: "091502",
        },
    )

    tick = BrokerMarketIndexTick.from_dict(payload)

    assert tick.index_code == "KOSPI"
    assert tick.price == 2812.34
    assert tick.change_value == -12.30
    assert payload["metadata"]["parser_status"] == "VERIFIED"
    assert payload["metadata"]["parser_evidence"]["mapping_status"] == (
        "VERIFIED_KRX_REALTIME"
    )
    assert payload["metadata"]["parser_evidence"]["requires_koa_studio_confirmation"] is False
    assert FID_CURRENT_PRICE in payload["metadata"]["raw_fids_present"]
    assert payload["metadata"]["raw_fids"] == {
        "10": "2,812.34",
        "11": "-12.30",
        "12": "-0.44",
        "20": "091502",
    }


@pytest.mark.parametrize(
    ("missing_fid", "reason_code"),
    (
        (FID_CURRENT_PRICE, "INDEX_PRICE_MISSING"),
        (FID_CHANGE_VALUE, "INDEX_CHANGE_VALUE_MISSING"),
        (FID_CHANGE_RATE, "INDEX_CHANGE_RATE_MISSING"),
        (FID_TRADE_TIME, "INDEX_TRADE_TIME_MISSING"),
    ),
)
def test_market_index_parser_fails_closed_when_required_fid_is_missing(
    missing_fid: int,
    reason_code: str,
) -> None:
    raw_fids = {
        FID_CURRENT_PRICE: "2812.34",
        FID_CHANGE_VALUE: "+5.12",
        FID_CHANGE_RATE: "+0.18",
        FID_TRADE_TIME: "091501",
    }
    raw_fids.pop(missing_fid)

    with pytest.raises(MarketIndexRealtimeParseError) as captured:
        parse_market_index_tick_from_fids(
            index_code="KOSPI",
            kiwoom_code="001",
            raw_fids=raw_fids,
            real_type="업종지수",
        )

    assert reason_code in captured.value.reason_codes


@pytest.mark.parametrize("trade_time", ("", "91501", "246001", "09A501"))
def test_market_index_parser_rejects_invalid_trade_time(trade_time: str) -> None:
    with pytest.raises(MarketIndexRealtimeParseError) as captured:
        parse_market_index_tick_from_fids(
            index_code="KOSPI",
            kiwoom_code="001",
            raw_fids={
                FID_CURRENT_PRICE: "2812.34",
                FID_CHANGE_VALUE: "+5.12",
                FID_CHANGE_RATE: "+0.18",
                FID_TRADE_TIME: trade_time,
            },
            real_type="업종지수",
        )

    expected = "INDEX_TRADE_TIME_MISSING" if not trade_time else "INDEX_TRADE_TIME_INVALID"
    assert expected in captured.value.reason_codes


def test_market_index_code_normalizer_does_not_require_stock_code() -> None:
    assert normalize_market_index_code("KOSPI") == "KOSPI"
    assert normalize_market_index_code("001") == "KOSPI"
    assert is_market_index_real_type("업종지수") is True
    assert is_market_index_real_type("업종등락") is True
    assert is_market_index_real_type("예상업종지수") is True
    assert market_index_realtime_fid_string()


def test_realtime_exchange_code_suffix_helpers() -> None:
    assert realtime_code_for_exchange("005930", "KRX") == "005930"
    assert realtime_code_for_exchange("A005930", "nxt") == "005930_NX"
    assert realtime_code_for_exchange("005930", "all") == "005930_AL"


def test_price_tick_parser_strips_nxt_suffix_and_keeps_exchange_metadata() -> None:
    payload = parse_price_tick_from_fids(
        code="A005930_NX",
        name="삼성전자",
        real_type="주식체결",
        raw_fids={
            FID_CURRENT_PRICE: "+70200",
            FID_CHANGE_RATE: "+1.23",
            FID_ACC_VOLUME: "123456",
            FID_ACC_TRADE_VALUE: "90",
            FID_OPEN_PRICE: "69000",
            FID_HIGH_PRICE: "70500",
            FID_LOW_PRICE: "68800",
            FID_TRADE_TIME: "091501",
            FID_BEST_ASK: "70200",
            FID_BEST_BID: "70100",
            FID_EXECUTION_STRENGTH: "101.5",
        },
    )

    assert payload["code"] == "005930"
    assert payload["metadata"]["exchange"] == "NXT"
    assert payload["metadata"]["kiwoom_code"] == "005930_NX"


def test_kiwoom_quote_real_data_emits_quote_without_price_tick() -> None:
    from gateway.kiwoom_client import KiwoomClient

    client = object.__new__(KiwoomClient)
    client.price_tick_received = Signal()
    client.quote_received = Signal()
    client.realtime_data_received = Signal()
    client.get_code_name = lambda code: "삼성전자"
    raw_values = {
        FID_BEST_ASK: "70200",
        FID_BEST_BID: "70100",
    }
    client._real_raw = lambda code, fid: raw_values.get(fid, "")
    price_ticks: list[dict[str, object]] = []
    quotes: list[dict[str, object]] = []
    raw_callbacks: list[tuple[str, str, bool]] = []
    client.price_tick_received.connect(price_ticks.append)
    client.quote_received.connect(quotes.append)
    client.realtime_data_received.connect(
        lambda code, real_type, present: raw_callbacks.append((code, real_type, present))
    )

    client._on_receive_real_data("005930", "주식우선호가", "")

    assert price_ticks == []
    assert raw_callbacks == [("005930", "주식우선호가", False)]
    assert quotes[0]["code"] == "005930"
    assert quotes[0]["quote_only"] is True


def test_kiwoom_market_index_real_data_emits_index_tick_not_price_tick() -> None:
    client = object.__new__(KiwoomClient)
    client.price_received = Signal()
    client.price_tick_received = Signal()
    client.quote_received = Signal()
    client.market_index_tick_received = Signal()
    client.realtime_data_received = Signal()
    client.realtime_parse_error = Signal()
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    raw_values = {
        FID_CURRENT_PRICE: "2812.34",
        FID_CHANGE_VALUE: "+5.12",
        FID_CHANGE_RATE: "+0.18",
        FID_TRADE_TIME: "091501",
    }
    client._market_index_real_raw = lambda code, fid: raw_values.get(fid, "")
    price_ticks: list[dict[str, object]] = []
    index_ticks: list[dict[str, object]] = []
    raw_callbacks: list[tuple[str, str, bool]] = []
    client.price_tick_received.connect(price_ticks.append)
    client.market_index_tick_received.connect(index_ticks.append)
    client.realtime_data_received.connect(
        lambda code, real_type, present: raw_callbacks.append((code, real_type, present))
    )

    client._on_receive_real_data("001", "업종지수", "")

    assert price_ticks == []
    assert raw_callbacks == [("KOSPI", "업종지수", False)]
    assert index_ticks[0]["index_code"] == "KOSPI"
    assert index_ticks[0]["price"] == 2812.34


def test_kiwoom_expected_market_index_callback_is_observed_without_parse_error() -> None:
    client = object.__new__(KiwoomClient)
    client.price_received = Signal()
    client.price_tick_received = Signal()
    client.quote_received = Signal()
    client.market_index_tick_received = Signal()
    client.realtime_data_received = Signal()
    client.realtime_parse_error = Signal()
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    raw_reads: list[int] = []
    client._market_index_real_raw = lambda code, fid: raw_reads.append(fid) or ""
    index_ticks: list[dict[str, object]] = []
    parse_errors: list[dict[str, object]] = []
    raw_callbacks: list[tuple[str, str, bool]] = []
    client.market_index_tick_received.connect(index_ticks.append)
    client.realtime_parse_error.connect(parse_errors.append)
    client.realtime_data_received.connect(
        lambda code, real_type, present: raw_callbacks.append((code, real_type, present))
    )

    client._on_receive_real_data("001", "예상업종지수", "")

    assert raw_callbacks == [("KOSPI", "예상업종지수", False)]
    assert raw_reads == []
    assert index_ticks == []
    assert parse_errors == []


def test_kiwoom_nxt_real_data_emits_base_code_with_exchange_metadata() -> None:
    client = object.__new__(KiwoomClient)
    client.price_received = Signal()
    client.price_tick_received = Signal()
    client.quote_received = Signal()
    client.realtime_data_received = Signal()
    client.realtime_parse_error = Signal()
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    client.get_code_name = lambda code: "삼성전자"
    raw_values = {
        FID_CURRENT_PRICE: "+70200",
        FID_CHANGE_RATE: "+1.23",
        FID_ACC_VOLUME: "123456",
        FID_ACC_TRADE_VALUE: "90",
        FID_OPEN_PRICE: "69000",
        FID_HIGH_PRICE: "70500",
        FID_LOW_PRICE: "68800",
        FID_TRADE_TIME: "091501",
        FID_BEST_ASK: "70200",
        FID_BEST_BID: "70100",
        FID_EXECUTION_STRENGTH: "101.5",
    }
    client._real_raw = lambda code, fid: raw_values.get(fid, "")
    price_ticks: list[dict[str, object]] = []
    raw_callbacks: list[tuple[str, str, bool]] = []
    client.price_tick_received.connect(price_ticks.append)
    client.realtime_data_received.connect(
        lambda code, real_type, present: raw_callbacks.append((code, real_type, present))
    )

    client._on_receive_real_data("005930_NX", "주식체결", "")

    assert raw_callbacks == [("005930", "주식체결", False)]
    assert price_ticks[0]["code"] == "005930"
    assert price_ticks[0]["metadata"]["exchange"] == "NXT"
    assert price_ticks[0]["metadata"]["kiwoom_code"] == "005930_NX"


def test_kiwoom_unregistered_stock_callback_is_dropped_before_fid_reads() -> None:
    from gateway.kiwoom_client import KiwoomClient

    client = object.__new__(KiwoomClient)
    client.price_received = Signal()
    client.price_tick_received = Signal()
    client.quote_received = Signal()
    client.market_index_tick_received = Signal()
    client.realtime_data_received = Signal()
    client.realtime_parse_error = Signal()
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    client._realtime_screen_codes = {"5000": {"005930"}}
    fid_reads: list[int] = []
    client._real_raw = lambda code, fid: fid_reads.append(fid) or "70000"
    ticks: list[dict[str, object]] = []
    audits: list[dict[str, object]] = []
    client.price_tick_received.connect(ticks.append)
    client.active_x_thread_audit.connect(audits.append)

    client._on_receive_real_data("000660", "주식체결", "")

    assert fid_reads == []
    assert ticks == []
    assert audits[-1]["callback_admitted"] is False
    assert audits[-1]["admission_reason"] == "UNREGISTERED_REALTIME_CODE"


def test_runtime_reports_unregistered_realtime_callback_admission_drops() -> None:
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=object())

    runtime.on_active_x_thread_audit(
        {
            "method": "OnReceiveRealData",
            "phase": "CALLBACK",
            "code": "000660",
            "kiwoom_code": "000660",
            "real_type": "주식체결",
            "callback_admitted": False,
            "admission_reason": "UNREGISTERED_REALTIME_CODE",
        }
    )

    payload = runtime.heartbeat_payload()
    assert payload["unregistered_realtime_callback_count"] == 1
    assert payload["latest_unregistered_realtime_callback"]["code"] == "000660"
    assert payload["latest_unregistered_realtime_callback"]["reason"] == (
        "UNREGISTERED_REALTIME_CODE"
    )


def test_kiwoom_thread_audit_pending_buffer_is_bounded() -> None:
    client = object.__new__(KiwoomClient)
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    audits: list[dict[str, object]] = []
    client.active_x_thread_audit.connect(audits.append)

    for sequence in range(MAX_PENDING_THREAD_AUDIT_EVENTS + 5):
        client._record_thread_audit("OnReceiveRealData", phase="CALLBACK", sequence=sequence)

    assert len(audits) == MAX_PENDING_THREAD_AUDIT_EVENTS + 5
    assert len(client._pending_thread_audit_events) == MAX_PENDING_THREAD_AUDIT_EVENTS
    assert client._pending_thread_audit_events[0]["sequence"] == 5
    assert client._pending_thread_audit_events[-1]["sequence"] == (
        MAX_PENDING_THREAD_AUDIT_EVENTS + 4
    )


def test_login_request_finishes_when_kiwoom_is_already_connected() -> None:
    class Client:
        def login(self) -> int:
            return 0

    class Runtime:
        def __init__(self) -> None:
            self._login_in_progress = False
            self.connected_calls: list[tuple[bool, int, str]] = []

        def request_login_started(self, *, threaded: bool) -> None:
            self._login_in_progress = True

        def request_login_failed(self, exc: Exception) -> None:
            raise AssertionError(exc)

        def kiwoom_logged_in(self) -> bool:
            return True

        def on_connected(self, ok: bool, code: int, message: str) -> None:
            self._login_in_progress = False
            self.connected_calls.append((ok, code, message))

    runtime = Runtime()

    request_kiwoom_login(Client(), runtime, threaded=False)

    assert runtime.connected_calls == [(True, 0, "already connected")]
    assert runtime._login_in_progress is False


def test_login_request_default_uses_non_threaded_main_path() -> None:
    class Client:
        def __init__(self) -> None:
            self.login_calls = 0

        def login(self) -> int:
            self.login_calls += 1
            return 0

    class Runtime:
        def __init__(self) -> None:
            self._login_in_progress = False
            self.threaded_values: list[bool] = []

        def request_login_started(self, *, threaded: bool) -> None:
            self._login_in_progress = True
            self.threaded_values.append(threaded)

        def request_login_failed(self, exc: Exception) -> None:
            raise AssertionError(exc)

        def kiwoom_logged_in(self) -> bool:
            return False

    client = Client()
    runtime = Runtime()

    request_kiwoom_login(client, runtime)

    assert client.login_calls == 1
    assert runtime.threaded_values == [False]


def test_login_request_finishes_connect_state_fallback_for_nonblocking_client() -> None:
    class Client:
        login_waits_for_event_loop = False

        def __init__(self) -> None:
            self.login_calls = 0

        def login(self) -> int:
            self.login_calls += 1
            return 0

    class Runtime:
        def __init__(self) -> None:
            self._login_in_progress = False
            self.connected_calls: list[tuple[bool, int, str]] = []

        def request_login_started(self, *, threaded: bool) -> None:
            self._login_in_progress = True

        def request_login_failed(self, exc: Exception) -> None:
            raise AssertionError(exc)

        def kiwoom_logged_in(self) -> bool:
            return True

        def on_connected(self, ok: bool, code: int, message: str) -> None:
            self._login_in_progress = False
            self.connected_calls.append((ok, code, message))

    client = Client()
    runtime = Runtime()

    request_kiwoom_login(client, runtime)

    assert client.login_calls == 1
    assert runtime.connected_calls == [(True, 0, "already connected")]
    assert runtime._login_in_progress is False
    assert KiwoomClient.login_waits_for_event_loop is False


def test_event_connect_callback_emits_connected_without_nested_loop() -> None:
    client = object.__new__(KiwoomClient)
    client.active_x_thread_audit = Signal()
    client.connected = Signal()
    client._pending_thread_audit_events = []
    connected_calls: list[tuple[bool, int, str]] = []
    client.connected.connect(
        lambda ok, code, message: connected_calls.append((bool(ok), int(code), str(message)))
    )

    client._on_event_connect(0)

    assert connected_calls == [(True, 0, "정상처리")]


def test_condition_ver_callback_emits_result_without_nested_loop() -> None:
    client = object.__new__(KiwoomClient)
    client.active_x_thread_audit = Signal()
    client.condition_state_changed = Signal()
    client.condition_load_result = Signal()
    client.condition_loaded = Signal()
    client._pending_thread_audit_events = []
    client.condition_load_state = ConditionLoadState.LOADING
    client.condition_name_list = lambda: []
    result_calls: list[tuple[bool, str]] = []
    client.condition_load_result.connect(
        lambda success, message: result_calls.append((bool(success), str(message)))
    )

    client._on_receive_condition_ver(1, "ok")

    assert client.condition_load_state is ConditionLoadState.LOADED
    assert result_calls == [(True, "ok")]


def test_kiwoom_client_creates_activex_with_set_control() -> None:
    class FakeQAxWidget:
        instances: list[FakeQAxWidget] = []

        def __init__(self, control_name: str | None = None) -> None:
            self.constructor_control_name = control_name
            self.control_name = ""
            self.set_control_calls: list[str] = []
            FakeQAxWidget.instances.append(self)

        def setControl(self, control_name: str) -> bool:
            self.control_name = str(control_name)
            self.set_control_calls.append(self.control_name)
            return self.control_name == "KHOPENAPI.KHOpenAPICtrl.1"

        def isNull(self) -> bool:
            return self.control_name != "KHOPENAPI.KHOpenAPICtrl.1"

    client = object.__new__(KiwoomClient)
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    audits: list[dict[str, object]] = []
    client.active_x_thread_audit.connect(audits.append)

    widget = client._create_ocx_widget(FakeQAxWidget)

    assert widget.set_control_calls == ["KHOPENAPI.KHOpenAPICtrl.1"]
    assert widget.constructor_control_name is None
    assert any(
        audit["method"] == "QAxWidget.setControl"
        and audit["phase"] == "RESULT"
        and audit["result"] is True
        for audit in audits
    )


def test_runtime_heartbeat_finishes_pending_login_when_connection_appears() -> None:
    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(realtime_codes=("005930", "000660")),
    )
    runtime.request_login_started(threaded=False)

    runtime.emit_heartbeat()

    assert runtime._login_in_progress is False
    assert runtime._login_result_code == 0
    assert client.registered_codes == {"005930", "000660"}
    assert runtime._registered_realtime_codes == {"005930", "000660"}


def test_runtime_clears_stale_realtime_subscriptions_on_login() -> None:
    client = MockKiwoomClient()
    client.registered_codes.add("005930")
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(clear_realtime_on_login=True),
    )

    runtime.on_connected(True, 0, "ok")

    assert client.remove_all_realtime_count == 1
    assert client.registered_codes == set()
    assert runtime._registered_realtime_codes == set()


def _disconnectable_mock_client() -> MockKiwoomClient:
    client = MockKiwoomClient()
    client.connect_state = True
    client.get_accounts = lambda: ["1234567890"] if client.connect_state else []
    return client


def test_runtime_reconnects_with_backoff_after_session_drop(monkeypatch) -> None:
    clock = {"now": datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)}
    monkeypatch.setattr("gateway.kiwoom_runtime.utc_now", lambda: clock["now"])

    client = _disconnectable_mock_client()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            realtime_codes=("005930",),
            login_reconnect_base_delay_sec=5.0,
            login_reconnect_max_delay_sec=300.0,
        ),
    )
    reconnect_calls: list[datetime] = []
    runtime.reconnect_login = lambda: reconnect_calls.append(clock["now"])

    runtime.on_connected(True, 0, "ok")
    assert client.registered_codes == {"005930"}
    assert runtime.heartbeat_payload()["reconnect_count"] == 0

    # Session drops: Kiwoom also loses the realtime registrations.
    client.connect_state = False
    client.registered_codes.clear()

    runtime.emit_heartbeat()
    payload = runtime.heartbeat_payload()
    assert payload["reconnect_pending"] is True
    assert reconnect_calls == []  # first heartbeat only schedules the retry

    def advance(seconds: float) -> None:
        clock["now"] = clock["now"] + timedelta(seconds=seconds)

    advance(5)
    runtime.emit_heartbeat()
    assert len(reconnect_calls) == 1

    advance(5)  # next retry is 10s of backoff away, so nothing fires yet
    runtime.emit_heartbeat()
    assert len(reconnect_calls) == 1

    advance(5)
    runtime.emit_heartbeat()
    assert len(reconnect_calls) == 2

    # Login succeeds again: counters reset and realtime is re-registered.
    client.connect_state = True
    runtime.request_login_started(threaded=False)
    runtime.on_connected(True, 0, "ok")
    payload = runtime.heartbeat_payload()
    assert payload["reconnect_count"] == 1
    assert payload["reconnect_pending"] is False
    assert payload["reconnect_attempt_count"] == 0
    assert client.registered_codes == {"005930"}


def test_runtime_reconnect_loop_ignores_never_connected_sessions(monkeypatch) -> None:
    clock = {"now": datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)}
    monkeypatch.setattr("gateway.kiwoom_runtime.utc_now", lambda: clock["now"])

    client = _disconnectable_mock_client()
    client.connect_state = False
    runtime = KiwoomGatewayRuntime(client=client, core_client=object())
    reconnect_calls: list[datetime] = []
    runtime.reconnect_login = lambda: reconnect_calls.append(clock["now"])

    for _ in range(3):
        clock["now"] = clock["now"] + timedelta(seconds=10)
        runtime.emit_heartbeat()

    assert reconnect_calls == []
    assert runtime.heartbeat_payload()["reconnect_pending"] is False


def test_runtime_session_disconnect_fails_claimed_unstarted_command(monkeypatch) -> None:
    clock = {"now": datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)}
    monkeypatch.setattr("gateway.kiwoom_runtime.utc_now", lambda: clock["now"])
    command = _live_sim_order_command(command_id="cmd_disconnect_unstarted")

    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    client = _disconnectable_mock_client()
    core = Core()
    runtime = KiwoomGatewayRuntime(client=client, core_client=core)
    runtime.on_connected(True, 0, "ok")
    runtime._claimed_not_started_commands[command.command_id] = command

    client.connect_state = False
    runtime.emit_heartbeat()
    runtime.flush_events()

    failed = [event for event in core.events if event.event_type == "command_failed"]
    assert len(failed) == 1
    assert failed[0].command_id == command.command_id
    assert failed[0].payload["error_message"] == "SESSION_DISCONNECTED_BEFORE_EXECUTION"
    assert runtime._claimed_not_started_commands == {}


def test_runtime_session_disconnect_leaves_started_command_unconfirmed(monkeypatch) -> None:
    clock = {"now": datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)}
    monkeypatch.setattr("gateway.kiwoom_runtime.utc_now", lambda: clock["now"])
    command = _live_sim_order_command(command_id="cmd_disconnect_started")

    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    class Handler:
        def handle(self, command: GatewayCommand) -> list[GatewayEvent]:
            return [make_command_started_event(command, source="kiwoom_gateway")]

    client = _disconnectable_mock_client()
    core = Core()
    runtime = KiwoomGatewayRuntime(client=client, core_client=core)
    runtime.command_handler = Handler()
    runtime.on_connected(True, 0, "ok")
    runtime.handle_commands([command])

    client.connect_state = False
    runtime.emit_heartbeat()
    runtime.flush_events()

    assert [event.event_type for event in core.events].count("command_started") == 1
    assert not any(
        event.event_type == "command_failed" and event.command_id == command.command_id
        for event in core.events
    )


def test_runtime_does_not_register_market_index_when_feature_flag_off() -> None:
    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            market_index_enabled=False,
            market_index_realtime_enabled=True,
            market_index_codes=("KOSPI", "KOSDAQ"),
        ),
    )

    runtime.on_connected(True, 0, "ok")

    assert client.registered_market_index_codes == set()
    assert runtime.heartbeat_payload()["market_index_registered_codes"] == []


def test_runtime_registers_market_index_realtime_when_feature_flags_on() -> None:
    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            market_index_enabled=True,
            market_index_realtime_enabled=True,
            market_index_codes=("KOSPI", "KOSDAQ"),
            market_index_screen_no="5700",
        ),
    )

    runtime.on_connected(True, 0, "ok")
    payload = runtime.heartbeat_payload()

    assert client.registered_market_index_codes == {"KOSPI", "KOSDAQ"}
    assert client.registered_market_index_kiwoom_codes == {"001", "101"}
    assert payload["market_index_registered_codes"] == ["KOSDAQ", "KOSPI"]
    assert payload["realtime_registration_requested_count"] == 0
    assert payload["market_index_adapter_health"] == "REGISTERED_WAITING_CALLBACK"


def test_runtime_recovers_market_index_when_stock_callbacks_are_active(monkeypatch) -> None:
    clock = {"now": datetime(2026, 7, 3, 0, 0, 0, tzinfo=UTC)}
    monkeypatch.setattr("gateway.kiwoom_runtime.utc_now", lambda: clock["now"])

    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            market_index_enabled=True,
            market_index_realtime_enabled=True,
            market_index_codes=("KOSPI", "KOSDAQ"),
            market_index_screen_no="5700",
            market_index_poll_sec=30.0,
            realtime_callback_timeout_sec=120.0,
        ),
    )

    runtime.on_connected(True, 0, "ok")
    clock["now"] = clock["now"] + timedelta(seconds=20)
    runtime.on_realtime_data(
        code="005930",
        real_type="주식체결",
        real_data_present=True,
    )
    runtime.emit_heartbeat()
    assert client.removed_market_index_codes == []

    clock["now"] = clock["now"] + timedelta(seconds=11)
    runtime.emit_heartbeat()
    payload = runtime.heartbeat_payload()

    assert set(client.removed_market_index_codes) == {"KOSPI", "KOSDAQ"}
    assert client.registered_market_index_codes == {"KOSPI", "KOSDAQ"}
    assert payload["market_index_recover_count"] == 1
    assert payload["latest_market_index_recover_at"] == "2026-07-03T00:00:31Z"
    assert any(
        event.event_type == "gateway_log"
        and event.payload["message"] == "market index realtime reset after waiting callback"
        for event in runtime._event_queue
    )


def test_mock_kiwoom_market_index_tick_flows_to_core_projection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "mock_index_flow.sqlite3")

    class ProjectingCoreClient:
        def post_event(self, event: GatewayEvent) -> dict[str, object]:
            result = append_gateway_event(connection, event)
            if result.status == "ACCEPTED" and not result.duplicate:
                process_market_index_event(connection, event)
            return {"accepted": result.accepted, "event_id": event.event_id}

    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=ProjectingCoreClient(),
        config=KiwoomGatewayRuntimeConfig(command_polling_enabled=False),
    )
    wire_kiwoom_signals(client, runtime)

    client.emit_market_index_tick(index_code="KOSPI", price=2810.5, change_rate=0.2)
    client.emit_market_index_tick(index_code="KOSDAQ", price=860.25, change_rate=-0.1)
    runtime.flush_events()

    kospi = get_latest_market_index_tick(connection, "KOSPI")
    kosdaq = get_latest_market_index_tick(connection, "KOSDAQ")
    stock_latest_count = connection.execute(
        "SELECT COUNT(*) AS count FROM market_ticks_latest"
    ).fetchone()["count"]

    assert kospi is not None
    assert kospi["price"] == 2810.5
    assert kosdaq is not None
    assert kosdaq["price"] == 860.25
    assert stock_latest_count == 0
    assert runtime.heartbeat_payload()["parsed_market_index_tick_count"] == 2


def test_runtime_heartbeat_skips_connect_state_fallback_for_event_loop_client() -> None:
    class EventLoopLoginClient(MockKiwoomClient):
        login_waits_for_event_loop = True

    client = EventLoopLoginClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(realtime_codes=("005930", "000660")),
    )
    runtime.request_login_started(threaded=False)

    runtime.emit_heartbeat()

    assert runtime._login_in_progress is True
    assert runtime._login_result_code is None
    assert client.registered_codes == set()
    assert any(
        event.event_type == "gateway_log"
        and event.payload["message"] == "LOGIN_CONNECT_STATE_FALLBACK_SKIPPED_EVENT_LOOP_CLIENT"
        for event in runtime._event_queue
    )


def test_runtime_reports_comm_connect_no_return_after_event_timeout() -> None:
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=object())

    runtime.on_active_x_thread_audit(
        {
            "method": "CommConnect",
            "phase": "CALL",
            "timestamp": "2026-06-29T09:00:00Z",
        }
    )
    runtime.on_active_x_thread_audit(
        {
            "method": "OnEventConnect",
            "phase": "TIMEOUT",
            "timeout_ms": 60000,
            "timestamp": "2026-06-29T09:01:00Z",
        }
    )

    payload = runtime.heartbeat_payload()

    assert payload["comm_connect_state"] == "EVENT_TIMEOUT_NO_COMM_CONNECT_RESULT"
    assert "COMM_CONNECT_NO_RETURN" in payload["login_block_reason_codes"]
    assert "ON_EVENT_CONNECT_TIMEOUT" in payload["login_block_reason_codes"]


def test_runtime_nxt_realtime_exchange_registers_suffixed_kiwoom_codes() -> None:
    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            realtime_codes=("005930", "000660"),
            realtime_exchange="NXT",
        ),
    )

    runtime.register_realtime_codes(["005930", "000660"])
    payload = runtime.heartbeat_payload()

    assert client.registered_codes == {"005930_NX", "000660_NX"}
    assert runtime._registered_realtime_codes == {"005930", "000660"}
    assert payload["realtime_exchange"] == "NXT"
    assert payload["realtime_registered_codes"] == ["000660", "005930"]
    assert payload["realtime_registered_kiwoom_codes"] == ["000660_NX", "005930_NX"]


def test_kiwoom_order_request_exchange_suffix_contract() -> None:
    request = KiwoomOrderRequest(
        account="1234567890",
        code="A005930",
        quantity=1,
        price=70000,
        side="BUY",
        tag="cmd_order_sor",
        command_id="cmd_order_sor",
        idempotency_key="idem-sor",
        order_exchange="SOR",
    )

    payload = request.to_dict()

    assert normalize_order_exchange("ALL") == "SOR"
    assert realtime_code_for_exchange("A005930", "KRX") == "005930"
    assert realtime_code_for_exchange("005930", "NXT") == "005930_NX"
    assert realtime_code_for_exchange("005930", "SOR") == "005930_AL"
    assert payload["code"] == "005930"
    assert payload["order_exchange"] == "SOR"
    assert payload["kiwoom_code"] == "005930_AL"


def test_runtime_realtime_registration_dedupes_already_registered() -> None:
    class TrackingClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.register_calls: list[list[str]] = []

        def register_realtime(self, codes, screen_no=None) -> None:  # type: ignore[no-untyped-def]
            self.register_calls.append(list(codes))
            super().register_realtime(codes, screen_no=screen_no)

    client = TrackingClient()
    runtime = KiwoomGatewayRuntime(client=client, core_client=object())

    runtime.register_realtime_codes(["005930", "000660"])
    runtime.register_realtime_codes(["A005930", "000660"])
    payload = runtime.heartbeat_payload()

    assert client.register_calls == [["005930", "000660"]]
    assert runtime._registered_realtime_codes == {"005930", "000660"}
    assert payload["realtime_registration_dedupe_count"] == 2
    assert runtime._event_queue[-1].event_type == "gateway_log"
    assert runtime._event_queue[-1].payload["skipped_already_registered_count"] == 2


def test_runtime_heartbeat_allows_market_data_login_fallback_without_account() -> None:
    class ConnectedWithoutAccounts(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.ocx = self

        def dynamicCall(self, signature: str) -> int:
            if str(signature).startswith("GetConnectState"):
                return 1
            return 0

        def get_accounts(self) -> list[str]:
            return []

    client = ConnectedWithoutAccounts()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(realtime_codes=("005930",)),
    )
    runtime.request_login_started(threaded=False)

    runtime.emit_heartbeat()

    payload = runtime.heartbeat_payload()

    assert runtime._login_in_progress is False
    assert runtime._login_result_code == 0
    assert client.registered_codes == {"005930"}
    assert payload["orderable"] is False


def test_runtime_disable_core_io_never_calls_core_client() -> None:
    class Core:
        def __init__(self) -> None:
            self.poll_calls = 0
            self.post_calls = 0

        def poll_commands(self, *, limit: int, wait_sec: float) -> list[GatewayCommand]:
            self.poll_calls += 1
            return []

        def post_event(self, event: GatewayEvent) -> None:
            self.post_calls += 1

    core = Core()
    runtime = KiwoomGatewayRuntime(
        client=MockKiwoomClient(),
        core_client=core,
        config=KiwoomGatewayRuntimeConfig(
            core_io_enabled=False,
            command_polling_enabled=False,
            event_posting_enabled=False,
        ),
    )

    runtime.emit("gateway_log", {"message": "local only"})
    runtime.flush_events()
    runtime.poll_and_handle_commands()
    payload = runtime.heartbeat_payload()

    assert core.poll_calls == 0
    assert core.post_calls == 0
    assert payload["core_io_enabled"] is False
    assert payload["local_event_count"] == 1
    assert payload["queued_event_count"] == 0


def test_core_io_worker_keeps_http_off_main_thread_and_commands_on_main_thread() -> None:
    main_thread_id = threading.get_ident()
    command = GatewayCommand(
        command_id="cmd_worker_register",
        command_type="register_realtime",
        source="core",
        payload={"codes": ["005930"]},
    )

    class Core:
        def __init__(self) -> None:
            self.commands = [command]
            self.events: list[GatewayEvent] = []
            self.poll_thread_ids: list[int] = []
            self.post_thread_ids: list[int] = []

        def poll_commands(self, *, limit: int, wait_sec: float) -> list[GatewayCommand]:
            self.poll_thread_ids.append(threading.get_ident())
            time.sleep(0.01)
            if self.commands:
                return [self.commands.pop(0)]
            return []

        def post_event(self, event: GatewayEvent) -> None:
            self.post_thread_ids.append(threading.get_ident())
            self.events.append(event)

    class TrackingClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.register_thread_ids: list[int] = []

        def register_realtime(self, codes, screen_no=None) -> None:  # type: ignore[no-untyped-def]
            self.register_thread_ids.append(threading.get_ident())
            super().register_realtime(codes, screen_no=screen_no)

    core = Core()
    client = TrackingClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=core,
        config=KiwoomGatewayRuntimeConfig(
            command_wait_sec=0.01,
            core_io_worker_enabled=True,
        ),
    )
    runtime.start_core_io_worker()
    try:
        runtime.emit("gateway_log", {"message": "queued through worker"})
        runtime.flush_events()
        _wait_until(lambda: bool(core.poll_thread_ids))
        _wait_until(lambda: _drain_worker_once(runtime) or bool(client.register_thread_ids))
        runtime.flush_events()
        _wait_until(lambda: len(core.events) >= 3)
    finally:
        runtime.close()

    assert core.poll_thread_ids
    assert core.post_thread_ids
    assert all(thread_id != main_thread_id for thread_id in core.poll_thread_ids)
    assert all(thread_id != main_thread_id for thread_id in core.post_thread_ids)
    assert client.register_thread_ids == [main_thread_id]
    assert client.registered_codes == {"005930"}
    assert [event.event_type for event in core.events][-2:] == [
        "command_started",
        "command_ack",
    ]


def test_core_io_worker_coalesces_price_ticks_when_queue_is_backed_up() -> None:
    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    worker = CoreIoWorker(
        core_client=Core(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=2,
    )
    first = GatewayEvent(
        event_id="evt_price_old",
        event_type="price_tick",
        source="kiwoom_gateway",
        payload={"code": "005930", "price": 70000},
    )
    other = GatewayEvent(
        event_id="evt_price_other",
        event_type="price_tick",
        source="kiwoom_gateway",
        payload={"code": "000660", "price": 120000},
    )
    latest = GatewayEvent(
        event_id="evt_price_latest",
        event_type="price_tick",
        source="kiwoom_gateway",
        payload={"code": "005930", "price": 70100},
    )

    worker.enqueue_event(first)
    worker.enqueue_event(other)
    worker.enqueue_event(latest)

    snapshot = worker.snapshot()
    assert snapshot.event_queue_size == 2
    assert snapshot.coalesced_count == 1
    assert worker._post_next_event() is True
    assert worker._core_client.events[0].event_id == "evt_price_latest"


def test_core_io_worker_batches_fresh_market_events_with_durable_fifo() -> None:
    class Core:
        def __init__(self) -> None:
            self.batches: list[list[GatewayEvent]] = []

        def post_events(self, events: list[GatewayEvent]) -> dict[str, object]:
            self.batches.append(list(events))
            return {
                "processed_count": len(events),
                "accepted_count": len(events),
                "failed_count": 0,
                "results": [{"accepted": True} for _ in events],
            }

    core = Core()
    worker = CoreIoWorker(
        core_client=core,
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=2,
        event_batch_size=6,
        market_batch_share=3,
    )
    for index in range(10):
        worker.enqueue_event(
            GatewayEvent(
                event_id=f"evt_condition_batch_{index}",
                event_type="condition_event",
                source="kiwoom_gateway",
                payload={"code": f"{index:06d}", "action": "ENTER"},
            )
        )
    for index, code in enumerate(("005930", "000660", "035420")):
        worker.enqueue_event(
            GatewayEvent(
                event_id=f"evt_price_batch_{index}",
                event_type="price_tick",
                source="kiwoom_gateway",
                payload={"code": code, "price": 70000 + index},
            )
        )

    assert worker._post_next_event_batch(core.post_events) is True

    event_types = [event.event_type for event in core.batches[0]]
    assert event_types.count("price_tick") == 3
    assert event_types.count("condition_event") == 3
    snapshot = worker.snapshot()
    assert snapshot.event_queue_size == 7
    assert snapshot.batch_post_count == 1
    assert snapshot.latest_batch_size == 6
    assert snapshot.market_event_queue_size == 0
    assert snapshot.durable_event_queue_size == 7


def test_core_io_data_plane_health_fails_closed_on_stale_market_backlog() -> None:
    worker = CoreIoWorker(
        core_client=object(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
    )
    snapshot = worker.snapshot()

    healthy = replace(
        snapshot,
        running=True,
        oldest_event_age_sec=1.0,
        oldest_market_event_age_sec=1.0,
    )
    stale = replace(
        healthy,
        oldest_event_age_sec=12.0,
        oldest_market_event_age_sec=12.0,
    )
    coalesced_market_buffer = replace(
        healthy,
        event_queue_size=400,
        market_event_queue_size=400,
        durable_event_queue_size=0,
    )
    durable_backlog = replace(
        healthy,
        event_queue_size=250,
        durable_event_queue_size=250,
    )

    assert _core_io_data_plane_health(healthy) == "HEALTHY"
    assert _core_io_data_plane_health(stale) == "STALE_MARKET_BACKLOG"
    assert _core_io_data_plane_health(coalesced_market_buffer) == "HEALTHY"
    assert _core_io_data_plane_health(durable_backlog) == "BACKLOG_DEGRADED"


def test_core_io_worker_prioritizes_latest_heartbeat_when_queue_is_backed_up() -> None:
    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    worker = CoreIoWorker(
        core_client=Core(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=2,
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_condition",
            event_type="condition_event",
            source="kiwoom_gateway",
            payload={"code": "005930", "condition_name": "A", "action": "ENTER"},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_price",
            event_type="price_tick",
            source="kiwoom_gateway",
            payload={"code": "005930", "price": 70000},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_heartbeat_old",
            event_type="heartbeat",
            source="kiwoom_gateway",
            payload={"sequence": 1},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_heartbeat_new",
            event_type="heartbeat",
            source="kiwoom_gateway",
            payload={"sequence": 2},
        )
    )

    snapshot = worker.snapshot()
    assert snapshot.event_queue_size == 3
    assert snapshot.coalesced_count == 1
    assert worker._post_next_event() is True
    assert worker._core_client.events[0].event_id == "evt_heartbeat_new"


def test_core_io_worker_prioritizes_latest_market_index_tick_when_queue_is_backed_up() -> None:
    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    worker = CoreIoWorker(
        core_client=Core(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=2,
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_condition",
            event_type="condition_event",
            source="kiwoom_gateway",
            payload={"code": "005930", "condition_name": "A", "action": "ENTER"},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_price",
            event_type="price_tick",
            source="kiwoom_gateway",
            payload={"code": "005930", "price": 70000},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_index_old",
            event_type="market_index_tick",
            source="kiwoom_gateway",
            payload={"index_code": "KOSPI", "price": 2800.0},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_index_latest",
            event_type="market_index_tick",
            source="kiwoom_gateway",
            payload={"index_code": "KOSPI", "price": 2801.0},
        )
    )

    snapshot = worker.snapshot()
    assert snapshot.event_queue_size == 3
    assert snapshot.coalesced_count == 1
    assert worker._post_next_event() is True
    assert worker._core_client.events[0].event_id == "evt_index_latest"


def test_core_io_worker_prioritizes_tr_response_behind_command_lifecycle() -> None:
    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    command = GatewayCommand(
        command_id="cmd_candidate_quote",
        command_type="request_tr",
        source="candidate_quote_refresh",
        idempotency_key="idem-candidate-quote",
        payload={},
    )
    worker = CoreIoWorker(
        core_client=Core(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=2,
    )
    worker.enqueue_event(make_command_started_event(command, source="kiwoom_gateway"))
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_condition",
            event_type="condition_event",
            source="kiwoom_gateway",
            payload={"code": "005930", "condition_name": "A", "action": "ENTER"},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_tr_response",
            event_type="tr_response",
            source="kiwoom_gateway",
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            payload={
                "request_id": "candidate_quote_refresh_005930_20260707T040500",
                "tr_code": "OPT10001",
                "code": "005930",
                "rows": [{"code": "005930", "current_price": "70000"}],
            },
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_ack",
            event_type="command_ack",
            source="kiwoom_gateway",
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            payload={"command_id": command.command_id, "status": "acked"},
        )
    )

    assert worker._post_next_event() is True
    assert worker._post_next_event() is True
    assert worker._post_next_event() is True
    assert [event.event_type for event in worker._core_client.events] == [
        "command_started",
        "command_ack",
        "tr_response",
    ]


def test_core_io_worker_prioritizes_command_lifecycle_over_heartbeat() -> None:
    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    worker = CoreIoWorker(
        core_client=Core(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=2,
    )
    command = GatewayCommand(
        command_id="cmd_order_priority",
        command_type="send_order",
        source="live_sim",
        idempotency_key="idem-cmd_order_priority",
        payload={},
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_condition",
            event_type="condition_event",
            source="kiwoom_gateway",
            payload={"code": "005930", "condition_name": "A", "action": "ENTER"},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_heartbeat",
            event_type="heartbeat",
            source="kiwoom_gateway",
            payload={"sequence": 1},
        )
    )
    worker.enqueue_event(make_command_started_event(command, source="kiwoom_gateway"))
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_ack",
            event_type="command_ack",
            source="kiwoom_gateway",
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            payload={"command_id": command.command_id, "status": "acked"},
        )
    )

    assert worker._post_next_event() is True
    assert worker._post_next_event() is True
    assert [event.event_type for event in worker._core_client.events] == [
        "command_started",
        "command_ack",
    ]


def test_core_io_worker_bounds_buffer_and_preserves_order_events() -> None:
    class Core:
        def post_event(self, event: GatewayEvent) -> None:
            raise RuntimeError("core unreachable")

    worker = CoreIoWorker(
        core_client=Core(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=1000,
        max_buffer_size=5,
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_started",
            event_type="command_started",
            source="kiwoom_gateway",
            payload={"command_id": "cmd_1"},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_ack",
            event_type="command_ack",
            source="kiwoom_gateway",
            payload={"command_id": "cmd_1"},
        )
    )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_exec",
            event_type="execution_event",
            source="kiwoom_gateway",
            payload={"code": "005930", "quantity": 1},
        )
    )
    for index in range(10):
        worker.enqueue_event(
            GatewayEvent(
                event_id=f"evt_tick_{index}",
                event_type="price_tick",
                source="kiwoom_gateway",
                # Distinct codes so coalescing cannot absorb the overflow.
                payload={"code": f"{index:06d}", "price": 1000 + index},
            )
        )

    snapshot = worker.snapshot()
    assert snapshot.event_queue_size == 5
    assert snapshot.dropped_count == 8
    assert snapshot.max_buffer_size == 5
    with worker._condition:
        queued_ids = [event.event_id for event in worker._events]
    assert "evt_started" in queued_ids
    assert "evt_ack" in queued_ids
    assert "evt_exec" in queued_ids
    # Oldest ticks were dropped first; the newest ticks survive.
    assert queued_ids[-1] == "evt_tick_9"


def test_core_io_worker_never_drops_protected_events_even_over_cap() -> None:
    class Core:
        def post_event(self, event: GatewayEvent) -> None:
            raise RuntimeError("core unreachable")

    worker = CoreIoWorker(
        core_client=Core(),
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=False,
        coalesce_after_size=1000,
        max_buffer_size=2,
    )
    for index in range(4):
        worker.enqueue_event(
            GatewayEvent(
                event_id=f"evt_ack_{index}",
                event_type="command_ack",
                source="kiwoom_gateway",
                payload={"command_id": f"cmd_{index}"},
            )
        )
    worker.enqueue_event(
        GatewayEvent(
            event_id="evt_tr_response",
            event_type="tr_response",
            source="kiwoom_gateway",
            payload={
                "request_id": "candidate_quote_refresh_005930_20260707T040500",
                "tr_code": "OPT10001",
                "code": "005930",
                "rows": [{"code": "005930", "current_price": "70000"}],
            },
        )
    )

    snapshot = worker.snapshot()
    assert snapshot.event_queue_size == 5
    assert snapshot.dropped_count == 0


def test_core_io_worker_polls_commands_while_event_queue_has_backlog() -> None:
    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def poll_commands(self, *, limit: int, wait_sec: float) -> list[GatewayCommand]:
            return []

        def post_event(self, event: GatewayEvent) -> None:
            time.sleep(0.01)
            self.events.append(event)

    core = Core()
    worker = CoreIoWorker(
        core_client=core,
        command_limit=1,
        command_wait_sec=0,
        command_polling_enabled=True,
        command_poll_interval_sec=0.02,
        coalesce_after_size=1000,
        max_buffer_size=200,
    )
    for index in range(100):
        worker.enqueue_event(
            GatewayEvent(
                event_id=f"evt_condition_{index}",
                event_type="condition_event",
                source="kiwoom_gateway",
                payload={"code": f"{index:06d}", "action": "ENTER"},
            )
        )

    worker.start()
    try:
        _wait_until(
            lambda: worker.snapshot().poll_count >= 2
            and worker.snapshot().event_queue_size > 0,
            timeout_sec=1.0,
        )
    finally:
        worker.stop()

    snapshot = worker.snapshot()
    assert snapshot.poll_count >= 2
    assert snapshot.event_queue_size > 0


def test_runtime_command_handler_exception_emits_failure_without_crashing() -> None:
    command = GatewayCommand(
        command_id="cmd_boom",
        command_type="register_realtime",
        source="core",
        payload={"codes": ["005930"]},
    )

    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def poll_commands(self, *, limit: int, wait_sec: float) -> list[GatewayCommand]:
            return [command]

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    class Handler:
        def handle(self, command: GatewayCommand) -> list[GatewayEvent]:
            raise RuntimeError("boom")

    core = Core()
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=core)
    runtime.command_handler = Handler()

    runtime.poll_and_handle_commands()

    assert [event.event_type for event in core.events] == [
        "command_failed",
        "gateway_error",
    ]
    assert core.events[0].command_id == "cmd_boom"
    assert runtime._last_error == "boom"


def test_runtime_tracks_realtime_register_command_after_ack() -> None:
    command = GatewayCommand(
        command_id="cmd_register_runtime",
        command_type="register_realtime",
        source="core",
        payload={"codes": ["A005930", "000660"]},
    )

    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def poll_commands(self, *, limit: int, wait_sec: float) -> list[GatewayCommand]:
            return [command]

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    client = MockKiwoomClient()
    core = Core()
    runtime = KiwoomGatewayRuntime(client=client, core_client=core)

    runtime.poll_and_handle_commands()

    assert client.registered_codes == {"005930", "000660"}
    assert runtime._registered_realtime_codes == {"005930", "000660"}
    assert [event.event_type for event in core.events] == ["command_started", "command_ack"]


def test_runtime_enforces_global_realtime_registration_cap() -> None:
    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(realtime_max_total=2),
    )

    runtime.register_realtime_codes(["005930", "000660", "035420"])

    assert client.registered_codes == {"005930", "000660"}
    assert runtime._registered_realtime_codes == {"005930", "000660"}
    heartbeat = runtime.heartbeat_payload()
    assert heartbeat["realtime_max_total"] == 2
    assert heartbeat["realtime_registration_budget_skip_count"] == 1


def test_runtime_rejects_register_command_that_exceeds_global_cap() -> None:
    command = GatewayCommand(
        command_id="cmd_register_over_budget",
        command_type="register_realtime",
        source="core",
        payload={"codes": ["000660"]},
    )

    class Core:
        def __init__(self) -> None:
            self.events: list[GatewayEvent] = []

        def poll_commands(self, *, limit: int, wait_sec: float) -> list[GatewayCommand]:
            return [command]

        def post_event(self, event: GatewayEvent) -> None:
            self.events.append(event)

    client = MockKiwoomClient()
    core = Core()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=core,
        config=KiwoomGatewayRuntimeConfig(realtime_max_total=1),
    )
    runtime.register_realtime_codes(["005930"])

    runtime.poll_and_handle_commands()

    assert client.registered_codes == {"005930"}
    assert [event.event_type for event in core.events] == [
        "command_failed",
        "gateway_error",
    ]
    assert (
        "REALTIME_SUBSCRIPTION_MAX_TOTAL_EXCEEDED"
        in core.events[0].payload["error_message"]
    )


def test_multi_condition_profiles_send_sequential_with_distinct_screens(monkeypatch) -> None:
    monkeypatch.setattr(
        "gateway.kiwoom_runtime.current_condition_session_profile",
        lambda: ConditionSessionProfile.OPENING_0900_0915,
    )
    client = MockKiwoomClient()
    client.set_conditions([(1, "Discovery"), (2, "Leader"), (3, "Pullback")])
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            condition_send_interval_sec=0,
            condition_profiles=(
                ConditionProfile(
                    condition_name="Discovery",
                    role=ConditionRole.DISCOVERY,
                    price_subscribe_policy=PriceSubscribePolicy.BATCH,
                ),
                ConditionProfile(
                    condition_name="Leader",
                    role=ConditionRole.LEADER,
                    price_subscribe_policy=PriceSubscribePolicy.IMMEDIATE,
                    priority=900,
                ),
                ConditionProfile(
                    condition_name="Pullback",
                    role=ConditionRole.PULLBACK,
                    realtime_search=False,
                    price_subscribe_policy=PriceSubscribePolicy.IMMEDIATE,
                    priority=800,
                ),
            ),
        ),
    )

    runtime.on_condition_loaded(client.condition_name_list())

    assert [call["screen_no"] for call in client.send_condition_calls] == [
        "7600",
        "7601",
        "7602",
    ]
    assert [call["condition_name"] for call in client.send_condition_calls] == [
        "Discovery",
        "Leader",
        "Pullback",
    ]
    assert client.send_condition_calls[2]["realtime"] is False
    assert runtime.heartbeat_payload()["condition_profile_screen_map"] == {
        "7600": "discovery:auto:discovery",
        "7601": "leader:auto:leader",
        "7602": "pullback:auto:pullback",
    }


def test_condition_send_pacing_never_sleeps_on_main_thread(monkeypatch) -> None:
    monkeypatch.setattr(
        "gateway.kiwoom_runtime.current_condition_session_profile",
        lambda: ConditionSessionProfile.OPENING_0900_0915,
    )

    def _forbidden_sleep(_seconds: float) -> None:
        raise AssertionError("time.sleep must not run on the Qt main thread")

    monkeypatch.setattr("gateway.kiwoom_runtime.time.sleep", _forbidden_sleep)

    clock = {"now": 100.0}
    monkeypatch.setattr("gateway.kiwoom_runtime.time.monotonic", lambda: clock["now"])

    scheduled: list[tuple[float, Callable[[], None]]] = []
    client = MockKiwoomClient()
    client.set_conditions([(1, "Discovery"), (2, "Leader"), (3, "Pullback")])
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        schedule_delayed=lambda delay, callback: scheduled.append((delay, callback)),
        config=KiwoomGatewayRuntimeConfig(
            condition_send_interval_sec=0.25,
            condition_profiles=(
                ConditionProfile(
                    condition_name="Discovery",
                    role=ConditionRole.DISCOVERY,
                    price_subscribe_policy=PriceSubscribePolicy.BATCH,
                ),
                ConditionProfile(
                    condition_name="Leader",
                    role=ConditionRole.LEADER,
                    price_subscribe_policy=PriceSubscribePolicy.IMMEDIATE,
                    priority=900,
                ),
                ConditionProfile(
                    condition_name="Pullback",
                    role=ConditionRole.PULLBACK,
                    realtime_search=False,
                    price_subscribe_policy=PriceSubscribePolicy.IMMEDIATE,
                    priority=800,
                ),
            ),
        ),
    )

    runtime.on_condition_loaded(client.condition_name_list())

    # Only the first profile is sent inline; the rest wait for the scheduler.
    assert [call["condition_name"] for call in client.send_condition_calls] == ["Discovery"]
    assert len(scheduled) == 1
    assert scheduled[0][0] > 0

    # Firing the scheduled callback after the interval sends the next profile.
    clock["now"] += 0.25
    scheduled[0][1]()
    assert [call["condition_name"] for call in client.send_condition_calls] == [
        "Discovery",
        "Leader",
    ]
    assert len(scheduled) == 2

    clock["now"] += 0.25
    scheduled[1][1]()
    assert [call["condition_name"] for call in client.send_condition_calls] == [
        "Discovery",
        "Leader",
        "Pullback",
    ]
    assert [call["screen_no"] for call in client.send_condition_calls] == [
        "7600",
        "7601",
        "7602",
    ]
    assert len(scheduled) == 2


def test_condition_tr_initial_results_batch_register_with_role_metadata(monkeypatch) -> None:
    monkeypatch.setattr(
        "gateway.kiwoom_runtime.current_condition_session_profile",
        lambda: ConditionSessionProfile.OPENING_0900_0915,
    )
    client = MockKiwoomClient()
    client.set_conditions([(1, "Discovery")])
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            condition_send_interval_sec=0,
            condition_profiles=(
                ConditionProfile(
                    condition_name="Discovery",
                    role=ConditionRole.DISCOVERY,
                    price_subscribe_policy=PriceSubscribePolicy.BATCH,
                    max_initial=10,
                ),
            ),
        ),
    )
    runtime.on_condition_loaded(client.condition_name_list())

    runtime.on_condition_tr_received(
        screen_no="7600",
        code_list="005930;000660;",
        condition_name="Discovery",
        condition_index=1,
    )

    events = [event for event in runtime._event_queue if event.event_type == "condition_event"]
    assert len(events) == 2
    first_metadata = events[0].payload["metadata"]
    assert first_metadata["condition_role"] == "DISCOVERY"
    assert first_metadata["condition_profile_id"] == "discovery:auto:discovery"
    assert first_metadata["sensor_evidence"] is True
    assert first_metadata["not_buy_signal"] is True
    assert first_metadata["condition_admission"]["register_batch"] is True
    assert client.registered_codes == {"005930", "000660"}


def test_condition_tr_initial_results_batch_once_and_ignores_realtime_rate(monkeypatch) -> None:
    monkeypatch.setattr(
        "gateway.kiwoom_runtime.current_condition_session_profile",
        lambda: ConditionSessionProfile.OPENING_0900_0915,
    )

    class TrackingClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.register_calls: list[list[str]] = []

        def register_realtime(self, codes, screen_no=None) -> None:  # type: ignore[no-untyped-def]
            self.register_calls.append(list(codes))
            super().register_realtime(codes, screen_no=screen_no)

    client = TrackingClient()
    client.set_conditions([(1, "Discovery")])
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            condition_send_interval_sec=0,
            condition_profiles=(
                ConditionProfile(
                    condition_name="Discovery",
                    role=ConditionRole.DISCOVERY,
                    price_subscribe_policy=PriceSubscribePolicy.BATCH,
                    max_initial=100,
                    max_realtime_per_min=1,
                ),
            ),
        ),
    )
    runtime.on_condition_loaded(client.condition_name_list())

    codes = [f"{index:06d}" for index in range(1000, 1100)]
    runtime.on_condition_tr_received(
        screen_no="7600",
        code_list=";".join(codes) + ";",
        condition_name="Discovery",
        condition_index=1,
    )

    assert len(client.register_calls) == 1
    assert len(client.register_calls[0]) == 80
    assert len(client.registered_codes) == 80


def test_condition_tr_initial_batch_respects_existing_registered_adaptive_cap(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "gateway.kiwoom_runtime.current_condition_session_profile",
        lambda: ConditionSessionProfile.OPENING_0900_0915,
    )

    class TrackingClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.register_calls: list[list[str]] = []

        def register_realtime(self, codes, screen_no=None) -> None:  # type: ignore[no-untyped-def]
            self.register_calls.append(list(codes))
            super().register_realtime(codes, screen_no=screen_no)

    existing_codes = {f"{index:06d}" for index in range(1000, 1070)}
    client = TrackingClient()
    client.set_conditions([(1, "Discovery")])
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            condition_send_interval_sec=0,
            condition_profiles=(
                ConditionProfile(
                    condition_name="Discovery",
                    role=ConditionRole.DISCOVERY,
                    price_subscribe_policy=PriceSubscribePolicy.BATCH,
                    max_initial=100,
                ),
            ),
        ),
    )
    runtime._registered_realtime_codes.update(existing_codes)
    runtime._parsed_price_tick_count = 1
    runtime.on_condition_loaded(client.condition_name_list())

    codes = [f"{index:06d}" for index in range(2000, 2100)]
    runtime.on_condition_tr_received(
        screen_no="7600",
        code_list=";".join(codes) + ";",
        condition_name="Discovery",
        condition_index=1,
    )

    assert len(client.register_calls) == 1
    assert len(client.register_calls[0]) == 10
    assert len(runtime._registered_realtime_codes) == 80


def test_real_condition_batch_policy_immediate_and_risk_block_no_subscribe(monkeypatch) -> None:
    monkeypatch.setattr(
        "gateway.kiwoom_runtime.current_condition_session_profile",
        lambda: ConditionSessionProfile.MORNING_TREND,
    )
    client = MockKiwoomClient()
    client.set_conditions([(2, "Leader"), (9, "RiskBlock")])
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            condition_send_interval_sec=0,
            condition_profiles=(
                ConditionProfile(
                    condition_name="Leader",
                    role=ConditionRole.LEADER,
                    price_subscribe_policy=PriceSubscribePolicy.BATCH,
                ),
                ConditionProfile(
                    condition_name="RiskBlock",
                    role=ConditionRole.RISK_BLOCK,
                    price_subscribe_policy=PriceSubscribePolicy.NONE,
                ),
            ),
        ),
    )
    runtime.on_condition_loaded(client.condition_name_list())

    leader_decision = runtime.on_condition_event(
        code="005930",
        event_type="I",
        condition_name="Leader",
        condition_index=2,
        source="real_condition",
    )
    risk_decision = runtime.on_condition_event(
        code="000660",
        event_type="I",
        condition_name="RiskBlock",
        condition_index=9,
        source="real_condition",
    )

    assert leader_decision is not None
    assert leader_decision.register_immediate is True
    assert "PRICE_SUBSCRIBE_BATCH_REALTIME_IMMEDIATE" in leader_decision.reason_codes
    assert risk_decision is not None
    assert risk_decision.subscribed is False
    assert "RISK_BLOCK_NO_PRICE_SUBSCRIBE" in risk_decision.reason_codes
    assert client.registered_codes == {"005930"}
    assert client.orders == []
    assert not any(
        event.event_type in {"command_started", "command_ack", "order_result"}
        for event in runtime._event_queue
    )


def test_runtime_recovers_realtime_registration_when_price_ticks_stall() -> None:
    class RecoverClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.remove_all_calls = 0

        def remove_all_realtime(self) -> None:
            self.remove_all_calls += 1
            super().remove_all_realtime()

    client = RecoverClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            realtime_recover_stale_sec=1,
            realtime_recover_interval_sec=60,
        ),
    )
    runtime.register_realtime_codes(["005930", "000660"])
    runtime._last_realtime_registration_at = datetime(2026, 1, 1, tzinfo=UTC)

    runtime.emit_heartbeat()

    assert client.remove_all_calls == 1
    assert client.registered_codes == {"005930", "000660"}
    assert runtime._realtime_recover_count == 1
    assert any(
        event.event_type == "gateway_log"
        and event.payload["message"] == "realtime registration reset after stale price tick"
        for event in runtime._event_queue
    )


def test_runtime_price_tick_refreshes_realtime_recovery_clock() -> None:
    class RecoverClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.remove_all_calls = 0

        def remove_all_realtime(self) -> None:
            self.remove_all_calls += 1
            super().remove_all_realtime()

    client = RecoverClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            realtime_recover_stale_sec=60,
            realtime_recover_interval_sec=60,
        ),
    )
    runtime.register_realtime_codes(["005930"])
    runtime._last_realtime_registration_at = datetime(2026, 1, 1, tzinfo=UTC)
    runtime.on_price_tick({"code": "005930", "price": 70000})

    runtime.emit_heartbeat()

    assert client.remove_all_calls == 0
    assert runtime._realtime_recover_count == 0


def test_runtime_records_quote_events_separately_from_price_ticks() -> None:
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=object())

    runtime.on_quote(
        {
            "code": "005930",
            "best_ask": 70200,
            "best_bid": 70100,
            "metadata": {"real_type": "주식우선호가"},
            "quote_only": True,
        }
    )
    payload = runtime.heartbeat_payload()

    assert runtime._last_price_tick_at is None
    assert runtime._last_quote_at is not None
    assert payload["quote_event_count"] == 1
    assert payload["realtime_real_type_counts"] == {"주식우선호가": 1}
    assert [event.event_type for event in runtime._event_queue] == ["quote_tick"]


def test_runtime_records_raw_realtime_callbacks_before_classification() -> None:
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=object())

    runtime.on_realtime_data(
        code="005930",
        real_type="업종지수",
        real_data_present=True,
    )
    payload = runtime.heartbeat_payload()

    assert payload["latest_realtime_callback_at"]
    assert payload["realtime_callback_count"] == 1
    assert payload["realtime_callback_real_type_counts"] == {"업종지수": 1}
    assert payload["realtime_real_type_counts"] == {}


def test_kiwoom_price_tick_parse_error_keeps_raw_callback_counter_separate() -> None:
    from gateway.kiwoom_client import KiwoomClient

    client = object.__new__(KiwoomClient)
    client.price_received = Signal()
    client.price_tick_received = Signal()
    client.quote_received = Signal()
    client.realtime_data_received = Signal()
    client.realtime_parse_error = Signal()
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    client.get_code_name = lambda code: (_ for _ in ()).throw(RuntimeError("name boom"))
    client._real_raw = lambda code, fid: "70000"
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=object())
    client.active_x_thread_audit.connect(
        lambda payload: runtime.on_active_x_thread_audit(dict(payload))
    )
    client.realtime_data_received.connect(
        lambda code, real_type, present: runtime.on_realtime_data(
            code=str(code),
            real_type=str(real_type),
            real_data_present=bool(present),
        )
    )
    client.realtime_parse_error.connect(
        lambda payload: runtime.on_realtime_parse_error(dict(payload))
    )

    client._on_receive_real_data("005930", "주식체결", "")
    payload = runtime.heartbeat_payload()

    assert payload["raw_callback_counts"]["OnReceiveRealData"] == 1
    assert payload["raw_realtime_callback_count"] == 1
    assert payload["parsed_price_tick_count"] == 0
    assert payload["realtime_parse_error_count"] == 1
    assert payload["realtime_subscription_health"] == "PARSE_ERROR"


def test_kiwoom_market_index_parse_error_is_separate_from_price_tick_errors() -> None:
    from gateway.kiwoom_client import KiwoomClient

    client = object.__new__(KiwoomClient)
    client.price_received = Signal()
    client.price_tick_received = Signal()
    client.quote_received = Signal()
    client.market_index_tick_received = Signal()
    client.realtime_data_received = Signal()
    client.realtime_parse_error = Signal()
    client.active_x_thread_audit = Signal()
    client._pending_thread_audit_events = []
    client._market_index_real_raw = lambda code, fid: ""
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=object())
    client.active_x_thread_audit.connect(
        lambda payload: runtime.on_active_x_thread_audit(dict(payload))
    )
    client.realtime_data_received.connect(
        lambda code, real_type, present: runtime.on_realtime_data(
            code=str(code),
            real_type=str(real_type),
            real_data_present=bool(present),
        )
    )
    client.realtime_parse_error.connect(
        lambda payload: runtime.on_realtime_parse_error(dict(payload))
    )

    client._on_receive_real_data("001", "업종지수", "")
    payload = runtime.heartbeat_payload()

    assert payload["raw_callback_counts"]["OnReceiveRealData"] == 1
    assert payload["raw_realtime_callback_count"] == 1
    assert payload["parsed_price_tick_count"] == 0
    assert payload["realtime_parse_error_count"] == 0
    assert payload["market_index_callback_count"] == 1
    assert payload["market_index_parse_error_count"] == 1
    assert payload["latest_market_index_parse_error"]["reason"] == "INDEX_PARSE_ERROR"
    assert payload["realtime_subscription_health"] == "NOT_REQUESTED"


def test_runtime_records_realtime_registration_result_in_heartbeat() -> None:
    runtime = KiwoomGatewayRuntime(client=MockKiwoomClient(), core_client=object())

    runtime.on_realtime_registration_result(
        {
            "screen_no": "5000",
            "codes": ["005930", "000660"],
            "fid_string": "10;12;13",
            "fid_count": 3,
            "opt_type": "0",
            "result_code": 0,
            "success": True,
        }
    )
    payload = runtime.heartbeat_payload()

    assert payload["latest_realtime_registration_result"]["result_code"] == 0
    assert payload["latest_realtime_registration_result"]["success"] is True
    assert runtime._event_queue[-1].event_type == "gateway_log"
    assert runtime._event_queue[-1].payload["message"] == "realtime registration result"


def test_realtime_registration_success_without_callback_reports_waiting_or_timeout() -> None:
    runtime = KiwoomGatewayRuntime(
        client=MockKiwoomClient(),
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(realtime_callback_timeout_sec=15),
    )

    runtime.register_realtime_codes(["005930"])
    runtime.on_realtime_registration_result(
        {"codes": ["005930"], "result_code": 0, "success": True}
    )
    waiting_payload = runtime.heartbeat_payload()
    runtime._last_realtime_registration_at = datetime(2026, 1, 1, tzinfo=UTC)
    timeout_payload = runtime.heartbeat_payload()

    assert waiting_payload["realtime_registration_requested_count"] == 1
    assert waiting_payload["realtime_registration_success_count"] == 1
    assert waiting_payload["realtime_subscription_health"] == "REGISTERED_WAITING_CALLBACK"
    assert timeout_payload["realtime_subscription_health"] == "CALLBACK_TIMEOUT"


def test_realtime_timeout_marks_core_io_blocking_when_main_thread_io_was_observed() -> None:
    runtime = KiwoomGatewayRuntime(
        client=MockKiwoomClient(),
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(realtime_callback_timeout_sec=15),
    )

    runtime.register_realtime_codes(["005930"])
    runtime.on_realtime_registration_result(
        {"codes": ["005930"], "result_code": 0, "success": True}
    )
    runtime._last_realtime_registration_at = datetime(2026, 1, 1, tzinfo=UTC)
    runtime._polled_count = 1

    assert runtime.heartbeat_payload()["realtime_subscription_health"] == (
        "CORE_IO_BLOCKING_SUSPECTED"
    )


def test_condition_load_timeout_retries_once_then_marks_callback_timeout() -> None:
    class ConditionClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.load_calls = 0

        def load_conditions(self) -> int:
            self.load_calls += 1
            return super().load_conditions()

    client = ConditionClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=object(),
        config=KiwoomGatewayRuntimeConfig(
            condition_load_timeout_sec=1,
            condition_load_max_retry=1,
        ),
    )
    runtime._load_conditions()
    runtime._condition_load_requested_at = datetime(2026, 1, 1, tzinfo=UTC)

    runtime.check_condition_load_timeout()
    assert client.load_calls == 2
    assert runtime.heartbeat_payload()["condition_load_state"] == "LOADING"

    runtime._condition_load_requested_at = datetime(2026, 1, 1, tzinfo=UTC)
    runtime.check_condition_load_timeout()
    payload = runtime.heartbeat_payload()

    assert payload["condition_load_state"] == "CALLBACK_TIMEOUT"
    assert payload["condition_callback_health"] == "ACTIVE_X_CALLBACK_SUSPECTED"
    assert payload["condition_load_timeout_count"] == 2
    assert runtime._event_queue[-1].event_type == "gateway_error"
    assert runtime._event_queue[-1].payload["message"] == "CONDITION_VER_CALLBACK_TIMEOUT"
    assert "ACTIVE_X_CALLBACK_SUSPECTED" in runtime._event_queue[-1].payload["reason_codes"]


def test_condition_event_parser_normalizes_action_and_code() -> None:
    enter = condition_event_payload(
        code="A005930",
        event_type="I",
        condition_name="Breakout",
        condition_index=7,
        name="삼성전자",
    )
    exit_event = condition_event_payload(
        code="A005930",
        event_type="D",
        condition_name="Breakout",
        condition_index=7,
        name="삼성전자",
    )

    assert BrokerConditionEvent.from_dict(enter).action.value == "ENTER"
    assert BrokerConditionEvent.from_dict(exit_event).action.value == "EXIT"
    assert enter["code"] == "005930"
    assert enter["metadata"]["condition_index"] == 7


def test_server_gubun_mapping_and_heartbeat_status_projection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "heartbeat.sqlite3")
    event = GatewayEvent(
        event_id="evt_kiwoom_heartbeat",
        event_type="heartbeat",
        source="kiwoom_gateway",
        ts=TS,
        payload={
            "status": "ok",
            "kiwoom_logged_in": True,
            "orderable": True,
            "broker_name": "KIWOOM",
            "broker_env": "SIMULATION",
            "server_mode": "SIMULATION",
            "account_mode": "SIMULATION",
            "server_gubun": "1",
        },
    )

    append_gateway_event(connection, event)
    status = {
        row["key"]: row["value"]
        for row in connection.execute("SELECT key, value FROM gateway_status")
    }
    connection.close()

    assert broker_env_from_server_gubun("1") == "SIMULATION"
    assert broker_env_from_server_gubun("0") == "REAL"
    assert status["broker_env"] == "SIMULATION"
    assert status["gateway_orderable"] == "true"


def test_kiwoom_handler_request_tr_emits_tr_response() -> None:
    client = MockKiwoomClient()
    client.set_tr_rows([{"종목코드": "005930", "종목명": "삼성전자", "현재가": "70000"}])
    handler = KiwoomGatewayCommandHandler(client)
    command = GatewayCommand(
        command_id="cmd_tr",
        command_type="request_tr",
        source="core",
        payload={
            "request_id": "tr1",
            "tr_code": "OPT10001",
            "request_name": "stock_basic",
            "metadata": {"projection_source": "test_kiwoom_tr_metadata"},
            "params": {"종목코드": "005930"},
            "fields": ["종목코드", "종목명", "현재가"],
        },
    )

    events = handler.handle(command)

    assert [event.event_type for event in events] == [
        "command_started",
        "tr_response",
        "command_ack",
    ]
    response = BrokerTrResponse.from_dict(events[1].payload)
    assert response.metadata["projection_source"] == "test_kiwoom_tr_metadata"
    assert response.rows[0]["종목코드"] == "005930"


def test_kiwoom_handler_request_tr_can_force_single_output_record() -> None:
    class MixedOutputTrClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.repeat_count_calls: list[tuple[str, str]] = []
            self.comm_data_calls: list[tuple[str, str, int, str]] = []

        def get_repeat_count(self, tr_code: str, rq_name: str) -> int:
            self.repeat_count_calls.append((tr_code, rq_name))
            return 20

        def get_comm_data(
            self,
            tr_code: str,
            rq_name: str,
            index: int,
            item_name: str,
        ) -> str:
            self.comm_data_calls.append((tr_code, rq_name, index, item_name))
            if rq_name != "업종현재가" or index != 0:
                return ""
            return {
                "현재가": "+7475.94",
                "전일대비": "+184.03",
                "등락률": "+2.52",
            }.get(item_name, "")

    client = MixedOutputTrClient()
    handler = KiwoomGatewayCommandHandler(client)
    command = GatewayCommand(
        command_id="cmd_index_tr",
        command_type="request_tr",
        source="core",
        payload={
            "request_id": "market_index_tr_bootstrap:KOSPI:test",
            "tr_code": "OPT20001",
            "request_name": "market_index_tr_bootstrap_kospi",
            "params": {"시장구분": "0", "업종코드": "001"},
            "fields": ["현재가", "전일대비", "등락률"],
            "row_mode": "single",
            "output_record_name": "업종현재가",
        },
    )

    events = handler.handle(command)
    response = BrokerTrResponse.from_dict(events[1].payload)

    assert [event.event_type for event in events] == [
        "command_started",
        "tr_response",
        "command_ack",
    ]
    assert response.rows == [
        {"현재가": "+7475.94", "전일대비": "+184.03", "등락률": "+2.52"}
    ]
    assert client.repeat_count_calls == []
    assert {call[1] for call in client.comm_data_calls} == {"업종현재가"}
    assert events[2].payload["details"]["warnings"] == [
        "TR_SINGLE_ROW_EXPLICIT:OPT20001"
    ]


def test_kiwoom_handler_request_tr_can_force_multi_output_record() -> None:
    class MultiOutputTrClient(MockKiwoomClient):
        def get_repeat_count(self, tr_code: str, rq_name: str) -> int:
            return 2 if rq_name == "거래대금상위" else 0

        def get_comm_data(
            self,
            tr_code: str,
            rq_name: str,
            index: int,
            item_name: str,
        ) -> str:
            if rq_name != "거래대금상위":
                return ""
            rows = (
                {"종목코드": "005930", "현재순위": "1"},
                {"종목코드": "000660", "현재순위": "2"},
            )
            return rows[index].get(item_name, "")

    client = MultiOutputTrClient()
    handler = KiwoomGatewayCommandHandler(client)
    command = GatewayCommand(
        command_id="cmd_scan_tr",
        command_type="request_tr",
        source="core",
        payload={
            "request_id": "market_scan:TRADE_VALUE:KOSPI:test",
            "tr_code": "OPT10032",
            "request_name": "market_scan_trade_value_kospi",
            "params": {"시장구분": "001"},
            "fields": ["종목코드", "현재순위"],
            "row_mode": "multi",
            "output_record_name": "거래대금상위",
        },
    )

    events = handler.handle(command)
    response = BrokerTrResponse.from_dict(events[1].payload)

    assert response.rows == [
        {"종목코드": "005930", "현재순위": "1"},
        {"종목코드": "000660", "현재순위": "2"},
    ]
    assert events[2].payload["details"]["warnings"] == []


def test_runtime_request_tr_completes_from_deferred_callback_without_blocking() -> None:
    class DeferredTrClient(MockKiwoomClient):
        def __init__(self) -> None:
            super().__init__()
            self.pending_tr: tuple[str, str, str] | None = None

        def comm_rq_data(
            self,
            rq_name: str,
            tr_code: str,
            prev_next: int,
            screen_no: str,
        ) -> int:
            del prev_next
            self.pending_tr = (str(screen_no), str(rq_name), str(tr_code))
            return 0

        def emit_pending_tr(self) -> None:
            assert self.pending_tr is not None
            screen_no, rq_name, tr_code = self.pending_tr
            self.tr_data_received.emit(
                screen_no,
                rq_name,
                tr_code,
                rq_name,
                "",
                0,
                "",
                "",
                "",
            )

    client = DeferredTrClient()
    client.set_tr_rows([{"종목코드": "005930", "종목명": "삼성전자", "현재가": "70000"}])
    runtime = KiwoomGatewayRuntime(client=client, core_client=object())
    command = GatewayCommand(
        command_id="cmd_async_tr",
        command_type="request_tr",
        source="core",
        payload={
            "request_id": "tr_async",
            "tr_code": "OPT10001",
            "request_name": "stock_basic",
            "metadata": {"projection_source": "test_kiwoom_tr_metadata"},
            "params": {"종목코드": "005930"},
            "fields": ["종목코드", "종목명", "현재가"],
        },
    )

    runtime.handle_commands([command])
    event_types_before_callback = [event.event_type for event in runtime._event_queue]
    client.emit_pending_tr()
    event_types_after_callback = [event.event_type for event in runtime._event_queue]
    response = next(event for event in runtime._event_queue if event.event_type == "tr_response")

    assert event_types_before_callback == ["command_started"]
    assert event_types_after_callback == ["command_started", "tr_response", "command_ack"]
    assert BrokerTrResponse.from_dict(response.payload).metadata[
        "projection_source"
    ] == "test_kiwoom_tr_metadata"
    assert BrokerTrResponse.from_dict(response.payload).rows[0]["종목코드"] == "005930"


def test_kiwoom_handler_register_realtime_and_send_condition_call_client() -> None:
    client = MockKiwoomClient()
    handler = KiwoomGatewayCommandHandler(client)

    register_events = handler.handle(
        GatewayCommand(
            command_id="cmd_register",
            command_type="register_realtime",
            source="core",
            payload={"codes": ["A005930", "000660"]},
        )
    )
    condition_events = handler.handle(
        GatewayCommand(
            command_id="cmd_condition",
            command_type="send_condition",
            source="core",
            payload={"condition_name": "Breakout", "condition_index": 3},
        )
    )

    assert client.registered_codes == {"005930", "000660"}
    assert client.send_condition_calls[0]["condition_name"] == "Breakout"
    assert register_events[-1].event_type == "command_ack"
    assert condition_events[-1].event_type == "command_ack"


def test_kiwoom_handler_live_sim_send_order_requires_safety_metadata() -> None:
    client = MockKiwoomClient()
    handler = KiwoomGatewayCommandHandler(
        client,
        on_durable_order_pre_ack=_durable_pre_ack_callback(),
    )

    rejected = handler.handle(
        GatewayCommand(
            command_id="cmd_rejected",
            command_type="send_order",
            source="live_sim",
            payload={"code": "005930"},
        )
    )
    accepted = handler.handle(_live_sim_order_command())

    assert rejected[0].event_type == "command_failed"
    assert "idempotency" in rejected[0].payload["error_message"]
    assert [event.event_type for event in accepted] == ["command_started", "command_ack"]
    assert len(client.orders) == 1
    assert client.orders[0].code == "005930"
    assert client.orders[0].order_exchange == "KRX"
    assert accepted[-1].payload["details"]["order_exchange"] == "KRX"


def test_kiwoom_handler_expired_live_sim_send_order_emits_command_failed() -> None:
    client = MockKiwoomClient()
    handler = KiwoomGatewayCommandHandler(client)
    base = _live_sim_order_command(command_id="cmd_expired_before_execution")
    command = GatewayCommand(
        command_id=base.command_id,
        command_type=base.command_type,
        source=base.source,
        idempotency_key=base.idempotency_key,
        payload={
            **base.payload,
            "_gateway_command_expires_at": "2020-01-01T00:00:00Z",
        },
    )

    events = handler.handle(command)

    assert [event.event_type for event in events] == ["command_failed"]
    assert events[0].payload["error_message"] == "EXPIRED_BEFORE_EXECUTION"
    assert client.orders == []


def test_kiwoom_handler_live_sim_send_order_forwards_nxt_exchange() -> None:
    client = MockKiwoomClient()
    handler = KiwoomGatewayCommandHandler(
        client,
        on_durable_order_pre_ack=_durable_pre_ack_callback(),
    )

    accepted = handler.handle(
        _live_sim_order_command(command_id="cmd_live_sim_nxt", order_exchange="NXT")
    )

    assert [event.event_type for event in accepted] == ["command_started", "command_ack"]
    assert len(client.orders) == 1
    assert client.orders[0].order_exchange == "NXT"
    assert client.orders[0].to_dict()["kiwoom_code"] == "005930_NX"
    assert accepted[-1].payload["details"]["order_exchange"] == "NXT"


def test_kiwoom_handler_rate_limit_delays_without_failure_event() -> None:
    now = [100.0]
    client = MockKiwoomClient()
    handler = KiwoomGatewayCommandHandler(client, clock=lambda: now[0])
    command = GatewayCommand(
        command_id="cmd_condition_rate_limit",
        command_type="send_condition",
        source="core",
        payload={"condition_name": "Breakout", "condition_index": 3},
    )

    first = handler.handle(command)
    second = handler.handle(
        GatewayCommand(
            command_id="cmd_condition_rate_limit_2",
            command_type="send_condition",
            source="core",
            payload={"condition_name": "Breakout", "condition_index": 3},
        )
    )

    assert [event.event_type for event in first] == ["command_started", "command_ack"]
    assert [event.event_type for event in second] == ["command_started", "rate_limited"]
    assert second[1].payload["reason"] == "type_min_interval"
    assert len(client.send_condition_calls) == 1


def test_kiwoom_handler_writes_order_pre_ack_journal_before_ack(tmp_path) -> None:
    journal_path = tmp_path / "orders.jsonl"
    client = MockKiwoomClient()
    durable_events: list[GatewayEvent] = []
    handler = KiwoomGatewayCommandHandler(
        client,
        on_durable_order_pre_ack=_durable_pre_ack_callback(durable_events),
        order_journal=OrderPreAckJournal(journal_path),
    )

    events = handler.handle(_live_sim_order_command(command_id="cmd_journaled"))

    journal_rows = [
        json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [event.event_type for event in events] == ["command_started", "command_ack"]
    assert [event.event_type for event in durable_events] == ["order_pre_ack"]
    assert [row["status"] for row in journal_rows] == ["PRE_ACK", "BROKER_ACCEPTED"]
    assert journal_rows[0]["command_id"] == "cmd_journaled"
    assert durable_events[0].command_id == "cmd_journaled"
    recovery_events = OrderPreAckJournal(journal_path).recovery_events(source="kiwoom_gateway")
    assert [event.event_type for event in recovery_events] == ["order_pre_ack"]
    assert recovery_events[0].payload["status"] == "RECOVERED_BROKER_ACCEPTED"


def test_kiwoom_handler_emits_order_start_before_send_order_call(tmp_path) -> None:
    async_events: list[GatewayEvent] = []

    class InspectingClient(MockKiwoomClient):
        def send_order(self, request: KiwoomOrderRequest) -> KiwoomOrderResult:
            assert [event.event_type for event in async_events] == [
                "command_started",
                "order_pre_ack",
            ]
            return super().send_order(request)

    client = InspectingClient()
    handler = KiwoomGatewayCommandHandler(
        client,
        on_async_events=lambda events: async_events.extend(events),
        on_durable_order_pre_ack=_durable_pre_ack_callback(async_events),
        order_journal=OrderPreAckJournal(tmp_path / "orders.jsonl"),
    )

    returned_events = handler.handle(
        _live_sim_order_command(command_id="cmd_started_before_send")
    )

    assert [event.event_type for event in async_events] == [
        "command_started",
        "order_pre_ack",
    ]
    assert [event.event_type for event in returned_events] == ["command_ack"]
    assert async_events[0].command_id == "cmd_started_before_send"


def test_kiwoom_handler_blocks_broker_call_without_durable_core_pre_ack() -> None:
    client = MockKiwoomClient()
    handler = KiwoomGatewayCommandHandler(client)

    events = handler.handle(
        _live_sim_order_command(command_id="cmd_missing_durable_pre_ack")
    )

    assert [event.event_type for event in events] == [
        "command_started",
        "command_failed",
    ]
    assert events[-1].payload["error_message"].startswith(
        "DURABLE_DB_PRE_ACK_FAILED"
    )
    assert client.orders == []


def test_kiwoom_handler_marks_broker_call_exception_unconfirmed(tmp_path) -> None:
    class RaisingClient(MockKiwoomClient):
        def send_order(self, request: KiwoomOrderRequest) -> KiwoomOrderResult:
            del request
            raise RuntimeError("COM transport outcome unknown")

    journal_path = tmp_path / "orders-unconfirmed.jsonl"
    client = RaisingClient()
    handler = KiwoomGatewayCommandHandler(
        client,
        on_durable_order_pre_ack=_durable_pre_ack_callback(),
        order_journal=OrderPreAckJournal(journal_path),
    )

    events = handler.handle(
        _live_sim_order_command(command_id="cmd_broker_call_unconfirmed")
    )
    journal_rows = [
        json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()
    ]

    assert [event.event_type for event in events] == [
        "command_started",
        "order_broker_unconfirmed",
    ]
    assert events[-1].payload["broker_call_attempted"] is True
    assert events[-1].payload["broker_acceptance_unknown"] is True
    assert [row["status"] for row in journal_rows] == ["PRE_ACK", "UNCONFIRMED"]
    assert OrderPreAckJournal(journal_path).recovery_events(
        source="kiwoom_gateway"
    ) == []
    assert client.orders == []


def test_runtime_posts_durable_pre_ack_synchronously_before_broker_call() -> None:
    posted_events: list[GatewayEvent] = []

    class Core:
        def post_event(self, event: GatewayEvent) -> dict[str, object]:
            assert client.orders == []
            posted_events.append(event)
            return {
                "accepted": True,
                "broker_boundary_state": "PRE_ACK_RECORDED",
                "durable_pre_ack_recorded": True,
            }

    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(client=client, core_client=Core())

    returned_events = runtime.command_handler.handle(
        _live_sim_order_command(command_id="cmd_runtime_durable_pre_ack")
    )
    heartbeat = runtime.heartbeat_payload()

    assert [event.event_type for event in posted_events] == ["order_pre_ack"]
    assert [event.event_type for event in returned_events] == ["command_ack"]
    assert len(client.orders) == 1
    assert heartbeat["durable_pre_ack_posted_count"] == 1
    assert heartbeat["last_durable_pre_ack_at"]
    assert heartbeat["last_durable_pre_ack_error"] == ""


def test_runtime_blocks_order_when_core_event_posting_is_disabled() -> None:
    class Core:
        def post_event(self, event: GatewayEvent) -> dict[str, object]:
            del event
            raise AssertionError("disabled event posting must not call Core")

    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=Core(),
        config=KiwoomGatewayRuntimeConfig(event_posting_enabled=False),
    )

    returned_events = runtime.command_handler.handle(
        _live_sim_order_command(command_id="cmd_event_posting_disabled")
    )

    assert [event.event_type for event in returned_events] == ["command_failed"]
    assert returned_events[0].payload["error_message"].startswith(
        "DURABLE_DB_PRE_ACK_FAILED"
    )
    assert "disabled" in runtime.heartbeat_payload()["last_durable_pre_ack_error"]
    assert client.orders == []


def test_kiwoom_handler_cancel_uses_same_durable_pre_ack_boundary() -> None:
    durable_events: list[GatewayEvent] = []
    client = MockKiwoomClient()
    handler = KiwoomGatewayCommandHandler(
        client,
        on_durable_order_pre_ack=_durable_pre_ack_callback(durable_events),
    )

    events = handler.handle(_live_sim_cancel_command("cmd_cancel_durable"))

    assert [event.event_type for event in durable_events] == ["order_pre_ack"]
    assert durable_events[0].payload["original_order_no"] == "SIM-ORIGINAL-1"
    assert [event.event_type for event in events] == [
        "command_started",
        "command_ack",
    ]
    assert len(client.orders) == 1
    assert client.orders[0].side == "BUY_CANCEL"


def test_pending_order_registry_uses_command_id_before_ambiguous_signature() -> None:
    registry = PendingOrderRegistry()
    request_one = KiwoomOrderRequest(
        account="1234567890",
        code="005930",
        quantity=1,
        price=70000,
        side="BUY",
        tag="cmd_order_1",
        command_id="cmd_order_1",
        idempotency_key="idem-1",
        metadata={"live_sim_intent_id": "intent-1"},
    )
    request_two = KiwoomOrderRequest(
        account="1234567890",
        code="005930",
        quantity=1,
        price=70000,
        side="BUY",
        tag="cmd_order_2",
        command_id="cmd_order_2",
        idempotency_key="idem-2",
        metadata={"live_sim_intent_id": "intent-2"},
    )
    command_one = _live_sim_order_command(command_id="cmd_order_1")
    command_two = _live_sim_order_command(command_id="cmd_order_2")

    registry.record_ack(
        command_one,
        request_one,
        KiwoomOrderResult(
            ok=True,
            code=0,
            message="accepted",
            request=request_one,
            order_no="broker-1",
        ),
    )
    registry.record_ack(
        command_two,
        request_two,
        KiwoomOrderResult(
            ok=True,
            code=0,
            message="accepted",
            request=request_two,
            order_no="broker-2",
        ),
    )

    ambiguous = registry.enrich_chejan_payload(
        {"account_id": "1234567890", "code": "005930", "side": "BUY"}
    )
    matched = registry.enrich_chejan_payload(
        {
            "account_id": "1234567890",
            "code": "005930",
            "side": "BUY",
            "command_id": "cmd_order_1",
        }
    )

    assert "command_id" not in ambiguous
    assert matched["command_id"] == "cmd_order_1"
    assert matched["live_sim_intent_id"] == "intent-1"


def test_kiwoom_runtime_dead_man_cancel_is_cancel_only_after_core_poll_failure() -> None:
    class FailingCoreClient:
        def poll_commands(self, *, limit: int = 20, wait_sec: float = 0) -> list[GatewayCommand]:
            raise RuntimeError("core unavailable")

    client = MockKiwoomClient()
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=FailingCoreClient(),
        config=KiwoomGatewayRuntimeConfig(
            dead_man_cancel_enabled=True,
            dead_man_cancel_core_stale_sec=0,
            core_io_enabled=True,
            command_polling_enabled=True,
            event_posting_enabled=False,
        ),
    )
    command = _live_sim_order_command(command_id="cmd_dead_man_source")
    request = KiwoomOrderRequest(
        account="1234567890",
        code="005930",
        quantity=3,
        price=70000,
        side="BUY",
        tag="cmd_dead_man_source",
        command_id=command.command_id,
        idempotency_key=command.idempotency_key or "",
        metadata={"live_sim_intent_id": "intent-dead-man"},
    )
    runtime.pending_orders.record_ack(
        command,
        request,
        KiwoomOrderResult(
            ok=True,
            code=0,
            message="accepted",
            request=request,
            order_no="DM-ORDER-1",
        ),
    )
    other_command = GatewayCommand(
        command_id="cmd_not_live_sim",
        command_type="send_order",
        source="manual",
        idempotency_key="idem-not-live-sim",
        payload={
            "account_id": "1234567890",
            "account_mode": "SIMULATION",
            "broker_env": "SIMULATION",
            "server_mode": "SIMULATION",
            "code": "000660",
            "side": "BUY",
            "quantity": 5,
        },
    )
    other_request = KiwoomOrderRequest(
        account="1234567890",
        code="000660",
        quantity=5,
        price=100000,
        side="BUY",
        tag="cmd_not_live_sim",
        command_id=other_command.command_id,
        idempotency_key=other_command.idempotency_key or "",
        metadata={},
    )
    runtime.pending_orders.record_ack(
        other_command,
        other_request,
        KiwoomOrderResult(
            ok=True,
            code=0,
            message="accepted",
            request=other_request,
            order_no="DM-ORDER-IGNORED",
        ),
    )

    runtime.poll_and_handle_commands()
    runtime.emit_heartbeat()

    assert len(client.orders) == 1
    cancel_request = client.orders[0]
    assert cancel_request.side == "BUY_CANCEL"
    assert cancel_request.original_order_no == "DM-ORDER-1"
    assert cancel_request.quantity == 3
    assert cancel_request.metadata["cancel_only"] is True


def test_kiwoom_handler_rejects_live_real_and_cancel_modify() -> None:
    real_client = MockKiwoomClient()
    real_client.server_gubun = "0"
    real_handler = KiwoomGatewayCommandHandler(real_client)
    mock_handler = KiwoomGatewayCommandHandler(MockKiwoomClient())

    real_rejected = real_handler.handle(
        _live_sim_order_command(command_id="cmd_real", order_exchange="NXT")
    )
    cancel_rejected = mock_handler.handle(
        GatewayCommand(
            command_id="cmd_cancel",
            command_type="cancel_order",
            source="core",
            payload={"code": "005930"},
        )
    )
    modify_rejected = mock_handler.handle(
        GatewayCommand(
            command_id="cmd_modify",
            command_type="modify_order",
            source="core",
            payload={"code": "005930"},
        )
    )

    assert real_rejected[0].event_type == "command_failed"
    assert "simulation server" in real_rejected[0].payload["error_message"]
    assert real_client.orders == []
    assert cancel_rejected[0].event_type == "command_failed"
    assert modify_rejected[0].event_type == "command_failed"


def _wait_until(predicate: Callable[[], bool], *, timeout_sec: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached before timeout")


def _drain_worker_once(runtime: KiwoomGatewayRuntime) -> bool:
    runtime.drain_core_io_worker()
    return False


def _durable_pre_ack_callback(
    captured: list[GatewayEvent] | None = None,
) -> Callable[[GatewayEvent], dict[str, object]]:
    def callback(event: GatewayEvent) -> dict[str, object]:
        if captured is not None:
            captured.append(event)
        return {
            "accepted": True,
            "broker_boundary_state": "PRE_ACK_RECORDED",
            "durable_pre_ack_recorded": True,
        }

    return callback


def _live_sim_order_command(
    command_id: str = "cmd_live_sim",
    *,
    order_exchange: str = "KRX",
) -> GatewayCommand:
    idempotency_key = f"idem-{command_id}"
    return GatewayCommand(
        command_id=command_id,
        command_type="send_order",
        source="live_sim",
        idempotency_key=idempotency_key,
        payload={
            "account_id": "1234567890",
            "account_mode": "SIMULATION",
            "broker_env": "SIMULATION",
            "server_mode": "SIMULATION",
            "code": "005930",
            "name": "삼성전자",
            "side": "BUY",
            "quantity": 1,
            "price": 70000,
            "limit_price": 70000,
            "order_type": "LIMIT",
            "hoga": "00",
            "order_exchange": order_exchange,
            "mode": "LIVE_SIM",
            "live_mode": "LIVE_SIM",
            "live_sim_intent_id": "live_sim_intent_1",
            "idempotency_key": idempotency_key,
            "metadata": {
                "source": "live_sim",
                "live_sim_only": True,
                "live_real_allowed": False,
                "live_sim_intent_id": "live_sim_intent_1",
                "idempotency_key": idempotency_key,
                "order_exchange": order_exchange,
            },
        },
    )


def _live_sim_cancel_command(command_id: str) -> GatewayCommand:
    idempotency_key = f"idem-{command_id}"
    return GatewayCommand(
        command_id=command_id,
        command_type="cancel_order",
        source="live_sim",
        idempotency_key=idempotency_key,
        payload={
            "account_id": "1234567890",
            "account_mode": "SIMULATION",
            "broker_env": "SIMULATION",
            "server_mode": "SIMULATION",
            "code": "005930",
            "side": "BUY_CANCEL",
            "quantity": 1,
            "original_order_no": "SIM-ORIGINAL-1",
            "mode": "LIVE_SIM",
            "live_mode": "LIVE_SIM",
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "idempotency_key": idempotency_key,
            "metadata": {
                "source": "live_sim",
                "live_sim_only": True,
                "live_real_allowed": False,
                "idempotency_key": idempotency_key,
                "cancel_intent_id": "cancel-intent-1",
                "original_live_sim_order_id": "live-sim-order-1",
            },
        },
    )
