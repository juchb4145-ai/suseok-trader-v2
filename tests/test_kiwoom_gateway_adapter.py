from __future__ import annotations

import importlib
import sys
from datetime import UTC, datetime

from apps.kiwoom_gateway import parse_args, request_kiwoom_login
from domain.broker.commands import GatewayCommand
from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.tr import BrokerTrResponse
from gateway.kiwoom_client import (
    FID_ACC_TRADE_VALUE,
    FID_ACC_VOLUME,
    FID_BEST_ASK,
    FID_BEST_BID,
    FID_CHANGE_RATE,
    FID_CURRENT_PRICE,
    FID_EXECUTION_STRENGTH,
    FID_HIGH_PRICE,
    FID_LOW_PRICE,
    FID_OPEN_PRICE,
    FID_TRADE_TIME,
    KiwoomClient,
    MockKiwoomClient,
    Signal,
    broker_env_from_server_gubun,
    condition_event_payload,
    is_quote_real_type,
    is_price_tick_real_type,
    parse_quote_from_fids,
    parse_price_tick_from_fids,
    realtime_code_for_exchange,
)
from gateway.kiwoom_command_handlers import KiwoomGatewayCommandHandler
from gateway.kiwoom_runtime import KiwoomGatewayRuntime, KiwoomGatewayRuntimeConfig
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


def test_kiwoom_gateway_realtime_exchange_option() -> None:
    assert parse_args([]).realtime_exchange == "krx"
    assert parse_args(["--realtime-exchange", "nxt"]).realtime_exchange == "nxt"
    assert parse_args(["--realtime-exchange", "all"]).realtime_exchange == "all"


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


def test_login_request_skips_connect_state_fallback_for_event_loop_client() -> None:
    class Client:
        login_waits_for_event_loop = True

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
    assert runtime.connected_calls == []
    assert runtime._login_in_progress is True
    assert KiwoomClient.login_waits_for_event_loop is True


def test_event_connect_callback_exits_login_loop_before_runtime_signal() -> None:
    class Loop:
        def __init__(self) -> None:
            self.exited = False

        def exit(self) -> None:
            self.exited = True

    client = object.__new__(KiwoomClient)
    client.active_x_thread_audit = Signal()
    client.connected = Signal()
    client._pending_thread_audit_events = []
    client._login_event_loop = Loop()
    client._login_callback_result = None
    connected_calls: list[tuple[bool, int, str]] = []
    client.connected.connect(
        lambda ok, code, message: connected_calls.append((bool(ok), int(code), str(message)))
    )

    client._on_event_connect(0)

    assert client._login_event_loop.exited is True
    assert client._login_callback_result == (0, "정상처리")
    assert connected_calls == []


def test_condition_ver_callback_exits_condition_loop_before_runtime_signal() -> None:
    class Loop:
        def __init__(self) -> None:
            self.exited = False

        def exit(self) -> None:
            self.exited = True

    client = object.__new__(KiwoomClient)
    client.active_x_thread_audit = Signal()
    client.condition_load_result = Signal()
    client.condition_loaded = Signal()
    client._pending_thread_audit_events = []
    client._condition_event_loop = Loop()
    client._condition_callback_result = None
    result_calls: list[tuple[bool, str]] = []
    client.condition_load_result.connect(
        lambda success, message: result_calls.append((bool(success), str(message)))
    )

    client._on_receive_condition_ver(1, "ok")

    assert client._condition_event_loop.exited is True
    assert client._condition_callback_result == (True, "ok")
    assert result_calls == []


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
    assert payload["condition_load_timeout_count"] == 2
    assert runtime._event_queue[-1].event_type == "gateway_error"
    assert runtime._event_queue[-1].payload["message"] == "CONDITION_VER_CALLBACK_TIMEOUT"


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
    assert response.rows[0]["종목코드"] == "005930"


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
    handler = KiwoomGatewayCommandHandler(client)

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


def test_kiwoom_handler_rejects_live_real_and_cancel_modify() -> None:
    real_client = MockKiwoomClient()
    real_client.server_gubun = "0"
    real_handler = KiwoomGatewayCommandHandler(real_client)
    mock_handler = KiwoomGatewayCommandHandler(MockKiwoomClient())

    real_rejected = real_handler.handle(_live_sim_order_command(command_id="cmd_real"))
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
    assert cancel_rejected[0].event_type == "command_failed"
    assert modify_rejected[0].event_type == "command_failed"


def _live_sim_order_command(command_id: str = "cmd_live_sim") -> GatewayCommand:
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
            },
        },
    )
