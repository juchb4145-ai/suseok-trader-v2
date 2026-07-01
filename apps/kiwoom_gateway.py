from __future__ import annotations

import argparse
import dataclasses as _dataclasses
import datetime as _datetime
import enum as _enum
import json
import os
import struct
import sys
import threading
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def configure_python_compat() -> None:
    if not hasattr(_datetime, "UTC"):
        _datetime.UTC = _datetime.timezone.utc  # noqa: UP017
    if not hasattr(_enum, "StrEnum"):

        class StrEnum(str, _enum.Enum):  # noqa: UP042
            pass

        _enum.StrEnum = StrEnum
    if sys.version_info < (3, 10) and not getattr(
        _dataclasses.dataclass,
        "_suseok_kw_only_compat",
        False,
    ):
        original_dataclass = _dataclasses.dataclass

        def dataclass_compat(cls=None, /, **kwargs):  # type: ignore[no-untyped-def]
            kwargs.pop("kw_only", None)
            kwargs.pop("slots", None)
            if cls is None:
                return lambda candidate: original_dataclass(candidate, **kwargs)
            return original_dataclass(cls, **kwargs)

        dataclass_compat._suseok_kw_only_compat = True  # type: ignore[attr-defined]
        _dataclasses.dataclass = dataclass_compat


configure_python_compat()


def configure_qt_paths() -> None:
    try:
        import PyQt5
    except ImportError:
        return
    pyqt_dir = Path(PyQt5.__file__).resolve().parent
    qt_root = pyqt_dir / "Qt5"
    platforms_dir = qt_root / "plugins" / "platforms"
    qt_bin = qt_root / "bin"
    if platforms_dir.exists():
        os.environ.setdefault("QT_QPA_PLATFORM_PLUGIN_PATH", str(platforms_dir))
    if qt_bin.exists():
        os.environ["PATH"] = f"{qt_bin}{os.pathsep}{os.environ.get('PATH', '')}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="32-bit Kiwoom OpenAPI+ Gateway Adapter")
    parser.add_argument("--core-url", default="http://127.0.0.1:8000")
    parser.add_argument(
        "--token",
        default=os.environ.get("GATEWAY_CORE_TOKEN") or os.environ.get("TRADING_CORE_TOKEN", ""),
    )
    parser.add_argument("--poll-wait-sec", type=float, default=1.0)
    parser.add_argument("--heartbeat-interval-sec", type=float, default=2.0)
    parser.add_argument("--command-limit", type=int, default=20)
    parser.add_argument("--condition-name", default=os.environ.get("KIWOOM_CONDITION_NAME"))
    parser.add_argument(
        "--condition-profiles",
        default=os.environ.get("KIWOOM_CONDITION_PROFILES", ""),
        help=(
            "JSON array/object of Kiwoom condition profiles. "
            "When omitted, --condition-name/--condition-index are used as one legacy profile."
        ),
    )
    parser.add_argument(
        "--condition-index",
        type=int,
        default=_optional_int_env("KIWOOM_CONDITION_INDEX"),
    )
    parser.add_argument("--condition-realtime", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--realtime-codes", default=os.environ.get("KIWOOM_REALTIME_CODES", ""))
    parser.add_argument(
        "--realtime-exchange",
        choices=("krx", "nxt", "all"),
        default=os.environ.get("KIWOOM_REALTIME_EXCHANGE", "krx").strip().lower(),
        help=(
            "Exchange code suffix for SetRealReg realtime code list: "
            "krx=005930, nxt=005930_NX, all=005930_AL."
        ),
    )
    parser.add_argument(
        "--realtime-recover-stale-sec",
        type=float,
        default=float(os.environ.get("KIWOOM_REALTIME_RECOVER_STALE_SEC", "45")),
    )
    parser.add_argument(
        "--realtime-recover-interval-sec",
        type=float,
        default=float(os.environ.get("KIWOOM_REALTIME_RECOVER_INTERVAL_SEC", "300")),
    )
    parser.add_argument(
        "--market-index-enabled",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("KIWOOM_MARKET_INDEX_ENABLED", False),
    )
    parser.add_argument(
        "--market-index-realtime-enabled",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("KIWOOM_MARKET_INDEX_REALTIME_ENABLED", False),
    )
    parser.add_argument(
        "--market-index-tr-bootstrap-enabled",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED", False),
    )
    parser.add_argument(
        "--market-index-codes",
        default=os.environ.get("KIWOOM_MARKET_INDEX_CODES", "KOSPI,KOSDAQ"),
    )
    parser.add_argument(
        "--market-index-screen-no",
        default=os.environ.get("KIWOOM_MARKET_INDEX_SCREEN_NO", "5700"),
    )
    parser.add_argument(
        "--market-index-poll-sec",
        type=float,
        default=float(os.environ.get("KIWOOM_MARKET_INDEX_POLL_SEC", "60.0")),
    )
    parser.add_argument("--observe-only", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--auto-login", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--threaded-login",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Deprecated for Kiwoom ActiveX stability. Default is --no-threaded-login "
            "so CommConnect runs on the Qt main thread."
        ),
    )
    parser.add_argument(
        "--disable-core-io",
        action="store_true",
        help="Diagnostic isolation mode: do not poll commands or post events to Core.",
    )
    parser.add_argument(
        "--disable-command-polling",
        action="store_true",
        help="Do not poll Core commands; Kiwoom ActiveX callbacks still run.",
    )
    parser.add_argument(
        "--disable-event-posting",
        action="store_true",
        help="Do not post Gateway events to Core; events are drained locally.",
    )
    parser.add_argument(
        "--isolation-report-interval-sec",
        type=float,
        default=5.0,
        help="Console status interval for --disable-core-io callback diagnostics.",
    )
    parser.add_argument(
        "--mock-client",
        action="store_true",
        help="Use an in-process mock Kiwoom client for adapter smoke tests.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run_gateway(args)
    except RuntimeError as exc:
        print(f"[kiwoom_gateway] {exc}", file=sys.stderr, flush=True)
        return 2


def run_gateway(args: argparse.Namespace) -> int:
    from domain.broker.condition_profiles import parse_condition_profiles
    from gateway.core_client import CoreClient
    from gateway.kiwoom_runtime import (
        KiwoomGatewayRuntime,
        KiwoomGatewayRuntimeConfig,
        wire_kiwoom_signals,
    )

    if not bool(getattr(args, "mock_client", False)) and _python_bitness() != 32:
        raise RuntimeError(
            "32-bit Python + PyQt5 32-bit + Kiwoom OpenAPI+ are required. "
            f"Current Python is {_python_bitness()}-bit."
        )

    configure_qt_paths()
    try:
        from PyQt5.QtCore import QTimer
        from PyQt5.QtWidgets import QApplication
    except ImportError as exc:
        raise RuntimeError(
            "PyQt5 is required to run apps.kiwoom_gateway. Install 32-bit PyQt5 in the "
            "same 32-bit Python environment as Kiwoom OpenAPI+."
        ) from exc

    app = QApplication.instance() or QApplication(sys.argv[:1])
    core_io_enabled = not bool(getattr(args, "disable_core_io", False))
    command_polling_enabled = core_io_enabled and not bool(
        getattr(args, "disable_command_polling", False)
    )
    event_posting_enabled = core_io_enabled and not bool(
        getattr(args, "disable_event_posting", False)
    )
    core_io_worker_enabled = core_io_enabled and (
        command_polling_enabled or event_posting_enabled
    )

    if bool(getattr(args, "mock_client", False)):
        from gateway.kiwoom_client import MockKiwoomClient, active_x_thread_audit

        client = MockKiwoomClient()
    else:
        from gateway.kiwoom_client import KiwoomClient, active_x_thread_audit

        client = KiwoomClient()

    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=CoreClient(core_url=args.core_url, token=args.token),
        config=KiwoomGatewayRuntimeConfig(
            command_limit=max(int(args.command_limit), 1),
            command_wait_sec=max(float(args.poll_wait_sec), 0.0),
            condition_name=args.condition_name,
            condition_index=args.condition_index,
            condition_realtime=bool(args.condition_realtime),
            condition_profiles=parse_condition_profiles(args.condition_profiles),
            realtime_codes=tuple(_parse_codes(args.realtime_codes)),
            realtime_exchange=str(args.realtime_exchange or "krx").upper(),
            realtime_recover_stale_sec=max(float(args.realtime_recover_stale_sec), 0.0),
            realtime_recover_interval_sec=max(float(args.realtime_recover_interval_sec), 1.0),
            market_index_enabled=bool(args.market_index_enabled),
            market_index_realtime_enabled=bool(args.market_index_realtime_enabled),
            market_index_tr_bootstrap_enabled=bool(args.market_index_tr_bootstrap_enabled),
            market_index_codes=tuple(_parse_codes(args.market_index_codes)),
            market_index_screen_no=str(args.market_index_screen_no or "5700"),
            market_index_poll_sec=max(float(args.market_index_poll_sec), 1.0),
            observe_only=bool(args.observe_only),
            core_io_enabled=core_io_enabled,
            command_polling_enabled=command_polling_enabled,
            event_posting_enabled=event_posting_enabled,
            core_io_worker_enabled=core_io_worker_enabled,
        ),
    )
    wire_kiwoom_signals(client, runtime)
    runtime.on_active_x_thread_audit(
        active_x_thread_audit("QApplication.event_loop", phase="READY")
    )
    runtime.start_core_io_worker()
    if not core_io_enabled:
        runtime.emit(
            "gateway_log",
            {
                "message": "CORE_IO_DISABLED_ISOLATION_MODE",
                "command_polling_enabled": False,
                "event_posting_enabled": False,
            },
        )

    heartbeat_timer = QTimer()
    heartbeat_timer.timeout.connect(lambda: _heartbeat_tick(runtime))
    heartbeat_timer.start(int(max(float(args.heartbeat_interval_sec), 1.0) * 1000))

    command_timer = QTimer()
    command_timer.timeout.connect(runtime.drain_core_io_worker)
    command_timer.start(100)

    flush_timer = QTimer()
    flush_timer.timeout.connect(runtime.flush_events)
    flush_timer.start(200)

    condition_watchdog_timer = QTimer()
    condition_watchdog_timer.timeout.connect(runtime.check_condition_load_timeout)
    condition_watchdog_timer.start(1000)

    isolation_report_timer = QTimer()
    if not core_io_enabled:
        interval_ms = int(max(float(args.isolation_report_interval_sec), 1.0) * 1000)
        max_reports = max(int(60000 / interval_ms), 1)
        report_state = {"count": 0}

        def report_isolation_status() -> None:
            _print_isolation_status(runtime)
            report_state["count"] += 1
            if report_state["count"] >= max_reports:
                isolation_report_timer.stop()

        isolation_report_timer.timeout.connect(report_isolation_status)
        isolation_report_timer.start(interval_ms)
        QTimer.singleShot(1000, report_isolation_status)

    QTimer.singleShot(100, lambda: _heartbeat_tick(runtime))
    if bool(args.auto_login):
        QTimer.singleShot(
            1500,
            lambda: request_kiwoom_login(
                client,
                runtime,
                threaded=bool(args.threaded_login),
            ),
        )
    else:
        runtime.emit("gateway_error", {"message": "KIWOOM_AUTO_LOGIN_DISABLED"})

    try:
        runtime.on_active_x_thread_audit(
            active_x_thread_audit("QApplication.exec", phase="CALL")
        )
        return int(app.exec_())
    finally:
        runtime.close()


def request_kiwoom_login(
    client: object,
    runtime: object,
    *,
    threaded: bool = False,
) -> None:
    if threaded and getattr(runtime, "_login_in_progress", False):
        return
    if threaded:
        emit = getattr(runtime, "emit", None)
        if callable(emit):
            emit(
                "gateway_log",
                {
                    "message": "THREADED_LOGIN_DEPRECATED_ACTIVE_X_UNSAFE",
                    "reason_codes": ["POSSIBLE_THREADING_ISSUE"],
                },
            )

    def run_login() -> None:
        runtime.request_login_started(threaded=threaded)
        try:
            client.login()
            if not bool(getattr(client, "login_waits_for_event_loop", False)):
                finish_pending_login = getattr(runtime, "_finish_pending_login_if_connected", None)
                if callable(finish_pending_login):
                    finish_pending_login()
                elif runtime.kiwoom_logged_in() and getattr(runtime, "_login_in_progress", False):
                    runtime.on_connected(True, 0, "already connected")
        except Exception as exc:
            runtime.request_login_failed(exc)

    if threaded:
        thread = threading.Thread(target=run_login, name="kiwoom-login-request", daemon=True)
        thread.start()
        return
    run_login()


def _heartbeat_tick(runtime: object) -> None:
    runtime.emit_heartbeat()
    runtime.flush_events()


def _print_isolation_status(runtime: object) -> None:
    status_payload = getattr(runtime, "isolation_status_payload", None)
    if not callable(status_payload):
        return
    print(
        "[kiwoom_gateway][isolation] "
        + json.dumps(status_payload(), ensure_ascii=False, sort_keys=True),
        flush=True,
    )


def _parse_codes(raw: object) -> list[str]:
    if raw is None:
        return []
    return [item.strip() for item in str(raw).replace(",", ";").split(";") if item.strip()]


def _optional_int_env(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    return int(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _python_bitness() -> int:
    return struct.calcsize("P") * 8


if __name__ == "__main__":
    raise SystemExit(main())
