from __future__ import annotations

import argparse
import os
import struct
import sys
import threading
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
    parser.add_argument("--token", default=os.environ.get("GATEWAY_CORE_TOKEN", ""))
    parser.add_argument("--poll-wait-sec", type=float, default=1.0)
    parser.add_argument("--heartbeat-interval-sec", type=float, default=2.0)
    parser.add_argument("--command-limit", type=int, default=20)
    parser.add_argument("--condition-name", default=os.environ.get("KIWOOM_CONDITION_NAME"))
    parser.add_argument(
        "--condition-index",
        type=int,
        default=_optional_int_env("KIWOOM_CONDITION_INDEX"),
    )
    parser.add_argument("--condition-realtime", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--realtime-codes", default=os.environ.get("KIWOOM_REALTIME_CODES", ""))
    parser.add_argument("--observe-only", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--auto-login", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--threaded-login", action=argparse.BooleanOptionalAction, default=True)
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

    if bool(getattr(args, "mock_client", False)):
        from gateway.kiwoom_client import MockKiwoomClient

        client = MockKiwoomClient()
    else:
        from gateway.kiwoom_client import KiwoomClient

        client = KiwoomClient()

    app = QApplication.instance() or QApplication(sys.argv[:1])
    runtime = KiwoomGatewayRuntime(
        client=client,
        core_client=CoreClient(core_url=args.core_url, token=args.token),
        config=KiwoomGatewayRuntimeConfig(
            command_limit=max(int(args.command_limit), 1),
            command_wait_sec=max(float(args.poll_wait_sec), 0.0),
            condition_name=args.condition_name,
            condition_index=args.condition_index,
            condition_realtime=bool(args.condition_realtime),
            realtime_codes=tuple(_parse_codes(args.realtime_codes)),
            observe_only=bool(args.observe_only),
        ),
    )
    wire_kiwoom_signals(client, runtime)

    heartbeat_timer = QTimer()
    heartbeat_timer.timeout.connect(lambda: _heartbeat_tick(runtime))
    heartbeat_timer.start(int(max(float(args.heartbeat_interval_sec), 1.0) * 1000))

    command_timer = QTimer()
    command_timer.timeout.connect(runtime.poll_and_handle_commands)
    command_timer.start(max(int(float(args.poll_wait_sec) * 1000), 200))

    flush_timer = QTimer()
    flush_timer.timeout.connect(runtime.flush_events)
    flush_timer.start(200)

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
        return int(app.exec_())
    finally:
        runtime.close()


def request_kiwoom_login(
    client: object,
    runtime: object,
    *,
    threaded: bool = True,
) -> None:
    if threaded and getattr(runtime, "_login_in_progress", False):
        return

    def run_login() -> None:
        runtime.request_login_started(threaded=threaded)
        try:
            client.login()
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


def _parse_codes(raw: object) -> list[str]:
    if raw is None:
        return []
    return [item.strip() for item in str(raw).replace(",", ";").split(";") if item.strip()]


def _optional_int_env(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    return int(raw)


def _python_bitness() -> int:
    return struct.calcsize("P") * 8


if __name__ == "__main__":
    raise SystemExit(main())
