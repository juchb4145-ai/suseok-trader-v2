from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from gateway.mock_runtime import MockGatewayRuntime
from gateway.settings import GatewaySettings, load_gateway_settings


def main() -> int:
    args = _parse_args()
    settings = _settings_from_args(args)

    runtime = MockGatewayRuntime(
        settings=settings,
        code=args.code,
        name=args.name,
        on_activity=_print_activity,
    )
    try:
        if settings.mock_once:
            snapshot = runtime.start_once()
        else:
            snapshot = runtime.run_forever()
    except KeyboardInterrupt:
        runtime.stop()
        snapshot = runtime.snapshot()
        print("Mock Gateway stopped by operator.")

    print(f"snapshot={json.dumps(snapshot, ensure_ascii=False, sort_keys=True)}")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the PR 3 mock Gateway transport harness.")
    parser.add_argument("--core-url", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--source", default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=None)
    parser.add_argument("--heartbeat-interval-sec", type=float, default=None)
    parser.add_argument("--price-tick-interval-sec", type=float, default=None)
    parser.add_argument("--command-wait-sec", type=float, default=None)
    parser.add_argument("--command-limit", type=int, default=None)
    parser.add_argument("--code", default="005930")
    parser.add_argument("--name", default="삼성전자")
    return parser.parse_args()


def _settings_from_args(args: argparse.Namespace) -> GatewaySettings:
    settings = load_gateway_settings()
    overrides: dict[str, Any] = {}
    if args.core_url is not None:
        overrides["core_url"] = args.core_url
    if args.token is not None:
        overrides["core_token"] = args.token
    if args.source is not None:
        overrides["source"] = args.source
    if args.once:
        overrides["mock_once"] = True
    if args.interval_sec is not None:
        overrides["poll_interval_sec"] = args.interval_sec
    if args.heartbeat_interval_sec is not None:
        overrides["heartbeat_interval_sec"] = args.heartbeat_interval_sec
    if args.price_tick_interval_sec is not None:
        overrides["mock_price_tick_interval_sec"] = args.price_tick_interval_sec
    if args.command_wait_sec is not None:
        overrides["command_wait_sec"] = args.command_wait_sec
    if args.command_limit is not None:
        overrides["command_limit"] = args.command_limit
    return replace(settings, **overrides)


def _print_activity(event_name: str, payload: Mapping[str, Any]) -> None:
    print(f"{event_name}={json.dumps(dict(payload), ensure_ascii=False, sort_keys=True)}")


if __name__ == "__main__":
    raise SystemExit(main())

