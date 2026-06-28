from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.operator.operator_status import build_operator_status
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect read-only operator status.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--include-ai", action="store_true")
    parser.add_argument("--include-debug", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        payload = build_operator_status(
            connection,
            settings=settings,
            trade_date=args.trade_date,
            include_no_buy_rebuild=True,
        )
    finally:
        connection.close()

    if args.json or args.include_debug:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    no_buy = payload.get("no_buy_sentinel") or {}
    live_sim = payload.get("live_sim", {}).get("status", {})
    ai = payload.get("ai_advisory", {})
    print(f"Operator status: {payload['core']['trading_mode']}")
    print(f"LIVE_SIM: enabled={live_sim.get('enabled')} kill={live_sim.get('kill_switch')}")
    print(f"AI advisory: provider={ai.get('provider')} latest={ai.get('latest_run')}")
    print(f"No-Buy: {no_buy.get('status')} read_only={payload['read_only']}")
    print(f"limit={args.limit} include_ai={args.include_ai}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
