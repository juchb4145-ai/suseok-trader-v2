from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.config import load_settings
    from services.runtime.live_sim_operating_orchestrator import (
        OperatingMode,
        run_live_sim_operating_cycle_once,
    )
    from storage.sqlite import initialize_database

    settings = load_settings()
    parser = argparse.ArgumentParser(description="Run one safe LIVE_SIM operating cycle.")
    parser.add_argument(
        "--mode",
        choices=[mode.value for mode in OperatingMode],
        default=settings.live_sim_operating_default_mode,
    )
    parser.add_argument("--trade-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--queue-commands",
        action="store_true",
        help=(
            "Allow command queueing only after mode, preflight, safety, settings, "
            "and budget pass."
        ),
    )
    parser.add_argument("--skip-ai", action="store_true")
    parser.add_argument("--skip-no-buy", action="store_true")
    args = parser.parse_args()

    connection = initialize_database(settings.trading_db_path)
    try:
        result = run_live_sim_operating_cycle_once(
            connection,
            mode=args.mode,
            queue_commands=args.queue_commands,
            trade_date=args.trade_date,
            limit=args.limit,
            include_ai=not args.skip_ai,
            include_no_buy=not args.skip_no_buy,
            settings=settings,
        )
        payload = result.to_dict()
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
