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
    from services.runtime.market_open_observe_cycle import (
        run_market_open_observe_cycle_once,
    )
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(
        description="Run one observe-only market-open pipeline cycle."
    )
    parser.add_argument("--trade-date")
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--no-write-run",
        action="store_true",
        help="Run the cycle without persisting market_open_observe_cycle_runs.",
    )
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = run_market_open_observe_cycle_once(
            connection,
            settings=settings,
            trade_date=args.trade_date,
            limit=args.limit,
            write_run=not args.no_write_run,
        )
        payload = result.to_dict()
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
