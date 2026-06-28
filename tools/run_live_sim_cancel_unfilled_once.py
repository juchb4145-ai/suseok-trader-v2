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
    from services.live_sim.live_sim_service import run_live_sim_cancel_unfilled_once
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(
        description="Run LIVE_SIM unfilled BUY cancel evaluation once."
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--queue-commands", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = run_live_sim_cancel_unfilled_once(
            connection,
            settings=settings,
            dry_run=args.dry_run or not args.queue_commands,
            queue_commands=args.queue_commands,
            limit=args.limit,
        )
    finally:
        connection.close()
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
