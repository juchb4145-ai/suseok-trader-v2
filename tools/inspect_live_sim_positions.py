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
    from services.live_sim.live_sim_service import list_live_sim_positions
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Inspect local LIVE_SIM positions.")
    parser.add_argument("--position-id")
    parser.add_argument("--code")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        rows = list_live_sim_positions(
            connection,
            code=args.code,
            limit=args.limit,
        )
        if args.position_id:
            rows = [row for row in rows if row["position_id"] == args.position_id]
    finally:
        connection.close()
    print(
        json.dumps(
            {"positions": rows, "live_sim_only": True, "live_real_allowed": False},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
