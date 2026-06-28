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
        get_latest_live_sim_operating_run,
        list_live_sim_operating_runs,
    )
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Inspect LIVE_SIM operating cycle runs.")
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.latest:
            payload = {"run": get_latest_live_sim_operating_run(connection)}
        else:
            payload = {"runs": list_live_sim_operating_runs(connection, limit=args.limit)}
        payload.update(
            {
                "live_sim_only": True,
                "live_real_allowed": False,
                "read_only": True,
                "no_order_side_effects": True,
            }
        )
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
