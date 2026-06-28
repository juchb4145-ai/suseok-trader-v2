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
    from services.live_sim.live_sim_service import reconcile_live_sim
    from services.live_sim.safety_gate import check_live_sim_safety_gate
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(
        description="Create a local-only LIVE_SIM reconcile snapshot."
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        snapshot = None if args.dry_run else reconcile_live_sim(connection, settings=settings)
        payload = {
            "reconcile": None if snapshot is None else snapshot.to_dict(),
            "dry_run": bool(args.dry_run),
            "safety_gate": check_live_sim_safety_gate(connection, settings).to_dict(),
            "live_sim_only": True,
            "live_real_allowed": False,
            "real_order_allowed": False,
        }
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
