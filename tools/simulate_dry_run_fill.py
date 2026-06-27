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
    from services.oms.dry_run_service import simulate_fill_dry_run_order
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Simulate an internal DRY_RUN fill.")
    parser.add_argument("--dry-run-order-id", required=True)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        execution = simulate_fill_dry_run_order(
            connection,
            args.dry_run_order_id,
            settings=settings,
        )
        payload = execution.to_dict()
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
