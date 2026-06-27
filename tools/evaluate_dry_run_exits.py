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
    from services.exit_engine import evaluate_all_dry_run_exits, evaluate_dry_run_exit_for_position
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Evaluate DRY_RUN exit observations.")
    parser.add_argument("--trade-date")
    parser.add_argument("--dry-run-position-id")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.dry_run_position_id:
            payload = evaluate_dry_run_exit_for_position(
                connection,
                args.dry_run_position_id,
                settings=settings,
            ).to_dict()
        else:
            payload = evaluate_all_dry_run_exits(
                connection,
                trade_date=args.trade_date,
                limit=args.limit,
                settings=settings,
            ).to_dict()
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
