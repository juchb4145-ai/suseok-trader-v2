from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.entry_timing.service import evaluate_entry_timing
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate entry timing and order plan drafts.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--candidate-instance-id", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Evaluate on demand without writing order plan draft tables.",
    )
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = evaluate_entry_timing(
            connection,
            trade_date=args.trade_date,
            candidate_instance_id=args.candidate_instance_id,
            limit=args.limit,
            write_order_plan_drafts=False if args.no_write else None,
            settings=settings,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
