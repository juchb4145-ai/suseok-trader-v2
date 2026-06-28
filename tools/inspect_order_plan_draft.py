from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.entry_timing.service import get_order_plan_draft, list_latest_order_plan_drafts
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect entry timing order plan drafts.")
    parser.add_argument("--order-plan-id", default=None)
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--code", default=None)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.order_plan_id:
            payload = {"order_plan_draft": get_order_plan_draft(connection, args.order_plan_id)}
        else:
            payload = {
                "order_plan_drafts": list_latest_order_plan_drafts(
                    connection,
                    trade_date=args.trade_date,
                    code=args.code,
                    limit=args.limit,
                )
            }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
