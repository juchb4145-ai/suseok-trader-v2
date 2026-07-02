from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.operator.no_buy_sentinel import rebuild_no_buy_sentinel_snapshot
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild a read-only No-Buy Sentinel snapshot.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--include-ai", action="store_true")
    parser.add_argument("--include-debug", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        snapshot = rebuild_no_buy_sentinel_snapshot(
            connection,
            settings=settings,
            trade_date=args.trade_date,
            limit=args.limit,
            include_ai=True if args.include_ai else None,
            include_debug=args.include_debug,
        ).to_dict()
    finally:
        connection.close()

    if args.json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    print(f"Saved No-Buy Sentinel snapshot: {snapshot['snapshot_id']}")
    print(f"status: {snapshot['status']}")
    funnel_line = _funnel_line(snapshot)
    if funnel_line:
        print(f"funnel: {funnel_line}")
    print(f"read_only: {snapshot['read_only']}")
    print(f"no_order_side_effects: {snapshot['no_order_side_effects']}")
    return 0


def _funnel_line(snapshot: dict) -> str:
    stages = (snapshot.get("stage_funnel") or {}).get("stages") or []
    if not stages:
        return ""
    return " -> ".join(
        f"{stage.get('stage')}={stage.get('survived_count', 0)}"
        for stage in stages
    )


if __name__ == "__main__":
    raise SystemExit(main())
