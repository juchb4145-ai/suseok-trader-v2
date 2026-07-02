from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.operator.no_buy_sentinel import build_no_buy_sentinel_snapshot
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect the read-only No-Buy Sentinel.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--include-ai", action="store_true")
    parser.add_argument("--include-debug", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        snapshot = build_no_buy_sentinel_snapshot(
            connection,
            settings=settings,
            trade_date=args.trade_date,
            manual=True,
            limit=args.limit,
            include_ai=True if args.include_ai else None,
            include_debug=args.include_debug,
            write_snapshot=False,
        ).to_dict()
    finally:
        connection.close()

    if args.json:
        print(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    print(f"No-Buy Sentinel: {snapshot['status']} ({snapshot['trade_date']})")
    print(
        "counts: "
        f"intent={snapshot['intent_count']} "
        f"order={snapshot['order_count']} "
        f"command={snapshot['command_count']} "
        f"PLAN_READY={snapshot['plan_ready_count']} "
        f"eligible={snapshot['buy_eligible_count']} "
        f"AI_SELECTED={snapshot['ai_selected_count']}"
    )
    funnel_line = _funnel_line(snapshot)
    if funnel_line:
        print(f"funnel: {funnel_line}")
    for item in snapshot["operator_checklist"]:
        print(f"- {item}")
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
