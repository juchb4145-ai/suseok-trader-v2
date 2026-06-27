from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.ai_sidecar.live_sim_review_workflows import (
        build_live_sim_order_reviews_for_trade_date,
    )
    from services.config import candidate_timezone, load_settings
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Build read-only LIVE_SIM order reviews.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--status", default=None)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--run-ai", action="store_true", default=False)
    args = parser.parse_args()

    settings = load_settings()
    trade_date = args.trade_date or datetime.now(
        candidate_timezone(settings.candidate_trade_date_timezone)
    ).date().isoformat()
    connection = initialize_database(settings.trading_db_path)
    try:
        results = build_live_sim_order_reviews_for_trade_date(
            connection,
            trade_date,
            status=args.status,
            limit=args.limit,
            run_ai=args.run_ai,
            settings=settings,
        )
        reports = [result.report.to_dict() for result in results if result.ok and result.report]
        errors = [result.error_message for result in results if not result.ok]
        payload = {
            "ok": not errors,
            "count": len(reports),
            "error_count": len(errors),
            "reports": reports,
            "errors": errors,
            "review_only": True,
            "order_action_allowed": False,
            "gateway_command_allowed": False,
            "live_real_allowed": False,
        }
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
