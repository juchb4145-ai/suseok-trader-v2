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
    from services.ai_sidecar.live_sim_review_workflows import build_live_sim_session_review
    from services.config import candidate_timezone, load_settings
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Build a read-only LIVE_SIM session review.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--run-ai", action="store_true", default=False)
    args = parser.parse_args()

    settings = load_settings()
    trade_date = args.trade_date or datetime.now(
        candidate_timezone(settings.candidate_trade_date_timezone)
    ).date().isoformat()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = build_live_sim_session_review(
            connection,
            trade_date,
            run_ai=args.run_ai,
            settings=settings,
        )
        report = result.report.to_dict() if result.report else None
        payload = {
            "ok": result.ok,
            "review_id": None if report is None else report["review_id"],
            "report": report,
            "error_message": result.error_message,
            "warnings": list(result.warnings),
            "review_only": True,
            "order_action_allowed": False,
            "gateway_command_allowed": False,
            "live_real_allowed": False,
        }
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
