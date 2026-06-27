from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.ai_sidecar.rca_workflows import build_no_trade_rca_report
    from services.config import load_settings
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Build a deterministic no-trade RCA report.")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--run-ai", action="store_true")
    parser.add_argument("--persist", default="true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    del args.limit

    settings = load_settings()
    trade_date = args.trade_date or _today(settings)
    connection = initialize_database(settings.trading_db_path)
    try:
        result = build_no_trade_rca_report(
            connection,
            trade_date,
            run_ai=args.run_ai,
            settings=settings,
        )
    finally:
        connection.close()

    payload = result.to_dict()
    if result.report is not None:
        payload = {
            "ok": result.ok,
            "report_id": result.report.report_id,
            "status": result.report.status.value,
            "summary": result.report.summary,
            "root_cause_category": result.report.root_cause_category.value,
            "ai_request_id": result.report.ai_request_id,
            "ai_insight_id": result.report.ai_insight_id,
            "no_trading_side_effects": result.report.no_trading_side_effects,
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.ok else 1


def _today(settings) -> str:
    from datetime import datetime

    from services.config import candidate_timezone

    timezone = candidate_timezone(settings.candidate_trade_date_timezone)
    return datetime.now(timezone).date().isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
