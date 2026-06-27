from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.ai_sidecar.rca_workflows import build_candidate_block_rca_reports_for_trade_date
    from services.config import load_settings
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(
        description="Build deterministic candidate block RCA reports in batch."
    )
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--states", default=None)
    parser.add_argument("--risk-status", default=None)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--run-ai", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        results = build_candidate_block_rca_reports_for_trade_date(
            connection,
            args.trade_date,
            states=_csv(args.states),
            risk_statuses=_csv(args.risk_status),
            limit=args.limit,
            run_ai=args.run_ai,
            settings=settings,
        )
    finally:
        connection.close()

    reports = [result.report for result in results if result.ok and result.report is not None]
    errors = [result.error_message for result in results if not result.ok]
    payload = {
        "ok": len(errors) == 0,
        "count": len(reports),
        "error_count": len(errors),
        "reports": [
            {
                "report_id": report.report_id,
                "status": report.status.value,
                "summary": report.summary,
                "root_cause_category": report.root_cause_category.value,
                "ai_request_id": report.ai_request_id,
                "ai_insight_id": report.ai_insight_id,
            }
            for report in reports
        ],
        "errors": errors,
        "no_trading_side_effects": True,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if len(errors) == 0 else 1


def _csv(value: str | None) -> list[str] | None:
    if value is None or not value.strip():
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
