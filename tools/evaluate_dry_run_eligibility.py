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
    from services.oms.dry_run_service import (
        evaluate_dry_run_candidates,
        evaluate_dry_run_eligibility,
    )
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Evaluate DRY_RUN OMS eligibility.")
    parser.add_argument("--trade-date")
    parser.add_argument("--candidate-instance-id")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.candidate_instance_id:
            result = evaluate_dry_run_eligibility(
                connection,
                args.candidate_instance_id,
                settings=settings,
            ).to_dict()
        else:
            result = evaluate_dry_run_candidates(
                connection,
                trade_date=args.trade_date,
                limit=args.limit,
                settings=settings,
            ).to_dict()
    finally:
        connection.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
