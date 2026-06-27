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
    from services.live_sim.live_sim_service import (
        evaluate_live_sim_candidates,
        evaluate_live_sim_eligibility,
    )
    from services.live_sim.safety_gate import check_live_sim_safety_gate
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Evaluate LIVE_SIM eligibility only.")
    parser.add_argument("--trade-date")
    parser.add_argument("--candidate-instance-id")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.candidate_instance_id:
            payload = evaluate_live_sim_eligibility(
                connection,
                args.candidate_instance_id,
                settings=settings,
            ).to_dict()
        else:
            payload = evaluate_live_sim_candidates(
                connection,
                trade_date=args.trade_date,
                limit=args.limit,
                settings=settings,
            ).to_dict()
        payload["safety_gate"] = check_live_sim_safety_gate(connection, settings).to_dict()
        payload["live_sim_only"] = True
        payload["live_real_allowed"] = False
        payload["real_order_allowed"] = False
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
