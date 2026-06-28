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
    from services.live_sim.order_plan_eligibility import (
        evaluate_live_sim_order_plan_eligibility,
    )
    from services.live_sim.safety_gate import check_live_sim_safety_gate
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Evaluate LIVE_SIM OrderPlan eligibility.")
    parser.add_argument("--order-plan-id", required=True)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        eligibility = evaluate_live_sim_order_plan_eligibility(
            connection,
            args.order_plan_id,
            settings=settings,
        )
        payload = {
            "eligibility": eligibility.to_dict(),
            "safety_gate": check_live_sim_safety_gate(connection, settings).to_dict(),
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
        }
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
