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
    from services.live_sim.live_sim_service import queue_live_sim_order_command
    from services.live_sim.safety_gate import check_live_sim_safety_gate
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(description="Queue a safety-gated LIVE_SIM order command.")
    parser.add_argument("--live-sim-intent-id", required=True)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        safety_gate = check_live_sim_safety_gate(connection, settings).to_dict()
        order = queue_live_sim_order_command(
            connection,
            args.live_sim_intent_id,
            settings=settings,
        )
        payload = {
            "order": order.to_dict(),
            "gateway_command_id": order.gateway_command_id,
            "idempotency_key": order.idempotency_key,
            "safety_gate": safety_gate,
            "live_sim_only": True,
            "live_real_allowed": False,
            "real_order_allowed": False,
        }
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
