from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.strategy_engine import (
    get_latest_strategy_observation,
    list_strategy_setup_observations,
)
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect observe-only strategy observation projection rows."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--candidate-instance-id")
    group.add_argument("--strategy-observation-id")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.candidate_instance_id:
            payload = get_latest_strategy_observation(
                connection,
                args.candidate_instance_id,
                include_setups=True,
            )
        else:
            payload = {
                "strategy_observation_id": args.strategy_observation_id,
                "setup_observations": list_strategy_setup_observations(
                    connection,
                    args.strategy_observation_id,
                ),
            }
    finally:
        connection.close()

    if payload is None:
        print("strategy observation not found")
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
