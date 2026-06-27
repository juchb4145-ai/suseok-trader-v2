from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.risk_gate import get_latest_risk_observation, get_risk_observation
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect observe-only risk observations.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--candidate-instance-id", default=None)
    target.add_argument("--risk-observation-id", default=None)
    parser.add_argument("--without-checks", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.candidate_instance_id:
            observation = get_latest_risk_observation(
                connection,
                args.candidate_instance_id,
                include_checks=not args.without_checks,
            )
        else:
            observation = get_risk_observation(
                connection,
                args.risk_observation_id,
                include_checks=not args.without_checks,
            )
    finally:
        connection.close()

    if observation is None:
        print("risk observation not found")
        return 1
    print(json.dumps(observation, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
