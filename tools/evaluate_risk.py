from __future__ import annotations

import argparse

from domain.strategy.status import StrategyObservationStatus
from services.config import load_settings
from services.risk_gate import evaluate_risk_observations
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate observe-only risk observations from strategy observations."
    )
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--candidate-instance-id", default=None)
    parser.add_argument("--strategy-observation-id", default=None)
    parser.add_argument(
        "--strategy-status",
        default=StrategyObservationStatus.MATCHED_OBSERVATION.value,
    )
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = evaluate_risk_observations(
            connection,
            trade_date=args.trade_date,
            candidate_instance_id=args.candidate_instance_id,
            strategy_observation_id=args.strategy_observation_id,
            strategy_status=(
                None
                if args.candidate_instance_id or args.strategy_observation_id
                else args.strategy_status
            ),
            limit=args.limit,
            settings=settings,
        )
    finally:
        connection.close()

    for key, value in result.to_dict().items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
