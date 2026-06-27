from __future__ import annotations

import argparse

from domain.candidate.state import CandidateState
from services.config import load_settings
from services.strategy_engine import evaluate_candidates
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate observe-only strategy observations for candidate episodes."
    )
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--candidate-instance-id", default=None)
    parser.add_argument("--state", default=CandidateState.CONTEXT_READY.value)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = evaluate_candidates(
            connection,
            trade_date=args.trade_date,
            candidate_state=None if args.candidate_instance_id else args.state,
            limit=args.limit,
            settings=settings,
            candidate_instance_id=args.candidate_instance_id,
        )
    finally:
        connection.close()

    for key, value in result.to_dict().items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
