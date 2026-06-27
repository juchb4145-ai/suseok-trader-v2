from __future__ import annotations

import argparse

from services.candidate_service import rebuild_candidates_from_observations
from services.config import load_settings
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild observe-only Candidate FSM projection from observations."
    )
    parser.add_argument("--trade-date", default=None)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = rebuild_candidates_from_observations(
            connection,
            trade_date=args.trade_date,
            settings=settings,
        )
    finally:
        connection.close()

    for key, value in result.to_dict().items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
