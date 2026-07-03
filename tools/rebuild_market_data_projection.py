from __future__ import annotations

import argparse

from services.config import load_settings
from services.market_data_service import rebuild_market_data_projection
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild read-only market data projection tables, including exchange/session "
            "metadata from accepted Gateway events."
        )
    )
    parser.add_argument(
        "--clear-projection",
        action="store_true",
        help="Clear market projection tables before replaying accepted Gateway events.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Replay only accepted market events after the projection watermark.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum accepted market events to replay.",
    )
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = rebuild_market_data_projection(
            connection,
            clear_projection=args.clear_projection,
            require_clear=args.clear_projection,
            incremental=args.incremental,
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
