from __future__ import annotations

import argparse

from services.config import load_settings
from storage.event_retention import prune_event_store_events
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prune old high-volume Gateway event-store rows."
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=None,
        help="Keep Gateway events newer than this many days.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum candidate events to delete in this run.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Delete rows. Without this flag the command runs as a dry-run.",
    )
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = prune_event_store_events(
            connection,
            settings=settings,
            retention_days=args.retention_days,
            dry_run=not args.execute,
            limit=args.limit,
        )
    finally:
        connection.close()

    for key, value in result.to_dict().items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
