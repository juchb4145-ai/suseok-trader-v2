from __future__ import annotations

import argparse

from services.config import load_settings
from services.theme_service import calculate_all_theme_snapshots, calculate_theme_snapshot
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild read-only theme snapshots from market data projection tables."
    )
    parser.add_argument("--theme-id", default=None)
    parser.add_argument("--calculated-at", default=None)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        if args.theme_id:
            snapshot = calculate_theme_snapshot(
                connection,
                args.theme_id,
                calculated_at=args.calculated_at,
                settings=settings,
            )
            result = {
                "processed_theme_count": 1,
                "snapshot_count": 1,
                "error_count": int(snapshot.metadata.get("member_error_count", 0)),
                "snapshot_id": snapshot.snapshot_id,
                "state": snapshot.state.value,
                "quality_status": snapshot.quality_status.value,
            }
        else:
            result = calculate_all_theme_snapshots(
                connection,
                calculated_at=args.calculated_at,
                settings=settings,
            ).to_dict()
    finally:
        connection.close()

    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
