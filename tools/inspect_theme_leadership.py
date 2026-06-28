from __future__ import annotations

import argparse
import json

from services.config import load_settings
from services.theme_leadership import rebuild_theme_leadership
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect observe-only realtime theme leadership and watchset."
    )
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--write-candidate-sources", action="store_true")
    parser.add_argument("--no-members", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = rebuild_theme_leadership(
            connection,
            trade_date=args.trade_date,
            write_candidate_sources=args.write_candidate_sources,
            settings=settings,
        )
    finally:
        connection.close()

    print(
        json.dumps(
            result.to_dict(include_members=not args.no_members), ensure_ascii=False, indent=2
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
