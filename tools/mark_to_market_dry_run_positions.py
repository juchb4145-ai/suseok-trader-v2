from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.config import load_settings
    from services.oms.dry_run_service import update_dry_run_positions_mark_to_market
    from storage.sqlite import initialize_database

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        payload = update_dry_run_positions_mark_to_market(connection, settings=settings)
    finally:
        connection.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
