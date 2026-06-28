from __future__ import annotations

import argparse
import json
from typing import Any

from domain.broker.utils import normalize_value
from services.ai_advisory.storage import list_errors
from services.config import load_settings
from storage.sqlite import initialize_database


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect advisory-only AI candidate errors.")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        payload = {
            "errors": list_errors(connection, limit=args.limit),
            "advisory_only": True,
            "no_order_side_effects": True,
        }
    finally:
        connection.close()
    print(_json(payload))


def _json(value: Any) -> str:
    return json.dumps(normalize_value(value), ensure_ascii=False, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
