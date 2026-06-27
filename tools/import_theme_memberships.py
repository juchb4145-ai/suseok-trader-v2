from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.config import load_settings
from services.theme_service import import_theme_memberships
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import read-only theme membership seed/import data."
    )
    parser.add_argument(
        "--file",
        default="data/themes/sample_themes.json",
        help="Theme membership JSON payload file.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Safely replace members only within the same theme/source scope.",
    )
    parser.add_argument("--source-type", default=None)
    parser.add_argument("--source-name", default=None)
    args = parser.parse_args()

    payload_path = Path(args.file)
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        result = import_theme_memberships(
            connection,
            payload,
            source_type=args.source_type,
            source_name=args.source_name,
            replace=args.replace,
        )
    finally:
        connection.close()

    for key, value in result.to_dict().items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
