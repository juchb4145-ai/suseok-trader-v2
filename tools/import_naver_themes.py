from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.config import load_settings
from services.theme_importers import NaverThemeImporter
from storage.sqlite import initialize_database


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Naver theme reference memberships and import them into Theme Service."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and normalize without modifying the database.",
    )
    parser.add_argument(
        "--limit-themes",
        type=int,
        default=None,
        help="Maximum themes to fetch, capped by NAVER_THEME_IMPORT_MAX_THEMES.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Deactivate missing members only in NAVER_REFERENCE/naver_theme scope.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write normalized payload and summary JSON to this path.",
    )
    args = parser.parse_args()

    settings = load_settings()
    importer = NaverThemeImporter(settings=settings)

    connection = None
    try:
        if not args.dry_run:
            connection = initialize_database(settings.trading_db_path)
        result = importer.run(
            connection=connection,
            dry_run=args.dry_run,
            limit_themes=args.limit_themes,
            replace=args.replace or None,
        )
    finally:
        if connection is not None:
            connection.close()

    _print_summary(result.to_dict(include_payload=False))
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result.to_dict(include_payload=True), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"output: {output_path}")
    return 0 if result.status != "ABORTED_EMPTY_FETCH" else 2


def _print_summary(result: dict[str, object]) -> None:
    for key in (
        "status",
        "dry_run",
        "replace",
        "fetched_theme_count",
        "fetched_member_count",
        "normalized_theme_count",
        "normalized_member_count",
        "duplicate_count",
        "parser_error_count",
        "skipped_theme_count",
        "batch_id",
    ):
        print(f"{key}: {result.get(key)}")
    print("sample_themes:")
    for theme in result.get("sample_themes", []):
        print(f"  - {theme}")
    print("sample_members:")
    for member in result.get("sample_members", []):
        print(f"  - {member}")


if __name__ == "__main__":
    raise SystemExit(main())
