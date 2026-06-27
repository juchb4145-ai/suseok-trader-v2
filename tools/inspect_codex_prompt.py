from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.ai_sidecar.codex_prompt_store import get_codex_prompt_draft
    from services.config import load_settings
    from storage.sqlite import open_connection

    parser = argparse.ArgumentParser(description="Inspect a stored Codex prompt draft.")
    parser.add_argument("--draft-id", required=True)
    parser.add_argument("--text", action="store_true")
    args = parser.parse_args()

    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        draft = get_codex_prompt_draft(connection, args.draft_id)
    finally:
        connection.close()

    if draft is None:
        print(json.dumps({"ok": False, "error_message": "draft not found"}, indent=2))
        return 1
    if args.text:
        print(draft["prompt_text"])
        return 0
    print(json.dumps({"ok": True, "draft": draft}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
