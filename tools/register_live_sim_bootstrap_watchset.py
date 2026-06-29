from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main() -> int:
    from services.config import load_settings
    from services.theme_leadership.bootstrap_watchset import (
        queue_bootstrap_realtime_registration,
        select_bootstrap_realtime_codes,
    )
    from storage.sqlite import initialize_database

    parser = argparse.ArgumentParser(
        description="Select and optionally queue observe-only realtime theme bootstrap codes."
    )
    parser.add_argument("--max-codes", type=int)
    parser.add_argument("--screen-no", default="5002")
    parser.add_argument("--ttl-sec", type=int, default=1800)
    parser.add_argument(
        "--anchor-codes",
        help="Comma-separated anchor codes. Defaults to market_ticks_latest codes.",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Queue a register_realtime GatewayCommand for the selected codes.",
    )
    args = parser.parse_args()

    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        anchors = _csv(args.anchor_codes)
        if args.queue:
            payload = queue_bootstrap_realtime_registration(
                connection,
                settings=settings,
                anchor_codes=anchors if anchors else None,
                max_codes=args.max_codes,
                screen_no=args.screen_no,
                ttl_sec=args.ttl_sec,
            ).to_dict()
        else:
            payload = {
                "status": "DRY_RUN",
                "selection": select_bootstrap_realtime_codes(
                    connection,
                    settings=settings,
                    anchor_codes=anchors if anchors else None,
                    max_codes=args.max_codes,
                ).to_dict(),
                "observe_only": True,
                "not_order_signal": True,
                "no_order_side_effects": True,
            }
    finally:
        connection.close()

    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
