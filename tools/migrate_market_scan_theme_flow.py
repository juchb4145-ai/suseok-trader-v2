from __future__ import annotations

from services.config import load_settings
from storage.sqlite import initialize_database


def main() -> None:
    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        print(f"market scan/theme flow schema migrated: {settings.trading_db_path}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
