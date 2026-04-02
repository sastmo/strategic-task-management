from __future__ import annotations

import os
import time
import traceback

from src.sync_to_db import sync_to_db

SYNC_SOURCE = os.getenv("SYNC_SOURCE", "/app/data/tasks.csv")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://stm_user:stm_password@db:5432/strategic_tasks",
)
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "60"))


def main() -> None:
    print(
        f"Auto sync started. Source={SYNC_SOURCE} Interval={SYNC_INTERVAL_SECONDS}s"
    )

    while True:
        try:
            sync_to_db(SYNC_SOURCE, DATABASE_URL)
        except Exception as exc:
            print(f"Auto sync failed: {exc}")
            traceback.print_exc()

        time.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()