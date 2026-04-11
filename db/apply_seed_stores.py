"""Execute db/seed_stores.sql using .env (same credentials as ETL)."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from etl.loader import get_connection


def main() -> None:
    sql_path = Path(__file__).with_name("seed_stores.sql")
    sql = sql_path.read_text(encoding="utf-8")
    batches = [b.strip() for b in re.split(r"(?im)^\s*GO\s*$", sql) if b.strip()]
    conn = get_connection(
        os.environ["DB_SERVER"],
        os.environ["DB_NAME"],
        os.environ["DB_USER"],
        os.environ["DB_PASSWORD"],
    )
    cur = conn.cursor()
    try:
        for batch in batches:
            if re.match(r"(?is)^\s*USE\s+\w+", batch):
                continue
            cur.execute(batch)
        conn.commit()
    finally:
        conn.close()
    print("dim_store: seed_stores.sql completed successfully.")


if __name__ == "__main__":
    main()
