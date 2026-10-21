from __future__ import annotations

from pathlib import Path

from .config import Settings, load_settings
from .db import pg_connect

SCHEMA = Path(__file__).with_name("schema.sql")


def migrate(settings: Settings) -> None:
    sql = SCHEMA.read_text(encoding="utf-8")
    with pg_connect(settings) as conn:
        conn.execute(sql)
        conn.commit()


if __name__ == "__main__":
    migrate(load_settings())
    print("migrated")
