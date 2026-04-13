from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "db"
DB_PATH = DB_DIR / "trading.db"
DB_SCHEMA_PATH = DB_DIR / "schema.sql"
