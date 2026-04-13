from __future__ import annotations

import sqlite3
from pathlib import Path


def insert_position_snapshot(
    db_path: Path,
    symbol: str,
    qty: float,
    avg_entry_price: float | None,
    market_price: float | None,
    market_value: float | None,
    unrealized_pl: float | None,
    source: str = "ALPACA",
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO position_snapshots (
                symbol,
                qty,
                avg_entry_price,
                market_price,
                market_value,
                unrealized_pl,
                source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                qty,
                avg_entry_price,
                market_price,
                market_value,
                unrealized_pl,
                source,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def fetch_latest_position_snapshot(db_path: Path, symbol: str) -> dict | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                symbol,
                qty,
                avg_entry_price,
                market_price,
                market_value,
                unrealized_pl,
                source,
                captured_at
            FROM position_snapshots
            WHERE symbol = ?
            ORDER BY captured_at DESC, id DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        return dict(row) if row else None


def delete_old_position_snapshots(db_path: Path, retention_days: int) -> int:
    days = max(int(retention_days), 1)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            DELETE FROM position_snapshots
            WHERE datetime(captured_at) < datetime('now', ?)
            """,
            (f"-{days} days",),
        )
        conn.commit()
        return int(cur.rowcount or 0)
