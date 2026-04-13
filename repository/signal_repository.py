from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


def insert_signal(
    db_path: Path,
    flow_type: str,
    symbol: str,
    decision: str,
    decision_score: float | None,
    indicator_snapshot: dict[str, Any],
    claude_snapshot: dict[str, Any],
    reason: str,
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO signals (
                flow_type,
                symbol,
                decision,
                decision_score,
                indicator_snapshot_json,
                claude_snapshot_json,
                reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                flow_type,
                symbol,
                decision,
                decision_score,
                json.dumps(indicator_snapshot, ensure_ascii=False),
                json.dumps(claude_snapshot, ensure_ascii=False),
                reason,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def delete_old_signals(db_path: Path, retention_days: int) -> int:
    days = max(int(retention_days), 1)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            DELETE FROM signals
            WHERE datetime(created_at) < datetime('now', ?)
            """,
            (f"-{days} days",),
        )
        conn.commit()
        return int(cur.rowcount or 0)
