from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


def insert_run_history(
    db_path: Path,
    started_at: str,
    finished_at: str,
    duration_sec: float,
    status: str,
    execution_mode: int,
    market_is_open: bool,
    allow_order_submission: bool,
    alpaca_enabled: bool,
    claude_enabled: bool,
    bought_count: int,
    sold_count: int,
    reconciled_count: int,
    recovered_manual_close_count: int,
    recovered_manual_partial_count: int,
    filled_count: int,
    analysis_executed: bool,
    note: str,
    error_text: str,
    result_payload: dict[str, Any],
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO run_history (
                started_at,
                finished_at,
                duration_sec,
                status,
                execution_mode,
                market_is_open,
                allow_order_submission,
                alpaca_enabled,
                claude_enabled,
                bought_count,
                sold_count,
                reconciled_count,
                recovered_manual_close_count,
                recovered_manual_partial_count,
                filled_count,
                analysis_executed,
                note,
                error_text,
                result_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                started_at,
                finished_at,
                max(float(duration_sec), 0.0),
                str(status),
                int(execution_mode),
                1 if bool(market_is_open) else 0,
                1 if bool(allow_order_submission) else 0,
                1 if bool(alpaca_enabled) else 0,
                1 if bool(claude_enabled) else 0,
                max(int(bought_count), 0),
                max(int(sold_count), 0),
                max(int(reconciled_count), 0),
                max(int(recovered_manual_close_count), 0),
                max(int(recovered_manual_partial_count), 0),
                max(int(filled_count), 0),
                1 if bool(analysis_executed) else 0,
                str(note or ""),
                str(error_text or ""),
                json.dumps(result_payload or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def fetch_recent_runs(db_path: Path, limit: int = 50) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                started_at,
                finished_at,
                duration_sec,
                status,
                execution_mode,
                market_is_open,
                allow_order_submission,
                alpaca_enabled,
                claude_enabled,
                bought_count,
                sold_count,
                reconciled_count,
                recovered_manual_close_count,
                recovered_manual_partial_count,
                filled_count,
                analysis_executed,
                note,
                error_text
            FROM run_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(int(limit), 1),),
        ).fetchall()
        return [dict(r) for r in rows]


def fetch_run_summary(db_path: Path) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_runs,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
                SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped_runs,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_runs,
                COALESCE(AVG(duration_sec), 0.0) AS avg_duration_sec,
                COALESCE(MAX(duration_sec), 0.0) AS max_duration_sec,
                COALESCE(SUM(filled_count), 0) AS total_filled_count,
                MAX(finished_at) AS last_finished_at
            FROM run_history
            """
        ).fetchone()
        return {
            "total_runs": int(row[0] or 0),
            "success_runs": int(row[1] or 0),
            "skipped_runs": int(row[2] or 0),
            "error_runs": int(row[3] or 0),
            "avg_duration_sec": round(float(row[4] or 0.0), 4),
            "max_duration_sec": round(float(row[5] or 0.0), 4),
            "total_filled_count": int(row[6] or 0),
            "last_finished_at": str(row[7] or ""),
        }
