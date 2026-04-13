from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from app_settings import get_sqlite_timezone_shift_modifier


def insert_order(
    db_path: Path,
    signal_id: int | None,
    alpaca_order_id: str | None,
    client_order_id: str | None,
    symbol: str,
    side: str,
    qty: float,
    status: str,
    order_type: str = "market",
    time_in_force: str = "day",
    requested_price: float | None = None,
    raw_request: dict[str, Any] | None = None,
    raw_response: dict[str, Any] | None = None,
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO orders (
                signal_id,
                alpaca_order_id,
                client_order_id,
                symbol,
                side,
                order_type,
                time_in_force,
                qty,
                requested_price,
                status,
                raw_request_json,
                raw_response_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                alpaca_order_id,
                client_order_id,
                symbol,
                side,
                order_type,
                time_in_force,
                qty,
                requested_price,
                status,
                json.dumps(raw_request or {}, ensure_ascii=False),
                json.dumps(raw_response or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def update_order_status_and_response(
    db_path: Path,
    order_id: int,
    status: str,
    raw_response: dict[str, Any] | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        if raw_response is None:
            conn.execute(
                """
                UPDATE orders
                SET status = ?
                WHERE id = ?
                """,
                (str(status or ""), order_id),
            )
        else:
            conn.execute(
                """
                UPDATE orders
                SET
                    status = ?,
                    raw_response_json = ?
                WHERE id = ?
                """,
                (str(status or ""), json.dumps(raw_response, ensure_ascii=False), order_id),
            )
        conn.commit()


def insert_order_log(
    db_path: Path,
    order_id: int,
    event_type: str,
    filled_qty: float | None = None,
    avg_fill_price: float | None = None,
    slippage_pct: float | None = None,
    capital_change_pct: float | None = None,
    realized_pnl_amount: float | None = None,
    outcome_tier: str | None = None,
    note: str = "",
    event_at: str | None = None,
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO order_logs (
                order_id,
                event_type,
                filled_qty,
                avg_fill_price,
                slippage_pct,
                capital_change_pct,
                realized_pnl_amount,
                outcome_tier,
                note,
                event_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))
            """,
            (
                order_id,
                event_type,
                filled_qty,
                avg_fill_price,
                slippage_pct,
                capital_change_pct,
                realized_pnl_amount,
                outcome_tier,
                note,
                event_at,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def has_order_log_event(
    db_path: Path,
    order_id: int,
    event_type: str,
) -> bool:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM order_logs
            WHERE order_id = ? AND event_type = ?
            LIMIT 1
            """,
            (order_id, event_type),
        ).fetchone()
        return row is not None


def open_trade(
    db_path: Path,
    symbol: str,
    entry_order_id: int,
    qty: float,
    entry_price: float,
    invested_amount: float,
    entry_slippage_pct: float | None = None,
    opened_at: str | None = None,
) -> int:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO trades (
                symbol,
                entry_order_id,
                qty,
                initial_qty,
                entry_price,
                invested_amount,
                entry_slippage_pct,
                take_profit_done,
                trailing_high_price,
                status,
                close_reason,
                opened_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, 'OPEN', '', COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))
            """,
            (
                symbol,
                entry_order_id,
                qty,
                qty,
                entry_price,
                invested_amount,
                entry_slippage_pct,
                opened_at,
            ),
        )
        inserted = int(conn.execute("SELECT changes()").fetchone()[0])
        if inserted > 0:
            row = conn.execute("SELECT last_insert_rowid()").fetchone()
            conn.commit()
            return int(row[0]) if row else 0

        # 既存行がある場合は既存IDを返して処理継続（重複で実行停止しない）
        row = conn.execute(
            """
            SELECT id
            FROM trades
            WHERE entry_order_id = ?
            LIMIT 1
            """,
            (entry_order_id,),
        ).fetchone()
        if row:
            conn.commit()
            return int(row[0])

        row = conn.execute(
            """
            SELECT id
            FROM trades
            WHERE symbol = ? AND status = 'OPEN'
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        conn.commit()
        if row:
            return int(row[0])
        raise sqlite3.IntegrityError("failed_to_open_trade_after_insert_or_ignore")


def increase_open_trade_after_additional_buy_fill(
    db_path: Path,
    trade_id: int,
    added_qty: float,
    fill_price: float,
) -> None:
    qty_add = max(float(added_qty), 0.0)
    fill_px = max(float(fill_price), 0.0)
    if qty_add <= 0 or fill_px <= 0:
        return

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT qty, initial_qty, invested_amount
            FROM trades
            WHERE id = ? AND status = 'OPEN'
            LIMIT 1
            """,
            (trade_id,),
        ).fetchone()
        if not row:
            return

        old_qty = max(float(row["qty"] or 0.0), 0.0)
        old_initial_qty = max(float(row["initial_qty"] or 0.0), 0.0)
        old_invested = max(float(row["invested_amount"] or 0.0), 0.0)

        new_qty = old_qty + qty_add
        new_initial_qty = old_initial_qty + qty_add if old_initial_qty > 0 else new_qty
        new_invested = old_invested + (qty_add * fill_px)
        new_entry_price = (new_invested / new_qty) if new_qty > 0 else fill_px

        conn.execute(
            """
            UPDATE trades
            SET
                qty = ?,
                initial_qty = ?,
                entry_price = ?,
                invested_amount = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status = 'OPEN'
            """,
            (
                new_qty,
                new_initial_qty,
                new_entry_price,
                new_invested,
                trade_id,
            ),
        )
        conn.commit()


def fetch_trade_by_entry_order_id(db_path: Path, entry_order_id: int) -> dict[str, Any] | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                symbol,
                entry_order_id,
                exit_order_id,
                qty,
                initial_qty,
                entry_price,
                exit_price,
                invested_amount,
                take_profit_done,
                trailing_high_price,
                gross_pnl_amount,
                gross_pnl_pct,
                status,
                close_reason,
                opened_at,
                closed_at
            FROM trades
            WHERE entry_order_id = ?
            LIMIT 1
            """,
            (entry_order_id,),
        ).fetchone()
        return dict(row) if row else None


def close_trade(
    db_path: Path,
    trade_id: int,
    exit_order_id: int,
    exit_price: float,
    gross_pnl_amount: float,
    gross_pnl_pct: float,
    outcome_tier: str,
    exit_slippage_pct: float | None = None,
    closed_at: str | None = None,
    close_reason: str = "sell_filled",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET
                exit_order_id = ?,
                exit_price = ?,
                gross_pnl_amount = ?,
                gross_pnl_pct = ?,
                outcome_tier = ?,
                exit_slippage_pct = ?,
                status = 'CLOSED',
                close_reason = ?,
                closed_at = COALESCE(?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ?
            """,
            (
                exit_order_id,
                exit_price,
                gross_pnl_amount,
                gross_pnl_pct,
                outcome_tier,
                exit_slippage_pct,
                str(close_reason or ""),
                closed_at,
                trade_id,
            ),
        )
        conn.commit()


def reduce_open_trade_after_partial_exit(
    db_path: Path,
    trade_id: int,
    remaining_qty: float,
    realized_pnl_delta: float,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET
                qty = ?,
                gross_pnl_amount = COALESCE(gross_pnl_amount, 0) + ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status = 'OPEN'
            """,
            (
                max(float(remaining_qty), 0.0),
                float(realized_pnl_delta),
                trade_id,
            ),
        )
        conn.commit()


def update_trade_exit_state(
    db_path: Path,
    trade_id: int,
    take_profit_done: bool | None = None,
    trailing_high_price: float | None = None,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET
                take_profit_done = CASE
                    WHEN ? IS NULL THEN take_profit_done
                    ELSE CASE WHEN ? = 1 THEN 1 ELSE 0 END
                END,
                trailing_high_price = CASE
                    WHEN ? IS NULL THEN trailing_high_price
                    WHEN trailing_high_price IS NULL THEN ?
                    WHEN ? > trailing_high_price THEN ?
                    ELSE trailing_high_price
                END,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status = 'OPEN'
            """,
            (
                None if take_profit_done is None else (1 if take_profit_done else 0),
                1 if bool(take_profit_done) else 0,
                trailing_high_price,
                trailing_high_price,
                trailing_high_price,
                trailing_high_price,
                trade_id,
            ),
        )
        conn.commit()


def sync_open_trade_position(
    db_path: Path,
    trade_id: int,
    qty: float,
    entry_price: float,
) -> None:
    qty_val = max(float(qty), 0.0)
    entry_val = max(float(entry_price), 0.0)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET
                qty = ?,
                initial_qty = CASE
                    WHEN COALESCE(initial_qty, 0) <= 0 AND ? > 0 THEN ?
                    ELSE initial_qty
                END,
                entry_price = CASE WHEN ? > 0 THEN ? ELSE entry_price END,
                -- invested_amount は初回エントリー元本として保持する。
                -- 部分利確後に qty 同期で更新すると最終 gross_pnl_pct が歪むため、
                -- 異常値(<=0)の補正時のみ再計算する。
                invested_amount = CASE
                    WHEN COALESCE(invested_amount, 0) <= 0 AND ? > 0 THEN (? * ?)
                    ELSE invested_amount
                END,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ? AND status = 'OPEN'
            """,
            (
                qty_val,
                qty_val,
                qty_val,
                entry_val,
                entry_val,
                entry_val,
                qty_val,
                entry_val,
                trade_id,
            ),
        )
        conn.commit()


def cancel_open_trade(db_path: Path, trade_id: int, reason: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET
                status = 'CANCELLED',
                close_reason = ?,
                closed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                gross_pnl_amount = COALESCE(gross_pnl_amount, 0),
                gross_pnl_pct = COALESCE(gross_pnl_pct, 0),
                outcome_tier = COALESCE(outcome_tier, 'LOSE')
            WHERE id = ? AND status = 'OPEN'
            """,
            (str(reason or ""), trade_id),
        )
        conn.commit()


def fetch_open_trade_by_symbol(db_path: Path, symbol: str) -> dict[str, Any] | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                symbol,
                entry_order_id,
                exit_order_id,
                qty,
                initial_qty,
                entry_price,
                exit_price,
                invested_amount,
                take_profit_done,
                trailing_high_price,
                gross_pnl_amount,
                gross_pnl_pct,
                status,
                close_reason,
                opened_at,
                closed_at
            FROM trades
            WHERE symbol = ? AND status = 'OPEN'
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        return dict(row) if row else None


def fetch_unresolved_orders(db_path: Path, limit: int = 200) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                signal_id,
                alpaca_order_id,
                symbol,
                side,
                qty,
                requested_price,
                status,
                raw_response_json,
                submitted_at
            FROM orders
            WHERE alpaca_order_id IS NOT NULL
              AND status NOT IN (
                  'filled', 'rejected', 'canceled', 'cancelled',
                  'expired', 'done_for_day', 'stopped', 'suspended', 'calculated'
              )
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [dict(r) for r in rows]


def fetch_order_by_alpaca_order_id(db_path: Path, alpaca_order_id: str) -> dict[str, Any] | None:
    key = str(alpaca_order_id or "").strip()
    if not key:
        return None
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT
                id,
                signal_id,
                alpaca_order_id,
                symbol,
                side,
                qty,
                requested_price,
                status,
                submitted_at
            FROM orders
            WHERE alpaca_order_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (key,),
        ).fetchone()
        return dict(row) if row else None


def fetch_open_trades(db_path: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                symbol,
                entry_order_id,
                qty,
                initial_qty,
                entry_price,
                invested_amount,
                take_profit_done,
                trailing_high_price,
                opened_at
            FROM trades
            WHERE status = 'OPEN'
            ORDER BY id DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def fetch_today_realized_pnl_amount(db_path: Path) -> float:
    shift = get_sqlite_timezone_shift_modifier()
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(COALESCE(realized_pnl_amount, 0.0)), 0.0)
            FROM order_logs
            WHERE event_type = 'SELL_FILLED'
              AND date(datetime(event_at, ?)) = date(datetime('now', ?))
            """
            ,
            (shift, shift),
        ).fetchone()
        return float(row[0]) if row else 0.0


def fetch_recent_order_logs(db_path: Path, limit: int = 50) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                ol.id,
                ol.order_id,
                o.symbol,
                o.side,
                ol.event_type,
                ol.filled_qty,
                ol.avg_fill_price,
                ol.slippage_pct,
                ol.capital_change_pct,
                ol.realized_pnl_amount,
                ol.outcome_tier,
                ol.note,
                ol.event_at
            FROM order_logs ol
            JOIN orders o ON o.id = ol.order_id
            ORDER BY ol.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def insert_analysis_report(
    db_path: Path,
    report_type: str,
    summary: str,
    payload: dict[str, Any],
) -> int:
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO analysis_reports (
                report_type,
                summary,
                payload_json
            )
            VALUES (?, ?, ?)
            """,
            (report_type, summary, json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
        return int(cur.lastrowid)


def fetch_trade_statistics(db_path: Path) -> dict[str, dict[str, float]]:
    shift = get_sqlite_timezone_shift_modifier()
    periods: dict[str, tuple[str, tuple[Any, ...]]] = {
        "total": ("", ()),
        "year": ("AND datetime(closed_at, ?) >= datetime('now', ?, '-1 year')", (shift, shift)),
        "half_year": ("AND datetime(closed_at, ?) >= datetime('now', ?, '-6 months')", (shift, shift)),
        "quarter": ("AND datetime(closed_at, ?) >= datetime('now', ?, '-3 months')", (shift, shift)),
        "month": ("AND datetime(closed_at, ?) >= datetime('now', ?, '-1 month')", (shift, shift)),
        "week": ("AND datetime(closed_at, ?) >= datetime('now', ?, '-7 days')", (shift, shift)),
        "day": ("AND date(datetime(closed_at, ?)) = date(datetime('now', ?))", (shift, shift)),
    }
    output: dict[str, dict[str, float]] = {}
    with sqlite3.connect(db_path) as conn:
        for key, (where_extra, params) in periods.items():
            sql = f"""
                SELECT
                    COUNT(*) AS trades,
                    COALESCE(SUM(gross_pnl_amount), 0.0) AS pnl_amount,
                    COALESCE(AVG(gross_pnl_pct), 0.0) AS avg_pnl_pct,
                    SUM(CASE WHEN gross_pnl_amount > 0 THEN 1 ELSE 0 END) AS wins
                FROM trades
                WHERE status = 'CLOSED'
                {where_extra}
            """
            row = conn.execute(sql, params).fetchone()
            trades = int(row[0]) if row else 0
            wins = int(row[3] or 0) if row else 0
            win_rate = (wins / trades * 100.0) if trades > 0 else 0.0
            output[key] = {
                "trades": trades,
                "wins": wins,
                "win_rate": round(win_rate, 2),
                "pnl_amount": round(float(row[1] if row else 0.0), 4),
                "avg_pnl_pct": round(float(row[2] if row else 0.0), 4),
            }
    return output
