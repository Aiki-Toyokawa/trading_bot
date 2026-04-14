from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app_settings import (
    BUY_CHECK_INTERVAL_SECONDS,
    DEBUG_EXECUTION_MODE,
    DEBUG_FORCE_RUN_WHEN_MARKET_CLOSED,
    ENABLE_DB_RETENTION_CLEANUP,
    MARKET_HOURS_GUARD_ENABLED,
    SELL_CHECK_INTERVAL_SECONDS,
    POSITION_SNAPSHOT_RETENTION_DAYS,
    SIGNAL_RETENTION_DAYS,
    get_timezone_mode_normalized,
)
from db_settings import DB_PATH, DB_SCHEMA_PATH
from flows.entry_flow import run_entry_flow
from flows.exit_flow import run_exit_flow
from flows.sync_flow import run_sync_flow
from infra.alpaca_client import AlpacaClient
from infra.claude_client import ClaudeClient
from infra.finviz_scraper import FinvizScraper
from infra.runtime_logger import log_section, log_step
from repository.order_repository import (
    fetch_recent_order_logs,
    fetch_trade_statistics,
    insert_analysis_report,
)
from repository.position_repository import delete_old_position_snapshots
from repository.run_repository import insert_run_history
from repository.signal_repository import delete_old_signals


def _ensure_runtime_migrations(conn: sqlite3.Connection) -> None:
    trades_cols = {
        str(row[1]).lower()
        for row in conn.execute("PRAGMA table_info(trades)").fetchall()
    }
    if "close_reason" not in trades_cols:
        conn.execute("ALTER TABLE trades ADD COLUMN close_reason TEXT NOT NULL DEFAULT ''")
    if "initial_qty" not in trades_cols:
        conn.execute("ALTER TABLE trades ADD COLUMN initial_qty REAL NOT NULL DEFAULT 0")
    if "take_profit_done" not in trades_cols:
        conn.execute("ALTER TABLE trades ADD COLUMN take_profit_done INTEGER NOT NULL DEFAULT 0")
    if "trailing_high_price" not in trades_cols:
        conn.execute("ALTER TABLE trades ADD COLUMN trailing_high_price REAL")

    conn.execute(
        """
        UPDATE trades
        SET initial_qty = qty
        WHERE COALESCE(initial_qty, 0) <= 0
        """
    )
    conn.execute(
        """
        UPDATE trades
        SET take_profit_done = CASE
            WHEN qty < initial_qty THEN 1
            ELSE take_profit_done
        END
        WHERE status = 'OPEN'
        """
    )

    # 既存データに同一symbolのOPEN重複がある場合、最新1件を残して古いものを自動補正
    duplicate_symbols = conn.execute(
        """
        SELECT symbol
        FROM trades
        WHERE status = 'OPEN'
        GROUP BY symbol
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    for (symbol,) in duplicate_symbols:
        rows = conn.execute(
            """
            SELECT id
            FROM trades
            WHERE symbol = ? AND status = 'OPEN'
            ORDER BY id DESC
            """,
            (symbol,),
        ).fetchall()
        keep_id = int(rows[0][0])
        for row in rows[1:]:
            conn.execute(
                """
                UPDATE trades
                SET
                    status = 'CANCELLED',
                    close_reason = 'duplicate_open_trade_auto_clean',
                    closed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    gross_pnl_amount = COALESCE(gross_pnl_amount, 0),
                    gross_pnl_pct = COALESCE(gross_pnl_pct, 0),
                    outcome_tier = COALESCE(outcome_tier, 'LOSE')
                WHERE id = ?
                """,
                (int(row[0]),),
            )
        conn.execute(
            """
            UPDATE trades
            SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE id = ?
            """,
            (keep_id,),
        )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_symbol_open
        ON trades(symbol)
        WHERE status = 'OPEN'
        """
    )

    order_log_cols = {
        str(row[1]).lower()
        for row in conn.execute("PRAGMA table_info(order_logs)").fetchall()
    }
    if "realized_pnl_amount" not in order_log_cols:
        conn.execute("ALTER TABLE order_logs ADD COLUMN realized_pnl_amount REAL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        )
        """
    )


def initialize_database() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = DB_SCHEMA_PATH.read_text(encoding="utf-8")
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(schema_sql)
        _ensure_runtime_migrations(conn)
        conn.commit()


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ[key] = value


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_runtime_state_value(db_path: Path, state_key: str) -> str:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT state_value
            FROM runtime_state
            WHERE state_key = ?
            LIMIT 1
            """,
            (state_key,),
        ).fetchone()
    if not row:
        return ""
    return str(row[0] or "")


def _set_runtime_state_value(db_path: Path, state_key: str, state_value: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO runtime_state (state_key, state_value, updated_at)
            VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at
            """,
            (state_key, state_value),
        )
        conn.commit()


def _status_text(ok: bool | None) -> str:
    if ok is True:
        return "OK"
    if ok is False:
        return "NG"
    return "UNKNOWN"


def _derive_run_history_fields(result: dict[str, Any], error_text: str) -> dict[str, Any]:
    debug = result.get("debug", {})
    market_gate = result.get("market_gate", {})
    sync_flow = result.get("sync_flow", {})
    exit_flow = result.get("exit_flow", {})
    entry_flow = result.get("entry_flow", {})
    analysis = result.get("analysis", {})

    execution_mode = _to_int(debug.get("execution_mode"), DEBUG_EXECUTION_MODE)
    if execution_mode not in {0, 1, 2, 3}:
        execution_mode = 0
    run_entry_enabled = execution_mode in {0, 1, 3}
    run_exit_enabled = execution_mode in {0, 1, 2}

    sold_count = _to_int(exit_flow.get("sold"), 0)
    bought_count = _to_int(entry_flow.get("bought"), 0)
    reconciled_count = _to_int(sync_flow.get("reconciled_orders_filled"), 0)
    recovered_manual_close_count = _to_int(sync_flow.get("recovered_manual_close"), 0)
    recovered_manual_partial_count = _to_int(sync_flow.get("recovered_manual_partial"), 0)
    filled_count = (
        sold_count
        + bought_count
        + reconciled_count
        + recovered_manual_close_count
        + recovered_manual_partial_count
    )
    analysis_executed = not bool(analysis.get("skipped", False))

    base_note = str(entry_flow.get("note") or exit_flow.get("note") or sync_flow.get("note") or "completed")

    entry_note = str(entry_flow.get("note") or "")
    exit_note = str(exit_flow.get("note") or "")

    # 「有効だったか」ではなく「そのrunで実際に処理したか」で判定する
    entry_skipped_notes = {
        "skipped_by_debug_mode",
        "skipped_market_closed",
        "skipped_account_error",
        "skipped_by_buy_interval",
    }
    exit_skipped_notes = {
        "skipped_by_debug_mode",
        "skipped_market_closed",
        "skipped_account_error",
    }

    entry_processed = run_entry_enabled and entry_note not in entry_skipped_notes
    exit_processed = run_exit_enabled and exit_note not in exit_skipped_notes

    if entry_processed and exit_processed:
        flow_note = "buy_and_sell_processed"
    elif entry_processed:
        flow_note = "buy_only_processed"
    elif exit_processed:
        flow_note = "sell_only_processed"
    else:
        flow_note = "no_flow_processed"
    note = f"{flow_note} | {base_note}"
    status = "SUCCESS"
    if error_text:
        status = "ERROR"
        note = "exception"
    else:
        skip_reasons = {
            "market_closed",
            "alpaca_account_unavailable",
            "skipped_market_closed",
            "skipped_account_error",
        }
        market_source = str(market_gate.get("source", ""))
        analysis_reason = str(analysis.get("reason", ""))
        if market_source == "account_error" or analysis_reason in skip_reasons:
            status = "SKIPPED"
        elif str(sync_flow.get("note", "")) in skip_reasons:
            status = "SKIPPED"
        elif str(entry_flow.get("note", "")) in skip_reasons:
            status = "SKIPPED"
        elif str(exit_flow.get("note", "")) in skip_reasons:
            status = "SKIPPED"

    return {
        "status": status,
        "execution_mode": execution_mode,
        "market_is_open": bool(market_gate.get("is_open", False)),
        "allow_order_submission": bool(debug.get("allow_order_submission", execution_mode == 0)),
        "alpaca_enabled": bool(result.get("alpaca_enabled", False)),
        "claude_enabled": bool(result.get("claude_enabled", False)),
        "bought_count": bought_count,
        "sold_count": sold_count,
        "reconciled_count": reconciled_count,
        "recovered_manual_close_count": recovered_manual_close_count,
        "recovered_manual_partial_count": recovered_manual_partial_count,
        "filled_count": filled_count,
        "analysis_executed": analysis_executed,
        "note": note,
    }


def run_once() -> dict[str, Any]:
    log_section("Run Start")
    log_step("環境変数読み込み開始")
    load_env_file(Path(__file__).resolve().parent / ".env")
    log_step("環境変数読み込み完了")

    log_step("DB初期化開始")
    initialize_database()
    log_step("DB初期化完了")
    if ENABLE_DB_RETENTION_CLEANUP:
        log_step("DB保持期間クリーンアップ開始")
        deleted_signals = delete_old_signals(DB_PATH, SIGNAL_RETENTION_DAYS)
        deleted_snapshots = delete_old_position_snapshots(DB_PATH, POSITION_SNAPSHOT_RETENTION_DAYS)
        log_step(
            "DB保持期間クリーンアップ完了: "
            f"signals={deleted_signals} position_snapshots={deleted_snapshots}"
        )

    log_section("Client Setup")
    alpaca = AlpacaClient(
        api_key=os.getenv("ALPC_API_KEY", ""),
        secret_key=os.getenv("ALPC_SECRET_KEY", ""),
    )
    claude = ClaudeClient(api_key=os.getenv("ANTHROPIC_API_KEY", ""))
    finviz = FinvizScraper()
    _alp_ok, alp_msg = alpaca.health_check()
    log_step(alp_msg)
    _cla_ok, cla_msg = claude.health_check()
    log_step(cla_msg)
    _fin_ok, fin_msg = finviz.health_check()
    log_step(fin_msg)
    log_step("Finvizクライアント準備完了")
    connectivity = {
        "alpaca_api": _status_text(_alp_ok),
        "anthropic_api": _status_text(_cla_ok),
        "finviz_web": _status_text(_fin_ok),
    }
    account_before = alpaca.get_account()
    log_step(
        "口座スナップショット(開始): "
        f"cash={account_before.get('cash')} equity={account_before.get('equity')} "
        f"buying_power={account_before.get('buying_power')}"
    )
    if alpaca.enabled and str(account_before.get("source", "")) != "ALPACA":
        err = str(account_before.get("error", "account_unavailable"))
        log_step(f"Alpaca口座情報取得失敗のため実行停止: {err}")
        return {
            "db_path": str(DB_PATH),
            "alpaca_enabled": alpaca.enabled,
            "claude_enabled": claude.enabled,
            "connectivity": connectivity,
            "debug": {
                "execution_mode": 0,
                "force_run_when_market_closed": DEBUG_FORCE_RUN_WHEN_MARKET_CLOSED,
                "allow_order_submission": False,
                "timezone_mode": get_timezone_mode_normalized(),
            },
            "account_before": account_before,
            "market_gate": {"is_open": False, "source": "account_error", "message": "skip_by_account_error"},
            "sync_flow": {"note": "skipped_account_error"},
            "exit_flow": {"note": "skipped_account_error", "checked": 0, "sold": 0},
            "entry_flow": {"note": "skipped_account_error", "bought": 0},
            "stats": fetch_trade_statistics(DB_PATH),
            "analysis": {"skipped": True, "reason": "alpaca_account_unavailable"},
        }

    log_section("Market Gate")
    market_gate = alpaca.market_gate_status()
    log_step(str(market_gate.get("message", "")))
    execution_mode = DEBUG_EXECUTION_MODE if DEBUG_EXECUTION_MODE in {0, 1, 2, 3} else 0
    if execution_mode != DEBUG_EXECUTION_MODE:
        log_step(f"Debug: 無効な DEBUG_EXECUTION_MODE={DEBUG_EXECUTION_MODE} のため 0 にフォールバック")

    market_is_open = bool(market_gate.get("is_open", False))
    if MARKET_HOURS_GUARD_ENABLED and not market_is_open:
        if DEBUG_FORCE_RUN_WHEN_MARKET_CLOSED:
            log_step("Debug: 市場時間外だが強制実行を許可")
        elif execution_mode in {1, 2, 3}:
            log_step("Debug: 市場時間外だが判定専用モードのため継続（注文なし）")
        else:
            log_step("市場時間外のため同期/売買/分析をスキップ")
            stats = fetch_trade_statistics(DB_PATH)
            account_after = alpaca.get_account()
            return {
                "db_path": str(DB_PATH),
                "alpaca_enabled": alpaca.enabled,
                "claude_enabled": claude.enabled,
                "connectivity": connectivity,
                "account_before": account_before,
                "account_after": account_after,
                "market_gate": market_gate,
                "sync_flow": {"note": "skipped_market_closed"},
                "exit_flow": {"note": "skipped_market_closed", "checked": 0, "sold": 0},
                "entry_flow": {"note": "skipped_market_closed", "bought": 0},
                "stats": stats,
                "analysis": {"skipped": True, "reason": "market_closed"},
            }

    run_entry_enabled = execution_mode in {0, 1, 3}
    run_exit_enabled = execution_mode in {0, 1, 2}
    allow_order_submission = execution_mode == 0
    if allow_order_submission and not alpaca.enabled:
        allow_order_submission = False
        log_step("Debug: Alpaca無効のため注文送信を自動で無効化（判定のみ継続）")
    log_step(
        "DebugMode: "
        f"mode={execution_mode} "
        f"run_exit={run_exit_enabled} run_entry={run_entry_enabled} "
        f"allow_order_submission={allow_order_submission}"
    )
    cycle_epoch = time.time()

    log_section("Sync Flow")
    sync_result = run_sync_flow(DB_PATH, alpaca, log_step_fn=log_step)

    log_section("Exit Flow")
    exit_check_at_epoch = cycle_epoch
    if run_exit_enabled:
        exit_result = run_exit_flow(
            DB_PATH,
            alpaca,
            log_step_fn=log_step,
            allow_order_submission=allow_order_submission,
        )
        _set_runtime_state_value(DB_PATH, "last_sell_check_at", str(exit_check_at_epoch))
    else:
        log_step("ExitFlow: DEBUG_EXECUTION_MODE によりスキップ")
        exit_result = {"note": "skipped_by_debug_mode", "checked": 0, "sold": 0}
    log_step(f"ExitFlow完了: checked={exit_result.get('checked', 0)} sold={exit_result.get('sold', 0)}")

    log_section("Entry Flow")
    now_epoch = cycle_epoch
    last_buy_check_at_raw = _get_runtime_state_value(DB_PATH, "last_buy_check_at")
    last_buy_check_at_epoch = _to_float(last_buy_check_at_raw, 0.0)
    last_sell_check_at_raw = _get_runtime_state_value(DB_PATH, "last_sell_check_at")
    last_sell_check_at_epoch = _to_float(last_sell_check_at_raw, 0.0)
    next_buy_due_epoch = last_buy_check_at_epoch + float(BUY_CHECK_INTERVAL_SECONDS)
    # BUYはSELLとカウントダウン基準を揃えるため、早期実行の許容を最小にする
    buy_interval_slack_sec = 0.25
    buy_interval_ready = (
        (last_buy_check_at_epoch <= 0.0)
        or ((now_epoch + buy_interval_slack_sec) >= next_buy_due_epoch)
    )

    if run_entry_enabled and not buy_interval_ready:
        wait_sec = max(next_buy_due_epoch - now_epoch, 0.0)
        elapsed_sec = max(now_epoch - last_buy_check_at_epoch, 0.0) if last_buy_check_at_epoch > 0 else 0.0
        log_step(
            "EntryFlow: BUY間隔ゲートによりスキップ "
            f"(interval={BUY_CHECK_INTERVAL_SECONDS}s elapsed={round(elapsed_sec, 1)}s next_in={round(wait_sec, 1)}s)"
        )
        entry_result = {
            "note": "skipped_by_buy_interval",
            "bought": 0,
            "rejected_count": 0,
            "next_buy_in_sec": round(wait_sec, 1),
            "elapsed_since_last_buy_check_sec": round(elapsed_sec, 1),
        }
    elif run_entry_enabled:
        entry_result = run_entry_flow(
            DB_PATH,
            alpaca,
            finviz,
            claude,
            log_step_fn=log_step,
            allow_order_submission=allow_order_submission,
        )
        _set_runtime_state_value(DB_PATH, "last_buy_check_at", str(cycle_epoch))
    else:
        log_step("EntryFlow: DEBUG_EXECUTION_MODE によりスキップ")
        entry_result = {"note": "skipped_by_debug_mode", "bought": 0, "rejected_count": 0}
    log_step(
        "EntryFlow完了: "
        f"note={entry_result.get('note')} "
        f"bought={entry_result.get('bought', 0)} "
        f"rejected={entry_result.get('rejected_count', 0)}"
    )

    log_section("Stats & Analysis")
    log_step("統計集計開始")
    stats = fetch_trade_statistics(DB_PATH)
    log_step("統計集計完了")
    sold_count = int(exit_result.get("sold", 0) or 0)
    bought_count = int(entry_result.get("bought", 0) or 0)
    reconciled_count = int(sync_result.get("reconciled_orders_filled", 0) or 0)
    recovered_manual_close_count = int(sync_result.get("recovered_manual_close", 0) or 0)
    recovered_manual_partial_count = int(sync_result.get("recovered_manual_partial", 0) or 0)
    filled_count = (
        sold_count
        + bought_count
        + reconciled_count
        + recovered_manual_close_count
        + recovered_manual_partial_count
    )
    analysis: dict[str, Any] = {}
    if filled_count > 0:
        log_step(f"約定件数={filled_count} のためログ分析を実行")
        log_step("ログ取得開始")
        recent_logs = fetch_recent_order_logs(DB_PATH, limit=30)
        log_step(f"ログ取得完了: {len(recent_logs)}件")
        log_step("Claudeログ分析開始")
        analysis = claude.analyze_logs(recent_logs=recent_logs, stats=stats)
        log_step("Claudeログ分析完了")
        log_step("分析レポート保存開始")
        insert_analysis_report(
            db_path=DB_PATH,
            report_type="RUN_REPORT",
            summary=str(analysis.get("summary", "")),
            payload=analysis,
        )
        log_step("分析レポート保存完了")
    else:
        log_step("約定が0件のためログ分析をスキップ")
        analysis = {
            "skipped": True,
            "reason": "no_filled_orders_in_this_run",
        }

    account_after = alpaca.get_account()
    log_step(
        "口座スナップショット(終了): "
        f"cash={account_after.get('cash')} equity={account_after.get('equity')} "
        f"buying_power={account_after.get('buying_power')}"
    )

    return {
        "db_path": str(DB_PATH),
        "alpaca_enabled": alpaca.enabled,
        "claude_enabled": claude.enabled,
        "connectivity": connectivity,
        "debug": {
            "execution_mode": execution_mode,
            "force_run_when_market_closed": DEBUG_FORCE_RUN_WHEN_MARKET_CLOSED,
            "allow_order_submission": allow_order_submission,
            "timezone_mode": get_timezone_mode_normalized(),
            "buy_check_interval_seconds": BUY_CHECK_INTERVAL_SECONDS,
            "sell_check_interval_seconds": SELL_CHECK_INTERVAL_SECONDS,
            "last_buy_check_at": last_buy_check_at_epoch,
            "last_sell_check_at": last_sell_check_at_epoch,
        },
        "account_before": account_before,
        "account_after": account_after,
        "market_gate": market_gate,
        "sync_flow": sync_result,
        "exit_flow": exit_result,
        "entry_flow": entry_result,
        "stats": stats,
        "analysis": analysis,
    }


def run_once_with_history() -> dict[str, Any]:
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    started_epoch = time.time()
    started_perf = time.perf_counter()
    result: dict[str, Any] = {}
    error_text = ""

    try:
        initialize_database()
        _set_runtime_state_value(DB_PATH, "run_in_progress", "1")
        _set_runtime_state_value(DB_PATH, "run_started_at_epoch", str(started_epoch))
        result = run_once()
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        log_step(f"RunError: {error_text}")
        result = {
            "db_path": str(DB_PATH),
            "alpaca_enabled": False,
            "claude_enabled": False,
            "connectivity": {
                "alpaca_api": "UNKNOWN",
                "anthropic_api": "UNKNOWN",
                "finviz_web": "UNKNOWN",
            },
            "analysis": {"skipped": True, "reason": "run_exception"},
            "error": error_text,
        }
    finally:
        finished_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        finished_epoch = time.time()
        duration_sec = time.perf_counter() - started_perf
        try:
            initialize_database()
            _set_runtime_state_value(DB_PATH, "run_in_progress", "0")
            _set_runtime_state_value(DB_PATH, "run_finished_at_epoch", str(finished_epoch))
            fields = _derive_run_history_fields(result, error_text)
            insert_run_history(
                db_path=DB_PATH,
                started_at=started_at,
                finished_at=finished_at,
                duration_sec=duration_sec,
                status=str(fields["status"]),
                execution_mode=int(fields["execution_mode"]),
                market_is_open=bool(fields["market_is_open"]),
                allow_order_submission=bool(fields["allow_order_submission"]),
                alpaca_enabled=bool(fields["alpaca_enabled"]),
                claude_enabled=bool(fields["claude_enabled"]),
                bought_count=int(fields["bought_count"]),
                sold_count=int(fields["sold_count"]),
                reconciled_count=int(fields["reconciled_count"]),
                recovered_manual_close_count=int(fields["recovered_manual_close_count"]),
                recovered_manual_partial_count=int(fields["recovered_manual_partial_count"]),
                filled_count=int(fields["filled_count"]),
                analysis_executed=bool(fields["analysis_executed"]),
                note=str(fields["note"]),
                error_text=error_text,
                result_payload=result,
            )
        except Exception as history_exc:
            log_step(f"RunHistory保存失敗: {history_exc}")

    return result


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    result = run_once_with_history()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if str(result.get("error", "")).strip():
        sys.exit(1)
