from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app_settings import classify_outcome_tier
from logic.exit_logic import calc_capital_change_pct, calc_slippage_pct
from repository.order_repository import (
    cancel_open_trade,
    close_trade,
    fetch_open_trade_by_symbol,
    has_order_log_event,
    fetch_order_by_alpaca_order_id,
    fetch_open_trades,
    fetch_trade_by_entry_order_id,
    fetch_unresolved_orders,
    increase_open_trade_after_additional_buy_fill,
    insert_order,
    insert_order_log,
    open_trade,
    reduce_open_trade_after_partial_exit,
    sync_open_trade_position,
    update_order_status_and_response,
)
from repository.signal_repository import insert_signal


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _log(log_step_fn: Any, message: str) -> None:
    if callable(log_step_fn):
        log_step_fn(message)


def _parse_iso(ts: Any) -> datetime | None:
    text = str(ts or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _extract_prev_filled_qty(row: dict[str, Any]) -> float:
    raw = row.get("raw_response_json")
    payload: dict[str, Any] = {}
    if isinstance(raw, dict):
        payload = raw
    elif isinstance(raw, str):
        text = raw.strip()
        if text:
            try:
                decoded = json.loads(text)
                if isinstance(decoded, dict):
                    payload = decoded
            except json.JSONDecodeError:
                payload = {}

    prev_filled = _to_float(payload.get("filled_qty"), 0.0)
    prev_status = str(payload.get("status", "")).lower()
    if prev_filled <= 0 and prev_status == "filled":
        prev_filled = _to_float(payload.get("qty"), 0.0)

    req_qty = _to_float(row.get("qty"), 0.0)
    if req_qty > 0:
        prev_filled = min(prev_filled, req_qty)
    return max(prev_filled, 0.0)


def _reconcile_unresolved_orders(db_path: Any, alpaca_client: Any, log_step_fn: Any) -> dict[str, Any]:
    unresolved = fetch_unresolved_orders(db_path, limit=300)
    if not unresolved:
        return {"checked": 0, "filled_resolved": 0, "details": []}

    checked = 0
    filled_resolved = 0
    details: list[dict[str, Any]] = []
    _log(log_step_fn, f"SyncFlow: 未確定注文の照合開始 count={len(unresolved)}")

    for row in unresolved:
        order_id = int(row["id"])
        alpaca_order_id = str(row.get("alpaca_order_id", "")).strip()
        if not alpaca_order_id:
            continue

        checked += 1
        polled = alpaca_client.get_order_by_id(alpaca_order_id)
        if not polled:
            continue

        prev_filled_qty = _extract_prev_filled_qty(row)
        status = str(polled.get("status", row.get("status", "unknown")))
        update_order_status_and_response(db_path=db_path, order_id=order_id, status=status, raw_response=polled)
        if not alpaca_client.is_effectively_filled(polled):
            continue

        symbol = str(row.get("symbol", "")).upper()
        side = str(row.get("side", "")).upper()
        req_qty = _to_float(row.get("qty"))
        cumulative_filled_qty = alpaca_client.extract_filled_qty(polled)
        if cumulative_filled_qty <= 0:
            continue
        if req_qty > 0:
            cumulative_filled_qty = min(req_qty, cumulative_filled_qty)
        filled_qty = cumulative_filled_qty - prev_filled_qty
        if req_qty > 0:
            remaining_req_qty = max(req_qty - prev_filled_qty, 0.0)
            filled_qty = min(filled_qty, remaining_req_qty)
        if filled_qty <= 1e-9:
            continue
        filled_price = _to_float(polled.get("filled_avg_price"), _to_float(row.get("requested_price")))
        if filled_price <= 0:
            continue

        if side == "BUY":
            slippage_pct = calc_slippage_pct("BUY", _to_float(row.get("requested_price"), filled_price), filled_price)
            insert_order_log(
                db_path=db_path,
                order_id=order_id,
                event_type="BUY_FILLED",
                filled_qty=filled_qty,
                avg_fill_price=filled_price,
                slippage_pct=slippage_pct,
                note="reconciled_delayed_fill_delta",
                event_at=(polled.get("filled_at") or polled.get("submitted_at")),
            )
            existing_trade = fetch_trade_by_entry_order_id(db_path, order_id)
            if existing_trade is None:
                open_trade(
                    db_path=db_path,
                    symbol=symbol,
                    entry_order_id=order_id,
                    qty=filled_qty,
                    entry_price=filled_price,
                    invested_amount=filled_qty * filled_price,
                )
            else:
                increase_open_trade_after_additional_buy_fill(
                    db_path=db_path,
                    trade_id=int(existing_trade["id"]),
                    added_qty=filled_qty,
                    fill_price=filled_price,
                )
            filled_resolved += 1
            details.append({"order_id": order_id, "symbol": symbol, "side": side, "action": "BUY_RECONCILED"})
            _log(log_step_fn, f"SyncFlow: 遅延約定反映 BUY {symbol} delta_qty={filled_qty}")
            continue

        if side == "SELL":
            trade = fetch_open_trade_by_symbol(db_path, symbol)
            if trade is None:
                continue

            entry_price = _to_float(trade.get("entry_price"))
            trade_qty = _to_float(trade.get("qty"))
            leg_pnl_amount = (filled_price - entry_price) * min(trade_qty, filled_qty)
            capital_change_pct = calc_capital_change_pct(entry_price, filled_price) or 0.0
            slippage_pct = calc_slippage_pct("SELL", _to_float(row.get("requested_price"), filled_price), filled_price)
            insert_order_log(
                db_path=db_path,
                order_id=order_id,
                event_type="SELL_FILLED",
                filled_qty=filled_qty,
                avg_fill_price=filled_price,
                slippage_pct=slippage_pct,
                capital_change_pct=capital_change_pct,
                realized_pnl_amount=leg_pnl_amount,
                note="reconciled_delayed_fill_delta",
                event_at=(polled.get("filled_at") or polled.get("submitted_at")),
            )

            remaining_qty = max(trade_qty - filled_qty, 0.0)
            accumulated_realized = _to_float(trade.get("gross_pnl_amount"), 0.0)
            total_realized = accumulated_realized + leg_pnl_amount
            invested_amount = _to_float(trade.get("invested_amount"), 0.0)

            if remaining_qty <= 1e-9:
                total_capital_change_pct = (total_realized / invested_amount * 100.0) if invested_amount > 0 else capital_change_pct
                outcome_tier = classify_outcome_tier(total_capital_change_pct)
                close_trade(
                    db_path=db_path,
                    trade_id=int(trade["id"]),
                    exit_order_id=order_id,
                    exit_price=filled_price,
                    gross_pnl_amount=total_realized,
                    gross_pnl_pct=total_capital_change_pct,
                    outcome_tier=outcome_tier,
                    exit_slippage_pct=slippage_pct,
                    closed_at=(polled.get("filled_at") or polled.get("submitted_at")),
                    close_reason="delayed_sell_fill_reconciled",
                )
                details.append({"order_id": order_id, "symbol": symbol, "side": side, "action": "SELL_RECONCILED"})
                _log(log_step_fn, f"SyncFlow: 遅延約定反映 SELL {symbol} delta_qty={filled_qty}")
            else:
                reduce_open_trade_after_partial_exit(
                    db_path=db_path,
                    trade_id=int(trade["id"]),
                    remaining_qty=remaining_qty,
                    realized_pnl_delta=leg_pnl_amount,
                )
                details.append({"order_id": order_id, "symbol": symbol, "side": side, "action": "SELL_PARTIAL_RECONCILED"})
                _log(
                    log_step_fn,
                    f"SyncFlow: 遅延部分約定反映 SELL {symbol} delta_qty={filled_qty} remain={remaining_qty}",
                )

            filled_resolved += 1

    _log(log_step_fn, f"SyncFlow: 未確定注文の照合完了 checked={checked} resolved={filled_resolved}")
    return {"checked": checked, "filled_resolved": filled_resolved, "details": details}


def run_sync_flow(db_path: Any, alpaca_client: Any, log_step_fn: Any = None) -> dict[str, Any]:
    _log(log_step_fn, "SyncFlow: 開始")
    if not getattr(alpaca_client, "enabled", False):
        _log(log_step_fn, "SyncFlow: Alpaca無効のためスキップ")
        return {"note": "alpaca_disabled", "imported": 0, "synced": 0, "cancelled": 0}

    reconcile_result = _reconcile_unresolved_orders(db_path, alpaca_client, log_step_fn)
    alpaca_positions = [
        p for p in alpaca_client.get_positions() if _to_float(p.get("qty"), 0.0) > 0.0
    ]
    open_trades = fetch_open_trades(db_path)

    alpaca_map: dict[str, dict[str, Any]] = {
        str(p.get("symbol", "")).upper(): p for p in alpaca_positions
    }
    db_symbols = {str(t.get("symbol", "")).upper() for t in open_trades}

    imported = 0
    synced = 0
    cancelled = 0
    recovered_manual_close = 0
    recovered_manual_partial = 0
    details: list[dict[str, Any]] = list(reconcile_result.get("details", []))

    # AlpacaにあるがDBにないポジションは取り込み
    for symbol, pos in alpaca_map.items():
        if not symbol:
            continue
        existing_trade = fetch_open_trade_by_symbol(db_path, symbol)
        if existing_trade:
            alpaca_qty = _to_float(pos.get("qty"))
            alpaca_entry = _to_float(pos.get("avg_entry_price")) or _to_float(pos.get("current_price"))
            db_qty = _to_float(existing_trade.get("qty"))
            db_entry = _to_float(existing_trade.get("entry_price"))
            qty_diff = abs(alpaca_qty - db_qty)
            entry_diff = abs(alpaca_entry - db_entry)
            if qty_diff > 1e-6 or (alpaca_entry > 0 and entry_diff > 1e-6):
                sync_open_trade_position(
                    db_path=db_path,
                    trade_id=int(existing_trade["id"]),
                    qty=alpaca_qty,
                    entry_price=alpaca_entry if alpaca_entry > 0 else db_entry,
                )
                synced += 1
                details.append(
                    {
                        "symbol": symbol,
                        "action": "SYNCED",
                        "db_qty_before": db_qty,
                        "alpaca_qty": alpaca_qty,
                    }
                )
                _log(log_step_fn, f"SyncFlow: 数量同期 {symbol} db_qty={db_qty} -> alpaca_qty={alpaca_qty}")
            continue

        qty = _to_float(pos.get("qty"))
        entry_price = _to_float(pos.get("avg_entry_price")) or _to_float(pos.get("current_price"))
        if qty <= 0 or entry_price <= 0:
            continue

        signal_id = insert_signal(
            db_path=db_path,
            flow_type="ENTRY",
            symbol=symbol,
            decision="BUY",
            decision_score=1.0,
            indicator_snapshot={
                "source": "SYNC",
                "qty": qty,
                "avg_entry_price": entry_price,
            },
            claude_snapshot={"source": "SYNC"},
            reason="sync_import_from_alpaca_position",
        )
        order_id = insert_order(
            db_path=db_path,
            signal_id=signal_id,
            alpaca_order_id=None,
            client_order_id=None,
            symbol=symbol,
            side="BUY",
            qty=qty,
            status="filled",
            requested_price=entry_price,
            raw_request={"source": "SYNC"},
            raw_response={"source": "SYNC"},
        )
        insert_order_log(
            db_path=db_path,
            order_id=order_id,
            event_type="BUY_FILLED",
            filled_qty=qty,
            avg_fill_price=entry_price,
            note="synced_from_alpaca_position",
        )
        open_trade(
            db_path=db_path,
            symbol=symbol,
            entry_order_id=order_id,
            qty=qty,
            entry_price=entry_price,
            invested_amount=qty * entry_price,
        )
        imported += 1
        details.append({"symbol": symbol, "action": "IMPORTED"})
        _log(log_step_fn, f"SyncFlow: 取り込み {symbol} qty={qty}")

    # DBにあるがAlpacaにないポジションを補正
    for trade in open_trades:
        symbol = str(trade.get("symbol", "")).upper()
        if symbol in alpaca_map:
            continue

        # 手動売却を履歴から復元できる場合は損益付きでクローズ
        sell_summary = alpaca_client.summarize_filled_sells_since(symbol, opened_at=trade.get("opened_at"))
        if sell_summary:
            alpaca_sell_id = str(sell_summary.get("latest_order_id") or "").strip()
            existing_order = fetch_order_by_alpaca_order_id(db_path, alpaca_sell_id) if alpaca_sell_id else None

            opened_at = _parse_iso(trade.get("opened_at"))
            filled_at = _parse_iso(sell_summary.get("latest_filled_at"))
            if (opened_at is None) or (filled_at is None) or (filled_at >= opened_at):
                filled_price = _to_float(sell_summary.get("weighted_avg_price"))
                if filled_price > 0:
                    trade_qty = _to_float(trade.get("qty"))
                    entry_price = _to_float(trade.get("entry_price"))
                    manual_filled_qty = _to_float(sell_summary.get("total_filled_qty"), 0.0)
                    if manual_filled_qty <= 0:
                        manual_filled_qty = trade_qty
                    manual_filled_qty = min(manual_filled_qty, trade_qty)
                    if manual_filled_qty <= 0:
                        continue

                    capital_change_pct = calc_capital_change_pct(entry_price, filled_price) or 0.0
                    leg_pnl_amount = (filled_price - entry_price) * manual_filled_qty
                    outcome_tier = classify_outcome_tier(capital_change_pct)

                    if existing_order:
                        order_id = int(existing_order["id"])
                    else:
                        signal_id = insert_signal(
                            db_path=db_path,
                            flow_type="EXIT",
                            symbol=symbol,
                            decision="SELL",
                            decision_score=1.0,
                            indicator_snapshot={"source": "SYNC_MANUAL_CLOSE", "entry_price": entry_price},
                            claude_snapshot={},
                            reason="sync_manual_close_recovered",
                        )
                        order_id = insert_order(
                            db_path=db_path,
                            signal_id=signal_id,
                            alpaca_order_id=alpaca_sell_id or None,
                            client_order_id=None,
                            symbol=symbol,
                            side="SELL",
                            qty=manual_filled_qty,
                            status="filled",
                            requested_price=filled_price,
                            raw_request={"source": "SYNC_MANUAL_CLOSE"},
                            raw_response=sell_summary.get("latest_raw", {}),
                        )

                    has_sell_log = has_order_log_event(
                        db_path=db_path,
                        order_id=order_id,
                        event_type="SELL_FILLED",
                    )
                    if not has_sell_log:
                        insert_order_log(
                            db_path=db_path,
                            order_id=order_id,
                            event_type="SELL_FILLED",
                            filled_qty=manual_filled_qty,
                            avg_fill_price=filled_price,
                            slippage_pct=calc_slippage_pct("SELL", filled_price, filled_price),
                            capital_change_pct=capital_change_pct,
                            realized_pnl_amount=leg_pnl_amount,
                            outcome_tier=outcome_tier,
                            note="manual_close_recovered_from_alpaca",
                            event_at=sell_summary.get("latest_filled_at"),
                        )
                    accumulated_realized = _to_float(trade.get("gross_pnl_amount"), 0.0)
                    invested_amount = _to_float(trade.get("invested_amount"), 0.0)
                    total_realized = accumulated_realized + leg_pnl_amount
                    remaining_qty = max(trade_qty - manual_filled_qty, 0.0)
                    if remaining_qty <= 1e-9:
                        total_capital_change_pct = (
                            (total_realized / invested_amount * 100.0)
                            if invested_amount > 0
                            else capital_change_pct
                        )
                        total_outcome_tier = classify_outcome_tier(total_capital_change_pct)
                        close_trade(
                            db_path=db_path,
                            trade_id=int(trade["id"]),
                            exit_order_id=order_id,
                            exit_price=filled_price,
                            gross_pnl_amount=total_realized,
                            gross_pnl_pct=total_capital_change_pct,
                            outcome_tier=total_outcome_tier,
                            exit_slippage_pct=0.0,
                            closed_at=sell_summary.get("latest_filled_at"),
                            close_reason="manual_close_recovered",
                        )
                        recovered_manual_close += 1
                        details.append({"symbol": symbol, "action": "MANUAL_CLOSE_RECOVERED"})
                        _log(log_step_fn, f"SyncFlow: 手動クローズ復元 {symbol} pnl_pct={round(capital_change_pct, 4)}")
                    else:
                        reduce_open_trade_after_partial_exit(
                            db_path=db_path,
                            trade_id=int(trade["id"]),
                            remaining_qty=remaining_qty,
                            realized_pnl_delta=leg_pnl_amount,
                        )
                        recovered_manual_partial += 1
                        details.append(
                            {
                                "symbol": symbol,
                                "action": "MANUAL_CLOSE_PARTIAL_RECOVERED",
                                "filled_qty": manual_filled_qty,
                                "remaining_qty": remaining_qty,
                            }
                        )
                        _log(
                            log_step_fn,
                            f"SyncFlow: 手動部分クローズ復元 {symbol} filled={manual_filled_qty} remain={remaining_qty}",
                        )
                    continue

        # 復元できない場合のみCANCELLED
        cancel_open_trade(
            db_path=db_path,
            trade_id=int(trade["id"]),
            reason="manual_or_external_close_detected",
        )
        cancelled += 1
        details.append({"symbol": symbol, "action": "CANCELLED"})
        _log(log_step_fn, f"SyncFlow: クローズ補正 {symbol} -> CANCELLED")

    _log(
        log_step_fn,
        "SyncFlow完了: "
        f"imported={imported} synced={synced} cancelled={cancelled} "
        f"recovered_manual_close={recovered_manual_close} "
        f"recovered_manual_partial={recovered_manual_partial} "
        f"reconciled_filled={int(reconcile_result.get('filled_resolved', 0))} "
        f"alpaca_positions={len(alpaca_positions)} db_open_before={len(db_symbols)}",
    )
    return {
        "note": "sync_completed",
        "imported": imported,
        "synced": synced,
        "cancelled": cancelled,
        "recovered_manual_close": recovered_manual_close,
        "recovered_manual_partial": recovered_manual_partial,
        "reconciled_orders_checked": int(reconcile_result.get("checked", 0)),
        "reconciled_orders_filled": int(reconcile_result.get("filled_resolved", 0)),
        "alpaca_positions": len(alpaca_positions),
        "db_open_before": len(db_symbols),
        "details": details,
    }
