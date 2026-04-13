from __future__ import annotations

from typing import Any

from app_settings import DAILY_LOSS_LIMIT, ORDER_EVENT_TYPES, classify_outcome_tier
from logic.exit_logic import calc_capital_change_pct, calc_slippage_pct, decide_exit
from repository.order_repository import (
    close_trade,
    fetch_open_trade_by_symbol,
    fetch_open_trades,
    insert_order,
    insert_order_log,
    reduce_open_trade_after_partial_exit,
    update_trade_exit_state,
)
from repository.position_repository import insert_position_snapshot
from repository.signal_repository import insert_signal


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _log(log_step_fn: Any, message: str) -> None:
    if callable(log_step_fn):
        log_step_fn(message)


def _build_simulated_positions(open_trades: list[dict[str, Any]], alpaca_client: Any) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for trade in open_trades:
        symbol = str(trade.get("symbol", "")).upper()
        qty = _to_float(trade.get("qty"))
        entry_price = _to_float(trade.get("entry_price"))
        current_price = alpaca_client.get_latest_price(symbol)
        positions.append(
            {
                "symbol": symbol,
                "qty": qty,
                "avg_entry_price": entry_price,
                "current_price": current_price,
                "market_value": current_price * qty,
                "unrealized_pl": (current_price - entry_price) * qty,
                "source": "SIMULATION",
            }
        )
    return positions


def run_exit_flow(
    db_path: Any,
    alpaca_client: Any,
    log_step_fn: Any = None,
    allow_order_submission: bool = True,
) -> dict[str, Any]:
    _log(log_step_fn, "ExitFlow: 開始")
    open_trades = fetch_open_trades(db_path)
    if not open_trades:
        _log(log_step_fn, "ExitFlow: open trade が存在しないためスキップ")
        return {"checked": 0, "sold": 0, "note": "open_trade_not_found"}

    positions = alpaca_client.get_positions()
    if not positions and not getattr(alpaca_client, "enabled", False):
        positions = _build_simulated_positions(open_trades, alpaca_client)
    if not positions:
        _log(log_step_fn, "ExitFlow: Alpaca上の保有ポジションが存在しないためスキップ")
        return {"checked": 0, "sold": 0, "note": "alpaca_position_not_found"}
    _log(log_step_fn, f"ExitFlow: positions={len(positions)} open_trades={len(open_trades)}")

    open_sell_symbols: set[str] = set()
    if allow_order_submission:
        for row in alpaca_client.get_open_orders():
            symbol = str(row.get("symbol", "")).upper()
            side = str(row.get("side", "")).lower()
            if symbol and side == "sell":
                open_sell_symbols.add(symbol)
        if open_sell_symbols:
            _log(log_step_fn, f"ExitFlow: 未約定SELL注文を検出 symbols={len(open_sell_symbols)}")

    sold_count = 0
    decision_sell_count = 0
    checked = 0
    details: list[dict[str, Any]] = []

    for pos in positions:
        symbol = str(pos.get("symbol", "")).upper()
        qty = _to_float(pos.get("qty"))
        if not symbol or qty <= 0:
            continue

        checked += 1
        current_price = _to_float(pos.get("current_price")) or alpaca_client.get_latest_price(symbol)
        avg_entry = _to_float(pos.get("avg_entry_price"))

        insert_position_snapshot(
            db_path=db_path,
            symbol=symbol,
            qty=qty,
            avg_entry_price=avg_entry,
            market_price=current_price,
            market_value=_to_float(pos.get("market_value")),
            unrealized_pl=_to_float(pos.get("unrealized_pl")),
            source=str(pos.get("source", "ALPACA")),
        )

        trade = fetch_open_trade_by_symbol(db_path, symbol)
        if not trade:
            details.append({"symbol": symbol, "action": "HOLD", "reason": "db_open_trade_missing"})
            _log(log_step_fn, f"ExitFlow: {symbol} DB上のopen tradeなし")
            continue

        bars = alpaca_client.get_daily_bars(symbol, limit=80)
        observed_high_price = 0.0
        if bool(int(_to_float(trade.get("take_profit_done"), 0.0))):
            observed_high_price = _to_float(alpaca_client.get_latest_minute_high(symbol), 0.0)
        decision = decide_exit(
            entry_price=_to_float(trade.get("entry_price")),
            current_price=current_price,
            current_qty=qty,
            bars=bars,
            take_profit_done=bool(int(_to_float(trade.get("take_profit_done"), 0.0))),
            trailing_high_price=_to_float(trade.get("trailing_high_price"), current_price),
            observed_high_price=observed_high_price,
        )

        signal_id = insert_signal(
            db_path=db_path,
            flow_type="EXIT",
            symbol=symbol,
            decision=decision.action,
            decision_score=None,
            indicator_snapshot={
                "current_price": current_price,
                "position_qty": qty,
                "entry_price": _to_float(trade.get("entry_price")),
            },
            claude_snapshot={},
            reason=decision.reason,
        )

        if decision.action != "SELL":
            if (
                decision.set_take_profit_done is not None
                or decision.set_trailing_high_price is not None
            ):
                update_trade_exit_state(
                    db_path=db_path,
                    trade_id=int(trade["id"]),
                    take_profit_done=decision.set_take_profit_done,
                    trailing_high_price=decision.set_trailing_high_price,
                )
            details.append({"symbol": symbol, "action": "HOLD", "reason": decision.reason})
            _log(log_step_fn, f"ExitFlow: {symbol} HOLD reason={decision.reason}")
            continue

        if not allow_order_submission:
            decision_sell_count += 1
            details.append(
                {
                    "symbol": symbol,
                    "action": "SELL_DECISION_ONLY",
                    "reason": decision.reason,
                    "qty": decision.target_qty,
                }
            )
            _log(
                log_step_fn,
                f"ExitFlow: SELL判定のみ {symbol} qty={decision.target_qty} reason={decision.reason} (注文なし)",
            )
            continue

        if symbol in open_sell_symbols:
            details.append({"symbol": symbol, "action": "HOLD", "reason": "open_sell_order_exists"})
            _log(log_step_fn, f"ExitFlow: {symbol} 既存の未約定SELL注文ありのためスキップ")
            continue

        _log(log_step_fn, f"ExitFlow: SELL発注 {symbol} qty={decision.target_qty} reason={decision.reason}")
        order_response = alpaca_client.submit_market_order(
            symbol=symbol,
            qty=decision.target_qty,
            side="sell",
        )
        order_response = alpaca_client.resolve_order_final_state(order_response)
        order_id = insert_order(
            db_path=db_path,
            signal_id=signal_id,
            alpaca_order_id=order_response.get("id"),
            client_order_id=None,
            symbol=symbol,
            side="SELL",
            qty=decision.target_qty,
            status=str(order_response.get("status", "unknown")),
            requested_price=decision.expected_price,
            raw_request={"symbol": symbol, "qty": decision.target_qty, "side": "sell"},
            raw_response=order_response,
        )

        if not alpaca_client.is_effectively_filled(order_response):
            details.append({"symbol": symbol, "action": "SELL", "reason": "not_filled"})
            _log(log_step_fn, f"ExitFlow: SELL未約定 {symbol}")
            continue

        filled_qty = alpaca_client.extract_filled_qty(order_response)
        if filled_qty <= 0:
            details.append({"symbol": symbol, "action": "SELL", "reason": "filled_qty_zero"})
            _log(log_step_fn, f"ExitFlow: SELL未約定(数量0) {symbol}")
            continue
        filled_qty = min(filled_qty, qty)

        filled_price = _to_float(order_response.get("filled_avg_price"), decision.expected_price)
        slippage_pct = calc_slippage_pct("SELL", decision.expected_price, filled_price)
        entry_price = _to_float(trade.get("entry_price"))
        leg_capital_change_pct = calc_capital_change_pct(entry_price, filled_price)
        if leg_capital_change_pct is None:
            leg_capital_change_pct = 0.0
        gross_pnl_amount = (filled_price - entry_price) * filled_qty

        if "SELL_FILLED" in ORDER_EVENT_TYPES:
            insert_order_log(
                db_path=db_path,
                order_id=order_id,
                event_type="SELL_FILLED",
                filled_qty=filled_qty,
                avg_fill_price=filled_price,
                slippage_pct=slippage_pct,
                capital_change_pct=leg_capital_change_pct,
                realized_pnl_amount=gross_pnl_amount,
                note=decision.reason,
            )

        trade_qty = _to_float(trade.get("qty"))
        remaining_qty = max(trade_qty - filled_qty, 0.0)
        accumulated_realized = _to_float(trade.get("gross_pnl_amount"), 0.0)
        total_realized = accumulated_realized + gross_pnl_amount
        invested_amount = _to_float(trade.get("invested_amount"), 0.0)

        if remaining_qty <= 1e-9:
            total_capital_change_pct = (
                (total_realized / invested_amount * 100.0) if invested_amount > 0 else leg_capital_change_pct
            )
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
            )
            sold_count += 1
            _log(
                log_step_fn,
                f"ExitFlow: SELL約定 {symbol} req_qty={decision.target_qty} filled_qty={filled_qty} pnl_pct={round(total_capital_change_pct, 4)} tier={outcome_tier}",
            )
            details.append(
                {
                    "symbol": symbol,
                    "action": "SELL",
                    "reason": decision.reason,
                    "requested_qty": decision.target_qty,
                    "filled_qty": filled_qty,
                    "capital_change_pct": round(total_capital_change_pct, 4),
                    "outcome_tier": outcome_tier,
                }
            )
        else:
            reduce_open_trade_after_partial_exit(
                db_path=db_path,
                trade_id=int(trade["id"]),
                remaining_qty=remaining_qty,
                realized_pnl_delta=gross_pnl_amount,
            )
            if (
                decision.set_take_profit_done is not None
                or decision.set_trailing_high_price is not None
            ):
                update_trade_exit_state(
                    db_path=db_path,
                    trade_id=int(trade["id"]),
                    take_profit_done=decision.set_take_profit_done,
                    trailing_high_price=decision.set_trailing_high_price,
                )
            sold_count += 1
            _log(
                log_step_fn,
                f"ExitFlow: SELL部分約定 {symbol} req_qty={decision.target_qty} filled_qty={filled_qty} remain_qty={round(remaining_qty, 6)}",
            )
            details.append(
                {
                    "symbol": symbol,
                    "action": "SELL_PARTIAL",
                    "reason": decision.reason,
                    "requested_qty": decision.target_qty,
                    "filled_qty": filled_qty,
                    "remaining_qty": remaining_qty,
                    "capital_change_pct": round(leg_capital_change_pct, 4),
                }
            )

    # 当日損失率はmain側のentry実行可否で使用するため、ここでは計算のみ返す
    return {
        "checked": checked,
        "sold": sold_count,
        "decision_sell_count": decision_sell_count,
        "details": details,
        "daily_loss_limit": DAILY_LOSS_LIMIT,
    }
