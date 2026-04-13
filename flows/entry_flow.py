from __future__ import annotations

import json
from typing import Any

from app_settings import (
    ALPACA_DATA_FEED,
    DAILY_LOSS_LIMIT,
    ENABLE_SCORE_WEIGHTED_SIZING,
    ENTRY_ORDER_MIN_SCORE,
    FINVIZ_CANDIDATE_LIMIT,
    MAX_NEW_ORDERS_PER_RUN,
    MAX_POSITIONS,
    MIN_AVG_VOLUME,
    ORDER_EVENT_TYPES,
    SCORE_WEIGHT_FLOOR,
    TEST_ENTRY_RELAX_MODE,
    TEST_ENTRY_ORDER_MIN_SCORE,
    TEST_MAX_NEW_ORDERS_PER_RUN_OVERRIDE,
    USE_PAPER_ACCOUNT,
    get_effective_min_avg_volume,
)
from logic.entry_logic import (
    build_entry_metrics,
    calculate_entry_qty,
    evaluate_entry_candidate,
    pick_top_candidates,
)
from logic.exit_logic import calc_slippage_pct
from repository.order_repository import (
    fetch_open_trade_by_symbol,
    fetch_open_trades,
    fetch_today_realized_pnl_amount,
    insert_order,
    insert_order_log,
    open_trade,
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


def run_entry_flow(
    db_path: Any,
    alpaca_client: Any,
    finviz_scraper: Any,
    claude_client: Any,
    log_step_fn: Any = None,
    allow_order_submission: bool = True,
) -> dict[str, Any]:
    _log(log_step_fn, "EntryFlow: 開始")
    account = alpaca_client.get_account()
    equity = max(_to_float(account.get("equity"), 0.0), 1.0)
    cash = max(_to_float(account.get("cash"), 0.0), 0.0)

    realized_today = fetch_today_realized_pnl_amount(db_path)
    realized_loss_rate = (realized_today / equity) if equity > 0 else 0.0
    if realized_loss_rate <= -DAILY_LOSS_LIMIT:
        _log(log_step_fn, "EntryFlow: 日次損失上限に到達、買いフローを停止")
        return {
            "note": "daily_loss_limit_reached",
            "realized_today": realized_today,
            "equity": equity,
        }

    live_positions = alpaca_client.get_positions()
    if getattr(alpaca_client, "enabled", False):
        current_position_count = len([p for p in live_positions if _to_float(p.get("qty")) > 0])
    else:
        current_position_count = len(fetch_open_trades(db_path))

    if MAX_POSITIONS > 0 and current_position_count >= MAX_POSITIONS:
        _log(
            log_step_fn,
            f"EntryFlow: 保有数が上限に到達 ({current_position_count}/{MAX_POSITIONS})",
        )
        return {
            "note": "max_positions_reached",
            "current_position_count": current_position_count,
        }

    max_new_positions: int | None = None
    if MAX_POSITIONS > 0:
        max_new_positions = max(0, MAX_POSITIONS - current_position_count)

    open_buy_symbols: set[str] = set()
    if allow_order_submission:
        for row in alpaca_client.get_open_orders():
            symbol = str(row.get("symbol", "")).upper()
            side = str(row.get("side", "")).lower()
            if symbol and side == "buy":
                open_buy_symbols.add(symbol)
        if open_buy_symbols:
            _log(log_step_fn, f"EntryFlow: 未約定BUY注文を検出 symbols={len(open_buy_symbols)}")

    _log(log_step_fn, "Finviz処理開始")
    candidates = finviz_scraper.fetch_candidates(limit=FINVIZ_CANDIDATE_LIMIT)
    _log(log_step_fn, f"Finviz処理完了: candidates={len(candidates)}")
    if not candidates:
        return {"note": "no_candidates"}

    _log(log_step_fn, "EntryFlow: 候補銘柄の市場データ取得/指標計算 開始")
    metrics_rows: list[dict[str, Any]] = []
    metrics_by_symbol: dict[str, Any] = {}
    data_skip_counts: dict[str, int] = {
        "bars_unavailable": 0,
        "bars_insufficient": 0,
        "metrics_invalid": 0,
    }
    for c in candidates:
        symbol = str(c.get("ticker", "")).upper()
        if not symbol:
            continue
        # ナンピン禁止: 既存オープントレード銘柄は新規買いしない
        if fetch_open_trade_by_symbol(db_path, symbol):
            continue

        bars = alpaca_client.get_daily_bars(symbol, limit=260)
        if not bars:
            data_skip_counts["bars_unavailable"] += 1
            continue
        if len(bars) < 202:
            data_skip_counts["bars_insufficient"] += 1
            continue
        metrics = build_entry_metrics(symbol, bars)
        if metrics is None:
            data_skip_counts["metrics_invalid"] += 1
            continue

        metrics_rows.append(
            {
                "ticker": metrics.ticker,
                "price": metrics.price,
                "avg_volume": metrics.avg_volume,
                "volume": metrics.volume,
                "sma50": metrics.sma50,
                "sma200": metrics.sma200,
                "rsi": metrics.rsi_now,
                "rsi_prev": metrics.rsi_prev,
                "atr": metrics.atr,
                "atr_pct": metrics.atr_pct,
                "trend_up": metrics.trend_up,
                "breakout": metrics.breakout,
                "price_to_sma50": metrics.price_to_sma50,
            }
        )
        metrics_by_symbol[metrics.ticker] = metrics
    _log(
        log_step_fn,
        f"EntryFlow: 候補銘柄の市場データ取得/指標計算 完了 metrics={len(metrics_rows)}",
    )
    skipped_total = sum(data_skip_counts.values())
    if skipped_total > 0:
        _log(
            log_step_fn,
            "EntryFlow: 市場データ不足で除外 "
            f"counts={json.dumps(data_skip_counts, ensure_ascii=False)}",
        )

    if not metrics_rows:
        _log(log_step_fn, "EntryFlow: 指標計算可能な候補が0件")
        return {
            "note": "metrics_unavailable",
            "data_skip_counts": data_skip_counts,
        }

    effective_min_avg_volume = float(get_effective_min_avg_volume(ALPACA_DATA_FEED))
    avg_vols = [float(r.get("avg_volume", 0.0)) for r in metrics_rows]
    if avg_vols:
        min_v = min(avg_vols)
        max_v = max(avg_vols)
        med_v = sorted(avg_vols)[len(avg_vols) // 2]
        _log(
            log_step_fn,
            "EntryFlow: avg_volume統計 "
            f"min={int(min_v)} median={int(med_v)} max={int(max_v)} "
            f"threshold_effective={int(effective_min_avg_volume)} threshold_base={int(MIN_AVG_VOLUME)} feed={ALPACA_DATA_FEED}",
        )
        if ALPACA_DATA_FEED == "iex" and MIN_AVG_VOLUME >= 500_000:
            _log(
                log_step_fn,
                "EntryFlow: 注意 IEX出来高はSIPより小さく、thresholdが高すぎると全却下されやすい",
            )

    _log(log_step_fn, f"Claudeスコアリング開始: target={len(metrics_rows)}")
    claude_scores = claude_client.score_candidates(metrics_rows)
    _log(log_step_fn, f"Claudeスコアリング完了: received={len(claude_scores)}")
    claude_map = {str(r.get("ticker", "")).upper(): r for r in claude_scores}

    evaluated: list[tuple[Any, dict[str, Any], bool, float, str]] = []
    for m_row in metrics_rows:
        metrics = metrics_by_symbol.get(str(m_row.get("ticker", "")).upper())
        if metrics is None:
            continue
        c_row = claude_map.get(metrics.ticker, {})
        ok, score, reason = evaluate_entry_candidate(
            metrics,
            c_row,
            effective_min_avg_volume=effective_min_avg_volume,
        )
        evaluated.append((metrics, c_row, ok, score, reason))

    rejected_reason_counts: dict[str, int] = {}
    for _, _, ok, _, reason in evaluated:
        if ok:
            continue
        rejected_reason_counts[reason] = rejected_reason_counts.get(reason, 0) + 1

    picks = pick_top_candidates(evaluated, max_new_positions=max_new_positions)
    max_new_orders_per_run = MAX_NEW_ORDERS_PER_RUN
    if USE_PAPER_ACCOUNT and TEST_ENTRY_RELAX_MODE and TEST_MAX_NEW_ORDERS_PER_RUN_OVERRIDE > 0:
        max_new_orders_per_run = TEST_MAX_NEW_ORDERS_PER_RUN_OVERRIDE
    if max_new_orders_per_run > 0:
        picks = picks[:max_new_orders_per_run]
    if not picks:
        # 候補はあったが全て条件未達。代表シグナルのみ保存。
        rejected_count = 0
        for metrics, c_row, ok, score, reason in evaluated:
            if ok:
                continue
            insert_signal(
                db_path=db_path,
                flow_type="ENTRY",
                symbol=metrics.ticker,
                decision="HOLD",
                decision_score=score,
                indicator_snapshot={
                    "price": metrics.price,
                    "rsi": metrics.rsi_now,
                    "sma50": metrics.sma50,
                    "sma200": metrics.sma200,
                    "avg_volume": metrics.avg_volume,
                },
                claude_snapshot=c_row,
                reason=reason,
            )
            rejected_count += 1
        _log(
            log_step_fn,
            f"EntryFlow: 全候補却下 rejected={rejected_count} reasons={json.dumps(rejected_reason_counts, ensure_ascii=False)}",
        )
        return {
            "note": "all_candidates_rejected",
            "rejected_count": rejected_count,
            "candidate_count": len(candidates),
            "evaluated_count": len(evaluated),
            "rejected_reason_counts": rejected_reason_counts,
            "data_skip_counts": data_skip_counts,
            "effective_min_avg_volume": int(effective_min_avg_volume),
        }

    _log(log_step_fn, f"EntryFlow: 発注候補 {len(picks)} 件")
    if ENABLE_SCORE_WEIGHTED_SIZING:
        _log(log_step_fn, "EntryFlow: 資金配分モード=score_weighted")
    else:
        _log(log_step_fn, "EntryFlow: 資金配分モード=equal")
    bought = 0
    decision_buy_count = 0
    details: list[dict[str, Any]] = []
    min_order_score = ENTRY_ORDER_MIN_SCORE
    if USE_PAPER_ACCOUNT and TEST_ENTRY_RELAX_MODE:
        min_order_score = TEST_ENTRY_ORDER_MIN_SCORE
    for idx, (metrics, c_row, score, reason) in enumerate(picks):
        remaining_picks = picks[idx:]
        score_weight = max(float(score), SCORE_WEIGHT_FLOOR)
        score_weight_sum = sum(max(float(p[2]), SCORE_WEIGHT_FLOOR) for p in remaining_picks)
        qty = calculate_entry_qty(
            price=metrics.price,
            cash=cash,
            equity=equity,
            slots_remaining=max(1, len(picks) - bought),
            score_weight=score_weight,
            score_weight_sum=score_weight_sum,
        )
        signal_id = insert_signal(
            db_path=db_path,
            flow_type="ENTRY",
            symbol=metrics.ticker,
            decision="BUY" if qty > 0 else "HOLD",
            decision_score=score,
            indicator_snapshot={
                "price": metrics.price,
                "rsi": metrics.rsi_now,
                "sma50": metrics.sma50,
                "sma200": metrics.sma200,
                "avg_volume": metrics.avg_volume,
                "volume": metrics.volume,
                "atr_pct": metrics.atr_pct,
                "breakout": metrics.breakout,
            },
            claude_snapshot=c_row,
            reason=reason if qty > 0 else "qty_calculation_failed",
        )
        if score < min_order_score:
            _log(log_step_fn, f"EntryFlow: {metrics.ticker} score={round(score,4)} が発注閾値未満のため見送り")
            details.append(
                {
                    "symbol": metrics.ticker,
                    "action": "HOLD",
                    "reason": "order_score_too_low",
                    "score": round(score, 4),
                    "min_order_score": round(float(min_order_score), 4),
                }
            )
            continue
        if qty <= 0:
            _log(log_step_fn, f"EntryFlow: {metrics.ticker} qty=0 のため見送り")
            details.append({"symbol": metrics.ticker, "action": "HOLD", "reason": "qty_zero"})
            continue

        if allow_order_submission and metrics.ticker in open_buy_symbols:
            _log(log_step_fn, f"EntryFlow: {metrics.ticker} 既存の未約定BUY注文ありのためスキップ")
            details.append(
                {
                    "symbol": metrics.ticker,
                    "action": "HOLD",
                    "reason": "open_buy_order_exists",
                }
            )
            continue

        if not allow_order_submission:
            decision_buy_count += 1
            _log(log_step_fn, f"EntryFlow: BUY判定のみ {metrics.ticker} qty={qty} (注文なし)")
            details.append(
                {
                    "symbol": metrics.ticker,
                    "action": "BUY_DECISION_ONLY",
                    "qty": qty,
                    "score": round(score, 4),
                }
            )
            continue

        _log(log_step_fn, f"EntryFlow: BUY発注 {metrics.ticker} qty={qty}")
        order_response = alpaca_client.submit_market_order(
            symbol=metrics.ticker,
            qty=qty,
            side="buy",
        )
        order_response = alpaca_client.resolve_order_final_state(order_response)
        status = str(order_response.get("status", "unknown"))
        order_id = insert_order(
            db_path=db_path,
            signal_id=signal_id,
            alpaca_order_id=order_response.get("id"),
            client_order_id=None,
            symbol=metrics.ticker,
            side="BUY",
            qty=qty,
            status=status,
            requested_price=metrics.price,
            raw_request={"symbol": metrics.ticker, "qty": qty, "side": "buy"},
            raw_response=order_response,
        )

        if not alpaca_client.is_effectively_filled(order_response):
            if "BUY_NOT_FILLED" in ORDER_EVENT_TYPES:
                insert_order_log(
                    db_path=db_path,
                    order_id=order_id,
                    event_type="BUY_NOT_FILLED",
                    note=status,
                )
            details.append({"symbol": metrics.ticker, "action": "BUY", "status": "NOT_FILLED"})
            _log(log_step_fn, f"EntryFlow: BUY未約定 {metrics.ticker}")
            continue

        filled_qty = alpaca_client.extract_filled_qty(order_response)
        if filled_qty <= 0:
            if "BUY_NOT_FILLED" in ORDER_EVENT_TYPES:
                insert_order_log(
                    db_path=db_path,
                    order_id=order_id,
                    event_type="BUY_NOT_FILLED",
                    note=f"{status}:filled_qty_zero",
                )
            details.append({"symbol": metrics.ticker, "action": "BUY", "status": "NOT_FILLED"})
            _log(log_step_fn, f"EntryFlow: BUY未約定(数量0) {metrics.ticker}")
            continue
        filled_qty = min(filled_qty, qty)

        filled_price = _to_float(order_response.get("filled_avg_price"), metrics.price)
        slippage_pct = calc_slippage_pct("BUY", metrics.price, filled_price)

        if "BUY_FILLED" in ORDER_EVENT_TYPES:
            insert_order_log(
                db_path=db_path,
                order_id=order_id,
                event_type="BUY_FILLED",
                filled_qty=filled_qty,
                avg_fill_price=filled_price,
                slippage_pct=slippage_pct,
                note="entry_filled",
            )

        open_trade(
            db_path=db_path,
            symbol=metrics.ticker,
            entry_order_id=order_id,
            qty=filled_qty,
            entry_price=filled_price,
            invested_amount=filled_price * filled_qty,
            entry_slippage_pct=slippage_pct,
        )

        cash -= filled_price * filled_qty
        bought += 1
        _log(
            log_step_fn,
            f"EntryFlow: BUY約定 {metrics.ticker} req_qty={qty} filled_qty={filled_qty} price={round(filled_price, 4)}",
        )
        details.append(
            {
                "symbol": metrics.ticker,
                "action": "BUY",
                "qty": filled_qty,
                "requested_qty": qty,
                "entry_price": round(filled_price, 4),
                "score": round(score, 4),
            }
        )

    return {
        "note": "entry_completed",
        "bought": bought,
        "decision_buy_count": decision_buy_count,
        "details": details,
        "slots_remaining_after": max(0, len(picks) - bought),
        "candidate_count": len(candidates),
        "evaluated_count": len(evaluated),
        "rejected_reason_counts": rejected_reason_counts,
        "data_skip_counts": data_skip_counts,
        "effective_min_avg_volume": int(effective_min_avg_volume),
        "min_order_score": float(min_order_score),
    }
