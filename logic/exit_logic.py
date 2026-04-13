from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from app_settings import (
    CRASH_EXIT_FROM_PREV_CLOSE_PCT,
    CRASH_TIGHTEN_FROM_PREV_CLOSE_PCT,
    ENABLE_CRASH_PROTECTION,
    ENABLE_ADAPTIVE_TRAILING_BOOST,
    REMAINING_HALF_MIN_PROFIT_RATE,
    REMAINING_HALF_MIN_PROFIT_RATE_STRONG,
    REMAINING_HALF_STRONG_FLOOR_TRIGGER_RATE,
    STOP_LOSS_RATE,
    TRAILING_BOOST_TRIGGER_RATE,
    TRAILING_RATE_STRONG_TREND,
    TRAILING_RATE_TIGHT,
    TRAILING_RATE_VERY_STRONG_TREND,
    TRAILING_VERY_STRONG_TRIGGER_RATE,
    get_effective_take_profit_rate,
    get_effective_trailing_rate,
)
from logic.entry_logic import sma


@dataclass
class ExitDecision:
    action: str
    reason: str
    target_qty: float
    expected_price: float
    set_take_profit_done: bool | None = None
    set_trailing_high_price: float | None = None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _calc_half_take_profit_qty(current_qty: float) -> float:
    qty = _to_float(current_qty, 0.0)
    if qty <= 0:
        return 0.0
    # 少量ポジションは全量売却で取りこぼしを防ぐ。
    if qty <= 1.0:
        return qty

    rounded_qty = round(qty)
    # 整数株運用時は端数(.5株)が残らないように半分を切り下げる。
    if abs(qty - float(rounded_qty)) <= 1e-9:
        half_qty = float(math.floor(rounded_qty * 0.5))
        return half_qty if half_qty > 0 else qty

    # 小数株運用時は通常の半分。
    half_qty = qty * 0.5
    return half_qty if half_qty > 0 else qty


def decide_exit(
    entry_price: float,
    current_price: float,
    current_qty: float,
    bars: list[dict[str, Any]],
    take_profit_done: bool,
    trailing_high_price: float | None,
    observed_high_price: float | None = None,
) -> ExitDecision:
    if entry_price <= 0 or current_price <= 0 or current_qty <= 0:
        return ExitDecision("HOLD", "invalid_position_or_price", 0.0, current_price)

    take_profit_rate = get_effective_take_profit_rate()
    trailing_rate_base = get_effective_trailing_rate()
    stop_price = entry_price * STOP_LOSS_RATE
    soft_tp_trigger = entry_price * take_profit_rate

    if current_price <= stop_price:
        return ExitDecision("SELL", "stop_loss", current_qty, current_price)

    closes = [_to_float(b.get("c")) for b in bars if _to_float(b.get("c")) > 0]
    # 「前日終値比」を優先するため、2本以上ある場合はひとつ前の足を参照する。
    prev_close = closes[-2] if len(closes) >= 2 else (closes[-1] if closes else 0.0)
    if ENABLE_CRASH_PROTECTION and prev_close > 0:
        prev_close_change_pct = (current_price - prev_close) / prev_close
        if prev_close_change_pct <= CRASH_EXIT_FROM_PREV_CLOSE_PCT:
            return ExitDecision("SELL", "crash_exit", current_qty, current_price)

    if not take_profit_done and current_price >= soft_tp_trigger:
        half_qty = _calc_half_take_profit_qty(current_qty)
        return ExitDecision(
            "SELL",
            "take_profit_half",
            half_qty,
            current_price,
            set_take_profit_done=True,
            set_trailing_high_price=current_price,
        )

    if take_profit_done:
        observed_high = _to_float(observed_high_price, 0.0)
        trailing_high = max(
            _to_float(trailing_high_price, current_price),
            current_price,
            observed_high,
        )
        gain_rate = current_price / entry_price if entry_price > 0 else 0.0
        trailing_rate = trailing_rate_base
        crash_tightened = False
        if ENABLE_CRASH_PROTECTION and prev_close > 0:
            prev_close_change_pct = (current_price - prev_close) / prev_close
            if prev_close_change_pct <= CRASH_TIGHTEN_FROM_PREV_CLOSE_PCT:
                trailing_rate = max(trailing_rate_base, TRAILING_RATE_TIGHT)
                crash_tightened = True

        # 利益を伸ばす最適化: 平常時かつ強トレンドならトレーリングをやや緩める
        # ただし急落警戒時は防御優先で緩和しない。
        if ENABLE_ADAPTIVE_TRAILING_BOOST and not crash_tightened:
            sma20 = sma(closes, 20)
            sma50 = sma(closes, 50)
            strong_trend = sma20 > 0 and sma50 > 0 and sma20 > sma50 and current_price >= sma20
            if strong_trend and gain_rate >= TRAILING_VERY_STRONG_TRIGGER_RATE:
                trailing_rate = min(trailing_rate, TRAILING_RATE_VERY_STRONG_TREND)
            elif strong_trend and gain_rate >= TRAILING_BOOST_TRIGGER_RATE:
                trailing_rate = min(trailing_rate, TRAILING_RATE_STRONG_TREND)

        trailing_stop = trailing_high * trailing_rate
        # 半利確後の残り半分は最低利益フロアを維持しつつ、十分な含み益時は引き上げる。
        min_profit_floor_rate = REMAINING_HALF_MIN_PROFIT_RATE
        if gain_rate >= REMAINING_HALF_STRONG_FLOOR_TRIGGER_RATE:
            min_profit_floor_rate = max(
                min_profit_floor_rate,
                REMAINING_HALF_MIN_PROFIT_RATE_STRONG,
            )
        min_profit_floor = entry_price * min_profit_floor_rate
        effective_stop = max(trailing_stop, min_profit_floor)
        if current_price <= effective_stop:
            return ExitDecision(
                "SELL",
                f"trailing_stop_{trailing_rate}_floor_{min_profit_floor_rate}",
                current_qty,
                current_price,
                set_take_profit_done=True,
                set_trailing_high_price=trailing_high,
            )
        return ExitDecision(
            "HOLD",
            "hold_trailing",
            0.0,
            current_price,
            set_take_profit_done=True,
            set_trailing_high_price=trailing_high,
        )

    return ExitDecision("HOLD", "hold", 0.0, current_price)


def calc_slippage_pct(side: str, requested_price: float, filled_price: float) -> float | None:
    if requested_price <= 0 or filled_price <= 0:
        return None
    side = side.upper()
    if side == "BUY":
        return (filled_price - requested_price) / requested_price * 100.0
    if side == "SELL":
        return (requested_price - filled_price) / requested_price * 100.0
    return None


def calc_capital_change_pct(entry_price: float, exit_price: float) -> float | None:
    if entry_price <= 0 or exit_price <= 0:
        return None
    return (exit_price - entry_price) / entry_price * 100.0
