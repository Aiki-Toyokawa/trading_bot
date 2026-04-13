from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from app_settings import (
    ALLOW_FULL_CASH_DEPLOYMENT,
    CLAUDE_TOP_PICK_LIMIT,
    ENABLE_RANGE_ENTRY_GUARD,
    ENABLE_SCORE_WEIGHTED_SIZING,
    ENABLE_RISK_CAP_PER_TRADE,
    ENTRY_MIN_SCORE,
    FULL_DEPLOYMENT_PER_TRADE_CAP_FRACTION,
    MAX_ATR_PCT,
    MAX_PRICE_USD,
    MIN_ATR_PCT,
    MIN_AVG_VOLUME,
    MIN_ORDER_NOTIONAL_USD,
    MIN_PRICE_USD,
    POSITION_SIZE_FRACTION,
    RANGE_ENTRY_MIN_CONFIDENCE,
    RISK_PER_TRADE,
    SCORE_WEIGHT_FLOOR,
    TEST_ALLOW_RSI_SOFT_PASS,
    TEST_ALLOW_TREND_OVERRIDE,
    TEST_ENTRY_RELAX_MODE,
    TEST_REQUIRE_TREND_REGIME_FOR_SOFT_PASS,
    TEST_RSI_SOFT_HIGH,
    TEST_RSI_SOFT_LOW,
    TEST_TREND_OVERRIDE_MIN_CONFIDENCE,
    TREND_MIN_CONFIDENCE,
    USE_PAPER_ACCOUNT,
    RSI_HIGH_DEFAULT,
    RSI_HIGH_STRONG_TREND,
    RSI_LOW,
    RSI_PERIOD,
    SMA_FAST_PERIOD,
    SMA_SLOW_PERIOD,
    STOP_LOSS_RATE,
    STRONG_TREND_THRESHOLD,
    VOLUME_SURGE_MULTIPLIER,
    get_effective_max_positions,
)


@dataclass
class EntryMetrics:
    ticker: str
    price: float
    avg_volume: float
    volume: float
    sma50: float
    sma200: float
    rsi_now: float
    rsi_prev: float
    atr: float
    trend_up: bool
    breakout: bool

    @property
    def atr_pct(self) -> float:
        if self.price <= 0:
            return 0.0
        return self.atr / self.price

    @property
    def price_to_sma50(self) -> float:
        if self.sma50 <= 0:
            return 1.0
        return self.price / self.sma50


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def sma(values: list[float], period: int) -> float:
    if len(values) < period or period <= 0:
        return 0.0
    return sum(values[-period:]) / period


def rsi(values: list[float], period: int = RSI_PERIOD) -> tuple[float, float]:
    if len(values) < period + 2:
        return 50.0, 50.0

    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(abs(min(diff, 0.0)))

    def _calc(index_end: int) -> float:
        g = gains[index_end - period : index_end]
        l = losses[index_end - period : index_end]
        avg_gain = sum(g) / period
        avg_loss = sum(l) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    now = _calc(len(gains))
    prev = _calc(len(gains) - 1)
    return now, prev


def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1 or len(highs) != len(lows) or len(lows) != len(closes):
        return 0.0
    trs: list[float] = []
    for i in range(1, len(closes)):
        high = highs[i]
        low = lows[i]
        prev_close = closes[i - 1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return 0.0
    return sum(trs[-period:]) / period


def get_rsi_threshold(sma50: float, sma200: float) -> tuple[float, float]:
    if sma200 <= 0:
        return RSI_LOW, RSI_HIGH_DEFAULT
    trend_strength = (sma50 - sma200) / sma200
    if trend_strength > STRONG_TREND_THRESHOLD:
        return RSI_LOW, RSI_HIGH_STRONG_TREND
    return RSI_LOW, RSI_HIGH_DEFAULT


def is_rsi_valid(rsi_now: float, rsi_prev: float, sma50: float, sma200: float) -> bool:
    low, high = get_rsi_threshold(sma50, sma200)
    return (low < rsi_now < high) and (rsi_now > rsi_prev)


def has_volume_surge(volume: float, avg_volume: float) -> bool:
    if avg_volume <= 0:
        return False
    return volume > avg_volume * VOLUME_SURGE_MULTIPLIER


def build_entry_metrics(symbol: str, bars: list[dict[str, Any]]) -> EntryMetrics | None:
    if len(bars) < SMA_SLOW_PERIOD + 2:
        return None
    closes = [_to_float(b.get("c")) for b in bars]
    highs = [_to_float(b.get("h"), _to_float(b.get("c"))) for b in bars]
    lows = [_to_float(b.get("l"), _to_float(b.get("c"))) for b in bars]
    volumes = [_to_float(b.get("v")) for b in bars]

    price = closes[-1]
    sma50 = sma(closes, SMA_FAST_PERIOD)
    sma200 = sma(closes, SMA_SLOW_PERIOD)
    rsi_now, rsi_prev = rsi(closes, RSI_PERIOD)
    atr_val = atr(highs, lows, closes, period=14)
    avg_volume_20 = sma(volumes, 20)
    volume_now = volumes[-1]

    trend_up = sma50 > sma200
    breakout = price >= max(closes[-20:])

    return EntryMetrics(
        ticker=symbol,
        price=price,
        avg_volume=avg_volume_20,
        volume=volume_now,
        sma50=sma50,
        sma200=sma200,
        rsi_now=rsi_now,
        rsi_prev=rsi_prev,
        atr=atr_val,
        trend_up=trend_up,
        breakout=breakout,
    )


def evaluate_entry_candidate(
    metrics: EntryMetrics,
    claude_row: dict[str, Any] | None,
    effective_min_avg_volume: float | None = None,
) -> tuple[bool, float, str]:
    if metrics.price < MIN_PRICE_USD or metrics.price > MAX_PRICE_USD:
        return False, 0.0, "price_out_of_range"

    min_avg_volume = float(MIN_AVG_VOLUME) if effective_min_avg_volume is None else float(effective_min_avg_volume)
    if metrics.avg_volume < min_avg_volume:
        return False, 0.0, "low_avg_volume"

    claude = claude_row or {}
    if bool(claude.get("avoid", False)):
        return False, 0.0, "claude_avoid"

    if str(claude.get("event_risk", "medium")).lower() == "high":
        return False, 0.0, "high_event_risk"

    confidence = _to_float(claude.get("confidence"), 0.5)
    sentiment = _to_float(claude.get("sentiment"), 0.0)
    regime = str(claude.get("market_regime", "range")).lower()
    relax_mode = bool(USE_PAPER_ACCOUNT and TEST_ENTRY_RELAX_MODE)

    trend_ok = bool(metrics.trend_up)
    if not trend_ok and relax_mode and TEST_ALLOW_TREND_OVERRIDE:
        regime_ok = (regime == "trend") if TEST_REQUIRE_TREND_REGIME_FOR_SOFT_PASS else True
        trend_ok = regime_ok and metrics.breakout and confidence >= TEST_TREND_OVERRIDE_MIN_CONFIDENCE
    if not trend_ok:
        return False, 0.0, "trend_not_up"

    rsi_ok = is_rsi_valid(metrics.rsi_now, metrics.rsi_prev, metrics.sma50, metrics.sma200)
    if not rsi_ok and relax_mode and TEST_ALLOW_RSI_SOFT_PASS:
        regime_ok = (regime == "trend") if TEST_REQUIRE_TREND_REGIME_FOR_SOFT_PASS else True
        rsi_ok = (
            regime_ok
            and (TEST_RSI_SOFT_LOW < metrics.rsi_now < TEST_RSI_SOFT_HIGH)
            and confidence >= TEST_TREND_OVERRIDE_MIN_CONFIDENCE
        )
    if not rsi_ok:
        return False, 0.0, "rsi_condition_failed"

    if regime == "trend" and confidence < TREND_MIN_CONFIDENCE:
        return False, 0.0, "trend_low_confidence"
    if ENABLE_RANGE_ENTRY_GUARD and regime != "trend" and confidence < RANGE_ENTRY_MIN_CONFIDENCE:
        return False, 0.0, "range_low_confidence"
    if metrics.atr_pct < MIN_ATR_PCT:
        return False, 0.0, "atr_too_low"
    if metrics.atr_pct > MAX_ATR_PCT:
        return False, 0.0, "atr_too_high"

    trend_strength = 0.0
    if metrics.sma200 > 0:
        trend_strength = max((metrics.sma50 - metrics.sma200) / metrics.sma200, 0.0)
    trend_strength_norm = max(0.0, min(trend_strength / 0.15, 1.0))

    atr_pref = 0.0
    if 0.015 <= metrics.atr_pct <= 0.06:
        atr_pref = 1.0
    elif metrics.atr_pct >= 0.09:
        atr_pref = -1.0

    score = 0.45
    score += 0.15 if metrics.breakout else 0.0
    score += 0.10 if has_volume_surge(metrics.volume, metrics.avg_volume) else 0.0
    score += 0.20 * max(0.0, min(1.0, confidence))
    score += 0.10 * max(-1.0, min(1.0, sentiment))
    score += 0.05 if regime == "trend" else 0.0
    score += 0.08 * trend_strength_norm
    score += 0.04 * atr_pref
    score = max(0.0, min(score, 1.0))

    if score < ENTRY_MIN_SCORE:
        return False, score, "score_too_low"
    return True, score, "entry_ok"


def calculate_entry_qty(
    price: float,
    cash: float,
    equity: float,
    slots_remaining: int,
    score_weight: float | None = None,
    score_weight_sum: float | None = None,
) -> float:
    if price <= 0 or cash <= 0 or equity <= 0 or slots_remaining <= 0:
        return 0.0

    if ALLOW_FULL_CASH_DEPLOYMENT:
        # 残キャッシュ配分:
        # - 既定は等分
        # - 有効化時は候補スコアに比例配分（高スコアに厚く配る）
        if (
            ENABLE_SCORE_WEIGHTED_SIZING
            and score_weight is not None
            and score_weight_sum is not None
            and score_weight_sum > 0
        ):
            weight = max(float(score_weight), SCORE_WEIGHT_FLOOR)
            budget_cap = cash * (weight / float(score_weight_sum))
        else:
            budget_cap = cash / slots_remaining
        # フル投下モードでも1銘柄への偏りを制限する
        budget_cap = min(budget_cap, equity * FULL_DEPLOYMENT_PER_TRADE_CAP_FRACTION)
    else:
        budget_cap = min(cash / slots_remaining, equity * POSITION_SIZE_FRACTION)

    risk_cap = equity * RISK_PER_TRADE
    stop_price = price * STOP_LOSS_RATE
    risk_per_share = max(price - stop_price, 0.01)

    qty_by_budget = budget_cap / price
    qty_by_risk = risk_cap / risk_per_share
    if ENABLE_RISK_CAP_PER_TRADE:
        qty = min(qty_by_budget, qty_by_risk)
    else:
        qty = qty_by_budget
    if qty * price < MIN_ORDER_NOTIONAL_USD:
        return 0.0
    if qty < 1:
        return 0.0
    return float(math.floor(qty))


def pick_top_candidates(
    evaluated: list[tuple[EntryMetrics, dict[str, Any], bool, float, str]],
    max_new_positions: int | None,
) -> list[tuple[EntryMetrics, dict[str, Any], float, str]]:
    buyables = [(m, c, s, r) for (m, c, ok, s, r) in evaluated if ok]
    buyables.sort(key=lambda x: x[2], reverse=True)

    max_pick = len(buyables)
    if max_new_positions is not None:
        max_pick = min(max_pick, max_new_positions)
    effective_max_positions = get_effective_max_positions()
    if effective_max_positions > 0:
        max_pick = min(max_pick, effective_max_positions)
    if CLAUDE_TOP_PICK_LIMIT > 0:
        max_pick = min(max_pick, CLAUDE_TOP_PICK_LIMIT)
    return buyables[:max_pick]
