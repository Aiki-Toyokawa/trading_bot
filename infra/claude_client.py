from __future__ import annotations

import json
import re
import urllib.request
from typing import Any

from app_settings import (
    CLAUDE_MAX_TOKENS_ENTRY_JSON,
    CLAUDE_MAX_TOKENS_LOG_ANALYSIS,
    CLAUDE_MODEL,
    HTTP_TIMEOUT_SECONDS,
)
from prompts.claude_prompts import build_entry_scoring_prompt, build_log_analysis_prompt


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class ClaudeClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key.strip()
        self.enabled = bool(self.api_key)

    def score_candidates(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not candidates:
            return []

        if not self.enabled:
            return [self._heuristic_score(c) for c in candidates]

        prompt = build_entry_scoring_prompt(candidates)
        text = self._call_claude(prompt, max_tokens=CLAUDE_MAX_TOKENS_ENTRY_JSON)
        parsed = self._extract_json(text)

        if isinstance(parsed, dict):
            parsed_list = [parsed]
        elif isinstance(parsed, list):
            parsed_list = parsed
        else:
            parsed_list = []

        by_ticker: dict[str, dict[str, Any]] = {}
        for row in parsed_list:
            normalized = self._normalize_score(row)
            by_ticker[normalized["ticker"]] = normalized

        results: list[dict[str, Any]] = []
        for c in candidates:
            ticker = str(c.get("ticker", "")).upper()
            if ticker in by_ticker:
                results.append(by_ticker[ticker])
            else:
                fallback = self._heuristic_score(c)
                fallback["ticker"] = ticker
                results.append(fallback)
        return results

    def analyze_logs(
        self,
        recent_logs: list[dict[str, Any]],
        stats: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.enabled:
            return self._local_analysis(recent_logs, stats)

        prompt = build_log_analysis_prompt(recent_logs, stats)
        text = self._call_claude(prompt, max_tokens=CLAUDE_MAX_TOKENS_LOG_ANALYSIS)
        parsed = self._extract_json(text)
        if isinstance(parsed, dict):
            summary = str(parsed.get("summary", "")).strip() or "分析結果なし"
            improvements = parsed.get("improvements", [])
            warnings = parsed.get("warnings", [])
            if not isinstance(improvements, list):
                improvements = [str(improvements)]
            if not isinstance(warnings, list):
                warnings = [str(warnings)]
            return {
                "summary": summary,
                "improvements": [str(v) for v in improvements][:3],
                "warnings": [str(v) for v in warnings][:3],
            }
        return self._local_analysis(recent_logs, stats)

    def health_check(self) -> tuple[bool, str]:
        if not self.enabled:
            return False, "AnthropicAPI: Disabled (API key not set)"
        text = self._call_claude("Return only: OK", max_tokens=12)
        if "OK" in text.upper():
            return True, "AnthropicAPI: Good✅"
        return False, "AnthropicAPI: Check Failed⚠️"

    def _call_claude(self, prompt: str, max_tokens: int) -> str:
        body = {
            "model": CLAUDE_MODEL,
            "max_tokens": max_tokens,
            "temperature": 0,
            "messages": [{"role": "user", "content": prompt}],
        }
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return ""

        contents = payload.get("content", [])
        texts: list[str] = []
        for item in contents:
            if isinstance(item, dict) and item.get("type") == "text":
                texts.append(str(item.get("text", "")))
        return "\n".join(texts).strip()

    @staticmethod
    def _extract_json(text: str) -> Any:
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
        if not match:
            return None
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return None

    def _heuristic_score(self, candidate: dict[str, Any]) -> dict[str, Any]:
        ticker = str(candidate.get("ticker", "")).upper()
        trend_up = bool(candidate.get("trend_up"))
        rsi = float(candidate.get("rsi", 50.0))
        atr_pct = float(candidate.get("atr_pct", 0.02))
        sentiment = _clip((float(candidate.get("price_to_sma50", 1.0)) - 1.0) * 10.0, -1.0, 1.0)

        if atr_pct >= 0.06:
            risk = "high"
        elif atr_pct >= 0.03:
            risk = "medium"
        else:
            risk = "low"

        regime = "trend" if trend_up else "range"
        vol_state = "expansion" if atr_pct >= 0.04 else "normal"
        avoid = (not trend_up) or (rsi >= 72.0) or (rsi <= 35.0)

        conf_base = 0.50
        conf_base += 0.20 if trend_up else -0.10
        conf_base += 0.10 if risk == "low" else (-0.10 if risk == "high" else 0.0)
        conf_base += sentiment * 0.10
        confidence = _clip(conf_base, 0.0, 1.0)

        return {
            "ticker": ticker,
            "market_regime": regime,
            "event_risk": risk,
            "sentiment": round(sentiment, 4),
            "volatility_state": vol_state,
            "avoid": avoid,
            "confidence": round(confidence, 4),
        }

    @staticmethod
    def _normalize_score(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raw = {}
        ticker = str(raw.get("ticker", "")).upper()
        regime = str(raw.get("market_regime", "range")).lower()
        if regime not in {"trend", "range"}:
            regime = "range"

        risk = str(raw.get("event_risk", "medium")).lower()
        if risk not in {"low", "medium", "high"}:
            risk = "medium"

        vol_state = str(raw.get("volatility_state", "normal")).lower()
        if vol_state not in {"normal", "expansion"}:
            vol_state = "normal"

        try:
            sentiment = float(raw.get("sentiment", 0.0))
        except (TypeError, ValueError):
            sentiment = 0.0
        sentiment = _clip(sentiment, -1.0, 1.0)

        try:
            confidence = float(raw.get("confidence", 0.5))
        except (TypeError, ValueError):
            confidence = 0.5
        confidence = _clip(confidence, 0.0, 1.0)

        return {
            "ticker": ticker,
            "market_regime": regime,
            "event_risk": risk,
            "sentiment": round(sentiment, 4),
            "volatility_state": vol_state,
            "avoid": bool(raw.get("avoid", False)),
            "confidence": round(confidence, 4),
        }

    @staticmethod
    def _local_analysis(
        recent_logs: list[dict[str, Any]],
        stats: dict[str, Any],
    ) -> dict[str, Any]:
        total = int(stats.get("total", {}).get("trades", 0))
        win_rate = float(stats.get("total", {}).get("win_rate", 0.0))
        summary = f"総トレード {total}件 / 勝率 {win_rate:.1f}%"
        return {
            "summary": summary,
            "improvements": [
                "BUY_NOT_FILLEDが多い場合は流動性フィルタを強化",
                "SELL判定前に出来高急減とATR拡大の同時検出を追加",
                "イベントリスクhigh銘柄の新規エントリー禁止を維持",
            ],
            "warnings": [
                f"直近ログ件数: {len(recent_logs)}",
            ],
        }
