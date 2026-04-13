from __future__ import annotations

import json
from typing import Any


ENTRY_SCORE_FORMAT = """
Respond in JSON format using the following structure:
{
  "ticker": "NVDA",
  "market_regime": "trend|range",
  "event_risk": "low|medium|high",
  "sentiment": -1.0 ~ +1.0,
  "volatility_state": "normal|expansion",
  "avoid": false,
  "confidence": 0.0 ~ 1.0
}
"""


def build_entry_scoring_prompt(candidates: list[dict[str, Any]]) -> str:
    return (
        "You are an AI specialized in organizing information for short-term trading of U.S. stocks. "
        "You do not make trading execution decisions. Structure ambiguous information. "
        "Evaluate the following candidate stocks and return them as a JSON array for each ticker. "
        "Required keys: ticker, market_regime, event_risk, sentiment, volatility_state, avoid, confidence. "
        "Respond with JSON only, Don't explain, Don't add text.\n\n"
        f"{ENTRY_SCORE_FORMAT}\n"
        "Candidate data:\n"
        f"{json.dumps(candidates, ensure_ascii=False)}"
    )


def build_log_analysis_prompt(
    recent_logs: list[dict[str, Any]],
    stats: dict[str, Any],
) -> str:
    return (
        "You are an AI specialized in analyzing automated trading logs. "
        "Always respond in Japanese. "
        "Provide a brief summary of the results and propose three improvements. "
        "Return the response in the following JSON format."
        '{"summary":"...","improvements":["...","...","..."],"warnings":["..."]}\n\n'
        f"recent_logs={json.dumps(recent_logs, ensure_ascii=False)}\n"
        f"stats={json.dumps(stats, ensure_ascii=False)}"
    )
