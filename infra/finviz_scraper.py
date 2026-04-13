from __future__ import annotations

import math
import re
import urllib.request
from typing import Any

from app_settings import (
    DEFAULT_FALLBACK_SYMBOLS,
    FINVIZ_CANDIDATE_LIMIT,
    HTTP_TIMEOUT_SECONDS,
    MAX_PRICE_USD,
    MIN_AVG_VOLUME,
    MIN_PRICE_USD,
)


class FinvizScraper:
    _ROW_TICKER_PATTERN = re.compile(r'data-boxover-ticker="([A-Z\.\-]+)"')
    _FALLBACK_TICKER_PATTERN = re.compile(r"quote\.ashx\?t=([A-Z\.\-]+)")
    _PAGE_SIZE = 20

    def health_check(self) -> tuple[bool, str]:
        try:
            html = self._fetch_screener_html(start_row=1)
            symbols = self._extract_tickers(html)
            if symbols:
                return True, "Finviz Web: OK"
            return False, "Finviz Web: NG (no_symbol_detected)"
        except Exception as exc:
            return False, f"Finviz Web: NG ({exc})"

    def fetch_candidates(self, limit: int = FINVIZ_CANDIDATE_LIMIT) -> list[dict[str, Any]]:
        limit = max(1, int(limit))
        try:
            unique: list[str] = []
            seen: set[str] = set()
            for start_row in range(1, limit + 1, self._PAGE_SIZE):
                html = self._fetch_screener_html(start_row=start_row)
                page_symbols = self._extract_tickers(html)
                if not page_symbols:
                    break
                for symbol in page_symbols:
                    normalized = symbol.strip().upper()
                    if not normalized or normalized in seen:
                        continue
                    seen.add(normalized)
                    unique.append(normalized)
                    if len(unique) >= limit:
                        break
                if len(unique) >= limit:
                    break
            if not unique:
                unique = list(DEFAULT_FALLBACK_SYMBOLS)[:limit]
        except Exception:
            unique = list(DEFAULT_FALLBACK_SYMBOLS)[:limit]

        return [{"ticker": s, "source": "FINVIZ"} for s in unique]

    def _fetch_screener_html(self, start_row: int = 1) -> str:
        min_vol_filter = self._map_avg_volume_filter(MIN_AVG_VOLUME)
        min_price_filter = self._map_min_price_filter(MIN_PRICE_USD)
        max_price_filter = self._map_max_price_filter(MAX_PRICE_USD)
        # 高流動性・価格帯・上昇トレンドを意識した基本条件（設定値連動）
        url = (
            "https://finviz.com/screener.ashx?v=111"
            f"&f=geo_usa,{min_vol_filter},{min_price_filter},{max_price_filter},"
            f"ta_sma50_pa,ta_sma200_pa&r={max(1, int(start_row))}"
        )
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                )
            },
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            return resp.read().decode("utf-8", errors="ignore")

    def _extract_tickers(self, html: str) -> list[str]:
        row_symbols = self._ROW_TICKER_PATTERN.findall(html)
        if row_symbols:
            return row_symbols
        return self._FALLBACK_TICKER_PATTERN.findall(html)

    @staticmethod
    def _map_avg_volume_filter(min_avg_volume: float) -> str:
        # Finviz average volume filterは千株単位。
        # 本番は厳密運用のため、設定値以上になる最小バケット（ceil）を採用する。
        buckets_k = [50, 100, 200, 300, 400, 500, 750, 1000, 2000, 5000, 10000]
        target_k = max(1, int(math.ceil(float(min_avg_volume) / 1000.0)))
        selected = buckets_k[-1]
        for b in buckets_k:
            if b >= target_k:
                selected = b
                break
        return f"sh_avgvol_o{selected}"

    @staticmethod
    def _map_min_price_filter(min_price_usd: float) -> str:
        buckets = [1, 2, 3, 4, 5, 7, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 300]
        target = max(1.0, float(min_price_usd))
        # 価格下限は設定値以上を保証するためceil
        selected = buckets[-1]
        for b in buckets:
            if float(b) >= target:
                selected = b
                break
        return f"sh_price_o{selected}"

    @staticmethod
    def _map_max_price_filter(max_price_usd: float) -> str:
        buckets = [1, 2, 3, 4, 5, 7, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 300]
        target = max(1.0, float(max_price_usd))
        # 価格上限は設定値以下を保証するためfloor
        selected = buckets[0]
        for b in buckets:
            if float(b) <= target:
                selected = b
            else:
                break
        return f"sh_price_u{selected}"
