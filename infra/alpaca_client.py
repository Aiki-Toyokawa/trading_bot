from __future__ import annotations

import json
import math
import time as time_module
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app_settings import (
    ALPACA_BASE_URL_LIVE,
    ALPACA_BASE_URL_PAPER,
    ALPACA_DATA_FEED,
    ALPACA_DATA_URL,
    HTTP_TIMEOUT_SECONDS,
    MARKET_HOURS_FALLBACK_ENABLED,
    MARKET_HOURS_FALLBACK_END,
    MARKET_HOURS_FALLBACK_SESSION_WEEKDAYS,
    MARKET_HOURS_FALLBACK_START,
    MARKET_HOURS_FALLBACK_TZ,
    MARKET_HOURS_GUARD_ENABLED,
    ENABLE_INTRADAY_HIGH_FOR_TRAILING,
    ORDER_STATUS_MAX_WAIT_SECONDS,
    ORDER_STATUS_POLL_INTERVAL_SECONDS,
    USE_PAPER_ACCOUNT,
)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class AlpacaClient:
    def __init__(self, api_key: str, secret_key: str) -> None:
        self.api_key = api_key.strip()
        self.secret_key = secret_key.strip()
        self.base_url = ALPACA_BASE_URL_PAPER if USE_PAPER_ACCOUNT else ALPACA_BASE_URL_LIVE
        self.data_url = ALPACA_DATA_URL
        self.data_feed = ALPACA_DATA_FEED
        self.enabled = bool(self.api_key and self.secret_key)

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        if not self.enabled:
            raise RuntimeError("Alpaca credentials are not configured.")

        final_url = url
        if params:
            query = urllib.parse.urlencode(params)
            final_url = f"{url}?{query}"

        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(
            final_url,
            data=data,
            headers=self._headers(),
            method=method,
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return {}
            return json.loads(raw)

    def get_account(self) -> dict[str, Any]:
        if not self.enabled:
            return {
                "id": "SIM-ACCOUNT",
                "cash": 10_000.0,
                "equity": 10_000.0,
                "buying_power": 10_000.0,
                "source": "SIMULATION",
            }
        try:
            payload = self._request("GET", f"{self.base_url}/v2/account")
            return {
                "id": payload.get("id"),
                "cash": _to_float(payload.get("cash")),
                "equity": _to_float(payload.get("equity")),
                "buying_power": _to_float(payload.get("buying_power")),
                "source": "ALPACA",
            }
        except Exception as exc:
            return {
                "id": "ALPACA-ACCOUNT-ERROR",
                "cash": 0.0,
                "equity": 0.0,
                "buying_power": 0.0,
                "source": "ALPACA_ERROR",
                "error": str(exc),
            }

    def health_check(self) -> tuple[bool, str]:
        if not self.enabled:
            return False, "AlpacaAPI: Disabled (API key not set)"
        account = self.get_account()
        if account.get("source") == "ALPACA":
            return True, "AlpacaAPI: Good✅"
        return False, "AlpacaAPI: Account Error⚠️"

    def get_market_clock(self) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        try:
            payload = self._request("GET", f"{self.base_url}/v2/clock")
            if isinstance(payload, dict):
                return payload
            return None
        except Exception:
            return None

    def market_gate_status(self) -> dict[str, Any]:
        if not MARKET_HOURS_GUARD_ENABLED:
            return {
                "is_open": True,
                "source": "disabled",
                "message": "MarketGate: Disabled",
            }

        clock = self.get_market_clock()
        if clock is not None and "is_open" in clock:
            is_open = bool(clock.get("is_open"))
            next_open = str(clock.get("next_open", ""))
            next_close = str(clock.get("next_close", ""))
            return {
                "is_open": is_open,
                "source": "alpaca_clock",
                "next_open": next_open,
                "next_close": next_close,
                "message": (
                    f"MarketGate: {'OPEN✅' if is_open else 'CLOSED'} "
                    f"(source=alpaca_clock next_open={next_open} next_close={next_close})"
                ),
            }

        if MARKET_HOURS_FALLBACK_ENABLED:
            is_open, note = self._fallback_market_window_check()
            return {
                "is_open": is_open,
                "source": "jst_fallback",
                "message": f"MarketGate: {'OPEN✅' if is_open else 'CLOSED'} (source=jst_fallback {note})",
            }

        return {
            "is_open": False,
            "source": "clock_unavailable",
            "message": "MarketGate: CLOSED (clock unavailable)",
        }

    def get_positions(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        try:
            payload = self._request("GET", f"{self.base_url}/v2/positions")
            if not isinstance(payload, list):
                return []
            positions = []
            for row in payload:
                positions.append(
                    {
                        "symbol": row.get("symbol", ""),
                        "qty": _to_float(row.get("qty")),
                        "avg_entry_price": _to_float(row.get("avg_entry_price")),
                        "current_price": _to_float(row.get("current_price")),
                        "market_value": _to_float(row.get("market_value")),
                        "unrealized_pl": _to_float(row.get("unrealized_pl")),
                        "source": "ALPACA",
                    }
                )
            return positions
        except Exception:
            return []

    def get_latest_price(self, symbol: str) -> float:
        if not symbol:
            return 0.0
        if self.enabled:
            try:
                payload = self._request(
                    "GET",
                    f"{self.data_url}/v2/stocks/{symbol}/bars/latest",
                    params={"feed": self.data_feed},
                )
                bar = payload.get("bar", {})
                return _to_float(bar.get("c"))
            except Exception:
                pass
        bars = self.get_daily_bars(symbol, limit=1)
        if not bars:
            return 0.0
        return _to_float(bars[-1].get("c"))

    def get_latest_minute_high(self, symbol: str) -> float:
        if not symbol or not ENABLE_INTRADAY_HIGH_FOR_TRAILING:
            return 0.0
        if self.enabled:
            try:
                payload = self._request(
                    "GET",
                    f"{self.data_url}/v2/stocks/{symbol}/bars/latest",
                    params={"feed": self.data_feed},
                )
                bar = payload.get("bar", {})
                high = _to_float(bar.get("h"), 0.0)
                if high > 0:
                    return high
            except Exception:
                return 0.0
            return 0.0
        # simulation時は分足を持たないため最新価格を代用
        return self.get_latest_price(symbol)

    def get_daily_bars(self, symbol: str, limit: int = 250) -> list[dict[str, Any]]:
        if self.enabled:
            try:
                end = datetime.now(timezone.utc)
                start = end - timedelta(days=max(450, limit * 3))
                payload = self._request(
                    "GET",
                    f"{self.data_url}/v2/stocks/{symbol}/bars",
                    params={
                        "timeframe": "1Day",
                        "start": start.isoformat().replace("+00:00", "Z"),
                        "end": end.isoformat().replace("+00:00", "Z"),
                        "limit": max(limit, 1),
                        "adjustment": "raw",
                        "feed": self.data_feed,
                    },
                )
                bars = payload.get("bars", [])
                if bars:
                    return bars
            except Exception:
                return []
            return []
        return self._synthetic_daily_bars(symbol, limit=max(limit, 1))

    def submit_market_order(self, symbol: str, qty: float, side: str) -> dict[str, Any]:
        qty = max(0.0, float(qty))
        market_price = self.get_latest_price(symbol)
        if qty <= 0 or market_price <= 0:
            return {
                "id": f"SIM-INVALID-{symbol}",
                "status": "rejected",
                "filled_avg_price": None,
                "qty": qty,
                "filled_qty": 0.0,
                "symbol": symbol,
                "side": side,
                "source": "SIMULATION",
            }

        if not self.enabled:
            return {
                "id": f"SIM-{symbol}-{int(datetime.now(timezone.utc).timestamp())}",
                "status": "filled",
                "filled_avg_price": market_price,
                "qty": qty,
                "filled_qty": qty,
                "symbol": symbol,
                "side": side,
                "source": "SIMULATION",
            }

        try:
            payload = self._request(
                "POST",
                f"{self.base_url}/v2/orders",
                body={
                    "symbol": symbol,
                    "qty": round(qty, 6),
                    "side": side.lower(),
                    "type": "market",
                    "time_in_force": "day",
                },
            )
            return {
                "id": payload.get("id"),
                "status": payload.get("status", "new"),
                "filled_avg_price": _to_float(payload.get("filled_avg_price"), market_price),
                "qty": _to_float(payload.get("qty"), qty),
                "filled_qty": _to_float(payload.get("filled_qty"), 0.0),
                "symbol": payload.get("symbol", symbol),
                "side": payload.get("side", side),
                "source": "ALPACA",
                "raw": payload,
            }
        except Exception as exc:
            return {
                "id": f"ALPACA-ERROR-{symbol}-{int(datetime.now(timezone.utc).timestamp())}",
                "status": "error",
                "filled_avg_price": None,
                "qty": qty,
                "filled_qty": 0.0,
                "symbol": symbol,
                "side": side,
                "source": "ALPACA_ERROR",
                "error": str(exc),
            }

    @staticmethod
    def is_effectively_filled(order_payload: dict[str, Any]) -> bool:
        status = str(order_payload.get("status", "")).lower()
        filled_qty = AlpacaClient.extract_filled_qty(order_payload)
        if status == "filled":
            return filled_qty > 0
        if status == "partially_filled":
            return filled_qty > 0
        return False

    @staticmethod
    def extract_filled_qty(order_payload: dict[str, Any]) -> float:
        filled_qty = _to_float(order_payload.get("filled_qty"), 0.0)
        if filled_qty > 0:
            return filled_qty
        status = str(order_payload.get("status", "")).lower()
        if status == "filled":
            return _to_float(order_payload.get("qty"), 0.0)
        return 0.0

    def get_open_orders(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        try:
            payload = self._request(
                "GET",
                f"{self.base_url}/v2/orders",
                params={"status": "open", "direction": "desc", "limit": 500},
            )
            if not isinstance(payload, list):
                return []
            rows: list[dict[str, Any]] = []
            for row in payload:
                if not isinstance(row, dict):
                    continue
                rows.append(
                    {
                        "id": row.get("id"),
                        "symbol": str(row.get("symbol", "")).upper(),
                        "side": str(row.get("side", "")).lower(),
                        "status": str(row.get("status", "")),
                        "qty": _to_float(row.get("qty"), 0.0),
                        "filled_qty": _to_float(row.get("filled_qty"), 0.0),
                    }
                )
            return rows
        except Exception:
            return []

    def cancel_order(self, order_id: str) -> bool:
        key = str(order_id or "").strip()
        if not self.enabled or not key:
            return False
        try:
            self._request("DELETE", f"{self.base_url}/v2/orders/{key}")
            return True
        except urllib.error.HTTPError as exc:
            # 既にキャンセル済み/約定済みなら成功扱いに寄せる
            if int(getattr(exc, "code", 0)) in {404, 422}:
                return True
            return False
        except Exception:
            return False

    def cancel_open_orders_by_status(self, target_statuses: set[str] | None = None) -> dict[str, Any]:
        statuses = {s.lower() for s in (target_statuses or {"accepted"})}
        cancelled = 0
        failed = 0
        skipped = 0
        affected: list[str] = []
        for row in self.get_open_orders():
            status = str(row.get("status", "")).lower()
            oid = str(row.get("id", "")).strip()
            if not oid:
                continue
            if status not in statuses:
                skipped += 1
                continue
            if self.cancel_order(oid):
                cancelled += 1
                affected.append(oid)
            else:
                failed += 1
        return {
            "target_statuses": sorted(statuses),
            "cancelled": cancelled,
            "failed": failed,
            "skipped": skipped,
            "order_ids": affected,
        }

    def submit_market_sell_for_all_positions(self) -> dict[str, Any]:
        if not self.enabled:
            return {"submitted": 0, "failed": 0, "details": [], "note": "alpaca_disabled"}
        submitted = 0
        failed = 0
        details: list[dict[str, Any]] = []
        open_orders = self.get_open_orders()
        open_sell_symbols = {
            str(o.get("symbol", "")).upper()
            for o in open_orders
            if str(o.get("side", "")).lower() == "sell"
        }
        for pos in self.get_positions():
            symbol = str(pos.get("symbol", "")).upper()
            qty = _to_float(pos.get("qty"), 0.0)
            if not symbol or qty <= 0:
                continue
            if symbol in open_sell_symbols:
                details.append({"symbol": symbol, "qty": qty, "action": "SKIP_OPEN_SELL_EXISTS"})
                continue
            result = self.submit_market_order(symbol=symbol, qty=qty, side="sell")
            ok = self.is_effectively_filled(result) or str(result.get("status", "")).lower() in {
                "new",
                "accepted",
                "pending_new",
            }
            if ok:
                submitted += 1
                details.append(
                    {
                        "symbol": symbol,
                        "qty": qty,
                        "action": "SELL_SUBMITTED",
                        "status": str(result.get("status", "")),
                        "order_id": result.get("id"),
                    }
                )
            else:
                failed += 1
                details.append(
                    {
                        "symbol": symbol,
                        "qty": qty,
                        "action": "SELL_FAILED",
                        "status": str(result.get("status", "")),
                        "error": result.get("error"),
                    }
                )
        return {"submitted": submitted, "failed": failed, "details": details}

    def get_closed_orders(self, symbol: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        params: dict[str, Any] = {
            "status": "closed",
            "direction": "desc",
            "limit": max(1, int(limit)),
        }
        if symbol:
            params["symbols"] = str(symbol).upper()
        try:
            payload = self._request(
                "GET",
                f"{self.base_url}/v2/orders",
                params=params,
            )
            if not isinstance(payload, list):
                return []
            rows: list[dict[str, Any]] = []
            for row in payload:
                if not isinstance(row, dict):
                    continue
                rows.append(
                    {
                        "id": row.get("id"),
                        "symbol": str(row.get("symbol", "")).upper(),
                        "side": str(row.get("side", "")).lower(),
                        "status": str(row.get("status", "")),
                        "qty": _to_float(row.get("qty"), 0.0),
                        "filled_qty": _to_float(row.get("filled_qty"), 0.0),
                        "filled_avg_price": _to_float(row.get("filled_avg_price"), 0.0),
                        "filled_at": row.get("filled_at"),
                        "submitted_at": row.get("submitted_at"),
                        "source": "ALPACA",
                        "raw": row,
                    }
                )
            return rows
        except Exception:
            return []

    def find_latest_filled_sell(self, symbol: str) -> dict[str, Any] | None:
        symbol_key = str(symbol or "").upper()
        if not symbol_key:
            return None
        for row in self.get_closed_orders(symbol=symbol_key, limit=200):
            if str(row.get("symbol", "")).upper() != symbol_key:
                continue
            if str(row.get("side", "")).lower() != "sell":
                continue
            if str(row.get("status", "")).lower() not in {"filled", "partially_filled"}:
                continue
            if _to_float(row.get("filled_qty"), 0.0) <= 0:
                continue
            if _to_float(row.get("filled_avg_price"), 0.0) <= 0:
                continue
            return row
        return None

    def summarize_filled_sells_since(
        self,
        symbol: str,
        opened_at: str | None = None,
    ) -> dict[str, Any] | None:
        symbol_key = str(symbol or "").upper()
        if not symbol_key:
            return None
        opened_dt: datetime | None = None
        if opened_at:
            text = str(opened_at).strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                opened_dt = datetime.fromisoformat(text)
            except ValueError:
                opened_dt = None

        total_qty = 0.0
        weighted_amount = 0.0
        latest_row: dict[str, Any] | None = None
        latest_dt: datetime | None = None

        for row in self.get_closed_orders(symbol=symbol_key, limit=500):
            if str(row.get("symbol", "")).upper() != symbol_key:
                continue
            if str(row.get("side", "")).lower() != "sell":
                continue
            if str(row.get("status", "")).lower() not in {"filled", "partially_filled"}:
                continue

            filled_qty = _to_float(row.get("filled_qty"), 0.0)
            filled_price = _to_float(row.get("filled_avg_price"), 0.0)
            if filled_qty <= 0 or filled_price <= 0:
                continue

            filled_at_text = str(row.get("filled_at") or "")
            filled_dt: datetime | None = None
            if filled_at_text:
                temp = filled_at_text[:-1] + "+00:00" if filled_at_text.endswith("Z") else filled_at_text
                try:
                    filled_dt = datetime.fromisoformat(temp)
                except ValueError:
                    filled_dt = None
            if opened_dt is not None and filled_dt is not None and filled_dt < opened_dt:
                continue

            total_qty += filled_qty
            weighted_amount += filled_qty * filled_price

            if latest_row is None:
                latest_row = row
                latest_dt = filled_dt
            else:
                if latest_dt is None or (filled_dt is not None and filled_dt > latest_dt):
                    latest_row = row
                    latest_dt = filled_dt

        if total_qty <= 0:
            return None
        avg_price = weighted_amount / total_qty
        return {
            "symbol": symbol_key,
            "total_filled_qty": total_qty,
            "weighted_avg_price": avg_price,
            "latest_order_id": (latest_row or {}).get("id"),
            "latest_filled_at": (latest_row or {}).get("filled_at"),
            "latest_raw": (latest_row or {}).get("raw", latest_row or {}),
        }

    def get_order_by_id(self, order_id: str) -> dict[str, Any] | None:
        if not self.enabled or not order_id:
            return None
        try:
            payload = self._request("GET", f"{self.base_url}/v2/orders/{order_id}")
            if not isinstance(payload, dict):
                return None
            return {
                "id": payload.get("id"),
                "status": payload.get("status", "unknown"),
                "filled_avg_price": payload.get("filled_avg_price"),
                "qty": _to_float(payload.get("qty"), 0.0),
                "filled_qty": _to_float(payload.get("filled_qty"), 0.0),
                "filled_at": payload.get("filled_at"),
                "submitted_at": payload.get("submitted_at"),
                "symbol": payload.get("symbol"),
                "side": payload.get("side"),
                "source": "ALPACA",
                "raw": payload,
            }
        except Exception:
            return None

    def resolve_order_final_state(
        self,
        order_payload: dict[str, Any],
        max_wait_seconds: int = ORDER_STATUS_MAX_WAIT_SECONDS,
        poll_interval_seconds: float = ORDER_STATUS_POLL_INTERVAL_SECONDS,
    ) -> dict[str, Any]:
        if not self.enabled:
            return order_payload
        if not isinstance(order_payload, dict):
            return {}

        order_id = str(order_payload.get("id") or "")
        if not order_id:
            return order_payload

        final_states = {
            "filled",
            "partially_filled",
            "canceled",
            "cancelled",
            "expired",
            "rejected",
            "done_for_day",
            "stopped",
            "suspended",
            "calculated",
            "error",
        }
        state = str(order_payload.get("status", "unknown")).lower()
        if state in final_states:
            return order_payload

        deadline = time_module.time() + max(1, int(max_wait_seconds))
        interval = max(float(poll_interval_seconds), 0.2)
        latest = order_payload
        while time_module.time() < deadline:
            polled = self.get_order_by_id(order_id)
            if polled:
                latest = polled
                state = str(polled.get("status", "unknown")).lower()
                if state in final_states:
                    return polled
            time_module.sleep(interval)
        return latest

    def has_open_order(self, symbol: str, side: str | None = None) -> bool:
        symbol_key = str(symbol or "").upper()
        side_key = str(side or "").lower()
        for order in self.get_open_orders():
            if symbol_key and str(order.get("symbol", "")).upper() != symbol_key:
                continue
            if side_key and str(order.get("side", "")).lower() != side_key:
                continue
            return True
        return False

    @staticmethod
    def _parse_hhmm(value: str) -> time:
        hh, mm = value.split(":")
        return time(hour=int(hh), minute=int(mm))

    def _fallback_market_window_check(self) -> tuple[bool, str]:
        tz = ZoneInfo(MARKET_HOURS_FALLBACK_TZ)
        now_local = datetime.now(tz)
        start_t = self._parse_hhmm(MARKET_HOURS_FALLBACK_START)
        end_t = self._parse_hhmm(MARKET_HOURS_FALLBACK_END)
        now_t = now_local.time()

        crosses_midnight = start_t > end_t
        if not crosses_midnight:
            in_time = start_t <= now_t <= end_t
            session_day = now_local.weekday()
        else:
            in_time = (now_t >= start_t) or (now_t <= end_t)
            if now_t >= start_t:
                session_day = now_local.weekday()
            else:
                session_day = (now_local - timedelta(days=1)).weekday()

        in_weekday = session_day in set(MARKET_HOURS_FALLBACK_SESSION_WEEKDAYS)
        is_open = in_time and in_weekday
        note = (
            f"now={now_local.isoformat()} start={MARKET_HOURS_FALLBACK_START} "
            f"end={MARKET_HOURS_FALLBACK_END} session_day={session_day}"
        )
        return is_open, note

    @staticmethod
    def _synthetic_daily_bars(symbol: str, limit: int) -> list[dict[str, Any]]:
        seed = sum(ord(c) for c in symbol.upper()) or 100
        base_price = float(20 + (seed % 180))
        now = datetime.now(timezone.utc)

        bars: list[dict[str, Any]] = []
        last_close = base_price
        for i in range(limit):
            age = limit - i
            drift = 0.0008 * (1 + (seed % 5) / 10.0)
            wave = math.sin((i + seed) / 7.0) * 0.008
            change = drift + wave
            close = max(1.0, last_close * (1 + change))
            high = close * 1.01
            low = close * 0.99
            open_price = (last_close + close) / 2
            volume = float(900_000 + ((seed * (i + 3)) % 900_000))
            bar_time = (now - timedelta(days=age)).isoformat()

            bars.append(
                {
                    "t": bar_time,
                    "o": round(open_price, 4),
                    "h": round(high, 4),
                    "l": round(low, 4),
                    "c": round(close, 4),
                    "v": int(volume),
                }
            )
            last_close = close
        return bars
