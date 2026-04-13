from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from app_settings import (
    BUY_CHECK_INTERVAL_SECONDS,
    MARKET_HOURS_FALLBACK_END,
    MARKET_HOURS_FALLBACK_SESSION_WEEKDAYS,
    MARKET_HOURS_FALLBACK_START,
    MARKET_HOURS_FALLBACK_TZ,
    SELL_CHECK_INTERVAL_SECONDS,
)
from db_settings import DB_PATH
from main import initialize_database
from repository.order_repository import fetch_recent_order_logs, fetch_trade_statistics
from repository.run_repository import fetch_recent_runs, fetch_run_summary

try:
    import psutil  # type: ignore
except Exception:
    psutil = None


SERVER_STARTED_AT = time.time()
_SYSTEM_CACHE_LOCK = threading.Lock()
_SYSTEM_CACHE: dict[str, Any] = {"fetched_at": 0.0, "payload": {}}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_iso_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_hhmm(value: str) -> tuple[int, int]:
    hh, mm = str(value).strip().split(":")
    return int(hh), int(mm)


def _read_raspi_metrics() -> dict[str, Any]:
    temperature_c: float | None = None
    cpu_clock_hz: int | None = None
    throttled_hex: str | None = None

    temp_file = Path("/sys/class/thermal/thermal_zone0/temp")
    if temp_file.exists():
        try:
            raw = temp_file.read_text(encoding="utf-8").strip()
            milli_c = float(raw)
            temperature_c = round(milli_c / 1000.0, 2)
        except Exception:
            temperature_c = None

    vcgencmd = shutil.which("vcgencmd")
    if vcgencmd:
        try:
            proc = subprocess.run(
                [vcgencmd, "measure_clock", "arm"],
                capture_output=True,
                text=True,
                timeout=1.0,
                check=False,
            )
            text = proc.stdout.strip()
            if "=" in text:
                cpu_clock_hz = _to_int(text.split("=", 1)[1].strip(), 0) or None
        except Exception:
            cpu_clock_hz = None

        try:
            proc = subprocess.run(
                [vcgencmd, "get_throttled"],
                capture_output=True,
                text=True,
                timeout=1.0,
                check=False,
            )
            text = proc.stdout.strip()
            if "=" in text:
                throttled_hex = text.split("=", 1)[1].strip()
        except Exception:
            throttled_hex = None

    return {
        "temperature_c": temperature_c,
        "cpu_clock_hz": cpu_clock_hz,
        "throttled_hex": throttled_hex,
        "is_raspi_metrics_available": bool(temp_file.exists() or vcgencmd),
    }


def _collect_system_metrics() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "now_utc": _utc_now_iso(),
        "monitor_uptime_sec": round(max(time.time() - SERVER_STARTED_AT, 0.0), 3),
        "platform": os.name,
        "cpu_count": os.cpu_count(),
    }

    if psutil is not None:
        try:
            payload["cpu_percent"] = round(float(psutil.cpu_percent(interval=0.0)), 2)
        except Exception:
            pass
        try:
            vm = psutil.virtual_memory()
            payload["memory_percent"] = round(float(vm.percent), 2)
            payload["memory_used_mb"] = round(float(vm.used) / (1024 * 1024), 2)
            payload["memory_total_mb"] = round(float(vm.total) / (1024 * 1024), 2)
        except Exception:
            pass
        try:
            du = psutil.disk_usage(str(DB_PATH.parent))
            payload["disk_percent"] = round(float(du.percent), 2)
            payload["disk_used_gb"] = round(float(du.used) / (1024**3), 3)
            payload["disk_total_gb"] = round(float(du.total) / (1024**3), 3)
        except Exception:
            pass
        try:
            payload["system_uptime_sec"] = round(time.time() - float(psutil.boot_time()), 3)
        except Exception:
            pass
    else:
        try:
            du = shutil.disk_usage(str(DB_PATH.parent))
            payload["disk_used_gb"] = round(float(du.used) / (1024**3), 3)
            payload["disk_total_gb"] = round(float(du.total) / (1024**3), 3)
            if du.total > 0:
                payload["disk_percent"] = round((float(du.used) / float(du.total)) * 100.0, 2)
        except Exception:
            pass

    payload.update(_read_raspi_metrics())
    return payload


def _get_system_metrics_cached(ttl_sec: float = 5.0) -> dict[str, Any]:
    now = time.time()
    with _SYSTEM_CACHE_LOCK:
        fetched_at = _to_float(_SYSTEM_CACHE.get("fetched_at"), 0.0)
        if (now - fetched_at) < max(ttl_sec, 0.5):
            return dict(_SYSTEM_CACHE.get("payload", {}))

        payload = _collect_system_metrics()
        _SYSTEM_CACHE["fetched_at"] = now
        _SYSTEM_CACHE["payload"] = payload
        return dict(payload)


def _fetch_open_trade_overview(db_path: Path) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS open_trade_count,
                COALESCE(SUM(invested_amount), 0.0) AS invested_amount_total,
                COALESCE(SUM(qty), 0.0) AS total_qty
            FROM trades
            WHERE status = 'OPEN'
            """
        ).fetchone()
        open_trade_count = _to_int(row[0], 0) if row else 0
        invested_amount_total = _to_float(row[1], 0.0) if row else 0.0
        total_qty = _to_float(row[2], 0.0) if row else 0.0

        urow = conn.execute(
            """
            WITH latest AS (
                SELECT symbol, MAX(id) AS max_id
                FROM position_snapshots
                GROUP BY symbol
            )
            SELECT COALESCE(SUM(ps.unrealized_pl), 0.0)
            FROM latest
            JOIN position_snapshots ps ON ps.id = latest.max_id
            """
        ).fetchone()
        latest_unrealized_total = _to_float(urow[0], 0.0) if urow else 0.0

        return {
            "open_trade_count": open_trade_count,
            "invested_amount_total": round(invested_amount_total, 4),
            "total_qty": round(total_qty, 6),
            "latest_unrealized_total": round(latest_unrealized_total, 4),
        }


def _fetch_trades_wins_timeseries(db_path: Path, limit: int = 240) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT closed_at, gross_pnl_amount
            FROM (
                SELECT closed_at, gross_pnl_amount
                FROM trades
                WHERE status = 'CLOSED'
                  AND closed_at IS NOT NULL
                ORDER BY datetime(closed_at) DESC
                LIMIT ?
            ) t
            ORDER BY datetime(closed_at) ASC
            """,
            (max(int(limit), 1),),
        ).fetchall()

    trades = 0
    wins = 0
    for row in rows:
        closed_at = str(row[0] or "")
        pnl = _to_float(row[1], 0.0)
        trades += 1
        if pnl > 0:
            wins += 1
        points.append(
            {
                "t": closed_at,
                "trades": trades,
                "wins": wins,
            }
        )
    return points


def _fetch_position_pie(db_path: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH latest AS (
                SELECT symbol, MAX(id) AS max_id
                FROM position_snapshots
                GROUP BY symbol
            ),
            latest_snap AS (
                SELECT ps.symbol, ps.market_value
                FROM latest
                JOIN position_snapshots ps ON ps.id = latest.max_id
            )
            SELECT
                t.symbol,
                COALESCE(latest_snap.market_value, (t.qty * t.entry_price), t.invested_amount, 0.0) AS value
            FROM trades t
            LEFT JOIN latest_snap ON latest_snap.symbol = t.symbol
            WHERE t.status = 'OPEN'
            """
        ).fetchall()

    result: list[dict[str, Any]] = []
    total = 0.0
    for symbol, value in rows:
        val = _to_float(value, 0.0)
        if val <= 0:
            continue
        total += val
        result.append({"symbol": str(symbol or ""), "value": round(val, 4)})

    if total <= 0:
        return []
    for row in result:
        row["ratio_pct"] = round((float(row["value"]) / total) * 100.0, 2)
    result.sort(key=lambda x: float(x.get("value", 0.0)), reverse=True)
    return result


def _fetch_open_position_rows(db_path: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH latest AS (
                SELECT symbol, MAX(id) AS max_id
                FROM position_snapshots
                GROUP BY symbol
            ),
            latest_snap AS (
                SELECT
                    ps.symbol,
                    ps.market_price,
                    ps.market_value,
                    ps.unrealized_pl,
                    ps.captured_at
                FROM latest
                JOIN position_snapshots ps ON ps.id = latest.max_id
            )
            SELECT
                t.symbol,
                t.qty,
                t.entry_price,
                COALESCE(latest_snap.market_price, t.entry_price, 0.0) AS current_price,
                COALESCE(latest_snap.market_value, t.qty * COALESCE(latest_snap.market_price, t.entry_price, 0.0), 0.0) AS market_value,
                COALESCE(
                    latest_snap.unrealized_pl,
                    (COALESCE(latest_snap.market_price, t.entry_price, 0.0) - t.entry_price) * t.qty,
                    0.0
                ) AS unrealized_pl,
                COALESCE(latest_snap.captured_at, '') AS snapshot_at
            FROM trades t
            LEFT JOIN latest_snap ON latest_snap.symbol = t.symbol
            WHERE t.status = 'OPEN'
            ORDER BY t.symbol ASC
            """
        ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row[0] or "")
        qty = _to_float(row[1], 0.0)
        entry_price = _to_float(row[2], 0.0)
        current_price = _to_float(row[3], 0.0)
        market_value = _to_float(row[4], 0.0)
        unrealized_pl = _to_float(row[5], 0.0)
        snapshot_at = str(row[6] or "")
        unrealized_pct = 0.0
        if entry_price > 0 and current_price > 0:
            unrealized_pct = ((current_price - entry_price) / entry_price) * 100.0
        result.append(
            {
                "symbol": symbol,
                "qty": round(qty, 6),
                "entry_price": round(entry_price, 4),
                "current_price": round(current_price, 4),
                "market_value": round(market_value, 4),
                "unrealized_pl": round(unrealized_pl, 4),
                "unrealized_pct": round(unrealized_pct, 4),
                "snapshot_at": snapshot_at,
            }
        )
    return result


def _fetch_latest_connectivity(db_path: Path) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, finished_at, result_json
            FROM run_history
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    if not row:
        return {
            "run_id": None,
            "finished_at": "",
            "alpaca_api": "UNKNOWN",
            "anthropic_api": "UNKNOWN",
            "finviz_web": "UNKNOWN",
        }

    run_id = _to_int(row[0], 0)
    finished_at = str(row[1] or "")
    raw_json = str(row[2] or "{}")
    payload: dict[str, Any] = {}
    try:
        parsed = json.loads(raw_json)
        if isinstance(parsed, dict):
            payload = parsed
    except Exception:
        payload = {}

    conn_status = payload.get("connectivity", {})
    if not isinstance(conn_status, dict):
        conn_status = {}

    def _norm(v: Any) -> str:
        text = str(v or "").strip().upper()
        if text in {"OK", "NG", "UNKNOWN"}:
            return text
        return "UNKNOWN"

    return {
        "run_id": run_id,
        "finished_at": finished_at,
        "alpaca_api": _norm(conn_status.get("alpaca_api")),
        "anthropic_api": _norm(conn_status.get("anthropic_api")),
        "finviz_web": _norm(conn_status.get("finviz_web")),
    }


def _build_market_timing_jst() -> dict[str, Any]:
    tz = ZoneInfo(MARKET_HOURS_FALLBACK_TZ or "Asia/Tokyo")
    now = datetime.now(tz)
    start_h, start_m = _parse_hhmm(MARKET_HOURS_FALLBACK_START)
    end_h, end_m = _parse_hhmm(MARKET_HOURS_FALLBACK_END)
    start_tuple = (start_h, start_m, 0, 0)
    end_tuple = (end_h, end_m, 0, 0)
    session_weekdays = set(int(v) for v in MARKET_HOURS_FALLBACK_SESSION_WEEKDAYS)
    now_tuple = (now.hour, now.minute, now.second, now.microsecond)
    crosses_midnight = (start_h, start_m) > (end_h, end_m)

    if not crosses_midnight:
        in_time = start_tuple <= now_tuple <= end_tuple
        session_day = now.weekday()
    else:
        in_time = (now_tuple >= start_tuple) or (now_tuple <= end_tuple)
        session_day = now.weekday() if now_tuple >= start_tuple else (now.weekday() - 1) % 7
    in_weekday = session_day in session_weekdays
    is_open = bool(in_time and in_weekday)

    if is_open:
        if crosses_midnight and now_tuple >= start_tuple:
            target = (now.replace(hour=end_h, minute=end_m, second=0, microsecond=0) + timedelta(days=1))
        else:
            target = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
        return {
            "state": "OPEN",
            "now_jst": now.isoformat(),
            "target_jst": target.isoformat(),
            "target_label": "close",
        }

    # CLOSED: 次の開場時刻を探索
    target: datetime | None = None
    for add_days in range(0, 8):
        day = now + timedelta(days=add_days)
        if day.weekday() not in session_weekdays:
            continue
        candidate = day.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
        if candidate <= now:
            continue
        target = candidate
        break
    if target is None:
        target = now + timedelta(days=1)

    return {
        "state": "CLOSED",
        "now_jst": now.isoformat(),
        "target_jst": target.isoformat(),
        "target_label": "open",
    }


def _get_running_main_started_utc() -> datetime | None:
    running_started: datetime | None = None
    if psutil is not None:
        try:
            for p in psutil.process_iter(attrs=["cmdline", "create_time"]):
                cmdline = p.info.get("cmdline") or []
                text = " ".join(str(x) for x in cmdline)
                if "main.py" in text and "trading_bot" in text:
                    created = _to_float(p.info.get("create_time"), 0.0)
                    if created > 0:
                        dt = datetime.fromtimestamp(created, tz=timezone.utc)
                        if running_started is None or dt > running_started:
                            running_started = dt
        except Exception:
            running_started = None
    return running_started


def _fetch_runtime_state_float(db_path: Path, state_key: str) -> float | None:
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
        return None
    try:
        return float(row[0])
    except (TypeError, ValueError):
        return None


def _build_task_timing(
    db_path: Path,
    interval_sec: int,
    running_started: datetime | None = None,
    runtime_state_key: str | None = None,
) -> dict[str, Any]:
    interval_sec = max(int(interval_sec), 1)
    now_utc = datetime.now(timezone.utc)

    if running_started is not None:
        return {
            "state": "RUNNING",
            "now_utc": _to_iso_z(now_utc),
            "target_at": _to_iso_z(running_started),
            "interval_sec": interval_sec,
        }

    started_dt: datetime | None = None
    if runtime_state_key:
        epoch = _fetch_runtime_state_float(db_path, runtime_state_key)
        if epoch is not None and epoch > 0:
            started_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)

    if started_dt is None:
        latest_runs = fetch_recent_runs(db_path, limit=1)
        latest = latest_runs[0] if latest_runs else {}
        started_dt = _parse_iso_utc(latest.get("started_at")) if latest else None

    if started_dt is None:
        started_dt = now_utc
    next_dt = started_dt + timedelta(seconds=interval_sec)
    if next_dt <= now_utc:
        elapsed_sec = max((now_utc - started_dt).total_seconds(), 0.0)
        steps = int(elapsed_sec // interval_sec) + 1
        next_dt = started_dt + timedelta(seconds=steps * interval_sec)
    return {
        "state": "WAITING",
        "now_utc": _to_iso_z(now_utc),
        "target_at": _to_iso_z(next_dt),
        "interval_sec": interval_sec,
    }


def build_charts(db_path: Path, limit: int = 240) -> dict[str, Any]:
    return {
        "generated_at": _utc_now_iso(),
        "trades_wins_series": _fetch_trades_wins_timeseries(db_path, limit=limit),
        "position_pie": _fetch_position_pie(db_path),
    }


def build_summary(db_path: Path) -> dict[str, Any]:
    stats = fetch_trade_statistics(db_path)
    run_summary = fetch_run_summary(db_path)
    open_overview = _fetch_open_trade_overview(db_path)
    system = _get_system_metrics_cached(ttl_sec=5.0)
    last_connectivity = _fetch_latest_connectivity(db_path)
    market_timing = _build_market_timing_jst()
    running_started = _get_running_main_started_utc()
    buy_timing = _build_task_timing(
        db_path=db_path,
        interval_sec=BUY_CHECK_INTERVAL_SECONDS,
        running_started=running_started,
        runtime_state_key="last_buy_check_at",
    )
    sell_timing = _build_task_timing(
        db_path=db_path,
        interval_sec=SELL_CHECK_INTERVAL_SECONDS,
        running_started=running_started,
        runtime_state_key=None,
    )
    return {
        "generated_at": _utc_now_iso(),
        "db_path": str(db_path),
        "stats": stats,
        "run_summary": run_summary,
        "open_overview": open_overview,
        "open_positions": _fetch_open_position_rows(db_path),
        "system": system,
        "last_connectivity": last_connectivity,
        "market_timing": market_timing,
        "buy_timing": buy_timing,
        "sell_timing": sell_timing,
        "run_timing": buy_timing,
    }


def build_system_only() -> dict[str, Any]:
    return {
        "generated_at": _utc_now_iso(),
        "system": _get_system_metrics_cached(ttl_sec=2.0),
    }


def _parse_limit(query: str, default: int, max_value: int = 200) -> int:
    params = parse_qs(query)
    raw = params.get("limit", [str(default)])[0]
    value = _to_int(raw, default)
    return max(1, min(value, max_value))


HTML_PAGE = """<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Trading Bot Monitor</title>
  <style>
    body { font-family: Consolas, "Courier New", monospace; margin: 16px; background: #0b0f0b; color:#d8ffd8; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: stretch; }
    .card { background:#121812; border:1px solid #5fae5f; border-radius:10px; padding:12px; box-shadow: 0 0 10px rgba(70, 180, 70, 0.16); }
    .card h3 { margin:0 0 8px 0; font-size:14px; }
    .kpi { width: 210px; min-height: 44px; height: auto; flex: 0 0 210px; padding:2px 8px; display:flex; flex-direction:column; justify-content:flex-start; }
    .kpi h3 { margin:0 0 1px 0; font-size:12px; line-height:1.15; padding-top:4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .kpi .mono { font-size:13px; line-height:1.15; padding-top:4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    table { width:100%; border-collapse: collapse; font-size: 12px; }
    th, td { border-bottom:1px solid #3e703e; text-align:left; padding:6px; vertical-align: top; color:#d8ffd8; }
    th { background:#1b271b; color:#eaffea; }
    .small { font-size: 11px; color:#bfffbf; }
    .mono { font-family: Consolas, monospace; }
    a { color:#cfffce; }
    canvas { width: 100%; height: 220px; background:#0d140d; border:1px solid #355c35; border-radius: 8px; }
    .legend-item { margin-right: 12px; display: inline-block; }
    .sw { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:4px; vertical-align: middle; }
    .charts-row { display:flex; gap:10px; flex-wrap:wrap; margin-top:10px; }
    .chart-box { flex:1; min-width:280px; }
    .chart-title { margin:0 0 6px 0; font-size:12px; color:#dfffdc; }
    .status-row { display:flex; align-items:center; margin:1px 0; font-size:11px; line-height:1.2; }
    .conn-label { display:inline-block; width:96px; }
    .status-dot { width:10px; height:10px; margin-right:6px; border-radius:2px; display:inline-block; }
    .dot-yellow { background:#ffe066; }
    .dot-orange { background:#ff9f43; }
    .dot-purple { background:#b085ff; }
    .status-ok { color:#cfffce; }
    .status-ng { color:#ffb0b0; }
    .status-unknown { color:#d7d7a9; }
    .topbar { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; }
    .topbar h2 { margin: 0; }
    .top-right { display:flex; gap:10px; flex-wrap:wrap; align-items:stretch; }
    .top-market, .top-buy, .top-sell, .top-conn { border:1px solid #5fae5f; border-radius:8px; padding:6px 8px; background:#121812; min-width: 240px; }
    .top-conn-title { font-size:11px; margin-bottom:4px; color:#dfffdc; }
    .market-open { color:#bfffbf; }
    .market-closed { color:#ffcb8b; }
    .run-running { color:#bfffbf; }
    .run-waiting { color:#cfe3ff; }
  </style>
</head>
<body>
  <div class="topbar">
    <h2>Trading Bot Monitor</h2>
    <div class="top-right">
      <div class="top-market">
        <div class="top-conn-title">Market Status (JST)</div>
        <div class="status-row"><span>Market: <span id="market-state" class="mono market-closed">CLOSED</span></span></div>
        <div class="status-row"><span id="market-countdown" class="mono">開場まで --:--:--.---</span></div>
        <div class="status-row"><span id="market-target" class="mono small">target: -</span></div>
      </div>
      <div class="top-buy">
        <div class="top-conn-title">Buy Status</div>
        <div class="status-row"><span>Buy: <span id="buy-state" class="mono run-waiting">WAITING</span></span></div>
        <div class="status-row"><span id="buy-countdown" class="mono">次回まで --:--:--.---</span></div>
        <div class="status-row"><span id="buy-interval" class="mono small">interval: 180s</span></div>
      </div>
      <div class="top-sell">
        <div class="top-conn-title">Sell Status</div>
        <div class="status-row"><span>Sell: <span id="sell-state" class="mono run-waiting">WAITING</span></span></div>
        <div class="status-row"><span id="sell-countdown" class="mono">次回まで --:--:--.---</span></div>
        <div class="status-row"><span id="sell-interval" class="mono small">interval: 60s</span></div>
      </div>
      <div class="top-conn">
        <div class="top-conn-title">接続状態（最終実行）</div>
        <div class="status-row"><span class="status-dot dot-yellow"></span><span><span class="conn-label">Alpaca APIs</span>: <span id="conn-alpaca" class="mono status-unknown">UNKNOWN</span></span></div>
        <div class="status-row"><span class="status-dot dot-orange"></span><span><span class="conn-label">Anthropic API</span>: <span id="conn-anthropic" class="mono status-unknown">UNKNOWN</span></span></div>
        <div class="status-row"><span class="status-dot dot-purple"></span><span><span class="conn-label">Finviz Web</span>: <span id="conn-finviz" class="mono status-unknown">UNKNOWN</span></span></div>
      </div>
    </div>
  </div>
  <div class="small">自動更新: 全体5秒 / System 2秒（ms表示）</div>
  <div class="row" id="kpi-row"></div>
  <div class="row" style="margin-top:10px;">
    <div class="card" style="flex:1; min-width:360px;">
      <h3>統計（total/day/week/month）</h3>
      <table>
        <thead>
          <tr>
            <th>期間</th><th>取引数</th><th>勝ち数</th><th>勝率(%)</th><th>損益額</th><th>平均損益率(%)</th>
          </tr>
        </thead>
        <tbody id="stats-body"></tbody>
      </table>
      <div class="charts-row">
        <div class="chart-box">
          <div class="chart-title">Position Pie (Current OPEN)</div>
          <canvas id="position-pie" width="420" height="220"></canvas>
          <div id="position-legend" class="small" style="margin-top:8px;"></div>
        </div>
        <div class="chart-box">
          <div class="chart-title">Trades / Wins (Time Series)</div>
          <canvas id="trades-line" width="420" height="220"></canvas>
          <div class="small" style="margin-top:8px;">
            <span class="legend-item"><span class="sw" style="background:#6de36d;"></span>Trades</span>
            <span class="legend-item"><span class="sw" style="background:#ffd166;"></span>Wins</span>
          </div>
        </div>
      </div>
    </div>
    <div class="card" style="flex:1; min-width:360px;">
      <h3>System</h3>
      <table>
        <thead>
          <tr><th>項目</th><th>値</th></tr>
        </thead>
        <tbody id="system-body"></tbody>
      </table>
    </div>
  </div>
  <div class="card" style="margin-top:10px;">
    <h3>直近Run</h3>
    <table>
      <thead>
        <tr>
          <th>ID</th><th>finished_at(JST)</th><th>status</th><th>label</th><th>mode</th><th>dur(s)</th>
          <th>bought</th><th>sold</th><th>filled</th><th>note</th>
        </tr>
      </thead>
      <tbody id="runs-body"></tbody>
    </table>
  </div>
  <div class="card" style="margin-top:10px;">
    <h3>直近Order Logs</h3>
    <table>
      <thead>
        <tr>
          <th>event_at</th><th>symbol</th><th>side</th><th>event</th><th>qty</th><th>price</th><th>pnl%</th><th>note</th>
        </tr>
      </thead>
      <tbody id="logs-body"></tbody>
    </table>
  </div>
  <div class="card" style="margin-top:10px;">
    <h3>Open Positions（購入価格 / 現在価格）</h3>
    <table>
      <thead>
        <tr>
          <th>symbol</th><th>qty</th><th>entry</th><th>current</th><th>uPnL</th><th>uPnL(%)</th><th>snapshot(JST)</th>
        </tr>
      </thead>
      <tbody id="positions-body"></tbody>
    </table>
  </div>
<script>
async function getJson(url) {
  const r = await fetch(url, {cache: "no-store"});
  if (!r.ok) throw new Error(url + " " + r.status);
  return r.json();
}
function esc(v) { return String(v ?? ""); }
function num(v, d=4) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(d) : "";
}
function parseMs(v) {
  const t = new Date(v || "").getTime();
  return Number.isFinite(t) ? t : null;
}
function fmtMs(ms) {
  const n = Math.max(0, Math.floor(Number(ms || 0)));
  const hh = Math.floor(n / 3600000);
  const mm = Math.floor((n % 3600000) / 60000);
  const ss = Math.floor((n % 60000) / 1000);
  const mmm = n % 1000;
  return `${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}:${String(ss).padStart(2, "0")}.${String(mmm).padStart(3, "0")}`;
}
function fmtJstLabel(iso) {
  const t = parseMs(iso);
  if (t === null) return "-";
  const d = new Date(t);
  const fmt = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  return fmt.format(d).split("/").join("-");
}
function fmtJstDateTime(iso) {
  const t = parseMs(iso);
  if (t === null) return "";
  const d = new Date(t);
  const fmt = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  return fmt.format(d).replaceAll("/", "-") + " JST";
}
function fmtUtcDateTime(iso) {
  const t = parseMs(iso);
  if (t === null) return "";
  const d = new Date(t);
  const fmt = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "UTC",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  return fmt.format(d).replaceAll("/", "-") + " UTC";
}
function fmtUtcDateTimeMs(ms) {
  const d = new Date(ms);
  const fmt = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "UTC",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  return fmt.format(d).replaceAll("/", "-") + " UTC";
}
function fmtJtcDateTimeMs(ms) {
  const d = new Date(ms);
  const fmt = new Intl.DateTimeFormat("ja-JP", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  return fmt.format(d).replaceAll("/", "-") + " JTC";
}
function fmtDurationSecMs(seconds) {
  const s = Number(seconds || 0);
  const ms = Math.max(0, Math.floor(s * 1000));
  return fmtMs(ms);
}
function statusClass(v) {
  const t = String(v || "UNKNOWN").toUpperCase();
  if (t === "OK") return "status-ok";
  if (t === "NG") return "status-ng";
  return "status-unknown";
}
const liveState = {
  market: null,
  buy: null,
  sell: null,
  system: null,
};
function renderKpi(summary) {
  const total = summary?.stats?.total ?? {};
  const run = summary?.run_summary ?? {};
  const open = summary?.open_overview ?? {};
  const cards = [
    ["累計取引数", total.trades ?? 0],
    ["勝率(%)", num(total.win_rate ?? 0, 2)],
    ["累計損益額($)", "$" + num(total.pnl_amount ?? 0, 4)],
    ["実行回数", run.total_runs ?? 0],
    ["最終実行", fmtJstDateTime(run.last_finished_at ?? "")],
    ["保有ポジション数", open.open_trade_count ?? 0],
  ];
  const row = document.getElementById("kpi-row");
  row.innerHTML = cards.map(([k,v]) => `<div class="card kpi"><h3>${esc(k)}</h3><div class="mono">${esc(v)}</div></div>`).join("");
}
function renderStats(stats) {
  const body = document.getElementById("stats-body");
  const periods = [
    ["total", "Total"],
    ["year", "Year"],
    ["half_year", "Half Year"],
    ["quarter", "Quarter"],
    ["month", "Month"],
    ["week", "Week"],
    ["day", "Day"],
  ];
  body.innerHTML = periods.map(([key, label]) => {
    const s = stats?.[key] ?? {};
    return `
      <tr>
        <td>${esc(label)}</td>
        <td>${esc(s.trades ?? 0)}</td>
        <td>${esc(s.wins ?? 0)}</td>
        <td>${num(s.win_rate ?? 0, 2)}</td>
        <td>${num(s.pnl_amount ?? 0, 4)}</td>
        <td>${num(s.avg_pnl_pct ?? 0, 4)}</td>
      </tr>`;
  }).join("");
}
function renderSystem(system) {
  const nowIso = String(system?.now_utc || "");
  const nowMs = parseMs(nowIso);
  const receivedAtMs = Date.now();
  liveState.system = {
    baseNowMs: nowMs,
    baseSystemUptimeSec: Number(system?.system_uptime_sec || 0),
    baseMonitorUptimeSec: Number(system?.monitor_uptime_sec || 0),
    receivedAtMs,
  };

  const body = document.getElementById("system-body");
  const rows = [
    ["現在時刻(UTC)", `<span id="sys-now-utc" class="mono">${esc(fmtUtcDateTime(nowIso))}</span>`],
    ["現在時刻(JTC)", `<span id="sys-now-jtc" class="mono">${esc(fmtJstDateTime(nowIso).replace("JST", "JTC"))}</span>`],
    ["プラットフォーム", system?.platform],
    ["CPUコア数", system?.cpu_count],
    ["CPU使用率(%)", system?.cpu_percent],
    ["メモリ使用率(%)", system?.memory_percent],
    ["メモリ使用(MB)", system?.memory_used_mb],
    ["メモリ合計(MB)", system?.memory_total_mb],
    ["ディスク使用率(%)", system?.disk_percent],
    ["ディスク使用(GB)", system?.disk_used_gb],
    ["ディスク合計(GB)", system?.disk_total_gb],
    ["OS稼働時間(ms)", `<span id="sys-uptime-os" class="mono">${esc(fmtDurationSecMs(system?.system_uptime_sec))}</span>`],
    ["監視Web稼働時間(ms)", `<span id="sys-uptime-monitor" class="mono">${esc(fmtDurationSecMs(system?.monitor_uptime_sec))}</span>`],
    ["CPU温度(℃)", system?.temperature_c],
    ["CPUクロック(Hz)", system?.cpu_clock_hz],
    ["スロットリング状態(hex)", system?.throttled_hex],
    ["RasPiメトリクス取得可否", system?.is_raspi_metrics_available],
  ];
  body.innerHTML = rows.map(([k, v]) => `
    <tr>
      <td>${esc(k)}</td>
      <td class="mono">${typeof v === "string" ? v : esc(v ?? "")}</td>
    </tr>`).join("");
  tickSystemLive();
}
function tickSystemLive() {
  const s = liveState.system;
  if (!s || s.baseNowMs === null) return;
  const nowClientMs = Date.now();
  const elapsedMs = Math.max(0, nowClientMs - Number(s.receivedAtMs || nowClientMs));
  const nowUtcMs = Number(s.baseNowMs) + elapsedMs;
  const osUptimeMs = (Number(s.baseSystemUptimeSec || 0) * 1000) + elapsedMs;
  const monitorUptimeMs = (Number(s.baseMonitorUptimeSec || 0) * 1000) + elapsedMs;

  const nowUtcEl = document.getElementById("sys-now-utc");
  const nowJtcEl = document.getElementById("sys-now-jtc");
  const osUptimeEl = document.getElementById("sys-uptime-os");
  const monUptimeEl = document.getElementById("sys-uptime-monitor");
  if (nowUtcEl) nowUtcEl.textContent = fmtUtcDateTimeMs(nowUtcMs);
  if (nowJtcEl) nowJtcEl.textContent = fmtJtcDateTimeMs(nowUtcMs);
  if (osUptimeEl) osUptimeEl.textContent = fmtMs(osUptimeMs);
  if (monUptimeEl) monUptimeEl.textContent = fmtMs(monitorUptimeMs);
}
function renderOpenPositions(rows) {
  const body = document.getElementById("positions-body");
  const items = Array.isArray(rows) ? rows : [];
  if (!body) return;
  if (!items.length) {
    body.innerHTML = `<tr><td colspan="7">OPEN position はありません</td></tr>`;
    return;
  }
  body.innerHTML = items.map(p => `
    <tr>
      <td>${esc(p.symbol)}</td>
      <td>${esc(p.qty)}</td>
      <td>${num(p.entry_price, 4)}</td>
      <td>${num(p.current_price, 4)}</td>
      <td>${num(p.unrealized_pl, 4)}</td>
      <td>${num(p.unrealized_pct, 4)}</td>
      <td class="mono">${esc(fmtJstDateTime(p.snapshot_at))}</td>
    </tr>`).join("");
}
function setStatusText(el, value) {
  if (!el) return;
  const raw = String(value || "UNKNOWN").toUpperCase();
  const label = raw === "OK" ? "OK✅" : (raw === "NG" ? "NG✖" : "UNKNOWN");
  el.textContent = label;
  el.classList.remove("status-ok", "status-ng", "status-unknown");
  el.classList.add(statusClass(raw));
}
function renderConnectivity(conn) {
  const c = conn || {};
  setStatusText(document.getElementById("conn-alpaca"), c.alpaca_api);
  setStatusText(document.getElementById("conn-anthropic"), c.anthropic_api);
  setStatusText(document.getElementById("conn-finviz"), c.finviz_web);
}
function renderMarketRun(summary) {
  liveState.market = summary?.market_timing ?? null;
  liveState.buy = summary?.buy_timing ?? summary?.run_timing ?? null;
  liveState.sell = summary?.sell_timing ?? null;
  tickMarketRun();
}
function tickMarketRun() {
  const now = Date.now();
  const market = liveState.market || {};
  const buy = liveState.buy || {};
  const sell = liveState.sell || {};

  const marketStateEl = document.getElementById("market-state");
  const marketCountdownEl = document.getElementById("market-countdown");
  const marketTargetEl = document.getElementById("market-target");
  if (marketStateEl && marketCountdownEl && marketTargetEl) {
    const state = String(market?.state || "CLOSED").toUpperCase();
    marketStateEl.textContent = state;
    marketStateEl.classList.remove("market-open", "market-closed");
    marketStateEl.classList.add(state === "OPEN" ? "market-open" : "market-closed");
    const target = parseMs(market?.target_jst);
    if (target === null) {
      marketCountdownEl.textContent = state === "OPEN" ? "閉場まで --:--:--.---" : "開場まで --:--:--.---";
      marketTargetEl.textContent = "target: -";
    } else {
      const remain = Math.max(target - now, 0);
      marketCountdownEl.textContent = state === "OPEN" ? `閉場まで ${fmtMs(remain)}` : `開場まで ${fmtMs(remain)}`;
      marketTargetEl.textContent = `target(JST): ${fmtJstLabel(market?.target_jst)}`;
    }
  }

  function renderTaskStatus(prefix, task, defaultIntervalSec) {
    const stateEl = document.getElementById(`${prefix}-state`);
    const countdownEl = document.getElementById(`${prefix}-countdown`);
    const intervalEl = document.getElementById(`${prefix}-interval`);
    if (!stateEl || !countdownEl) return;

    const state = String(task?.state || "UNKNOWN").toUpperCase();
    stateEl.textContent = state;
    stateEl.classList.remove("run-running", "run-waiting");
    stateEl.classList.add(state === "RUNNING" ? "run-running" : "run-waiting");

    const intervalSec = Number(task?.interval_sec || defaultIntervalSec);
    if (intervalEl) {
      intervalEl.textContent = `interval: ${Number.isFinite(intervalSec) ? intervalSec : defaultIntervalSec}s`;
    }

    const target = parseMs(task?.target_at);
    if (target === null) {
      countdownEl.textContent = "実行情報: --:--:--.---";
    } else if (state === "RUNNING") {
      const elapsed = Math.max(now - target, 0);
      countdownEl.textContent = `実行中 ${fmtMs(elapsed)}`;
    } else {
      const remain = Math.max(target - now, 0);
      countdownEl.textContent = `次回まで ${fmtMs(remain)}`;
    }
  }

  renderTaskStatus("buy", buy, 180);
  renderTaskStatus("sell", sell, 60);
}
function renderRuns(runs) {
  const body = document.getElementById("runs-body");
  function getRunLabel(r) {
    const bought = Number(r?.bought_count || 0);
    const sold = Number(r?.sold_count || 0);
    if (bought > 0 && sold > 0) return "BUY+SELL run";
    if (bought > 0) return "BUY run";
    if (sold > 0) return "SELL run";
    return "NO_FILL";
  }
  body.innerHTML = (runs || []).map(r => `
    <tr>
      <td>${esc(r.id)}</td>
      <td class="mono">${esc(fmtJstDateTime(r.finished_at))}</td>
      <td>${esc(r.status)}</td>
      <td>${esc(getRunLabel(r))}</td>
      <td>${esc(r.execution_mode)}</td>
      <td>${num(r.duration_sec, 3)}</td>
      <td>${esc(r.bought_count)}</td>
      <td>${esc(r.sold_count)}</td>
      <td>${esc(r.filled_count)}</td>
      <td>${esc(r.note)}</td>
    </tr>`).join("");
}
function renderLogs(logs) {
  const body = document.getElementById("logs-body");
  body.innerHTML = (logs || []).map(l => `
    <tr>
      <td class="mono">${esc(l.event_at)}</td>
      <td>${esc(l.symbol)}</td>
      <td>${esc(l.side)}</td>
      <td>${esc(l.event_type)}</td>
      <td>${esc(l.filled_qty)}</td>
      <td>${esc(l.avg_fill_price)}</td>
      <td>${esc(l.capital_change_pct)}</td>
      <td>${esc(l.note)}</td>
    </tr>`).join("");
}
function drawPie(canvasId, legendId, data) {
  const canvas = document.getElementById(canvasId);
  const legend = document.getElementById(legendId);
  if (!canvas || !legend) return;
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  const items = (data || []).filter(x => Number(x?.value) > 0);
  if (!items.length) {
    ctx.fillStyle = "#bfffbf";
    ctx.font = "13px Consolas";
    ctx.fillText("OPEN position がありません", 20, h / 2);
    legend.innerHTML = "";
    return;
  }

  const colors = ["#77e377", "#ffd166", "#7ce3ff", "#ff9f9f", "#c8a3ff", "#98f5b5", "#f7e26b"];
  const cx = Math.floor(w * 0.5);
  const cy = Math.floor(h * 0.5);
  const radius = Math.min(w, h) * 0.33;
  const total = items.reduce((s, x) => s + Number(x.value || 0), 0);
  let start = -Math.PI / 2;

  items.forEach((it, idx) => {
    const v = Number(it.value || 0);
    const ratio = total > 0 ? v / total : 0;
    const end = start + (Math.PI * 2 * ratio);
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, radius, start, end);
    ctx.closePath();
    ctx.fillStyle = colors[idx % colors.length];
    ctx.fill();
    start = end;
  });

  ctx.beginPath();
  ctx.arc(cx, cy, radius * 0.55, 0, Math.PI * 2);
  ctx.fillStyle = "#0d140d";
  ctx.fill();
  ctx.fillStyle = "#d8ffd8";
  ctx.font = "bold 14px Consolas";
  ctx.fillText("OPEN", cx - 18, cy - 4);
  ctx.font = "12px Consolas";
  ctx.fillText(String(items.length) + " symbols", cx - 30, cy + 14);

  legend.innerHTML = items.map((it, idx) => `
    <span class="legend-item">
      <span class="sw" style="background:${colors[idx % colors.length]}"></span>
      ${esc(it.symbol)} ${num(it.ratio_pct, 2)}%
    </span>
  `).join("");
}
function drawLineChart(canvasId, series) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  const points = (series || []).filter(x => x && x.t);
  if (!points.length) {
    ctx.fillStyle = "#bfffbf";
    ctx.font = "13px Consolas";
    ctx.fillText("CLOSED trades がありません", 20, h / 2);
    return;
  }

  const padL = 48, padR = 18, padT = 18, padB = 32;
  const chartW = w - padL - padR;
  const chartH = h - padT - padB;
  const maxY = Math.max(...points.map(p => Math.max(Number(p.trades || 0), Number(p.wins || 0))), 1);
  const minT = new Date(points[0].t).getTime();
  const maxT = new Date(points[points.length - 1].t).getTime();
  const spanT = Math.max(maxT - minT, 1);

  function sx(t) { return padL + ((t - minT) / spanT) * chartW; }
  function sy(v) { return padT + chartH - (Math.max(0, Number(v || 0)) / maxY) * chartH; }

  ctx.strokeStyle = "#355c35";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padL, padT);
  ctx.lineTo(padL, padT + chartH);
  ctx.lineTo(padL + chartW, padT + chartH);
  ctx.stroke();

  ctx.fillStyle = "#9bc89b";
  ctx.font = "11px Consolas";
  ctx.fillText("0", 18, padT + chartH + 3);
  ctx.fillText(String(maxY), 12, padT + 4);

  function drawSeries(key, color) {
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    points.forEach((p, idx) => {
      const t = new Date(p.t).getTime();
      const x = sx(t);
      const y = sy(p[key]);
      if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
  }
  drawSeries("trades", "#6de36d");
  drawSeries("wins", "#ffd166");

  const tStart = points[0].t || "";
  const tEnd = points[points.length - 1].t || "";
  ctx.fillStyle = "#9bc89b";
  ctx.font = "11px Consolas";
  ctx.fillText(String(tStart).replace("T", " ").slice(0, 16), padL, h - 8);
  const endLabel = String(tEnd).replace("T", " ").slice(0, 16);
  const tw = ctx.measureText(endLabel).width;
  ctx.fillText(endLabel, w - padR - tw, h - 8);
}
async function refresh() {
  try {
    const [summary, runs, logs, charts] = await Promise.all([
      getJson("/api/summary"),
      getJson("/api/runs?limit=25"),
      getJson("/api/logs?limit=25"),
      getJson("/api/charts?limit=240"),
    ]);
    renderKpi(summary);
    renderConnectivity(summary.last_connectivity);
    renderMarketRun(summary);
    renderStats(summary.stats);
    renderSystem(summary.system);
    renderOpenPositions(summary.open_positions);
    drawPie("position-pie", "position-legend", charts.position_pie || []);
    drawLineChart("trades-line", charts.trades_wins_series || []);
    renderRuns(runs.runs);
    renderLogs(logs.logs);
  } catch (e) {
    console.error(e);
  }
}
async function refreshSystemOnly() {
  try {
    const payload = await getJson("/api/system");
    renderSystem(payload?.system ?? {});
  } catch (e) {
    console.error(e);
  }
}
refresh();
setInterval(tickMarketRun, 100);
setInterval(tickSystemLive, 100);
setInterval(refreshSystemOnly, 2000);
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


class MonitorHandler(BaseHTTPRequestHandler):
    db_path = DB_PATH

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self, html: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = html.encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/":
                self._send_html(HTML_PAGE)
                return
            if path == "/api/summary":
                self._send_json(build_summary(self.db_path))
                return
            if path == "/api/system":
                self._send_json(build_system_only())
                return
            if path == "/api/runs":
                limit = _parse_limit(parsed.query, default=50, max_value=300)
                self._send_json(
                    {
                        "generated_at": _utc_now_iso(),
                        "runs": fetch_recent_runs(self.db_path, limit=limit),
                    }
                )
                return
            if path == "/api/logs":
                limit = _parse_limit(parsed.query, default=50, max_value=300)
                self._send_json(
                    {
                        "generated_at": _utc_now_iso(),
                        "logs": fetch_recent_order_logs(self.db_path, limit=limit),
                    }
                )
                return
            if path == "/api/charts":
                limit = _parse_limit(parsed.query, default=240, max_value=1000)
                self._send_json(build_charts(self.db_path, limit=limit))
                return
            if path == "/api/health":
                self._send_json({"status": "ok", "at": _utc_now_iso()})
                return
            self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trading bot lightweight monitor web.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    initialize_database()
    server = ThreadingHTTPServer((args.host, int(args.port)), MonitorHandler)
    print(
        json.dumps(
            {
                "message": "monitor_web started",
                "host": args.host,
                "port": int(args.port),
                "db_path": str(DB_PATH),
                "url": f"http://{args.host}:{int(args.port)}/",
            },
            ensure_ascii=False,
        )
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
