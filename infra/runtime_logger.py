from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app_settings import get_runtime_timezone_label, get_runtime_timezone_offset_hours


def _now() -> str:
    offset_hours = get_runtime_timezone_offset_hours()
    tzinfo = timezone(timedelta(hours=offset_hours))
    return datetime.now(tzinfo).strftime("%Y-%m-%d %H:%M:%S")


def log_section(title: str) -> None:
    print(f"\n[{_now()} {get_runtime_timezone_label()}] ===== {title} =====")


def log_step(message: str) -> None:
    print(f"[{_now()} {get_runtime_timezone_label()}] {message}")
