from typing import Any, Union
from datetime import datetime


def format_currency(val: Union[int, float, None]) -> str:
    """Formats numeric values into localized currency strings ($X,XXX.XX)."""
    if val is None:
        return "$0.00"
    try:
        return f"${float(val):,.2f}"
    except (ValueError, TypeError):
        return "$0.00"


def format_compact_number(val: Union[int, float, None]) -> str:
    """Formats large numbers into compact suffixes (1.2K, 3.4M)."""
    if val is None:
        return "0"
    try:
        num = float(val)
        if abs(num) >= 1_000_000:
            return f"{num / 1_000_000:.1f}M"
        elif abs(num) >= 1_000:
            return f"{num / 1_000:.1f}K"
        else:
            return f"{num:,.0f}"
    except (ValueError, TypeError):
        return "0"


def parse_timestamp_safe(ts_str: Any) -> str:
    """Parses arbitrary timestamp strings and returns clean human-readable date/time."""
    if not ts_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_str)[:19]
