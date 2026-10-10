from typing import Optional
import streamlit as st


def render_kpi_card(
    title: str,
    value: str,
    subtitle: str = "",
    delta: Optional[str] = None,
    delta_type: str = "up",  # "up", "down", "neutral"
    icon: str = "📊",
) -> None:
    """Renders a modern corporate KPI card with hover animations and glassmorphism."""
    delta_html = ""
    if delta:
        badge_class = (
            "trend-badge-up" if delta_type == "up" else 
            "trend-badge-down" if delta_type == "down" else 
            "trend-badge-neutral"
        )
        symbol = "↑" if delta_type == "up" else "↓" if delta_type == "down" else "•"
        delta_html = f'<span class="{badge_class}">{symbol} {delta}</span>'

    card_html = f"""
    <div class="apex-metric-card">
        <div class="apex-card-title">
            <span>{icon}</span> {title}
        </div>
        <div class="apex-card-value">{value}</div>
        <div class="apex-card-subtitle">
            {delta_html}
            <span>{subtitle}</span>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def render_status_pill(status: str, label: str) -> str:
    """Renders a pill badge showing status with pulsating dot."""
    if status.upper() in ["ONLINE", "HEALTHY", "RUNNING", "CONNECTED", "LOADED"]:
        pill_class = "status-online"
        pulse_class = "pulse-green"
    elif status.upper() in ["WARNING", "STANDBY", "READY", "IDLE"]:
        pill_class = "status-warning"
        pulse_class = "pulse-green"
    else:
        pill_class = "status-offline"
        pulse_class = "pulse-red"

    return f"""
    <span class="status-pill {pill_class}">
        <span class="pulse-dot {pulse_class}"></span>
        {label}: <b>{status}</b>
    </span>
    """
