import streamlit as st
from pathlib import Path


def load_custom_css():
    """Reads and injects frontend/styles/custom.css into the active Streamlit app."""
    css_path = Path(__file__).resolve().parents[1] / "styles" / "custom.css"
    if css_path.exists():
        with open(css_path, "r", encoding="utf-8") as f:
            css = f.read()
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)


# Themed Plotly Palette
PLOTLY_THEME = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {"family": "Inter, sans-serif", "color": "#94a3b8"},
        "title": {"font": {"color": "#f8fafc", "size": 16, "family": "Inter, sans-serif"}},
        "xaxis": {
            "gridcolor": "rgba(255, 255, 255, 0.05)",
            "zerolinecolor": "rgba(255, 255, 255, 0.08)",
            "tickfont": {"color": "#94a3b8"},
        },
        "yaxis": {
            "gridcolor": "rgba(255, 255, 255, 0.05)",
            "zerolinecolor": "rgba(255, 255, 255, 0.08)",
            "tickfont": {"color": "#94a3b8"},
        },
        "colorway": [
            "#3b82f6",  # Blue
            "#06b6d4",  # Cyan
            "#10b981",  # Emerald Green
            "#f59e0b",  # Amber
            "#8b5cf6",  # Purple
            "#ec4899",  # Pink
            "#6366f1",  # Indigo
        ],
        "margin": {"l": 40, "r": 20, "t": 40, "b": 40},
    }
}
