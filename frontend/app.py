import sys
from pathlib import Path

# Add project root to sys.path so frontend imports work cleanly
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st

# Configure Streamlit page settings
st.set_page_config(
    page_title="ApexAnalytics | Big Data Financial Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

from frontend.components.sidebar import render_sidebar
from frontend.config.settings import config
from frontend.pages import (
    render_about_page,
    render_business_insights_page,
    render_database_explorer_page,
    render_grafana_page,
    render_home_page,
    render_live_analytics_page,
    render_machine_learning_page,
    render_settings_page,
    render_system_health_page,
)
from frontend.theme.theme import load_custom_css


def main():
    # Inject corporate dark theme styling
    load_custom_css()

    # Render sidebar and get user navigation choice
    selected_page = render_sidebar()

    # Handle auto-refresh on Live Analytics page if enabled
    if selected_page == "Live Analytics":
        refresh_enabled = st.session_state.get("auto_refresh", True)
        refresh_interval = st.session_state.get("refresh_rate", 3)
        if refresh_enabled and st_autorefresh:
            st_autorefresh(interval=refresh_interval * 1000, key="live_analytics_refresher")

    # Page Router
    try:
        if selected_page == "Home":
            render_home_page()
        elif selected_page == "Live Analytics":
            render_live_analytics_page()
        elif selected_page == "Machine Learning":
            render_machine_learning_page()
        elif selected_page == "Database Explorer":
            render_database_explorer_page()
        elif selected_page == "Business Insights":
            render_business_insights_page()
        elif selected_page == "System Health":
            render_system_health_page()
        elif selected_page == "Grafana":
            render_grafana_page()
        elif selected_page == "Settings":
            render_settings_page()
        elif selected_page == "About":
            render_about_page()
        else:
            render_home_page()
    except Exception as e:
        st.error(f"An unexpected error occurred while rendering '{selected_page}': {e}")
        st.info("The application caught this exception to prevent a crash. Please check your database connection or settings.")


if __name__ == "__main__":
    main()
