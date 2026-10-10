import streamlit as st
from streamlit_option_menu import option_menu
from frontend.config.settings import config
from frontend.services.health_service import health_service


def render_sidebar() -> str:
    """Renders the enterprise navigation sidebar and returns the selected page name."""
    with st.sidebar:
        # Brand Header & Logo
        st.markdown(
            """
            <div style="display: flex; align-items: center; gap: 12px; padding: 10px 0 20px 0;">
                <div style="background: linear-gradient(135deg, #3b82f6, #06b6d4); width: 42px; height: 42px; border-radius: 10px; display: flex; align-items: center; justify-content: center; box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);">
                    <span style="font-size: 22px;">⚡</span>
                </div>
                <div>
                    <div style="font-size: 19px; font-weight: 800; color: #ffffff; letter-spacing: -0.02em;">ApexAnalytics</div>
                    <div style="font-size: 11px; font-weight: 600; color: #38bdf8; text-transform: uppercase; letter-spacing: 0.08em;">Big Data Platform</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("<hr style='margin: 0 0 16px 0; border-color: rgba(255,255,255,0.08);'>", unsafe_allow_html=True)

        # Menu options
        menu_items = [
            "Home",
            "Live Analytics",
            "Machine Learning",
            "Database Explorer",
            "Business Insights",
            "System Health",
            "Grafana",
            "Settings",
            "About",
        ]

        menu_icons = [
            "house-door-fill",
            "activity",
            "cpu-fill",
            "database-fill",
            "graph-up-arrow",
            "heart-pulse-fill",
            "bar-chart-steps",
            "gear-fill",
            "info-circle-fill",
        ]

        # Use option_menu with custom styling matching our dark theme
        selected = option_menu(
            menu_title=None,
            options=menu_items,
            icons=menu_icons,
            menu_icon="cast",
            default_index=0,
            styles={
                "container": {
                    "padding": "0!important",
                    "background-color": "transparent",
                },
                "icon": {"color": "#94a3b8", "font-size": "15px"},
                "nav-link": {
                    "font-size": "13px",
                    "font-weight": "500",
                    "color": "#94a3b8",
                    "text-align": "left",
                    "margin": "3px 0",
                    "padding": "8px 12px",
                    "border-radius": "8px",
                    "transition": "all 0.2s ease",
                },
                "nav-link-selected": {
                    "background-color": "rgba(59, 130, 246, 0.15)",
                    "color": "#38bdf8",
                    "font-weight": "600",
                    "border-left": "3px solid #38bdf8",
                },
            },
        )

        st.markdown("<hr style='margin: 16px 0; border-color: rgba(255,255,255,0.08);'>", unsafe_allow_html=True)

        # Quick Status Indicators at bottom of sidebar
        db_stat = health_service.check_sqlite_status()
        kafka_stat = health_service.check_kafka_status()
        
        status_color = "#10b981" if db_stat["online"] else "#ef4444"
        st.markdown(
            f"""
            <div style="background: rgba(17, 24, 39, 0.6); padding: 12px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.05); font-size: 11px;">
                <div style="color: #64748b; font-weight: 600; text-transform: uppercase; margin-bottom: 6px;">Pipeline Pulse</div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                    <span style="color: #94a3b8;">SQLite WAL:</span>
                    <span style="color: {status_color}; font-weight: 600;">{db_stat['status']}</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #94a3b8;">Kafka Broker:</span>
                    <span style="color: {'#10b981' if kafka_stat['online'] else '#f59e0b'}; font-weight: 600;">{kafka_stat['status']}</span>
                </div>
            </div>
            <div style="text-align: center; color: #475569; font-size: 10px; margin-top: 14px;">
                {config.project_version}
            </div>
            """,
            unsafe_allow_html=True,
        )

        return selected
