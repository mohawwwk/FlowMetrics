import streamlit as st
from pathlib import Path

from frontend.config.settings import config
from frontend.database.db_reader import db_service


def render_settings_page():
    """Renders application settings, telemetry interval toggles, and storage paths."""
    st.markdown(
        """
        <div style="font-size: 26px; font-weight: 800; color: #ffffff; letter-spacing: -0.02em;">
            Platform Settings & Dashboard Preferences
        </div>
        <div style="color: #94a3b8; font-size: 13px; margin-bottom: 20px;">
            Customize streaming auto-refresh frequency, storage paths, and export formats.
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">⚙️ Streaming & Telemetry Controls</div>
                </div>
            """,
            unsafe_allow_html=True,
        )
        refresh_sec = st.slider(
            "Live Analytics Auto-Refresh Interval (Seconds)",
            min_value=1,
            max_value=30,
            value=st.session_state.get("refresh_rate", 3),
            step=1,
            help="Controls how frequently the Live Analytics view re-polls the database.",
        )
        st.session_state["refresh_rate"] = refresh_sec

        enable_auto_refresh = st.toggle("Enable Live Telemetry Auto-Refresh", value=st.session_state.get("auto_refresh", True))
        st.session_state["auto_refresh"] = enable_auto_refresh

        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown(
            """
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">💾 Storage & Database Path</div>
                </div>
            """,
            unsafe_allow_html=True,
        )
        db_path_input = st.text_input(
            "SQLite Database File Path",
            value=str(config.get_absolute_db_path()),
            help="Absolute or relative path to the SQLite WAL database.",
        )
        is_valid = Path(db_path_input).exists()
        if is_valid:
            st.success(f"✓ Valid database detected ({round(Path(db_path_input).stat().st_size / (1024*1024), 2)} MB)")
        else:
            st.error("✗ Database path does not exist on disk.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown(
            """
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">🎨 Visual Theme & Display</div>
                </div>
            """,
            unsafe_allow_html=True,
        )
        st.selectbox("Color Palette", options=["Enterprise Dark (Blue / Cyan Accent)", "Midnight Slate", "High-Contrast Terminal"], index=0)
        st.selectbox("Default Chart Renderer", options=["Plotly Interactive SVG/WebGL", "Standard SVG"], index=0)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown(
            """
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">📤 Data Export Preferences</div>
                </div>
            """,
            unsafe_allow_html=True,
        )
        st.selectbox("Default Export Format", options=["CSV (Comma-Separated Values)", "JSON (JSON Lines)", "Parquet"], index=0)
        st.checkbox("Include Table Header Metadata in Exports", value=True)
        st.markdown("</div>", unsafe_allow_html=True)

    if st.button("💾 Save Preferences"):
        st.toast("Settings saved successfully!", icon="✅")
