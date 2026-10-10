import streamlit as st
from frontend.config.settings import config


def render_grafana_page():
    """Renders Grafana integration with embedded iframe view and direct launch links."""
    st.markdown(
        """
        <div style="font-size: 26px; font-weight: 800; color: #ffffff; letter-spacing: -0.02em;">
            Grafana Enterprise Dashboard Integration
        </div>
        <div style="color: #94a3b8; font-size: 13px; margin-bottom: 20px;">
            Production Grafana monitoring embedding automated provisioning dashboards for revenue and ML evaluation.
        </div>
        """,
        unsafe_allow_html=True,
    )

    grafana_base = config.grafana_url.rstrip("/")

    # Quick Launch Cards
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            f"""
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">📊 Revenue & Orders Dashboard</div>
                    <span style="font-size: 12px; color: #38bdf8; font-weight: 600;">Phase 6 Provisioned</span>
                </div>
                <div style="font-size: 13px; color: #94a3b8; margin-bottom: 14px;">
                    Tracks gross revenue, orders per minute, top selling products, country breakdown, and average order value.
                </div>
                <a href="{grafana_base}/d/ecommerce-revenue/ecommerce-revenue-analytics" target="_blank" 
                   style="display: inline-block; background: #3b82f6; color: #ffffff; padding: 8px 16px; border-radius: 6px; text-decoration: none; font-weight: 600; font-size: 13px;">
                   🚀 Launch Revenue Dashboard in Grafana ↗
                </a>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            f"""
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">🧠 ML Predictions Performance</div>
                    <span style="font-size: 12px; color: #10b981; font-weight: 600;">Phase 6 Provisioned</span>
                </div>
                <div style="font-size: 13px; color: #94a3b8; margin-bottom: 14px;">
                    Visualizes predicted revenue vs actual purchase value, absolute error histograms, and residual trends.
                </div>
                <a href="{grafana_base}/d/ecommerce-ml/ecommerce-ml-predictions" target="_blank" 
                   style="display: inline-block; background: #10b981; color: #ffffff; padding: 8px 16px; border-radius: 6px; text-decoration: none; font-weight: 600; font-size: 13px;">
                   🚀 Launch ML Dashboard in Grafana ↗
                </a>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Embedded Iframe View
    st.markdown("### 🖥️ Live Grafana Dashboard Viewer")
    dashboard_choice = st.radio(
        "Choose Dashboard to Embed:",
        options=["Revenue Analytics Dashboard", "ML Predictions Dashboard", "Grafana Home Portal"],
        horizontal=True,
    )

    if dashboard_choice == "Revenue Analytics Dashboard":
        embed_url = f"{grafana_base}/d/ecommerce-revenue/ecommerce-revenue-analytics?kiosk"
    elif dashboard_choice == "ML Predictions Dashboard":
        embed_url = f"{grafana_base}/d/ecommerce-ml/ecommerce-ml-predictions?kiosk"
    else:
        embed_url = f"{grafana_base}/?kiosk"

    st.markdown(
        f"""
        <div style="border-radius: 12px; overflow: hidden; border: 1px solid rgba(255,255,255,0.1); margin-top: 10px;">
            <iframe 
                src="{embed_url}" 
                width="100%" 
                height="650" 
                frameborder="0"
                style="background: #111827;">
            </iframe>
        </div>
        <div style="font-size: 12px; color: #64748b; margin-top: 8px;">
            💡 Note: If Grafana displays a connection refused message, ensure Grafana service is running at <code>{grafana_base}</code> or click the launch buttons above.
        </div>
        """,
        unsafe_allow_html=True,
    )
