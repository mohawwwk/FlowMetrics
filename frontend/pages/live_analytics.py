import streamlit as st
import pandas as pd

from frontend.charts.plotly_charts import (
    create_country_pie,
    create_hourly_heatmap,
    create_order_velocity_area,
    create_revenue_timeline,
    create_top_products_bar,
)
from frontend.components.kpi_card import render_kpi_card
from frontend.database.db_reader import db_service


def render_live_analytics_page():
    """Renders the Live Analytics real-time streaming dashboard."""
    col_title, col_ctrl = st.columns([2.5, 1.0])
    with col_title:
        st.markdown(
            """
            <div style="font-size: 26px; font-weight: 800; color: #ffffff; letter-spacing: -0.02em;">
                Real-Time Streaming Analytics & Telemetry
            </div>
            <div style="color: #94a3b8; font-size: 13px; margin-bottom: 20px;">
                Live streaming order rate, revenue velocity, country breakdown, and transaction logs.
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_ctrl:
        st.markdown(
            """
            <div style="display: flex; justify-content: flex-end; align-items: center; height: 100%;">
                <span class="status-pill status-online">
                    <span class="pulse-dot pulse-green"></span>
                    Live Streaming Ingest
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Key Telemetry KPI Row
    stats = db_service.get_database_stats()
    orders_min_df = db_service.get_orders_per_minute(limit=10)
    current_opm = int(orders_min_df["orders_count"].iloc[0]) if not orders_min_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_kpi_card(
            title="Live Revenue",
            value=f"${stats['total_revenue']:,.2f}",
            subtitle="Current Ingest Total",
            delta="+18.4% velocity",
            delta_type="up",
            icon="💰",
        )
    with c2:
        render_kpi_card(
            title="Total Transactions",
            value=f"{stats['total_transactions']:,}",
            subtitle="Processed Line Items",
            delta="100% Deliverability",
            delta_type="up",
            icon="🧾",
        )
    with c3:
        render_kpi_card(
            title="Average Basket",
            value=f"${stats['avg_order_value']:.2f}",
            subtitle="Average Order Value",
            delta="+2.1% AOV",
            delta_type="up",
            icon="🛒",
        )
    with c4:
        render_kpi_card(
            title="Orders Per Min",
            value=f"{current_opm} opm",
            subtitle="Windowed Throughput",
            delta="Real-Time Window",
            delta_type="neutral",
            icon="⚡",
        )

    st.markdown("<div style='height: 14px;'></div>", unsafe_allow_html=True)

    # Charts Row 1: Revenue Timeline & Country Share
    ch_col1, ch_col2 = st.columns([1.8, 1.2])

    recent_tx = db_service.get_recent_transactions(limit=100)
    country_df = db_service.get_country_distribution(limit=8)

    with ch_col1:
        fig_rev = create_revenue_timeline(recent_tx, title="Revenue Stream Velocity")
        st.plotly_chart(fig_rev, use_container_width=True)

    with ch_col2:
        fig_country = create_country_pie(country_df, title="Geographic Revenue Share")
        st.plotly_chart(fig_country, use_container_width=True)

    # Charts Row 2: Top Products & Order Velocity
    ch_col3, ch_col4 = st.columns([1.5, 1.5])

    top_prods = db_service.get_top_products(limit=10)

    with ch_col3:
        fig_prod = create_top_products_bar(top_prods, title="Top 10 Products by Revenue")
        st.plotly_chart(fig_prod, use_container_width=True)

    with ch_col4:
        fig_vel = create_order_velocity_area(orders_min_df, title="1-Minute Windowed Order Velocity")
        st.plotly_chart(fig_vel, use_container_width=True)

    # Hourly Heatmap Row
    hourly_df = db_service.get_hourly_sales()
    if not hourly_df.empty:
        fig_heat = create_hourly_heatmap(hourly_df, title="24-Hour Purchase Distribution Density")
        st.plotly_chart(fig_heat, use_container_width=True)

    # Streaming Tables: Top Customers & Live Transaction Stream
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    t_col1, t_col2 = st.columns([1.2, 1.8])

    with t_col1:
        st.markdown("#### 🏆 Top Customer Leaderboard")
        cust_df = db_service.get_customer_leaderboard(limit=8)
        if not cust_df.empty:
            st.dataframe(
                cust_df[["customer_id", "country", "total_orders", "total_spent"]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Customer aggregates computing...")

    with t_col2:
        st.markdown("#### ⚡ Live Ingestion Log Stream")
        if not recent_tx.empty:
            cols = ["id", "invoice_no", "stock_code", "quantity", "unit_price", "revenue", "country"]
            st.dataframe(
                recent_tx[cols].head(8),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No incoming transactions.")
