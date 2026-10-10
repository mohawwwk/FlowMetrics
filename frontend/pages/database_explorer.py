import streamlit as st
import pandas as pd

from frontend.database.db_reader import db_service
from frontend.config.settings import config


def render_database_explorer_page():
    """Renders the Read-Only Database Browser & Query Inspection Page."""
    st.markdown(
        """
        <div style="font-size: 26px; font-weight: 800; color: #ffffff; letter-spacing: -0.02em;">
            Database Explorer & Table Inspector
        </div>
        <div style="color: #94a3b8; font-size: 13px; margin-bottom: 20px;">
            Read-only interface to inspect SQLite WAL tables, filter records, analyze indexes, and export data.
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not db_service.is_connected():
        st.error(f"Database file not accessible at {config.get_absolute_db_path()}. Please verify the path.")
        return

    tables = db_service.get_table_names()
    if not tables:
        st.warning("No user tables detected in database.")
        return

    # Table Selection & Filter Bar
    c_tbl, c_search, c_sort, c_order = st.columns([1.5, 2.0, 1.2, 0.8])

    with c_tbl:
        selected_table = st.selectbox("Select Table", options=tables, index=0)

    with c_search:
        search_query = st.text_input("Search (any text field)", value="", placeholder="Type invoice, customer, etc...")

    # Discover columns for selected table
    cols_df = db_service.execute_query_df(f"PRAGMA table_info({selected_table});")
    col_names = cols_df["name"].tolist() if not cols_df.empty else []

    with c_sort:
        default_sort = "id" if "id" in col_names else (col_names[0] if col_names else None)
        sort_col = st.selectbox("Sort Column", options=col_names, index=col_names.index(default_sort) if default_sort in col_names else 0)

    with c_order:
        sort_order = st.selectbox("Order", options=["DESC", "ASC"], index=0)

    # Pagination controls
    page_size = 25
    page_col1, page_col2, page_col3 = st.columns([1, 2, 1])

    # Fetch total and data
    # First query to get total
    initial_df, total_rows = db_service.browse_table(
        selected_table,
        search_query=search_query,
        sort_col=sort_col,
        sort_order=sort_order,
        page=1,
        page_size=page_size,
    )

    total_pages = max(1, (total_rows + page_size - 1) // page_size)

    with page_col1:
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)

    # Fetch actual page if page > 1
    if current_page > 1:
        page_df, _ = db_service.browse_table(
            selected_table,
            search_query=search_query,
            sort_col=sort_col,
            sort_order=sort_order,
            page=current_page,
            page_size=page_size,
        )
    else:
        page_df = initial_df

    with page_col2:
        st.markdown(
            f"<div style='margin-top: 28px; color: #94a3b8; font-size: 13px;'>"
            f"Showing {len(page_df)} of <b>{total_rows:,}</b> rows (Page {current_page} of {total_pages})</div>",
            unsafe_allow_html=True,
        )

    with page_col3:
        # Export CSV Button
        if not page_df.empty:
            csv_data = page_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Export Page (CSV)",
                data=csv_data,
                file_name=f"{selected_table}_page_{current_page}.csv",
                mime="text/csv",
                key="download_csv_btn",
            )

    # Table Display
    st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
    if not page_df.empty:
        st.dataframe(page_df, use_container_width=True, hide_index=True)
    else:
        st.info("No matching records found for the specified filter.")

    # Table Schema & Index Information Drawer
    with st.expander(f"🔍 Schema & Index Specification for `{selected_table}`"):
        col_sch, col_idx = st.columns(2)
        with col_sch:
            st.markdown("<b>Column Definitions:</b>", unsafe_allow_html=True)
            if not cols_df.empty:
                st.dataframe(cols_df[["cid", "name", "type", "notnull", "pk"]], use_container_width=True, hide_index=True)
        with col_idx:
            st.markdown("<b>Associated Indexes:</b>", unsafe_allow_html=True)
            idx_df = db_service.execute_query_df(f"PRAGMA index_list({selected_table});")
            if not idx_df.empty:
                st.dataframe(idx_df[["name", "unique", "origin"]], use_container_width=True, hide_index=True)
            else:
                st.write("No custom indexes.")
