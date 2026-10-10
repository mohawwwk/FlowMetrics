from typing import Optional
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from frontend.theme.theme import PLOTLY_THEME


def apply_theme(fig: go.Figure, title: str = "", height: int = 380) -> go.Figure:
    """Applies corporate dark theme styling to a Plotly figure."""
    layout_cfg = PLOTLY_THEME["layout"].copy()
    fig.update_layout(
        paper_bgcolor=layout_cfg["paper_bgcolor"],
        plot_bgcolor=layout_cfg["plot_bgcolor"],
        font=layout_cfg["font"],
        colorway=layout_cfg["colorway"],
        margin=layout_cfg["margin"],
        height=height,
        title=dict(
            text=f"<b>{title}</b>" if title else "",
            font=dict(size=14, color="#f8fafc", family="Inter, sans-serif"),
            x=0.02,
            y=0.96,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color="#94a3b8", size=11),
        ),
        hoverlabel=dict(
            bgcolor="#1e293b",
            font_size=12,
            font_family="Inter, sans-serif",
            font_color="#f8fafc",
            bordercolor="rgba(255, 255, 255, 0.1)",
        ),
    )
    fig.update_xaxes(
        gridcolor=layout_cfg["xaxis"]["gridcolor"],
        zerolinecolor=layout_cfg["xaxis"]["zerolinecolor"],
        tickfont=layout_cfg["xaxis"]["tickfont"],
        showline=False,
    )
    fig.update_yaxes(
        gridcolor=layout_cfg["yaxis"]["gridcolor"],
        zerolinecolor=layout_cfg["yaxis"]["zerolinecolor"],
        tickfont=layout_cfg["yaxis"]["tickfont"],
        showline=False,
    )
    return fig


def create_revenue_timeline(df: pd.DataFrame, title: str = "Real-Time Revenue Timeline") -> go.Figure:
    """Creates a smooth area/line chart for revenue trend."""
    if df.empty or "invoice_timestamp" not in df.columns or "revenue" not in df.columns:
        fig = go.Figure()
        return apply_theme(fig, title)

    # Sort and aggregate
    df_plot = df.copy()
    df_plot["timestamp"] = pd.to_datetime(df_plot["invoice_timestamp"], errors="coerce")
    df_plot = df_plot.dropna(subset=["timestamp"]).sort_values("timestamp")
    
    # Resample or group by minute
    grouped = df_plot.groupby(pd.Grouper(key="timestamp", freq="1min"))["revenue"].sum().reset_index()
    if grouped.empty or len(grouped) < 2:
        grouped = df_plot[["timestamp", "revenue"]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=grouped["timestamp"],
            y=grouped["revenue"],
            mode="lines",
            name="Gross Revenue",
            line=dict(color="#3b82f6", width=2.5, shape="spline"),
            fill="tozeroy",
            fillcolor="rgba(59, 130, 246, 0.12)",
        )
    )
    return apply_theme(fig, title)


def create_top_products_bar(df: pd.DataFrame, title: str = "Top Products by Revenue") -> go.Figure:
    """Creates a horizontal bar chart of highest revenue products."""
    if df.empty or "description" not in df.columns or "total_revenue" not in df.columns:
        fig = go.Figure()
        return apply_theme(fig, title)

    df_plot = df.head(10).sort_values("total_revenue", ascending=True)
    # Truncate long descriptions
    df_plot["short_desc"] = df_plot["description"].apply(
        lambda x: (str(x)[:28] + "...") if len(str(x)) > 28 else str(x)
    )

    fig = go.Figure(
        go.Bar(
            x=df_plot["total_revenue"],
            y=df_plot["short_desc"],
            orientation="h",
            marker=dict(
                color=df_plot["total_revenue"],
                colorscale=[[0, "#1d4ed8"], [1, "#06b6d4"]],
                line=dict(color="rgba(255, 255, 255, 0.1)", width=1),
            ),
            text=df_plot["total_revenue"].apply(lambda v: f"${v:,.0f}"),
            textposition="auto",
            hovertemplate="<b>%{y}</b><br>Revenue: $%{x:,.2f}<extra></extra>",
        )
    )
    return apply_theme(fig, title)


def create_country_pie(df: pd.DataFrame, title: str = "Geographic Market Share") -> go.Figure:
    """Creates a modern donut chart for country revenue distribution."""
    if df.empty or "country" not in df.columns or "total_revenue" not in df.columns:
        fig = go.Figure()
        return apply_theme(fig, title)

    df_top = df.head(6).copy()
    other_val = df.iloc[6:]["total_revenue"].sum() if len(df) > 6 else 0
    if other_val > 0:
        other_row = pd.DataFrame([{"country": "Other Markets", "total_revenue": other_val}])
        df_top = pd.concat([df_top, other_row], ignore_index=True)

    fig = go.Figure(
        go.Pie(
            labels=df_top["country"],
            values=df_top["total_revenue"],
            hole=0.6,
            textinfo="percent+label",
            textposition="outside",
            marker=dict(
                colors=["#3b82f6", "#06b6d4", "#10b981", "#f59e0b", "#8b5cf6", "#64748b"],
                line=dict(color="#111827", width=2),
            ),
            hovertemplate="<b>%{label}</b><br>Revenue: $%{value:,.2f}<br>Share: %{percent}<extra></extra>",
        )
    )
    return apply_theme(fig, title)


def create_order_velocity_area(df: pd.DataFrame, title: str = "Orders Velocity (Windowed Rate)") -> go.Figure:
    """Creates an area chart showing order frequency and throughput."""
    if df.empty or "orders_count" not in df.columns:
        fig = go.Figure()
        return apply_theme(fig, title)

    x_col = "window_start" if "window_start" in df.columns else df.index
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df[x_col],
            y=df["orders_count"],
            mode="lines+markers",
            name="Orders / Window",
            line=dict(color="#10b981", width=2),
            marker=dict(size=5, color="#10b981"),
            fill="tozeroy",
            fillcolor="rgba(16, 185, 129, 0.12)",
        )
    )
    return apply_theme(fig, title)


def create_actual_vs_predicted_scatter(df: pd.DataFrame, title: str = "Actual vs Predicted Revenue") -> go.Figure:
    """Creates a scatter plot comparing actual revenue against ML predictions."""
    if df.empty or "actual_revenue" not in df.columns or "predicted_revenue" not in df.columns:
        fig = go.Figure()
        return apply_theme(fig, title)

    sample = df.head(500)
    max_val = max(sample["actual_revenue"].max(), sample["predicted_revenue"].max(), 50.0)

    fig = go.Figure()
    # Ideal diagonal line
    fig.add_trace(
        go.Scatter(
            x=[0, max_val],
            y=[0, max_val],
            mode="lines",
            name="Perfect Identity (y=x)",
            line=dict(color="rgba(255, 255, 255, 0.3)", dash="dash", width=1.5),
        )
    )
    # Scatter points
    fig.add_trace(
        go.Scatter(
            x=sample["actual_revenue"],
            y=sample["predicted_revenue"],
            mode="markers",
            name="Transactions",
            marker=dict(
                size=7,
                color=sample["abs_error"] if "abs_error" in sample.columns else "#3b82f6",
                colorscale="Viridis",
                showscale=True,
                colorbar=dict(title=dict(text="Error ($)", font=dict(color="#94a3b8", size=10)), thickness=10),
                opacity=0.8,
            ),
            hovertemplate="Actual: $%{x:.2f}<br>Predicted: $%{y:.2f}<extra></extra>",
        )
    )
    fig.update_xaxes(title="Actual Revenue ($)")
    fig.update_yaxes(title="Predicted Revenue ($)")
    return apply_theme(fig, title)


def create_residuals_histogram(df: pd.DataFrame, title: str = "Model Prediction Error Distribution") -> go.Figure:
    """Creates a histogram distribution of prediction residuals (Absolute Error)."""
    if df.empty or "abs_error" not in df.columns:
        fig = go.Figure()
        return apply_theme(fig, title)

    fig = go.Figure(
        go.Histogram(
            x=df["abs_error"],
            nbinsx=30,
            marker=dict(
                color="#06b6d4",
                line=dict(color="#111827", width=1),
            ),
            hovertemplate="Error Range: %{x}<br>Frequency: %{y}<extra></extra>",
        )
    )
    fig.update_xaxes(title="Absolute Error ($)")
    fig.update_yaxes(title="Frequency")
    return apply_theme(fig, title)


def create_hourly_heatmap(df: pd.DataFrame, title: str = "Hourly Sales Velocity Heatmap") -> go.Figure:
    """Creates a heatmap of transaction frequency and volume across hours."""
    if df.empty or "hour_of_day" not in df.columns:
        fig = go.Figure()
        return apply_theme(fig, title)

    hours = list(range(24))
    sales_map = {row["hour_of_day"]: row["total_revenue"] for _, row in df.iterrows()}
    values = [[sales_map.get(h, 0.0) for h in hours]]

    fig = go.Figure(
        go.Heatmap(
            z=values,
            x=[f"{h:02d}:00" for h in hours],
            y=["Revenue ($)"],
            colorscale=[[0, "#0b0f19"], [0.5, "#1e3a8a"], [1, "#06b6d4"]],
            showscale=True,
            hovertemplate="Hour: %{x}<br>Volume: $%{z:,.2f}<extra></extra>",
        )
    )
    return apply_theme(fig, title, height=220)
