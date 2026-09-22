"""DataForge analytics dashboard (Streamlit).

Reads the dbt marts and monitoring schema directly from PostgreSQL.

    streamlit run dashboard/app.py --server.port 8501
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataforge.config import get_settings  # noqa: E402
from dataforge.db import DatabaseUnavailable, connect  # noqa: E402

MARTS = "analytics_marts"

# Validated categorical palette (fixed slot order - never cycled) and a single-hue sequential ramp.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
SEQUENTIAL = ["#dbe8f9", "#a9c6ee", "#6fa0e2", "#2a78d6", "#1b56a3"]
GRID = "rgba(128,128,128,0.25)"
STATUS = {
    "pass": "#008300",
    "success": "#008300",
    "warn": "#eda100",
    "fail": "#e34948",
    "failed": "#e34948",
    "running": "#2a78d6",
}

st.set_page_config(page_title="DataForge Analytics", page_icon="📦", layout="wide")


# ---------------------------------------------------------------------- data access
@st.cache_data(ttl=60, show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    with connect(get_settings(), autocommit=True) as conn:
        rows = conn.execute(sql, params).fetchall()
    return pd.DataFrame(rows)


def style(fig: go.Figure, height: int = 320) -> go.Figure:
    # text colours are left to Streamlit's theme (light/dark) so labels stay readable in both
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=36, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=12),
        legend=dict(orientation="h", y=-0.2, title=None),
        hovermode="x unified",
        title=dict(font=dict(size=14)),
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID, title=None)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, title=None)
    return fig


def money(v) -> str:
    if v is None or pd.isna(v):
        return "-"
    v = float(v)
    return f"${v / 1e6:,.2f}M" if abs(v) >= 1e6 else f"${v / 1e3:,.1f}K" if abs(v) >= 1e3 else f"${v:,.0f}"


def num(v) -> str:
    return "-" if v is None or pd.isna(v) else f"{float(v):,.0f}"


# ------------------------------------------------------------------------- pages
def page_overview():
    st.subheader("Business overview")
    tot = q(
        f"SELECT sum(revenue_usd) revenue, sum(revenue_orders) orders, sum(units_sold) units FROM {MARTS}.daily_sales_summary"
    ).iloc[0]
    cust = q(
        f"SELECT count(*) FILTER (WHERE lifecycle_stage='active') active, count(*) total FROM {MARTS}.customer_lifetime_value"
    ).iloc[0]
    inv = q(
        f"SELECT sum(inventory_value_usd) value, sum(on_hand_units) units, count(*) FILTER (WHERE is_low_stock) low FROM {MARTS}.inventory_turnover"
    ).iloc[0]
    ship = q(
        f"SELECT sum(avg_delivery_days*delivered)/nullif(sum(delivered),0) days, sum(on_time_rate*delivered)/nullif(sum(delivered),0) on_time FROM {MARTS}.shipping_performance"
    ).iloc[0]
    aov = float(tot["revenue"]) / float(tot["orders"]) if tot["orders"] else 0

    c = st.columns(6)
    c[0].metric("Total revenue", money(tot["revenue"]))
    c[1].metric("Revenue orders", num(tot["orders"]))
    c[2].metric("Active customers (90d)", num(cust["active"]), f"of {num(cust['total'])}")
    c[3].metric(
        "Inventory value", money(inv["value"]), f"{num(inv['low'])} low-stock SKUs", delta_color="inverse"
    )
    c[4].metric("Avg order value", f"${aov:,.2f}")
    c[5].metric(
        "Avg delivery time", f"{float(ship['days']):.1f} days", f"{float(ship['on_time']) * 100:.0f}% on time"
    )

    st.markdown("---")
    left, right = st.columns([3, 2])
    monthly = q(
        f"SELECT year_month, revenue_usd, gross_margin_usd, revenue_orders, active_customers, new_customers FROM {MARTS}.monthly_revenue_summary ORDER BY year_month"
    )
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=monthly.year_month,
            y=monthly.revenue_usd,
            name="Revenue",
            line=dict(color=SERIES[0], width=2),
            mode="lines+markers",
            marker=dict(size=6),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=monthly.year_month,
            y=monthly.gross_margin_usd,
            name="Gross margin",
            line=dict(color=SERIES[2], width=2),
            mode="lines",
        )
    )
    fig.update_layout(title="Revenue over time (USD, monthly)")
    left.plotly_chart(style(fig), use_container_width=True)

    cats = q(
        f"SELECT parent_category_name, sum(revenue_usd) revenue_usd FROM {MARTS}.category_revenue GROUP BY 1 ORDER BY 2 DESC"
    )
    fig = px.bar(
        cats,
        x="revenue_usd",
        y="parent_category_name",
        orientation="h",
        title="Revenue by top-level category",
    )
    fig.update_traces(
        marker_color=SERIES[0], marker_line_width=0, hovertemplate="%{y}: $%{x:,.0f}<extra></extra>"
    )
    fig.update_yaxes(autorange="reversed")
    right.plotly_chart(style(fig), use_container_width=True)

    left, right = st.columns(2)
    prods = q(
        f"SELECT product_name, revenue_usd FROM {MARTS}.product_sales_metrics ORDER BY revenue_usd DESC LIMIT 12"
    )
    fig = px.bar(prods, x="revenue_usd", y="product_name", orientation="h", title="Top products by revenue")
    fig.update_traces(marker_color=SERIES[0], hovertemplate="%{y}: $%{x:,.0f}<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    left.plotly_chart(style(fig, 380), use_container_width=True)

    reg = q(
        f"SELECT region_name, sum(revenue_usd) revenue_usd, sum(avg_delivery_days*shipments)/nullif(sum(shipments),0) delivery_days FROM {MARTS}.regional_performance GROUP BY 1 ORDER BY 2 DESC"
    )
    fig = px.bar(
        reg,
        x="revenue_usd",
        y="region_name",
        orientation="h",
        title="Regional performance (revenue; hover for delivery days)",
        custom_data=["delivery_days"],
    )
    fig.update_traces(
        marker_color=SERIES[0],
        hovertemplate="%{y}: $%{x:,.0f}<br>avg delivery %{customdata[0]:.1f} days<extra></extra>",
    )
    fig.update_yaxes(autorange="reversed")
    right.plotly_chart(style(fig, 380), use_container_width=True)


def page_sales():
    st.subheader("Sales")
    daily = q(
        f"SELECT sales_date, revenue_usd, revenue_orders, new_customers FROM {MARTS}.daily_sales_summary ORDER BY sales_date"
    )
    daily["revenue_7d"] = daily.revenue_usd.rolling(7).mean()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=daily.sales_date,
            y=daily.revenue_usd,
            name="Daily revenue",
            line=dict(color=SERIES[0], width=1),
            opacity=0.5,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=daily.sales_date, y=daily.revenue_7d, name="7-day average", line=dict(color=SERIES[1], width=2)
        )
    )
    fig.update_layout(title="Daily revenue (USD)")
    st.plotly_chart(style(fig), use_container_width=True)

    left, right = st.columns(2)
    cat = q(
        f"SELECT order_month, parent_category_name, sum(revenue_usd) revenue_usd FROM {MARTS}.category_revenue GROUP BY 1,2 ORDER BY 1"
    )
    order = cat.groupby("parent_category_name").revenue_usd.sum().sort_values(ascending=False).index.tolist()
    fig = px.area(
        cat,
        x="order_month",
        y="revenue_usd",
        color="parent_category_name",
        category_orders={"parent_category_name": order},
        color_discrete_sequence=SERIES,
        title="Monthly revenue by category",
    )
    fig.update_traces(line=dict(width=1))
    left.plotly_chart(style(fig, 380), use_container_width=True)

    decl = q(
        f"SELECT product_name, category_name, revenue_prior_90d, revenue_last_90d, revenue_trend_ratio FROM {MARTS}.product_sales_metrics WHERE is_declining ORDER BY revenue_prior_90d DESC LIMIT 15"
    )
    right.markdown("**Products with declining sales** (last 90 days vs prior 90 days)")
    right.dataframe(
        decl.rename(
            columns={
                "revenue_prior_90d": "prior 90d $",
                "revenue_last_90d": "last 90d $",
                "revenue_trend_ratio": "ratio",
            }
        ),
        use_container_width=True,
        hide_index=True,
        height=380,
    )

    monthly = q(
        f"SELECT year_month, revenue_usd, revenue_orders, avg_order_value_usd, active_customers, new_customers, revenue_mom_growth FROM {MARTS}.monthly_revenue_summary ORDER BY year_month DESC"
    )
    st.markdown("**Monthly summary**")
    st.dataframe(monthly, use_container_width=True, hide_index=True)


def page_inventory():
    st.subheader("Inventory")
    wh = q(
        f"SELECT warehouse_name, region_code, on_hand_units, inventory_value_usd, skus_out_of_stock, capacity_utilisation, units_shipped_90d, shrinkage_rate_90d FROM {MARTS}.warehouse_performance ORDER BY inventory_value_usd DESC"
    )
    left, right = st.columns(2)
    fig = px.bar(
        wh,
        x="inventory_value_usd",
        y="warehouse_name",
        orientation="h",
        title="Inventory value by warehouse (USD)",
    )
    fig.update_traces(marker_color=SERIES[0], hovertemplate="%{y}: $%{x:,.0f}<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    left.plotly_chart(style(fig, 380), use_container_width=True)
    fig = px.bar(
        wh, x="on_hand_units", y="warehouse_name", orientation="h", title="Inventory levels (units on hand)"
    )
    fig.update_traces(marker_color=SERIES[2], hovertemplate="%{y}: %{x:,.0f} units<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    right.plotly_chart(style(fig, 380), use_container_width=True)

    st.markdown("**Warehouse performance**")
    st.dataframe(wh, use_container_width=True, hide_index=True)

    low = q(
        f"SELECT product_name, category_name, on_hand_units, velocity_units_per_day, days_of_cover, turnover_ratio_90d, high_velocity_low_stock FROM {MARTS}.inventory_turnover WHERE is_low_stock OR turnover_ratio_90d > 2 ORDER BY days_of_cover NULLS LAST LIMIT 40"
    )
    st.markdown(
        "**Low stock & high velocity products** (days of cover below 14, or turnover > 2x in 90 days)"
    )
    st.dataframe(low, use_container_width=True, hide_index=True)

    turn = q(
        f"SELECT category_name, avg(turnover_ratio_90d) turnover FROM {MARTS}.inventory_turnover WHERE turnover_ratio_90d IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
    )
    fig = px.bar(
        turn,
        x="turnover",
        y="category_name",
        orientation="h",
        title="Average 90-day inventory turnover ratio by category",
    )
    fig.update_traces(marker_color=SERIES[0], hovertemplate="%{y}: %{x:.2f}x<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(style(fig, 460), use_container_width=True)


def page_customers():
    st.subheader("Customers")
    top = q(
        f"SELECT customer_id, full_name, customer_segment, region_code, total_orders, net_lifetime_value_usd, avg_order_value_usd, last_order_date, lifecycle_stage FROM {MARTS}.customer_lifetime_value ORDER BY net_lifetime_value_usd DESC LIMIT 25"
    )
    stages = q(f"SELECT lifecycle_stage, count(*) customers FROM {MARTS}.customer_lifetime_value GROUP BY 1")
    seg = q(
        f"SELECT customer_segment, avg(net_lifetime_value_usd) avg_value, count(*) customers FROM {MARTS}.customer_lifetime_value GROUP BY 1 ORDER BY 2 DESC"
    )
    left, right = st.columns(2)
    order = ["active", "at_risk", "churned", "never_purchased"]
    fig = px.bar(
        stages,
        x="lifecycle_stage",
        y="customers",
        category_orders={"lifecycle_stage": order},
        title="Customer lifecycle",
    )
    fig.update_traces(marker_color=SERIES[0], hovertemplate="%{x}: %{y:,.0f}<extra></extra>")
    left.plotly_chart(style(fig), use_container_width=True)
    fig = px.bar(
        seg, x="customer_segment", y="avg_value", title="Average net lifetime value by segment (USD)"
    )
    fig.update_traces(marker_color=SERIES[0], hovertemplate="%{x}: $%{y:,.0f}<extra></extra>")
    right.plotly_chart(style(fig), use_container_width=True)
    st.markdown("**Highest lifetime value customers**")
    st.dataframe(top, use_container_width=True, hide_index=True)


def page_shipping():
    st.subheader("Shipping & suppliers")
    reg = q(
        f"SELECT region_name, sum(avg_delivery_days*delivered)/nullif(sum(delivered),0) delivery_days, sum(on_time_rate*delivered)/nullif(sum(delivered),0) on_time, max(sla_days) sla FROM {MARTS}.shipping_performance GROUP BY 1 ORDER BY 2"
    )
    left, right = st.columns(2)
    fig = px.bar(
        reg,
        x="delivery_days",
        y="region_name",
        orientation="h",
        title="Average delivery time by region (days)",
        custom_data=["sla", "on_time"],
    )
    fig.update_traces(
        marker_color=SERIES[0],
        hovertemplate="%{y}: %{x:.2f} days (SLA %{customdata[0]:.1f}, on-time %{customdata[1]:.0%})<extra></extra>",
    )
    fig.update_yaxes(autorange="reversed")
    left.plotly_chart(style(fig, 380), use_container_width=True)
    car = q(
        f"SELECT carrier, sum(avg_delivery_days*delivered)/nullif(sum(delivered),0) delivery_days, sum(exceptions)::numeric/nullif(sum(shipments),0) exception_rate FROM {MARTS}.shipping_performance GROUP BY 1 ORDER BY 2"
    )
    fig = px.bar(
        car,
        x="delivery_days",
        y="carrier",
        orientation="h",
        title="Carrier performance (avg delivery days)",
        custom_data=["exception_rate"],
    )
    fig.update_traces(
        marker_color=SERIES[0],
        hovertemplate="%{y}: %{x:.2f} days, exceptions %{customdata[0]:.1%}<extra></extra>",
    )
    fig.update_yaxes(autorange="reversed")
    right.plotly_chart(style(fig, 380), use_container_width=True)

    trend = q(
        f"SELECT order_month, sum(avg_delivery_days*delivered)/nullif(sum(delivered),0) delivery_days, sum(on_time_rate*delivered)/nullif(sum(delivered),0) on_time FROM {MARTS}.shipping_performance GROUP BY 1 ORDER BY 1"
    )
    fig = go.Figure(
        go.Scatter(
            x=trend.order_month,
            y=trend.delivery_days,
            line=dict(color=SERIES[0], width=2),
            name="Avg delivery days",
        )
    )
    fig.update_layout(title="Delivery time trend (days)")
    st.plotly_chart(style(fig, 260), use_container_width=True)

    sup = q(
        f"SELECT supplier_name, quality_tier, units_received, units_defective, defect_rate, return_rate, revenue_usd FROM {MARTS}.supplier_performance WHERE units_received > 0 ORDER BY defect_rate DESC LIMIT 20"
    )
    st.markdown("**Suppliers with the highest defect rate** (defective units / units received)")
    st.dataframe(sup, use_container_width=True, hide_index=True)


def page_pipeline():
    st.subheader("Pipeline health")
    runs = q(
        "SELECT run_id, mode, batch_id, status, started_at, finished_at, duration_seconds, rows_ingested, rows_rejected, rows_quarantined, rows_loaded, quality_status FROM monitoring.pipeline_runs ORDER BY started_at DESC LIMIT 20"
    )
    if runs.empty:
        st.info(
            "No pipeline runs recorded yet. Run `python -m dataforge.pipeline.runner --batch 2025-11-30`."
        )
        return
    last = runs.iloc[0]
    c = st.columns(6)
    c[0].metric("Last run", str(last.finished_at or last.started_at)[:16].replace("T", " "), last.run_id)
    c[1].metric("Status", str(last.status).upper())
    c[2].metric(
        "Duration",
        f"{float(last.duration_seconds or 0) / 60:.1f} min",
        f"batch {last.batch_id} ({last.mode})",
    )
    c[3].metric("Rows processed", num(last.rows_ingested))
    c[4].metric(
        "Rows quarantined",
        num(int(last.rows_rejected or 0) + int(last.rows_quarantined or 0)),
        f"{int(last.rows_rejected or 0)} parse / {int(last.rows_quarantined or 0)} rule",
        delta_color="off",
    )
    c[5].metric("Quality status", str(last.quality_status or "n/a").upper())

    tasks = q(
        "SELECT task_name, status, duration_seconds, rows_in, rows_out, rows_rejected FROM monitoring.task_runs WHERE run_id=%s ORDER BY started_at",
        (last.run_id,),
    )
    left, right = st.columns([3, 2])
    fig = px.bar(
        tasks,
        x="duration_seconds",
        y="task_name",
        orientation="h",
        title="Task duration (seconds)",
        color="status",
        color_discrete_map={"success": SERIES[0], "failed": STATUS["fail"], "running": SERIES[3]},
    )
    fig.update_traces(hovertemplate="%{y}: %{x:.1f}s<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    left.plotly_chart(style(fig, 560), use_container_width=True)

    qres = q(
        "SELECT stage, dataset, rule_id, severity, failed_rows, total_rows, failure_rate, passed FROM monitoring.quality_results WHERE run_id=%s AND failed_rows > 0 ORDER BY failed_rows DESC",
        (last.run_id,),
    )
    right.markdown("**Quality rule hits** (rows quarantined / flagged per rule)")
    right.dataframe(qres, use_container_width=True, hide_index=True, height=560)

    drift = q(
        "SELECT dataset, change_type, column_name, severity, batch_id, detected_at FROM monitoring.schema_events ORDER BY detected_at DESC LIMIT 20"
    )
    if not drift.empty:
        st.markdown("**Schema drift events**")
        st.dataframe(drift, use_container_width=True, hide_index=True)

    st.markdown("**Run history**")
    st.dataframe(runs, use_container_width=True, hide_index=True)


PAGES = {
    "Overview": page_overview,
    "Sales": page_sales,
    "Inventory": page_inventory,
    "Customers": page_customers,
    "Shipping & Suppliers": page_shipping,
    "Pipeline Health": page_pipeline,
}


def main():
    st.title("DataForge - E-commerce Analytics")
    st.caption("Gold-layer analytics backed by PostgreSQL + dbt marts. All data is synthetic.")
    choice = st.sidebar.radio("Section", list(PAGES))
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Warehouse: {get_settings().redacted_dsn()}")
    try:
        PAGES[choice]()
    except DatabaseUnavailable as e:
        st.error(f"PostgreSQL is unavailable: {e}")
    except Exception as e:  # missing marts before the first pipeline run
        st.error(f"Could not load this section: {type(e).__name__}: {e}")
        st.info("Run the pipeline first: python -m dataforge.pipeline.runner --batch 2025-11-30")


main()
