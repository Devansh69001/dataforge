"""DataForge analytics API.

Thin read-only layer over the dbt marts (analytics_marts.*) and the monitoring schema.
It exists so downstream consumers (BI embeds, ML feature jobs, alerting) get stable JSON
without knowing the warehouse layout.

    uvicorn api.main:app --port 8000
"""

from __future__ import annotations

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataforge import __version__  # noqa: E402
from dataforge.config import get_settings  # noqa: E402
from dataforge.db import DatabaseUnavailable, fetch_all, fetch_one, ping  # noqa: E402
from dataforge.monitoring.tracker import list_runs  # noqa: E402

MARTS = "analytics_marts"

app = FastAPI(
    title="DataForge Analytics API",
    version=__version__,
    description="Read-only access to the DataForge gold layer (dbt marts) and pipeline telemetry.",
)


def _clean(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        out.append(
            {
                k: (float(v) if isinstance(v, Decimal) else v.isoformat() if hasattr(v, "isoformat") else v)
                for k, v in r.items()
            }
        )
    return out


def _query(sql: str, params: Any = None) -> list[dict[str, Any]]:
    try:
        return _clean(fetch_all(sql, params))
    except DatabaseUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:  # missing mart (pipeline not run yet), bad SQL param ...
        raise HTTPException(status_code=500, detail=f"query failed: {type(e).__name__}: {e}") from e


# ------------------------------------------------------------------ health
@app.get("/health")
def health() -> dict[str, Any]:
    s = get_settings()
    db_ok = ping(s)
    last = None
    if db_ok:
        try:
            last = fetch_one(
                "SELECT run_id, finished_at, batch_id FROM monitoring.pipeline_runs WHERE status='success' ORDER BY finished_at DESC LIMIT 1"
            )
        except Exception:
            last = None
    body = {
        "status": "ok" if db_ok else "degraded",
        "version": __version__,
        "database": "ok" if db_ok else "unavailable",
        "last_successful_run": _clean([last])[0] if last else None,
    }
    return JSONResponse(body, status_code=200 if db_ok else 503)


# ------------------------------------------------------------------- sales
@app.get("/metrics/sales")
def sales(
    granularity: str = Query("monthly", pattern="^(daily|monthly)$"),
    date_from: date | None = Query(None, alias="from"),
    date_to: date | None = Query(None, alias="to"),
    limit: int = Query(400, ge=1, le=5000),
) -> dict[str, Any]:
    if granularity == "daily":
        sql = f"SELECT sales_date, year_month, orders_placed, revenue_orders, revenue_usd, gross_margin_usd, units_sold, active_customers, new_customers, avg_order_value_usd FROM {MARTS}.daily_sales_summary WHERE (%(f)s::date IS NULL OR sales_date >= %(f)s::date) AND (%(t)s::date IS NULL OR sales_date <= %(t)s::date) ORDER BY sales_date DESC LIMIT %(l)s"
    else:
        sql = f"SELECT year_month, month_start, orders_placed, revenue_orders, revenue_usd, gross_margin_usd, gross_margin_pct, units_sold, active_customers, new_customers, avg_order_value_usd, revenue_mom_growth FROM {MARTS}.monthly_revenue_summary WHERE (%(f)s::date IS NULL OR month_start >= %(f)s::date) AND (%(t)s::date IS NULL OR month_start <= %(t)s::date) ORDER BY year_month DESC LIMIT %(l)s"
    rows = _query(sql, {"f": date_from, "t": date_to, "l": limit})
    totals = _query(
        f"SELECT sum(revenue_usd) AS revenue_usd, sum(revenue_orders) AS revenue_orders, sum(units_sold) AS units_sold FROM {MARTS}.daily_sales_summary WHERE (%(f)s::date IS NULL OR sales_date >= %(f)s::date) AND (%(t)s::date IS NULL OR sales_date <= %(t)s::date)",
        {"f": date_from, "t": date_to},
    )
    return {"granularity": granularity, "totals": totals[0] if totals else {}, "rows": rows}


@app.get("/metrics/sales/categories")
def sales_categories(
    month: str | None = Query(None, pattern=r"^\d{4}-\d{2}$"), limit: int = Query(20, ge=1, le=100)
) -> dict[str, Any]:
    if month:
        rows = _query(
            f"SELECT parent_category_name, category_name, order_month, orders, units_sold, revenue_usd, gross_margin_usd, revenue_share FROM {MARTS}.category_revenue WHERE order_month = %(m)s ORDER BY revenue_usd DESC LIMIT %(l)s",
            {"m": month, "l": limit},
        )
    else:
        rows = _query(
            f"SELECT parent_category_name, category_name, sum(orders) AS orders, sum(units_sold) AS units_sold, sum(revenue_usd) AS revenue_usd, sum(gross_margin_usd) AS gross_margin_usd FROM {MARTS}.category_revenue GROUP BY 1,2 ORDER BY revenue_usd DESC LIMIT %(l)s",
            {"l": limit},
        )
    return {"month": month, "rows": rows}


@app.get("/metrics/sales/products")
def sales_products(limit: int = Query(20, ge=1, le=500), declining: bool = False) -> dict[str, Any]:
    where = "WHERE is_declining" if declining else ""
    order = (
        "ORDER BY revenue_trend_ratio ASC NULLS LAST, revenue_prior_90d DESC"
        if declining
        else "ORDER BY revenue_usd DESC"
    )
    rows = _query(
        f"SELECT product_id, product_name, brand, category_name, revenue_usd, units_sold, orders, revenue_last_90d, revenue_prior_90d, revenue_trend_ratio, is_declining, velocity_units_per_day, revenue_rank FROM {MARTS}.product_sales_metrics {where} {order} LIMIT %(l)s",
        {"l": limit},
    )
    return {"declining_only": declining, "rows": rows}


# --------------------------------------------------------------- inventory
@app.get("/metrics/inventory")
def inventory(limit: int = Query(25, ge=1, le=500)) -> dict[str, Any]:
    summary = _query(
        f"SELECT sum(on_hand_units) AS on_hand_units, sum(inventory_value_usd) AS inventory_value_usd, count(*) AS products, count(*) FILTER (WHERE is_low_stock) AS low_stock_products, count(*) FILTER (WHERE high_velocity_low_stock) AS high_velocity_low_stock FROM {MARTS}.inventory_turnover"
    )
    low = _query(
        f"SELECT product_id, product_name, category_name, on_hand_units, velocity_units_per_day, days_of_cover, turnover_ratio_90d, inventory_value_usd FROM {MARTS}.inventory_turnover WHERE is_low_stock ORDER BY days_of_cover ASC NULLS LAST LIMIT %(l)s",
        {"l": limit},
    )
    wh = _query(
        f"SELECT warehouse_id, warehouse_name, region_code, on_hand_units, inventory_value_usd, skus_out_of_stock, capacity_utilisation, units_shipped_90d, shrinkage_rate_90d, avg_delivery_days, on_time_rate FROM {MARTS}.warehouse_performance ORDER BY inventory_value_usd DESC"
    )
    return {"summary": summary[0] if summary else {}, "low_stock": low, "warehouses": wh}


# --------------------------------------------------------------- customers
@app.get("/metrics/customers")
def customers(limit: int = Query(20, ge=1, le=500)) -> dict[str, Any]:
    top = _query(
        f"SELECT customer_id, full_name, customer_segment, region_code, total_orders, lifetime_revenue_usd, net_lifetime_value_usd, avg_order_value_usd, last_order_date, lifecycle_stage FROM {MARTS}.customer_lifetime_value ORDER BY net_lifetime_value_usd DESC LIMIT %(l)s",
        {"l": limit},
    )
    stages = _query(
        f"SELECT lifecycle_stage, count(*) AS customers, sum(net_lifetime_value_usd) AS net_value_usd FROM {MARTS}.customer_lifetime_value GROUP BY 1 ORDER BY 2 DESC"
    )
    segments = _query(
        f"SELECT customer_segment, count(*) AS customers, avg(net_lifetime_value_usd) AS avg_net_value_usd FROM {MARTS}.customer_lifetime_value GROUP BY 1 ORDER BY 3 DESC"
    )
    return {"top_customers": top, "lifecycle": stages, "segments": segments}


# ---------------------------------------------------------------- shipping
@app.get("/metrics/shipping")
def shipping(month: str | None = Query(None, pattern=r"^\d{4}-\d{2}$")) -> dict[str, Any]:
    flt = "WHERE order_month = %(m)s" if month else ""
    regions = _query(
        f"SELECT region_code, region_name, sum(shipments) AS shipments, sum(delivered) AS delivered, sum(avg_delivery_days*delivered)/nullif(sum(delivered),0) AS avg_delivery_days, sum(on_time_rate*delivered)/nullif(sum(delivered),0) AS on_time_rate, sum(exceptions)::numeric/nullif(sum(shipments),0) AS exception_rate FROM {MARTS}.shipping_performance {flt} GROUP BY 1,2 ORDER BY avg_delivery_days",
        {"m": month},
    )
    carriers = _query(
        f"SELECT carrier, sum(shipments) AS shipments, sum(avg_delivery_days*delivered)/nullif(sum(delivered),0) AS avg_delivery_days, sum(on_time_rate*delivered)/nullif(sum(delivered),0) AS on_time_rate, sum(exceptions)::numeric/nullif(sum(shipments),0) AS exception_rate FROM {MARTS}.shipping_performance {flt} GROUP BY 1 ORDER BY avg_delivery_days",
        {"m": month},
    )
    return {"month": month, "regions": regions, "carriers": carriers}


# -------------------------------------------------------------- suppliers
@app.get("/metrics/suppliers")
def suppliers(limit: int = Query(20, ge=1, le=500)) -> dict[str, Any]:
    rows = _query(
        f"SELECT supplier_id, supplier_name, quality_tier, products_supplied, units_received, units_defective, defect_rate, return_rate, revenue_usd, defect_rank FROM {MARTS}.supplier_performance WHERE units_received > 0 ORDER BY defect_rate DESC NULLS LAST LIMIT %(l)s",
        {"l": limit},
    )
    return {"rows": rows}


# --------------------------------------------------------------- pipeline
@app.get("/pipeline/status")
def pipeline_status() -> dict[str, Any]:
    run = _query(
        "SELECT run_id, mode, batch_id, status, started_at, finished_at, duration_seconds, rows_ingested, rows_rejected, rows_quarantined, rows_loaded, quality_status, error FROM monitoring.pipeline_runs ORDER BY started_at DESC LIMIT 1"
    )
    if not run:
        docs = list_runs(limit=1)
        return {"run": docs[0] if docs else None, "tasks": [], "quality": {}, "source": "file"}
    r = run[0]
    tasks = _query(
        "SELECT task_name, status, duration_seconds, rows_in, rows_out, rows_rejected, error FROM monitoring.task_runs WHERE run_id = %(r)s ORDER BY started_at",
        {"r": r["run_id"]},
    )
    quality = _query(
        "SELECT stage, count(*) AS checks, count(*) FILTER (WHERE NOT passed) AS failed, count(*) FILTER (WHERE NOT passed AND severity IN ('error','reject')) AS failed_blocking FROM monitoring.quality_results WHERE run_id = %(r)s GROUP BY stage",
        {"r": r["run_id"]},
    )
    drift = _query(
        "SELECT dataset, change_type, column_name, severity FROM monitoring.schema_events WHERE run_id = %(r)s",
        {"r": r["run_id"]},
    )
    watermarks = _query(
        "SELECT dataset, last_batch_id, high_watermark_ts, updated_at FROM monitoring.watermarks ORDER BY dataset"
    )
    return {
        "run": r,
        "tasks": tasks,
        "quality": {q["stage"]: q for q in quality},
        "schema_events": drift,
        "watermarks": watermarks,
        "source": "database",
    }


@app.get("/pipeline/runs")
def pipeline_runs(limit: int = Query(20, ge=1, le=200)) -> dict[str, Any]:
    rows = _query(
        "SELECT run_id, mode, batch_id, status, started_at, finished_at, duration_seconds, rows_ingested, rows_rejected, rows_quarantined, rows_loaded, quality_status FROM monitoring.pipeline_runs ORDER BY started_at DESC LIMIT %(l)s",
        {"l": limit},
    )
    return {"rows": rows}


@app.get("/pipeline/quality")
def pipeline_quality(run_id: str | None = None, failed_only: bool = False) -> dict[str, Any]:
    if run_id is None:
        last = _query("SELECT run_id FROM monitoring.pipeline_runs ORDER BY started_at DESC LIMIT 1")
        if not last:
            return {"run_id": None, "rows": []}
        run_id = last[0]["run_id"]
    flt = "AND NOT passed" if failed_only else ""
    rows = _query(
        f"SELECT stage, dataset, rule_id, severity, total_rows, failed_rows, failure_rate, threshold, passed FROM monitoring.quality_results WHERE run_id = %(r)s {flt} ORDER BY passed, stage, dataset, rule_id",
        {"r": run_id},
    )
    return {"run_id": run_id, "rows": rows}
