-- Order headers are authoritative for revenue; their lines can be partially quarantined
-- (invalid quantity, unknown product ...). Silver already recomputes inconsistent line
-- totals, so a header/lines gap means missing lines. The gap is expected to be small:
-- fail when more than 10% of revenue orders do not reconcile (upstream breakage signal;
-- the synthetic defect profile yields ~8.5%).
with stats as (
    select
        count(*) as revenue_orders,
        count(*) filter (where not lines_reconciled) as unreconciled
    from {{ ref('stg_orders') }}
    where is_revenue
)
select *
from stats
where revenue_orders > 0 and unreconciled::numeric / revenue_orders > 0.10
