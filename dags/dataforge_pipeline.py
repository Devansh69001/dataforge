"""DataForge daily pipeline DAG.

The graph is generated from `dataforge.pipeline.spec.PIPELINE`, so Airflow runs exactly
the same tasks, in the same order, as the CLI runner. Each task is a PythonOperator that
re-opens the shared run document (data/monitoring/runs/<run_id>.json) and calls the task
function; state flows between tasks through that document, not through XCom payloads.

    batch_id  = the DAG's logical date (YYYY-MM-DD) unless overridden via dag_run.conf
    run_id    = "airflow_<dag_run.run_id>" (safe to retry: every task is idempotent)

Schedule: daily at 02:00 UTC. Retries: 2 with 5-minute delay (3 for source checks).
Failure handling: the failing task records its error in monitoring.task_runs; the run is
marked failed by `on_failure_callback`, and the whole graph can be resumed from the failed
task with the CLI (`--from-task`) or by clearing the task in Airflow.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

REPO_ROOT = Path(os.environ.get("DATAFORGE_REPO_ROOT", Path(__file__).resolve().parent.parent))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dataforge.pipeline.spec import PIPELINE  # noqa: E402


def _batch_id(context) -> str:
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    return conf.get("batch_id") or context["ds"]


def _run_id(context) -> str:
    return "airflow_" + context["dag_run"].run_id.replace(":", "").replace("+", "_")


def _execute(task_name: str, **context):
    from dataforge.pipeline.tasks import TASK_FUNCTIONS, make_context

    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    ctx = make_context(
        run_id=_run_id(context),
        batch_id=_batch_id(context),
        mode=conf.get("mode"),
        triggered_by="airflow",
        force_ingest=bool(conf.get("force_ingest", False)),
    )
    if task_name == PIPELINE[0].name:
        ctx.tracker.start()
    result = TASK_FUNCTIONS[task_name](ctx)
    if task_name == PIPELINE[-1].name:
        ctx.tracker.finish("success")
    # keep XCom small: only counts, never data
    return {k: v for k, v in (result or {}).items() if isinstance(v, (int, float, str, bool))}


def _on_failure(context):
    from dataforge.pipeline.tasks import make_context

    try:
        ctx = make_context(run_id=_run_id(context), batch_id=_batch_id(context), triggered_by="airflow")
        ctx.tracker.finish(
            "failed", error=f"task {context['task_instance'].task_id} failed: {context.get('exception')}"
        )
    except Exception:  # never mask the original failure
        pass


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="dataforge_pipeline",
    description="Bronze -> Silver (Spark) -> Gold -> PostgreSQL -> dbt marts -> quality gates -> publish",
    start_date=datetime(2025, 11, 30),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dataforge", "e-commerce", "spark", "dbt"],
    doc_md=__doc__,
) as dag:
    operators: dict[str, PythonOperator] = {}
    groups: dict[str, TaskGroup] = {}

    for spec in PIPELINE:
        kwargs = dict(
            task_id=spec.name,
            python_callable=_execute,
            op_kwargs={"task_name": spec.name},
            retries=spec.retries,
            doc=spec.description,
        )
        if spec.group:
            if spec.group not in groups:
                groups[spec.group] = TaskGroup(group_id=spec.group, dag=dag)
            with groups[spec.group]:
                operators[spec.name] = PythonOperator(**kwargs)
        else:
            operators[spec.name] = PythonOperator(**kwargs)

    for spec in PIPELINE:
        for up in spec.upstream:
            operators[up] >> operators[spec.name]
