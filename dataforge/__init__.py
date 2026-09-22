"""DataForge - production-style data engineering & analytics platform.

Layers:
    generators  -> reproducible synthetic multi-source data (with controlled defects)
    ingestion   -> heterogeneous source readers + bronze landing
    quality     -> declarative validation rules, quarantine, warehouse checks
    spark       -> PySpark silver (clean/dedup/validate) and gold (dims/facts) jobs
    warehouse   -> PostgreSQL DDL, idempotent upsert loader, watermarks
    monitoring  -> run/task/quality telemetry
    pipeline    -> task functions shared by the CLI runner and the Airflow DAG
"""

__version__ = "0.1.0"
