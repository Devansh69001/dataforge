-- Pipeline telemetry. Written by dataforge.monitoring; read by the API, the dashboard
-- and the freshness / quality checks.

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id              TEXT PRIMARY KEY,
    mode                TEXT NOT NULL,              -- initial | incremental | rerun
    batch_id            TEXT,
    status              TEXT NOT NULL,              -- running | success | failed
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    duration_seconds    NUMERIC(10,2),
    rows_ingested       BIGINT DEFAULT 0,
    rows_rejected       BIGINT DEFAULT 0,           -- parse-level rejects at ingestion
    rows_quarantined    BIGINT DEFAULT 0,           -- rule-level rejects in silver
    rows_loaded         BIGINT DEFAULT 0,
    quality_status      TEXT,                       -- pass | warn | fail
    error               TEXT,
    triggered_by        TEXT,
    details             JSONB
);

CREATE TABLE IF NOT EXISTS monitoring.task_runs (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL REFERENCES monitoring.pipeline_runs(run_id) ON DELETE CASCADE,
    task_name           TEXT NOT NULL,
    status              TEXT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    duration_seconds    NUMERIC(10,2),
    rows_in             BIGINT,
    rows_out            BIGINT,
    rows_rejected       BIGINT,
    error               TEXT,
    details             JSONB
);
CREATE INDEX IF NOT EXISTS ix_task_runs_run ON monitoring.task_runs (run_id);

CREATE TABLE IF NOT EXISTS monitoring.quality_results (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    stage           TEXT NOT NULL,      -- silver | warehouse | dbt
    dataset         TEXT NOT NULL,
    rule_id         TEXT NOT NULL,
    severity        TEXT NOT NULL,      -- reject | warn | error
    total_rows      BIGINT,
    failed_rows     BIGINT,
    failure_rate    NUMERIC(10,6),
    threshold       NUMERIC(10,6),
    passed          BOOLEAN NOT NULL,
    details         JSONB,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_quality_results_run ON monitoring.quality_results (run_id);

CREATE TABLE IF NOT EXISTS monitoring.schema_events (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT,
    dataset         TEXT NOT NULL,
    change_type     TEXT NOT NULL,      -- new_column | missing_optional | missing_required
    column_name     TEXT NOT NULL,
    severity        TEXT NOT NULL,
    batch_id        TEXT,
    source_file     TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monitoring.ingestion_batches (
    dataset         TEXT NOT NULL,
    file_hash       TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    batch_id        TEXT NOT NULL,
    rows            BIGINT,
    rejected        BIGINT,
    bronze_path     TEXT,
    run_id          TEXT,
    status          TEXT,
    ingested_at     TIMESTAMPTZ,
    PRIMARY KEY (dataset, file_hash)
);

CREATE TABLE IF NOT EXISTS monitoring.watermarks (
    dataset             TEXT PRIMARY KEY,
    last_batch_id       TEXT,
    high_watermark_ts   TIMESTAMPTZ,
    last_run_id         TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
