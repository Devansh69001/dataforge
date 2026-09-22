-- DataForge warehouse layout
--   warehouse   dimensional model loaded from the gold zone (Spark)
--   analytics   dbt marts built on top of warehouse.*
--   monitoring  pipeline telemetry: runs, tasks, quality results, schema events, watermarks
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;
