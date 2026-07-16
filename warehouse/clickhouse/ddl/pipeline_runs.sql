-- pipeline_runs — audit log for every DAG run (FR-14)
-- ReplacingMergeTree: rerun deduplicates by (run_id, execution_date).

CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
    run_id          String,
    execution_date  DateTime,
    status          LowCardinality(String),
    rows_silver     Int64,
    rows_rejects    Int64,
    rows_gold       Int64,
    duration_sec    Float64,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (run_id, execution_date)
SETTINGS index_granularity = 8192;
