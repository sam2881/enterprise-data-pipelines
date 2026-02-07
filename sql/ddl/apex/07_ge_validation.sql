-- =============================================================================
-- GE Validation Result Tables
-- =============================================================================
-- Stores Great Expectations checkpoint results from Spark validation jobs.
-- Two tables: detailed per-expectation results and per-run summaries.
-- =============================================================================

-- Per-expectation results (one row per rule per run)
CREATE TABLE IF NOT EXISTS ge_validation_result (
    result_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_id         VARCHAR(200) NOT NULL,
    run_id          VARCHAR(200) NOT NULL,
    sequence        INTEGER NOT NULL DEFAULT 0,
    validation_type VARCHAR(50) NOT NULL,        -- SCHEMA, SEMANTIC, QUALITY
    zone_level      VARCHAR(20) NOT NULL,         -- BRONZE, SILVER, GOLD
    batch_identifier VARCHAR(500),
    expectation_suite_name VARCHAR(200),
    column_name     VARCHAR(200),
    expectation_type VARCHAR(200) NOT NULL,
    result          VARCHAR(20) NOT NULL,         -- PASSED, FAILED, ERROR
    element_count   BIGINT DEFAULT 0,
    error_count     BIGINT DEFAULT 0,
    error_rate      DECIMAL(10,6) DEFAULT 0,
    partial_unexpected_list JSONB DEFAULT '[]',
    observed_value  TEXT,
    kwargs          JSONB DEFAULT '{}',
    severity        VARCHAR(20) DEFAULT 'ERROR',
    weight          DECIMAL(5,2) DEFAULT 1.0,
    rule_name       VARCHAR(200),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ge_result_feed_run
    ON ge_validation_result(feed_id, run_id);
CREATE INDEX IF NOT EXISTS idx_ge_result_created
    ON ge_validation_result(created_at);

-- Per-run summary (one row per checkpoint execution)
CREATE TABLE IF NOT EXISTS ge_validation_summary (
    summary_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_id         VARCHAR(200) NOT NULL,
    run_id          VARCHAR(200) NOT NULL,
    checkpoint_name VARCHAR(200),
    expectation_suite_name VARCHAR(200),
    validation_type VARCHAR(50),
    zone_level      VARCHAR(20),
    total_expectations    INTEGER DEFAULT 0,
    successful_expectations INTEGER DEFAULT 0,
    unsuccessful_expectations INTEGER DEFAULT 0,
    quality_score   DECIMAL(6,2) DEFAULT 0,
    success         BOOLEAN DEFAULT FALSE,
    records_validated BIGINT DEFAULT 0,
    duration_seconds DECIMAL(10,3) DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, run_id, checkpoint_name)
);

CREATE INDEX IF NOT EXISTS idx_ge_summary_feed_run
    ON ge_validation_summary(feed_id, run_id);
