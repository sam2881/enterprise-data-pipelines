-- ═══════════════════════════════════════════════════════════════════════════
-- APEX Metadata: Observability Metrics
-- Stores historical pipeline metrics for anomaly detection baselines
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS platform_observability_metrics (
    metric_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id          VARCHAR(100) NOT NULL,
    contract_id      UUID,
    execution_id     UUID NOT NULL,
    execution_date   DATE NOT NULL,
    zone_level       VARCHAR(20) NOT NULL,

    -- Volume metrics
    row_count        BIGINT,
    byte_count       BIGINT,

    -- Quality metrics
    quality_score    NUMERIC(5,2),
    null_rate_pct    NUMERIC(5,2),
    duplicate_rate_pct NUMERIC(5,2),

    -- Freshness metrics
    expected_arrival TIMESTAMP,
    actual_arrival   TIMESTAMP,
    delay_minutes    NUMERIC(10,2),

    -- Statistical baselines (JSON)
    column_stats     JSONB,  -- {"col_name": {"mean": X, "stddev": Y, "min": Z, "max": W, "null_pct": N}}

    -- Drift flags
    schema_drift     BOOLEAN DEFAULT false,
    volume_drift     BOOLEAN DEFAULT false,
    freshness_drift  BOOLEAN DEFAULT false,
    statistical_drift BOOLEAN DEFAULT false,

    created_at       TIMESTAMP NOT NULL DEFAULT now(),

    CONSTRAINT chk_obs_zone CHECK (zone_level IN ('BRONZE', 'SILVER', 'GOLD'))
);

CREATE INDEX IF NOT EXISTS idx_obs_feed_date
    ON platform_observability_metrics(feed_id, execution_date DESC);

CREATE INDEX IF NOT EXISTS idx_obs_contract_zone
    ON platform_observability_metrics(contract_id, zone_level, execution_date DESC);

-- View for computing rolling averages (anomaly baseline)
CREATE OR REPLACE VIEW v_observability_baseline AS
SELECT
    feed_id,
    zone_level,
    AVG(row_count) AS avg_row_count,
    STDDEV(row_count) AS stddev_row_count,
    AVG(quality_score) AS avg_quality_score,
    AVG(delay_minutes) AS avg_delay_minutes,
    COUNT(*) AS sample_count,
    MAX(execution_date) AS last_execution_date
FROM platform_observability_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY feed_id, zone_level;

COMMENT ON TABLE platform_observability_metrics IS
    'Historical pipeline metrics for anomaly detection. 30-day rolling window used for baselines.';
