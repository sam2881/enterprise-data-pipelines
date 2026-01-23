-- Enterprise Agentic Data Platform
-- DDL: Data Quality Rules Table
-- Version: 1.0.0

-- Drop existing table if recreating
-- DROP TABLE IF EXISTS data_quality_rule CASCADE;

CREATE TABLE IF NOT EXISTS data_quality_rule (
    dq_rule_id          SERIAL PRIMARY KEY,
    pipeline_id         INTEGER NOT NULL REFERENCES pipeline(pipeline_id),
    rule_name           VARCHAR(255) NOT NULL,
    rule_type           VARCHAR(50) NOT NULL,
    column_name         VARCHAR(255),
    rule_expression     TEXT NOT NULL,
    threshold           DECIMAL(5,4),
    severity            VARCHAR(20) NOT NULL DEFAULT 'warning',
    is_blocking         BOOLEAN DEFAULT FALSE,
    is_active           BOOLEAN DEFAULT TRUE,
    created_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT chk_dq_rule_type CHECK (rule_type IN ('not_null', 'unique', 'range', 'regex', 'referential', 'custom')),
    CONSTRAINT chk_dq_severity CHECK (severity IN ('info', 'warning', 'error', 'critical'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dq_pipeline_id ON data_quality_rule(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_dq_rule_type ON data_quality_rule(rule_type);
CREATE INDEX IF NOT EXISTS idx_dq_is_blocking ON data_quality_rule(is_blocking);

-- Comments
COMMENT ON TABLE data_quality_rule IS 'Data quality validation rules';
COMMENT ON COLUMN data_quality_rule.rule_type IS 'Type of quality check';
COMMENT ON COLUMN data_quality_rule.threshold IS 'Acceptable threshold (e.g., 0.99 for 99%)';
COMMENT ON COLUMN data_quality_rule.is_blocking IS 'Whether failure blocks pipeline';
