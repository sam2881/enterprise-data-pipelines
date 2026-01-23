-- Enterprise Agentic Data Platform
-- DDL: Transformation Rules Table
-- Version: 1.0.0

-- Drop existing table if recreating
-- DROP TABLE IF EXISTS transformation_rule CASCADE;

CREATE TABLE IF NOT EXISTS transformation_rule (
    rule_id             SERIAL PRIMARY KEY,
    pipeline_id         INTEGER NOT NULL REFERENCES pipeline(pipeline_id),
    schema_version_id   INTEGER REFERENCES schema_version(schema_version_id),
    rule_name           VARCHAR(255) NOT NULL,
    rule_type           VARCHAR(50) NOT NULL,
    source_column       VARCHAR(255),
    target_column       VARCHAR(255) NOT NULL,
    transformation_expr TEXT NOT NULL,
    rule_order          INTEGER NOT NULL DEFAULT 0,
    is_active           BOOLEAN DEFAULT TRUE,
    created_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT chk_rule_type CHECK (rule_type IN ('rename', 'cast', 'derive', 'filter', 'aggregate', 'join', 'custom'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_transform_pipeline_id ON transformation_rule(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_transform_schema_id ON transformation_rule(schema_version_id);
CREATE INDEX IF NOT EXISTS idx_transform_rule_order ON transformation_rule(pipeline_id, rule_order);

-- Comments
COMMENT ON TABLE transformation_rule IS 'Transformation rules for pipeline processing';
COMMENT ON COLUMN transformation_rule.rule_type IS 'Type of transformation operation';
COMMENT ON COLUMN transformation_rule.transformation_expr IS 'SQL/Spark expression for transformation';
COMMENT ON COLUMN transformation_rule.rule_order IS 'Order of rule execution';
