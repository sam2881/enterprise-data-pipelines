-- Enterprise Agentic Data Platform
-- DDL: Schema Version Table
-- Version: 1.0.0

-- Drop existing table if recreating
-- DROP TABLE IF EXISTS schema_version CASCADE;

CREATE TABLE IF NOT EXISTS schema_version (
    schema_version_id   SERIAL PRIMARY KEY,
    pipeline_id         INTEGER NOT NULL REFERENCES pipeline(pipeline_id),
    version             INTEGER NOT NULL,
    schema_definition   JSONB NOT NULL,
    change_type         VARCHAR(50) NOT NULL,
    change_description  TEXT,
    is_current          BOOLEAN DEFAULT TRUE,
    effective_from      TIMESTAMP WITH TIME ZONE NOT NULL,
    effective_to        TIMESTAMP WITH TIME ZONE,
    created_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_change_type CHECK (change_type IN ('initial', 'add_column', 'remove_column', 'modify_column', 'full_replace')),
    CONSTRAINT uq_pipeline_version UNIQUE (pipeline_id, version)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_schema_pipeline_id ON schema_version(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_schema_is_current ON schema_version(is_current);
CREATE INDEX IF NOT EXISTS idx_schema_effective ON schema_version(effective_from, effective_to);

-- Partial index for current schema
CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_current_unique
    ON schema_version(pipeline_id)
    WHERE is_current = TRUE;

-- Comments
COMMENT ON TABLE schema_version IS 'Schema version history with SCD Type 2 tracking';
COMMENT ON COLUMN schema_version.version IS 'Incrementing version number';
COMMENT ON COLUMN schema_version.schema_definition IS 'Full schema definition as JSON';
COMMENT ON COLUMN schema_version.change_type IS 'Type of schema change';
COMMENT ON COLUMN schema_version.is_current IS 'Flag for current active version';
