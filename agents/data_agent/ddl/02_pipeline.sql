-- Enterprise Agentic Data Platform
-- DDL: Pipeline Table
-- Version: 1.0.0

-- Drop existing table if recreating
-- DROP TABLE IF EXISTS pipeline CASCADE;

CREATE TABLE IF NOT EXISTS pipeline (
    pipeline_id         SERIAL PRIMARY KEY,
    pipeline_name       VARCHAR(255) NOT NULL,
    domain              VARCHAR(100) NOT NULL,
    source_type         VARCHAR(50) NOT NULL,
    source_system       VARCHAR(100) NOT NULL,
    environment         VARCHAR(20) NOT NULL,
    data_sensitivity    VARCHAR(50) NOT NULL,
    business_owner      VARCHAR(255) NOT NULL,
    technical_owner     VARCHAR(255) NOT NULL,
    description         TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    created_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT chk_environment CHECK (environment IN ('dev', 'qa', 'prod')),
    CONSTRAINT chk_source_type CHECK (source_type IN ('file', 'database', 'streaming', 'api')),
    CONSTRAINT chk_sensitivity CHECK (data_sensitivity IN ('public', 'internal', 'confidential', 'restricted')),
    CONSTRAINT uq_pipeline_name_env UNIQUE (pipeline_name, environment)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_name_env ON pipeline(pipeline_name, environment);
CREATE INDEX IF NOT EXISTS idx_pipeline_domain ON pipeline(domain);
CREATE INDEX IF NOT EXISTS idx_pipeline_source_type ON pipeline(source_type);
CREATE INDEX IF NOT EXISTS idx_pipeline_is_active ON pipeline(is_active);

-- Comments
COMMENT ON TABLE pipeline IS 'Pipeline metadata registry';
COMMENT ON COLUMN pipeline.pipeline_id IS 'Unique identifier for the pipeline';
COMMENT ON COLUMN pipeline.pipeline_name IS 'Human-readable pipeline name';
COMMENT ON COLUMN pipeline.domain IS 'Business domain (e.g., sales, finance)';
COMMENT ON COLUMN pipeline.source_type IS 'Type of data source';
COMMENT ON COLUMN pipeline.environment IS 'Target environment (dev/qa/prod)';
COMMENT ON COLUMN pipeline.data_sensitivity IS 'Data classification level';
