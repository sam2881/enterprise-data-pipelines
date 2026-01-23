-- Enterprise Agentic Data Platform
-- DDL: Execution Records Table
-- Version: 1.0.0

-- Drop existing table if recreating
-- DROP TABLE IF EXISTS execution CASCADE;

CREATE TABLE IF NOT EXISTS execution (
    execution_id        SERIAL PRIMARY KEY,
    pipeline_id         INTEGER NOT NULL REFERENCES pipeline(pipeline_id),
    request_id          VARCHAR(255) NOT NULL,
    execution_status    VARCHAR(50) NOT NULL,
    environment         VARCHAR(20) NOT NULL,
    triggered_by        VARCHAR(255) NOT NULL,
    artifacts_generated JSONB,
    branch_name         VARCHAR(255),
    commit_sha          VARCHAR(64),
    pr_url              VARCHAR(500),
    cicd_build_id       VARCHAR(255),
    started_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at        TIMESTAMP WITH TIME ZONE,
    error_message       TEXT,
    execution_metadata  JSONB,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_exec_status CHECK (execution_status IN ('pending', 'planning', 'generating', 'validating', 'awaiting_approval', 'deploying', 'success', 'failed', 'cancelled')),
    CONSTRAINT chk_exec_environment CHECK (environment IN ('dev', 'qa', 'prod'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_exec_pipeline_id ON execution(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_exec_request_id ON execution(request_id);
CREATE INDEX IF NOT EXISTS idx_exec_status ON execution(execution_status);
CREATE INDEX IF NOT EXISTS idx_exec_started_at ON execution(started_at);
CREATE INDEX IF NOT EXISTS idx_exec_environment ON execution(environment);

-- Comments
COMMENT ON TABLE execution IS 'Pipeline generation execution records';
COMMENT ON COLUMN execution.request_id IS 'Unique request identifier for tracing';
COMMENT ON COLUMN execution.execution_status IS 'Current status of execution';
COMMENT ON COLUMN execution.artifacts_generated IS 'List of generated artifact paths';
