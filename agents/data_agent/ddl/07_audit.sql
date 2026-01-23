-- Enterprise Agentic Data Platform
-- DDL: Audit Log Table
-- Version: 1.0.0

-- Drop existing table if recreating
-- DROP TABLE IF EXISTS audit_log CASCADE;

CREATE TABLE IF NOT EXISTS audit_log (
    log_id              VARCHAR(255) PRIMARY KEY,
    agent_name          VARCHAR(100) NOT NULL,
    action              VARCHAR(100) NOT NULL,
    pipeline_id         INTEGER REFERENCES pipeline(pipeline_id),
    request_id          VARCHAR(255),
    input_state         JSONB,
    output_state        JSONB,
    decision_reasoning  TEXT,
    duration_ms         INTEGER,
    status              VARCHAR(50) NOT NULL,
    error_details       JSONB,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_audit_status CHECK (status IN ('started', 'success', 'failed', 'skipped'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_audit_agent_name ON audit_log(agent_name);
CREATE INDEX IF NOT EXISTS idx_audit_pipeline_id ON audit_log(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_audit_request_id ON audit_log(request_id);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_audit_status ON audit_log(status);

-- Partitioning by time (optional, for high-volume deployments)
-- Consider partitioning by created_at for retention management

-- Comments
COMMENT ON TABLE audit_log IS 'Audit trail for all agent actions';
COMMENT ON COLUMN audit_log.log_id IS 'Unique log entry identifier (UUID)';
COMMENT ON COLUMN audit_log.agent_name IS 'Name of agent that performed action';
COMMENT ON COLUMN audit_log.decision_reasoning IS 'Explanation of why action was taken';
COMMENT ON COLUMN audit_log.duration_ms IS 'Time taken to complete action in milliseconds';

-- Approval tracking table
CREATE TABLE IF NOT EXISTS approval_request (
    approval_id         SERIAL PRIMARY KEY,
    execution_id        INTEGER NOT NULL REFERENCES execution(execution_id),
    approval_type       VARCHAR(50) NOT NULL,
    requested_by        VARCHAR(255) NOT NULL,
    requested_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    approver            VARCHAR(255),
    approved_at         TIMESTAMP WITH TIME ZONE,
    status              VARCHAR(50) NOT NULL DEFAULT 'pending',
    comments            TEXT,
    expires_at          TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT chk_approval_type CHECK (approval_type IN ('prod_deployment', 'schema_change', 'breaking_change', 'manual_override')),
    CONSTRAINT chk_approval_status CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'cancelled'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_approval_execution_id ON approval_request(execution_id);
CREATE INDEX IF NOT EXISTS idx_approval_status ON approval_request(status);
CREATE INDEX IF NOT EXISTS idx_approval_expires_at ON approval_request(expires_at);

-- Comments
COMMENT ON TABLE approval_request IS 'Human approval requests for sensitive operations';
COMMENT ON COLUMN approval_request.approval_type IS 'Type of approval required';
COMMENT ON COLUMN approval_request.expires_at IS 'Deadline for approval decision';
