"""Named SQL queries for metadata operations."""

# Placeholder - implement in Phase 3
# See docs/CLAUDE_CODE_CONTEXT.md for full implementation pattern

# Pipeline queries
GET_PIPELINE_BY_NAME = """
SELECT * FROM pipeline
WHERE pipeline_name = :name
AND environment = :env
AND is_active = TRUE
"""

GET_PIPELINE_BY_ID = """
SELECT * FROM pipeline
WHERE pipeline_id = :pid
"""

INSERT_PIPELINE = """
INSERT INTO pipeline (
    pipeline_name, domain, source_type, source_system,
    environment, data_sensitivity, business_owner,
    technical_owner, is_active, created_by
) VALUES (
    :pipeline_name, :domain, :source_type, :source_system,
    :environment, :data_sensitivity, :business_owner,
    :technical_owner, TRUE, :created_by
)
RETURNING pipeline_id
"""

# Schema version queries
GET_CURRENT_SCHEMA = """
SELECT * FROM schema_version
WHERE pipeline_id = :pid
AND is_current = TRUE
"""

GET_SCHEMA_HISTORY = """
SELECT * FROM schema_version
WHERE pipeline_id = :pid
ORDER BY version DESC
"""

CLOSE_CURRENT_SCHEMA = """
UPDATE schema_version
SET is_current = FALSE, effective_to = NOW()
WHERE pipeline_id = :pid AND is_current = TRUE
"""

INSERT_SCHEMA_VERSION = """
INSERT INTO schema_version (
    pipeline_id, version, schema_definition, change_type,
    change_description, is_current, effective_from, created_by
) VALUES (
    :pipeline_id, :version, :schema_definition, :change_type,
    :change_description, TRUE, NOW(), :created_by
)
RETURNING schema_version_id
"""

# Execution queries
INSERT_EXECUTION = """
INSERT INTO execution (
    pipeline_id, request_id, execution_status, environment,
    triggered_by, artifacts_generated, branch_name, commit_sha,
    pr_url, cicd_build_id, started_at
) VALUES (
    :pipeline_id, :request_id, :execution_status, :environment,
    :triggered_by, :artifacts_generated, :branch_name, :commit_sha,
    :pr_url, :cicd_build_id, NOW()
)
RETURNING execution_id
"""

UPDATE_EXECUTION_STATUS = """
UPDATE execution
SET
    execution_status = :status,
    completed_at = CASE WHEN :status IN ('success', 'failed') THEN NOW() ELSE NULL END,
    error_message = :error_message
WHERE execution_id = :execution_id
"""

# Audit log queries
INSERT_AUDIT_LOG = """
INSERT INTO audit_log (
    log_id, agent_name, action, pipeline_id,
    input_state, output_state, decision_reasoning,
    duration_ms, status
) VALUES (
    :log_id, :agent_name, :action, :pipeline_id,
    :input_state, :output_state, :decision_reasoning,
    :duration_ms, :status
)
"""
