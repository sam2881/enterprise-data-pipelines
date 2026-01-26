-- =============================================================================
-- UPDATE PIPELINE METADATA
-- Generated for: ebcdic_customer_pipeline
-- Generated at: 2026-01-26
-- Update Type: execution_policy_update
-- =============================================================================
-- Template for updating EBCDIC pipeline execution policy
-- =============================================================================

BEGIN;

-- =============================================================================
-- EXECUTION POLICY UPDATE
-- =============================================================================

UPDATE pipeline_execution_policies
SET
    schedule_interval = '@daily',
    processing_mode = 'batch',
    retry_count = 3,
    retry_delay_minutes = 10,
    sla_seconds = 7200,
    alert_emails = '["data-team@example.com", "mainframe-team@example.com"]'::jsonb,
    updated_at = NOW()
WHERE pipeline_id = (SELECT pipeline_id FROM pipeline_registry WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev');

-- =============================================================================
-- LOG UPDATE EVENT
-- =============================================================================

INSERT INTO pipeline_events (
    dag_id,
    event_type,
    event_data,
    jira_ticket,
    environment
)
VALUES (
    'ebcdic_customer_pipeline',
    'pipeline_updated',
    '{
        "update_type": "execution_policy_update",
        "updated_by": "data-agent",
        "changes": {
            "retry_count": "2 -> 3",
            "retry_delay_minutes": "5 -> 10",
            "sla_seconds": "3600 -> 7200"
        }
    }'::jsonb,
    'DATA-5678',
    'dev'
);

-- Update pipeline registry timestamp
UPDATE pipeline_registry
SET updated_at = NOW()
WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev';

COMMIT;

-- =============================================================================
-- END OF UPDATE METADATA
-- =============================================================================
