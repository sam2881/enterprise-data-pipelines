-- =============================================================================
-- INSERT PIPELINE METADATA
-- Generated for: ebcdic_customer_pipeline
-- Generated at: 2026-01-26
-- Jira Ticket: DATA-5678
-- =============================================================================
-- EBCDIC/Cobrix Pipeline: Customer Master from Mainframe
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. PIPELINE REGISTRY (Master record)
-- -----------------------------------------------------------------------------
INSERT INTO pipeline_registry (
    dag_id,
    pipeline_name,
    domain,
    subdomain,
    product_code,
    description,
    owner_team,
    owner_email,
    jira_ticket,
    jira_epic,
    environment,
    status,
    version
) VALUES (
    'ebcdic_customer_pipeline',
    'Customer Master from Mainframe',
    'customer',
    'mainframe-migration',
    'cust_master_ebcdic',
    'Ingests EBCDIC customer records from mainframe using Cobrix, applies cleaning transforms, and loads to BigQuery',
    'mainframe-migration-team',
    'data-team@example.com',
    'DATA-5678',
    'DATA-5000',
    'dev',
    'active',
    '1.0.0'
)
ON CONFLICT (dag_id, environment) DO UPDATE SET
    pipeline_name = EXCLUDED.pipeline_name,
    domain = EXCLUDED.domain,
    subdomain = EXCLUDED.subdomain,
    product_code = EXCLUDED.product_code,
    description = EXCLUDED.description,
    owner_team = EXCLUDED.owner_team,
    owner_email = EXCLUDED.owner_email,
    jira_ticket = EXCLUDED.jira_ticket,
    updated_at = NOW()
RETURNING pipeline_id;

-- -----------------------------------------------------------------------------
-- 2. SOURCE CONFIGURATION (EBCDIC with Copybook)
-- -----------------------------------------------------------------------------
INSERT INTO pipeline_sources (
    pipeline_id,
    source_type,
    source_format,
    source_bucket,
    source_prefix,
    file_pattern,
    copybook_path,
    code_page,
    record_format,
    record_length,
    created_at
)
SELECT
    pipeline_id,
    'file_ebcdic',
    'ebcdic_copybook',
    'gs://landing-mainframe',
    'customer/',
    'CUST*.DAT',
    'gs://copybooks/customer_record.cpy',
    'cp037',
    'fixed',
    180,
    NOW()
FROM pipeline_registry
WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev'
ON CONFLICT (pipeline_id) DO UPDATE SET
    source_type = EXCLUDED.source_type,
    source_format = EXCLUDED.source_format,
    updated_at = NOW();

-- -----------------------------------------------------------------------------
-- 3. SCHEMA DEFINITION (Version 1)
-- -----------------------------------------------------------------------------
UPDATE pipeline_schemas
SET is_current = FALSE, updated_at = NOW()
WHERE pipeline_id = (SELECT pipeline_id FROM pipeline_registry WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev')
  AND is_current = TRUE;

INSERT INTO pipeline_schemas (
    pipeline_id,
    schema_version,
    is_current,
    columns,
    primary_keys,
    partition_columns,
    cluster_columns
)
SELECT
    pipeline_id,
    1,
    TRUE,
    '[
        {"name": "cust_id", "type": "STRING", "nullable": false, "description": "Customer ID"},
        {"name": "cust_name", "type": "STRING", "nullable": false, "description": "Customer Name"},
        {"name": "cust_address", "type": "STRING", "nullable": true, "description": "Customer Address"},
        {"name": "cust_balance", "type": "DECIMAL(11,2)", "nullable": true, "description": "Account Balance"},
        {"name": "cust_status", "type": "STRING", "nullable": false, "description": "Status Code"},
        {"name": "cust_create_date", "type": "DATE", "nullable": false, "description": "Create Date"}
    ]'::jsonb,
    '["cust_id"]'::jsonb,
    '["_ingested_date"]'::jsonb,
    '["cust_status"]'::jsonb
FROM pipeline_registry
WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev';

-- -----------------------------------------------------------------------------
-- 4. TARGET CONFIGURATION
-- -----------------------------------------------------------------------------
INSERT INTO pipeline_targets (
    pipeline_id,
    target_zone,
    bq_project,
    bq_dataset,
    bq_table,
    bq_location,
    write_mode,
    merge_keys,
    partition_field,
    cluster_fields,
    destination_model
)
SELECT
    pipeline_id,
    'gold',
    'enterprise-data-lake',
    'customer_gold',
    'customer_master',
    'US',
    'merge',
    '["cust_id"]'::jsonb,
    '_ingested_date',
    '["cust_status"]'::jsonb,
    'flat'
FROM pipeline_registry
WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev'
ON CONFLICT (pipeline_id) DO UPDATE SET
    target_zone = EXCLUDED.target_zone,
    bq_dataset = EXCLUDED.bq_dataset,
    bq_table = EXCLUDED.bq_table,
    write_mode = EXCLUDED.write_mode,
    updated_at = NOW();

-- -----------------------------------------------------------------------------
-- 5. QUALITY RULES
-- -----------------------------------------------------------------------------
INSERT INTO pipeline_quality_rules (
    pipeline_id,
    rule_name,
    rule_type,
    column_name,
    config,
    severity,
    threshold_pct,
    is_active
)
SELECT
    pipeline_id,
    'cust_id_not_null',
    'not_null',
    'cust_id',
    '{}'::jsonb,
    'critical',
    100.0,
    TRUE
FROM pipeline_registry
WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev';

INSERT INTO pipeline_quality_rules (
    pipeline_id,
    rule_name,
    rule_type,
    column_name,
    config,
    severity,
    threshold_pct,
    is_active
)
SELECT
    pipeline_id,
    'cust_id_unique',
    'unique',
    'cust_id',
    '{}'::jsonb,
    'critical',
    100.0,
    TRUE
FROM pipeline_registry
WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev';

-- -----------------------------------------------------------------------------
-- 6. EXECUTION POLICY
-- -----------------------------------------------------------------------------
INSERT INTO pipeline_execution_policies (
    pipeline_id,
    schedule_interval,
    start_date,
    end_date,
    catchup,
    processing_mode,
    max_active_runs,
    retry_count,
    retry_delay_minutes,
    sla_seconds,
    requires_human_approval,
    approval_groups,
    alert_emails
)
SELECT
    pipeline_id,
    '@daily',
    CURRENT_DATE,
    NULL,
    false,
    'batch',
    1,
    2,
    5,
    3600,
    false,
    '[]'::jsonb,
    '["data-team@example.com"]'::jsonb
FROM pipeline_registry
WHERE dag_id = 'ebcdic_customer_pipeline' AND environment = 'dev'
ON CONFLICT (pipeline_id) DO UPDATE SET
    schedule_interval = EXCLUDED.schedule_interval,
    processing_mode = EXCLUDED.processing_mode,
    retry_count = EXCLUDED.retry_count,
    updated_at = NOW();

-- -----------------------------------------------------------------------------
-- 7. LOG PIPELINE EVENT
-- -----------------------------------------------------------------------------
INSERT INTO pipeline_events (
    dag_id,
    event_type,
    event_data,
    jira_ticket,
    environment
)
VALUES (
    'ebcdic_customer_pipeline',
    'pipeline_created',
    '{
        "action": "insert",
        "source_type": "file_ebcdic",
        "target_zone": "gold",
        "schema_version": 1,
        "transformation_count": 0,
        "quality_rule_count": 2,
        "created_by": "data-agent"
    }'::jsonb,
    'DATA-5678',
    'dev'
);

COMMIT;

-- =============================================================================
-- END OF INSERT METADATA
-- =============================================================================
