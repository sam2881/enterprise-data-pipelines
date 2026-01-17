-- =============================================================================
-- INSERT METADATA SQL - FULL SCHEMA DEFINITION
-- =============================================================================
-- Pipeline ID: hr_employee_data_pipeline_data-2001
-- Jira Ticket: DATA-2001
-- Generated: 2026-01-17T14:42:09.164050
-- Version: 1
--
-- THIS IS THE SINGLE SOURCE OF TRUTH FOR THE PIPELINE
-- Schema is stored here for:
--   1. Runtime processing (Bronze -> Silver -> Gold)
--   2. Schema evolution tracking
--   3. Data validation rules
--   4. Transformation logic
--
-- FUTURE CHANGES: Use update_metadata.sql for schema evolution
-- =============================================================================

SET search_path TO pipeline_metadata, public;

BEGIN;

-- =============================================================================
-- 1. PIPELINE REGISTRY
-- =============================================================================

INSERT INTO pipeline_registry (
    pipeline_id,
    pipeline_name,
    pipeline_description,
    jira_ticket_id,
    source_type,
    source_system,
    source_location,
    source_format,
    metadata_location,
    file_encoding,
    processing_mode,
    load_strategy,
    cdc_enabled,
    cdc_strategy,
    scd_type,
    modeling_strategy,
    target_project,
    target_dataset,
    target_table,
    schedule_cron,
    business_domain,
    owner_team,
    owner_email,
    sla_minutes,
    schema_version,
    is_active,
    created_at,
    created_by
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'HR Employee Data Pipeline',
    'Auto-generated pipeline from Jira DATA-2001',
    'DATA-2001',
    'excel',
    'Human Resources',
    'gs://hr-data-bucket/input/data/',
    'xlsx',
    'gs://hr-data-bucket/input/metadata/',
    'utf-8',
    'batch',
    'full',
    false,
    NULL,
    1,
    'medallion',
    'enterprise-data',
    'hr_analytics',
    'dim_employee',
    '0 6 1 * *',
    'Human Resources',
    'HR Analytics Team',
    'hr-analytics@company.com',
    60,
    1,
    true,
    NOW(),
    'data_agent_v2'
);

-- =============================================================================
-- 2. SCHEMA DEFINITIONS (FULL SCHEMA FROM GCS METADATA)
-- =============================================================================
-- This is the COMPLETE schema definition
-- Bronze layer: ALL columns as STRING
-- Silver layer: Typed columns as defined below


INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'employee_id',
    1,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    false,
    true,
    true,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Unique employee identifier',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'first_name',
    2,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    false,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Employee first name',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'last_name',
    3,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    false,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Employee last name',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'email',
    4,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    false,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Corporate email address',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'department',
    5,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    true,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Department name',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'job_title',
    6,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    true,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Job title',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'hire_date',
    7,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'DATE',
    false,
    false,
    false,
    false,
    NULL,
    'YYYY-MM-DD',
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Date of hire',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'salary',
    8,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'DECIMAL(12,2)',
    true,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Annual salary',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'manager_id',
    9,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    true,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Manager employee ID',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'location',
    10,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    true,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Office location',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id,
    column_name,
    column_order,
    source_type,
    bronze_type,
    silver_type,
    is_nullable,
    is_primary_key,
    is_business_key,
    is_partition_key,
    default_value,
    format_pattern,
    position_start,
    position_length,
    is_pii,
    pii_category,
    masking_strategy,
    description,
    schema_version,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'status',
    11,
    'STRING',
    'STRING',  -- Bronze: Always STRING
    'STRING',
    false,
    false,
    false,
    false,
    NULL,
    NULL,
    NULL,
    NULL,
    false,
    NULL,
    NULL,
    'Employment status',
    1,
    true,
    NOW()
);

INSERT INTO schema_definitions (
    pipeline_id, column_name, column_order, source_type, bronze_type, silver_type,
    is_nullable, is_primary_key, is_business_key, is_partition_key, description,
    schema_version, is_active, created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001', 'reporting_date', 12, 'DATE', 'STRING', 'DATE',
    false, false, false, true, 'Business reporting date',
    1, true, NOW()
);

INSERT INTO schema_definitions (
    pipeline_id, column_name, column_order, source_type, bronze_type, silver_type,
    is_nullable, is_primary_key, is_business_key, is_partition_key, description,
    schema_version, is_active, created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001', 'run_id', 13, 'STRING', 'STRING', 'STRING',
    false, false, false, false, 'Pipeline run identifier',
    1, true, NOW()
);

INSERT INTO schema_definitions (
    pipeline_id, column_name, column_order, source_type, bronze_type, silver_type,
    is_nullable, is_primary_key, is_business_key, is_partition_key, description,
    schema_version, is_active, created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001', 'record_uuid', 14, 'STRING', 'STRING', 'STRING',
    false, false, false, false, 'Unique record identifier',
    1, true, NOW()
);

INSERT INTO schema_definitions (
    pipeline_id, column_name, column_order, source_type, bronze_type, silver_type,
    is_nullable, is_primary_key, is_business_key, is_partition_key, description,
    schema_version, is_active, created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001', 'ingestion_ts', 15, 'TIMESTAMP', 'STRING', 'TIMESTAMP',
    false, false, false, false, 'Ingestion timestamp',
    1, true, NOW()
);

INSERT INTO schema_definitions (
    pipeline_id, column_name, column_order, source_type, bronze_type, silver_type,
    is_nullable, is_primary_key, is_business_key, is_partition_key, description,
    schema_version, is_active, created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001', 'source_system', 16, 'STRING', 'STRING', 'STRING',
    false, false, false, false, 'Source system identifier',
    1, true, NOW()
);

INSERT INTO schema_definitions (
    pipeline_id, column_name, column_order, source_type, bronze_type, silver_type,
    is_nullable, is_primary_key, is_business_key, is_partition_key, description,
    schema_version, is_active, created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001', 'source_file', 17, 'STRING', 'STRING', 'STRING',
    true, false, false, false, 'Source file path',
    1, true, NOW()
);

-- =============================================================================
-- 3. VALIDATION RULES
-- =============================================================================

INSERT INTO validation_rules (
    pipeline_id,
    rule_name,
    column_name,
    rule_type,
    rule_expression,
    parameters,
    severity,
    action_on_failure,
    error_message,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'employee_id_not_null',
    'employee_id',
    'not_null',
    NULL,
    '{}'::jsonb,
    'error',
    'reject',
    NULL,
    true,
    NOW()
);

INSERT INTO validation_rules (
    pipeline_id,
    rule_name,
    column_name,
    rule_type,
    rule_expression,
    parameters,
    severity,
    action_on_failure,
    error_message,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'employee_id_unique',
    'employee_id',
    'unique',
    NULL,
    '{}'::jsonb,
    'error',
    'reject',
    NULL,
    true,
    NOW()
);

INSERT INTO validation_rules (
    pipeline_id,
    rule_name,
    column_name,
    rule_type,
    rule_expression,
    parameters,
    severity,
    action_on_failure,
    error_message,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'email_format',
    'email',
    'regex',
    '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    '{}'::jsonb,
    'error',
    'reject',
    NULL,
    true,
    NOW()
);

INSERT INTO validation_rules (
    pipeline_id,
    rule_name,
    column_name,
    rule_type,
    rule_expression,
    parameters,
    severity,
    action_on_failure,
    error_message,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'salary_positive',
    'salary',
    'range',
    NULL,
    '{"min": 0, "max": 10000000}'::jsonb,
    'warning',
    'warn',
    NULL,
    true,
    NOW()
);

INSERT INTO validation_rules (
    pipeline_id,
    rule_name,
    column_name,
    rule_type,
    rule_expression,
    parameters,
    severity,
    action_on_failure,
    error_message,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'status_values',
    'status',
    'in_list',
    NULL,
    '{"values": ["Active", "Inactive"]}'::jsonb,
    'error',
    'reject',
    NULL,
    true,
    NOW()
);

-- =============================================================================
-- 4. TRANSFORMATION RULES
-- =============================================================================

INSERT INTO transformation_rules (
    pipeline_id,
    rule_name,
    rule_type,
    rule_order,
    apply_in_layer,
    source_columns,
    target_column,
    expression,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'full_name',
    'derive',
    1,
    'silver',
    '["first_name", "last_name"]'::jsonb,
    'full_name',
    'CONCAT(first_name, '' '', last_name)',
    true,
    NOW()
);

-- =============================================================================
-- 5. BIGQUERY CONFIGURATION
-- =============================================================================

INSERT INTO bigquery_configuration (
    pipeline_id,
    project_id,
    dataset_id,
    table_name,
    load_strategy,
    write_disposition,
    partition_field,
    partition_type,
    clustering_columns,
    is_active,
    created_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    'enterprise-data',
    'hr_analytics',
    'dim_employee',
    'full',
    'WRITE_TRUNCATE',
    'reporting_date',
    'DAY',
    NULL,
    true,
    NOW()
);

-- =============================================================================
-- COMMIT TRANSACTION
-- =============================================================================

COMMIT;

-- Verify insertion
SELECT 'Pipeline registered successfully' as status, pipeline_id, pipeline_name
FROM pipeline_registry WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001';

SELECT 'Schema columns' as info, COUNT(*) as column_count
FROM schema_definitions WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001';
