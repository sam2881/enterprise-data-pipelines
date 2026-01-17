-- =============================================================================
-- UPDATE METADATA SQL - SCHEMA EVOLUTION
-- =============================================================================
-- Pipeline ID: hr_employee_data_pipeline_data-2001
-- Generated: 2026-01-17T14:42:09.164216
--
-- USE THIS FILE FOR:
--   1. Adding new columns
--   2. Changing data types
--   3. Updating validation rules
--   4. Modifying transformation logic
--
-- IMPORTANT: Never delete columns, only mark as inactive
-- IMPORTANT: Increment schema_version for tracking
-- =============================================================================

SET search_path TO pipeline_metadata, public;

BEGIN;

-- =============================================================================
-- 1. INCREMENT SCHEMA VERSION
-- =============================================================================

UPDATE pipeline_registry
SET
    schema_version = schema_version + 1,
    updated_at = NOW(),
    updated_by = 'data_agent_v2'
WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001';

-- =============================================================================
-- 2. ADD NEW COLUMN (Example)
-- =============================================================================
-- Uncomment and modify as needed:

-- INSERT INTO schema_definitions (
--     pipeline_id, column_name, column_order, source_type, bronze_type, silver_type,
--     is_nullable, description, schema_version, is_active, created_at
-- ) VALUES (
--     'hr_employee_data_pipeline_data-2001',
--     'new_column_name',
--     (SELECT COALESCE(MAX(column_order), 0) + 1 FROM schema_definitions WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001'),
--     'STRING',
--     'STRING',
--     'STRING',
--     true,
--     'New column added via schema evolution',
--     (SELECT schema_version FROM pipeline_registry WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001'),
--     true,
--     NOW()
-- );

-- =============================================================================
-- 3. MODIFY COLUMN TYPE (Example)
-- =============================================================================
-- Uncomment and modify as needed:

-- UPDATE schema_definitions
-- SET
--     silver_type = 'NEW_TYPE',
--     schema_version = (SELECT schema_version FROM pipeline_registry WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001'),
--     updated_at = NOW()
-- WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001'
-- AND column_name = 'column_to_modify';

-- =============================================================================
-- 4. DEACTIVATE COLUMN (Never delete, mark inactive)
-- =============================================================================
-- Uncomment and modify as needed:

-- UPDATE schema_definitions
-- SET
--     is_active = false,
--     updated_at = NOW()
-- WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001'
-- AND column_name = 'column_to_remove';

-- =============================================================================
-- 5. ADD/UPDATE VALIDATION RULE (Example)
-- =============================================================================
-- Uncomment and modify as needed:

-- INSERT INTO validation_rules (
--     pipeline_id, rule_name, column_name, rule_type, rule_expression,
--     severity, action_on_failure, is_active, created_at
-- ) VALUES (
--     'hr_employee_data_pipeline_data-2001',
--     'new_validation_rule',
--     'column_name',
--     'regex',
--     '^[A-Z]{3}$',
--     'error',
--     'reject',
--     true,
--     NOW()
-- );

-- =============================================================================
-- 6. LOG SCHEMA EVOLUTION
-- =============================================================================

INSERT INTO schema_evolution_history (
    pipeline_id,
    old_version,
    new_version,
    change_type,
    change_description,
    changed_by,
    changed_at
) VALUES (
    'hr_employee_data_pipeline_data-2001',
    (SELECT schema_version - 1 FROM pipeline_registry WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001'),
    (SELECT schema_version FROM pipeline_registry WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001'),
    'schema_update',
    'Schema evolution applied',
    'data_agent_v2',
    NOW()
);

COMMIT;

-- Verify update
SELECT 'Schema updated' as status, pipeline_id, schema_version
FROM pipeline_registry WHERE pipeline_id = 'hr_employee_data_pipeline_data-2001';
