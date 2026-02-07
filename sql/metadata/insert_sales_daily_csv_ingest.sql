-- =============================================================================
-- INSERT PIPELINE METADATA
-- Generated for: sales_daily_csv_ingest
-- Feed Group: fg_sales_daily
-- =============================================================================

BEGIN;

-- 1. PIPELINE REGISTRY
INSERT INTO pipeline_registry (
    dag_id, pipeline_name, domain, subdomain, product_code,
    description, owner_team, owner_email, jira_ticket,
    environment, status, version
) VALUES (
    'sales_daily_csv_ingest',
    'Daily Sales CSV Ingest',
    'sales',
    NULL,
    'SALES-CSV',
    'Daily sales CSV file ingest pipeline',
    'apex-data-agent',
    'data-team@company.com',
    NULL,
    'dev',
    'active',
    '2.0.0'
)
ON CONFLICT (dag_id, environment) DO UPDATE SET
    pipeline_name = EXCLUDED.pipeline_name,
    domain = EXCLUDED.domain,
    description = EXCLUDED.description,
    version = EXCLUDED.version,
    updated_at = NOW()
RETURNING pipeline_id;

-- 2. SOURCE CONFIGURATION
INSERT INTO pipeline_sources (
    pipeline_id, source_type, source_format,
    source_bucket, source_prefix, file_pattern,
    delimiter, has_header, encoding, created_at
)
SELECT
    pipeline_id,
    'gcs_file',
    'csv',
    'data-lake-raw',
    'inbound/sales/',
    '*.csv',
    ',',
    true,
    'UTF-8',
    NOW()
FROM pipeline_registry
WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev'
ON CONFLICT (pipeline_id) DO UPDATE SET
    source_type = EXCLUDED.source_type,
    source_format = EXCLUDED.source_format,
    updated_at = NOW();

-- 3. SCHEMA DEFINITION
UPDATE pipeline_schemas
SET is_current = FALSE, updated_at = NOW()
WHERE pipeline_id = (
    SELECT pipeline_id FROM pipeline_registry
    WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev'
) AND is_current = TRUE;

INSERT INTO pipeline_schemas (
    pipeline_id, schema_version, is_current, columns,
    primary_keys, partition_columns, cluster_columns
)
SELECT
    pipeline_id,
    1,
    TRUE,
    '[
        {"name": "order_id", "data_type": "STRING", "nullable": false},
        {"name": "customer_id", "data_type": "STRING", "nullable": false},
        {"name": "order_date", "data_type": "DATE", "nullable": false},
        {"name": "product_name", "data_type": "STRING", "nullable": true},
        {"name": "quantity", "data_type": "INTEGER", "nullable": true},
        {"name": "unit_price", "data_type": "DECIMAL", "nullable": true, "precision": 10, "scale": 2},
        {"name": "total_amount", "data_type": "DECIMAL", "nullable": true, "precision": 12, "scale": 2},
        {"name": "region", "data_type": "STRING", "nullable": true}
    ]'::jsonb,
    '["order_id"]'::jsonb,
    '["order_date"]'::jsonb,
    '["customer_id", "region"]'::jsonb
FROM pipeline_registry
WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev';

-- 4. TARGET CONFIGURATION
INSERT INTO pipeline_targets (
    pipeline_id, target_zone, bq_dataset, bq_table,
    write_mode, merge_keys, partition_field, cluster_fields,
    destination_model
)
SELECT
    pipeline_id,
    'gold',
    'sales_data',
    'daily_sales',
    'merge',
    '["order_id"]'::jsonb,
    'order_date',
    '["customer_id", "region"]'::jsonb,
    'flat'
FROM pipeline_registry
WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev'
ON CONFLICT (pipeline_id) DO UPDATE SET
    target_zone = EXCLUDED.target_zone,
    bq_dataset = EXCLUDED.bq_dataset,
    bq_table = EXCLUDED.bq_table,
    write_mode = EXCLUDED.write_mode,
    updated_at = NOW();

-- 5. QUALITY RULES
INSERT INTO pipeline_quality_rules (
    pipeline_id, rule_name, rule_type, column_name,
    config, severity, threshold_pct, is_active
)
SELECT pipeline_id, 'order_id_not_null', 'not_null', 'order_id',
    '{}'::jsonb, 'ERROR', 100.0, TRUE
FROM pipeline_registry
WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev';

INSERT INTO pipeline_quality_rules (
    pipeline_id, rule_name, rule_type, column_name,
    config, severity, threshold_pct, is_active
)
SELECT pipeline_id, 'order_id_unique', 'unique', 'order_id',
    '{}'::jsonb, 'ERROR', 100.0, TRUE
FROM pipeline_registry
WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev';

INSERT INTO pipeline_quality_rules (
    pipeline_id, rule_name, rule_type, column_name,
    config, severity, threshold_pct, is_active
)
SELECT pipeline_id, 'total_amount_range', 'range', 'total_amount',
    '{"min": 0, "max": 1000000}'::jsonb, 'ERROR', 100.0, TRUE
FROM pipeline_registry
WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev';

-- 6. EXECUTION POLICY
INSERT INTO pipeline_execution_policies (
    pipeline_id, schedule_interval, start_date, catchup,
    processing_mode, max_active_runs, retry_count, retry_delay_minutes,
    alert_emails
)
SELECT
    pipeline_id,
    '@daily',
    '2024-01-01',
    false,
    'batch',
    1,
    3,
    5,
    '["data-team@company.com"]'::jsonb
FROM pipeline_registry
WHERE dag_id = 'sales_daily_csv_ingest' AND environment = 'dev'
ON CONFLICT (pipeline_id) DO UPDATE SET
    schedule_interval = EXCLUDED.schedule_interval,
    retry_count = EXCLUDED.retry_count,
    updated_at = NOW();

-- 7. LOG PIPELINE EVENT
INSERT INTO pipeline_events (
    dag_id, event_type, event_data, environment
) VALUES (
    'sales_daily_csv_ingest',
    'pipeline_created',
    '{
        "action": "insert",
        "source_type": "gcs_file",
        "target_zone": "gold",
        "schema_version": 1,
        "quality_rule_count": 3,
        "created_by": "apex-data-agent",
        "feed_group_id": "fg_sales_daily"
    }'::jsonb,
    'dev'
);

COMMIT;

-- =============================================================================
-- END OF INSERT METADATA
-- =============================================================================
