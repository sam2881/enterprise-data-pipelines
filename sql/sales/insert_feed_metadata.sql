-- ═══════════════════════════════════════════════════════════════════════════════
-- APEX Data Agent - Feed Metadata Insert
-- Generated: 2026-02-05
-- Pipeline: sales_daily_pipeline
-- Feed ID: 1001
-- ═══════════════════════════════════════════════════════════════════════════════

-- Insert Feed Group
INSERT INTO feed_group_details (
    feed_group_id,
    feed_group_name,
    domain,
    source_system,
    owner,
    description,
    is_active,
    created_at,
    updated_at
) VALUES (
    100,
    'sales_daily_group',
    'sales',
    'gcs',
    'apex-data-agent',
    'Daily sales data ingestion from CSV files',
    TRUE,
    NOW(),
    NOW()
) ON CONFLICT (feed_group_id) DO UPDATE SET
    feed_group_name = EXCLUDED.feed_group_name,
    description = EXCLUDED.description,
    updated_at = NOW();

-- Insert Feed Details
INSERT INTO feed_details (
    feed_id,
    feed_group_id,
    feed_name,
    source_type,
    source_config,
    file_format,
    delimiter,
    has_header,
    encoding,
    is_active,
    created_at,
    updated_at
) VALUES (
    1001,
    100,
    'sales_daily',
    'file_csv',
    '{"bucket": "apex-landing", "prefix": "sales/daily/", "file_pattern": "sales_*.csv"}'::jsonb,
    'csv',
    ',',
    TRUE,
    'UTF-8',
    TRUE,
    NOW(),
    NOW()
) ON CONFLICT (feed_id) DO UPDATE SET
    source_config = EXCLUDED.source_config,
    updated_at = NOW();

-- Insert Contract Details
INSERT INTO contract_details (
    contract_id,
    feed_id,
    contract_type,
    pattern_code,
    primary_keys,
    partition_column,
    quality_threshold,
    schema_evolution_mode,
    is_active,
    created_at,
    updated_at
) VALUES (
    2001,
    1001,
    'STANDARD',
    'P01',
    ARRAY['transaction_id'],
    'transaction_date',
    0.95,
    'strict',
    TRUE,
    NOW(),
    NOW()
) ON CONFLICT (contract_id) DO UPDATE SET
    quality_threshold = EXCLUDED.quality_threshold,
    updated_at = NOW();

-- Insert Schema Version
INSERT INTO schema_versions (
    schema_id,
    feed_id,
    version_number,
    schema_definition,
    is_current,
    created_at
) VALUES (
    3001,
    1001,
    1,
    '{
        "columns": [
            {"name": "transaction_id", "type": "STRING", "nullable": false, "description": "Unique transaction identifier"},
            {"name": "customer_id", "type": "STRING", "nullable": false, "description": "Customer identifier"},
            {"name": "product_id", "type": "STRING", "nullable": false, "description": "Product identifier"},
            {"name": "quantity", "type": "INTEGER", "nullable": false, "description": "Quantity purchased"},
            {"name": "unit_price", "type": "NUMERIC", "nullable": false, "description": "Price per unit"},
            {"name": "transaction_date", "type": "DATE", "nullable": false, "description": "Date of transaction"},
            {"name": "store_id", "type": "STRING", "nullable": true, "description": "Store identifier"}
        ]
    }'::jsonb,
    TRUE,
    NOW()
) ON CONFLICT (schema_id) DO UPDATE SET
    schema_definition = EXCLUDED.schema_definition,
    is_current = TRUE;

-- Insert Zone Configurations
INSERT INTO zone_configurations (
    zone_config_id,
    feed_id,
    zone_level,
    dataset_name,
    table_name,
    write_mode,
    partition_config,
    is_active,
    created_at
) VALUES
    (4001, 1001, 'raw', 'raw_sales', 'sales_daily_raw', 'append',
     '{"partition_column": "ingestion_date", "partition_type": "DAY"}'::jsonb, TRUE, NOW()),
    (4002, 1001, 'silver', 'silver_sales', 'sales_daily_silver', 'append',
     '{"partition_column": "transaction_date", "partition_type": "DAY"}'::jsonb, TRUE, NOW()),
    (4003, 1001, 'gold', 'gold_sales', 'sales_daily_gold', 'merge',
     '{"partition_column": "transaction_date", "partition_type": "DAY"}'::jsonb, TRUE, NOW())
ON CONFLICT (zone_config_id) DO UPDATE SET
    dataset_name = EXCLUDED.dataset_name,
    table_name = EXCLUDED.table_name,
    write_mode = EXCLUDED.write_mode;

-- Insert Quality Rules
INSERT INTO quality_rules (
    rule_id,
    feed_id,
    zone_level,
    rule_name,
    rule_type,
    column_name,
    rule_config,
    severity,
    is_active,
    created_at
) VALUES
    (5001, 1001, 'raw', 'transaction_id_not_null', 'not_null', 'transaction_id',
     '{}'::jsonb, 'ERROR', TRUE, NOW()),
    (5002, 1001, 'raw', 'quantity_positive', 'range', 'quantity',
     '{"min": 1}'::jsonb, 'ERROR', TRUE, NOW()),
    (5003, 1001, 'raw', 'unit_price_positive', 'range', 'unit_price',
     '{"min": 0.01}'::jsonb, 'ERROR', TRUE, NOW()),
    (5004, 1001, 'silver', 'customer_id_not_null', 'not_null', 'customer_id',
     '{}'::jsonb, 'ERROR', TRUE, NOW()),
    (5005, 1001, 'silver', 'transaction_id_unique', 'unique', 'transaction_id',
     '{}'::jsonb, 'ERROR', TRUE, NOW())
ON CONFLICT (rule_id) DO UPDATE SET
    rule_config = EXCLUDED.rule_config,
    is_active = EXCLUDED.is_active;

-- Insert DAG Template Reference
INSERT INTO dag_templates (
    template_id,
    template_name,
    pattern_code,
    template_path,
    description,
    is_active,
    created_at
) VALUES (
    6001,
    'P01 File Medallion Pipeline',
    'P01',
    'dags/sales_daily_pipeline.py',
    'Standard file ingestion with medallion architecture (Raw → Refined → Trusted)',
    TRUE,
    NOW()
) ON CONFLICT (template_id) DO UPDATE SET
    template_path = EXCLUDED.template_path,
    is_active = EXCLUDED.is_active;

-- Log the generation event
INSERT INTO platform_audit_log (
    event_type,
    feed_id,
    feed_group_id,
    event_data,
    created_by,
    created_at
) VALUES (
    'PIPELINE_GENERATED',
    1001,
    100,
    '{
        "dag_id": "sales_daily_pipeline",
        "pattern_code": "P01",
        "zones": ["raw", "silver", "gold"],
        "generated_by": "APEX Data Agent v2.0"
    }'::jsonb,
    'apex-data-agent',
    NOW()
);

-- Commit
COMMIT;
