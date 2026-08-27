-- =============================================================================
-- Enterprise Data Platform - PostgreSQL Metadata Schema
-- Purpose: Store DAG configurations, track executions, manage pipelines
-- DAGs infer configuration from this schema at runtime (metadata-driven)
-- =============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- CORE TABLES
-- =============================================================================

-- Pipeline Registry: Master record for each pipeline
CREATE TABLE IF NOT EXISTS platform_pipeline_registry (
    pipeline_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dag_id VARCHAR(255) NOT NULL UNIQUE,
    pipeline_name VARCHAR(255) NOT NULL,
    domain VARCHAR(100) NOT NULL,
    subdomain VARCHAR(100),
    product_code VARCHAR(100) NOT NULL,
    description TEXT,

    -- Ownership
    owner_team VARCHAR(100) NOT NULL,
    owner_email VARCHAR(255) NOT NULL,

    -- Jira Integration
    jira_ticket VARCHAR(50),
    jira_epic VARCHAR(50),

    -- Environment & Status
    environment VARCHAR(20) NOT NULL DEFAULT 'dev', -- dev, staging, prod
    status VARCHAR(20) NOT NULL DEFAULT 'active', -- active, paused, deprecated
    version VARCHAR(20) NOT NULL DEFAULT '1.0.0',

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deployed_at TIMESTAMP WITH TIME ZONE,

    -- Indexes
    CONSTRAINT unique_dag_env UNIQUE (dag_id, environment)
);

CREATE INDEX idx_pipeline_domain ON platform_pipeline_registry(domain);
CREATE INDEX idx_pipeline_jira ON platform_pipeline_registry(jira_ticket);
CREATE INDEX idx_pipeline_status ON platform_pipeline_registry(status);

-- =============================================================================
-- SOURCE CONFIGURATION
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_sources (
    source_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Source Type
    source_type VARCHAR(50) NOT NULL, -- file_csv, file_json, database_postgres, streaming_kafka, api_rest, dtsx
    source_format VARCHAR(50), -- csv, json, xml, parquet, avro, ebcdic, excel

    -- File Sources
    source_bucket VARCHAR(255),
    source_prefix VARCHAR(500),
    file_pattern VARCHAR(255) DEFAULT '*',
    delimiter VARCHAR(10) DEFAULT ',',
    has_header BOOLEAN DEFAULT TRUE,
    encoding VARCHAR(50) DEFAULT 'utf-8',

    -- Database Sources
    connection_id VARCHAR(100),
    source_schema VARCHAR(100),
    source_table VARCHAR(255),
    source_query TEXT,
    extraction_mode VARCHAR(20) DEFAULT 'full', -- full, incremental, cdc
    watermark_column VARCHAR(100),

    -- Streaming Sources
    kafka_bootstrap_servers VARCHAR(500),
    kafka_topic VARCHAR(255),
    kafka_consumer_group VARCHAR(255),
    pubsub_subscription VARCHAR(255),

    -- API Sources
    api_endpoint VARCHAR(500),
    api_method VARCHAR(10) DEFAULT 'GET',
    api_headers JSONB DEFAULT '{}',
    api_pagination_type VARCHAR(20), -- offset, cursor, page_number

    -- DTSX Migration
    dtsx_location VARCHAR(500),
    dtsx_package_name VARCHAR(255),
    original_server VARCHAR(255),

    -- Fixed Width / EBCDIC
    field_specs JSONB DEFAULT '[]',
    copybook JSONB DEFAULT '{}',
    record_length INTEGER,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_source_pipeline ON platform_pipeline_sources(pipeline_id);
CREATE INDEX idx_source_type ON platform_pipeline_sources(source_type);

-- =============================================================================
-- SCHEMA DEFINITION
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_schemas (
    schema_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,
    platform_schema_version VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    is_current BOOLEAN DEFAULT TRUE,

    -- Columns stored as JSONB array
    -- Example: [{"name": "id", "type": "integer", "nullable": false, "pii": false, "pk": true}]
    columns JSONB NOT NULL DEFAULT '[]',

    -- Primary Keys
    primary_keys JSONB DEFAULT '["id"]',

    -- Partitioning
    partition_columns JSONB DEFAULT '[]',
    cluster_columns JSONB DEFAULT '[]',

    -- Schema metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(100)
);

CREATE INDEX idx_schema_pipeline ON platform_pipeline_schemas(pipeline_id);
CREATE INDEX idx_schema_current ON platform_pipeline_schemas(is_current) WHERE is_current = TRUE;

-- =============================================================================
-- TARGET CONFIGURATION
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_targets (
    target_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Target Zone
    target_zone VARCHAR(20) NOT NULL DEFAULT 'gold', -- bronze, silver, gold

    -- BigQuery Target
    bq_project VARCHAR(255),
    bq_dataset VARCHAR(255) NOT NULL,
    bq_table VARCHAR(255) NOT NULL,
    bq_location VARCHAR(20) DEFAULT 'US',

    -- Write Mode
    write_mode VARCHAR(20) NOT NULL DEFAULT 'append', -- append, overwrite, merge, scd_type_2
    merge_keys JSONB DEFAULT '[]',

    -- Data Model Type
    destination_model VARCHAR(20) DEFAULT 'flat', -- flat, dimensional, data_vault, obt

    -- Data Vault specific
    hub_table VARCHAR(255),
    sat_table VARCHAR(255),
    link_tables JSONB DEFAULT '[]',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_target_pipeline ON platform_pipeline_targets(pipeline_id);

-- =============================================================================
-- TRANSFORMATIONS
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_transformations (
    transform_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Transform Type
    transform_type VARCHAR(50) NOT NULL, -- rename, cast, derive, filter, aggregate, join, window, scd, nl_transform
    transform_order INTEGER NOT NULL DEFAULT 0,
    zone VARCHAR(20) NOT NULL DEFAULT 'silver', -- bronze, silver, gold

    -- Transform Configuration (type-specific)
    config JSONB NOT NULL DEFAULT '{}',
    /*
    Examples:
    - rename: {"source": "old_name", "target": "new_name"}
    - cast: {"column": "amount", "cast_type": "float"}
    - derive: {"column": "full_name", "expression": "first_name || ' ' || last_name"}
    - filter: {"condition": "status = 'active'"}
    - aggregate: {"group_by": ["region"], "aggregations": [{"column": "sales", "function": "sum", "alias": "total_sales"}]}
    - join: {"left_table": "customers", "right_table": "orders", "join_type": "left", "join_keys": [{"left": "id", "right": "customer_id"}]}
    - window: {"partition_by": ["customer_id"], "order_by": [{"column": "date", "direction": "desc"}], "functions": [{"type": "row_number", "alias": "rank"}]}
    - scd: {"type": 2, "tracked_columns": ["name", "email"], "effective_from": "valid_from", "effective_to": "valid_to"}
    - nl_transform: {"description": "Calculate running total of sales by customer ordered by date", "generated_code": "..."}
    */

    -- Natural Language Transform (LLM-generated)
    nl_description TEXT,
    generated_pyspark TEXT,
    generated_sql TEXT,

    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_transform_pipeline ON platform_pipeline_transformations(pipeline_id);
CREATE INDEX idx_transform_zone ON platform_pipeline_transformations(zone);

-- =============================================================================
-- DATA QUALITY RULES
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_quality_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Rule Definition
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL, -- not_null, unique, range, regex, referential, freshness, custom
    column_name VARCHAR(100),

    -- Rule Configuration
    config JSONB NOT NULL DEFAULT '{}',
    /*
    Examples:
    - not_null: {"column": "id"}
    - unique: {"column": "email"}
    - range: {"column": "age", "min": 0, "max": 150}
    - regex: {"column": "email", "pattern": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"}
    - referential: {"column": "customer_id", "reference_table": "dim_customers", "reference_column": "id"}
    - freshness: {"column": "updated_at", "max_age_hours": 24}
    - custom: {"expression": "amount > 0 AND amount < 1000000"}
    */

    severity VARCHAR(20) NOT NULL DEFAULT 'warning', -- warning, error, critical
    threshold_pct DECIMAL(5,2) DEFAULT 100.0, -- % of records that must pass

    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_quality_pipeline ON platform_pipeline_quality_rules(pipeline_id);

-- =============================================================================
-- EXECUTION POLICY
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_execution_policies (
    policy_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Schedule
    schedule_interval VARCHAR(100), -- cron expression or @daily, @hourly, etc.
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    end_date DATE,
    catchup BOOLEAN DEFAULT FALSE,

    -- Processing Mode
    processing_mode VARCHAR(20) DEFAULT 'batch', -- batch, micro_batch, streaming
    max_active_runs INTEGER DEFAULT 1,

    -- Retry Policy
    retry_count INTEGER DEFAULT 2,
    retry_delay_minutes INTEGER DEFAULT 5,
    timeout_hours INTEGER DEFAULT 2,

    -- SLA
    sla_hours DECIMAL(5,2),

    -- Approval
    requires_human_approval BOOLEAN DEFAULT FALSE,
    approval_groups JSONB DEFAULT '[]', -- ["data-stewards", "domain-owners"]

    -- Alerting
    alert_emails JSONB DEFAULT '[]',
    slack_webhook VARCHAR(500),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_policy_pipeline UNIQUE (pipeline_id)
);

-- =============================================================================
-- EXECUTION TRACKING
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_executions (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id),
    dag_run_id VARCHAR(255) NOT NULL,

    -- Execution Details
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,

    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'running', -- running, success, failed, skipped

    -- Metrics
    records_read BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,

    -- Quality
    quality_score DECIMAL(5,2),
    quality_passed BOOLEAN,

    -- Error Details
    error_message TEXT,
    error_task VARCHAR(255),

    -- Metadata
    triggered_by VARCHAR(100), -- scheduler, manual, backfill
    airflow_log_url VARCHAR(500),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_execution_pipeline ON platform_pipeline_executions(pipeline_id);
CREATE INDEX idx_execution_date ON platform_pipeline_executions(execution_date);
CREATE INDEX idx_execution_status ON platform_pipeline_executions(status);

-- =============================================================================
-- WATERMARKS (for incremental loads)
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_watermarks (
    watermark_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dag_id VARCHAR(255) NOT NULL,
    environment VARCHAR(20) NOT NULL DEFAULT 'dev',

    last_watermark VARCHAR(255) NOT NULL,
    watermark_type VARCHAR(20) DEFAULT 'timestamp', -- timestamp, integer, string

    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_watermark UNIQUE (dag_id, environment)
);

-- =============================================================================
-- PIPELINE EVENTS (audit log)
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dag_id VARCHAR(255) NOT NULL,
    execution_date VARCHAR(50),

    event_type VARCHAR(50) NOT NULL, -- STARTED, SUCCESS, FAILED, APPROVAL_REQUESTED, APPROVED, REJECTED
    event_data JSONB DEFAULT '{}',

    jira_ticket VARCHAR(50),
    environment VARCHAR(20),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_event_dag ON platform_pipeline_events(dag_id);
CREATE INDEX idx_event_type ON platform_pipeline_events(event_type);
CREATE INDEX idx_event_date ON platform_pipeline_events(created_at);

-- =============================================================================
-- VIEWS
-- =============================================================================

-- Active pipelines with latest execution
CREATE OR REPLACE VIEW v_pipeline_status AS
SELECT
    pr.dag_id,
    pr.pipeline_name,
    pr.domain,
    pr.product_code,
    pr.environment,
    pr.status,
    pr.jira_ticket,
    pr.owner_team,
    pe.execution_date as last_execution_date,
    pe.status as last_execution_status,
    pe.records_written as last_records_written,
    pe.quality_score as last_quality_score,
    pep.schedule_interval
FROM platform_pipeline_registry pr
LEFT JOIN LATERAL (
    SELECT * FROM platform_pipeline_executions
    WHERE pipeline_id = pr.pipeline_id
    ORDER BY execution_date DESC
    LIMIT 1
) pe ON TRUE
LEFT JOIN platform_pipeline_execution_policies pep ON pep.pipeline_id = pr.pipeline_id
WHERE pr.status = 'active';

-- Pipeline configuration summary
CREATE OR REPLACE VIEW v_pipeline_config AS
SELECT
    pr.dag_id,
    pr.pipeline_name,
    pr.domain,
    pr.environment,
    ps.source_type,
    ps.source_format,
    pt.bq_dataset,
    pt.bq_table,
    pt.write_mode,
    pt.destination_model,
    pep.schedule_interval,
    pep.requires_human_approval,
    (SELECT COUNT(*) FROM platform_pipeline_transformations WHERE pipeline_id = pr.pipeline_id AND is_active = TRUE) as transform_count,
    (SELECT COUNT(*) FROM platform_pipeline_quality_rules WHERE pipeline_id = pr.pipeline_id AND is_active = TRUE) as quality_rule_count
FROM platform_pipeline_registry pr
LEFT JOIN platform_pipeline_sources ps ON ps.pipeline_id = pr.pipeline_id
LEFT JOIN platform_pipeline_targets pt ON pt.pipeline_id = pr.pipeline_id
LEFT JOIN platform_pipeline_execution_policies pep ON pep.pipeline_id = pr.pipeline_id;

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Get pipeline configuration as JSON (for DAG runtime)
CREATE OR REPLACE FUNCTION get_pipeline_config(p_dag_id VARCHAR, p_environment VARCHAR DEFAULT 'dev')
RETURNS JSONB AS $$
DECLARE
    result JSONB;
    v_pipeline_id UUID;
BEGIN
    -- Get pipeline ID
    SELECT pipeline_id INTO v_pipeline_id
    FROM platform_pipeline_registry
    WHERE dag_id = p_dag_id AND environment = p_environment;

    IF v_pipeline_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- Build configuration JSON
    SELECT jsonb_build_object(
        'pipeline', (SELECT row_to_json(pr.*) FROM platform_pipeline_registry pr WHERE pipeline_id = v_pipeline_id),
        'source', (SELECT row_to_json(ps.*) FROM platform_pipeline_sources ps WHERE pipeline_id = v_pipeline_id LIMIT 1),
        'schema', (SELECT row_to_json(psc.*) FROM platform_pipeline_schemas psc WHERE pipeline_id = v_pipeline_id AND is_current = TRUE LIMIT 1),
        'target', (SELECT row_to_json(pt.*) FROM platform_pipeline_targets pt WHERE pipeline_id = v_pipeline_id LIMIT 1),
        'transformations', (SELECT jsonb_agg(row_to_json(ptr.*) ORDER BY transform_order) FROM platform_pipeline_transformations ptr WHERE pipeline_id = v_pipeline_id AND is_active = TRUE),
        'quality_rules', (SELECT jsonb_agg(row_to_json(pqr.*)) FROM platform_pipeline_quality_rules pqr WHERE pipeline_id = v_pipeline_id AND is_active = TRUE),
        'execution_policy', (SELECT row_to_json(pep.*) FROM platform_pipeline_execution_policies pep WHERE pipeline_id = v_pipeline_id)
    ) INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Register new pipeline
CREATE OR REPLACE FUNCTION register_pipeline(
    p_dag_id VARCHAR,
    p_pipeline_name VARCHAR,
    p_domain VARCHAR,
    p_product_code VARCHAR,
    p_owner_team VARCHAR,
    p_owner_email VARCHAR,
    p_jira_ticket VARCHAR DEFAULT NULL,
    p_environment VARCHAR DEFAULT 'dev',
    p_description TEXT DEFAULT NULL
)
RETURNS UUID AS $$
DECLARE
    v_pipeline_id UUID;
BEGIN
    INSERT INTO platform_pipeline_registry (
        dag_id, pipeline_name, domain, product_code,
        owner_team, owner_email, jira_ticket, environment, description
    ) VALUES (
        p_dag_id, p_pipeline_name, p_domain, p_product_code,
        p_owner_team, p_owner_email, p_jira_ticket, p_environment, p_description
    )
    ON CONFLICT (dag_id, environment)
    DO UPDATE SET
        pipeline_name = EXCLUDED.pipeline_name,
        domain = EXCLUDED.domain,
        product_code = EXCLUDED.product_code,
        owner_team = EXCLUDED.owner_team,
        owner_email = EXCLUDED.owner_email,
        jira_ticket = EXCLUDED.jira_ticket,
        description = EXCLUDED.description,
        updated_at = NOW()
    RETURNING pipeline_id INTO v_pipeline_id;

    RETURN v_pipeline_id;
END;
$$ LANGUAGE plpgsql;

-- Log pipeline execution
CREATE OR REPLACE FUNCTION log_pipeline_execution(
    p_dag_id VARCHAR,
    p_dag_run_id VARCHAR,
    p_execution_date TIMESTAMP WITH TIME ZONE,
    p_status VARCHAR,
    p_records_read BIGINT DEFAULT 0,
    p_records_written BIGINT DEFAULT 0,
    p_quality_score DECIMAL DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS UUID AS $$
DECLARE
    v_execution_id UUID;
    v_pipeline_id UUID;
BEGIN
    -- Get pipeline ID
    SELECT pipeline_id INTO v_pipeline_id
    FROM platform_pipeline_registry
    WHERE dag_id = p_dag_id
    LIMIT 1;

    INSERT INTO platform_pipeline_executions (
        pipeline_id, dag_run_id, execution_date, start_time,
        status, records_read, records_written, quality_score, error_message
    ) VALUES (
        v_pipeline_id, p_dag_run_id, p_execution_date, NOW(),
        p_status, p_records_read, p_records_written, p_quality_score, p_error_message
    )
    RETURNING execution_id INTO v_execution_id;

    RETURN v_execution_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SAMPLE DATA INSERT (for testing)
-- =============================================================================

-- Example: Insert a sample pipeline
/*
SELECT register_pipeline(
    'sales_customer_ingest',
    'Customer Data Ingestion',
    'sales',
    'customers',
    'data-engineering',
    'data-eng@company.com',
    'DATA-1234',
    'dev',
    'Ingest customer data from CRM to BigQuery'
);

-- Add source configuration
INSERT INTO platform_pipeline_sources (pipeline_id, source_type, source_format, source_bucket, source_prefix, file_pattern)
SELECT pipeline_id, 'file_csv', 'csv', 'company-raw-data', 'sales/customers', '*.csv'
FROM platform_pipeline_registry WHERE dag_id = 'sales_customer_ingest';

-- Add schema
INSERT INTO platform_pipeline_schemas (pipeline_id, columns, primary_keys)
SELECT pipeline_id,
    '[{"name": "customer_id", "type": "string", "nullable": false, "pk": true},
      {"name": "name", "type": "string", "nullable": false},
      {"name": "email", "type": "string", "nullable": true},
      {"name": "created_at", "type": "timestamp", "nullable": false}]'::jsonb,
    '["customer_id"]'::jsonb
FROM platform_pipeline_registry WHERE dag_id = 'sales_customer_ingest';

-- Add target
INSERT INTO platform_pipeline_targets (pipeline_id, bq_dataset, bq_table, write_mode)
SELECT pipeline_id, 'sales', 'customers', 'merge'
FROM platform_pipeline_registry WHERE dag_id = 'sales_customer_ingest';

-- Add quality rules
INSERT INTO platform_pipeline_quality_rules (pipeline_id, rule_name, rule_type, column_name, severity, config)
SELECT pipeline_id, 'customer_id_not_null', 'not_null', 'customer_id', 'critical', '{}'::jsonb
FROM platform_pipeline_registry WHERE dag_id = 'sales_customer_ingest';

-- Add execution policy
INSERT INTO platform_pipeline_execution_policies (pipeline_id, schedule_interval, retry_count, requires_human_approval)
SELECT pipeline_id, '@daily', 3, FALSE
FROM platform_pipeline_registry WHERE dag_id = 'sales_customer_ingest';
*/
