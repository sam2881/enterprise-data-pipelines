-- =============================================================================
-- ENTERPRISE METADATA TABLES FOR DATA PIPELINE AGENT
-- =============================================================================
--
-- WHY: All pipeline configuration is stored in PostgreSQL metadata tables.
--      DAGs read from these tables at runtime - NO hard-coded values.
--      This enables:
--      1. Zero-code pipeline changes (update metadata, not code)
--      2. Full audit trail of all configurations
--      3. Incremental loading via watermarks
--      4. Dynamic schema evolution
--
-- USAGE: Run this script to initialize the metadata database
--        psql -h localhost -U admin -d agentdb -f enterprise_metadata_tables.sql
--
-- =============================================================================

-- Schema for all pipeline metadata
CREATE SCHEMA IF NOT EXISTS pipeline_metadata;
SET search_path TO pipeline_metadata, public;

-- =============================================================================
-- 1. FEED REGISTRY - Master table for all data feeds
-- =============================================================================
-- WHY: Central registry of all data feeds. DAG reads this to understand
--      source/target systems, schedule, and processing configuration.

CREATE TABLE IF NOT EXISTS feed_registry (
    feed_id VARCHAR(100) PRIMARY KEY,
    feed_name VARCHAR(255) NOT NULL,
    feed_description TEXT,

    -- Source Configuration
    source_type VARCHAR(50) NOT NULL,  -- excel, oracle, kafka, postgresql, mainframe, multi_file, cdc
    source_system VARCHAR(255) NOT NULL,
    source_owner_email VARCHAR(255),
    source_connection_id VARCHAR(100),  -- FK to connection_registry

    -- Target Configuration
    target_catalog VARCHAR(100) DEFAULT 'data_catalog',
    target_database VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_format VARCHAR(50) DEFAULT 'iceberg',

    -- Processing Configuration
    processing_mode VARCHAR(50) DEFAULT 'batch',  -- batch, streaming, micro_batch
    load_strategy VARCHAR(50) DEFAULT 'full',     -- full, incremental, cdc, merge
    watermark_column VARCHAR(100),                -- For incremental loads
    lookback_hours INTEGER DEFAULT 0,             -- Late-arriving data window
    batch_size INTEGER DEFAULT 100000,
    parallel_degree INTEGER DEFAULT 1,

    -- Scheduling
    schedule_type VARCHAR(50) DEFAULT 'cron',     -- cron, trigger, event, manual
    schedule_cron VARCHAR(100),
    schedule_timezone VARCHAR(50) DEFAULT 'UTC',
    schedule_start_date DATE,
    schedule_end_date DATE,

    -- Data Modeling
    modeling_strategy VARCHAR(50) DEFAULT 'medallion',  -- medallion, data_vault, star, snowflake, flat
    scd_type INTEGER DEFAULT 1,                   -- 1, 2, 3
    merge_keys TEXT[],                            -- Array of merge key columns
    partition_columns TEXT[],                     -- Array of partition columns
    sort_columns TEXT[],                          -- Array of sort columns

    -- Governance
    business_domain VARCHAR(100),
    data_classification VARCHAR(50) DEFAULT 'internal',  -- public, internal, confidential, restricted
    retention_days INTEGER DEFAULT 365,
    regulatory_requirements TEXT[],               -- SOX, GDPR, PCI-DSS, etc.
    approval_required BOOLEAN DEFAULT FALSE,

    -- Ownership
    owner_team VARCHAR(100),
    owner_email VARCHAR(255),
    steward_email VARCHAR(255),
    cost_center VARCHAR(50),

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_paused BOOLEAN DEFAULT FALSE,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(100),

    -- Constraints
    CONSTRAINT chk_source_type CHECK (source_type IN ('excel', 'oracle', 'postgresql', 'sqlserver', 'kafka', 'mainframe', 'multi_file', 'cdc', 'api', 'gcs', 's3')),
    CONSTRAINT chk_processing_mode CHECK (processing_mode IN ('batch', 'streaming', 'micro_batch')),
    CONSTRAINT chk_load_strategy CHECK (load_strategy IN ('full', 'incremental', 'cdc', 'merge', 'append')),
    CONSTRAINT chk_modeling_strategy CHECK (modeling_strategy IN ('medallion', 'data_vault', 'star', 'snowflake', 'flat'))
);

-- Index for common lookups
CREATE INDEX IF NOT EXISTS idx_feed_registry_active ON feed_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_feed_registry_domain ON feed_registry(business_domain);
CREATE INDEX IF NOT EXISTS idx_feed_registry_source ON feed_registry(source_type);

-- =============================================================================
-- 2. FEED COLUMNS - Column-level metadata for each feed
-- =============================================================================
-- WHY: Defines schema for each feed. Used by DAG to:
--      1. Generate bronze schema (all STRING)
--      2. Cast types for silver layer
--      3. Apply transformations and defaults

CREATE TABLE IF NOT EXISTS feed_columns (
    column_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id) ON DELETE CASCADE,

    -- Column Definition
    column_name VARCHAR(255) NOT NULL,
    column_order INTEGER NOT NULL,
    source_type VARCHAR(50) NOT NULL,        -- Original type in source
    target_type VARCHAR(100) NOT NULL,       -- Target type (STRING, INTEGER, DECIMAL(18,2), DATE, etc.)

    -- Column Properties
    is_nullable BOOLEAN DEFAULT TRUE,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_business_key BOOLEAN DEFAULT FALSE,
    is_partition_key BOOLEAN DEFAULT FALSE,
    is_sort_key BOOLEAN DEFAULT FALSE,

    -- Default & Transform
    default_value TEXT,
    transform_expression TEXT,               -- SQL expression for Silver layer
    format_pattern VARCHAR(100),             -- Date/number format

    -- Derivation (computed columns)
    is_derived BOOLEAN DEFAULT FALSE,
    derivation_formula TEXT,                 -- e.g., "quantity * unit_price"
    derivation_dependencies TEXT[],          -- Columns this depends on

    -- PII & Security
    is_pii BOOLEAN DEFAULT FALSE,
    pii_category VARCHAR(50),                -- name, contact, address, government_id, financial
    pii_sensitivity VARCHAR(20),             -- high, medium, low
    masking_strategy VARCHAR(50),            -- tokenize, hash, mask, redact, none

    -- Documentation
    description TEXT,
    example_values TEXT[],
    allowed_values TEXT[],                   -- Enum values

    -- Foreign Keys
    fk_feed_id VARCHAR(100),                 -- References another feed
    fk_column_name VARCHAR(255),             -- References column in that feed

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (feed_id, column_name)
);

CREATE INDEX IF NOT EXISTS idx_feed_columns_feed ON feed_columns(feed_id);
CREATE INDEX IF NOT EXISTS idx_feed_columns_pii ON feed_columns(is_pii) WHERE is_pii = TRUE;

-- =============================================================================
-- 3. FEED VALIDATION RULES - Data quality rules per feed
-- =============================================================================
-- WHY: Defines validation rules applied at Bronze/Silver/Gold layers.
--      Rules drive data quality checks and determine reject/warn actions.

CREATE TABLE IF NOT EXISTS feed_validation_rules (
    rule_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id) ON DELETE CASCADE,

    -- Rule Definition
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,          -- not_null, unique, range, pattern, in_set, freshness, custom, referential, rollup
    column_name VARCHAR(255),                -- NULL for table-level rules

    -- Rule Parameters
    parameters JSONB DEFAULT '{}',           -- Rule-specific parameters
    -- Examples:
    -- range: {"min": 0, "max": 10000}
    -- pattern: {"regex": "^[A-Z]{2}-[0-9]+$"}
    -- in_set: {"values": ["ACTIVE", "INACTIVE", "PENDING"]}
    -- referential: {"ref_feed": "departments", "ref_column": "dept_id"}
    -- rollup: {"source_column": "amount", "target_column": "total", "tolerance": 0.01}

    -- Severity & Action
    severity VARCHAR(20) DEFAULT 'error',    -- warning, error, critical
    action_on_failure VARCHAR(20) DEFAULT 'reject',  -- reject, warn, skip, quarantine

    -- Scope
    apply_in_layer VARCHAR(20) DEFAULT 'silver',  -- bronze, silver, gold

    -- Error Handling
    error_code VARCHAR(50),
    error_message_template TEXT,             -- "Column {column} failed rule {rule}: {value}"

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feed_validation_feed ON feed_validation_rules(feed_id);
CREATE INDEX IF NOT EXISTS idx_feed_validation_layer ON feed_validation_rules(apply_in_layer);

-- =============================================================================
-- 4. FEED SLA - Service Level Agreements per feed
-- =============================================================================
-- WHY: Defines SLAs for freshness, completeness, latency.
--      DAG uses these to validate processing and alert on breaches.

CREATE TABLE IF NOT EXISTS feed_sla (
    sla_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id) ON DELETE CASCADE,

    -- SLA Metrics
    freshness_hours INTEGER DEFAULT 24,            -- Max age of data
    latency_threshold_minutes INTEGER DEFAULT 60,  -- Max processing time
    completeness_threshold DECIMAL(5,4) DEFAULT 0.99,  -- Min % rows expected
    accuracy_threshold DECIMAL(5,4) DEFAULT 0.999,     -- Min % accuracy

    -- Alerting
    alert_on_breach BOOLEAN DEFAULT TRUE,
    alert_channels TEXT[] DEFAULT '{"email"}',     -- email, slack, pagerduty
    alert_emails TEXT[],

    -- Business Hours
    business_hours_only BOOLEAN DEFAULT FALSE,
    business_start_hour INTEGER DEFAULT 8,
    business_end_hour INTEGER DEFAULT 18,
    business_timezone VARCHAR(50) DEFAULT 'UTC',

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (feed_id)
);

-- =============================================================================
-- 5. FEED STATE - Runtime state for incremental processing
-- =============================================================================
-- WHY: Tracks watermarks, last successful runs, and incremental state.
--      DAG queries this to determine what data to process.

CREATE TABLE IF NOT EXISTS feed_state (
    state_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id) ON DELETE CASCADE,
    posting_date DATE NOT NULL,

    -- Watermark for Incremental
    last_watermark TEXT,                     -- Last processed value (timestamp, ID, etc.)
    watermark_type VARCHAR(20),              -- timestamp, numeric, string

    -- Processing State
    status VARCHAR(50) DEFAULT 'pending',    -- pending, running, success, failed, partial
    last_successful_run TIMESTAMP,
    last_failed_run TIMESTAMP,

    -- Row Counts
    source_row_count BIGINT DEFAULT 0,
    bronze_row_count BIGINT DEFAULT 0,
    silver_row_count BIGINT DEFAULT 0,
    gold_row_count BIGINT DEFAULT 0,
    rejected_row_count BIGINT DEFAULT 0,

    -- Error Info
    error_message TEXT,
    error_details JSONB,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (feed_id, posting_date)
);

CREATE INDEX IF NOT EXISTS idx_feed_state_feed ON feed_state(feed_id);
CREATE INDEX IF NOT EXISTS idx_feed_state_status ON feed_state(status);
CREATE INDEX IF NOT EXISTS idx_feed_state_date ON feed_state(posting_date DESC);

-- =============================================================================
-- 6. RUN AUDIT LOG - Comprehensive audit trail for all runs
-- =============================================================================
-- WHY: Every DAG run creates audit records. Enables:
--      1. Full lineage tracking
--      2. Debugging failed runs
--      3. Performance monitoring
--      4. Compliance reporting

CREATE TABLE IF NOT EXISTS run_audit_log (
    audit_id SERIAL PRIMARY KEY,

    -- Run Identification
    group_run_id VARCHAR(100) NOT NULL,      -- Group all files in one batch
    run_id VARCHAR(100) NOT NULL,            -- Individual file/task run ID
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255),

    -- Execution Context
    posting_date DATE NOT NULL,
    execution_date TIMESTAMP NOT NULL,

    -- Source Details
    source_file_name VARCHAR(500),
    source_file_path TEXT,
    source_file_size BIGINT,
    source_file_hash VARCHAR(64),            -- SHA-256 hash
    source_row_count BIGINT,

    -- Processing Metrics
    layer VARCHAR(20),                       -- source, transient, bronze, silver, gold, rejected
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DECIMAL(12,3),

    -- Row Counts
    input_row_count BIGINT DEFAULT 0,
    output_row_count BIGINT DEFAULT 0,
    rejected_row_count BIGINT DEFAULT 0,

    -- Status
    status VARCHAR(50) NOT NULL,             -- started, success, failed, skipped, partial
    error_code VARCHAR(50),
    error_message TEXT,
    error_stack_trace TEXT,

    -- Quality Metrics
    validation_passed BOOLEAN,
    validation_pass_rate DECIMAL(5,4),
    quality_score DECIMAL(5,4),

    -- Schema Info
    schema_version VARCHAR(50),
    schema_hash VARCHAR(64),

    -- Lineage
    parent_run_id VARCHAR(100),              -- For dependency tracking

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_run_audit_group ON run_audit_log(group_run_id);
CREATE INDEX IF NOT EXISTS idx_run_audit_feed ON run_audit_log(feed_id);
CREATE INDEX IF NOT EXISTS idx_run_audit_date ON run_audit_log(posting_date DESC);
CREATE INDEX IF NOT EXISTS idx_run_audit_status ON run_audit_log(status);

-- =============================================================================
-- 7. FILE TRACKING - Track all processed files for duplicate detection
-- =============================================================================
-- WHY: Prevents reprocessing of already-processed files.
--      DAG checks this before processing any file.

CREATE TABLE IF NOT EXISTS file_tracking (
    file_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),

    -- File Identity
    file_name VARCHAR(500) NOT NULL,
    file_path TEXT NOT NULL,
    file_hash VARCHAR(64),                   -- SHA-256 for duplicate detection
    file_size BIGINT,

    -- Processing Info
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_processed_at TIMESTAMP,
    processing_count INTEGER DEFAULT 0,

    -- Status
    status VARCHAR(50) DEFAULT 'pending',    -- pending, processing, processed, duplicate, rejected

    -- Associated Run
    group_run_id VARCHAR(100),
    run_id VARCHAR(100),

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (feed_id, file_hash)
);

CREATE INDEX IF NOT EXISTS idx_file_tracking_feed ON file_tracking(feed_id);
CREATE INDEX IF NOT EXISTS idx_file_tracking_status ON file_tracking(status);
CREATE INDEX IF NOT EXISTS idx_file_tracking_hash ON file_tracking(file_hash);

-- =============================================================================
-- 8. CONNECTION REGISTRY - External system connections
-- =============================================================================
-- WHY: Centralized connection management. Secrets stored separately (Vault/Secret Manager).

CREATE TABLE IF NOT EXISTS connection_registry (
    connection_id VARCHAR(100) PRIMARY KEY,
    connection_name VARCHAR(255) NOT NULL,
    connection_type VARCHAR(50) NOT NULL,    -- oracle, postgresql, kafka, gcs, s3, sftp, api

    -- Connection Details (non-sensitive)
    host VARCHAR(500),
    port INTEGER,
    database_name VARCHAR(255),
    schema_name VARCHAR(255),
    extra_config JSONB DEFAULT '{}',

    -- Secret Reference (actual secrets in Vault)
    secret_path VARCHAR(500),                -- Path in secret manager

    -- Status
    is_active BOOLEAN DEFAULT TRUE,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 9. DATA VAULT METADATA - For Data Vault 2.0 modeling
-- =============================================================================
-- WHY: Stores hub/link/satellite definitions for DV2.0 pipelines.

CREATE TABLE IF NOT EXISTS data_vault_entities (
    entity_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id) ON DELETE CASCADE,

    -- Entity Definition
    entity_type VARCHAR(20) NOT NULL,        -- hub, link, satellite, pit, bridge
    entity_name VARCHAR(255) NOT NULL,

    -- Hub/Link Specific
    business_keys TEXT[],                    -- For hubs
    hub_references TEXT[],                   -- For links (array of hub names)

    -- Satellite Specific
    parent_entity VARCHAR(255),              -- Hub or Link this satellite attaches to
    satellite_type VARCHAR(50),              -- descriptive, effectivity, status

    -- Hash Configuration
    hash_algorithm VARCHAR(20) DEFAULT 'MD5',
    hash_columns TEXT[],

    -- Target Table
    target_table VARCHAR(255) NOT NULL,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_dv_entity_type CHECK (entity_type IN ('hub', 'link', 'satellite', 'pit', 'bridge'))
);

-- =============================================================================
-- 10. SCHEMA EVOLUTION HISTORY - Track schema changes over time
-- =============================================================================
-- WHY: For CDC and evolving schemas, track version history.

CREATE TABLE IF NOT EXISTS schema_evolution (
    evolution_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),

    -- Version Info
    schema_version VARCHAR(50) NOT NULL,
    effective_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Change Details
    change_type VARCHAR(50) NOT NULL,        -- column_added, column_removed, column_renamed, type_changed
    column_name VARCHAR(255),
    old_definition JSONB,
    new_definition JSONB,

    -- Compatibility
    is_breaking_change BOOLEAN DEFAULT FALSE,
    migration_sql TEXT,

    -- Approval
    approved_by VARCHAR(100),
    approved_at TIMESTAMP,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_schema_evolution_feed ON schema_evolution(feed_id);
CREATE INDEX IF NOT EXISTS idx_schema_evolution_version ON schema_evolution(schema_version);

-- =============================================================================
-- 11. FEED DEPENDENCIES - Multi-file and cross-feed dependencies
-- =============================================================================
-- WHY: Defines dependency graph for multi-file batches.

CREATE TABLE IF NOT EXISTS feed_dependencies (
    dependency_id SERIAL PRIMARY KEY,

    -- Parent Feed (depends on child)
    parent_feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),

    -- Child Feed (must complete first)
    child_feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),

    -- Dependency Type
    dependency_type VARCHAR(50) DEFAULT 'hard',  -- hard, soft, optional

    -- Validation
    validation_type VARCHAR(50),             -- row_count, referential, rollup
    validation_config JSONB,

    -- Timing
    wait_timeout_minutes INTEGER DEFAULT 60,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (parent_feed_id, child_feed_id)
);

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Function to get next incremental watermark
CREATE OR REPLACE FUNCTION get_next_watermark(
    p_feed_id VARCHAR(100),
    p_posting_date DATE
) RETURNS TEXT AS $$
DECLARE
    v_watermark TEXT;
BEGIN
    SELECT last_watermark INTO v_watermark
    FROM feed_state
    WHERE feed_id = p_feed_id
    AND status = 'success'
    ORDER BY posting_date DESC
    LIMIT 1;

    RETURN COALESCE(v_watermark, '1970-01-01T00:00:00Z');
END;
$$ LANGUAGE plpgsql;

-- Function to generate unique run IDs
CREATE OR REPLACE FUNCTION generate_run_id(
    p_feed_id VARCHAR(100),
    p_posting_date DATE
) RETURNS VARCHAR(100) AS $$
BEGIN
    RETURN p_feed_id || '_' || TO_CHAR(p_posting_date, 'YYYYMMDD') || '_' ||
           TO_CHAR(CURRENT_TIMESTAMP, 'HH24MISS') || '_' ||
           SUBSTRING(MD5(RANDOM()::TEXT), 1, 8);
END;
$$ LANGUAGE plpgsql;

-- Function to generate group run ID
CREATE OR REPLACE FUNCTION generate_group_run_id(
    p_feed_id VARCHAR(100),
    p_posting_date DATE
) RETURNS VARCHAR(100) AS $$
BEGIN
    RETURN 'GRP_' || p_feed_id || '_' || TO_CHAR(p_posting_date, 'YYYYMMDD') || '_' ||
           TO_CHAR(CURRENT_TIMESTAMP, 'HH24MISS');
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SAMPLE DATA INSERTION (for testing)
-- =============================================================================

-- Insert sample feed: customer_transactions
INSERT INTO feed_registry (
    feed_id, feed_name, feed_description,
    source_type, source_system, source_owner_email,
    target_database, target_table,
    processing_mode, load_strategy,
    schedule_cron, schedule_timezone, schedule_start_date,
    modeling_strategy, scd_type, partition_columns, sort_columns,
    business_domain, data_classification, retention_days,
    owner_team, owner_email, cost_center,
    is_active
) VALUES (
    'customer_transactions',
    'Customer Transactions Pipeline',
    'Monthly sales transactions from Finance department',
    'excel', 'Finance Monthly Reports', 'finance-team@company.com',
    'finance', 'monthly_sales',
    'batch', 'full',
    '0 6 1 * *', 'America/New_York', '2024-02-01',
    'medallion', 1, ARRAY['sale_month'], ARRAY['sale_date'],
    'Finance', 'internal', 2555,
    'Data Platform', 'data.engineer@company.com', 'FIN-001',
    TRUE
) ON CONFLICT (feed_id) DO NOTHING;

-- Insert columns for customer_transactions
INSERT INTO feed_columns (feed_id, column_name, column_order, source_type, target_type, is_nullable, is_business_key, description)
VALUES
    ('customer_transactions', 'transaction_id', 1, 'string', 'STRING', FALSE, TRUE, 'Unique transaction identifier'),
    ('customer_transactions', 'sale_date', 2, 'string', 'DATE', FALSE, FALSE, 'Date of sale'),
    ('customer_transactions', 'customer_name', 3, 'string', 'STRING', TRUE, FALSE, 'Customer name'),
    ('customer_transactions', 'product_code', 4, 'string', 'STRING', FALSE, FALSE, 'Product SKU code'),
    ('customer_transactions', 'quantity', 5, 'number', 'INTEGER', FALSE, FALSE, 'Quantity sold'),
    ('customer_transactions', 'unit_price', 6, 'number', 'DECIMAL(18,2)', FALSE, FALSE, 'Price per unit'),
    ('customer_transactions', 'amount', 7, 'number', 'DECIMAL(18,2)', FALSE, FALSE, 'Total sale amount'),
    ('customer_transactions', 'region', 8, 'string', 'STRING', TRUE, FALSE, 'Sales region')
ON CONFLICT (feed_id, column_name) DO NOTHING;

-- Insert validation rules
INSERT INTO feed_validation_rules (feed_id, rule_name, rule_type, column_name, parameters, severity, action_on_failure, apply_in_layer)
VALUES
    ('customer_transactions', 'transaction_id_not_null', 'not_null', 'transaction_id', '{}', 'critical', 'reject', 'bronze'),
    ('customer_transactions', 'transaction_id_unique', 'unique', 'transaction_id', '{}', 'critical', 'reject', 'silver'),
    ('customer_transactions', 'quantity_range', 'range', 'quantity', '{"min": 1, "max": 10000}', 'warning', 'warn', 'silver'),
    ('customer_transactions', 'amount_positive', 'positive', 'amount', '{}', 'warning', 'warn', 'silver')
ON CONFLICT DO NOTHING;

-- Insert SLA
INSERT INTO feed_sla (feed_id, freshness_hours, latency_threshold_minutes, completeness_threshold, accuracy_threshold, alert_emails)
VALUES ('customer_transactions', 24, 60, 0.99, 0.999, ARRAY['data-alerts@company.com'])
ON CONFLICT (feed_id) DO NOTHING;

-- Grant permissions
GRANT USAGE ON SCHEMA pipeline_metadata TO PUBLIC;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA pipeline_metadata TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pipeline_metadata TO PUBLIC;

-- =============================================================================
-- END OF ENTERPRISE METADATA TABLES
-- =============================================================================
