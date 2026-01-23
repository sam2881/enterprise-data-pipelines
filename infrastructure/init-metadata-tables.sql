-- =============================================================================
-- ENTERPRISE DATA PIPELINE METADATA TABLES
-- =============================================================================
-- This creates the metadata schema and all required tables for the
-- Data Agent V2 metadata-driven architecture.
--
-- PostgreSQL is the SINGLE SOURCE OF TRUTH for all pipeline configuration.
-- Spark processors read this metadata at runtime.
-- =============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS pipeline_metadata;
SET search_path TO pipeline_metadata, public;

-- =============================================================================
-- 1. PIPELINE REGISTRY - Master pipeline configuration
-- =============================================================================
CREATE TABLE IF NOT EXISTS pipeline_registry (
    pipeline_id VARCHAR(255) PRIMARY KEY,
    pipeline_name VARCHAR(500) NOT NULL,
    pipeline_description TEXT,
    jira_ticket_id VARCHAR(50),

    -- Source Configuration
    source_type VARCHAR(50) NOT NULL,  -- excel, csv, json, parquet, jdbc, kafka, etc.
    source_system VARCHAR(255),
    source_location TEXT NOT NULL,     -- GCS path or connection string
    source_format VARCHAR(50),
    metadata_location TEXT,            -- GCS path for metadata files
    file_encoding VARCHAR(50) DEFAULT 'utf-8',

    -- Processing Configuration
    processing_mode VARCHAR(50) DEFAULT 'batch',  -- batch, streaming, micro-batch
    load_strategy VARCHAR(50) DEFAULT 'full',     -- full, incremental, cdc
    cdc_enabled BOOLEAN DEFAULT FALSE,
    cdc_strategy VARCHAR(50),                     -- change_date, audit_columns, log_based
    scd_type INTEGER DEFAULT 1,                   -- 1, 2, 3
    modeling_strategy VARCHAR(50) DEFAULT 'medallion',  -- medallion, one-big-table, data-vault

    -- Target Configuration
    target_project VARCHAR(255),
    target_dataset VARCHAR(255),
    target_table VARCHAR(255),

    -- Schedule & SLA
    schedule_cron VARCHAR(100),

    -- Ownership
    business_domain VARCHAR(255),
    owner_team VARCHAR(255),
    owner_email VARCHAR(255),
    sla_minutes INTEGER DEFAULT 60,

    -- Versioning & Status
    schema_version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(255),
    updated_at TIMESTAMP DEFAULT NOW(),
    updated_by VARCHAR(255)
);

-- =============================================================================
-- 2. SCHEMA DEFINITIONS - Column-level metadata
-- =============================================================================
CREATE TABLE IF NOT EXISTS schema_definitions (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) REFERENCES pipeline_registry(pipeline_id),
    column_name VARCHAR(255) NOT NULL,
    column_order INTEGER NOT NULL,

    -- Type Information
    source_type VARCHAR(100),          -- Original type from source
    bronze_type VARCHAR(100) DEFAULT 'STRING',  -- Always STRING in Bronze
    silver_type VARCHAR(100),          -- Target type after casting

    -- Constraints
    is_nullable BOOLEAN DEFAULT TRUE,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_business_key BOOLEAN DEFAULT FALSE,
    is_partition_key BOOLEAN DEFAULT FALSE,
    default_value TEXT,

    -- Parsing (for fixed-width, positional)
    format_pattern VARCHAR(255),       -- Date format, regex pattern
    position_start INTEGER,            -- For fixed-width files
    position_length INTEGER,           -- For fixed-width files

    -- PII/Security
    is_pii BOOLEAN DEFAULT FALSE,
    pii_category VARCHAR(50),          -- name, email, ssn, phone, address
    masking_strategy VARCHAR(50),      -- hash, redact, encrypt, tokenize

    -- Documentation
    description TEXT,

    -- Versioning
    schema_version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(pipeline_id, column_name, schema_version)
);

-- =============================================================================
-- 3. VALIDATION RULES - Data quality rules
-- =============================================================================
CREATE TABLE IF NOT EXISTS validation_rules (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) REFERENCES pipeline_registry(pipeline_id),
    rule_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),          -- NULL for row-level rules

    -- Rule Definition
    rule_type VARCHAR(50) NOT NULL,    -- not_null, unique, regex, range, in_list, custom
    rule_expression TEXT,              -- SQL expression or regex pattern
    parameters JSONB DEFAULT '{}',     -- Additional parameters (min, max, values, etc.)

    -- Behavior
    severity VARCHAR(50) DEFAULT 'error',  -- error, warning, info
    action_on_failure VARCHAR(50) DEFAULT 'reject',  -- reject, warn, quarantine
    error_message TEXT,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- 4. TRANSFORMATION RULES - Column transformations
-- =============================================================================
CREATE TABLE IF NOT EXISTS transformation_rules (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) REFERENCES pipeline_registry(pipeline_id),
    rule_name VARCHAR(255) NOT NULL,

    -- Transformation Definition
    rule_type VARCHAR(50) NOT NULL,    -- cast, derive, aggregate, lookup, standardize
    rule_order INTEGER DEFAULT 1,      -- Execution order
    apply_in_layer VARCHAR(50) DEFAULT 'silver',  -- bronze, silver, gold

    -- Source/Target
    source_columns JSONB,              -- Array of source column names
    target_column VARCHAR(255),
    expression TEXT,                   -- SQL expression or function

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- 5. BIGQUERY CONFIGURATION - Gold layer target config
-- =============================================================================
CREATE TABLE IF NOT EXISTS bigquery_configuration (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) REFERENCES pipeline_registry(pipeline_id) UNIQUE,

    -- Target
    project_id VARCHAR(255) NOT NULL,
    dataset_id VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,

    -- Load Settings
    load_strategy VARCHAR(50) DEFAULT 'full',  -- full, incremental, merge
    write_disposition VARCHAR(50) DEFAULT 'WRITE_TRUNCATE',

    -- Partitioning
    partition_field VARCHAR(255),
    partition_type VARCHAR(50),        -- DAY, MONTH, YEAR, HOUR
    partition_expiration_days INTEGER,

    -- Clustering
    clustering_columns JSONB,          -- Array of column names

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- 6. PIPELINE RUN HISTORY - Audit/execution tracking
-- =============================================================================
CREATE TABLE IF NOT EXISTS pipeline_run_history (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) REFERENCES pipeline_registry(pipeline_id),
    run_id VARCHAR(255) NOT NULL UNIQUE,

    -- Execution Info
    airflow_dag_id VARCHAR(255),
    airflow_run_id VARCHAR(255),
    reporting_date DATE,

    -- Status
    status VARCHAR(50) NOT NULL,       -- running, success, failed, cancelled
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,

    -- Metrics
    bronze_record_count BIGINT,
    silver_valid_count BIGINT,
    silver_invalid_count BIGINT,
    gold_record_count BIGINT,

    -- Errors
    error_message TEXT,
    error_details JSONB,

    -- Created
    created_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- 7. SCHEMA EVOLUTION HISTORY - Track schema changes
-- =============================================================================
CREATE TABLE IF NOT EXISTS schema_evolution_history (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(255) REFERENCES pipeline_registry(pipeline_id),

    -- Version Info
    old_version INTEGER NOT NULL,
    new_version INTEGER NOT NULL,

    -- Change Details
    change_type VARCHAR(50) NOT NULL,  -- add_column, drop_column, modify_column, rename_column
    column_name VARCHAR(255),
    old_definition JSONB,
    new_definition JSONB,

    -- Metadata
    change_reason TEXT,
    jira_ticket_id VARCHAR(50),

    -- Audit
    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(255)
);

-- =============================================================================
-- INDEXES
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_schema_definitions_pipeline ON schema_definitions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_validation_rules_pipeline ON validation_rules(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_transformation_rules_pipeline ON transformation_rules(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_run_history_pipeline ON pipeline_run_history(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_run_history_status ON pipeline_run_history(status);
CREATE INDEX IF NOT EXISTS idx_run_history_date ON pipeline_run_history(reporting_date);

-- =============================================================================
-- DONE
-- =============================================================================
SELECT 'Metadata tables created successfully' as status;
