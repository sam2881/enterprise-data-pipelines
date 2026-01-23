-- =============================================================================
-- AI Agent Platform - PostgreSQL Initialization
-- =============================================================================
-- This script initializes all databases and tables needed by the platform.
-- Runs automatically when postgres container starts for the first time.
-- =============================================================================

-- Create additional databases
CREATE DATABASE langfuse;
CREATE DATABASE iceberg_catalog;
CREATE DATABASE airflow;

-- Grant permissions to admin user
GRANT ALL PRIVILEGES ON DATABASE langfuse TO admin;
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO admin;
GRANT ALL PRIVILEGES ON DATABASE airflow TO admin;

-- Switch to agentdb (main database)
\c agentdb;

-- =============================================================================
-- Agent Events & Workflow State (Backend Orchestrator)
-- =============================================================================

CREATE TABLE IF NOT EXISTS agent_events (
    event_id SERIAL PRIMARY KEY,
    agent_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSONB,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_events_agent ON agent_events(agent_name);
CREATE INDEX IF NOT EXISTS idx_agent_events_status ON agent_events(status);
CREATE INDEX IF NOT EXISTS idx_agent_events_created ON agent_events(created_at DESC);

CREATE TABLE IF NOT EXISTS workflow_states (
    workflow_id VARCHAR(100) PRIMARY KEY,
    state JSONB NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Audit Logging (Governance & Compliance)
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id SERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50) NOT NULL,
    actor VARCHAR(100) NOT NULL,
    action VARCHAR(100) NOT NULL,
    resource VARCHAR(255),
    outcome VARCHAR(20),
    risk_level VARCHAR(20),
    details JSONB,
    ai_system_id VARCHAR(100),
    ai_decision_explanation TEXT,
    human_oversight_applied BOOLEAN DEFAULT FALSE,
    confidence_score DECIMAL(5,4),
    data_subjects_affected INTEGER DEFAULT 0,
    pii_involved BOOLEAN DEFAULT FALSE,
    cross_border_transfer BOOLEAN DEFAULT FALSE,
    checksum VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_log(actor);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_risk ON audit_log(risk_level);

-- =============================================================================
-- Metadata-Driven Pipeline Tables (Data Agent)
-- =============================================================================

-- Feed Registry
CREATE TABLE IF NOT EXISTS feed_registry (
    feed_id VARCHAR(100) PRIMARY KEY,
    feed_name VARCHAR(255) NOT NULL,
    source_type VARCHAR(50) NOT NULL DEFAULT 'csv',
    file_pattern VARCHAR(500) NOT NULL,
    file_delimiter VARCHAR(10) DEFAULT ',',
    file_header BOOLEAN DEFAULT TRUE,
    schedule_interval VARCHAR(50) DEFAULT '@daily',
    owner_email VARCHAR(255),
    team VARCHAR(100),
    retry_count INTEGER DEFAULT 3,
    retry_delay_minutes INTEGER DEFAULT 5,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_feed_registry_active ON feed_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_feed_registry_team ON feed_registry(team);

-- Feed Columns
CREATE TABLE IF NOT EXISTS feed_columns (
    column_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),
    column_name VARCHAR(255) NOT NULL,
    column_order INTEGER NOT NULL,
    source_type VARCHAR(50) DEFAULT 'string',
    target_type VARCHAR(50) NOT NULL,
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_nullable BOOLEAN DEFAULT TRUE,
    default_value VARCHAR(255),
    transform_expression VARCHAR(500),
    description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, column_name)
);

CREATE INDEX IF NOT EXISTS idx_feed_columns_feed ON feed_columns(feed_id);

-- Feed Validation Rules
CREATE TABLE IF NOT EXISTS feed_validation (
    rule_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    column_name VARCHAR(255),
    parameters JSONB DEFAULT '{}',
    severity VARCHAR(20) DEFAULT 'error',
    action_on_failure VARCHAR(20) DEFAULT 'reject',
    apply_in_layer VARCHAR(20) DEFAULT 'silver',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, rule_name)
);

CREATE INDEX IF NOT EXISTS idx_feed_validation_feed ON feed_validation(feed_id);

-- Feed Targets
CREATE TABLE IF NOT EXISTS feed_targets (
    target_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),
    layer VARCHAR(20) NOT NULL,
    database_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    partition_columns TEXT[] DEFAULT ARRAY['posting_date'],
    cluster_columns TEXT[] DEFAULT '{}',
    write_mode VARCHAR(20) DEFAULT 'append',
    file_format VARCHAR(20) DEFAULT 'parquet',
    compression VARCHAR(20) DEFAULT 'snappy',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, layer)
);

CREATE INDEX IF NOT EXISTS idx_feed_targets_feed ON feed_targets(feed_id);

-- Feed Aggregations (Gold Layer)
CREATE TABLE IF NOT EXISTS feed_aggregations (
    aggregation_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),
    aggregation_name VARCHAR(255) NOT NULL,
    group_by_columns TEXT[] NOT NULL,
    aggregation_expressions JSONB NOT NULL,
    filter_condition VARCHAR(500),
    target_database VARCHAR(100) DEFAULT 'gold',
    target_schema VARCHAR(100) DEFAULT 'analytics',
    target_table VARCHAR(255) NOT NULL,
    partition_columns TEXT[] DEFAULT ARRAY['_posting_date'],
    write_mode VARCHAR(20) DEFAULT 'overwrite',
    file_format VARCHAR(20) DEFAULT 'parquet',
    compression VARCHAR(20) DEFAULT 'snappy',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, aggregation_name)
);

CREATE INDEX IF NOT EXISTS idx_feed_aggregations_feed ON feed_aggregations(feed_id);

-- Feed State (Execution tracking)
CREATE TABLE IF NOT EXISTS feed_state (
    state_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed_registry(feed_id),
    posting_date DATE NOT NULL,
    last_watermark VARCHAR(255),
    last_successful_run TIMESTAMP,
    last_row_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, posting_date)
);

CREATE INDEX IF NOT EXISTS idx_feed_state_feed ON feed_state(feed_id);
CREATE INDEX IF NOT EXISTS idx_feed_state_status ON feed_state(status);

-- Feed Audit Log
CREATE TABLE IF NOT EXISTS feed_audit_log (
    audit_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    layer VARCHAR(20) NOT NULL,
    posting_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    input_rows INTEGER DEFAULT 0,
    output_rows INTEGER DEFAULT 0,
    rejected_rows INTEGER DEFAULT 0,
    validation_pass_rate DECIMAL(5,2),
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feed_audit_feed ON feed_audit_log(feed_id);
CREATE INDEX IF NOT EXISTS idx_feed_audit_run ON feed_audit_log(run_id);
CREATE INDEX IF NOT EXISTS idx_feed_audit_date ON feed_audit_log(posting_date DESC);

-- =============================================================================
-- Pipeline Requests & Artifacts (Generated Pipelines)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_requests (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) UNIQUE NOT NULL,
    source_uri TEXT NOT NULL,
    source_type VARCHAR(32) DEFAULT 'gcs',
    target_layer VARCHAR(16) DEFAULT 'silver',
    schedule VARCHAR(32) DEFAULT '@daily',
    business_context TEXT,
    jira_key VARCHAR(32),
    status VARCHAR(32) DEFAULT 'pending',
    risk_score DECIMAL(5,4) DEFAULT 0,
    requires_approval BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_artifacts (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) REFERENCES pipeline_requests(request_id),
    artifact_type VARCHAR(32) NOT NULL,
    content TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    source_config JSONB NOT NULL,
    target_config JSONB NOT NULL,
    schedule VARCHAR(50),
    owner VARCHAR(100),
    team VARCHAR(100),
    tags TEXT[],
    status VARCHAR(20) DEFAULT 'draft',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_versions (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(100) NOT NULL REFERENCES pipeline_metadata(pipeline_id),
    version VARCHAR(20) NOT NULL,
    spark_code TEXT,
    dag_code TEXT,
    dq_rules JSONB,
    commit_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(pipeline_id, version)
);

CREATE TABLE IF NOT EXISTS pipeline_executions (
    id SERIAL PRIMARY KEY,
    pipeline_id VARCHAR(100) NOT NULL,
    version VARCHAR(20),
    run_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    input_rows BIGINT,
    output_rows BIGINT,
    error_message TEXT,
    metrics JSONB
);

-- Agent execution logs
CREATE TABLE IF NOT EXISTS agent_execution_logs (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64),
    agent_name VARCHAR(64) NOT NULL,
    task_type VARCHAR(64),
    status VARCHAR(32),
    input_hash VARCHAR(32),
    output_hash VARCHAR(32),
    duration_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_requests_status ON pipeline_requests(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_requests_jira_key ON pipeline_requests(jira_key);
CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_request_id ON pipeline_artifacts(request_id);
CREATE INDEX IF NOT EXISTS idx_agent_logs_request_id ON agent_execution_logs(request_id);

-- =============================================================================
-- Update Trigger
-- =============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to tables with updated_at
DO $$
DECLARE
    t text;
BEGIN
    FOR t IN
        SELECT table_name
        FROM information_schema.columns
        WHERE column_name = 'updated_at'
        AND table_schema = 'public'
    LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS update_%s_updated_at ON %s;
            CREATE TRIGGER update_%s_updated_at
            BEFORE UPDATE ON %s
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        ', t, t, t, t);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- GCP-NATIVE METADATA-DRIVEN PIPELINE TABLES (V2)
-- =============================================================================
-- These tables support the new fully automated, GCP-native pipeline system.
-- Key principle: ONLY provide source location + schema. EVERYTHING else automated.
-- =============================================================================

-- Source Registry
CREATE TABLE IF NOT EXISTS source_registry (
    source_id           VARCHAR(50) PRIMARY KEY,
    source_name         VARCHAR(100) NOT NULL,
    source_type         VARCHAR(50) NOT NULL,  -- gcs, database, api, sftp, kafka
    connection_config   JSONB,
    description         TEXT,
    owner_email         VARCHAR(255),
    team                VARCHAR(100),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by          VARCHAR(100),
    updated_by          VARCHAR(100)
);

-- Feed Group
CREATE TABLE IF NOT EXISTS feed_group (
    group_id            VARCHAR(50) PRIMARY KEY,
    group_name          VARCHAR(100) NOT NULL,
    source_id           VARCHAR(50) REFERENCES source_registry(source_id),
    description         TEXT,
    spark_config_id     VARCHAR(50),
    default_schedule    VARCHAR(50) DEFAULT '@daily',
    notification_email  VARCHAR(255),
    slack_channel       VARCHAR(100),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Extended Feed Schema (V2 - with all features)
CREATE TABLE IF NOT EXISTS feed_schema (
    schema_id           SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    column_name         VARCHAR(255) NOT NULL,
    column_order        INTEGER NOT NULL,
    source_data_type    VARCHAR(50),
    target_data_type    VARCHAR(50) NOT NULL,
    is_nullable         BOOLEAN DEFAULT TRUE,
    is_primary_key      BOOLEAN DEFAULT FALSE,
    is_partition_key    BOOLEAN DEFAULT FALSE,
    transform_expr      TEXT,
    is_pii              BOOLEAN DEFAULT FALSE,
    pii_action          VARCHAR(50),
    not_null            BOOLEAN DEFAULT FALSE,
    unique_check        BOOLEAN DEFAULT FALSE,
    description         TEXT,
    business_name       VARCHAR(255),
    version             INTEGER DEFAULT 1,
    is_active           BOOLEAN DEFAULT TRUE,
    added_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deprecated_at       TIMESTAMP,
    UNIQUE(feed_id, column_name, version)
);

CREATE INDEX IF NOT EXISTS idx_feed_schema_feed ON feed_schema(feed_id);

-- Feed Contract (File patterns, paths)
CREATE TABLE IF NOT EXISTS feed_contract (
    contract_id         SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    file_pattern        VARCHAR(500) NOT NULL,
    file_format         VARCHAR(50) DEFAULT 'csv',
    delimiter           VARCHAR(10) DEFAULT ',',
    quote_char          VARCHAR(10) DEFAULT '"',
    escape_char         VARCHAR(10) DEFAULT '\\',
    has_header          BOOLEAN DEFAULT TRUE,
    encoding            VARCHAR(50) DEFAULT 'UTF-8',
    null_value          VARCHAR(50) DEFAULT '',
    source_path         VARCHAR(500) NOT NULL,
    bronze_path         VARCHAR(500),
    silver_path         VARCHAR(500),
    gold_path           VARCHAR(500),
    rejected_path       VARCHAR(500),
    compression         VARCHAR(50),
    record_length       INTEGER,
    merge_files         BOOLEAN DEFAULT TRUE,
    ingestion_frequency VARCHAR(50) DEFAULT '30 11 18 * *',
    soft_fail           BOOLEAN DEFAULT FALSE,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feed_contract_feed ON feed_contract(feed_id);

-- Feed Target Mapping (Layer-specific targets)
CREATE TABLE IF NOT EXISTS feed_target_mapping (
    target_id           SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    layer               VARCHAR(20) NOT NULL,
    bq_project          VARCHAR(100),
    bq_dataset          VARCHAR(100),
    bq_table            VARCHAR(200),
    gcs_bucket          VARCHAR(200),
    gcs_path            VARCHAR(500),
    partition_columns   TEXT[],
    clustering_columns  TEXT[],
    write_mode          VARCHAR(50) DEFAULT 'append',
    auto_create_table   BOOLEAN DEFAULT TRUE,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, layer)
);

CREATE INDEX IF NOT EXISTS idx_feed_target_feed ON feed_target_mapping(feed_id);

-- Transformation Rules (Spark SQL stored in metadata)
CREATE TABLE IF NOT EXISTS transformation_rules (
    rule_id             SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    rule_name           VARCHAR(100) NOT NULL,
    source_layer        VARCHAR(20) NOT NULL,
    target_layer        VARCHAR(20) NOT NULL,
    transform_sql       TEXT NOT NULL,
    execution_order     INTEGER DEFAULT 1,
    condition_sql       TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transformation_feed ON transformation_rules(feed_id);

-- Aggregation Rules (Gold layer SQL in metadata)
CREATE TABLE IF NOT EXISTS aggregation_rules (
    agg_id              SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    aggregation_name    VARCHAR(100) NOT NULL,
    target_table        VARCHAR(200) NOT NULL,
    aggregation_sql     TEXT,
    group_by_columns    TEXT[],
    aggregations        JSONB,
    filter_condition    TEXT,
    write_mode          VARCHAR(50) DEFAULT 'append',
    schedule_override   VARCHAR(50),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, aggregation_name)
);

CREATE INDEX IF NOT EXISTS idx_aggregation_feed ON aggregation_rules(feed_id);

-- Validation Rules (Great Expectations style)
CREATE TABLE IF NOT EXISTS validation_rules (
    validation_id       SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    rule_name           VARCHAR(100) NOT NULL,
    apply_layer         VARCHAR(20) NOT NULL,
    column_name         VARCHAR(255),
    rule_type           VARCHAR(100) NOT NULL,
    parameters          JSONB,
    severity            VARCHAR(20) DEFAULT 'error',
    action_on_failure   VARCHAR(50) DEFAULT 'reject_row',
    error_threshold     DECIMAL(5,2) DEFAULT 0,
    custom_sql          TEXT,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_validation_feed ON validation_rules(feed_id);

-- Spark Config (Dataproc settings)
CREATE TABLE IF NOT EXISTS spark_config (
    config_id           VARCHAR(50) PRIMARY KEY,
    config_name         VARCHAR(100) NOT NULL,
    master_machine_type VARCHAR(50) DEFAULT 'n1-standard-4',
    worker_machine_type VARCHAR(50) DEFAULT 'n1-standard-4',
    num_workers         INTEGER DEFAULT 2,
    num_preemptible     INTEGER DEFAULT 0,
    executor_memory     VARCHAR(20) DEFAULT '4g',
    executor_cores      INTEGER DEFAULT 2,
    driver_memory       VARCHAR(20) DEFAULT '4g',
    spark_properties    JSONB,
    idle_delete_ttl     INTEGER DEFAULT 600,
    labels              JSONB,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline Runs (V2 - enhanced tracking)
CREATE TABLE IF NOT EXISTS pipeline_runs_v2 (
    run_id              VARCHAR(100) PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    posting_date        DATE NOT NULL,
    dag_run_id          VARCHAR(255),
    trigger_type        VARCHAR(50),
    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP,
    duration_seconds    INTEGER,
    status              VARCHAR(50) NOT NULL,
    layer_metrics       JSONB,
    validation_pass_rate DECIMAL(5,2),
    validation_results  JSONB,
    error_message       TEXT,
    error_details       JSONB,
    source_files        TEXT[],
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_v2_feed ON pipeline_runs_v2(feed_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_v2_date ON pipeline_runs_v2(posting_date);

-- Schema Versions (For evolution)
CREATE TABLE IF NOT EXISTS schema_versions (
    version_id          SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    version             INTEGER NOT NULL,
    schema_json         JSONB NOT NULL,
    change_type         VARCHAR(50),
    change_summary      TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by          VARCHAR(100),
    bq_applied          BOOLEAN DEFAULT FALSE,
    bq_applied_at       TIMESTAMP,
    UNIQUE(feed_id, version)
);

-- History Load Requests
CREATE TABLE IF NOT EXISTS history_load_requests (
    request_id          SERIAL PRIMARY KEY,
    feed_id             VARCHAR(100) NOT NULL,
    start_date          DATE NOT NULL,
    end_date            DATE NOT NULL,
    status              VARCHAR(50) DEFAULT 'pending',
    total_days          INTEGER,
    completed_days      INTEGER DEFAULT 0,
    failed_days         INTEGER DEFAULT 0,
    requested_by        VARCHAR(100),
    requested_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    notes               TEXT
);

-- Insert default Spark configs
INSERT INTO spark_config (config_id, config_name, master_machine_type, worker_machine_type, num_workers, num_preemptible, executor_memory, driver_memory)
VALUES
    ('small', 'Small Cluster', 'n1-standard-2', 'n1-standard-2', 2, 0, '2g', '2g'),
    ('medium', 'Medium Cluster', 'n1-standard-4', 'n1-standard-4', 2, 2, '4g', '4g'),
    ('large', 'Large Cluster', 'n1-standard-8', 'n1-standard-8', 4, 4, '8g', '8g')
ON CONFLICT (config_id) DO NOTHING;

-- =============================================================================
-- V2 VIEWS
-- =============================================================================

-- Active feeds with status
CREATE OR REPLACE VIEW v_feed_status AS
SELECT
    f.feed_id,
    f.feed_name,
    f.schedule_interval,
    f.is_active,
    fs.posting_date as last_posting_date,
    fs.status as last_status,
    fs.last_row_count,
    fs.last_successful_run,
    fs.error_message
FROM feed_registry f
LEFT JOIN LATERAL (
    SELECT * FROM feed_state
    WHERE feed_id = f.feed_id
    ORDER BY posting_date DESC
    LIMIT 1
) fs ON TRUE;

-- Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;

-- Done
SELECT 'AI Agent Platform database initialized successfully!' as status;
