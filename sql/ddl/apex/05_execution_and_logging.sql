-- ═══════════════════════════════════════════════════════════════════════════
-- APEX DATA AGENT - EXECUTION AND LOGGING TABLES
-- Part 5: Pipeline Execution, Task Execution, Audit, Lineage, Errors
-- ═══════════════════════════════════════════════════════════════════════════

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 1. PIPELINE EXECUTION                                                    │
-- │    Tracks each pipeline run                                              │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS pipeline_execution (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_id UUID NOT NULL REFERENCES feed(feed_id),
    dag_run_id VARCHAR(500) NOT NULL,
    execution_date DATE NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 1,  -- Auto-increments per feed_id + execution_date re-run

    -- Timing
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP,

    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- PENDING, RUNNING, SUCCESS, FAILED, SKIPPED, NO_FILE, TIMEOUT

    -- Trigger
    trigger_type VARCHAR(50),  -- scheduled, manual, backfill, external

    -- Parameters
    parameters JSONB,

    -- Metrics
    records_processed BIGINT DEFAULT 0,
    bytes_processed BIGINT DEFAULT 0,

    -- Error info
    error_message TEXT,
    error_code VARCHAR(50),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_execution_feed ON pipeline_execution(feed_id);
CREATE INDEX idx_execution_date ON pipeline_execution(execution_date);
CREATE INDEX idx_execution_status ON pipeline_execution(status);
CREATE INDEX idx_execution_start ON pipeline_execution(start_ts);
CREATE INDEX idx_execution_dag_run ON pipeline_execution(dag_run_id);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 2. TASK EXECUTION                                                        │
-- │    Tracks individual task runs within a pipeline                         │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS task_execution (
    task_exec_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline_execution(execution_id),
    task_id VARCHAR(200) NOT NULL,
    task_type VARCHAR(100),  -- PYTHON, SPARK, SENSOR, BRANCH

    -- Timing
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP,

    -- Status
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',

    -- Metrics
    records_read BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    records_rejected BIGINT DEFAULT 0,
    bytes_read BIGINT DEFAULT 0,
    bytes_written BIGINT DEFAULT 0,

    -- Error info
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,

    -- Spark-specific
    spark_application_id VARCHAR(200),
    spark_history_url VARCHAR(500),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_task_execution_exec ON task_execution(execution_id);
CREATE INDEX idx_task_execution_task ON task_execution(task_id);
CREATE INDEX idx_task_execution_status ON task_execution(status);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 3. AUDIT LOG                                                             │
-- │    Zone-level audit trail for data processing                            │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline_execution(execution_id),
    zone_level VARCHAR(20) NOT NULL,
    action_type VARCHAR(100) NOT NULL,  -- FILE_FOUND, COPY, MOVE, TRANSFORM, VALIDATE, etc.
    entity_name VARCHAR(500),  -- File path, table name, etc.
    record_count BIGINT DEFAULT 0,
    message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_execution ON audit_log(execution_id);
CREATE INDEX idx_audit_zone ON audit_log(zone_level);
CREATE INDEX idx_audit_action ON audit_log(action_type);
CREATE INDEX idx_audit_created ON audit_log(created_at);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 4. DATA LINEAGE                                                          │
-- │    Tracks data flow between entities                                     │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS data_lineage (
    lineage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline_execution(execution_id),
    source_entity VARCHAR(500) NOT NULL,
    target_entity VARCHAR(500) NOT NULL,
    transform_type VARCHAR(100),  -- COPY, VIEW, SPARK, MERGE
    column_mapping JSONB,  -- Maps source columns to target columns
    row_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_lineage_execution ON data_lineage(execution_id);
CREATE INDEX idx_lineage_source ON data_lineage(source_entity);
CREATE INDEX idx_lineage_target ON data_lineage(target_entity);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 5. VALIDATION LOG                                                        │
-- │    Records validation rule execution results                             │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS validation_log (
    validation_log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline_execution(execution_id),
    validation_id UUID NOT NULL REFERENCES validation_rule(validation_id),
    zone_level VARCHAR(20) NOT NULL,
    validation_type VARCHAR(50) NOT NULL,
    rule_name VARCHAR(200) NOT NULL,

    -- Results
    total_records BIGINT NOT NULL,
    passed_records BIGINT NOT NULL,
    failed_records BIGINT NOT NULL,
    pass_percentage DECIMAL(5,2) NOT NULL,
    threshold_percentage DECIMAL(5,2) NOT NULL,

    -- Outcome
    is_passed BOOLEAN NOT NULL,
    is_blocking BOOLEAN NOT NULL,

    -- Debug info
    sample_failures JSONB,  -- Sample of failed records
    execution_time_ms INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_validation_log_execution ON validation_log(execution_id);
CREATE INDEX idx_validation_log_zone ON validation_log(zone_level);
CREATE INDEX idx_validation_log_passed ON validation_log(is_passed);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 6. ERROR LOG                                                             │
-- │    Detailed error tracking with context                                  │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS error_log (
    error_log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID REFERENCES pipeline_execution(execution_id),
    task_exec_id UUID REFERENCES task_execution(task_exec_id),
    error_type VARCHAR(100) NOT NULL,
    error_code VARCHAR(50),
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    error_context JSONB,
    is_transient BOOLEAN DEFAULT false,
    retry_count INTEGER DEFAULT 0,
    resolution_status VARCHAR(50) DEFAULT 'UNRESOLVED',  -- UNRESOLVED, RESOLVED, IGNORED
    resolution_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP
);

CREATE INDEX idx_error_execution ON error_log(execution_id);
CREATE INDEX idx_error_type ON error_log(error_type);
CREATE INDEX idx_error_status ON error_log(resolution_status);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 7. METADATA AUDIT LOG                                                    │
-- │    Tracks all metadata changes                                           │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS metadata_audit_log (
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    operation_type VARCHAR(20) NOT NULL,  -- INSERT, UPDATE, DELETE, SCHEMA_EVOLUTION
    record_id UUID NOT NULL,
    old_values JSONB,
    new_values JSONB,
    change_reason TEXT,
    executed_by VARCHAR(100) NOT NULL,
    execution_context VARCHAR(500),  -- DAG run ID, ticket ID, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metadata_audit_table ON metadata_audit_log(table_name);
CREATE INDEX idx_metadata_audit_record ON metadata_audit_log(record_id);
CREATE INDEX idx_metadata_audit_operation ON metadata_audit_log(operation_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 8. AGENT DECISION LOG                                                    │
-- │    Tracks autonomous agent decisions                                     │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS agent_decision_log (
    decision_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    decision_type VARCHAR(100) NOT NULL,  -- TEMPLATE_SELECTION, PATTERN_MATCH, etc.
    input_context JSONB NOT NULL,
    decision_made VARCHAR(200) NOT NULL,
    decision_rationale TEXT NOT NULL,
    alternatives_considered JSONB,
    confidence_score DECIMAL(3,2),  -- 0.00 to 1.00
    execution_id UUID REFERENCES pipeline_execution(execution_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_agent_decision_type ON agent_decision_log(decision_type);
CREATE INDEX idx_agent_decision_created ON agent_decision_log(created_at);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 9. TEMPLATE CHANGE LOG                                                   │
-- │    Tracks changes to DAG templates                                       │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS template_change_log (
    change_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    template_id UUID NOT NULL REFERENCES dag_template(template_id),
    change_type VARCHAR(50) NOT NULL,  -- EXTENSION, MODIFICATION, DEPRECATION
    change_description TEXT NOT NULL,
    previous_template TEXT,
    new_template TEXT,
    affected_pipeline_count INTEGER,
    affected_pipelines JSONB,
    changed_by VARCHAR(100) NOT NULL,
    change_reason TEXT,
    rollback_available BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_template_change_template ON template_change_log(template_id);
CREATE INDEX idx_template_change_type ON template_change_log(change_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 10. SLA BREACH LOG                                                       │
-- │    Tracks SLA violations                                                 │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS sla_breach_log (
    breach_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sla_id UUID NOT NULL REFERENCES sla_definition(sla_id),
    execution_id UUID NOT NULL REFERENCES pipeline_execution(execution_id),
    breach_type VARCHAR(50) NOT NULL,  -- WARNING, CRITICAL
    expected_value INTEGER,
    actual_value INTEGER,
    breach_message TEXT,
    notified BOOLEAN DEFAULT false,
    acknowledged BOOLEAN DEFAULT false,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sla_breach_sla ON sla_breach_log(sla_id);
CREATE INDEX idx_sla_breach_execution ON sla_breach_log(execution_id);
CREATE INDEX idx_sla_breach_type ON sla_breach_log(breach_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 11. EXECUTION COST LOG                                                   │
-- │    Tracks resource usage and costs                                       │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS execution_cost_log (
    cost_log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    execution_id UUID NOT NULL REFERENCES pipeline_execution(execution_id),
    resource_type VARCHAR(50) NOT NULL,  -- SPARK, STORAGE, NETWORK
    resource_details JSONB,
    quantity DECIMAL(15,4),
    unit VARCHAR(20),  -- CORE_HOURS, GB, GB_TRANSFERRED
    unit_cost DECIMAL(10,4),
    total_cost DECIMAL(15,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cost_execution ON execution_cost_log(execution_id);
CREATE INDEX idx_cost_resource ON execution_cost_log(resource_type);
