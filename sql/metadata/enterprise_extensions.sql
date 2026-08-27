-- =============================================================================
-- ENTERPRISE METADATA EXTENSIONS
-- Purpose: Extend the core schema with enterprise-grade capabilities
-- These are ADDITIVE changes - do not modify existing tables
-- =============================================================================

-- =============================================================================
-- A. DATA LINEAGE & DEPENDENCIES
-- Purpose: Track data flow between pipelines for impact analysis
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_data_lineage (
    lineage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Source and Target Pipelines
    upstream_pipeline_id UUID REFERENCES platform_pipeline_registry(pipeline_id),
    downstream_pipeline_id UUID REFERENCES platform_pipeline_registry(pipeline_id),

    -- Relationship Details
    relationship_type VARCHAR(50) NOT NULL, -- 'reads_from', 'writes_to', 'depends_on', 'triggers'
    lineage_depth INTEGER DEFAULT 1,        -- Distance from root source

    -- Column-Level Lineage (optional)
    source_columns JSONB DEFAULT '[]',
    target_columns JSONB DEFAULT '[]',
    transformation_logic TEXT,

    -- Metadata
    is_active BOOLEAN DEFAULT TRUE,
    lineage_version INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_lineage UNIQUE (upstream_pipeline_id, downstream_pipeline_id, relationship_type)
);

CREATE INDEX idx_lineage_upstream ON platform_data_lineage(upstream_pipeline_id);
CREATE INDEX idx_lineage_downstream ON platform_data_lineage(downstream_pipeline_id);

-- =============================================================================
-- B. DATA CONTRACTS
-- Purpose: Define expectations for data producers and consumers
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_data_contracts (
    contract_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Contract Identity
    contract_name VARCHAR(255) NOT NULL,
    contract_version VARCHAR(20) NOT NULL DEFAULT '1.0.0',

    -- Schema Contract
    schema_hash VARCHAR(64),               -- SHA256 of schema definition
    schema_compatibility VARCHAR(20) DEFAULT 'backward', -- backward, forward, full, none

    -- Freshness SLA
    freshness_sla_hours INTEGER,           -- Max acceptable data age
    freshness_alert_threshold_hours INTEGER,

    -- Completeness
    completeness_threshold_pct DECIMAL(5,2) DEFAULT 99.00,
    row_count_min INTEGER,
    row_count_max INTEGER,

    -- Quality Gates
    quality_score_min DECIMAL(5,2) DEFAULT 95.00,
    critical_columns JSONB DEFAULT '[]',   -- Columns that must never be null

    -- Contract Status
    status VARCHAR(20) DEFAULT 'active',   -- draft, active, deprecated
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiration_date DATE,

    -- Ownership
    owner_team VARCHAR(100),
    consumer_teams JSONB DEFAULT '[]',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_contract_version UNIQUE (pipeline_id, contract_version)
);

CREATE INDEX idx_contract_pipeline ON platform_data_contracts(pipeline_id);
CREATE INDEX idx_contract_status ON platform_data_contracts(status);

-- =============================================================================
-- C. SCHEMA CHANGE HISTORY
-- Purpose: Track all schema evolution for audit and rollback
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_schema_changes (
    change_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schema_id UUID NOT NULL REFERENCES platform_pipeline_schemas(schema_id),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id),

    -- Change Details
    change_type VARCHAR(50) NOT NULL,      -- add_column, drop_column, rename_column, modify_type, reorder
    change_description TEXT,

    -- Before/After State
    previous_schema_version INTEGER,
    new_schema_version INTEGER,
    column_name VARCHAR(255),
    previous_definition JSONB,             -- Column definition before change
    new_definition JSONB,                  -- Column definition after change

    -- Impact Assessment
    is_breaking_change BOOLEAN DEFAULT FALSE,
    affected_pipelines JSONB DEFAULT '[]', -- List of downstream pipelines
    migration_required BOOLEAN DEFAULT FALSE,
    migration_script TEXT,

    -- Approval & Audit
    requested_by VARCHAR(255) NOT NULL,
    approved_by VARCHAR(255),
    approval_status VARCHAR(20) DEFAULT 'pending', -- pending, approved, rejected
    approved_at TIMESTAMP WITH TIME ZONE,

    -- Rollback Support
    is_rolled_back BOOLEAN DEFAULT FALSE,
    rolled_back_at TIMESTAMP WITH TIME ZONE,
    rollback_reason TEXT,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT valid_version_progression CHECK (new_schema_version > previous_schema_version)
);

CREATE INDEX idx_schema_change_pipeline ON platform_schema_changes(pipeline_id);
CREATE INDEX idx_schema_change_type ON platform_schema_changes(change_type);
CREATE INDEX idx_schema_change_approval ON platform_schema_changes(approval_status);

-- =============================================================================
-- D. TRANSFORMATION RULES (Enhanced for NLP Support)
-- Purpose: Store business logic with support for multiple input formats
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_transformation_rules (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Rule Identity
    rule_name VARCHAR(255) NOT NULL,
    rule_version INTEGER DEFAULT 1,

    -- Processing Layer
    layer VARCHAR(20) NOT NULL,            -- landing, bronze, silver, gold, trusted
    execution_order INTEGER NOT NULL DEFAULT 1,

    -- Rule Type & Logic
    rule_type VARCHAR(50) NOT NULL,        -- cast, derive, aggregate, filter, join, window, dedupe, pivot, mask
    input_format VARCHAR(20) DEFAULT 'structured', -- structured, sql, nl_description, yaml

    -- Source Columns
    source_columns JSONB DEFAULT '[]',     -- Input columns for this rule
    target_column VARCHAR(255),            -- Output column name

    -- Rule Definition (multiple formats supported)
    structured_config JSONB,               -- Structured rule definition
    sql_expression TEXT,                   -- Raw SQL copy-paste
    nl_description TEXT,                   -- Natural language description
    yaml_config TEXT,                      -- YAML configuration

    -- Generated Code (LLM output)
    generated_pyspark TEXT,                -- Generated PySpark code
    generated_sql TEXT,                    -- Generated SQL
    generation_confidence DECIMAL(3,2),    -- LLM confidence score (0.00-1.00)

    -- Validation
    is_validated BOOLEAN DEFAULT FALSE,
    validation_errors JSONB DEFAULT '[]',

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    effective_start_date DATE DEFAULT CURRENT_DATE,
    effective_end_date DATE,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(255),

    CONSTRAINT unique_rule_order UNIQUE (pipeline_id, layer, execution_order)
);

CREATE INDEX idx_transform_rule_pipeline ON platform_transformation_rules(pipeline_id);
CREATE INDEX idx_transform_rule_layer ON platform_transformation_rules(layer);
CREATE INDEX idx_transform_rule_type ON platform_transformation_rules(rule_type);

-- =============================================================================
-- E. COST TRACKING
-- Purpose: Track GCP resource costs per pipeline for FinOps
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_costs (
    cost_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id),

    -- Time Period
    cost_date DATE NOT NULL,
    cost_period VARCHAR(20) DEFAULT 'daily', -- hourly, daily, weekly, monthly

    -- Resource Costs (in USD)
    dataproc_cost DECIMAL(12,4) DEFAULT 0,
    bigquery_cost DECIMAL(12,4) DEFAULT 0,
    storage_cost DECIMAL(12,4) DEFAULT 0,
    dataflow_cost DECIMAL(12,4) DEFAULT 0,
    pubsub_cost DECIMAL(12,4) DEFAULT 0,
    composer_cost DECIMAL(12,4) DEFAULT 0,
    total_cost DECIMAL(12,4) GENERATED ALWAYS AS (
        dataproc_cost + bigquery_cost + storage_cost + dataflow_cost + pubsub_cost + composer_cost
    ) STORED,

    -- Resource Usage
    dataproc_vcpu_hours DECIMAL(10,2) DEFAULT 0,
    bigquery_bytes_processed BIGINT DEFAULT 0,
    bigquery_slots_hours DECIMAL(10,2) DEFAULT 0,
    storage_gb DECIMAL(10,2) DEFAULT 0,

    -- Budget Tracking
    budget_allocated DECIMAL(12,4),
    budget_remaining DECIMAL(12,4),
    is_over_budget BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_cost_period UNIQUE (pipeline_id, cost_date, cost_period)
);

CREATE INDEX idx_cost_pipeline ON platform_pipeline_costs(pipeline_id);
CREATE INDEX idx_cost_date ON platform_pipeline_costs(cost_date);

-- =============================================================================
-- F. GOVERNANCE & APPROVALS
-- Purpose: Multi-step approval workflows for production changes
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_approval_requests (
    approval_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id),

    -- Request Details
    request_type VARCHAR(50) NOT NULL,     -- deploy_prod, schema_change, access_grant, cost_increase
    request_description TEXT,

    -- Requestor
    requested_by VARCHAR(255) NOT NULL,
    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Current State
    current_step INTEGER DEFAULT 1,
    total_steps INTEGER DEFAULT 1,
    status VARCHAR(20) DEFAULT 'pending',  -- pending, in_review, approved, rejected, cancelled

    -- Approval Chain (JSONB for flexibility)
    approval_chain JSONB DEFAULT '[]',     -- [{step: 1, approver: "team-lead", status: "approved"}, ...]

    -- Outcome
    final_decision VARCHAR(20),            -- approved, rejected
    decision_reason TEXT,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Related Artifacts
    related_change_id UUID,                -- Link to platform_schema_changes if applicable
    related_execution_id UUID,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_approval_pipeline ON platform_approval_requests(pipeline_id);
CREATE INDEX idx_approval_status ON platform_approval_requests(status);
CREATE INDEX idx_approval_requestor ON platform_approval_requests(requested_by);

-- =============================================================================
-- G. DATA PRODUCTS REGISTRY
-- Purpose: Link pipelines to business domains and data products
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_data_products (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Product Identity
    product_name VARCHAR(255) NOT NULL UNIQUE,
    product_description TEXT,

    -- Business Domain
    business_domain VARCHAR(100) NOT NULL,
    business_subdomain VARCHAR(100),
    business_owner VARCHAR(255),

    -- Classification
    platform_data_classification VARCHAR(50) DEFAULT 'internal', -- public, internal, confidential, restricted
    sensitivity_level VARCHAR(20) DEFAULT 'low',        -- low, medium, high, critical

    -- Compliance
    compliance_tags JSONB DEFAULT '[]',    -- ['GDPR', 'SOX', 'HIPAA']
    retention_days INTEGER DEFAULT 365,

    -- Access Control
    access_groups JSONB DEFAULT '[]',      -- Groups with access

    -- Status
    status VARCHAR(20) DEFAULT 'active',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Link table: Pipelines to Data Products (many-to-many)
CREATE TABLE IF NOT EXISTS platform_pipeline_data_products (
    pipeline_id UUID REFERENCES platform_pipeline_registry(pipeline_id) ON DELETE CASCADE,
    product_id UUID REFERENCES platform_data_products(product_id) ON DELETE CASCADE,
    relationship_type VARCHAR(50) DEFAULT 'produces', -- produces, consumes
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (pipeline_id, product_id)
);

CREATE INDEX idx_product_domain ON platform_data_products(business_domain);
CREATE INDEX idx_product_classification ON platform_data_products(platform_data_classification);

-- =============================================================================
-- H. OBSERVABILITY METRICS
-- Purpose: Track performance metrics for trending and alerting
-- =============================================================================

CREATE TABLE IF NOT EXISTS platform_pipeline_metrics (
    metric_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES platform_pipeline_registry(pipeline_id),
    execution_id UUID REFERENCES platform_pipeline_executions(execution_id),

    -- Time Window
    metric_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    metric_window VARCHAR(20) DEFAULT '1h', -- 1m, 5m, 1h, 1d

    -- Performance Metrics
    execution_duration_seconds INTEGER,
    records_per_second DECIMAL(12,2),
    bytes_per_second DECIMAL(12,2),

    -- Resource Metrics
    peak_memory_mb INTEGER,
    avg_cpu_percent DECIMAL(5,2),
    shuffle_bytes BIGINT,
    spill_bytes BIGINT,

    -- Quality Metrics
    null_rate_pct DECIMAL(5,2),
    duplicate_rate_pct DECIMAL(5,2),
    schema_drift_detected BOOLEAN DEFAULT FALSE,

    -- SLA Metrics
    sla_target_seconds INTEGER,
    sla_met BOOLEAN,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_metrics_pipeline ON platform_pipeline_metrics(pipeline_id);
CREATE INDEX idx_metrics_timestamp ON platform_pipeline_metrics(metric_timestamp);
CREATE INDEX idx_metrics_sla ON platform_pipeline_metrics(sla_met);

-- Partition by month for large scale
-- Note: Implement table partitioning in production

-- =============================================================================
-- ENTERPRISE VIEWS
-- =============================================================================

-- View: Pipeline Health Dashboard
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT
    pr.pipeline_id,
    pr.dag_id,
    pr.pipeline_name,
    pr.domain,
    pr.environment,
    pr.status,

    -- Latest Execution
    le.status AS last_execution_status,
    le.executed_at AS last_execution_time,
    le.records_written AS last_records,
    le.quality_score AS last_quality_score,

    -- Contract Compliance
    dc.freshness_sla_hours,
    dc.completeness_threshold_pct,
    dc.quality_score_min,

    -- Cost (last 30 days)
    COALESCE(cost_30d.total_cost, 0) AS cost_last_30_days,

    -- Health Score (calculated)
    CASE
        WHEN le.status = 'success' AND le.quality_score >= COALESCE(dc.quality_score_min, 95) THEN 'healthy'
        WHEN le.status = 'success' AND le.quality_score < COALESCE(dc.quality_score_min, 95) THEN 'warning'
        WHEN le.status = 'failed' THEN 'critical'
        ELSE 'unknown'
    END AS health_status

FROM platform_pipeline_registry pr
LEFT JOIN LATERAL (
    SELECT status, executed_at, records_written, quality_score
    FROM platform_pipeline_executions pe
    WHERE pe.pipeline_id = pr.pipeline_id
    ORDER BY executed_at DESC
    LIMIT 1
) le ON TRUE
LEFT JOIN platform_data_contracts dc ON dc.pipeline_id = pr.pipeline_id AND dc.status = 'active'
LEFT JOIN LATERAL (
    SELECT SUM(total_cost) AS total_cost
    FROM platform_pipeline_costs pc
    WHERE pc.pipeline_id = pr.pipeline_id
    AND pc.cost_date >= CURRENT_DATE - INTERVAL '30 days'
) cost_30d ON TRUE
WHERE pr.status = 'active';

-- View: Lineage Graph (for visualization)
CREATE OR REPLACE VIEW v_lineage_graph AS
SELECT
    dl.lineage_id,
    up.dag_id AS source_dag,
    up.pipeline_name AS source_name,
    up.domain AS source_domain,
    dp.dag_id AS target_dag,
    dp.pipeline_name AS target_name,
    dp.domain AS target_domain,
    dl.relationship_type,
    dl.lineage_depth
FROM platform_data_lineage dl
JOIN platform_pipeline_registry up ON dl.upstream_pipeline_id = up.pipeline_id
JOIN platform_pipeline_registry dp ON dl.downstream_pipeline_id = dp.pipeline_id
WHERE dl.is_active = TRUE;

-- View: Schema Change Audit
CREATE OR REPLACE VIEW v_schema_audit AS
SELECT
    sc.change_id,
    pr.dag_id,
    pr.pipeline_name,
    sc.change_type,
    sc.column_name,
    sc.is_breaking_change,
    sc.previous_schema_version,
    sc.new_schema_version,
    sc.requested_by,
    sc.approval_status,
    sc.approved_by,
    sc.created_at AS change_requested_at,
    sc.approved_at
FROM platform_schema_changes sc
JOIN platform_pipeline_registry pr ON sc.pipeline_id = pr.pipeline_id
ORDER BY sc.created_at DESC;

-- =============================================================================
-- ENTERPRISE FUNCTIONS
-- =============================================================================

-- Function: Get Impact Analysis for Pipeline Change
CREATE OR REPLACE FUNCTION get_downstream_impact(p_pipeline_id UUID)
RETURNS TABLE (
    affected_pipeline_id UUID,
    dag_id VARCHAR,
    pipeline_name VARCHAR,
    domain VARCHAR,
    lineage_depth INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE downstream AS (
        -- Base case: direct dependents
        SELECT
            dl.downstream_pipeline_id,
            1 AS depth
        FROM platform_data_lineage dl
        WHERE dl.upstream_pipeline_id = p_pipeline_id
        AND dl.is_active = TRUE

        UNION ALL

        -- Recursive case: dependents of dependents
        SELECT
            dl.downstream_pipeline_id,
            d.depth + 1
        FROM platform_data_lineage dl
        JOIN downstream d ON dl.upstream_pipeline_id = d.downstream_pipeline_id
        WHERE dl.is_active = TRUE
        AND d.depth < 10  -- Prevent infinite loops
    )
    SELECT DISTINCT
        pr.pipeline_id,
        pr.dag_id,
        pr.pipeline_name,
        pr.domain,
        MIN(d.depth) AS lineage_depth
    FROM downstream d
    JOIN platform_pipeline_registry pr ON d.downstream_pipeline_id = pr.pipeline_id
    GROUP BY pr.pipeline_id, pr.dag_id, pr.pipeline_name, pr.domain
    ORDER BY lineage_depth, pr.dag_id;
END;
$$ LANGUAGE plpgsql;

-- Function: Calculate Pipeline Cost Trend
CREATE OR REPLACE FUNCTION get_cost_trend(
    p_pipeline_id UUID,
    p_days INTEGER DEFAULT 30
)
RETURNS TABLE (
    cost_date DATE,
    total_cost DECIMAL,
    cost_change_pct DECIMAL,
    running_avg_7d DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        pc.cost_date,
        pc.total_cost,
        ROUND(
            (pc.total_cost - LAG(pc.total_cost) OVER (ORDER BY pc.cost_date)) /
            NULLIF(LAG(pc.total_cost) OVER (ORDER BY pc.cost_date), 0) * 100,
            2
        ) AS cost_change_pct,
        ROUND(AVG(pc.total_cost) OVER (
            ORDER BY pc.cost_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS running_avg_7d
    FROM platform_pipeline_costs pc
    WHERE pc.pipeline_id = p_pipeline_id
    AND pc.cost_date >= CURRENT_DATE - p_days
    ORDER BY pc.cost_date;
END;
$$ LANGUAGE plpgsql;

-- Function: Register Transformation Rule (NLP-aware)
CREATE OR REPLACE FUNCTION register_transformation_rule(
    p_pipeline_id UUID,
    p_rule_name VARCHAR,
    p_layer VARCHAR,
    p_rule_type VARCHAR,
    p_input_format VARCHAR,
    p_source_columns JSONB,
    p_target_column VARCHAR,
    p_structured_config JSONB DEFAULT NULL,
    p_sql_expression TEXT DEFAULT NULL,
    p_nl_description TEXT DEFAULT NULL,
    p_created_by VARCHAR DEFAULT 'system'
)
RETURNS UUID AS $$
DECLARE
    v_rule_id UUID;
    v_next_order INTEGER;
BEGIN
    -- Get next execution order for this layer
    SELECT COALESCE(MAX(execution_order), 0) + 1 INTO v_next_order
    FROM platform_transformation_rules
    WHERE pipeline_id = p_pipeline_id AND layer = p_layer;

    -- Insert the rule
    INSERT INTO platform_transformation_rules (
        pipeline_id, rule_name, layer, execution_order, rule_type,
        input_format, source_columns, target_column,
        structured_config, sql_expression, nl_description, created_by
    ) VALUES (
        p_pipeline_id, p_rule_name, p_layer, v_next_order, p_rule_type,
        p_input_format, p_source_columns, p_target_column,
        p_structured_config, p_sql_expression, p_nl_description, p_created_by
    )
    RETURNING rule_id INTO v_rule_id;

    RETURN v_rule_id;
END;
$$ LANGUAGE plpgsql;

-- Function: Create Approval Request
CREATE OR REPLACE FUNCTION create_approval_request(
    p_pipeline_id UUID,
    p_request_type VARCHAR,
    p_description TEXT,
    p_requested_by VARCHAR,
    p_approval_chain JSONB
)
RETURNS UUID AS $$
DECLARE
    v_approval_id UUID;
    v_total_steps INTEGER;
BEGIN
    -- Calculate total approval steps
    v_total_steps := jsonb_array_length(p_approval_chain);

    INSERT INTO platform_approval_requests (
        pipeline_id, request_type, request_description,
        requested_by, approval_chain, total_steps
    ) VALUES (
        p_pipeline_id, p_request_type, p_description,
        p_requested_by, p_approval_chain, v_total_steps
    )
    RETURNING approval_id INTO v_approval_id;

    RETURN v_approval_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- METADATA INSERT TEMPLATES (for DAG generator)
-- =============================================================================

-- These are template patterns - actual values come from Jinja2 templates
COMMENT ON TABLE platform_pipeline_registry IS 'INSERT: When new pipeline onboarded. UPDATE: When config changes.';
COMMENT ON TABLE platform_pipeline_sources IS 'INSERT: With new pipeline. UPDATE: When source changes.';
COMMENT ON TABLE platform_pipeline_schemas IS 'INSERT: Each schema version. UPDATE: Set is_current=false on new version.';
COMMENT ON TABLE platform_transformation_rules IS 'INSERT: Each rule defined. UPDATE: When rule logic changes.';
COMMENT ON TABLE platform_data_contracts IS 'INSERT: When SLA defined. UPDATE: When thresholds change.';
COMMENT ON TABLE platform_schema_changes IS 'INSERT: Every schema modification. NEVER UPDATE.';
COMMENT ON TABLE platform_pipeline_costs IS 'INSERT: Daily cost aggregation. NEVER UPDATE.';
COMMENT ON TABLE platform_pipeline_metrics IS 'INSERT: After each execution. NEVER UPDATE.';
