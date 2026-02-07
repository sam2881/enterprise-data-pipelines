-- ═══════════════════════════════════════════════════════════════════════════
-- APEX DATA AGENT - VALIDATION AND QUALITY TABLES
-- Part 4: Validation Rules, Quality Expectations
-- ═══════════════════════════════════════════════════════════════════════════

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 1. VALIDATION RULE                                                       │
-- │    Schema, semantic, and quality validation rules                        │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS validation_rule (
    validation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id UUID NOT NULL REFERENCES data_contract(contract_id),
    zone_level VARCHAR(20) NOT NULL,  -- BRONZE, SILVER, GOLD
    validation_type VARCHAR(20) NOT NULL,  -- SCHEMA, SEMANTIC, QUALITY, REFERENTIAL, BUSINESS

    rule_name VARCHAR(200) NOT NULL,
    rule_expression TEXT NOT NULL,  -- SQL expression that returns boolean
    rule_description TEXT,

    -- Severity and thresholds
    severity VARCHAR(20) NOT NULL DEFAULT 'WARNING',  -- INFO, WARNING, ERROR, CRITICAL
    threshold_pct DECIMAL(5,2) DEFAULT 100.0,  -- Acceptable pass percentage
    is_blocking BOOLEAN DEFAULT false,  -- Block pipeline on failure?

    -- For column-specific validations
    column_name VARCHAR(200),

    -- Error handling
    error_action VARCHAR(50) DEFAULT 'LOG',  -- LOG, QUARANTINE, REJECT, FAIL

    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (contract_id, zone_level, rule_name)
);

CREATE INDEX idx_validation_contract ON validation_rule(contract_id);
CREATE INDEX idx_validation_zone ON validation_rule(zone_level);
CREATE INDEX idx_validation_type ON validation_rule(validation_type);
CREATE INDEX idx_validation_severity ON validation_rule(severity);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 2. QUALITY EXPECTATION                                                   │
-- │    Great Expectations style quality rules                                │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS quality_expectation (
    expectation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id UUID NOT NULL REFERENCES data_contract(contract_id),
    zone_level VARCHAR(20) NOT NULL,

    -- Expectation definition
    expectation_type VARCHAR(100) NOT NULL,  -- expect_column_values_to_not_be_null, etc.
    column_name VARCHAR(200),
    kwargs JSONB DEFAULT '{}',  -- Additional parameters

    -- Metadata
    meta JSONB DEFAULT '{}',  -- Metadata like notes, author

    -- Thresholds
    mostly DECIMAL(5,4) DEFAULT 1.0,  -- 0.95 means 95% must pass
    result_format VARCHAR(50) DEFAULT 'SUMMARY',  -- SUMMARY, COMPLETE, BOOLEAN_ONLY

    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_expectation_contract ON quality_expectation(contract_id);
CREATE INDEX idx_expectation_zone ON quality_expectation(zone_level);
CREATE INDEX idx_expectation_type ON quality_expectation(expectation_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 3. SLA DEFINITION                                                        │
-- │    Service Level Agreement definitions                                   │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS sla_definition (
    sla_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_id UUID NOT NULL REFERENCES feed(feed_id),
    sla_type VARCHAR(50) NOT NULL,  -- COMPLETION_TIME, DATA_FRESHNESS, QUALITY
    target_value INTEGER NOT NULL,  -- Minutes for time, percentage for quality
    warning_threshold INTEGER,
    critical_threshold INTEGER,
    notification_channels JSONB,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sla_feed ON sla_definition(feed_id);
CREATE INDEX idx_sla_type ON sla_definition(sla_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 4. PIPELINE DEPENDENCY                                                   │
-- │    Upstream/downstream pipeline dependencies                             │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS pipeline_dependency (
    dependency_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    downstream_feed_id UUID NOT NULL REFERENCES feed(feed_id),
    upstream_feed_id UUID NOT NULL REFERENCES feed(feed_id),
    dependency_type VARCHAR(50) NOT NULL,  -- HARD, SOFT, BEST_EFFORT
    required_status VARCHAR(20) DEFAULT 'SUCCESS',  -- SUCCESS, NO_FILE
    lookback_hours INTEGER DEFAULT 24,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (downstream_feed_id, upstream_feed_id)
);

CREATE INDEX idx_dependency_downstream ON pipeline_dependency(downstream_feed_id);
CREATE INDEX idx_dependency_upstream ON pipeline_dependency(upstream_feed_id);
