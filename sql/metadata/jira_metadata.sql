-- =============================================================================
-- JIRA METADATA TABLES
-- Purpose: Track Jira ticket ingestion and pipeline lifecycle management
-- Supports: New Pipeline, Migration, Enhancement ticket types
-- =============================================================================

-- =============================================================================
-- A. JIRA TICKETS REGISTRY
-- Purpose: Central registry for all Jira tickets that trigger pipeline creation
-- =============================================================================

CREATE TABLE IF NOT EXISTS jira_tickets (
    jira_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Ticket Identity
    ticket_id VARCHAR(50) NOT NULL UNIQUE,  -- e.g., DATA-1234
    ticket_type VARCHAR(50) NOT NULL,       -- new_pipeline, migration, enhancement, bug_fix, data_request

    -- Ticket Details
    summary VARCHAR(500) NOT NULL,
    detailed_description TEXT,

    -- Source & Target
    source_systems JSONB DEFAULT '[]',      -- ['SAP', 'Salesforce', 'Oracle']
    target_domain VARCHAR(100) NOT NULL,    -- e.g., 'sales', 'finance', 'hr'
    target_subdomain VARCHAR(100),          -- e.g., 'customers', 'orders'

    -- Priority & Status
    priority VARCHAR(20) NOT NULL DEFAULT 'medium',  -- low, medium, high, critical
    status VARCHAR(50) NOT NULL DEFAULT 'open',      -- open, in_progress, blocked, completed, cancelled

    -- Jira Metadata
    jira_project VARCHAR(50),
    jira_epic VARCHAR(50),
    jira_labels JSONB DEFAULT '[]',
    jira_components JSONB DEFAULT '[]',

    -- Linked Pipelines
    pipeline_ids JSONB DEFAULT '[]',        -- UUIDs of pipelines created from this ticket

    -- Migration Specific
    is_migration BOOLEAN DEFAULT FALSE,
    migration_scope JSONB DEFAULT NULL,
    /* migration_scope example:
    {
        "legacy_system": "SSIS",
        "package_count": 15,
        "dtsx_paths": ["pkg1.dtsx", "pkg2.dtsx"],
        "target_platform": "Airflow+Dataproc",
        "estimated_complexity": "high"
    }
    */

    -- Gold Zone Modeling Preferences
    gold_modeling_strategy VARCHAR(50),     -- data_vault_2, star_schema, snowflake_schema, flat_table
    gold_zone_config JSONB DEFAULT NULL,
    /* gold_zone_config example:
    {
        "modeling_strategy": "star_schema",
        "fact_table": { "table_name": "fact_sales", "grain_columns": ["customer_id", "date_key"] },
        "dimensions": ["dim_customer", "dim_product", "dim_date"]
    }
    */

    -- SLA & Timeline
    requested_date DATE,
    due_date DATE,
    sla_target_hours INTEGER,

    -- Ownership
    reporter VARCHAR(255),
    assignee VARCHAR(255),
    team VARCHAR(100),

    -- Approval Status
    requires_approval BOOLEAN DEFAULT FALSE,
    approval_status VARCHAR(20) DEFAULT 'not_required',  -- not_required, pending, approved, rejected
    approved_by VARCHAR(255),
    approved_at TIMESTAMP WITH TIME ZONE,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,

    -- External Links
    confluence_link VARCHAR(500),
    github_pr_link VARCHAR(500),

    CONSTRAINT valid_priority CHECK (priority IN ('low', 'medium', 'high', 'critical')),
    CONSTRAINT valid_status CHECK (status IN ('open', 'in_progress', 'blocked', 'completed', 'cancelled', 'on_hold'))
);

CREATE INDEX idx_jira_ticket_id ON jira_tickets(ticket_id);
CREATE INDEX idx_jira_status ON jira_tickets(status);
CREATE INDEX idx_jira_type ON jira_tickets(ticket_type);
CREATE INDEX idx_jira_domain ON jira_tickets(target_domain);
CREATE INDEX idx_jira_assignee ON jira_tickets(assignee);
CREATE INDEX idx_jira_priority ON jira_tickets(priority);

-- =============================================================================
-- B. JIRA TICKET REQUIREMENTS
-- Purpose: Track individual requirements/user stories within a ticket
-- =============================================================================

CREATE TABLE IF NOT EXISTS jira_ticket_requirements (
    requirement_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    jira_id UUID NOT NULL REFERENCES jira_tickets(jira_id) ON DELETE CASCADE,

    -- Requirement Details
    requirement_type VARCHAR(50) NOT NULL,  -- source_extraction, transformation, quality_rule, sla, schema_change
    requirement_description TEXT NOT NULL,

    -- Technical Details
    affected_columns JSONB DEFAULT '[]',    -- Columns this requirement affects
    transformation_config JSONB DEFAULT '{}',

    -- Input Mode (how requirement was captured)
    input_mode VARCHAR(20) DEFAULT 'structured',  -- structured, natural_language, sql_paste, yaml
    original_input TEXT,                    -- Original NL or SQL text
    parsed_config JSONB DEFAULT '{}',       -- Structured config after parsing

    -- Status
    status VARCHAR(20) DEFAULT 'pending',   -- pending, implemented, validated, rejected
    implemented_in_pipeline_id UUID,

    -- Ordering
    execution_order INTEGER DEFAULT 1,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_requirement_jira ON jira_ticket_requirements(jira_id);
CREATE INDEX idx_requirement_type ON jira_ticket_requirements(requirement_type);

-- =============================================================================
-- C. JIRA TICKET EVENTS
-- Purpose: Audit log of all events on a Jira ticket
-- =============================================================================

CREATE TABLE IF NOT EXISTS jira_ticket_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    jira_id UUID NOT NULL REFERENCES jira_tickets(jira_id) ON DELETE CASCADE,

    -- Event Details
    event_type VARCHAR(50) NOT NULL,        -- created, updated, status_changed, pipeline_created, approved, rejected
    event_description TEXT,

    -- Before/After State
    previous_value JSONB DEFAULT '{}',
    new_value JSONB DEFAULT '{}',

    -- Related Entities
    pipeline_id UUID,
    requirement_id UUID,

    -- Actor
    performed_by VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_ticket_event_jira ON jira_ticket_events(jira_id);
CREATE INDEX idx_ticket_event_type ON jira_ticket_events(event_type);
CREATE INDEX idx_ticket_event_date ON jira_ticket_events(created_at);

-- =============================================================================
-- D. JIRA TO PIPELINE MAPPING
-- Purpose: Explicit many-to-many relationship between Jira tickets and pipelines
-- =============================================================================

CREATE TABLE IF NOT EXISTS jira_pipeline_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    jira_id UUID NOT NULL REFERENCES jira_tickets(jira_id) ON DELETE CASCADE,
    pipeline_id UUID NOT NULL REFERENCES pipeline_registry(pipeline_id) ON DELETE CASCADE,

    -- Relationship Type
    relationship_type VARCHAR(50) DEFAULT 'created_from',  -- created_from, modified_by, referenced_in

    -- Status
    is_primary BOOLEAN DEFAULT TRUE,        -- Is this the primary ticket for the pipeline?

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(255),

    CONSTRAINT unique_jira_pipeline UNIQUE (jira_id, pipeline_id)
);

CREATE INDEX idx_mapping_jira ON jira_pipeline_mapping(jira_id);
CREATE INDEX idx_mapping_pipeline ON jira_pipeline_mapping(pipeline_id);

-- =============================================================================
-- E. JOIN DEPENDENCIES (for Gold Zone Modeling)
-- Purpose: Track join dependencies for pipelines, including future/placeholder tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS join_dependencies (
    dependency_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES pipeline_registry(pipeline_id) ON DELETE CASCADE,
    jira_id UUID REFERENCES jira_tickets(jira_id),

    -- Join Configuration
    join_name VARCHAR(255) NOT NULL,
    join_type VARCHAR(20) NOT NULL,         -- inner, left, right, full, cross

    -- Left Side (the pipeline's own table)
    left_entity VARCHAR(255) NOT NULL,
    left_keys JSONB NOT NULL DEFAULT '[]',  -- ['customer_id', 'date_key']

    -- Right Side (existing or future table)
    right_entity_name VARCHAR(255) NOT NULL,
    right_keys JSONB NOT NULL DEFAULT '[]',

    -- Right Entity Status
    right_entity_exists BOOLEAN DEFAULT TRUE,
    right_entity_pipeline_id UUID,          -- If existing, reference to pipeline
    expected_pipeline_ticket VARCHAR(50),   -- If future, the Jira ticket expected to create it
    placeholder_contract BOOLEAN DEFAULT FALSE,  -- Is this a placeholder waiting for another pipeline?

    -- Join Metadata
    expected_grain VARCHAR(255),            -- e.g., 'customer_id + date'
    cardinality VARCHAR(10) DEFAULT '1-N',  -- 1-1, 1-N, N-1, N-N

    -- Validation
    is_validated BOOLEAN DEFAULT FALSE,
    validation_errors JSONB DEFAULT '[]',
    last_validated_at TIMESTAMP WITH TIME ZONE,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT valid_join_type CHECK (join_type IN ('inner', 'left', 'right', 'full', 'cross')),
    CONSTRAINT valid_cardinality CHECK (cardinality IN ('1-1', '1-N', 'N-1', 'N-N'))
);

CREATE INDEX idx_join_dep_pipeline ON join_dependencies(pipeline_id);
CREATE INDEX idx_join_dep_jira ON join_dependencies(jira_id);
CREATE INDEX idx_join_dep_right_entity ON join_dependencies(right_entity_name);

-- =============================================================================
-- JIRA VIEWS
-- =============================================================================

-- View: Jira Ticket Dashboard
CREATE OR REPLACE VIEW v_jira_dashboard AS
SELECT
    jt.ticket_id,
    jt.ticket_type,
    jt.summary,
    jt.target_domain,
    jt.priority,
    jt.status,
    jt.assignee,
    jt.team,
    jt.due_date,
    jt.is_migration,
    jt.gold_modeling_strategy,

    -- Pipeline Count
    jsonb_array_length(jt.pipeline_ids) AS pipeline_count,

    -- Migration Details
    CASE WHEN jt.is_migration THEN jt.migration_scope->>'legacy_system' END AS legacy_system,
    CASE WHEN jt.is_migration THEN (jt.migration_scope->>'package_count')::int END AS package_count,

    -- SLA Status
    CASE
        WHEN jt.status = 'completed' THEN 'completed'
        WHEN jt.due_date < CURRENT_DATE THEN 'overdue'
        WHEN jt.due_date <= CURRENT_DATE + INTERVAL '2 days' THEN 'at_risk'
        ELSE 'on_track'
    END AS sla_status,

    -- Days Open
    EXTRACT(DAY FROM NOW() - jt.created_at)::int AS days_open,

    jt.created_at,
    jt.updated_at
FROM jira_tickets jt
ORDER BY
    CASE jt.priority
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'medium' THEN 3
        WHEN 'low' THEN 4
    END,
    jt.due_date NULLS LAST;

-- View: Jira to Pipeline Lineage
CREATE OR REPLACE VIEW v_jira_pipeline_lineage AS
SELECT
    jt.ticket_id,
    jt.summary AS ticket_summary,
    jt.ticket_type,
    jt.target_domain,
    pr.dag_id,
    pr.pipeline_name,
    pr.environment,
    pr.status AS pipeline_status,
    jpm.relationship_type,
    jpm.is_primary,
    jpm.created_at AS mapping_created_at
FROM jira_tickets jt
JOIN jira_pipeline_mapping jpm ON jt.jira_id = jpm.jira_id
JOIN pipeline_registry pr ON jpm.pipeline_id = pr.pipeline_id
ORDER BY jt.ticket_id, pr.dag_id;

-- View: Pending Join Dependencies
CREATE OR REPLACE VIEW v_pending_join_dependencies AS
SELECT
    jd.dependency_id,
    jd.join_name,
    pr.dag_id AS pipeline_dag_id,
    pr.pipeline_name,
    jd.left_entity,
    jd.right_entity_name,
    jd.join_type,
    jd.cardinality,
    jd.expected_pipeline_ticket,
    jd.placeholder_contract,
    jt.status AS expected_ticket_status,
    CASE
        WHEN jd.right_entity_exists THEN 'resolved'
        WHEN jd.placeholder_contract AND jt.status IN ('completed', 'in_progress') THEN 'in_progress'
        ELSE 'blocked'
    END AS resolution_status
FROM join_dependencies jd
JOIN pipeline_registry pr ON jd.pipeline_id = pr.pipeline_id
LEFT JOIN jira_tickets jt ON jt.ticket_id = jd.expected_pipeline_ticket
WHERE jd.placeholder_contract = TRUE
ORDER BY jd.created_at;

-- =============================================================================
-- JIRA FUNCTIONS
-- =============================================================================

-- Function: Register Jira Ticket
CREATE OR REPLACE FUNCTION register_jira_ticket(
    p_ticket_id VARCHAR,
    p_ticket_type VARCHAR,
    p_summary VARCHAR,
    p_target_domain VARCHAR,
    p_priority VARCHAR DEFAULT 'medium',
    p_reporter VARCHAR DEFAULT NULL,
    p_assignee VARCHAR DEFAULT NULL,
    p_team VARCHAR DEFAULT NULL,
    p_source_systems JSONB DEFAULT '[]',
    p_detailed_description TEXT DEFAULT NULL,
    p_is_migration BOOLEAN DEFAULT FALSE,
    p_migration_scope JSONB DEFAULT NULL,
    p_gold_modeling_strategy VARCHAR DEFAULT NULL,
    p_gold_zone_config JSONB DEFAULT NULL
)
RETURNS UUID AS $$
DECLARE
    v_jira_id UUID;
BEGIN
    INSERT INTO jira_tickets (
        ticket_id, ticket_type, summary, target_domain,
        priority, reporter, assignee, team,
        source_systems, detailed_description,
        is_migration, migration_scope,
        gold_modeling_strategy, gold_zone_config
    ) VALUES (
        p_ticket_id, p_ticket_type, p_summary, p_target_domain,
        p_priority, p_reporter, p_assignee, p_team,
        p_source_systems, p_detailed_description,
        p_is_migration, p_migration_scope,
        p_gold_modeling_strategy, p_gold_zone_config
    )
    ON CONFLICT (ticket_id) DO UPDATE SET
        summary = EXCLUDED.summary,
        detailed_description = EXCLUDED.detailed_description,
        priority = EXCLUDED.priority,
        assignee = EXCLUDED.assignee,
        source_systems = EXCLUDED.source_systems,
        migration_scope = EXCLUDED.migration_scope,
        gold_zone_config = EXCLUDED.gold_zone_config,
        updated_at = NOW()
    RETURNING jira_id INTO v_jira_id;

    -- Log event
    INSERT INTO jira_ticket_events (jira_id, event_type, event_description, performed_by)
    VALUES (v_jira_id, 'created', 'Jira ticket registered in platform', p_reporter);

    RETURN v_jira_id;
END;
$$ LANGUAGE plpgsql;

-- Function: Link Pipeline to Jira Ticket
CREATE OR REPLACE FUNCTION link_pipeline_to_jira(
    p_ticket_id VARCHAR,
    p_pipeline_id UUID,
    p_relationship_type VARCHAR DEFAULT 'created_from',
    p_created_by VARCHAR DEFAULT 'system'
)
RETURNS UUID AS $$
DECLARE
    v_jira_id UUID;
    v_mapping_id UUID;
BEGIN
    -- Get Jira UUID
    SELECT jira_id INTO v_jira_id
    FROM jira_tickets
    WHERE ticket_id = p_ticket_id;

    IF v_jira_id IS NULL THEN
        RAISE EXCEPTION 'Jira ticket % not found', p_ticket_id;
    END IF;

    -- Create mapping
    INSERT INTO jira_pipeline_mapping (jira_id, pipeline_id, relationship_type, created_by)
    VALUES (v_jira_id, p_pipeline_id, p_relationship_type, p_created_by)
    ON CONFLICT (jira_id, pipeline_id) DO UPDATE SET
        relationship_type = EXCLUDED.relationship_type
    RETURNING mapping_id INTO v_mapping_id;

    -- Update pipeline_ids array in jira_tickets
    UPDATE jira_tickets
    SET pipeline_ids = pipeline_ids || to_jsonb(p_pipeline_id::text),
        updated_at = NOW()
    WHERE jira_id = v_jira_id
    AND NOT (pipeline_ids ? p_pipeline_id::text);

    -- Log event
    INSERT INTO jira_ticket_events (jira_id, event_type, event_description, pipeline_id, performed_by)
    VALUES (v_jira_id, 'pipeline_created', 'Pipeline linked to ticket', p_pipeline_id, p_created_by);

    RETURN v_mapping_id;
END;
$$ LANGUAGE plpgsql;

-- Function: Update Jira Ticket Status
CREATE OR REPLACE FUNCTION update_jira_status(
    p_ticket_id VARCHAR,
    p_new_status VARCHAR,
    p_performed_by VARCHAR DEFAULT 'system'
)
RETURNS VOID AS $$
DECLARE
    v_jira_id UUID;
    v_old_status VARCHAR;
BEGIN
    -- Get current status
    SELECT jira_id, status INTO v_jira_id, v_old_status
    FROM jira_tickets
    WHERE ticket_id = p_ticket_id;

    IF v_jira_id IS NULL THEN
        RAISE EXCEPTION 'Jira ticket % not found', p_ticket_id;
    END IF;

    -- Update status
    UPDATE jira_tickets
    SET status = p_new_status,
        updated_at = NOW(),
        completed_at = CASE WHEN p_new_status = 'completed' THEN NOW() ELSE completed_at END
    WHERE jira_id = v_jira_id;

    -- Log event
    INSERT INTO jira_ticket_events (
        jira_id, event_type, event_description,
        previous_value, new_value, performed_by
    ) VALUES (
        v_jira_id, 'status_changed', 'Ticket status updated',
        jsonb_build_object('status', v_old_status),
        jsonb_build_object('status', p_new_status),
        p_performed_by
    );
END;
$$ LANGUAGE plpgsql;

-- Function: Register Join Dependency (with placeholder support)
CREATE OR REPLACE FUNCTION register_join_dependency(
    p_pipeline_id UUID,
    p_join_name VARCHAR,
    p_join_type VARCHAR,
    p_left_entity VARCHAR,
    p_left_keys JSONB,
    p_right_entity_name VARCHAR,
    p_right_keys JSONB,
    p_cardinality VARCHAR DEFAULT '1-N',
    p_right_entity_exists BOOLEAN DEFAULT TRUE,
    p_right_entity_pipeline_id UUID DEFAULT NULL,
    p_expected_pipeline_ticket VARCHAR DEFAULT NULL,
    p_jira_id UUID DEFAULT NULL
)
RETURNS UUID AS $$
DECLARE
    v_dependency_id UUID;
    v_is_placeholder BOOLEAN;
BEGIN
    -- Determine if this is a placeholder contract
    v_is_placeholder := NOT p_right_entity_exists AND p_expected_pipeline_ticket IS NOT NULL;

    INSERT INTO join_dependencies (
        pipeline_id, jira_id, join_name, join_type,
        left_entity, left_keys, right_entity_name, right_keys,
        cardinality, right_entity_exists, right_entity_pipeline_id,
        expected_pipeline_ticket, placeholder_contract
    ) VALUES (
        p_pipeline_id, p_jira_id, p_join_name, p_join_type,
        p_left_entity, p_left_keys, p_right_entity_name, p_right_keys,
        p_cardinality, p_right_entity_exists, p_right_entity_pipeline_id,
        p_expected_pipeline_ticket, v_is_placeholder
    )
    RETURNING dependency_id INTO v_dependency_id;

    RETURN v_dependency_id;
END;
$$ LANGUAGE plpgsql;

-- Function: Resolve Placeholder Dependency (when expected pipeline is created)
CREATE OR REPLACE FUNCTION resolve_join_dependency(
    p_right_entity_name VARCHAR,
    p_right_entity_pipeline_id UUID
)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE join_dependencies
    SET right_entity_exists = TRUE,
        right_entity_pipeline_id = p_right_entity_pipeline_id,
        placeholder_contract = FALSE,
        updated_at = NOW()
    WHERE right_entity_name = p_right_entity_name
    AND placeholder_contract = TRUE;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function: Get Jira Ticket Summary for Pipeline Form
CREATE OR REPLACE FUNCTION get_jira_ticket_summary(p_ticket_id VARCHAR)
RETURNS JSONB AS $$
DECLARE
    result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'ticket_id', jt.ticket_id,
        'ticket_type', jt.ticket_type,
        'summary', jt.summary,
        'detailed_description', jt.detailed_description,
        'target_domain', jt.target_domain,
        'target_subdomain', jt.target_subdomain,
        'source_systems', jt.source_systems,
        'priority', jt.priority,
        'status', jt.status,
        'assignee', jt.assignee,
        'team', jt.team,
        'is_migration', jt.is_migration,
        'migration_scope', jt.migration_scope,
        'gold_modeling_strategy', jt.gold_modeling_strategy,
        'gold_zone_config', jt.gold_zone_config,
        'pipeline_count', jsonb_array_length(jt.pipeline_ids),
        'requirements', (
            SELECT jsonb_agg(jsonb_build_object(
                'requirement_id', r.requirement_id,
                'requirement_type', r.requirement_type,
                'requirement_description', r.requirement_description,
                'status', r.status
            ))
            FROM jira_ticket_requirements r
            WHERE r.jira_id = jt.jira_id
        )
    ) INTO result
    FROM jira_tickets jt
    WHERE jt.ticket_id = p_ticket_id;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- Trigger: Update jira_tickets.updated_at on changes
CREATE OR REPLACE FUNCTION update_jira_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jira_tickets_updated
    BEFORE UPDATE ON jira_tickets
    FOR EACH ROW
    EXECUTE FUNCTION update_jira_timestamp();

CREATE TRIGGER trg_join_dependencies_updated
    BEFORE UPDATE ON join_dependencies
    FOR EACH ROW
    EXECUTE FUNCTION update_jira_timestamp();

-- =============================================================================
-- SAMPLE DATA (for testing)
-- =============================================================================

/*
-- Example: Register a new pipeline ticket
SELECT register_jira_ticket(
    'DATA-1234',
    'new_pipeline',
    'Create customer 360 pipeline with sales data',
    'sales',
    'high',
    'john.doe@company.com',
    'jane.smith@company.com',
    'data-engineering',
    '["SAP", "Salesforce", "Oracle"]'::jsonb,
    'Build a comprehensive customer 360 view combining data from SAP, Salesforce, and Oracle.',
    FALSE,
    NULL,
    'star_schema',
    '{"fact_table": {"table_name": "fact_customer_360", "grain_columns": ["customer_id", "date_key"]}}'::jsonb
);

-- Example: Register a migration ticket
SELECT register_jira_ticket(
    'DATA-5678',
    'migration',
    'Migrate SSIS sales reporting packages to Airflow',
    'sales',
    'critical',
    'john.doe@company.com',
    'jane.smith@company.com',
    'data-engineering',
    '["SQL Server"]'::jsonb,
    'Migrate 15 SSIS packages from on-prem SQL Server to GCP.',
    TRUE,
    '{"legacy_system": "SSIS", "package_count": 15, "dtsx_paths": ["sales_daily.dtsx", "sales_weekly.dtsx"], "target_platform": "Airflow+Dataproc", "estimated_complexity": "high"}'::jsonb,
    NULL,
    NULL
);

-- Example: Link pipeline to ticket
SELECT link_pipeline_to_jira(
    'DATA-1234',
    (SELECT pipeline_id FROM pipeline_registry WHERE dag_id = 'customer_360_pipeline'),
    'created_from',
    'jane.smith@company.com'
);

-- Example: Register a join dependency with placeholder
SELECT register_join_dependency(
    (SELECT pipeline_id FROM pipeline_registry WHERE dag_id = 'customer_360_pipeline'),
    'customer_orders_join',
    'left',
    'fact_customer_360',
    '["customer_id"]'::jsonb,
    'dim_product',
    '["product_id"]'::jsonb,
    '1-N',
    FALSE,
    NULL,
    'DATA-9999',  -- Expected ticket that will create dim_product
    (SELECT jira_id FROM jira_tickets WHERE ticket_id = 'DATA-1234')
);
*/
