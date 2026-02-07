-- ═══════════════════════════════════════════════════════════════════════════
-- APEX DATA AGENT - CORE METADATA TABLES
-- Part 2: Source Registry, Domain Registry, Feed Group, Feed
-- ═══════════════════════════════════════════════════════════════════════════

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 1. CONNECTION REGISTRY                                                   │
-- │    Stores database/API connection configurations                         │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS connection_registry (
    connection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_code VARCHAR(100) NOT NULL UNIQUE,
    connection_name VARCHAR(200) NOT NULL,
    connection_type VARCHAR(50) NOT NULL,  -- JDBC, REST, KAFKA, GCS, S3
    host VARCHAR(500),
    port INTEGER,
    database_name VARCHAR(200),
    schema_name VARCHAR(200),
    credentials_secret_path VARCHAR(500),  -- Path to secret in Vault/Secret Manager
    extra_config JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_connection_code ON connection_registry(connection_code);
CREATE INDEX idx_connection_type ON connection_registry(connection_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 2. DOMAIN REGISTRY                                                       │
-- │    Business domains for data organization                                │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS domain_registry (
    domain_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    domain_code VARCHAR(50) NOT NULL UNIQUE,
    domain_name VARCHAR(200) NOT NULL,
    domain_description TEXT,
    owner_email VARCHAR(200),
    steward_email VARCHAR(200),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_domain_code ON domain_registry(domain_code);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 3. SOURCE REGISTRY                                                       │
-- │    External data sources (vendors, systems)                              │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS source_registry (
    source_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_code VARCHAR(100) NOT NULL UNIQUE,
    source_name VARCHAR(200) NOT NULL,
    source_type VARCHAR(50) NOT NULL,  -- FILE, DATABASE, API, KAFKA, STREAMING, LEGACY, SAAS
    connection_id UUID REFERENCES connection_registry(connection_id),
    domain_id UUID REFERENCES domain_registry(domain_id),
    business_unit VARCHAR(200),
    owner_email VARCHAR(200),
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_source_code ON source_registry(source_code);
CREATE INDEX idx_source_type ON source_registry(source_type);
CREATE INDEX idx_source_domain ON source_registry(domain_id);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 4. DAG TEMPLATE                                                          │
-- │    Jinja2 DAG templates for 9 canonical patterns                         │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS dag_template (
    template_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    template_code VARCHAR(100) NOT NULL UNIQUE,
    template_name VARCHAR(200) NOT NULL,
    template_type VARCHAR(20) NOT NULL,  -- BATCH, STREAMING, HYBRID, CDC
    pattern_id VARCHAR(10) NOT NULL,  -- P01, P02, P03, etc.
    short_description TEXT,
    detailed_description TEXT,
    use_cases TEXT[],
    source_types_supported TEXT[],
    load_types_supported TEXT[],
    target_models_supported TEXT[],
    supports_streaming BOOLEAN DEFAULT false,
    supports_cdc BOOLEAN DEFAULT false,
    supports_scd BOOLEAN DEFAULT false,
    supports_data_vault BOOLEAN DEFAULT false,
    supports_star_schema BOOLEAN DEFAULT false,
    supports_large_files BOOLEAN DEFAULT false,
    supports_legacy_migration BOOLEAN DEFAULT false,
    required_metadata_tables TEXT[],
    required_spark_jobs TEXT[],
    matching_keywords TEXT[],
    exclusion_keywords TEXT[],
    default_spark_config JSONB DEFAULT '{}',
    jinja_template TEXT NOT NULL,
    template_variables JSONB DEFAULT '{}',
    task_groups TEXT[],
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_template_code ON dag_template(template_code);
CREATE INDEX idx_template_pattern ON dag_template(pattern_id);
CREATE INDEX idx_template_type ON dag_template(template_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 5. FEED GROUP                                                            │
-- │    Logical grouping of feeds from same source                            │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS feed_group (
    feed_group_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID NOT NULL REFERENCES source_registry(source_id),
    template_id UUID REFERENCES dag_template(template_id),
    feed_group_code VARCHAR(100) NOT NULL UNIQUE,
    feed_group_name VARCHAR(200) NOT NULL,
    feed_group_type VARCHAR(50) NOT NULL,  -- FILE, DATABASE, API, STREAMING
    notification_email VARCHAR(500),
    table_load_setting JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_feed_group_code ON feed_group(feed_group_code);
CREATE INDEX idx_feed_group_source ON feed_group(source_id);
CREATE INDEX idx_feed_group_template ON feed_group(template_id);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 6. FEED                                                                  │
-- │    Individual data feed/pipeline definition                              │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS feed (
    feed_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_group_id UUID NOT NULL REFERENCES feed_group(feed_group_id),
    feed_code VARCHAR(100) NOT NULL UNIQUE,
    feed_name VARCHAR(200) NOT NULL,
    feed_type VARCHAR(20) NOT NULL,  -- BATCH, STREAMING, HYBRID
    schedule_cron VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    start_date DATE NOT NULL,
    end_date DATE,
    owner VARCHAR(200),
    description TEXT,
    jira_ticket VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_feed_code ON feed(feed_code);
CREATE INDEX idx_feed_group ON feed(feed_group_id);
CREATE INDEX idx_feed_schedule ON feed(schedule_cron);
CREATE INDEX idx_feed_active ON feed(is_active);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 7. SPARK CONFIG                                                          │
-- │    Spark job configurations per feed group                               │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS spark_config (
    spark_config_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_group_id UUID NOT NULL REFERENCES feed_group(feed_group_id),
    config_name VARCHAR(100) DEFAULT 'default',
    executor_instances INTEGER DEFAULT 4,
    executor_memory VARCHAR(20) DEFAULT '4g',
    executor_cores INTEGER DEFAULT 2,
    driver_memory VARCHAR(20) DEFAULT '2g',
    shuffle_partitions INTEGER DEFAULT 200,
    adaptive_enabled BOOLEAN DEFAULT true,
    extra_conf JSONB DEFAULT '{}',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (feed_group_id, config_name)
);

CREATE INDEX idx_spark_config_feed_group ON spark_config(feed_group_id);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 8. NOTIFICATION CONFIG                                                   │
-- │    Notification settings for pipeline alerts                             │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS notification_config (
    notification_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_id UUID REFERENCES feed(feed_id),
    feed_group_id UUID REFERENCES feed_group(feed_group_id),
    notification_type VARCHAR(50) NOT NULL,  -- EMAIL, SLACK, PAGERDUTY, TEAMS
    recipients JSONB NOT NULL,  -- List of email addresses or channel IDs
    on_success BOOLEAN DEFAULT false,
    on_failure BOOLEAN DEFAULT true,
    on_sla_breach BOOLEAN DEFAULT true,
    message_template TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_notification_feed ON notification_config(feed_id);
CREATE INDEX idx_notification_feed_group ON notification_config(feed_group_id);

-- ═══════════════════════════════════════════════════════════════════════════
-- 9. watermark_tracking: Incremental load watermark state
-- ═══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS watermark_tracking (
    watermark_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL REFERENCES feed(feed_id),
    contract_id VARCHAR(100) NOT NULL,
    column_name VARCHAR(200) NOT NULL,
    column_type VARCHAR(50) NOT NULL DEFAULT 'TIMESTAMP',
    last_value TEXT NOT NULL,
    last_execution_id VARCHAR(100) NOT NULL,
    last_execution_date TIMESTAMP NOT NULL,
    row_count BIGINT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(feed_id, contract_id, column_name)
);

CREATE INDEX idx_watermark_feed ON watermark_tracking(feed_id);
CREATE INDEX idx_watermark_contract ON watermark_tracking(contract_id);
