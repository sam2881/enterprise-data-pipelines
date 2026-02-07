-- ═══════════════════════════════════════════════════════════════════════════
-- APEX DATA AGENT - DATA CONTRACT AND SCHEMA TABLES
-- Part 3: Data Contract, Schema Version, View Definition
-- ═══════════════════════════════════════════════════════════════════════════

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 1. DATA CONTRACT                                                         │
-- │    Defines data expectations and paths for each feed                     │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS data_contract (
    contract_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    feed_id UUID NOT NULL REFERENCES feed(feed_id),
    contract_type VARCHAR(20) NOT NULL,  -- FILE, TABLE, API, STREAM

    -- File-specific fields
    file_pattern VARCHAR(500),
    file_format VARCHAR(50),  -- CSV, JSON, PARQUET, AVRO, XML, FIXED_WIDTH, EBCDIC

    -- Path configurations
    source_path VARCHAR(1000),
    raw_path VARCHAR(1000),
    transient_path VARCHAR(1000),
    rejected_path VARCHAR(1000),
    bronze_path VARCHAR(1000),
    silver_path VARCHAR(1000),
    gold_path VARCHAR(1000),

    -- Processing configuration
    ingestion_freq VARCHAR(100),
    load_type VARCHAR(20) NOT NULL,  -- FULL, INCREMENTAL, APPEND, CDC, WATERMARK, MERGE
    watermark_column VARCHAR(100),
    watermark_value VARCHAR(200),

    -- Error handling
    soft_fail BOOLEAN DEFAULT false,
    timeout_minutes INTEGER DEFAULT 120,
    poke_interval_sec INTEGER DEFAULT 60,

    -- File metadata
    is_compressed BOOLEAN DEFAULT false,
    compression_type VARCHAR(20),  -- GZIP, SNAPPY, LZ4, ZSTD
    is_encrypted BOOLEAN DEFAULT false,
    encryption_type VARCHAR(50),

    -- Target tables
    bronze_table VARCHAR(200),
    silver_table VARCHAR(200),
    gold_table VARCHAR(200),

    -- Keys
    primary_keys TEXT[],
    partition_columns TEXT[],
    clustering_columns TEXT[],

    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_contract_feed ON data_contract(feed_id);
CREATE INDEX idx_contract_type ON data_contract(contract_type);
CREATE INDEX idx_contract_load_type ON data_contract(load_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 2. SCHEMA VERSION                                                        │
-- │    Versioned schema definitions for each contract                        │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS schema_version (
    schema_version_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id UUID NOT NULL REFERENCES data_contract(contract_id),
    version_number INTEGER NOT NULL,

    -- Schema definition (JSON array of column definitions)
    schema_json JSONB NOT NULL,

    -- File format details
    record_length INTEGER,  -- For fixed-width files
    row_delimiter VARCHAR(20) DEFAULT '\n',
    col_delimiter VARCHAR(20) DEFAULT ',',
    quote_char VARCHAR(5) DEFAULT '"',
    escape_char VARCHAR(5) DEFAULT '\\',
    header_rows INTEGER DEFAULT 1,
    footer_rows INTEGER DEFAULT 0,
    encoding VARCHAR(50) DEFAULT 'UTF-8',

    -- For EBCDIC/COBOL
    copybook_content TEXT,
    code_page VARCHAR(20),

    -- Version control
    is_current BOOLEAN DEFAULT true,
    effective_from DATE NOT NULL,
    effective_to DATE,
    change_reason TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (contract_id, version_number)
);

CREATE INDEX idx_schema_contract ON schema_version(contract_id);
CREATE INDEX idx_schema_current ON schema_version(is_current);
CREATE INDEX idx_schema_version ON schema_version(contract_id, version_number);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 3. VIEW DEFINITION                                                       │
-- │    SQL view definitions for zone transitions                             │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS view_definition (
    view_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id UUID NOT NULL REFERENCES data_contract(contract_id),
    zone_level VARCHAR(20) NOT NULL,  -- BRONZE, SILVER, GOLD
    view_name VARCHAR(200) NOT NULL,
    view_sql TEXT NOT NULL,

    -- View configuration
    materialized BOOLEAN DEFAULT false,
    refresh_mode VARCHAR(20) DEFAULT 'FULL',  -- FULL, INCREMENTAL, STREAMING

    -- Dependencies
    dependencies JSONB DEFAULT '[]',  -- List of upstream tables/views

    -- Output schema (optional, for validation)
    output_schema JSONB,

    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (contract_id, zone_level, view_name)
);

CREATE INDEX idx_view_contract ON view_definition(contract_id);
CREATE INDEX idx_view_zone ON view_definition(zone_level);
CREATE INDEX idx_view_name ON view_definition(view_name);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 4. TRANSFORMATION RULE                                                   │
-- │    Reusable transformation rules for Silver/Gold zones                   │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS transformation_rule (
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_code VARCHAR(100) NOT NULL UNIQUE,
    rule_name VARCHAR(200) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,  -- CLEANSING, DERIVATION, AGGREGATION, LOOKUP, SCD2

    -- Rule definition
    source_columns TEXT[],
    target_column VARCHAR(200),
    transformation_expression TEXT NOT NULL,  -- SQL expression

    -- For lookup rules
    lookup_table VARCHAR(200),
    lookup_join_condition TEXT,

    -- For SCD rules
    scd_type INTEGER,  -- 1, 2, 3
    tracked_columns TEXT[],

    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transform_rule_code ON transformation_rule(rule_code);
CREATE INDEX idx_transform_rule_type ON transformation_rule(rule_type);

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 5. CONTRACT TRANSFORMATION (Junction table)                              │
-- │    Links contracts to transformation rules                               │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS contract_transformation (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id UUID NOT NULL REFERENCES data_contract(contract_id),
    rule_id UUID NOT NULL REFERENCES transformation_rule(rule_id),
    zone_level VARCHAR(20) NOT NULL,  -- SILVER or GOLD
    execution_order INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (contract_id, rule_id, zone_level)
);

CREATE INDEX idx_contract_transform_contract ON contract_transformation(contract_id);
CREATE INDEX idx_contract_transform_rule ON contract_transformation(rule_id);
