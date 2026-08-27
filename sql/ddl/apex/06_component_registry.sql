-- ═══════════════════════════════════════════════════════════════════════════
-- APEX DATA AGENT - COMPONENT REGISTRY
-- Part 6: Template Registry, Utility Registry, Component Catalog
--
-- PURPOSE: Single source of truth for all APEX components.
-- The agent checks this registry before generating ANY code.
-- If a component exists here → reuse it.  If not → generate + register.
--
-- FLOW:
-- 1. Agent receives request
-- 2. Queries template_registry → finds matching pattern template
-- 3. Queries utility_registry → finds available dag_utilities functions
-- 4. Generates only what's missing
-- 5. Registers new components back into registry
-- ═══════════════════════════════════════════════════════════════════════════


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 1. TEMPLATE REGISTRY                                                    │
-- │    Catalogs every Jinja2 template: DAG patterns, Spark jobs, SQL        │
-- │    The agent queries this to select the right template                  │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS template_registry (
    template_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identity
    template_code VARCHAR(50) NOT NULL UNIQUE,          -- e.g., "P01", "P02", "SPARK_RAW_TO_BRONZE"
    template_name VARCHAR(200) NOT NULL,                -- Human-readable name
    template_type VARCHAR(30) NOT NULL,                 -- DAG_PATTERN, SPARK_JOB, SQL_VIEW, SQL_DDL

    -- File location
    template_path VARCHAR(500) NOT NULL,                -- Relative path from templates/
    file_hash VARCHAR(64),                              -- SHA-256 of file contents (change detection)

    -- Pattern classification (for DAG_PATTERN type)
    pattern_code VARCHAR(10),                           -- P01..P09 (NULL for non-DAG templates)
    source_types VARCHAR(50)[],                         -- Which source types this template handles
    contract_types VARCHAR(50)[],                       -- Which contract types (STANDARD, SCD2, DATA_VAULT, etc.)
    load_types VARCHAR(50)[],                           -- Which load types (FULL, INCREMENTAL, CDC, etc.)

    -- Capabilities
    supported_zones VARCHAR(20)[],                      -- Which zones: BRONZE, SILVER, GOLD, etc.
    requires_modules VARCHAR(100)[],                    -- dag_utilities modules used by this template
    spark_jobs_used VARCHAR(100)[],                     -- Spark jobs referenced by this template

    -- Template variables (what context this template expects)
    required_variables JSONB NOT NULL DEFAULT '[]',     -- List of required Jinja2 variables
    optional_variables JSONB NOT NULL DEFAULT '[]',     -- List of optional variables with defaults

    -- Selection rules (used by registry manager to auto-select)
    selection_priority INTEGER NOT NULL DEFAULT 100,    -- Lower = higher priority when multiple match
    selection_rules JSONB NOT NULL DEFAULT '{}',        -- Additional matching rules as JSON
    -- Example: {"min_file_size_gb": 10, "requires_copybook": true}

    -- Versioning
    template_version VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    changelog TEXT,

    -- Status
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_deprecated BOOLEAN NOT NULL DEFAULT FALSE,
    deprecated_by VARCHAR(50),                          -- template_code of replacement

    -- Metadata
    description TEXT,
    tags VARCHAR(50)[],
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by VARCHAR(100)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_template_registry_type ON template_registry(template_type);
CREATE INDEX IF NOT EXISTS idx_template_registry_pattern ON template_registry(pattern_code);
CREATE INDEX IF NOT EXISTS idx_template_registry_active ON template_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_template_registry_source_types ON template_registry USING GIN(source_types);
CREATE INDEX IF NOT EXISTS idx_template_registry_contract_types ON template_registry USING GIN(contract_types);


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 2. UTILITY REGISTRY                                                     │
-- │    Catalogs every function/class in dag_utilities                       │
-- │    The agent queries this to know what's available                      │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS utility_registry (
    utility_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identity
    module_name VARCHAR(100) NOT NULL,                  -- e.g., "core", "spark", "validation"
    submodule_name VARCHAR(100),                        -- e.g., "metadata_client", "schema_validator"
    class_name VARCHAR(200),                            -- e.g., "MetadataClient", NULL for standalone functions
    function_name VARCHAR(200),                         -- e.g., "get_feed_config", NULL for class-level
    fully_qualified_name VARCHAR(500) NOT NULL UNIQUE,  -- e.g., "dag_utilities.core.MetadataClient.get_feed_config"

    -- File location
    file_path VARCHAR(500) NOT NULL,                    -- Relative path from src/
    line_number INTEGER,                                -- Line number in file

    -- Signature
    parameters JSONB NOT NULL DEFAULT '[]',             -- Parameter list with types
    return_type VARCHAR(200),                           -- Return type annotation
    is_async BOOLEAN NOT NULL DEFAULT FALSE,

    -- Classification
    component_type VARCHAR(30) NOT NULL,                -- CLASS, METHOD, FUNCTION, CONSTANT
    category VARCHAR(50) NOT NULL,                      -- CORE, SPARK, STORAGE, VALIDATION, etc.

    -- Dependencies
    imports_required VARCHAR(500)[],                    -- What imports are needed to use this
    depends_on VARCHAR(500)[],                          -- Other utility functions this depends on

    -- Usage info
    used_by_templates VARCHAR(50)[],                    -- template_codes that use this utility
    usage_example TEXT,                                 -- Code snippet showing usage

    -- Documentation
    description TEXT,
    docstring TEXT,
    tags VARCHAR(50)[],

    -- Versioning
    version VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_deprecated BOOLEAN NOT NULL DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by VARCHAR(100)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_utility_registry_module ON utility_registry(module_name);
CREATE INDEX IF NOT EXISTS idx_utility_registry_category ON utility_registry(category);
CREATE INDEX IF NOT EXISTS idx_utility_registry_type ON utility_registry(component_type);
CREATE INDEX IF NOT EXISTS idx_utility_registry_active ON utility_registry(is_active);
CREATE INDEX IF NOT EXISTS idx_utility_registry_templates ON utility_registry USING GIN(used_by_templates);


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 3. SPARK JOB REGISTRY                                                   │
-- │    Catalogs the 5 canonical Spark jobs and any custom ones              │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS spark_job_registry (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Identity
    job_code VARCHAR(100) NOT NULL UNIQUE,              -- e.g., "RAW_TO_BRONZE", "BRONZE_SCHEMA_VALIDATION"
    job_name VARCHAR(200) NOT NULL,
    job_path VARCHAR(500) NOT NULL,                     -- File path relative to src/

    -- Zone transition
    source_zone VARCHAR(20),                            -- Input zone (RAW, BRONZE, SILVER)
    target_zone VARCHAR(20),                            -- Output zone (BRONZE, SILVER, GOLD)

    -- Capabilities
    supported_operations VARCHAR(50)[],                 -- What this job can do
    -- RAW_TO_BRONZE: ["INGEST", "TYPE_CAST", "AUDIT_COLUMNS"]
    -- BRONZE_TO_SILVER: ["VIEW_TRANSFORM", "DEDUPLICATE", "BUSINESS_KEY"]
    -- SILVER_TO_GOLD: ["AGGREGATE", "SCD2", "MERGE", "DIMENSION_LOAD", "FACT_LOAD"]

    supported_file_formats VARCHAR(30)[],               -- CSV, PARQUET, JSON, AVRO, EBCDIC, etc.
    supported_source_types VARCHAR(30)[],               -- FILE, DATABASE, STREAMING, API, LEGACY

    -- Arguments
    required_arguments JSONB NOT NULL DEFAULT '[]',     -- CLI arguments this job requires
    optional_arguments JSONB NOT NULL DEFAULT '[]',     -- Optional CLI arguments

    -- Dependencies
    dag_utilities_used VARCHAR(500)[],                  -- dag_utilities functions this job uses
    spark_packages VARCHAR(500)[],                      -- Additional Spark packages needed

    -- Performance
    default_executor_memory VARCHAR(10) DEFAULT '4g',
    default_executor_cores INTEGER DEFAULT 2,
    default_num_executors INTEGER DEFAULT 2,

    -- Documentation
    description TEXT,
    tags VARCHAR(50)[],

    -- Versioning
    version VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_spark_job_registry_zones ON spark_job_registry(source_zone, target_zone);


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 4. COMPONENT CHANGE LOG                                                 │
-- │    Tracks every change to any registered component                     │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS component_change_log (
    change_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    component_type VARCHAR(30) NOT NULL,                -- TEMPLATE, UTILITY, SPARK_JOB
    component_id UUID NOT NULL,                         -- FK to template_registry, utility_registry, or spark_job_registry
    change_type VARCHAR(20) NOT NULL,                   -- CREATED, UPDATED, DEPRECATED, DELETED
    previous_version VARCHAR(20),
    new_version VARCHAR(20),
    change_description TEXT,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    change_diff JSONB                                   -- What specifically changed
);

CREATE INDEX IF NOT EXISTS idx_component_change_log_type ON component_change_log(component_type);
CREATE INDEX IF NOT EXISTS idx_component_change_log_time ON component_change_log(changed_at);


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ 5. SEED DATA - Register existing APEX components                       │
-- └─────────────────────────────────────────────────────────────────────────┘

-- Register 9 pipeline pattern templates
INSERT INTO template_registry (template_code, template_name, template_type, template_path, pattern_code, source_types, contract_types, load_types, supported_zones, requires_modules, spark_jobs_used, required_variables, selection_priority, description, tags)
VALUES
    ('P01', 'File Medallion Pipeline', 'DAG_PATTERN', 'patterns/p01_file_medallion.py.jinja2', 'P01',
     ARRAY['FILE'], ARRAY['STANDARD'], ARRAY['FULL', 'APPEND'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'notification', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_SEMANTIC_VALIDATION', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "start_date"]'::jsonb,
     100, 'Standard file ingestion with full medallion architecture (Bronze → Silver → Gold)', ARRAY['file', 'medallion', 'standard']),

    ('P02', 'Big Data File Pipeline', 'DAG_PATTERN', 'patterns/p02_bigdata_file.py.jinja2', 'P02',
     ARRAY['FILE'], ARRAY['STANDARD'], ARRAY['FULL', 'APPEND'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "start_date"]'::jsonb,
     90, 'Large file processing with partitioned reads and dynamic cluster scaling', ARRAY['bigdata', 'partitioned', 'scaling']),

    ('P03', 'Database Lakehouse Pipeline', 'DAG_PATTERN', 'patterns/p03_database_lakehouse.py.jinja2', 'P03',
     ARRAY['DATABASE', 'NOSQL'], ARRAY['STANDARD'], ARRAY['FULL', 'INCREMENTAL', 'CDC'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_SEMANTIC_VALIDATION', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "source_connection_id", "start_date"]'::jsonb,
     100, 'Database to lakehouse with JDBC extraction, CDC, and incremental merge', ARRAY['database', 'cdc', 'lakehouse', 'merge']),

    ('P04', 'Legacy Migration Pipeline', 'DAG_PATTERN', 'patterns/p04_legacy_migration.py.jinja2', 'P04',
     ARRAY['LEGACY'], ARRAY['STANDARD'], ARRAY['FULL'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "legacy_source_type", "start_date"]'::jsonb,
     100, 'Legacy system migration: DTSX, COBOL copybook, AS400, EBCDIC with Cobrix', ARRAY['legacy', 'dtsx', 'cobol', 'ebcdic', 'migration']),

    ('P05', 'Streaming Batch Pipeline', 'DAG_PATTERN', 'patterns/p05_streaming_batch.py.jinja2', 'P05',
     ARRAY['STREAMING'], ARRAY['STANDARD'], ARRAY['APPEND'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "streaming_source", "topic_name", "checkpoint_location", "start_date"]'::jsonb,
     100, 'Micro-batch from Kafka/Pub/Sub/Kinesis with offset management', ARRAY['streaming', 'kafka', 'micro-batch']),

    ('P06', 'API SaaS Pipeline', 'DAG_PATTERN', 'patterns/p06_api_saas.py.jinja2', 'P06',
     ARRAY['API'], ARRAY['STANDARD'], ARRAY['FULL', 'INCREMENTAL'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_SCHEMA_VALIDATION', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "api_type", "api_connection_id", "api_endpoint", "start_date"]'::jsonb,
     100, 'REST API and SaaS ingestion with pagination, rate limiting, and OAuth', ARRAY['api', 'rest', 'saas', 'pagination']),

    ('P07', 'SCD Type 2 Pipeline', 'DAG_PATTERN', 'patterns/p07_scd2.py.jinja2', 'P07',
     ARRAY['FILE', 'DATABASE'], ARRAY['SCD2'], ARRAY['FULL', 'INCREMENTAL'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "business_keys", "tracked_columns", "start_date"]'::jsonb,
     80, 'Slowly Changing Dimensions Type 2 with hash-based change detection', ARRAY['scd2', 'dimension', 'history']),

    ('P08', 'Data Vault Pipeline', 'DAG_PATTERN', 'patterns/p08_data_vault.py.jinja2', 'P08',
     ARRAY['FILE', 'DATABASE'], ARRAY['DATA_VAULT'], ARRAY['FULL', 'INCREMENTAL'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "record_source", "hubs", "links", "satellites", "start_date"]'::jsonb,
     80, 'Data Vault 2.0 with Hub, Link, and Satellite loading', ARRAY['data-vault', 'hub', 'link', 'satellite']),

    ('P09', 'Star Schema Pipeline', 'DAG_PATTERN', 'patterns/p09_star_schema.py.jinja2', 'P09',
     ARRAY['FILE', 'DATABASE'], ARRAY['STAR_SCHEMA'], ARRAY['FULL', 'INCREMENTAL'],
     ARRAY['BRONZE', 'SILVER', 'GOLD'],
     ARRAY['core', 'spark', 'storage', 'validation', 'logging', 'remediation'],
     ARRAY['RAW_TO_BRONZE', 'BRONZE_TO_SILVER', 'SILVER_TO_GOLD'],
     '["feed_id", "contract_id", "dag_id", "domain", "dimensions", "fact_table", "start_date"]'::jsonb,
     80, 'Star schema dimensional modeling with fact and dimension loading', ARRAY['star-schema', 'dimension', 'fact'])

ON CONFLICT (template_code) DO NOTHING;


-- Register 5 canonical Spark jobs
INSERT INTO spark_job_registry (job_code, job_name, job_path, source_zone, target_zone, supported_operations, supported_file_formats, supported_source_types, required_arguments, description, tags)
VALUES
    ('RAW_TO_BRONZE', 'Raw to Bronze Ingestion', 'spark_jobs/raw_to_bronze.py', 'RAW', 'BRONZE',
     ARRAY['INGEST', 'TYPE_CAST', 'AUDIT_COLUMNS', 'PARTITION'],
     ARRAY['CSV', 'PARQUET', 'JSON', 'AVRO', 'ORC', 'XML', 'EXCEL', 'FIXED_WIDTH', 'EBCDIC'],
     ARRAY['FILE', 'DATABASE', 'STREAMING', 'API', 'LEGACY'],
     '[{"name": "--feed-id", "required": true}, {"name": "--contract-id", "required": true}, {"name": "--execution-id", "required": true}, {"name": "--execution-date", "required": true}, {"name": "--metadata-db", "required": true}]'::jsonb,
     'Ingests raw data to Bronze zone with schema enforcement, type casting, and audit columns',
     ARRAY['bronze', 'ingest', 'raw']),

    ('BRONZE_SCHEMA_VALIDATION', 'Bronze Schema Validation', 'spark_jobs/bronze_schema_validation.py', 'BRONZE', 'BRONZE',
     ARRAY['COLUMN_PRESENCE', 'NOT_NULL', 'PRIMARY_KEY', 'DATA_TYPE', 'CUSTOM_RULES'],
     NULL, NULL,
     '[{"name": "--feed-id", "required": true}, {"name": "--contract-id", "required": true}, {"name": "--execution-id", "required": true}, {"name": "--metadata-db", "required": true}]'::jsonb,
     'Validates Bronze data against schema contract: column presence, nullability, types, primary keys',
     ARRAY['validation', 'schema', 'bronze']),

    ('BRONZE_TO_SILVER', 'Bronze to Silver Transform', 'spark_jobs/v2/promote_bronze_to_silver.py', 'BRONZE', 'SILVER',
     ARRAY['VIEW_TRANSFORM', 'DEDUPLICATE', 'BUSINESS_KEY', 'CLEAN', 'HASH_GENERATE'],
     NULL, NULL,
     '[{"name": "--feed-id", "required": true}, {"name": "--contract-id", "required": true}, {"name": "--execution-id", "required": true}, {"name": "--execution-date", "required": true}, {"name": "--metadata-db", "required": true}]'::jsonb,
     'Transforms Bronze to Silver using view-based SQL, deduplication, and business key generation',
     ARRAY['silver', 'transform', 'deduplicate']),

    ('SILVER_SEMANTIC_VALIDATION', 'Silver Semantic Validation', 'spark_jobs/silver_semantic_validation.py', 'SILVER', 'SILVER',
     ARRAY['BUSINESS_RULES', 'REFERENTIAL_INTEGRITY', 'CROSS_FIELD', 'RANGE_CHECK', 'SCD2_VALIDATION', 'DATA_VAULT_VALIDATION'],
     NULL, NULL,
     '[{"name": "--feed-id", "required": true}, {"name": "--contract-id", "required": true}, {"name": "--execution-id", "required": true}, {"name": "--metadata-db", "required": true}]'::jsonb,
     'Validates Silver data against business rules: referential integrity, cross-field validation, range checks',
     ARRAY['validation', 'semantic', 'silver', 'business-rules']),

    ('SILVER_TO_GOLD', 'Silver to Gold Transform', 'spark_jobs/v2/build_gold_layer.py', 'SILVER', 'GOLD',
     ARRAY['AGGREGATE', 'SCD2', 'MERGE', 'DIMENSION_LOAD', 'FACT_LOAD', 'SURROGATE_KEY', 'HUB_LOAD', 'LINK_LOAD', 'SATELLITE_LOAD', 'DETECT_CHANGES'],
     NULL, NULL,
     '[{"name": "--feed-id", "required": true}, {"name": "--contract-id", "required": true}, {"name": "--execution-id", "required": true}, {"name": "--execution-date", "required": true}, {"name": "--metadata-db", "required": true}]'::jsonb,
     'Transforms Silver to Gold with aggregations, SCD2, dimensional loading, Data Vault, and star schema support',
     ARRAY['gold', 'aggregate', 'scd2', 'dimension', 'fact'])

ON CONFLICT (job_code) DO NOTHING;
