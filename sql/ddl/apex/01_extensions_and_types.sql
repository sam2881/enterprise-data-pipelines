-- ═══════════════════════════════════════════════════════════════════════════
-- APEX DATA AGENT - EXTENSIONS AND TYPES
-- Part 1: PostgreSQL Extensions and Custom Types
-- ═══════════════════════════════════════════════════════════════════════════

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Custom ENUM types for APEX
DO $$ BEGIN
    CREATE TYPE source_type_enum AS ENUM (
        'FILE', 'DATABASE', 'API', 'KAFKA', 'STREAMING', 'LEGACY', 'SAAS'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE file_format_enum AS ENUM (
        'CSV', 'JSON', 'PARQUET', 'AVRO', 'XML', 'FIXED_WIDTH', 'EBCDIC', 'ORC'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE load_type_enum AS ENUM (
        'FULL', 'INCREMENTAL', 'APPEND', 'CDC', 'WATERMARK', 'MERGE'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE zone_level_enum AS ENUM (
        'RAW', 'TRANSIENT', 'BRONZE', 'SILVER', 'GOLD'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE validation_type_enum AS ENUM (
        'SCHEMA', 'SEMANTIC', 'QUALITY', 'REFERENTIAL', 'BUSINESS'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE severity_enum AS ENUM (
        'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE execution_status_enum AS ENUM (
        'PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED', 'NO_FILE', 'TIMEOUT'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE template_type_enum AS ENUM (
        'BATCH', 'STREAMING', 'HYBRID', 'CDC'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE pattern_code_enum AS ENUM (
        'P01', 'P02', 'P03', 'P04', 'P05', 'P06', 'P07', 'P08', 'P09'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE feed_type_enum AS ENUM (
        'BATCH', 'STREAMING', 'HYBRID'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE contract_type_enum AS ENUM (
        'FILE', 'TABLE', 'API', 'STREAM'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Comment for documentation
COMMENT ON TYPE source_type_enum IS 'Types of data sources supported by APEX';
COMMENT ON TYPE zone_level_enum IS 'Data zones in medallion architecture: RAW -> TRANSIENT -> BRONZE -> SILVER -> GOLD';
COMMENT ON TYPE pattern_code_enum IS '9 canonical pipeline patterns: P01=FILE_MEDALLION, P02=BIGDATA_FILE, P03=DATABASE_LAKEHOUSE, P04=LEGACY_MIGRATION, P05=STREAMING_BATCH, P06=API_SAAS, P07=SCD2, P08=DATA_VAULT, P09=STAR_SCHEMA';
