-- ═══════════════════════════════════════════════════════════════════════════
-- APEX v2.2 — Data Catalog & Discovery
-- ═══════════════════════════════════════════════════════════════════════════
-- Provides data asset registry, business glossary, and tag taxonomy
-- for enterprise data discovery and self-service analytics.

-- ───────────────────────────────────────────────────────────────────────────
-- 1. DATA ASSET REGISTRY
-- ───────────────────────────────────────────────────────────────────────────
-- Tracks every table/dataset/file across all medallion zones.
-- Auto-populated by audit_logger after each zone transition.

CREATE TABLE IF NOT EXISTS platform_data_asset (
    asset_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_name      VARCHAR(255) NOT NULL,
    asset_type      VARCHAR(50)  NOT NULL CHECK (asset_type IN ('TABLE', 'VIEW', 'DATASET', 'FILE', 'STREAM')),
    zone_level      zone_level_enum NOT NULL,
    feed_id         VARCHAR(100),
    contract_id     UUID,
    dataset_path    VARCHAR(500),
    schema_json     JSONB,
    row_count       BIGINT,
    size_bytes      BIGINT,
    owner           VARCHAR(100),
    domain          VARCHAR(100),
    tags            JSONB DEFAULT '[]'::jsonb,
    description     TEXT,
    last_refreshed_at  TIMESTAMP,
    quality_score   NUMERIC(5,2),
    freshness_hours NUMERIC(8,2),
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT now(),
    updated_at      TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_data_asset_name   ON platform_data_asset (asset_name);
CREATE INDEX IF NOT EXISTS idx_data_asset_zone   ON platform_data_asset (zone_level);
CREATE INDEX IF NOT EXISTS idx_data_asset_domain ON platform_data_asset (domain);
CREATE INDEX IF NOT EXISTS idx_data_asset_feed   ON platform_data_asset (feed_id);
CREATE INDEX IF NOT EXISTS idx_data_asset_tags   ON platform_data_asset USING gin (tags);

-- Full-text search index for asset discovery
CREATE INDEX IF NOT EXISTS idx_data_asset_search ON platform_data_asset
    USING gin (to_tsvector('english', coalesce(asset_name, '') || ' ' || coalesce(description, '') || ' ' || coalesce(domain, '')));


-- ───────────────────────────────────────────────────────────────────────────
-- 2. BUSINESS GLOSSARY
-- ───────────────────────────────────────────────────────────────────────────
-- Maps business terms to technical assets for self-service discovery.

CREATE TABLE IF NOT EXISTS platform_business_term (
    term_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    term_name       VARCHAR(255) NOT NULL UNIQUE,
    definition      TEXT NOT NULL,
    domain          VARCHAR(100),
    owner           VARCHAR(100),
    related_assets  JSONB DEFAULT '[]'::jsonb,
    synonyms        JSONB DEFAULT '[]'::jsonb,
    examples        JSONB DEFAULT '[]'::jsonb,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT now(),
    updated_at      TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_business_term_name   ON platform_business_term (term_name);
CREATE INDEX IF NOT EXISTS idx_business_term_domain ON platform_business_term (domain);
CREATE INDEX IF NOT EXISTS idx_business_term_search ON platform_business_term
    USING gin (to_tsvector('english', coalesce(term_name, '') || ' ' || coalesce(definition, '') || ' ' || coalesce(domain, '')));


-- ───────────────────────────────────────────────────────────────────────────
-- 3. TAG TAXONOMY
-- ───────────────────────────────────────────────────────────────────────────
-- Hierarchical classification system for data assets.

CREATE TABLE IF NOT EXISTS platform_tag_taxonomy (
    tag_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tag_name        VARCHAR(100) NOT NULL,
    tag_category    VARCHAR(50)  NOT NULL CHECK (tag_category IN ('SENSITIVITY', 'DOMAIN', 'QUALITY', 'COMPLIANCE', 'LIFECYCLE', 'CUSTOM')),
    parent_tag_id   UUID REFERENCES platform_tag_taxonomy(tag_id),
    description     TEXT,
    color           VARCHAR(7),
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT now(),
    UNIQUE (tag_name, tag_category)
);

-- Seed standard sensitivity tags
INSERT INTO platform_tag_taxonomy (tag_name, tag_category, description) VALUES
    ('PUBLIC',       'SENSITIVITY', 'Public data, no access restrictions'),
    ('INTERNAL',     'SENSITIVITY', 'Internal use only'),
    ('CONFIDENTIAL', 'SENSITIVITY', 'Confidential, restricted access'),
    ('PII',          'SENSITIVITY', 'Contains personally identifiable information'),
    ('PHI',          'SENSITIVITY', 'Contains protected health information'),
    ('PCI',          'SENSITIVITY', 'Contains payment card data')
ON CONFLICT (tag_name, tag_category) DO NOTHING;

-- Seed compliance tags
INSERT INTO platform_tag_taxonomy (tag_name, tag_category, description) VALUES
    ('GDPR',     'COMPLIANCE', 'Subject to GDPR regulations'),
    ('CCPA',     'COMPLIANCE', 'Subject to CCPA regulations'),
    ('HIPAA',    'COMPLIANCE', 'Subject to HIPAA regulations'),
    ('SOX',      'COMPLIANCE', 'Subject to SOX compliance'),
    ('PCI-DSS',  'COMPLIANCE', 'Subject to PCI-DSS standards')
ON CONFLICT (tag_name, tag_category) DO NOTHING;

-- Seed lifecycle tags
INSERT INTO platform_tag_taxonomy (tag_name, tag_category, description) VALUES
    ('RAW',       'LIFECYCLE', 'Raw unprocessed data'),
    ('CURATED',   'LIFECYCLE', 'Cleaned and validated data'),
    ('CERTIFIED', 'LIFECYCLE', 'Production-certified data product'),
    ('DEPRECATED','LIFECYCLE', 'Deprecated, pending removal')
ON CONFLICT (tag_name, tag_category) DO NOTHING;
