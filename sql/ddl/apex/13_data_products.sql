-- ═══════════════════════════════════════════════════════════════════════════
-- APEX v2.2 — Data Products & Data Mesh
-- ═══════════════════════════════════════════════════════════════════════════
-- Data product registry and subscription workflow for data mesh architecture.

-- ───────────────────────────────────────────────────────────────────────────
-- 1. DATA PRODUCT REGISTRY
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS data_product (
    product_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_name        VARCHAR(255) NOT NULL,
    domain              VARCHAR(100) NOT NULL,
    owner               VARCHAR(100) NOT NULL,
    description         TEXT,
    sla_freshness_hours INTEGER,
    sla_quality_score   NUMERIC(5,2),
    output_assets       JSONB DEFAULT '[]'::jsonb,
    input_feeds         JSONB DEFAULT '[]'::jsonb,
    schema_contract     JSONB,
    documentation_url   VARCHAR(500),
    version             VARCHAR(20) DEFAULT '1.0.0',
    status              VARCHAR(20) DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'PUBLISHED', 'DEPRECATED', 'ARCHIVED')),
    published_at        TIMESTAMP,
    is_active           BOOLEAN DEFAULT true,
    created_at          TIMESTAMP DEFAULT now(),
    updated_at          TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_data_product_domain ON data_product (domain);
CREATE INDEX IF NOT EXISTS idx_data_product_status ON data_product (status);
CREATE INDEX IF NOT EXISTS idx_data_product_owner  ON data_product (owner);


-- ───────────────────────────────────────────────────────────────────────────
-- 2. DATA PRODUCT SUBSCRIPTION
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS data_product_subscription (
    subscription_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID NOT NULL REFERENCES data_product(product_id),
    subscriber      VARCHAR(255) NOT NULL,
    subscriber_type VARCHAR(50) DEFAULT 'USER' CHECK (subscriber_type IN ('USER', 'GROUP', 'SERVICE_ACCOUNT', 'PIPELINE')),
    access_level    VARCHAR(50) DEFAULT 'READ' CHECK (access_level IN ('READ', 'MASKED_READ')),
    status          VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'REVOKED')),
    justification   TEXT,
    requested_at    TIMESTAMP DEFAULT now(),
    approved_by     VARCHAR(100),
    approved_at     TIMESTAMP,
    expires_at      TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_subscription_product ON data_product_subscription (product_id);
CREATE INDEX IF NOT EXISTS idx_subscription_status  ON data_product_subscription (status);


-- ───────────────────────────────────────────────────────────────────────────
-- 3. VIEWS
-- ───────────────────────────────────────────────────────────────────────────

-- View: Published data products with subscriber count
CREATE OR REPLACE VIEW v_data_product_catalog AS
SELECT
    dp.product_id,
    dp.product_name,
    dp.domain,
    dp.owner,
    dp.description,
    dp.version,
    dp.status,
    dp.sla_freshness_hours,
    dp.sla_quality_score,
    dp.published_at,
    COUNT(DISTINCT dps.subscription_id) FILTER (WHERE dps.status = 'APPROVED') AS active_subscribers,
    COUNT(DISTINCT dps.subscription_id) FILTER (WHERE dps.status = 'PENDING')  AS pending_requests
FROM data_product dp
LEFT JOIN data_product_subscription dps ON dp.product_id = dps.product_id
WHERE dp.is_active = true
GROUP BY dp.product_id, dp.product_name, dp.domain, dp.owner, dp.description,
         dp.version, dp.status, dp.sla_freshness_hours, dp.sla_quality_score, dp.published_at
ORDER BY dp.domain, dp.product_name;
