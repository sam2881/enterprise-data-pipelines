-- ═══════════════════════════════════════════════════════════════════════════
-- APEX v2.2 — Data Governance & RBAC
-- ═══════════════════════════════════════════════════════════════════════════
-- Access policies, data classification, and compliance tracking.

-- ───────────────────────────────────────────────────────────────────────────
-- 1. ACCESS POLICY
-- ───────────────────────────────────────────────────────────────────────────
-- Defines who can access what resources at what level.

CREATE TABLE IF NOT EXISTS access_policy (
    policy_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_name     VARCHAR(255) NOT NULL,
    resource_type   VARCHAR(50) NOT NULL CHECK (resource_type IN ('DATASET', 'TABLE', 'COLUMN', 'ZONE', 'DOMAIN')),
    resource_id     VARCHAR(500) NOT NULL,
    principal_type  VARCHAR(50) NOT NULL CHECK (principal_type IN ('USER', 'GROUP', 'SERVICE_ACCOUNT', 'ROLE')),
    principal_id    VARCHAR(255) NOT NULL,
    access_level    VARCHAR(50) NOT NULL CHECK (access_level IN ('READ', 'WRITE', 'ADMIN', 'MASKED_READ')),
    conditions      JSONB,
    expires_at      TIMESTAMP,
    is_active       BOOLEAN DEFAULT true,
    created_by      VARCHAR(100),
    approved_by     VARCHAR(100),
    created_at      TIMESTAMP DEFAULT now(),
    updated_at      TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_access_policy_resource  ON access_policy (resource_type, resource_id);
CREATE INDEX IF NOT EXISTS idx_access_policy_principal ON access_policy (principal_type, principal_id);


-- ───────────────────────────────────────────────────────────────────────────
-- 2. DATA CLASSIFICATION
-- ───────────────────────────────────────────────────────────────────────────
-- Links PII detection results to data assets.
-- Auto-populated by pii_detection.persist_classifications().

CREATE TABLE IF NOT EXISTS data_classification (
    classification_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id            UUID REFERENCES data_asset(asset_id),
    column_name         VARCHAR(255) NOT NULL,
    classification_type VARCHAR(50) NOT NULL CHECK (classification_type IN ('PII', 'PHI', 'FINANCIAL', 'CONFIDENTIAL', 'PUBLIC', 'INTERNAL')),
    pii_type            VARCHAR(50),
    confidence          NUMERIC(3,2) CHECK (confidence >= 0 AND confidence <= 1),
    masking_strategy    VARCHAR(50) CHECK (masking_strategy IN ('REDACT', 'HASH', 'TOKENIZE', 'PARTIAL_MASK', 'ENCRYPT', 'NULL', 'FAKE')),
    policy_tag          VARCHAR(255),
    is_enforced         BOOLEAN DEFAULT false,
    detected_at         TIMESTAMP DEFAULT now(),
    reviewed_by         VARCHAR(100),
    reviewed_at         TIMESTAMP,
    UNIQUE (asset_id, column_name, classification_type)
);

CREATE INDEX IF NOT EXISTS idx_classification_asset  ON data_classification (asset_id);
CREATE INDEX IF NOT EXISTS idx_classification_type   ON data_classification (classification_type);
CREATE INDEX IF NOT EXISTS idx_classification_pii    ON data_classification (pii_type);


-- ───────────────────────────────────────────────────────────────────────────
-- 3. ACCESS REQUEST / AUDIT
-- ───────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS access_request (
    request_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requester       VARCHAR(255) NOT NULL,
    resource_type   VARCHAR(50) NOT NULL,
    resource_id     VARCHAR(500) NOT NULL,
    access_level    VARCHAR(50) NOT NULL,
    justification   TEXT,
    status          VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED')),
    reviewed_by     VARCHAR(100),
    reviewed_at     TIMESTAMP,
    expires_at      TIMESTAMP,
    created_at      TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_access_request_status ON access_request (status);


-- ───────────────────────────────────────────────────────────────────────────
-- 4. VIEWS
-- ───────────────────────────────────────────────────────────────────────────

-- View: All PII columns across the platform
CREATE OR REPLACE VIEW v_pii_columns AS
SELECT
    da.asset_name,
    da.zone_level,
    da.domain,
    dc.column_name,
    dc.pii_type,
    dc.confidence,
    dc.masking_strategy,
    dc.is_enforced,
    dc.policy_tag,
    dc.detected_at
FROM data_classification dc
JOIN data_asset da ON dc.asset_id = da.asset_id
WHERE dc.classification_type IN ('PII', 'PHI')
  AND da.is_active = true
ORDER BY da.domain, da.asset_name, dc.column_name;
