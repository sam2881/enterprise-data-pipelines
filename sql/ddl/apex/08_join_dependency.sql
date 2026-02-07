-- ═══════════════════════════════════════════════════════════════════════════
-- APEX Metadata: Join Dependencies
-- Defines multi-table join relationships for Gold zone processing
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS join_dependency (
    join_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id      UUID NOT NULL REFERENCES data_contract(contract_id),
    source_table     VARCHAR(255) NOT NULL,
    target_table     VARCHAR(255) NOT NULL,
    join_type        VARCHAR(20)  NOT NULL DEFAULT 'inner',
    join_keys        JSONB        NOT NULL,  -- [{"left": "col1", "right": "col2"}]
    join_order       INTEGER      NOT NULL DEFAULT 1,
    select_columns   JSONB,                  -- Optional: columns to keep from right table
    null_safe        BOOLEAN      NOT NULL DEFAULT false,
    broadcast        BOOLEAN      NOT NULL DEFAULT false,  -- Broadcast hint for small tables
    is_active        BOOLEAN      NOT NULL DEFAULT true,
    created_at       TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP    NOT NULL DEFAULT now(),

    CONSTRAINT chk_join_type CHECK (
        join_type IN ('inner', 'left', 'right', 'full', 'cross', 'semi', 'anti')
    )
);

CREATE INDEX IF NOT EXISTS idx_join_dep_contract
    ON join_dependency(contract_id) WHERE is_active = true;

COMMENT ON TABLE join_dependency IS
    'Multi-table join definitions for Gold zone. Processed by join_executor.py in join_order sequence.';
