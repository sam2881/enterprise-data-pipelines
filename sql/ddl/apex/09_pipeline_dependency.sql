-- ═══════════════════════════════════════════════════════════════════════════
-- APEX Metadata: Cross-Pipeline Dependencies
-- Defines upstream/downstream relationships between feeds for DAG orchestration
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pipeline_dependency (
    dependency_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    upstream_feed_id    VARCHAR(100) NOT NULL,
    upstream_dag_id     VARCHAR(255),          -- Airflow DAG ID of upstream
    upstream_task_id    VARCHAR(255) DEFAULT 'finalize_execution',
    downstream_feed_id  VARCHAR(100) NOT NULL,
    dependency_type     VARCHAR(20)  NOT NULL DEFAULT 'DATA',
    required_zone       VARCHAR(20)  NOT NULL DEFAULT 'SILVER',
    timeout_seconds     INTEGER      NOT NULL DEFAULT 3600,
    poke_interval       INTEGER      NOT NULL DEFAULT 300,
    is_active           BOOLEAN      NOT NULL DEFAULT true,
    created_at          TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at          TIMESTAMP    NOT NULL DEFAULT now(),

    CONSTRAINT chk_dep_type CHECK (dependency_type IN ('DATA', 'SCHEMA', 'TEMPORAL')),
    CONSTRAINT chk_zone CHECK (required_zone IN ('BRONZE', 'SILVER', 'GOLD')),
    CONSTRAINT chk_no_self_dep CHECK (upstream_feed_id <> downstream_feed_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_dep_downstream
    ON pipeline_dependency(downstream_feed_id) WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_pipeline_dep_upstream
    ON pipeline_dependency(upstream_feed_id) WHERE is_active = true;

COMMENT ON TABLE pipeline_dependency IS
    'Cross-pipeline dependencies. Generates ExternalTaskSensor in downstream DAGs.';
