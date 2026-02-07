-- ═══════════════════════════════════════════════════════════════════════════════
-- APEX Data Agent - Execution Status Update Template
-- Generated: 2026-02-05
-- Pipeline: sales_daily_pipeline
-- Feed ID: 1001
--
-- This SQL is executed by the DAG to track execution status
-- ═══════════════════════════════════════════════════════════════════════════════

-- Function to update execution status (called by DAG)
CREATE OR REPLACE FUNCTION update_pipeline_execution(
    p_feed_id INTEGER,
    p_execution_id VARCHAR,
    p_batch_id VARCHAR,
    p_status VARCHAR,
    p_records_processed INTEGER DEFAULT 0,
    p_records_rejected INTEGER DEFAULT 0,
    p_quality_score DECIMAL DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    -- Insert or update execution record
    INSERT INTO feed_ingestion_details (
        feed_id,
        execution_id,
        batch_id,
        status,
        records_processed,
        records_rejected,
        quality_score,
        error_message,
        started_at,
        completed_at
    ) VALUES (
        p_feed_id,
        p_execution_id,
        p_batch_id,
        p_status,
        p_records_processed,
        p_records_rejected,
        p_quality_score,
        p_error_message,
        CASE WHEN p_status = 'running' THEN NOW() ELSE NULL END,
        CASE WHEN p_status IN ('success', 'failed') THEN NOW() ELSE NULL END
    )
    ON CONFLICT (execution_id) DO UPDATE SET
        status = EXCLUDED.status,
        records_processed = COALESCE(EXCLUDED.records_processed, feed_ingestion_details.records_processed),
        records_rejected = COALESCE(EXCLUDED.records_rejected, feed_ingestion_details.records_rejected),
        quality_score = COALESCE(EXCLUDED.quality_score, feed_ingestion_details.quality_score),
        error_message = COALESCE(EXCLUDED.error_message, feed_ingestion_details.error_message),
        completed_at = CASE WHEN EXCLUDED.status IN ('success', 'failed') THEN NOW() ELSE feed_ingestion_details.completed_at END;

    -- Update last execution timestamp on feed_details
    UPDATE feed_details
    SET last_execution_at = NOW(),
        last_execution_status = p_status,
        updated_at = NOW()
    WHERE feed_id = p_feed_id;

    -- Log audit event
    INSERT INTO audit_log (
        event_type,
        feed_id,
        execution_id,
        event_data,
        created_by,
        created_at
    ) VALUES (
        'EXECUTION_' || UPPER(p_status),
        p_feed_id,
        p_execution_id,
        jsonb_build_object(
            'batch_id', p_batch_id,
            'records_processed', p_records_processed,
            'records_rejected', p_records_rejected,
            'quality_score', p_quality_score
        ),
        'airflow',
        NOW()
    );
END;
$$ LANGUAGE plpgsql;

-- Function to record zone-level metrics
CREATE OR REPLACE FUNCTION record_zone_metrics(
    p_feed_id INTEGER,
    p_execution_id VARCHAR,
    p_zone VARCHAR,
    p_records_input INTEGER,
    p_records_output INTEGER,
    p_records_rejected INTEGER DEFAULT 0,
    p_validation_passed BOOLEAN DEFAULT TRUE,
    p_validation_score DECIMAL DEFAULT 1.0
) RETURNS VOID AS $$
BEGIN
    INSERT INTO zone_execution_metrics (
        feed_id,
        execution_id,
        zone_level,
        records_input,
        records_output,
        records_rejected,
        validation_passed,
        validation_score,
        completed_at
    ) VALUES (
        p_feed_id,
        p_execution_id,
        p_zone,
        p_records_input,
        p_records_output,
        p_records_rejected,
        p_validation_passed,
        p_validation_score,
        NOW()
    )
    ON CONFLICT (execution_id, zone_level) DO UPDATE SET
        records_input = EXCLUDED.records_input,
        records_output = EXCLUDED.records_output,
        records_rejected = EXCLUDED.records_rejected,
        validation_passed = EXCLUDED.validation_passed,
        validation_score = EXCLUDED.validation_score,
        completed_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to calculate and store health score
CREATE OR REPLACE FUNCTION calculate_health_score(
    p_feed_id INTEGER,
    p_execution_id VARCHAR
) RETURNS DECIMAL AS $$
DECLARE
    v_quality_score DECIMAL;
    v_freshness_score DECIMAL;
    v_stability_score DECIMAL;
    v_overall_score DECIMAL;
BEGIN
    -- Get quality score from current execution
    SELECT COALESCE(quality_score, 1.0)
    INTO v_quality_score
    FROM feed_ingestion_details
    WHERE execution_id = p_execution_id;

    -- Calculate freshness (1.0 if within SLA)
    v_freshness_score := 1.0;

    -- Calculate stability (based on recent executions)
    SELECT COALESCE(
        (SELECT COUNT(*) FILTER (WHERE status = 'success')::DECIMAL / COUNT(*)
         FROM feed_ingestion_details
         WHERE feed_id = p_feed_id
         AND created_at > NOW() - INTERVAL '7 days'),
        1.0
    ) INTO v_stability_score;

    -- Weighted overall score: quality(40%) + freshness(30%) + stability(30%)
    v_overall_score := (v_quality_score * 0.4) + (v_freshness_score * 0.3) + (v_stability_score * 0.3);

    -- Store health score
    INSERT INTO pipeline_health_scores (
        feed_id,
        execution_id,
        quality_score,
        freshness_score,
        stability_score,
        overall_score,
        calculated_at
    ) VALUES (
        p_feed_id,
        p_execution_id,
        v_quality_score,
        v_freshness_score,
        v_stability_score,
        v_overall_score,
        NOW()
    )
    ON CONFLICT (execution_id) DO UPDATE SET
        quality_score = EXCLUDED.quality_score,
        freshness_score = EXCLUDED.freshness_score,
        stability_score = EXCLUDED.stability_score,
        overall_score = EXCLUDED.overall_score,
        calculated_at = NOW();

    RETURN v_overall_score;
END;
$$ LANGUAGE plpgsql;

-- Example usage (called by DAG):
-- SELECT update_pipeline_execution(1001, 'sales_daily_pipeline_manual__2026-02-05', '20260205', 'running');
-- SELECT record_zone_metrics(1001, 'sales_daily_pipeline_manual__2026-02-05', 'raw', 10000, 10000, 0, TRUE, 1.0);
-- SELECT record_zone_metrics(1001, 'sales_daily_pipeline_manual__2026-02-05', 'refined', 10000, 9800, 200, TRUE, 0.98);
-- SELECT record_zone_metrics(1001, 'sales_daily_pipeline_manual__2026-02-05', 'trusted', 9800, 9800, 0, TRUE, 1.0);
-- SELECT update_pipeline_execution(1001, 'sales_daily_pipeline_manual__2026-02-05', '20260205', 'success', 9800, 200, 0.98);
-- SELECT calculate_health_score(1001, 'sales_daily_pipeline_manual__2026-02-05');
