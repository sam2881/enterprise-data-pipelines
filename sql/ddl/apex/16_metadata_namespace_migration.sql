-- Metadata namespace migration
-- Moves platform metadata tables into the platform_* namespace.
-- Existing consumers can continue reading through compatibility views.

BEGIN;

DO $$
DECLARE
    table_name TEXT;
    metadata_tables CONSTANT TEXT[] := ARRAY[
        'connection_registry', 'domain_registry', 'source_registry', 'dag_template',
        'feed_group', 'feed', 'spark_config', 'notification_config', 'watermark_tracking',
        'data_contract', 'schema_version', 'view_definition', 'transformation_rule',
        'contract_transformation', 'validation_rule', 'quality_expectation',
        'sla_definition', 'pipeline_dependency', 'pipeline_execution', 'task_execution',
        'audit_log', 'data_lineage', 'validation_log', 'error_log', 'metadata_audit_log',
        'agent_decision_log', 'template_change_log', 'sla_breach_log', 'execution_cost_log',
        'template_registry', 'utility_registry', 'spark_job_registry', 'validation_result',
        'validation_summary', 'join_dependency', 'observability_metrics', 'data_asset',
        'business_term', 'tag_taxonomy', 'access_policy', 'data_classification',
        'access_request', 'data_product', 'data_product_subscription', 'pipeline_registry',
        'pipeline_sources', 'pipeline_schemas', 'pipeline_targets', 'pipeline_transformations',
        'pipeline_quality_rules', 'pipeline_execution_policies', 'pipeline_executions',
        'pipeline_watermarks', 'pipeline_events', 'data_contracts', 'data_products',
        'transformation_rules', 'schema_changes', 'pipeline_costs', 'approval_requests',
        'pipeline_data_products', 'pipeline_metrics', 'jira_tickets', 'jira_ticket_events',
        'jira_ticket_requirements', 'jira_pipeline_mapping', 'join_dependencies'
    ];
BEGIN
    FOREACH table_name IN ARRAY metadata_tables LOOP
        IF to_regclass(format('public.%s', table_name)) IS NOT NULL
           AND to_regclass(format('public.platform_%s', table_name)) IS NULL THEN
            EXECUTE format('ALTER TABLE public.%I RENAME TO %I', table_name, 'platform_' || table_name);
        ELSIF to_regclass(format('public.%s', table_name)) IS NOT NULL
              AND to_regclass(format('public.platform_%s', table_name)) IS NOT NULL THEN
            RAISE EXCEPTION 'Both legacy table % and platform_% exist', table_name, table_name;
        END IF;
    END LOOP;

    FOREACH table_name IN ARRAY metadata_tables LOOP
        IF to_regclass(format('public.platform_%s', table_name)) IS NOT NULL
           AND to_regclass(format('public.%s', table_name)) IS NULL THEN
            EXECUTE format(
                'CREATE OR REPLACE VIEW public.%I AS SELECT * FROM public.%I',
                table_name,
                'platform_' || table_name
            );
        END IF;
    END LOOP;
END $$;

COMMIT;
