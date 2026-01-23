-- Insert Orders Pipeline Metadata
INSERT INTO pipeline_registry (pipeline_name, domain, source_type, target_dataset, schedule, environment)
VALUES ('csv_orders_medallion_dag', 'sales', 'file', 'sales_raw', '@daily', 'dev')
ON CONFLICT (pipeline_name, environment) DO UPDATE SET updated_at = NOW();
