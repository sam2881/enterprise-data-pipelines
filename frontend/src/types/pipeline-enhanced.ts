/**
 * Enhanced Pipeline Types - Extended types for enterprise features
 *
 * Includes:
 * - DTSX/SSIS Migration support
 * - Natural Language transformations
 * - Extended source types (EBCDIC, Fixed-Width, Excel)
 * - Destination data model types (Data Vault, OBT)
 */

import {
  PipelineIdentity,
  SourceConfig,
  SchemaDefinition,
  TargetConfig,
  TransformationRule,
  DataQualityRule,
  ExecutionPolicy,
  Environment,
  DataType,
} from './pipeline'

// =============================================================================
// Extended Source Types
// =============================================================================

export type ExtendedSourceType =
  | 'file_csv'
  | 'file_tsv'
  | 'file_json'
  | 'file_xml'
  | 'file_parquet'
  | 'file_avro'
  | 'file_excel'
  | 'file_fixed_width'
  | 'file_ebcdic'
  | 'database_oracle'
  | 'database_sqlserver'
  | 'database_db2'
  | 'database_postgres'
  | 'database_mysql'
  | 'database_snowflake'
  | 'streaming_kafka'
  | 'streaming_pubsub'
  | 'streaming_kinesis'
  | 'api_rest'
  | 'api_graphql'
  | 'dtsx_migration'
  | 'cdc_debezium'

export type ExtractionMode = 'full' | 'incremental' | 'cdc'

export type DestinationModel = 'flat' | 'dimensional' | 'data_vault' | 'obt'

// =============================================================================
// DTSX Migration Configuration
// =============================================================================

export interface DTSXMigrationConfig {
  source_type: 'dtsx_migration'
  dtsx_location: string  // GCS path to DTSX file
  dtsx_package_name: string
  original_server?: string
  connection_mapping: Record<string, string>  // SSIS conn -> Airflow conn
  // Parsed components (populated after parsing)
  parsed_sources?: DTSXSource[]
  parsed_transformations?: DTSXTransform[]
  parsed_destinations?: DTSXDestination[]
  parsed_variables?: Record<string, string>
}

export interface DTSXSource {
  name: string
  type: string
  connection_name?: string
  query?: string
  table_name?: string
  columns: Array<{ name: string; type: string }>
}

export interface DTSXTransform {
  name: string
  type: string
  config: Record<string, unknown>
}

export interface DTSXDestination {
  name: string
  type: string
  table_name?: string
}

// =============================================================================
// Fixed-Width / EBCDIC Configuration
// =============================================================================

export interface FieldSpec {
  name: string
  start: number
  end: number
  type?: 'string' | 'integer' | 'decimal' | 'date'
  format?: string  // for dates
  decimal_places?: number
}

export interface CopybookConfig {
  record_length: number
  fields: FieldSpec[]
}

export interface FixedWidthSourceConfig {
  source_type: 'file_fixed_width'
  gcs_path: string
  field_specs: FieldSpec[]
  encoding?: string
  skip_header_lines?: number
}

export interface EBCDICSourceConfig {
  source_type: 'file_ebcdic'
  gcs_path: string
  copybook: CopybookConfig
  encoding?: string  // cp037, cp500, etc.
}

export interface ExcelSourceConfig {
  source_type: 'file_excel'
  gcs_path: string
  sheet_name?: string
  skip_rows?: number
  use_cols?: string[]
  header_row?: number
}

// =============================================================================
// Extended Database Source with CDC
// =============================================================================

export interface DatabaseCDCSourceConfig {
  source_type: 'database_oracle' | 'database_sqlserver' | 'database_db2' | 'database_postgres' | 'database_mysql'
  connection_id: string
  source_schema: string
  source_table: string
  source_query?: string
  extraction_mode: ExtractionMode
  watermark_column?: string
  watermark_type?: 'timestamp' | 'integer'
  // CDC specific
  cdc_enabled?: boolean
  kafka_bootstrap_servers?: string
  kafka_topic?: string
  kafka_consumer_group?: string
}

// =============================================================================
// Extended Target Configuration
// =============================================================================

export interface ExtendedTargetConfig extends TargetConfig {
  destination_model: DestinationModel
  // Data Vault specific
  hub_table?: string
  sat_table?: string
  link_tables?: string[]
  hub_keys?: string[]
  satellite_attrs?: string[]
  // SCD specific
  scd_type?: 1 | 2
  tracked_columns?: string[]
  effective_from_column?: string
  effective_to_column?: string
  is_current_column?: string
}

// =============================================================================
// Natural Language Transformation
// =============================================================================

export interface NLTransformRule {
  rule_id: string
  rule_type: 'nl_transform'
  zone: 'bronze' | 'silver' | 'gold'
  nl_description: string
  generated_pyspark?: string
  generated_sql?: string
  confidence?: number
  is_validated?: boolean
}

export interface JoinTransformConfig {
  rule_type: 'join'
  left_table: string
  right_table: string
  right_source?: string  // GCS/BQ path to right table
  join_type: 'inner' | 'left' | 'right' | 'outer' | 'cross'
  join_keys: Array<{ left: string; right: string }>
  select_columns?: string[]
  output_table?: string
}

export interface AggregationTransformConfig {
  rule_type: 'aggregate'
  group_by: string[]
  aggregations: Array<{
    column: string
    function: 'sum' | 'count' | 'avg' | 'min' | 'max' | 'first' | 'last' | 'collect_list'
    alias: string
  }>
  having?: string
  order_by?: Array<{ column: string; direction: 'asc' | 'desc' }>
  output_table?: string
}

export interface WindowTransformConfig {
  rule_type: 'window'
  partition_by: string[]
  order_by: Array<{ column: string; direction: 'asc' | 'desc' }>
  functions: Array<{
    type: 'row_number' | 'rank' | 'dense_rank' | 'lag' | 'lead' | 'sum' | 'avg'
    column?: string
    offset?: number
    alias: string
  }>
  output_table?: string
}

export type ExtendedTransformRule =
  | TransformationRule
  | NLTransformRule
  | JoinTransformConfig
  | AggregationTransformConfig
  | WindowTransformConfig

// =============================================================================
// Enhanced Pipeline Intent
// =============================================================================

export type ExtendedSourceConfig =
  | SourceConfig
  | DTSXMigrationConfig
  | FixedWidthSourceConfig
  | EBCDICSourceConfig
  | ExcelSourceConfig
  | DatabaseCDCSourceConfig

export interface EnhancedPipelineIntent {
  request_id?: string
  pipeline_identity: PipelineIdentity
  source_config: ExtendedSourceConfig
  schema_definition: SchemaDefinition
  target_config: ExtendedTargetConfig
  transformation_rules: ExtendedTransformRule[]
  data_quality_rules: DataQualityRule[]
  execution_policy: ExecutionPolicy
  jira_ticket?: string
  created_by?: string
  // DTSX Migration specific
  dtsx_migration?: boolean
  // Natural Language transforms
  nl_transforms_enabled?: boolean
}

// =============================================================================
// Pipeline Templates
// =============================================================================

export interface PipelineTemplate {
  template_id: string
  template_name: string
  description: string
  category: 'file' | 'database' | 'streaming' | 'api' | 'migration'
  source_type: ExtendedSourceType
  icon?: string
  tags: string[]
  default_config: Partial<EnhancedPipelineIntent>
}

export const PIPELINE_TEMPLATES: PipelineTemplate[] = [
  {
    template_id: 'csv_to_bq',
    template_name: 'CSV to BigQuery',
    description: 'Ingest CSV files from GCS to BigQuery with quality checks',
    category: 'file',
    source_type: 'file_csv',
    icon: 'file-text',
    tags: ['csv', 'gcs', 'bigquery', 'batch'],
    default_config: {
      source_config: {
        source_type: 'file',
        file_format: 'csv',
        header: true,
      } as any,
      target_config: {
        target_zone: 'gold',
        write_mode: 'append',
        destination_model: 'flat',
      } as any,
    },
  },
  {
    template_id: 'postgres_cdc',
    template_name: 'PostgreSQL CDC',
    description: 'Real-time CDC from PostgreSQL using Debezium',
    category: 'database',
    source_type: 'database_postgres',
    icon: 'database',
    tags: ['postgres', 'cdc', 'debezium', 'real-time'],
    default_config: {
      source_config: {
        source_type: 'database_postgres',
        extraction_mode: 'cdc',
        cdc_enabled: true,
      } as any,
      target_config: {
        target_zone: 'gold',
        write_mode: 'merge',
        destination_model: 'flat',
      } as any,
    },
  },
  {
    template_id: 'ssis_migration',
    template_name: 'SSIS/DTSX Migration',
    description: 'Migrate SSIS packages to modern Airflow pipelines',
    category: 'migration',
    source_type: 'dtsx_migration',
    icon: 'arrow-right-circle',
    tags: ['ssis', 'dtsx', 'migration', 'legacy'],
    default_config: {
      dtsx_migration: true,
      target_config: {
        target_zone: 'gold',
        write_mode: 'append',
        destination_model: 'flat',
      } as any,
    },
  },
  {
    template_id: 'mainframe_ebcdic',
    template_name: 'Mainframe EBCDIC Ingest',
    description: 'Parse EBCDIC files using COBOL copybook definitions',
    category: 'file',
    source_type: 'file_ebcdic',
    icon: 'hard-drive',
    tags: ['mainframe', 'ebcdic', 'cobol', 'legacy'],
    default_config: {
      source_config: {
        source_type: 'file_ebcdic',
        encoding: 'cp037',
      } as any,
    },
  },
  {
    template_id: 'kafka_streaming',
    template_name: 'Kafka Streaming Ingest',
    description: 'Consume events from Kafka topics in micro-batches',
    category: 'streaming',
    source_type: 'streaming_kafka',
    icon: 'activity',
    tags: ['kafka', 'streaming', 'events', 'real-time'],
    default_config: {
      source_config: {
        source_type: 'streaming',
        starting_offsets: 'latest',
      } as any,
      execution_policy: {
        processing_mode: 'micro_batch',
        schedule: '*/5 * * * *',
      } as any,
    },
  },
  {
    template_id: 'data_vault',
    template_name: 'Data Vault Pipeline',
    description: 'Load data into Data Vault model (Hub + Satellite)',
    category: 'database',
    source_type: 'database_postgres',
    icon: 'layers',
    tags: ['data-vault', 'hub', 'satellite', 'historical'],
    default_config: {
      target_config: {
        destination_model: 'data_vault',
        write_mode: 'append',
      } as any,
    },
  },
  {
    template_id: 'scd_type_2',
    template_name: 'SCD Type 2 Dimension',
    description: 'Track historical changes with effective dates',
    category: 'database',
    source_type: 'database_postgres',
    icon: 'git-branch',
    tags: ['scd', 'dimension', 'history', 'slowly-changing'],
    default_config: {
      target_config: {
        write_mode: 'scd2',
        scd_type: 2,
        effective_from_column: 'effective_from',
        effective_to_column: 'effective_to',
        is_current_column: 'is_current',
      } as any,
    },
  },
  {
    template_id: 'rest_api',
    template_name: 'REST API Ingest',
    description: 'Paginated extraction from REST APIs',
    category: 'api',
    source_type: 'api_rest',
    icon: 'globe',
    tags: ['api', 'rest', 'pagination', 'http'],
    default_config: {
      source_config: {
        source_type: 'api',
        method: 'GET',
        pagination_type: 'offset',
      } as any,
    },
  },
]

// =============================================================================
// Form Helpers
// =============================================================================

export const SOURCE_TYPE_OPTIONS = [
  { value: 'file_csv', label: 'CSV File', category: 'File' },
  { value: 'file_json', label: 'JSON File', category: 'File' },
  { value: 'file_xml', label: 'XML File', category: 'File' },
  { value: 'file_parquet', label: 'Parquet File', category: 'File' },
  { value: 'file_excel', label: 'Excel File', category: 'File' },
  { value: 'file_fixed_width', label: 'Fixed-Width File', category: 'File' },
  { value: 'file_ebcdic', label: 'EBCDIC (Mainframe)', category: 'File' },
  { value: 'database_postgres', label: 'PostgreSQL', category: 'Database' },
  { value: 'database_mysql', label: 'MySQL', category: 'Database' },
  { value: 'database_oracle', label: 'Oracle', category: 'Database' },
  { value: 'database_sqlserver', label: 'SQL Server', category: 'Database' },
  { value: 'database_db2', label: 'IBM DB2', category: 'Database' },
  { value: 'streaming_kafka', label: 'Kafka', category: 'Streaming' },
  { value: 'streaming_pubsub', label: 'Google Pub/Sub', category: 'Streaming' },
  { value: 'api_rest', label: 'REST API', category: 'API' },
  { value: 'api_graphql', label: 'GraphQL API', category: 'API' },
  { value: 'dtsx_migration', label: 'SSIS/DTSX Migration', category: 'Migration' },
  { value: 'cdc_debezium', label: 'CDC (Debezium)', category: 'Streaming' },
]

export const DESTINATION_MODEL_OPTIONS = [
  { value: 'flat', label: 'Flat Table', description: 'Simple table structure' },
  { value: 'dimensional', label: 'Star Schema', description: 'Fact + Dimension tables' },
  { value: 'data_vault', label: 'Data Vault', description: 'Hub + Satellite + Link' },
  { value: 'obt', label: 'One Big Table (OBT)', description: 'Denormalized wide table' },
]

export const WRITE_MODE_OPTIONS = [
  { value: 'append', label: 'Append', description: 'Add new records to existing data' },
  { value: 'overwrite', label: 'Overwrite', description: 'Replace all existing data' },
  { value: 'merge', label: 'Merge (Upsert)', description: 'Update existing, insert new' },
  { value: 'scd2', label: 'SCD Type 2', description: 'Track history with effective dates' },
]
