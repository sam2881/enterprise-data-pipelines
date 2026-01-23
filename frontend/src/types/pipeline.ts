/**
 * Pipeline Types - TypeScript definitions for the Data Agent Pipeline system
 *
 * These types mirror the backend Pydantic models and LangGraph state.
 * Used for pipeline configuration, status tracking, and real-time updates.
 */

// =============================================================================
// Pipeline Status Types
// =============================================================================

export type PipelinePhase =
  | 'init'
  | 'planning'
  | 'generating'
  | 'validating'
  | 'awaiting_approval'
  | 'deploying'
  | 'complete'
  | 'failed'

export type PipelineAction =
  | 'create_new'
  | 'upgrade_schema'
  | 'no_change'
  | 'recreate'

export type Environment = 'dev' | 'staging' | 'prod'

export type SourceType = 'file' | 'database' | 'api' | 'streaming'

export type ProcessingMode = 'batch' | 'micro_batch' | 'streaming'

export type TargetZone = 'bronze' | 'silver' | 'gold'

export type DataType =
  | 'string'
  | 'integer'
  | 'long'
  | 'float'
  | 'double'
  | 'boolean'
  | 'date'
  | 'timestamp'
  | 'decimal'
  | 'array'
  | 'struct'
  | 'map'

// =============================================================================
// Pipeline Identity
// =============================================================================

export interface PipelineIdentity {
  pipeline_name: string
  domain: string
  subdomain?: string
  owner_team: string
  owner_email: string
  environment: Environment
  jira_ticket?: string
  description?: string
  tags?: string[]
}

// =============================================================================
// Source Configuration
// =============================================================================

export interface FileSourceConfig {
  source_type: 'file'
  gcs_path: string
  file_format: 'parquet' | 'json' | 'csv' | 'avro' | 'orc'
  compression?: 'gzip' | 'snappy' | 'lz4' | 'zstd' | 'none'
  delimiter?: string
  header?: boolean
  partition_columns?: string[]
}

export interface DatabaseSourceConfig {
  source_type: 'database'
  connection_name: string
  database: string
  schema: string
  table: string
  cdc_enabled: boolean
  cdc_column?: string
  watermark_column?: string
  fetch_size?: number
}

export interface ApiSourceConfig {
  source_type: 'api'
  endpoint_url: string
  method: 'GET' | 'POST'
  auth_type: 'oauth2' | 'api_key' | 'basic' | 'none'
  auth_secret_name?: string
  pagination_type?: 'offset' | 'cursor' | 'link' | 'none'
  rate_limit_per_second?: number
}

export interface StreamingSourceConfig {
  source_type: 'streaming'
  kafka_topic: string
  kafka_bootstrap_servers: string
  consumer_group: string
  starting_offsets: 'earliest' | 'latest'
  schema_registry_url?: string
}

export type SourceConfig =
  | FileSourceConfig
  | DatabaseSourceConfig
  | ApiSourceConfig
  | StreamingSourceConfig

// =============================================================================
// Schema Definition
// =============================================================================

export interface ColumnDefinition {
  name: string
  data_type: DataType
  nullable: boolean
  description?: string
  pii: boolean
  primary_key?: boolean
  precision?: number
  scale?: number
}

export interface SchemaDefinition {
  columns: ColumnDefinition[]
  partition_columns?: string[]
  clustering_columns?: string[]
  schema_version?: string
}

// =============================================================================
// Target Configuration
// =============================================================================

export interface TargetConfig {
  target_zone: TargetZone
  target_dataset: string
  target_table: string
  write_mode: 'append' | 'overwrite' | 'merge' | 'scd2'
  merge_keys?: string[]
  scd2_columns?: string[]
  partition_by?: string[]
  cluster_by?: string[]
}

// =============================================================================
// Transformation Rules
// =============================================================================

export interface TransformationRule {
  rule_id: string
  rule_type: 'rename' | 'cast' | 'derive' | 'filter' | 'aggregate' | 'join' | 'custom'
  source_column?: string
  target_column: string
  expression?: string
  description?: string
}

// =============================================================================
// Data Quality Rules
// =============================================================================

export interface DataQualityRule {
  rule_id: string
  rule_type: 'not_null' | 'unique' | 'range' | 'regex' | 'referential' | 'custom'
  column: string
  expression?: string
  threshold?: number
  severity: 'error' | 'warning'
  description?: string
}

// =============================================================================
// Execution Policy
// =============================================================================

export interface ExecutionPolicy {
  schedule: string
  processing_mode: ProcessingMode
  retry_count: number
  retry_delay_seconds: number
  timeout_seconds: number
  sla_hours?: number
  alerting_email?: string
  human_approval_required: boolean
  dry_run?: boolean
}

// =============================================================================
// Pipeline Intent (Main Request Object)
// =============================================================================

export interface PipelineIntent {
  request_id?: string
  pipeline_identity: PipelineIdentity
  source_config: SourceConfig
  schema_definition: SchemaDefinition
  target_config: TargetConfig
  transformation_rules: TransformationRule[]
  data_quality_rules: DataQualityRule[]
  execution_policy: ExecutionPolicy
  jira_ticket?: string
  created_by?: string
}

// =============================================================================
// Pipeline Status (Response Object)
// =============================================================================

export interface PipelineStatus {
  request_id: string
  pipeline_id?: number
  status: PipelinePhase
  phase: PipelinePhase
  message: string
  progress_pct: number
  timestamp: string
  error?: string
  started_at?: string
  completed_at?: string
  human_approval_required: boolean
  human_approval_received: boolean
  approved_by?: string
  approved_at?: string
}

// =============================================================================
// Pipeline Artifacts
// =============================================================================

export interface GeneratedArtifact {
  artifact_type: 'dag' | 'spark_bronze' | 'spark_silver' | 'spark_gold' | 'sql' | 'dq_config'
  file_name: string
  content: string
  template_used: string
}

export interface PipelineArtifacts {
  dag_code: string
  spark_jobs: Record<string, string>
  sql_scripts?: Record<string, string>
  dq_config?: Record<string, unknown>
  artifacts_list: GeneratedArtifact[]
}

// =============================================================================
// Validation Results
// =============================================================================

export interface ValidationError {
  artifact: string
  error_type: 'syntax' | 'security' | 'schema' | 'logic'
  message: string
  line?: number
  severity: 'error' | 'warning'
}

export interface ValidationResult {
  is_valid: boolean
  errors: ValidationError[]
  warnings: ValidationError[]
  security_scan_passed: boolean
  schema_compatible: boolean
}

// =============================================================================
// Deployment Result
// =============================================================================

export interface DeploymentResult {
  success: boolean
  branch_name: string
  commit_sha?: string
  pr_url?: string
  pr_number?: number
  ci_triggered: boolean
  deployed_at: string
  error_message?: string
}

// =============================================================================
// Form State Types (for UI)
// =============================================================================

export interface PipelineFormState {
  currentStep: number
  isSubmitting: boolean
  isValid: boolean
  errors: Record<string, string>
  touched: Record<string, boolean>
}

export interface PipelineFormData {
  identity: Partial<PipelineIdentity>
  source: Partial<SourceConfig>
  schema: Partial<SchemaDefinition>
  target: Partial<TargetConfig>
  transformations: TransformationRule[]
  dataQuality: DataQualityRule[]
  execution: Partial<ExecutionPolicy>
}

// =============================================================================
// WebSocket Events
// =============================================================================

export interface PipelineStatusEvent {
  event: 'status_update'
  data: PipelineStatus
}

export interface PipelineErrorEvent {
  event: 'error'
  data: {
    request_id: string
    error: string
  }
}

export interface PipelineCompleteEvent {
  event: 'complete'
  data: {
    request_id: string
    pipeline_id: number
    deployment: DeploymentResult
  }
}

export type PipelineEvent =
  | PipelineStatusEvent
  | PipelineErrorEvent
  | PipelineCompleteEvent

// =============================================================================
// API Response Types
// =============================================================================

export interface CreatePipelineResponse {
  request_id: string
  status: PipelinePhase
  pipeline_id?: number
  message: string
}

export interface ApprovalResponse {
  status: 'approval_processed'
  approved: boolean
  approver: string
  message: string
}

export interface TemplatesResponse {
  dag_templates: string[]
  spark_templates: string[]
}

// =============================================================================
// Helper Types
// =============================================================================

export interface PipelineListItem {
  request_id: string
  pipeline_name: string
  domain: string
  environment: Environment
  status: PipelinePhase
  progress_pct: number
  created_at: string
  owner_email: string
}

export interface PipelineDetails extends PipelineStatus {
  intent: PipelineIntent
  artifacts?: PipelineArtifacts
  validation?: ValidationResult
  deployment?: DeploymentResult
}

// =============================================================================
// Form Step Configuration
// =============================================================================

export interface FormStepConfig {
  id: string
  title: string
  description: string
  icon: string
  isOptional?: boolean
}

export const PIPELINE_FORM_STEPS: FormStepConfig[] = [
  {
    id: 'identity',
    title: 'Pipeline Identity',
    description: 'Name, domain, and ownership',
    icon: 'identification',
  },
  {
    id: 'source',
    title: 'Source Configuration',
    description: 'Data source and connection',
    icon: 'database',
  },
  {
    id: 'schema',
    title: 'Schema Definition',
    description: 'Column definitions and types',
    icon: 'table',
  },
  {
    id: 'target',
    title: 'Target Configuration',
    description: 'Destination and write mode',
    icon: 'cloud-upload',
  },
  {
    id: 'transformations',
    title: 'Transformations',
    description: 'Data transformations',
    icon: 'code',
    isOptional: true,
  },
  {
    id: 'dataQuality',
    title: 'Data Quality',
    description: 'Validation rules',
    icon: 'shield-check',
    isOptional: true,
  },
  {
    id: 'execution',
    title: 'Execution Policy',
    description: 'Schedule and settings',
    icon: 'clock',
  },
]

// =============================================================================
// Default Values
// =============================================================================

export const DEFAULT_EXECUTION_POLICY: ExecutionPolicy = {
  schedule: '@daily',
  processing_mode: 'batch',
  retry_count: 3,
  retry_delay_seconds: 300,
  timeout_seconds: 3600,
  sla_hours: 4,
  human_approval_required: false,
  dry_run: false,
}

export const DEFAULT_COLUMN: ColumnDefinition = {
  name: '',
  data_type: 'string',
  nullable: true,
  pii: false,
}

export const DEFAULT_TARGET_CONFIG: Partial<TargetConfig> = {
  target_zone: 'bronze',
  write_mode: 'append',
}
