/**
 * Pipeline Components - Barrel exports
 *
 * This file exports all pipeline-related components for easy importing.
 *
 * Usage:
 *   import { PipelineConfigForm, PipelineProgress, NLTransformInput } from '@/components/pipeline'
 */

export { PipelineConfigForm } from './PipelineConfigForm'
export { PipelineProgress } from './PipelineProgress'
export { default as NLTransformInput } from './NLTransformInput'
export { default as DTSXMigrationForm } from './DTSXMigrationForm'

// Re-export types from base pipeline
export type {
  PipelineIntent,
  PipelineIdentity,
  SourceConfig,
  SchemaDefinition,
  TargetConfig,
  TransformationRule,
  DataQualityRule,
  ExecutionPolicy,
  PipelineStatus,
  PipelinePhase,
  PipelineArtifacts,
  ValidationResult,
  DeploymentResult,
  PipelineFormState,
  PipelineFormData,
  PipelineEvent,
  CreatePipelineResponse,
  ApprovalResponse,
  TemplatesResponse,
  PipelineListItem,
  PipelineDetails,
} from '@/types/pipeline'

// Re-export enhanced types
export type {
  PipelineTemplate,
  ExtendedSourceConfig,
  ExtendedTransformRule,
  NLTransformRule,
  JoinTransformConfig,
  AggregationTransformConfig,
  WindowTransformConfig,
  DTSXMigrationConfig,
  EnhancedPipelineIntent,
  ExtendedSourceType,
  DestinationModel,
} from '@/types/pipeline-enhanced'

// Re-export constants
export { PIPELINE_TEMPLATES, SOURCE_TYPE_OPTIONS, DESTINATION_MODEL_OPTIONS, WRITE_MODE_OPTIONS } from '@/types/pipeline-enhanced'
