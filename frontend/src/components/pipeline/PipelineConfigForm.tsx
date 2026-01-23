'use client'

/**
 * PipelineConfigForm - Multi-step accordion form for pipeline configuration
 *
 * This component provides a comprehensive form for configuring data pipelines.
 * Features:
 * - 7-step accordion navigation
 * - Real-time validation
 * - Dynamic source type switching
 * - Schema column editor
 * - Transformation rule builder
 * - Data quality rule builder
 */

import React, { useState, useCallback, useMemo } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import {
  PipelineIntent,
  PipelineIdentity,
  SourceConfig,
  SchemaDefinition,
  TargetConfig,
  TransformationRule,
  DataQualityRule,
  ExecutionPolicy,
  ColumnDefinition,
  Environment,
  SourceType,
  ProcessingMode,
  TargetZone,
  DataType,
  PIPELINE_FORM_STEPS,
  DEFAULT_EXECUTION_POLICY,
  DEFAULT_COLUMN,
  DEFAULT_TARGET_CONFIG,
} from '@/types/pipeline'

// =============================================================================
// Component Props
// =============================================================================

interface PipelineConfigFormProps {
  jiraTicket?: string
  onSubmit: (intent: PipelineIntent) => Promise<void>
  onCancel?: () => void
  isSubmitting?: boolean
  className?: string
}

// =============================================================================
// Form State Types
// =============================================================================

interface FormState {
  identity: Partial<PipelineIdentity>
  source: Partial<SourceConfig>
  schema: SchemaDefinition
  target: Partial<TargetConfig>
  transformations: TransformationRule[]
  dataQuality: DataQualityRule[]
  execution: ExecutionPolicy
}

// =============================================================================
// Accordion Step Component
// =============================================================================

interface AccordionStepProps {
  step: number
  currentStep: number
  title: string
  description: string
  isComplete: boolean
  isOptional?: boolean
  onClick: () => void
  children: React.ReactNode
}

function AccordionStep({
  step,
  currentStep,
  title,
  description,
  isComplete,
  isOptional,
  onClick,
  children,
}: AccordionStepProps) {
  const isOpen = step === currentStep
  const isPast = step < currentStep

  return (
    <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={onClick}
        className={cn(
          'w-full px-4 py-3 flex items-center justify-between',
          'bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700',
          'transition-colors duration-150',
          isOpen && 'bg-blue-50 dark:bg-blue-900/20'
        )}
      >
        <div className="flex items-center gap-3">
          <div
            className={cn(
              'w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium',
              isComplete
                ? 'bg-green-500 text-white'
                : isOpen
                ? 'bg-blue-500 text-white'
                : 'bg-gray-300 dark:bg-gray-600 text-gray-600 dark:text-gray-300'
            )}
          >
            {isComplete ? '✓' : step + 1}
          </div>
          <div className="text-left">
            <div className="font-medium text-gray-900 dark:text-white flex items-center gap-2">
              {title}
              {isOptional && (
                <Badge variant="info" className="text-xs">
                  Optional
                </Badge>
              )}
            </div>
            <div className="text-sm text-gray-500 dark:text-gray-400">
              {description}
            </div>
          </div>
        </div>
        <svg
          className={cn(
            'w-5 h-5 text-gray-400 transition-transform duration-200',
            isOpen && 'rotate-180'
          )}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M19 9l-7 7-7-7"
          />
        </svg>
      </button>
      {isOpen && (
        <div className="px-4 py-4 bg-white dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700">
          {children}
        </div>
      )}
    </div>
  )
}

// =============================================================================
// Main Component
// =============================================================================

export function PipelineConfigForm({
  jiraTicket,
  onSubmit,
  onCancel,
  isSubmitting = false,
  className,
}: PipelineConfigFormProps) {
  // Form state
  const [currentStep, setCurrentStep] = useState(0)
  const [formState, setFormState] = useState<FormState>({
    identity: {
      jira_ticket: jiraTicket,
      environment: 'dev',
    },
    source: {
      source_type: 'file',
    },
    schema: {
      columns: [],
    },
    target: DEFAULT_TARGET_CONFIG,
    transformations: [],
    dataQuality: [],
    execution: DEFAULT_EXECUTION_POLICY,
  })
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Step completion status
  const stepCompletion = useMemo(() => {
    return PIPELINE_FORM_STEPS.map((step, index) => {
      switch (index) {
        case 0: // Identity
          return !!(
            formState.identity.pipeline_name &&
            formState.identity.domain &&
            formState.identity.owner_team &&
            formState.identity.owner_email
          )
        case 1: // Source
          return !!(
            formState.source.source_type &&
            ((formState.source.source_type === 'file' &&
              (formState.source as any).gcs_path) ||
              (formState.source.source_type === 'database' &&
                (formState.source as any).connection_name) ||
              (formState.source.source_type === 'api' &&
                (formState.source as any).endpoint_url) ||
              (formState.source.source_type === 'streaming' &&
                (formState.source as any).kafka_topic))
          )
        case 2: // Schema
          return formState.schema.columns.length > 0
        case 3: // Target
          return !!(
            formState.target.target_dataset && formState.target.target_table
          )
        case 4: // Transformations (optional)
          return true
        case 5: // Data Quality (optional)
          return true
        case 6: // Execution
          return !!formState.execution.schedule
        default:
          return false
      }
    })
  }, [formState])

  // Update form state helper
  const updateFormState = useCallback(
    <K extends keyof FormState>(section: K, updates: Partial<FormState[K]>) => {
      setFormState((prev) => ({
        ...prev,
        [section]: { ...prev[section], ...updates },
      }))
    },
    []
  )

  // Handle source type change
  const handleSourceTypeChange = useCallback((sourceType: SourceType) => {
    const baseConfig = { source_type: sourceType }
    setFormState((prev) => ({
      ...prev,
      source: baseConfig as any,
    }))
  }, [])

  // Add column
  const addColumn = useCallback(() => {
    setFormState((prev) => ({
      ...prev,
      schema: {
        ...prev.schema,
        columns: [...prev.schema.columns, { ...DEFAULT_COLUMN }],
      },
    }))
  }, [])

  // Update column
  const updateColumn = useCallback(
    (index: number, updates: Partial<ColumnDefinition>) => {
      setFormState((prev) => ({
        ...prev,
        schema: {
          ...prev.schema,
          columns: prev.schema.columns.map((col, i) =>
            i === index ? { ...col, ...updates } : col
          ),
        },
      }))
    },
    []
  )

  // Remove column
  const removeColumn = useCallback((index: number) => {
    setFormState((prev) => ({
      ...prev,
      schema: {
        ...prev.schema,
        columns: prev.schema.columns.filter((_, i) => i !== index),
      },
    }))
  }, [])

  // Add transformation rule
  const addTransformation = useCallback(() => {
    const newRule: TransformationRule = {
      rule_id: `tr_${Date.now()}`,
      rule_type: 'derive',
      target_column: '',
      expression: '',
    }
    setFormState((prev) => ({
      ...prev,
      transformations: [...prev.transformations, newRule],
    }))
  }, [])

  // Add data quality rule
  const addDataQualityRule = useCallback(() => {
    const newRule: DataQualityRule = {
      rule_id: `dq_${Date.now()}`,
      rule_type: 'not_null',
      column: '',
      severity: 'error',
    }
    setFormState((prev) => ({
      ...prev,
      dataQuality: [...prev.dataQuality, newRule],
    }))
  }, [])

  // Validate form
  const validateForm = useCallback((): boolean => {
    const newErrors: Record<string, string> = {}

    // Identity validation
    if (!formState.identity.pipeline_name) {
      newErrors['identity.pipeline_name'] = 'Pipeline name is required'
    }
    if (!formState.identity.domain) {
      newErrors['identity.domain'] = 'Domain is required'
    }
    if (!formState.identity.owner_team) {
      newErrors['identity.owner_team'] = 'Owner team is required'
    }
    if (!formState.identity.owner_email) {
      newErrors['identity.owner_email'] = 'Owner email is required'
    }

    // Source validation
    if (
      formState.source.source_type === 'file' &&
      !(formState.source as any).gcs_path
    ) {
      newErrors['source.gcs_path'] = 'GCS path is required'
    }

    // Schema validation
    if (formState.schema.columns.length === 0) {
      newErrors['schema.columns'] = 'At least one column is required'
    }

    // Target validation
    if (!formState.target.target_dataset) {
      newErrors['target.target_dataset'] = 'Target dataset is required'
    }
    if (!formState.target.target_table) {
      newErrors['target.target_table'] = 'Target table is required'
    }

    setErrors(newErrors)
    return Object.keys(newErrors).length === 0
  }, [formState])

  // Handle submit
  const handleSubmit = useCallback(async () => {
    if (!validateForm()) {
      return
    }

    const intent: PipelineIntent = {
      pipeline_identity: formState.identity as PipelineIdentity,
      source_config: formState.source as SourceConfig,
      schema_definition: formState.schema,
      target_config: formState.target as TargetConfig,
      transformation_rules: formState.transformations,
      data_quality_rules: formState.dataQuality,
      execution_policy: formState.execution,
      jira_ticket: jiraTicket,
    }

    await onSubmit(intent)
  }, [formState, jiraTicket, onSubmit, validateForm])

  // Navigate steps
  const goToStep = useCallback((step: number) => {
    setCurrentStep(step)
  }, [])

  const nextStep = useCallback(() => {
    if (currentStep < PIPELINE_FORM_STEPS.length - 1) {
      setCurrentStep((prev) => prev + 1)
    }
  }, [currentStep])

  const prevStep = useCallback(() => {
    if (currentStep > 0) {
      setCurrentStep((prev) => prev - 1)
    }
  }, [currentStep])

  return (
    <div className={cn('space-y-4', className)}>
      {/* Form Steps */}
      <div className="space-y-2">
        {/* Step 1: Pipeline Identity */}
        <AccordionStep
          step={0}
          currentStep={currentStep}
          title="Pipeline Identity"
          description="Name, domain, and ownership"
          isComplete={stepCompletion[0]}
          onClick={() => goToStep(0)}
        >
          <div className="grid grid-cols-2 gap-4">
            <Input
              label="Pipeline Name"
              placeholder="e.g., orders_daily_ingest"
              value={formState.identity.pipeline_name || ''}
              onChange={(e) =>
                updateFormState('identity', { pipeline_name: e.target.value })
              }
              error={errors['identity.pipeline_name']}
              required
            />
            <Input
              label="Domain"
              placeholder="e.g., sales"
              value={formState.identity.domain || ''}
              onChange={(e) =>
                updateFormState('identity', { domain: e.target.value })
              }
              error={errors['identity.domain']}
              required
            />
            <Input
              label="Subdomain"
              placeholder="e.g., orders"
              value={formState.identity.subdomain || ''}
              onChange={(e) =>
                updateFormState('identity', { subdomain: e.target.value })
              }
            />
            <Input
              label="Owner Team"
              placeholder="e.g., data-engineering"
              value={formState.identity.owner_team || ''}
              onChange={(e) =>
                updateFormState('identity', { owner_team: e.target.value })
              }
              error={errors['identity.owner_team']}
              required
            />
            <Input
              label="Owner Email"
              type="email"
              placeholder="team@company.com"
              value={formState.identity.owner_email || ''}
              onChange={(e) =>
                updateFormState('identity', { owner_email: e.target.value })
              }
              error={errors['identity.owner_email']}
              required
            />
            <Select
              label="Environment"
              value={formState.identity.environment || 'dev'}
              onChange={(e) =>
                updateFormState('identity', {
                  environment: e.target.value as Environment,
                })
              }
              options={[
                { value: 'dev', label: 'Development' },
                { value: 'staging', label: 'Staging' },
                { value: 'prod', label: 'Production' },
              ]}
            />
            <div className="col-span-2">
              <Input
                label="Description"
                placeholder="Brief description of this pipeline"
                value={formState.identity.description || ''}
                onChange={(e) =>
                  updateFormState('identity', { description: e.target.value })
                }
              />
            </div>
          </div>
          <div className="mt-4 flex justify-end">
            <Button onClick={nextStep}>Next</Button>
          </div>
        </AccordionStep>

        {/* Step 2: Source Configuration */}
        <AccordionStep
          step={1}
          currentStep={currentStep}
          title="Source Configuration"
          description="Data source and connection"
          isComplete={stepCompletion[1]}
          onClick={() => goToStep(1)}
        >
          <div className="space-y-4">
            <Select
              label="Source Type"
              value={formState.source.source_type || 'file'}
              onChange={(e) =>
                handleSourceTypeChange(e.target.value as SourceType)
              }
              options={[
                { value: 'file', label: 'File (GCS)' },
                { value: 'database', label: 'Database' },
                { value: 'api', label: 'REST API' },
                { value: 'streaming', label: 'Streaming (Kafka)' },
              ]}
            />

            {/* File Source Fields */}
            {formState.source.source_type === 'file' && (
              <div className="grid grid-cols-2 gap-4">
                <div className="col-span-2">
                  <Input
                    label="GCS Path"
                    placeholder="gs://bucket/path/to/data"
                    value={(formState.source as any).gcs_path || ''}
                    onChange={(e) =>
                      updateFormState('source', { gcs_path: e.target.value } as any)
                    }
                    error={errors['source.gcs_path']}
                    required
                  />
                </div>
                <Select
                  label="File Format"
                  value={(formState.source as any).file_format || 'parquet'}
                  onChange={(e) =>
                    updateFormState('source', { file_format: e.target.value } as any)
                  }
                  options={[
                    { value: 'parquet', label: 'Parquet' },
                    { value: 'json', label: 'JSON' },
                    { value: 'csv', label: 'CSV' },
                    { value: 'avro', label: 'Avro' },
                    { value: 'orc', label: 'ORC' },
                  ]}
                />
                <Select
                  label="Compression"
                  value={(formState.source as any).compression || 'snappy'}
                  onChange={(e) =>
                    updateFormState('source', { compression: e.target.value } as any)
                  }
                  options={[
                    { value: 'none', label: 'None' },
                    { value: 'gzip', label: 'Gzip' },
                    { value: 'snappy', label: 'Snappy' },
                    { value: 'lz4', label: 'LZ4' },
                    { value: 'zstd', label: 'Zstd' },
                  ]}
                />
              </div>
            )}

            {/* Database Source Fields */}
            {formState.source.source_type === 'database' && (
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Connection Name"
                  placeholder="e.g., postgres_prod"
                  value={(formState.source as any).connection_name || ''}
                  onChange={(e) =>
                    updateFormState('source', {
                      connection_name: e.target.value,
                    } as any)
                  }
                  required
                />
                <Input
                  label="Database"
                  placeholder="e.g., mydb"
                  value={(formState.source as any).database || ''}
                  onChange={(e) =>
                    updateFormState('source', { database: e.target.value } as any)
                  }
                  required
                />
                <Input
                  label="Schema"
                  placeholder="e.g., public"
                  value={(formState.source as any).schema || ''}
                  onChange={(e) =>
                    updateFormState('source', { schema: e.target.value } as any)
                  }
                  required
                />
                <Input
                  label="Table"
                  placeholder="e.g., orders"
                  value={(formState.source as any).table || ''}
                  onChange={(e) =>
                    updateFormState('source', { table: e.target.value } as any)
                  }
                  required
                />
                <div className="col-span-2 flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="cdc_enabled"
                    checked={(formState.source as any).cdc_enabled || false}
                    onChange={(e) =>
                      updateFormState('source', {
                        cdc_enabled: e.target.checked,
                      } as any)
                    }
                    className="h-4 w-4 rounded border-gray-300"
                  />
                  <label htmlFor="cdc_enabled" className="text-sm text-gray-700 dark:text-gray-300">
                    Enable CDC (Change Data Capture)
                  </label>
                </div>
              </div>
            )}

            {/* API Source Fields */}
            {formState.source.source_type === 'api' && (
              <div className="grid grid-cols-2 gap-4">
                <div className="col-span-2">
                  <Input
                    label="Endpoint URL"
                    placeholder="https://api.example.com/data"
                    value={(formState.source as any).endpoint_url || ''}
                    onChange={(e) =>
                      updateFormState('source', { endpoint_url: e.target.value } as any)
                    }
                    required
                  />
                </div>
                <Select
                  label="HTTP Method"
                  value={(formState.source as any).method || 'GET'}
                  onChange={(e) =>
                    updateFormState('source', { method: e.target.value } as any)
                  }
                  options={[
                    { value: 'GET', label: 'GET' },
                    { value: 'POST', label: 'POST' },
                  ]}
                />
                <Select
                  label="Authentication"
                  value={(formState.source as any).auth_type || 'none'}
                  onChange={(e) =>
                    updateFormState('source', { auth_type: e.target.value } as any)
                  }
                  options={[
                    { value: 'none', label: 'None' },
                    { value: 'api_key', label: 'API Key' },
                    { value: 'oauth2', label: 'OAuth2' },
                    { value: 'basic', label: 'Basic Auth' },
                  ]}
                />
              </div>
            )}

            {/* Streaming Source Fields */}
            {formState.source.source_type === 'streaming' && (
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Kafka Topic"
                  placeholder="e.g., orders-events"
                  value={(formState.source as any).kafka_topic || ''}
                  onChange={(e) =>
                    updateFormState('source', { kafka_topic: e.target.value } as any)
                  }
                  required
                />
                <Input
                  label="Bootstrap Servers"
                  placeholder="kafka:9092"
                  value={(formState.source as any).kafka_bootstrap_servers || ''}
                  onChange={(e) =>
                    updateFormState('source', {
                      kafka_bootstrap_servers: e.target.value,
                    } as any)
                  }
                  required
                />
                <Input
                  label="Consumer Group"
                  placeholder="e.g., pipeline-consumer"
                  value={(formState.source as any).consumer_group || ''}
                  onChange={(e) =>
                    updateFormState('source', { consumer_group: e.target.value } as any)
                  }
                  required
                />
                <Select
                  label="Starting Offsets"
                  value={(formState.source as any).starting_offsets || 'latest'}
                  onChange={(e) =>
                    updateFormState('source', { starting_offsets: e.target.value } as any)
                  }
                  options={[
                    { value: 'latest', label: 'Latest' },
                    { value: 'earliest', label: 'Earliest' },
                  ]}
                />
              </div>
            )}
          </div>
          <div className="mt-4 flex justify-between">
            <Button variant="outline" onClick={prevStep}>
              Previous
            </Button>
            <Button onClick={nextStep}>Next</Button>
          </div>
        </AccordionStep>

        {/* Step 3: Schema Definition */}
        <AccordionStep
          step={2}
          currentStep={currentStep}
          title="Schema Definition"
          description="Column definitions and types"
          isComplete={stepCompletion[2]}
          onClick={() => goToStep(2)}
        >
          <div className="space-y-4">
            {errors['schema.columns'] && (
              <div className="text-red-500 text-sm">{errors['schema.columns']}</div>
            )}

            {/* Column List */}
            <div className="space-y-2">
              {formState.schema.columns.map((column, index) => (
                <div
                  key={index}
                  className="flex items-center gap-2 p-2 bg-gray-50 dark:bg-gray-800 rounded"
                >
                  <Input
                    placeholder="Column name"
                    value={column.name}
                    onChange={(e) => updateColumn(index, { name: e.target.value })}
                    className="flex-1"
                  />
                  <Select
                    value={column.data_type}
                    onChange={(e) =>
                      updateColumn(index, { data_type: e.target.value as DataType })
                    }
                    options={[
                      { value: 'string', label: 'String' },
                      { value: 'integer', label: 'Integer' },
                      { value: 'long', label: 'Long' },
                      { value: 'float', label: 'Float' },
                      { value: 'double', label: 'Double' },
                      { value: 'boolean', label: 'Boolean' },
                      { value: 'date', label: 'Date' },
                      { value: 'timestamp', label: 'Timestamp' },
                      { value: 'decimal', label: 'Decimal' },
                    ]}
                    className="w-32"
                  />
                  <div className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={column.nullable}
                      onChange={(e) =>
                        updateColumn(index, { nullable: e.target.checked })
                      }
                      title="Nullable"
                    />
                    <span className="text-xs text-gray-500">Null</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      type="checkbox"
                      checked={column.pii}
                      onChange={(e) => updateColumn(index, { pii: e.target.checked })}
                      title="PII"
                    />
                    <span className="text-xs text-gray-500">PII</span>
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => removeColumn(index)}
                    className="text-red-500 hover:text-red-700"
                  >
                    ✕
                  </Button>
                </div>
              ))}
            </div>

            <Button variant="outline" onClick={addColumn}>
              + Add Column
            </Button>
          </div>
          <div className="mt-4 flex justify-between">
            <Button variant="outline" onClick={prevStep}>
              Previous
            </Button>
            <Button onClick={nextStep}>Next</Button>
          </div>
        </AccordionStep>

        {/* Step 4: Target Configuration */}
        <AccordionStep
          step={3}
          currentStep={currentStep}
          title="Target Configuration"
          description="Destination and write mode"
          isComplete={stepCompletion[3]}
          onClick={() => goToStep(3)}
        >
          <div className="grid grid-cols-2 gap-4">
            <Select
              label="Target Zone"
              value={formState.target.target_zone || 'bronze'}
              onChange={(e) =>
                updateFormState('target', {
                  target_zone: e.target.value as TargetZone,
                })
              }
              options={[
                { value: 'bronze', label: 'Bronze (Raw)' },
                { value: 'silver', label: 'Silver (Cleansed)' },
                { value: 'gold', label: 'Gold (Aggregated)' },
              ]}
            />
            <Select
              label="Write Mode"
              value={formState.target.write_mode || 'append'}
              onChange={(e) =>
                updateFormState('target', { write_mode: e.target.value as any })
              }
              options={[
                { value: 'append', label: 'Append' },
                { value: 'overwrite', label: 'Overwrite' },
                { value: 'merge', label: 'Merge (Upsert)' },
                { value: 'scd2', label: 'SCD Type 2' },
              ]}
            />
            <Input
              label="Target Dataset"
              placeholder="e.g., raw_sales"
              value={formState.target.target_dataset || ''}
              onChange={(e) =>
                updateFormState('target', { target_dataset: e.target.value })
              }
              error={errors['target.target_dataset']}
              required
            />
            <Input
              label="Target Table"
              placeholder="e.g., orders"
              value={formState.target.target_table || ''}
              onChange={(e) =>
                updateFormState('target', { target_table: e.target.value })
              }
              error={errors['target.target_table']}
              required
            />
            {(formState.target.write_mode === 'merge' ||
              formState.target.write_mode === 'scd2') && (
              <div className="col-span-2">
                <Input
                  label="Merge Keys (comma-separated)"
                  placeholder="e.g., order_id, customer_id"
                  value={(formState.target.merge_keys || []).join(', ')}
                  onChange={(e) =>
                    updateFormState('target', {
                      merge_keys: e.target.value
                        .split(',')
                        .map((k) => k.trim())
                        .filter(Boolean),
                    })
                  }
                />
              </div>
            )}
          </div>
          <div className="mt-4 flex justify-between">
            <Button variant="outline" onClick={prevStep}>
              Previous
            </Button>
            <Button onClick={nextStep}>Next</Button>
          </div>
        </AccordionStep>

        {/* Step 5: Transformations */}
        <AccordionStep
          step={4}
          currentStep={currentStep}
          title="Transformations"
          description="Data transformations"
          isComplete={stepCompletion[4]}
          isOptional
          onClick={() => goToStep(4)}
        >
          <div className="space-y-4">
            {formState.transformations.map((rule, index) => (
              <div
                key={rule.rule_id}
                className="p-3 bg-gray-50 dark:bg-gray-800 rounded space-y-2"
              >
                <div className="flex items-center gap-2">
                  <Select
                    value={rule.rule_type}
                    onChange={(e) => {
                      const newRules = [...formState.transformations]
                      newRules[index] = { ...rule, rule_type: e.target.value as any }
                      setFormState((prev) => ({
                        ...prev,
                        transformations: newRules,
                      }))
                    }}
                    options={[
                      { value: 'rename', label: 'Rename' },
                      { value: 'cast', label: 'Cast' },
                      { value: 'derive', label: 'Derive' },
                      { value: 'filter', label: 'Filter' },
                      { value: 'custom', label: 'Custom SQL' },
                    ]}
                    className="w-32"
                  />
                  <Input
                    placeholder="Target column"
                    value={rule.target_column}
                    onChange={(e) => {
                      const newRules = [...formState.transformations]
                      newRules[index] = { ...rule, target_column: e.target.value }
                      setFormState((prev) => ({
                        ...prev,
                        transformations: newRules,
                      }))
                    }}
                    className="flex-1"
                  />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setFormState((prev) => ({
                        ...prev,
                        transformations: prev.transformations.filter(
                          (_, i) => i !== index
                        ),
                      }))
                    }}
                    className="text-red-500"
                  >
                    ✕
                  </Button>
                </div>
                <Input
                  placeholder="Expression (e.g., UPPER(column_name))"
                  value={rule.expression || ''}
                  onChange={(e) => {
                    const newRules = [...formState.transformations]
                    newRules[index] = { ...rule, expression: e.target.value }
                    setFormState((prev) => ({
                      ...prev,
                      transformations: newRules,
                    }))
                  }}
                />
              </div>
            ))}
            <Button variant="outline" onClick={addTransformation}>
              + Add Transformation
            </Button>
          </div>
          <div className="mt-4 flex justify-between">
            <Button variant="outline" onClick={prevStep}>
              Previous
            </Button>
            <Button onClick={nextStep}>Next</Button>
          </div>
        </AccordionStep>

        {/* Step 6: Data Quality */}
        <AccordionStep
          step={5}
          currentStep={currentStep}
          title="Data Quality"
          description="Validation rules"
          isComplete={stepCompletion[5]}
          isOptional
          onClick={() => goToStep(5)}
        >
          <div className="space-y-4">
            {formState.dataQuality.map((rule, index) => (
              <div
                key={rule.rule_id}
                className="p-3 bg-gray-50 dark:bg-gray-800 rounded space-y-2"
              >
                <div className="flex items-center gap-2">
                  <Select
                    value={rule.rule_type}
                    onChange={(e) => {
                      const newRules = [...formState.dataQuality]
                      newRules[index] = { ...rule, rule_type: e.target.value as any }
                      setFormState((prev) => ({
                        ...prev,
                        dataQuality: newRules,
                      }))
                    }}
                    options={[
                      { value: 'not_null', label: 'Not Null' },
                      { value: 'unique', label: 'Unique' },
                      { value: 'range', label: 'Range Check' },
                      { value: 'regex', label: 'Regex Pattern' },
                      { value: 'custom', label: 'Custom' },
                    ]}
                    className="w-32"
                  />
                  <Input
                    placeholder="Column"
                    value={rule.column}
                    onChange={(e) => {
                      const newRules = [...formState.dataQuality]
                      newRules[index] = { ...rule, column: e.target.value }
                      setFormState((prev) => ({
                        ...prev,
                        dataQuality: newRules,
                      }))
                    }}
                    className="flex-1"
                  />
                  <Select
                    value={rule.severity}
                    onChange={(e) => {
                      const newRules = [...formState.dataQuality]
                      newRules[index] = { ...rule, severity: e.target.value as any }
                      setFormState((prev) => ({
                        ...prev,
                        dataQuality: newRules,
                      }))
                    }}
                    options={[
                      { value: 'error', label: 'Error' },
                      { value: 'warning', label: 'Warning' },
                    ]}
                    className="w-28"
                  />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setFormState((prev) => ({
                        ...prev,
                        dataQuality: prev.dataQuality.filter((_, i) => i !== index),
                      }))
                    }}
                    className="text-red-500"
                  >
                    ✕
                  </Button>
                </div>
              </div>
            ))}
            <Button variant="outline" onClick={addDataQualityRule}>
              + Add Data Quality Rule
            </Button>
          </div>
          <div className="mt-4 flex justify-between">
            <Button variant="outline" onClick={prevStep}>
              Previous
            </Button>
            <Button onClick={nextStep}>Next</Button>
          </div>
        </AccordionStep>

        {/* Step 7: Execution Policy */}
        <AccordionStep
          step={6}
          currentStep={currentStep}
          title="Execution Policy"
          description="Schedule and settings"
          isComplete={stepCompletion[6]}
          onClick={() => goToStep(6)}
        >
          <div className="grid grid-cols-2 gap-4">
            <Input
              label="Schedule (Cron)"
              placeholder="e.g., @daily or 0 0 * * *"
              value={formState.execution.schedule}
              onChange={(e) =>
                updateFormState('execution', { schedule: e.target.value })
              }
            />
            <Select
              label="Processing Mode"
              value={formState.execution.processing_mode}
              onChange={(e) =>
                updateFormState('execution', {
                  processing_mode: e.target.value as ProcessingMode,
                })
              }
              options={[
                { value: 'batch', label: 'Batch' },
                { value: 'micro_batch', label: 'Micro-batch' },
                { value: 'streaming', label: 'Streaming' },
              ]}
            />
            <Input
              label="Retry Count"
              type="number"
              value={formState.execution.retry_count}
              onChange={(e) =>
                updateFormState('execution', {
                  retry_count: parseInt(e.target.value) || 0,
                })
              }
            />
            <Input
              label="Retry Delay (seconds)"
              type="number"
              value={formState.execution.retry_delay_seconds}
              onChange={(e) =>
                updateFormState('execution', {
                  retry_delay_seconds: parseInt(e.target.value) || 0,
                })
              }
            />
            <Input
              label="Timeout (seconds)"
              type="number"
              value={formState.execution.timeout_seconds}
              onChange={(e) =>
                updateFormState('execution', {
                  timeout_seconds: parseInt(e.target.value) || 0,
                })
              }
            />
            <Input
              label="SLA (hours)"
              type="number"
              value={formState.execution.sla_hours || ''}
              onChange={(e) =>
                updateFormState('execution', {
                  sla_hours: parseInt(e.target.value) || undefined,
                })
              }
            />
            <Input
              label="Alerting Email"
              type="email"
              placeholder="alerts@company.com"
              value={formState.execution.alerting_email || ''}
              onChange={(e) =>
                updateFormState('execution', { alerting_email: e.target.value })
              }
            />
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="human_approval"
                  checked={formState.execution.human_approval_required}
                  onChange={(e) =>
                    updateFormState('execution', {
                      human_approval_required: e.target.checked,
                    })
                  }
                  className="h-4 w-4 rounded border-gray-300"
                />
                <label htmlFor="human_approval" className="text-sm text-gray-700 dark:text-gray-300">
                  Require Human Approval
                </label>
              </div>
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="dry_run"
                  checked={formState.execution.dry_run || false}
                  onChange={(e) =>
                    updateFormState('execution', { dry_run: e.target.checked })
                  }
                  className="h-4 w-4 rounded border-gray-300"
                />
                <label htmlFor="dry_run" className="text-sm text-gray-700 dark:text-gray-300">
                  Dry Run
                </label>
              </div>
            </div>
          </div>
          <div className="mt-4 flex justify-between">
            <Button variant="outline" onClick={prevStep}>
              Previous
            </Button>
          </div>
        </AccordionStep>
      </div>

      {/* Form Actions */}
      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="text-sm text-gray-500 dark:text-gray-400">
            {stepCompletion.filter(Boolean).length} of {PIPELINE_FORM_STEPS.length} steps complete
          </div>
          <div className="flex gap-2">
            {onCancel && (
              <Button variant="outline" onClick={onCancel} disabled={isSubmitting}>
                Cancel
              </Button>
            )}
            <Button
              onClick={handleSubmit}
              disabled={isSubmitting || !stepCompletion.every((s, i) => s || PIPELINE_FORM_STEPS[i].isOptional)}
              isLoading={isSubmitting}
            >
              {isSubmitting ? 'Creating Pipeline...' : 'Create Pipeline'}
            </Button>
          </div>
        </div>
      </Card>
    </div>
  )
}

export default PipelineConfigForm
