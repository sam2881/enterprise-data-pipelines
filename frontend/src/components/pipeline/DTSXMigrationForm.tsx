'use client'

/**
 * DTSXMigrationForm - SSIS/DTSX Migration Configuration Component
 *
 * Allows users to migrate SSIS packages to modern Airflow pipelines.
 * Features:
 * - Upload/specify DTSX file location
 * - Parse and preview SSIS components
 * - Map SSIS connections to Airflow connections
 * - Select destination data model
 */

import React, { useState, useCallback } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import {
  DTSXMigrationConfig,
  DTSXSource,
  DTSXTransform,
  DTSXDestination,
  DESTINATION_MODEL_OPTIONS,
} from '@/types/pipeline-enhanced'

// =============================================================================
// Props
// =============================================================================

interface DTSXMigrationFormProps {
  onChange: (config: Partial<DTSXMigrationConfig>) => void
  className?: string
}

interface ParsedPackage {
  package_name: string
  sources: DTSXSource[]
  transformations: DTSXTransform[]
  destinations: DTSXDestination[]
  connections: Record<string, { type: string; server?: string; database?: string }>
  variables: Record<string, string>
  sql_tasks: Array<{ name: string; sql?: string }>
}

// =============================================================================
// Main Component
// =============================================================================

export function DTSXMigrationForm({ onChange, className }: DTSXMigrationFormProps) {
  const [dtsx_location, setDtsxLocation] = useState('')
  const [original_server, setOriginalServer] = useState('')
  const [isParsing, setIsParsing] = useState(false)
  const [parsedPackage, setParsedPackage] = useState<ParsedPackage | null>(null)
  const [parseError, setParseError] = useState<string | null>(null)
  const [connectionMapping, setConnectionMapping] = useState<Record<string, string>>({})
  const [destinationModel, setDestinationModel] = useState<string>('flat')

  // Available Airflow connections (would come from API in production)
  const availableConnections = [
    { id: 'postgres_default', label: 'PostgreSQL Default' },
    { id: 'mssql_legacy', label: 'SQL Server Legacy' },
    { id: 'oracle_prod', label: 'Oracle Production' },
    { id: 'bigquery_default', label: 'BigQuery Default' },
    { id: 'gcs_raw', label: 'GCS Raw Data' },
    { id: 'gcs_processed', label: 'GCS Processed' },
  ]

  // Parse DTSX file
  const parseDTSX = useCallback(async () => {
    if (!dtsx_location.trim()) return

    setIsParsing(true)
    setParseError(null)

    try {
      const response = await fetch('/api/v2/data-agent/parse-dtsx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dtsx_location }),
      })

      if (!response.ok) {
        throw new Error('Failed to parse DTSX file')
      }

      const result = await response.json()
      setParsedPackage(result)

      // Initialize connection mapping with empty values
      const initialMapping: Record<string, string> = {}
      Object.keys(result.connections || {}).forEach((conn) => {
        initialMapping[conn] = ''
      })
      setConnectionMapping(initialMapping)

      // Update parent with parsed data
      onChange({
        source_type: 'dtsx_migration',
        dtsx_location,
        dtsx_package_name: result.package_name,
        original_server,
        parsed_sources: result.sources,
        parsed_transformations: result.transformations,
        parsed_destinations: result.destinations,
        parsed_variables: result.variables,
      })
    } catch (err) {
      setParseError(err instanceof Error ? err.message : 'Failed to parse DTSX')
    } finally {
      setIsParsing(false)
    }
  }, [dtsx_location, original_server, onChange])

  // Update connection mapping
  const updateConnectionMapping = useCallback((ssisConn: string, airflowConn: string) => {
    const newMapping = { ...connectionMapping, [ssisConn]: airflowConn }
    setConnectionMapping(newMapping)
    onChange({ connection_mapping: newMapping })
  }, [connectionMapping, onChange])

  return (
    <div className={cn('space-y-6', className)}>
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="w-12 h-12 bg-orange-100 dark:bg-orange-900/30 rounded-lg flex items-center justify-center">
          <span className="text-2xl">📦</span>
        </div>
        <div>
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
            SSIS/DTSX Migration
          </h3>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Migrate your SSIS packages to modern Airflow pipelines
          </p>
        </div>
      </div>

      {/* DTSX Location Input */}
      <Card className="p-4 space-y-4">
        <h4 className="font-medium text-gray-900 dark:text-white">1. DTSX Package Location</h4>

        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              DTSX File Path (GCS)
            </label>
            <Input
              value={dtsx_location}
              onChange={(e) => setDtsxLocation(e.target.value)}
              placeholder="gs://your-bucket/ssis-packages/MyPackage.dtsx"
            />
            <p className="mt-1 text-xs text-gray-500">
              Upload your .dtsx file to GCS and provide the path
            </p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Original SSIS Server (Optional)
            </label>
            <Input
              value={original_server}
              onChange={(e) => setOriginalServer(e.target.value)}
              placeholder="e.g., PROD-SSIS-01"
            />
          </div>

          <Button
            onClick={parseDTSX}
            disabled={!dtsx_location.trim() || isParsing}
            className="w-full"
          >
            {isParsing ? (
              <>
                <svg className="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Parsing DTSX...
              </>
            ) : (
              'Parse DTSX Package'
            )}
          </Button>

          {parseError && (
            <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
              <p className="text-sm text-red-600 dark:text-red-400">{parseError}</p>
            </div>
          )}
        </div>
      </Card>

      {/* Parsed Package Preview */}
      {parsedPackage && (
        <>
          {/* Package Summary */}
          <Card className="p-4 space-y-4">
            <div className="flex items-center justify-between">
              <h4 className="font-medium text-gray-900 dark:text-white">
                2. Parsed Package: {parsedPackage.package_name}
              </h4>
              <Badge variant="success">Parsed</Badge>
            </div>

            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="text-center p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div className="text-2xl font-bold text-blue-600">{parsedPackage.sources.length}</div>
                <div className="text-xs text-gray-500">Sources</div>
              </div>
              <div className="text-center p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div className="text-2xl font-bold text-purple-600">{parsedPackage.transformations.length}</div>
                <div className="text-xs text-gray-500">Transforms</div>
              </div>
              <div className="text-center p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div className="text-2xl font-bold text-green-600">{parsedPackage.destinations.length}</div>
                <div className="text-xs text-gray-500">Destinations</div>
              </div>
              <div className="text-center p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                <div className="text-2xl font-bold text-orange-600">{parsedPackage.sql_tasks.length}</div>
                <div className="text-xs text-gray-500">SQL Tasks</div>
              </div>
            </div>

            {/* Sources */}
            {parsedPackage.sources.length > 0 && (
              <div>
                <h5 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Data Sources</h5>
                <div className="space-y-2">
                  {parsedPackage.sources.map((source, idx) => (
                    <div
                      key={idx}
                      className="flex items-center justify-between p-2 bg-blue-50 dark:bg-blue-900/20 rounded"
                    >
                      <div className="flex items-center gap-2">
                        <Badge variant="info">{source.type}</Badge>
                        <span className="text-sm font-medium">{source.name}</span>
                      </div>
                      <span className="text-xs text-gray-500">
                        {source.table_name || (source.query ? 'Custom SQL' : '')}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Transformations */}
            {parsedPackage.transformations.length > 0 && (
              <div>
                <h5 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Transformations</h5>
                <div className="flex flex-wrap gap-2">
                  {parsedPackage.transformations.map((transform, idx) => (
                    <Badge key={idx} variant="default">
                      {transform.type}: {transform.name}
                    </Badge>
                  ))}
                </div>
              </div>
            )}

            {/* Destinations */}
            {parsedPackage.destinations.length > 0 && (
              <div>
                <h5 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Destinations</h5>
                <div className="space-y-2">
                  {parsedPackage.destinations.map((dest, idx) => (
                    <div
                      key={idx}
                      className="flex items-center justify-between p-2 bg-green-50 dark:bg-green-900/20 rounded"
                    >
                      <div className="flex items-center gap-2">
                        <Badge variant="success">{dest.type}</Badge>
                        <span className="text-sm font-medium">{dest.name}</span>
                      </div>
                      <span className="text-xs text-gray-500">{dest.table_name}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </Card>

          {/* Connection Mapping */}
          <Card className="p-4 space-y-4">
            <h4 className="font-medium text-gray-900 dark:text-white">3. Connection Mapping</h4>
            <p className="text-sm text-gray-500">
              Map SSIS connections to your Airflow connection IDs
            </p>

            <div className="space-y-3">
              {Object.entries(parsedPackage.connections).map(([ssisConn, details]) => (
                <div key={ssisConn} className="flex items-center gap-4">
                  <div className="flex-1">
                    <div className="text-sm font-medium text-gray-700 dark:text-gray-300">
                      {ssisConn}
                    </div>
                    <div className="text-xs text-gray-500">
                      {details.type} - {details.server || details.database || 'N/A'}
                    </div>
                  </div>
                  <span className="text-gray-400">→</span>
                  <Select
                    value={connectionMapping[ssisConn] || ''}
                    onChange={(e) => updateConnectionMapping(ssisConn, e.target.value)}
                    options={[
                      { value: '', label: 'Select Airflow connection' },
                      ...availableConnections.map(c => ({ value: c.id, label: c.label })),
                    ]}
                    className="flex-1"
                  />
                </div>
              ))}
            </div>
          </Card>

          {/* Destination Data Model */}
          <Card className="p-4 space-y-4">
            <h4 className="font-medium text-gray-900 dark:text-white">4. Destination Data Model</h4>
            <p className="text-sm text-gray-500">
              Choose how data should be modeled in the target
            </p>

            <div className="grid grid-cols-2 gap-3">
              {DESTINATION_MODEL_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => {
                    setDestinationModel(option.value)
                    onChange({ destination_model: option.value } as any)
                  }}
                  className={cn(
                    'p-4 rounded-lg border-2 text-left transition-colors',
                    destinationModel === option.value
                      ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                      : 'border-gray-200 dark:border-gray-700 hover:border-gray-300'
                  )}
                >
                  <div className="font-medium text-gray-900 dark:text-white">
                    {option.label}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    {option.description}
                  </div>
                </button>
              ))}
            </div>
          </Card>

          {/* Migration Summary */}
          <Card className="p-4 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-white dark:bg-gray-800 rounded-full flex items-center justify-center">
                ✨
              </div>
              <div>
                <h4 className="font-medium text-gray-900 dark:text-white">Ready to Migrate</h4>
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  Your SSIS package will be converted to a modern Airflow DAG with PySpark tasks
                </p>
              </div>
            </div>
          </Card>
        </>
      )}
    </div>
  )
}

export default DTSXMigrationForm
