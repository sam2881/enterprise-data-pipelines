'use client'

/**
 * NLTransformInput - Natural Language Transformation Input Component
 *
 * Allows users to describe complex transformations in natural language.
 * Integrates with LLM backend to generate PySpark/SQL code.
 *
 * Features:
 * - Natural language input for transformations
 * - Real-time code generation preview
 * - Join builder with visual interface
 * - Aggregation builder
 * - Window function builder
 */

import React, { useState, useCallback } from 'react'
import { Button } from '@/components/ui/Button'
import { Input } from '@/components/ui/Input'
import { Select } from '@/components/ui/Select'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import {
  NLTransformRule,
  JoinTransformConfig,
  AggregationTransformConfig,
  WindowTransformConfig,
  ExtendedTransformRule,
} from '@/types/pipeline-enhanced'

// =============================================================================
// Props
// =============================================================================

interface NLTransformInputProps {
  zone: 'bronze' | 'silver' | 'gold'
  schema: Array<{ name: string; type: string }>
  onTransformAdd: (transform: ExtendedTransformRule) => void
  className?: string
}

interface TransformPreview {
  pyspark: string
  sql?: string
  confidence: number
  warnings: string[]
}

// =============================================================================
// Transform Type Tabs
// =============================================================================

type TransformTab = 'natural_language' | 'join' | 'aggregate' | 'window'

// =============================================================================
// Main Component
// =============================================================================

export function NLTransformInput({
  zone,
  schema,
  onTransformAdd,
  className,
}: NLTransformInputProps) {
  const [activeTab, setActiveTab] = useState<TransformTab>('natural_language')
  const [nlDescription, setNlDescription] = useState('')
  const [isGenerating, setIsGenerating] = useState(false)
  const [preview, setPreview] = useState<TransformPreview | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Join state
  const [joinConfig, setJoinConfig] = useState<Partial<JoinTransformConfig>>({
    rule_type: 'join',
    join_type: 'left',
    join_keys: [{ left: '', right: '' }],
  })

  // Aggregation state
  const [aggConfig, setAggConfig] = useState<Partial<AggregationTransformConfig>>({
    rule_type: 'aggregate',
    group_by: [],
    aggregations: [{ column: '', function: 'sum', alias: '' }],
  })

  // Window state
  const [windowConfig, setWindowConfig] = useState<Partial<WindowTransformConfig>>({
    rule_type: 'window',
    partition_by: [],
    order_by: [{ column: '', direction: 'desc' }],
    functions: [{ type: 'row_number', alias: 'row_num' }],
  })

  // Generate code from natural language
  const generateFromNL = useCallback(async () => {
    if (!nlDescription.trim()) return

    setIsGenerating(true)
    setError(null)

    try {
      const response = await fetch('/api/v2/data-agent/nl-transform', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          description: nlDescription,
          schema: Object.fromEntries(schema.map(c => [c.name, c.type])),
          zone,
        }),
      })

      if (!response.ok) {
        throw new Error('Failed to generate code')
      }

      const result = await response.json()
      setPreview({
        pyspark: result.pyspark_code,
        sql: result.sql_code,
        confidence: result.confidence,
        warnings: result.warnings || [],
      })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate code')
    } finally {
      setIsGenerating(false)
    }
  }, [nlDescription, schema, zone])

  // Add transform to list
  const addTransform = useCallback(() => {
    if (activeTab === 'natural_language' && preview) {
      const transform: NLTransformRule = {
        rule_id: `nl_${Date.now()}`,
        rule_type: 'nl_transform',
        zone,
        nl_description: nlDescription,
        generated_pyspark: preview.pyspark,
        generated_sql: preview.sql,
        confidence: preview.confidence,
        is_validated: true,
      }
      onTransformAdd(transform)
      setNlDescription('')
      setPreview(null)
    } else if (activeTab === 'join') {
      onTransformAdd({
        ...joinConfig,
        rule_id: `join_${Date.now()}`,
      } as JoinTransformConfig)
    } else if (activeTab === 'aggregate') {
      onTransformAdd({
        ...aggConfig,
        rule_id: `agg_${Date.now()}`,
      } as AggregationTransformConfig)
    } else if (activeTab === 'window') {
      onTransformAdd({
        ...windowConfig,
        rule_id: `window_${Date.now()}`,
      } as WindowTransformConfig)
    }
  }, [activeTab, preview, nlDescription, zone, joinConfig, aggConfig, windowConfig, onTransformAdd])

  return (
    <div className={cn('space-y-4', className)}>
      {/* Tab Navigation */}
      <div className="flex border-b border-gray-200 dark:border-gray-700">
        {[
          { id: 'natural_language', label: 'Natural Language', icon: '💬' },
          { id: 'join', label: 'Join Tables', icon: '🔗' },
          { id: 'aggregate', label: 'Aggregation', icon: '📊' },
          { id: 'window', label: 'Window Function', icon: '📈' },
        ].map((tab) => (
          <button
            key={tab.id}
            type="button"
            onClick={() => setActiveTab(tab.id as TransformTab)}
            className={cn(
              'px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors',
              activeTab === tab.id
                ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'
            )}
          >
            <span className="mr-2">{tab.icon}</span>
            {tab.label}
          </button>
        ))}
      </div>

      {/* Natural Language Tab */}
      {activeTab === 'natural_language' && (
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Describe your transformation in plain English
            </label>
            <textarea
              value={nlDescription}
              onChange={(e) => setNlDescription(e.target.value)}
              placeholder="Example: Calculate the running total of sales by customer, ordered by date descending, and rank each transaction"
              rows={3}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-800 dark:text-white"
            />
            <p className="mt-1 text-xs text-gray-500">
              Supports: filtering, joining, aggregations, window functions, deduplication, sorting, and more
            </p>
          </div>

          <div className="flex gap-2">
            <Button
              onClick={generateFromNL}
              disabled={!nlDescription.trim() || isGenerating}
              className="flex-1"
            >
              {isGenerating ? (
                <>
                  <svg className="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                  </svg>
                  Generating...
                </>
              ) : (
                'Generate Code'
              )}
            </Button>
          </div>

          {error && (
            <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
              <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
            </div>
          )}

          {preview && (
            <Card className="p-4 space-y-3">
              <div className="flex items-center justify-between">
                <h4 className="font-medium text-gray-900 dark:text-white">Generated Code</h4>
                <Badge
                  variant={preview.confidence >= 0.8 ? 'success' : preview.confidence >= 0.6 ? 'warning' : 'danger'}
                >
                  {Math.round(preview.confidence * 100)}% confidence
                </Badge>
              </div>

              {preview.warnings.length > 0 && (
                <div className="p-2 bg-yellow-50 dark:bg-yellow-900/20 rounded text-sm">
                  {preview.warnings.map((w, i) => (
                    <p key={i} className="text-yellow-700 dark:text-yellow-400">⚠️ {w}</p>
                  ))}
                </div>
              )}

              <div>
                <label className="text-xs font-medium text-gray-500 dark:text-gray-400">PySpark</label>
                <pre className="mt-1 p-3 bg-gray-900 text-green-400 text-xs rounded-md overflow-x-auto">
                  {preview.pyspark}
                </pre>
              </div>

              {preview.sql && (
                <div>
                  <label className="text-xs font-medium text-gray-500 dark:text-gray-400">SQL Equivalent</label>
                  <pre className="mt-1 p-3 bg-gray-900 text-blue-400 text-xs rounded-md overflow-x-auto">
                    {preview.sql}
                  </pre>
                </div>
              )}

              <Button onClick={addTransform} variant="primary" className="w-full">
                Add Transform to Pipeline
              </Button>
            </Card>
          )}
        </div>
      )}

      {/* Join Tab */}
      {activeTab === 'join' && (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Left Table (Current)
              </label>
              <Input
                value={joinConfig.left_table || ''}
                onChange={(e) => setJoinConfig({ ...joinConfig, left_table: e.target.value })}
                placeholder="df (current DataFrame)"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Right Table
              </label>
              <Input
                value={joinConfig.right_table || ''}
                onChange={(e) => setJoinConfig({ ...joinConfig, right_table: e.target.value })}
                placeholder="gs://bucket/path/to/table"
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Join Type
            </label>
            <Select
              value={joinConfig.join_type || 'left'}
              onChange={(e) => setJoinConfig({ ...joinConfig, join_type: e.target.value as any })}
              options={[
                { value: 'inner', label: 'Inner Join' },
                { value: 'left', label: 'Left Join' },
                { value: 'right', label: 'Right Join' },
                { value: 'outer', label: 'Full Outer Join' },
              ]}
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Join Keys
            </label>
            {(joinConfig.join_keys || []).map((key, idx) => (
              <div key={idx} className="flex gap-2 mb-2">
                <Select
                  value={key.left}
                  onChange={(e) => {
                    const newKeys = [...(joinConfig.join_keys || [])]
                    newKeys[idx] = { ...newKeys[idx], left: e.target.value }
                    setJoinConfig({ ...joinConfig, join_keys: newKeys })
                  }}
                  options={[
                    { value: '', label: 'Select left column' },
                    ...schema.map(c => ({ value: c.name, label: c.name })),
                  ]}
                  className="flex-1"
                />
                <span className="flex items-center text-gray-500">=</span>
                <Input
                  value={key.right}
                  onChange={(e) => {
                    const newKeys = [...(joinConfig.join_keys || [])]
                    newKeys[idx] = { ...newKeys[idx], right: e.target.value }
                    setJoinConfig({ ...joinConfig, join_keys: newKeys })
                  }}
                  placeholder="Right column"
                  className="flex-1"
                />
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    const newKeys = (joinConfig.join_keys || []).filter((_, i) => i !== idx)
                    setJoinConfig({ ...joinConfig, join_keys: newKeys })
                  }}
                >
                  ✕
                </Button>
              </div>
            ))}
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                setJoinConfig({
                  ...joinConfig,
                  join_keys: [...(joinConfig.join_keys || []), { left: '', right: '' }],
                })
              }}
            >
              + Add Join Key
            </Button>
          </div>

          <Button onClick={addTransform} variant="primary" className="w-full">
            Add Join Transform
          </Button>
        </div>
      )}

      {/* Aggregation Tab */}
      {activeTab === 'aggregate' && (
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Group By Columns
            </label>
            <div className="flex flex-wrap gap-2">
              {schema.map((col) => (
                <label key={col.name} className="flex items-center gap-1">
                  <input
                    type="checkbox"
                    checked={(aggConfig.group_by || []).includes(col.name)}
                    onChange={(e) => {
                      const current = aggConfig.group_by || []
                      const newGroupBy = e.target.checked
                        ? [...current, col.name]
                        : current.filter(c => c !== col.name)
                      setAggConfig({ ...aggConfig, group_by: newGroupBy })
                    }}
                    className="rounded border-gray-300"
                  />
                  <span className="text-sm">{col.name}</span>
                </label>
              ))}
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Aggregations
            </label>
            {(aggConfig.aggregations || []).map((agg, idx) => (
              <div key={idx} className="flex gap-2 mb-2">
                <Select
                  value={agg.function}
                  onChange={(e) => {
                    const newAggs = [...(aggConfig.aggregations || [])]
                    newAggs[idx] = { ...newAggs[idx], function: e.target.value as any }
                    setAggConfig({ ...aggConfig, aggregations: newAggs })
                  }}
                  options={[
                    { value: 'sum', label: 'SUM' },
                    { value: 'count', label: 'COUNT' },
                    { value: 'avg', label: 'AVG' },
                    { value: 'min', label: 'MIN' },
                    { value: 'max', label: 'MAX' },
                    { value: 'first', label: 'FIRST' },
                  ]}
                  className="w-24"
                />
                <Select
                  value={agg.column}
                  onChange={(e) => {
                    const newAggs = [...(aggConfig.aggregations || [])]
                    newAggs[idx] = { ...newAggs[idx], column: e.target.value }
                    setAggConfig({ ...aggConfig, aggregations: newAggs })
                  }}
                  options={[
                    { value: '', label: 'Column' },
                    ...schema.map(c => ({ value: c.name, label: c.name })),
                  ]}
                  className="flex-1"
                />
                <Input
                  value={agg.alias}
                  onChange={(e) => {
                    const newAggs = [...(aggConfig.aggregations || [])]
                    newAggs[idx] = { ...newAggs[idx], alias: e.target.value }
                    setAggConfig({ ...aggConfig, aggregations: newAggs })
                  }}
                  placeholder="Alias"
                  className="flex-1"
                />
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    const newAggs = (aggConfig.aggregations || []).filter((_, i) => i !== idx)
                    setAggConfig({ ...aggConfig, aggregations: newAggs })
                  }}
                >
                  ✕
                </Button>
              </div>
            ))}
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                setAggConfig({
                  ...aggConfig,
                  aggregations: [...(aggConfig.aggregations || []), { column: '', function: 'sum', alias: '' }],
                })
              }}
            >
              + Add Aggregation
            </Button>
          </div>

          <Button onClick={addTransform} variant="primary" className="w-full">
            Add Aggregation Transform
          </Button>
        </div>
      )}

      {/* Window Function Tab */}
      {activeTab === 'window' && (
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Partition By
            </label>
            <div className="flex flex-wrap gap-2">
              {schema.map((col) => (
                <label key={col.name} className="flex items-center gap-1">
                  <input
                    type="checkbox"
                    checked={(windowConfig.partition_by || []).includes(col.name)}
                    onChange={(e) => {
                      const current = windowConfig.partition_by || []
                      const newPartition = e.target.checked
                        ? [...current, col.name]
                        : current.filter(c => c !== col.name)
                      setWindowConfig({ ...windowConfig, partition_by: newPartition })
                    }}
                    className="rounded border-gray-300"
                  />
                  <span className="text-sm">{col.name}</span>
                </label>
              ))}
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Order By
            </label>
            {(windowConfig.order_by || []).map((order, idx) => (
              <div key={idx} className="flex gap-2 mb-2">
                <Select
                  value={order.column}
                  onChange={(e) => {
                    const newOrders = [...(windowConfig.order_by || [])]
                    newOrders[idx] = { ...newOrders[idx], column: e.target.value }
                    setWindowConfig({ ...windowConfig, order_by: newOrders })
                  }}
                  options={[
                    { value: '', label: 'Select column' },
                    ...schema.map(c => ({ value: c.name, label: c.name })),
                  ]}
                  className="flex-1"
                />
                <Select
                  value={order.direction}
                  onChange={(e) => {
                    const newOrders = [...(windowConfig.order_by || [])]
                    newOrders[idx] = { ...newOrders[idx], direction: e.target.value as 'asc' | 'desc' }
                    setWindowConfig({ ...windowConfig, order_by: newOrders })
                  }}
                  options={[
                    { value: 'asc', label: 'ASC' },
                    { value: 'desc', label: 'DESC' },
                  ]}
                  className="w-24"
                />
              </div>
            ))}
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Window Functions
            </label>
            {(windowConfig.functions || []).map((func, idx) => (
              <div key={idx} className="flex gap-2 mb-2">
                <Select
                  value={func.type}
                  onChange={(e) => {
                    const newFuncs = [...(windowConfig.functions || [])]
                    newFuncs[idx] = { ...newFuncs[idx], type: e.target.value as any }
                    setWindowConfig({ ...windowConfig, functions: newFuncs })
                  }}
                  options={[
                    { value: 'row_number', label: 'ROW_NUMBER' },
                    { value: 'rank', label: 'RANK' },
                    { value: 'dense_rank', label: 'DENSE_RANK' },
                    { value: 'lag', label: 'LAG' },
                    { value: 'lead', label: 'LEAD' },
                    { value: 'sum', label: 'Running SUM' },
                  ]}
                  className="w-32"
                />
                {['lag', 'lead', 'sum'].includes(func.type) && (
                  <Select
                    value={func.column || ''}
                    onChange={(e) => {
                      const newFuncs = [...(windowConfig.functions || [])]
                      newFuncs[idx] = { ...newFuncs[idx], column: e.target.value }
                      setWindowConfig({ ...windowConfig, functions: newFuncs })
                    }}
                    options={[
                      { value: '', label: 'Column' },
                      ...schema.map(c => ({ value: c.name, label: c.name })),
                    ]}
                    className="flex-1"
                  />
                )}
                <Input
                  value={func.alias}
                  onChange={(e) => {
                    const newFuncs = [...(windowConfig.functions || [])]
                    newFuncs[idx] = { ...newFuncs[idx], alias: e.target.value }
                    setWindowConfig({ ...windowConfig, functions: newFuncs })
                  }}
                  placeholder="Alias"
                  className="flex-1"
                />
              </div>
            ))}
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                setWindowConfig({
                  ...windowConfig,
                  functions: [...(windowConfig.functions || []), { type: 'row_number', alias: '' }],
                })
              }}
            >
              + Add Function
            </Button>
          </div>

          <Button onClick={addTransform} variant="primary" className="w-full">
            Add Window Transform
          </Button>
        </div>
      )}
    </div>
  )
}

export default NLTransformInput
