'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { PageLayout } from '@/components/layout/PageLayout'
import { Card, CardContent } from '@/components/ui/Card'
import { LoadingSpinner } from '@/components/ui/LoadingSpinner'
import { WorkflowVisualization, WorkflowData } from '@/components/workflow/WorkflowVisualization'
import { Badge } from '@/components/ui/Badge'
import { Activity, CheckCircle2, XCircle, Clock, AlertCircle, Database, Server, Plus } from 'lucide-react'

// API Configuration - PRODUCTION
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

type WorkflowType = 'incident' | 'pipeline' | 'all'

// Fetch workflows from production API
async function fetchWorkflows(): Promise<WorkflowData[]> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/workflows`, {
      headers: {
        'Content-Type': 'application/json',
      },
    })

    if (!response.ok) {
      return []
    }

    const data = await response.json()
    return data.workflows || []
  } catch {
    return []
  }
}

// Fetch LangGraph definitions for both agent types
async function fetchWorkflowDefinitions() {
  try {
    const [incidentRes, dataRes] = await Promise.all([
      fetch(`${API_BASE_URL}/api/langgraph/definition`),
      fetch(`${API_BASE_URL}/api/langgraph/definition/data`)
    ])

    return {
      incident: incidentRes.ok ? await incidentRes.json() : null,
      data: dataRes.ok ? await dataRes.json() : null
    }
  } catch {
    return { incident: null, data: null }
  }
}

// Fetch pipelines from Data Agent
async function fetchPipelines() {
  try {
    const response = await fetch(`${API_BASE_URL}/api/pipelines`)
    if (!response.ok) return []
    const data = await response.json()
    return data.pipelines || []
  } catch {
    return []
  }
}

export default function WorkflowsPage() {
  const [activeTab, setActiveTab] = useState<WorkflowType>('all')
  const [showCreateModal, setShowCreateModal] = useState(false)

  // Fetch workflows from backend API
  const { data: workflows = [], isLoading: workflowsLoading } = useQuery({
    queryKey: ['workflows'],
    queryFn: fetchWorkflows,
    refetchInterval: 5000,
    retry: 3,
  })

  // Fetch pipeline requests
  const { data: pipelines = [], isLoading: pipelinesLoading } = useQuery({
    queryKey: ['pipelines'],
    queryFn: fetchPipelines,
    refetchInterval: 5000,
  })

  // Fetch workflow definitions
  const { data: definitions } = useQuery({
    queryKey: ['workflow-definitions'],
    queryFn: fetchWorkflowDefinitions,
  })

  const isLoading = workflowsLoading || pipelinesLoading

  if (isLoading) {
    return (
      <PageLayout title="Workflows">
        <div className="flex items-center justify-center h-96">
          <LoadingSpinner size="lg" text="Loading workflows..." />
        </div>
      </PageLayout>
    )
  }

  // Filter by type
  const incidentWorkflows = workflows.filter((w: any) => w.type === 'incident' || !w.type)
  const pipelineWorkflows = pipelines

  const displayWorkflows = activeTab === 'incident' ? incidentWorkflows
    : activeTab === 'pipeline' ? pipelineWorkflows
    : [...incidentWorkflows, ...pipelineWorkflows.map((p: any) => ({ ...p, type: 'pipeline' }))]

  const activeCount = displayWorkflows.filter((w: any) => w.status === 'active' || w.status === 'in_progress').length
  const completedCount = displayWorkflows.filter((w: any) => w.status === 'completed' || w.status === 'success').length
  const failedCount = displayWorkflows.filter((w: any) => w.status === 'failed' || w.status === 'error').length

  return (
    <PageLayout
      title="Unified Workflows"
      subtitle="LangGraph-powered multi-agent orchestration for IT Service and Data Engineering"
    >
      {/* Agent Type Tabs */}
      <div className="flex items-center gap-4 mb-6">
        <div className="flex bg-gray-100 rounded-lg p-1">
          <button
            onClick={() => setActiveTab('all')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
              activeTab === 'all' ? 'bg-white shadow text-gray-900' : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            All Workflows
          </button>
          <button
            onClick={() => setActiveTab('incident')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors flex items-center gap-2 ${
              activeTab === 'incident' ? 'bg-white shadow text-gray-900' : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <Server className="w-4 h-4" />
            IT Service
          </button>
          <button
            onClick={() => setActiveTab('pipeline')}
            className={`px-4 py-2 rounded-md text-sm font-medium transition-colors flex items-center gap-2 ${
              activeTab === 'pipeline' ? 'bg-white shadow text-gray-900' : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            <Database className="w-4 h-4" />
            Data Pipelines
          </button>
        </div>

        <div className="flex-1" />

        {activeTab === 'pipeline' && (
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <Plus className="w-4 h-4" />
            Create Pipeline
          </button>
        )}
      </div>

      {/* Stats Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Total Workflows</p>
                <p className="text-2xl font-bold mt-1">{displayWorkflows.length}</p>
              </div>
              <Activity className="w-8 h-8 text-blue-500" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Active</p>
                <p className="text-2xl font-bold mt-1 text-blue-600">{activeCount}</p>
              </div>
              <Clock className="w-8 h-8 text-blue-500" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Completed</p>
                <p className="text-2xl font-bold mt-1 text-green-600">{completedCount}</p>
              </div>
              <CheckCircle2 className="w-8 h-8 text-green-500" />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Failed</p>
                <p className="text-2xl font-bold mt-1 text-red-600">{failedCount}</p>
              </div>
              <XCircle className="w-8 h-8 text-red-500" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Workflow Definition Preview */}
      {definitions && (
        <Card className="mb-8">
          <CardContent className="p-6">
            <h3 className="text-lg font-semibold mb-4">
              {activeTab === 'pipeline' ? 'Data Pipeline Workflow' :
               activeTab === 'incident' ? 'Incident Resolution Workflow' :
               'Workflow Architecture'}
            </h3>
            <div className="flex flex-wrap gap-2">
              {(activeTab === 'pipeline' ? definitions.data?.phases : definitions.incident?.phases)?.map((phase: any) => (
                <div
                  key={phase.name}
                  className="flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium text-white"
                  style={{ backgroundColor: phase.color }}
                >
                  {phase.name}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Workflow List */}
      {displayWorkflows.length === 0 ? (
        <Card>
          <CardContent className="p-12 text-center">
            {activeTab === 'pipeline' ? (
              <>
                <Database className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <p className="text-gray-600">No data pipelines</p>
                <p className="text-sm text-gray-500 mt-1">
                  Create a new pipeline to generate Spark, Airflow, and DQ code
                </p>
                <button
                  onClick={() => setShowCreateModal(true)}
                  className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                >
                  Create Pipeline
                </button>
              </>
            ) : (
              <>
                <Activity className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <p className="text-gray-600">No active workflows</p>
                <p className="text-sm text-gray-500 mt-1">
                  Workflows will appear here when incidents or pipelines are processed
                </p>
              </>
            )}
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-6">
          {displayWorkflows.map((workflow: any) => (
            <Card key={workflow.workflow_id || workflow.request_id}>
              <CardContent className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    {workflow.type === 'pipeline' ? (
                      <Database className="w-5 h-5 text-purple-600" />
                    ) : (
                      <Server className="w-5 h-5 text-blue-600" />
                    )}
                    <h3 className="text-lg font-semibold">
                      {workflow.type === 'pipeline'
                        ? `Pipeline ${workflow.request_id}`
                        : `Incident ${workflow.incident_id}`}
                    </h3>
                    <Badge variant={
                      workflow.status === 'completed' || workflow.status === 'success' ? 'success'
                      : workflow.status === 'failed' || workflow.status === 'error' ? 'error'
                      : 'info'
                    }>
                      {workflow.status}
                    </Badge>
                    {workflow.requires_approval && (
                      <Badge variant="warning">Needs Approval</Badge>
                    )}
                  </div>
                  <span className="text-sm text-gray-500">
                    {workflow.created_at ? new Date(workflow.created_at).toLocaleString() : ''}
                  </span>
                </div>

                {/* Pipeline-specific details */}
                {workflow.type === 'pipeline' && workflow.request && (
                  <div className="grid grid-cols-3 gap-4 mb-4 text-sm">
                    <div>
                      <span className="text-gray-500">Source:</span>
                      <span className="ml-2 font-mono">{workflow.request.source_uri}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Target Layer:</span>
                      <Badge variant="default" className="ml-2">{workflow.request.target_layer}</Badge>
                    </div>
                    <div>
                      <span className="text-gray-500">Risk Score:</span>
                      <span className={`ml-2 font-semibold ${
                        (workflow.result?.risk_score || 0) > 0.7 ? 'text-red-600' : 'text-green-600'
                      }`}>
                        {((workflow.result?.risk_score || 0) * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                )}

                {/* Workflow Visualization for incidents */}
                {workflow.type !== 'pipeline' && workflow.steps && (
                  <WorkflowVisualization
                    workflow={workflow}
                    onApprove={(stepId) => console.log('Approve:', stepId)}
                    onReject={(stepId) => console.log('Reject:', stepId)}
                  />
                )}

                {/* Pipeline generated artifacts */}
                {workflow.type === 'pipeline' && workflow.result && (
                  <div className="flex gap-2 mt-4">
                    {workflow.result.spark && (
                      <a
                        href={`/workflows/${workflow.request_id}/spark`}
                        className="px-3 py-1.5 bg-orange-100 text-orange-700 rounded-md text-sm hover:bg-orange-200"
                      >
                        View Spark Code
                      </a>
                    )}
                    {workflow.result.dag && (
                      <a
                        href={`/workflows/${workflow.request_id}/dag`}
                        className="px-3 py-1.5 bg-green-100 text-green-700 rounded-md text-sm hover:bg-green-200"
                      >
                        View DAG
                      </a>
                    )}
                    {workflow.result.dq && (
                      <a
                        href={`/workflows/${workflow.request_id}/dq`}
                        className="px-3 py-1.5 bg-purple-100 text-purple-700 rounded-md text-sm hover:bg-purple-200"
                      >
                        View DQ Rules
                      </a>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Create Pipeline Modal */}
      {showCreateModal && (
        <CreatePipelineModal onClose={() => setShowCreateModal(false)} />
      )}
    </PageLayout>
  )
}

// Create Pipeline Modal Component
function CreatePipelineModal({ onClose }: { onClose: () => void }) {
  const [formData, setFormData] = useState({
    source_uri: '',
    source_type: 'gcs',
    target_layer: 'silver',
    business_context: '',
    schedule: '@daily'
  })
  const [isSubmitting, setIsSubmitting] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsSubmitting(true)

    try {
      const response = await fetch(`${API_BASE_URL}/api/pipelines`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData)
      })

      if (response.ok) {
        onClose()
        window.location.reload()
      }
    } catch (error) {
      console.error('Failed to create pipeline:', error)
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg p-6">
        <h2 className="text-xl font-semibold mb-4">Create Data Pipeline</h2>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Source URI
            </label>
            <input
              type="text"
              value={formData.source_uri}
              onChange={(e) => setFormData({ ...formData, source_uri: e.target.value })}
              placeholder="gs://bucket/path/to/data"
              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              required
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Source Type
              </label>
              <select
                value={formData.source_type}
                onChange={(e) => setFormData({ ...formData, source_type: e.target.value })}
                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
              >
                <option value="gcs">Google Cloud Storage</option>
                <option value="bigquery">BigQuery</option>
                <option value="postgres">PostgreSQL</option>
                <option value="mysql">MySQL</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Target Layer
              </label>
              <select
                value={formData.target_layer}
                onChange={(e) => setFormData({ ...formData, target_layer: e.target.value })}
                className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
              >
                <option value="bronze">Bronze (Raw)</option>
                <option value="silver">Silver (Cleaned)</option>
                <option value="gold">Gold (Aggregated)</option>
              </select>
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Business Context
            </label>
            <textarea
              value={formData.business_context}
              onChange={(e) => setFormData({ ...formData, business_context: e.target.value })}
              placeholder="Describe the business purpose of this pipeline..."
              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
              rows={3}
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Schedule
            </label>
            <select
              value={formData.schedule}
              onChange={(e) => setFormData({ ...formData, schedule: e.target.value })}
              className="w-full px-3 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500"
            >
              <option value="@hourly">Hourly</option>
              <option value="@daily">Daily</option>
              <option value="@weekly">Weekly</option>
              <option value="@monthly">Monthly</option>
            </select>
          </div>

          <div className="flex justify-end gap-3 mt-6">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSubmitting}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
            >
              {isSubmitting ? 'Creating...' : 'Create Pipeline'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
