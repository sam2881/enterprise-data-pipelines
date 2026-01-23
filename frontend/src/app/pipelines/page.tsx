'use client'

/**
 * Pipelines Page - Data Agent UI
 *
 * Comprehensive UI for the Data Agent pipeline generation workflow:
 * - Create new pipelines from UI form
 * - View active/completed pipelines
 * - Monitor pipeline generation progress in real-time
 * - Approve/reject pipeline deployments
 */

import { useState, useEffect, useMemo } from 'react'
import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus,
  Database,
  Search,
  Filter,
  RefreshCw,
  Clock,
  CheckCircle,
  XCircle,
  ChevronRight,
  Activity,
  GitBranch,
  FileCode,
  Play,
  Pause,
  Eye,
  ArrowLeft,
  Code,
  Layers,
  Settings,
  AlertCircle,
  TrendingUp,
} from 'lucide-react'
import { PageLayout } from '@/components/layout/PageLayout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { Button } from '@/components/ui/Button'
import { PipelineConfigForm } from '@/components/pipeline/PipelineConfigForm'
import { PipelineProgress } from '@/components/pipeline/PipelineProgress'
import { getApiBaseUrl } from '@/lib/constants'
import {
  PipelineIntent,
  PipelinePhase,
  PipelineListItem,
  CreatePipelineResponse,
} from '@/types/pipeline'

// =============================================================================
// Types
// =============================================================================

type ViewMode = 'list' | 'create' | 'progress'

interface PipelineListResponse {
  pipelines: PipelineListItem[]
  count: number
}

// =============================================================================
// Phase Configuration
// =============================================================================

const phaseConfig: Record<PipelinePhase, { label: string; color: string; bgColor: string; icon: React.ReactNode }> = {
  init: { label: 'Initializing', color: 'text-gray-400', bgColor: 'bg-gray-500/20', icon: <Clock className="w-3 h-3" /> },
  planning: { label: 'Planning', color: 'text-blue-400', bgColor: 'bg-blue-500/20', icon: <Layers className="w-3 h-3" /> },
  generating: { label: 'Generating', color: 'text-indigo-400', bgColor: 'bg-indigo-500/20', icon: <Code className="w-3 h-3" /> },
  validating: { label: 'Validating', color: 'text-purple-400', bgColor: 'bg-purple-500/20', icon: <CheckCircle className="w-3 h-3" /> },
  awaiting_approval: { label: 'Awaiting Approval', color: 'text-yellow-400', bgColor: 'bg-yellow-500/20', icon: <Pause className="w-3 h-3" /> },
  deploying: { label: 'Deploying', color: 'text-cyan-400', bgColor: 'bg-cyan-500/20', icon: <GitBranch className="w-3 h-3" /> },
  complete: { label: 'Complete', color: 'text-green-400', bgColor: 'bg-green-500/20', icon: <CheckCircle className="w-3 h-3" /> },
  failed: { label: 'Failed', color: 'text-red-400', bgColor: 'bg-red-500/20', icon: <XCircle className="w-3 h-3" /> },
}

// =============================================================================
// Status Badge Component
// =============================================================================

function PipelineStatusBadge({ status }: { status: PipelinePhase }) {
  const config = phaseConfig[status] || phaseConfig.init

  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${config.bgColor} ${config.color}`}>
      {config.icon}
      {config.label}
    </span>
  )
}

// =============================================================================
// Environment Badge Component
// =============================================================================

function EnvironmentBadge({ env }: { env: string }) {
  const config: Record<string, { className: string }> = {
    prod: { className: 'bg-red-500/20 text-red-400 border-red-500/30' },
    staging: { className: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30' },
    dev: { className: 'bg-green-500/20 text-green-400 border-green-500/30' },
  }

  const envConfig = config[env] || config.dev

  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${envConfig.className}`}>
      {env.toUpperCase()}
    </span>
  )
}

// =============================================================================
// Stats Card Component
// =============================================================================

interface StatsCardProps {
  label: string
  value: number | string
  icon: React.ReactNode
  trend?: { value: number; direction: 'up' | 'down' }
  className?: string
}

function StatsCard({ label, value, icon, trend, className = '' }: StatsCardProps) {
  return (
    <div className={`bg-gray-800/50 rounded-xl border border-gray-700/50 p-4 ${className}`}>
      <div className="flex items-center justify-between">
        <div className="p-2 rounded-lg bg-gray-700/50">
          {icon}
        </div>
        {trend && (
          <div className={`flex items-center gap-1 text-xs ${trend.direction === 'up' ? 'text-green-400' : 'text-gray-400'}`}>
            <TrendingUp className={`w-3 h-3 ${trend.direction === 'down' ? 'rotate-180' : ''}`} />
            {trend.value}%
          </div>
        )}
      </div>
      <div className="mt-3">
        <div className="text-2xl font-bold text-white">{value}</div>
        <div className="text-sm text-gray-400">{label}</div>
      </div>
    </div>
  )
}

// =============================================================================
// Pipeline Row Component
// =============================================================================

function PipelineRow({ pipeline, onViewProgress }: { pipeline: PipelineListItem; onViewProgress: (id: string) => void }) {
  const isInProgress = ['planning', 'generating', 'validating', 'deploying'].includes(pipeline.status)

  return (
    <div
      className="flex items-center gap-4 p-4 bg-gray-800/30 hover:bg-gray-800/60 rounded-xl border border-gray-700/30 hover:border-gray-600/50 transition-all group cursor-pointer"
      onClick={() => onViewProgress(pipeline.request_id)}
    >
      {/* Status Indicator */}
      <div className={`w-1 h-14 rounded-full ${
        pipeline.status === 'complete' ? 'bg-green-500' :
        pipeline.status === 'failed' ? 'bg-red-500' :
        pipeline.status === 'awaiting_approval' ? 'bg-yellow-500' :
        isInProgress ? 'bg-blue-500 animate-pulse' :
        'bg-gray-500'
      }`} />

      {/* Icon */}
      <div className="p-3 rounded-xl bg-blue-500/10">
        <Database className="w-5 h-5 text-blue-400" />
      </div>

      {/* Main Content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-3 mb-1">
          <span className="text-white font-medium group-hover:text-blue-300 transition-colors">
            {pipeline.pipeline_name}
          </span>
          <EnvironmentBadge env={pipeline.environment} />
        </div>
        <div className="flex items-center gap-4 text-xs text-gray-500">
          <span className="flex items-center gap-1">
            <Layers className="w-3 h-3" />
            {pipeline.domain}
          </span>
          <span className="flex items-center gap-1">
            <Clock className="w-3 h-3" />
            {new Date(pipeline.created_at).toLocaleDateString()}
          </span>
          <span className="text-gray-600">
            ID: {pipeline.request_id.substring(0, 8)}...
          </span>
        </div>

        {/* Progress Bar */}
        {isInProgress && (
          <div className="mt-2 flex items-center gap-2">
            <div className="flex-1 h-1.5 bg-gray-700 rounded-full overflow-hidden max-w-48">
              <div
                className="h-full bg-blue-500 rounded-full transition-all duration-500"
                style={{ width: `${pipeline.progress_pct}%` }}
              />
            </div>
            <span className="text-xs text-gray-400">{pipeline.progress_pct}%</span>
          </div>
        )}
      </div>

      {/* Status & Action */}
      <div className="flex items-center gap-4">
        <PipelineStatusBadge status={pipeline.status} />
        <Button
          variant="ghost"
          size="sm"
          className="text-gray-400 hover:text-white"
          onClick={(e) => {
            e.stopPropagation()
            onViewProgress(pipeline.request_id)
          }}
        >
          <Eye className="w-4 h-4 mr-1" />
          View
        </Button>
      </div>
    </div>
  )
}

// =============================================================================
// Main Component
// =============================================================================

export default function PipelinesPage() {
  const [viewMode, setViewMode] = useState<ViewMode>('list')
  const [selectedPipelineId, setSelectedPipelineId] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [envFilter, setEnvFilter] = useState('')
  const [apiUrl, setApiUrl] = useState('')
  const queryClient = useQueryClient()
  const router = useRouter()
  const searchParams = useSearchParams()

  useEffect(() => {
    setApiUrl(getApiBaseUrl())
  }, [])

  // Check for pipeline ID in URL
  useEffect(() => {
    const id = searchParams.get('id')
    if (id) {
      setSelectedPipelineId(id)
      setViewMode('progress')
    }
  }, [searchParams])

  // Fetch pipelines list
  const {
    data: pipelinesData,
    isLoading,
    error,
    refetch,
  } = useQuery<PipelineListResponse>({
    queryKey: ['pipelines'],
    queryFn: async () => {
      const response = await fetch(`${apiUrl}/api/v2/data-agent/pipelines`)
      if (!response.ok) {
        throw new Error('Failed to fetch pipelines')
      }
      return response.json()
    },
    enabled: !!apiUrl,
    refetchInterval: 10000,
  })

  // Create pipeline mutation
  const createPipeline = useMutation({
    mutationFn: async (intent: PipelineIntent): Promise<CreatePipelineResponse> => {
      const response = await fetch(`${apiUrl}/api/v2/data-agent/pipelines`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(intent),
      })
      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.detail || 'Failed to create pipeline')
      }
      return response.json()
    },
    onSuccess: (data) => {
      setSelectedPipelineId(data.request_id)
      setViewMode('progress')
      queryClient.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })

  // Calculate stats
  const stats = useMemo(() => {
    const pipelines = pipelinesData?.pipelines || []
    const total = pipelines.length
    const inProgress = pipelines.filter(p =>
      ['planning', 'generating', 'validating', 'deploying'].includes(p.status)
    ).length
    const pendingApproval = pipelines.filter(p => p.status === 'awaiting_approval').length
    const completed = pipelines.filter(p => p.status === 'complete').length
    const failed = pipelines.filter(p => p.status === 'failed').length

    return { total, inProgress, pendingApproval, completed, failed }
  }, [pipelinesData])

  // Filter pipelines
  const filteredPipelines = useMemo(() => {
    const pipelines = pipelinesData?.pipelines || []

    return pipelines.filter((pipeline) => {
      if (searchQuery) {
        const query = searchQuery.toLowerCase()
        const matchesSearch =
          pipeline.pipeline_name.toLowerCase().includes(query) ||
          pipeline.domain.toLowerCase().includes(query) ||
          pipeline.request_id.toLowerCase().includes(query)
        if (!matchesSearch) return false
      }

      if (statusFilter && pipeline.status !== statusFilter) {
        return false
      }

      if (envFilter && pipeline.environment !== envFilter) {
        return false
      }

      return true
    })
  }, [pipelinesData, searchQuery, statusFilter, envFilter])

  const handleSubmit = async (intent: PipelineIntent) => {
    await createPipeline.mutateAsync(intent)
  }

  const handleViewProgress = (requestId: string) => {
    setSelectedPipelineId(requestId)
    setViewMode('progress')
    router.push(`/pipelines?id=${requestId}`)
  }

  const handleBackToList = () => {
    setSelectedPipelineId(null)
    setViewMode('list')
    router.push('/pipelines')
  }

  const clearFilters = () => {
    setSearchQuery('')
    setStatusFilter('')
    setEnvFilter('')
  }

  const hasActiveFilters = searchQuery || statusFilter || envFilter

  // =============================================================================
  // Render Pipeline List View
  // =============================================================================

  const renderPipelineList = () => (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-900 to-gray-800 text-white p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-2xl font-bold text-white flex items-center gap-3">
              <div className="p-2 rounded-lg bg-blue-500/10">
                <Database className="w-6 h-6 text-blue-400" />
              </div>
              Data Pipeline Agent
            </h1>
            <p className="text-gray-400 mt-1">
              AI-powered data pipeline generation with RAG-based script discovery
            </p>
          </div>
          <div className="flex items-center gap-3">
            <Button
              variant="outline"
              onClick={() => refetch()}
              className="border-gray-700 text-gray-300 hover:bg-gray-800"
            >
              <RefreshCw className="w-4 h-4 mr-2" />
              Refresh
            </Button>
            <Button
              onClick={() => setViewMode('create')}
              className="bg-blue-600 hover:bg-blue-700"
            >
              <Plus className="w-4 h-4 mr-2" />
              Create Pipeline
            </Button>
          </div>
        </div>

        {/* Stats Grid */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
          <StatsCard
            label="Total Pipelines"
            value={stats.total}
            icon={<Database className="w-5 h-5 text-gray-400" />}
          />
          <StatsCard
            label="In Progress"
            value={stats.inProgress}
            icon={<Activity className="w-5 h-5 text-blue-400" />}
            className={stats.inProgress > 0 ? 'border-blue-500/30' : ''}
          />
          <StatsCard
            label="Pending Approval"
            value={stats.pendingApproval}
            icon={<Clock className="w-5 h-5 text-yellow-400" />}
            className={stats.pendingApproval > 0 ? 'border-yellow-500/30' : ''}
          />
          <StatsCard
            label="Completed"
            value={stats.completed}
            icon={<CheckCircle className="w-5 h-5 text-green-400" />}
            trend={stats.completed > 0 ? { value: 15, direction: 'up' } : undefined}
          />
          <StatsCard
            label="Failed"
            value={stats.failed}
            icon={<XCircle className="w-5 h-5 text-red-400" />}
            className={stats.failed > 0 ? 'border-red-500/30' : ''}
          />
        </div>

        {/* Filters */}
        <div className="bg-gray-800/30 rounded-xl border border-gray-700/30 p-4 mb-6">
          <div className="flex flex-wrap items-center gap-4">
            {/* Search */}
            <div className="relative flex-1 min-w-64">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
              <input
                type="text"
                placeholder="Search pipelines by name, domain, or ID..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full bg-gray-800 border border-gray-700 text-gray-300 text-sm rounded-lg pl-10 pr-4 py-2.5 focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-colors placeholder-gray-500"
              />
            </div>

            {/* Status Filter */}
            <div className="relative">
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="appearance-none bg-gray-800 border border-gray-700 text-gray-300 text-sm rounded-lg px-3 py-2 pr-8 focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-colors"
              >
                <option value="">All Statuses</option>
                <option value="planning">Planning</option>
                <option value="generating">Generating</option>
                <option value="validating">Validating</option>
                <option value="awaiting_approval">Awaiting Approval</option>
                <option value="deploying">Deploying</option>
                <option value="complete">Complete</option>
                <option value="failed">Failed</option>
              </select>
              <Filter className="absolute right-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500 pointer-events-none" />
            </div>

            {/* Environment Filter */}
            <div className="relative">
              <select
                value={envFilter}
                onChange={(e) => setEnvFilter(e.target.value)}
                className="appearance-none bg-gray-800 border border-gray-700 text-gray-300 text-sm rounded-lg px-3 py-2 pr-8 focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-colors"
              >
                <option value="">All Environments</option>
                <option value="dev">Development</option>
                <option value="staging">Staging</option>
                <option value="prod">Production</option>
              </select>
              <Filter className="absolute right-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500 pointer-events-none" />
            </div>

            {hasActiveFilters && (
              <Button
                variant="ghost"
                onClick={clearFilters}
                className="text-gray-400 hover:text-white"
              >
                <XCircle className="w-4 h-4 mr-1" />
                Clear
              </Button>
            )}
          </div>
        </div>

        {/* Results Info */}
        <div className="flex items-center justify-between mb-4">
          <p className="text-sm text-gray-400">
            Showing <span className="text-white font-medium">{filteredPipelines.length}</span> of{' '}
            <span className="text-white font-medium">{stats.total}</span> pipelines
          </p>
          {hasActiveFilters && (
            <p className="text-sm text-blue-400">
              Filters applied
            </p>
          )}
        </div>

        {/* Pipelines List */}
        {isLoading ? (
          <div className="flex flex-col items-center justify-center py-20">
            <RefreshCw className="w-8 h-8 text-blue-400 animate-spin mb-4" />
            <span className="text-gray-400">Loading pipelines...</span>
          </div>
        ) : error ? (
          <div className="text-center py-20 bg-gray-800/30 rounded-xl border border-red-500/30">
            <AlertCircle className="w-16 h-16 text-red-400/50 mx-auto mb-4" />
            <h3 className="text-xl font-semibold text-white mb-2">
              Failed to load pipelines
            </h3>
            <p className="text-gray-400 mb-6">
              Make sure the Data Agent backend is running.
            </p>
            <Button variant="outline" onClick={() => refetch()}>
              Try Again
            </Button>
          </div>
        ) : filteredPipelines.length === 0 ? (
          <div className="text-center py-20 bg-gray-800/30 rounded-xl border border-gray-700/30">
            <Database className="w-16 h-16 text-blue-500/30 mx-auto mb-4" />
            <h3 className="text-xl font-semibold text-white mb-2">
              {hasActiveFilters ? 'No matching pipelines' : 'No pipelines yet'}
            </h3>
            <p className="text-gray-400 mb-6">
              {hasActiveFilters
                ? 'Try adjusting your filters to find what you\'re looking for.'
                : 'Create your first data pipeline to get started.'}
            </p>
            {hasActiveFilters ? (
              <Button variant="outline" onClick={clearFilters}>
                Clear Filters
              </Button>
            ) : (
              <Button onClick={() => setViewMode('create')} className="bg-blue-600 hover:bg-blue-700">
                <Plus className="w-4 h-4 mr-2" />
                Create Your First Pipeline
              </Button>
            )}
          </div>
        ) : (
          <div className="space-y-3">
            {filteredPipelines.map((pipeline) => (
              <PipelineRow
                key={pipeline.request_id}
                pipeline={pipeline}
                onViewProgress={handleViewProgress}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  )

  // =============================================================================
  // Render Create Form View
  // =============================================================================

  const renderCreateForm = () => (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-900 to-gray-800 text-white p-6">
      <div className="max-w-5xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <button
              onClick={handleBackToList}
              className="flex items-center gap-2 text-gray-400 hover:text-white mb-4 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Back to Pipelines
            </button>
            <h1 className="text-2xl font-bold text-white flex items-center gap-3">
              <div className="p-2 rounded-lg bg-blue-500/10">
                <Plus className="w-6 h-6 text-blue-400" />
              </div>
              Create New Pipeline
            </h1>
            <p className="text-gray-400 mt-1">
              Configure your data pipeline. RAG will automatically find matching scripts.
            </p>
          </div>
        </div>

        {/* Form */}
        <Card className="bg-gray-800/50 border-gray-700/50">
          <CardContent className="p-6">
            <PipelineConfigForm
              onSubmit={handleSubmit}
              onCancel={handleBackToList}
              isSubmitting={createPipeline.isPending}
            />
          </CardContent>
        </Card>
      </div>
    </div>
  )

  // =============================================================================
  // Render Progress View
  // =============================================================================

  const renderProgressView = () => (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-900 to-gray-800 text-white p-6">
      <div className="max-w-5xl mx-auto">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <button
              onClick={handleBackToList}
              className="flex items-center gap-2 text-gray-400 hover:text-white mb-4 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Back to Pipelines
            </button>
            <h1 className="text-2xl font-bold text-white flex items-center gap-3">
              <div className="p-2 rounded-lg bg-blue-500/10">
                <Activity className="w-6 h-6 text-blue-400" />
              </div>
              Pipeline Progress
            </h1>
            <p className="text-gray-400 mt-1">
              Real-time progress tracking for pipeline generation
            </p>
          </div>
        </div>

        {/* Progress Component */}
        {selectedPipelineId && (
          <PipelineProgress
            requestId={selectedPipelineId}
            onClose={() => {
              queryClient.invalidateQueries({ queryKey: ['pipelines'] })
              handleBackToList()
            }}
          />
        )}
      </div>
    </div>
  )

  // =============================================================================
  // Main Render
  // =============================================================================

  return (
    <PageLayout>
      {viewMode === 'list' && renderPipelineList()}
      {viewMode === 'create' && renderCreateForm()}
      {viewMode === 'progress' && renderProgressView()}
    </PageLayout>
  )
}
