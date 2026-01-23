'use client'

/**
 * Jira Ticket Detail Page with Data Pipeline Integration
 *
 * Features:
 * - Three tabs: Details, Configure Pipeline, Agent Progress
 * - Pipeline configuration form for creating data pipelines
 * - Real-time pipeline progress tracking via WebSocket
 * - Existing code generation functionality preserved
 */

import { useState, useCallback } from 'react'
import { useParams, useRouter } from 'next/navigation'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { PageLayout } from '@/components/layout/PageLayout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { Button } from '@/components/ui/Button'
import { api, QUERY_KEYS } from '@/lib/api'
import { PipelineConfigForm } from '@/components/pipeline/PipelineConfigForm'
import { PipelineProgress } from '@/components/pipeline/PipelineProgress'
import { PipelineIntent, PipelineStatus } from '@/types/pipeline'
import Link from 'next/link'
import { cn } from '@/lib/utils'
import {
  Code, GitPullRequest, Clock, User, Tag, ExternalLink, Play,
  CheckCircle, AlertCircle, FileCode, ArrowLeft, BookOpen, Bug, FileText, GitBranch,
  Settings, Activity, Database
} from 'lucide-react'

// =============================================================================
// Types
// =============================================================================

interface JiraTicket {
  ticket_id: string
  summary: string
  description: string
  status: string
  priority: string
  issue_type: string
  assignee: { name: string } | null
  reporter: { name: string } | null
  labels: string[]
  created: string
  updated: string
  url: string
}

interface ProcessResult {
  ticket_id: string
  status: string
  pr_url?: string
  pr_number?: number
  steps: Array<{ step: string; status: string; branch?: string; file?: string; chars?: number }>
  error?: string
}

type TabId = 'details' | 'configure' | 'progress'

interface Tab {
  id: TabId
  label: string
  icon: React.ReactNode
}

// =============================================================================
// Constants
// =============================================================================

const statusColors: Record<string, string> = {
  'To Do': 'bg-gray-500',
  'In Progress': 'bg-blue-500',
  'Done': 'bg-green-500',
}

const priorityColors: Record<string, string> = {
  'Critical': 'bg-red-500',
  'High': 'bg-orange-500',
  'Medium': 'bg-yellow-500',
  'Low': 'bg-green-500',
}

const TABS: Tab[] = [
  { id: 'details', label: 'Details', icon: <FileText className="h-4 w-4" /> },
  { id: 'configure', label: 'Configure Pipeline', icon: <Settings className="h-4 w-4" /> },
  { id: 'progress', label: 'Agent Progress', icon: <Activity className="h-4 w-4" /> },
]

// =============================================================================
// Tab Components
// =============================================================================

interface TabButtonProps {
  tab: Tab
  isActive: boolean
  onClick: () => void
  badge?: number
}

function TabButton({ tab, isActive, onClick, badge }: TabButtonProps) {
  return (
    <button
      onClick={onClick}
      className={cn(
        'flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors',
        isActive
          ? 'border-blue-500 text-blue-500'
          : 'border-transparent text-gray-400 hover:text-gray-300 hover:border-gray-600'
      )}
    >
      {tab.icon}
      {tab.label}
      {badge !== undefined && badge > 0 && (
        <Badge variant="info" className="ml-1 text-xs">
          {badge}
        </Badge>
      )}
    </button>
  )
}

// =============================================================================
// Main Page Component
// =============================================================================

export default function JiraTicketDetailPage() {
  const params = useParams()
  const router = useRouter()
  const ticketId = params.id as string
  const queryClient = useQueryClient()

  // Tab state
  const [activeTab, setActiveTab] = useState<TabId>('details')

  // Code generation state (existing functionality)
  const [isProcessing, setIsProcessing] = useState(false)
  const [processResult, setProcessResult] = useState<ProcessResult | null>(null)

  // Pipeline state
  const [pipelineRequestId, setPipelineRequestId] = useState<string | null>(null)
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null)
  const [isCreatingPipeline, setIsCreatingPipeline] = useState(false)

  // Fetch ticket data
  const { data: ticket, isLoading } = useQuery<JiraTicket>({
    queryKey: [QUERY_KEYS.JIRA_TICKETS, ticketId],
    queryFn: () => api.getJiraTicket(ticketId),
    enabled: !!ticketId,
  })

  // Fetch active pipelines for this ticket
  const { data: activePipelines } = useQuery({
    queryKey: [QUERY_KEYS.PIPELINES, ticketId],
    queryFn: () => api.getPipelinesByTicket(ticketId),
    enabled: !!ticketId,
    refetchInterval: pipelineRequestId ? 5000 : false,
  })

  // Code generation mutation (existing functionality)
  const processTicketMutation = useMutation({
    mutationFn: () => api.processJiraTicket(ticketId),
    onSuccess: (data) => {
      setProcessResult(data)
      setIsProcessing(false)
      queryClient.invalidateQueries({ queryKey: ['github-prs'] })
    },
    onError: (error: any) => {
      setIsProcessing(false)
      setProcessResult({
        ticket_id: ticketId,
        status: 'failed',
        error: error.message,
        steps: [],
      })
    },
  })

  // Pipeline creation mutation
  const createPipelineMutation = useMutation({
    mutationFn: (intent: PipelineIntent) => api.createPipeline(intent),
    onSuccess: (data) => {
      setPipelineRequestId(data.request_id)
      setIsCreatingPipeline(false)
      setActiveTab('progress')
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.PIPELINES] })
    },
    onError: (error: any) => {
      setIsCreatingPipeline(false)
      console.error('Pipeline creation failed:', error)
    },
  })

  // Pipeline approval mutation
  const approvePipelineMutation = useMutation({
    mutationFn: (requestId: string) => api.approvePipeline(requestId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.PIPELINES] })
    },
  })

  // Pipeline rejection mutation
  const rejectPipelineMutation = useMutation({
    mutationFn: (requestId: string) => api.rejectPipeline(requestId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.PIPELINES] })
    },
  })

  // Handlers
  const handleProcess = () => {
    setIsProcessing(true)
    setProcessResult(null)
    processTicketMutation.mutate()
  }

  const handlePipelineSubmit = useCallback(async (intent: PipelineIntent) => {
    setIsCreatingPipeline(true)
    await createPipelineMutation.mutateAsync(intent)
  }, [createPipelineMutation])

  const handlePipelineApprove = useCallback(async (requestId: string) => {
    await approvePipelineMutation.mutateAsync(requestId)
  }, [approvePipelineMutation])

  const handlePipelineReject = useCallback(async (requestId: string) => {
    await rejectPipelineMutation.mutateAsync(requestId)
  }, [rejectPipelineMutation])

  const handlePipelineRetry = useCallback(() => {
    setPipelineRequestId(null)
    setPipelineStatus(null)
    setActiveTab('configure')
  }, [])

  // Loading state
  if (isLoading) {
    return (
      <PageLayout title="Loading...">
        <div className="flex justify-center py-12">
          <div className="animate-spin h-8 w-8 border-4 border-blue-500 border-t-transparent rounded-full" />
        </div>
      </PageLayout>
    )
  }

  // Not found state
  if (!ticket) {
    return (
      <PageLayout title="Not Found">
        <Card className="bg-gray-800 border-gray-700">
          <CardContent className="py-12 text-center">
            <AlertCircle className="h-12 w-12 mx-auto text-red-500 mb-4" />
            <p className="text-gray-400">Ticket {ticketId} not found</p>
            <Link href="/jira">
              <Button className="mt-4 border-gray-500 bg-transparent text-gray-300">
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back
              </Button>
            </Link>
          </CardContent>
        </Card>
      </PageLayout>
    )
  }

  const Icon = ticket.issue_type === 'Story' ? BookOpen : ticket.issue_type === 'Bug' ? Bug : FileCode

  return (
    <PageLayout title={ticket.ticket_id} subtitle={ticket.summary}>
      {/* Back button */}
      <Link href="/jira">
        <Button variant="ghost" size="sm" className="mb-4">
          <ArrowLeft className="h-4 w-4 mr-2" />
          Back
        </Button>
      </Link>

      {/* Header Card */}
      <Card className="bg-gray-800 border-gray-700 mb-6">
        <CardContent className="pt-6">
          <div className="flex items-start justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-500/20 rounded-lg">
                <Icon className="h-5 w-5" />
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <span className="text-blue-400 font-mono">{ticket.ticket_id}</span>
                  <Badge className={statusColors[ticket.status] || 'bg-gray-500'}>
                    {ticket.status}
                  </Badge>
                </div>
                <span className="text-sm text-gray-500">{ticket.issue_type}</span>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Badge className={priorityColors[ticket.priority] || 'bg-gray-500'}>
                {ticket.priority}
              </Badge>
              <a href={ticket.url} target="_blank" rel="noopener noreferrer">
                <Button className="border-gray-500 bg-transparent text-gray-300" size="sm">
                  <ExternalLink className="h-4 w-4 mr-2" />
                  Jira
                </Button>
              </a>
            </div>
          </div>
          <h1 className="text-2xl font-bold text-white">{ticket.summary}</h1>
        </CardContent>
      </Card>

      {/* Tabs */}
      <div className="border-b border-gray-700 mb-6">
        <div className="flex gap-4">
          {TABS.map((tab) => (
            <TabButton
              key={tab.id}
              tab={tab}
              isActive={activeTab === tab.id}
              onClick={() => setActiveTab(tab.id)}
              badge={tab.id === 'progress' && activePipelines?.length ? activePipelines.length : undefined}
            />
          ))}
        </div>
      </div>

      {/* Tab Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Main Content Area */}
        <div className="lg:col-span-2 space-y-6">
          {/* Details Tab */}
          {activeTab === 'details' && (
            <>
              {/* Code Generation Action */}
              <Card className="bg-gray-800 border-gray-700">
                <CardContent className="pt-6">
                  <h3 className="text-lg font-semibold text-white mb-4">Code Generation</h3>
                  <p className="text-gray-400 text-sm mb-4">
                    Analyze this ticket and automatically generate code changes with a pull request.
                  </p>
                  <Button
                    onClick={handleProcess}
                    disabled={isProcessing}
                    className="bg-green-600 hover:bg-green-700"
                  >
                    {isProcessing ? (
                      <>
                        <div className="animate-spin h-4 w-4 border-2 border-white border-t-transparent rounded-full mr-2" />
                        Processing...
                      </>
                    ) : (
                      <>
                        <Play className="h-4 w-4 mr-2" />
                        Generate Code & Create PR
                      </>
                    )}
                  </Button>
                </CardContent>
              </Card>

              {/* Code Generation Result */}
              {processResult && (
                <Card
                  className={
                    processResult.status === 'completed'
                      ? 'bg-green-900/20 border-green-700'
                      : 'bg-red-900/20 border-red-700'
                  }
                >
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      {processResult.status === 'completed' ? (
                        <>
                          <CheckCircle className="h-5 w-5 text-green-500" />
                          <span className="text-green-400">Complete</span>
                        </>
                      ) : (
                        <>
                          <AlertCircle className="h-5 w-5 text-red-500" />
                          <span className="text-red-400">Failed</span>
                        </>
                      )}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2 mb-4">
                      {processResult.steps.map((step, i) => (
                        <div key={i} className="flex items-center gap-2 p-2 bg-gray-800/50 rounded">
                          {step.status === 'done' ? (
                            <CheckCircle className="h-4 w-4 text-green-500" />
                          ) : (
                            <AlertCircle className="h-4 w-4 text-red-500" />
                          )}
                          <span className="text-white capitalize">{step.step}</span>
                          {step.branch && (
                            <span className="text-gray-400 text-sm">
                              <GitBranch className="h-3 w-3 inline mr-1" />
                              {step.branch}
                            </span>
                          )}
                          {step.file && (
                            <span className="text-gray-400 text-sm">
                              <FileText className="h-3 w-3 inline mr-1" />
                              {step.file}
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                    {processResult.pr_url && (
                      <div className="p-4 bg-green-900/30 rounded border border-green-700 flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <GitPullRequest className="h-6 w-6 text-green-500" />
                          <span className="text-white">PR #{processResult.pr_number}</span>
                        </div>
                        <a href={processResult.pr_url} target="_blank" rel="noopener noreferrer">
                          <Button className="bg-green-600">
                            <ExternalLink className="h-4 w-4 mr-2" />
                            View PR
                          </Button>
                        </a>
                      </div>
                    )}
                    {processResult.error && (
                      <p className="text-red-400 p-4 bg-red-900/30 rounded">{processResult.error}</p>
                    )}
                  </CardContent>
                </Card>
              )}

              {/* Description */}
              <Card className="bg-gray-800 border-gray-700">
                <CardHeader>
                  <CardTitle>
                    <FileText className="h-5 w-5 inline mr-2" />
                    Description
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <pre className="whitespace-pre-wrap text-gray-300 text-sm">
                    {ticket.description || 'No description'}
                  </pre>
                </CardContent>
              </Card>
            </>
          )}

          {/* Configure Pipeline Tab */}
          {activeTab === 'configure' && (
            <Card className="bg-gray-800 border-gray-700">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Database className="h-5 w-5" />
                  Configure Data Pipeline
                </CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-gray-400 text-sm mb-6">
                  Create a data pipeline by configuring the source, schema, target, and execution policy.
                  The AI agent will generate Airflow DAGs and Spark jobs based on your configuration.
                </p>
                <PipelineConfigForm
                  jiraTicket={ticketId}
                  onSubmit={handlePipelineSubmit}
                  onCancel={() => setActiveTab('details')}
                  isSubmitting={isCreatingPipeline}
                />
              </CardContent>
            </Card>
          )}

          {/* Agent Progress Tab */}
          {activeTab === 'progress' && (
            <>
              {pipelineRequestId ? (
                <PipelineProgress
                  requestId={pipelineRequestId}
                  initialStatus={pipelineStatus || undefined}
                  onApprove={handlePipelineApprove}
                  onReject={handlePipelineReject}
                  onRetry={handlePipelineRetry}
                  onClose={() => setPipelineRequestId(null)}
                  showArtifacts
                />
              ) : activePipelines && activePipelines.length > 0 ? (
                <div className="space-y-4">
                  <h3 className="text-lg font-semibold text-white">Active Pipelines</h3>
                  {activePipelines.map((pipeline: PipelineStatus) => (
                    <Card
                      key={pipeline.request_id}
                      className="bg-gray-800 border-gray-700 cursor-pointer hover:border-gray-600"
                      onClick={() => setPipelineRequestId(pipeline.request_id)}
                    >
                      <CardContent className="pt-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <p className="text-white font-medium">{pipeline.request_id}</p>
                            <p className="text-gray-400 text-sm">{pipeline.message}</p>
                          </div>
                          <div className="flex items-center gap-2">
                            <Badge
                              variant={
                                pipeline.status === 'complete'
                                  ? 'success'
                                  : pipeline.status === 'failed'
                                  ? 'error'
                                  : 'warning'
                              }
                            >
                              {pipeline.status}
                            </Badge>
                            <span className="text-gray-400 text-sm">{pipeline.progress_pct}%</span>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              ) : (
                <Card className="bg-gray-800 border-gray-700">
                  <CardContent className="py-12 text-center">
                    <Activity className="h-12 w-12 mx-auto text-gray-500 mb-4" />
                    <p className="text-gray-400">No active pipelines</p>
                    <p className="text-gray-500 text-sm mt-2">
                      Configure a pipeline to see progress here
                    </p>
                    <Button
                      className="mt-4"
                      variant="outline"
                      onClick={() => setActiveTab('configure')}
                    >
                      <Settings className="h-4 w-4 mr-2" />
                      Configure Pipeline
                    </Button>
                  </CardContent>
                </Card>
              )}
            </>
          )}
        </div>

        {/* Sidebar */}
        <div className="space-y-6">
          {/* Details Card */}
          <Card className="bg-gray-800 border-gray-700">
            <CardHeader>
              <CardTitle className="text-sm text-gray-400">Details</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <p className="text-xs text-gray-500 mb-1">Status</p>
                <Badge className={statusColors[ticket.status] || 'bg-gray-500'}>
                  {ticket.status}
                </Badge>
              </div>
              <div>
                <p className="text-xs text-gray-500 mb-1">Priority</p>
                <Badge className={priorityColors[ticket.priority] || 'bg-gray-500'}>
                  {ticket.priority}
                </Badge>
              </div>
              <div>
                <p className="text-xs text-gray-500 mb-1">Type</p>
                <span className="text-gray-300 flex items-center gap-2">
                  <Icon className="h-4 w-4" />
                  {ticket.issue_type}
                </span>
              </div>
              {ticket.assignee && (
                <div>
                  <p className="text-xs text-gray-500 mb-1">Assignee</p>
                  <span className="text-gray-300 flex items-center gap-2">
                    <User className="h-4 w-4" />
                    {ticket.assignee.name}
                  </span>
                </div>
              )}
              <div>
                <p className="text-xs text-gray-500 mb-1">Created</p>
                <span className="text-gray-300 flex items-center gap-2">
                  <Clock className="h-4 w-4" />
                  {new Date(ticket.created).toLocaleDateString()}
                </span>
              </div>
            </CardContent>
          </Card>

          {/* Labels Card */}
          {ticket.labels.length > 0 && (
            <Card className="bg-gray-800 border-gray-700">
              <CardHeader>
                <CardTitle className="text-sm text-gray-400">
                  <Tag className="h-4 w-4 inline mr-2" />
                  Labels
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {ticket.labels.map((l) => (
                    <Badge key={l} className="border-gray-500 bg-transparent text-gray-300">
                      {l}
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Agent Capabilities Card */}
          <Card className="bg-gray-800 border-gray-700">
            <CardHeader>
              <CardTitle className="text-sm text-gray-400">
                <Code className="h-4 w-4 inline mr-2" />
                Agent Capabilities
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-3 text-sm">
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Analyze requirements from Jira</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Generate Airflow DAGs</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Generate PySpark jobs</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Validate syntax & security</span>
                </li>
                <li className="flex items-start gap-2 text-gray-400">
                  <CheckCircle className="h-4 w-4 text-green-500 mt-0.5" />
                  <span>Create pull requests</span>
                </li>
              </ul>
            </CardContent>
          </Card>

          {/* Quick Actions */}
          <Card className="bg-gray-800 border-gray-700">
            <CardHeader>
              <CardTitle className="text-sm text-gray-400">Quick Actions</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              <Button
                variant="outline"
                className="w-full justify-start"
                onClick={() => setActiveTab('configure')}
              >
                <Database className="h-4 w-4 mr-2" />
                Create Data Pipeline
              </Button>
              <Button
                variant="outline"
                className="w-full justify-start"
                onClick={handleProcess}
                disabled={isProcessing}
              >
                <Code className="h-4 w-4 mr-2" />
                Generate Code
              </Button>
            </CardContent>
          </Card>
        </div>
      </div>
    </PageLayout>
  )
}
