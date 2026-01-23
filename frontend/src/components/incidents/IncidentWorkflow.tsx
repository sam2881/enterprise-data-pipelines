'use client'

import { useState } from 'react'
import {
  Brain,
  Bot,
  CheckCircle,
  UserCheck,
  Zap,
  Database,
  CheckSquare,
  AlertCircle,
  ChevronRight,
  Clock,
  TrendingUp,
  X,
  Check,
  ChevronDown,
  ChevronUp,
  FileInput,
  FileText,
  Scale,
  Tag,
  Search,
  GitBranch,
  Merge,
  Target,
  Shield,
  ShieldCheck,
  Play,
  ClipboardCheck,
  XCircle,
  BookOpen,
  Lightbulb
} from 'lucide-react'
import { Badge } from '@/components/ui/Badge'
import { Button } from '@/components/ui/Button'

export interface WorkflowStepData {
  id: string
  name: string
  description: string
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | 'waiting_approval'
  timestamp?: string
  duration?: string

  // Step-specific data
  analysis?: {
    classification: string
    category: string
    severity: string
    reasoning: string
    confidence: number
    key_signals: string[]
    root_cause_hypothesis: string
  }

  agent_selection?: {
    selected_agent: string
    agent_name: string
    confidence: number
    reasoning: string
    alternative_agents?: string[]
  }

  human_override?: {
    override_used: boolean
    selected_agent?: string
    reason?: string
  }

  resolution_plan?: {
    steps: Array<{
      step_number: number
      action: string
      reasoning: string
      expected_impact: string
      rollback_step?: string
    }>
    estimated_time: string
    risk_level: 'low' | 'medium' | 'high'
    requires_approval: boolean
  }

  approval?: {
    status: 'pending' | 'approved' | 'rejected'
    approver?: string
    approved_at?: string
    comments?: string
  }

  execution?: {
    status: 'running' | 'completed' | 'failed'
    logs: string[]
    started_at?: string
    completed_at?: string
    error?: string
  }

  confirmation?: {
    status: 'pending' | 'confirmed' | 'failed'
    validation_checks: Array<{
      name: string
      status: 'passed' | 'failed'
      details: string
    }>
  }

  learning?: {
    rag_updated: boolean
    graph_updated: boolean
    relationships_created: number
  }
}

interface IncidentWorkflowProps {
  incident_id: string
  steps: WorkflowStepData[]
  onApprove?: (stepId: string, comments: string) => void
  onReject?: (stepId: string, reason: string) => void
  onOverrideAgent?: (agentId: string) => void
  onRetry?: (stepId: string) => void
}

export function IncidentWorkflow({
  incident_id,
  steps,
  onApprove,
  onReject,
  onOverrideAgent,
  onRetry
}: IncidentWorkflowProps) {
  const [expandedStep, setExpandedStep] = useState<string | null>(null)
  const [approvalComments, setApprovalComments] = useState('')
  const [rejectionReason, setRejectionReason] = useState('')

  const getStepIcon = (stepId: string) => {
    // Support 7-Node LangGraph workflow (Simplified Architecture)
    // Single LLM Judge after Plan Generation for safety/feasibility validation
    const icons: Record<string, any> = {
      // 7-Node Architecture
      'receive_parse': FileInput,        // 1. Receive & Parse incident
      'swarm_rag': Database,             // 2. Swarm RAG Search (Vector + Graph + Keyword)
      'generate_plan': BookOpen,         // 3. Generate Plan
      'llm_judge': Brain,                // 4. LLM Judge (validates plan safety/feasibility)
      'control_plane': Shield,           // 5. Control Plane (HITL approval)
      'execute': Play,                   // 6. Execute (GitHub Actions)
      'verify_close': CheckCircle,       // 7. Verify & Close
      // Legacy support for old step IDs
      'step1_analysis': Brain,
      'step2_agent_selection': Bot,
      'step3_human_override': UserCheck,
      'step4_resolution_plan': CheckCircle,
      'step5_approval': UserCheck,
      'step6_execution': Zap,
      'step7_confirmation': CheckSquare,
      'step8_learning': Database
    }
    return icons[stepId] || AlertCircle
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800 border-green-200'
      case 'in_progress':
        return 'bg-blue-100 text-blue-800 border-blue-200'
      case 'waiting_approval':
        return 'bg-yellow-100 text-yellow-800 border-yellow-200'
      case 'failed':
        return 'bg-red-100 text-red-800 border-red-200'
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <Check className="w-5 h-5 text-green-600" />
      case 'in_progress':
        return <Clock className="w-5 h-5 text-blue-600 animate-spin" />
      case 'waiting_approval':
        return <UserCheck className="w-5 h-5 text-yellow-600" />
      case 'failed':
        return <X className="w-5 h-5 text-red-600" />
      default:
        return <AlertCircle className="w-5 h-5 text-gray-400" />
    }
  }

  const currentStepIndex = steps.findIndex(s => s.status === 'in_progress' || s.status === 'waiting_approval')
  const progress = currentStepIndex >= 0 ? ((currentStepIndex + 1) / steps.length) * 100 : 0

  return (
    <div className="space-y-6">
      {/* Progress Bar */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Workflow Progress</h3>
            <p className="text-sm text-gray-500">
              Step {currentStepIndex + 1} of {steps.length}
            </p>
          </div>
          <div className="text-2xl font-bold text-blue-600">
            {Math.round(progress)}%
          </div>
        </div>

        <div className="relative">
          <div className="h-3 bg-gray-200 rounded-full overflow-hidden">
            <div
              className="h-full bg-gradient-to-r from-blue-500 via-purple-500 to-green-500 transition-all duration-500"
              style={{ width: `${progress}%` }}
            />
          </div>
        </div>
      </div>

      {/* Workflow Steps */}
      <div className="space-y-4">
        {steps.map((step, index) => {
          const Icon = getStepIcon(step.id)
          const isExpanded = expandedStep === step.id
          const isLast = index === steps.length - 1

          return (
            <div key={step.id} className="relative">
              {/* Connector Line */}
              {!isLast && (
                <div className="absolute left-8 top-16 bottom-0 w-0.5 bg-gray-200 -mb-4" />
              )}

              {/* Step Card */}
              <div className={`bg-white rounded-lg border-2 transition-all ${
                step.status === 'in_progress'
                  ? 'border-blue-500 shadow-lg'
                  : step.status === 'completed'
                  ? 'border-green-200'
                  : 'border-gray-200'
              }`}>
                <div className="p-6">
                  <div className="flex items-start gap-4">
                    {/* Step Icon */}
                    <div className={`flex-shrink-0 w-16 h-16 rounded-full flex items-center justify-center ${
                      step.status === 'completed' ? 'bg-green-100' :
                      step.status === 'in_progress' ? 'bg-blue-100' :
                      step.status === 'waiting_approval' ? 'bg-yellow-100' :
                      step.status === 'failed' ? 'bg-red-100' :
                      'bg-gray-100'
                    }`}>
                      <Icon className={`w-8 h-8 ${
                        step.status === 'completed' ? 'text-green-600' :
                        step.status === 'in_progress' ? 'text-blue-600' :
                        step.status === 'waiting_approval' ? 'text-yellow-600' :
                        step.status === 'failed' ? 'text-red-600' :
                        'text-gray-400'
                      }`} />
                    </div>

                    {/* Step Content */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between mb-2">
                        <div className="flex items-center gap-3">
                          <h3 className="text-lg font-semibold text-gray-900">
                            {step.name}
                          </h3>
                          <Badge className={getStatusColor(step.status)}>
                            {step.status.replace('_', ' ').toUpperCase()}
                          </Badge>
                        </div>

                        <div className="flex items-center gap-2">
                          {step.timestamp && (
                            <span className="text-sm text-gray-500">
                              {new Date(step.timestamp).toLocaleTimeString()}
                            </span>
                          )}
                          {step.duration && (
                            <span className="text-sm text-gray-500">
                              • {step.duration}
                            </span>
                          )}
                          {getStatusIcon(step.status)}
                        </div>
                      </div>

                      <p className="text-gray-600 mb-4">{step.description}</p>

                      {/* Step-Specific Content Preview */}
                      {step.analysis && (
                        <div className="bg-gray-50 rounded-lg p-4 space-y-2">
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-gray-700">Classification:</span>
                            <span className="text-gray-900">{step.analysis.classification}</span>
                            <Badge className="ml-2">Confidence: {(step.analysis.confidence * 100).toFixed(0)}%</Badge>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-gray-700">Severity:</span>
                            <Badge className={
                              step.analysis.severity === 'critical' ? 'bg-red-100 text-red-800' :
                              step.analysis.severity === 'high' ? 'bg-orange-100 text-orange-800' :
                              step.analysis.severity === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                              'bg-blue-100 text-blue-800'
                            }>
                              {step.analysis.severity.toUpperCase()}
                            </Badge>
                          </div>
                        </div>
                      )}

                      {step.agent_selection && (
                        <div className="bg-gray-50 rounded-lg p-4">
                          <div className="flex items-center gap-2 mb-2">
                            <Bot className="w-5 h-5 text-blue-600" />
                            <span className="font-medium text-gray-900">{step.agent_selection.agent_name}</span>
                            <Badge>Confidence: {(step.agent_selection.confidence * 100).toFixed(0)}%</Badge>
                          </div>
                          <p className="text-sm text-gray-600">{step.agent_selection.reasoning}</p>
                        </div>
                      )}

                      {step.resolution_plan && (
                        <div className="bg-gray-50 rounded-lg p-4">
                          <div className="flex items-center justify-between mb-3">
                            <span className="font-medium text-gray-900">
                              {step.resolution_plan.steps.length} Step Plan
                            </span>
                            <div className="flex items-center gap-2">
                              <Badge className={
                                step.resolution_plan.risk_level === 'high' ? 'bg-red-100 text-red-800' :
                                step.resolution_plan.risk_level === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                'bg-green-100 text-green-800'
                              }>
                                {step.resolution_plan.risk_level.toUpperCase()} RISK
                              </Badge>
                              <span className="text-sm text-gray-500">
                                Est. {step.resolution_plan.estimated_time}
                              </span>
                            </div>
                          </div>
                        </div>
                      )}

                      {/* Approval UI */}
                      {step.status === 'waiting_approval' && step.approval && (
                        <div className="mt-4 bg-yellow-50 border border-yellow-200 rounded-lg p-4">
                          <div className="flex items-center gap-2 mb-3">
                            <UserCheck className="w-5 h-5 text-yellow-600" />
                            <span className="font-semibold text-yellow-900">Human Approval Required</span>
                          </div>

                          <textarea
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg mb-3"
                            placeholder="Add comments or modifications..."
                            value={approvalComments}
                            onChange={(e) => setApprovalComments(e.target.value)}
                            rows={3}
                          />

                          <div className="flex gap-3">
                            <Button
                              onClick={() => {
                                onApprove?.(step.id, approvalComments)
                                setApprovalComments('')
                              }}
                              className="flex-1 bg-green-600 hover:bg-green-700 text-white"
                            >
                              <Check className="w-4 h-4 mr-2" />
                              Approve
                            </Button>
                            <Button
                              onClick={() => {
                                onReject?.(step.id, rejectionReason || 'Rejected')
                                setRejectionReason('')
                              }}
                              className="flex-1 bg-red-600 hover:bg-red-700 text-white"
                            >
                              <X className="w-4 h-4 mr-2" />
                              Reject
                            </Button>
                          </div>
                        </div>
                      )}

                      {/* Expand/Collapse Button */}
                      <button
                        onClick={() => setExpandedStep(isExpanded ? null : step.id)}
                        className="mt-4 flex items-center gap-2 text-blue-600 hover:text-blue-700 font-medium"
                      >
                        {isExpanded ? (
                          <>
                            <ChevronUp className="w-4 h-4" />
                            Hide Details
                          </>
                        ) : (
                          <>
                            <ChevronDown className="w-4 h-4" />
                            Show Details
                          </>
                        )}
                      </button>

                      {/* Expanded Details */}
                      {isExpanded && (
                        <div className="mt-4 pt-4 border-t border-gray-200 space-y-4">
                          {step.analysis && (
                            <div>
                              <h4 className="font-semibold text-gray-900 mb-2">Full Analysis</h4>
                              <div className="bg-gray-50 rounded-lg p-4 space-y-3">
                                <div>
                                  <span className="text-sm font-medium text-gray-700">Reasoning:</span>
                                  <p className="text-sm text-gray-600 mt-1">{step.analysis.reasoning}</p>
                                </div>
                                <div>
                                  <span className="text-sm font-medium text-gray-700">Root Cause Hypothesis:</span>
                                  <p className="text-sm text-gray-600 mt-1">{step.analysis.root_cause_hypothesis}</p>
                                </div>
                                <div>
                                  <span className="text-sm font-medium text-gray-700">Key Signals:</span>
                                  <div className="flex flex-wrap gap-2 mt-1">
                                    {step.analysis.key_signals.map((signal, i) => (
                                      <Badge key={i} className="bg-blue-100 text-blue-800">
                                        {signal}
                                      </Badge>
                                    ))}
                                  </div>
                                </div>
                              </div>
                            </div>
                          )}

                          {step.resolution_plan && (
                            <div>
                              <h4 className="font-semibold text-gray-900 mb-2">Resolution Plan</h4>
                              <div className="space-y-3">
                                {step.resolution_plan.steps.map((planStep) => (
                                  <div key={planStep.step_number} className="bg-gray-50 rounded-lg p-4">
                                    <div className="flex items-start gap-3">
                                      <div className="flex-shrink-0 w-8 h-8 rounded-full bg-blue-100 flex items-center justify-center">
                                        <span className="text-sm font-semibold text-blue-600">
                                          {planStep.step_number}
                                        </span>
                                      </div>
                                      <div className="flex-1">
                                        <p className="font-medium text-gray-900 mb-1">{planStep.action}</p>
                                        <p className="text-sm text-gray-600 mb-2">{planStep.reasoning}</p>
                                        <div className="flex items-center gap-4 text-sm">
                                          <span className="text-gray-500">
                                            Impact: {planStep.expected_impact}
                                          </span>
                                          {planStep.rollback_step && (
                                            <span className="text-orange-600">
                                              Rollback: {planStep.rollback_step}
                                            </span>
                                          )}
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}

                          {step.execution && (
                            <div>
                              <h4 className="font-semibold text-gray-900 mb-2">Execution Logs</h4>
                              <div className="bg-gray-900 rounded-lg p-4 font-mono text-sm text-green-400 max-h-64 overflow-y-auto">
                                {step.execution.logs.map((log, i) => (
                                  <div key={i}>{log}</div>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
