'use client'

import { useState } from 'react'
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card'
import { Badge } from '../ui/Badge'
import {
  CheckCircle2,
  Circle,
  Clock,
  AlertCircle,
  ArrowRight,
  Brain,
  UserCheck,
  Zap,
  Database,
  CheckCheck
} from 'lucide-react'

export interface WorkflowStep {
  id: string
  name: string
  description: string
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | 'waiting_approval'
  timestamp?: string
  duration?: string
  agent?: string
  llm_model?: string
  confidence?: number
  output?: any
}

export interface WorkflowData {
  incident_id: string
  workflow_id: string
  status: 'active' | 'completed' | 'failed'
  current_step: number
  steps: WorkflowStep[]
  created_at: string
  updated_at: string
}

interface WorkflowVisualizationProps {
  workflow: WorkflowData
  onApprove?: (stepId: string) => void
  onReject?: (stepId: string) => void
}

// Step icons aligned with 7-node LangGraph architecture
const stepIcons: Record<string, any> = {
  receive_parse: Database,      // 1. Receive & Parse
  swarm_rag: Database,          // 2. Swarm RAG Search
  generate_plan: Brain,         // 3. Generate Plan (LLM)
  llm_judge: UserCheck,         // 4. LLM Judge
  control_plane: AlertCircle,   // 5. Control Plane (HITL)
  execute: Zap,                 // 6. Execute
  verify_close: CheckCheck,     // 7. Verify & Close
}

const statusColors = {
  pending: 'bg-gray-100 border-gray-300 text-gray-600',
  in_progress: 'bg-blue-50 border-blue-300 text-blue-700',
  completed: 'bg-green-50 border-green-300 text-green-700',
  failed: 'bg-red-50 border-red-300 text-red-700',
  waiting_approval: 'bg-amber-50 border-amber-300 text-amber-700',
}

const statusIcons = {
  pending: Circle,
  in_progress: Clock,
  completed: CheckCircle2,
  failed: AlertCircle,
  waiting_approval: UserCheck,
}

export function WorkflowVisualization({ workflow, onApprove, onReject }: WorkflowVisualizationProps) {
  const [expandedStep, setExpandedStep] = useState<string | null>(null)

  const getStepIcon = (step: WorkflowStep) => {
    const Icon = stepIcons[step.id] || Circle
    return Icon
  }

  const getStatusIcon = (status: WorkflowStep['status']) => {
    const Icon = statusIcons[status]
    return Icon
  }

  return (
    <div className="space-y-6">
      {/* Workflow Header */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Incident Resolution Workflow</CardTitle>
              <p className="text-sm text-gray-500 mt-1">
                Workflow ID: {workflow.workflow_id}
              </p>
            </div>
            <div className="text-right">
              <Badge
                variant={workflow.status === 'completed' ? 'success' : workflow.status === 'failed' ? 'error' : 'info'}
              >
                {workflow.status.toUpperCase()}
              </Badge>
              <p className="text-xs text-gray-500 mt-1">
                Step {workflow.current_step} of {workflow.steps.length}
              </p>
            </div>
          </div>
        </CardHeader>
      </Card>

      {/* Progress Bar */}
      <div className="relative">
        <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
          <div
            className="h-full bg-gradient-to-r from-blue-500 to-green-500 transition-all duration-500"
            style={{ width: `${(workflow.current_step / workflow.steps.length) * 100}%` }}
          />
        </div>
        <div className="absolute -top-1 left-0 w-full flex justify-between">
          {workflow.steps.map((_, index) => (
            <div
              key={index}
              className={`w-4 h-4 rounded-full border-2 ${
                index < workflow.current_step
                  ? 'bg-green-500 border-green-500'
                  : index === workflow.current_step
                  ? 'bg-blue-500 border-blue-500 animate-pulse'
                  : 'bg-gray-200 border-gray-300'
              }`}
            />
          ))}
        </div>
      </div>

      {/* Workflow Steps */}
      <div className="space-y-4">
        {workflow.steps.map((step, index) => {
          const StepIcon = getStepIcon(step)
          const StatusIcon = getStatusIcon(step.status)
          const isExpanded = expandedStep === step.id
          const isActive = index === workflow.current_step
          const isCompleted = index < workflow.current_step

          return (
            <div key={step.id} className="relative">
              {/* Connector Line */}
              {index < workflow.steps.length - 1 && (
                <div className="absolute left-6 top-14 w-0.5 h-full bg-gray-200" />
              )}

              <Card
                variant="bordered"
                className={`relative transition-all duration-300 ${
                  isActive ? 'ring-2 ring-blue-500 shadow-lg' : ''
                } ${isCompleted ? 'opacity-75' : ''}`}
              >
                <CardContent className="p-4">
                  <div className="flex items-start gap-4">
                    {/* Step Icon */}
                    <div className={`relative flex-shrink-0 w-12 h-12 rounded-full flex items-center justify-center border-2 ${statusColors[step.status]}`}>
                      <StepIcon className="w-6 h-6" />
                      <div className="absolute -bottom-1 -right-1">
                        <StatusIcon className="w-4 h-4" />
                      </div>
                    </div>

                    {/* Step Content */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-start justify-between gap-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-2">
                            <h3 className="font-semibold text-gray-900">{step.name}</h3>
                            <Badge variant={step.status === 'completed' ? 'success' : step.status === 'failed' ? 'error' : 'default'}>
                              {step.status.replace('_', ' ')}
                            </Badge>
                          </div>
                          <p className="text-sm text-gray-600 mt-1">{step.description}</p>

                          {/* Step Metadata */}
                          <div className="flex flex-wrap gap-4 mt-3 text-xs text-gray-500">
                            {step.timestamp && (
                              <span className="flex items-center gap-1">
                                <Clock className="w-3 h-3" />
                                {new Date(step.timestamp).toLocaleTimeString()}
                              </span>
                            )}
                            {step.duration && (
                              <span>Duration: {step.duration}</span>
                            )}
                            {step.agent && (
                              <span className="flex items-center gap-1">
                                <Zap className="w-3 h-3" />
                                Agent: {step.agent}
                              </span>
                            )}
                            {step.llm_model && (
                              <span className="flex items-center gap-1">
                                <Brain className="w-3 h-3" />
                                Model: {step.llm_model}
                              </span>
                            )}
                            {step.confidence !== undefined && (
                              <span>Confidence: {(step.confidence * 100).toFixed(0)}%</span>
                            )}
                          </div>

                          {/* HITL Approval Actions */}
                          {step.status === 'waiting_approval' && (onApprove || onReject) && (
                            <div className="mt-4 flex gap-2">
                              {onApprove && (
                                <button
                                  onClick={() => onApprove(step.id)}
                                  className="px-4 py-2 bg-green-600 text-white text-sm font-medium rounded-lg hover:bg-green-700 transition-colors"
                                >
                                  Approve
                                </button>
                              )}
                              {onReject && (
                                <button
                                  onClick={() => onReject(step.id)}
                                  className="px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700 transition-colors"
                                >
                                  Reject
                                </button>
                              )}
                            </div>
                          )}

                          {/* Expandable Output */}
                          {step.output && (
                            <button
                              onClick={() => setExpandedStep(isExpanded ? null : step.id)}
                              className="mt-3 text-sm text-blue-600 hover:text-blue-700 font-medium"
                            >
                              {isExpanded ? 'Hide Details' : 'Show Details'}
                            </button>
                          )}

                          {/* Expanded Details */}
                          {isExpanded && step.output && (
                            <div className="mt-3 p-4 bg-gray-50 rounded-lg">
                              <pre className="text-xs text-gray-700 overflow-x-auto">
                                {JSON.stringify(step.output, null, 2)}
                              </pre>
                            </div>
                          )}
                        </div>

                        {/* Arrow to Next Step */}
                        {index < workflow.steps.length - 1 && (
                          <ArrowRight className="w-5 h-5 text-gray-400 flex-shrink-0 mt-1" />
                        )}
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          )
        })}
      </div>

      {/* Workflow Timeline */}
      <Card>
        <CardHeader>
          <CardTitle>Timeline</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-sm text-gray-600 space-y-1">
            <div className="flex justify-between">
              <span>Started:</span>
              <span className="font-medium">{new Date(workflow.created_at).toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span>Last Updated:</span>
              <span className="font-medium">{new Date(workflow.updated_at).toLocaleString()}</span>
            </div>
            {workflow.status === 'completed' && (
              <div className="flex justify-between text-green-600">
                <span>Completed:</span>
                <span className="font-medium">{new Date(workflow.updated_at).toLocaleString()}</span>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
