'use client'

import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { QUERY_KEYS } from '@/lib/constants'
import { Card, CardContent } from '../ui/Card'
import { Badge } from '../ui/Badge'
import {
  CheckCircle,
  XCircle,
  Clock,
  AlertTriangle,
  FileCode,
  Play,
  Shield,
  GitBranch,
  RefreshCw
} from 'lucide-react'
import toast from 'react-hot-toast'

interface Approval {
  id: string
  incident_id: string
  workflow_id: string
  status: string
  plan: {
    action_type: string
    script_id: string
    script_path: string
    workflow_name: string
    steps: Array<{ step: number; action: string; timeout: number }>
    confidence: number
  }
  judge_score: {
    quality_score: number
    safety_passed: boolean
    risk_level: string
    reasoning: string
  }
  approval_decision: {
    route: string
    risk_level: string
    risk_score: number
    reasoning: string
  }
  created_at: string
  requires_action?: 'approval' | 'retry'
  error?: string
}

interface LangGraphApprovalListProps {
  approvals: Approval[]
  onUpdate: () => void
}

export function LangGraphApprovalList({ approvals, onUpdate }: LangGraphApprovalListProps) {
  const [approving, setApproving] = useState<string | null>(null)
  const [rejecting, setRejecting] = useState<string | null>(null)
  const [retrying, setRetrying] = useState<string | null>(null)
  const queryClient = useQueryClient()

  const approveMutation = useMutation({
    mutationFn: (incidentId: string) => api.approveIncident(incidentId, {
      approver: 'admin',
      reason: 'Approved via UI'
    }),
    onSuccess: (data) => {
      if (data.status === 'approved_but_trigger_failed') {
        toast.error('Approved but GitHub trigger failed. Click Retry to try again.')
      } else {
        toast.success('Approval granted - workflow continuing')
      }
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.APPROVALS] })
      onUpdate()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to approve')
    },
    onSettled: () => setApproving(null)
  })

  const rejectMutation = useMutation({
    mutationFn: (incidentId: string) => api.rejectIncident(incidentId, {
      approver: 'admin',
      reason: 'Rejected via UI'
    }),
    onSuccess: () => {
      toast.success('Plan rejected')
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.APPROVALS] })
      onUpdate()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to reject')
    },
    onSettled: () => setRejecting(null)
  })

  const retryMutation = useMutation({
    mutationFn: (incidentId: string) => api.retryGitHubTrigger(incidentId),
    onSuccess: (data) => {
      if (data.status === 'retry_success') {
        toast.success('GitHub Actions workflow triggered successfully!')
      } else {
        toast.error('Retry failed. Try again later.')
      }
      queryClient.invalidateQueries({ queryKey: [QUERY_KEYS.APPROVALS] })
      onUpdate()
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Retry failed')
    },
    onSettled: () => setRetrying(null)
  })

  const handleApprove = (incidentId: string) => {
    setApproving(incidentId)
    approveMutation.mutate(incidentId)
  }

  const handleReject = (incidentId: string) => {
    setRejecting(incidentId)
    rejectMutation.mutate(incidentId)
  }

  const handleRetry = (incidentId: string) => {
    setRetrying(incidentId)
    retryMutation.mutate(incidentId)
  }

  const getRiskBadgeVariant = (riskLevel: string) => {
    switch (riskLevel?.toLowerCase()) {
      case 'critical':
        return 'error'
      case 'high':
        return 'error'
      case 'medium':
        return 'warning'
      case 'low':
        return 'success'
      default:
        return 'info'
    }
  }

  if (approvals.length === 0) {
    return (
      <Card variant="bordered">
        <CardContent className="p-12 text-center">
          <CheckCircle className="h-12 w-12 text-green-500 mx-auto mb-4" />
          <h3 className="text-lg font-semibold text-gray-900 mb-2">No Pending Approvals</h3>
          <p className="text-gray-500">All workflows are either auto-approved or already processed.</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4">
      {approvals.map((approval) => (
        <Card key={approval.id} variant="bordered" className="hover:shadow-md transition-shadow">
          <CardContent className="p-6">
            <div className="flex items-start justify-between">
              {/* Left side - Info */}
              <div className="flex items-start gap-4 flex-1">
                <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-orange-100 text-orange-600">
                  <AlertTriangle className="h-6 w-6" />
                </div>

                <div className="flex-1">
                  {/* Header */}
                  <div className="flex items-center gap-3 mb-3">
                    <h4 className="font-semibold text-gray-900">
                      Incident: {approval.incident_id}
                    </h4>
                    {approval.requires_action === 'retry' ? (
                      <Badge variant="error">Needs Retry</Badge>
                    ) : (
                      <Badge variant="warning">Pending Approval</Badge>
                    )}
                    <Badge variant={getRiskBadgeVariant(approval.approval_decision?.risk_level)}>
                      {approval.approval_decision?.risk_level || 'Unknown'} Risk
                    </Badge>
                  </div>

                  {/* Error message for failed triggers */}
                  {approval.requires_action === 'retry' && approval.error && (
                    <div className="mb-3 p-2 bg-red-50 border border-red-200 rounded text-sm text-red-700">
                      <strong>Trigger Failed:</strong> {approval.error}
                    </div>
                  )}

                  {/* Plan Details */}
                  <div className="grid grid-cols-2 gap-4 mb-4">
                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-sm">
                        <FileCode className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Script:</span>
                        <span className="font-mono text-blue-600">{approval.plan?.script_id}</span>
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <Play className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Action:</span>
                        <span>{approval.plan?.action_type}</span>
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <GitBranch className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Workflow:</span>
                        <span>{approval.plan?.workflow_name}</span>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-sm">
                        <Shield className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Judge Score:</span>
                        <span className={approval.judge_score?.quality_score >= 6 ? 'text-green-600' : 'text-red-600'}>
                          {approval.judge_score?.quality_score?.toFixed(1)}/10
                        </span>
                        {approval.judge_score?.safety_passed && (
                          <Badge variant="success" size="sm">Safety ✓</Badge>
                        )}
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <Clock className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Route:</span>
                        <span className="uppercase text-orange-600 font-medium">
                          {approval.approval_decision?.route}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 text-sm">
                        <AlertTriangle className="h-4 w-4 text-gray-400" />
                        <span className="font-medium text-gray-700">Risk Score:</span>
                        <span>{(approval.approval_decision?.risk_score * 100)?.toFixed(0)}%</span>
                      </div>
                    </div>
                  </div>

                  {/* Steps */}
                  {approval.plan?.steps && (
                    <div className="mb-4 p-3 bg-gray-50 rounded-lg">
                      <h5 className="text-sm font-medium text-gray-700 mb-2">Execution Steps:</h5>
                      <div className="space-y-1">
                        {approval.plan.steps.map((step, idx) => (
                          <div key={idx} className="flex items-center gap-2 text-sm text-gray-600">
                            <span className="flex h-5 w-5 items-center justify-center rounded-full bg-blue-100 text-blue-600 text-xs font-medium">
                              {step.step}
                            </span>
                            <span>{step.action}</span>
                            <span className="text-gray-400">({step.timeout}s timeout)</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Reasoning */}
                  {approval.approval_decision?.reasoning && (
                    <div className="text-sm text-gray-500 italic">
                      "{approval.approval_decision.reasoning}"
                    </div>
                  )}
                </div>
              </div>

              {/* Right side - Actions */}
              <div className="flex flex-col gap-2 ml-4">
                {approval.requires_action === 'retry' ? (
                  /* Retry button for failed triggers */
                  <button
                    onClick={() => handleRetry(approval.incident_id)}
                    disabled={retrying === approval.incident_id}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    {retrying === approval.incident_id ? (
                      <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                    ) : (
                      <RefreshCw className="h-4 w-4" />
                    )}
                    Retry Trigger
                  </button>
                ) : (
                  /* Approve/Reject buttons for pending approvals */
                  <>
                    <button
                      onClick={() => handleApprove(approval.incident_id)}
                      disabled={approving === approval.incident_id || rejecting === approval.incident_id}
                      className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      {approving === approval.incident_id ? (
                        <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                      ) : (
                        <CheckCircle className="h-4 w-4" />
                      )}
                      Approve
                    </button>
                    <button
                      onClick={() => handleReject(approval.incident_id)}
                      disabled={approving === approval.incident_id || rejecting === approval.incident_id}
                      className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                    >
                      {rejecting === approval.incident_id ? (
                        <div className="h-4 w-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                      ) : (
                        <XCircle className="h-4 w-4" />
                      )}
                      Reject
                    </button>
                  </>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
