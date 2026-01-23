'use client'

/**
 * PipelineProgress - Real-time pipeline execution progress display
 *
 * Features:
 * - WebSocket connection for live status updates
 * - Visual progress bar and phase indicators
 * - Approval action buttons for PROD deployments
 * - Error display and retry capability
 * - Artifact preview when complete
 */

import React, { useEffect, useState, useCallback } from 'react'
import { Button } from '@/components/ui/Button'
import { Card } from '@/components/ui/Card'
import { Badge } from '@/components/ui/Badge'
import { cn } from '@/lib/utils'
import {
  PipelineStatus,
  PipelinePhase,
  PipelineArtifacts,
  DeploymentResult,
} from '@/types/pipeline'

// =============================================================================
// Component Props
// =============================================================================

interface PipelineProgressProps {
  requestId: string
  initialStatus?: PipelineStatus
  onApprove?: (requestId: string) => Promise<void>
  onReject?: (requestId: string) => Promise<void>
  onRetry?: () => void
  onClose?: () => void
  showArtifacts?: boolean
  className?: string
}

// =============================================================================
// Phase Configuration
// =============================================================================

interface PhaseConfig {
  label: string
  icon: string
  description: string
}

const PHASES: Record<PipelinePhase, PhaseConfig> = {
  init: {
    label: 'Initializing',
    icon: '🚀',
    description: 'Setting up pipeline request',
  },
  planning: {
    label: 'Planning',
    icon: '📋',
    description: 'Analyzing intent and selecting templates',
  },
  generating: {
    label: 'Generating',
    icon: '⚙️',
    description: 'Creating DAGs and Spark jobs',
  },
  validating: {
    label: 'Validating',
    icon: '🔍',
    description: 'Checking syntax and security',
  },
  awaiting_approval: {
    label: 'Awaiting Approval',
    icon: '✋',
    description: 'Waiting for human approval',
  },
  deploying: {
    label: 'Deploying',
    icon: '📦',
    description: 'Committing to Git and triggering CI/CD',
  },
  complete: {
    label: 'Complete',
    icon: '✅',
    description: 'Pipeline deployed successfully',
  },
  failed: {
    label: 'Failed',
    icon: '❌',
    description: 'Pipeline generation failed',
  },
}

const PHASE_ORDER: PipelinePhase[] = [
  'init',
  'planning',
  'generating',
  'validating',
  'awaiting_approval',
  'deploying',
  'complete',
]

// =============================================================================
// Phase Step Component
// =============================================================================

interface PhaseStepProps {
  phase: PipelinePhase
  currentPhase: PipelinePhase
  index: number
}

function PhaseStep({ phase, currentPhase, index }: PhaseStepProps) {
  const config = PHASES[phase]
  const currentIndex = PHASE_ORDER.indexOf(currentPhase)
  const isComplete = index < currentIndex
  const isCurrent = phase === currentPhase
  const isFailed = currentPhase === 'failed'

  // Skip approval step in visualization if not needed
  if (phase === 'awaiting_approval' && !isCurrent && index > currentIndex) {
    return null
  }

  return (
    <div className="flex items-center">
      <div
        className={cn(
          'flex items-center justify-center w-10 h-10 rounded-full border-2 transition-all',
          isComplete && 'bg-green-500 border-green-500 text-white',
          isCurrent && !isFailed && 'bg-blue-500 border-blue-500 text-white animate-pulse',
          isCurrent && isFailed && 'bg-red-500 border-red-500 text-white',
          !isComplete && !isCurrent && 'bg-gray-100 border-gray-300 text-gray-400 dark:bg-gray-800 dark:border-gray-600'
        )}
      >
        {isComplete ? (
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
        ) : (
          <span className="text-lg">{config.icon}</span>
        )}
      </div>
      {index < PHASE_ORDER.length - 1 && (
        <div
          className={cn(
            'w-12 h-1 mx-1',
            index < currentIndex ? 'bg-green-500' : 'bg-gray-200 dark:bg-gray-700'
          )}
        />
      )}
    </div>
  )
}

// =============================================================================
// Main Component
// =============================================================================

export function PipelineProgress({
  requestId,
  initialStatus,
  onApprove,
  onReject,
  onRetry,
  onClose,
  showArtifacts = false,
  className,
}: PipelineProgressProps) {
  const [status, setStatus] = useState<PipelineStatus | null>(initialStatus || null)
  const [isConnected, setIsConnected] = useState(false)
  const [isApproving, setIsApproving] = useState(false)
  const [isRejecting, setIsRejecting] = useState(false)
  const [artifacts, setArtifacts] = useState<PipelineArtifacts | null>(null)
  const [deployment, setDeployment] = useState<DeploymentResult | null>(null)

  // WebSocket connection
  useEffect(() => {
    if (!requestId) return

    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const wsHost = process.env.NEXT_PUBLIC_WS_HOST || window.location.host
    const wsUrl = `${wsProtocol}//${wsHost}/ws/pipelines/${requestId}`

    let ws: WebSocket | null = null
    let reconnectTimeout: NodeJS.Timeout | null = null

    const connect = () => {
      ws = new WebSocket(wsUrl)

      ws.onopen = () => {
        console.log('WebSocket connected for pipeline:', requestId)
        setIsConnected(true)
      }

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)

          if (data.event === 'status_update') {
            setStatus(data.data)
          } else if (data.event === 'complete') {
            setStatus(data.data.status)
            if (data.data.deployment) {
              setDeployment(data.data.deployment)
            }
          } else if (data.event === 'artifacts') {
            setArtifacts(data.data)
          }
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error)
        }
      }

      ws.onclose = () => {
        console.log('WebSocket disconnected')
        setIsConnected(false)

        // Reconnect after 3 seconds if not complete or failed
        if (status?.status !== 'complete' && status?.status !== 'failed') {
          reconnectTimeout = setTimeout(connect, 3000)
        }
      }

      ws.onerror = (error) => {
        console.error('WebSocket error:', error)
      }
    }

    connect()

    return () => {
      if (ws) {
        ws.close()
      }
      if (reconnectTimeout) {
        clearTimeout(reconnectTimeout)
      }
    }
  }, [requestId])

  // Handle approval
  const handleApprove = useCallback(async () => {
    if (!onApprove) return

    setIsApproving(true)
    try {
      await onApprove(requestId)
    } catch (error) {
      console.error('Approval failed:', error)
    } finally {
      setIsApproving(false)
    }
  }, [onApprove, requestId])

  // Handle rejection
  const handleReject = useCallback(async () => {
    if (!onReject) return

    setIsRejecting(true)
    try {
      await onReject(requestId)
    } catch (error) {
      console.error('Rejection failed:', error)
    } finally {
      setIsRejecting(false)
    }
  }, [onReject, requestId])

  // Get current phase config
  const currentPhase = status?.status || 'init'
  const phaseConfig = PHASES[currentPhase]

  return (
    <Card className={cn('p-6', className)}>
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
            Pipeline Generation Progress
          </h3>
          <p className="text-sm text-gray-500 dark:text-gray-400">
            Request ID: {requestId}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge
            variant={
              currentPhase === 'complete'
                ? 'success'
                : currentPhase === 'failed'
                ? 'error'
                : 'warning'
            }
          >
            {phaseConfig.label}
          </Badge>
          {isConnected && (
            <Badge variant="success" className="text-xs">
              Live
            </Badge>
          )}
        </div>
      </div>

      {/* Progress Bar */}
      <div className="mb-6">
        <div className="flex justify-between text-sm text-gray-500 dark:text-gray-400 mb-2">
          <span>{status?.message || 'Initializing...'}</span>
          <span>{status?.progress_pct || 0}%</span>
        </div>
        <div className="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className={cn(
              'h-full transition-all duration-500',
              currentPhase === 'failed' ? 'bg-red-500' : 'bg-blue-500'
            )}
            style={{ width: `${status?.progress_pct || 0}%` }}
          />
        </div>
      </div>

      {/* Phase Steps */}
      <div className="flex items-center justify-center mb-6 overflow-x-auto">
        {PHASE_ORDER.slice(0, -1).map((phase, index) => (
          <PhaseStep
            key={phase}
            phase={phase}
            currentPhase={currentPhase}
            index={index}
          />
        ))}
      </div>

      {/* Current Phase Details */}
      <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-4 mb-6">
        <div className="flex items-center gap-3">
          <span className="text-2xl">{phaseConfig.icon}</span>
          <div>
            <h4 className="font-medium text-gray-900 dark:text-white">
              {phaseConfig.label}
            </h4>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {phaseConfig.description}
            </p>
          </div>
        </div>
      </div>

      {/* Approval Actions */}
      {currentPhase === 'awaiting_approval' && status?.human_approval_required && (
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4 mb-6">
          <div className="flex items-start gap-3">
            <span className="text-2xl">⚠️</span>
            <div className="flex-1">
              <h4 className="font-medium text-yellow-800 dark:text-yellow-200">
                Human Approval Required
              </h4>
              <p className="text-sm text-yellow-700 dark:text-yellow-300 mt-1">
                This pipeline is targeting a production environment or includes schema changes.
                Please review the generated artifacts before approving.
              </p>
              <div className="flex gap-2 mt-4">
                <Button
                  onClick={handleApprove}
                  isLoading={isApproving}
                  disabled={isRejecting}
                >
                  Approve & Deploy
                </Button>
                <Button
                  variant="outline"
                  onClick={handleReject}
                  isLoading={isRejecting}
                  disabled={isApproving}
                  className="text-red-600 border-red-300 hover:bg-red-50"
                >
                  Reject
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Error Display */}
      {currentPhase === 'failed' && status?.error && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 mb-6">
          <div className="flex items-start gap-3">
            <span className="text-2xl">❌</span>
            <div className="flex-1">
              <h4 className="font-medium text-red-800 dark:text-red-200">
                Pipeline Generation Failed
              </h4>
              <p className="text-sm text-red-700 dark:text-red-300 mt-1 font-mono">
                {status.error}
              </p>
              {onRetry && (
                <Button
                  variant="outline"
                  onClick={onRetry}
                  className="mt-4"
                >
                  Retry
                </Button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Success Display */}
      {currentPhase === 'complete' && (
        <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg p-4 mb-6">
          <div className="flex items-start gap-3">
            <span className="text-2xl">✅</span>
            <div className="flex-1">
              <h4 className="font-medium text-green-800 dark:text-green-200">
                Pipeline Deployed Successfully
              </h4>
              {deployment && (
                <div className="text-sm text-green-700 dark:text-green-300 mt-2 space-y-1">
                  <p>
                    <span className="font-medium">Branch:</span> {deployment.branch_name}
                  </p>
                  {deployment.pr_url && (
                    <p>
                      <span className="font-medium">Pull Request:</span>{' '}
                      <a
                        href={deployment.pr_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="underline hover:no-underline"
                      >
                        #{deployment.pr_number}
                      </a>
                    </p>
                  )}
                  {deployment.commit_sha && (
                    <p>
                      <span className="font-medium">Commit:</span>{' '}
                      <code className="bg-green-100 dark:bg-green-800 px-1 rounded">
                        {deployment.commit_sha.substring(0, 8)}
                      </code>
                    </p>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Artifacts Preview */}
      {showArtifacts && artifacts && currentPhase === 'complete' && (
        <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
          <div className="bg-gray-50 dark:bg-gray-800 px-4 py-2 border-b border-gray-200 dark:border-gray-700">
            <h4 className="font-medium text-gray-900 dark:text-white">
              Generated Artifacts
            </h4>
          </div>
          <div className="p-4 space-y-4">
            {/* DAG Code */}
            {artifacts.dag_code && (
              <div>
                <h5 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Airflow DAG
                </h5>
                <pre className="bg-gray-900 text-gray-100 p-3 rounded text-xs overflow-x-auto max-h-48">
                  {artifacts.dag_code.substring(0, 500)}
                  {artifacts.dag_code.length > 500 && '...'}
                </pre>
              </div>
            )}

            {/* Spark Jobs */}
            {artifacts.spark_jobs && Object.keys(artifacts.spark_jobs).length > 0 && (
              <div>
                <h5 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Spark Jobs ({Object.keys(artifacts.spark_jobs).length})
                </h5>
                <div className="flex flex-wrap gap-2">
                  {Object.keys(artifacts.spark_jobs).map((jobName) => (
                    <Badge key={jobName} variant="info">
                      {jobName}
                    </Badge>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Footer Actions */}
      <div className="flex justify-end mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
        {onClose && (
          <Button variant="outline" onClick={onClose}>
            Close
          </Button>
        )}
      </div>

      {/* Timestamps */}
      <div className="mt-4 text-xs text-gray-400 dark:text-gray-500 flex justify-between">
        {status?.started_at && (
          <span>Started: {new Date(status.started_at).toLocaleString()}</span>
        )}
        {status?.completed_at && (
          <span>Completed: {new Date(status.completed_at).toLocaleString()}</span>
        )}
      </div>
    </Card>
  )
}

export default PipelineProgress
