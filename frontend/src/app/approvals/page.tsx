'use client'

import { useQuery } from '@tanstack/react-query'
import { PageLayout } from '@/components/layout/PageLayout'
import { LangGraphApprovalList } from '@/components/approvals/LangGraphApprovalList'
import { LoadingSpinner } from '@/components/ui/LoadingSpinner'
import { api } from '@/lib/api'
import { QUERY_KEYS } from '@/lib/constants'
import { CheckCircle, Clock, XCircle, RefreshCw, Shield } from 'lucide-react'
import { Card, CardContent } from '@/components/ui/Card'

export default function ApprovalsPage() {
  const { data: approvals = [], isLoading, refetch, isFetching } = useQuery({
    queryKey: [QUERY_KEYS.APPROVALS],
    queryFn: () => api.getPendingApprovals(),
    refetchInterval: 5000, // Refetch every 5 seconds for critical approvals
  })

  const pendingCount = approvals.length

  return (
    <PageLayout
      title="HITL Approvals"
      subtitle="Human-in-the-loop approval workflows for incident remediation"
    >
      {/* Header with refresh */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br from-orange-500 to-red-600 shadow-lg">
            <Shield className="h-6 w-6 text-white" />
          </div>
          <div>
            <h2 className="text-xl font-semibold text-gray-900">Pending Approvals</h2>
            <p className="text-sm text-gray-500">
              Review and approve remediation plans before execution
            </p>
          </div>
        </div>
        <button
          onClick={() => refetch()}
          disabled={isFetching}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
        <Card variant="bordered">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Pending Review</p>
                <p className="text-3xl font-bold text-orange-600">{pendingCount}</p>
                <p className="text-xs text-gray-500 mt-1">Awaiting human approval</p>
              </div>
              <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-orange-100">
                <Clock className="h-7 w-7 text-orange-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card variant="bordered">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">Auto-Refresh</p>
                <p className="text-3xl font-bold text-blue-600">5s</p>
                <p className="text-xs text-gray-500 mt-1">Real-time updates</p>
              </div>
              <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-blue-100">
                <RefreshCw className="h-7 w-7 text-blue-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card variant="bordered">
          <CardContent className="p-6">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-600">HITL Mode</p>
                <p className="text-3xl font-bold text-green-600">Active</p>
                <p className="text-xs text-gray-500 mt-1">Human-in-the-loop enabled</p>
              </div>
              <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-green-100">
                <CheckCircle className="h-7 w-7 text-green-600" />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Info Banner */}
      <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
        <div className="flex items-start gap-3">
          <Shield className="h-5 w-5 text-blue-600 mt-0.5" />
          <div>
            <h4 className="font-medium text-blue-900">How HITL Approvals Work</h4>
            <p className="text-sm text-blue-700 mt-1">
              When a remediation plan is generated, the LLM Judge evaluates its safety and quality.
              If the plan requires human oversight (medium/high risk), it appears here for your review.
              Click <strong>Approve</strong> to execute the plan via GitHub Actions, or <strong>Reject</strong> to cancel.
            </p>
          </div>
        </div>
      </div>

      {/* Approvals List */}
      {isLoading ? (
        <div className="flex items-center justify-center h-96">
          <LoadingSpinner size="lg" text="Loading approvals..." />
        </div>
      ) : (
        <LangGraphApprovalList approvals={approvals} onUpdate={refetch} />
      )}
    </PageLayout>
  )
}
