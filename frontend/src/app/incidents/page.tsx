'use client'

import { useState, useMemo, useEffect } from 'react'
import Link from 'next/link'
import {
  Plus,
  AlertCircle,
  Search,
  Filter,
  RefreshCw,
  Clock,
  CheckCircle,
  XCircle,
  ChevronRight,
  TrendingUp,
  TrendingDown,
  Activity,
  Zap,
  Users,
  Server,
  AlertTriangle,
  Bot,
  Play,
  Pause,
  Sparkles,
  ArrowRight,
  Cpu,
  LayoutGrid,
  List,
} from 'lucide-react'
import { PageLayout } from '@/components/layout/PageLayout'
import { CreateIncidentModal } from '@/components/incidents/CreateIncidentModal'
import { getApiBaseUrl } from '@/lib/constants'

// =============================================================================
// Types
// =============================================================================

interface Incident {
  incident_id: string
  short_description: string
  description?: string
  status: string
  priority: string
  category?: string
  affected_service?: string
  affected_users?: number
  assigned_to?: string
  created_at: string
  updated_at?: string
}

// =============================================================================
// Status Badge Component
// =============================================================================

function StatusBadge({ status }: { status: string }) {
  const configs: Record<string, { label: string; className: string; icon: React.ReactNode }> = {
    '1': {
      label: 'New',
      className: 'bg-gradient-to-r from-cyan-500/20 to-blue-500/20 text-cyan-400 border-cyan-500/40 shadow-cyan-500/10 shadow-lg',
      icon: <AlertCircle className="w-3 h-3" />
    },
    'new': {
      label: 'New',
      className: 'bg-gradient-to-r from-cyan-500/20 to-blue-500/20 text-cyan-400 border-cyan-500/40 shadow-cyan-500/10 shadow-lg',
      icon: <AlertCircle className="w-3 h-3" />
    },
    '2': {
      label: 'In Progress',
      className: 'bg-gradient-to-r from-blue-500/20 to-indigo-500/20 text-blue-400 border-blue-500/40 shadow-blue-500/10 shadow-lg',
      icon: <Activity className="w-3 h-3" />
    },
    'in_progress': {
      label: 'In Progress',
      className: 'bg-gradient-to-r from-blue-500/20 to-indigo-500/20 text-blue-400 border-blue-500/40 shadow-blue-500/10 shadow-lg',
      icon: <Activity className="w-3 h-3" />
    },
    '3': {
      label: 'On Hold',
      className: 'bg-gradient-to-r from-yellow-500/20 to-amber-500/20 text-yellow-400 border-yellow-500/40 shadow-yellow-500/10 shadow-lg',
      icon: <Clock className="w-3 h-3" />
    },
    'on_hold': {
      label: 'On Hold',
      className: 'bg-gradient-to-r from-yellow-500/20 to-amber-500/20 text-yellow-400 border-yellow-500/40 shadow-yellow-500/10 shadow-lg',
      icon: <Clock className="w-3 h-3" />
    },
    '6': {
      label: 'Resolved',
      className: 'bg-gradient-to-r from-emerald-500/20 to-green-500/20 text-emerald-400 border-emerald-500/40 shadow-emerald-500/10 shadow-lg',
      icon: <CheckCircle className="w-3 h-3" />
    },
    'resolved': {
      label: 'Resolved',
      className: 'bg-gradient-to-r from-emerald-500/20 to-green-500/20 text-emerald-400 border-emerald-500/40 shadow-emerald-500/10 shadow-lg',
      icon: <CheckCircle className="w-3 h-3" />
    },
    '7': {
      label: 'Closed',
      className: 'bg-gradient-to-r from-gray-500/20 to-slate-500/20 text-gray-400 border-gray-500/40',
      icon: <XCircle className="w-3 h-3" />
    },
    'closed': {
      label: 'Closed',
      className: 'bg-gradient-to-r from-gray-500/20 to-slate-500/20 text-gray-400 border-gray-500/40',
      icon: <XCircle className="w-3 h-3" />
    },
    'pending_approval': {
      label: 'Pending Approval',
      className: 'bg-gradient-to-r from-orange-500/20 to-amber-500/20 text-orange-400 border-orange-500/40 shadow-orange-500/10 shadow-lg animate-pulse',
      icon: <Pause className="w-3 h-3" />
    },
    'awaiting_approval': {
      label: 'Awaiting Approval',
      className: 'bg-gradient-to-r from-orange-500/20 to-amber-500/20 text-orange-400 border-orange-500/40 shadow-orange-500/10 shadow-lg animate-pulse',
      icon: <Pause className="w-3 h-3" />
    },
  }

  const normalizedStatus = status?.toLowerCase() || 'unknown'
  const statusConfig = configs[normalizedStatus] || configs[status] || {
    label: status || 'Unknown',
    className: 'bg-gray-500/20 text-gray-400 border-gray-500/40',
    icon: null
  }

  return (
    <span className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold border backdrop-blur-sm ${statusConfig.className}`}>
      {statusConfig.icon}
      {statusConfig.label}
    </span>
  )
}

// =============================================================================
// Priority Badge Component
// =============================================================================

function PriorityBadge({ priority }: { priority: string }) {
  const configs: Record<string, { label: string; className: string }> = {
    '1': { label: 'P1 Critical', className: 'bg-gradient-to-r from-red-500 to-rose-500 text-white shadow-lg shadow-red-500/30' },
    '2': { label: 'P2 High', className: 'bg-gradient-to-r from-orange-500 to-amber-500 text-white shadow-lg shadow-orange-500/30' },
    '3': { label: 'P3 Medium', className: 'bg-gradient-to-r from-yellow-500 to-amber-400 text-gray-900 shadow-lg shadow-yellow-500/30' },
    '4': { label: 'P4 Low', className: 'bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/30' },
    '5': { label: 'P5 Planning', className: 'bg-gradient-to-r from-gray-500 to-gray-600 text-white' },
  }

  const priorityConfig = configs[priority] || { label: `P${priority}`, className: 'bg-gray-500 text-white' }

  return (
    <span className={`inline-flex items-center px-2.5 py-1 rounded-lg text-[10px] font-bold tracking-wide ${priorityConfig.className}`}>
      {priorityConfig.label}
    </span>
  )
}

// =============================================================================
// Stats Card Component
// =============================================================================

type ColorVariant = 'gray' | 'blue' | 'red' | 'yellow' | 'green' | 'purple'

interface StatsCardProps {
  label: string
  value: number | string
  icon: React.ReactNode
  trend?: { value: number; direction: 'up' | 'down' }
  color?: ColorVariant
  highlight?: boolean
}

function StatsCard({ label, value, icon, trend, color = 'gray', highlight = false }: StatsCardProps) {
  const colorClasses: Record<ColorVariant, {
    border: string;
    gradient: string;
    iconBg: string;
    glow: string;
  }> = {
    gray: {
      border: 'border-white/10',
      gradient: 'from-gray-500/10 to-transparent',
      iconBg: 'bg-gray-500/20',
      glow: '',
    },
    blue: {
      border: 'border-blue-500/30',
      gradient: 'from-blue-500/10 to-transparent',
      iconBg: 'bg-gradient-to-br from-blue-500 to-cyan-600',
      glow: 'shadow-blue-500/20',
    },
    red: {
      border: 'border-red-500/30',
      gradient: 'from-red-500/10 to-transparent',
      iconBg: 'bg-gradient-to-br from-red-500 to-rose-600',
      glow: 'shadow-red-500/20',
    },
    yellow: {
      border: 'border-amber-500/30',
      gradient: 'from-amber-500/10 to-transparent',
      iconBg: 'bg-gradient-to-br from-amber-500 to-orange-600',
      glow: 'shadow-amber-500/20',
    },
    green: {
      border: 'border-emerald-500/30',
      gradient: 'from-emerald-500/10 to-transparent',
      iconBg: 'bg-gradient-to-br from-emerald-500 to-green-600',
      glow: 'shadow-emerald-500/20',
    },
    purple: {
      border: 'border-purple-500/30',
      gradient: 'from-purple-500/10 to-transparent',
      iconBg: 'bg-gradient-to-br from-purple-500 to-violet-600',
      glow: 'shadow-purple-500/20',
    },
  }

  const classes = colorClasses[color]

  return (
    <div className={`
      relative overflow-hidden rounded-2xl border backdrop-blur-xl
      bg-gradient-to-br ${classes.gradient} bg-[#0d1117]/80
      ${highlight ? classes.border : 'border-white/10'}
      ${highlight ? `shadow-lg ${classes.glow}` : ''}
      transition-all duration-300 hover:scale-[1.02]
    `}>
      {/* Decorative gradient */}
      {highlight && (
        <div className={`absolute -top-10 -right-10 w-24 h-24 ${classes.iconBg} rounded-full blur-2xl opacity-30`} />
      )}

      <div className="relative p-5">
        <div className="flex items-center justify-between mb-3">
          <div className={`p-2.5 rounded-xl ${highlight ? classes.iconBg : 'bg-white/5'} ${highlight ? 'shadow-lg' : ''}`}>
            {icon}
          </div>
          {trend && (
            <div className={`flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-semibold ${
              trend.direction === 'up'
                ? 'bg-red-500/15 text-red-400'
                : 'bg-emerald-500/15 text-emerald-400'
            }`}>
              {trend.direction === 'up' ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
              {trend.value}%
            </div>
          )}
        </div>
        <div className="text-3xl font-bold text-white mb-1">{value}</div>
        <div className="text-sm text-gray-400 font-medium">{label}</div>
      </div>
    </div>
  )
}

// =============================================================================
// Filter Button Component
// =============================================================================

interface FilterButtonProps {
  label: string
  options: { value: string; label: string }[]
  value: string
  onChange: (value: string) => void
}

function FilterButton({ label, options, value, onChange }: FilterButtonProps) {
  return (
    <div className="relative group">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={`
          appearance-none cursor-pointer
          bg-white/5 hover:bg-white/10
          border border-white/10 hover:border-white/20
          text-gray-300 text-sm font-medium
          rounded-xl px-4 py-2.5 pr-10
          focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500/50
          transition-all duration-200
          ${value ? 'bg-blue-500/10 border-blue-500/30 text-blue-400' : ''}
        `}
      >
        <option value="" className="bg-[#0d1117]">{label}</option>
        {options.map(opt => (
          <option key={opt.value} value={opt.value} className="bg-[#0d1117]">{opt.label}</option>
        ))}
      </select>
      <Filter className={`absolute right-3 top-1/2 -translate-y-1/2 w-4 h-4 pointer-events-none transition-colors ${value ? 'text-blue-400' : 'text-gray-500'}`} />
    </div>
  )
}

// =============================================================================
// Incident Row Component
// =============================================================================

function IncidentRow({ incident }: { incident: Incident }) {
  const priorityColors: Record<string, string> = {
    '1': 'from-red-500 to-rose-500',
    '2': 'from-orange-500 to-amber-500',
    '3': 'from-yellow-500 to-amber-400',
    '4': 'from-blue-500 to-cyan-500',
    '5': 'from-gray-500 to-gray-600',
  }

  const formatRelativeTime = (dateString: string) => {
    try {
      const date = new Date(dateString)
      const now = new Date()
      const diffMs = now.getTime() - date.getTime()
      const diffMins = Math.floor(diffMs / 60000)
      const diffHours = Math.floor(diffMins / 60)
      const diffDays = Math.floor(diffHours / 24)

      if (diffMins < 60) return `${diffMins}m ago`
      if (diffHours < 24) return `${diffHours}h ago`
      return `${diffDays}d ago`
    } catch {
      return 'Unknown'
    }
  }

  const isPendingApproval = ['pending_approval', 'awaiting_approval'].includes(incident.status?.toLowerCase())

  return (
    <Link
      href={`/incidents/${incident.incident_id}`}
      className="block group"
    >
      <div className={`
        relative overflow-hidden rounded-2xl border backdrop-blur-xl
        bg-[#0d1117]/80
        ${isPendingApproval ? 'border-orange-500/30 hover:border-orange-500/50' : 'border-white/10 hover:border-white/20'}
        transition-all duration-300
        hover:shadow-lg hover:-translate-y-0.5
        ${isPendingApproval ? 'hover:shadow-orange-500/10' : ''}
      `}>
        {/* Priority indicator bar */}
        <div className={`absolute left-0 top-0 bottom-0 w-1 bg-gradient-to-b ${priorityColors[incident.priority] || 'from-gray-500 to-gray-600'}`} />

        {/* Pending approval glow effect */}
        {isPendingApproval && (
          <div className="absolute inset-0 bg-gradient-to-r from-orange-500/5 to-transparent pointer-events-none" />
        )}

        <div className="relative flex items-center gap-5 p-5 pl-6">
          {/* Main Content */}
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-3 mb-2">
              <span className="text-sm font-bold text-blue-400 group-hover:text-blue-300 transition-colors">
                {incident.incident_id}
              </span>
              <PriorityBadge priority={incident.priority} />
              {incident.category && (
                <span className="text-[10px] text-gray-500 px-2 py-1 bg-white/5 rounded-lg font-medium">
                  {incident.category}
                </span>
              )}
            </div>

            <p className="text-white font-medium truncate group-hover:text-gray-100 transition-colors mb-2">
              {incident.short_description || 'No description'}
            </p>

            <div className="flex items-center gap-4 text-xs text-gray-500">
              {incident.affected_service && (
                <span className="flex items-center gap-1.5 px-2 py-1 bg-white/5 rounded-lg">
                  <Server className="w-3 h-3 text-gray-400" />
                  {incident.affected_service}
                </span>
              )}
              {incident.affected_users && incident.affected_users > 0 && (
                <span className="flex items-center gap-1.5 px-2 py-1 bg-white/5 rounded-lg">
                  <Users className="w-3 h-3 text-gray-400" />
                  {incident.affected_users} users
                </span>
              )}
              <span className="flex items-center gap-1.5">
                <Clock className="w-3 h-3" />
                {formatRelativeTime(incident.created_at)}
              </span>
            </div>
          </div>

          {/* Status & Action */}
          <div className="flex items-center gap-4">
            <StatusBadge status={incident.status} />
            <div className="p-2 rounded-lg bg-white/5 group-hover:bg-white/10 transition-colors">
              <ChevronRight className="w-5 h-5 text-gray-500 group-hover:text-white group-hover:translate-x-0.5 transition-all duration-200" />
            </div>
          </div>
        </div>
      </div>
    </Link>
  )
}

// =============================================================================
// Loading Component
// =============================================================================

function LoadingState() {
  return (
    <div className="flex flex-col items-center justify-center py-20">
      <div className="relative mb-6">
        <div className="w-16 h-16 rounded-full border-4 border-purple-500/20" />
        <div className="w-16 h-16 rounded-full border-4 border-purple-500 border-t-transparent animate-spin absolute top-0 left-0" />
        <div className="absolute inset-0 flex items-center justify-center">
          <Cpu className="w-6 h-6 text-purple-400" />
        </div>
      </div>
      <p className="text-lg font-semibold text-white">Loading incidents</p>
      <p className="text-sm text-gray-500">Fetching data from ServiceNow...</p>
    </div>
  )
}

// =============================================================================
// Empty State Component
// =============================================================================

function EmptyState({ hasFilters, onClearFilters }: { hasFilters: boolean; onClearFilters: () => void }) {
  return (
    <div className="text-center py-20 rounded-3xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl">
      <div className="w-20 h-20 mx-auto mb-6 rounded-full bg-gradient-to-br from-emerald-500/20 to-green-500/20 flex items-center justify-center">
        <CheckCircle className="w-10 h-10 text-emerald-500" />
      </div>
      <h3 className="text-2xl font-bold text-white mb-2">
        {hasFilters ? 'No matching incidents' : 'No active incidents'}
      </h3>
      <p className="text-gray-400 mb-8 max-w-md mx-auto">
        {hasFilters
          ? 'Try adjusting your filters to find what you\'re looking for.'
          : 'All systems are operating normally. No incidents require attention.'}
      </p>
      {hasFilters && (
        <button
          onClick={onClearFilters}
          className="inline-flex items-center gap-2 px-6 py-3 text-sm font-semibold text-white bg-gradient-to-r from-blue-500 to-cyan-500 hover:from-blue-400 hover:to-cyan-400 rounded-xl shadow-lg shadow-blue-500/25 transition-all duration-200"
        >
          <XCircle className="w-4 h-4" />
          Clear Filters
        </button>
      )}
    </div>
  )
}

// =============================================================================
// Main Component
// =============================================================================

export default function IncidentsPage() {
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false)
  const [searchQuery, setSearchQuery] = useState('')
  const [priorityFilter, setPriorityFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [incidents, setIncidents] = useState<Incident[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [dataSource, setDataSource] = useState<string>('')

  // Fetch incidents
  const fetchIncidents = async () => {
    setIsLoading(true)
    setError(null)

    try {
      const apiUrl = getApiBaseUrl()
      const response = await fetch(`${apiUrl}/api/incidents`)

      if (!response.ok) {
        throw new Error(`API error: ${response.status}`)
      }

      const data = await response.json()
      setIncidents(data.incidents || [])
      setDataSource(data.source || 'unknown')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch incidents')
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchIncidents()
    const interval = setInterval(fetchIncidents, 30000)
    return () => clearInterval(interval)
  }, [])

  // Calculate stats
  const stats = useMemo(() => {
    const resolvedStatuses = ['6', '7', 'resolved', 'closed']
    const pendingStatuses = ['pending_approval', 'awaiting_approval']

    const total = incidents.length
    const active = incidents.filter((i) => !resolvedStatuses.includes(i.status?.toLowerCase())).length
    const critical = incidents.filter((i) => i.priority === '1' && !resolvedStatuses.includes(i.status?.toLowerCase())).length
    const pending = incidents.filter((i) => pendingStatuses.includes(i.status?.toLowerCase())).length
    const resolved = incidents.filter((i) => resolvedStatuses.includes(i.status?.toLowerCase())).length

    return { total, active, critical, pending, resolved }
  }, [incidents])

  // Filter incidents
  const filteredIncidents = useMemo(() => {
    return incidents.filter((incident) => {
      if (searchQuery) {
        const query = searchQuery.toLowerCase()
        const matchesSearch =
          incident.incident_id.toLowerCase().includes(query) ||
          incident.short_description?.toLowerCase().includes(query) ||
          incident.category?.toLowerCase().includes(query) ||
          incident.affected_service?.toLowerCase().includes(query)
        if (!matchesSearch) return false
      }

      if (priorityFilter && incident.priority !== priorityFilter) {
        return false
      }

      if (statusFilter && incident.status !== statusFilter) {
        return false
      }

      return true
    })
  }, [incidents, searchQuery, priorityFilter, statusFilter])

  const clearFilters = () => {
    setSearchQuery('')
    setPriorityFilter('')
    setStatusFilter('')
  }

  const hasActiveFilters = searchQuery || priorityFilter || statusFilter

  return (
    <PageLayout>
      <div className="min-h-screen p-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-8">
          <div className="flex items-center gap-5">
            <div className="p-4 rounded-2xl bg-gradient-to-br from-red-500 to-rose-600 shadow-xl shadow-red-500/25">
              <AlertCircle className="w-7 h-7 text-white" />
            </div>
            <div>
              <h1 className="text-3xl font-bold text-white tracking-tight">
                IT Service Agent
              </h1>
              <div className="flex items-center gap-3 mt-1">
                <span className="text-gray-500">Automated incident resolution via ServiceNow</span>
                {dataSource && (
                  <span className={`text-xs px-2.5 py-1 rounded-full font-medium ${
                    dataSource === 'mock'
                      ? 'bg-amber-500/15 text-amber-400 border border-amber-500/30'
                      : 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30'
                  }`}>
                    {dataSource === 'mock' ? 'Demo Mode' : 'Live'}
                  </span>
                )}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <button
              onClick={fetchIncidents}
              disabled={isLoading}
              className="flex items-center gap-2 px-4 py-2.5 text-sm text-gray-400 hover:text-white bg-white/5 hover:bg-white/10 rounded-xl border border-white/10 hover:border-white/20 transition-all disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            <button
              onClick={() => setIsCreateModalOpen(true)}
              className="flex items-center gap-2 px-5 py-2.5 text-sm font-semibold text-white bg-gradient-to-r from-red-500 to-rose-500 hover:from-red-400 hover:to-rose-400 rounded-xl shadow-lg shadow-red-500/25 transition-all duration-200"
            >
              <Plus className="w-4 h-4" />
              Create Incident
            </button>
          </div>
        </div>

        {/* Error Banner */}
        {error && (
          <div className="bg-red-500/10 border border-red-500/30 rounded-2xl p-4 mb-8 flex items-center gap-4">
            <div className="p-2 rounded-lg bg-red-500/20">
              <AlertTriangle className="w-5 h-5 text-red-400" />
            </div>
            <span className="text-red-400 flex-1">{error}</span>
            <button
              onClick={fetchIncidents}
              className="flex items-center gap-2 px-4 py-2 text-sm text-red-400 hover:text-red-300 bg-red-500/10 hover:bg-red-500/20 rounded-lg transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
              Retry
            </button>
          </div>
        )}

        {/* Stats Grid */}
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-8">
          <StatsCard
            label="Total Incidents"
            value={stats.total}
            icon={<AlertCircle className="w-5 h-5 text-gray-400" />}
            color="gray"
          />
          <StatsCard
            label="Active"
            value={stats.active}
            icon={<Activity className="w-5 h-5 text-white" />}
            trend={stats.active > 10 ? { value: 12, direction: 'up' } : undefined}
            color="blue"
            highlight={stats.active > 0}
          />
          <StatsCard
            label="Critical (P1)"
            value={stats.critical}
            icon={<Zap className="w-5 h-5 text-white" />}
            highlight={stats.critical > 0}
            color="red"
          />
          <StatsCard
            label="Pending Approval"
            value={stats.pending}
            icon={<Clock className="w-5 h-5 text-white" />}
            highlight={stats.pending > 0}
            color="yellow"
          />
          <StatsCard
            label="Resolved"
            value={stats.resolved}
            icon={<CheckCircle className="w-5 h-5 text-white" />}
            trend={{ value: 8, direction: 'down' }}
            color="green"
            highlight={stats.resolved > 0}
          />
        </div>

        {/* Filters */}
        <div className="rounded-2xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl p-5 mb-6">
          <div className="flex flex-wrap items-center gap-4">
            {/* Search */}
            <div className="relative flex-1 min-w-80">
              <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-500" />
              <input
                type="text"
                placeholder="Search by ID, description, category, service..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full bg-white/5 border border-white/10 hover:border-white/20 focus:border-blue-500/50 text-white text-sm rounded-xl pl-12 pr-4 py-3 focus:outline-none focus:ring-2 focus:ring-blue-500/30 transition-all placeholder-gray-500"
              />
            </div>

            {/* Filter Buttons */}
            <FilterButton
              label="All Priorities"
              options={[
                { value: '1', label: 'P1 Critical' },
                { value: '2', label: 'P2 High' },
                { value: '3', label: 'P3 Medium' },
                { value: '4', label: 'P4 Low' },
              ]}
              value={priorityFilter}
              onChange={setPriorityFilter}
            />

            <FilterButton
              label="All Statuses"
              options={[
                { value: '1', label: 'New' },
                { value: '2', label: 'In Progress' },
                { value: '3', label: 'On Hold' },
                { value: 'pending_approval', label: 'Pending Approval' },
                { value: '6', label: 'Resolved' },
                { value: '7', label: 'Closed' },
              ]}
              value={statusFilter}
              onChange={setStatusFilter}
            />

            {hasActiveFilters && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-2 px-4 py-2.5 text-sm text-gray-400 hover:text-white hover:bg-white/10 rounded-xl border border-white/10 hover:border-white/20 transition-all"
              >
                <XCircle className="w-4 h-4" />
                Clear filters
              </button>
            )}
          </div>
        </div>

        {/* Results Info */}
        <div className="flex items-center justify-between mb-5">
          <p className="text-sm text-gray-400">
            Showing <span className="text-white font-semibold">{filteredIncidents.length}</span> of{' '}
            <span className="text-white font-semibold">{incidents.length}</span> incidents
          </p>
          {hasActiveFilters && (
            <p className="text-sm text-blue-400 flex items-center gap-2 px-3 py-1 bg-blue-500/10 rounded-lg">
              <Filter className="w-3 h-3" />
              Filters applied
            </p>
          )}
        </div>

        {/* Incidents List */}
        {isLoading ? (
          <LoadingState />
        ) : filteredIncidents.length === 0 ? (
          <EmptyState hasFilters={!!hasActiveFilters} onClearFilters={clearFilters} />
        ) : (
          <div className="space-y-3">
            {filteredIncidents.map((incident) => (
              <IncidentRow key={incident.incident_id} incident={incident} />
            ))}
          </div>
        )}
      </div>

      {/* Create Modal */}
      <CreateIncidentModal
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onSuccess={() => {
          fetchIncidents()
          setIsCreateModalOpen(false)
        }}
      />
    </PageLayout>
  )
}
