'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { getApiBaseUrl } from '@/lib/constants'
import {
  AlertCircle,
  CheckCircle,
  XCircle,
  ArrowRight,
  RefreshCw,
  TrendingUp,
  TrendingDown,
  Activity,
  Zap,
  Clock,
  Bot,
  Ticket,
  Sparkles,
  Shield,
  GitBranch,
  Play,
  Pause,
  Workflow,
  ArrowUpRight,
  Server,
  Database,
  Cloud,
  AlertTriangle,
  CircleDot,
  Cpu,
  Globe,
  BarChart3,
  Layers,
  Terminal,
  Radio,
} from 'lucide-react'

// =============================================================================
// Types
// =============================================================================

interface Incident {
  incident_id: string
  short_description: string
  status: string
  priority: string
  category?: string
  affected_service?: string
  created_at?: string
  workflow_status?: string
}

interface WorkflowExecution {
  id: string
  incident_id: string
  current_node: string
  status: 'running' | 'paused' | 'completed' | 'failed'
  started_at: string
}

// =============================================================================
// Status & Priority Components
// =============================================================================

function StatusBadge({ status, type = 'incident' }: { status: string; type?: 'incident' | 'workflow' }) {
  if (type === 'workflow') {
    const configs: Record<string, { label: string; className: string; icon: React.ReactNode }> = {
      running: {
        label: 'Running',
        className: 'bg-gradient-to-r from-emerald-500/20 to-green-500/20 text-emerald-400 border-emerald-500/40 shadow-emerald-500/10 shadow-lg',
        icon: <Play className="w-3 h-3" />
      },
      paused: {
        label: 'Awaiting Approval',
        className: 'bg-gradient-to-r from-amber-500/20 to-orange-500/20 text-amber-400 border-amber-500/40 shadow-amber-500/10 shadow-lg',
        icon: <Pause className="w-3 h-3" />
      },
      completed: {
        label: 'Completed',
        className: 'bg-gradient-to-r from-green-500/20 to-emerald-500/20 text-green-400 border-green-500/40',
        icon: <CheckCircle className="w-3 h-3" />
      },
      failed: {
        label: 'Failed',
        className: 'bg-gradient-to-r from-red-500/20 to-rose-500/20 text-red-400 border-red-500/40',
        icon: <XCircle className="w-3 h-3" />
      },
    }
    const config = configs[status] || configs.running
    return (
      <span className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold border backdrop-blur-sm ${config.className}`}>
        {config.icon}
        {config.label}
      </span>
    )
  }

  const configs: Record<string, { label: string; className: string }> = {
    '1': { label: 'New', className: 'bg-cyan-500/15 text-cyan-400 border-cyan-500/30' },
    '2': { label: 'In Progress', className: 'bg-blue-500/15 text-blue-400 border-blue-500/30' },
    '3': { label: 'On Hold', className: 'bg-yellow-500/15 text-yellow-400 border-yellow-500/30' },
    '6': { label: 'Resolved', className: 'bg-green-500/15 text-green-400 border-green-500/30' },
    '7': { label: 'Closed', className: 'bg-gray-500/15 text-gray-400 border-gray-500/30' },
    'pending_approval': { label: 'Pending Approval', className: 'bg-orange-500/15 text-orange-400 border-orange-500/30' },
  }

  const config = configs[status] || { label: 'Unknown', className: 'bg-gray-500/15 text-gray-400 border-gray-500/30' }
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold border ${config.className}`}>
      {config.label}
    </span>
  )
}

function PriorityBadge({ priority }: { priority: string }) {
  const configs: Record<string, { label: string; className: string }> = {
    '1': { label: 'P1', className: 'bg-gradient-to-r from-red-500 to-rose-500 text-white shadow-lg shadow-red-500/30' },
    '2': { label: 'P2', className: 'bg-gradient-to-r from-orange-500 to-amber-500 text-white shadow-lg shadow-orange-500/30' },
    '3': { label: 'P3', className: 'bg-gradient-to-r from-yellow-500 to-amber-400 text-gray-900 shadow-lg shadow-yellow-500/30' },
    '4': { label: 'P4', className: 'bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/30' },
    '5': { label: 'P5', className: 'bg-gradient-to-r from-gray-500 to-gray-600 text-white' },
  }
  const config = configs[priority] || { label: `P${priority}`, className: 'bg-gray-500 text-white' }
  return (
    <span className={`inline-flex items-center justify-center px-2 py-0.5 rounded-md text-[10px] font-bold ${config.className}`}>
      {config.label}
    </span>
  )
}

// =============================================================================
// Modern Metric Card
// =============================================================================

type ColorVariant = 'red' | 'orange' | 'yellow' | 'blue' | 'green' | 'purple' | 'cyan'

interface MetricCardProps {
  label: string
  value: number | string
  icon: React.ReactNode
  trend?: { value: number; direction: 'up' | 'down' }
  color: ColorVariant
  href?: string
  subtitle?: string
}

function MetricCard({ label, value, icon, trend, color, href, subtitle }: MetricCardProps) {
  const colorClasses: Record<ColorVariant, {
    gradient: string;
    border: string;
    glow: string;
    iconBg: string;
    text: string;
  }> = {
    red: {
      gradient: 'from-red-500/10 via-transparent to-transparent',
      border: 'border-red-500/20 hover:border-red-500/40',
      glow: 'hover:shadow-red-500/10',
      iconBg: 'bg-gradient-to-br from-red-500 to-rose-600',
      text: 'text-red-400',
    },
    orange: {
      gradient: 'from-orange-500/10 via-transparent to-transparent',
      border: 'border-orange-500/20 hover:border-orange-500/40',
      glow: 'hover:shadow-orange-500/10',
      iconBg: 'bg-gradient-to-br from-orange-500 to-amber-600',
      text: 'text-orange-400',
    },
    yellow: {
      gradient: 'from-yellow-500/10 via-transparent to-transparent',
      border: 'border-yellow-500/20 hover:border-yellow-500/40',
      glow: 'hover:shadow-yellow-500/10',
      iconBg: 'bg-gradient-to-br from-yellow-500 to-amber-500',
      text: 'text-yellow-400',
    },
    blue: {
      gradient: 'from-blue-500/10 via-transparent to-transparent',
      border: 'border-blue-500/20 hover:border-blue-500/40',
      glow: 'hover:shadow-blue-500/10',
      iconBg: 'bg-gradient-to-br from-blue-500 to-cyan-600',
      text: 'text-blue-400',
    },
    green: {
      gradient: 'from-emerald-500/10 via-transparent to-transparent',
      border: 'border-emerald-500/20 hover:border-emerald-500/40',
      glow: 'hover:shadow-emerald-500/10',
      iconBg: 'bg-gradient-to-br from-emerald-500 to-green-600',
      text: 'text-emerald-400',
    },
    purple: {
      gradient: 'from-purple-500/10 via-transparent to-transparent',
      border: 'border-purple-500/20 hover:border-purple-500/40',
      glow: 'hover:shadow-purple-500/10',
      iconBg: 'bg-gradient-to-br from-purple-500 to-violet-600',
      text: 'text-purple-400',
    },
    cyan: {
      gradient: 'from-cyan-500/10 via-transparent to-transparent',
      border: 'border-cyan-500/20 hover:border-cyan-500/40',
      glow: 'hover:shadow-cyan-500/10',
      iconBg: 'bg-gradient-to-br from-cyan-500 to-blue-600',
      text: 'text-cyan-400',
    },
  }

  const classes = colorClasses[color]

  const Card = (
    <div className={`
      relative overflow-hidden rounded-2xl border backdrop-blur-xl
      bg-gradient-to-br ${classes.gradient} bg-[#0d1117]/80
      ${classes.border} ${classes.glow}
      transition-all duration-500 group cursor-pointer
      hover:shadow-2xl hover:-translate-y-1
    `}>
      {/* Decorative gradient orb */}
      <div className={`absolute -top-12 -right-12 w-32 h-32 ${classes.iconBg} rounded-full blur-3xl opacity-20 group-hover:opacity-30 transition-opacity`} />

      <div className="relative p-6">
        <div className="flex items-start justify-between mb-4">
          <div className={`p-3 rounded-xl ${classes.iconBg} shadow-lg group-hover:scale-110 transition-transform duration-300`}>
            {icon}
          </div>
          {trend && (
            <div className={`flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-semibold ${
              trend.direction === 'up'
                ? 'bg-emerald-500/15 text-emerald-400'
                : 'bg-red-500/15 text-red-400'
            }`}>
              {trend.direction === 'up' ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
              {trend.value}%
            </div>
          )}
        </div>

        <div className="space-y-1">
          <div className="text-4xl font-bold text-white tracking-tight">{value}</div>
          <div className="text-sm font-medium text-gray-400">{label}</div>
          {subtitle && <div className="text-xs text-gray-600">{subtitle}</div>}
        </div>

        {href && (
          <div className={`mt-4 flex items-center gap-1 text-xs font-medium ${classes.text} opacity-0 group-hover:opacity-100 transition-all duration-300`}>
            View details <ArrowRight className="w-3 h-3 group-hover:translate-x-1 transition-transform" />
          </div>
        )}
      </div>
    </div>
  )

  if (href) {
    return <Link href={href}>{Card}</Link>
  }
  return Card
}

// =============================================================================
// Agent Status Card
// =============================================================================

interface AgentStatusProps {
  name: string
  description: string
  status: 'active' | 'idle' | 'error'
  stats: { label: string; value: string | number }[]
  icon: React.ReactNode
  color: 'red' | 'blue' | 'purple' | 'green'
  href: string
}

function AgentStatusCard({ name, description, status, stats, icon, color, href }: AgentStatusProps) {
  const statusConfig = {
    active: { label: 'Active', dotClass: 'bg-emerald-500', pulse: true, textClass: 'text-emerald-400' },
    idle: { label: 'Idle', dotClass: 'bg-amber-500', pulse: false, textClass: 'text-amber-400' },
    error: { label: 'Error', dotClass: 'bg-red-500', pulse: true, textClass: 'text-red-400' },
  }

  const colorConfig = {
    red: {
      gradient: 'from-red-500 via-rose-500 to-pink-500',
      bgGradient: 'from-red-500/5 to-transparent',
      iconBg: 'bg-gradient-to-br from-red-500 to-rose-600',
      border: 'border-red-500/20 hover:border-red-500/40',
      statBg: 'bg-red-500/5 border-red-500/10',
      buttonBg: 'bg-gradient-to-r from-red-500 to-rose-500 hover:from-red-400 hover:to-rose-400',
    },
    blue: {
      gradient: 'from-blue-500 via-cyan-500 to-teal-500',
      bgGradient: 'from-blue-500/5 to-transparent',
      iconBg: 'bg-gradient-to-br from-blue-500 to-cyan-600',
      border: 'border-blue-500/20 hover:border-blue-500/40',
      statBg: 'bg-blue-500/5 border-blue-500/10',
      buttonBg: 'bg-gradient-to-r from-blue-500 to-cyan-500 hover:from-blue-400 hover:to-cyan-400',
    },
    purple: {
      gradient: 'from-purple-500 via-violet-500 to-indigo-500',
      bgGradient: 'from-purple-500/5 to-transparent',
      iconBg: 'bg-gradient-to-br from-purple-500 to-violet-600',
      border: 'border-purple-500/20 hover:border-purple-500/40',
      statBg: 'bg-purple-500/5 border-purple-500/10',
      buttonBg: 'bg-gradient-to-r from-purple-500 to-violet-500 hover:from-purple-400 hover:to-violet-400',
    },
    green: {
      gradient: 'from-emerald-500 via-green-500 to-teal-500',
      bgGradient: 'from-emerald-500/5 to-transparent',
      iconBg: 'bg-gradient-to-br from-emerald-500 to-green-600',
      border: 'border-emerald-500/20 hover:border-emerald-500/40',
      statBg: 'bg-emerald-500/5 border-emerald-500/10',
      buttonBg: 'bg-gradient-to-r from-emerald-500 to-green-500 hover:from-emerald-400 hover:to-green-400',
    },
  }

  const config = statusConfig[status]
  const colors = colorConfig[color]

  return (
    <Link href={href} className="block group">
      <div className={`
        relative overflow-hidden rounded-3xl border backdrop-blur-xl
        bg-gradient-to-br ${colors.bgGradient} bg-[#0d1117]/80
        ${colors.border}
        transition-all duration-500
        hover:shadow-2xl hover:-translate-y-1
      `}>
        {/* Top gradient bar */}
        <div className={`h-1 bg-gradient-to-r ${colors.gradient}`} />

        {/* Decorative background elements */}
        <div className={`absolute top-0 right-0 w-64 h-64 bg-gradient-to-br ${colors.gradient} rounded-full blur-3xl opacity-5 group-hover:opacity-10 transition-opacity`} />

        <div className="relative p-6">
          {/* Header */}
          <div className="flex items-start justify-between mb-6">
            <div className="flex items-center gap-4">
              <div className={`p-4 rounded-2xl ${colors.iconBg} shadow-xl group-hover:scale-110 transition-transform duration-300`}>
                {icon}
              </div>
              <div>
                <h3 className="text-xl font-bold text-white group-hover:text-white transition-colors">
                  {name}
                </h3>
                <p className="text-sm text-gray-500">{description}</p>
              </div>
            </div>
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-white/5 border border-white/10">
              <div className="relative">
                <div className={`w-2 h-2 rounded-full ${config.dotClass}`} />
                {config.pulse && (
                  <div className={`absolute inset-0 w-2 h-2 rounded-full ${config.dotClass} animate-ping opacity-75`} />
                )}
              </div>
              <span className={`text-xs font-medium ${config.textClass}`}>{config.label}</span>
            </div>
          </div>

          {/* Stats Grid */}
          <div className="grid grid-cols-2 gap-3 mb-6">
            {stats.map((stat, idx) => (
              <div key={idx} className={`rounded-xl p-4 border ${colors.statBg} backdrop-blur-sm`}>
                <div className="text-2xl font-bold text-white">{stat.value}</div>
                <div className="text-xs text-gray-500 font-medium">{stat.label}</div>
              </div>
            ))}
          </div>

          {/* Action Button */}
          <button className={`
            w-full flex items-center justify-center gap-2 py-3 rounded-xl
            ${colors.buttonBg}
            text-white font-semibold text-sm
            shadow-lg transition-all duration-300
            group-hover:shadow-xl
          `}>
            Open Agent <ArrowUpRight className="w-4 h-4 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />
          </button>
        </div>
      </div>
    </Link>
  )
}

// =============================================================================
// Live Workflow Card
// =============================================================================

function LiveWorkflowCard({ execution }: { execution: WorkflowExecution }) {
  const nodeLabels: Record<string, { label: string; step: number }> = {
    'ingest': { label: 'Ingesting Data', step: 1 },
    'parse': { label: 'Parsing Content', step: 2 },
    'classify': { label: 'Classifying Issue', step: 3 },
    'swarm_rag': { label: 'Script Matching', step: 4 },
    'generate_plan': { label: 'Generating Plan', step: 5 },
    'judge': { label: 'LLM Evaluation', step: 6 },
    'control_plane': { label: 'Policy Check', step: 7 },
    'await_approval': { label: 'Awaiting Approval', step: 8 },
    'execute': { label: 'Executing Remediation', step: 9 },
    'verify': { label: 'Verifying Results', step: 10 },
    'close_ticket': { label: 'Closing Ticket', step: 11 },
    'feedback_loop': { label: 'Recording Feedback', step: 12 },
  }

  const nodeInfo = nodeLabels[execution.current_node] || { label: execution.current_node, step: 0 }
  const progress = Math.round((nodeInfo.step / 12) * 100)

  return (
    <Link
      href={`/incidents/${execution.incident_id}`}
      className="block group"
    >
      <div className="relative overflow-hidden rounded-2xl border border-blue-500/20 hover:border-blue-500/40 bg-gradient-to-br from-blue-500/5 to-transparent bg-[#0d1117]/80 backdrop-blur-xl transition-all duration-300 hover:shadow-lg hover:shadow-blue-500/10">
        {/* Progress bar */}
        <div className="h-0.5 bg-gray-800">
          <div
            className="h-full bg-gradient-to-r from-blue-500 to-cyan-500 transition-all duration-500"
            style={{ width: `${progress}%` }}
          />
        </div>

        <div className="p-5">
          <div className="flex items-center gap-4">
            <div className="relative">
              <div className={`p-3 rounded-xl ${
                execution.status === 'running'
                  ? 'bg-gradient-to-br from-blue-500 to-cyan-600'
                  : 'bg-gradient-to-br from-amber-500 to-orange-600'
              } shadow-lg`}>
                <Workflow className="w-5 h-5 text-white" />
              </div>
              {execution.status === 'running' && (
                <div className="absolute -top-1 -right-1">
                  <span className="relative flex h-3 w-3">
                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                    <span className="relative inline-flex rounded-full h-3 w-3 bg-emerald-500 border-2 border-[#0d1117]" />
                  </span>
                </div>
              )}
            </div>

            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-3 mb-1">
                <span className="text-sm font-bold text-white">{execution.incident_id}</span>
                <StatusBadge status={execution.status} type="workflow" />
              </div>
              <div className="flex items-center gap-3 text-xs text-gray-500">
                <span className="flex items-center gap-1">
                  <Terminal className="w-3 h-3" />
                  Step {nodeInfo.step}/12: <span className="text-gray-300 font-medium">{nodeInfo.label}</span>
                </span>
                <span className="text-gray-700">|</span>
                <span className="flex items-center gap-1">
                  <Clock className="w-3 h-3" />
                  {new Date(execution.started_at).toLocaleTimeString()}
                </span>
              </div>
            </div>

            <ArrowRight className="w-5 h-5 text-gray-600 group-hover:text-blue-400 group-hover:translate-x-1 transition-all duration-300" />
          </div>
        </div>
      </div>
    </Link>
  )
}

// =============================================================================
// Recent Activity Item
// =============================================================================

function ActivityItem({ incident }: { incident: Incident }) {
  const priorityColors: Record<string, string> = {
    '1': 'from-red-500 to-rose-500',
    '2': 'from-orange-500 to-amber-500',
    '3': 'from-yellow-500 to-amber-400',
    '4': 'from-blue-500 to-cyan-500',
    '5': 'from-gray-500 to-gray-600',
  }

  return (
    <Link
      href={`/incidents/${incident.incident_id}`}
      className="flex items-center gap-4 p-4 hover:bg-white/5 rounded-xl transition-all duration-200 group"
    >
      <div className={`w-1 h-12 rounded-full bg-gradient-to-b ${priorityColors[incident.priority] || 'from-gray-500 to-gray-600'}`} />

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1.5">
          <span className="text-sm font-bold text-blue-400 group-hover:text-blue-300 transition-colors">{incident.incident_id}</span>
          <PriorityBadge priority={incident.priority} />
          {incident.category && (
            <span className="text-[10px] text-gray-500 px-2 py-0.5 bg-white/5 rounded-md font-medium">
              {incident.category}
            </span>
          )}
        </div>
        <p className="text-sm text-gray-400 truncate group-hover:text-gray-300 transition-colors">
          {incident.short_description || 'No description'}
        </p>
        {incident.affected_service && (
          <div className="flex items-center gap-1.5 mt-1.5 text-xs text-gray-600">
            <Server className="w-3 h-3" />
            <span>{incident.affected_service}</span>
          </div>
        )}
      </div>

      <StatusBadge status={incident.status} type="incident" />
    </Link>
  )
}

// =============================================================================
// System Health Component
// =============================================================================

function SystemHealthPanel() {
  const services = [
    { name: 'LangGraph Engine', status: 'online', latency: '45ms', icon: <Workflow className="w-4 h-4" />, color: 'text-blue-400' },
    { name: 'Kafka Streaming', status: 'online', latency: '8ms', icon: <Radio className="w-4 h-4" />, color: 'text-emerald-400' },
    { name: 'ServiceNow MCP', status: 'online', latency: '120ms', icon: <Globe className="w-4 h-4" />, color: 'text-purple-400' },
    { name: 'Swarm RAG', status: 'online', latency: '85ms', icon: <Sparkles className="w-4 h-4" />, color: 'text-amber-400' },
    { name: 'PostgreSQL', status: 'online', latency: '12ms', icon: <Database className="w-4 h-4" />, color: 'text-cyan-400' },
  ]

  return (
    <div className="rounded-3xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl overflow-hidden">
      <div className="flex items-center justify-between p-6 border-b border-white/5">
        <h3 className="font-bold text-white flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-emerald-500 to-green-600">
            <Shield className="w-5 h-5 text-white" />
          </div>
          System Health
        </h3>
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-full bg-emerald-500/10 border border-emerald-500/30">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
            <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
          </span>
          <span className="text-xs font-semibold text-emerald-400">All Systems Operational</span>
        </div>
      </div>

      <div className="p-4 space-y-2">
        {services.map((service, idx) => (
          <div
            key={idx}
            className="flex items-center justify-between p-4 rounded-xl bg-white/[0.02] hover:bg-white/[0.04] border border-white/5 transition-all duration-200"
          >
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-lg bg-white/5 ${service.color}`}>
                {service.icon}
              </div>
              <span className="text-sm font-medium text-gray-300">{service.name}</span>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-xs text-gray-500 font-mono bg-black/20 px-2 py-1 rounded">{service.latency}</span>
              <div className="w-2 h-2 rounded-full bg-emerald-500 shadow-lg shadow-emerald-500/50" />
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// =============================================================================
// Main Dashboard Component
// =============================================================================

export default function DashboardPage() {
  const [incidents, setIncidents] = useState<Incident[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [apiUrl, setApiUrl] = useState('')
  const [dataSource, setDataSource] = useState<string>('')

  // Mock workflow executions for demo
  const [workflows] = useState<WorkflowExecution[]>([
    { id: '1', incident_id: 'INC0010001', current_node: 'swarm_rag', status: 'running', started_at: new Date().toISOString() },
    { id: '2', incident_id: 'INC0010002', current_node: 'await_approval', status: 'paused', started_at: new Date(Date.now() - 300000).toISOString() },
  ])

  useEffect(() => {
    setApiUrl(getApiBaseUrl())
  }, [])

  useEffect(() => {
    const fetchData = async () => {
      if (!apiUrl) return

      try {
        const incRes = await fetch(`${apiUrl}/api/incidents`)
        if (incRes.ok) {
          const incData = await incRes.json()
          setIncidents(incData.incidents || [])
          setDataSource(incData.source || 'unknown')
        }
        setError(null)
      } catch (err) {
        setError(`Failed to connect to backend`)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
    const interval = setInterval(fetchData, 30000)
    return () => clearInterval(interval)
  }, [apiUrl])

  // Calculate stats
  const stats = {
    activeIncidents: incidents.filter(i => !['6', '7'].includes(i.status)).length,
    pendingApproval: incidents.filter(i => i.status === 'pending_approval').length,
    criticalCount: incidents.filter(i => i.priority === '1' && !['6', '7'].includes(i.status)).length,
    resolvedToday: incidents.filter(i => ['6', '7'].includes(i.status)).length,
    activeWorkflows: workflows.filter(w => w.status === 'running').length,
  }

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="flex flex-col items-center gap-6">
          <div className="relative">
            <div className="w-20 h-20 rounded-full border-4 border-purple-500/20" />
            <div className="w-20 h-20 rounded-full border-4 border-purple-500 border-t-transparent animate-spin absolute top-0 left-0" />
            <div className="absolute inset-0 flex items-center justify-center">
              <Cpu className="w-8 h-8 text-purple-400" />
            </div>
          </div>
          <div className="text-center">
            <p className="text-lg font-semibold text-white">Loading Dashboard</p>
            <p className="text-sm text-gray-500">Connecting to AI agents...</p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="sticky top-0 z-50 border-b border-white/5 bg-[#0a0f1a]/80 backdrop-blur-xl">
        <div className="px-8 py-5">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-5">
              <div className="relative">
                <div className="p-3 rounded-2xl bg-gradient-to-br from-violet-500 via-purple-500 to-fuchsia-500 shadow-xl shadow-purple-500/25">
                  <Sparkles className="w-7 h-7 text-white" />
                </div>
                <div className="absolute -bottom-1 -right-1 w-4 h-4 rounded-full bg-emerald-500 border-2 border-[#0a0f1a]" />
              </div>
              <div>
                <h1 className="text-2xl font-bold text-white tracking-tight">AI Agent Platform</h1>
                <div className="flex items-center gap-3 mt-1">
                  <span className="text-sm text-gray-500">Enterprise IT Service Management</span>
                  {dataSource && (
                    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
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
                onClick={() => window.location.reload()}
                className="flex items-center gap-2 px-4 py-2.5 text-sm text-gray-400 hover:text-white bg-white/5 hover:bg-white/10 rounded-xl border border-white/10 hover:border-white/20 transition-all duration-200"
              >
                <RefreshCw className="w-4 h-4" />
                Refresh
              </button>
              <Link
                href="/incidents"
                className="flex items-center gap-2 px-5 py-2.5 text-sm font-semibold text-white bg-gradient-to-r from-red-500 to-rose-500 hover:from-red-400 hover:to-rose-400 rounded-xl shadow-lg shadow-red-500/25 transition-all duration-200"
              >
                <AlertCircle className="w-4 h-4" />
                View Incidents
              </Link>
            </div>
          </div>
        </div>
      </header>

      <main className="px-8 py-8">
        {/* Error Banner */}
        {error && (
          <div className="bg-red-500/10 border border-red-500/30 rounded-2xl p-4 mb-8 flex items-center gap-4">
            <div className="p-2 rounded-lg bg-red-500/20">
              <AlertTriangle className="w-5 h-5 text-red-400" />
            </div>
            <span className="text-red-400 flex-1">{error}</span>
            <button
              onClick={() => window.location.reload()}
              className="flex items-center gap-2 px-4 py-2 text-sm text-red-400 hover:text-red-300 bg-red-500/10 hover:bg-red-500/20 rounded-lg transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
              Retry
            </button>
          </div>
        )}

        {/* Welcome Section */}
        <div className="mb-8">
          <h2 className="text-3xl font-bold text-white mb-2">Welcome back</h2>
          <p className="text-gray-500">Here's what's happening with your AI agents today.</p>
        </div>

        {/* Key Metrics */}
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-8">
          <MetricCard
            label="Active Incidents"
            value={stats.activeIncidents}
            icon={<AlertCircle className="w-5 h-5 text-white" />}
            color="red"
            href="/incidents"
            subtitle="Requires attention"
          />
          <MetricCard
            label="Critical (P1)"
            value={stats.criticalCount}
            icon={<Zap className="w-5 h-5 text-white" />}
            color="orange"
            href="/incidents?priority=1"
            subtitle="High priority"
          />
          <MetricCard
            label="Pending Approval"
            value={stats.pendingApproval}
            icon={<Clock className="w-5 h-5 text-white" />}
            color="yellow"
            href="/approvals"
            subtitle="Awaiting action"
          />
          <MetricCard
            label="Active Workflows"
            value={stats.activeWorkflows}
            icon={<Activity className="w-5 h-5 text-white" />}
            color="blue"
            href="/workflows"
            subtitle="Currently running"
          />
          <MetricCard
            label="Resolved Today"
            value={stats.resolvedToday}
            icon={<CheckCircle className="w-5 h-5 text-white" />}
            trend={{ value: 12, direction: 'up' }}
            color="green"
            subtitle="Successfully closed"
          />
        </div>

        {/* Agent Cards */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <AgentStatusCard
            name="IT Service Agent"
            description="ServiceNow incident automation"
            status={stats.activeWorkflows > 0 ? 'active' : 'idle'}
            icon={<AlertCircle className="w-6 h-6 text-white" />}
            color="red"
            href="/incidents"
            stats={[
              { label: 'Active Incidents', value: stats.activeIncidents },
              { label: 'Pending Approval', value: stats.pendingApproval },
              { label: 'Avg Resolution', value: '2.4h' },
              { label: 'Success Rate', value: '94%' },
            ]}
          />

          <AgentStatusCard
            name="Jira Agent"
            description="Jira issue management & pipelines"
            status="idle"
            icon={<Ticket className="w-6 h-6 text-white" />}
            color="blue"
            href="/jira"
            stats={[
              { label: 'Open Tickets', value: 0 },
              { label: 'In Progress', value: 0 },
              { label: 'Linked PRs', value: 0 },
              { label: 'Closed Today', value: 0 },
            ]}
          />
        </div>

        {/* Live Workflows Section */}
        {workflows.length > 0 && (
          <div className="mb-8">
            <div className="flex items-center justify-between mb-5">
              <h3 className="text-xl font-bold text-white flex items-center gap-3">
                <div className="p-2 rounded-lg bg-gradient-to-br from-blue-500 to-cyan-600">
                  <Activity className="w-5 h-5 text-white" />
                </div>
                Live Workflows
                <span className="px-2.5 py-1 text-xs font-semibold bg-blue-500/15 text-blue-400 border border-blue-500/30 rounded-full">
                  {workflows.filter(w => w.status === 'running').length} active
                </span>
              </h3>
              <Link href="/workflows" className="flex items-center gap-2 text-sm text-blue-400 hover:text-blue-300 font-medium transition-colors">
                View all workflows <ArrowRight className="w-4 h-4" />
              </Link>
            </div>
            <div className="space-y-3">
              {workflows.map(workflow => (
                <LiveWorkflowCard key={workflow.id} execution={workflow} />
              ))}
            </div>
          </div>
        )}

        {/* Recent Activity & System Health */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Recent Incidents */}
          <div className="rounded-3xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl overflow-hidden">
            <div className="flex items-center justify-between p-6 border-b border-white/5">
              <h3 className="font-bold text-white flex items-center gap-3">
                <div className="p-2 rounded-lg bg-gradient-to-br from-red-500 to-rose-600">
                  <AlertCircle className="w-5 h-5 text-white" />
                </div>
                Recent Incidents
              </h3>
              <Link href="/incidents" className="flex items-center gap-2 text-sm text-red-400 hover:text-red-300 font-medium transition-colors">
                View all <ArrowRight className="w-4 h-4" />
              </Link>
            </div>
            <div className="divide-y divide-white/5">
              {incidents.length === 0 ? (
                <div className="p-12 text-center">
                  <div className="w-16 h-16 mx-auto mb-4 rounded-full bg-emerald-500/10 flex items-center justify-center">
                    <CheckCircle className="w-8 h-8 text-emerald-500" />
                  </div>
                  <p className="text-lg font-semibold text-white mb-1">No active incidents</p>
                  <p className="text-sm text-gray-500">All systems are running smoothly</p>
                </div>
              ) : (
                incidents.slice(0, 5).map(incident => (
                  <ActivityItem key={incident.incident_id} incident={incident} />
                ))
              )}
            </div>
          </div>

          {/* System Health */}
          <SystemHealthPanel />
        </div>

        {/* Footer */}
        <div className="mt-16 text-center">
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white/5 border border-white/10">
            <Cpu className="w-4 h-4 text-purple-400" />
            <span className="text-sm text-gray-500">
              AI Agent Platform v6.0 | Event-Driven Architecture | LangGraph | Swarm RAG
            </span>
          </div>
        </div>
      </main>
    </div>
  )
}
