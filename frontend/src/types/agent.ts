export type AgentStatus = 'active' | 'idle' | 'error' | 'offline'

export interface Agent {
  name: string
  display_name: string
  description: string
  status: AgentStatus
  health_check?: {
    last_check: string
    status: 'healthy' | 'degraded' | 'unhealthy'
    response_time_ms?: number
  }
  capabilities: string[]
  metrics?: AgentMetrics
}

export interface AgentMetrics {
  total_tasks: number
  successful_tasks: number
  failed_tasks: number
  success_rate: number
  average_response_time_ms: number
  tasks_last_24h: number
  current_load: number
  last_active?: string
}

export interface AgentLog {
  timestamp: string
  level: 'info' | 'warning' | 'error'
  message: string
  task_id?: string
  metadata?: Record<string, any>
}

export const AGENT_NAMES = {
  // IT Service Agents
  SERVICENOW: 'servicenow',
  JIRA: 'jira',
  GITHUB: 'github',
  INFRASTRUCTURE: 'infrastructure',
  GCP: 'gcp',
  KUBERNETES: 'kubernetes',
  GCP_MONITOR: 'gcp_monitor',
  LLM: 'llm',
  // Data Engineering Agents
  DATA_PIPELINE: 'data-pipeline',
  // MCP Servers (Shared)
  SERVICENOW_MCP: 'servicenow-mcp',
  GCP_MCP: 'gcp-mcp',
  GITHUB_MCP: 'github-mcp',
  JIRA_MCP: 'jira-mcp',
  GCS_MCP: 'gcs-mcp',
  ICEBERG_MCP: 'iceberg-mcp',
  LLM_MCP: 'llm-mcp',
} as const

export type AgentCategory = 'it-service' | 'data' | 'shared'

export const AGENT_DISPLAY_NAMES: Record<string, string> = {
  // IT Service Agents
  servicenow: 'ServiceNow Agent',
  jira: 'Jira Agent',
  github: 'GitHub Actions Agent',
  infrastructure: 'Infrastructure Agent',
  gcp: 'GCP Agent',
  kubernetes: 'Kubernetes Agent',
  gcp_monitor: 'GCP Monitor Agent',
  llm: 'LLM Intelligence Agent',
  // Data Engineering Agents
  'data-pipeline': 'Data Pipeline Agent',
  // MCP Servers
  'servicenow-mcp': 'ServiceNow MCP',
  'gcp-mcp': 'GCP MCP',
  'github-mcp': 'GitHub MCP',
  'jira-mcp': 'Jira MCP',
  'gcs-mcp': 'GCS MCP Server',
  'iceberg-mcp': 'Iceberg MCP Server',
  'llm-mcp': 'LLM MCP Server',
}

export const AGENT_CATEGORIES: Record<string, AgentCategory> = {
  servicenow: 'it-service',
  jira: 'it-service',
  github: 'it-service',
  gcp: 'it-service',
  kubernetes: 'it-service',
  'data-pipeline': 'data',
  llm: 'shared',
  'gcs-mcp': 'shared',
  'iceberg-mcp': 'shared',
  'llm-mcp': 'shared',
}
