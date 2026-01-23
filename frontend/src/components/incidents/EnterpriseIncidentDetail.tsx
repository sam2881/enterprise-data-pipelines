'use client'

import { useState, useRef, useEffect } from 'react'
import { Badge } from '../ui/Badge'
import { Button } from '../ui/Button'
import {
  Play, CheckCircle, XCircle, Loader2, ArrowLeft,
  ThumbsUp, ThumbsDown, AlertTriangle, Zap,
  Download, Upload, Brain, Search, FileCode, Shield,
  Clock, Rocket, CheckCheck, RefreshCw, BookOpen,
  Sparkles, Server, Users, Calendar, GitBranch, Terminal,
  Send, Bot, User, MessageSquare, ChevronDown, ChevronUp,
  ExternalLink, Copy, Tag, Activity
} from 'lucide-react'
import toast from 'react-hot-toast'
import { useRouter } from 'next/navigation'
import { getApiBaseUrl } from '@/lib/constants'

const API_BASE = getApiBaseUrl()

// ============================================================================
// TYPE DEFINITIONS
// ============================================================================
interface WorkflowStep {
  id: number
  name: string
  phase: string
  icon: any
  description: string
  status: 'pending' | 'running' | 'completed' | 'failed' | 'awaiting_approval'
  output?: any
  error?: string
}

interface EnterpriseIncidentDetailProps {
  incident: any
}

interface ChatMessage {
  id: string
  role: 'user' | 'assistant' | 'system'
  content: string
  timestamp: string
}

interface SelectedScript {
  name: string
  path: string
  confidence: number
  match_reason: string
  risk_level: string
  keywords: string[]
  estimated_time: string
}

// ============================================================================
// 12-NODE WORKFLOW STEPS
// ============================================================================
const INITIAL_STEPS: WorkflowStep[] = [
  { id: 1, name: 'Ingest', phase: 'Ingestion', icon: Download, description: 'Receive incident from ServiceNow via Kafka', status: 'pending' },
  { id: 2, name: 'Parse', phase: 'Ingestion', icon: Upload, description: 'Extract service, error patterns, resources', status: 'pending' },
  { id: 3, name: 'Classify', phase: 'Classification', icon: Brain, description: 'LLM classifies incident type (GCP, Airflow, K8s)', status: 'pending' },
  { id: 4, name: 'Swarm RAG', phase: 'Retrieval', icon: Search, description: 'Hybrid search: Vector + Graph + Keyword', status: 'pending' },
  { id: 5, name: 'Generate Plan', phase: 'Planning', icon: FileCode, description: 'Create remediation plan with rollback strategy', status: 'pending' },
  { id: 6, name: 'LLM Judge', phase: 'Validation', icon: Shield, description: 'Evaluate plan quality, safety, feasibility', status: 'pending' },
  { id: 7, name: 'Control Plane', phase: 'Approval', icon: Shield, description: 'Risk assessment and approval routing', status: 'pending' },
  { id: 8, name: 'Await Approval', phase: 'Approval', icon: Clock, description: 'Human-in-the-loop checkpoint', status: 'pending' },
  { id: 9, name: 'Execute', phase: 'Execution', icon: Rocket, description: 'Trigger GitHub Actions remediation', status: 'pending' },
  { id: 10, name: 'Verify Fix', phase: 'Execution', icon: CheckCheck, description: 'Check service health post-remediation', status: 'pending' },
  { id: 11, name: 'Close Ticket', phase: 'Completion', icon: CheckCircle, description: 'Update ServiceNow with resolution', status: 'pending' },
  { id: 12, name: 'Feedback Loop', phase: 'Completion', icon: RefreshCw, description: 'Update RAG for continuous learning', status: 'pending' },
]

// Phase colors for dark theme
const PHASE_COLORS: Record<string, { gradient: string; bg: string; border: string; text: string }> = {
  'Ingestion': { gradient: 'from-blue-500 to-cyan-500', bg: 'bg-blue-500/10', border: 'border-blue-500/30', text: 'text-blue-400' },
  'Classification': { gradient: 'from-purple-500 to-violet-500', bg: 'bg-purple-500/10', border: 'border-purple-500/30', text: 'text-purple-400' },
  'Retrieval': { gradient: 'from-cyan-500 to-teal-500', bg: 'bg-cyan-500/10', border: 'border-cyan-500/30', text: 'text-cyan-400' },
  'Planning': { gradient: 'from-amber-500 to-orange-500', bg: 'bg-amber-500/10', border: 'border-amber-500/30', text: 'text-amber-400' },
  'Validation': { gradient: 'from-pink-500 to-rose-500', bg: 'bg-pink-500/10', border: 'border-pink-500/30', text: 'text-pink-400' },
  'Approval': { gradient: 'from-red-500 to-orange-500', bg: 'bg-red-500/10', border: 'border-red-500/30', text: 'text-red-400' },
  'Execution': { gradient: 'from-emerald-500 to-green-500', bg: 'bg-emerald-500/10', border: 'border-emerald-500/30', text: 'text-emerald-400' },
  'Completion': { gradient: 'from-green-500 to-teal-500', bg: 'bg-green-500/10', border: 'border-green-500/30', text: 'text-green-400' },
}

// ============================================================================
// CHAT COMPONENT
// ============================================================================
function IncidentChatPanel({ incident, selectedScripts }: { incident: any; selectedScripts: SelectedScript[] }) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [isExpanded, setIsExpanded] = useState(true)
  const messagesEndRef = useRef<HTMLDivElement>(null)

  const incidentId = incident?.incident_id || incident?.number || 'Unknown'

  // Initialize with context message
  useEffect(() => {
    const contextMessage: ChatMessage = {
      id: '1',
      role: 'system',
      content: `I'm your AI assistant for incident **${incidentId}**. I have full context including:

• **Incident**: ${incident?.short_description || 'No description'}
• **Category**: ${incident?.category || 'Unknown'}
• **Priority**: P${incident?.priority || '3'}
• **Scripts Found**: ${selectedScripts.length} remediation scripts matched

${selectedScripts.length > 0 ? `**Top Scripts:**
${selectedScripts.slice(0, 3).map((s, i) => `${i + 1}. ${s.name} (${(s.confidence * 100).toFixed(0)}% match)`).join('\n')}` : ''}

How can I help you analyze or resolve this incident?`,
      timestamp: new Date().toISOString()
    }
    setMessages([contextMessage])
  }, [incident, selectedScripts])

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const handleSend = async () => {
    if (!input.trim() || isLoading) return

    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      role: 'user',
      content: input,
      timestamp: new Date().toISOString()
    }

    setMessages(prev => [...prev, userMessage])
    setInput('')
    setIsLoading(true)

    try {
      const response = await fetch(`${API_BASE}/api/v1/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          incident_id: incidentId,
          message: input,
          context: {
            incident,
            scripts: selectedScripts
          }
        })
      })

      if (response.ok) {
        const data = await response.json()
        const assistantMessage: ChatMessage = {
          id: (Date.now() + 1).toString(),
          role: 'assistant',
          content: data.response || 'I apologize, but I could not generate a response.',
          timestamp: new Date().toISOString()
        }
        setMessages(prev => [...prev, assistantMessage])
      } else {
        throw new Error('Chat API error')
      }
    } catch (error) {
      // Fallback response when API is not available
      const fallbackMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: `Based on the incident context for **${incidentId}**:

**Analysis:**
- Category: ${incident?.category || 'Unknown'}
- This appears to be a ${incident?.priority === '1' ? 'critical' : 'standard'} incident

**Recommended Actions:**
${selectedScripts.length > 0
  ? `1. Run the top-matched script: **${selectedScripts[0]?.name}**
2. Monitor the execution in the workflow panel
3. Verify the fix using post-execution checks`
  : `1. Review the incident description for root cause
2. Check related services and dependencies
3. Consider manual remediation steps`}

Would you like me to explain any of these steps in more detail?`,
        timestamp: new Date().toISOString()
      }
      setMessages(prev => [...prev, fallbackMessage])
    } finally {
      setIsLoading(false)
    }
  }

  const suggestedQuestions = [
    "What's the root cause?",
    "Explain the fix",
    "Show similar incidents",
    "What are the risks?"
  ]

  return (
    <div className="rounded-2xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-4 border-b border-white/5 hover:bg-white/5 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-purple-500 to-violet-600">
            <MessageSquare className="w-4 h-4 text-white" />
          </div>
          <div className="text-left">
            <h3 className="font-semibold text-white text-sm">AI Assistant</h3>
            <p className="text-xs text-gray-500">Context-aware chat</p>
          </div>
        </div>
        {isExpanded ? (
          <ChevronUp className="w-4 h-4 text-gray-500" />
        ) : (
          <ChevronDown className="w-4 h-4 text-gray-500" />
        )}
      </button>

      {isExpanded && (
        <>
          {/* Messages */}
          <div className="h-80 overflow-y-auto p-4 space-y-4">
            {messages.map((message) => (
              <div
                key={message.id}
                className={`flex gap-3 ${message.role === 'user' ? 'flex-row-reverse' : 'flex-row'}`}
              >
                <div className={`flex-shrink-0 w-7 h-7 rounded-lg flex items-center justify-center ${
                  message.role === 'user'
                    ? 'bg-gradient-to-br from-blue-500 to-cyan-500'
                    : 'bg-gradient-to-br from-purple-500 to-violet-500'
                }`}>
                  {message.role === 'user' ? (
                    <User className="w-4 h-4 text-white" />
                  ) : (
                    <Bot className="w-4 h-4 text-white" />
                  )}
                </div>
                <div className={`flex-1 max-w-[85%] ${message.role === 'user' ? 'text-right' : 'text-left'}`}>
                  <div className={`inline-block rounded-xl px-4 py-2.5 text-sm ${
                    message.role === 'user'
                      ? 'bg-gradient-to-r from-blue-500 to-cyan-500 text-white'
                      : 'bg-white/5 border border-white/10 text-gray-300'
                  }`}>
                    <div className="whitespace-pre-wrap text-left">{message.content}</div>
                  </div>
                  <div className="text-[10px] text-gray-600 mt-1">
                    {new Date(message.timestamp).toLocaleTimeString()}
                  </div>
                </div>
              </div>
            ))}
            {isLoading && (
              <div className="flex gap-3">
                <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-purple-500 to-violet-500 flex items-center justify-center">
                  <Bot className="w-4 h-4 text-white" />
                </div>
                <div className="bg-white/5 border border-white/10 rounded-xl px-4 py-3">
                  <div className="flex items-center gap-2">
                    <Loader2 className="w-4 h-4 text-purple-400 animate-spin" />
                    <span className="text-sm text-gray-400">Analyzing...</span>
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* Suggested Questions */}
          {messages.length <= 2 && (
            <div className="px-4 py-2 border-t border-white/5">
              <div className="flex flex-wrap gap-2">
                {suggestedQuestions.map((q, i) => (
                  <button
                    key={i}
                    onClick={() => setInput(q)}
                    className="text-xs bg-white/5 border border-white/10 rounded-lg px-3 py-1.5 text-gray-400 hover:text-white hover:bg-white/10 hover:border-white/20 transition-all"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Input */}
          <div className="p-4 border-t border-white/5">
            <div className="flex gap-2">
              <input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && handleSend()}
                placeholder="Ask about this incident..."
                className="flex-1 bg-white/5 border border-white/10 text-white text-sm rounded-xl px-4 py-2.5 placeholder-gray-500 focus:outline-none focus:border-purple-500/50 focus:ring-1 focus:ring-purple-500/30 transition-all"
                disabled={isLoading}
              />
              <button
                onClick={handleSend}
                disabled={!input.trim() || isLoading}
                className="p-2.5 rounded-xl bg-gradient-to-r from-purple-500 to-violet-500 text-white hover:from-purple-400 hover:to-violet-400 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
              >
                <Send className="w-4 h-4" />
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

// ============================================================================
// TOP SCRIPTS PANEL
// ============================================================================
function TopScriptsPanel({ scripts, onSelectScript }: { scripts: SelectedScript[]; onSelectScript: (script: SelectedScript) => void }) {
  const [isExpanded, setIsExpanded] = useState(true)

  const getRiskColor = (risk: string) => {
    switch (risk?.toLowerCase()) {
      case 'low': return 'text-emerald-400 bg-emerald-500/10 border-emerald-500/30'
      case 'medium': return 'text-amber-400 bg-amber-500/10 border-amber-500/30'
      case 'high': return 'text-red-400 bg-red-500/10 border-red-500/30'
      default: return 'text-gray-400 bg-gray-500/10 border-gray-500/30'
    }
  }

  return (
    <div className="rounded-2xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-4 border-b border-white/5 hover:bg-white/5 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-gradient-to-br from-cyan-500 to-teal-600">
            <Search className="w-4 h-4 text-white" />
          </div>
          <div className="text-left">
            <h3 className="font-semibold text-white text-sm">Swarm RAG Results</h3>
            <p className="text-xs text-gray-500">Top {scripts.length} matched scripts</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="px-2 py-0.5 rounded-full text-xs font-semibold bg-cyan-500/15 text-cyan-400 border border-cyan-500/30">
            {scripts.length} found
          </span>
          {isExpanded ? (
            <ChevronUp className="w-4 h-4 text-gray-500" />
          ) : (
            <ChevronDown className="w-4 h-4 text-gray-500" />
          )}
        </div>
      </button>

      {isExpanded && (
        <div className="p-4 space-y-3">
          {scripts.length === 0 ? (
            <div className="text-center py-8">
              <Search className="w-10 h-10 text-gray-600 mx-auto mb-3" />
              <p className="text-gray-400 text-sm">Run workflow to find matching scripts</p>
            </div>
          ) : (
            scripts.map((script, index) => (
              <div
                key={script.name}
                className={`relative rounded-xl border p-4 transition-all hover:bg-white/5 cursor-pointer ${
                  index === 0
                    ? 'border-cyan-500/30 bg-cyan-500/5'
                    : 'border-white/10 bg-white/[0.02]'
                }`}
                onClick={() => onSelectScript(script)}
              >
                {/* Rank Badge */}
                <div className={`absolute -top-2 -left-2 w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                  index === 0
                    ? 'bg-gradient-to-br from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                    : 'bg-white/10 text-gray-400'
                }`}>
                  {index + 1}
                </div>

                {/* Script Info */}
                <div className="ml-4">
                  <div className="flex items-center justify-between mb-2">
                    <h4 className="font-semibold text-white text-sm">{script.name}</h4>
                    <span className={`px-2 py-0.5 rounded text-[10px] font-bold border ${getRiskColor(script.risk_level)}`}>
                      {script.risk_level?.toUpperCase() || 'UNKNOWN'}
                    </span>
                  </div>

                  {/* Confidence Bar */}
                  <div className="mb-2">
                    <div className="flex items-center justify-between text-xs mb-1">
                      <span className="text-gray-500">Confidence</span>
                      <span className="text-cyan-400 font-semibold">{(script.confidence * 100).toFixed(0)}%</span>
                    </div>
                    <div className="h-1.5 bg-white/10 rounded-full overflow-hidden">
                      <div
                        className="h-full bg-gradient-to-r from-cyan-500 to-teal-500 rounded-full transition-all"
                        style={{ width: `${script.confidence * 100}%` }}
                      />
                    </div>
                  </div>

                  {/* Match Reason */}
                  <p className="text-xs text-gray-500 mb-2">{script.match_reason}</p>

                  {/* Keywords */}
                  {script.keywords && script.keywords.length > 0 && (
                    <div className="flex flex-wrap gap-1">
                      {script.keywords.slice(0, 3).map((kw, i) => (
                        <span key={i} className="text-[10px] px-1.5 py-0.5 rounded bg-white/5 text-gray-500">
                          {kw}
                        </span>
                      ))}
                    </div>
                  )}

                  {/* Estimated Time */}
                  {script.estimated_time && (
                    <div className="flex items-center gap-1 mt-2 text-xs text-gray-500">
                      <Clock className="w-3 h-3" />
                      <span>{script.estimated_time}</span>
                    </div>
                  )}
                </div>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  )
}

// ============================================================================
// MAIN COMPONENT
// ============================================================================
export function EnterpriseIncidentDetail({ incident }: EnterpriseIncidentDetailProps) {
  const router = useRouter()
  const [steps, setSteps] = useState<WorkflowStep[]>(INITIAL_STEPS)
  const [isRunning, setIsRunning] = useState(false)
  const [awaitingApproval, setAwaitingApproval] = useState(false)
  const [selectedScript, setSelectedScript] = useState<any>(null)
  const [topScripts, setTopScripts] = useState<SelectedScript[]>([])

  const incidentId = incident?.incident_id || incident?.number || 'Unknown'

  // Update step status
  const updateStep = (stepId: number, status: WorkflowStep['status'], output?: any, error?: string) => {
    setSteps(prev => prev.map(s =>
      s.id === stepId ? { ...s, status, output, error } : s
    ))
  }

  // Execute workflow node via API
  const executeNode = async (nodeId: number, scriptName?: string): Promise<any> => {
    const response = await fetch(`${API_BASE}/api/langgraph/node/${nodeId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        workflow_id: `WF-${Date.now()}`,
        incident_id: incidentId,
        node_id: nodeId,
        script_name: scriptName || selectedScript?.name || 'start_gcp_instance.sh',
        input_data: {}
      })
    })

    if (!response.ok) {
      throw new Error(`Node ${nodeId} failed: ${response.status}`)
    }

    return await response.json()
  }

  // Run the full 12-node workflow
  const runWorkflow = async () => {
    if (isRunning) return
    setIsRunning(true)
    setSteps(INITIAL_STEPS)
    setTopScripts([])

    try {
      for (let nodeId = 1; nodeId <= 12; nodeId++) {
        updateStep(nodeId, 'running')

        try {
          const result = await executeNode(nodeId)

          if (result.status === 'completed') {
            updateStep(nodeId, 'completed', result.output)

            // Extract top scripts from Swarm RAG (node 4)
            if (nodeId === 4 && result.output?.matched_scripts) {
              const scripts: SelectedScript[] = result.output.matched_scripts.map((s: any) => ({
                name: s.name || s.script_name,
                path: s.path || s.script_path,
                confidence: s.confidence || s.score || 0.8,
                match_reason: s.match_reason || s.reason || 'Matched via hybrid search',
                risk_level: s.risk_level || s.risk || 'medium',
                keywords: s.keywords || [],
                estimated_time: s.estimated_time || '~5 min'
              }))
              setTopScripts(scripts.slice(0, 3))

              // Auto-select top script
              if (scripts.length > 0) {
                setSelectedScript(scripts[0])
              }
            }

            if (nodeId === 8 && result.output?.requires_approval) {
              updateStep(8, 'awaiting_approval', result.output)
              setAwaitingApproval(true)
              setSelectedScript(result.output?.script || selectedScript)
              toast.success('Workflow paused - awaiting human approval')
              return
            }
          } else if (result.status === 'awaiting_approval') {
            updateStep(nodeId, 'awaiting_approval', result.output)
            setAwaitingApproval(true)
            toast.success('Workflow paused - awaiting approval')
            return
          } else if (result.status === 'failed') {
            updateStep(nodeId, 'failed', null, result.error || 'Unknown error')
            toast.error(`Step ${nodeId} failed`)
            setIsRunning(false)
            return
          }
        } catch (nodeError: any) {
          updateStep(nodeId, 'failed', null, nodeError.message)
          toast.error(`Step ${nodeId} failed: ${nodeError.message}`)
          setIsRunning(false)
          return
        }
      }

      toast.success('Workflow completed successfully!')
      setIsRunning(false)
    } catch (error: any) {
      toast.error(`Workflow error: ${error.message}`)
      setIsRunning(false)
    }
  }

  // Continue workflow after approval
  const handleApprove = async () => {
    const scriptToRun = selectedScript?.name || 'start_gcp_instance.sh'
    updateStep(8, 'completed', { approved: true, by: 'admin', timestamp: new Date().toISOString(), script: scriptToRun })
    setAwaitingApproval(false)
    toast.success(`Approved! Executing ${scriptToRun}...`)

    try {
      for (let nodeId = 9; nodeId <= 12; nodeId++) {
        updateStep(nodeId, 'running')

        try {
          const result = await executeNode(nodeId, scriptToRun)

          if (result.status === 'completed') {
            updateStep(nodeId, 'completed', result.output)

            if (nodeId === 9 && result.output?.github_status) {
              if (result.output.github_status === 'triggered') {
                toast.success(`GitHub Actions triggered! Run ID: ${result.output.github_run_id || 'pending'}`)
              } else if (result.output.github_status === 'simulated') {
                toast('GitHub Actions simulated (token not configured)', { icon: 'ℹ️' })
              } else {
                toast(`GitHub: ${result.output.github_status}`, { icon: '⚠️' })
              }
            }
          } else if (result.status === 'failed') {
            updateStep(nodeId, 'failed', null, result.error)
            toast.error(`Step ${nodeId} failed`)
            setIsRunning(false)
            return
          }
        } catch (nodeError: any) {
          updateStep(nodeId, 'failed', null, nodeError.message)
          toast.error(`Step ${nodeId} failed`)
          setIsRunning(false)
          return
        }
      }

      toast.success('Workflow completed successfully!')
      setIsRunning(false)
    } catch (error: any) {
      toast.error(`Workflow error: ${error.message}`)
      setIsRunning(false)
    }
  }

  // Reject workflow
  const handleReject = () => {
    updateStep(8, 'failed', null, 'Rejected by user')
    setAwaitingApproval(false)
    setIsRunning(false)
    toast.success('Workflow rejected')
  }

  // Handle script selection from panel
  const handleSelectScript = (script: SelectedScript) => {
    setSelectedScript(script)
    toast.success(`Selected: ${script.name}`)
  }

  // Get status icon
  const getStatusIcon = (step: WorkflowStep) => {
    const IconComponent = step.icon
    switch (step.status) {
      case 'completed':
        return <CheckCircle className="w-5 h-5 text-emerald-400" />
      case 'running':
        return <Loader2 className="w-5 h-5 text-blue-400 animate-spin" />
      case 'failed':
        return <XCircle className="w-5 h-5 text-red-400" />
      case 'awaiting_approval':
        return <AlertTriangle className="w-5 h-5 text-amber-400 animate-pulse" />
      default:
        return <IconComponent className="w-5 h-5 text-gray-500" />
    }
  }

  const completedSteps = steps.filter(s => s.status === 'completed').length
  const progressPercent = Math.round((completedSteps / 12) * 100)
  const phases = ['Ingestion', 'Classification', 'Retrieval', 'Planning', 'Validation', 'Approval', 'Execution', 'Completion']

  const priorityConfig: Record<string, { label: string; className: string }> = {
    '1': { label: 'P1 Critical', className: 'bg-gradient-to-r from-red-500 to-rose-500 text-white shadow-lg shadow-red-500/30' },
    '2': { label: 'P2 High', className: 'bg-gradient-to-r from-orange-500 to-amber-500 text-white shadow-lg shadow-orange-500/30' },
    '3': { label: 'P3 Medium', className: 'bg-gradient-to-r from-yellow-500 to-amber-400 text-gray-900 shadow-lg shadow-yellow-500/30' },
    '4': { label: 'P4 Low', className: 'bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/30' },
    '5': { label: 'P5 Planning', className: 'bg-gradient-to-r from-gray-500 to-gray-600 text-white' },
  }

  const pConfig = priorityConfig[incident?.priority] || priorityConfig['3']

  return (
    <div className="min-h-screen">
      {/* Header */}
      <div className="sticky top-0 z-50 border-b border-white/5 bg-[#0a0f1a]/95 backdrop-blur-xl">
        <div className="px-8 py-5">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-5">
              <button
                onClick={() => router.push('/incidents')}
                className="p-2.5 rounded-xl bg-white/5 hover:bg-white/10 border border-white/10 hover:border-white/20 transition-all duration-200"
              >
                <ArrowLeft className="w-5 h-5 text-gray-400" />
              </button>
              <div className="flex items-center gap-4">
                <div className="p-3 rounded-2xl bg-gradient-to-br from-indigo-500 to-purple-600 shadow-lg shadow-indigo-500/25">
                  <Zap className="w-6 h-6 text-white" />
                </div>
                <div>
                  <div className="flex items-center gap-3 mb-1">
                    <h1 className="text-2xl font-bold text-white tracking-tight">{incidentId}</h1>
                    <span className={`px-3 py-1 rounded-lg text-xs font-bold ${pConfig.className}`}>
                      {pConfig.label}
                    </span>
                  </div>
                  <p className="text-sm text-gray-400 max-w-xl truncate">
                    {incident?.short_description || 'No description'}
                  </p>
                </div>
              </div>
            </div>

            <div className="flex items-center gap-6">
              {/* Progress */}
              <div className="flex items-center gap-4">
                <div className="text-right">
                  <p className="text-xs text-gray-500 font-medium">Progress</p>
                  <p className="text-lg font-bold text-white">{completedSteps}/12</p>
                </div>
                <div className="relative w-40 h-2 bg-white/10 rounded-full overflow-hidden">
                  <div
                    className="absolute inset-y-0 left-0 bg-gradient-to-r from-indigo-500 to-purple-500 rounded-full transition-all duration-500"
                    style={{ width: `${progressPercent}%` }}
                  />
                </div>
              </div>

              {/* Start Button */}
              <button
                onClick={runWorkflow}
                disabled={isRunning && !awaitingApproval}
                className={`
                  flex items-center gap-2 px-6 py-3 rounded-xl font-semibold text-sm
                  transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed
                  ${isRunning && !awaitingApproval
                    ? 'bg-white/10 text-gray-400 border border-white/10'
                    : 'bg-gradient-to-r from-indigo-500 to-purple-500 hover:from-indigo-400 hover:to-purple-400 text-white shadow-lg shadow-indigo-500/25'
                  }
                `}
              >
                {isRunning && !awaitingApproval ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Running...
                  </>
                ) : (
                  <>
                    <Play className="w-4 h-4" />
                    Start Workflow
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Approval Banner */}
      {awaitingApproval && (
        <div className="bg-gradient-to-r from-amber-500 to-orange-500 shadow-lg">
          <div className="px-8 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <div className="p-3 rounded-xl bg-white/20 backdrop-blur-sm">
                  <AlertTriangle className="w-6 h-6 text-white" />
                </div>
                <div>
                  <p className="font-bold text-white text-lg">Human Approval Required</p>
                  <p className="text-sm text-amber-100">
                    {selectedScript ? `Script: ${selectedScript.name}` : 'Review and approve the remediation plan to continue'}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-3">
                <button
                  onClick={handleApprove}
                  className="flex items-center gap-2 px-6 py-3 bg-white text-amber-600 font-bold rounded-xl hover:bg-amber-50 transition-colors shadow-lg"
                >
                  <ThumbsUp className="w-4 h-4" />
                  Approve
                </button>
                <button
                  onClick={handleReject}
                  className="flex items-center gap-2 px-6 py-3 bg-white/10 text-white font-semibold rounded-xl hover:bg-white/20 border border-white/30 transition-colors"
                >
                  <ThumbsDown className="w-4 h-4" />
                  Reject
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Main Content - Two Column Layout */}
      <div className="px-8 py-8">
        <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
          {/* Left Column - Workflow */}
          <div className="xl:col-span-2 space-y-6">
            {/* Incident Summary */}
            <div className="rounded-2xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl p-5">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                <div className="p-3 rounded-xl bg-white/5 border border-white/5">
                  <div className="flex items-center gap-2 text-gray-500 text-xs font-medium uppercase tracking-wider mb-1">
                    <Server className="w-3 h-3" />
                    Category
                  </div>
                  <p className="font-semibold text-white">{incident?.category || 'Unknown'}</p>
                </div>
                <div className="p-3 rounded-xl bg-white/5 border border-white/5">
                  <div className="flex items-center gap-2 text-gray-500 text-xs font-medium uppercase tracking-wider mb-1">
                    <GitBranch className="w-3 h-3" />
                    Status
                  </div>
                  <p className="font-semibold text-white">{incident?.status === '6' ? 'Resolved' : 'Open'}</p>
                </div>
                <div className="p-3 rounded-xl bg-white/5 border border-white/5">
                  <div className="flex items-center gap-2 text-gray-500 text-xs font-medium uppercase tracking-wider mb-1">
                    <Calendar className="w-3 h-3" />
                    Created
                  </div>
                  <p className="font-semibold text-white text-sm">{incident?.created_at || 'Unknown'}</p>
                </div>
                <div className="p-3 rounded-xl bg-white/5 border border-white/5">
                  <div className="flex items-center gap-2 text-gray-500 text-xs font-medium uppercase tracking-wider mb-1">
                    <Users className="w-3 h-3" />
                    Assigned
                  </div>
                  <p className="font-semibold text-white">{incident?.assigned_to || 'Unassigned'}</p>
                </div>
              </div>
            </div>

            {/* Workflow Steps */}
            <div className="rounded-2xl border border-white/10 bg-[#0d1117]/80 backdrop-blur-xl overflow-hidden">
              <div className="flex items-center justify-between p-5 border-b border-white/5">
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-lg bg-gradient-to-br from-indigo-500 to-purple-600">
                    <BookOpen className="w-4 h-4 text-white" />
                  </div>
                  <h2 className="font-bold text-white">12-Node LangGraph Workflow</h2>
                </div>
                <div className="flex items-center gap-4 text-xs">
                  {[
                    { color: 'bg-emerald-500', label: 'Done' },
                    { color: 'bg-blue-500 animate-pulse', label: 'Running' },
                    { color: 'bg-amber-500', label: 'Waiting' },
                  ].map(item => (
                    <span key={item.label} className="flex items-center gap-1.5 text-gray-500">
                      <span className={`w-2 h-2 rounded-full ${item.color}`} />
                      {item.label}
                    </span>
                  ))}
                </div>
              </div>

              <div className="p-5 space-y-6">
                {phases.map(phase => {
                  const phaseSteps = steps.filter(s => s.phase === phase)
                  const phaseColors = PHASE_COLORS[phase]
                  const phaseCompleted = phaseSteps.filter(s => s.status === 'completed').length

                  return (
                    <div key={phase}>
                      <div className="flex items-center gap-3 mb-3">
                        <span className={`px-3 py-1 rounded-full text-[10px] font-bold bg-gradient-to-r ${phaseColors.gradient} text-white`}>
                          {phase}
                        </span>
                        <div className="flex-1 h-px bg-white/5" />
                        <span className="text-[10px] text-gray-600">{phaseCompleted}/{phaseSteps.length}</span>
                      </div>

                      <div className="grid grid-cols-2 gap-3">
                        {phaseSteps.map((step) => {
                          const isActive = step.status === 'running' || step.status === 'awaiting_approval'
                          const isCompleted = step.status === 'completed'
                          const isFailed = step.status === 'failed'

                          return (
                            <div
                              key={step.id}
                              className={`relative rounded-xl border p-4 transition-all ${
                                isActive ? `${phaseColors.border} ${phaseColors.bg}` :
                                isCompleted ? 'border-emerald-500/30 bg-emerald-500/5' :
                                isFailed ? 'border-red-500/30 bg-red-500/5' :
                                'border-white/5 bg-white/[0.02]'
                              }`}
                            >
                              <div className="flex items-start gap-3">
                                <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold ${
                                  isCompleted ? 'bg-gradient-to-br from-emerald-500 to-green-600 text-white' :
                                  step.status === 'running' ? 'bg-gradient-to-br from-blue-500 to-cyan-500 text-white' :
                                  isFailed ? 'bg-gradient-to-br from-red-500 to-rose-600 text-white' :
                                  step.status === 'awaiting_approval' ? 'bg-gradient-to-br from-amber-500 to-orange-500 text-white' :
                                  'bg-white/10 text-gray-500'
                                }`}>
                                  {step.id}
                                </div>
                                <div className="flex-1 min-w-0">
                                  <div className="flex items-center gap-2 mb-1">
                                    <h3 className="font-semibold text-white text-sm">{step.name}</h3>
                                    {getStatusIcon(step)}
                                  </div>
                                  <p className="text-xs text-gray-500">{step.description}</p>

                                  {step.status === 'awaiting_approval' && (
                                    <div className="flex gap-2 mt-3">
                                      <button onClick={handleApprove} className="flex items-center gap-1 px-3 py-1.5 bg-emerald-500 text-white text-xs font-semibold rounded-lg hover:bg-emerald-400">
                                        <ThumbsUp className="w-3 h-3" /> Approve
                                      </button>
                                      <button onClick={handleReject} className="flex items-center gap-1 px-3 py-1.5 bg-red-500 text-white text-xs font-semibold rounded-lg hover:bg-red-400">
                                        <ThumbsDown className="w-3 h-3" /> Reject
                                      </button>
                                    </div>
                                  )}
                                </div>
                              </div>
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>

          {/* Right Column - Scripts & Chat */}
          <div className="space-y-6">
            {/* Top Scripts Panel */}
            <TopScriptsPanel scripts={topScripts} onSelectScript={handleSelectScript} />

            {/* Chat Panel */}
            <IncidentChatPanel incident={incident} selectedScripts={topScripts} />
          </div>
        </div>
      </div>
    </div>
  )
}
