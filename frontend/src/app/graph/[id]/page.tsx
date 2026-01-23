'use client'

import { useState, useEffect, useRef } from 'react'
import { useParams } from 'next/navigation'
import {
  GitBranch, Zap, Activity, CheckCircle, XCircle, Loader2,
  AlertTriangle, ZoomIn, ZoomOut, RefreshCw, Download, Info,
  ArrowLeft, Clock, Database, Brain, Shield, Play, Settings
} from 'lucide-react'
import { getApiBaseUrl } from '@/lib/constants'

// Use centralized API URL resolution
const API_BASE = getApiBaseUrl()

// ============================================================================
// TYPES
// ============================================================================
interface GraphNode {
  id: number
  name: string
  phase: string
  type: string
  description: string
}

interface GraphEdge {
  from: number
  to: number
}

interface GraphPhase {
  name: string
  nodes: number[]
  color: string
}

interface GraphDefinition {
  nodes: GraphNode[]
  edges: GraphEdge[]
  phases: GraphPhase[]
}

// ============================================================================
// PHASE COLORS AND ICONS - Aligned with 7-node architecture
// ============================================================================
const PHASE_CONFIG: Record<string, { color: string; bgColor: string; icon: any }> = {
  'Ingestion': { color: '#3B82F6', bgColor: 'bg-blue-500', icon: Database },
  'Retrieval': { color: '#06B6D4', bgColor: 'bg-cyan-500', icon: Database },
  'Planning': { color: '#F59E0B', bgColor: 'bg-amber-500', icon: Brain },
  'Validation': { color: '#8B5CF6', bgColor: 'bg-purple-500', icon: Shield },
  'Approval': { color: '#EF4444', bgColor: 'bg-red-500', icon: AlertTriangle },
  'Execution': { color: '#10B981', bgColor: 'bg-emerald-500', icon: Play },
  'Completion': { color: '#22C55E', bgColor: 'bg-green-500', icon: CheckCircle },
}

const NODE_TYPE_CONFIG: Record<string, { label: string; icon: any }> = {
  'processor': { label: 'Processor', icon: Settings },
  'llm': { label: 'LLM', icon: Brain },
  'judge': { label: 'Judge', icon: Shield },
  'retriever': { label: 'Retriever', icon: Database },
  'human': { label: 'Human', icon: AlertTriangle },
  'executor': { label: 'Executor', icon: Play }
}

// ============================================================================
// FULL GRAPH VIEW PAGE
// ============================================================================
export default function GraphViewPage() {
  const params = useParams()
  const incidentId = params.id as string

  const [graphDef, setGraphDef] = useState<GraphDefinition | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [zoom, setZoom] = useState(1)
  const [hoveredNode, setHoveredNode] = useState<number | null>(null)
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
  const svgRef = useRef<SVGSVGElement>(null)

  // Fetch graph definition
  useEffect(() => {
    const fetchGraph = async () => {
      try {
        setLoading(true)
        const res = await fetch(`${API_BASE}/api/langgraph/definition`)
        if (!res.ok) throw new Error('Failed to fetch graph definition')
        const data = await res.json()
        setGraphDef(data)
        setError(null)
      } catch (err: any) {
        console.error('Graph fetch error:', err)
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchGraph()
  }, [])

  // Calculate node positions in a snake-like flowchart layout
  const getNodePosition = (nodeId: number) => {
    const nodesPerRow = 6
    const nodeWidth = 180
    const nodeHeight = 100
    const horizontalGap = 30
    const verticalGap = 80

    const row = Math.floor((nodeId - 1) / nodesPerRow)
    const col = (nodeId - 1) % nodesPerRow
    const isReversedRow = row % 2 === 1
    const actualCol = isReversedRow ? (nodesPerRow - 1 - col) : col

    return {
      x: 80 + actualCol * (nodeWidth + horizontalGap),
      y: 100 + row * (nodeHeight + verticalGap)
    }
  }

  // Get phase color
  const getPhaseColor = (phase: string) => PHASE_CONFIG[phase]?.color || '#6B7280'
  const getPhaseConfig = (phase: string) => PHASE_CONFIG[phase] || { color: '#6B7280', bgColor: 'bg-gray-500', icon: Settings }
  const getNodeTypeConfig = (type: string) => NODE_TYPE_CONFIG[type] || { label: type, icon: Settings }

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-950 flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="w-12 h-12 text-blue-500 animate-spin mx-auto mb-4" />
          <p className="text-white text-lg">Loading LangGraph Visualization...</p>
        </div>
      </div>
    )
  }

  if (error || !graphDef) {
    return (
      <div className="min-h-screen bg-gray-950 flex items-center justify-center">
        <div className="text-center">
          <AlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
          <p className="text-white text-lg">Error loading graph: {error}</p>
          <button
            onClick={() => window.location.reload()}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            Retry
          </button>
        </div>
      </div>
    )
  }

  const nodes = graphDef.nodes
  const edges = graphDef.edges
  const phases = graphDef.phases

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <div className="bg-gray-900 border-b border-gray-800 px-6 py-4">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button
              onClick={() => window.close()}
              className="flex items-center gap-2 text-gray-400 hover:text-white transition-colors"
            >
              <ArrowLeft className="w-5 h-5" />
              Back
            </button>
            <div className="h-6 w-px bg-gray-700" />
            <div className="flex items-center gap-2">
              <GitBranch className="w-6 h-6 text-indigo-400" />
              <h1 className="text-xl font-bold">LangGraph Workflow Visualization</h1>
            </div>
            {incidentId && (
              <span className="px-3 py-1 bg-indigo-600/20 text-indigo-400 rounded-full text-sm font-medium">
                Incident: {incidentId}
              </span>
            )}
          </div>

          {/* Zoom Controls */}
          <div className="flex items-center gap-2">
            <button
              onClick={() => setZoom(z => Math.max(0.5, z - 0.1))}
              className="p-2 bg-gray-800 rounded hover:bg-gray-700 transition-colors"
            >
              <ZoomOut className="w-4 h-4" />
            </button>
            <span className="text-gray-400 text-sm w-12 text-center">{Math.round(zoom * 100)}%</span>
            <button
              onClick={() => setZoom(z => Math.min(2, z + 0.1))}
              className="p-2 bg-gray-800 rounded hover:bg-gray-700 transition-colors"
            >
              <ZoomIn className="w-4 h-4" />
            </button>
            <button
              onClick={() => setZoom(1)}
              className="p-2 bg-gray-800 rounded hover:bg-gray-700 transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>

      <div className="flex">
        {/* Left Sidebar - Phase Legend & Stats */}
        <div className="w-80 bg-gray-900 border-r border-gray-800 p-6 overflow-y-auto" style={{ height: 'calc(100vh - 73px)' }}>
          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <Activity className="w-5 h-5 text-indigo-400" />
            Workflow Phases
          </h2>

          <div className="space-y-3 mb-8">
            {phases.map((phase, idx) => {
              const config = getPhaseConfig(phase.name)
              const PhaseIcon = config.icon
              return (
                <div
                  key={idx}
                  className="bg-gray-800 rounded-lg p-4 border border-gray-700"
                >
                  <div className="flex items-center gap-3 mb-2">
                    <div className={`w-10 h-10 rounded-lg ${config.bgColor} flex items-center justify-center`}>
                      <PhaseIcon className="w-5 h-5 text-white" />
                    </div>
                    <div>
                      <h3 className="font-semibold">{phase.name}</h3>
                      <p className="text-xs text-gray-400">{phase.nodes.length} nodes</p>
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-1 mt-2">
                    {phase.nodes.map(nodeId => {
                      const node = nodes.find(n => n.id === nodeId)
                      return (
                        <span
                          key={nodeId}
                          className="px-2 py-1 bg-gray-700 rounded text-xs cursor-pointer hover:bg-gray-600"
                          onClick={() => setSelectedNode(node || null)}
                        >
                          {nodeId}. {node?.name.split(' ')[0]}
                        </span>
                      )
                    })}
                  </div>
                </div>
              )
            })}
          </div>

          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <Info className="w-5 h-5 text-indigo-400" />
            Node Types
          </h2>

          <div className="space-y-2">
            {Object.entries(NODE_TYPE_CONFIG).map(([type, config]) => {
              const TypeIcon = config.icon
              const count = nodes.filter(n => n.type === type).length
              return (
                <div key={type} className="flex items-center justify-between py-2">
                  <div className="flex items-center gap-2">
                    <TypeIcon className="w-4 h-4 text-gray-400" />
                    <span className="text-sm">{config.label}</span>
                  </div>
                  <span className="px-2 py-0.5 bg-gray-800 rounded text-xs">{count}</span>
                </div>
              )
            })}
          </div>

          <div className="mt-8 pt-6 border-t border-gray-800">
            <h2 className="text-lg font-semibold mb-4">Graph Statistics</h2>
            <div className="grid grid-cols-2 gap-3">
              <div className="bg-gray-800 rounded-lg p-3 text-center">
                <p className="text-2xl font-bold text-blue-400">{nodes.length}</p>
                <p className="text-xs text-gray-400">Total Nodes</p>
              </div>
              <div className="bg-gray-800 rounded-lg p-3 text-center">
                <p className="text-2xl font-bold text-purple-400">{edges.length}</p>
                <p className="text-xs text-gray-400">Connections</p>
              </div>
              <div className="bg-gray-800 rounded-lg p-3 text-center">
                <p className="text-2xl font-bold text-green-400">{phases.length}</p>
                <p className="text-xs text-gray-400">Phases</p>
              </div>
              <div className="bg-gray-800 rounded-lg p-3 text-center">
                <p className="text-2xl font-bold text-orange-400">
                  {nodes.filter(n => n.type === 'llm').length}
                </p>
                <p className="text-xs text-gray-400">LLM Nodes</p>
              </div>
            </div>
          </div>
        </div>

        {/* Main Graph Area */}
        <div className="flex-1 overflow-auto p-6" style={{ height: 'calc(100vh - 73px)' }}>
          <div className="bg-gray-900 rounded-xl border border-gray-800 p-4 min-h-full">
            {/* Graph Canvas */}
            <div className="overflow-auto" style={{ maxHeight: 'calc(100vh - 180px)' }}>
              <svg
                ref={svgRef}
                width={1400 * zoom}
                height={600 * zoom}
                viewBox="0 0 1400 600"
                className="mx-auto"
              >
                {/* Defs */}
                <defs>
                  <marker
                    id="arrowhead-large"
                    markerWidth="12"
                    markerHeight="8"
                    refX="10"
                    refY="4"
                    orient="auto"
                  >
                    <polygon points="0 0, 12 4, 0 8" fill="#4B5563" />
                  </marker>
                  <filter id="glow-effect">
                    <feGaussianBlur stdDeviation="3" result="coloredBlur" />
                    <feMerge>
                      <feMergeNode in="coloredBlur" />
                      <feMergeNode in="SourceGraphic" />
                    </feMerge>
                  </filter>
                  <linearGradient id="node-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                    <stop offset="0%" stopColor="#374151" />
                    <stop offset="100%" stopColor="#1F2937" />
                  </linearGradient>
                </defs>

                {/* Background Pattern */}
                <pattern id="grid-pattern" width="50" height="50" patternUnits="userSpaceOnUse">
                  <path d="M 50 0 L 0 0 0 50" fill="none" stroke="#1F2937" strokeWidth="1" />
                </pattern>
                <rect width="100%" height="100%" fill="url(#grid-pattern)" />

                {/* Edges */}
                {edges.map((edge, idx) => {
                  const fromPos = getNodePosition(edge.from)
                  const toPos = getNodePosition(edge.to)
                  const dx = toPos.x - fromPos.x
                  const dy = toPos.y - fromPos.y

                  let path = ''
                  if (Math.abs(dy) > 50) {
                    // Different rows - curved path
                    path = `M ${fromPos.x + 90} ${fromPos.y + 50}
                            Q ${fromPos.x + 90} ${fromPos.y + 50 + dy/2}
                              ${toPos.x + 90} ${toPos.y}`
                  } else {
                    // Same row - straight line
                    path = `M ${fromPos.x + 180} ${fromPos.y + 35}
                            L ${toPos.x} ${toPos.y + 35}`
                  }

                  return (
                    <path
                      key={idx}
                      d={path}
                      fill="none"
                      stroke="#4B5563"
                      strokeWidth="2"
                      markerEnd="url(#arrowhead-large)"
                      className="transition-all duration-300"
                    />
                  )
                })}

                {/* Nodes */}
                {nodes.map((node) => {
                  const pos = getNodePosition(node.id)
                  const phaseColor = getPhaseColor(node.phase)
                  const isHovered = hoveredNode === node.id
                  const isSelected = selectedNode?.id === node.id
                  const nodeTypeConfig = getNodeTypeConfig(node.type)

                  return (
                    <g
                      key={node.id}
                      transform={`translate(${pos.x}, ${pos.y})`}
                      onClick={() => setSelectedNode(node)}
                      onMouseEnter={() => setHoveredNode(node.id)}
                      onMouseLeave={() => setHoveredNode(null)}
                      className="cursor-pointer"
                      filter={isSelected ? 'url(#glow-effect)' : undefined}
                    >
                      {/* Node Card */}
                      <rect
                        x="0"
                        y="0"
                        width="180"
                        height="70"
                        rx="10"
                        fill={isHovered || isSelected ? '#374151' : 'url(#node-gradient)'}
                        stroke={isSelected ? phaseColor : '#374151'}
                        strokeWidth={isSelected ? '3' : '2'}
                        className="transition-all duration-200"
                      />

                      {/* Phase Color Bar */}
                      <rect
                        x="0"
                        y="0"
                        width="180"
                        height="8"
                        rx="10"
                        ry="10"
                        fill={phaseColor}
                        clipPath="inset(0 0 50% 0 round 10px)"
                      />

                      {/* Node ID Badge */}
                      <circle
                        cx="20"
                        cy="38"
                        r="16"
                        fill={phaseColor}
                      />
                      <text
                        x="20"
                        y="43"
                        textAnchor="middle"
                        fill="white"
                        fontSize="12"
                        fontWeight="bold"
                      >
                        {node.id}
                      </text>

                      {/* Node Name */}
                      <text
                        x="45"
                        y="32"
                        fill="white"
                        fontSize="11"
                        fontWeight="600"
                      >
                        {node.name.length > 16 ? node.name.slice(0, 16) + '...' : node.name}
                      </text>

                      {/* Node Type */}
                      <text
                        x="45"
                        y="50"
                        fill="#9CA3AF"
                        fontSize="9"
                      >
                        {nodeTypeConfig.label} | {node.phase}
                      </text>

                      {/* Type Icon Badge */}
                      <rect
                        x="150"
                        y="28"
                        width="22"
                        height="22"
                        rx="4"
                        fill="#1F2937"
                      />
                    </g>
                  )
                })}
              </svg>
            </div>
          </div>
        </div>

        {/* Right Sidebar - Node Details */}
        {selectedNode && (
          <div className="w-80 bg-gray-900 border-l border-gray-800 p-6 overflow-y-auto" style={{ height: 'calc(100vh - 73px)' }}>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold">Node Details</h2>
              <button
                onClick={() => setSelectedNode(null)}
                className="text-gray-400 hover:text-white"
              >
                <XCircle className="w-5 h-5" />
              </button>
            </div>

            <div className="bg-gray-800 rounded-lg p-4 mb-4">
              <div className="flex items-center gap-3 mb-3">
                <div
                  className="w-12 h-12 rounded-lg flex items-center justify-center text-white font-bold text-xl"
                  style={{ backgroundColor: getPhaseColor(selectedNode.phase) }}
                >
                  {selectedNode.id}
                </div>
                <div>
                  <h3 className="font-semibold text-lg">{selectedNode.name}</h3>
                  <p className="text-sm text-gray-400">{selectedNode.phase} Phase</p>
                </div>
              </div>
            </div>

            <div className="space-y-4">
              <div>
                <label className="text-sm text-gray-400">Description</label>
                <p className="mt-1 text-sm">{selectedNode.description}</p>
              </div>

              <div>
                <label className="text-sm text-gray-400">Node Type</label>
                <p className="mt-1 flex items-center gap-2">
                  <span className="px-2 py-1 bg-gray-700 rounded text-sm">
                    {getNodeTypeConfig(selectedNode.type).label}
                  </span>
                </p>
              </div>

              <div>
                <label className="text-sm text-gray-400">Phase</label>
                <p className="mt-1 flex items-center gap-2">
                  <span
                    className="px-2 py-1 rounded text-sm"
                    style={{ backgroundColor: getPhaseColor(selectedNode.phase) + '30', color: getPhaseColor(selectedNode.phase) }}
                  >
                    {selectedNode.phase}
                  </span>
                </p>
              </div>

              <div>
                <label className="text-sm text-gray-400">Connections</label>
                <div className="mt-2 space-y-2">
                  {edges.filter(e => e.from === selectedNode.id).map(e => {
                    const targetNode = nodes.find(n => n.id === e.to)
                    return (
                      <div key={e.to} className="flex items-center gap-2 text-sm">
                        <span className="text-gray-400">To:</span>
                        <span className="px-2 py-1 bg-gray-700 rounded">
                          {e.to}. {targetNode?.name}
                        </span>
                      </div>
                    )
                  })}
                  {edges.filter(e => e.to === selectedNode.id).map(e => {
                    const sourceNode = nodes.find(n => n.id === e.from)
                    return (
                      <div key={e.from} className="flex items-center gap-2 text-sm">
                        <span className="text-gray-400">From:</span>
                        <span className="px-2 py-1 bg-gray-700 rounded">
                          {e.from}. {sourceNode?.name}
                        </span>
                      </div>
                    )
                  })}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
