'use client'

import { useState, useRef, useEffect } from 'react'
import { Send, Bot, User, Sparkles, Loader, AlertCircle } from 'lucide-react'
import { Button } from '@/components/ui/Button'

// API Configuration - PRODUCTION
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant' | 'system'
  content: string
  timestamp: string
  metadata?: {
    confidence?: number
    sources?: string[]
    reasoning?: string
  }
}

interface IncidentChatProps {
  incident_id: string
  incident_context: {
    incident: any
    rag_context: any[]
    graph_context: any
  }
  onSendMessage?: (message: string) => Promise<ChatMessage>
}

// PRODUCTION: Send message to LLM backend
async function sendChatMessage(
  incident_id: string,
  message: string,
  context: any
): Promise<ChatMessage> {
  const response = await fetch(`${API_BASE_URL}/api/v1/chat`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      incident_id,
      message,
      context: {
        rag_results: context.rag_context || [],
        graph_context: context.graph_context || {},
        incident: context.incident || {},
      },
    }),
  })

  if (!response.ok) {
    throw new Error(`Chat API failed: ${response.statusText}`)
  }

  const data = await response.json()
  return {
    id: Date.now().toString(),
    role: 'assistant',
    content: data.response,
    timestamp: new Date().toISOString(),
    metadata: {
      confidence: data.confidence,
      sources: data.sources || ['RAG', 'Graph DB'],
      reasoning: data.reasoning,
    },
  }
}

export function IncidentChat({
  incident_id,
  incident_context,
  onSendMessage
}: IncidentChatProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      id: '1',
      role: 'system',
      content: `I'm your AI assistant for incident ${incident_id}. I have full context including:\n\n• Incident details from ServiceNow\n• ${incident_context.rag_context?.length || 0} similar past incidents from RAG (${incident_context.rag_context?.length > 0 ? incident_context.rag_context.map((r: any) => `${(r.similarity * 100).toFixed(0)}% match`).join(', ') : 'retrieving...'})\n• Graph DB: ${incident_context.graph_context?.related_incidents?.length || 0} related incidents, ${incident_context.graph_context?.affected_services?.length || 0} affected services\n• Current workflow state\n\nHow can I help you analyze or resolve this incident?`,
      timestamp: new Date().toISOString()
    }
  ])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)

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
      // PRODUCTION: Use custom handler or call production API
      if (onSendMessage) {
        const response = await onSendMessage(input)
        setMessages(prev => [...prev, response])
      } else {
        // Call production chat API
        const response = await sendChatMessage(incident_id, input, incident_context)
        setMessages(prev => [...prev, response])
      }
    } catch (error) {
      const errorMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        role: 'system',
        content: `Error: ${error instanceof Error ? error.message : 'Failed to get response'}. Please check if the API is running.`,
        timestamp: new Date().toISOString()
      }
      setMessages(prev => [...prev, errorMessage])
    } finally {
      setIsLoading(false)
    }
  }

  const suggestedQuestions = [
    "What's the root cause of this issue?",
    "How do we fix this?",
    "Which agent should handle this?",
    "Show me similar incidents",
    "What's the expected resolution time?"
  ]

  return (
    <div className="flex flex-col h-full bg-white rounded-lg border border-gray-200 overflow-hidden">
      {/* Header */}
      <div className="flex-shrink-0 bg-gradient-to-r from-blue-600 to-purple-600 text-white p-4">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-full bg-white/20 flex items-center justify-center">
            <Sparkles className="w-6 h-6" />
          </div>
          <div>
            <h3 className="font-semibold">AI Context-Aware Assistant</h3>
            <p className="text-sm text-white/80">
              Full incident context • RAG • Graph DB • Real-time
            </p>
          </div>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((message) => (
          <div
            key={message.id}
            className={`flex gap-3 ${
              message.role === 'user' ? 'flex-row-reverse' : 'flex-row'
            }`}
          >
            {/* Avatar */}
            <div
              className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${
                message.role === 'user'
                  ? 'bg-blue-100'
                  : message.role === 'system'
                  ? 'bg-gray-100'
                  : 'bg-purple-100'
              }`}
            >
              {message.role === 'user' ? (
                <User className="w-5 h-5 text-blue-600" />
              ) : (
                <Bot className="w-5 h-5 text-purple-600" />
              )}
            </div>

            {/* Message Content */}
            <div
              className={`flex-1 max-w-[80%] ${
                message.role === 'user' ? 'text-right' : 'text-left'
              }`}
            >
              <div
                className={`inline-block rounded-lg px-4 py-2 ${
                  message.role === 'user'
                    ? 'bg-blue-600 text-white'
                    : message.role === 'system'
                    ? 'bg-gray-100 text-gray-800'
                    : 'bg-purple-50 text-gray-900 border border-purple-200'
                }`}
              >
                <div className="whitespace-pre-wrap">{message.content}</div>

                {/* Metadata */}
                {message.metadata && message.role === 'assistant' && (
                  <div className="mt-3 pt-3 border-t border-purple-200 space-y-2 text-sm">
                    {message.metadata.confidence && (
                      <div className="flex items-center gap-2">
                        <span className="text-purple-600 font-medium">Confidence:</span>
                        <span className="text-gray-700">
                          {(message.metadata.confidence * 100).toFixed(0)}%
                        </span>
                      </div>
                    )}
                    {message.metadata.sources && (
                      <div className="flex items-center gap-2">
                        <span className="text-purple-600 font-medium">Sources:</span>
                        <span className="text-gray-700">
                          {message.metadata.sources.join(', ')}
                        </span>
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Timestamp */}
              <div className="text-xs text-gray-500 mt-1">
                {new Date(message.timestamp).toLocaleTimeString()}
              </div>
            </div>
          </div>
        ))}

        {/* Loading */}
        {isLoading && (
          <div className="flex gap-3">
            <div className="flex-shrink-0 w-8 h-8 rounded-full bg-purple-100 flex items-center justify-center">
              <Bot className="w-5 h-5 text-purple-600" />
            </div>
            <div className="bg-purple-50 rounded-lg px-4 py-3 border border-purple-200">
              <div className="flex items-center gap-2">
                <Loader className="w-4 h-4 text-purple-600 animate-spin" />
                <span className="text-gray-600">Analyzing with RAG + Graph context...</span>
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Suggested Questions */}
      {messages.length <= 2 && (
        <div className="flex-shrink-0 px-4 py-2 bg-gray-50 border-t border-gray-200">
          <div className="text-xs text-gray-500 mb-2">Suggested questions:</div>
          <div className="flex flex-wrap gap-2">
            {suggestedQuestions.map((question, i) => (
              <button
                key={i}
                onClick={() => setInput(question)}
                className="text-xs bg-white border border-gray-300 rounded-full px-3 py-1 hover:bg-blue-50 hover:border-blue-300 transition-colors"
              >
                {question}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Input */}
      <div className="flex-shrink-0 p-4 border-t border-gray-200 bg-white">
        <div className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSend()}
            placeholder="Ask anything about this incident..."
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            disabled={isLoading}
          />
          <Button
            onClick={handleSend}
            disabled={!input.trim() || isLoading}
            className="bg-blue-600 hover:bg-blue-700 text-white px-6"
          >
            {isLoading ? (
              <Loader className="w-5 h-5 animate-spin" />
            ) : (
              <Send className="w-5 h-5" />
            )}
          </Button>
        </div>
      </div>
    </div>
  )
}
