'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { cn } from '@/lib/utils'
import {
  LayoutDashboard,
  AlertCircle,
  CheckCircle,
  Bot,
  Activity,
  Settings,
  Ticket,
  ChevronRight,
  Clock,
  ListChecks,
  GitPullRequest,
  BarChart3,
  Cpu,
  Workflow,
  Sparkles,
  Shield,
  Zap,
  Database,
  Globe,
} from 'lucide-react'
import { useState, useEffect } from 'react'

// =============================================================================
// Navigation Configuration
// =============================================================================

interface NavItem {
  name: string
  href: string
  icon: React.ComponentType<{ className?: string }>
  badge?: number
}

interface NavSection {
  id: string
  title: string
  icon: React.ComponentType<{ className?: string }>
  gradient: string
  items: NavItem[]
}

const navSections: NavSection[] = [
  {
    id: 'incidents',
    title: 'Incident Management',
    icon: Shield,
    gradient: 'from-red-500 to-orange-500',
    items: [
      { name: 'All Incidents', href: '/incidents', icon: Zap },
      { name: 'Pending Approvals', href: '/approvals', icon: Clock },
      { name: 'Active Workflows', href: '/workflows', icon: Workflow },
      { name: 'Resolved', href: '/incidents?filter=resolved', icon: CheckCircle },
    ],
  },
  {
    id: 'jira',
    title: 'Jira Integration',
    icon: Ticket,
    gradient: 'from-blue-500 to-cyan-500',
    items: [
      { name: 'All Tickets', href: '/jira', icon: Ticket },
      { name: 'My Assignments', href: '/jira?filter=assigned', icon: ListChecks },
      { name: 'Pull Requests', href: '/jira?filter=pr', icon: GitPullRequest },
      { name: 'Reports', href: '/jira?filter=reports', icon: BarChart3 },
    ],
  },
]

// =============================================================================
// Sidebar Component
// =============================================================================

export function Sidebar() {
  const pathname = usePathname()
  const [expandedSections, setExpandedSections] = useState<string[]>(['incidents'])
  const [isHovered, setIsHovered] = useState(false)

  useEffect(() => {
    if (pathname?.startsWith('/incidents') || pathname?.startsWith('/workflows') || pathname?.startsWith('/approvals')) {
      if (!expandedSections.includes('incidents')) {
        setExpandedSections(prev => [...prev, 'incidents'])
      }
    }
    if (pathname?.startsWith('/jira')) {
      if (!expandedSections.includes('jira')) {
        setExpandedSections(prev => [...prev, 'jira'])
      }
    }
  }, [pathname])

  const toggleSection = (sectionId: string) => {
    setExpandedSections(prev =>
      prev.includes(sectionId)
        ? prev.filter(id => id !== sectionId)
        : [...prev, sectionId]
    )
  }

  return (
    <aside
      className="flex h-screen w-[280px] flex-col bg-[#0a0f1a]/95 backdrop-blur-xl border-r border-white/5"
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* Logo Section */}
      <div className="flex items-center gap-4 px-6 h-20 border-b border-white/5">
        <Link href="/" className="flex items-center gap-4 group">
          <div className="relative">
            <div className="w-11 h-11 rounded-2xl bg-gradient-to-br from-blue-500 via-purple-500 to-pink-500 p-[2px] group-hover:shadow-lg group-hover:shadow-purple-500/25 transition-all duration-300">
              <div className="w-full h-full rounded-[14px] bg-[#0a0f1a] flex items-center justify-center">
                <Cpu className="w-5 h-5 text-white" />
              </div>
            </div>
            <div className="absolute -bottom-0.5 -right-0.5 w-3.5 h-3.5 rounded-full bg-emerald-500 border-2 border-[#0a0f1a]" />
          </div>
          <div>
            <h1 className="text-base font-bold text-white tracking-tight">AI Agent</h1>
            <p className="text-[11px] text-gray-500 font-medium">Platform v6.0</p>
          </div>
        </Link>
      </div>

      {/* Dashboard Link */}
      <div className="px-4 pt-6 pb-2">
        <Link
          href="/"
          className={cn(
            'group flex items-center gap-3 px-4 py-3 rounded-2xl transition-all duration-300',
            pathname === '/'
              ? 'bg-gradient-to-r from-blue-500/15 to-purple-500/15 border border-white/10'
              : 'hover:bg-white/5'
          )}
        >
          <div className={cn(
            'w-10 h-10 rounded-xl flex items-center justify-center transition-all duration-300',
            pathname === '/'
              ? 'bg-gradient-to-br from-blue-500 to-purple-500 shadow-lg shadow-blue-500/25'
              : 'bg-white/5 group-hover:bg-white/10'
          )}>
            <LayoutDashboard className={cn(
              'w-5 h-5 transition-colors',
              pathname === '/' ? 'text-white' : 'text-gray-400 group-hover:text-white'
            )} />
          </div>
          <div>
            <span className={cn(
              'text-sm font-semibold transition-colors',
              pathname === '/' ? 'text-white' : 'text-gray-400 group-hover:text-white'
            )}>Dashboard</span>
            <p className="text-[11px] text-gray-600">Analytics & Overview</p>
          </div>
        </Link>
      </div>

      {/* Navigation Sections */}
      <nav className="flex-1 px-4 py-4 overflow-y-auto space-y-6">
        {navSections.map((section) => {
          const SectionIcon = section.icon
          const isExpanded = expandedSections.includes(section.id)
          const isActive = section.items.some(item =>
            pathname === item.href || pathname?.startsWith(item.href.split('?')[0] + '/')
          )

          return (
            <div key={section.id} className="space-y-2">
              {/* Section Header */}
              <button
                onClick={() => toggleSection(section.id)}
                className={cn(
                  'w-full group flex items-center gap-3 px-4 py-3 rounded-2xl transition-all duration-300',
                  isActive || isExpanded
                    ? 'bg-white/5 border border-white/5'
                    : 'hover:bg-white/5'
                )}
              >
                <div className={cn(
                  'w-10 h-10 rounded-xl flex items-center justify-center transition-all duration-300',
                  isActive
                    ? `bg-gradient-to-br ${section.gradient} shadow-lg`
                    : 'bg-white/5 group-hover:bg-white/10'
                )}>
                  <SectionIcon className={cn(
                    'w-5 h-5 transition-colors',
                    isActive ? 'text-white' : 'text-gray-400 group-hover:text-white'
                  )} />
                </div>
                <div className="flex-1 text-left">
                  <span className={cn(
                    'text-sm font-semibold transition-colors',
                    isActive ? 'text-white' : 'text-gray-400 group-hover:text-white'
                  )}>{section.title}</span>
                </div>
                <ChevronRight className={cn(
                  'w-4 h-4 text-gray-500 transition-all duration-300',
                  isExpanded && 'rotate-90'
                )} />
              </button>

              {/* Section Items */}
              <div className={cn(
                'overflow-hidden transition-all duration-300 ease-out',
                isExpanded ? 'max-h-[400px] opacity-100' : 'max-h-0 opacity-0'
              )}>
                <div className="pl-6 space-y-1 pt-1">
                  {section.items.map((item) => {
                    const ItemIcon = item.icon
                    const isItemActive = pathname === item.href ||
                      (pathname === item.href.split('?')[0] && item.href.includes('?'))

                    return (
                      <Link
                        key={item.href}
                        href={item.href}
                        className={cn(
                          'group flex items-center gap-3 px-4 py-2.5 rounded-xl transition-all duration-200',
                          isItemActive
                            ? 'bg-white/10 text-white'
                            : 'text-gray-500 hover:text-gray-300 hover:bg-white/5'
                        )}
                      >
                        <ItemIcon className={cn(
                          'w-4 h-4 transition-colors',
                          isItemActive ? 'text-white' : 'text-gray-500 group-hover:text-gray-300'
                        )} />
                        <span className="text-[13px] font-medium">{item.name}</span>
                        {item.badge !== undefined && item.badge > 0 && (
                          <span className={cn(
                            'ml-auto px-2 py-0.5 rounded-full text-[10px] font-bold',
                            `bg-gradient-to-r ${section.gradient} text-white`
                          )}>
                            {item.badge}
                          </span>
                        )}
                      </Link>
                    )
                  })}
                </div>
              </div>
            </div>
          )
        })}
      </nav>

      {/* Bottom Section */}
      <div className="p-4 border-t border-white/5 space-y-4">
        {/* Settings Link */}
        <Link
          href="/settings"
          className={cn(
            'group flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200',
            pathname === '/settings'
              ? 'bg-white/10 text-white'
              : 'text-gray-500 hover:text-white hover:bg-white/5'
          )}
        >
          <Settings className="w-5 h-5" />
          <span className="text-sm font-medium">Settings</span>
        </Link>

        {/* System Status */}
        <div className="p-4 rounded-2xl bg-gradient-to-br from-white/5 to-white/[0.02] border border-white/5">
          <div className="flex items-center justify-between mb-4">
            <span className="text-[11px] font-semibold text-gray-400 uppercase tracking-wider">System</span>
            <div className="flex items-center gap-2">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
              </span>
              <span className="text-[11px] font-semibold text-emerald-400">Online</span>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            {[
              { icon: Workflow, label: 'LangGraph', color: 'text-blue-400' },
              { icon: Activity, label: 'Kafka', color: 'text-emerald-400' },
              { icon: Database, label: 'RAG', color: 'text-purple-400' },
              { icon: Globe, label: 'MCP', color: 'text-amber-400' },
            ].map((item) => (
              <div key={item.label} className="flex items-center gap-2">
                <item.icon className={cn('w-3.5 h-3.5', item.color)} />
                <span className="text-[11px] text-gray-500">{item.label}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </aside>
  )
}
