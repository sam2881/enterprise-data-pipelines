import { HTMLAttributes, forwardRef } from 'react'
import { cn } from '@/lib/utils'

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: 'default' | 'success' | 'warning' | 'error' | 'info' | 'purple' | 'cyan'
  size?: 'sm' | 'md' | 'lg'
  glow?: boolean
}

export const Badge = forwardRef<HTMLSpanElement, BadgeProps>(
  ({ className, variant = 'default', size = 'md', glow = false, children, ...props }, ref) => {
    const variants = {
      default: `
        bg-gradient-to-r from-gray-500/20 to-slate-500/20
        text-gray-400
        border-gray-500/30
      `,
      success: `
        bg-gradient-to-r from-emerald-500/20 to-green-500/20
        text-emerald-400
        border-emerald-500/30
        ${glow ? 'shadow-lg shadow-emerald-500/20' : ''}
      `,
      warning: `
        bg-gradient-to-r from-amber-500/20 to-orange-500/20
        text-amber-400
        border-amber-500/30
        ${glow ? 'shadow-lg shadow-amber-500/20' : ''}
      `,
      error: `
        bg-gradient-to-r from-red-500/20 to-rose-500/20
        text-red-400
        border-red-500/30
        ${glow ? 'shadow-lg shadow-red-500/20' : ''}
      `,
      info: `
        bg-gradient-to-r from-blue-500/20 to-cyan-500/20
        text-blue-400
        border-blue-500/30
        ${glow ? 'shadow-lg shadow-blue-500/20' : ''}
      `,
      purple: `
        bg-gradient-to-r from-purple-500/20 to-violet-500/20
        text-purple-400
        border-purple-500/30
        ${glow ? 'shadow-lg shadow-purple-500/20' : ''}
      `,
      cyan: `
        bg-gradient-to-r from-cyan-500/20 to-teal-500/20
        text-cyan-400
        border-cyan-500/30
        ${glow ? 'shadow-lg shadow-cyan-500/20' : ''}
      `,
    }

    const sizes = {
      sm: 'px-2 py-0.5 text-[10px]',
      md: 'px-2.5 py-1 text-xs',
      lg: 'px-3 py-1.5 text-sm',
    }

    return (
      <span
        ref={ref}
        className={cn(
          'inline-flex items-center rounded-full border font-semibold transition-all backdrop-blur-sm',
          variants[variant],
          sizes[size],
          className
        )}
        {...props}
      >
        {children}
      </span>
    )
  }
)

Badge.displayName = 'Badge'
