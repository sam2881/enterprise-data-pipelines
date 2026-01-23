import { ButtonHTMLAttributes, forwardRef } from 'react'
import { cn } from '@/lib/utils'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'outline' | 'ghost' | 'danger' | 'success'
  size?: 'sm' | 'md' | 'lg'
  isLoading?: boolean
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = 'primary', size = 'md', isLoading, disabled, children, ...props }, ref) => {
    const baseStyles = `
      inline-flex items-center justify-center font-semibold
      transition-all duration-200
      focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-[#0a0f1a]
      disabled:pointer-events-none disabled:opacity-50
    `

    const variants = {
      primary: `
        bg-gradient-to-r from-blue-500 to-cyan-500
        hover:from-blue-400 hover:to-cyan-400
        text-white
        shadow-lg shadow-blue-500/25
        hover:shadow-xl hover:shadow-blue-500/30
        focus-visible:ring-blue-500
        rounded-xl
      `,
      secondary: `
        bg-white/10
        hover:bg-white/15
        text-white
        border border-white/10
        hover:border-white/20
        focus-visible:ring-white/50
        rounded-xl
      `,
      outline: `
        bg-transparent
        border border-white/20
        hover:bg-white/5
        hover:border-white/30
        text-white
        focus-visible:ring-white/50
        rounded-xl
      `,
      ghost: `
        bg-transparent
        hover:bg-white/10
        text-gray-400
        hover:text-white
        focus-visible:ring-white/50
        rounded-xl
      `,
      danger: `
        bg-gradient-to-r from-red-500 to-rose-500
        hover:from-red-400 hover:to-rose-400
        text-white
        shadow-lg shadow-red-500/25
        hover:shadow-xl hover:shadow-red-500/30
        focus-visible:ring-red-500
        rounded-xl
      `,
      success: `
        bg-gradient-to-r from-emerald-500 to-green-500
        hover:from-emerald-400 hover:to-green-400
        text-white
        shadow-lg shadow-emerald-500/25
        hover:shadow-xl hover:shadow-emerald-500/30
        focus-visible:ring-emerald-500
        rounded-xl
      `,
    }

    const sizes = {
      sm: 'h-8 px-3 text-xs gap-1.5',
      md: 'h-10 px-4 text-sm gap-2',
      lg: 'h-12 px-6 text-base gap-2.5',
    }

    return (
      <button
        ref={ref}
        className={cn(baseStyles, variants[variant], sizes[size], className)}
        disabled={disabled || isLoading}
        {...props}
      >
        {isLoading && (
          <svg
            className="h-4 w-4 animate-spin"
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            />
          </svg>
        )}
        {children}
      </button>
    )
  }
)

Button.displayName = 'Button'
