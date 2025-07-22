"use client"

import { cn } from "@/lib/utils"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { ReactNode, useState } from "react"
import { ChevronDown, ChevronRight, FileText } from "lucide-react"

interface SidebarItemProps {
  href: string
  label: string
  icon?: ReactNode
  isActive?: boolean
  isChild?: boolean
  hasChildren?: boolean
  isExpanded?: boolean
  onToggle?: () => void
}

export function SidebarItem({
  href,
  label,
  icon,
  isActive = false,
  isChild = false,
  hasChildren = false,
  isExpanded = false,
  onToggle,
}: SidebarItemProps) {
  const pathname = usePathname()
  const active = isActive || pathname === href
  const Icon = icon || FileText

  const content = (
    <div
      className={cn(
        "flex items-center gap-3 rounded-md px-3 py-3 text-sm font-medium transition-colors",
        isChild ? "pl-9" : "",
        active
          ? "bg-primary-50 text-primary-700"
          : "text-muted-foreground hover:bg-primary-50 hover:text-primary-600"
      )}
    >
      {hasChildren ? (
        <button
          type="button"
          className="flex items-center gap-2 w-full text-left"
          onClick={(e) => {
            e.preventDefault()
            e.stopPropagation()
            onToggle?.()
          }}
        >
          <div className="flex items-center gap-2">
            <Icon className="h-5 w-5 flex-shrink-0" />
            <span>{label}</span>
          </div>
          {isExpanded ? (
            <ChevronDown className="ml-auto h-4 w-4 flex-shrink-0" />
          ) : (
            <ChevronRight className="ml-auto h-4 w-4 flex-shrink-0" />
          )}
        </button>
      ) : (
        <>
          <Icon className="h-5 w-5 flex-shrink-0" />
          <span>{label}</span>
        </>
      )}
    </div>
  )

  if (hasChildren) {
    return <div className="w-full">{content}</div>
  }

  return (
    <Link href={href} className="block w-full">
      {content}
    </Link>
  )
}
