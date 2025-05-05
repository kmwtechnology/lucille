"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Activity, Database, Home, Play, Settings, Menu, X } from "lucide-react"
import { useState, useEffect } from "react"

export default function Sidebar() {
  const pathname = usePathname()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  
  // Close sidebar when route changes on mobile
  useEffect(() => {
    setSidebarOpen(false)
  }, [pathname])
  
  // Close sidebar when clicking outside on mobile
  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 768) {
        // If screen size becomes desktop, ensure sidebar state is reset
        setSidebarOpen(false)
      }
    }
    
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  const links = [
    { href: "/", label: "Dashboard", icon: Home },
    { href: "/configs", label: "Configurations", icon: Settings },
    { href: "/runs", label: "Runs", icon: Play },
    { href: "/connectors", label: "Connectors", icon: Database },
    { href: "/health", label: "Health", icon: Activity },
  ]

  return (
    <>
      {/* Mobile menu button */}
      <div className="md:hidden fixed top-4 left-4 z-50">
        <Button 
          variant="outline" 
          size="icon" 
          onClick={() => setSidebarOpen(true)}
          className="bg-white shadow-md h-10 w-10"
          aria-label="Open menu"
        >
          <Menu className="h-5 w-5" />
        </Button>
      </div>

      {/* Desktop sidebar */}
      <aside className="hidden md:block w-64 shrink-0 border-r bg-white">
        <div className="flex h-16 items-center border-b px-6 tech-gradient">
          <Link href="/" className="flex items-center gap-2 font-bold text-xl text-white">
            <Database className="h-6 w-6" />
            <span>Lucille Admin</span>
          </Link>
        </div>
        <div className="py-4">
          <nav className="space-y-1 px-3">
            {links.map((link) => {
              const Icon = link.icon
              const isActive = pathname === link.href

              return (
                <Link
                  key={link.href}
                  href={link.href}
                  className={cn(
                    "flex items-center gap-3 rounded-md px-3 py-3 text-sm font-medium",
                    isActive
                      ? "bg-primary-50 text-primary-700"
                      : "text-muted-foreground hover:bg-primary-50 hover:text-primary-600",
                  )}
                >
                  <Icon className="h-5 w-5" />
                  {link.label}
                </Link>
              )
            })}
          </nav>
        </div>
      </aside>

      {/* Mobile sidebar */}
      <div
        className={cn(
          "fixed inset-y-0 left-0 z-40 w-72 bg-white border-r transition-transform duration-200 ease-in-out md:hidden",
          sidebarOpen ? "translate-x-0" : "-translate-x-full",
        )}
      >
        <div className="flex h-16 items-center border-b px-6 tech-gradient">
          <Link href="/" className="flex items-center gap-2 font-bold text-xl text-white">
            <Database className="h-6 w-6" />
            <span>Lucille Admin</span>
          </Link>
          <Button
            variant="ghost"
            size="icon"
            className="ml-auto text-white hover:bg-white/20"
            onClick={() => setSidebarOpen(false)}
            aria-label="Close menu"
          >
            <X className="h-5 w-5" />
          </Button>
        </div>
        <div className="py-4">
          <nav className="space-y-1 px-3">
            {links.map((link) => {
              const Icon = link.icon
              const isActive = pathname === link.href

              return (
                <Link
                  key={link.href}
                  href={link.href}
                  className={cn(
                    "flex items-center gap-3 rounded-md px-4 py-4 text-base font-medium",
                    isActive
                      ? "bg-primary-50 text-primary-700"
                      : "text-muted-foreground hover:bg-primary-50 hover:text-primary-600",
                  )}
                  onClick={() => setSidebarOpen(false)}
                >
                  <Icon className="h-6 w-6" />
                  {link.label}
                </Link>
              )
            })}
          </nav>
        </div>
      </div>

      {/* Backdrop for mobile */}
      {sidebarOpen && (
        <div 
          className="fixed inset-0 z-30 bg-black/50 md:hidden" 
          onClick={() => setSidebarOpen(false)}
          aria-hidden="true"
        />
      )}
    </>
  )
}
