import { NavLink, useLocation } from "react-router-dom"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Activity, BookOpen, ChevronDown, Database, Home, Layers, Play, Settings, Menu, X, Zap } from "lucide-react"
import { useState, useEffect } from "react"

const techGradient = "linear-gradient(135deg, #0fb6c6, #0094ff)"

export default function Sidebar() {
  const { pathname } = useLocation()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    documentation: true
  })

  // Close sidebar when route changes on mobile
  useEffect(() => {
    setSidebarOpen(false)
  }, [pathname])

  // Close sidebar on resize to desktop
  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 768) {
        setSidebarOpen(false)
      }
    }
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  const toggleSection = (section: string) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }))
  }

  const links = [
    { href: "/", label: "Dashboard", icon: Home },
    { href: "/configs", label: "Configurations", icon: Settings },
    { href: "/runs", label: "Runs", icon: Play },
    {
      href: "#",
      label: "Documentation",
      icon: BookOpen,
      children: [
        { href: "/configs/connectors", label: "Connectors", icon: Zap },
        { href: "/configs/stages", label: "Pipeline Stages", icon: Layers },
        { href: "/configs/indexers", label: "Indexers", icon: Activity },
      ]
    }
  ] as const

  const renderNavItems = (items: typeof links, isMobile = false) => {
    return items.map((link) => {
      const Icon = link.icon
      const hasChildren = "children" in link && link.children && link.children.length > 0
      const isExpanded = expandedSections[link.label.toLowerCase()] ?? true

      if (hasChildren) {
        return (
          <div key={link.label} className="space-y-1">
            <button
              onClick={() => toggleSection(link.label.toLowerCase())}
              className={cn(
                "flex items-center justify-between w-full text-left rounded-md px-3 py-3 text-sm font-medium",
                "text-muted-foreground hover:bg-primary-50 hover:text-primary-600"
              )}
            >
              <div className="flex items-center gap-3">
                <Icon className="h-5 w-5" />
                <span>{link.label}</span>
              </div>
              <ChevronDown
                className={cn(
                  "h-4 w-4 transition-transform duration-200",
                  isExpanded ? "rotate-180" : ""
                )}
              />
            </button>
            {isExpanded && (
              <div className="ml-6 space-y-1">
                {"children" in link && link.children?.map((child) => (
                  <NavLink
                    key={child.href}
                    to={child.href}
                    end
                    className={({ isActive }) => cn(
                      "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium",
                      isActive
                        ? "bg-primary-50 text-primary-700"
                        : "text-muted-foreground hover:bg-primary-50 hover:text-primary-600",
                    )}
                    onClick={() => isMobile && setSidebarOpen(false)}
                  >
                    <child.icon className="h-4 w-4" />
                    {child.label}
                  </NavLink>
                ))}
              </div>
            )}
          </div>
        )
      }

      return (
        <NavLink
          key={link.href}
          to={link.href}
          end
          className={({ isActive }) => cn(
            "flex items-center gap-3 rounded-md px-3 py-3 text-sm font-medium",
            isActive
              ? "bg-primary-50 text-primary-700"
              : "text-muted-foreground hover:bg-primary-50 hover:text-primary-600",
          )}
          onClick={() => isMobile && setSidebarOpen(false)}
        >
          <Icon className="h-5 w-5" />
          {link.label}
        </NavLink>
      )
    })
  }

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
        <div className="flex h-16 items-center border-b px-6" style={{ background: techGradient }}>
          <NavLink to="/" className="flex items-center gap-2 font-bold text-xl text-white">
            <Database className="h-6 w-6" />
            <span>Lucille Admin</span>
          </NavLink>
        </div>
        <div className="py-4">
          <nav className="space-y-1 px-3">
            {renderNavItems(links)}
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
        <div className="flex h-16 items-center border-b px-6" style={{ background: techGradient }}>
          <NavLink to="/" className="flex items-center gap-2 font-bold text-xl text-white">
            <Database className="h-6 w-6" />
            <span>Lucille Admin</span>
          </NavLink>
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
            {renderNavItems(links, true)}
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
