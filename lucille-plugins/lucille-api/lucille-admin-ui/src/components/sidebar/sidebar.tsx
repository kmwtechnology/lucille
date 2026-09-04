import { NavLink, useLocation } from "react-router-dom"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button/button"
import { Activity, BookOpen, ChevronDown, Database, Home, Layers, Play, Settings, Menu, X, Zap } from "lucide-react"
import { useState, useEffect } from "react"
import styles from "./sidebar.module.css"

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
              className={styles.sectionToggle}
            >
              <div className={styles.sectionToggleInner}>
                <Icon className="h-5 w-5" />
                <span>{link.label}</span>
              </div>
              <ChevronDown
                className={cn(styles.chevron, isExpanded && styles.chevronExpanded)}
              />
            </button>
            {isExpanded && (
              <div className={styles.childrenWrapper}>
                {"children" in link && link.children?.map((child) => (
                  <NavLink
                    key={child.href}
                    to={child.href}
                    end
                    className={({ isActive }) =>
                      cn(styles.navLinkChild, isActive && styles.navLinkActive)
                    }
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
          className={({ isActive }) =>
            cn(styles.navLink, isActive && styles.navLinkActive)
          }
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
      <div className={styles.mobileMenuButton}>
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
      <aside className={styles.desktopSidebar}>
        <div className={styles.brandHeader}>
          <NavLink to="/" className={styles.brandLink}>
            <Database className="h-6 w-6" />
            <span>Lucille Admin</span>
          </NavLink>
        </div>
        <div className={styles.navWrapper}>
          <nav className={styles.nav}>
            {renderNavItems(links)}
          </nav>
        </div>
      </aside>

      {/* Mobile sidebar */}
      <div
        className={cn(
          styles.mobileSidebar,
          sidebarOpen ? styles.mobileSidebarOpen : styles.mobileSidebarClosed,
        )}
      >
        <div className={styles.brandHeader}>
          <NavLink to="/" className={styles.brandLink}>
            <Database className="h-6 w-6" />
            <span>Lucille Admin</span>
          </NavLink>
          <Button
            variant="ghost"
            size="icon"
            className={styles.closeButton}
            onClick={() => setSidebarOpen(false)}
            aria-label="Close menu"
          >
            <X className="h-5 w-5" />
          </Button>
        </div>
        <div className={styles.navWrapper}>
          <nav className={styles.nav}>
            {renderNavItems(links, true)}
          </nav>
        </div>
      </div>

      {/* Backdrop for mobile */}
      {sidebarOpen && (
        <div
          className={styles.backdrop}
          onClick={() => setSidebarOpen(false)}
          aria-hidden="true"
        />
      )}
    </>
  )
}
