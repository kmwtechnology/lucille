import { NavLink } from "react-router-dom"
import { cn } from "@/lib/utils"
import { Database, ExternalLink } from "lucide-react"
import { NAV_LINKS } from "@/lib/constants"
import styles from "./sidebar.module.css"

export default function Sidebar() {
  return (
    <aside className={styles.sidebar}>
      <div className={styles.brandHeader}>
        <NavLink to="/" className={styles.brandLink}>
          <Database className="h-6 w-6" />
          <span>Lucille Admin</span>
        </NavLink>
      </div>
      <div className={styles.navWrapper}>
        <nav className={styles.nav}>
          {NAV_LINKS.map((link) => {
            const Icon = link.icon

            if ("external" in link && link.external) {
              return (
                <a
                  key={link.href}
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className={styles.docsLink}
                >
                  <Icon className="h-5 w-5" />
                  <span>{link.label}</span>
                  <ExternalLink className={styles.externalIcon} aria-hidden="true" />
                </a>
              )
            }

            return (
              <NavLink
                key={link.href}
                to={link.href}
                end
                className={({ isActive }) => cn(styles.navLink, isActive && styles.navLinkActive)}
              >
                <Icon className="h-5 w-5" />
                {link.label}
              </NavLink>
            )
          })}
        </nav>
      </div>
    </aside>
  )
}
