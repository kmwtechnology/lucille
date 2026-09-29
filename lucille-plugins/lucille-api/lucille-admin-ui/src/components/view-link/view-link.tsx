import { ArrowRight } from "lucide-react"
import { Link } from "@/components/ui/link/link"
import styles from "./view-link.module.css"

/** Full-width ghost link with a trailing arrow, used in card footers ("View", "Manage Configs"). */
export function ViewLink({ to, label }: { to: string; label: string }) {
  return (
    <Link variant="ghost" size="sm" className={styles.viewLink} to={to}>
      {label} <ArrowRight className="ml-1 h-3 w-3 sm:h-4 sm:w-4" />
    </Link>
  )
}
