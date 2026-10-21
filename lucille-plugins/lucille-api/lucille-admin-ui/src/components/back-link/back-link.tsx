import { ArrowLeft } from "lucide-react"
import { Link } from "@/components/ui/link/link"
import styles from "./back-link.module.css"

/** A ghost-styled "back" link with a left arrow, used at the top of detail pages. */
export function BackLink({ to, label }: { to: string; label: string }) {
  return (
    <Link variant="ghost" size="sm" className={styles.backLink} to={to}>
      <ArrowLeft className="mr-1 h-4 w-4" /> {label}
    </Link>
  )
}
