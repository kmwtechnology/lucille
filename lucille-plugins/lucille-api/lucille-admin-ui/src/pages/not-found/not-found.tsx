import { FileQuestion, Home } from "lucide-react"
import { Link } from "@/components/ui/link/link"
import { EmptyState } from "@/components/empty-state/empty-state"
import styles from "./not-found.module.css"

/** Catch-all page shown for unknown or malformed routes (client-side 404). */
export default function NotFound() {
  return (
    <div className={styles.page}>
      <EmptyState
        icon={FileQuestion}
        title="Page not found"
        subtext="The page you're looking for doesn't exist or may have been moved."
        action={
          <Link variant="default" size="default" to="/">
            <Home className="mr-2 h-4 w-4" /> Back to Dashboard
          </Link>
        }
      />
    </div>
  )
}
