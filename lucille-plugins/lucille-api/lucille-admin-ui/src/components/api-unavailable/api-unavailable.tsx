import { useState } from "react"
import { AlertTriangle, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button/button"
import { useHealth } from "@/hooks/use-health-status"
import styles from "./api-unavailable.module.css"

const MIN_RETRY_FEEDBACK_MS = 800

/** Single site-wide error screen shown when the Lucille API is unreachable. */
export function ApiUnavailable() {
  const { recheck } = useHealth()
  const [retrying, setRetrying] = useState(false)

  async function handleRetry() {
    setRetrying(true)
    try {
      await Promise.all([recheck(), new Promise((resolve) => setTimeout(resolve, MIN_RETRY_FEEDBACK_MS))])
    } finally {
      setRetrying(false)
    }
  }

  return (
    <div className={styles.container} role="alert">
      <div className={styles.content}>
        <AlertTriangle className={styles.icon} aria-hidden="true" />
        <h1 className={styles.title}>Can't reach the Lucille API</h1>
        <p className={styles.message}>
          The Lucille API server is unavailable. This page will recover automatically once it is
          back. See the Documentation link in the sidebar for help getting it running.
        </p>
        <div className={styles.actions}>
          <Button variant="default" onClick={handleRetry} disabled={retrying} aria-busy={retrying}>
            {retrying && <Loader2 className="mr-2 h-4 w-4 animate-spin" aria-hidden="true" />}
            {retrying ? "Retrying..." : "Retry"}
          </Button>
        </div>
      </div>
    </div>
  )
}
