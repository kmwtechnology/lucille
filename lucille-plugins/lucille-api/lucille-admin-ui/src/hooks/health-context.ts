import { createContext } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"

export interface HealthContextValue {
  status: HealthStatus
  /** Triggers an immediate health re-check outside the normal poll interval. Resolves to the new status. */
  recheck: () => Promise<HealthStatus>
  /**
   * Called when a request fails in a way that could mean the API is down (network error or 5xx,
   * including the dev proxy's 500 when the API isn't running). Counts as a failed probe and
   * re-checks immediately. Resolves to the resulting status so the caller can decide whether to
   * show its own error or defer to the site-wide error screen.
   */
  reportFailure: () => Promise<HealthStatus>
}

export const HealthContext = createContext<HealthContextValue | null>(null)
