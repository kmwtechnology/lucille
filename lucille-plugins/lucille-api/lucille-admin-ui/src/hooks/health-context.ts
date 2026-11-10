import { createContext } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"

export interface HealthContextValue {
  status: HealthStatus
  recheck: () => Promise<void>
  reportFailure: () => void
}

export const HealthContext = createContext<HealthContextValue | null>(null)
