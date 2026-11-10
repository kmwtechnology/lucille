import { useContext } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"
import { HealthContext, type HealthContextValue } from "./health-context"

/** Full health context (status plus recheck). Must be used under <HealthProvider>. */
export function useHealth(): HealthContextValue {
  const ctx = useContext(HealthContext)
  if (!ctx) throw new Error("useHealth must be used within a HealthProvider")
  return ctx
}

export function useHealthStatus(): HealthStatus {
  return useHealth().status
}
