import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type { ReactNode } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"
import { API_BASE, HEALTH_POLL_INTERVAL_MS } from "@/lib/constants"
import { HealthContext } from "./health-context"

/** Consecutive failed probes required before the API is reported unavailable (1 = no debounce). */
const FAILURES_BEFORE_UNAVAILABLE = 1

/** Polls API health once for the whole app, flipping to "unavailable" after enough failed probes. */
export function HealthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<HealthStatus>("loading")
  const failures = useRef(0)
  const controllerRef = useRef<AbortController | null>(null)

  const check = useCallback(async () => {
    controllerRef.current?.abort()
    const controller = new AbortController()
    controllerRef.current = controller

    function recordFailure() {
      failures.current += 1
      if (failures.current >= FAILURES_BEFORE_UNAVAILABLE) {
        setStatus("unavailable")
      }
    }

    try {
      const liveRes = await fetch(`${API_BASE}/v1/livez`, { signal: controller.signal })
      if (controller.signal.aborted) return
      if (!liveRes.ok) {
        recordFailure()
        return
      }
      const readyRes = await fetch(`${API_BASE}/v1/readyz`, { signal: controller.signal })
      if (controller.signal.aborted) return
      failures.current = 0
      setStatus(readyRes.ok ? "healthy" : "notready")
    } catch {
      if (!controller.signal.aborted) recordFailure()
    }
  }, [])

  useEffect(() => {
    check()
    const id = setInterval(check, HEALTH_POLL_INTERVAL_MS)
    return () => {
      controllerRef.current?.abort()
      clearInterval(id)
    }
  }, [check])

  const reportFailure = useCallback(() => {
    failures.current = Math.max(failures.current, FAILURES_BEFORE_UNAVAILABLE - 1)
    check()
  }, [check])

  const value = useMemo(
    () => ({ status, recheck: check, reportFailure }),
    [status, check, reportFailure]
  )

  return <HealthContext.Provider value={value}>{children}</HealthContext.Provider>
}
