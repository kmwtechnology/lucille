import { useState, useEffect } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"
import { API_BASE, HEALTH_POLL_INTERVAL_MS } from "@/lib/constants"

export function useHealthStatus(): HealthStatus {
  const [status, setStatus] = useState<HealthStatus>("loading")

  useEffect(() => {
    const controller = new AbortController()

    async function check() {
      try {
        const liveRes = await fetch(`${API_BASE}/v1/livez`, { signal: controller.signal })
        if (!liveRes.ok) {
          setStatus("unavailable")
          return
        }
        const readyRes = await fetch(`${API_BASE}/v1/readyz`, { signal: controller.signal })
        setStatus(readyRes.ok ? "healthy" : "notready")
      } catch {
        if (!controller.signal.aborted) {
          setStatus("unavailable")
        }
      }
    }

    // Re-check on an interval so the status reflects the API going down after mount.
    check()
    const id = setInterval(check, HEALTH_POLL_INTERVAL_MS)
    return () => {
      controller.abort()
      clearInterval(id)
    }
  }, [])

  return status
}
