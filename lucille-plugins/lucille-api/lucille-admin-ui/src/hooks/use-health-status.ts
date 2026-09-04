import { useState, useEffect } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"

const API_BASE = import.meta.env.VITE_API_BASE ?? ""

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

    check()
    return () => controller.abort()
  }, [])

  return status
}
