import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type { ReactNode } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"
import { API_BASE, HEALTH_POLL_INTERVAL_MS } from "@/lib/constants"
import { HealthContext } from "./health-context"

/** Consecutive failed probes required before the API is reported unavailable (1 = no debounce). */
const FAILURES_BEFORE_UNAVAILABLE = 1

interface InFlightProbe {
  promise: Promise<HealthStatus>
  /** True when the probe was started in response to a reported request failure. */
  afterFailure: boolean
}

/** Polls API health once for the whole app, flipping to "unavailable" after enough failed probes. */
export function HealthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<HealthStatus>("loading")
  const statusRef = useRef<HealthStatus>("loading")
  const failures = useRef(0)
  const controllerRef = useRef<AbortController | null>(null)
  const mountedRef = useRef(true)
  // Concurrent callers (several widgets failing at once, the poller, Retry) share one probe
  // instead of each firing their own or aborting each other.
  const inFlightRef = useRef<InFlightProbe | null>(null)

  const update = useCallback((next: HealthStatus) => {
    statusRef.current = next
    setStatus(next)
  }, [])

  const probe = useCallback(async (): Promise<HealthStatus> => {
    if (!mountedRef.current) return statusRef.current
    const controller = new AbortController()
    controllerRef.current = controller
    const { signal } = controller

    function recordFailure(): HealthStatus {
      failures.current += 1
      if (failures.current >= FAILURES_BEFORE_UNAVAILABLE) update("unavailable")
      return statusRef.current
    }

    try {
      const liveRes = await fetch(`${API_BASE}/v1/livez`, { signal })
      if (signal.aborted) return statusRef.current
      if (!liveRes.ok) return recordFailure()

      const readyRes = await fetch(`${API_BASE}/v1/readyz`, { signal })
      if (signal.aborted) return statusRef.current
      failures.current = 0
      const next: HealthStatus = readyRes.ok ? "healthy" : "notready"
      update(next)
      return next
    } catch {
      return signal.aborted ? statusRef.current : recordFailure()
    }
  }, [update])

  /** Starts a probe; if one is already running, the new one runs right after it finishes. */
  const startProbe = useCallback((afterFailure: boolean): Promise<HealthStatus> => {
    const previous = inFlightRef.current?.promise
    const promise: Promise<HealthStatus> = (previous ? previous.then(probe, probe) : probe()).finally(() => {
      if (inFlightRef.current?.promise === promise) inFlightRef.current = null
    })
    inFlightRef.current = { promise, afterFailure }
    return promise
  }, [probe])

  const check = useCallback(
    (): Promise<HealthStatus> => inFlightRef.current?.promise ?? startProbe(false),
    [startProbe],
  )

  // A request already failed, so count it as a strike. Only a probe started after the failure
  // can confirm or clear it, so don't reuse one that was already running beforehand.
  const reportFailure = useCallback((): Promise<HealthStatus> => {
    failures.current = Math.max(failures.current, FAILURES_BEFORE_UNAVAILABLE - 1)
    const current = inFlightRef.current
    return current?.afterFailure ? current.promise : startProbe(true)
  }, [startProbe])

  useEffect(() => {
    mountedRef.current = true
    check()
    const id = setInterval(check, HEALTH_POLL_INTERVAL_MS)
    return () => {
      mountedRef.current = false
      controllerRef.current?.abort()
      inFlightRef.current = null
      clearInterval(id)
    }
  }, [check])

  const value = useMemo(
    () => ({ status, recheck: check, reportFailure }),
    [status, check, reportFailure],
  )

  return <HealthContext.Provider value={value}>{children}</HealthContext.Provider>
}
