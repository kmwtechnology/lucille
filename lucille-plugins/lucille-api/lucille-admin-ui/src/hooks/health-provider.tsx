import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type { ReactNode } from "react"
import type { HealthStatus } from "@/pages/dashboard/dashboard.types"
import { API_BASE, HEALTH_POLL_INTERVAL_MS, HEALTH_PROBE_TIMEOUT_MS } from "@/lib/constants"
import { HealthContext } from "./health-context"

/** Consecutive failed probes required before the API is reported unavailable (1 = no debounce). */
const FAILURES_BEFORE_UNAVAILABLE = 2

interface InFlightProbe {
  promise: Promise<HealthStatus>
  afterFailure: boolean
}

/** Polls API health once for the whole app, flipping to "unavailable" after enough failed probes. */
export function HealthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<HealthStatus>("loading")
  const statusRef = useRef<HealthStatus>("loading")
  const failures = useRef(0)
  const controllerRef = useRef<AbortController | null>(null)
  const mountedRef = useRef(true)
  const inFlightRef = useRef<InFlightProbe | null>(null)
  const failureSeqRef = useRef(0)

  const update = useCallback((next: HealthStatus) => {
    statusRef.current = next
    setStatus(next)
  }, [])

  const probe = useCallback(async (): Promise<HealthStatus> => {
    if (!mountedRef.current) return statusRef.current
    const controller = new AbortController()
    controllerRef.current = controller
    const { signal } = controller
    const startSeq = failureSeqRef.current

    // A hung API never answers, so abort the probe after a deadline and count it as a failure.
    // The flag separates a timeout (failure) from an unmount abort (silently cancelled).
    let timedOut = false
    const timeoutId = setTimeout(() => {
      timedOut = true
      controller.abort()
    }, HEALTH_PROBE_TIMEOUT_MS)
    const cancelled = () => signal.aborted && !timedOut

    function recordFailure(): HealthStatus {
      failures.current += 1
      if (failures.current >= FAILURES_BEFORE_UNAVAILABLE) update("unavailable")
      return statusRef.current
    }

    try {
      const liveRes = await fetch(`${API_BASE}/v1/livez`, { signal })
      if (cancelled()) return statusRef.current
      if (!liveRes.ok) return recordFailure()

      const readyRes = await fetch(`${API_BASE}/v1/readyz`, { signal })
      if (cancelled()) return statusRef.current
      if (failureSeqRef.current !== startSeq) return statusRef.current
      failures.current = 0
      const next: HealthStatus = readyRes.ok ? "healthy" : "notready"
      update(next)
      return next
    } catch {
      return cancelled() ? statusRef.current : recordFailure()
    } finally {
      clearTimeout(timeoutId)
    }
  }, [update])

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


  const reportFailure = useCallback((): Promise<HealthStatus> => {
    failures.current = Math.max(failures.current, FAILURES_BEFORE_UNAVAILABLE - 1)
    failureSeqRef.current += 1
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
