import { useState, useEffect, useRef, useContext } from "react"
import { errorMessageFrom } from "@/lib/utils"
import { API_BASE } from "@/lib/constants"
import { authHeader, notifyUnauthorized } from "@/lib/auth-token"
import { HealthContext } from "./health-context"

export type FetchState<T> =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "success"; data: T }

export interface UseFetchOptions<T> {
  pollIntervalMs?: number
  stopPollingWhen?: (data: T) => boolean
  reloadKey?: number
}

/** Thrown for failures that could mean the API itself is down (network error or 5xx). */
class PossibleOutageError extends Error {}

export function useFetch<T>(path: string, options: UseFetchOptions<T> = {}): FetchState<T> {
  const { pollIntervalMs, stopPollingWhen, reloadKey } = options
  const [state, setState] = useState<FetchState<T>>({ status: "loading" })

  const stopPollingWhenRef = useRef(stopPollingWhen)
  stopPollingWhenRef.current = stopPollingWhen

  const health = useContext(HealthContext)
  const reportFailureRef = useRef(health?.reportFailure)
  reportFailureRef.current = health?.reportFailure

  useEffect(() => {
    const controller = new AbortController()
    let intervalId: ReturnType<typeof setInterval> | undefined

    async function load() {
      try {
        let res: Response
        try {
          res = await fetch(`${API_BASE}${path}`, { signal: controller.signal, headers: authHeader() })
        } catch (err) {
          // fetch rejects with a TypeError on network failure (e.g. connection refused).
          throw new PossibleOutageError(err instanceof Error ? err.message : "Network error")
        }
        if (controller.signal.aborted) return
        if (res.status === 401) notifyUnauthorized()
        if (!res.ok) {
          const message = await errorMessageFrom(res)
          // 5xx includes the Vite dev proxy's 500 when the API isn't running.
          throw res.status >= 500 ? new PossibleOutageError(message) : new Error(message)
        }
        const data: T = await res.json()
        if (controller.signal.aborted) return

        setState({ status: "success", data })

        const reachedTerminalState = stopPollingWhenRef.current?.(data) ?? false
        if (intervalId && reachedTerminalState) {
          clearInterval(intervalId)
          intervalId = undefined
        }
      } catch (err) {
        if (controller.signal.aborted) return

        // Hold possible-outage errors until a health probe answers. If the API is down, the
        // site-wide error screen takes over and this widget never shows its own error.
        const reportFailure = reportFailureRef.current
        if (err instanceof PossibleOutageError && reportFailure) {
          const healthStatus = await reportFailure()
          if (controller.signal.aborted || healthStatus === "unavailable") return
        }

        setState({
          status: "error",
          error: err instanceof Error ? err.message : "Unknown error",
        })
      }
    }

    load()

    if (pollIntervalMs) {
      intervalId = setInterval(load, pollIntervalMs)
    }

    return () => {
      controller.abort()
      if (intervalId) clearInterval(intervalId)
    }
  }, [path, pollIntervalMs, reloadKey])

  return state
}
