import { useState, useEffect, useRef } from "react"
import { errorMessageFrom } from "@/lib/utils"
import { API_BASE } from "@/lib/constants"

export type FetchState<T> =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "success"; data: T }

export interface UseFetchOptions<T> {
  pollIntervalMs?: number
  stopPollingWhen?: (data: T) => boolean
  reloadKey?: number
}

export function useFetch<T>(path: string, options: UseFetchOptions<T> = {}): FetchState<T> {
  const { pollIntervalMs, stopPollingWhen, reloadKey } = options
  const [state, setState] = useState<FetchState<T>>({ status: "loading" })

  const stopPollingWhenRef = useRef(stopPollingWhen)
  stopPollingWhenRef.current = stopPollingWhen

  useEffect(() => {
    const controller = new AbortController()
    let intervalId: ReturnType<typeof setInterval> | undefined

    async function load() {
      try {
        const res = await fetch(`${API_BASE}${path}`, { signal: controller.signal })
        if (controller.signal.aborted) return
        if (!res.ok) throw new Error(await errorMessageFrom(res))
        const data: T = await res.json()
        if (controller.signal.aborted) return

        setState({ status: "success", data })

        const reachedTerminalState = stopPollingWhenRef.current?.(data) ?? false
        if (intervalId && reachedTerminalState) {
          clearInterval(intervalId)
          intervalId = undefined
        }
      } catch (err) {
        if (!controller.signal.aborted) {
          setState({
            status: "error",
            error: err instanceof Error ? err.message : "Unknown error",
          })
        }
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
