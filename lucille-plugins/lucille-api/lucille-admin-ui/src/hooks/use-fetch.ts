import { useState, useEffect, useRef } from "react"

export type FetchState<T> =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "success"; data: T }

const API_BASE = import.meta.env.VITE_API_BASE ?? ""

// Surface the API error body's `message`, never the raw body; fall back to the status line.
async function errorMessageFrom(res: Response): Promise<string> {
  const statusLine = `${res.status} ${res.statusText}`.trim() || "Request failed"

  const isJson = res.headers.get("content-type")?.includes("application/json")
  if (!isJson) return statusLine

  const body: unknown = await res.json().catch(() => null)
  if (body && typeof body === "object" && typeof (body as Record<string, unknown>).message === "string") {
    return (body as { message: string }).message
  }
  return statusLine
}

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
