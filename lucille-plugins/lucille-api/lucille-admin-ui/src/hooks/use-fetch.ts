import { useState, useEffect } from "react"

export type FetchState<T> =
  | { status: "loading" }
  | { status: "error"; error: string }
  | { status: "success"; data: T }

const API_BASE = import.meta.env.VITE_API_BASE ?? ""

export function useFetch<T>(path: string, interval?: number): FetchState<T> {
  const [state, setState] = useState<FetchState<T>>({ status: "loading" })

  useEffect(() => {
    const controller = new AbortController()

    async function load() {
      try {
        const res = await fetch(`${API_BASE}${path}`, { signal: controller.signal })
        if (controller.signal.aborted) return
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
        const data: T = await res.json()
        if (!controller.signal.aborted) {
          setState({ status: "success", data })
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

    const id = interval ? setInterval(load, interval) : undefined

    return () => {
      controller.abort()
      if (id) clearInterval(id)
    }
  }, [path, interval])

  return state
}
