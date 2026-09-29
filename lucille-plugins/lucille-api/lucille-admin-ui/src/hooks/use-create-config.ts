import { useState } from "react"
import { api } from "@/lib/api"

export type CreateConfigState =
  | { status: "idle" }
  | { status: "submitting" }
  | { status: "success"; configId: string }
  | { status: "error"; error: string }

export interface UseCreateConfig {
  state: CreateConfigState
  /** POST the given config object. Returns the new configId, or null on failure. */
  submit: (config: unknown) => Promise<string | null>
  /** Reset back to the idle state (e.g. to clear a previous error). */
  reset: () => void
}

/**
 * Hook for creating a config via POST /v1/config. Unlike useFetch, the request
 * is triggered manually via submit() rather than on mount.
 */
export function useCreateConfig(): UseCreateConfig {
  const [state, setState] = useState<CreateConfigState>({ status: "idle" })

  async function submit(config: unknown): Promise<string | null> {
    setState({ status: "submitting" })
    try {
      const configId = await api.postConfig(config)
      setState({ status: "success", configId })
      return configId
    } catch (err) {
      setState({
        status: "error",
        error: err instanceof Error ? err.message : "Unknown error",
      })
      return null
    }
  }

  function reset() {
    setState({ status: "idle" })
  }

  return { state, submit, reset }
}
