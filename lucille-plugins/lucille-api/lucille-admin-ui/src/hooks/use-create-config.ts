import { useState } from "react"
import { api } from "@/lib/api"

export type CreateConfigState =
  | { status: "idle" }
  | { status: "submitting" }
  | { status: "success"; configId: string }
  | { status: "error"; error: string }

export interface UseCreateConfig {
  state: CreateConfigState
  submit: (body: string) => Promise<string | null>
  reset: () => void
}

/**
 * Hook for creating a config via POST /v1/config. Unlike useFetch, the request
 * is triggered manually via submit() rather than on mount.
 */
export function useCreateConfig(): UseCreateConfig {
  const [state, setState] = useState<CreateConfigState>({ status: "idle" })

  async function submit(body: string): Promise<string | null> {
    setState({ status: "submitting" })
    try {
      const configId = await api.postConfig(body)
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
