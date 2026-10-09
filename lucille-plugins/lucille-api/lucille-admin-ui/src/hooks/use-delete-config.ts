import { useState } from "react"
import { api } from "@/lib/api"

export type DeleteConfigState =
  | { status: "idle" }
  | { status: "deleting" }
  | { status: "error"; error: string }

export interface UseDeleteConfig {
  state: DeleteConfigState
  remove: (configId: string) => Promise<boolean>
  reset: () => void
}

/** Hook for deleting a config via DELETE /v1/config/{id}. */
export function useDeleteConfig(): UseDeleteConfig {
  const [state, setState] = useState<DeleteConfigState>({ status: "idle" })

  async function remove(configId: string): Promise<boolean> {
    setState({ status: "deleting" })
    try {
      await api.deleteConfig(configId)
      setState({ status: "idle" })
      return true
    } catch (err) {
      setState({
        status: "error",
        error: err instanceof Error ? err.message : "Unknown error",
      })
      return false
    }
  }

  function reset() {
    setState({ status: "idle" })
  }

  return { state, remove, reset }
}
