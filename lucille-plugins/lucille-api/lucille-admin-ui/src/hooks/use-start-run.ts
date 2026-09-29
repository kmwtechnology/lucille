import { useState } from "react"
import { api } from "@/lib/api"

export type StartRunState =
  | { status: "idle" }
  | { status: "submitting" }
  | { status: "success"; runId: string }
  | { status: "error"; error: string }

export interface UseStartRun {
  state: StartRunState
  /** POST a run for the given configId. Returns the new runId, or null on failure. */
  submit: (configId: string) => Promise<string | null>
  reset: () => void
}

/**
 * Hook for starting a run via POST /v1/run. Triggered manually via submit().
 */
export function useStartRun(): UseStartRun {
  const [state, setState] = useState<StartRunState>({ status: "idle" })

  async function submit(configId: string): Promise<string | null> {
    setState({ status: "submitting" })
    try {
      const runId = await api.startRun(configId)
      setState({ status: "success", runId })
      return runId
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
