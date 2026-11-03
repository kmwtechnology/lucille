// Tests the useStartRun hook: idle -> success/error states and the returned runId.
import { describe, it, expect } from "vitest"
import { act, renderHook } from "@testing-library/react"
import { useStartRun } from "@/hooks/use-start-run"
import { mockFetch } from "@/test/helpers/fetch-mock"

describe("useStartRun", () => {
  // No run started yet -> idle.
  it("starts idle", () => {
    const { result } = renderHook(() => useStartRun())
    expect(result.current.state.status).toBe("idle")
  })

  // A successful start resolves with the runId and leaves success state.
  it("transitions to success and returns the runId", async () => {
    mockFetch({ "/v1/run": { json: { runId: "run-7" } } })
    const { result } = renderHook(() => useStartRun())

    let returned: string | null = null
    await act(async () => {
      returned = await result.current.submit("cfg-1")
    })

    expect(returned).toBe("run-7")
    expect(result.current.state).toEqual({ status: "success", runId: "run-7" })
  })

  // A failed start (e.g. already running) resolves null and surfaces the error.
  it("transitions to error and returns null on failure", async () => {
    mockFetch({ "/v1/run": { status: 409, json: { message: "busy" } } })
    const { result } = renderHook(() => useStartRun())

    let returned: string | null = "unset"
    await act(async () => {
      returned = await result.current.submit("cfg-1")
    })

    expect(returned).toBeNull()
    expect(result.current.state).toEqual({ status: "error", error: "busy" })
  })
})
