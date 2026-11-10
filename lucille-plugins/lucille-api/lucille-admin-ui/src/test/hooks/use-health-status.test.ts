// Tests the useHealthStatus hook: healthy / notready / unavailable from the livez and readyz checks.
import { describe, it, expect } from "vitest"
import { renderHook, waitFor } from "@testing-library/react"
import { useHealthStatus } from "@/hooks/use-health-status"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

describe("useHealthStatus", () => {
  // Both probes OK -> the API is up and ready.
  it("reports healthy when livez and readyz both succeed", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
    })

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("healthy"))
  })

  // Alive but not ready -> process is up but can't serve yet (distinct from down).
  it("reports notready when livez succeeds but readyz fails", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 503 },
    })

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("notready"))
  })

  // Liveness failing short-circuits to unavailable; readyz isn't consulted.
  it("reports unavailable when livez fails", async () => {
    mockFetch({
      "/v1/livez": { status: 500 },
      "/v1/readyz": { status: 200 },
    })

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })

  // A network error (API unreachable) is treated as unavailable, not a crash.
  it("reports unavailable when the request throws", async () => {
    mockFetchReject()

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })
})
