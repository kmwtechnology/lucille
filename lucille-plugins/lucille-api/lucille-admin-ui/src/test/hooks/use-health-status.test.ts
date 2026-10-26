import { describe, it, expect } from "vitest"
import { renderHook, waitFor } from "@testing-library/react"
import { useHealthStatus } from "@/hooks/use-health-status"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

describe("useHealthStatus", () => {
  it("reports healthy when livez and readyz both succeed", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
    })

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("healthy"))
  })

  it("reports notready when livez succeeds but readyz fails", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 503 },
    })

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("notready"))
  })

  it("reports unavailable when livez fails", async () => {
    mockFetch({
      "/v1/livez": { status: 500 },
      "/v1/readyz": { status: 200 },
    })

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })

  it("reports unavailable when the request throws", async () => {
    mockFetchReject()

    const { result } = renderHook(() => useHealthStatus())

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })
})
