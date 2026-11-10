// Tests HealthProvider + useHealthStatus: healthy / notready / unavailable from the livez and readyz checks,
// including immediate escalation via reportFailure and recovery once the API returns.
import type { ReactNode } from "react"
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest"
import { act, renderHook, waitFor } from "@testing-library/react"
import { HealthProvider } from "@/hooks/health-provider"
import { useHealth, useHealthStatus } from "@/hooks/use-health-status"
import { HEALTH_POLL_INTERVAL_MS } from "@/lib/constants"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

function wrapper({ children }: { children: ReactNode }) {
  return <HealthProvider>{children}</HealthProvider>
}

/** Let the initial probe and the next poll (the second consecutive failure) run. */
async function advanceOnePoll() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(HEALTH_POLL_INTERVAL_MS)
  })
}

describe("useHealthStatus", () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // Both probes OK -> the API is up and ready.
  it("reports healthy when livez and readyz both succeed", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
    })

    const { result } = renderHook(() => useHealthStatus(), { wrapper })

    await waitFor(() => expect(result.current).toBe("healthy"))
  })

  // Alive but not ready -> process is up but can't serve yet (distinct from down).
  it("reports notready when livez succeeds but readyz fails", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 503 },
    })

    const { result } = renderHook(() => useHealthStatus(), { wrapper })

    await waitFor(() => expect(result.current).toBe("notready"))
  })

  // Liveness failing short-circuits to unavailable; readyz isn't consulted.
  it("reports unavailable when livez fails", async () => {
    mockFetch({
      "/v1/livez": { status: 500 },
      "/v1/readyz": { status: 200 },
    })

    const { result } = renderHook(() => useHealthStatus(), { wrapper })

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })

  // A network error (API unreachable) is treated as unavailable, not a crash.
  it("reports unavailable when the request throws", async () => {
    mockFetchReject()

    const { result } = renderHook(() => useHealthStatus(), { wrapper })

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })

  // A failed request elsewhere triggers an immediate probe instead of waiting for the next poll.
  it("reportFailure escalates to unavailable without waiting for the next poll", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
    })
    const { result } = renderHook(() => useHealth(), { wrapper })
    await waitFor(() => expect(result.current.status).toBe("healthy"))

    mockFetchReject()
    act(() => result.current.reportFailure())

    await waitFor(() => expect(result.current.status).toBe("unavailable"))
  })

  // Polling continues while down, so the status recovers on its own when the API returns.
  it("recovers to healthy when the API comes back", async () => {
    mockFetchReject()
    const { result } = renderHook(() => useHealthStatus(), { wrapper })
    await waitFor(() => expect(result.current).toBe("unavailable"))

    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
    })
    await advanceOnePoll()

    await waitFor(() => expect(result.current).toBe("healthy"))
  })
})
