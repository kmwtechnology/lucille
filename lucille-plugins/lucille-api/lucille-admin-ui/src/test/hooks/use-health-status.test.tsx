// Tests HealthProvider + useHealthStatus: healthy / notready / unavailable from the livez and readyz checks,
// including immediate escalation via reportFailure and recovery once the API returns.
import type { ReactNode } from "react"
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest"
import { act, renderHook, waitFor } from "@testing-library/react"
import { HealthProvider } from "@/hooks/health-provider"
import { useHealth, useHealthStatus } from "@/hooks/use-health-status"
import { HEALTH_POLL_INTERVAL_MS, HEALTH_PROBE_TIMEOUT_MS } from "@/lib/constants"
import { mockFetch, mockFetchHang, mockFetchReject } from "@/test/helpers/fetch-mock"

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
    await advanceOnePoll()

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })

  // A network error (API unreachable) is treated as unavailable, not a crash.
  it("reports unavailable when the request throws", async () => {
    mockFetchReject()

    const { result } = renderHook(() => useHealthStatus(), { wrapper })
    await advanceOnePoll()

    await waitFor(() => expect(result.current).toBe("unavailable"))
  })

  // A hung API (accepts connections, never answers) is caught by the probe timeout.
  it("reports unavailable when the API hangs", async () => {
    const fetchMock = mockFetchHang()

    const { result } = renderHook(() => useHealthStatus(), { wrapper })

    // First probe times out (one failure), the next poll's probe times out too.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(HEALTH_POLL_INTERVAL_MS + HEALTH_PROBE_TIMEOUT_MS)
    })

    expect(result.current).toBe("unavailable")
    // Each poll started a fresh probe rather than waiting forever on the first one.
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(2)
  })

  // A slow but responsive API (answers before the deadline) is not treated as hung.
  it("does not fail a probe that answers before the timeout", async () => {
    vi.stubGlobal("fetch", async () => {
      await new Promise((resolve) => setTimeout(resolve, HEALTH_PROBE_TIMEOUT_MS - 500))
      return new Response("", { status: 200 })
    })

    const { result } = renderHook(() => useHealthStatus(), { wrapper })
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2 * (HEALTH_PROBE_TIMEOUT_MS - 500))
    })

    expect(result.current).toBe("healthy")
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
    let reported: string | undefined
    await act(async () => {
      reported = await result.current.reportFailure()
    })

    expect(reported).toBe("unavailable")
    expect(result.current.status).toBe("unavailable")
  })

  // A probe that began before the outage can't vouch for a failure seen after it, so
  // reportFailure must run its own probe rather than reuse the in-flight one.
  it("reportFailure does not reuse a probe that started before the failure", async () => {
    let releaseLivez: () => void = () => {}
    let apiDown = false
    mockFetch({
      "/v1/livez": () => (apiDown ? { status: 500 } : { status: 200 }),
      "/v1/readyz": { status: 200 },
    })
    const original = globalThis.fetch
    // Hold the first livez response so the initial probe is still in flight.
    const gate = new Promise<void>((resolve) => (releaseLivez = resolve))
    let first = true
    vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
      const response = original(input, init) // resolve the mock now, while the API is still up
      if (first && String(input).includes("/v1/livez")) {
        first = false
        await gate
      }
      return response
    })

    const { result } = renderHook(() => useHealth(), { wrapper })

    let reported: string | undefined
    await act(async () => {
      const pending = result.current.reportFailure()
      releaseLivez() // the pre-outage probe now answers healthy...
      apiDown = true // ...but the API is down for the probe that follows
      reported = await pending
    })

    expect(reported).toBe("unavailable")
    expect(result.current.status).toBe("unavailable")
  })

  // Polling continues while down, so the status recovers on its own when the API returns.
  it("recovers to healthy when the API comes back", async () => {
    mockFetchReject()
    const { result } = renderHook(() => useHealthStatus(), { wrapper })
    await advanceOnePoll()
    await waitFor(() => expect(result.current).toBe("unavailable"))

    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
    })
    await advanceOnePoll()

    await waitFor(() => expect(result.current).toBe("healthy"))
  })
})
