// Tests useFetch's outage handling: possible-outage errors are held until a health probe answers,
// so widgets never show their own error when the API is down.
import type { ReactNode } from "react"
import { describe, it, expect } from "vitest"
import { renderHook, waitFor } from "@testing-library/react"
import { HealthProvider } from "@/hooks/health-provider"
import { useFetch } from "@/hooks/use-fetch"
import { useHealthStatus } from "@/hooks/use-health-status"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

function wrapper({ children }: { children: ReactNode }) {
  return <HealthProvider>{children}</HealthProvider>
}

/** Render useFetch alongside the health status so tests can wait for the probe to settle. */
function renderFetch(path: string) {
  return renderHook(() => ({ fetch: useFetch(path), health: useHealthStatus() }), { wrapper })
}

describe("useFetch", () => {
  // The Vite dev proxy answers 500 for everything when the API is down.
  it("never surfaces an error when a 500 coincides with the API being down", async () => {
    mockFetch({
      "/v1/livez": { status: 500 },
      "/v1/systemstats": { status: 500, json: { message: "proxy error" } },
    })

    const { result } = renderFetch("/v1/systemstats")

    await waitFor(() => expect(result.current.health).toBe("unavailable"))
    expect(result.current.fetch.status).toBe("loading")
  })

  // Connection refused (served from the API jar) behaves the same way.
  it("never surfaces an error on a network failure while the API is down", async () => {
    mockFetchReject()

    const { result } = renderFetch("/v1/systemstats")

    await waitFor(() => expect(result.current.health).toBe("unavailable"))
    expect(result.current.fetch.status).toBe("loading")
  })

  // API is up but this endpoint fails: the error shows once the probe says the API is healthy.
  it("surfaces a 500 when the API is healthy", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
      "/v1/systemstats": { status: 500, json: { message: "boom" } },
    })

    const { result } = renderFetch("/v1/systemstats")

    await waitFor(() => expect(result.current.fetch).toEqual({ status: "error", error: "boom" }))
  })

  // Client errors aren't outages, so they surface without consulting health.
  it("surfaces a 4xx error immediately", async () => {
    mockFetch({
      "/v1/livez": { status: 500 },
      "/v1/config/missing": { status: 404, json: { message: "not found" } },
    })

    const { result } = renderFetch("/v1/config/missing")

    await waitFor(() => expect(result.current.fetch).toEqual({ status: "error", error: "not found" }))
  })
})
