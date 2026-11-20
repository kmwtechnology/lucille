// Tests Layout's API-down gate: one error screen replaces the page, the sidebar docs link stays.
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest"
import type { ReactElement } from "react"
import { act, render, screen } from "@testing-library/react"
import { MemoryRouter, Route, Routes } from "react-router-dom"
import Layout from "@/components/layout/layout"
import Dashboard from "@/pages/dashboard/dashboard"
import { HealthProvider } from "@/hooks/health-provider"
import {
  DOCS_URL,
  HEALTH_POLL_INTERVAL_MS,
  HEALTH_PROBE_TIMEOUT_MS,
} from "@/lib/constants"
import { mockFetch, mockFetchHang, mockFetchReject } from "@/test/helpers/fetch-mock"

// Sidebar reads auth state; the Layout gate doesn't care, so stub it as "no auth required".
vi.mock("@/lib/auth-context", () => ({
  useAuth: () => ({ status: "authenticated", authRequired: false, login: vi.fn(), logout: vi.fn() }),
}))

function renderLayout(page: ReactElement = <div>page content</div>) {
  return render(
    <MemoryRouter>
      <HealthProvider>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={page} />
          </Route>
        </Routes>
      </HealthProvider>
    </MemoryRouter>,
  )
}

describe("Layout", () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  // Healthy API: the routed page renders and no error screen is shown.
  it("renders the page when the API is healthy", async () => {
    mockFetch({ "/v1/livez": { status: 200 }, "/v1/readyz": { status: 200 } })

    renderLayout()

    expect(await screen.findByText("page content")).toBeInTheDocument()
    expect(screen.queryByRole("alert")).not.toBeInTheDocument()
  })

  // API down: a single error screen replaces the page, and the sidebar docs link remains.
  it("shows one error screen and keeps the docs link when the API is unreachable", async () => {
    mockFetchReject()

    renderLayout()
    await act(async () => {
      await vi.advanceTimersByTimeAsync(HEALTH_POLL_INTERVAL_MS)
    })

    expect(await screen.findByText("Can't reach the Lucille API")).toBeInTheDocument()
    expect(screen.queryByText("page content")).not.toBeInTheDocument()
    expect(screen.getAllByRole("alert")).toHaveLength(1)
    expect(screen.getByRole("link", { name: /documentation/i })).toHaveAttribute("href", DOCS_URL)
  })

  // The API shuts down while the dashboard is open. Via the Vite dev proxy every request then
  // returns 500. No per-card error may render at any point before the error screen takes over.
  it("never shows per-card errors when the API shuts down mid-session", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
      "/v1/config": { json: {} },
      "/v1/run": { json: [] },
    })
    const { container } = renderLayout(<Dashboard />)
    expect(await screen.findByText("Total Configs")).toBeInTheDocument()

    let sawCardError = false
    const observer = new MutationObserver(() => {
      if (container.textContent?.includes("An error has occurred")) sawCardError = true
    })
    observer.observe(container, { childList: true, subtree: true, characterData: true })

    mockFetch({ "/v1": { status: 500, body: "proxy error" } })
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2 * HEALTH_POLL_INTERVAL_MS)
    })

    expect(await screen.findByText("Can't reach the Lucille API")).toBeInTheDocument()
    observer.disconnect()
    expect(sawCardError).toBe(false)
  })

  // The API hangs (accepts connections, never answers) while the dashboard is open. Widget
  // fetches never settle, so only the probe timeout can detect it.
  it("switches to the error screen when the API hangs mid-session", async () => {
    mockFetch({
      "/v1/livez": { status: 200 },
      "/v1/readyz": { status: 200 },
      "/v1/config": { json: {} },
      "/v1/run": { json: [] },
    })
    const { container } = renderLayout(<Dashboard />)
    expect(await screen.findByText("Total Configs")).toBeInTheDocument()

    mockFetchHang()
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2 * HEALTH_POLL_INTERVAL_MS + HEALTH_PROBE_TIMEOUT_MS)
    })

    expect(screen.getByText("Can't reach the Lucille API")).toBeInTheDocument()
    expect(container.textContent).not.toContain("An error has occurred")
  })
})
