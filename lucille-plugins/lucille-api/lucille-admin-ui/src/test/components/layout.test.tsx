// Tests Layout's API-down gate: one error screen replaces the page, the sidebar docs link stays.
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest"
import { render, screen } from "@testing-library/react"
import { MemoryRouter, Route, Routes } from "react-router-dom"
import Layout from "@/components/layout/layout"
import { HealthProvider } from "@/hooks/health-provider"
import { DOCS_URL } from "@/lib/constants"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

// Sidebar reads auth state; the Layout gate doesn't care, so stub it as "no auth required".
vi.mock("@/lib/auth-context", () => ({
  useAuth: () => ({ status: "authenticated", authRequired: false, login: vi.fn(), logout: vi.fn() }),
}))

function renderLayout() {
  return render(
    <MemoryRouter>
      <HealthProvider>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<div>page content</div>} />
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

    expect(await screen.findByText("Can't reach the Lucille API")).toBeInTheDocument()
    expect(screen.queryByText("page content")).not.toBeInTheDocument()
    expect(screen.getAllByRole("alert")).toHaveLength(1)
    expect(screen.getByRole("link", { name: /documentation/i })).toHaveAttribute("href", DOCS_URL)
  })
})
