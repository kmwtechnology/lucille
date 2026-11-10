// Tests the Dashboard page: config/run counts and per-card error states.
import { describe, it, expect } from "vitest"
import { screen } from "@testing-library/react"
import Dashboard from "@/pages/dashboard/dashboard"
import { HealthProvider } from "@/hooks/health-provider"
import { renderWithRouter } from "@/test/helpers/render"
import { mockFetch } from "@/test/helpers/fetch-mock"

function healthyRoutes(overrides: Record<string, unknown> = {}) {
  return {
    "/v1/livez": { status: 200 },
    "/v1/readyz": { status: 200 },
    "/v1/config": { json: { "cfg-1": {}, "cfg-2": {} } },
    "/v1/run": {
      json: [
        { runId: "r1", configId: "c", startTime: null, endTime: null, runResult: null, runType: null, done: true },
        { runId: "r2", configId: "c", startTime: null, endTime: null, runResult: null, runType: null, done: false },
      ],
    },
    ...overrides,
  }
}

describe("Dashboard page", () => {
  // Config/run cards show the totals (and the active-run count) from the API.
  it("shows config and run counts on success", async () => {
    mockFetch(healthyRoutes())
    renderWithRouter(<HealthProvider><Dashboard /></HealthProvider>)

    expect(await screen.findByText("1 Active")).toBeInTheDocument()
    expect(screen.getAllByText("2")).toHaveLength(2)
  })

  // A failing config call shows a per-card error without taking down the page.
  it("shows an error message when the config list fails", async () => {
    mockFetch(healthyRoutes({ "/v1/config": { status: 500, json: { message: "boom" } } }))
    renderWithRouter(<HealthProvider><Dashboard /></HealthProvider>)

    expect(await screen.findByText("An error has occurred")).toBeInTheDocument()
    expect(screen.getByText("Unable to load configuration data")).toBeInTheDocument()
    expect(await screen.findByText("1 Active")).toBeInTheDocument()
  })
})
