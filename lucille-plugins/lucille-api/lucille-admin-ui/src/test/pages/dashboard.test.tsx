// Tests the Dashboard page: health status, resource stats, config/run counts, and per-card error states.
import { describe, it, expect } from "vitest"
import { screen } from "@testing-library/react"
import Dashboard from "@/pages/dashboard/dashboard"
import { HealthProvider } from "@/hooks/health-provider"
import { renderWithRouter } from "@/test/helpers/render"
import { mockFetch } from "@/test/helpers/fetch-mock"
import type { SystemStats } from "@/types/api"

const stats: SystemStats = {
  cpu: { percent: 12, used: 1, available: 7, total: 8, loadAverage: 0.5 },
  ram: { total: 16, available: 8, used: 8, percent: 50 },
  jvm: { total: 4, free: 1, used: 3, percent: 75 },
  storage: { total: 100, available: 40, used: 60, percent: 60 },
}

function healthyRoutes(overrides: Record<string, unknown> = {}) {
  return {
    "/v1/livez": { status: 200 },
    "/v1/readyz": { status: 200 },
    "/v1/systemstats": { json: stats },
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
  // With everything up, the page shows Healthy plus the storage/RAM/CPU/JVM percentages.
  it("shows healthy API status and the resource stats", async () => {
    mockFetch(healthyRoutes())

    renderWithRouter(<HealthProvider><Dashboard /></HealthProvider>)

    expect(await screen.findByText("Healthy")).toBeInTheDocument()
    expect(screen.getByText("60%")).toBeInTheDocument()
    expect(screen.getByText("50%")).toBeInTheDocument()
    expect(screen.getByText("12%")).toBeInTheDocument()
    expect(screen.getByText("75%")).toBeInTheDocument()
  })

  // Config/run cards show the totals (and the active-run count) from the API.
  it("shows config and run counts on success", async () => {
    mockFetch(healthyRoutes())

    renderWithRouter(<HealthProvider><Dashboard /></HealthProvider>)

    expect(await screen.findByText("1 Active")).toBeInTheDocument()
    expect(screen.getAllByText("2")).toHaveLength(2)
  })

  // A failing systemstats call shows a per-card error without taking down the page.
  it("shows an error message when systemstats fails", async () => {
    mockFetch(healthyRoutes({ "/v1/systemstats": { status: 500, json: { message: "boom" } } }))

    renderWithRouter(<HealthProvider><Dashboard /></HealthProvider>)

    const errors = await screen.findAllByText("An error has occurred")
    expect(errors.length).toBeGreaterThan(0)
  })
})
