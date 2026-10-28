import { describe, it, expect } from "vitest"
import { screen } from "@testing-library/react"
import Dashboard from "@/pages/dashboard/dashboard"
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
  it("shows healthy API status and the resource stats", async () => {
    mockFetch(healthyRoutes())

    renderWithRouter(<Dashboard />)

    expect(await screen.findByText("Healthy")).toBeInTheDocument()
    expect(screen.getByText("60%")).toBeInTheDocument()
    expect(screen.getByText("50%")).toBeInTheDocument()
    expect(screen.getByText("12%")).toBeInTheDocument()
    expect(screen.getByText("75%")).toBeInTheDocument()
  })

  it("shows config and run counts on success", async () => {
    mockFetch(healthyRoutes())

    renderWithRouter(<Dashboard />)

    expect(await screen.findByText("1 Active")).toBeInTheDocument()
    expect(screen.getAllByText("2")).toHaveLength(2)
  })

  it("shows Unhealthy when the liveness check fails", async () => {
    mockFetch(healthyRoutes({ "/v1/livez": { status: 500 } }))

    renderWithRouter(<Dashboard />)

    expect(await screen.findByText("Unhealthy")).toBeInTheDocument()
  })

  it("shows an error message when systemstats fails", async () => {
    mockFetch(healthyRoutes({ "/v1/systemstats": { status: 500, json: { message: "boom" } } }))

    renderWithRouter(<Dashboard />)

    const errors = await screen.findAllByText("An error has occurred")
    expect(errors.length).toBeGreaterThan(0)
  })
})
