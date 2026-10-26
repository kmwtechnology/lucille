import { describe, it, expect } from "vitest"
import { screen } from "@testing-library/react"
import RunDetail from "@/pages/run-detail/run-detail"
import { renderWithRouter } from "@/test/helpers/render"
import { mockFetch, mockFetchReject } from "@/test/helpers/fetch-mock"

function renderAt(id: string) {
  return renderWithRouter(<RunDetail />, {
    path: "/runs/:id",
    route: `/runs/${id}`,
  })
}

describe("RunDetail page", () => {
  it("shows the error state when the run cannot be loaded", async () => {
    mockFetchReject()

    renderAt("missing")

    expect(await screen.findByText("Run not found")).toBeInTheDocument()
  })

  it("renders run id, status, details, and result on success", async () => {
    mockFetch({
      "/v1/run/": {
        json: {
          runId: "run-42",
          configId: "cfg-1",
          startTime: 1_700_000_000,
          endTime: 1_700_000_030,
          runResult: { status: true, message: "all good", runId: "run-42" },
          runType: "FULL",
          done: true,
        },
      },
    })

    renderAt("run-42")

    expect(await screen.findByText("run-42")).toBeInTheDocument()
    expect(screen.getByText("Succeeded")).toBeInTheDocument()
    expect(screen.getByText("cfg-1")).toBeInTheDocument()
    expect(screen.getByText("all good")).toBeInTheDocument()
  })

  it("shows a Running badge for an in-progress run", async () => {
    mockFetch({
      "/v1/run/": {
        json: {
          runId: "run-99",
          configId: "cfg-1",
          startTime: 1_700_000_000,
          endTime: null,
          runResult: null,
          runType: null,
          done: false,
        },
      },
    })

    renderAt("run-99")

    expect(await screen.findByText("run-99")).toBeInTheDocument()
    expect(screen.getByText("Running")).toBeInTheDocument()
  })
})
